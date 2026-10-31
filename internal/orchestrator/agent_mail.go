package orchestrator

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type GetMailIdentityInput struct{}

type SetMailIdentityInput struct {
	Address string `json:"address,omitempty"`
	Name    string `json:"name"`
}

type ListMailboxesInput struct {
	Host            string `json:"host,omitempty"`
	OnlineOnly      bool   `json:"onlineOnly,omitempty"`
	IncludeReleased bool   `json:"includeReleased,omitempty"`
	Workspace       string `json:"workspace,omitempty"`
}

type ListMailboxesResult struct {
	Mailboxes []protocol.AgentMailbox `json:"mailboxes"`
}

type SendMailInput struct {
	IdempotencyKey string   `json:"idempotencyKey"`
	From           string   `json:"from,omitempty"`
	To             string   `json:"to,omitempty"`
	ReplyToID      string   `json:"replyToId,omitempty"`
	Subject        string   `json:"subject,omitempty"`
	Body           string   `json:"body"`
	ArtifactIDs    []string `json:"artifactIds,omitempty"`
	Wake           *bool    `json:"wake,omitempty"`
}

type SendMailResult struct {
	Message   protocol.MailMessage  `json:"message"`
	Recipient protocol.AgentMailbox `json:"recipient"`
	Replayed  bool                  `json:"replayed"`
}

type ReadMailInput struct {
	Address       string `json:"address,omitempty"`
	Box           string `json:"box,omitempty"`
	UnreadOnly    bool   `json:"unreadOnly,omitempty"`
	AfterSequence int64  `json:"afterSequence,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	MarkRead      *bool  `json:"markRead,omitempty"`
}

type ReadMailResult struct {
	Mailbox  protocol.AgentMailbox  `json:"mailbox"`
	Messages []protocol.MailMessage `json:"messages"`
	HasMore  bool                   `json:"hasMore"`
}

type WaitForMailInput struct {
	TimeoutMilliseconds int   `json:"timeoutMilliseconds,omitempty"`
	WaitMilliseconds    int   `json:"waitMs,omitempty"`
	Limit               int   `json:"limit,omitempty"`
	AfterSequence       int64 `json:"afterSequence,omitempty"`
	MarkRead            *bool `json:"markRead,omitempty"`
}

type WaitForMailResult struct {
	Mailbox  protocol.AgentMailbox  `json:"mailbox"`
	Messages []protocol.MailMessage `json:"messages"`
	TimedOut bool                   `json:"timedOut"`
}

func (service *Service) subscribeMail() (<-chan protocol.MailMessage, func()) {
	subscriberID := service.mailSubscriberSequence.Add(1)
	channel := make(chan protocol.MailMessage, 64)
	service.mailSubscriberMutex.Lock()
	service.mailSubscribers[subscriberID] = channel
	service.mailSubscriberMutex.Unlock()

	unsubscribe := func() {
		service.mailSubscriberMutex.Lock()
		if existing, found := service.mailSubscribers[subscriberID]; found {
			delete(service.mailSubscribers, subscriberID)
			close(existing)
		}
		service.mailSubscriberMutex.Unlock()
	}
	return channel, unsubscribe
}

func (service *Service) broadcastMail(message protocol.MailMessage) {
	service.mailSubscriberMutex.Lock()
	defer service.mailSubscriberMutex.Unlock()
	for _, channel := range service.mailSubscribers {
		select {
		case channel <- message:
		default:
		}
	}
}

func (service *Service) closeMailSubscribers() {
	service.mailSubscriberMutex.Lock()
	defer service.mailSubscriberMutex.Unlock()
	for subscriberID, channel := range service.mailSubscribers {
		delete(service.mailSubscribers, subscriberID)
		close(channel)
	}
}

func (service *Service) defaultMasterHost() string {
	if hostname, err := os.Hostname(); err == nil && strings.TrimSpace(hostname) != "" {
		return protocol.NormalizeHostnameSlug(hostname)
	}
	return "local"
}

func (service *Service) RegisterClientMailbox(
	clientID string,
	sessionID string,
	registration *protocol.ClientMailRegistration,
) (protocol.AgentMailbox, error) {
	var mailbox protocol.AgentMailbox
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		nowText := service.nowUTC()
		mailbox = service.upsertClientMailboxInState(state, clientID, sessionID, registration, nowText)
		return nil
	})
	return mailbox, err
}

func (service *Service) DisconnectClientMailbox(sessionID string) {
	if sessionID == "" {
		return
	}
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		nowText := service.nowUTC()
		for mailboxID, mailbox := range state.Mailboxes {
			if mailbox.Kind == "mcp-session" && mailbox.SessionID == sessionID && mailbox.Status == "online" {
				mailbox.Status = "offline"
				mailbox.UpdatedAt = nowText
				mailbox.LastSeenAt = nowText
				state.Mailboxes[mailboxID] = mailbox
			}
		}
		return nil
	})
}

func (service *Service) GetMailIdentity(caller CallerIdentity) (protocol.AgentMailbox, error) {
	var mailbox protocol.AgentMailbox
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		nowText := service.nowUTC()
		resolved, resolveErr := service.ensureCallerMailboxInState(state, caller, "", nowText)
		if resolveErr != nil {
			return resolveErr
		}
		mailbox = resolved
		return nil
	})
	return mailbox, err
}

func (service *Service) SetMailIdentity(
	caller CallerIdentity,
	input SetMailIdentityInput,
) (protocol.AgentMailbox, error) {
	rawName := strings.TrimSpace(input.Name)
	if rawName == "" {
		return protocol.AgentMailbox{}, invalidRequest("name is required")
	}

	var updated protocol.AgentMailbox
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		nowText := service.nowUTC()

		var target protocol.AgentMailbox
		if strings.TrimSpace(input.Address) != "" {
			if caller.Kind != "operator" && caller.Kind != "orchestrator-client" {
				return forbiddenError("worker run can only rename its own mailbox")
			}
			resolvedTarget, found := resolveMailboxInState(state, input.Address)
			if !found {
				return notFound(fmt.Sprintf("mailbox %q does not exist", input.Address))
			}
			target = resolvedTarget
		} else {
			resolvedCaller, resolveErr := service.ensureCallerMailboxInState(state, caller, "", nowText)
			if resolveErr != nil {
				return resolveErr
			}
			target = resolvedCaller
		}

		newHandle, newHost, newAddress, parseErr := protocol.NormalizeRenameTarget(rawName, target.Host)
		if parseErr != nil {
			return invalidRequest(parseErr.Error())
		}
		if newHost != target.Host && caller.Kind != "operator" {
			return invalidRequest(fmt.Sprintf("cannot change mailbox host from %q to %q; pass a handle name without '@'", target.Host, newHost))
		}

		if newAddress == target.Address {
			target.CustomHandle = true
			target.UpdatedAt = nowText
			target.LastSeenAt = nowText
			state.Mailboxes[target.ID] = target
			updated = target
			return nil
		}

		for otherID, other := range state.Mailboxes {
			if otherID == target.ID {
				continue
			}
			if other.Address == newAddress {
				if isActiveMailboxStatus(other.Status) {
					return conflictError("address_in_use", fmt.Sprintf("mail address %q is already in use by an active agent", newAddress))
				}
				return conflictError("address_in_use", fmt.Sprintf("mail address %q is already assigned to another mailbox", newAddress))
			}
		}

		// Remove newAddress from any stale alias list on other mailboxes so routing is unambiguous.
		for otherID, other := range state.Mailboxes {
			if otherID == target.ID || len(other.Aliases) == 0 {
				continue
			}
			filtered := other.Aliases[:0]
			changed := false
			for _, alias := range other.Aliases {
				if alias == newAddress {
					changed = true
					continue
				}
				filtered = append(filtered, alias)
			}
			if changed {
				other.Aliases = slices.Clone(filtered)
				state.Mailboxes[otherID] = other
			}
		}

		previousAddress := target.Address
		if previousAddress != "" && previousAddress != newAddress && !slices.Contains(target.Aliases, previousAddress) {
			target.Aliases = append(target.Aliases, previousAddress)
		}
		target.Aliases = slices.DeleteFunc(target.Aliases, func(alias string) bool {
			return alias == newAddress
		})
		target.Handle = newHandle
		target.Host = newHost
		target.Address = newAddress
		target.CustomHandle = true
		target.UpdatedAt = nowText
		target.LastSeenAt = nowText
		state.Mailboxes[target.ID] = target
		updated = target
		return nil
	})
	return updated, err
}

func (service *Service) ListMailboxes(input ListMailboxesInput) (ListMailboxesResult, error) {
	hostFilter := strings.ToLower(strings.TrimSpace(input.Host))
	workspaceFilter := strings.TrimSpace(input.Workspace)

	var mailboxes []protocol.AgentMailbox
	err := service.store.Read(func(state *store.State) error {
		for _, mailbox := range state.Mailboxes {
			if !input.IncludeReleased && mailbox.Status == "released" {
				continue
			}
			if input.OnlineOnly && !isActiveMailboxStatus(mailbox.Status) {
				continue
			}
			if hostFilter != "" && mailbox.Host != hostFilter {
				continue
			}
			if workspaceFilter != "" && !strings.Contains(mailbox.Workspace, workspaceFilter) {
				continue
			}
			mailboxCopy := mailbox
			mailboxCopy.UnreadCount = countUnreadMailForMailbox(state, mailbox.ID)
			mailboxes = append(mailboxes, mailboxCopy)
		}
		return nil
	})
	if err != nil {
		return ListMailboxesResult{}, err
	}

	slices.SortFunc(mailboxes, func(left, right protocol.AgentMailbox) int {
		leftActive := isActiveMailboxStatus(left.Status)
		rightActive := isActiveMailboxStatus(right.Status)
		if leftActive != rightActive {
			if leftActive {
				return -1
			}
			return 1
		}
		if compared := cmp.Compare(left.Host, right.Host); compared != 0 {
			return compared
		}
		if compared := cmp.Compare(left.Handle, right.Handle); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})
	if mailboxes == nil {
		mailboxes = []protocol.AgentMailbox{}
	}
	return ListMailboxesResult{Mailboxes: mailboxes}, nil
}

func (service *Service) SendMail(
	caller CallerIdentity,
	input SendMailInput,
) (SendMailResult, error) {
	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" || !protocol.IsBoundedString(idempotencyKey, 128) {
		return SendMailResult{}, invalidRequest("idempotencyKey is required and must be at most 128 bytes")
	}
	body := strings.TrimSpace(input.Body)
	if body == "" || !protocol.IsBoundedString(body, protocol.TextMaximumBytes) {
		return SendMailResult{}, invalidRequest("body must be non-empty and within 64 KiB")
	}
	subject := strings.TrimSpace(input.Subject)
	if !protocol.IsBoundedString(subject, protocol.MailSubjectMaximumBytes) {
		return SendMailResult{}, invalidRequest(fmt.Sprintf("subject must be at most %d bytes", protocol.MailSubjectMaximumBytes))
	}

	wakeRequested := true
	if input.Wake != nil {
		wakeRequested = *input.Wake
	}

	sourceKey := caller.SourceKey()
	receiptKey := sourceKey + "\x00mail\x00" + idempotencyKey
	digest := canonicalDigest(input)

	var result SendMailResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		if existingReceipt, found := state.MailReceipts[receiptKey]; found {
			if existingReceipt.Digest != digest {
				return conflictError("idempotency_conflict", "idempotencyKey was already used for a different send_mail request")
			}
			existingMessage := state.MailMessages[existingReceipt.MessageID]
			recipient := state.Mailboxes[existingMessage.ToMailboxID]
			recipient.UnreadCount = countUnreadMailForMailbox(state, recipient.ID)
			result = SendMailResult{
				Message:   existingMessage,
				Recipient: recipient,
				Replayed:  true,
			}
			return nil
		}

		nowText := service.nowUTC()
		senderMailbox, senderErr := service.ensureCallerMailboxInState(state, caller, input.From, nowText)
		if senderErr != nil {
			return senderErr
		}

		targetQuery := strings.TrimSpace(input.To)
		var replyTarget *protocol.MailMessage
		if strings.TrimSpace(input.ReplyToID) != "" {
			original, exists := state.MailMessages[strings.TrimSpace(input.ReplyToID)]
			if !exists {
				return notFound(fmt.Sprintf("replyToId %q does not exist", input.ReplyToID))
			}
			replyTarget = &original
			if targetQuery == "" {
				targetQuery = original.FromAddress
			}
			if subject == "" && original.Subject != "" {
				if strings.HasPrefix(strings.ToLower(original.Subject), "re:") {
					subject = original.Subject
				} else {
					subject = "Re: " + original.Subject
				}
			}
		}
		if targetQuery == "" {
			return invalidRequest("to is required (for example \"claude1234@fabrica\") unless replyToId is specified")
		}

		recipientMailbox, found := resolveMailboxInState(state, targetQuery)
		if !found {
			return notFound(fmt.Sprintf("recipient mailbox %q does not exist; call list_mailboxes to inspect registered agents", targetQuery))
		}

		for _, artifactID := range input.ArtifactIDs {
			if _, exists := state.Artifacts[artifactID]; !exists {
				return invalidRequest(fmt.Sprintf("artifact %q does not exist", artifactID))
			}
		}

		sequence := nextMailSequenceForMailbox(state, recipientMailbox.ID)
		redactedSubject := service.redactor.Redact(subject)
		redactedBody := service.redactor.Redact(body)
		replyToID := ""
		if replyTarget != nil {
			replyToID = replyTarget.ID
		}

		message := protocol.MailMessage{
			ID:             protocol.NewIdentifier("mail"),
			Sequence:       sequence,
			SourceKey:      sourceKey,
			IdempotencyKey: idempotencyKey,
			Digest:         digest,
			FromMailboxID:  senderMailbox.ID,
			FromAddress:    senderMailbox.Address,
			ToMailboxID:    recipientMailbox.ID,
			ToAddress:      recipientMailbox.Address,
			ReplyToID:      replyToID,
			Subject:        redactedSubject,
			Body:           redactedBody,
			ArtifactIDs:    slices.Clone(input.ArtifactIDs),
			WakeRequested:  wakeRequested,
			Read:           false,
			CreatedAt:      nowText,
		}

		shouldPushToSession := false
		targetSessionID := ""
		shouldTriggerSchedule := false

		if wakeRequested {
			switch recipientMailbox.Kind {
			case "mcp-session":
				if recipientMailbox.Status == "online" && recipientMailbox.SessionID != "" && service.hasLiveOrchestratorSession(recipientMailbox.SessionID) {
					message.DeliveredPushAt = nowText
					shouldPushToSession = true
					targetSessionID = recipientMailbox.SessionID
				}
			case "instance":
				if recipientMailbox.InstanceID != "" {
					if instance, instanceExists := state.Instances[recipientMailbox.InstanceID]; instanceExists &&
						(instance.Status == "ready" || instance.Status == "idle") {
						wakeTaskID := service.createMailWakeTaskInState(state, instance, recipientMailbox, []protocol.MailMessage{message}, nowText)
						message.WakeDispatched = true
						message.WakeTaskID = wakeTaskID
						message.Read = true
						message.ReadAt = nowText
						shouldTriggerSchedule = true
					}
				}
			}
		}

		state.MailMessages[message.ID] = message
		state.MailReceipts[receiptKey] = protocol.MailReceipt{
			ID:             protocol.NewIdentifier("mrcpt"),
			SourceKey:      sourceKey,
			IdempotencyKey: idempotencyKey,
			Digest:         digest,
			MessageID:      message.ID,
			CreatedAt:      nowText,
		}

		recipientMailbox.UnreadCount = countUnreadMailForMailbox(state, recipientMailbox.ID)
		recipientMailbox.UpdatedAt = nowText
		state.Mailboxes[recipientMailbox.ID] = recipientMailbox

		result = SendMailResult{
			Message:   message,
			Recipient: recipientMailbox,
			Replayed:  false,
		}

		committedMessage := message
		recipientAddress := recipientMailbox.Address
		mutation.OnCommit(func() {
			service.broadcastMail(committedMessage)
			if shouldPushToSession && targetSessionID != "" {
				service.sendToOrchestratorSession(targetSessionID, protocol.OrchestratorServerMessage{
					Type: "mail.received",
					Mail: &protocol.MailNotification{
						MailboxAddress: recipientAddress,
						Message:        committedMessage,
						OccurredAt:     committedMessage.CreatedAt,
					},
					At: service.nowUTC(),
				})
			}
			if shouldTriggerSchedule {
				service.TriggerSchedule()
			}
		})
		return nil
	})
	return result, err
}

func (service *Service) ReadMail(
	caller CallerIdentity,
	input ReadMailInput,
) (ReadMailResult, error) {
	box := strings.ToLower(strings.TrimSpace(input.Box))
	if box == "" {
		box = "inbox"
	}
	if box != "inbox" && box != "sent" {
		return ReadMailResult{}, invalidRequest("box must be inbox or sent")
	}

	limit := input.Limit
	if limit <= 0 {
		limit = protocol.DefaultMailReadLimit
	}
	if limit > protocol.MaximumMailReadLimit {
		limit = protocol.MaximumMailReadLimit
	}

	markRead := box == "inbox"
	if input.MarkRead != nil {
		markRead = *input.MarkRead && box == "inbox"
	}

	var result ReadMailResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		nowText := service.nowUTC()
		var targetMailbox protocol.AgentMailbox
		if strings.TrimSpace(input.Address) != "" {
			if caller.Kind == "run" {
				callerMailbox, resolveErr := service.ensureCallerMailboxInState(state, caller, "", nowText)
				if resolveErr != nil {
					return resolveErr
				}
				resolved, found := resolveMailboxInState(state, input.Address)
				if !found {
					return notFound(fmt.Sprintf("mailbox %q does not exist", input.Address))
				}
				if resolved.ID != callerMailbox.ID {
					return forbiddenError("worker run can only read its own mailbox")
				}
				targetMailbox = resolved
			} else {
				resolved, found := resolveMailboxInState(state, input.Address)
				if !found {
					return notFound(fmt.Sprintf("mailbox %q does not exist", input.Address))
				}
				targetMailbox = resolved
			}
		} else {
			resolved, resolveErr := service.ensureCallerMailboxInState(state, caller, "", nowText)
			if resolveErr != nil {
				return resolveErr
			}
			targetMailbox = resolved
		}

		matched := collectMailMessages(state, targetMailbox.ID, box, input.UnreadOnly, input.AfterSequence)
		hasMore := false
		if len(matched) > limit {
			hasMore = true
			matched = matched[:limit]
		}

		if markRead && len(matched) > 0 {
			for index := range matched {
				if !matched[index].Read {
					matched[index].Read = true
					matched[index].ReadAt = nowText
					state.MailMessages[matched[index].ID] = matched[index]
				}
			}
		}

		targetMailbox.UnreadCount = countUnreadMailForMailbox(state, targetMailbox.ID)
		targetMailbox.LastSeenAt = nowText
		state.Mailboxes[targetMailbox.ID] = targetMailbox

		result = ReadMailResult{
			Mailbox:  targetMailbox,
			Messages: matched,
			HasMore:  hasMore,
		}
		return nil
	})
	return result, err
}

func (service *Service) WaitForMail(
	operationContext context.Context,
	caller CallerIdentity,
	input WaitForMailInput,
) (WaitForMailResult, error) {
	waitMilliseconds := input.TimeoutMilliseconds
	if waitMilliseconds == 0 && input.WaitMilliseconds > 0 {
		waitMilliseconds = input.WaitMilliseconds
	}
	if waitMilliseconds < 0 {
		waitMilliseconds = 0
	}
	if waitMilliseconds > protocol.MaximumWaitMilliseconds {
		waitMilliseconds = protocol.MaximumWaitMilliseconds
	}

	limit := input.Limit
	if limit <= 0 {
		limit = protocol.DefaultMailReadLimit
	}
	if limit > protocol.MaximumMailReadLimit {
		limit = protocol.MaximumMailReadLimit
	}

	markRead := true
	if input.MarkRead != nil {
		markRead = *input.MarkRead
	}

	mailChannel, unsubscribe := service.subscribeMail()
	defer unsubscribe()

	targetMailbox, err := service.GetMailIdentity(caller)
	if err != nil {
		return WaitForMailResult{}, err
	}

	collectPending := func() (WaitForMailResult, error) {
		readResult, readErr := service.ReadMail(caller, ReadMailInput{
			Box:           "inbox",
			UnreadOnly:    input.AfterSequence == 0,
			AfterSequence: input.AfterSequence,
			Limit:         limit,
			MarkRead:      &markRead,
		})
		if readErr != nil {
			return WaitForMailResult{}, readErr
		}
		return WaitForMailResult{
			Mailbox:  readResult.Mailbox,
			Messages: readResult.Messages,
			TimedOut: false,
		}, nil
	}

	initial, err := collectPending()
	if err != nil || len(initial.Messages) > 0 || waitMilliseconds == 0 {
		if err == nil && len(initial.Messages) == 0 {
			initial.TimedOut = true
		}
		return initial, err
	}

	waitTimer := time.NewTimer(time.Duration(waitMilliseconds) * time.Millisecond)
	defer waitTimer.Stop()

	for {
		select {
		case <-operationContext.Done():
			polled, pollErr := collectPending()
			if pollErr == nil && len(polled.Messages) == 0 {
				polled.TimedOut = true
			}
			return polled, pollErr
		case <-waitTimer.C:
			polled, pollErr := collectPending()
			if pollErr == nil && len(polled.Messages) == 0 {
				polled.TimedOut = true
			}
			return polled, pollErr
		case incoming, open := <-mailChannel:
			if !open {
				polled, pollErr := collectPending()
				if pollErr == nil && len(polled.Messages) == 0 {
					polled.TimedOut = true
				}
				return polled, pollErr
			}
			if incoming.ToMailboxID != targetMailbox.ID {
				continue
			}
			polled, pollErr := collectPending()
			if pollErr != nil || len(polled.Messages) > 0 {
				return polled, pollErr
			}
		}
	}
}

func (service *Service) hasLiveOrchestratorSession(sessionID string) bool {
	if sessionID == "" {
		return false
	}
	service.clientsMutex.RLock()
	_, exists := service.clients[sessionID]
	service.clientsMutex.RUnlock()
	return exists
}

func (service *Service) upsertClientMailboxInState(
	state *store.State,
	clientID string,
	sessionID string,
	registration *protocol.ClientMailRegistration,
	nowText string,
) protocol.AgentMailbox {
	host := service.defaultMasterHost()
	harnessSlug := ""
	requestedName := ""
	workspace := ""
	pushCapable := false
	processID := 0

	explicitHarnessSlug := ""
	if registration != nil {
		if strings.TrimSpace(registration.Host) != "" {
			host = protocol.NormalizeHostnameSlug(registration.Host)
		}
		if strings.TrimSpace(registration.Harness) != "" {
			explicitHarnessSlug = protocol.NormalizeHarnessMailSlug(registration.Harness)
			harnessSlug = explicitHarnessSlug
		}
		requestedName = strings.TrimSpace(registration.RequestedName)
		workspace = strings.TrimSpace(registration.Workspace)
		pushCapable = registration.PushCapable
		processID = registration.ProcessID
	}
	if harnessSlug == "" && clientID != "" {
		candidateSlug := protocol.NormalizeHarnessMailSlug(clientID)
		if candidateSlug != "agent" {
			harnessSlug = candidateSlug
		}
	}
	if harnessSlug == "" {
		harnessSlug = "agent"
	}

	var existingID string
	for mailboxID, candidate := range state.Mailboxes {
		if candidate.Kind != "mcp-session" {
			continue
		}
		if sessionID != "" && candidate.SessionID == sessionID {
			existingID = mailboxID
			break
		}
	}
	if existingID == "" && clientID != "" {
		for mailboxID, candidate := range state.Mailboxes {
			if candidate.Kind == "mcp-session" && candidate.ClientID == clientID && (!isActiveMailboxStatus(candidate.Status) || sessionID == "") {
				existingID = mailboxID
				break
			}
		}
	}
	// Allow a restarted named session (e.g. --name backend) to reclaim an offline mailbox with that address.
	if existingID == "" && requestedName != "" {
		if _, _, requestedAddress, err := protocol.NormalizeRenameTarget(requestedName, host); err == nil {
			for mailboxID, candidate := range state.Mailboxes {
				if candidate.Kind == "mcp-session" && candidate.Address == requestedAddress && !isActiveMailboxStatus(candidate.Status) {
					existingID = mailboxID
					break
				}
			}
		}
	}

	if existingID != "" {
		mailbox := state.Mailboxes[existingID]
		mailbox.Status = "online"
		mailbox.ClientID = clientID
		mailbox.SessionID = sessionID
		if registration != nil && strings.TrimSpace(registration.Host) != "" && !mailbox.CustomHandle {
			if host != mailbox.Host {
				newHandle := allocateUniqueHandle(state, mailbox.Handle, host, mailbox.ID)
				if mailbox.Address != "" && !slices.Contains(mailbox.Aliases, mailbox.Address) {
					mailbox.Aliases = append(mailbox.Aliases, mailbox.Address)
				}
				mailbox.Handle = newHandle
				mailbox.Host = host
				mailbox.Address = newHandle + "@" + host
			}
		}
		previousHarness := mailbox.Harness
		if explicitHarnessSlug != "" && explicitHarnessSlug != "agent" {
			mailbox.Harness = explicitHarnessSlug
		} else if harnessSlug != "" && (mailbox.Harness == "" || mailbox.Harness == "agent") {
			mailbox.Harness = harnessSlug
		}
		if workspace != "" {
			mailbox.Workspace = workspace
		}
		if pushCapable {
			mailbox.PushCapable = true
		}
		if requestedName != "" && !mailbox.CustomHandle {
			if targetHandle, targetHost, _, err := protocol.NormalizeRenameTarget(requestedName, mailbox.Host); err == nil {
				uniqueHandle := allocateUniqueHandle(state, targetHandle, targetHost, mailbox.ID)
				newAddress := uniqueHandle + "@" + targetHost
				if mailbox.Address != "" && mailbox.Address != newAddress && !slices.Contains(mailbox.Aliases, mailbox.Address) {
					mailbox.Aliases = append(mailbox.Aliases, mailbox.Address)
				}
				mailbox.Handle = uniqueHandle
				mailbox.Host = targetHost
				mailbox.Address = newAddress
				mailbox.CustomHandle = true
			}
		} else if !mailbox.CustomHandle && harnessSlug != "agent" &&
			(strings.HasPrefix(mailbox.Handle, "agent") || (explicitHarnessSlug != "" && explicitHarnessSlug != "agent" && previousHarness != explicitHarnessSlug)) {
			numericSuffix := shortNumericSeed(sessionID+":"+clientID, processID)
			if strings.HasPrefix(mailbox.Handle, "agent") && processID <= 0 {
				if trimmedSuffix := strings.TrimPrefix(mailbox.Handle, "agent"); trimmedSuffix != "" {
					upgradedBase := harnessSlug + trimmedSuffix
					uniqueHandle := allocateUniqueHandle(state, upgradedBase, mailbox.Host, mailbox.ID)
					newAddress := uniqueHandle + "@" + mailbox.Host
					if mailbox.Address != "" && mailbox.Address != newAddress && !slices.Contains(mailbox.Aliases, mailbox.Address) {
						mailbox.Aliases = append(mailbox.Aliases, mailbox.Address)
					}
					mailbox.Handle = uniqueHandle
					mailbox.Address = newAddress
					numericSuffix = -1
				}
			}
			if numericSuffix > 0 {
				upgradedBase := fmt.Sprintf("%s%d", harnessSlug, numericSuffix)
				uniqueHandle := allocateUniqueHandle(state, upgradedBase, mailbox.Host, mailbox.ID)
				newAddress := uniqueHandle + "@" + mailbox.Host
				if mailbox.Address != "" && mailbox.Address != newAddress && !slices.Contains(mailbox.Aliases, mailbox.Address) {
					mailbox.Aliases = append(mailbox.Aliases, mailbox.Address)
				}
				mailbox.Handle = uniqueHandle
				mailbox.Address = newAddress
			}
		}
		mailbox.UnreadCount = countUnreadMailForMailbox(state, mailbox.ID)
		mailbox.UpdatedAt = nowText
		mailbox.LastSeenAt = nowText
		state.Mailboxes[mailbox.ID] = mailbox
		return mailbox
	}

	mailboxID := protocol.NewIdentifier("mbox")
	customHandle := false
	var chosenHandle string
	chosenHost := host

	if requestedName != "" {
		if targetHandle, targetHost, _, err := protocol.NormalizeRenameTarget(requestedName, host); err == nil {
			chosenHost = targetHost
			chosenHandle = allocateUniqueHandle(state, targetHandle, chosenHost, mailboxID)
			customHandle = true
		}
	}
	if chosenHandle == "" {
		numericSuffix := shortNumericSeed(sessionID+":"+clientID, processID)
		baseHandle := fmt.Sprintf("%s%d", harnessSlug, numericSuffix)
		chosenHandle = allocateUniqueHandle(state, baseHandle, chosenHost, mailboxID)
	}

	mailbox := protocol.AgentMailbox{
		ID:           mailboxID,
		Address:      chosenHandle + "@" + chosenHost,
		Handle:       chosenHandle,
		Host:         chosenHost,
		CustomHandle: customHandle,
		Kind:         "mcp-session",
		Status:       "online",
		Harness:      harnessSlug,
		Workspace:    workspace,
		PushCapable:  pushCapable,
		ClientID:     clientID,
		SessionID:    sessionID,
		UnreadCount:  0,
		CreatedAt:    nowText,
		UpdatedAt:    nowText,
		LastSeenAt:   nowText,
	}
	state.Mailboxes[mailbox.ID] = mailbox
	return mailbox
}

func (service *Service) ensureInstanceMailboxInState(
	state *store.State,
	instance protocol.AgentInstance,
	allocation protocol.InstanceAllocation,
	nowText string,
) protocol.AgentMailbox {
	for mailboxID, existing := range state.Mailboxes {
		if existing.Kind == "instance" && existing.InstanceID == instance.ID {
			existing.Status = mailboxStatusForInstance(instance.Status)
			if allocation.NodeID != "" {
				existing.NodeID = allocation.NodeID
			}
			if allocation.Workspace != "" {
				existing.Workspace = allocation.Workspace
			}
			if allocation.HarnessID != "" {
				existing.Harness = protocol.NormalizeHarnessMailSlug(allocation.HarnessID)
			}
			existing.PushCapable = true
			existing.UnreadCount = countUnreadMailForMailbox(state, existing.ID)
			existing.UpdatedAt = nowText
			existing.LastSeenAt = nowText
			state.Mailboxes[mailboxID] = existing
			return existing
		}
	}

	host := service.defaultMasterHost()
	if node, exists := state.Nodes[allocation.NodeID]; exists {
		if strings.TrimSpace(node.Name) != "" {
			host = protocol.NormalizeHostnameSlug(node.Name)
		} else if strings.TrimSpace(node.ID) != "" {
			host = protocol.NormalizeHostnameSlug(node.ID)
		}
	} else if strings.TrimSpace(allocation.NodeID) != "" {
		host = protocol.NormalizeHostnameSlug(allocation.NodeID)
	}

	harnessSlug := protocol.NormalizeHarnessMailSlug(allocation.HarnessID)
	mailboxID := protocol.NewIdentifier("mbox")
	customHandle := false
	var chosenHandle string

	if instance.Purpose != nil && instance.Purpose.Name != nil && strings.TrimSpace(*instance.Purpose.Name) != "" {
		if sanitized, ok := protocol.SanitizeMailHandle(*instance.Purpose.Name); ok {
			chosenHandle = allocateUniqueHandle(state, sanitized, host, mailboxID)
			customHandle = true
		}
	}
	if chosenHandle == "" {
		numericSuffix := shortNumericSeed(instance.ID, 0)
		chosenHandle = allocateUniqueHandle(state, fmt.Sprintf("%s%d", harnessSlug, numericSuffix), host, mailboxID)
	}

	mailbox := protocol.AgentMailbox{
		ID:           mailboxID,
		Address:      chosenHandle + "@" + host,
		Handle:       chosenHandle,
		Host:         host,
		CustomHandle: customHandle,
		Kind:         "instance",
		Status:       mailboxStatusForInstance(instance.Status),
		Harness:      harnessSlug,
		Workspace:    allocation.Workspace,
		PushCapable:  true,
		InstanceID:   instance.ID,
		NodeID:       allocation.NodeID,
		ThreadID:     instance.ThreadID,
		UnreadCount:  0,
		CreatedAt:    nowText,
		UpdatedAt:    nowText,
		LastSeenAt:   nowText,
	}
	state.Mailboxes[mailbox.ID] = mailbox
	return mailbox
}

func (service *Service) syncInstanceMailboxInState(
	state *store.State,
	instanceID string,
	nowText string,
) {
	instance, exists := state.Instances[instanceID]
	if !exists {
		return
	}
	allocation, _ := state.LatestAllocationForInstance(instanceID)
	service.ensureInstanceMailboxInState(state, instance, allocation, nowText)
}

func (service *Service) enqueuePendingInstanceMailWakeInState(
	state *store.State,
	instanceID string,
	nowText string,
) bool {
	instance, exists := state.Instances[instanceID]
	if !exists || (instance.Status != "ready" && instance.Status != "idle") {
		return false
	}

	var targetMailbox protocol.AgentMailbox
	foundMailbox := false
	for _, mailbox := range state.Mailboxes {
		if mailbox.Kind == "instance" && mailbox.InstanceID == instanceID {
			targetMailbox = mailbox
			foundMailbox = true
			break
		}
	}
	if !foundMailbox {
		return false
	}

	var pendingMessages []protocol.MailMessage
	for _, message := range state.MailMessages {
		if message.ToMailboxID == targetMailbox.ID && !message.Read && message.WakeRequested && !message.WakeDispatched {
			pendingMessages = append(pendingMessages, message)
		}
	}
	if len(pendingMessages) == 0 {
		return false
	}

	slices.SortFunc(pendingMessages, func(left, right protocol.MailMessage) int {
		if compared := cmp.Compare(left.Sequence, right.Sequence); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})

	wakeTaskID := service.createMailWakeTaskInState(state, instance, targetMailbox, pendingMessages, nowText)
	for _, pending := range pendingMessages {
		pending.WakeDispatched = true
		pending.WakeTaskID = wakeTaskID
		pending.Read = true
		pending.ReadAt = nowText
		state.MailMessages[pending.ID] = pending
	}
	targetMailbox.UnreadCount = countUnreadMailForMailbox(state, targetMailbox.ID)
	targetMailbox.UpdatedAt = nowText
	state.Mailboxes[targetMailbox.ID] = targetMailbox
	return true
}

func (service *Service) createMailWakeTaskInState(
	state *store.State,
	instance protocol.AgentInstance,
	recipientMailbox protocol.AgentMailbox,
	messages []protocol.MailMessage,
	nowText string,
) string {
	threadID := instance.ThreadID
	if threadID == "" {
		threadID = protocol.NewIdentifier("thread")
		instance.ThreadID = threadID
		state.Instances[instance.ID] = instance
	}
	if _, exists := state.Threads[threadID]; !exists {
		state.Threads[threadID] = protocol.Thread{
			ID:        threadID,
			Title:     fmt.Sprintf("Agent Mail (%s)", recipientMailbox.Address),
			Status:    "open",
			CreatedAt: nowText,
			UpdatedAt: nowText,
		}
	}

	requirements := instanceRequirementsToExecutionRequirements(instance.Requirements)
	if allocation, hasAllocation := state.ActiveAllocationForInstance(instance.ID); hasAllocation && allocation.Workspace != "" {
		if requirements.Workspace == nil {
			requirements.Workspace = &protocol.WorkspaceRequirements{Path: allocation.Workspace}
		} else if requirements.Workspace.Path == "" {
			workspaceCopy := *requirements.Workspace
			workspaceCopy.Path = allocation.Workspace
			requirements.Workspace = &workspaceCopy
		}
	}
	// Preserve session binding continuity across wake turns by avoiding fresh worktree isolation on wake tasks.
	if requirements.Workspace != nil && requirements.Workspace.Isolation != "" {
		workspaceCopy := *requirements.Workspace
		workspaceCopy.Isolation = ""
		requirements.Workspace = &workspaceCopy
	}

	first := messages[0]
	title := "Mail from " + first.FromAddress
	if len(messages) > 1 {
		title = fmt.Sprintf("Mail (%d messages, latest from %s)", len(messages), messages[len(messages)-1].FromAddress)
	} else if first.Subject != "" {
		title = fmt.Sprintf("Mail from %s: %s", first.FromAddress, protocol.TruncateUTF8Head(first.Subject, 120))
	}

	var promptBuilder strings.Builder
	fmt.Fprintf(&promptBuilder, "You received Orchestra mail addressed to %s.\n", recipientMailbox.Address)
	for _, message := range messages {
		fmt.Fprintf(&promptBuilder, "\n--- Mail Message %s ---\n", message.ID)
		fmt.Fprintf(&promptBuilder, "From: %s\n", message.FromAddress)
		fmt.Fprintf(&promptBuilder, "To: %s\n", message.ToAddress)
		if message.Subject != "" {
			fmt.Fprintf(&promptBuilder, "Subject: %s\n", message.Subject)
		}
		if message.ReplyToID != "" {
			fmt.Fprintf(&promptBuilder, "In-Reply-To: %s\n", message.ReplyToID)
		}
		if len(message.ArtifactIDs) > 0 {
			fmt.Fprintf(&promptBuilder, "Attached Artifacts: %s\n", strings.Join(message.ArtifactIDs, ", "))
		}
		promptBuilder.WriteString("\n")
		promptBuilder.WriteString(message.Body)
		if !strings.HasSuffix(message.Body, "\n") {
			promptBuilder.WriteByte('\n')
		}
	}
	fmt.Fprintf(
		&promptBuilder,
		"\nHandle the request above. If the sender expects a response or follow-up, reply using `send_mail` (`to: %q`, `replyToId: %q`), then finalize this turn with `update_task`.\n",
		messages[len(messages)-1].FromAddress,
		messages[len(messages)-1].ID,
	)

	taskID := protocol.NewIdentifier("task")
	task := protocol.Task{
		ID:       taskID,
		ThreadID: threadID,
		Origin: protocol.TaskOrigin{
			Kind: "operator",
		},
		Title:               title,
		Instructions:        protocol.TruncateUTF8Head(promptBuilder.String(), protocol.PromptMaximumBytes),
		Status:              "ready",
		AttemptCount:        0,
		MaxAttempts:         protocol.MaximumTaskAttempts,
		Dependencies:        []protocol.TaskDependency{},
		Requirements:        requirements,
		PlacementInstanceID: instance.ID,
		PreferredInstanceID: instance.ID,
		CreatedAt:           nowText,
		UpdatedAt:           nowText,
	}
	state.Tasks[taskID] = task
	return taskID
}

func (service *Service) ensureCallerMailboxInState(
	state *store.State,
	caller CallerIdentity,
	fromOverride string,
	nowText string,
) (protocol.AgentMailbox, error) {
	switch caller.Kind {
	case "run":
		if caller.InstanceID == "" {
			return protocol.AgentMailbox{}, forbiddenError("worker run has no associated instance mailbox")
		}
		instance, exists := state.Instances[caller.InstanceID]
		if !exists {
			return protocol.AgentMailbox{}, notFound(fmt.Sprintf("instance %s does not exist", caller.InstanceID))
		}
		allocation, _ := state.LatestAllocationForInstance(instance.ID)
		return service.ensureInstanceMailboxInState(state, instance, allocation, nowText), nil

	case "orchestrator-client":
		return service.upsertClientMailboxInState(state, caller.ClientID, caller.SessionID, nil, nowText), nil

	case "operator":
		defaultHost := service.defaultMasterHost()
		requested := strings.TrimSpace(fromOverride)
		if requested == "" {
			requested = "operator@" + defaultHost
		}
		handle, host, address, err := protocol.NormalizeRenameTarget(requested, defaultHost)
		if err != nil {
			return protocol.AgentMailbox{}, invalidRequest(err.Error())
		}
		for mailboxID, existing := range state.Mailboxes {
			if existing.Address == address {
				existing.LastSeenAt = nowText
				existing.UnreadCount = countUnreadMailForMailbox(state, existing.ID)
				state.Mailboxes[mailboxID] = existing
				return existing, nil
			}
		}
		mailbox := protocol.AgentMailbox{
			ID:           protocol.NewIdentifier("mbox"),
			Address:      address,
			Handle:       handle,
			Host:         host,
			CustomHandle: true,
			Kind:         "operator",
			Status:       "online",
			Harness:      "operator",
			UnreadCount:  0,
			CreatedAt:    nowText,
			UpdatedAt:    nowText,
			LastSeenAt:   nowText,
		}
		state.Mailboxes[mailbox.ID] = mailbox
		return mailbox, nil

	default:
		return protocol.AgentMailbox{}, forbiddenError("caller cannot access agent mail")
	}
}

func resolveMailboxInState(state *store.State, rawTarget string) (protocol.AgentMailbox, bool) {
	trimmed := strings.ToLower(strings.TrimSpace(rawTarget))
	if trimmed == "" {
		return protocol.AgentMailbox{}, false
	}

	if strings.Contains(trimmed, "@") {
		_, _, normalizedAddress, ok := protocol.ParseMailAddress(trimmed)
		if !ok {
			return protocol.AgentMailbox{}, false
		}
		var fallback protocol.AgentMailbox
		hasFallback := false
		for _, mailbox := range state.Mailboxes {
			if mailbox.Address == normalizedAddress {
				if isActiveMailboxStatus(mailbox.Status) {
					return mailbox, true
				}
				if !hasFallback || mailbox.UpdatedAt > fallback.UpdatedAt {
					fallback = mailbox
					hasFallback = true
				}
			}
		}
		if hasFallback {
			return fallback, true
		}

		for _, mailbox := range state.Mailboxes {
			if slices.Contains(mailbox.Aliases, normalizedAddress) {
				if isActiveMailboxStatus(mailbox.Status) {
					return mailbox, true
				}
				if !hasFallback || mailbox.UpdatedAt > fallback.UpdatedAt {
					fallback = mailbox
					hasFallback = true
				}
			}
		}
		return fallback, hasFallback
	}

	handle, ok := protocol.NormalizeMailHandle(trimmed)
	if !ok {
		return protocol.AgentMailbox{}, false
	}
	var activeMatches []protocol.AgentMailbox
	var inactiveMatches []protocol.AgentMailbox
	for _, mailbox := range state.Mailboxes {
		if mailbox.Handle == handle {
			if isActiveMailboxStatus(mailbox.Status) {
				activeMatches = append(activeMatches, mailbox)
			} else if mailbox.Status != "released" {
				inactiveMatches = append(inactiveMatches, mailbox)
			}
		}
	}
	if len(activeMatches) == 1 {
		return activeMatches[0], true
	}
	if len(activeMatches) == 0 && len(inactiveMatches) == 1 {
		return inactiveMatches[0], true
	}
	return protocol.AgentMailbox{}, false
}

func allocateUniqueHandle(
	state *store.State,
	desiredHandle string,
	host string,
	excludeMailboxID string,
) string {
	normalizedBase, ok := protocol.NormalizeMailHandle(desiredHandle)
	if !ok {
		normalizedBase = "agent"
	}
	candidate := normalizedBase
	for suffix := 2; suffix < 100000; suffix++ {
		address := candidate + "@" + host
		taken := false
		for mailboxID, existing := range state.Mailboxes {
			if mailboxID == excludeMailboxID {
				continue
			}
			if existing.Address == address || (isActiveMailboxStatus(existing.Status) && slices.Contains(existing.Aliases, address)) {
				taken = true
				break
			}
		}
		if !taken {
			return candidate
		}
		suffixText := fmt.Sprintf("-%d", suffix)
		maximumBaseLength := protocol.MailHandleMaximumBytes - len(suffixText)
		baseSegment := normalizedBase
		if len(baseSegment) > maximumBaseLength {
			baseSegment = strings.Trim(baseSegment[:maximumBaseLength], "-_.")
		}
		candidate = baseSegment + suffixText
	}
	return fmt.Sprintf("%s-%d", normalizedBase, time.Now().UnixNano()%10000)
}

func shortNumericSeed(seed string, processID int) int {
	if processID >= 100 && processID <= 99999 {
		return processID
	}
	if processID > 99999 {
		return (processID % 9000) + 1000
	}
	digest := sha256.Sum256([]byte(seed))
	number := int(binary.BigEndian.Uint32(digest[:4])%9000) + 1000
	return number
}

func isActiveMailboxStatus(status string) bool {
	return status == "online" || status == "idle" || status == "busy"
}

func mailboxStatusForInstance(instanceStatus string) string {
	switch instanceStatus {
	case "busy":
		return "busy"
	case "draining", "released", "failed":
		return "released"
	default:
		return "idle"
	}
}

func countUnreadMailForMailbox(state *store.State, mailboxID string) int {
	count := 0
	for _, message := range state.MailMessages {
		if message.ToMailboxID == mailboxID && !message.Read {
			count++
		}
	}
	return count
}

func nextMailSequenceForMailbox(state *store.State, mailboxID string) int64 {
	var maximumSequence int64
	for _, message := range state.MailMessages {
		if message.ToMailboxID == mailboxID && message.Sequence > maximumSequence {
			maximumSequence = message.Sequence
		}
	}
	return maximumSequence + 1
}

func collectMailMessages(
	state *store.State,
	mailboxID string,
	box string,
	unreadOnly bool,
	afterSequence int64,
) []protocol.MailMessage {
	var matched []protocol.MailMessage
	for _, message := range state.MailMessages {
		if box == "sent" {
			if message.FromMailboxID != mailboxID {
				continue
			}
		} else {
			if message.ToMailboxID != mailboxID {
				continue
			}
			if unreadOnly && message.Read {
				continue
			}
			if afterSequence > 0 && message.Sequence <= afterSequence {
				continue
			}
		}
		matched = append(matched, message)
	}

	slices.SortFunc(matched, func(left, right protocol.MailMessage) int {
		if box == "inbox" {
			if compared := cmp.Compare(left.Sequence, right.Sequence); compared != 0 {
				return compared
			}
		}
		if compared := cmp.Compare(left.CreatedAt, right.CreatedAt); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})
	if matched == nil {
		matched = []protocol.MailMessage{}
	}
	return matched
}
