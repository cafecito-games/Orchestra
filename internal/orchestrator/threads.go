package orchestrator

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/activitylog"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type CreateThreadInput struct {
	Title                 string            `json:"title"`
	Summary               string            `json:"summary,omitempty"`
	Attach                bool              `json:"attach,omitempty"`
	CoordinatorInstanceID string            `json:"coordinatorInstanceId,omitempty"`
	Labels                map[string]string `json:"labels,omitempty"`
}

type ListThreadsInput struct {
	Status string   `json:"status,omitempty"`
	Labels []string `json:"labels,omitempty"`
	Limit  int      `json:"limit,omitempty"`
}

type UpdateThreadInput struct {
	ThreadID              string            `json:"threadId"`
	IdempotencyKey        string            `json:"idempotencyKey,omitempty"`
	Title                 *string           `json:"title,omitempty"`
	Summary               *string           `json:"summary,omitempty"`
	Status                *string           `json:"status,omitempty"`
	CoordinatorInstanceID NullableString    `json:"coordinatorInstanceId"`
	SetLabels             map[string]string `json:"setLabels,omitempty"`
	UnsetLabels           []string          `json:"unsetLabels,omitempty"`
	IfLabels              map[string]string `json:"ifLabels,omitempty"`
	IfUnsetLabels         []string          `json:"ifUnsetLabels,omitempty"`
}

type AttachThreadInput struct {
	ThreadID string `json:"threadId"`
}

type DetachThreadInput struct {
	ThreadID string `json:"threadId"`
}

type GetThreadContextInput struct {
	ThreadID string `json:"threadId"`
}

type ThreadContextResponse struct {
	Thread           protocol.Thread             `json:"thread"`
	Attachment       *protocol.ThreadAttachment  `json:"attachment,omitempty"`
	Tasks            []protocol.Task             `json:"tasks"`
	Instances        []protocol.AgentInstance    `json:"instances"`
	Runs             []protocol.Run              `json:"runs"`
	PendingApprovals []protocol.ApprovalRequest  `json:"pendingApprovals"`
	Artifacts        []protocol.Artifact         `json:"artifacts"`
	RecentMessages   []protocol.TaskMessage      `json:"recentMessages"`
	Room             protocol.ThreadRoomHead     `json:"room"`
	Stall            *protocol.ThreadStallReport `json:"stall,omitempty"`
	EventStream      protocol.ThreadEventStream  `json:"eventStream"`
}

func (service *Service) CreateThread(
	caller CallerIdentity,
	input CreateThreadInput,
) (protocol.Thread, *protocol.ThreadAttachment, error) {
	if caller.Scope != nil {
		if err := requireScopeOperation(caller, protocol.AccessOpDelegate); err != nil {
			return protocol.Thread{}, nil, err
		}
		if len(caller.Scope.ThreadIDs) > 0 {
			return protocol.Thread{}, nil, forbiddenError("thread-scoped access grant cannot create new threads")
		}
	}
	title := strings.TrimSpace(input.Title)
	if title == "" || !protocol.IsBoundedString(title, 512) {
		return protocol.Thread{}, nil, invalidRequest("thread title must be a non-empty string up to 512 bytes")
	}
	summary := strings.TrimSpace(input.Summary)
	if !protocol.IsBoundedString(summary, 4096) {
		return protocol.Thread{}, nil, invalidRequest("thread summary exceeds 4096 bytes")
	}
	normalizedLabels, labelErr := protocol.NormalizeLabels(input.Labels, service.redactor, false)
	if labelErr != nil {
		return protocol.Thread{}, nil, labelValidationError(labelErr)
	}

	var created protocol.Thread
	var createdAttachment *protocol.ThreadAttachment

	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		nowText := service.nowUTC()
		threadID := protocol.NewIdentifier("thread")
		var orchestratorIdentity *protocol.ThreadOrchestrator
		if caller.Kind == "orchestrator-client" && caller.ClientID != "" {
			orchestratorIdentity = &protocol.ThreadOrchestrator{
				Kind:     "orchestrator-client",
				ClientID: caller.ClientID,
			}
		} else if caller.Kind == "run" && caller.InstanceID != "" && caller.CanDelegate {
			orchestratorIdentity = &protocol.ThreadOrchestrator{
				Kind:       "instance",
				InstanceID: caller.InstanceID,
			}
		}

		created = protocol.Thread{
			ID:           threadID,
			Title:        title,
			Summary:      summary,
			Status:       "open",
			Labels:       protocol.CloneLabels(normalizedLabels),
			Orchestrator: orchestratorIdentity,
			CreatedAt:    nowText,
			UpdatedAt:    nowText,
		}
		if strings.TrimSpace(input.CoordinatorInstanceID) != "" {
			if bindErr := service.bindThreadCoordinatorInState(state, mutation, caller, &created, input.CoordinatorInstanceID, nowText); bindErr != nil {
				return bindErr
			}
		}
		state.Threads[threadID] = created

		if input.Attach && caller.ClientID != "" {
			attachment := protocol.ThreadAttachment{
				ID:              protocol.NewIdentifier("attach"),
				ThreadID:        threadID,
				ClientID:        caller.ClientID,
				SessionID:       caller.SessionID,
				Status:          "attached",
				LastHeartbeatAt: nowText,
				CreatedAt:       nowText,
				UpdatedAt:       nowText,
			}
			state.Attachments[attachment.ID] = attachment
			createdAttachment = &attachment
		}
		return nil
	})
	if err != nil {
		return protocol.Thread{}, nil, err
	}
	service.logger.Info(
		"thread",
		"created",
		activitylog.String("thread", created.ID),
		activitylog.String("title", created.Title),
		activitylog.String("by", caller.SourceKey()),
	)
	return created, createdAttachment, nil
}

func (service *Service) ListThreads(input ListThreadsInput) ([]protocol.Thread, error) {
	if input.Status != "" && !slices.Contains(protocol.ThreadStatuses, input.Status) {
		return nil, invalidRequest("status must be open or resolved")
	}
	labelFilters, filterErr := protocol.ParseLabelFilters(input.Labels)
	if filterErr != nil {
		return nil, invalidRequest(filterErr.Error())
	}
	limit := input.Limit
	if limit <= 0 || limit > 200 {
		limit = 100
	}

	var threads []protocol.Thread
	err := service.store.Read(func(state *store.State) error {
		for _, thread := range state.Threads {
			if input.Status != "" && thread.Status != input.Status {
				continue
			}
			if !protocol.MatchesLabelFilters(thread.Labels, labelFilters) {
				continue
			}
			threads = append(threads, thread)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(threads, func(left, right protocol.Thread) int {
		if compared := cmp.Compare(right.UpdatedAt, left.UpdatedAt); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})
	if len(threads) > limit {
		threads = threads[:limit]
	}
	if threads == nil {
		threads = []protocol.Thread{}
	}
	return threads, nil
}

func (service *Service) AttachThread(
	caller CallerIdentity,
	input AttachThreadInput,
) (protocol.ThreadAttachment, string, error) {
	if !protocol.IsIdentifier(input.ThreadID) {
		return protocol.ThreadAttachment{}, "", invalidRequest("threadId is required")
	}
	clientID := caller.ClientID
	if clientID == "" {
		clientID = "mcp-http"
	}
	sessionID := caller.SessionID
	if sessionID == "" {
		sessionID = clientID
	}

	var attached protocol.ThreadAttachment
	var latestCursor string

	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		if _, exists := state.Threads[input.ThreadID]; !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", input.ThreadID))
		}
		nowText := service.nowUTC()
		var displacedSessionID string

		for attachmentID, existing := range state.Attachments {
			if existing.ThreadID != input.ThreadID || existing.Status != "attached" {
				continue
			}
			if existing.ClientID == clientID && existing.SessionID == sessionID {
				existing.LastHeartbeatAt = nowText
				existing.UpdatedAt = nowText
				state.Attachments[attachmentID] = existing
				attached = existing
				stream := state.EventStreams[input.ThreadID]
				cursor, _ := protocol.EncodeEventCursor(input.ThreadID, caller.SourceKey(), stream.Head)
				latestCursor = cursor
				return nil
			}
			displacedSessionID = existing.SessionID
			existing.Status = "detached"
			existing.DetachedReason = "replaced"
			existing.UpdatedAt = nowText
			state.Attachments[attachmentID] = existing
		}

		stream := state.EventStreams[input.ThreadID]
		attached = protocol.ThreadAttachment{
			ID:              protocol.NewIdentifier("attach"),
			ThreadID:        input.ThreadID,
			ClientID:        clientID,
			SessionID:       sessionID,
			Status:          "attached",
			LastHeartbeatAt: nowText,
			AcknowledgedSeq: stream.Head,
			LastDoorbellSeq: stream.Head,
			CreatedAt:       nowText,
			UpdatedAt:       nowText,
		}
		state.Attachments[attached.ID] = attached
		cursor, _ := protocol.EncodeEventCursor(input.ThreadID, caller.SourceKey(), stream.Head)
		latestCursor = cursor

		if displacedSessionID != "" {
			threadID := input.ThreadID
			mutation.OnCommit(func() {
				service.sendToOrchestratorSession(displacedSessionID, protocol.OrchestratorServerMessage{
					Type:     "attachment.replaced",
					ThreadID: threadID,
					Reason:   "another orchestrator session attached to this thread",
					At:       service.nowUTC(),
				})
			})
		}
		return nil
	})
	if err != nil {
		return protocol.ThreadAttachment{}, "", err
	}
	return attached, latestCursor, nil
}

func (service *Service) DetachThread(caller CallerIdentity, input DetachThreadInput) (bool, error) {
	if !protocol.IsIdentifier(input.ThreadID) {
		return false, invalidRequest("threadId is required")
	}
	detached := false
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		if _, exists := state.Threads[input.ThreadID]; !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", input.ThreadID))
		}
		nowText := service.nowUTC()
		for attachmentID, existing := range state.Attachments {
			if existing.ThreadID != input.ThreadID || existing.Status != "attached" {
				continue
			}
			if caller.ClientID != "" && existing.ClientID != caller.ClientID {
				return forbiddenError("thread is attached to a different orchestrator client")
			}
			existing.Status = "detached"
			existing.DetachedReason = "detached-by-caller"
			existing.UpdatedAt = nowText
			state.Attachments[attachmentID] = existing
			detached = true
		}
		return nil
	})
	return detached, err
}

func (service *Service) UpdateThread(caller CallerIdentity, input UpdateThreadInput) (protocol.Thread, error) {
	threadID := input.ThreadID
	if threadID == "" && caller.Kind == "run" {
		threadID = caller.ThreadID
	}
	if !protocol.IsIdentifier(threadID) {
		return protocol.Thread{}, invalidRequest("threadId is required")
	}
	if caller.Kind == "run" && caller.ThreadID != "" && caller.ThreadID != threadID {
		return protocol.Thread{}, forbiddenError("worker run cannot update another thread")
	}
	if err := requireScopeOperation(caller, protocol.AccessOpPost); err != nil {
		return protocol.Thread{}, err
	}
	if err := requireScopeThread(caller, threadID); err != nil {
		return protocol.Thread{}, err
	}

	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey != "" && !protocol.IsBoundedString(idempotencyKey, 128) {
		return protocol.Thread{}, invalidRequest("idempotencyKey must be at most 128 bytes")
	}
	normalizedSetLabels, setErr := protocol.NormalizeLabels(input.SetLabels, service.redactor, false)
	if setErr != nil {
		return protocol.Thread{}, labelValidationError(setErr)
	}
	normalizedUnsetLabels := make([]string, 0, len(input.UnsetLabels))
	for _, rawKey := range input.UnsetLabels {
		key, keyErr := protocol.ValidateLabelKey(rawKey, false)
		if keyErr != nil {
			return protocol.Thread{}, labelValidationError(keyErr)
		}
		if !slices.Contains(normalizedUnsetLabels, key) {
			normalizedUnsetLabels = append(normalizedUnsetLabels, key)
		}
	}
	slices.Sort(normalizedUnsetLabels)

	normalizedIfLabels, ifErr := protocol.NormalizeLabels(input.IfLabels, service.redactor, true)
	if ifErr != nil {
		return protocol.Thread{}, invalidRequest(ifErr.Error())
	}
	normalizedIfUnsetLabels := make([]string, 0, len(input.IfUnsetLabels))
	for _, rawKey := range input.IfUnsetLabels {
		key, keyErr := protocol.ValidateLabelKey(rawKey, true)
		if keyErr != nil {
			return protocol.Thread{}, invalidRequest(keyErr.Error())
		}
		if !slices.Contains(normalizedIfUnsetLabels, key) {
			normalizedIfUnsetLabels = append(normalizedIfUnsetLabels, key)
		}
	}
	slices.Sort(normalizedIfUnsetLabels)

	sourceKey := caller.SourceKey()
	receiptKey := sourceKey + "\x00thread_update\x00" + threadID + "\x00" + idempotencyKey
	digest := canonicalDigest(struct {
		ThreadID              string            `json:"threadId"`
		Title                 *string           `json:"title,omitempty"`
		Summary               *string           `json:"summary,omitempty"`
		Status                *string           `json:"status,omitempty"`
		CoordinatorInstanceID NullableString    `json:"coordinatorInstanceId"`
		SetLabels             map[string]string `json:"setLabels,omitempty"`
		UnsetLabels           []string          `json:"unsetLabels,omitempty"`
		IfLabels              map[string]string `json:"ifLabels,omitempty"`
		IfUnsetLabels         []string          `json:"ifUnsetLabels,omitempty"`
	}{
		ThreadID:              threadID,
		Title:                 input.Title,
		Summary:               input.Summary,
		Status:                input.Status,
		CoordinatorInstanceID: input.CoordinatorInstanceID,
		SetLabels:             normalizedSetLabels,
		UnsetLabels:           normalizedUnsetLabels,
		IfLabels:              normalizedIfLabels,
		IfUnsetLabels:         normalizedIfUnsetLabels,
	})

	var updated protocol.Thread
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		thread, exists := state.Threads[threadID]
		if !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}
		if idempotencyKey != "" {
			if existingReceipt, found := state.TaskUpdates[receiptKey]; found {
				if existingReceipt.Digest != digest {
					return conflictError("idempotency_conflict", "idempotencyKey was already used for a different thread update")
				}
				updated = thread
				return nil
			}
		}

		var labelConflicts []map[string]any
		if len(normalizedIfLabels) > 0 {
			keys := make([]string, 0, len(normalizedIfLabels))
			for key := range normalizedIfLabels {
				keys = append(keys, key)
			}
			slices.Sort(keys)
			for _, key := range keys {
				expected := normalizedIfLabels[key]
				actual, present := thread.Labels[key]
				if !present || actual != expected {
					entry := map[string]any{
						"key":      key,
						"expected": expected,
						"present":  present,
					}
					if present {
						entry["actual"] = actual
					}
					labelConflicts = append(labelConflicts, entry)
				}
			}
		}
		for _, key := range normalizedIfUnsetLabels {
			if actual, present := thread.Labels[key]; present {
				labelConflicts = append(labelConflicts, map[string]any{
					"key":           key,
					"expectedUnset": true,
					"actual":        actual,
				})
			}
		}
		if len(labelConflicts) > 0 {
			currentLabels := protocol.CloneLabels(thread.Labels)
			if currentLabels == nil {
				currentLabels = map[string]string{}
			}
			return conflictErrorWithDetail(
				"label_conflict",
				fmt.Sprintf("thread %s label precondition failed", threadID),
				map[string]any{
					"threadId":      threadID,
					"currentLabels": currentLabels,
					"conflicts":     labelConflicts,
				},
			)
		}

		nextLabels := protocol.CloneLabels(thread.Labels)
		if len(normalizedSetLabels) > 0 || len(normalizedUnsetLabels) > 0 {
			if nextLabels == nil {
				nextLabels = map[string]string{}
			}
			for _, key := range normalizedUnsetLabels {
				delete(nextLabels, key)
			}
			maps.Copy(nextLabels, normalizedSetLabels)
			if len(nextLabels) > protocol.MaximumLabelsPerResource {
				return invalidRequest(fmt.Sprintf("thread labels exceed maximum of %d entries", protocol.MaximumLabelsPerResource))
			}
			if len(nextLabels) == 0 {
				nextLabels = nil
			}
			thread.Labels = nextLabels
		}

		if input.Title != nil {
			trimmed := strings.TrimSpace(*input.Title)
			if trimmed == "" || !protocol.IsBoundedString(trimmed, 512) {
				return invalidRequest("thread title must be between 1 and 512 bytes")
			}
			thread.Title = trimmed
		}
		if input.Summary != nil {
			trimmed := strings.TrimSpace(*input.Summary)
			if !protocol.IsBoundedString(trimmed, 4096) {
				return invalidRequest("thread summary exceeds 4096 bytes")
			}
			thread.Summary = trimmed
		}
		if input.Status != nil {
			normalizedStatus := *input.Status
			if normalizedStatus == "completed" {
				normalizedStatus = "resolved"
			}
			if !slices.Contains(protocol.ThreadStatuses, normalizedStatus) {
				return invalidRequest("thread status must be open or resolved")
			}
			thread.Status = normalizedStatus
		}
		nowText := service.nowUTC()
		if input.CoordinatorInstanceID.Set {
			targetInstanceID := ""
			if input.CoordinatorInstanceID.Value != nil {
				targetInstanceID = *input.CoordinatorInstanceID.Value
			}
			if bindErr := service.bindThreadCoordinatorInState(state, mutation, caller, &thread, targetInstanceID, nowText); bindErr != nil {
				return bindErr
			}
		}
		if idempotencyKey != "" {
			state.TaskUpdates[receiptKey] = protocol.TaskUpdateReceipt{
				ID:             protocol.NewIdentifier("upd"),
				TaskID:         threadID,
				SourceKey:      sourceKey,
				IdempotencyKey: idempotencyKey,
				Digest:         digest,
				CreatedAt:      nowText,
			}
		}
		thread.UpdatedAt = nowText
		state.Threads[threadID] = thread
		if thread.Status == "resolved" {
			cancelThreadSchedulesInState(state, threadID, nowText)
		}
		updated = thread
		return nil
	})
	if err == nil && input.Status != nil {
		service.logger.Info(
			"thread",
			"updated",
			activitylog.String("thread", updated.ID),
			activitylog.String("status", updated.Status),
			activitylog.String("title", updated.Title),
		)
	}
	return updated, err
}

func (service *Service) GetThreadContext(threadID string) (ThreadContextResponse, error) {
	if !protocol.IsIdentifier(threadID) {
		return ThreadContextResponse{}, invalidRequest("threadId is required")
	}
	var response ThreadContextResponse
	err := service.store.Read(func(state *store.State) error {
		thread, exists := state.Threads[threadID]
		if !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}
		response.Thread = thread
		if attachment, found := state.ActiveAttachmentForThread(threadID); found {
			attachmentCopy := attachment
			response.Attachment = &attachmentCopy
		}
		response.Tasks = collectThreadTasks(state, threadID)
		response.Instances = collectThreadInstances(state, threadID)
		response.Runs = collectThreadRuns(state, threadID)
		for _, approval := range state.Approvals {
			if approval.ThreadID == threadID && approval.Status == "pending" {
				response.PendingApprovals = append(response.PendingApprovals, approval)
			}
		}
		slices.SortFunc(response.PendingApprovals, func(left, right protocol.ApprovalRequest) int {
			return cmp.Compare(left.RequestedAt, right.RequestedAt)
		})
		for _, artifact := range state.Artifacts {
			if artifact.ThreadID == threadID {
				response.Artifacts = append(response.Artifacts, artifact)
			}
		}
		slices.SortFunc(response.Artifacts, func(left, right protocol.Artifact) int {
			return cmp.Compare(left.CreatedAt, right.CreatedAt)
		})
		messages := collectThreadMessages(state, threadID)
		if len(messages) > 50 {
			messages = messages[len(messages)-50:]
		}
		response.RecentMessages = messages
		if roomHead, hasRoom := state.RoomHeads[threadID]; hasRoom {
			response.Room = roomHead
		} else {
			response.Room = protocol.ThreadRoomHead{
				ThreadID:      threadID,
				DirectoryName: protocol.RoomDirectoryName(threadID, thread.CreatedAt),
				CreatedAt:     thread.CreatedAt,
				UpdatedAt:     thread.UpdatedAt,
			}
		}
		stream, hasStream := state.EventStreams[threadID]
		if !hasStream {
			stream = protocol.ThreadEventStream{ThreadID: threadID}
		}
		response.EventStream = stream
		response.Stall = service.redactStallReport(EvaluateThreadStall(state, threadID, service.now()))
		return nil
	})
	if err != nil {
		return ThreadContextResponse{}, err
	}
	if response.PendingApprovals == nil {
		response.PendingApprovals = []protocol.ApprovalRequest{}
	}
	if response.Artifacts == nil {
		response.Artifacts = []protocol.Artifact{}
	}
	if response.RecentMessages == nil {
		response.RecentMessages = []protocol.TaskMessage{}
	}
	return response, nil
}

func (service *Service) GetThreadDetail(threadID string) (protocol.ThreadDetailResponse, error) {
	if !protocol.IsIdentifier(threadID) {
		return protocol.ThreadDetailResponse{}, invalidRequest("threadId is required")
	}
	var detail protocol.ThreadDetailResponse
	err := service.store.Read(func(state *store.State) error {
		thread, exists := state.Threads[threadID]
		if !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}
		detail.Thread = thread
		if attachment, found := state.ActiveAttachmentForThread(threadID); found {
			attachmentCopy := attachment
			detail.Attachment = &attachmentCopy
		}
		detail.Tasks = collectThreadTasks(state, threadID)
		detail.Instances = collectThreadInstances(state, threadID)
		for _, instance := range detail.Instances {
			for _, allocation := range state.Allocations {
				if allocation.InstanceID == instance.ID {
					detail.Allocations = append(detail.Allocations, allocation)
				}
			}
		}
		slices.SortFunc(detail.Allocations, func(left, right protocol.InstanceAllocation) int {
			return cmp.Compare(left.CreatedAt, right.CreatedAt)
		})
		detail.Runs = collectThreadRuns(state, threadID)
		for _, approval := range state.Approvals {
			if approval.ThreadID == threadID {
				detail.Approvals = append(detail.Approvals, approval)
			}
		}
		slices.SortFunc(detail.Approvals, func(left, right protocol.ApprovalRequest) int {
			return cmp.Compare(left.RequestedAt, right.RequestedAt)
		})
		for _, artifact := range state.Artifacts {
			if artifact.ThreadID == threadID {
				detail.Artifacts = append(detail.Artifacts, artifact)
			}
		}
		slices.SortFunc(detail.Artifacts, func(left, right protocol.Artifact) int {
			return cmp.Compare(left.CreatedAt, right.CreatedAt)
		})
		for _, lease := range state.WorkspaceLeases {
			if lease.ThreadID == threadID {
				detail.WorkspaceLeases = append(detail.WorkspaceLeases, lease)
			}
		}
		slices.SortFunc(detail.WorkspaceLeases, func(left, right protocol.WorkspaceLease) int {
			return cmp.Compare(left.CreatedAt, right.CreatedAt)
		})
		for _, binding := range state.SessionBindings {
			if binding.ThreadID == threadID {
				detail.SessionBindings = append(detail.SessionBindings, binding)
			}
		}
		detail.Messages = collectThreadMessages(state, threadID)
		if roomHead, hasRoom := state.RoomHeads[threadID]; hasRoom {
			detail.Room = roomHead
		} else {
			detail.Room = protocol.ThreadRoomHead{
				ThreadID:      threadID,
				DirectoryName: protocol.RoomDirectoryName(threadID, thread.CreatedAt),
				CreatedAt:     thread.CreatedAt,
				UpdatedAt:     thread.UpdatedAt,
			}
		}
		for _, run := range detail.Runs {
			if activity, exists := state.RunActivities[run.ID]; exists {
				detail.Activities = append(detail.Activities, activity)
			}
			if transcript, exists := state.RunTranscripts[run.ID]; exists {
				detail.Transcripts = append(detail.Transcripts, transcript)
			}
		}
		stream, hasStream := state.EventStreams[threadID]
		if !hasStream {
			stream = protocol.ThreadEventStream{ThreadID: threadID}
		}
		detail.EventStream = stream
		detail.Stall = service.redactStallReport(EvaluateThreadStall(state, threadID, service.now()))
		return nil
	})
	if err != nil {
		return protocol.ThreadDetailResponse{}, err
	}
	if detail.Allocations == nil {
		detail.Allocations = []protocol.InstanceAllocation{}
	}
	if detail.Approvals == nil {
		detail.Approvals = []protocol.ApprovalRequest{}
	}
	if detail.Artifacts == nil {
		detail.Artifacts = []protocol.Artifact{}
	}
	if detail.WorkspaceLeases == nil {
		detail.WorkspaceLeases = []protocol.WorkspaceLease{}
	}
	if detail.SessionBindings == nil {
		detail.SessionBindings = []protocol.HarnessSessionBinding{}
	}
	if detail.Activities == nil {
		detail.Activities = []protocol.RunActivity{}
	}
	if detail.Transcripts == nil {
		detail.Transcripts = []protocol.RunTranscript{}
	}
	return detail, nil
}

func collectThreadTasks(state *store.State, threadID string) []protocol.Task {
	tasks := []protocol.Task{}
	for _, task := range state.Tasks {
		if task.ThreadID == threadID {
			tasks = append(tasks, task)
		}
	}
	slices.SortFunc(tasks, func(left, right protocol.Task) int {
		if compared := cmp.Compare(left.CreatedAt, right.CreatedAt); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})
	return tasks
}

func collectThreadInstances(state *store.State, threadID string) []protocol.AgentInstance {
	instances := []protocol.AgentInstance{}
	for _, instance := range state.Instances {
		if instance.ThreadID == threadID {
			instances = append(instances, instance)
		}
	}
	slices.SortFunc(instances, func(left, right protocol.AgentInstance) int {
		if compared := cmp.Compare(left.CreatedAt, right.CreatedAt); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})
	return instances
}

func collectThreadRuns(state *store.State, threadID string) []protocol.Run {
	runs := []protocol.Run{}
	for _, run := range state.Runs {
		if run.ThreadID == threadID {
			runs = append(runs, run)
		}
	}
	slices.SortFunc(runs, func(left, right protocol.Run) int {
		if compared := cmp.Compare(left.CreatedAt, right.CreatedAt); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})
	return runs
}

func collectThreadMessages(state *store.State, threadID string) []protocol.TaskMessage {
	messages := []protocol.TaskMessage{}
	for _, message := range state.Messages {
		if message.ThreadID == threadID {
			messages = append(messages, message)
		}
	}
	slices.SortFunc(messages, func(left, right protocol.TaskMessage) int {
		if compared := cmp.Compare(left.CreatedAt, right.CreatedAt); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})
	return messages
}

func (service *Service) runDoorbellLoop(operationContext context.Context) {
	eventChannel, unsubscribe := service.store.SubscribeEvents()
	defer unsubscribe()

	timers := map[string]*time.Timer{}
	fireThread := make(chan string, 64)

	defer func() {
		for _, timer := range timers {
			timer.Stop()
		}
	}()

	for {
		select {
		case <-operationContext.Done():
			return
		case events, ok := <-eventChannel:
			if !ok {
				return
			}
			urgentByThread := map[string]bool{}
			for _, event := range events {
				if event.ThreadID == "" {
					continue
				}
				urgentEvent := event.Type == "approval.requested" || event.Type == "thread.stalled"
				if !urgentEvent && event.Detail != nil {
					if urgentFlag, ok := event.Detail["urgentDoorbell"].(bool); ok && urgentFlag {
						urgentEvent = true
					}
				}
				if urgentEvent {
					urgentByThread[event.ThreadID] = true
				} else if _, seen := urgentByThread[event.ThreadID]; !seen {
					urgentByThread[event.ThreadID] = false
				}
			}
			for threadID, urgent := range urgentByThread {
				if urgent {
					if existing, exists := timers[threadID]; exists {
						existing.Stop()
						delete(timers, threadID)
					}
					service.emitDoorbellIfAttached(threadID)
					continue
				}
				if _, exists := timers[threadID]; !exists {
					targetThreadID := threadID
					timers[threadID] = time.AfterFunc(service.doorbellDebounce, func() {
						select {
						case fireThread <- targetThreadID:
						default:
						}
					})
				}
			}
		case threadID := <-fireThread:
			delete(timers, threadID)
			_, emitted, _ := service.CheckThreadStall(threadID)
			if !emitted {
				service.emitDoorbellIfAttached(threadID)
			}
		}
	}
}

func (service *Service) emitDoorbellIfAttached(threadID string) {
	_ = service.emitDoorbellWithOverride(threadID, "", false)
}

func (service *Service) emitDoorbellWithOverride(
	threadID string,
	overrideSummary string,
	forceUrgent bool,
) string {
	var sessionID string
	var notification *protocol.DoorbellNotification

	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		thread, exists := state.Threads[threadID]
		if !exists {
			return nil
		}
		attachment, attached := state.ActiveAttachmentForThread(threadID)
		if !attached {
			return nil
		}
		stream := state.EventStreams[threadID]
		if stream.Head <= attachment.AcknowledgedSeq || stream.Head <= attachment.LastDoorbellSeq {
			return nil
		}

		var approvalIDs []string
		for _, approval := range state.Approvals {
			if approval.ThreadID == threadID && approval.Status == "pending" {
				approvalIDs = append(approvalIDs, approval.ID)
			}
		}
		slices.Sort(approvalIDs)

		latestSummary := strings.TrimSpace(overrideSummary)
		latestType := ""
		events := state.ThreadEvents[threadID]
		if len(events) > 0 {
			latestType = events[len(events)-1].Type
			if latestSummary == "" {
				latestSummary = events[len(events)-1].Summary
			}
		}
		if latestSummary == "" {
			latestSummary = fmt.Sprintf("Thread %q has new activity", thread.Title)
		}
		latestSummary = service.redactor.Redact(latestSummary)

		attachment.LastDoorbellSeq = stream.Head
		attachment.UpdatedAt = service.nowUTC()
		state.Attachments[attachment.ID] = attachment

		sessionID = attachment.SessionID
		notification = &protocol.DoorbellNotification{
			ThreadID:        threadID,
			ThreadTitle:     thread.Title,
			ThroughSequence: stream.Head,
			Summary:         latestSummary,
			Urgent:          forceUrgent || len(approvalIDs) > 0 || latestType == "thread.stalled",
			ApprovalIDs:     approvalIDs,
			OccurredAt:      service.nowUTC(),
		}
		return nil
	})

	if sessionID != "" && notification != nil {
		service.sendToOrchestratorSession(sessionID, protocol.OrchestratorServerMessage{
			Type:     "doorbell",
			Doorbell: notification,
			At:       notification.OccurredAt,
		})
		return sessionID
	}
	return ""
}
