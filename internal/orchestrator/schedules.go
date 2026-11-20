package orchestrator

import (
	"cmp"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/activitylog"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type ScheduleMessageInput struct {
	IdempotencyKey      string                     `json:"idempotencyKey"`
	TargetKind          string                     `json:"targetKind"`
	ThreadID            string                     `json:"threadId,omitempty"`
	Channel             string                     `json:"channel,omitempty"`
	Kind                string                     `json:"kind,omitempty"`
	Topic               string                     `json:"topic,omitempty"`
	Labels              map[string]string          `json:"labels,omitempty"`
	Mentions            []string                   `json:"mentions,omitempty"`
	To                  string                     `json:"to,omitempty"`
	Subject             string                     `json:"subject,omitempty"`
	Body                string                     `json:"body"`
	Blocks              []protocol.StructuredBlock `json:"blocks,omitempty"`
	Wake                *bool                      `json:"wake,omitempty"`
	InSeconds           int                        `json:"inSeconds,omitempty"`
	DeliverAt           string                     `json:"deliverAt,omitempty"`
	EverySeconds        int                        `json:"everySeconds,omitempty"`
	MaxOccurrences      int                        `json:"maxOccurrences,omitempty"`
	UntilAt             string                     `json:"untilAt,omitempty"`
	CancelOnThreadClose *bool                      `json:"cancelOnThreadClose,omitempty"`
}

type ScheduleMessageResult struct {
	Schedule protocol.ScheduledMessage `json:"schedule"`
	Replayed bool                      `json:"replayed"`
}

type ListSchedulesInput struct {
	ThreadID   string `json:"threadId,omitempty"`
	TargetKind string `json:"targetKind,omitempty"`
	Status     string `json:"status,omitempty"`
}

type CancelScheduleInput struct {
	ScheduleID string `json:"scheduleId"`
}

func invalidBlockError(err error) *OperationError {
	return newError("invalid_block", http.StatusBadRequest, err.Error())
}

func scheduleCreatorKey(caller CallerIdentity) string {
	if caller.GrantID != "" {
		return "grant:" + caller.GrantID
	}
	if sourceKey := caller.SourceKey(); sourceKey != "" {
		return sourceKey
	}
	return "orchestrator-client:default"
}

func (service *Service) ScheduleMessage(caller CallerIdentity, input ScheduleMessageInput) (ScheduleMessageResult, error) {
	if err := requireScopeOperation(caller, protocol.AccessOpPost); err != nil {
		return ScheduleMessageResult{}, err
	}

	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" || !protocol.IsBoundedString(idempotencyKey, 128) {
		return ScheduleMessageResult{}, invalidRequest("idempotencyKey is required and must be at most 128 bytes")
	}

	targetKind := strings.ToLower(strings.TrimSpace(input.TargetKind))
	if targetKind == "" {
		switch {
		case strings.TrimSpace(input.ThreadID) != "":
			targetKind = protocol.ScheduleTargetKindRoom
		case strings.TrimSpace(input.To) != "":
			targetKind = protocol.ScheduleTargetKindMail
		}
	}
	if !slices.Contains(protocol.ScheduleTargetKinds, targetKind) {
		return ScheduleMessageResult{}, invalidRequest("targetKind must be room or mail")
	}

	body := strings.TrimSpace(input.Body)
	if body == "" || !protocol.IsBoundedString(body, protocol.TextMaximumBytes) {
		return ScheduleMessageResult{}, invalidRequest(fmt.Sprintf("body must be a non-empty string up to %d bytes", protocol.TextMaximumBytes))
	}
	redactedBody := service.redactor.Redact(body)

	normalizedBlocks, blockErr := protocol.NormalizeStructuredBlocks(input.Blocks, service.redactor)
	if blockErr != nil {
		return ScheduleMessageResult{}, invalidBlockError(blockErr)
	}

	var threadID, channel, roomKind, redactedTopic string
	var normalizedLabels map[string]string
	var mentions []string
	var toAddress, redactedSubject string

	switch targetKind {
	case protocol.ScheduleTargetKindRoom:
		threadID = strings.TrimSpace(input.ThreadID)
		if threadID == "" && caller.Kind == "run" {
			threadID = caller.ThreadID
		}
		if !protocol.IsIdentifier(threadID) {
			return ScheduleMessageResult{}, invalidRequest("threadId is required for room schedules")
		}
		if caller.Kind == "run" && caller.ThreadID != "" && caller.ThreadID != threadID {
			return ScheduleMessageResult{}, forbiddenError("worker run cannot schedule messages in another thread")
		}
		if err := requireScopeThread(caller, threadID); err != nil {
			return ScheduleMessageResult{}, err
		}
		var channelErr error
		channel, channelErr = protocol.NormalizeRoomChannel(input.Channel)
		if channelErr != nil {
			return ScheduleMessageResult{}, invalidRequest(channelErr.Error())
		}
		if err := requireScopeRoomChannel(caller, channel); err != nil {
			return ScheduleMessageResult{}, err
		}
		roomKind = strings.TrimSpace(input.Kind)
		if roomKind == "" {
			roomKind = "guidance"
		}
		if !slices.Contains(protocol.RoomMessageKinds, roomKind) {
			return ScheduleMessageResult{}, invalidRequest("kind must be one of " + strings.Join(protocol.RoomMessageKinds, ", "))
		}
		topic := strings.TrimSpace(input.Topic)
		if !protocol.IsBoundedString(topic, protocol.RoomTopicMaximumBytes) {
			return ScheduleMessageResult{}, invalidRequest(fmt.Sprintf("topic must not exceed %d bytes", protocol.RoomTopicMaximumBytes))
		}
		redactedTopic = service.redactor.Redact(topic)
		var labelErr error
		normalizedLabels, labelErr = protocol.NormalizeLabels(input.Labels, service.redactor, false)
		if labelErr != nil {
			return ScheduleMessageResult{}, labelValidationError(labelErr)
		}
		if slices.ContainsFunc(input.Mentions, service.redactor.ContainsSecret) {
			return ScheduleMessageResult{}, invalidRequest("room mentions must not contain secret material")
		}
		parsedMentions, mentionErr := protocol.ParseRoomMentions(redactedBody, input.Mentions)
		if mentionErr != nil {
			return ScheduleMessageResult{}, invalidRequest(mentionErr.Error())
		}
		mentions = slices.Clone(parsedMentions.Tokens)
	case protocol.ScheduleTargetKindMail:
		toAddress = strings.TrimSpace(input.To)
		if toAddress == "" {
			return ScheduleMessageResult{}, invalidRequest("to address is required for mail schedules")
		}
		subject := strings.TrimSpace(input.Subject)
		if !protocol.IsBoundedString(subject, protocol.MailSubjectMaximumBytes) {
			return ScheduleMessageResult{}, invalidRequest(fmt.Sprintf("subject must not exceed %d bytes", protocol.MailSubjectMaximumBytes))
		}
		redactedSubject = service.redactor.Redact(subject)
	}

	if input.InSeconds < 0 {
		return ScheduleMessageResult{}, invalidRequest("inSeconds must be non-negative")
	}
	if input.InSeconds > 0 && strings.TrimSpace(input.DeliverAt) != "" {
		return ScheduleMessageResult{}, invalidRequest("specify at most one of inSeconds or deliverAt")
	}
	if input.EverySeconds < 0 || (input.EverySeconds > 0 && input.EverySeconds < protocol.MinimumRecurringIntervalSeconds) {
		return ScheduleMessageResult{}, invalidRequest(fmt.Sprintf("everySeconds must be at least %d seconds for recurring schedules", protocol.MinimumRecurringIntervalSeconds))
	}

	now := service.now()
	var firstDeliverAt time.Time
	switch {
	case strings.TrimSpace(input.DeliverAt) != "":
		parsed, valid := protocol.ParseTimestamp(strings.TrimSpace(input.DeliverAt))
		if !valid {
			return ScheduleMessageResult{}, invalidRequest("deliverAt must be a valid RFC 3339 timestamp")
		}
		firstDeliverAt = parsed.UTC()
	case input.InSeconds > 0:
		firstDeliverAt = now.Add(time.Duration(input.InSeconds) * time.Second).UTC()
	case input.EverySeconds > 0:
		firstDeliverAt = now.Add(time.Duration(input.EverySeconds) * time.Second).UTC()
	default:
		return ScheduleMessageResult{}, invalidRequest("specify inSeconds, deliverAt, or everySeconds")
	}

	var untilAtText string
	if rawUntil := strings.TrimSpace(input.UntilAt); rawUntil != "" {
		parsedUntil, valid := protocol.ParseTimestamp(rawUntil)
		if !valid {
			return ScheduleMessageResult{}, invalidRequest("untilAt must be a valid RFC 3339 timestamp")
		}
		if parsedUntil.Before(firstDeliverAt) {
			return ScheduleMessageResult{}, invalidRequest("untilAt must not be before first delivery time")
		}
		untilAtText = parsedUntil.UTC().Format(time.RFC3339Nano)
	}

	maxOccurrences := 1
	if input.EverySeconds > 0 {
		maxOccurrences = input.MaxOccurrences
		if maxOccurrences <= 0 {
			maxOccurrences = protocol.DefaultMaxRecurringOccurrences
		}
		if maxOccurrences > protocol.MaximumRecurringOccurrences {
			return ScheduleMessageResult{}, invalidRequest(fmt.Sprintf("maxOccurrences must not exceed %d", protocol.MaximumRecurringOccurrences))
		}
	} else if input.MaxOccurrences > 1 {
		return ScheduleMessageResult{}, invalidRequest("maxOccurrences > 1 requires everySeconds")
	}

	wake := input.Wake == nil || *input.Wake
	cancelOnClose := targetKind == protocol.ScheduleTargetKindRoom && (input.CancelOnThreadClose == nil || *input.CancelOnThreadClose)

	sourceKey := scheduleCreatorKey(caller)
	digest := canonicalDigest(struct {
		TargetKind          string                     `json:"targetKind"`
		ThreadID            string                     `json:"threadId,omitempty"`
		Channel             string                     `json:"channel,omitempty"`
		Kind                string                     `json:"kind,omitempty"`
		Topic               string                     `json:"topic,omitempty"`
		Labels              map[string]string          `json:"labels,omitempty"`
		Mentions            []string                   `json:"mentions,omitempty"`
		To                  string                     `json:"to,omitempty"`
		Subject             string                     `json:"subject,omitempty"`
		Body                string                     `json:"body"`
		Blocks              []protocol.StructuredBlock `json:"blocks,omitempty"`
		Wake                bool                       `json:"wake"`
		EverySeconds        int                        `json:"everySeconds,omitempty"`
		MaxOccurrences      int                        `json:"maxOccurrences"`
		UntilAt             string                     `json:"untilAt,omitempty"`
		CancelOnThreadClose bool                       `json:"cancelOnThreadClose,omitempty"`
	}{
		TargetKind:          targetKind,
		ThreadID:            threadID,
		Channel:             channel,
		Kind:                roomKind,
		Topic:               redactedTopic,
		Labels:              normalizedLabels,
		Mentions:            mentions,
		To:                  toAddress,
		Subject:             redactedSubject,
		Body:                redactedBody,
		Blocks:              normalizedBlocks,
		Wake:                wake,
		EverySeconds:        input.EverySeconds,
		MaxOccurrences:      maxOccurrences,
		UntilAt:             untilAtText,
		CancelOnThreadClose: cancelOnClose,
	})

	var result ScheduleMessageResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		for _, existing := range state.Schedules {
			if existing.SourceKey == sourceKey && existing.IdempotencyKey == idempotencyKey {
				if existing.Digest != digest {
					return conflictError("idempotency_conflict", "idempotencyKey was already used for a different scheduled message")
				}
				result = ScheduleMessageResult{Schedule: existing, Replayed: true}
				return nil
			}
		}

		if targetKind == protocol.ScheduleTargetKindRoom {
			if _, exists := state.Threads[threadID]; !exists {
				return notFound(fmt.Sprintf("thread %s does not exist", threadID))
			}
		}

		activeForCaller := 0
		activeForThread := 0
		for _, existing := range state.Schedules {
			if existing.Status != protocol.ScheduleStatusActive {
				continue
			}
			if existing.SourceKey == sourceKey {
				activeForCaller++
			}
			if threadID != "" && existing.ThreadID == threadID {
				activeForThread++
			}
		}
		if activeForCaller >= protocol.MaximumSchedulesPerCaller {
			return conflictError("schedule_limit_exceeded", fmt.Sprintf("caller has reached maximum of %d active schedules", protocol.MaximumSchedulesPerCaller))
		}
		if threadID != "" && activeForThread >= protocol.MaximumSchedulesPerThread {
			return conflictError("schedule_limit_exceeded", fmt.Sprintf("thread %s has reached maximum of %d active schedules", threadID, protocol.MaximumSchedulesPerThread))
		}

		nowText := now.UTC().Format(time.RFC3339Nano)
		record := protocol.ScheduledMessage{
			ID:                  protocol.NewIdentifier("sched"),
			SourceKey:           sourceKey,
			IdempotencyKey:      idempotencyKey,
			Digest:              digest,
			CreatorKind:         cmp.Or(caller.Kind, "orchestrator-client"),
			CreatorClientID:     caller.ClientID,
			CreatorOperatorID:   caller.OperatorID,
			CreatorRunID:        caller.RunID,
			CreatorInstanceID:   caller.InstanceID,
			PrincipalKind:       caller.PrincipalKind,
			PrincipalID:         caller.PrincipalID,
			VerifiedNode:        caller.VerifiedNode,
			TargetKind:          targetKind,
			ThreadID:            threadID,
			Channel:             channel,
			Kind:                roomKind,
			Topic:               redactedTopic,
			Labels:              protocol.CloneLabels(normalizedLabels),
			Mentions:            slices.Clone(mentions),
			To:                  toAddress,
			Subject:             redactedSubject,
			Body:                redactedBody,
			Blocks:              normalizedBlocks,
			Wake:                wake,
			EverySeconds:        input.EverySeconds,
			MaxOccurrences:      maxOccurrences,
			OccurrencesFired:    0,
			UntilAt:             untilAtText,
			CancelOnThreadClose: cancelOnClose,
			NextDeliverAt:       firstDeliverAt.Format(time.RFC3339Nano),
			Status:              protocol.ScheduleStatusActive,
			CreatedAt:           nowText,
			UpdatedAt:           nowText,
		}
		state.Schedules[record.ID] = record
		result = ScheduleMessageResult{Schedule: record, Replayed: false}
		return nil
	})
	if err == nil && !result.Replayed {
		service.logger.Info(
			"schedule",
			"created",
			activitylog.String("schedule", result.Schedule.ID),
			activitylog.String("target", result.Schedule.TargetKind),
			activitylog.String("thread", result.Schedule.ThreadID),
			activitylog.String("next", result.Schedule.NextDeliverAt),
		)
	}
	return result, err
}

func (service *Service) ListSchedules(caller CallerIdentity, input ListSchedulesInput) ([]protocol.ScheduledMessage, error) {
	if err := requireScopeOperation(caller, protocol.AccessOpRead); err != nil {
		return nil, err
	}
	if input.ThreadID != "" {
		if !protocol.IsIdentifier(input.ThreadID) {
			return nil, invalidRequest("threadId is malformed")
		}
		if err := requireScopeThread(caller, input.ThreadID); err != nil {
			return nil, err
		}
	}
	if input.TargetKind != "" && !slices.Contains(protocol.ScheduleTargetKinds, input.TargetKind) {
		return nil, invalidRequest("targetKind must be room or mail")
	}
	if input.Status != "" && !slices.Contains(protocol.ScheduleStatuses, input.Status) {
		return nil, invalidRequest("status must be active, completed, or cancelled")
	}

	var schedules []protocol.ScheduledMessage
	err := service.store.Read(func(state *store.State) error {
		for _, schedule := range state.Schedules {
			if input.ThreadID != "" && schedule.ThreadID != input.ThreadID {
				continue
			}
			if input.TargetKind != "" && schedule.TargetKind != input.TargetKind {
				continue
			}
			if input.Status != "" && schedule.Status != input.Status {
				continue
			}
			if caller.Scope != nil && schedule.ThreadID != "" && !protocol.ScopeAllowsThread(caller.Scope, schedule.ThreadID) {
				continue
			}
			schedules = append(schedules, schedule)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(schedules, func(left, right protocol.ScheduledMessage) int {
		if compared := cmp.Compare(left.NextDeliverAt, right.NextDeliverAt); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})
	if schedules == nil {
		schedules = []protocol.ScheduledMessage{}
	}
	return schedules, nil
}

func (service *Service) CancelSchedule(caller CallerIdentity, input CancelScheduleInput) (protocol.ScheduledMessage, error) {
	if err := requireScopeOperation(caller, protocol.AccessOpPost); err != nil {
		return protocol.ScheduledMessage{}, err
	}
	scheduleID := strings.TrimSpace(input.ScheduleID)
	if !protocol.IsIdentifier(scheduleID) {
		return protocol.ScheduledMessage{}, invalidRequest("scheduleId is required")
	}
	callerKey := scheduleCreatorKey(caller)
	var cancelled protocol.ScheduledMessage
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		schedule, exists := state.Schedules[scheduleID]
		if !exists {
			return notFound(fmt.Sprintf("schedule %s does not exist", scheduleID))
		}
		if schedule.ThreadID != "" {
			if err := requireScopeThread(caller, schedule.ThreadID); err != nil {
				return err
			}
		}
		isFullOperator := caller.Kind == "operator" && caller.Scope == nil && caller.GrantID == ""
		isCreator := schedule.SourceKey == callerKey ||
			(caller.GrantID == "" && caller.ClientID != "" && schedule.CreatorClientID == caller.ClientID)
		if !isFullOperator && !isCreator {
			return forbiddenError("only the schedule creator or an operator may cancel this schedule")
		}
		if schedule.Status == protocol.ScheduleStatusActive {
			nowText := service.nowUTC()
			schedule.Status = protocol.ScheduleStatusCancelled
			schedule.CancelledAt = nowText
			schedule.UpdatedAt = nowText
			state.Schedules[schedule.ID] = schedule
		}
		cancelled = schedule
		return nil
	})
	return cancelled, err
}

func cancelThreadSchedulesInState(state *store.State, threadID string, nowText string) {
	for scheduleID, schedule := range state.Schedules {
		if schedule.Status == protocol.ScheduleStatusActive &&
			schedule.TargetKind == protocol.ScheduleTargetKindRoom &&
			schedule.ThreadID == threadID &&
			schedule.CancelOnThreadClose {
			schedule.Status = protocol.ScheduleStatusCancelled
			schedule.CancelledAt = nowText
			schedule.UpdatedAt = nowText
			state.Schedules[scheduleID] = schedule
		}
	}
}

func (service *Service) processDueSchedules() int {
	now := service.now()
	var due []protocol.ScheduledMessage
	_ = service.store.Read(func(state *store.State) error {
		for _, schedule := range state.Schedules {
			if schedule.Status != protocol.ScheduleStatusActive {
				continue
			}
			deliverAt, valid := protocol.ParseTimestamp(schedule.NextDeliverAt)
			if !valid || !now.Before(deliverAt) {
				due = append(due, schedule)
			}
		}
		return nil
	})
	if len(due) == 0 {
		return 0
	}
	slices.SortFunc(due, func(left, right protocol.ScheduledMessage) int {
		if compared := cmp.Compare(left.NextDeliverAt, right.NextDeliverAt); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})

	deliveredCount := 0
	for _, schedule := range due {
		nextOccurrence := schedule.OccurrencesFired + 1
		deliveryKey := fmt.Sprintf("sched:%s:occ:%d", schedule.ID, nextOccurrence)
		wakeFlag := schedule.Wake
		deliveryCaller := CallerIdentity{
			Kind:          cmp.Or(schedule.CreatorKind, "operator"),
			ClientID:      schedule.CreatorClientID,
			OperatorID:    cmp.Or(schedule.CreatorOperatorID, "scheduler"),
			PrincipalKind: schedule.PrincipalKind,
			PrincipalID:   schedule.PrincipalID,
			VerifiedNode:  schedule.VerifiedNode,
		}

		var deliveryErr error
		skipBecauseThreadClosed := false
		if schedule.TargetKind == protocol.ScheduleTargetKindRoom {
			_ = service.store.Read(func(state *store.State) error {
				thread, exists := state.Threads[schedule.ThreadID]
				if !exists || (schedule.CancelOnThreadClose && thread.Status == "resolved") {
					skipBecauseThreadClosed = true
				}
				return nil
			})
			if !skipBecauseThreadClosed {
				_, deliveryErr = service.PostRoomMessage(deliveryCaller, PostRoomMessageInput{
					ThreadID:       schedule.ThreadID,
					IdempotencyKey: deliveryKey,
					Channel:        schedule.Channel,
					Kind:           schedule.Kind,
					Topic:          schedule.Topic,
					Labels:         protocol.CloneLabels(schedule.Labels),
					Body:           schedule.Body,
					Blocks:         schedule.Blocks,
					Mentions:       slices.Clone(schedule.Mentions),
					Wake:           &wakeFlag,
				})
			}
		} else {
			_, deliveryErr = service.SendMail(deliveryCaller, SendMailInput{
				IdempotencyKey: deliveryKey,
				To:             schedule.To,
				Subject:        schedule.Subject,
				Body:           schedule.Body,
				Blocks:         schedule.Blocks,
				Wake:           &wakeFlag,
			})
		}

		nowText := service.now().UTC().Format(time.RFC3339Nano)
		_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
			current, exists := state.Schedules[schedule.ID]
			if !exists || current.Status != protocol.ScheduleStatusActive {
				return nil
			}
			if skipBecauseThreadClosed {
				current.Status = protocol.ScheduleStatusCancelled
				current.CancelledAt = nowText
				current.UpdatedAt = nowText
				state.Schedules[current.ID] = current
				return nil
			}
			if deliveryErr != nil {
				current.LastError = service.redactor.Redact(deliveryErr.Error())
				current.UpdatedAt = nowText
			} else {
				current.LastError = ""
				current.OccurrencesFired = nextOccurrence
				current.LastDeliveredAt = nowText
				deliveredCount++
			}

			if current.EverySeconds <= 0 || current.OccurrencesFired >= current.MaxOccurrences {
				current.Status = protocol.ScheduleStatusCompleted
				current.UpdatedAt = nowText
				state.Schedules[current.ID] = current
				return nil
			}

			prevDeliverAt, valid := protocol.ParseTimestamp(current.NextDeliverAt)
			if !valid {
				prevDeliverAt = now
			}
			interval := time.Duration(current.EverySeconds) * time.Second
			nextDeliver := prevDeliverAt.Add(interval)
			for !nextDeliver.After(now) {
				nextDeliver = nextDeliver.Add(interval)
			}
			if current.UntilAt != "" {
				if untilAt, untilValid := protocol.ParseTimestamp(current.UntilAt); untilValid && nextDeliver.After(untilAt) {
					current.Status = protocol.ScheduleStatusCompleted
					current.UpdatedAt = nowText
					state.Schedules[current.ID] = current
					return nil
				}
			}
			current.NextDeliverAt = nextDeliver.UTC().Format(time.RFC3339Nano)
			current.UpdatedAt = nowText
			state.Schedules[current.ID] = current
			return nil
		})
	}
	return deliveredCount
}
