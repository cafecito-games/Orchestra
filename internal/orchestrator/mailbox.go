package orchestrator

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type SendTaskMessageInput struct {
	ThreadID       string                    `json:"threadId,omitempty"`
	ToTaskID       string                    `json:"toTaskId,omitempty"`
	ToOrchestrator bool                      `json:"toOrchestrator,omitempty"`
	Recipient      *protocol.MessageEndpoint `json:"recipient,omitempty"`
	ReplyToID      string                    `json:"replyToId,omitempty"`
	IdempotencyKey string                    `json:"idempotencyKey"`
	Kind           string                    `json:"kind"`
	Body           string                    `json:"body"`
	ArtifactIDs    []string                  `json:"artifactIds,omitempty"`
}

type SendTaskMessageResult struct {
	Message  protocol.TaskMessage `json:"message"`
	Replayed bool                 `json:"replayed"`
}

type GetThreadEventsInput struct {
	ThreadID string   `json:"threadId"`
	Cursor   string   `json:"cursor,omitempty"`
	Limit    int      `json:"limit,omitempty"`
	Types    []string `json:"types,omitempty"`
}

type ThreadEventsResponse struct {
	ThreadID      string                    `json:"threadId"`
	Events        []protocol.TaskEventEntry `json:"events"`
	NextCursor    string                    `json:"nextCursor"`
	Cursor        string                    `json:"cursor"`
	CursorExpired bool                      `json:"cursorExpired"`
	HasMore       bool                      `json:"hasMore"`
	HeadSequence  int64                     `json:"headSequence"`
}

type WaitForTaskEventsInput struct {
	Cursor              string   `json:"cursor,omitempty"`
	WaitMilliseconds    int      `json:"waitMs,omitempty"`
	TimeoutMilliseconds int      `json:"timeoutMilliseconds,omitempty"`
	Limit               int      `json:"limit,omitempty"`
	MaximumEvents       int      `json:"maximumEvents,omitempty"`
	Types               []string `json:"types,omitempty"`
	AcknowledgeThrough  int64    `json:"acknowledgeThrough,omitempty"`
}

func (service *Service) SendTaskMessage(
	caller CallerIdentity,
	input SendTaskMessageInput,
) (SendTaskMessageResult, error) {
	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" || !protocol.IsBoundedString(idempotencyKey, 128) {
		return SendTaskMessageResult{}, invalidRequest("idempotencyKey is required and must be at most 128 bytes")
	}
	if !slices.Contains(protocol.TaskMessageKinds, input.Kind) {
		return SendTaskMessageResult{}, invalidRequest("kind must be one of progress, question, guidance, blocker, result")
	}
	body := strings.TrimSpace(input.Body)
	if body == "" || !protocol.IsBoundedString(body, protocol.TextMaximumBytes) {
		return SendTaskMessageResult{}, invalidRequest("body must be non-empty and within 64 KiB")
	}

	if input.Recipient != nil {
		if input.Recipient.Kind == "orchestrator" {
			input.ToOrchestrator = true
		} else if input.Recipient.TaskID != "" && input.ToTaskID == "" {
			input.ToTaskID = input.Recipient.TaskID
		}
	}

	sourceKey := caller.SourceKey()
	digest := canonicalDigest(input)

	var result SendTaskMessageResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		for _, existing := range state.Messages {
			if existing.SourceKey == sourceKey && existing.IdempotencyKey == idempotencyKey {
				if existing.Digest != digest {
					return conflictError("idempotency_conflict", "idempotencyKey was already used for a different task message")
				}
				result = SendTaskMessageResult{Message: existing, Replayed: true}
				return nil
			}
		}

		threadID := strings.TrimSpace(input.ThreadID)
		if caller.Kind == "run" {
			if threadID == "" {
				threadID = caller.ThreadID
			} else if caller.ThreadID != "" && threadID != caller.ThreadID {
				return forbiddenError("worker run cannot send messages into another thread")
			}
		} else if threadID == "" && input.ToTaskID != "" {
			if targetTask, exists := state.Tasks[input.ToTaskID]; exists {
				threadID = targetTask.ThreadID
			}
		}

		if !protocol.IsIdentifier(threadID) {
			return invalidRequest("threadId is required")
		}
		if _, exists := state.Threads[threadID]; !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}

		var sender protocol.MessageEndpoint
		var recipient protocol.MessageEndpoint

		if caller.Kind == "run" {
			sender = protocol.MessageEndpoint{
				Kind:   "task",
				TaskID: caller.TaskID,
				RunID:  caller.RunID,
			}
			if input.ToOrchestrator || input.ToTaskID == "" {
				recipient = protocol.MessageEndpoint{Kind: "orchestrator"}
			} else {
				targetTask, exists := state.Tasks[input.ToTaskID]
				if !exists || targetTask.ThreadID != threadID {
					return notFound(fmt.Sprintf("recipient task %s does not exist in thread %s", input.ToTaskID, threadID))
				}
				if !isLineageRelated(state, caller.TaskID, targetTask.ID) {
					return forbiddenError("worker run can only message the orchestrator, its parent task, or its delegated child tasks")
				}
				recipient = protocol.MessageEndpoint{Kind: "task", TaskID: targetTask.ID}
			}
		} else {
			sender = protocol.MessageEndpoint{
				Kind:     "orchestrator",
				ClientID: caller.ClientID,
			}
			if input.ToTaskID == "" {
				return invalidRequest("orchestrator messages must specify toTaskId")
			}
			targetTask, exists := state.Tasks[input.ToTaskID]
			if !exists || targetTask.ThreadID != threadID {
				return notFound(fmt.Sprintf("recipient task %s does not exist in thread %s", input.ToTaskID, threadID))
			}
			recipient = protocol.MessageEndpoint{Kind: "task", TaskID: targetTask.ID}
		}

		if input.ReplyToID != "" {
			replyTarget, exists := state.Messages[input.ReplyToID]
			if !exists || replyTarget.ThreadID != threadID {
				return invalidRequest(fmt.Sprintf("replyToId %q does not exist in thread %s", input.ReplyToID, threadID))
			}
		}

		for _, artifactID := range input.ArtifactIDs {
			artifact, exists := state.Artifacts[artifactID]
			if !exists || artifact.ThreadID != threadID {
				return invalidRequest(fmt.Sprintf("artifact %q does not exist in thread %s", artifactID, threadID))
			}
		}

		nowText := service.nowUTC()
		message := protocol.TaskMessage{
			ID:             protocol.NewIdentifier("msg"),
			ThreadID:       threadID,
			SourceKey:      sourceKey,
			IdempotencyKey: idempotencyKey,
			Digest:         digest,
			Sender:         sender,
			Recipient:      recipient,
			ReplyToID:      input.ReplyToID,
			Kind:           input.Kind,
			Body:           service.redactor.Redact(body),
			ArtifactIDs:    slices.Clone(input.ArtifactIDs),
			CreatedAt:      nowText,
		}
		state.Messages[message.ID] = message
		result = SendTaskMessageResult{Message: message, Replayed: false}
		return nil
	})
	return result, err
}

func (service *Service) GetThreadEvents(
	caller CallerIdentity,
	input GetThreadEventsInput,
) (ThreadEventsResponse, error) {
	if !protocol.IsIdentifier(input.ThreadID) {
		return ThreadEventsResponse{}, invalidRequest("threadId is required")
	}
	limit := input.Limit
	if limit <= 0 || limit > protocol.MaximumEventsPerWait {
		limit = protocol.MaximumEventsPerWait
	}
	readerKey := caller.SourceKey()
	if readerKey == "" {
		readerKey = "orchestrator-client:default"
	}

	var cursorSequence int64
	if input.Cursor != "" {
		decoded, err := protocol.DecodeEventCursor(input.Cursor, input.ThreadID, readerKey)
		if err != nil {
			return ThreadEventsResponse{}, invalidRequest(err.Error())
		}
		cursorSequence = decoded
	}

	var response ThreadEventsResponse
	var acknowledgeSeq int64
	var shouldAcknowledge bool

	err := service.store.Read(func(state *store.State) error {
		if _, exists := state.Threads[input.ThreadID]; !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", input.ThreadID))
		}
		stream := state.EventStreams[input.ThreadID]
		response.ThreadID = input.ThreadID
		response.HeadSequence = stream.Head

		if cursorSequence < stream.Floor {
			nextCursor, cursorErr := protocol.EncodeEventCursor(input.ThreadID, readerKey, stream.Floor)
			if cursorErr != nil {
				return cursorErr
			}
			response.Events = []protocol.TaskEventEntry{}
			response.NextCursor = nextCursor
			response.Cursor = nextCursor
			response.CursorExpired = true
			response.HasMore = stream.Head > stream.Floor
			return nil
		}

		allEvents := state.ThreadEvents[input.ThreadID]
		filtered := make([]protocol.TaskEventEntry, 0, min(limit, len(allEvents)))
		lastScannedSequence := cursorSequence
		hasMore := false

		for _, entry := range allEvents {
			if entry.Sequence <= cursorSequence {
				continue
			}
			if len(filtered) >= limit {
				hasMore = true
				break
			}
			lastScannedSequence = entry.Sequence
			if len(input.Types) > 0 && !slices.Contains(input.Types, entry.Type) {
				continue
			}
			filtered = append(filtered, entry)
		}
		if !hasMore && stream.Head > lastScannedSequence {
			lastScannedSequence = stream.Head
		}

		nextCursor, cursorErr := protocol.EncodeEventCursor(input.ThreadID, readerKey, lastScannedSequence)
		if cursorErr != nil {
			return cursorErr
		}
		response.Events = filtered
		response.NextCursor = nextCursor
		response.Cursor = nextCursor
		response.HasMore = hasMore

		if attachment, attached := state.ActiveAttachmentForThread(input.ThreadID); attached &&
			caller.ClientID != "" && attachment.ClientID == caller.ClientID && lastScannedSequence > attachment.AcknowledgedSeq {
			acknowledgeSeq = lastScannedSequence
			shouldAcknowledge = true
		}
		return nil
	})
	if err != nil {
		return ThreadEventsResponse{}, err
	}

	if shouldAcknowledge {
		_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
			attachment, attached := state.ActiveAttachmentForThread(input.ThreadID)
			if attached && attachment.ClientID == caller.ClientID && acknowledgeSeq > attachment.AcknowledgedSeq {
				attachment.AcknowledgedSeq = acknowledgeSeq
				attachment.LastHeartbeatAt = service.nowUTC()
				attachment.UpdatedAt = attachment.LastHeartbeatAt
				state.Attachments[attachment.ID] = attachment
			}
			return nil
		})
	}
	return response, nil
}

func (service *Service) WaitForTaskEvents(
	operationContext context.Context,
	caller CallerIdentity,
	input WaitForTaskEventsInput,
) (ThreadEventsResponse, error) {
	if caller.Kind != "run" || caller.ThreadID == "" {
		return ThreadEventsResponse{}, forbiddenError("wait_for_task_events is only available to an active worker run")
	}

	waitMilliseconds := input.WaitMilliseconds
	if waitMilliseconds == 0 && input.TimeoutMilliseconds > 0 {
		waitMilliseconds = input.TimeoutMilliseconds
	}
	if waitMilliseconds < 0 {
		waitMilliseconds = 0
	}
	if waitMilliseconds > protocol.MaximumWaitMilliseconds {
		waitMilliseconds = protocol.MaximumWaitMilliseconds
	}
	limit := input.Limit
	if limit <= 0 && input.MaximumEvents > 0 {
		limit = input.MaximumEvents
	}
	if limit <= 0 || limit > protocol.MaximumEventsPerWait {
		limit = protocol.MaximumEventsPerWait
	}

	readerKey := caller.SourceKey()
	var cursorSequence int64
	if input.Cursor != "" {
		decoded, err := protocol.DecodeEventCursor(input.Cursor, caller.ThreadID, readerKey)
		if err != nil {
			return ThreadEventsResponse{}, invalidRequest(err.Error())
		}
		cursorSequence = decoded
	}

	eventChannel, unsubscribe := service.store.SubscribeEvents()
	defer unsubscribe()

	collectRelevant := func() (ThreadEventsResponse, error) {
		var result ThreadEventsResponse
		err := service.store.Read(func(state *store.State) error {
			stream := state.EventStreams[caller.ThreadID]
			result.ThreadID = caller.ThreadID
			result.HeadSequence = stream.Head

			if cursorSequence < stream.Floor {
				nextCursor, cursorErr := protocol.EncodeEventCursor(caller.ThreadID, readerKey, stream.Floor)
				if cursorErr != nil {
					return cursorErr
				}
				result.Events = []protocol.TaskEventEntry{}
				result.NextCursor = nextCursor
				result.Cursor = nextCursor
				result.CursorExpired = true
				result.HasMore = stream.Head > stream.Floor
				return nil
			}

			childTaskIDs := map[string]bool{}
			for _, candidate := range state.Tasks {
				if caller.TaskID != "" && candidate.ParentTaskID == caller.TaskID {
					childTaskIDs[candidate.ID] = true
				}
			}
			taskHasUnreadNudge := false
			if caller.TaskID != "" {
				if callerTask, exists := state.Tasks[caller.TaskID]; exists {
					taskHasUnreadNudge = callerTask.RequiredRoomSequence > callerTask.LastReadRoomSequence
				}
			}

			allEvents := state.ThreadEvents[caller.ThreadID]
			filtered := make([]protocol.TaskEventEntry, 0)
			lastScanned := cursorSequence
			hasMore := false

			for _, entry := range allEvents {
				if entry.Sequence <= cursorSequence {
					continue
				}
				if !isEventVisibleToWorker(entry, caller.TaskID, caller.RunID, childTaskIDs, taskHasUnreadNudge) {
					lastScanned = entry.Sequence
					continue
				}
				if len(input.Types) > 0 && !slices.Contains(input.Types, entry.Type) {
					lastScanned = entry.Sequence
					continue
				}
				if len(filtered) >= limit {
					hasMore = true
					break
				}
				lastScanned = entry.Sequence
				filtered = append(filtered, entry)
			}
			if !hasMore && stream.Head > lastScanned {
				lastScanned = stream.Head
			}

			nextCursor, cursorErr := protocol.EncodeEventCursor(caller.ThreadID, readerKey, lastScanned)
			if cursorErr != nil {
				return cursorErr
			}
			result.Events = filtered
			result.NextCursor = nextCursor
			result.Cursor = nextCursor
			result.HasMore = hasMore
			return nil
		})
		return result, err
	}

	initial, err := collectRelevant()
	if err != nil || initial.CursorExpired || len(initial.Events) > 0 || waitMilliseconds == 0 {
		return initial, err
	}

	waitTimer := time.NewTimer(time.Duration(waitMilliseconds) * time.Millisecond)
	defer waitTimer.Stop()

	for {
		select {
		case <-operationContext.Done():
			return collectRelevant()
		case <-waitTimer.C:
			return collectRelevant()
		case batch, ok := <-eventChannel:
			if !ok {
				return collectRelevant()
			}
			relevantThread := false
			for _, event := range batch {
				if event.ThreadID == caller.ThreadID {
					relevantThread = true
					break
				}
			}
			if !relevantThread {
				continue
			}
			polled, pollErr := collectRelevant()
			if pollErr != nil || polled.CursorExpired || len(polled.Events) > 0 {
				return polled, pollErr
			}
		}
	}
}

func isLineageRelated(state *store.State, callerTaskID, otherTaskID string) bool {
	if callerTaskID == "" || otherTaskID == "" {
		return false
	}
	if callerTaskID == otherTaskID {
		return true
	}
	callerTask, callerExists := state.Tasks[callerTaskID]
	otherTask, otherExists := state.Tasks[otherTaskID]
	if !callerExists || !otherExists {
		return false
	}
	return callerTask.ParentTaskID == otherTaskID || otherTask.ParentTaskID == callerTaskID
}

func isMessageVisibleToTask(message protocol.TaskMessage, taskID, runID string) bool {
	if taskID != "" && (message.Recipient.TaskID == taskID || message.Sender.TaskID == taskID) {
		return true
	}
	if runID != "" && (message.Recipient.RunID == runID || message.Sender.RunID == runID) {
		return true
	}
	return false
}

func isEventVisibleToWorker(
	entry protocol.TaskEventEntry,
	taskID string,
	runID string,
	childTaskIDs map[string]bool,
	taskHasUnreadNudge bool,
) bool {
	if entry.Type == "room.message_posted" {
		return false
	}
	if entry.Type == "room.nudged" {
		return taskHasUnreadNudge && taskID != "" && entry.TaskID == taskID
	}
	if entry.Type == "thread.updated" || entry.Type == "artifact.posted" {
		return true
	}
	if entry.Message != nil {
		return isMessageVisibleToTask(*entry.Message, taskID, runID) ||
			childTaskIDs[entry.Message.Sender.TaskID] ||
			childTaskIDs[entry.Message.Recipient.TaskID]
	}
	if taskID != "" && (entry.TaskID == taskID || childTaskIDs[entry.TaskID]) {
		return true
	}
	if runID != "" && entry.RunID == runID {
		return true
	}
	return false
}
