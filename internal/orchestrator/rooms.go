package orchestrator

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type ReadRoomInput struct {
	ThreadID      string   `json:"threadId,omitempty"`
	AfterSequence int64    `json:"afterSequence,omitempty"`
	Kinds         []string `json:"kinds,omitempty"`
	Limit         int      `json:"limit,omitempty"`
}

type ReadRoomResult struct {
	ThreadID         string                  `json:"threadId"`
	Room             protocol.ThreadRoomHead `json:"room"`
	Path             string                  `json:"path,omitempty"`
	Messages         []protocol.RoomMessage  `json:"messages"`
	HeadSequence     int64                   `json:"headSequence"`
	LastReadSequence int64                   `json:"lastReadSequence"`
	RequiredSequence int64                   `json:"requiredSequence"`
	UnreadNudge      bool                    `json:"unreadNudge"`
	ThroughSequence  int64                   `json:"throughSequence"`
	HasMore          bool                    `json:"hasMore"`
}

type PostRoomMessageInput struct {
	ThreadID           string   `json:"threadId,omitempty"`
	IdempotencyKey     string   `json:"idempotencyKey"`
	Kind               string   `json:"kind"`
	Topic              string   `json:"topic,omitempty"`
	Body               string   `json:"body"`
	ReplyToSequence    *int64   `json:"replyToSequence,omitempty"`
	SupersedesSequence *int64   `json:"supersedesSequence,omitempty"`
	ArtifactIDs        []string `json:"artifactIds,omitempty"`
	Nudge              bool     `json:"nudge,omitempty"`
	NudgeTaskIDs       []string `json:"nudgeTaskIds,omitempty"`
}

type PostRoomMessageResult struct {
	ThreadID      string                  `json:"threadId"`
	Room          protocol.ThreadRoomHead `json:"room"`
	Message       protocol.RoomMessage    `json:"message"`
	NudgedTaskIDs []string                `json:"nudgedTaskIds"`
	Replayed      bool                    `json:"replayed"`
}

type NudgeRoomInput struct {
	ThreadID       string   `json:"threadId"`
	IdempotencyKey string   `json:"idempotencyKey"`
	TaskIDs        []string `json:"taskIds,omitempty"`
}

type NudgeRoomResult struct {
	ThreadID         string                  `json:"threadId"`
	Room             protocol.ThreadRoomHead `json:"room"`
	RequiredSequence int64                   `json:"requiredSequence"`
	NudgedTaskIDs    []string                `json:"nudgedTaskIds"`
	Replayed         bool                    `json:"replayed"`
}

func (service *Service) ReadRoom(caller CallerIdentity, input ReadRoomInput) (ReadRoomResult, error) {
	threadID := strings.TrimSpace(input.ThreadID)
	if caller.Kind == "run" {
		if caller.RunID == "" || caller.ThreadID == "" {
			return ReadRoomResult{}, forbiddenError("read_room requires an active worker run")
		}
		if threadID != "" && threadID != caller.ThreadID {
			return ReadRoomResult{}, forbiddenError("worker run cannot read another thread's room")
		}
		threadID = caller.ThreadID
	} else if !protocol.IsIdentifier(threadID) {
		return ReadRoomResult{}, invalidRequest("threadId is required")
	}

	if input.AfterSequence < 0 {
		return ReadRoomResult{}, invalidRequest("afterSequence must be greater than or equal to 0")
	}
	for _, kind := range input.Kinds {
		if !slices.Contains(protocol.RoomMessageKinds, kind) {
			return ReadRoomResult{}, invalidRequest(fmt.Sprintf("unknown room message kind %q", kind))
		}
	}
	if input.Limit < 0 {
		return ReadRoomResult{}, invalidRequest("limit must be positive")
	}
	limit := input.Limit
	if limit == 0 {
		limit = protocol.DefaultRoomReadLimit
	}
	if limit > protocol.MaximumRoomReadLimit {
		limit = protocol.MaximumRoomReadLimit
	}

	var result ReadRoomResult
	shouldAdvanceTask := false
	var targetAdvanceSequence int64

	err := service.store.Read(func(state *store.State) error {
		thread, exists := state.Threads[threadID]
		if !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}
		head, hasHead := state.RoomHeads[threadID]
		if !hasHead {
			head = protocol.ThreadRoomHead{
				ThreadID:      threadID,
				DirectoryName: protocol.RoomDirectoryName(threadID, thread.CreatedAt),
				HeadSequence:  0,
				CreatedAt:     thread.CreatedAt,
				UpdatedAt:     thread.UpdatedAt,
			}
		}

		allMessages := state.RoomMessages[threadID]
		filtered := make([]protocol.RoomMessage, 0, min(limit, len(allMessages)))
		throughSequence := min(input.AfterSequence, head.HeadSequence)
		hasMore := false

		for _, entry := range allMessages {
			if entry.Sequence <= input.AfterSequence {
				continue
			}
			if len(filtered) >= limit {
				hasMore = true
				break
			}
			throughSequence = entry.Sequence
			if len(input.Kinds) > 0 && !slices.Contains(input.Kinds, entry.Kind) {
				continue
			}
			filtered = append(filtered, entry)
		}
		if !hasMore && head.HeadSequence > throughSequence && input.AfterSequence <= head.HeadSequence {
			throughSequence = head.HeadSequence
		}

		result.ThreadID = threadID
		result.Room = head
		result.Messages = filtered
		result.HeadSequence = head.HeadSequence
		result.ThroughSequence = throughSequence
		result.HasMore = hasMore

		if caller.Kind == "run" && caller.TaskID != "" {
			if task, taskExists := state.Tasks[caller.TaskID]; taskExists && task.ThreadID == threadID {
				lastRead := task.LastReadRoomSequence
				canAdvance := input.AfterSequence <= lastRead || task.RequiredRoomSequence <= lastRead
				if canAdvance && throughSequence > lastRead {
					lastRead = throughSequence
					targetAdvanceSequence = throughSequence
					shouldAdvanceTask = true
				}
				result.LastReadSequence = lastRead
				result.RequiredSequence = task.RequiredRoomSequence
				result.UnreadNudge = task.RequiredRoomSequence > lastRead
			}
		}
		return nil
	})
	if err != nil {
		return ReadRoomResult{}, err
	}

	if shouldAdvanceTask {
		_, mutateErr := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
			task, taskExists := state.Tasks[caller.TaskID]
			if !taskExists || task.ThreadID != threadID {
				return nil
			}
			if targetAdvanceSequence > task.LastReadRoomSequence {
				task.LastReadRoomSequence = targetAdvanceSequence
				task.UpdatedAt = service.nowUTC()
				state.Tasks[task.ID] = task
			}
			result.LastReadSequence = task.LastReadRoomSequence
			result.RequiredSequence = task.RequiredRoomSequence
			result.UnreadNudge = task.RequiredRoomSequence > task.LastReadRoomSequence
			return nil
		})
		if mutateErr != nil {
			return ReadRoomResult{}, mutateErr
		}
	}

	if caller.Kind != "run" {
		if path, exists, pathErr := service.store.RoomJSONLPath(threadID); pathErr == nil && exists {
			result.Path = path
		}
	}

	return result, nil
}

func (service *Service) PostRoomMessage(caller CallerIdentity, input PostRoomMessageInput) (PostRoomMessageResult, error) {
	threadID := strings.TrimSpace(input.ThreadID)
	if caller.Kind == "run" {
		if caller.RunID == "" || caller.ThreadID == "" {
			return PostRoomMessageResult{}, forbiddenError("post_room_message requires an active worker run")
		}
		if threadID != "" && threadID != caller.ThreadID {
			return PostRoomMessageResult{}, forbiddenError("worker run cannot post to another thread's room")
		}
		threadID = caller.ThreadID
		if !caller.CanDelegate && (input.Nudge || len(input.NudgeTaskIDs) > 0) {
			return PostRoomMessageResult{}, forbiddenError("non-delegating worker runs cannot nudge tasks")
		}
		if caller.CanDelegate && input.Nudge {
			return PostRoomMessageResult{}, forbiddenError("delegating worker runs must specify nudgeTaskIds limited to delegated tasks")
		}
	} else if !protocol.IsIdentifier(threadID) {
		return PostRoomMessageResult{}, invalidRequest("threadId is required")
	}

	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" || !protocol.IsBoundedString(idempotencyKey, 128) {
		return PostRoomMessageResult{}, invalidRequest("idempotencyKey must be between 1 and 128 bytes")
	}
	kind := strings.TrimSpace(input.Kind)
	if !slices.Contains(protocol.RoomMessageKinds, kind) {
		return PostRoomMessageResult{}, invalidRequest(fmt.Sprintf("kind must be one of %s", strings.Join(protocol.RoomMessageKinds, ", ")))
	}
	topic := strings.TrimSpace(input.Topic)
	if !protocol.IsBoundedString(topic, protocol.RoomTopicMaximumBytes) {
		return PostRoomMessageResult{}, invalidRequest(fmt.Sprintf("topic must not exceed %d bytes", protocol.RoomTopicMaximumBytes))
	}
	body := strings.TrimSpace(input.Body)
	if body == "" || !protocol.IsBoundedString(body, protocol.TextMaximumBytes) {
		return PostRoomMessageResult{}, invalidRequest(fmt.Sprintf("body must be a non-empty string up to %d bytes", protocol.TextMaximumBytes))
	}

	normalizedInput := PostRoomMessageInput{
		ThreadID:           threadID,
		IdempotencyKey:     idempotencyKey,
		Kind:               kind,
		Topic:              topic,
		Body:               body,
		ReplyToSequence:    input.ReplyToSequence,
		SupersedesSequence: input.SupersedesSequence,
		ArtifactIDs:        slices.Clone(input.ArtifactIDs),
		Nudge:              input.Nudge,
		NudgeTaskIDs:       slices.Clone(input.NudgeTaskIDs),
	}
	digest := canonicalDigest(normalizedInput)
	sourceKey := caller.SourceKey()

	var result PostRoomMessageResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		thread, exists := state.Threads[threadID]
		if !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}
		head, hasHead := state.RoomHeads[threadID]
		if !hasHead {
			head = protocol.ThreadRoomHead{
				ThreadID:      threadID,
				DirectoryName: protocol.RoomDirectoryName(threadID, thread.CreatedAt),
				HeadSequence:  0,
				CreatedAt:     thread.CreatedAt,
				UpdatedAt:     thread.UpdatedAt,
			}
		}

		receiptKey := sourceKey + "\x00" + threadID + "\x00" + idempotencyKey
		if existingReceipt, found := state.RoomMessageReceipts[receiptKey]; found {
			if existingReceipt.Digest != digest {
				return conflictError("idempotency_conflict", "idempotencyKey was already used for a different room message")
			}
			for _, existingMessage := range state.RoomMessages[threadID] {
				if existingMessage.Sequence == existingReceipt.Sequence {
					nudgedCopy := slices.Clone(existingReceipt.NudgedTaskIDs)
					if nudgedCopy == nil {
						nudgedCopy = []string{}
					}
					result = PostRoomMessageResult{
						ThreadID:      threadID,
						Room:          head,
						Message:       existingMessage,
						NudgedTaskIDs: nudgedCopy,
						Replayed:      true,
					}
					return nil
				}
			}
		}

		if input.ReplyToSequence != nil {
			if *input.ReplyToSequence < 1 || *input.ReplyToSequence > head.HeadSequence {
				return invalidRequest(fmt.Sprintf("replyToSequence %d is out of range (1..%d)", *input.ReplyToSequence, head.HeadSequence))
			}
		}
		if input.SupersedesSequence != nil {
			if *input.SupersedesSequence < 1 || *input.SupersedesSequence > head.HeadSequence {
				return invalidRequest(fmt.Sprintf("supersedesSequence %d is out of range (1..%d)", *input.SupersedesSequence, head.HeadSequence))
			}
		}

		var validatedArtifactIDs []string
		for _, artifactID := range input.ArtifactIDs {
			trimmedArtifactID := strings.TrimSpace(artifactID)
			artifact, artifactExists := state.Artifacts[trimmedArtifactID]
			if !artifactExists || artifact.ThreadID != threadID {
				return invalidRequest(fmt.Sprintf("unknown artifact %q in thread %s", artifactID, threadID))
			}
			if !slices.Contains(validatedArtifactIDs, trimmedArtifactID) {
				validatedArtifactIDs = append(validatedArtifactIDs, trimmedArtifactID)
			}
		}

		var validatedNudgeTaskIDs []string
		for _, targetTaskID := range input.NudgeTaskIDs {
			trimmedTaskID := strings.TrimSpace(targetTaskID)
			targetTask, taskExists := state.Tasks[trimmedTaskID]
			if !taskExists || targetTask.ThreadID != threadID {
				return invalidRequest(fmt.Sprintf("unknown task %q in thread %s", targetTaskID, threadID))
			}
			if caller.Kind == "run" && !isDelegatedDescendant(state, caller.TaskID, trimmedTaskID) {
				return forbiddenError(fmt.Sprintf("delegating worker run cannot nudge non-delegated task %s", trimmedTaskID))
			}
			if !slices.Contains(validatedNudgeTaskIDs, trimmedTaskID) {
				validatedNudgeTaskIDs = append(validatedNudgeTaskIDs, trimmedTaskID)
			}
		}

		nowText := service.nowUTC()
		nextSequence := head.HeadSequence + 1

		var replyCopy *int64
		if input.ReplyToSequence != nil {
			value := *input.ReplyToSequence
			replyCopy = &value
		}
		var supersedesCopy *int64
		if input.SupersedesSequence != nil {
			value := *input.SupersedesSequence
			supersedesCopy = &value
		}

		message := protocol.RoomMessage{
			Sequence:           nextSequence,
			ID:                 protocol.NewRoomMessageIdentifier(),
			ThreadID:           threadID,
			CreatedAt:          nowText,
			Author:             service.roomAuthorForCaller(state, caller),
			Kind:               kind,
			Topic:              service.redactor.Redact(topic),
			Body:               service.redactor.Redact(body),
			ReplyToSequence:    replyCopy,
			SupersedesSequence: supersedesCopy,
			ArtifactIDs:        validatedArtifactIDs,
		}

		head.HeadSequence = nextSequence
		head.UpdatedAt = nowText
		state.RoomHeads[threadID] = head
		state.RoomMessages[threadID] = append(state.RoomMessages[threadID], message)

		if caller.Kind == "run" && caller.TaskID != "" {
			if callerTask, taskExists := state.Tasks[caller.TaskID]; taskExists && callerTask.ThreadID == threadID {
				if callerTask.LastReadRoomSequence == nextSequence-1 {
					callerTask.LastReadRoomSequence = nextSequence
					callerTask.UpdatedAt = nowText
					state.Tasks[callerTask.ID] = callerTask
				}
			}
		}

		detail := map[string]any{
			"roomSequence": nextSequence,
			"kind":         message.Kind,
		}
		if message.Topic != "" {
			detail["topic"] = message.Topic
		}
		mutation.EmitEvent(protocol.TaskEventEntry{
			ThreadID:   threadID,
			Type:       "room.message_posted",
			TaskID:     caller.TaskID,
			RunID:      caller.RunID,
			MessageID:  message.ID,
			Summary:    fmt.Sprintf("room %s #%d posted", message.Kind, nextSequence),
			Detail:     detail,
			OccurredAt: nowText,
		})

		nudgedTaskIDs := []string{}
		if input.Nudge || len(validatedNudgeTaskIDs) > 0 {
			nudgedTaskIDs = applyRoomNudgeInState(state, mutation, threadID, nextSequence, input.Nudge, validatedNudgeTaskIDs, nowText)
		}

		state.RoomMessageReceipts[receiptKey] = protocol.RoomMessageReceipt{
			ID:             protocol.NewIdentifier("rmrcpt"),
			ThreadID:       threadID,
			SourceKey:      sourceKey,
			IdempotencyKey: idempotencyKey,
			Digest:         digest,
			Sequence:       nextSequence,
			NudgedTaskIDs:  slices.Clone(nudgedTaskIDs),
			CreatedAt:      nowText,
		}

		result = PostRoomMessageResult{
			ThreadID:      threadID,
			Room:          head,
			Message:       message,
			NudgedTaskIDs: nudgedTaskIDs,
			Replayed:      false,
		}
		return nil
	})
	if err != nil {
		return PostRoomMessageResult{}, err
	}
	return result, nil
}

func (service *Service) NudgeRoom(caller CallerIdentity, input NudgeRoomInput) (NudgeRoomResult, error) {
	if caller.Kind == "run" {
		return NudgeRoomResult{}, forbiddenError("nudge_room is only available to orchestrator and operator callers")
	}
	threadID := strings.TrimSpace(input.ThreadID)
	if !protocol.IsIdentifier(threadID) {
		return NudgeRoomResult{}, invalidRequest("threadId is required")
	}
	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" || !protocol.IsBoundedString(idempotencyKey, 128) {
		return NudgeRoomResult{}, invalidRequest("idempotencyKey must be between 1 and 128 bytes")
	}

	normalizedInput := NudgeRoomInput{
		ThreadID:       threadID,
		IdempotencyKey: idempotencyKey,
		TaskIDs:        slices.Clone(input.TaskIDs),
	}
	digest := canonicalDigest(normalizedInput)
	sourceKey := caller.SourceKey()

	var result NudgeRoomResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		thread, exists := state.Threads[threadID]
		if !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}
		head, hasHead := state.RoomHeads[threadID]
		if !hasHead {
			head = protocol.ThreadRoomHead{
				ThreadID:      threadID,
				DirectoryName: protocol.RoomDirectoryName(threadID, thread.CreatedAt),
				HeadSequence:  0,
				CreatedAt:     thread.CreatedAt,
				UpdatedAt:     thread.UpdatedAt,
			}
		}

		receiptKey := sourceKey + "\x00" + threadID + "\x00" + idempotencyKey
		if existingReceipt, found := state.RoomNudgeReceipts[receiptKey]; found {
			if existingReceipt.Digest != digest {
				return conflictError("idempotency_conflict", "idempotencyKey was already used for a different room nudge")
			}
			nudgedCopy := slices.Clone(existingReceipt.NudgedTaskIDs)
			if nudgedCopy == nil {
				nudgedCopy = []string{}
			}
			result = NudgeRoomResult{
				ThreadID:         threadID,
				Room:             head,
				RequiredSequence: existingReceipt.RequiredSequence,
				NudgedTaskIDs:    nudgedCopy,
				Replayed:         true,
			}
			return nil
		}

		if head.HeadSequence <= 0 {
			return invalidRequest("cannot nudge tasks when the thread room has no messages")
		}

		var validatedTaskIDs []string
		for _, targetTaskID := range input.TaskIDs {
			trimmedTaskID := strings.TrimSpace(targetTaskID)
			targetTask, taskExists := state.Tasks[trimmedTaskID]
			if !taskExists || targetTask.ThreadID != threadID {
				return invalidRequest(fmt.Sprintf("unknown task %q in thread %s", targetTaskID, threadID))
			}
			if !slices.Contains(validatedTaskIDs, trimmedTaskID) {
				validatedTaskIDs = append(validatedTaskIDs, trimmedTaskID)
			}
		}

		nowText := service.nowUTC()
		allNonTerminal := len(validatedTaskIDs) == 0
		nudgedTaskIDs := applyRoomNudgeInState(state, mutation, threadID, head.HeadSequence, allNonTerminal, validatedTaskIDs, nowText)

		state.RoomNudgeReceipts[receiptKey] = protocol.RoomNudgeReceipt{
			ID:               protocol.NewIdentifier("rnrcpt"),
			ThreadID:         threadID,
			SourceKey:        sourceKey,
			IdempotencyKey:   idempotencyKey,
			Digest:           digest,
			RequiredSequence: head.HeadSequence,
			NudgedTaskIDs:    slices.Clone(nudgedTaskIDs),
			CreatedAt:        nowText,
		}

		result = NudgeRoomResult{
			ThreadID:         threadID,
			Room:             head,
			RequiredSequence: head.HeadSequence,
			NudgedTaskIDs:    nudgedTaskIDs,
			Replayed:         false,
		}
		return nil
	})
	if err != nil {
		return NudgeRoomResult{}, err
	}
	return result, nil
}

func (service *Service) roomAuthorForCaller(state *store.State, caller CallerIdentity) protocol.RoomAuthor {
	switch caller.Kind {
	case "run":
		title := ""
		if caller.TaskID != "" {
			if task, exists := state.Tasks[caller.TaskID]; exists {
				title = service.redactor.Redact(task.Title)
			}
		}
		return protocol.RoomAuthor{
			Kind:   "task",
			TaskID: caller.TaskID,
			RunID:  caller.RunID,
			Title:  title,
		}
	case "orchestrator-client":
		return protocol.RoomAuthor{
			Kind:     "orchestrator",
			ClientID: service.redactor.Redact(caller.ClientID),
		}
	default:
		operatorID := caller.OperatorID
		if operatorID == "" {
			operatorID = "cli"
		}
		return protocol.RoomAuthor{
			Kind:       "operator",
			OperatorID: service.redactor.Redact(operatorID),
		}
	}
}

func isDelegatedDescendant(state *store.State, ancestorTaskID string, targetTaskID string) bool {
	if ancestorTaskID == "" || targetTaskID == "" || ancestorTaskID == targetTaskID {
		return false
	}
	currentID := targetTaskID
	for range len(state.Tasks) + 1 {
		task, exists := state.Tasks[currentID]
		if !exists || task.ParentTaskID == "" {
			return false
		}
		if task.ParentTaskID == ancestorTaskID {
			return true
		}
		currentID = task.ParentTaskID
	}
	return false
}

func applyRoomNudgeInState(
	state *store.State,
	mutation *store.Mutation,
	threadID string,
	requiredSequence int64,
	allNonTerminal bool,
	explicitTaskIDs []string,
	nowText string,
) []string {
	var targetTaskIDs []string
	if allNonTerminal {
		for taskID, task := range state.Tasks {
			if task.ThreadID == threadID && !protocol.IsTerminalTaskStatus(task.Status) {
				targetTaskIDs = append(targetTaskIDs, taskID)
			}
		}
		slices.Sort(targetTaskIDs)
	} else {
		for _, taskID := range explicitTaskIDs {
			task, exists := state.Tasks[taskID]
			if exists && task.ThreadID == threadID && !protocol.IsTerminalTaskStatus(task.Status) {
				targetTaskIDs = append(targetTaskIDs, taskID)
			}
		}
		slices.Sort(targetTaskIDs)
	}

	nudged := make([]string, 0, len(targetTaskIDs))
	for _, taskID := range targetTaskIDs {
		task := state.Tasks[taskID]
		if requiredSequence > task.RequiredRoomSequence {
			task.RequiredRoomSequence = requiredSequence
			task.UpdatedAt = nowText
			state.Tasks[taskID] = task
		}
		nudged = append(nudged, taskID)
		mutation.EmitEvent(protocol.TaskEventEntry{
			ThreadID: threadID,
			Type:     "room.nudged",
			TaskID:   taskID,
			Summary:  fmt.Sprintf("room nudge through sequence %d", task.RequiredRoomSequence),
			Detail: map[string]any{
				"requiredRoomSequence": task.RequiredRoomSequence,
				"lastReadRoomSequence": task.LastReadRoomSequence,
			},
			OccurredAt: nowText,
		})
	}
	return nudged
}

func buildTaskRoomSummary(state *store.State, threadID string, task protocol.Task) protocol.TaskRoomSummary {
	headSequence := state.RoomHeads[threadID].HeadSequence
	return protocol.TaskRoomSummary{
		HeadSequence:     headSequence,
		LastReadSequence: task.LastReadRoomSequence,
		RequiredSequence: task.RequiredRoomSequence,
		UnreadNudge:      task.RequiredRoomSequence > task.LastReadRoomSequence,
	}
}
