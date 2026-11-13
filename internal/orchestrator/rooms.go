package orchestrator

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/activitylog"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type ReadRoomInput struct {
	ThreadID      string   `json:"threadId,omitempty"`
	Channel       string   `json:"channel,omitempty"`
	AfterSequence int64    `json:"afterSequence,omitempty"`
	Kinds         []string `json:"kinds,omitempty"`
	Limit         int      `json:"limit,omitempty"`
}

type ReadRoomResult struct {
	ThreadID         string                  `json:"threadId"`
	Channel          string                  `json:"channel,omitempty"`
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
	Channel            string   `json:"channel,omitempty"`
	Kind               string   `json:"kind"`
	Topic              string   `json:"topic,omitempty"`
	Body               string   `json:"body"`
	Mentions           []string `json:"mentions,omitempty"`
	Wake               *bool    `json:"wake,omitempty"`
	ReplyToSequence    *int64   `json:"replyToSequence,omitempty"`
	SupersedesSequence *int64   `json:"supersedesSequence,omitempty"`
	ArtifactIDs        []string `json:"artifactIds,omitempty"`
	Nudge              bool     `json:"nudge,omitempty"`
	NudgeTaskIDs       []string `json:"nudgeTaskIds,omitempty"`
}

type PostRoomMessageResult struct {
	ThreadID         string                  `json:"threadId"`
	Room             protocol.ThreadRoomHead `json:"room"`
	Message          protocol.RoomMessage    `json:"message"`
	NudgedTaskIDs    []string                `json:"nudgedTaskIds"`
	WokenInstanceIDs []string                `json:"wokenInstanceIds,omitempty"`
	WakeTaskIDs      []string                `json:"wakeTaskIds,omitempty"`
	Replayed         bool                    `json:"replayed"`
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

	readChannel, channelErr := protocol.NormalizeRoomReadChannel(input.Channel)
	if channelErr != nil {
		return ReadRoomResult{}, invalidRequest(channelErr.Error())
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
	shouldAdvanceInstance := false
	shouldAdvanceAttachment := false
	var targetTaskSequence int64
	var targetInstanceSequence int64
	var targetAttachmentSequence int64

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

		var callerSubscriptions []string
		if caller.Kind == "run" {
			instanceID := resolveCallerInstanceID(state, caller)
			if instance, hasInstance := state.Instances[instanceID]; hasInstance {
				callerSubscriptions = instance.RoomChannels
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
			if !roomEntryMatchesReadChannel(entry, readChannel, caller.Kind, callerSubscriptions) {
				throughSequence = entry.Sequence
				continue
			}
			if len(input.Kinds) > 0 && !slices.Contains(input.Kinds, entry.Kind) {
				throughSequence = entry.Sequence
				continue
			}
			if len(filtered) >= limit {
				hasMore = true
				break
			}
			throughSequence = entry.Sequence
			filtered = append(filtered, entry)
		}
		if !hasMore && head.HeadSequence > throughSequence && input.AfterSequence <= head.HeadSequence {
			throughSequence = head.HeadSequence
		}

		includesMain := readChannel == "" || readChannel == protocol.DefaultRoomChannel || readChannel == protocol.AllRoomChannels

		result.ThreadID = threadID
		result.Channel = readChannel
		result.Room = head
		result.Messages = filtered
		result.HeadSequence = head.HeadSequence
		result.ThroughSequence = throughSequence
		result.HasMore = hasMore

		if caller.Kind == "run" {
			if caller.TaskID != "" {
				if task, taskExists := state.Tasks[caller.TaskID]; taskExists && task.ThreadID == threadID {
					lastRead := task.LastReadRoomSequence
					skippedMain := !includesMain && hasMainRoomMessageBetween(allMessages, lastRead, throughSequence)
					canAdvance := !skippedMain && (input.AfterSequence <= lastRead || task.RequiredRoomSequence <= lastRead)
					if canAdvance && throughSequence > lastRead {
						lastRead = throughSequence
						targetTaskSequence = throughSequence
						shouldAdvanceTask = true
					}
					result.LastReadSequence = lastRead
					result.RequiredSequence = task.RequiredRoomSequence
					result.UnreadNudge = task.RequiredRoomSequence > lastRead
				}
			}
			instanceID := resolveCallerInstanceID(state, caller)
			if instance, instanceExists := state.Instances[instanceID]; instanceExists && instance.ThreadID == threadID {
				skippedSubscribed := readChannel != "" && readChannel != protocol.AllRoomChannels &&
					hasOtherSubscribedChannelRoomMessageBetween(
						allMessages,
						readChannel,
						instance.RoomChannels,
						instance.LastReadRoomSequence,
						throughSequence,
					)
				if !skippedSubscribed && len(input.Kinds) == 0 &&
					input.AfterSequence <= instance.LastReadRoomSequence &&
					throughSequence > instance.LastReadRoomSequence {
					targetInstanceSequence = throughSequence
					shouldAdvanceInstance = true
				}
				if caller.TaskID == "" {
					lastRead := instance.LastReadRoomSequence
					if shouldAdvanceInstance {
						lastRead = targetInstanceSequence
					}
					result.LastReadSequence = lastRead
				}
			}
		} else if caller.ClientID != "" {
			if attachment, attached := state.ActiveAttachmentForThread(threadID); attached && attachment.ClientID == caller.ClientID {
				skippedUnread := readChannel != "" && readChannel != protocol.AllRoomChannels &&
					hasOtherChannelRoomMessageBetween(allMessages, readChannel, attachment.LastReadRoomSequence, throughSequence)
				if !skippedUnread && len(input.Kinds) == 0 &&
					input.AfterSequence <= attachment.LastReadRoomSequence &&
					throughSequence > attachment.LastReadRoomSequence {
					targetAttachmentSequence = throughSequence
					shouldAdvanceAttachment = true
				}
				lastRead := attachment.LastReadRoomSequence
				if shouldAdvanceAttachment {
					lastRead = targetAttachmentSequence
				}
				result.LastReadSequence = lastRead
			}
		}
		return nil
	})
	if err != nil {
		return ReadRoomResult{}, err
	}

	if shouldAdvanceTask || shouldAdvanceInstance || shouldAdvanceAttachment {
		_, mutateErr := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
			nowText := service.nowUTC()
			if shouldAdvanceTask && caller.TaskID != "" {
				if task, taskExists := state.Tasks[caller.TaskID]; taskExists && task.ThreadID == threadID {
					if targetTaskSequence > task.LastReadRoomSequence {
						task.LastReadRoomSequence = targetTaskSequence
						task.UpdatedAt = nowText
						state.Tasks[task.ID] = task
					}
					result.LastReadSequence = task.LastReadRoomSequence
					result.RequiredSequence = task.RequiredRoomSequence
					result.UnreadNudge = task.RequiredRoomSequence > task.LastReadRoomSequence
				}
			}
			if shouldAdvanceInstance {
				instanceID := resolveCallerInstanceID(state, caller)
				if instance, instanceExists := state.Instances[instanceID]; instanceExists && instance.ThreadID == threadID {
					if targetInstanceSequence > instance.LastReadRoomSequence {
						instance.LastReadRoomSequence = targetInstanceSequence
						instance.UpdatedAt = nowText
						state.Instances[instance.ID] = instance
					}
				}
			}
			if shouldAdvanceAttachment && caller.ClientID != "" {
				if attachment, attached := state.ActiveAttachmentForThread(threadID); attached && attachment.ClientID == caller.ClientID {
					if targetAttachmentSequence > attachment.LastReadRoomSequence {
						attachment.LastReadRoomSequence = targetAttachmentSequence
						attachment.LastHeartbeatAt = nowText
						attachment.UpdatedAt = nowText
						state.Attachments[attachment.ID] = attachment
					}
					result.LastReadSequence = attachment.LastReadRoomSequence
				}
			}
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
		if !caller.CanDelegate && input.Nudge {
			return PostRoomMessageResult{}, forbiddenError("non-delegating worker runs cannot broadcast thread-wide room nudges; target nudgeTaskIds or @mention handles")
		}
	} else if !protocol.IsIdentifier(threadID) {
		return PostRoomMessageResult{}, invalidRequest("threadId is required")
	}

	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" || !protocol.IsBoundedString(idempotencyKey, 128) {
		return PostRoomMessageResult{}, invalidRequest("idempotencyKey must be between 1 and 128 bytes")
	}
	normalizedChannel, channelErr := protocol.NormalizeRoomChannel(input.Channel)
	if channelErr != nil {
		return PostRoomMessageResult{}, invalidRequest(channelErr.Error())
	}
	if service.redactor.ContainsSecret(normalizedChannel) {
		return PostRoomMessageResult{}, invalidRequest("room channel must not contain secret material")
	}
	kind := strings.TrimSpace(input.Kind)
	if !slices.Contains(protocol.RoomMessageKinds, kind) {
		return PostRoomMessageResult{}, invalidRequest("kind must be one of " + strings.Join(protocol.RoomMessageKinds, ", "))
	}
	topic := strings.TrimSpace(input.Topic)
	if !protocol.IsBoundedString(topic, protocol.RoomTopicMaximumBytes) {
		return PostRoomMessageResult{}, invalidRequest(fmt.Sprintf("topic must not exceed %d bytes", protocol.RoomTopicMaximumBytes))
	}
	body := strings.TrimSpace(input.Body)
	if body == "" || !protocol.IsBoundedString(body, protocol.TextMaximumBytes) {
		return PostRoomMessageResult{}, invalidRequest(fmt.Sprintf("body must be a non-empty string up to %d bytes", protocol.TextMaximumBytes))
	}
	if slices.ContainsFunc(input.Mentions, service.redactor.ContainsSecret) {
		return PostRoomMessageResult{}, invalidRequest("room mentions must not contain secret material")
	}

	redactedTopic := service.redactor.Redact(topic)
	redactedBody := service.redactor.Redact(body)
	parsedMentions, mentionErr := protocol.ParseRoomMentions(redactedBody, input.Mentions)
	if mentionErr != nil {
		if errors.Is(mentionErr, protocol.ErrRoomMainChannelMention) {
			return PostRoomMessageResult{}, invalidRequest(mentionErr.Error())
		}
		return PostRoomMessageResult{}, invalidRequest(mentionErr.Error())
	}

	wakeEnabled := input.Wake == nil || *input.Wake
	var normalizedWake *bool
	if !wakeEnabled {
		disabled := false
		normalizedWake = &disabled
	}

	normalizedInput := PostRoomMessageInput{
		ThreadID:           threadID,
		IdempotencyKey:     idempotencyKey,
		Channel:            normalizedChannel,
		Kind:               kind,
		Topic:              topic,
		Body:               body,
		Mentions:           slices.Clone(parsedMentions.Tokens),
		Wake:               normalizedWake,
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
						ThreadID:         threadID,
						Room:             head,
						Message:          existingMessage,
						NudgedTaskIDs:    nudgedCopy,
						WokenInstanceIDs: slices.Clone(existingReceipt.WokenInstanceIDs),
						WakeTaskIDs:      slices.Clone(existingReceipt.WakeTaskIDs),
						Replayed:         true,
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
			if caller.Kind == "run" && trimmedTaskID == caller.TaskID {
				continue
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
			Channel:            normalizedChannel,
			Topic:              redactedTopic,
			Body:               redactedBody,
			Mentions:           slices.Clone(parsedMentions.Tokens),
			ReplyToSequence:    replyCopy,
			SupersedesSequence: supersedesCopy,
			ArtifactIDs:        validatedArtifactIDs,
		}

		head.HeadSequence = nextSequence
		head.UpdatedAt = nowText
		state.RoomHeads[threadID] = head
		state.RoomMessages[threadID] = append(state.RoomMessages[threadID], message)

		callerInstanceID := resolveCallerInstanceID(state, caller)
		if caller.Kind == "run" {
			if caller.TaskID != "" {
				if callerTask, taskExists := state.Tasks[caller.TaskID]; taskExists && callerTask.ThreadID == threadID {
					if callerTask.LastReadRoomSequence == nextSequence-1 {
						callerTask.LastReadRoomSequence = nextSequence
						callerTask.UpdatedAt = nowText
						state.Tasks[callerTask.ID] = callerTask
					}
				}
			}
			if callerInstanceID != "" {
				if callerInstance, instanceExists := state.Instances[callerInstanceID]; instanceExists && callerInstance.ThreadID == threadID {
					if nextSequence > callerInstance.LastPostRoomSequence {
						callerInstance.LastPostRoomSequence = nextSequence
					}
					if callerInstance.LastReadRoomSequence == nextSequence-1 {
						callerInstance.LastReadRoomSequence = nextSequence
					}
					callerInstance.UpdatedAt = nowText
					state.Instances[callerInstance.ID] = callerInstance
				}
			}
		} else if caller.ClientID != "" {
			if attachment, attached := state.ActiveAttachmentForThread(threadID); attached && attachment.ClientID == caller.ClientID {
				if attachment.LastReadRoomSequence == nextSequence-1 {
					attachment.LastReadRoomSequence = nextSequence
					attachment.LastHeartbeatAt = nowText
					attachment.UpdatedAt = nowText
					state.Attachments[attachment.ID] = attachment
				}
			}
		}

		var wokenInstanceIDs []string
		var wakeTaskIDs []string
		shouldTriggerSchedule := false
		var doorbellSessionIDs []string
		notifyAttachedDoorbell := false

		if wakeEnabled && (parsedMentions.All || len(parsedMentions.Channels) > 0 || len(parsedMentions.Handles) > 0) {
			matchedInstanceIDs := resolveMentionedThreadInstancesInState(state, threadID, callerInstanceID, parsedMentions)
			for _, instanceID := range matchedInstanceIDs {
				instance := state.Instances[instanceID]
				runningTaskID, queuedWakeTaskID := findInstanceActiveTasksInThread(state, threadID, instanceID)
				switch {
				case runningTaskID != "":
					if !slices.Contains(validatedNudgeTaskIDs, runningTaskID) {
						validatedNudgeTaskIDs = append(validatedNudgeTaskIDs, runningTaskID)
					}
				case queuedWakeTaskID != "":
					appendMessageToQueuedRoomWakeTaskInState(state, queuedWakeTaskID, instance, message, nowText)
					wokenInstanceIDs = append(wokenInstanceIDs, instanceID)
					wakeTaskIDs = append(wakeTaskIDs, queuedWakeTaskID)
					shouldTriggerSchedule = true
				case instance.Status == "ready" || instance.Status == "idle":
					wakeTaskID := service.createRoomWakeTaskInState(state, caller, instance, message, nowText)
					wokenInstanceIDs = append(wokenInstanceIDs, instanceID)
					wakeTaskIDs = append(wakeTaskIDs, wakeTaskID)
					shouldTriggerSchedule = true
				}
			}
			doorbellSessionIDs, notifyAttachedDoorbell = resolveMentionedOrchestratorSessionsInState(state, threadID, caller, parsedMentions)
		}

		detail := map[string]any{
			"roomSequence": nextSequence,
			"channel":      message.Channel,
			"kind":         message.Kind,
		}
		if message.Topic != "" {
			detail["topic"] = message.Topic
		}
		if len(message.Mentions) > 0 {
			detail["mentions"] = slices.Clone(message.Mentions)
		}
		if len(wokenInstanceIDs) > 0 {
			detail["wokenInstanceIds"] = slices.Clone(wokenInstanceIDs)
		}
		if len(wakeTaskIDs) > 0 {
			detail["wakeTaskIds"] = slices.Clone(wakeTaskIDs)
		}

		mutation.EmitEvent(protocol.TaskEventEntry{
			ThreadID:   threadID,
			Type:       "room.message_posted",
			TaskID:     caller.TaskID,
			RunID:      caller.RunID,
			InstanceID: callerInstanceID,
			MessageID:  message.ID,
			Summary:    fmt.Sprintf("room %s #%d posted", message.Kind, nextSequence),
			Detail:     detail,
			OccurredAt: nowText,
		})

		nudgedTaskIDs := []string{}
		if input.Nudge || len(validatedNudgeTaskIDs) > 0 {
			nudgedTaskIDs = applyRoomNudgeInState(state, mutation, threadID, nextSequence, input.Nudge, validatedNudgeTaskIDs, caller.TaskID, nowText)
		}

		state.RoomMessageReceipts[receiptKey] = protocol.RoomMessageReceipt{
			ID:               protocol.NewIdentifier("rmrcpt"),
			ThreadID:         threadID,
			SourceKey:        sourceKey,
			IdempotencyKey:   idempotencyKey,
			Digest:           digest,
			Sequence:         nextSequence,
			NudgedTaskIDs:    slices.Clone(nudgedTaskIDs),
			WokenInstanceIDs: slices.Clone(wokenInstanceIDs),
			WakeTaskIDs:      slices.Clone(wakeTaskIDs),
			CreatedAt:        nowText,
		}

		result = PostRoomMessageResult{
			ThreadID:         threadID,
			Room:             head,
			Message:          message,
			NudgedTaskIDs:    nudgedTaskIDs,
			WokenInstanceIDs: wokenInstanceIDs,
			WakeTaskIDs:      wakeTaskIDs,
			Replayed:         false,
		}

		if shouldTriggerSchedule || len(doorbellSessionIDs) > 0 || notifyAttachedDoorbell {
			messageCopy := message
			threadTitle := thread.Title
			sessionTargets := slices.Clone(doorbellSessionIDs)
			notifyAttached := notifyAttachedDoorbell
			triggerSchedule := shouldTriggerSchedule
			mutation.OnCommit(func() {
				if triggerSchedule {
					service.TriggerSchedule()
				}
				if notifyAttached || len(sessionTargets) > 0 {
					service.pushRoomMentionDoorbells(threadID, threadTitle, messageCopy, sessionTargets, notifyAttached)
				}
			})
		}
		return nil
	})
	if err != nil {
		return PostRoomMessageResult{}, err
	}
	if !result.Replayed {
		service.logger.Info(
			"room",
			"posted",
			activitylog.String("thread", result.ThreadID),
			activitylog.Int64("seq", result.Message.Sequence),
			activitylog.String("channel", result.Message.Channel),
			activitylog.String("kind", result.Message.Kind),
			activitylog.String("topic", result.Message.Topic),
			activitylog.Int("nudged", len(result.NudgedTaskIDs)),
			activitylog.Int("woken", len(result.WokenInstanceIDs)),
		)
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
		nudgedTaskIDs := applyRoomNudgeInState(state, mutation, threadID, head.HeadSequence, allNonTerminal, validatedTaskIDs, "", nowText)

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
		instanceID := resolveCallerInstanceID(state, caller)
		handle := ""
		if instanceID != "" {
			if instance, exists := state.Instances[instanceID]; exists {
				primaryHandle, _ := instanceHandlesInState(state, instance)
				handle = service.redactor.Redact(primaryHandle)
			}
		}
		return protocol.RoomAuthor{
			Kind:       "task",
			TaskID:     caller.TaskID,
			RunID:      caller.RunID,
			InstanceID: instanceID,
			Handle:     handle,
			Title:      title,
		}
	case "orchestrator-client":
		handle := ""
		for _, mailbox := range state.Mailboxes {
			if mailbox.Kind != "mcp-session" {
				continue
			}
			if (caller.SessionID != "" && mailbox.SessionID == caller.SessionID) ||
				(caller.SessionID == "" && caller.ClientID != "" && mailbox.ClientID == caller.ClientID) {
				handle = service.redactor.Redact(mailbox.Handle)
				break
			}
		}
		return protocol.RoomAuthor{
			Kind:     "orchestrator",
			Handle:   handle,
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

func resolveCallerInstanceID(state *store.State, caller CallerIdentity) string {
	if caller.InstanceID != "" {
		return caller.InstanceID
	}
	if caller.RunID != "" {
		if run, exists := state.Runs[caller.RunID]; exists {
			return run.InstanceID
		}
	}
	return ""
}

func roomEntryMatchesReadChannel(
	entry protocol.RoomMessage,
	readChannel string,
	callerKind string,
	callerSubscriptions []string,
) bool {
	if readChannel == protocol.AllRoomChannels {
		return true
	}
	if readChannel == "" && callerKind != "run" {
		return true
	}
	entryChannel := protocol.EffectiveRoomChannel(entry.Channel)
	if readChannel != "" {
		return entryChannel == readChannel
	}
	return protocol.InstanceSubscribesToRoomChannel(callerSubscriptions, entryChannel)
}

func hasMainRoomMessageBetween(messages []protocol.RoomMessage, afterSequence int64, throughSequence int64) bool {
	for _, entry := range messages {
		if entry.Sequence <= afterSequence {
			continue
		}
		if entry.Sequence > throughSequence {
			break
		}
		if protocol.EffectiveRoomChannel(entry.Channel) == protocol.DefaultRoomChannel {
			return true
		}
	}
	return false
}

func hasOtherSubscribedChannelRoomMessageBetween(
	messages []protocol.RoomMessage,
	selectedChannel string,
	subscribedChannels []string,
	afterSequence int64,
	throughSequence int64,
) bool {
	for _, entry := range messages {
		if entry.Sequence <= afterSequence {
			continue
		}
		if entry.Sequence > throughSequence {
			break
		}
		entryChannel := protocol.EffectiveRoomChannel(entry.Channel)
		if entryChannel != selectedChannel && protocol.InstanceSubscribesToRoomChannel(subscribedChannels, entryChannel) {
			return true
		}
	}
	return false
}

func hasOtherChannelRoomMessageBetween(
	messages []protocol.RoomMessage,
	selectedChannel string,
	afterSequence int64,
	throughSequence int64,
) bool {
	for _, entry := range messages {
		if entry.Sequence <= afterSequence {
			continue
		}
		if entry.Sequence > throughSequence {
			break
		}
		if protocol.EffectiveRoomChannel(entry.Channel) != selectedChannel {
			return true
		}
	}
	return false
}

func instanceHandlesInState(state *store.State, instance protocol.AgentInstance) (string, []string) {
	var primaryHandle string
	var handles []string
	foundMailbox := false

	for _, mailbox := range state.Mailboxes {
		if mailbox.Kind != "instance" || mailbox.InstanceID != instance.ID {
			continue
		}
		foundMailbox = true
		if normalized, ok := protocol.NormalizeMailHandle(mailbox.Handle); ok {
			if primaryHandle == "" {
				primaryHandle = normalized
			}
			if !slices.Contains(handles, normalized) {
				handles = append(handles, normalized)
			}
		}
		for _, alias := range mailbox.Aliases {
			if parsedHandle, _, _, ok := protocol.ParseMailAddress(alias); ok {
				if !slices.Contains(handles, parsedHandle) {
					handles = append(handles, parsedHandle)
				}
			} else if normalized, ok := protocol.NormalizeMailHandle(alias); ok {
				if !slices.Contains(handles, normalized) {
					handles = append(handles, normalized)
				}
			}
		}
	}

	if !foundMailbox && instance.Purpose != nil && instance.Purpose.Name != nil {
		if sanitized, ok := protocol.SanitizeMailHandle(*instance.Purpose.Name); ok {
			primaryHandle = sanitized
			handles = append(handles, sanitized)
		}
	}

	return primaryHandle, handles
}

func mailboxHandles(mailbox protocol.AgentMailbox) []string {
	var handles []string
	if normalized, ok := protocol.NormalizeMailHandle(mailbox.Handle); ok {
		handles = append(handles, normalized)
	}
	for _, alias := range mailbox.Aliases {
		if parsedHandle, _, _, ok := protocol.ParseMailAddress(alias); ok {
			if !slices.Contains(handles, parsedHandle) {
				handles = append(handles, parsedHandle)
			}
		} else if normalized, ok := protocol.NormalizeMailHandle(alias); ok {
			if !slices.Contains(handles, normalized) {
				handles = append(handles, normalized)
			}
		}
	}
	return handles
}

func instanceMatchesParsedMentions(
	state *store.State,
	instance protocol.AgentInstance,
	parsedMentions protocol.ParsedRoomMentions,
) bool {
	if parsedMentions.All {
		return true
	}
	for _, mentionedChannel := range parsedMentions.Channels {
		if protocol.InstanceSubscribesToRoomChannel(instance.RoomChannels, mentionedChannel) {
			return true
		}
	}
	if len(parsedMentions.Handles) > 0 {
		_, candidateHandles := instanceHandlesInState(state, instance)
		for _, candidateHandle := range candidateHandles {
			for _, mentionedHandle := range parsedMentions.Handles {
				if protocol.MatchesHandleMention(candidateHandle, mentionedHandle) {
					return true
				}
			}
		}
	}
	return false
}

func resolveMentionedThreadInstancesInState(
	state *store.State,
	threadID string,
	excludeInstanceID string,
	parsedMentions protocol.ParsedRoomMentions,
) []string {
	var matchedIDs []string
	for instanceID, instance := range state.Instances {
		if instance.ThreadID != threadID || instanceID == excludeInstanceID {
			continue
		}
		if protocol.IsTerminalInstanceStatus(instance.Status) || instance.Status == "draining" {
			continue
		}
		if instanceMatchesParsedMentions(state, instance, parsedMentions) {
			matchedIDs = append(matchedIDs, instanceID)
		}
	}
	slices.Sort(matchedIDs)
	return matchedIDs
}

func findInstanceActiveTasksInThread(
	state *store.State,
	threadID string,
	instanceID string,
) (runningTaskID string, queuedRoomWakeTaskID string) {
	for taskID, task := range state.Tasks {
		if task.ThreadID != threadID {
			continue
		}
		switch task.Status {
		case "running":
			if (task.Assignment != nil && task.Assignment.InstanceID == instanceID) || task.PlacementInstanceID == instanceID {
				runningTaskID = taskID
			}
		case "ready":
			if task.PlacementInstanceID == instanceID {
				if task.Origin.Kind == "room-wake" && task.AttemptCount == 0 {
					queuedRoomWakeTaskID = taskID
				} else if runningTaskID == "" {
					runningTaskID = taskID
				}
			}
		}
	}
	return runningTaskID, queuedRoomWakeTaskID
}

func formatRoomAuthorLabel(author protocol.RoomAuthor) string {
	if author.Handle != "" {
		return "@" + author.Handle
	}
	switch author.Kind {
	case "task":
		if author.Title != "" {
			return fmt.Sprintf("task %q (%s)", author.Title, author.TaskID)
		}
		if author.TaskID != "" {
			return "task " + author.TaskID
		}
		return "worker run " + author.RunID
	case "orchestrator":
		if author.ClientID != "" {
			return "coordinator (" + author.ClientID + ")"
		}
		return "coordinator"
	default:
		if author.OperatorID != "" {
			return "operator (" + author.OperatorID + ")"
		}
		return "operator"
	}
}

func formatRoomWakeMessageBlock(message protocol.RoomMessage) string {
	var builder strings.Builder
	channel := protocol.EffectiveRoomChannel(message.Channel)
	fmt.Fprintf(&builder, "--- Room #%d ---\n", message.Sequence)
	fmt.Fprintf(&builder, "Channel: %s\n", channel)
	fmt.Fprintf(&builder, "Kind: %s\n", message.Kind)
	fmt.Fprintf(&builder, "Author: %s\n", formatRoomAuthorLabel(message.Author))
	if message.Topic != "" {
		fmt.Fprintf(&builder, "Topic: %s\n", message.Topic)
	}
	if message.ReplyToSequence != nil {
		fmt.Fprintf(&builder, "In-Reply-To: #%d\n", *message.ReplyToSequence)
	}
	if message.SupersedesSequence != nil {
		fmt.Fprintf(&builder, "Supersedes: #%d\n", *message.SupersedesSequence)
	}
	if len(message.ArtifactIDs) > 0 {
		fmt.Fprintf(&builder, "Artifacts: %s\n", strings.Join(message.ArtifactIDs, ", "))
	}
	builder.WriteString("\n")
	builder.WriteString(message.Body)
	if !strings.HasSuffix(message.Body, "\n") {
		builder.WriteByte('\n')
	}
	return builder.String()
}

func (service *Service) createRoomWakeTaskInState(
	state *store.State,
	caller CallerIdentity,
	instance protocol.AgentInstance,
	message protocol.RoomMessage,
	nowText string,
) string {
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
	if requirements.Workspace != nil && requirements.Workspace.Isolation != "" {
		workspaceCopy := *requirements.Workspace
		workspaceCopy.Isolation = ""
		requirements.Workspace = &workspaceCopy
	}

	channel := protocol.EffectiveRoomChannel(message.Channel)
	authorLabel := formatRoomAuthorLabel(message.Author)
	title := fmt.Sprintf("Room #%d (%s/%s) from %s", message.Sequence, channel, message.Kind, authorLabel)
	title = protocol.TruncateUTF8Head(title, 240)

	readChannelHint := ""
	if !protocol.InstanceSubscribesToRoomChannel(instance.RoomChannels, channel) {
		readChannelHint = channel
	}

	var promptBuilder strings.Builder
	primaryHandle, _ := instanceHandlesInState(state, instance)
	if primaryHandle != "" {
		fmt.Fprintf(&promptBuilder, "You were woken on room #%d (@%s) in thread %s.\n\n", message.Sequence, primaryHandle, instance.ThreadID)
	} else {
		fmt.Fprintf(&promptBuilder, "You were woken on room #%d in thread %s.\n\n", message.Sequence, instance.ThreadID)
	}
	promptBuilder.WriteString(formatRoomWakeMessageBlock(message))
	if readChannelHint != "" {
		fmt.Fprintf(
			&promptBuilder,
			"\nCall `read_room` (`afterSequence: %d`, `channel: %q`) to catch up from your room cursor, post any response or peer handoff with `post_room_message`, and finalize this turn with `update_task`.\n",
			instance.LastReadRoomSequence,
			readChannelHint,
		)
	} else {
		fmt.Fprintf(
			&promptBuilder,
			"\nCall `read_room` (`afterSequence: %d`) to catch up from your room cursor, post any response or peer handoff with `post_room_message`, and finalize this turn with `update_task`.\n",
			instance.LastReadRoomSequence,
		)
	}

	taskID := protocol.NewIdentifier("task")
	task := protocol.Task{
		ID:       taskID,
		ThreadID: instance.ThreadID,
		Origin: protocol.TaskOrigin{
			Kind:         "room-wake",
			SubmissionID: fmt.Sprintf("room-seq-%d", message.Sequence),
			ClientID:     caller.ClientID,
			RunID:        caller.RunID,
			TaskID:       caller.TaskID,
			InstanceID:   resolveCallerInstanceID(state, caller),
		},
		Title:                title,
		Instructions:         protocol.TruncateUTF8Head(promptBuilder.String(), protocol.PromptMaximumBytes),
		Status:               "ready",
		AttemptCount:         0,
		MaxAttempts:          protocol.MaximumTaskAttempts,
		Dependencies:         []protocol.TaskDependency{},
		Requirements:         requirements,
		PlacementInstanceID:  instance.ID,
		PreferredInstanceID:  instance.ID,
		LastReadRoomSequence: instance.LastReadRoomSequence,
		CreatedAt:            nowText,
		UpdatedAt:            nowText,
	}
	state.Tasks[taskID] = task
	return taskID
}

func appendMessageToQueuedRoomWakeTaskInState(
	state *store.State,
	taskID string,
	instance protocol.AgentInstance,
	message protocol.RoomMessage,
	nowText string,
) {
	task, exists := state.Tasks[taskID]
	if !exists {
		return
	}
	channel := protocol.EffectiveRoomChannel(message.Channel)
	authorLabel := formatRoomAuthorLabel(message.Author)
	task.Title = protocol.TruncateUTF8Head(
		fmt.Sprintf("Room messages through #%d (latest %s/%s from %s)", message.Sequence, channel, message.Kind, authorLabel),
		240,
	)
	combined := task.Instructions + "\n" + formatRoomWakeMessageBlock(message)
	task.Instructions = protocol.TruncateUTF8Head(combined, protocol.PromptMaximumBytes)
	task.UpdatedAt = nowText
	state.Tasks[taskID] = task
	_ = instance
}

func resolveMentionedOrchestratorSessionsInState(
	state *store.State,
	threadID string,
	caller CallerIdentity,
	parsedMentions protocol.ParsedRoomMentions,
) ([]string, bool) {
	attachment, hasAttachment := state.ActiveAttachmentForThread(threadID)
	callerSessionID := ""
	if caller.Kind == "orchestrator-client" {
		callerSessionID = caller.SessionID
	}

	var sessionIDs []string
	notifyAttached := false
	addSession := func(sessionID string) {
		if sessionID == "" || sessionID == callerSessionID {
			return
		}
		if !slices.Contains(sessionIDs, sessionID) {
			sessionIDs = append(sessionIDs, sessionID)
		}
	}

	mentionsCoordinatorRole := parsedMentions.All ||
		slices.Contains(parsedMentions.Handles, "coordinator") ||
		slices.Contains(parsedMentions.Handles, "orchestrator")

	if mentionsCoordinatorRole && hasAttachment && attachment.SessionID != "" && attachment.SessionID != callerSessionID {
		if caller.Kind != "orchestrator-client" || caller.SessionID != "" || caller.ClientID != attachment.ClientID {
			notifyAttached = true
			addSession(attachment.SessionID)
		}
	}

	if len(parsedMentions.Handles) > 0 {
		for _, mailbox := range state.Mailboxes {
			if mailbox.Kind != "mcp-session" || mailbox.Status != "online" || mailbox.SessionID == "" {
				continue
			}
			if mailbox.SessionID == callerSessionID {
				continue
			}
			matched := false
			for _, candidateHandle := range mailboxHandles(mailbox) {
				for _, mentionedHandle := range parsedMentions.Handles {
					if protocol.MatchesHandleMention(candidateHandle, mentionedHandle) {
						matched = true
						break
					}
				}
				if matched {
					break
				}
			}
			if !matched {
				continue
			}
			addSession(mailbox.SessionID)
			if hasAttachment && (mailbox.SessionID == attachment.SessionID || (attachment.ClientID != "" && mailbox.ClientID == attachment.ClientID)) {
				notifyAttached = true
			}
		}
	}

	slices.Sort(sessionIDs)
	return sessionIDs, notifyAttached
}

func (service *Service) pushRoomMentionDoorbells(
	threadID string,
	threadTitle string,
	message protocol.RoomMessage,
	targetSessionIDs []string,
	notifyAttached bool,
) {
	summary := fmt.Sprintf(
		"Room #%d (%s/%s) from %s: %s",
		message.Sequence,
		protocol.EffectiveRoomChannel(message.Channel),
		message.Kind,
		formatRoomAuthorLabel(message.Author),
		protocol.TruncateUTF8Head(message.Body, 160),
	)

	var streamHead int64
	var attachedSessionID string
	_ = service.store.Read(func(state *store.State) error {
		streamHead = state.EventStreams[threadID].Head
		if attachment, attached := state.ActiveAttachmentForThread(threadID); attached {
			attachedSessionID = attachment.SessionID
		}
		return nil
	})
	if notifyAttached {
		_ = service.emitDoorbellWithOverride(threadID, summary, true)
	}
	occurredAt := service.nowUTC()
	for _, sessionID := range targetSessionIDs {
		if sessionID == "" || (notifyAttached && sessionID == attachedSessionID) {
			continue
		}
		notification := &protocol.DoorbellNotification{
			ThreadID:        threadID,
			ThreadTitle:     threadTitle,
			ThroughSequence: streamHead,
			Summary:         summary,
			Urgent:          true,
			OccurredAt:      occurredAt,
		}
		service.sendToOrchestratorSession(sessionID, protocol.OrchestratorServerMessage{
			Type:     "doorbell",
			Doorbell: notification,
			At:       occurredAt,
		})
	}
}

func applyRoomNudgeInState(
	state *store.State,
	mutation *store.Mutation,
	threadID string,
	requiredSequence int64,
	allNonTerminal bool,
	explicitTaskIDs []string,
	excludeTaskID string,
	nowText string,
) []string {
	currentWakeSubmissionID := fmt.Sprintf("room-seq-%d", requiredSequence)
	var targetTaskIDs []string
	if allNonTerminal {
		for taskID, task := range state.Tasks {
			if taskID == excludeTaskID {
				continue
			}
			if task.Origin.Kind == "room-wake" && task.AttemptCount == 0 && task.Origin.SubmissionID == currentWakeSubmissionID {
				continue
			}
			if task.ThreadID == threadID && !protocol.IsTerminalTaskStatus(task.Status) {
				targetTaskIDs = append(targetTaskIDs, taskID)
			}
		}
		slices.Sort(targetTaskIDs)
	} else {
		for _, taskID := range explicitTaskIDs {
			if taskID == excludeTaskID {
				continue
			}
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
