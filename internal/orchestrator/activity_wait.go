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

type WaitForActivityInput struct {
	ThreadID            string   `json:"threadId,omitempty"`
	EventCursor         string   `json:"eventCursor,omitempty"`
	Cursor              string   `json:"cursor,omitempty"`
	EventTypes          []string `json:"eventTypes,omitempty"`
	AfterRoomSequence   *int64   `json:"afterRoomSequence,omitempty"`
	AfterMailSequence   int64    `json:"afterMailSequence,omitempty"`
	UnreadMailOnly      *bool    `json:"unreadMailOnly,omitempty"`
	MarkMailRead        *bool    `json:"markMailRead,omitempty"`
	TimeoutMilliseconds int      `json:"timeoutMilliseconds,omitempty"`
	WaitMilliseconds    int      `json:"waitMs,omitempty"`
	Limit               int      `json:"limit,omitempty"`
}

type WaitForActivityResult struct {
	ThreadID         string                    `json:"threadId,omitempty"`
	WakeSources      []string                  `json:"wakeSources"`
	Events           []protocol.TaskEventEntry `json:"events"`
	EventCursor      string                    `json:"eventCursor,omitempty"`
	CursorExpired    bool                      `json:"cursorExpired"`
	RoomMessages     []protocol.RoomMessage    `json:"roomMessages"`
	RoomHeadSequence int64                     `json:"roomHeadSequence"`
	Mailbox          *protocol.AgentMailbox    `json:"mailbox,omitempty"`
	MailMessages     []protocol.MailMessage    `json:"mailMessages"`
	TimedOut         bool                      `json:"timedOut"`
}

func (service *Service) WaitForActivity(
	operationContext context.Context,
	caller CallerIdentity,
	input WaitForActivityInput,
) (WaitForActivityResult, error) {
	if input.AfterMailSequence < 0 {
		return WaitForActivityResult{}, invalidRequest("afterMailSequence must be non-negative")
	}
	if input.AfterRoomSequence != nil && *input.AfterRoomSequence < 0 {
		return WaitForActivityResult{}, invalidRequest("afterRoomSequence must be non-negative")
	}

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
	if limit <= 0 || limit > protocol.MaximumEventsPerWait {
		limit = protocol.MaximumEventsPerWait
	}
	mailLimit := min(limit, protocol.MaximumMailReadLimit)

	unreadMailOnly := input.AfterMailSequence == 0
	if input.UnreadMailOnly != nil {
		unreadMailOnly = *input.UnreadMailOnly
	}
	markMailRead := true
	if input.MarkMailRead != nil {
		markMailRead = *input.MarkMailRead
	}

	rawEventCursor := strings.TrimSpace(input.EventCursor)
	if rawEventCursor == "" {
		rawEventCursor = strings.TrimSpace(input.Cursor)
	}

	readerKey := caller.SourceKey()
	if readerKey == "" {
		readerKey = "orchestrator-client:default"
	}

	threadID := strings.TrimSpace(input.ThreadID)
	if caller.Kind == "run" {
		if threadID == "" {
			threadID = caller.ThreadID
		} else if caller.ThreadID != "" && threadID != caller.ThreadID {
			return WaitForActivityResult{}, forbiddenError("worker run cannot wait on activity in another thread")
		}
	}

	eventChannel, unsubscribeEvents := service.store.SubscribeEvents()
	defer unsubscribeEvents()

	mailChannel, unsubscribeMail := service.subscribeMail()
	defer unsubscribeMail()

	targetMailbox, err := service.GetMailIdentity(caller)
	if err != nil {
		return WaitForActivityResult{}, err
	}

	var eventBaselineSequence int64
	var roomBaselineSequence int64

	if err := service.store.Read(func(state *store.State) error {
		if threadID == "" && caller.Kind != "run" && caller.ClientID != "" {
			var attachedThreadIDs []string
			for _, attachment := range state.Attachments {
				if attachment.Status == "attached" && attachment.ClientID == caller.ClientID {
					attachedThreadIDs = append(attachedThreadIDs, attachment.ThreadID)
				}
			}
			if len(attachedThreadIDs) == 1 {
				threadID = attachedThreadIDs[0]
			}
		}

		if threadID == "" {
			if rawEventCursor != "" {
				return invalidRequest("threadId is required when eventCursor is provided without an attached thread")
			}
			if input.AfterRoomSequence != nil {
				return invalidRequest("threadId is required when afterRoomSequence is provided without an attached thread")
			}
			return nil
		}

		if !protocol.IsIdentifier(threadID) {
			return invalidRequest("threadId is malformed")
		}
		if _, exists := state.Threads[threadID]; !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}

		if rawEventCursor != "" {
			decodedSequence, decodeErr := protocol.DecodeEventCursor(rawEventCursor, threadID, readerKey)
			if decodeErr != nil {
				return invalidRequest(decodeErr.Error())
			}
			eventBaselineSequence = decodedSequence
		} else {
			eventBaselineSequence = state.EventStreams[threadID].Head
		}

		if input.AfterRoomSequence != nil {
			roomBaselineSequence = *input.AfterRoomSequence
		} else {
			roomBaselineSequence = state.RoomHeads[threadID].HeadSequence
		}
		return nil
	}); err != nil {
		return WaitForActivityResult{}, err
	}

	collectSnapshot := func() (WaitForActivityResult, bool, error) {
		snapshot := WaitForActivityResult{
			ThreadID:     threadID,
			WakeSources:  []string{},
			Events:       []protocol.TaskEventEntry{},
			RoomMessages: []protocol.RoomMessage{},
			MailMessages: []protocol.MailMessage{},
		}

		var acknowledgeSequence int64
		var shouldAcknowledge bool
		var advanceTaskRoomSequence int64
		var shouldAdvanceTaskRoom bool

		if threadID != "" {
			if readErr := service.store.Read(func(state *store.State) error {
				if _, exists := state.Threads[threadID]; !exists {
					return notFound(fmt.Sprintf("thread %s does not exist", threadID))
				}

				roomHead := state.RoomHeads[threadID]
				snapshot.RoomHeadSequence = roomHead.HeadSequence
				allRoomMessages := state.RoomMessages[threadID]
				for _, entry := range allRoomMessages {
					if entry.Sequence <= roomBaselineSequence {
						continue
					}
					if len(snapshot.RoomMessages) >= limit {
						break
					}
					entryCopy := entry
					entryCopy.ArtifactIDs = slices.Clone(entry.ArtifactIDs)
					snapshot.RoomMessages = append(snapshot.RoomMessages, entryCopy)
				}

				stream := state.EventStreams[threadID]
				if eventBaselineSequence < stream.Floor {
					nextCursor, cursorErr := protocol.EncodeEventCursor(threadID, readerKey, stream.Floor)
					if cursorErr != nil {
						return cursorErr
					}
					snapshot.EventCursor = nextCursor
					snapshot.CursorExpired = true
					return nil
				}

				childTaskIDs := map[string]bool{}
				taskHasUnreadNudge := false
				if caller.Kind == "run" {
					for _, candidate := range state.Tasks {
						if caller.TaskID != "" && candidate.ParentTaskID == caller.TaskID {
							childTaskIDs[candidate.ID] = true
						}
					}
					if caller.TaskID != "" {
						if callerTask, taskExists := state.Tasks[caller.TaskID]; taskExists {
							taskHasUnreadNudge = callerTask.RequiredRoomSequence > callerTask.LastReadRoomSequence
						}
					}
				}

				allEvents := state.ThreadEvents[threadID]
				lastScanned := eventBaselineSequence
				hasMoreEvents := false
				for _, entry := range allEvents {
					if entry.Sequence <= eventBaselineSequence {
						continue
					}
					if caller.Kind == "run" &&
						entry.Type != "room.message_posted" &&
						!isEventVisibleToWorker(entry, caller.TaskID, caller.RunID, childTaskIDs, taskHasUnreadNudge) {
						lastScanned = entry.Sequence
						continue
					}
					if len(input.EventTypes) > 0 && !slices.Contains(input.EventTypes, entry.Type) {
						lastScanned = entry.Sequence
						continue
					}
					if len(snapshot.Events) >= limit {
						hasMoreEvents = true
						break
					}
					lastScanned = entry.Sequence
					snapshot.Events = append(snapshot.Events, entry)
				}
				if !hasMoreEvents && stream.Head > lastScanned {
					lastScanned = stream.Head
				}

				nextCursor, cursorErr := protocol.EncodeEventCursor(threadID, readerKey, lastScanned)
				if cursorErr != nil {
					return cursorErr
				}
				snapshot.EventCursor = nextCursor

				if caller.Kind == "run" && caller.TaskID != "" && len(snapshot.RoomMessages) > 0 {
					if callerTask, taskExists := state.Tasks[caller.TaskID]; taskExists && callerTask.ThreadID == threadID {
						lastReturnedRoomSeq := snapshot.RoomMessages[len(snapshot.RoomMessages)-1].Sequence
						if roomBaselineSequence <= callerTask.LastReadRoomSequence && lastReturnedRoomSeq > callerTask.LastReadRoomSequence {
							advanceTaskRoomSequence = lastReturnedRoomSeq
							shouldAdvanceTaskRoom = true
						}
					}
				}
				if caller.Kind != "run" && caller.ClientID != "" {
					if attachment, attached := state.ActiveAttachmentForThread(threadID); attached &&
						attachment.ClientID == caller.ClientID && lastScanned > attachment.AcknowledgedSeq {
						acknowledgeSequence = lastScanned
						shouldAcknowledge = true
					}
				}
				return nil
			}); readErr != nil {
				return WaitForActivityResult{}, false, readErr
			}
		}

		if shouldAcknowledge || shouldAdvanceTaskRoom {
			_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
				nowText := service.nowUTC()
				if shouldAcknowledge {
					attachment, attached := state.ActiveAttachmentForThread(threadID)
					if attached && attachment.ClientID == caller.ClientID && acknowledgeSequence > attachment.AcknowledgedSeq {
						attachment.AcknowledgedSeq = acknowledgeSequence
						attachment.LastHeartbeatAt = nowText
						attachment.UpdatedAt = nowText
						state.Attachments[attachment.ID] = attachment
					}
				}
				if shouldAdvanceTaskRoom {
					if callerTask, taskExists := state.Tasks[caller.TaskID]; taskExists && callerTask.ThreadID == threadID &&
						advanceTaskRoomSequence > callerTask.LastReadRoomSequence {
						callerTask.LastReadRoomSequence = advanceTaskRoomSequence
						callerTask.UpdatedAt = nowText
						state.Tasks[callerTask.ID] = callerTask
					}
				}
				return nil
			})
		}

		mailResult, mailErr := service.ReadMail(caller, ReadMailInput{
			Box:           "inbox",
			UnreadOnly:    unreadMailOnly,
			AfterSequence: input.AfterMailSequence,
			Limit:         mailLimit,
			MarkRead:      &markMailRead,
		})
		if mailErr != nil {
			return WaitForActivityResult{}, false, mailErr
		}
		mailboxCopy := mailResult.Mailbox
		snapshot.Mailbox = &mailboxCopy
		if mailResult.Messages != nil {
			snapshot.MailMessages = mailResult.Messages
		}

		if len(snapshot.Events) > 0 || snapshot.CursorExpired {
			snapshot.WakeSources = append(snapshot.WakeSources, "events")
		}
		if len(snapshot.RoomMessages) > 0 {
			snapshot.WakeSources = append(snapshot.WakeSources, "room")
		}
		if len(snapshot.MailMessages) > 0 {
			snapshot.WakeSources = append(snapshot.WakeSources, "mail")
		}

		return snapshot, len(snapshot.WakeSources) > 0, nil
	}

	initialSnapshot, hasActivity, err := collectSnapshot()
	if err != nil {
		return WaitForActivityResult{}, err
	}
	if hasActivity {
		return initialSnapshot, nil
	}
	if waitMilliseconds == 0 {
		initialSnapshot.TimedOut = true
		return initialSnapshot, nil
	}

	waitTimer := time.NewTimer(time.Duration(waitMilliseconds) * time.Millisecond)
	defer waitTimer.Stop()

	for {
		select {
		case <-operationContext.Done():
			polled, woke, pollErr := collectSnapshot()
			if pollErr != nil {
				return WaitForActivityResult{}, pollErr
			}
			polled.TimedOut = !woke
			return polled, nil
		case <-waitTimer.C:
			polled, woke, pollErr := collectSnapshot()
			if pollErr != nil {
				return WaitForActivityResult{}, pollErr
			}
			polled.TimedOut = !woke
			return polled, nil
		case eventBatch, open := <-eventChannel:
			if !open {
				polled, woke, pollErr := collectSnapshot()
				if pollErr != nil {
					return WaitForActivityResult{}, pollErr
				}
				polled.TimedOut = !woke
				return polled, nil
			}
			if threadID == "" {
				continue
			}
			relevantThread := false
			for _, event := range eventBatch {
				if event.ThreadID == threadID {
					relevantThread = true
					break
				}
			}
			if !relevantThread {
				continue
			}
			polled, woke, pollErr := collectSnapshot()
			if pollErr != nil || woke {
				return polled, pollErr
			}
		case incomingMail, open := <-mailChannel:
			if !open {
				polled, woke, pollErr := collectSnapshot()
				if pollErr != nil {
					return WaitForActivityResult{}, pollErr
				}
				polled.TimedOut = !woke
				return polled, nil
			}
			if incomingMail.ToMailboxID != targetMailbox.ID {
				continue
			}
			polled, woke, pollErr := collectSnapshot()
			if pollErr != nil || woke {
				return polled, pollErr
			}
		}
	}
}
