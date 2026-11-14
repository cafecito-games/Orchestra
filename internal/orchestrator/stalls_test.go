package orchestrator

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

func TestEvaluateThreadStall_Scenarios(t *testing.T) {
	fixedNow := time.Date(2026, 11, 13, 22, 0, 0, 0, time.UTC)
	baseTimestamp := "2026-11-13T21:55:00Z"
	laterTimestamp := "2026-11-13T21:58:00Z"
	threadID := "thread-stall-unit"

	verifierName := "verifier"
	reviewerTwoName := "reviewer-2"

	newBaseState := func() *store.State {
		state := store.NewState()
		state.Threads[threadID] = protocol.Thread{
			ID:        threadID,
			Title:     "Stall evaluation thread",
			Status:    "open",
			CreatedAt: baseTimestamp,
			UpdatedAt: baseTimestamp,
		}
		state.Attachments["attach-1"] = protocol.ThreadAttachment{
			ID:                   "attach-1",
			ThreadID:             threadID,
			ClientID:             "client-coord",
			SessionID:            "sess-coord",
			Status:               "attached",
			LastHeartbeatAt:      baseTimestamp,
			AcknowledgedSeq:      1,
			LastDoorbellSeq:      1,
			LastReadRoomSequence: 0,
			CreatedAt:            baseTimestamp,
			UpdatedAt:            baseTimestamp,
		}
		state.EventStreams[threadID] = protocol.ThreadEventStream{
			ThreadID:  threadID,
			Head:      1,
			Floor:     0,
			UpdatedAt: baseTimestamp,
		}
		state.ThreadEvents[threadID] = []protocol.TaskEventEntry{{
			Sequence:   1,
			ThreadID:   threadID,
			Type:       "thread.updated",
			Status:     "open",
			Summary:    "Thread created",
			OccurredAt: baseTimestamp,
		}}
		state.RoomHeads[threadID] = protocol.ThreadRoomHead{
			ThreadID:      threadID,
			DirectoryName: protocol.RoomDirectoryName(threadID, baseTimestamp),
			HeadSequence:  0,
			CreatedAt:     baseTimestamp,
			UpdatedAt:     baseTimestamp,
		}
		return state
	}

	testCases := []struct {
		name              string
		configure         func(state *store.State)
		expectNil         bool
		expectedKind      string
		expectedReasons   []string
		expectedRoomSeq   int64
		expectedBlockedID string
		expectedHandle    string
	}{
		{
			name:      "empty open thread is not stalled",
			configure: func(state *store.State) {},
			expectNil: true,
		},
		{
			name: "resolved thread is never stalled",
			configure: func(state *store.State) {
				thread := state.Threads[threadID]
				thread.Status = "resolved"
				state.Threads[threadID] = thread
				state.Tasks["task-blocked"] = protocol.Task{
					ID: "task-blocked", ThreadID: threadID, Status: "blocked", CreatedAt: baseTimestamp, UpdatedAt: baseTimestamp,
				}
			},
			expectNil: true,
		},
		{
			name: "active running run prevents stall",
			configure: func(state *store.State) {
				state.Runs["run-active"] = protocol.Run{
					ID: "run-active", ThreadID: threadID, Status: "running", CreatedAt: baseTimestamp,
				}
				state.Tasks["task-blocked"] = protocol.Task{
					ID: "task-blocked", ThreadID: threadID, Status: "blocked", CreatedAt: baseTimestamp, UpdatedAt: baseTimestamp,
				}
			},
			expectNil: true,
		},
		{
			name: "ready runnable task prevents stall",
			configure: func(state *store.State) {
				state.Tasks["task-ready"] = protocol.Task{
					ID: "task-ready", ThreadID: threadID, Status: "ready", CreatedAt: baseTimestamp, UpdatedAt: baseTimestamp,
				}
				state.Tasks["task-pending"] = protocol.Task{
					ID: "task-pending", ThreadID: threadID, Status: "pending", CreatedAt: baseTimestamp, UpdatedAt: baseTimestamp,
				}
			},
			expectNil: true,
		},
		{
			name: "provisioning instance prevents stall",
			configure: func(state *store.State) {
				state.Instances["inst-prov"] = protocol.AgentInstance{
					ID: "inst-prov", ThreadID: threadID, Status: "provisioning", CreatedAt: baseTimestamp, UpdatedAt: baseTimestamp,
				}
				state.Tasks["task-blocked"] = protocol.Task{
					ID: "task-blocked", ThreadID: threadID, Status: "blocked", CreatedAt: baseTimestamp, UpdatedAt: baseTimestamp,
				}
			},
			expectNil: true,
		},
		{
			name: "unread room message after coordinator cursor triggers unread_by_coordinator",
			configure: func(state *store.State) {
				state.RoomHeads[threadID] = protocol.ThreadRoomHead{
					ThreadID: threadID, DirectoryName: "room_test", HeadSequence: 1, CreatedAt: baseTimestamp, UpdatedAt: laterTimestamp,
				}
				state.RoomMessages[threadID] = []protocol.RoomMessage{{
					Sequence:  1,
					ID:        "rmsg_1",
					ThreadID:  threadID,
					CreatedAt: laterTimestamp,
					Author:    protocol.RoomAuthor{Kind: "task", TaskID: "task-1", Handle: "author"},
					Kind:      "finding",
					Channel:   "main",
					Body:      "Schema migration ready for coordinator review",
				}}
			},
			expectedKind:    protocol.StallKindUnreadByCoordinator,
			expectedReasons: []string{protocol.StallKindUnreadByCoordinator},
			expectedRoomSeq: 1,
		},
		{
			name: "coordinator own room post does not trigger unread_by_coordinator",
			configure: func(state *store.State) {
				state.RoomHeads[threadID] = protocol.ThreadRoomHead{
					ThreadID: threadID, DirectoryName: "room_test", HeadSequence: 1, CreatedAt: baseTimestamp, UpdatedAt: laterTimestamp,
				}
				state.RoomMessages[threadID] = []protocol.RoomMessage{{
					Sequence:  1,
					ID:        "rmsg_1",
					ThreadID:  threadID,
					CreatedAt: laterTimestamp,
					Author:    protocol.RoomAuthor{Kind: "orchestrator", ClientID: "client-coord"},
					Kind:      "guidance",
					Channel:   "main",
					Body:      "Initial instructions posted by coordinator",
				}}
			},
			expectNil: true,
		},
		{
			name: "unacknowledged terminal task update triggers unaddressed_result",
			configure: func(state *store.State) {
				state.Tasks["task-done"] = protocol.Task{
					ID: "task-done", ThreadID: threadID, Title: "Build API", Status: "completed",
					CreatedAt: baseTimestamp, UpdatedAt: laterTimestamp, FinishedAt: laterTimestamp,
				}
				state.EventStreams[threadID] = protocol.ThreadEventStream{
					ThreadID: threadID, Head: 2, Floor: 0, UpdatedAt: laterTimestamp,
				}
				state.ThreadEvents[threadID] = append(state.ThreadEvents[threadID], protocol.TaskEventEntry{
					Sequence:   2,
					ThreadID:   threadID,
					Type:       "task.updated",
					TaskID:     "task-done",
					Status:     "completed",
					Summary:    "API built",
					OccurredAt: laterTimestamp,
				})
			},
			expectedKind:    protocol.StallKindUnaddressedResult,
			expectedReasons: []string{protocol.StallKindUnaddressedResult},
		},
		{
			name: "unanswered mention when wake run completed without room reply",
			configure: func(state *store.State) {
				state.Instances["inst-verifier"] = protocol.AgentInstance{
					ID:                   "inst-verifier",
					ThreadID:             threadID,
					Purpose:              &protocol.InstancePurpose{Name: &verifierName},
					Status:               "idle",
					LastReadRoomSequence: 2,
					LastPostRoomSequence: 1,
					CreatedAt:            baseTimestamp,
					UpdatedAt:            laterTimestamp,
				}
				state.RoomHeads[threadID] = protocol.ThreadRoomHead{
					ThreadID: threadID, DirectoryName: "room_test", HeadSequence: 2, CreatedAt: baseTimestamp, UpdatedAt: laterTimestamp,
				}
				state.RoomMessages[threadID] = []protocol.RoomMessage{
					{
						Sequence:  1,
						ID:        "rmsg_1",
						ThreadID:  threadID,
						CreatedAt: baseTimestamp,
						Author:    protocol.RoomAuthor{Kind: "task", InstanceID: "inst-verifier", Handle: "verifier"},
						Kind:      "finding",
						Channel:   "main",
						Body:      "Earlier note",
					},
					{
						Sequence:  2,
						ID:        "rmsg_2",
						ThreadID:  threadID,
						CreatedAt: laterTimestamp,
						Author:    protocol.RoomAuthor{Kind: "task", InstanceID: "inst-author", Handle: "author"},
						Kind:      "question",
						Channel:   "main",
						Body:      "@verifier please verify commit abc123",
						Mentions:  []string{"@verifier"},
					},
				}
				wakeTaskID := "task-wake-2"
				state.Tasks[wakeTaskID] = protocol.Task{
					ID:                  wakeTaskID,
					ThreadID:            threadID,
					Origin:              protocol.TaskOrigin{Kind: "room-wake", SubmissionID: "room-seq-2"},
					PlacementInstanceID: "inst-verifier",
					Status:              "completed",
					CreatedAt:           laterTimestamp,
					UpdatedAt:           laterTimestamp,
					FinishedAt:          laterTimestamp,
				}
				state.Runs["run-wake-2"] = protocol.Run{
					ID:         "run-wake-2",
					ThreadID:   threadID,
					InstanceID: "inst-verifier",
					TaskID:     &wakeTaskID,
					Status:     "completed",
					CreatedAt:  laterTimestamp,
					FinishedAt: &laterTimestamp,
				}
				attachment := state.Attachments["attach-1"]
				attachment.LastReadRoomSequence = 2
				state.Attachments["attach-1"] = attachment
			},
			expectedKind:    protocol.StallKindUnansweredMention,
			expectedReasons: []string{protocol.StallKindUnansweredMention},
			expectedRoomSeq: 2,
			expectedHandle:  "verifier",
		},
		{
			name: "unanswered prefix mention when numbered member wake run failed",
			configure: func(state *store.State) {
				state.Instances["inst-rev-2"] = protocol.AgentInstance{
					ID:        "inst-rev-2",
					ThreadID:  threadID,
					Purpose:   &protocol.InstancePurpose{Name: &reviewerTwoName},
					Status:    "idle",
					CreatedAt: baseTimestamp,
					UpdatedAt: laterTimestamp,
				}
				state.RoomHeads[threadID] = protocol.ThreadRoomHead{
					ThreadID: threadID, DirectoryName: "room_test", HeadSequence: 1, CreatedAt: baseTimestamp, UpdatedAt: laterTimestamp,
				}
				state.RoomMessages[threadID] = []protocol.RoomMessage{{
					Sequence:  1,
					ID:        "rmsg_1",
					ThreadID:  threadID,
					CreatedAt: laterTimestamp,
					Author:    protocol.RoomAuthor{Kind: "operator", OperatorID: "cli"},
					Kind:      "guidance",
					Channel:   "main",
					Body:      "@reviewer check edge cases",
					Mentions:  []string{"@reviewer"},
				}}
				state.Runs["run-failed-wake"] = protocol.Run{
					ID:         "run-failed-wake",
					ThreadID:   threadID,
					InstanceID: "inst-rev-2",
					Status:     "failed",
					CreatedAt:  laterTimestamp,
					FinishedAt: &laterTimestamp,
				}
				attachment := state.Attachments["attach-1"]
				attachment.LastReadRoomSequence = 1
				state.Attachments["attach-1"] = attachment
			},
			expectedKind:    protocol.StallKindUnansweredMention,
			expectedReasons: []string{protocol.StallKindUnansweredMention},
			expectedRoomSeq: 1,
			expectedHandle:  "reviewer-2",
		},
		{
			name: "pending downstream task with failed upstream triggers blocked_tasks",
			configure: func(state *store.State) {
				state.Tasks["task-upstream"] = protocol.Task{
					ID: "task-upstream", ThreadID: threadID, Status: "failed",
					CreatedAt: baseTimestamp, UpdatedAt: laterTimestamp, FinishedAt: laterTimestamp,
				}
				state.Tasks["task-downstream"] = protocol.Task{
					ID:            "task-downstream",
					ThreadID:      threadID,
					Status:        "pending",
					BlockerReason: "upstream task-upstream failed",
					Dependencies:  []protocol.TaskDependency{{TaskID: "task-upstream", Policy: "require-success"}},
					CreatedAt:     baseTimestamp,
					UpdatedAt:     laterTimestamp,
				}
			},
			expectedKind:      protocol.StallKindBlockedTasks,
			expectedReasons:   []string{protocol.StallKindBlockedTasks},
			expectedBlockedID: "task-downstream",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			state := newBaseState()
			testCase.configure(state)

			report := EvaluateThreadStall(state, threadID, fixedNow)
			if testCase.expectNil {
				assert.Nil(t, report)
				return
			}

			require.NotNil(t, report)
			assert.True(t, report.Stalled)
			assert.Equal(t, threadID, report.ThreadID)
			assert.Equal(t, testCase.expectedKind, report.Kind)
			assert.Equal(t, testCase.expectedReasons, report.Reasons)
			assert.NotEmpty(t, report.Fingerprint)
			assert.NotEmpty(t, report.Summary)
			if testCase.expectedRoomSeq > 0 {
				assert.Equal(t, testCase.expectedRoomSeq, report.RoomSequence)
			}
			if testCase.expectedBlockedID != "" {
				assert.Contains(t, report.BlockedTaskIDs, testCase.expectedBlockedID)
			}
			if testCase.expectedHandle != "" {
				require.NotEmpty(t, report.Conditions)
				assert.Equal(t, testCase.expectedHandle, report.Conditions[0].Handle)
			}
		})
	}
}

func TestCheckThreadStall_DeduplicationDoorbellAndThreadDetailExposure(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store:            openedStore,
		Token:            "master-secret-token",
		DoorbellDebounce: 2 * time.Second,
	})
	require.NoError(t, err)
	defer service.Close()

	httpServer := httptest.NewServer(service.Handler())
	defer httpServer.Close()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/orchestrator-client"
	connection, dialResponse, err := websocket.Dial(t.Context(), wsURL, &websocket.DialOptions{
		HTTPHeader: map[string][]string{
			"Authorization": {"Bearer master-secret-token"},
		},
	})
	if dialResponse != nil && dialResponse.Body != nil {
		_ = dialResponse.Body.Close()
	}
	require.NoError(t, err)
	defer connection.CloseNow()

	registerPayload, err := json.Marshal(protocol.OrchestratorClientMessage{
		Type:            "register",
		ProtocolVersion: protocol.ProtocolVersion,
		ClientID:        "coord-client",
		SessionID:       "coord-session",
		Mail: &protocol.ClientMailRegistration{
			RequestedName: "lead",
			Harness:       "claude",
			PushCapable:   true,
		},
	})
	require.NoError(t, err)
	require.NoError(t, connection.Write(t.Context(), websocket.MessageText, registerPayload))

	readDeadline, cancelRead := context.WithTimeout(t.Context(), 2*time.Second)
	_, rawWelcome, err := connection.Read(readDeadline)
	cancelRead()
	require.NoError(t, err)
	var welcomeMessage protocol.OrchestratorServerMessage
	require.NoError(t, json.Unmarshal(rawWelcome, &welcomeMessage))
	require.Equal(t, "welcome", welcomeMessage.Type)
	sessionID := welcomeMessage.SessionID
	if sessionID == "" {
		sessionID = "coord-session"
	}

	coordinatorCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "coord-client",
		SessionID: sessionID,
	}

	thread, _, err := service.CreateThread(coordinatorCaller, CreateThreadInput{
		Title: "Peer handoff stall thread",
	})
	require.NoError(t, err)
	_, _, err = service.AttachThread(coordinatorCaller, AttachThreadInput{ThreadID: thread.ID})
	require.NoError(t, err)

	workspace := t.TempDir()
	nowText := service.nowUTC()
	authorName := "author"
	verifierName := "verifier"
	authorTaskID := "task-author-1"

	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		state.Nodes["node-1"] = protocol.ComputeNode{
			ID: "node-1", Name: "node-1", Kind: "local", Platform: "darwin-arm64",
			Status: "online", LastSeen: nowText, Concurrency: 4, WorkspaceRoots: []string{workspace}, Version: "1.0.0",
			Harnesses: []protocol.HarnessProfile{{
				ID: "claude-cli", Label: "Claude", Binary: "/bin/echo", Available: true,
				AuthMode: "local-subscription", Models: []string{"sonnet"}, Transports: []string{"acp-v1"},
			}},
		}
		state.Instances["inst-author"] = protocol.AgentInstance{
			ID: "inst-author", ThreadID: thread.ID, Purpose: &protocol.InstancePurpose{Name: &authorName},
			Status: "busy", CreatedAt: nowText, UpdatedAt: nowText,
			Lease: protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
		}
		state.Allocations["alloc-author"] = protocol.InstanceAllocation{
			ID: "alloc-author", InstanceID: "inst-author", NodeID: "node-1", HarnessID: "claude-cli", Model: "sonnet",
			Transport: "acp-v1", Workspace: workspace, Status: "active", CreatedAt: nowText, UpdatedAt: nowText,
			Lease: protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
		}
		service.ensureInstanceMailboxInState(state, state.Instances["inst-author"], state.Allocations["alloc-author"], nowText)

		state.Instances["inst-verifier"] = protocol.AgentInstance{
			ID: "inst-verifier", ThreadID: thread.ID, Purpose: &protocol.InstancePurpose{Name: &verifierName},
			Status: "idle", CreatedAt: nowText, UpdatedAt: nowText,
			Lease: protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
		}
		state.Allocations["alloc-verifier"] = protocol.InstanceAllocation{
			ID: "alloc-verifier", InstanceID: "inst-verifier", NodeID: "node-1", HarnessID: "claude-cli", Model: "sonnet",
			Transport: "acp-v1", Workspace: workspace, Status: "active", CreatedAt: nowText, UpdatedAt: nowText,
			Lease: protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
		}
		service.ensureInstanceMailboxInState(state, state.Instances["inst-verifier"], state.Allocations["alloc-verifier"], nowText)

		attempt := 1
		state.Tasks[authorTaskID] = protocol.Task{
			ID: authorTaskID, ThreadID: thread.ID, Title: "Author work", Instructions: "Write feature",
			Status: "running", AttemptCount: 1, MaxAttempts: 3, CreatedAt: nowText, UpdatedAt: nowText,
			Assignment: &protocol.TaskAssignment{
				RunID: "run-author-1", InstanceID: "inst-author", AllocationID: "alloc-author", NodeID: "node-1", Attempt: 1, AssignedAt: nowText,
			},
		}
		state.Runs["run-author-1"] = protocol.Run{
			ID: "run-author-1", ThreadID: thread.ID, InstanceID: "inst-author", AllocationID: "alloc-author", NodeID: "node-1",
			HarnessID: "claude-cli", Model: "sonnet", Transport: "acp-v1", Workspace: workspace,
			Status: "running", CreatedAt: nowText, TaskID: &authorTaskID, Attempt: &attempt,
		}
		return nil
	})
	require.NoError(t, err)

	authorCaller := CallerIdentity{
		Kind:       "run",
		RunID:      "run-author-1",
		TaskID:     authorTaskID,
		ThreadID:   thread.ID,
		InstanceID: "inst-author",
	}

	handoffPost, err := service.PostRoomMessage(authorCaller, PostRoomMessageInput{
		IdempotencyKey: "author-handoff-1",
		Kind:           "finding",
		Body:           "@verifier please review commit 12345; token master-secret-token must never appear",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"inst-verifier"}, handoffPost.WokenInstanceIDs)
	require.Len(t, handoffPost.WakeTaskIDs, 1)
	wakeTaskID := handoffPost.WakeTaskIDs[0]

	// While author is running and verifier has a ready/running wake task, thread is NOT stalled.
	notStalledYet, emittedEarly, err := service.CheckThreadStall(thread.ID)
	require.NoError(t, err)
	assert.False(t, emittedEarly)
	assert.Nil(t, notStalledYet)

	authorOut := "author finished handoff"
	service.handleRunCompleted("node-1", protocol.SlaveControlMessage{
		Type:   "run.completed",
		RunID:  "run-author-1",
		Output: &authorOut,
	})

	// Dispatch and complete verifier's wake task WITHOUT verifier posting back to the room.
	var verifierWakeRunID string
	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		wakeTask := state.Tasks[wakeTaskID]
		if wakeTask.Status == "ready" {
			effectiveRequirements := mergeProjectProfileRequirements(state, wakeTask.Requirements)
			instance := state.Instances["inst-verifier"]
			allocation := state.Allocations["alloc-verifier"]
			dispatched := service.assignTaskToResident(state, &wakeTask, &instance, &allocation, effectiveRequirements, service.nowUTC())
			verifierWakeRunID = dispatched.RunID
		} else if wakeTask.Assignment != nil {
			verifierWakeRunID = wakeTask.Assignment.RunID
		}
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, verifierWakeRunID)

	silentVerifierOutput := "looked at files and exited silently without posting"
	service.handleRunCompleted("node-1", protocol.SlaveControlMessage{
		Type:   "run.completed",
		RunID:  verifierWakeRunID,
		Output: &silentVerifierOutput,
	})

	// Evaluate and emit stall event.
	stallReport, emitted, err := service.CheckThreadStall(thread.ID)
	require.NoError(t, err)
	require.True(t, emitted)
	require.NotNil(t, stallReport)
	assert.True(t, stallReport.Stalled)
	assert.Equal(t, protocol.StallKindUnansweredMention, stallReport.Kind)
	assert.Equal(t, int64(1), stallReport.RoomSequence)
	assert.NotContains(t, stallReport.Summary, "master-secret-token")

	// Urgent doorbell should arrive on the attached coordinator WebSocket session.
	doorbellContext, cancelDoorbell := context.WithTimeout(t.Context(), 2*time.Second)
	_, rawDoorbell, err := connection.Read(doorbellContext)
	cancelDoorbell()
	require.NoError(t, err)
	var serverMessage protocol.OrchestratorServerMessage
	require.NoError(t, json.Unmarshal(rawDoorbell, &serverMessage))
	require.Equal(t, "doorbell", serverMessage.Type)
	require.NotNil(t, serverMessage.Doorbell)
	assert.Equal(t, thread.ID, serverMessage.Doorbell.ThreadID)
	assert.True(t, serverMessage.Doorbell.Urgent)
	assert.Contains(t, serverMessage.Doorbell.Summary, "@verifier")

	// GetThreadDetail and GetThreadContext must expose Stall.
	detail, err := service.GetThreadDetail(thread.ID)
	require.NoError(t, err)
	require.NotNil(t, detail.Stall)
	assert.Equal(t, protocol.StallKindUnansweredMention, detail.Stall.Kind)

	threadContext, err := service.GetThreadContext(thread.ID)
	require.NoError(t, err)
	require.NotNil(t, threadContext.Stall)
	assert.Equal(t, protocol.StallKindUnansweredMention, threadContext.Stall.Kind)

	// Repeated checks with no room/task/run sequence change MUST NOT emit duplicate thread.stalled events.
	repeatReport, repeatEmitted, err := service.CheckThreadStall(thread.ID)
	require.NoError(t, err)
	assert.False(t, repeatEmitted)
	require.NotNil(t, repeatReport)
	assert.Equal(t, stallReport.Fingerprint, repeatReport.Fingerprint)

	eventsAfterRepeat, err := service.GetThreadEvents(coordinatorCaller, GetThreadEventsInput{
		ThreadID: thread.ID,
		Types:    []string{"thread.stalled"},
	})
	require.NoError(t, err)
	require.Len(t, eventsAfterRepeat.Events, 1)

	// Verifier posts a room reply (with Wake: false so author stays idle): unanswered_mention clears,
	// while room sequence changes to #2 so unread_by_coordinator triggers a fresh stall fingerprint.
	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		state.Runs["run-verifier-reply"] = protocol.Run{
			ID: "run-verifier-reply", ThreadID: thread.ID, InstanceID: "inst-verifier", NodeID: "node-1",
			Status: "running", CreatedAt: service.nowUTC(),
		}
		return nil
	})
	require.NoError(t, err)

	wakeFalse := false
	verifierCaller := CallerIdentity{
		Kind:       "run",
		RunID:      "run-verifier-reply",
		ThreadID:   thread.ID,
		InstanceID: "inst-verifier",
	}
	replySeq := int64(1)
	_, err = service.PostRoomMessage(verifierCaller, PostRoomMessageInput{
		IdempotencyKey:  "verifier-reply-2",
		Kind:            "answer",
		Body:            "Verified #1 cleanly.",
		ReplyToSequence: &replySeq,
		Wake:            &wakeFalse,
	})
	require.NoError(t, err)

	replyDone := "posted answer"
	service.handleRunCompleted("node-1", protocol.SlaveControlMessage{
		Type:   "run.completed",
		RunID:  "run-verifier-reply",
		Output: &replyDone,
	})

	afterReplyReport, emittedAfterReply, err := service.CheckThreadStall(thread.ID)
	require.NoError(t, err)
	require.True(t, emittedAfterReply)
	require.NotNil(t, afterReplyReport)
	assert.Equal(t, protocol.StallKindUnreadByCoordinator, afterReplyReport.Kind)
	assert.NotContains(t, afterReplyReport.Reasons, protocol.StallKindUnansweredMention)

	// Coordinator reads room and acknowledges thread events -> thread is no longer stalled!
	readRoomResult, err := service.ReadRoom(coordinatorCaller, ReadRoomInput{ThreadID: thread.ID})
	require.NoError(t, err)
	assert.Equal(t, int64(2), readRoomResult.LastReadSequence)

	_, err = service.GetThreadEvents(coordinatorCaller, GetThreadEventsInput{ThreadID: thread.ID})
	require.NoError(t, err)

	clearedDetail, err := service.GetThreadDetail(thread.ID)
	require.NoError(t, err)
	assert.Nil(t, clearedDetail.Stall)

	clearedContext, err := service.GetThreadContext(thread.ID)
	require.NoError(t, err)
	assert.Nil(t, clearedContext.Stall)
}
