package orchestrator

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

func TestWaitForActivity_Validation(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	coordinatorCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "coord-client",
		SessionID: "coord-session",
	}
	threadAlpha, _, err := service.CreateThread(coordinatorCaller, CreateThreadInput{Title: "Alpha"})
	require.NoError(t, err)
	threadBeta, _, err := service.CreateThread(coordinatorCaller, CreateThreadInput{Title: "Beta"})
	require.NoError(t, err)

	workerCaller := CallerIdentity{
		Kind:       "run",
		RunID:      "run-alpha-1",
		TaskID:     "task-alpha-1",
		ThreadID:   threadAlpha.ID,
		InstanceID: "inst-alpha-1",
	}

	negativeRoomSequence := int64(-1)
	zeroRoomSequence := int64(0)

	testCases := []struct {
		name         string
		caller       CallerIdentity
		input        WaitForActivityInput
		expectedCode string
	}{
		{
			name:   "negative afterMailSequence rejected",
			caller: coordinatorCaller,
			input: WaitForActivityInput{
				ThreadID:          threadAlpha.ID,
				AfterMailSequence: -3,
			},
			expectedCode: "invalid_request",
		},
		{
			name:   "negative afterRoomSequence rejected",
			caller: coordinatorCaller,
			input: WaitForActivityInput{
				ThreadID:          threadAlpha.ID,
				AfterRoomSequence: &negativeRoomSequence,
			},
			expectedCode: "invalid_request",
		},
		{
			name:   "eventCursor without threadId or attachment rejected",
			caller: coordinatorCaller,
			input: WaitForActivityInput{
				EventCursor: "cur_invalid",
			},
			expectedCode: "invalid_request",
		},
		{
			name:   "afterRoomSequence without threadId or attachment rejected",
			caller: coordinatorCaller,
			input: WaitForActivityInput{
				AfterRoomSequence: &zeroRoomSequence,
			},
			expectedCode: "invalid_request",
		},
		{
			name:   "malformed threadId rejected",
			caller: coordinatorCaller,
			input: WaitForActivityInput{
				ThreadID: strings.Repeat("t", protocol.IdentifierMaximumBytes+1),
			},
			expectedCode: "invalid_request",
		},
		{
			name:   "missing thread returns not_found",
			caller: coordinatorCaller,
			input: WaitForActivityInput{
				ThreadID: "thr_nonexistent_12345",
			},
			expectedCode: "not_found",
		},
		{
			name:   "worker run cannot wait on another thread",
			caller: workerCaller,
			input: WaitForActivityInput{
				ThreadID: threadBeta.ID,
			},
			expectedCode: "forbidden",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, waitErr := service.WaitForActivity(t.Context(), testCase.caller, testCase.input)
			assertOperationErrorCodeValue(t, waitErr, testCase.expectedCode)
		})
	}
}

func TestWaitForActivity_ImmediateWakeSources(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	coordinatorCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "coord-immediate",
		SessionID: "sess-immediate",
	}
	senderCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "sender-immediate",
		SessionID: "sess-sender",
	}

	coordinatorMailbox, err := service.SetMailIdentity(coordinatorCaller, SetMailIdentityInput{Name: "coordinator"})
	require.NoError(t, err)
	_, err = service.SetMailIdentity(senderCaller, SetMailIdentityInput{Name: "author"})
	require.NoError(t, err)

	thread, _, err := service.CreateThread(coordinatorCaller, CreateThreadInput{Title: "Immediate Wake Thread"})
	require.NoError(t, err)

	baselineCursor, err := protocol.EncodeEventCursor(thread.ID, coordinatorCaller.SourceKey(), 0)
	require.NoError(t, err)

	postedRoom, err := service.PostRoomMessage(senderCaller, PostRoomMessageInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "room-post-1",
		Kind:           "finding",
		Body:           "Initial room finding ready",
	})
	require.NoError(t, err)

	t.Run("omitted cursors baseline at current head and time out immediately when idle", func(t *testing.T) {
		result, waitErr := service.WaitForActivity(t.Context(), coordinatorCaller, WaitForActivityInput{
			ThreadID:            thread.ID,
			TimeoutMilliseconds: 0,
		})
		require.NoError(t, waitErr)
		assert.True(t, result.TimedOut)
		assert.Empty(t, result.WakeSources)
		assert.Empty(t, result.Events)
		assert.Empty(t, result.RoomMessages)
		assert.Empty(t, result.MailMessages)
		assert.Equal(t, postedRoom.Message.Sequence, result.RoomHeadSequence)
	})

	t.Run("explicit afterRoomSequence returns existing room entries immediately", func(t *testing.T) {
		zeroSequence := int64(0)
		result, waitErr := service.WaitForActivity(t.Context(), coordinatorCaller, WaitForActivityInput{
			ThreadID:            thread.ID,
			AfterRoomSequence:   &zeroSequence,
			TimeoutMilliseconds: 2000,
		})
		require.NoError(t, waitErr)
		assert.False(t, result.TimedOut)
		assert.Equal(t, []string{"room"}, result.WakeSources)
		require.Len(t, result.RoomMessages, 1)
		assert.Equal(t, postedRoom.Message.ID, result.RoomMessages[0].ID)
	})

	t.Run("explicit eventCursor returns unseen thread events immediately", func(t *testing.T) {
		result, waitErr := service.WaitForActivity(t.Context(), coordinatorCaller, WaitForActivityInput{
			ThreadID:            thread.ID,
			EventCursor:         baselineCursor,
			TimeoutMilliseconds: 2000,
		})
		require.NoError(t, waitErr)
		assert.False(t, result.TimedOut)
		assert.Equal(t, []string{"events"}, result.WakeSources)
		assert.NotEmpty(t, result.Events)
		assert.NotEqual(t, baselineCursor, result.EventCursor)
	})

	t.Run("unread mail in inbox returns immediately and marks mail read", func(t *testing.T) {
		sentMail, sendErr := service.SendMail(senderCaller, SendMailInput{
			To:             coordinatorMailbox.Address,
			IdempotencyKey: "mail-immediate-1",
			Subject:        "Deliverable ready",
			Body:           "Please review commit",
		})
		require.NoError(t, sendErr)

		result, waitErr := service.WaitForActivity(t.Context(), coordinatorCaller, WaitForActivityInput{
			ThreadID:            thread.ID,
			TimeoutMilliseconds: 2000,
		})
		require.NoError(t, waitErr)
		assert.False(t, result.TimedOut)
		assert.Equal(t, []string{"mail"}, result.WakeSources)
		require.Len(t, result.MailMessages, 1)
		assert.Equal(t, sentMail.Message.ID, result.MailMessages[0].ID)
		require.NotNil(t, result.Mailbox)
		assert.Equal(t, coordinatorMailbox.Address, result.Mailbox.Address)

		followUp, followUpErr := service.WaitForActivity(t.Context(), coordinatorCaller, WaitForActivityInput{
			ThreadID:            thread.ID,
			TimeoutMilliseconds: 0,
		})
		require.NoError(t, followUpErr)
		assert.True(t, followUp.TimedOut)
		assert.Empty(t, followUp.MailMessages)
	})
}

func TestWaitForActivity_BlockingWakeAndMCPDispatch(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	coordinatorCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "coord-park",
		SessionID: "sess-park",
	}
	otherCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "other-park",
		SessionID: "sess-other",
	}

	_, err = service.SetMailIdentity(coordinatorCaller, SetMailIdentityInput{Name: "coord"})
	require.NoError(t, err)
	otherMailbox, err := service.SetMailIdentity(otherCaller, SetMailIdentityInput{Name: "bystander"})
	require.NoError(t, err)

	threadMain, _, err := service.CreateThread(coordinatorCaller, CreateThreadInput{Title: "Main Swarm Thread"})
	require.NoError(t, err)
	threadOther, _, err := service.CreateThread(otherCaller, CreateThreadInput{Title: "Unrelated Thread"})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(coordinatorCaller, SubmitTasksInput{
		ThreadID:       threadMain.ID,
		IdempotencyKey: "park-worker-task",
		Tasks: []SubmittedTaskSpec{
			{ClientTaskID: "worker", Title: "Worker task", Instructions: "Park for activity"},
		},
	})
	require.NoError(t, err)
	workerTaskID := submitted.ClientTaskMap["worker"]
	nowText := service.nowUTC()

	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		state.Nodes["node-park"] = protocol.ComputeNode{
			ID:       "node-park",
			Name:     "node-park",
			Kind:     "local",
			Platform: "darwin-arm64",
			Status:   "online",
			LastSeen: nowText,
		}
		state.Instances["inst-park"] = protocol.AgentInstance{
			ID:        "inst-park",
			ThreadID:  threadMain.ID,
			Handle:    "verifier",
			Status:    "busy",
			CreatedAt: nowText,
			UpdatedAt: nowText,
		}
		state.Runs["run-park"] = protocol.Run{
			ID:         "run-park",
			ThreadID:   threadMain.ID,
			TaskID:     &workerTaskID,
			NodeID:     "node-park",
			InstanceID: "inst-park",
			Status:     "running",
			CreatedAt:  nowText,
		}
		return nil
	})

	workerCaller := CallerIdentity{
		Kind:       "run",
		RunID:      "run-park",
		TaskID:     workerTaskID,
		ThreadID:   threadMain.ID,
		InstanceID: "inst-park",
	}
	workerMailbox, err := service.SetMailIdentity(workerCaller, SetMailIdentityInput{Name: "verifier"})
	require.NoError(t, err)

	t.Run("wakes early on incoming mail via worker hub tool while ignoring unrelated traffic", func(t *testing.T) {
		type toolOutcome struct {
			result WaitForActivityResult
			err    error
		}
		outcomeChannel := make(chan toolOutcome, 1)
		startedAt := time.Now()

		rawArguments, marshalErr := json.Marshal(WaitForActivityInput{
			TimeoutMilliseconds: 4000,
		})
		require.NoError(t, marshalErr)

		go func() {
			rawResult, execErr := service.executeHubToolForSlave(
				context.Background(),
				"node-park",
				"run-park",
				"wait_for_activity",
				rawArguments,
			)
			if execErr != nil {
				outcomeChannel <- toolOutcome{err: execErr}
				return
			}
			var decoded WaitForActivityResult
			decodeErr := json.Unmarshal(rawResult, &decoded)
			outcomeChannel <- toolOutcome{result: decoded, err: decodeErr}
		}()

		time.Sleep(40 * time.Millisecond)

		_, unrelatedMailErr := service.SendMail(coordinatorCaller, SendMailInput{
			To:             otherMailbox.Address,
			IdempotencyKey: "unrelated-mail",
			Body:           "Not for verifier",
		})
		require.NoError(t, unrelatedMailErr)

		_, unrelatedRoomErr := service.PostRoomMessage(otherCaller, PostRoomMessageInput{
			ThreadID:       threadOther.ID,
			IdempotencyKey: "unrelated-room",
			Kind:           "finding",
			Body:           "Different thread post",
		})
		require.NoError(t, unrelatedRoomErr)

		_, targetMailErr := service.SendMail(coordinatorCaller, SendMailInput{
			To:             workerMailbox.Address,
			IdempotencyKey: "target-mail",
			Subject:        "Please verify",
			Body:           "Branch swarm/quota-roster is ready",
		})
		require.NoError(t, targetMailErr)

		outcome := <-outcomeChannel
		require.NoError(t, outcome.err)
		assert.Less(t, time.Since(startedAt), 2*time.Second)
		assert.False(t, outcome.result.TimedOut)
		assert.Equal(t, threadMain.ID, outcome.result.ThreadID)
		assert.Equal(t, []string{"mail"}, outcome.result.WakeSources)
		require.Len(t, outcome.result.MailMessages, 1)
		assert.Equal(t, "Branch swarm/quota-roster is ready", outcome.result.MailMessages[0].Body)
	})

	t.Run("wakes early on blackboard room post via orchestrator MCP tool", func(t *testing.T) {
		type toolOutcome struct {
			result WaitForActivityResult
			err    error
		}
		outcomeChannel := make(chan toolOutcome, 1)
		startedAt := time.Now()

		rawArguments, marshalErr := json.Marshal(WaitForActivityInput{
			ThreadID:            threadMain.ID,
			TimeoutMilliseconds: 4000,
		})
		require.NoError(t, marshalErr)

		go func() {
			rawResult, execErr := service.ExecuteOrchestratorTool(
				context.Background(),
				coordinatorCaller,
				"wait_for_activity",
				rawArguments,
			)
			if execErr != nil {
				outcomeChannel <- toolOutcome{err: execErr}
				return
			}
			var decoded WaitForActivityResult
			decodeErr := json.Unmarshal(rawResult, &decoded)
			outcomeChannel <- toolOutcome{result: decoded, err: decodeErr}
		}()

		time.Sleep(40 * time.Millisecond)

		posted, postErr := service.PostRoomMessage(workerCaller, PostRoomMessageInput{
			IdempotencyKey: "verifier-result-post",
			Kind:           "decision",
			Body:           "Verified cleanly",
		})
		require.NoError(t, postErr)

		outcome := <-outcomeChannel
		require.NoError(t, outcome.err)
		assert.Less(t, time.Since(startedAt), 2*time.Second)
		assert.False(t, outcome.result.TimedOut)
		assert.Contains(t, outcome.result.WakeSources, "room")
		assert.Contains(t, outcome.result.WakeSources, "events")
		require.Len(t, outcome.result.RoomMessages, 1)
		assert.Equal(t, posted.Message.ID, outcome.result.RoomMessages[0].ID)
	})

	t.Run("parks until timeout when no matching activity arrives", func(t *testing.T) {
		result, waitErr := service.WaitForActivity(t.Context(), coordinatorCaller, WaitForActivityInput{
			ThreadID:            threadMain.ID,
			TimeoutMilliseconds: 60,
		})
		require.NoError(t, waitErr)
		assert.True(t, result.TimedOut)
		assert.Empty(t, result.WakeSources)
	})
}
