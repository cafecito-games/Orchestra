package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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

func assertOperationErrorCodeValue(t *testing.T, err error, expectedCode string) {
	t.Helper()
	require.Error(t, err)
	operationError, ok := errors.AsType[*OperationError](err)
	require.True(t, ok, "expected error to be *OperationError, got %T: %v", err, err)
	assert.Equal(t, expectedCode, operationError.Code)
}

func mutateStore(t *testing.T, openedStore *store.Store, mutationFunction func(state *store.State, mutation *store.Mutation) error) {
	t.Helper()
	_, err := openedStore.Mutate(mutationFunction)
	require.NoError(t, err)
}

func TestSubmitTasksDAGCycleRejectionAndPersistenceIsolation(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	caller := CallerIdentity{Kind: "operator", OperatorID: "admin"}
	thread, _, err := service.CreateThread(caller, CreateThreadInput{
		Title: "DAG Cycle Verification Thread",
	})
	require.NoError(t, err)

	t.Run("self-dependency rejected without persisting tasks", func(t *testing.T) {
		_, submitError := service.SubmitTasks(caller, SubmitTasksInput{
			ThreadID:       thread.ID,
			IdempotencyKey: "self-dependency-submission",
			Tasks: []SubmittedTaskSpec{
				{
					ClientTaskID: "task-self",
					Title:        "Self dependent task",
					Instructions: "Inspect self dependency",
					Dependencies: []SubmittedDependency{
						{ClientTaskID: "task-self"},
					},
				},
			},
		})
		assertOperationErrorCodeValue(t, submitError, "invalid_request")

		require.NoError(t, openedStore.Read(func(state *store.State) error {
			for _, task := range state.Tasks {
				assert.NotEqual(t, "Self dependent task", task.Title)
				assert.NotEqual(t, thread.ID, task.ThreadID)
			}
			return nil
		}))
	})

	t.Run("multi-node cycle A -> B -> C -> A rejected without persisting partial tasks", func(t *testing.T) {
		_, submitError := service.SubmitTasks(caller, SubmitTasksInput{
			ThreadID:       thread.ID,
			IdempotencyKey: "multi-node-cycle-submission",
			Tasks: []SubmittedTaskSpec{
				{
					ClientTaskID: "task-a",
					Title:        "Task Alpha",
					Instructions: "First cycle node",
					Dependencies: []SubmittedDependency{
						{ClientTaskID: "task-c"},
					},
				},
				{
					ClientTaskID: "task-b",
					Title:        "Task Beta",
					Instructions: "Second cycle node",
					Dependencies: []SubmittedDependency{
						{ClientTaskID: "task-a"},
					},
				},
				{
					ClientTaskID: "task-c",
					Title:        "Task Gamma",
					Instructions: "Third cycle node",
					Dependencies: []SubmittedDependency{
						{ClientTaskID: "task-b"},
					},
				},
			},
		})
		assertOperationErrorCodeValue(t, submitError, "cycle_detected")

		require.NoError(t, openedStore.Read(func(state *store.State) error {
			for _, task := range state.Tasks {
				assert.NotEqual(t, "Task Alpha", task.Title)
				assert.NotEqual(t, "Task Beta", task.Title)
				assert.NotEqual(t, "Task Gamma", task.Title)
			}
			return nil
		}))
	})

	t.Run("valid tasks can still be submitted after cycle rejection", func(t *testing.T) {
		result, submitError := service.SubmitTasks(caller, SubmitTasksInput{
			ThreadID:       thread.ID,
			IdempotencyKey: "valid-linear-submission",
			Tasks: []SubmittedTaskSpec{
				{
					ClientTaskID: "task-first",
					Title:        "First linear task",
					Instructions: "Run initial step",
				},
				{
					ClientTaskID: "task-second",
					Title:        "Second linear task",
					Instructions: "Run subsequent step",
					Dependencies: []SubmittedDependency{
						{ClientTaskID: "task-first"},
					},
				},
			},
		})
		require.NoError(t, submitError)
		assert.Len(t, result.Tasks, 2)
	})
}

func TestTaskRetryExhaustionAndDependencyFailurePolicies(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	caller := CallerIdentity{Kind: "operator", OperatorID: "admin"}
	thread, _, err := service.CreateThread(caller, CreateThreadInput{
		Title: "Retry and Dependency Policy Thread",
	})
	require.NoError(t, err)

	submitResult, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "upstream-and-downstream-batch",
		Tasks: []SubmittedTaskSpec{
			{
				ClientTaskID: "upstream-task",
				Title:        "Upstream failing task",
				Instructions: "Execute attempts until exhaustion",
				MaxAttempts:  2,
			},
			{
				ClientTaskID: "downstream-strict",
				Title:        "Downstream strict dependent",
				Instructions: "Awaits upstream completion",
				Dependencies: []SubmittedDependency{
					{
						ClientTaskID: "upstream-task",
						Policy:       "require-success",
					},
				},
			},
			{
				ClientTaskID: "downstream-lenient",
				Title:        "Downstream lenient dependent",
				Instructions: "Runs after upstream reaches terminal state",
				Dependencies: []SubmittedDependency{
					{
						ClientTaskID: "upstream-task",
						Policy:       "allow-failure",
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, submitResult.Tasks, 3)

	upstreamTaskID := submitResult.ClientTaskMap["upstream-task"]
	downstreamStrictID := submitResult.ClientTaskMap["downstream-strict"]
	downstreamLenientID := submitResult.ClientTaskMap["downstream-lenient"]

	require.NoError(t, openedStore.Read(func(state *store.State) error {
		assert.Equal(t, "ready", state.Tasks[upstreamTaskID].Status)
		assert.Equal(t, "pending", state.Tasks[downstreamStrictID].Status)
		assert.Equal(t, "pending", state.Tasks[downstreamLenientID].Status)
		return nil
	}))

	// Attempt 1: simulate dispatch and failure
	nowText := service.nowUTC()
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		upstreamTask := state.Tasks[upstreamTaskID]
		upstreamTask.Status = "running"
		upstreamTask.AttemptCount = 1
		upstreamTask.Assignment = &protocol.TaskAssignment{
			RunID:      "run-attempt-1",
			NodeID:     "node-worker-1",
			InstanceID: "instance-worker-1",
			Attempt:    1,
			AssignedAt: nowText,
		}
		state.Tasks[upstreamTaskID] = upstreamTask
		state.Runs["run-attempt-1"] = protocol.Run{
			ID:         "run-attempt-1",
			ThreadID:   thread.ID,
			TaskID:     &upstreamTaskID,
			NodeID:     "node-worker-1",
			InstanceID: "instance-worker-1",
			Status:     "running",
			CreatedAt:  nowText,
		}
		return nil
	})

	failureMessage := "process exit code 1"
	service.handleRunFailed("node-worker-1", protocol.SlaveControlMessage{
		RunID: "run-attempt-1",
		Error: &failureMessage,
	}, true)

	// After attempt 1: upstream task should retry and return to ready because AttemptCount (1) < MaxAttempts (2)
	require.NoError(t, openedStore.Read(func(state *store.State) error {
		upstreamTask := state.Tasks[upstreamTaskID]
		assert.Equal(t, "ready", upstreamTask.Status)
		assert.Nil(t, upstreamTask.Assignment)
		assert.Contains(t, upstreamTask.Summary, "retrying after attempt 1 failed")
		assert.Equal(t, "pending", state.Tasks[downstreamStrictID].Status)
		assert.Equal(t, "pending", state.Tasks[downstreamLenientID].Status)
		return nil
	}))

	// Attempt 2: simulate second dispatch and failure
	nowText = service.nowUTC()
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		upstreamTask := state.Tasks[upstreamTaskID]
		upstreamTask.Status = "running"
		upstreamTask.AttemptCount = 2
		upstreamTask.Assignment = &protocol.TaskAssignment{
			RunID:      "run-attempt-2",
			NodeID:     "node-worker-1",
			InstanceID: "instance-worker-1",
			Attempt:    2,
			AssignedAt: nowText,
		}
		state.Tasks[upstreamTaskID] = upstreamTask
		state.Runs["run-attempt-2"] = protocol.Run{
			ID:         "run-attempt-2",
			ThreadID:   thread.ID,
			TaskID:     &upstreamTaskID,
			NodeID:     "node-worker-1",
			InstanceID: "instance-worker-1",
			Status:     "running",
			CreatedAt:  nowText,
		}
		return nil
	})

	exhaustionMessage := "process fatal crash"
	service.handleRunFailed("node-worker-1", protocol.SlaveControlMessage{
		RunID: "run-attempt-2",
		Error: &exhaustionMessage,
	}, true)

	// After attempt 2: upstream exhausted retries (2 >= 2) -> terminal failed.
	// Downstream strict task must be blocked with blocker reason.
	// Downstream lenient task must transition to ready.
	require.NoError(t, openedStore.Read(func(state *store.State) error {
		upstreamTask := state.Tasks[upstreamTaskID]
		assert.Equal(t, "failed", upstreamTask.Status)
		assert.NotEmpty(t, upstreamTask.FinishedAt)

		downstreamStrict := state.Tasks[downstreamStrictID]
		assert.Equal(t, "blocked", downstreamStrict.Status)
		assert.Contains(t, downstreamStrict.BlockerReason, "failed")

		downstreamLenient := state.Tasks[downstreamLenientID]
		assert.Equal(t, "ready", downstreamLenient.Status)
		return nil
	}))
}

func TestActiveTaskCancellationDispatchesToSlaveAndCleansAllocations(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	httpServer := httptest.NewServer(service.Handler())
	defer httpServer.Close()

	wsURL := strings.Replace(httpServer.URL, "http://", "ws://", 1) + "/control-agent"
	dialContext, cancelDial := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelDial()

	slaveConnection, dialResponse, err := websocket.Dial(dialContext, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer master-secret-token"}},
	})
	if dialResponse != nil && dialResponse.Body != nil {
		_ = dialResponse.Body.Close()
	}
	require.NoError(t, err)
	defer slaveConnection.Close(websocket.StatusNormalClosure, "test completed")

	nowText := service.nowUTC()
	registerMessage, err := json.Marshal(protocol.SlaveControlMessage{
		Type:            "register",
		ProtocolVersion: protocol.ProtocolVersion,
		Node: &protocol.ComputeNode{
			ID:          "slave-node-active",
			Name:        "slave-node-active",
			Kind:        "local",
			Platform:    "linux-amd64",
			Status:      "online",
			LastSeen:    nowText,
			Concurrency: 2,
			Version:     "1.0.0",
		},
	})
	require.NoError(t, err)
	require.NoError(t, slaveConnection.Write(t.Context(), websocket.MessageText, registerMessage))

	syncCompleteMessage, err := json.Marshal(protocol.SlaveControlMessage{
		Type: "sync.complete",
	})
	require.NoError(t, err)
	require.NoError(t, slaveConnection.Write(t.Context(), websocket.MessageText, syncCompleteMessage))

	require.Eventually(t, func() bool {
		return service.isNodeLiveAndSynced("slave-node-active")
	}, 5*time.Second, 25*time.Millisecond)

	caller := CallerIdentity{Kind: "operator", OperatorID: "admin"}
	thread, _, err := service.CreateThread(caller, CreateThreadInput{
		Title: "Cancellation Verification Thread",
	})
	require.NoError(t, err)

	taskID := protocol.NewIdentifier("task")
	runID := protocol.NewIdentifier("run")
	instanceID := protocol.NewIdentifier("inst")
	allocationID := protocol.NewIdentifier("alloc")
	approvalID := protocol.NewIdentifier("app")

	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		state.Instances[instanceID] = protocol.AgentInstance{
			ID:          instanceID,
			ThreadID:    thread.ID,
			Status:      "busy",
			AutoManaged: true,
			Lease: protocol.InstanceLease{
				ExpiresAt:          extendLeaseTimestamp(nowText, 300),
				IdleTimeoutSeconds: 300,
			},
			CreatedAt: nowText,
			UpdatedAt: nowText,
		}
		state.Allocations[allocationID] = protocol.InstanceAllocation{
			ID:         allocationID,
			InstanceID: instanceID,
			NodeID:     "slave-node-active",
			Status:     "active",
			CreatedAt:  nowText,
			UpdatedAt:  nowText,
		}
		state.Tasks[taskID] = protocol.Task{
			ID:           taskID,
			ThreadID:     thread.ID,
			Title:        "Active task to cancel",
			Instructions: "Long running execution",
			Status:       "running",
			AttemptCount: 1,
			MaxAttempts:  3,
			Assignment: &protocol.TaskAssignment{
				RunID:      runID,
				NodeID:     "slave-node-active",
				InstanceID: instanceID,
				Attempt:    1,
				AssignedAt: nowText,
			},
			CreatedAt: nowText,
			UpdatedAt: nowText,
		}
		state.Runs[runID] = protocol.Run{
			ID:           runID,
			ThreadID:     thread.ID,
			TaskID:       &taskID,
			NodeID:       "slave-node-active",
			InstanceID:   instanceID,
			AllocationID: allocationID,
			Status:       "running",
			CreatedAt:    nowText,
		}
		state.Approvals[approvalID] = protocol.ApprovalRequest{
			ID:            approvalID,
			ThreadID:      thread.ID,
			RunID:         runID,
			NodeID:        "slave-node-active",
			InstanceID:    instanceID,
			AllocationID:  allocationID,
			Status:        "pending",
			DeliveryState: "sent",
			Title:         "Pending dangerous command",
			RequestedAt:   nowText,
			ExpiresAt:     extendLeaseTimestamp(nowText, 600),
			UpdatedAt:     nowText,
		}
		return nil
	})

	cancelResult, err := service.CancelTarget(taskID)
	require.NoError(t, err)
	assert.Equal(t, "task", cancelResult.Kind)
	require.NotNil(t, cancelResult.Task)
	assert.Equal(t, "cancelled", cancelResult.Task.Status)

	var masterMessage protocol.MasterControlMessage
	require.Eventually(t, func() bool {
		readContext, cancelRead := context.WithTimeout(t.Context(), 1*time.Second)
		defer cancelRead()
		_, payload, readErr := slaveConnection.Read(readContext)
		if readErr != nil {
			return false
		}
		var received protocol.MasterControlMessage
		if json.Unmarshal(payload, &received) != nil {
			return false
		}
		if received.Type == "cancel" && received.RunID == runID {
			masterMessage = received
			return true
		}
		return false
	}, 5*time.Second, 25*time.Millisecond)
	assert.Equal(t, "cancel", masterMessage.Type)
	assert.Equal(t, runID, masterMessage.RunID)

	require.NoError(t, openedStore.Read(func(state *store.State) error {
		task := state.Tasks[taskID]
		assert.Equal(t, "cancelled", task.Status)
		assert.NotEmpty(t, task.FinishedAt)

		runRecord := state.Runs[runID]
		assert.Equal(t, "cancelled", runRecord.Status)
		require.NotNil(t, runRecord.FinishedAt)

		approval := state.Approvals[approvalID]
		assert.Equal(t, "cancelled", approval.Status)
		assert.Equal(t, "not-applied", approval.DeliveryState)

		instance := state.Instances[instanceID]
		assert.Equal(t, "idle", instance.Status)
		return nil
	}))
}

func TestTaskCompletionBlockedByUnreadRoomNudgesAcrossMultipleRooms(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	operatorCaller := CallerIdentity{Kind: "operator", OperatorID: "admin"}

	threadAlpha, _, err := service.CreateThread(operatorCaller, CreateThreadInput{
		Title: "Thread Alpha Room",
	})
	require.NoError(t, err)

	threadBeta, _, err := service.CreateThread(operatorCaller, CreateThreadInput{
		Title: "Thread Beta Room",
	})
	require.NoError(t, err)

	taskAlphaID := protocol.NewIdentifier("task")
	taskBetaID := protocol.NewIdentifier("task")
	runAlphaID := protocol.NewIdentifier("run")
	runBetaID := protocol.NewIdentifier("run")
	nowText := service.nowUTC()

	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		state.Tasks[taskAlphaID] = protocol.Task{
			ID:           taskAlphaID,
			ThreadID:     threadAlpha.ID,
			Title:        "Alpha worker task",
			Instructions: "Alpha instructions",
			Status:       "running",
			AttemptCount: 1,
			MaxAttempts:  3,
			Assignment: &protocol.TaskAssignment{
				RunID:      runAlphaID,
				NodeID:     "node-local",
				Attempt:    1,
				AssignedAt: nowText,
			},
			CreatedAt: nowText,
			UpdatedAt: nowText,
		}
		state.Runs[runAlphaID] = protocol.Run{
			ID:        runAlphaID,
			ThreadID:  threadAlpha.ID,
			TaskID:    &taskAlphaID,
			NodeID:    "node-local",
			Status:    "running",
			CreatedAt: nowText,
		}
		state.Tasks[taskBetaID] = protocol.Task{
			ID:           taskBetaID,
			ThreadID:     threadBeta.ID,
			Title:        "Beta worker task",
			Instructions: "Beta instructions",
			Status:       "running",
			AttemptCount: 1,
			MaxAttempts:  3,
			Assignment: &protocol.TaskAssignment{
				RunID:      runBetaID,
				NodeID:     "node-local",
				Attempt:    1,
				AssignedAt: nowText,
			},
			CreatedAt: nowText,
			UpdatedAt: nowText,
		}
		state.Runs[runBetaID] = protocol.Run{
			ID:        runBetaID,
			ThreadID:  threadBeta.ID,
			TaskID:    &taskBetaID,
			NodeID:    "node-local",
			Status:    "running",
			CreatedAt: nowText,
		}
		return nil
	})

	callerAlpha := CallerIdentity{Kind: "run", RunID: runAlphaID, TaskID: taskAlphaID, ThreadID: threadAlpha.ID}
	callerBeta := CallerIdentity{Kind: "run", RunID: runBetaID, TaskID: taskBetaID, ThreadID: threadBeta.ID}

	_, err = service.PostRoomMessage(operatorCaller, PostRoomMessageInput{
		ThreadID:       threadAlpha.ID,
		IdempotencyKey: "alpha-nudge-1",
		Kind:           "guidance",
		Topic:          "architecture",
		Body:           "Alpha team update",
		NudgeTaskIDs:   []string{taskAlphaID},
	})
	require.NoError(t, err)

	_, err = service.PostRoomMessage(operatorCaller, PostRoomMessageInput{
		ThreadID:       threadBeta.ID,
		IdempotencyKey: "beta-nudge-1",
		Kind:           "decision",
		Topic:          "api-contract",
		Body:           "Beta team update",
		NudgeTaskIDs:   []string{taskBetaID},
	})
	require.NoError(t, err)

	_, updateAlphaErr := service.UpdateTask(callerAlpha, UpdateTaskInput{
		IdempotencyKey: "complete-alpha-fail",
		Completion: &TaskCompletionInput{
			Summary: "Attempted completion before reading Alpha room",
		},
	})
	assertOperationErrorCodeValue(t, updateAlphaErr, "unread_room_nudge")

	_, updateBetaErr := service.UpdateTask(callerBeta, UpdateTaskInput{
		IdempotencyKey: "complete-beta-fail",
		Completion: &TaskCompletionInput{
			Summary: "Attempted completion before reading Beta room",
		},
	})
	assertOperationErrorCodeValue(t, updateBetaErr, "unread_room_nudge")

	// Post a second nudge to Beta before it has read the room
	_, err = service.PostRoomMessage(operatorCaller, PostRoomMessageInput{
		ThreadID:       threadBeta.ID,
		IdempotencyKey: "beta-nudge-2",
		Kind:           "finding",
		Topic:          "gotcha",
		Body:           "Second beta discovery",
		NudgeTaskIDs:   []string{taskBetaID},
	})
	require.NoError(t, err)

	alphaReadResult, err := service.ReadRoom(callerAlpha, ReadRoomInput{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), alphaReadResult.LastReadSequence)
	assert.False(t, alphaReadResult.UnreadNudge)

	completedAlphaTask, err := service.UpdateTask(callerAlpha, UpdateTaskInput{
		IdempotencyKey: "complete-alpha-success",
		Status:         "completed",
		Completion: &TaskCompletionInput{
			Summary: "Alpha task completed after acknowledging nudges",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "completed", completedAlphaTask.Status)

	// Task Beta completion must still be blocked because second Beta room nudge has not been acknowledged
	_, updateBetaErr2 := service.UpdateTask(callerBeta, UpdateTaskInput{
		IdempotencyKey: "complete-beta-fail-2",
		Completion: &TaskCompletionInput{
			Summary: "Still unread second Beta room nudge",
		},
	})
	assertOperationErrorCodeValue(t, updateBetaErr2, "unread_room_nudge")

	betaReadResult, err := service.ReadRoom(callerBeta, ReadRoomInput{})
	require.NoError(t, err)
	assert.Equal(t, int64(2), betaReadResult.LastReadSequence)
	assert.False(t, betaReadResult.UnreadNudge)

	completedBetaTask, err := service.UpdateTask(callerBeta, UpdateTaskInput{
		IdempotencyKey: "complete-beta-success",
		Status:         "completed",
		Completion: &TaskCompletionInput{
			Summary: "Beta task completed after acknowledging nudges",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "completed", completedBetaTask.Status)
}

func TestToolApprovalInvariants(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	caller := CallerIdentity{Kind: "operator", OperatorID: "admin"}
	thread, _, err := service.CreateThread(caller, CreateThreadInput{
		Title: "Approval Invariants Thread",
	})
	require.NoError(t, err)

	approvalID := protocol.NewIdentifier("app")
	nowText := service.nowUTC()

	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		state.Approvals[approvalID] = protocol.ApprovalRequest{
			ID:           approvalID,
			ThreadID:     thread.ID,
			RunID:        "run-test-approval",
			NodeID:       "node-test",
			InstanceID:   "inst-test",
			AllocationID: "alloc-test",
			Status:       "pending",
			Title:        "Execute sensitive action",
			Options: []protocol.ApprovalOption{
				{
					ID:    "option-allow-once",
					Kind:  "allow_once",
					Label: "Allow once",
				},
				{
					ID:    "option-reject-once",
					Kind:  "reject_once",
					Label: "Reject once",
				},
			},
			RequestedAt: nowText,
			ExpiresAt:   extendLeaseTimestamp(nowText, 600),
			UpdatedAt:   nowText,
		}
		return nil
	})

	t.Run("resolving non-existent approval fails cleanly with not_found", func(t *testing.T) {
		_, resolveError := service.ResolveApproval(caller, ResolveApprovalInput{
			ApprovalID: "app-does-not-exist",
			OptionID:   "option-allow-once",
		})
		assertOperationErrorCodeValue(t, resolveError, "not_found")
	})

	t.Run("passing unknown option ID fails cleanly with invalid_request", func(t *testing.T) {
		_, resolveError := service.ResolveApproval(caller, ResolveApprovalInput{
			ApprovalID: approvalID,
			OptionID:   "option-unknown-fake",
		})
		assertOperationErrorCodeValue(t, resolveError, "invalid_request")

		require.NoError(t, openedStore.Read(func(state *store.State) error {
			assert.Equal(t, "pending", state.Approvals[approvalID].Status)
			return nil
		}))
	})

	var resolvedApproval protocol.ApprovalRequest
	t.Run("resolving pending approval succeeds", func(t *testing.T) {
		resolved, resolveError := service.ResolveApproval(caller, ResolveApprovalInput{
			ApprovalID: approvalID,
			OptionID:   "option-allow-once",
		})
		require.NoError(t, resolveError)
		assert.Equal(t, "approved", resolved.Status)
		assert.Equal(t, "option-allow-once", resolved.SelectedOptionID)
		assert.NotEmpty(t, resolved.ResolvedAt)
		resolvedApproval = resolved
	})

	t.Run("idempotent same-decision replay succeeds and returns existing resolved approval", func(t *testing.T) {
		replayedOption, replayOptionError := service.ResolveApproval(caller, ResolveApprovalInput{
			ApprovalID: approvalID,
			OptionID:   "option-allow-once",
		})
		require.NoError(t, replayOptionError)
		assert.Equal(t, resolvedApproval.ID, replayedOption.ID)
		assert.Equal(t, resolvedApproval.Status, replayedOption.Status)
		assert.Equal(t, resolvedApproval.SelectedOptionID, replayedOption.SelectedOptionID)
		assert.Equal(t, resolvedApproval.ResolvedAt, replayedOption.ResolvedAt)

		replayedStatus, replayStatusError := service.ResolveApproval(caller, ResolveApprovalInput{
			ApprovalID: approvalID,
			Status:     "approved",
		})
		require.NoError(t, replayStatusError)
		assert.Equal(t, resolvedApproval.ID, replayedStatus.ID)
		assert.Equal(t, resolvedApproval.Status, replayedStatus.Status)
		assert.Equal(t, resolvedApproval.SelectedOptionID, replayedStatus.SelectedOptionID)
		assert.Equal(t, resolvedApproval.ResolvedAt, replayedStatus.ResolvedAt)
	})

	t.Run("conflicting re-resolution on already-resolved approval returns already_resolved conflict error", func(t *testing.T) {
		_, conflictOptionError := service.ResolveApproval(caller, ResolveApprovalInput{
			ApprovalID: approvalID,
			OptionID:   "option-reject-once",
		})
		assertOperationErrorCodeValue(t, conflictOptionError, "already_resolved")

		_, conflictStatusError := service.ResolveApproval(caller, ResolveApprovalInput{
			ApprovalID: approvalID,
			Status:     "rejected",
		})
		assertOperationErrorCodeValue(t, conflictStatusError, "already_resolved")

		_, conflictCancelError := service.ResolveApproval(caller, ResolveApprovalInput{
			ApprovalID: approvalID,
			Status:     "cancelled",
		})
		assertOperationErrorCodeValue(t, conflictCancelError, "already_resolved")
	})

	t.Run("resolving expired approval with approval decision returns already_resolved conflict error", func(t *testing.T) {
		expiredApprovalID := protocol.NewIdentifier("app")
		mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
			state.Approvals[expiredApprovalID] = protocol.ApprovalRequest{
				ID:           expiredApprovalID,
				ThreadID:     thread.ID,
				RunID:        "run-test-expired",
				NodeID:       "node-test",
				InstanceID:   "inst-test",
				AllocationID: "alloc-test",
				Status:       "expired",
				Title:        "Expired action",
				Options: []protocol.ApprovalOption{
					{
						ID:    "option-expired-allow",
						Kind:  "allow_once",
						Label: "Allow once",
					},
				},
				RequestedAt: nowText,
				ExpiresAt:   nowText,
				UpdatedAt:   nowText,
			}
			return nil
		})

		_, conflictExpiredError := service.ResolveApproval(caller, ResolveApprovalInput{
			ApprovalID: expiredApprovalID,
			OptionID:   "option-expired-allow",
		})
		assertOperationErrorCodeValue(t, conflictExpiredError, "already_resolved")
	})
}
