package orchestrator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

func TestCoordinatorWake_BindingInvariants(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{Store: openedStore, Token: "coord-bind-token"})
	require.NoError(t, err)
	defer service.Close()

	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "coord-test", SessionID: "sess-1"}
	threadA, _, err := service.CreateThread(caller, CreateThreadInput{Title: "Thread A"})
	require.NoError(t, err)
	threadB, _, err := service.CreateThread(caller, CreateThreadInput{Title: "Thread B"})
	require.NoError(t, err)

	nowText := service.nowUTC()
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		state.Instances["inst-no-delegate"] = protocol.AgentInstance{
			ID: "inst-no-delegate", ThreadID: threadA.ID, Status: "idle",
			Delegation: protocol.InstanceDelegationPolicy{CanDelegate: false},
			CreatedAt:  nowText, UpdatedAt: nowText,
		}
		state.Instances["inst-other-thread"] = protocol.AgentInstance{
			ID: "inst-other-thread", ThreadID: threadB.ID, Status: "idle",
			Delegation: protocol.InstanceDelegationPolicy{CanDelegate: true},
			CreatedAt:  nowText, UpdatedAt: nowText,
		}
		state.Instances["inst-draining"] = protocol.AgentInstance{
			ID: "inst-draining", ThreadID: threadA.ID, Status: "draining",
			Delegation: protocol.InstanceDelegationPolicy{CanDelegate: true},
			CreatedAt:  nowText, UpdatedAt: nowText,
		}
		state.Instances["inst-coord-ok"] = protocol.AgentInstance{
			ID: "inst-coord-ok", ThreadID: threadA.ID, Status: "idle",
			Delegation: protocol.InstanceDelegationPolicy{CanDelegate: true},
			CreatedAt:  nowText, UpdatedAt: nowText,
		}
		return nil
	})

	tests := []struct {
		name       string
		instanceID string
		wantCode   string
	}{
		{name: "non-delegating instance rejected", instanceID: "inst-no-delegate", wantCode: "forbidden"},
		{name: "cross-thread instance rejected", instanceID: "inst-other-thread", wantCode: "forbidden"},
		{name: "draining instance rejected", instanceID: "inst-draining", wantCode: "instance_unavailable"},
		{name: "missing instance rejected", instanceID: "inst-missing", wantCode: "not_found"},
		{name: "valid delegating same-thread instance bound", instanceID: "inst-coord-ok", wantCode: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idCopy := tc.instanceID
			updated, updateErr := service.UpdateThread(caller, UpdateThreadInput{
				ThreadID:              threadA.ID,
				CoordinatorInstanceID: NullableString{Set: true, Value: &idCopy},
			})
			if tc.wantCode == "" {
				require.NoError(t, updateErr)
				require.NotNil(t, updated.Orchestrator)
				assert.Equal(t, "instance", updated.Orchestrator.Kind)
				assert.Equal(t, tc.instanceID, updated.Orchestrator.InstanceID)
			} else {
				require.Error(t, updateErr)
				var opErr *OperationError
				require.ErrorAs(t, updateErr, &opErr)
				assert.Equal(t, tc.wantCode, opErr.Code)
			}
		})
	}

	// Unbind via null CoordinatorInstanceID.
	unbound, err := service.UpdateThread(caller, UpdateThreadInput{
		ThreadID:              threadA.ID,
		CoordinatorInstanceID: NullableString{Set: true, Value: nil},
	})
	require.NoError(t, err)
	assert.Nil(t, unbound.Orchestrator)
}

func TestCoordinatorWake_CoalescingParallelCompletionsAndReplayIdempotency(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{Store: openedStore, Token: "coord-coalesce-token"})
	require.NoError(t, err)
	defer service.Close()

	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "coord-client", SessionID: "sess-c"}
	thread, _, err := service.CreateThread(caller, CreateThreadInput{Title: "Parallel Coalesce Thread"})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "sub-parallel",
		Tasks: []SubmittedTaskSpec{
			{ClientTaskID: "w1", Title: "Worker 1", Instructions: "Work 1"},
			{ClientTaskID: "w2", Title: "Worker 2", Instructions: "Work 2"},
		},
	})
	require.NoError(t, err)
	w1ID := submitted.ClientTaskMap["w1"]
	w2ID := submitted.ClientTaskMap["w2"]

	nowText := service.nowUTC()
	const coordID = "inst-coord-1"
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		state.Instances[coordID] = protocol.AgentInstance{
			ID: coordID, ThreadID: thread.ID, Status: "idle",
			Delegation: protocol.InstanceDelegationPolicy{CanDelegate: true},
			Lease:      protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
			CreatedAt:  nowText, UpdatedAt: nowText,
		}
		state.Instances["inst-worker-1"] = protocol.AgentInstance{
			ID: "inst-worker-1", ThreadID: thread.ID, Status: "busy",
			CreatedAt: nowText, UpdatedAt: nowText,
		}
		state.Instances["inst-worker-2"] = protocol.AgentInstance{
			ID: "inst-worker-2", ThreadID: thread.ID, Status: "busy",
			CreatedAt: nowText, UpdatedAt: nowText,
		}
		for idx, taskID := range []string{w1ID, w2ID} {
			runID := "run-w-" + string(rune('1'+idx))
			instID := "inst-worker-" + string(rune('1'+idx))
			taskCopy := taskID
			tRec := state.Tasks[taskID]
			tRec.Status = "running"
			tRec.AttemptCount = 1
			tRec.Assignment = &protocol.TaskAssignment{RunID: runID, InstanceID: instID, NodeID: "node-1", Attempt: 1, AssignedAt: nowText}
			state.Tasks[taskID] = tRec
			state.Runs[runID] = protocol.Run{
				ID: runID, ThreadID: thread.ID, TaskID: &taskCopy, InstanceID: instID, NodeID: "node-1", Status: "running", CreatedAt: nowText,
			}
		}
		return nil
	})

	coordCopy := coordID
	_, err = service.UpdateThread(caller, UpdateThreadInput{
		ThreadID:              thread.ID,
		CoordinatorInstanceID: NullableString{Set: true, Value: &coordCopy},
	})
	require.NoError(t, err)

	// Both worker tasks complete simultaneously before the coordinator wakes up.
	service.handleRunCompleted("node-1", protocol.SlaveControlMessage{RunID: "run-w-1", Output: new("worker 1 done")})
	service.handleRunCompleted("node-1", protocol.SlaveControlMessage{RunID: "run-w-2", Output: new("worker 2 done")})

	// Exactly ONE coordinator-wake task must be queued, coalescing BOTH worker completions.
	ctxResp, err := service.GetThreadContext(thread.ID)
	require.NoError(t, err)
	require.NotNil(t, ctxResp.Thread.Orchestrator)
	assert.Equal(t, coordID, ctxResp.Thread.Orchestrator.InstanceID)
	assert.NotEmpty(t, ctxResp.Thread.Orchestrator.PendingWakeTaskID)
	assert.Greater(t, ctxResp.Thread.Orchestrator.PendingWakeEventSequence, ctxResp.Thread.Orchestrator.LastHandledEventSequence)

	var wakeTasks []protocol.Task
	for _, task := range ctxResp.Tasks {
		if task.Origin.Kind == coordinatorWakeOriginKind {
			wakeTasks = append(wakeTasks, task)
		}
	}
	require.Len(t, wakeTasks, 1, "parallel completions must coalesce into a single coordinator wake task")
	assert.Equal(t, coordID, wakeTasks[0].PlacementInstanceID)
	assert.Contains(t, wakeTasks[0].Instructions, w1ID)
	assert.Contains(t, wakeTasks[0].Instructions, w2ID)

	// Event replay of an already-queued sequence must NOT create a duplicate wake task.
	pendingSeq := ctxResp.Thread.Orchestrator.PendingWakeEventSequence
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		woke := service.scheduleCoordinatorWakeInState(state, mutation, thread.ID, "task.completed", "replayed event", "inst-worker-1", pendingSeq, service.nowUTC())
		assert.False(t, woke, "replaying an already-pending event sequence must be a no-op")
		return nil
	})

	// Self-event loop suppression: coordinator completing its own wake task must NOT trigger another wake.
	wakeTaskID := wakeTasks[0].ID
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		wakeTask := state.Tasks[wakeTaskID]
		wakeTask.Status = "running"
		wakeTask.AttemptCount = 1
		wakeTask.Assignment = &protocol.TaskAssignment{RunID: "run-coord-1", InstanceID: coordID, NodeID: "node-1", Attempt: 1, AssignedAt: nowText}
		state.Tasks[wakeTaskID] = wakeTask
		state.Runs["run-coord-1"] = protocol.Run{
			ID: "run-coord-1", ThreadID: thread.ID, TaskID: &wakeTaskID, InstanceID: coordID, NodeID: "node-1", Status: "running", CreatedAt: nowText,
		}
		coordInst := state.Instances[coordID]
		coordInst.Status = "busy"
		state.Instances[coordID] = coordInst
		service.markCoordinatorRunDispatchedInState(state, thread.ID, coordID, wakeTaskID, service.nowUTC())
		return nil
	})
	service.handleRunCompleted("node-1", protocol.SlaveControlMessage{RunID: "run-coord-1", Output: new("coordinator turn complete")})

	ctxAfter, err := service.GetThreadContext(thread.ID)
	require.NoError(t, err)
	wakeCountAfter := 0
	for _, task := range ctxAfter.Tasks {
		if task.Origin.Kind == coordinatorWakeOriginKind {
			wakeCountAfter++
		}
	}
	assert.Equal(t, 1, wakeCountAfter, "coordinator completing its own turn must not self-wake")
	assert.Empty(t, ctxAfter.Thread.Orchestrator.PendingWakeTaskID)
	assert.GreaterOrEqual(t, ctxAfter.Thread.Orchestrator.LastHandledEventSequence, pendingSeq)
}

func TestCoordinatorWake_BlockedFailedApprovalMessageAndReleaseCleanup(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{Store: openedStore, Token: "coord-triggers-token"})
	require.NoError(t, err)
	defer service.Close()

	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "coord-trig", SessionID: "sess-t"}
	thread, _, err := service.CreateThread(caller, CreateThreadInput{Title: "Coordinator Triggers"})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "sub-triggers",
		Tasks: []SubmittedTaskSpec{
			{ClientTaskID: "t-block", Title: "Blockable", Instructions: "Work", MaxAttempts: 1},
			{ClientTaskID: "t-fail", Title: "Failable", Instructions: "Work", MaxAttempts: 1},
		},
	})
	require.NoError(t, err)
	blockID := submitted.ClientTaskMap["t-block"]
	failID := submitted.ClientTaskMap["t-fail"]

	nowText := service.nowUTC()
	const coordID = "inst-coord-trig"
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		state.Instances[coordID] = protocol.AgentInstance{
			ID: coordID, ThreadID: thread.ID, Status: "idle",
			Delegation: protocol.InstanceDelegationPolicy{CanDelegate: true},
			Lease:      protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
			CreatedAt:  nowText, UpdatedAt: nowText,
		}
		blockTask := state.Tasks[blockID]
		blockTask.Status = "running"
		blockTask.AttemptCount = 1
		blockTask.Assignment = &protocol.TaskAssignment{RunID: "run-b", InstanceID: "inst-w", NodeID: "node-1", Attempt: 1, AssignedAt: nowText}
		state.Tasks[blockID] = blockTask
		state.Runs["run-b"] = protocol.Run{
			ID: "run-b", ThreadID: thread.ID, TaskID: &blockID, InstanceID: "inst-w", NodeID: "node-1", Status: "running", CreatedAt: nowText,
		}
		failTask := state.Tasks[failID]
		failTask.Status = "running"
		failTask.AttemptCount = 1
		failTask.Assignment = &protocol.TaskAssignment{RunID: "run-f", InstanceID: "inst-w", NodeID: "node-1", Attempt: 1, AssignedAt: nowText}
		state.Tasks[failID] = failTask
		state.Runs["run-f"] = protocol.Run{
			ID: "run-f", ThreadID: thread.ID, TaskID: &failID, InstanceID: "inst-w", NodeID: "node-1", Status: "running", CreatedAt: nowText,
		}
		state.Approvals["appr-1"] = protocol.ApprovalRequest{
			ID: "appr-1", RunID: "run-f", ThreadID: thread.ID, TaskID: failID, InstanceID: "inst-w", NodeID: "node-1",
			Title: "Run command", Options: []protocol.ApprovalOption{{ID: "opt-allow", Label: "Allow", Kind: "allow-once"}},
			Status: "pending", DeliveryState: "pending", RequestedAt: nowText, ExpiresAt: extendLeaseTimestamp(nowText, 600), UpdatedAt: nowText,
		}
		return nil
	})

	coordCopy := coordID
	_, err = service.UpdateThread(caller, UpdateThreadInput{
		ThreadID:              thread.ID,
		CoordinatorInstanceID: NullableString{Set: true, Value: &coordCopy},
	})
	require.NoError(t, err)

	// 1. Worker yields as blocked -> wakes coordinator.
	reason := "Need API contract"
	_, err = service.UpdateTask(
		CallerIdentity{Kind: "run", RunID: "run-b", TaskID: blockID, ThreadID: thread.ID, InstanceID: "inst-w"},
		UpdateTaskInput{IdempotencyKey: "block-wake-1", Status: "blocked", BlockerReason: &reason},
	)
	require.NoError(t, err)

	// 2. Operator resolves approval while run-f is still active -> coalesces into the queued wake task.
	_, err = service.ResolveApproval(CallerIdentity{Kind: "operator", OperatorID: "admin"}, ResolveApprovalInput{
		ApprovalID: "appr-1", Status: "approved",
	})
	require.NoError(t, err)

	// 3. Worker fails -> coalesces into the queued wake task.
	service.handleRunFailed("node-1", protocol.SlaveControlMessage{RunID: "run-f", Error: new("crash")}, true)

	// 4. Worker sends message to orchestrator -> coalesces into the queued wake task.
	_, err = service.SendTaskMessage(
		CallerIdentity{Kind: "run", RunID: "run-b", TaskID: blockID, ThreadID: thread.ID, InstanceID: "inst-w"},
		SendTaskMessageInput{IdempotencyKey: "msg-coord-1", ToOrchestrator: true, Kind: "question", Body: "Which endpoint?"},
	)
	require.NoError(t, err)

	ctxResp, err := service.GetThreadContext(thread.ID)
	require.NoError(t, err)
	require.NotNil(t, ctxResp.Thread.Orchestrator)
	wakeTaskID := ctxResp.Thread.Orchestrator.PendingWakeTaskID
	require.NotEmpty(t, wakeTaskID)

	var queuedWake protocol.Task
	for _, task := range ctxResp.Tasks {
		if task.ID == wakeTaskID {
			queuedWake = task
		}
	}
	assert.Contains(t, queuedWake.Instructions, "task.blocked")
	assert.Contains(t, queuedWake.Instructions, "task.failed")
	assert.Contains(t, queuedWake.Instructions, "approval.resolved")
	assert.Contains(t, queuedWake.Instructions, "message.posted")

	// 5. Releasing the coordinator instance cancels the queued wake task and clears binding cleanly.
	_, err = service.ReleaseInstance(caller, ReleaseInstanceInput{
		InstanceID:     coordID,
		IdempotencyKey: "rel-coord-1",
		Mode:           "drain",
	})
	require.NoError(t, err)

	require.NoError(t, openedStore.Read(func(state *store.State) error {
		assert.Nil(t, state.Threads[thread.ID].Orchestrator, "releasing coordinator must unbind it")
		assert.Equal(t, "cancelled", state.Tasks[wakeTaskID].Status, "queued wake task must be cancelled on coordinator release")
		return nil
	}))
}
