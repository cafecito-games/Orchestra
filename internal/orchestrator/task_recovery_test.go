package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestTaskYieldOnBlock_NonDestructiveDownstreamAndUnblock(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	const secretToken = "sk-ant-api03-verysecretvalue1234567890"
	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-token-yield",
	})
	require.NoError(t, err)
	defer service.Close()

	httpServer := httptest.NewServer(service.Handler())
	defer httpServer.Close()

	wsURL := strings.Replace(httpServer.URL, "http://", "ws://", 1) + "/control-agent"
	dialContext, cancelDial := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelDial()
	slaveConn, dialResp, err := websocket.Dial(dialContext, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer master-token-yield"}},
	})
	if dialResp != nil && dialResp.Body != nil {
		_ = dialResp.Body.Close()
	}
	require.NoError(t, err)
	defer slaveConn.Close(websocket.StatusNormalClosure, "done")

	nowText := service.nowUTC()
	regPayload, err := json.Marshal(protocol.SlaveControlMessage{
		Type:            "register",
		ProtocolVersion: protocol.ProtocolVersion,
		Node: &protocol.ComputeNode{
			ID:               "node-yield-1",
			Name:             "node-yield-1",
			Kind:             "local",
			Platform:         "linux-amd64",
			Status:           "online",
			LastSeen:         nowText,
			Version:          "1.0.0",
			Concurrency:      1,
			InstanceCapacity: new(4),
			WorkspaceRoots:   []string{"/workspace"},
			Harnesses: []protocol.HarnessProfile{{
				ID:          protocol.HarnessCodexCLI,
				Label:       "codex",
				Description: "Codex CLI",
				Available:   true,
				Transports:  []string{protocol.TransportACP},
				Models:      []string{"default"},
				AuthMode:    "local-subscription",
			}},
		},
		At: nowText,
	})
	require.NoError(t, err)
	require.NoError(t, slaveConn.Write(dialContext, websocket.MessageText, regPayload))

	_, _, err = slaveConn.Read(dialContext) // ping
	require.NoError(t, err)

	syncPayload, err := json.Marshal(protocol.SlaveControlMessage{
		Type:              "sync.complete",
		ActiveRuns:        new(0),
		ActiveInstances:   new(0),
		ActiveRunIDs:      &[]string{},
		ActiveInstanceIDs: &[]string{},
		At:                service.nowUTC(),
	})
	require.NoError(t, err)
	require.NoError(t, slaveConn.Write(dialContext, websocket.MessageText, syncPayload))

	require.Eventually(t, func() bool {
		return service.isNodeLiveAndSynced("node-yield-1")
	}, 2*time.Second, 20*time.Millisecond)

	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "coord-yield", SessionID: "sess-yield"}
	thread, _, err := service.CreateThread(caller, CreateThreadInput{Title: "Yield on block DAG"})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "sub-yield-dag",
		Tasks: []SubmittedTaskSpec{
			{
				ClientTaskID: "upstream",
				Title:        "Upstream task",
				Instructions: "Do upstream work",
				MaxAttempts:  1,
			},
			{
				ClientTaskID: "downstream-strict",
				Title:        "Downstream strict",
				Instructions: "Depends on upstream",
				Dependencies: []SubmittedDependency{{ClientTaskID: "upstream", Policy: "require-success"}},
			},
			{
				ClientTaskID: "downstream-lenient",
				Title:        "Downstream lenient",
				Instructions: "Allows upstream failure",
				Dependencies: []SubmittedDependency{{ClientTaskID: "upstream", Policy: "allow-failure"}},
			},
		},
	})
	require.NoError(t, err)
	upstreamID := submitted.ClientTaskMap["upstream"]
	strictID := submitted.ClientTaskMap["downstream-strict"]
	lenientID := submitted.ClientTaskMap["downstream-lenient"]

	const runID = "run-yield-1"
	const instanceID = "inst-yield-1"
	const allocationID = "alloc-yield-1"
	const leaseID = "lease-yield-1"
	const bindingID = "sbind-yield-1"
	const worktreePath = "/workspace/.orchestra/worktrees/lease-yield-1"

	// Put upstream task into running state with an active run, git-worktree lease, and warm session binding.
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		state.Instances[instanceID] = protocol.AgentInstance{
			ID:        instanceID,
			ThreadID:  thread.ID,
			Status:    "busy",
			Lease:     protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
			CreatedAt: nowText,
			UpdatedAt: nowText,
		}
		state.Allocations[allocationID] = protocol.InstanceAllocation{
			ID:         allocationID,
			InstanceID: instanceID,
			NodeID:     "node-yield-1",
			HarnessID:  protocol.HarnessCodexCLI,
			Model:      "default",
			Transport:  protocol.TransportACP,
			Workspace:  "/workspace/repo",
			Status:     "active",
			CreatedAt:  nowText,
			UpdatedAt:  nowText,
		}
		state.WorkspaceLeases[leaseID] = protocol.WorkspaceLease{
			ID:           leaseID,
			ThreadID:     thread.ID,
			TaskID:       upstreamID,
			RunID:        runID,
			NodeID:       "node-yield-1",
			Policy:       protocol.WorkspaceIsolationGitWorktree,
			Cleanup:      protocol.WorkspaceCleanupWhenUnchanged,
			Root:         "/workspace",
			SourcePath:   "/workspace/repo",
			BaseRevision: "refs/heads/main",
			Branch:       protocol.WorkspaceLeaseBranch(upstreamID, runID),
			WorktreePath: worktreePath,
			Status:       "active",
			CreatedAt:    nowText,
			UpdatedAt:    nowText,
		}
		state.SessionBindings[bindingID] = protocol.HarnessSessionBinding{
			ID:                bindingID,
			ThreadID:          thread.ID,
			InstanceID:        instanceID,
			AllocationID:      allocationID,
			NodeID:            "node-yield-1",
			HarnessID:         protocol.HarnessCodexCLI,
			Transport:         protocol.TransportACP,
			Workspace:         worktreePath,
			WorkspaceLeaseID:  leaseID,
			ProviderSessionID: "prov-sess-yield-1",
			Status:            "idle",
			CreatedByRunID:    runID,
			LastRunID:         runID,
			CreatedAt:         nowText,
			UpdatedAt:         nowText,
		}
		leaseIDCopy := leaseID
		upstreamIDCopy := upstreamID
		state.Runs[runID] = protocol.Run{
			ID:               runID,
			ThreadID:         thread.ID,
			InstanceID:       instanceID,
			AllocationID:     allocationID,
			NodeID:           "node-yield-1",
			HarnessID:        protocol.HarnessCodexCLI,
			Model:            "default",
			Workspace:        worktreePath,
			Status:           "running",
			Output:           "partial work output",
			TaskID:           &upstreamIDCopy,
			Attempt:          new(1),
			Transport:        protocol.TransportACP,
			WorkspaceLeaseID: &leaseIDCopy,
			CreatedAt:        nowText,
		}
		upstream := state.Tasks[upstreamID]
		upstream.Status = "running"
		upstream.AttemptCount = 1
		upstream.Requirements = protocol.ExecutionRequirements{
			Workspace: &protocol.WorkspaceRequirements{
				Path:      "/workspace/repo",
				Writable:  true,
				Isolation: protocol.WorkspaceIsolationGitWorktree,
			},
		}
		upstream.Assignment = &protocol.TaskAssignment{
			RunID:        runID,
			InstanceID:   instanceID,
			AllocationID: allocationID,
			NodeID:       "node-yield-1",
			Attempt:      1,
			AssignedAt:   nowText,
		}
		state.Tasks[upstreamID] = upstream
		return nil
	})

	// Worker yields on block with a secret in blockerReason.
	blockerKind := protocol.TaskBlockerKindNeedsInput
	blockerReason := "Need product decision for token=" + secretToken
	runCaller := CallerIdentity{
		Kind:       "run",
		RunID:      runID,
		TaskID:     upstreamID,
		ThreadID:   thread.ID,
		InstanceID: instanceID,
	}
	blockedTask, err := service.UpdateTask(runCaller, UpdateTaskInput{
		IdempotencyKey: "yield-block-1",
		Status:         "blocked",
		BlockerKind:    &blockerKind,
		BlockerReason:  &blockerReason,
	})
	require.NoError(t, err)
	assert.Equal(t, "blocked", blockedTask.Status)
	assert.Equal(t, protocol.TaskBlockerKindNeedsInput, blockedTask.BlockerKind)
	assert.NotContains(t, blockedTask.BlockerReason, secretToken, "blockerReason must be redacted")
	assert.Nil(t, blockedTask.Assignment, "assignment must be cleared on yield")
	assert.Equal(t, instanceID, blockedTask.PreferredInstanceID)
	assert.Equal(t, leaseID, blockedTask.RetainedLeaseID)
	assert.Equal(t, "partial work output", blockedTask.LastRunOutput)

	// Verify slave received cancel with Mode="yield".
	readCtx, cancelRead := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancelRead()
	var controlMsg protocol.MasterControlMessage
	for {
		_, rawMsg, readErr := slaveConn.Read(readCtx)
		require.NoError(t, readErr)
		var candidate protocol.MasterControlMessage
		require.NoError(t, json.Unmarshal(rawMsg, &candidate))
		if candidate.Type == "cancel" {
			controlMsg = candidate
			break
		}
	}
	assert.Equal(t, "cancel", controlMsg.Type)
	assert.Equal(t, runID, controlMsg.RunID)
	assert.Equal(t, "yield", controlMsg.Mode)

	// Verify active run settled, node concurrency released, instance idle, lease retained,
	// and downstream tasks stayed pending (non-destructive) with diagnostic referencing upstreamID.
	require.NoError(t, openedStore.Read(func(state *store.State) error {
		assert.Equal(t, 0, countActiveRunsOnNode(state, "node-yield-1"))
		assert.Equal(t, "cancelled", state.Runs[runID].Status)
		assert.Equal(t, "idle", state.Instances[instanceID].Status)

		lease := state.WorkspaceLeases[leaseID]
		assert.Equal(t, "retained", lease.Status)
		assert.Equal(t, "task-blocked", lease.RetentionReason)

		strictTask := state.Tasks[strictID]
		assert.Equal(t, "pending", strictTask.Status, "require-success dependent must remain pending while upstream is blocked")
		require.NotNil(t, strictTask.PlacementDiagnostic)
		require.Len(t, strictTask.PlacementDiagnostic.Unsatisfied, 1)
		assert.Equal(t, "dependency", strictTask.PlacementDiagnostic.Unsatisfied[0].Kind)
		assert.Equal(t, "dependency="+upstreamID, strictTask.PlacementDiagnostic.Unsatisfied[0].Requirement)
		assert.Contains(t, strictTask.PlacementDiagnostic.Unsatisfied[0].Detail, upstreamID)

		lenientTask := state.Tasks[lenientID]
		assert.Equal(t, "pending", lenientTask.Status, "allow-failure dependent must also wait while upstream is blocked")
		return nil
	}))

	// Late run.cancelled or run.failed from the yielded run must NOT overwrite the blocked task.
	service.handleRunCancelled("node-yield-1", protocol.SlaveControlMessage{RunID: runID})
	service.handleRunFailed("node-yield-1", protocol.SlaveControlMessage{RunID: runID, Error: new("late error")}, false)
	require.NoError(t, openedStore.Read(func(state *store.State) error {
		assert.Equal(t, "blocked", state.Tasks[upstreamID].Status)
		return nil
	}))

	// Unblock task with a resolution note containing a secret; verify redaction, idempotency, and lease/binding reuse.
	unblockNote := "Proceed with v2 schema, ignore key=" + secretToken
	unblockRes, err := service.UnblockTask(caller, UnblockTaskInput{
		TaskID:         upstreamID,
		IdempotencyKey: "unblock-key-1",
		Note:           unblockNote,
	})
	require.NoError(t, err)
	assert.False(t, unblockRes.Replayed)
	assert.Equal(t, "ready", unblockRes.Task.Status)
	assert.Empty(t, unblockRes.Task.BlockerKind)
	assert.Empty(t, unblockRes.Task.BlockerReason)
	assert.GreaterOrEqual(t, unblockRes.Task.MaxAttempts, 2, "unblocking must preserve attempt headroom")

	// Replay with identical key and note returns Replayed=true.
	replayRes, err := service.UnblockTask(caller, UnblockTaskInput{
		TaskID:         upstreamID,
		IdempotencyKey: "unblock-key-1",
		Note:           unblockNote,
	})
	require.NoError(t, err)
	assert.True(t, replayRes.Replayed)

	// Replay with same key and different note returns idempotency_conflict.
	_, conflictErr := service.UnblockTask(caller, UnblockTaskInput{
		TaskID:         upstreamID,
		IdempotencyKey: "unblock-key-1",
		Note:           "different note",
	})
	require.Error(t, conflictErr)
	var opErr *OperationError
	require.ErrorAs(t, conflictErr, &opErr)
	assert.Equal(t, "idempotency_conflict", opErr.Code)

	// Verify scheduler dispatches resumed run reusing the EXACT retained lease and warm session binding,
	// with redacted guidance note in the prompt.
	var dispatchMsg protocol.MasterControlMessage
	readDispatchCtx, cancelDispatch := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancelDispatch()
	for {
		_, payload, readErr := slaveConn.Read(readDispatchCtx)
		require.NoError(t, readErr)
		var msg protocol.MasterControlMessage
		require.NoError(t, json.Unmarshal(payload, &msg))
		if msg.Type == "dispatch" {
			dispatchMsg = msg
			break
		}
	}
	require.NotNil(t, dispatchMsg.Run)
	require.NotNil(t, dispatchMsg.WorkspaceLease)
	assert.Equal(t, leaseID, dispatchMsg.WorkspaceLease.ID, "resumed run must reuse retained git-worktree lease")
	assert.Equal(t, worktreePath, dispatchMsg.WorkspaceLease.WorktreePath)
	require.NotNil(t, dispatchMsg.SessionBinding)
	assert.Equal(t, bindingID, dispatchMsg.SessionBinding.ID, "resumed run must reuse warm session binding")
	assert.Contains(t, dispatchMsg.Run.Prompt, "Proceed with v2 schema")
	assert.NotContains(t, dispatchMsg.Run.Prompt, secretToken, "resolution note must be redacted in prompt")
	assert.Contains(t, dispatchMsg.Run.Prompt, "partial work output")

	// Complete the resumed run and verify downstream tasks become ready.
	resumedRunID := dispatchMsg.Run.ID
	service.handleRunCompleted("node-yield-1", protocol.SlaveControlMessage{
		RunID:  resumedRunID,
		Output: new("completed after unblock"),
	})
	require.NoError(t, openedStore.Read(func(state *store.State) error {
		assert.Equal(t, "completed", state.Tasks[upstreamID].Status)
		assert.Equal(t, "ready", state.Tasks[strictID].Status)
		assert.Nil(t, state.Tasks[strictID].PlacementDiagnostic)
		assert.Equal(t, "ready", state.Tasks[lenientID].Status)
		return nil
	}))
}

func TestReopenTask_RecoversFailedTaskAndDownstreamDependents(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{Store: openedStore, Token: "reopen-token"})
	require.NoError(t, err)
	defer service.Close()

	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "coord-reopen", SessionID: "sess-reopen"}
	thread, _, err := service.CreateThread(caller, CreateThreadInput{Title: "Reopen DAG"})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "sub-reopen-dag",
		Tasks: []SubmittedTaskSpec{
			{ClientTaskID: "up", Title: "Upstream", Instructions: "Work", MaxAttempts: 1},
			{
				ClientTaskID: "down",
				Title:        "Downstream",
				Instructions: "Depends on up",
				Dependencies: []SubmittedDependency{{ClientTaskID: "up", Policy: "require-success"}},
			},
		},
	})
	require.NoError(t, err)
	upID := submitted.ClientTaskMap["up"]
	downID := submitted.ClientTaskMap["down"]

	// Fail upstream via exhausted attempt.
	nowText := service.nowUTC()
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		up := state.Tasks[upID]
		up.Status = "running"
		up.AttemptCount = 1
		up.Assignment = &protocol.TaskAssignment{RunID: "run-fail-1", NodeID: "node-1", InstanceID: "inst-1", Attempt: 1, AssignedAt: nowText}
		state.Tasks[upID] = up
		state.Runs["run-fail-1"] = protocol.Run{
			ID: "run-fail-1", ThreadID: thread.ID, TaskID: &upID, NodeID: "node-1", InstanceID: "inst-1", Status: "running", CreatedAt: nowText,
		}
		return nil
	})
	service.handleRunFailed("node-1", protocol.SlaveControlMessage{RunID: "run-fail-1", Error: new("boom")}, true)

	// Confirm upstream is failed and downstream is auto-blocked with BlockerKind=dependency.
	require.NoError(t, openedStore.Read(func(state *store.State) error {
		assert.Equal(t, "failed", state.Tasks[upID].Status)
		assert.Equal(t, "blocked", state.Tasks[downID].Status)
		assert.Equal(t, protocol.TaskBlockerKindDependency, state.Tasks[downID].BlockerKind)
		return nil
	}))

	// Unblocking downstream while upstream is still failed must fail with dependency_failed.
	_, depErr := service.UnblockTask(caller, UnblockTaskInput{TaskID: downID})
	require.Error(t, depErr)
	var opErr *OperationError
	require.ErrorAs(t, depErr, &opErr)
	assert.Equal(t, "dependency_failed", opErr.Code)

	// Reopen upstream with fresh attempt budget.
	reopenRes, err := service.ReopenTask(caller, ReopenTaskInput{
		TaskID:             upID,
		IdempotencyKey:     "reopen-key-1",
		Note:               "Transient outage resolved",
		AdditionalAttempts: 2,
	})
	require.NoError(t, err)
	assert.False(t, reopenRes.Replayed)
	assert.Equal(t, "ready", reopenRes.Task.Status)
	assert.Empty(t, reopenRes.Task.FinishedAt)
	assert.Equal(t, 1, reopenRes.Task.AttemptCount)
	assert.Equal(t, 3, reopenRes.Task.MaxAttempts)

	// Downstream must automatically recover from dependency-blocked back to pending (waiting).
	require.NoError(t, openedStore.Read(func(state *store.State) error {
		down := state.Tasks[downID]
		assert.Equal(t, "pending", down.Status)
		assert.Empty(t, down.BlockerKind)
		assert.Empty(t, down.BlockerReason)
		return nil
	}))

	// Idempotent replay succeeds; conflicting payload fails.
	replayRes, err := service.ReopenTask(caller, ReopenTaskInput{
		TaskID:             upID,
		IdempotencyKey:     "reopen-key-1",
		Note:               "Transient outage resolved",
		AdditionalAttempts: 2,
	})
	require.NoError(t, err)
	assert.True(t, replayRes.Replayed)

	_, conflictErr := service.ReopenTask(caller, ReopenTaskInput{
		TaskID:             upID,
		IdempotencyKey:     "reopen-key-1",
		Note:               "Changed note",
		AdditionalAttempts: 2,
	})
	require.Error(t, conflictErr)
	require.ErrorAs(t, conflictErr, &opErr)
	assert.Equal(t, "idempotency_conflict", opErr.Code)
}

func TestTaskRecovery_AuthorizationAndRESTAndMCP(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	const bearerToken = "auth-recovery-token"
	service, err := NewService(ServiceOptions{Store: openedStore, Token: bearerToken})
	require.NoError(t, err)
	defer service.Close()

	coordCaller := CallerIdentity{Kind: "orchestrator-client", ClientID: "coord-auth", SessionID: "sess-auth"}
	threadA, _, err := service.CreateThread(coordCaller, CreateThreadInput{Title: "Thread A"})
	require.NoError(t, err)
	threadB, _, err := service.CreateThread(coordCaller, CreateThreadInput{Title: "Thread B"})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(coordCaller, SubmitTasksInput{
		ThreadID:       threadA.ID,
		IdempotencyKey: "sub-auth",
		Tasks: []SubmittedTaskSpec{
			{ClientTaskID: "parent", Title: "Parent", Instructions: "Parent task"},
			{ClientTaskID: "sibling", Title: "Sibling", Instructions: "Sibling task"},
		},
	})
	require.NoError(t, err)
	parentID := submitted.ClientTaskMap["parent"]
	siblingID := submitted.ClientTaskMap["sibling"]

	nowText := service.nowUTC()
	childID := protocol.NewIdentifier("task")
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		state.Tasks[childID] = protocol.Task{
			ID:             childID,
			ThreadID:       threadA.ID,
			ParentTaskID:   parentID,
			CreatedByRunID: "run-parent",
			Title:          "Delegated child",
			Instructions:   "Child work",
			Status:         "blocked",
			BlockerKind:    protocol.TaskBlockerKindAwaitingDecision,
			BlockerReason:  "Needs parent decision",
			MaxAttempts:    3,
			CreatedAt:      nowText,
			UpdatedAt:      nowText,
		}
		sib := state.Tasks[siblingID]
		sib.Status = "failed"
		sib.FinishedAt = nowText
		state.Tasks[siblingID] = sib
		return nil
	})

	tests := []struct {
		name      string
		caller    CallerIdentity
		action    string
		taskID    string
		expectErr string
	}{
		{
			name:      "non-delegating run cannot unblock",
			caller:    CallerIdentity{Kind: "run", RunID: "run-parent", TaskID: parentID, ThreadID: threadA.ID, CanDelegate: false},
			action:    "unblock",
			taskID:    childID,
			expectErr: "forbidden",
		},
		{
			name:      "delegating run in another thread cannot unblock",
			caller:    CallerIdentity{Kind: "run", RunID: "run-other", TaskID: parentID, ThreadID: threadB.ID, CanDelegate: true},
			action:    "unblock",
			taskID:    childID,
			expectErr: "forbidden",
		},
		{
			name:      "delegating sibling run cannot unblock unrelated child",
			caller:    CallerIdentity{Kind: "run", RunID: "run-sib", TaskID: siblingID, ThreadID: threadA.ID, CanDelegate: true},
			action:    "unblock",
			taskID:    childID,
			expectErr: "forbidden",
		},
		{
			name:      "delegating sibling run cannot reopen unrelated failed task",
			caller:    CallerIdentity{Kind: "run", RunID: "run-parent", TaskID: parentID, ThreadID: threadA.ID, CanDelegate: true},
			action:    "reopen",
			taskID:    siblingID,
			expectErr: "forbidden",
		},
		{
			name:      "delegating parent run can unblock its own child",
			caller:    CallerIdentity{Kind: "run", RunID: "run-parent", TaskID: parentID, ThreadID: threadA.ID, CanDelegate: true},
			action:    "unblock",
			taskID:    childID,
			expectErr: "",
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var callErr error
			if testCase.action == "unblock" {
				_, callErr = service.UnblockTask(testCase.caller, UnblockTaskInput{TaskID: testCase.taskID})
			} else {
				_, callErr = service.ReopenTask(testCase.caller, ReopenTaskInput{TaskID: testCase.taskID})
			}
			if testCase.expectErr == "" {
				require.NoError(t, callErr)
			} else {
				require.Error(t, callErr)
				var opErr *OperationError
				require.ErrorAs(t, callErr, &opErr)
				assert.Equal(t, testCase.expectErr, opErr.Code)
			}
		})
	}

	// Verify REST endpoints enforce bearer auth and execute reopen/unblock.
	httpServer := httptest.NewServer(service.Handler())
	defer httpServer.Close()

	unauthReq, err := http.NewRequestWithContext(t.Context(), http.MethodPost, httpServer.URL+"/api/tasks/"+siblingID+"/reopen", bytes.NewReader([]byte(`{"note":"retry"}`)))
	require.NoError(t, err)
	unauthResp, err := http.DefaultClient.Do(unauthReq)
	require.NoError(t, err)
	_ = unauthResp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, unauthResp.StatusCode)

	authReq, err := http.NewRequestWithContext(t.Context(), http.MethodPost, httpServer.URL+"/api/tasks/"+siblingID+"/reopen", bytes.NewReader([]byte(`{"note":"retry via api","additionalAttempts":2}`)))
	require.NoError(t, err)
	authReq.Header.Set("Authorization", "Bearer "+bearerToken)
	authReq.Header.Set("Content-Type", "application/json")
	authResp, err := http.DefaultClient.Do(authReq)
	require.NoError(t, err)
	defer authResp.Body.Close()
	assert.Equal(t, http.StatusOK, authResp.StatusCode)
	var apiResult ReopenTaskResult
	require.NoError(t, json.NewDecoder(authResp.Body).Decode(&apiResult))
	assert.Equal(t, "ready", apiResult.Task.Status)
	assert.Equal(t, 2, apiResult.Task.MaxAttempts)

	// Verify MCP tool dispatch for unblock_task.
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		task := state.Tasks[siblingID]
		task.Status = "blocked"
		task.BlockerKind = protocol.TaskBlockerKindExternalDependency
		task.BlockerReason = "Waiting on CI"
		state.Tasks[siblingID] = task
		return nil
	})
	rawArgs, err := json.Marshal(UnblockTaskInput{TaskID: siblingID, IdempotencyKey: "mcp-unblock-1", Note: "CI green"})
	require.NoError(t, err)
	rawRes, err := service.ExecuteOrchestratorTool(t.Context(), coordCaller, "unblock_task", rawArgs)
	require.NoError(t, err)
	var mcpResult UnblockTaskResult
	require.NoError(t, json.Unmarshal(rawRes, &mcpResult))
	assert.Equal(t, "ready", mcpResult.Task.Status)
	assert.False(t, mcpResult.Replayed)
}
