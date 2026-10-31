package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

func assertOperationErrorCode(t *testing.T, err error, expectedCode string) {
	t.Helper()
	require.Error(t, err)
	var operationErr *OperationError
	require.ErrorAs(t, err, &operationErr)
	require.Equal(t, expectedCode, operationErr.Code)
}

func TestRoomBlackboardIsolationValidationRedactionAndIdempotency(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	orchestratorCaller := CallerIdentity{Kind: "orchestrator-client", ClientID: "orch-main", SessionID: "sess-1"}
	threadAlpha, _, err := service.CreateThread(orchestratorCaller, CreateThreadInput{Title: "Alpha thread"})
	require.NoError(t, err)
	threadBeta, _, err := service.CreateThread(orchestratorCaller, CreateThreadInput{Title: "Beta thread"})
	require.NoError(t, err)

	submittedAlpha, err := service.SubmitTasks(orchestratorCaller, SubmitTasksInput{
		ThreadID:       threadAlpha.ID,
		IdempotencyKey: "sub-alpha",
		Tasks: []SubmittedTaskSpec{
			{ClientTaskID: "parent", Title: "Parent", Instructions: "Coordinate"},
			{ClientTaskID: "sibling", Title: "Sibling", Instructions: "Implement"},
		},
	})
	require.NoError(t, err)
	parentTaskID := submittedAlpha.ClientTaskMap["parent"]
	siblingTaskID := submittedAlpha.ClientTaskMap["sibling"]

	submittedBeta, err := service.SubmitTasks(orchestratorCaller, SubmitTasksInput{
		ThreadID:       threadBeta.ID,
		IdempotencyKey: "sub-beta",
		Tasks: []SubmittedTaskSpec{
			{ClientTaskID: "other", Title: "Other thread task", Instructions: "Isolated"},
		},
	})
	require.NoError(t, err)
	otherTaskID := submittedBeta.ClientTaskMap["other"]

	nowText := service.nowUTC()
	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		state.Runs["run-parent"] = protocol.Run{
			ID: "run-parent", ThreadID: threadAlpha.ID, InstanceID: "inst-parent", NodeID: "node-1",
			Status: "running", CreatedAt: nowText, TaskID: &parentTaskID,
		}
		state.Runs["run-sibling"] = protocol.Run{
			ID: "run-sibling", ThreadID: threadAlpha.ID, InstanceID: "inst-sibling", NodeID: "node-1",
			Status: "running", CreatedAt: nowText, TaskID: &siblingTaskID,
		}
		state.Runs["run-other"] = protocol.Run{
			ID: "run-other", ThreadID: threadBeta.ID, InstanceID: "inst-other", NodeID: "node-1",
			Status: "running", CreatedAt: nowText, TaskID: &otherTaskID,
		}
		childTask := protocol.Task{
			ID:           "task-child",
			ThreadID:     threadAlpha.ID,
			ParentTaskID: parentTaskID,
			Title:        "Delegated child",
			Instructions: "Subtask",
			Status:       "running",
			MaxAttempts:  3,
			Origin:       protocol.TaskOrigin{Kind: "run", RunID: "run-parent", TaskID: parentTaskID},
			CreatedAt:    nowText,
			UpdatedAt:    nowText,
		}
		state.Tasks[childTask.ID] = childTask
		return nil
	})
	require.NoError(t, err)

	parentCaller := CallerIdentity{
		Kind: "run", RunID: "run-parent", TaskID: parentTaskID, ThreadID: threadAlpha.ID, InstanceID: "inst-parent", CanDelegate: true,
	}
	siblingCaller := CallerIdentity{
		Kind: "run", RunID: "run-sibling", TaskID: siblingTaskID, ThreadID: threadAlpha.ID, InstanceID: "inst-sibling", CanDelegate: false,
	}
	otherCaller := CallerIdentity{
		Kind: "run", RunID: "run-other", TaskID: otherTaskID, ThreadID: threadBeta.ID, InstanceID: "inst-other", CanDelegate: false,
	}

	invalidSequence := int64(1)
	_, err = service.PostRoomMessage(siblingCaller, PostRoomMessageInput{
		IdempotencyKey: "bad-kind",
		Kind:           "rumor",
		Body:           "invalid kind",
	})
	assertOperationErrorCode(t, err, "invalid_request")

	_, err = service.PostRoomMessage(siblingCaller, PostRoomMessageInput{
		IdempotencyKey:  "bad-reply",
		Kind:            "finding",
		Body:            "replies to future",
		ReplyToSequence: &invalidSequence,
	})
	assertOperationErrorCode(t, err, "invalid_request")

	_, err = service.PostRoomMessage(siblingCaller, PostRoomMessageInput{
		IdempotencyKey: "bad-artifact",
		Kind:           "finding",
		Body:           "unknown artifact",
		ArtifactIDs:    []string{"art-missing"},
	})
	assertOperationErrorCode(t, err, "invalid_request")

	_, err = service.PostRoomMessage(siblingCaller, PostRoomMessageInput{
		IdempotencyKey: "leaf-nudge",
		Kind:           "guidance",
		Body:           "try nudge",
		NudgeTaskIDs:   []string{parentTaskID},
	})
	assertOperationErrorCode(t, err, "forbidden")

	_, err = service.PostRoomMessage(parentCaller, PostRoomMessageInput{
		IdempotencyKey: "parent-nudge-sibling",
		Kind:           "guidance",
		Body:           "cannot nudge sibling",
		NudgeTaskIDs:   []string{siblingTaskID},
	})
	assertOperationErrorCode(t, err, "forbidden")

	postedOne, err := service.PostRoomMessage(siblingCaller, PostRoomMessageInput{
		IdempotencyKey: "post-1",
		Kind:           "contract",
		Topic:          "auth header master-secret-token",
		Body:           "Use X-Request-ID header; never leak master-secret-token",
	})
	require.NoError(t, err)
	require.False(t, postedOne.Replayed)
	require.Equal(t, int64(1), postedOne.Message.Sequence)
	require.True(t, strings.HasPrefix(postedOne.Message.ID, "rmsg_"))
	require.NotContains(t, postedOne.Message.Topic, "master-secret-token")
	require.NotContains(t, postedOne.Message.Body, "master-secret-token")

	siblingContext, err := service.GetTaskContext(siblingCaller)
	require.NoError(t, err)
	require.Equal(t, int64(1), siblingContext.Room.HeadSequence)
	require.Equal(t, int64(1), siblingContext.Room.LastReadSequence)
	require.False(t, siblingContext.Room.UnreadNudge)

	replayedOne, err := service.PostRoomMessage(siblingCaller, PostRoomMessageInput{
		IdempotencyKey: "post-1",
		Kind:           "contract",
		Topic:          "auth header master-secret-token",
		Body:           "Use X-Request-ID header; never leak master-secret-token",
	})
	require.NoError(t, err)
	require.True(t, replayedOne.Replayed)
	require.Equal(t, postedOne.Message.ID, replayedOne.Message.ID)
	require.Equal(t, int64(1), replayedOne.Message.Sequence)

	_, err = service.PostRoomMessage(siblingCaller, PostRoomMessageInput{
		IdempotencyKey: "post-1",
		Kind:           "contract",
		Topic:          "different",
		Body:           "different payload",
	})
	assertOperationErrorCode(t, err, "idempotency_conflict")

	parentRead, err := service.ReadRoom(parentCaller, ReadRoomInput{})
	require.NoError(t, err)
	require.Len(t, parentRead.Messages, 1)
	require.Equal(t, int64(1), parentRead.LastReadSequence)

	sequenceOne := int64(1)
	postedTwo, err := service.PostRoomMessage(parentCaller, PostRoomMessageInput{
		IdempotencyKey:  "post-2",
		Kind:            "guidance",
		Body:            "Follow contract #1 in child work",
		ReplyToSequence: &sequenceOne,
		NudgeTaskIDs:    []string{"task-child"},
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), postedTwo.Message.Sequence)
	require.Equal(t, []string{"task-child"}, postedTwo.NudgedTaskIDs)

	_, err = service.ReadRoom(otherCaller, ReadRoomInput{ThreadID: threadAlpha.ID})
	assertOperationErrorCode(t, err, "forbidden")
	otherOwnRead, err := service.ReadRoom(otherCaller, ReadRoomInput{})
	require.NoError(t, err)
	require.Empty(t, otherOwnRead.Messages)

	jsonlPath, exists, err := openedStore.RoomJSONLPath(threadAlpha.ID)
	require.NoError(t, err)
	require.True(t, exists)
	rawJSONL, err := os.ReadFile(jsonlPath)
	require.NoError(t, err)
	require.NotContains(t, string(rawJSONL), "master-secret-token")
	lines := strings.Split(strings.TrimSuffix(string(rawJSONL), "\n"), "\n")
	require.Len(t, lines, 2)
}

func TestRoomSafePointsWaitEventsUpdateTaskGateAndEndOfTurnFollowUp(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	orchestratorCaller := CallerIdentity{Kind: "orchestrator-client", ClientID: "orch-main", SessionID: "sess-1"}
	thread, _, err := service.CreateThread(orchestratorCaller, CreateThreadInput{Title: "Nudge safe points"})
	require.NoError(t, err)

	rawWorkspace := t.TempDir()

	nowText := service.nowUTC()
	instanceCapacity := 4
	activeInstances := 2
	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		state.Nodes["node-1"] = protocol.ComputeNode{
			ID:               "node-1",
			Name:             "node-1",
			Kind:             "local",
			Platform:         "darwin-arm64",
			Status:           "online",
			LastSeen:         nowText,
			Concurrency:      4,
			InstanceCapacity: &instanceCapacity,
			ActiveInstances:  &activeInstances,
			WorkspaceRoots:   []string{rawWorkspace},
			Version:          "1.0.0",
			Harnesses: []protocol.HarnessProfile{{
				ID:         "claude-cli",
				Label:      "Claude",
				Binary:     "/bin/echo",
				Available:  true,
				AuthMode:   "local-subscription",
				Models:     []string{"sonnet"},
				Transports: []string{"acp-v1", protocol.TransportNativeCLI},
			}},
		}
		state.CapabilityReports["node-1"] = protocol.NodeCapabilityReport{
			NodeID: "node-1",
			At:     nowText,
			Evidence: []protocol.NodeCapabilityEvidence{
				{CapabilityID: "os", Source: "runtime", Success: true, NormalizedValue: "darwin", ObservedAt: nowText},
				{CapabilityID: "architecture", Source: "runtime", Success: true, NormalizedValue: "arm64", ObservedAt: nowText},
				{CapabilityID: "workspace-writable", Source: "runtime", Success: true, NormalizedValue: "true", ObservedAt: nowText},
			},
		}
		return nil
	})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(orchestratorCaller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "sub-safe-points",
		Tasks: []SubmittedTaskSpec{
			{ClientTaskID: "gate-task", Title: "Completion Gate Task", Instructions: "Call update_task"},
			{ClientTaskID: "hold-session", Title: "Session Hold Task", Instructions: "Finish turn 1"},
			{ClientTaskID: "hold-fallback", Title: "Fallback Hold Task", Instructions: "Finish without session"},
		},
	})
	require.NoError(t, err)
	gateTaskID := submitted.ClientTaskMap["gate-task"]
	sessionTaskID := submitted.ClientTaskMap["hold-session"]
	fallbackTaskID := submitted.ClientTaskMap["hold-fallback"]

	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		attempt := 1
		state.Instances["inst-gate"] = protocol.AgentInstance{
			ID: "inst-gate", ThreadID: thread.ID, Status: "busy", CreatedAt: nowText, UpdatedAt: nowText,
			Lease: protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
		}
		state.Allocations["alloc-gate"] = protocol.InstanceAllocation{
			ID: "alloc-gate", InstanceID: "inst-gate", NodeID: "node-1", HarnessID: "claude-cli", Model: "sonnet",
			Transport: "acp-v1", Workspace: rawWorkspace, Status: "active", CreatedAt: nowText, UpdatedAt: nowText,
			Lease: protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
		}
		state.Runs["run-gate"] = protocol.Run{
			ID: "run-gate", ThreadID: thread.ID, InstanceID: "inst-gate", AllocationID: "alloc-gate", NodeID: "node-1",
			HarnessID: "claude-cli", Model: "sonnet", Transport: "acp-v1", Workspace: rawWorkspace,
			Status: "running", CreatedAt: nowText, TaskID: &gateTaskID, Attempt: &attempt,
		}
		gateTask := state.Tasks[gateTaskID]
		gateTask.Status = "running"
		gateTask.AttemptCount = 1
		gateTask.Assignment = &protocol.TaskAssignment{
			RunID: "run-gate", InstanceID: "inst-gate", AllocationID: "alloc-gate", NodeID: "node-1", Attempt: 1, AssignedAt: nowText,
		}
		state.Tasks[gateTaskID] = gateTask

		state.Instances["inst-session"] = protocol.AgentInstance{
			ID: "inst-session", ThreadID: thread.ID, Status: "busy", AutoManaged: true, CreatedAt: nowText, UpdatedAt: nowText,
			Lease: protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
		}
		state.Allocations["alloc-session"] = protocol.InstanceAllocation{
			ID: "alloc-session", InstanceID: "inst-session", NodeID: "node-1", HarnessID: "claude-cli", Model: "sonnet",
			Transport: "acp-v1", Workspace: rawWorkspace, Status: "active", CreatedAt: nowText, UpdatedAt: nowText,
			Lease: protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
		}
		state.SessionBindings["bind-session"] = protocol.HarnessSessionBinding{
			ID: "bind-session", ThreadID: thread.ID, InstanceID: "inst-session", AllocationID: "alloc-session",
			NodeID: "node-1", HarnessID: "claude-cli", Transport: "acp-v1", Workspace: rawWorkspace,
			ProviderSessionID: "prov-sess-42", Status: "idle", CreatedByRunID: "run-session-1", LastRunID: "run-session-1",
			CreatedAt: nowText, UpdatedAt: nowText,
		}
		state.Runs["run-session-1"] = protocol.Run{
			ID: "run-session-1", ThreadID: thread.ID, InstanceID: "inst-session", AllocationID: "alloc-session", NodeID: "node-1",
			HarnessID: "claude-cli", Model: "sonnet", Transport: "acp-v1", Workspace: rawWorkspace,
			Status: "running", CreatedAt: nowText, TaskID: &sessionTaskID, Attempt: &attempt,
		}
		sessionTask := state.Tasks[sessionTaskID]
		sessionTask.Status = "running"
		sessionTask.AttemptCount = 1
		sessionTask.Assignment = &protocol.TaskAssignment{
			RunID: "run-session-1", InstanceID: "inst-session", AllocationID: "alloc-session", NodeID: "node-1", Attempt: 1, AssignedAt: nowText,
		}
		state.Tasks[sessionTaskID] = sessionTask

		state.Instances["inst-fallback"] = protocol.AgentInstance{
			ID: "inst-fallback", ThreadID: thread.ID, Status: "busy", AutoManaged: true, CreatedAt: nowText, UpdatedAt: nowText,
			Lease: protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
		}
		state.Allocations["alloc-fallback"] = protocol.InstanceAllocation{
			ID: "alloc-fallback", InstanceID: "inst-fallback", NodeID: "node-1", HarnessID: "claude-cli", Model: "sonnet",
			Transport: protocol.TransportNativeCLI, Workspace: rawWorkspace, Status: "active", CreatedAt: nowText, UpdatedAt: nowText,
			Lease: protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
		}
		state.Runs["run-fallback-1"] = protocol.Run{
			ID: "run-fallback-1", ThreadID: thread.ID, InstanceID: "inst-fallback", AllocationID: "alloc-fallback", NodeID: "node-1",
			HarnessID: "claude-cli", Model: "sonnet", Transport: protocol.TransportNativeCLI, Workspace: rawWorkspace,
			Status: "running", CreatedAt: nowText, TaskID: &fallbackTaskID, Attempt: &attempt,
		}
		fallbackTask := state.Tasks[fallbackTaskID]
		fallbackTask.Status = "running"
		fallbackTask.AttemptCount = 1
		fallbackTask.Assignment = &protocol.TaskAssignment{
			RunID: "run-fallback-1", InstanceID: "inst-fallback", AllocationID: "alloc-fallback", NodeID: "node-1", Attempt: 1, AssignedAt: nowText,
		}
		state.Tasks[fallbackTaskID] = fallbackTask
		return nil
	})
	require.NoError(t, err)

	gateCaller := CallerIdentity{Kind: "run", RunID: "run-gate", TaskID: gateTaskID, ThreadID: thread.ID, InstanceID: "inst-gate"}

	initialWait, err := service.WaitForTaskEvents(context.Background(), gateCaller, WaitForTaskEventsInput{TimeoutMilliseconds: 0})
	require.NoError(t, err)
	waitDone := make(chan ThreadEventsResponse, 1)
	go func() {
		waited, waitErr := service.WaitForTaskEvents(context.Background(), gateCaller, WaitForTaskEventsInput{
			Cursor:              initialWait.Cursor,
			TimeoutMilliseconds: 3000,
		})
		if waitErr == nil {
			waitDone <- waited
		}
	}()

	time.Sleep(30 * time.Millisecond)
	_, err = service.PostRoomMessage(orchestratorCaller, PostRoomMessageInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "orch-guidance-1",
		Kind:           "guidance",
		Topic:          "stop-and-align",
		Body:           "Switch output format to YAML before completing",
		Nudge:          true,
	})
	require.NoError(t, err)

	select {
	case waited := <-waitDone:
		foundNudge := false
		for _, event := range waited.Events {
			if event.Type == "room.nudged" && event.TaskID == gateTaskID {
				foundNudge = true
			}
		}
		require.True(t, foundNudge, "expected wait_for_task_events to surface room.nudged")
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for room.nudged event")
	}

	_, err = service.UpdateTask(gateCaller, UpdateTaskInput{
		IdempotencyKey: "complete-attempt-1",
		Completion: &TaskCompletionInput{
			Summary: "Completed without reading room",
		},
	})
	assertOperationErrorCode(t, err, "unread_room_nudge")

	readResult, err := service.ReadRoom(gateCaller, ReadRoomInput{})
	require.NoError(t, err)
	require.Equal(t, int64(1), readResult.LastReadSequence)
	require.False(t, readResult.UnreadNudge)

	completedGateTask, err := service.UpdateTask(gateCaller, UpdateTaskInput{
		IdempotencyKey: "complete-attempt-2",
		Completion: &TaskCompletionInput{
			Summary: "Completed in YAML after reading room guidance",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "Completed in YAML after reading room guidance", completedGateTask.Summary)

	turnOneOutput := "initial draft output before reading room"
	service.handleRunCompleted("node-1", protocol.SlaveControlMessage{
		Type:   "run.completed",
		RunID:  "run-session-1",
		Output: &turnOneOutput,
	})

	var dispatchedSessionMessage protocol.MasterControlMessage
	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		task := state.Tasks[sessionTaskID]
		require.Equal(t, "ready", task.Status)
		require.Equal(t, int64(1), task.NudgedAtRoomSequence)
		require.Equal(t, "inst-session", task.PreferredInstanceID)

		effectiveRequirements := mergeProjectProfileRequirements(state, task.Requirements)
		instance := state.Instances["inst-session"]
		allocation := state.Allocations["alloc-session"]
		dispatchedSessionMessage = service.assignTaskToResident(state, &task, &instance, &allocation, effectiveRequirements, service.nowUTC())
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, dispatchedSessionMessage.SessionBinding)
	require.Equal(t, "prov-sess-42", dispatchedSessionMessage.SessionBinding.ProviderSessionID)
	require.Contains(t, dispatchedSessionMessage.SessionBinding.ResumePrompt, "read_room")

	followUpSessionCaller := CallerIdentity{
		Kind: "run", RunID: dispatchedSessionMessage.RunID, TaskID: sessionTaskID, ThreadID: thread.ID, InstanceID: "inst-session",
	}
	_, err = service.ReadRoom(followUpSessionCaller, ReadRoomInput{})
	require.NoError(t, err)
	followUpOutput := "reconciled output after reading room"
	service.handleRunCompleted("node-1", protocol.SlaveControlMessage{
		Type:   "run.completed",
		RunID:  dispatchedSessionMessage.RunID,
		Output: &followUpOutput,
	})
	require.NoError(t, openedStore.Read(func(state *store.State) error {
		require.Equal(t, "completed", state.Tasks[sessionTaskID].Status)
		return nil
	}))

	fallbackTurnOneOutput := "fallback draft output"
	service.handleRunCompleted("node-1", protocol.SlaveControlMessage{
		Type:   "run.completed",
		RunID:  "run-fallback-1",
		Output: &fallbackTurnOneOutput,
	})

	var dispatchedFallbackMessage protocol.MasterControlMessage
	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		task := state.Tasks[fallbackTaskID]
		require.Equal(t, "ready", task.Status)
		require.Equal(t, int64(1), task.NudgedAtRoomSequence)

		effectiveRequirements := mergeProjectProfileRequirements(state, task.Requirements)
		instance := state.Instances["inst-fallback"]
		allocation := state.Allocations["alloc-fallback"]
		dispatchedFallbackMessage = service.assignTaskToResident(state, &task, &instance, &allocation, effectiveRequirements, service.nowUTC())
		return nil
	})
	require.NoError(t, err)
	require.Nil(t, dispatchedFallbackMessage.SessionBinding)
	require.Contains(t, dispatchedFallbackMessage.Run.Prompt, "fallback draft output")
	require.Contains(t, dispatchedFallbackMessage.Run.Prompt, "read_room")

	ignoredOutput := "finished follow-up without calling read_room"
	service.handleRunCompleted("node-1", protocol.SlaveControlMessage{
		Type:   "run.completed",
		RunID:  dispatchedFallbackMessage.RunID,
		Output: &ignoredOutput,
	})
	require.NoError(t, openedStore.Read(func(state *store.State) error {
		require.Equal(t, "completed", state.Tasks[fallbackTaskID].Status)
		return nil
	}))

	httpServer := httptest.NewServer(service.Handler())
	defer httpServer.Close()

	unauthorizedResponse, err := http.Get(httpServer.URL + "/api/threads/" + thread.ID + "/room")
	require.NoError(t, err)
	unauthorizedResponse.Body.Close()
	require.Equal(t, http.StatusUnauthorized, unauthorizedResponse.StatusCode)

	postBody, _ := json.Marshal(map[string]any{
		"idempotencyKey": "http-op-post-1",
		"kind":           "decision",
		"topic":          "operator-note",
		"body":           "Ship YAML v2 schema",
	})
	postRequest, err := http.NewRequest(http.MethodPost, httpServer.URL+"/api/threads/"+thread.ID+"/room/messages", bytes.NewReader(postBody))
	require.NoError(t, err)
	postRequest.Header.Set("Authorization", "Bearer master-secret-token")
	postRequest.Header.Set("Content-Type", "application/json")
	postResponse, err := http.DefaultClient.Do(postRequest)
	require.NoError(t, err)
	defer postResponse.Body.Close()
	require.Equal(t, http.StatusCreated, postResponse.StatusCode)

	getRequest, err := http.NewRequest(http.MethodGet, httpServer.URL+"/api/threads/"+thread.ID+"/room?after=1&kind=decision", nil)
	require.NoError(t, err)
	getRequest.Header.Set("Authorization", "Bearer master-secret-token")
	getResponse, err := http.DefaultClient.Do(getRequest)
	require.NoError(t, err)
	defer getResponse.Body.Close()
	require.Equal(t, http.StatusOK, getResponse.StatusCode)
	var roomHTTPPayload struct {
		Path     string                 `json:"path"`
		Messages []protocol.RoomMessage `json:"messages"`
	}
	require.NoError(t, json.NewDecoder(getResponse.Body).Decode(&roomHTTPPayload))
	require.NotEmpty(t, roomHTTPPayload.Path)
	require.Len(t, roomHTTPPayload.Messages, 1)
	require.Equal(t, int64(2), roomHTTPPayload.Messages[0].Sequence)
}
