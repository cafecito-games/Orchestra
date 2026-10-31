package orchestrator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/slave"
	"github.com/cafecito-games/Orchestra/internal/store"
	"github.com/cafecito-games/Orchestra/internal/tailscaleauth"
	"github.com/cafecito-games/Orchestra/internal/workspace"
)

func TestNodeFencingTaskDAGSchedulerMailboxApprovalsArtifactsAndMCP(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store:            openedStore,
		Token:            "master-secret-token",
		DoorbellDebounce: 40 * time.Millisecond,
	})
	require.NoError(t, err)
	defer service.Close()

	httpServer := httptest.NewServer(service.Handler())
	defer httpServer.Close()

	unauthorizedRequest, err := http.NewRequestWithContext(t.Context(), http.MethodGet, httpServer.URL+"/api/status", nil)
	require.NoError(t, err)
	unauthorizedResponse, err := http.DefaultClient.Do(unauthorizedRequest)
	require.NoError(t, err)
	_ = unauthorizedResponse.Body.Close()
	require.Equal(t, http.StatusUnauthorized, unauthorizedResponse.StatusCode)

	rawWorkspace := t.TempDir()
	canonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{rawWorkspace})
	require.NoError(t, err)

	fakeClaude := filepath.Join(t.TempDir(), "claude")
	script := `#!/bin/sh
echo '{"type":"assistant","session_id":"sess-orch-1","message":{"content":[{"type":"text","text":"running step"}]}}'
echo '{"type":"result","result":"step succeeded"}'
`
	require.NoError(t, os.WriteFile(fakeClaude, []byte(script), 0o755))

	profiles := []protocol.HarnessProfile{{
		ID:          "claude-cli",
		Label:       "Claude Code",
		Description: "2.1.231",
		Binary:      fakeClaude,
		Available:   true,
		AuthMode:    "local-subscription",
		Models:      []string{"sonnet"},
		Transports:  []string{protocol.TransportNativeCLI},
	}}
	nowText := time.Now().UTC().Format(time.RFC3339Nano)
	capabilityReport := protocol.NodeCapabilityReport{
		NodeID: "worker-alpha",
		At:     nowText,
		Evidence: []protocol.NodeCapabilityEvidence{
			{CapabilityID: "os", Source: "runtime", Success: true, NormalizedValue: "darwin", ObservedAt: nowText},
			{CapabilityID: "architecture", Source: "runtime", Success: true, NormalizedValue: "arm64", ObservedAt: nowText},
			{CapabilityID: "workspace-writable", Source: "runtime", Success: true, NormalizedValue: "true", ObservedAt: nowText},
			{CapabilityID: "label:gpu", Source: "configured", Success: true, NormalizedValue: "gpu", ObservedAt: nowText},
			{CapabilityID: "toolchain:go", Source: "configured", Success: true, NormalizedValue: "1.26.2", ObservedAt: nowText},
		},
	}

	instanceCapacity := 4
	activeInstances := 0
	workerClient := slave.NewClient(slave.ClientOptions{
		Endpoint: httpServer.URL,
		Token:    "master-secret-token",
		Node: protocol.ComputeNode{
			ID:               "worker-alpha",
			Name:             "worker-alpha",
			Kind:             "local",
			Platform:         "darwin-arm64",
			Status:           "online",
			LastSeen:         nowText,
			Concurrency:      2,
			InstanceCapacity: &instanceCapacity,
			ActiveInstances:  &activeInstances,
			WorkspaceRoots:   canonicalRoots,
			Version:          "1.0.0",
			Harnesses:        profiles,
		},
		Runner:           harness.NewRunner(profiles),
		WorkspaceManager: workspace.NewManager(canonicalRoots, nil),
		CapabilityReport: &capabilityReport,
	})

	workerContext, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- workerClient.Run(workerContext)
	}()

	require.Eventually(t, func() bool {
		return service.isNodeLiveAndSynced("worker-alpha")
	}, 5*time.Second, 25*time.Millisecond)

	wsURL := strings.Replace(httpServer.URL, "http://", "ws://", 1) + "/control-agent"
	duplicateConnection, duplicateResponse, err := websocket.Dial(context.Background(), wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer master-secret-token"}},
	})
	if duplicateResponse != nil && duplicateResponse.Body != nil {
		_ = duplicateResponse.Body.Close()
	}
	require.NoError(t, err)
	registrationBytes, _ := json.Marshal(protocol.SlaveControlMessage{
		Type:            "register",
		ProtocolVersion: protocol.ProtocolVersion,
		Node: &protocol.ComputeNode{
			ID:             "worker-alpha",
			Name:           "worker-alpha",
			Kind:           "local",
			Platform:       "darwin-arm64",
			Status:         "online",
			LastSeen:       nowText,
			Concurrency:    1,
			WorkspaceRoots: canonicalRoots,
			Version:        "1.0.0",
		},
	})
	require.NoError(t, duplicateConnection.Write(context.Background(), websocket.MessageText, registrationBytes))
	_, _, duplicateReadErr := duplicateConnection.Read(context.Background())
	require.Error(t, duplicateReadErr)
	require.Equal(t, websocket.StatusPolicyViolation, websocket.CloseStatus(duplicateReadErr))
	require.True(t, service.isNodeLiveAndSynced("worker-alpha"))

	orchestratorWSURL := strings.Replace(httpServer.URL, "http://", "ws://", 1) + "/orchestrator-client"
	orchestratorConnection, orchestratorResponse, err := websocket.Dial(context.Background(), orchestratorWSURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer master-secret-token"}},
	})
	if orchestratorResponse != nil && orchestratorResponse.Body != nil {
		_ = orchestratorResponse.Body.Close()
	}
	require.NoError(t, err)
	defer orchestratorConnection.Close(websocket.StatusNormalClosure, "done")

	clientRegistrationBytes, _ := json.Marshal(protocol.OrchestratorClientMessage{
		Type:            "register",
		ProtocolVersion: protocol.ProtocolVersion,
		ClientID:        "agent-main",
		SessionID:       "session-main",
	})
	require.NoError(t, orchestratorConnection.Write(context.Background(), websocket.MessageText, clientRegistrationBytes))

	_, welcomePayload, err := orchestratorConnection.Read(context.Background())
	require.NoError(t, err)
	var welcome protocol.OrchestratorServerMessage
	require.NoError(t, json.Unmarshal(welcomePayload, &welcome))
	require.Equal(t, "welcome", welcome.Type)

	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "agent-main", SessionID: "session-main"}
	thread, attachment, err := service.CreateThread(caller, CreateThreadInput{
		Title:  "Distributed Build Pipeline",
		Attach: true,
	})
	require.NoError(t, err)
	require.NotNil(t, attachment)

	_, cycleErr := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "cycle-batch",
		Tasks: []SubmittedTaskSpec{
			{ClientTaskID: "a", Title: "A", Instructions: "Do A", Dependencies: []SubmittedDependency{{ClientTaskID: "b"}}},
			{ClientTaskID: "b", Title: "B", Instructions: "Do B", Dependencies: []SubmittedDependency{{ClientTaskID: "a"}}},
		},
	})
	require.Error(t, cycleErr)
	require.Contains(t, cycleErr.Error(), "cycle_detected")

	submitted, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "dag-batch-1",
		Tasks: []SubmittedTaskSpec{
			{
				ClientTaskID: "step-1",
				Title:        "Compile shaders",
				Instructions: "Compile GPU shaders",
				Requirements: protocol.ExecutionRequirements{
					Labels:     []string{"gpu"},
					Toolchains: map[string]string{"go": ">=1.26"},
				},
			},
			{
				ClientTaskID: "step-2",
				Title:        "Package bundle",
				Instructions: "Bundle compiled shaders",
				Dependencies: []SubmittedDependency{{ClientTaskID: "step-1", Policy: "require-success"}},
			},
		},
	})
	require.NoError(t, err)
	require.False(t, submitted.Replayed)
	require.Len(t, submitted.Tasks, 2)
	require.Equal(t, "ready", submitted.Tasks[0].Status)
	require.Equal(t, "pending", submitted.Tasks[1].Status)

	replayed, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "dag-batch-1",
		Tasks: []SubmittedTaskSpec{
			{
				ClientTaskID: "step-1",
				Title:        "Compile shaders",
				Instructions: "Compile GPU shaders",
				Requirements: protocol.ExecutionRequirements{
					Labels:     []string{"gpu"},
					Toolchains: map[string]string{"go": ">=1.26"},
				},
			},
			{
				ClientTaskID: "step-2",
				Title:        "Package bundle",
				Instructions: "Bundle compiled shaders",
				Dependencies: []SubmittedDependency{{ClientTaskID: "step-1", Policy: "require-success"}},
			},
		},
	})
	require.NoError(t, err)
	require.True(t, replayed.Replayed)

	step2ID := submitted.ClientTaskMap["step-2"]
	require.Eventually(t, func() bool {
		contextSnapshot, contextError := service.GetThreadContext(thread.ID)
		if contextError != nil {
			return false
		}
		for _, task := range contextSnapshot.Tasks {
			if task.ID == step2ID && task.Status == "completed" {
				return true
			}
		}
		return false
	}, 6*time.Second, 30*time.Millisecond)

	doorbellDeadline, cancelDoorbell := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelDoorbell()
	_, doorbellPayload, err := orchestratorConnection.Read(doorbellDeadline)
	require.NoError(t, err)
	var doorbellMessage protocol.OrchestratorServerMessage
	require.NoError(t, json.Unmarshal(doorbellPayload, &doorbellMessage))
	require.Equal(t, "doorbell", doorbellMessage.Type)
	require.NotNil(t, doorbellMessage.Doorbell)
	require.Equal(t, thread.ID, doorbellMessage.Doorbell.ThreadID)

	eventsResponse, err := service.GetThreadEvents(caller, GetThreadEventsInput{ThreadID: thread.ID})
	require.NoError(t, err)
	require.NotEmpty(t, eventsResponse.Events)
	require.NotEmpty(t, eventsResponse.NextCursor)

	artifactBytes := []byte("shader-binary-output")
	sum := sha256.Sum256(artifactBytes)
	digest := hex.EncodeToString(sum[:])

	postedArtifact, err := service.PostArtifact(caller, PostArtifactInput{
		ThreadID:  thread.ID,
		TaskID:    step2ID,
		Name:      "shaders.bin",
		MediaType: "application/octet-stream",
		SizeBytes: int64(len(artifactBytes)),
		SHA256:    digest,
	})
	require.NoError(t, err)

	putRequest, err := http.NewRequestWithContext(t.Context(), http.MethodPut, httpServer.URL+postedArtifact.Upload.Path, bytes.NewReader(artifactBytes))
	require.NoError(t, err)
	putResponse, err := http.DefaultClient.Do(putRequest)
	require.NoError(t, err)
	putResponse.Body.Close()
	require.Equal(t, http.StatusOK, putResponse.StatusCode)

	getRequest, err := http.NewRequestWithContext(t.Context(), http.MethodGet, httpServer.URL+postedArtifact.Artifact.DownloadPath, nil)
	require.NoError(t, err)
	getRequest.Header.Set("Authorization", "Bearer master-secret-token")
	getResponse, err := http.DefaultClient.Do(getRequest)
	require.NoError(t, err)
	downloaded, err := io.ReadAll(getResponse.Body)
	getResponse.Body.Close()
	require.NoError(t, err)
	require.Equal(t, artifactBytes, downloaded)

	mcpInitializeBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"orchestrator-test","version":"1.0.0"}}}`
	mcpRequest, err := http.NewRequestWithContext(t.Context(), http.MethodPost, httpServer.URL+"/mcp", strings.NewReader(mcpInitializeBody))
	require.NoError(t, err)
	mcpRequest.Header.Set("Authorization", "Bearer master-secret-token")
	mcpRequest.Header.Set("Content-Type", "application/json")
	mcpRequest.Header.Set("Accept", "application/json, text/event-stream")
	mcpResponse, err := http.DefaultClient.Do(mcpRequest)
	require.NoError(t, err)
	mcpSession := mcpResponse.Header.Get("Mcp-Session-Id")
	mcpResponse.Body.Close()
	require.NotEmpty(t, mcpSession)

	mcpListBody := `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`
	listRequest, err := http.NewRequestWithContext(t.Context(), http.MethodPost, httpServer.URL+"/mcp", strings.NewReader(mcpListBody))
	require.NoError(t, err)
	listRequest.Header.Set("Authorization", "Bearer master-secret-token")
	listRequest.Header.Set("Content-Type", "application/json")
	listRequest.Header.Set("Accept", "application/json, text/event-stream")
	listRequest.Header.Set("MCP-Protocol-Version", "2025-06-18")
	listRequest.Header.Set("Mcp-Session-Id", mcpSession)
	listResponse, err := http.DefaultClient.Do(listRequest)
	require.NoError(t, err)
	listPayload, err := io.ReadAll(listResponse.Body)
	listResponse.Body.Close()
	require.NoError(t, err)
	require.Contains(t, string(listPayload), "get_execution_inventory")
	require.Contains(t, string(listPayload), "resolve_approval")

	cancelWorker()
	require.NoError(t, <-workerDone)
}

func TestSchedulerNativeSessionBindingReuseWorkspaceAndTransportIsolation(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store:            openedStore,
		Token:            "master-secret-token",
		DoorbellDebounce: 25 * time.Millisecond,
	})
	require.NoError(t, err)
	defer service.Close()

	httpServer := httptest.NewServer(service.Handler())
	defer httpServer.Close()

	rawWorkspaceA := t.TempDir()
	rawWorkspaceB := t.TempDir()
	canonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{rawWorkspaceA, rawWorkspaceB})
	require.NoError(t, err)
	workspaceA := canonicalRoots[0]
	workspaceB := canonicalRoots[1]

	invocationsPath := filepath.Join(t.TempDir(), "invocations.log")
	fakeClaude := filepath.Join(t.TempDir(), "claude")
	script := `#!/bin/sh
printf '%s\n===INVOCATION===\n' "$*" >> "` + invocationsPath + `"
case "$*" in
  *"ROTATE_NATIVE_SESSION"*)
    echo '{"type":"assistant","session_id":"sess-native-rotated","message":{"content":[{"type":"text","text":"rotated turn"}]}}'
    echo '{"type":"result","result":"rotated completed"}'
    ;;
  *"WORKSPACE_B_TASK"*)
    echo '{"type":"assistant","session_id":"sess-workspace-b","message":{"content":[{"type":"text","text":"workspace b turn"}]}}'
    echo '{"type":"result","result":"workspace b completed"}'
    ;;
  *"FAIL_BEFORE_SESSION_EVENT"*)
    echo 'session resume rejected immediately' >&2
    exit 1
    ;;
  *)
    echo '{"type":"assistant","session_id":"sess-native-1","message":{"content":[{"type":"text","text":"standard turn"}]}}'
    echo '{"type":"result","result":"standard completed"}'
    ;;
esac
`
	require.NoError(t, os.WriteFile(fakeClaude, []byte(script), 0o755))

	profiles := []protocol.HarnessProfile{{
		ID:          protocol.HarnessClaudeCLI,
		Label:       "Claude Code",
		Description: "2.1.231",
		Binary:      fakeClaude,
		Available:   true,
		AuthMode:    "local-subscription",
		Models:      []string{"sonnet"},
		Transports:  []string{protocol.TransportNativeCLI},
	}}
	nowText := time.Now().UTC().Format(time.RFC3339Nano)
	instanceCapacity := 4
	activeInstances := 0
	workerClient := slave.NewClient(slave.ClientOptions{
		Endpoint: httpServer.URL,
		Token:    "master-secret-token",
		Node: protocol.ComputeNode{
			ID:               "worker-resume",
			Name:             "worker-resume",
			Kind:             "local",
			Platform:         "darwin-arm64",
			Status:           "online",
			LastSeen:         nowText,
			Concurrency:      2,
			InstanceCapacity: &instanceCapacity,
			ActiveInstances:  &activeInstances,
			WorkspaceRoots:   canonicalRoots,
			Version:          "1.0.0",
			Harnesses:        profiles,
		},
		Runner:           harness.NewRunner(profiles),
		WorkspaceManager: workspace.NewManager(canonicalRoots, nil),
	})

	workerContext, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- workerClient.Run(workerContext)
	}()

	require.Eventually(t, func() bool {
		return service.isNodeLiveAndSynced("worker-resume")
	}, 5*time.Second, 25*time.Millisecond)

	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "agent-main", SessionID: "session-main"}
	thread, _, err := service.CreateThread(caller, CreateThreadInput{
		Title:  "Native Session Resume Thread",
		Attach: true,
	})
	require.NoError(t, err)

	waitForTaskCompleted := func(taskID string) protocol.ThreadDetailResponse {
		t.Helper()
		var detail protocol.ThreadDetailResponse
		require.Eventually(t, func() bool {
			snapshot, snapshotErr := service.GetThreadDetail(thread.ID)
			if snapshotErr != nil {
				return false
			}
			for _, task := range snapshot.Tasks {
				if task.ID == taskID && task.Status == "completed" {
					detail = snapshot
					return true
				}
			}
			return false
		}, 6*time.Second, 25*time.Millisecond)
		return detail
	}

	batch1, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "resume-batch-1",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "task-1",
			Title:        "First native turn",
			Instructions: "FIRST_NATIVE_TURN",
			Requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{Path: workspaceA},
			},
		}},
	})
	require.NoError(t, err)
	task1ID := batch1.ClientTaskMap["task-1"]
	detail1 := waitForTaskCompleted(task1ID)

	var firstBinding protocol.HarnessSessionBinding
	require.Eventually(t, func() bool {
		snapshot, snapshotErr := service.GetThreadDetail(thread.ID)
		if snapshotErr != nil {
			return false
		}
		for _, binding := range snapshot.SessionBindings {
			if binding.ProviderSessionID == "sess-native-1" && binding.Status == "idle" {
				firstBinding = binding
				return true
			}
		}
		return false
	}, 3*time.Second, 20*time.Millisecond)
	require.Equal(t, protocol.TransportNativeCLI, firstBinding.Transport)
	require.Equal(t, workspaceA, firstBinding.Workspace)
	require.Len(t, detail1.Instances, 1)
	residentInstanceID := detail1.Instances[0].ID

	// Seed a synthetic idle ACP binding on the same resident instance/allocation to verify transport isolation:
	// the native-cli scheduler must ignore the acp-v1 binding and select firstBinding (native-cli).
	_, err = openedStore.Mutate(func(state *store.State, _ *store.Mutation) error {
		state.SessionBindings["sbind-synthetic-acp"] = protocol.HarnessSessionBinding{
			ID:                "sbind-synthetic-acp",
			ThreadID:          thread.ID,
			InstanceID:        firstBinding.InstanceID,
			AllocationID:      firstBinding.AllocationID,
			NodeID:            firstBinding.NodeID,
			HarnessID:         firstBinding.HarnessID,
			Transport:         protocol.TransportACP,
			Workspace:         workspaceA,
			ProviderSessionID: "sess-acp-never-use",
			Status:            "idle",
			CreatedByRunID:    firstBinding.CreatedByRunID,
			LastRunID:         firstBinding.LastRunID,
			CreatedAt:         "9999-12-31T23:59:59Z",
			UpdatedAt:         "9999-12-31T23:59:59Z",
		}
		return nil
	})
	require.NoError(t, err)

	batch2, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "resume-batch-2",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "task-2",
			Title:        "Second native turn",
			Instructions: "SECOND_NATIVE_TURN",
			InstanceID:   residentInstanceID,
			Requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{Path: workspaceA},
			},
		}},
	})
	require.NoError(t, err)
	task2ID := batch2.ClientTaskMap["task-2"]
	detail2 := waitForTaskCompleted(task2ID)

	var task2Run protocol.Run
	for _, runRecord := range detail2.Runs {
		if runRecord.TaskID != nil && *runRecord.TaskID == task2ID {
			task2Run = runRecord
			break
		}
	}
	require.NotNil(t, task2Run.SessionBindingID, "second task on same resident instance and workspace must reuse idle native-cli session binding")
	require.Equal(t, firstBinding.ID, *task2Run.SessionBindingID)

	readInvocations := func() []string {
		t.Helper()
		rawBytes, readErr := os.ReadFile(invocationsPath)
		require.NoError(t, readErr)
		trimmed := strings.TrimSuffix(string(rawBytes), "\n===INVOCATION===\n")
		if trimmed == "" {
			return nil
		}
		return strings.Split(trimmed, "\n===INVOCATION===\n")
	}

	invocationLines := readInvocations()
	require.Len(t, invocationLines, 2)
	require.NotContains(t, invocationLines[0], "--resume")
	require.Contains(t, invocationLines[1], "--resume sess-native-1")
	require.NotContains(t, invocationLines[1], "sess-acp-never-use")

	// Provider session rotation marks the previous binding replaced and records a new idle binding.
	batch3, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "resume-batch-3",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "task-3",
			Title:        "Rotated native turn",
			Instructions: "ROTATE_NATIVE_SESSION",
			InstanceID:   residentInstanceID,
			Requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{Path: workspaceA},
			},
		}},
	})
	require.NoError(t, err)
	task3ID := batch3.ClientTaskMap["task-3"]
	_ = waitForTaskCompleted(task3ID)

	require.Eventually(t, func() bool {
		snapshot, snapshotErr := service.GetThreadDetail(thread.ID)
		if snapshotErr != nil {
			return false
		}
		replacedFound := false
		rotatedIdleFound := false
		for _, binding := range snapshot.SessionBindings {
			if binding.ID == firstBinding.ID && binding.Status == "replaced" && binding.ReplacedByBindingID != "" {
				replacedFound = true
			}
			if binding.ProviderSessionID == "sess-native-rotated" && binding.Status == "idle" {
				rotatedIdleFound = true
			}
		}
		return replacedFound && rotatedIdleFound
	}, 3*time.Second, 20*time.Millisecond)

	// Workspace isolation: a task targeting workspaceB must not resume workspaceA's session binding.
	batch4, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "resume-batch-4",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "task-4",
			Title:        "Workspace B turn",
			Instructions: "WORKSPACE_B_TASK",
			Requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{Path: workspaceB},
			},
		}},
	})
	require.NoError(t, err)
	task4ID := batch4.ClientTaskMap["task-4"]
	_ = waitForTaskCompleted(task4ID)

	invocationLines = readInvocations()
	require.Len(t, invocationLines, 4)
	require.NotContains(t, invocationLines[3], "--resume")

	// Verify reverse transport isolation as well: querying acp-v1 on workspaceB (where only a native-cli binding exists) returns false.
	var workspaceBBindingID string
	var workspaceBInstanceID string
	err = openedStore.Read(func(state *store.State) error {
		for _, binding := range state.SessionBindings {
			if binding.Workspace == workspaceB && binding.Transport == protocol.TransportNativeCLI && binding.Status == "idle" {
				workspaceBBindingID = binding.ID
				workspaceBInstanceID = binding.InstanceID
				_, foundCross := findIdleSessionBinding(
					state,
					binding.InstanceID,
					binding.AllocationID,
					binding.HarnessID,
					protocol.TransportACP,
					workspaceB,
				)
				require.False(t, foundCross)
				return nil
			}
		}
		require.FailNow(t, "expected idle native-cli binding for workspaceB")
		return nil
	})
	require.NoError(t, err)

	// An immediate CLI resume failure before emitting any session event transitions the binding to failed,
	// so the next task on that resident instance starts a clean session without --resume.
	batch5, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "resume-batch-5",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "task-5",
			Title:        "Failing resumed turn",
			Instructions: "FAIL_BEFORE_SESSION_EVENT",
			MaxAttempts:  1,
			InstanceID:   workspaceBInstanceID,
			Requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{Path: workspaceB},
			},
		}},
	})
	require.NoError(t, err)
	task5ID := batch5.ClientTaskMap["task-5"]
	require.Eventually(t, func() bool {
		snapshot, snapshotErr := service.GetThreadDetail(thread.ID)
		if snapshotErr != nil {
			return false
		}
		for _, task := range snapshot.Tasks {
			if task.ID == task5ID && task.Status == "failed" {
				return true
			}
		}
		return false
	}, 6*time.Second, 25*time.Millisecond)

	require.Eventually(t, func() bool {
		snapshot, snapshotErr := service.GetThreadDetail(thread.ID)
		if snapshotErr != nil {
			return false
		}
		for _, binding := range snapshot.SessionBindings {
			if binding.ID == workspaceBBindingID && binding.Status == "failed" {
				return true
			}
		}
		return false
	}, 3*time.Second, 20*time.Millisecond)

	batch6, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "resume-batch-6",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "task-6",
			Title:        "Clean turn after failed resume",
			Instructions: "WORKSPACE_B_TASK",
			InstanceID:   workspaceBInstanceID,
			Requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{Path: workspaceB},
			},
		}},
	})
	require.NoError(t, err)
	task6ID := batch6.ClientTaskMap["task-6"]
	_ = waitForTaskCompleted(task6ID)

	invocationLines = readInvocations()
	require.Len(t, invocationLines, 6)
	require.Contains(t, invocationLines[4], "--resume sess-workspace-b")
	require.NotContains(t, invocationLines[5], "--resume")

	cancelWorker()
	require.NoError(t, <-workerDone)
}

func TestTailscaleAuthModeHTTPAndWebSocketEnforcement(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	verifier, err := tailscaleauth.NewVerifier(context.Background(), tailscaleauth.Options{
		CommandRunner: func(_ context.Context, _ string, arguments ...string) ([]byte, error) {
			if len(arguments) >= 2 && arguments[0] == "status" {
				return []byte(`{
					"MagicDNSSuffix": "tail999.ts.net",
					"Self": {
						"HostName": "main-mac",
						"DNSName": "main-mac.tail999.ts.net.",
						"UserID": 42,
						"TailscaleIPs": ["100.88.1.10"]
					},
					"User": {
						"42": {"ID": 42, "LoginName": "christian@example.com"}
					}
				}`), nil
			}
			if len(arguments) >= 3 && arguments[0] == "whois" {
				switch arguments[2] {
				case "100.88.1.25:51000":
					return []byte(`{
						"Node": {"Name": "worker.tail999.ts.net.", "Addresses": ["100.88.1.25/32"]},
						"UserProfile": {"ID": 42, "LoginName": "christian@example.com"}
					}`), nil
				case "100.88.1.40:52000":
					return []byte(`{
						"Node": {"Name": "guest.tail999.ts.net.", "Addresses": ["100.88.1.40/32"]},
						"UserProfile": {"ID": 99, "LoginName": "guest@example.com"}
					}`), nil
				}
			}
			return nil, io.EOF
		},
	})
	require.NoError(t, err)

	service, err := NewService(ServiceOptions{
		Store:             openedStore,
		TailscaleVerifier: verifier,
	})
	require.NoError(t, err)
	defer service.Close()

	handler := service.Handler()

	// Authorized remote tailnet peer succeeds with zero Authorization header.
	allowedRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://100.88.1.10:8787/api/status", nil)
	allowedRequest.RemoteAddr = "100.88.1.25:51000"
	allowedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(allowedRecorder, allowedRequest)
	require.Equal(t, http.StatusOK, allowedRecorder.Code)

	// Shared/other tailnet user is rejected with 401.
	guestRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://100.88.1.10:8787/api/status", nil)
	guestRequest.RemoteAddr = "100.88.1.40:52000"
	guestRecorder := httptest.NewRecorder()
	handler.ServeHTTP(guestRecorder, guestRequest)
	require.Equal(t, http.StatusUnauthorized, guestRecorder.Code)
	require.Contains(t, guestRecorder.Body.String(), "guest@example.com")

	// Browser Origin request is rejected even from Self/loopback across REST, /mcp, and WebSocket endpoints.
	for _, path := range []string{"/api/status", "/mcp", "/control-agent", "/orchestrator-client"} {
		browserRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://100.88.1.10:8787"+path, nil)
		browserRequest.RemoteAddr = "100.88.1.10:53000"
		browserRequest.Header.Set("Origin", "https://evil.example")
		browserRecorder := httptest.NewRecorder()
		handler.ServeHTTP(browserRecorder, browserRequest)
		require.Equal(t, http.StatusUnauthorized, browserRecorder.Code, "expected 401 on %s", path)
		require.Contains(t, browserRecorder.Body.String(), "browser-origin")
	}

	// Non-Tailscale LAN IP is rejected.
	lanRequest := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "http://100.88.1.10:8787/api/status", nil)
	lanRequest.RemoteAddr = "192.168.1.50:54000"
	lanRecorder := httptest.NewRecorder()
	handler.ServeHTTP(lanRecorder, lanRequest)
	require.Equal(t, http.StatusUnauthorized, lanRecorder.Code)
}
