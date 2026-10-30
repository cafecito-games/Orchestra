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

	unauthorizedResponse, err := http.Get(httpServer.URL + "/api/status")
	require.NoError(t, err)
	unauthorizedResponse.Body.Close()
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
	duplicateConnection, _, err := websocket.Dial(context.Background(), wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer master-secret-token"}},
	})
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
	orchestratorConnection, _, err := websocket.Dial(context.Background(), orchestratorWSURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer master-secret-token"}},
	})
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

	putRequest, err := http.NewRequest(http.MethodPut, httpServer.URL+postedArtifact.Upload.Path, bytes.NewReader(artifactBytes))
	require.NoError(t, err)
	putResponse, err := http.DefaultClient.Do(putRequest)
	require.NoError(t, err)
	putResponse.Body.Close()
	require.Equal(t, http.StatusOK, putResponse.StatusCode)

	getRequest, err := http.NewRequest(http.MethodGet, httpServer.URL+postedArtifact.Artifact.DownloadPath, nil)
	require.NoError(t, err)
	getRequest.Header.Set("Authorization", "Bearer master-secret-token")
	getResponse, err := http.DefaultClient.Do(getRequest)
	require.NoError(t, err)
	downloaded, err := io.ReadAll(getResponse.Body)
	getResponse.Body.Close()
	require.NoError(t, err)
	require.Equal(t, artifactBytes, downloaded)

	mcpInitializeBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`
	mcpRequest, err := http.NewRequest(http.MethodPost, httpServer.URL+"/mcp", strings.NewReader(mcpInitializeBody))
	require.NoError(t, err)
	mcpRequest.Header.Set("Authorization", "Bearer master-secret-token")
	mcpResponse, err := http.DefaultClient.Do(mcpRequest)
	require.NoError(t, err)
	mcpSession := mcpResponse.Header.Get("Mcp-Session-Id")
	mcpResponse.Body.Close()
	require.NotEmpty(t, mcpSession)

	mcpListBody := `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`
	listRequest, err := http.NewRequest(http.MethodPost, httpServer.URL+"/mcp", strings.NewReader(mcpListBody))
	require.NoError(t, err)
	listRequest.Header.Set("Authorization", "Bearer master-secret-token")
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
