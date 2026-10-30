package slave

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/workspace"
)

func TestParseConfigurationFromFlagsAndEnvironment(t *testing.T) {
	workspaceRoot := t.TempDir()
	dataRoot := filepath.Join(t.TempDir(), "data")
	environmentValues := map[string]string{
		"ORCHESTRA_ENDPOINT":    "http://100.64.0.1:8787",
		"ORCHESTRA_NAME":        "Studio Mac",
		"ORCHESTRA_TOKEN":       "secret-bearer-token",
		"WORKSPACE_ROOTS":       workspaceRoot,
		"ORCHESTRA_LABELS":      "macos,apple-silicon",
		"ORCHESTRA_TOOLCHAINS":  "go=1.26.2",
		"ORCHESTRA_DATA_ROOT":   dataRoot,
		"ORCHESTRA_CONCURRENCY": "4",
	}
	configuration, err := ParseConfiguration(
		[]string{"--approval-policy", "claude-cli=auto", "--instance-capacity", "6"},
		func(name string) (string, bool) {
			value, exists := environmentValues[name]
			return value, exists
		},
	)
	require.NoError(t, err)
	require.Equal(t, "http://100.64.0.1:8787", configuration.ControlEndpoint)
	require.Equal(t, "studio-mac", configuration.NodeID)
	require.Equal(t, "secret-bearer-token", configuration.Token)
	require.Equal(t, 4, configuration.Concurrency)
	require.Equal(t, 6, configuration.InstanceCapacity)
	require.Len(t, configuration.WorkspaceRoots, 1)
	require.Equal(t, []string{"macos", "apple-silicon"}, configuration.Labels)
	require.Equal(t, "auto", configuration.ApprovalPolicies.For("claude-cli"))
	require.Equal(t, "manual", configuration.ApprovalPolicies.For("codex-cli"))
}

func TestSlaveClientRegistrationInstanceLifecycleLeaseAndHubRPC(t *testing.T) {
	temporaryDirectory := t.TempDir()
	canonicalRoots, err := CanonicalizeWorkspaceRoots([]string{temporaryDirectory})
	require.NoError(t, err)
	workspaceRoot := canonicalRoots[0]

	fakeClaude := filepath.Join(t.TempDir(), "claude")
	script := `#!/bin/sh
echo '{"type":"assistant","session_id":"sess-42","message":{"content":[{"type":"text","text":"working on task"}]}}'
echo '{"type":"result","result":"completed cleanly"}'
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
	runner := harness.NewRunner(profiles)
	workspaceManager := workspace.NewManager(canonicalRoots, nil)

	var serverMutex sync.Mutex
	var receivedMessages []protocol.SlaveControlMessage
	var activeSocket *websocket.Conn
	admitConnection := false
	uploadedArtifacts := map[string][]byte{}

	messageSignal := make(chan protocol.SlaveControlMessage, 64)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/artifacts/art-1/content", func(writer http.ResponseWriter, request *http.Request) {
		require.Equal(t, "Bearer test-token", request.Header.Get("Authorization"))
		bodyBytes, readErr := io.ReadAll(request.Body)
		require.NoError(t, readErr)
		serverMutex.Lock()
		uploadedArtifacts["art-1"] = bodyBytes
		serverMutex.Unlock()
		writer.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/control-agent", func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		connection, acceptErr := websocket.Accept(writer, request, nil)
		if acceptErr != nil {
			return
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")

		serverMutex.Lock()
		shouldAdmit := admitConnection
		if !shouldAdmit {
			admitConnection = true
		} else {
			activeSocket = connection
		}
		serverMutex.Unlock()

		for {
			_, payload, readErr := connection.Read(request.Context())
			if readErr != nil {
				return
			}
			var message protocol.SlaveControlMessage
			if err := json.Unmarshal(payload, &message); err != nil {
				continue
			}
			if message.Type == "register" && !shouldAdmit {
				_ = connection.Close(websocket.StatusPolicyViolation, "node already connected")
				return
			}
			serverMutex.Lock()
			receivedMessages = append(receivedMessages, message)
			serverMutex.Unlock()
			messageSignal <- message

			switch message.Type {
			case "register":
				pingBytes, _ := json.Marshal(protocol.MasterControlMessage{
					Type: "ping",
					At:   time.Now().UTC().Format(time.RFC3339Nano),
				})
				_ = connection.Write(request.Context(), websocket.MessageText, pingBytes)
			case "workspace.lease":
				if message.Lease != nil && message.Lease.Status == "active" {
					confirmBytes, _ := json.Marshal(protocol.MasterControlMessage{
						Type:    "workspace.lease.confirmed",
						LeaseID: message.Lease.LeaseID,
						Status:  "active",
					})
					_ = connection.Write(request.Context(), websocket.MessageText, confirmBytes)
				}
			case "hub.rpc.request":
				var response protocol.MasterControlMessage
				if message.Method == "get_task_context" {
					response = protocol.MasterControlMessage{
						Type:      "hub.rpc.response",
						RequestID: message.RequestID,
						Result:    json.RawMessage(`{"task":{"id":"task-1"}}`),
					}
				} else if message.Method == "post_artifact" {
					response = protocol.MasterControlMessage{
						Type:      "hub.rpc.response",
						RequestID: message.RequestID,
						Result:    json.RawMessage(`{"artifact":{"id":"art-1"},"upload":{"path":"/api/artifacts/art-1/content","token":"u-token","expiresAt":"2030-01-01T00:00:00Z"}}`),
					}
				}
				responseBytes, _ := json.Marshal(response)
				_ = connection.Write(request.Context(), websocket.MessageText, responseBytes)
			}
		}
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	instanceCapacity := 4
	activeInstances := 0
	client := NewClient(ClientOptions{
		Endpoint:          server.URL,
		Token:             "test-token",
		ReconnectMinDelay: 25 * time.Millisecond,
		ReconnectMaxDelay: 50 * time.Millisecond,
		Node: protocol.ComputeNode{
			ID:               "worker-1",
			Name:             "worker-1",
			Kind:             "local",
			Platform:         "darwin-arm64",
			Status:           "online",
			LastSeen:         time.Now().UTC().Format(time.RFC3339Nano),
			Concurrency:      2,
			InstanceCapacity: &instanceCapacity,
			ActiveInstances:  &activeInstances,
			WorkspaceRoots:   canonicalRoots,
			Version:          "1.0.0",
			Harnesses:        profiles,
		},
		Runner:           runner,
		WorkspaceManager: workspaceManager,
	})

	queuedText := "buffered-before-admission"
	client.sendOrQueue(protocol.SlaveControlMessage{
		Type:  "run.output",
		RunID: "run-buffered",
		Chunk: &queuedText,
	})

	runContext, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	done := make(chan error, 1)
	go func() {
		done <- client.Run(runContext)
	}()

	waitForType := func(expectedType string) protocol.SlaveControlMessage {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case message := <-messageSignal:
				if message.Type == expectedType {
					return message
				}
			case <-deadline:
				t.Fatalf("timed out waiting for message type %q", expectedType)
			}
		}
	}

	_ = waitForType("register")
	bufferedOut := waitForType("run.output")
	require.Equal(t, "run-buffered", bufferedOut.RunID)
	_ = waitForType("sync.complete")

	sendMaster := func(message protocol.MasterControlMessage) {
		t.Helper()
		serverMutex.Lock()
		socket := activeSocket
		serverMutex.Unlock()
		require.NotNil(t, socket)
		encoded, marshalErr := json.Marshal(message)
		require.NoError(t, marshalErr)
		require.NoError(t, socket.Write(context.Background(), websocket.MessageText, encoded))
	}

	nowText := time.Now().UTC().Format(time.RFC3339Nano)
	instance := protocol.AgentInstance{
		ID:         "inst-1",
		ThreadID:   "thread-1",
		Creator:    protocol.InstanceCreator{Kind: "orchestrator-client", ClientID: "client-1"},
		Delegation: protocol.InstanceDelegationPolicy{CanDelegate: true},
		Lease:      protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: nowText},
		Status:     "provisioning",
		CreatedAt:  nowText,
		UpdatedAt:  nowText,
	}
	allocation := protocol.InstanceAllocation{
		ID:         "alloc-1",
		InstanceID: "inst-1",
		NodeID:     "worker-1",
		HarnessID:  "claude-cli",
		Model:      "sonnet",
		Transport:  protocol.TransportNativeCLI,
		Workspace:  workspaceRoot,
		Lease:      protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: nowText},
		Status:     "provisioning",
		CreatedAt:  nowText,
		UpdatedAt:  nowText,
	}
	sendMaster(protocol.MasterControlMessage{
		Type:       "instance.provision",
		Instance:   &instance,
		Allocation: &allocation,
	})
	readyMessage := waitForType("instance.ready")
	require.Equal(t, "inst-1", readyMessage.InstanceID)
	require.Equal(t, "alloc-1", readyMessage.AllocationID)

	artifactPath := filepath.Join(workspaceRoot, "report.txt")
	require.NoError(t, os.WriteFile(artifactPath, []byte("hello-artifact"), 0o600))
	mcpGrant, err := client.mcpServer.Grant("run-rpc", workspaceRoot, true)
	require.NoError(t, err)
	rpcPayload := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_task_context","arguments":{}}}`
	httpRequest, err := http.NewRequest(http.MethodPost, mcpGrant.URL, strings.NewReader(rpcPayload))
	require.NoError(t, err)
	httpRequest.Header.Set("Authorization", "Bearer "+mcpGrant.Token)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json, text/event-stream")
	httpRequest.Header.Set("MCP-Protocol-Version", "2025-06-18")
	httpResponse, err := http.DefaultClient.Do(httpRequest)
	require.NoError(t, err)
	bodyBytes, err := io.ReadAll(httpResponse.Body)
	httpResponse.Body.Close()
	require.NoError(t, err)
	require.Contains(t, string(bodyBytes), "task-1")

	uploadErr := client.uploadArtifactContent(context.Background(), "/api/artifacts/art-1/content", bytes.NewReader([]byte("hello-artifact")), int64(len("hello-artifact")))
	require.NoError(t, uploadErr)
	serverMutex.Lock()
	require.Equal(t, []byte("hello-artifact"), uploadedArtifacts["art-1"])
	serverMutex.Unlock()
	client.mcpServer.Revoke(mcpGrant.Token)

	taskID := "task-1"
	leaseGrant := protocol.WorkspaceLeaseGrant{
		ID:           "lease-1",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationExclusiveExisting,
		Cleanup:      protocol.WorkspaceCleanupWhenUnchanged,
		Root:         workspaceRoot,
		SourcePath:   workspaceRoot,
		WorktreePath: workspaceRoot,
	}
	sendMaster(protocol.MasterControlMessage{
		Type: "dispatch",
		Run: &protocol.Run{
			ID:           "run-1",
			ThreadID:     "thread-1",
			InstanceID:   "inst-1",
			AllocationID: "alloc-1",
			NodeID:       "worker-1",
			HarnessID:    "claude-cli",
			Model:        "sonnet",
			Workspace:    workspaceRoot,
			Prompt:       "Test prompt",
			Status:       "queued",
			TaskID:       &taskID,
			Transport:    protocol.TransportNativeCLI,
			CreatedAt:    nowText,
		},
		WorkspaceLease: &leaseGrant,
	})

	_ = waitForType("run.started")
	completedMessage := waitForType("run.completed")
	require.Equal(t, "run-1", completedMessage.RunID)
	require.NotNil(t, completedMessage.Output)
	require.Equal(t, "completed cleanly", *completedMessage.Output)
	require.Equal(t, "sess-42", completedMessage.ProviderSessionID)

	sendMaster(protocol.MasterControlMessage{
		Type:         "instance.release",
		InstanceID:   "inst-1",
		AllocationID: "alloc-1",
		Mode:         "drain",
	})
	releasedMessage := waitForType("instance.released")
	require.Equal(t, "inst-1", releasedMessage.InstanceID)
	require.Equal(t, "alloc-1", releasedMessage.AllocationID)

	cancelClient()
	require.NoError(t, <-done)
}
