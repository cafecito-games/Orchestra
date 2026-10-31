package slave

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/workspace"
)

func TestParseConfigurationDisableSelfUpdate(t *testing.T) {
	workspaceRoot := t.TempDir()

	fromFlag, err := ParseConfiguration(
		[]string{"--workspace-root", workspaceRoot, "--disable-self-update"},
		func(string) (string, bool) { return "", false },
	)
	require.NoError(t, err)
	assert.True(t, fromFlag.DisableSelfUpdate)

	fromEnvironment, err := ParseConfiguration(
		[]string{"--workspace-root", workspaceRoot},
		func(name string) (string, bool) {
			if name == "ORCHESTRA_DISABLE_SELF_UPDATE" {
				return "true", true
			}
			return "", false
		},
	)
	require.NoError(t, err)
	assert.True(t, fromEnvironment.DisableSelfUpdate)
}

func TestSlaveSelfUpdateDrainsActiveRunPreservesResidentHandoffAndExecs(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX shell script fixture")
	}

	canonicalRoots, err := CanonicalizeWorkspaceRoots([]string{t.TempDir()})
	require.NoError(t, err)
	workspaceRoot := canonicalRoots[0]
	dataRoot := filepath.Join(t.TempDir(), "slave-data")

	installedBinaryPath := filepath.Join(t.TempDir(), "orchestra")
	initialScript := fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = \"version\" ]; then\n  echo '{\"version\":\"0.1.0\",\"protocolVersion\":%q}'\n  exit 0\nfi\nexit 0\n", protocol.ProtocolVersion)
	require.NoError(t, os.WriteFile(installedBinaryPath, []byte(initialScript), 0o755))
	installedBinaryPath, err = filepath.EvalSymlinks(installedBinaryPath)
	require.NoError(t, err)

	updatedBinaryBytes := fmt.Appendf(nil, "#!/bin/sh\nif [ \"$1\" = \"version\" ]; then\n  echo '{\"version\":\"0.2.0\",\"protocolVersion\":%q}'\n  exit 0\nfi\nexit 0\n", protocol.ProtocolVersion)
	updatedSum := sha256.Sum256(updatedBinaryBytes)
	updatedSHA256 := hex.EncodeToString(updatedSum[:])

	gateFile := filepath.Join(t.TempDir(), "allow-finish")
	fakeClaude := filepath.Join(t.TempDir(), "claude")
	claudeScript := fmt.Sprintf(`#!/bin/sh
echo '{"type":"assistant","session_id":"sess-drain-1","message":{"content":[{"type":"text","text":"working before update"}]}}'
while [ ! -f %q ]; do
  sleep 0.02
done
echo '{"type":"result","result":"drained cleanly"}'
`, gateFile)
	require.NoError(t, os.WriteFile(fakeClaude, []byte(claudeScript), 0o755))

	profiles := []protocol.HarnessProfile{{
		ID:         "claude-cli",
		Label:      "Claude Code",
		Binary:     fakeClaude,
		Available:  true,
		AuthMode:   "local-subscription",
		Models:     []string{"sonnet"},
		Transports: []string{protocol.TransportNativeCLI},
	}}

	var serverMutex sync.Mutex
	var activeSocket *websocket.Conn
	var recordedEvents []string
	var reportedSyncInstances []string
	var updateStatuses []protocol.NodeUpdateReport

	mux := http.NewServeMux()
	mux.HandleFunc("/api/updates/bundles/0.2.0/linux-amd64", func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer update-token" {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		writer.Header().Set("Content-Type", "application/octet-stream")
		_, _ = writer.Write(updatedBinaryBytes)
	})
	mux.HandleFunc("/control-agent", func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer update-token" {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		connection, acceptErr := websocket.Accept(writer, request, nil)
		if acceptErr != nil {
			return
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")

		serverMutex.Lock()
		activeSocket = connection
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

			serverMutex.Lock()
			switch message.Type {
			case "register":
				if message.Node != nil {
					recordedEvents = append(recordedEvents, "register:"+message.Node.Version)
				}
				pingBytes, _ := json.Marshal(protocol.MasterControlMessage{Type: "ping", At: protocol.NowTimestamp()})
				_ = connection.Write(request.Context(), websocket.MessageText, pingBytes)
			case "sync.complete":
				recordedEvents = append(recordedEvents, "sync.complete")
				if message.ActiveInstanceIDs != nil {
					reportedSyncInstances = slices.Clone(*message.ActiveInstanceIDs)
				}
			case "instance.ready":
				recordedEvents = append(recordedEvents, "instance.ready:"+message.InstanceID)
			case "run.started":
				recordedEvents = append(recordedEvents, "run.started:"+message.RunID)
			case "run.completed":
				recordedEvents = append(recordedEvents, "run.completed:"+message.RunID)
			case "node.update.status":
				if message.NodeUpdate != nil {
					recordedEvents = append(recordedEvents, "update.status:"+message.NodeUpdate.Status)
					updateStatuses = append(updateStatuses, *message.NodeUpdate)
				}
			}
			serverMutex.Unlock()
		}
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	sendMasterMessage := func(message protocol.MasterControlMessage) {
		t.Helper()
		serverMutex.Lock()
		socket := activeSocket
		serverMutex.Unlock()
		require.NotNil(t, socket)
		encoded, marshalErr := json.Marshal(message)
		require.NoError(t, marshalErr)
		require.NoError(t, socket.Write(t.Context(), websocket.MessageText, encoded))
	}

	var execCalledMutex sync.Mutex
	var execBinary string
	var execArguments []string
	var execEnvironment []string

	instanceCapacity := 4
	activeInstances := 0
	nowText := protocol.NowTimestamp()

	clientV1 := NewClient(ClientOptions{
		Endpoint: server.URL,
		Token:    "update-token",
		DataRoot: dataRoot,
		Node: protocol.ComputeNode{
			ID:               "worker-drain",
			Name:             "Worker Drain",
			Kind:             "local",
			Platform:         "linux-amd64",
			Status:           "online",
			LastSeen:         nowText,
			Concurrency:      2,
			InstanceCapacity: &instanceCapacity,
			ActiveInstances:  &activeInstances,
			WorkspaceRoots:   canonicalRoots,
			Version:          "0.1.0",
			Harnesses:        profiles,
		},
		Runner:             harness.NewRunner(profiles),
		WorkspaceManager:   workspace.NewManager(canonicalRoots, nil),
		ExecutablePath:     installedBinaryPath,
		ProcessArguments:   []string{"slave", "--id", "worker-drain"},
		ProcessEnvironment: []string{"ORCHESTRA_TOKEN=update-token"},
		ExecReplacement: func(binaryPath string, arguments []string, environment []string) error {
			execCalledMutex.Lock()
			defer execCalledMutex.Unlock()
			execBinary = binaryPath
			execArguments = slices.Clone(arguments)
			execEnvironment = slices.Clone(environment)
			return nil
		},
	})

	clientDone := make(chan error, 1)
	go func() {
		clientDone <- clientV1.Run(t.Context())
	}()

	require.Eventually(t, func() bool {
		serverMutex.Lock()
		defer serverMutex.Unlock()
		return slices.Contains(recordedEvents, "sync.complete")
	}, 5*time.Second, 20*time.Millisecond)

	warmInstance := protocol.AgentInstance{
		ID:         "inst-warm-keep",
		ThreadID:   "thr-1",
		Creator:    protocol.InstanceCreator{Kind: "orchestrator-client", ClientID: "client-1"},
		Delegation: protocol.InstanceDelegationPolicy{CanDelegate: true},
		Lease:      protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: nowText},
		Status:     "provisioning",
		CreatedAt:  nowText,
		UpdatedAt:  nowText,
	}
	warmAllocation := protocol.InstanceAllocation{
		ID:         "alloc-warm-keep",
		InstanceID: "inst-warm-keep",
		NodeID:     "worker-drain",
		HarnessID:  "claude-cli",
		Model:      "sonnet",
		Transport:  protocol.TransportNativeCLI,
		Workspace:  workspaceRoot,
		Lease:      protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: nowText},
		Status:     "provisioning",
		CreatedAt:  nowText,
		UpdatedAt:  nowText,
	}
	sendMasterMessage(protocol.MasterControlMessage{
		Type:         "instance.provision",
		InstanceID:   warmInstance.ID,
		AllocationID: warmAllocation.ID,
		Instance:     &warmInstance,
		Allocation:   &warmAllocation,
		At:           nowText,
	})

	require.Eventually(t, func() bool {
		serverMutex.Lock()
		defer serverMutex.Unlock()
		return slices.Contains(recordedEvents, "instance.ready:inst-warm-keep")
	}, 5*time.Second, 20*time.Millisecond)

	runInstance := warmInstance
	runInstance.ID = "inst-active-run"
	runAllocation := warmAllocation
	runAllocation.ID = "alloc-active-run"
	runAllocation.InstanceID = "inst-active-run"
	dispatchedRun := protocol.Run{
		ID:           "run-drain-1",
		ThreadID:     "thr-1",
		InstanceID:   runInstance.ID,
		AllocationID: runAllocation.ID,
		NodeID:       "worker-drain",
		HarnessID:    "claude-cli",
		Model:        "sonnet",
		Transport:    protocol.TransportNativeCLI,
		Workspace:    workspaceRoot,
		Prompt:       "Perform pre-update work",
		Status:       "queued",
		CreatedAt:    nowText,
	}
	sendMasterMessage(protocol.MasterControlMessage{
		Type:         "dispatch",
		RunID:        dispatchedRun.ID,
		InstanceID:   runInstance.ID,
		AllocationID: runAllocation.ID,
		Instance:     &runInstance,
		Allocation:   &runAllocation,
		Run:          &dispatchedRun,
		At:           nowText,
	})

	require.Eventually(t, func() bool {
		serverMutex.Lock()
		defer serverMutex.Unlock()
		return slices.Contains(recordedEvents, "run.started:run-drain-1")
	}, 5*time.Second, 20*time.Millisecond)

	directive := protocol.NodeUpdateDirective{
		UpdateID:        "upd-drain-1",
		TargetVersion:   "0.2.0",
		ProtocolVersion: protocol.ProtocolVersion,
		DownloadPath:    "/api/updates/bundles/0.2.0/linux-amd64",
		ExpectedSHA256:  updatedSHA256,
		SizeBytes:       int64(len(updatedBinaryBytes)),
	}
	sendMasterMessage(protocol.MasterControlMessage{
		Type:       "node.update",
		NodeUpdate: &directive,
		At:         protocol.NowTimestamp(),
	})

	require.Eventually(t, func() bool {
		serverMutex.Lock()
		defer serverMutex.Unlock()
		return slices.Contains(recordedEvents, "update.status:draining")
	}, 5*time.Second, 20*time.Millisecond)

	execCalledMutex.Lock()
	assert.Empty(t, execBinary, "slave must not exec replacement while active run is still executing")
	execCalledMutex.Unlock()

	require.NoError(t, os.WriteFile(gateFile, []byte("open\n"), 0o600))

	select {
	case runErr := <-clientDone:
		require.NoError(t, runErr)
	case <-time.After(8 * time.Second):
		require.FailNow(t, "timed out waiting for slave Run() to exit into ExecReplacement")
	}

	execCalledMutex.Lock()
	assert.Equal(t, installedBinaryPath, execBinary)
	assert.Equal(t, []string{"slave", "--id", "worker-drain"}, execArguments)
	assert.Equal(t, []string{"ORCHESTRA_TOKEN=update-token"}, execEnvironment)
	execCalledMutex.Unlock()

	onDiskBytes, err := os.ReadFile(installedBinaryPath)
	require.NoError(t, err)
	assert.Equal(t, updatedBinaryBytes, onDiskBytes)

	serverMutex.Lock()
	completedIndex := slices.Index(recordedEvents, "run.completed:run-drain-1")
	restartingIndex := slices.Index(recordedEvents, "update.status:restarting")
	serverMutex.Unlock()
	require.NotEqual(t, -1, completedIndex, "run.completed must be delivered before restart")
	require.NotEqual(t, -1, restartingIndex, "restarting update status must be delivered")
	assert.Less(t, completedIndex, restartingIndex, "run.completed must precede update.status:restarting")

	// Simulate the execve'd v0.2.0 slave starting up with the same DataRoot and reconnecting.
	clientV2Context, cancelV2 := context.WithCancel(t.Context())
	defer cancelV2()
	clientV2 := NewClient(ClientOptions{
		Endpoint: server.URL,
		Token:    "update-token",
		DataRoot: dataRoot,
		Node: protocol.ComputeNode{
			ID:               "worker-drain",
			Name:             "Worker Drain",
			Kind:             "local",
			Platform:         "linux-amd64",
			Status:           "online",
			LastSeen:         protocol.NowTimestamp(),
			Concurrency:      2,
			InstanceCapacity: &instanceCapacity,
			ActiveInstances:  &activeInstances,
			WorkspaceRoots:   canonicalRoots,
			Version:          "0.2.0",
			Harnesses:        profiles,
		},
		Runner:           harness.NewRunner(profiles),
		WorkspaceManager: workspace.NewManager(canonicalRoots, nil),
		ExecutablePath:   installedBinaryPath,
	})
	go func() {
		_ = clientV2.Run(clientV2Context)
	}()

	require.Eventually(t, func() bool {
		serverMutex.Lock()
		defer serverMutex.Unlock()
		return slices.Contains(recordedEvents, "register:0.2.0") &&
			slices.Contains(reportedSyncInstances, "inst-warm-keep")
	}, 5*time.Second, 20*time.Millisecond)

	assert.NoFileExists(t, clientV2.handoffFilePath())
}

func TestSlaveSelfUpdateRejectsDisabledAndChecksumMismatch(t *testing.T) {
	canonicalRoots, err := CanonicalizeWorkspaceRoots([]string{t.TempDir()})
	require.NoError(t, err)
	installedBinaryPath := filepath.Join(t.TempDir(), "orchestra")
	originalBytes := []byte("original-binary")
	require.NoError(t, os.WriteFile(installedBinaryPath, originalBytes, 0o755))

	var serverMutex sync.Mutex
	var activeSocket *websocket.Conn
	var synced1 bool
	var reports []protocol.NodeUpdateReport

	mux := http.NewServeMux()
	mux.HandleFunc("/api/updates/bundles/0.2.0/linux-amd64", func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("corrupted-payload"))
	})
	mux.HandleFunc("/control-agent", func(writer http.ResponseWriter, request *http.Request) {
		connection, acceptErr := websocket.Accept(writer, request, nil)
		if acceptErr != nil {
			return
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")
		serverMutex.Lock()
		activeSocket = connection
		serverMutex.Unlock()
		for {
			_, payload, readErr := connection.Read(request.Context())
			if readErr != nil {
				return
			}
			var message protocol.SlaveControlMessage
			if json.Unmarshal(payload, &message) != nil {
				continue
			}
			if message.Type == "register" {
				pingBytes, _ := json.Marshal(protocol.MasterControlMessage{Type: "ping", At: protocol.NowTimestamp()})
				_ = connection.Write(request.Context(), websocket.MessageText, pingBytes)
			}
			if message.Type == "sync.complete" {
				serverMutex.Lock()
				synced1 = true
				serverMutex.Unlock()
			}
			if message.Type == "node.update.status" && message.NodeUpdate != nil {
				serverMutex.Lock()
				reports = append(reports, *message.NodeUpdate)
				serverMutex.Unlock()
			}
		}
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	// 1. Disabled self-update node rejects immediately.
	disabledContext, cancelDisabled := context.WithCancel(t.Context())
	disabledDone := make(chan error, 1)
	disabledClient := NewClient(ClientOptions{
		Endpoint:          server.URL,
		Token:             "tok",
		DisableSelfUpdate: true,
		ExecutablePath:    installedBinaryPath,
		Node: protocol.ComputeNode{
			ID:             "worker-disabled",
			Name:           "Worker Disabled",
			Kind:           "local",
			Platform:       "linux-amd64",
			Status:         "online",
			LastSeen:       protocol.NowTimestamp(),
			Concurrency:    1,
			WorkspaceRoots: canonicalRoots,
			Version:        "0.1.0",
		},
		Runner:           harness.NewRunner(nil),
		WorkspaceManager: workspace.NewManager(canonicalRoots, nil),
	})
	go func() {
		disabledDone <- disabledClient.Run(disabledContext)
	}()

	require.Eventually(t, func() bool {
		serverMutex.Lock()
		defer serverMutex.Unlock()
		return synced1 && activeSocket != nil
	}, 5*time.Second, 20*time.Millisecond)

	disabledDirective := protocol.NodeUpdateDirective{
		UpdateID:        "upd-disabled",
		TargetVersion:   "0.2.0",
		ProtocolVersion: protocol.ProtocolVersion,
		DownloadPath:    "/api/updates/bundles/0.2.0/linux-amd64",
		ExpectedSHA256:  stringsRepeatHex("a", 64),
		SizeBytes:       17,
	}
	encodedDirective, err := json.Marshal(protocol.MasterControlMessage{
		Type:       "node.update",
		NodeUpdate: &disabledDirective,
		At:         protocol.NowTimestamp(),
	})
	require.NoError(t, err)
	serverMutex.Lock()
	require.NoError(t, activeSocket.Write(t.Context(), websocket.MessageText, encodedDirective))
	serverMutex.Unlock()

	require.Eventually(t, func() bool {
		serverMutex.Lock()
		defer serverMutex.Unlock()
		return len(reports) >= 1 && reports[0].UpdateID == "upd-disabled" && reports[0].Status == "failed"
	}, 5*time.Second, 20*time.Millisecond)
	cancelDisabled()
	<-disabledDone

	// 2. Enabled self-update node rejects mismatched SHA-256 without overwriting binary.
	mux2 := http.NewServeMux()
	mux2.HandleFunc("/api/updates/bundles/0.2.0/linux-amd64", func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte("corrupted-payload"))
	})
	var server2Mutex sync.Mutex
	var activeSocket2 *websocket.Conn
	var synced2 bool
	var reports2 []protocol.NodeUpdateReport
	mux2.HandleFunc("/control-agent", func(writer http.ResponseWriter, request *http.Request) {
		connection, acceptErr := websocket.Accept(writer, request, nil)
		if acceptErr != nil {
			return
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")
		server2Mutex.Lock()
		activeSocket2 = connection
		server2Mutex.Unlock()
		for {
			_, payload, readErr := connection.Read(request.Context())
			if readErr != nil {
				return
			}
			var message protocol.SlaveControlMessage
			if json.Unmarshal(payload, &message) != nil {
				continue
			}
			if message.Type == "register" {
				pingBytes, _ := json.Marshal(protocol.MasterControlMessage{Type: "ping", At: protocol.NowTimestamp()})
				_ = connection.Write(request.Context(), websocket.MessageText, pingBytes)
			}
			if message.Type == "sync.complete" {
				server2Mutex.Lock()
				synced2 = true
				server2Mutex.Unlock()
			}
			if message.Type == "node.update.status" && message.NodeUpdate != nil {
				server2Mutex.Lock()
				reports2 = append(reports2, *message.NodeUpdate)
				server2Mutex.Unlock()
			}
		}
	})
	server2 := httptest.NewServer(mux2)
	defer server2.Close()

	enabledContext, cancelEnabled := context.WithCancel(t.Context())
	defer cancelEnabled()
	enabledClient := NewClient(ClientOptions{
		Endpoint:       server2.URL,
		Token:          "tok",
		ExecutablePath: installedBinaryPath,
		Node: protocol.ComputeNode{
			ID:             "worker-enabled",
			Name:           "Worker Enabled",
			Kind:           "local",
			Platform:       "linux-amd64",
			Status:         "online",
			LastSeen:       protocol.NowTimestamp(),
			Concurrency:    1,
			WorkspaceRoots: canonicalRoots,
			Version:        "0.1.0",
		},
		Runner:           harness.NewRunner(nil),
		WorkspaceManager: workspace.NewManager(canonicalRoots, nil),
	})
	go func() {
		_ = enabledClient.Run(enabledContext)
	}()

	require.Eventually(t, func() bool {
		server2Mutex.Lock()
		defer server2Mutex.Unlock()
		return synced2 && activeSocket2 != nil
	}, 5*time.Second, 20*time.Millisecond)

	badDigestDirective := protocol.NodeUpdateDirective{
		UpdateID:        "upd-bad-sha",
		TargetVersion:   "0.2.0",
		ProtocolVersion: protocol.ProtocolVersion,
		DownloadPath:    "/api/updates/bundles/0.2.0/linux-amd64",
		ExpectedSHA256:  stringsRepeatHex("b", 64),
		SizeBytes:       int64(len("corrupted-payload")),
	}
	encodedBadDirective, err := json.Marshal(protocol.MasterControlMessage{
		Type:       "node.update",
		NodeUpdate: &badDigestDirective,
		At:         protocol.NowTimestamp(),
	})
	require.NoError(t, err)
	server2Mutex.Lock()
	require.NoError(t, activeSocket2.Write(t.Context(), websocket.MessageText, encodedBadDirective))
	server2Mutex.Unlock()

	require.Eventually(t, func() bool {
		server2Mutex.Lock()
		defer server2Mutex.Unlock()
		for _, item := range reports2 {
			if item.UpdateID == "upd-bad-sha" && item.Status == "failed" {
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)

	afterBytes, err := os.ReadFile(installedBinaryPath)
	require.NoError(t, err)
	assert.Equal(t, originalBytes, afterBytes)
}

func stringsRepeatHex(character string, count int) string {
	var result strings.Builder
	for range count {
		result.WriteString(character)
	}
	return result.String()
}
