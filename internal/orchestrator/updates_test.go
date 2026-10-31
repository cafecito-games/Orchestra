package orchestrator

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/selfupdate"
	"github.com/cafecito-games/Orchestra/internal/slave"
	"github.com/cafecito-games/Orchestra/internal/store"
	"github.com/cafecito-games/Orchestra/internal/workspace"
)

func buildTestReleaseTarGz(t *testing.T, binaryName string, payload []byte) ([]byte, string) {
	t.Helper()
	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	tarWriter := tar.NewWriter(gzipWriter)
	require.NoError(t, tarWriter.WriteHeader(&tar.Header{
		Name:     binaryName,
		Mode:     0o755,
		Size:     int64(len(payload)),
		Typeflag: tar.TypeReg,
	}))
	_, err := tarWriter.Write(payload)
	require.NoError(t, err)
	require.NoError(t, tarWriter.Close())
	require.NoError(t, gzipWriter.Close())

	archiveBytes := buffer.Bytes()
	digest := sha256.Sum256(archiveBytes)
	return archiveBytes, hex.EncodeToString(digest[:])
}

func startMockGitHubReleaseServer(t *testing.T, version string, platforms []string, binaryPayload []byte) *httptest.Server {
	t.Helper()
	tagName := "v" + version
	archives := map[string][]byte{}
	var checksumLines []string

	for _, platform := range platforms {
		osName, archName, ok := strings.Cut(platform, "-")
		require.True(t, ok)
		binaryName := "orchestra"
		if osName == "windows" {
			binaryName = "orchestra.exe"
		}
		archiveName := fmt.Sprintf("orchestra_%s_%s_%s.tar.gz", version, osName, archName)
		archiveBytes, digestHex := buildTestReleaseTarGz(t, binaryName, binaryPayload)
		archives[archiveName] = archiveBytes
		checksumLines = append(checksumLines, fmt.Sprintf("%s  %s", digestHex, archiveName))
	}
	checksumPayload := []byte(strings.Join(checksumLines, "\n") + "\n")

	var server *httptest.Server
	mux := http.NewServeMux()
	writeReleaseMetadata := func(writer http.ResponseWriter) {
		type assetItem struct {
			Name               string `json:"name"`
			URL                string `json:"url"`
			BrowserDownloadURL string `json:"browser_download_url"`
			Size               int64  `json:"size"`
		}
		assets := []assetItem{{
			Name:               "checksums.txt",
			URL:                server.URL + "/assets/checksums.txt",
			BrowserDownloadURL: server.URL + "/assets/checksums.txt",
			Size:               int64(len(checksumPayload)),
		}}
		for archiveName, archiveBytes := range archives {
			assets = append(assets, assetItem{
				Name:               archiveName,
				URL:                server.URL + "/assets/" + archiveName,
				BrowserDownloadURL: server.URL + "/assets/" + archiveName,
				Size:               int64(len(archiveBytes)),
			})
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"tag_name":     tagName,
			"published_at": "2026-10-31T12:00:00Z",
			"html_url":     server.URL + "/releases/" + tagName,
			"assets":       assets,
		})
	}

	mux.HandleFunc("/repos/cafecito-games/Orchestra/releases/latest", func(writer http.ResponseWriter, request *http.Request) {
		writeReleaseMetadata(writer)
	})
	mux.HandleFunc("/repos/cafecito-games/Orchestra/releases/tags/"+tagName, func(writer http.ResponseWriter, request *http.Request) {
		writeReleaseMetadata(writer)
	})
	mux.HandleFunc("/assets/checksums.txt", func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write(checksumPayload)
	})
	for archiveName, archiveBytes := range archives {
		payloadCopy := archiveBytes
		mux.HandleFunc("/assets/"+archiveName, func(writer http.ResponseWriter, request *http.Request) {
			_, _ = writer.Write(payloadCopy)
		})
	}

	server = httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

func TestUpdateNodesEndToEndDrainScheduleHoldResidentPreservationAndAuth(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX shell script fixture")
	}

	updatedScript := fmt.Appendf(nil, "#!/bin/sh\nif [ \"$1\" = \"version\" ]; then\n  echo '{\"version\":\"0.2.0\",\"protocolVersion\":%q}'\n  exit 0\nfi\nexit 0\n", protocol.ProtocolVersion)
	releaseServer := startMockGitHubReleaseServer(t, "0.2.0", []string{"linux-amd64"}, updatedScript)

	releaseClient, err := selfupdate.NewReleaseClient(selfupdate.ReleaseClientOptions{
		APIBaseURL:        releaseServer.URL,
		Repository:        "cafecito-games/Orchestra",
		DisableGHFallback: true,
	})
	require.NoError(t, err)

	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	const masterToken = "fleet-update-secret"
	service, err := NewService(ServiceOptions{
		Store:            openedStore,
		Token:            masterToken,
		ReleaseClient:    releaseClient,
		DoorbellDebounce: 25 * time.Millisecond,
	})
	require.NoError(t, err)
	defer service.Close()

	masterServer := httptest.NewServer(service.Handler())
	defer masterServer.Close()

	// Worker run callers must be forbidden from triggering node self-updates.
	_, runCallerErr := service.ExecuteOrchestratorTool(
		t.Context(),
		CallerIdentity{Kind: "run", RunID: "run-unauthorized"},
		"update_nodes",
		json.RawMessage(`{"version":"latest"}`),
	)
	require.Error(t, runCallerErr)
	assert.Contains(t, runCallerErr.Error(), "not authorized")

	canonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{t.TempDir()})
	require.NoError(t, err)
	workspaceRoot := canonicalRoots[0]

	gateFile := filepath.Join(t.TempDir(), "drain-gate")
	fakeClaude := filepath.Join(t.TempDir(), "claude")
	claudeScript := fmt.Sprintf(`#!/bin/sh
case "$*" in
  *BLOCK_UNTIL_GATE*)
    while [ ! -f %q ]; do
      sleep 0.02
    done
    ;;
esac
echo '{"type":"assistant","session_id":"sess-upd","message":{"content":[{"type":"text","text":"working"}]}}'
echo '{"type":"result","result":"task-ok"}'
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

	updatableBinaryPath := filepath.Join(t.TempDir(), "worker-updatable-bin", "orchestra")
	require.NoError(t, os.MkdirAll(filepath.Dir(updatableBinaryPath), 0o755))
	initialScript := fmt.Appendf(nil, "#!/bin/sh\nif [ \"$1\" = \"version\" ]; then\n  echo '{\"version\":\"0.1.0\",\"protocolVersion\":%q}'\n  exit 0\nfi\nexit 0\n", protocol.ProtocolVersion)
	require.NoError(t, os.WriteFile(updatableBinaryPath, initialScript, 0o755))

	updatableDataRoot := filepath.Join(t.TempDir(), "updatable-data")
	instanceCapacity := 4
	activeInstances := 0

	var replacementStarted sync.Once
	replacementContext, cancelReplacement := context.WithCancel(t.Context())
	defer cancelReplacement()

	startUpdatableNode := func(version string, onExec func(string, []string, []string) error) *slave.Client {
		observedAt := protocol.NowTimestamp()
		capabilityReport := protocol.NodeCapabilityReport{
			NodeID: "worker-updatable",
			At:     observedAt,
			Evidence: []protocol.NodeCapabilityEvidence{
				{CapabilityID: "label:updatable", Source: "configured", Success: true, NormalizedValue: "updatable", ObservedAt: observedAt},
			},
		}
		return slave.NewClient(slave.ClientOptions{
			Endpoint:       masterServer.URL,
			Token:          masterToken,
			DataRoot:       updatableDataRoot,
			ExecutablePath: updatableBinaryPath,
			Node: protocol.ComputeNode{
				ID:               "worker-updatable",
				Name:             "Worker Updatable",
				Kind:             "home-server",
				Platform:         "linux-amd64",
				Status:           "online",
				LastSeen:         observedAt,
				Concurrency:      2,
				InstanceCapacity: &instanceCapacity,
				ActiveInstances:  &activeInstances,
				WorkspaceRoots:   canonicalRoots,
				Version:          version,
				Harnesses:        profiles,
			},
			Runner:           harness.NewRunner(profiles),
			WorkspaceManager: workspace.NewManager(canonicalRoots, nil),
			CapabilityReport: &capabilityReport,
			ExecReplacement:  onExec,
		})
	}

	workerV1Done := make(chan error, 1)
	workerV1 := startUpdatableNode("0.1.0", func(binaryPath string, arguments []string, environment []string) error {
		replacementStarted.Do(func() {
			workerV2 := startUpdatableNode("0.2.0", nil)
			go func() {
				_ = workerV2.Run(replacementContext)
			}()
		})
		return nil
	})
	go func() {
		workerV1Done <- workerV1.Run(t.Context())
	}()

	// Also connect a second node with DisableSelfUpdate=true.
	pinnedContext, cancelPinned := context.WithCancel(t.Context())
	defer cancelPinned()
	workerPinned := slave.NewClient(slave.ClientOptions{
		Endpoint:          masterServer.URL,
		Token:             masterToken,
		DisableSelfUpdate: true,
		Node: protocol.ComputeNode{
			ID:                 "worker-pinned",
			Name:               "Worker Pinned",
			Kind:               "local",
			Platform:           "linux-amd64",
			Status:             "online",
			LastSeen:           protocol.NowTimestamp(),
			Concurrency:        2,
			InstanceCapacity:   &instanceCapacity,
			ActiveInstances:    &activeInstances,
			WorkspaceRoots:     canonicalRoots,
			Version:            "0.1.0",
			SelfUpdateDisabled: true,
			Harnesses:          profiles,
		},
		Runner:           harness.NewRunner(profiles),
		WorkspaceManager: workspace.NewManager(canonicalRoots, nil),
	})
	go func() {
		_ = workerPinned.Run(pinnedContext)
	}()

	require.Eventually(t, func() bool {
		return service.isNodeLiveAndSynced("worker-updatable") && service.isNodeLiveAndSynced("worker-pinned")
	}, 5*time.Second, 25*time.Millisecond)

	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "orch-test", SessionID: "sess-1"}
	warmThread, _, err := service.CreateThread(caller, CreateThreadInput{Title: "Warm Resident Thread"})
	require.NoError(t, err)
	thread, _, err := service.CreateThread(caller, CreateThreadInput{Title: "Fleet Self-Update Thread"})
	require.NoError(t, err)

	// Spawn a warm resident instance on worker-updatable that must survive the in-place update.
	warmTitle := "Warm Reviewer"
	harnessList := []string{"claude-cli"}
	labelList := []string{"updatable"}
	spawnedWarm, err := service.SpawnInstance(caller, SpawnInstanceInput{
		ThreadID:       warmThread.ID,
		IdempotencyKey: "warm-resident-1",
		Purpose:        &protocol.InstancePurpose{Title: &warmTitle},
		Requirements: &protocol.InstanceExecutionRequirements{
			HarnessIDs: &harnessList,
			Labels:     &labelList,
			Workspace:  &protocol.InstanceWorkspaceRequirements{Path: &workspaceRoot},
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		inspected, inspectErr := service.GetInstance(caller, GetInstanceInput{InstanceID: spawnedWarm.Instance.ID})
		return inspectErr == nil && (inspected.Instance.Status == "ready" || inspected.Instance.Status == "idle")
	}, 5*time.Second, 25*time.Millisecond)

	// Submit a gated active task on worker-updatable.
	firstBatch, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "pre-update-task",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "active-before-update",
			Title:        "Long running task",
			Instructions: "BLOCK_UNTIL_GATE",
			Requirements: protocol.ExecutionRequirements{
				HarnessIDs: []string{"claude-cli"},
				Workspace:  &protocol.WorkspaceRequirements{Path: workspaceRoot},
			},
			PlacementOverride: &protocol.PlacementOverride{NodeID: "worker-updatable"},
		}},
	})
	require.NoError(t, err)
	activeTaskID := firstBatch.Tasks[0].ID

	require.Eventually(t, func() bool {
		snapshot, getErr := service.GetThreadContext(thread.ID)
		if getErr != nil {
			return false
		}
		for _, task := range snapshot.Tasks {
			if task.ID == activeTaskID && task.Status == "running" {
				return true
			}
		}
		return false
	}, 5*time.Second, 25*time.Millisecond)

	// Trigger fleet update via MCP tool `update_nodes`.
	rawUpdateResult, err := service.ExecuteOrchestratorTool(
		t.Context(),
		caller,
		"update_nodes",
		json.RawMessage(`{"version":"latest"}`),
	)
	require.NoError(t, err)
	var updateResponse UpdateNodesResponse
	require.NoError(t, json.Unmarshal(rawUpdateResult, &updateResponse))
	assert.Equal(t, "0.2.0", updateResponse.TargetVersion)
	assert.Equal(t, "v0.2.0", updateResponse.ReleaseTag)
	require.Len(t, updateResponse.Nodes, 2)

	// Unauthenticated bundle requests must be rejected with 401.
	unauthRequest, err := http.NewRequestWithContext(t.Context(), http.MethodGet, masterServer.URL+"/api/updates/bundles/0.2.0/linux-amd64", nil)
	require.NoError(t, err)
	unauthResponse, err := http.DefaultClient.Do(unauthRequest)
	require.NoError(t, err)
	_ = unauthResponse.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, unauthResponse.StatusCode)

	byNode := map[string]protocol.NodeUpdateState{}
	for _, entry := range updateResponse.Nodes {
		byNode[entry.NodeID] = entry
	}
	assert.Equal(t, "queued", byNode["worker-updatable"].Status)
	assert.Equal(t, "skipped", byNode["worker-pinned"].Status)

	// Wait until worker-updatable reports "draining" and node status shows "updating".
	require.Eventually(t, func() bool {
		details, listErr := service.ListNodeDetails()
		if listErr != nil {
			return false
		}
		for _, detail := range details {
			if detail.Node.ID == "worker-updatable" {
				return detail.Node.Status == "updating" &&
					detail.UpdateStatus != nil &&
					detail.UpdateStatus.Status == "draining"
			}
		}
		return false
	}, 5*time.Second, 25*time.Millisecond)

	// Submit a follow-up task pinned to worker-updatable while it is updating:
	// scheduler must hold it in "ready" with a "node-updating" placement diagnostic.
	secondBatch, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "queued-during-update",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "after-update",
			Title:        "Post-update verification task",
			Instructions: "Verify upgraded worker",
			Requirements: protocol.ExecutionRequirements{
				HarnessIDs: []string{"claude-cli"},
				Workspace:  &protocol.WorkspaceRequirements{Path: workspaceRoot},
			},
			PlacementOverride: &protocol.PlacementOverride{NodeID: "worker-updatable"},
		}},
	})
	require.NoError(t, err)
	queuedTaskID := secondBatch.Tasks[0].ID

	require.Eventually(t, func() bool {
		snapshot, getErr := service.GetThreadContext(thread.ID)
		if getErr != nil {
			return false
		}
		for _, task := range snapshot.Tasks {
			if task.ID == queuedTaskID && task.Status == "ready" && task.PlacementDiagnostic != nil {
				for _, unsatisfied := range task.PlacementDiagnostic.Unsatisfied {
					if unsatisfied.Kind == "node-updating" && unsatisfied.NodeID == "worker-updatable" {
						return true
					}
				}
			}
		}
		return false
	}, 5*time.Second, 25*time.Millisecond)

	// Open the gate so the draining run finishes cleanly, allowing worker-updatable to exec into 0.2.0.
	require.NoError(t, os.WriteFile(gateFile, []byte("open\n"), 0o600))

	select {
	case v1Err := <-workerV1Done:
		require.NoError(t, v1Err)
	case <-time.After(8 * time.Second):
		require.FailNow(t, "timed out waiting for workerV1 to exit into replacement")
	}

	// Verify worker-updatable reconnects as 0.2.0, update status becomes "succeeded",
	// warm resident instance remains intact, and both tasks complete!
	require.Eventually(t, func() bool {
		details, listErr := service.ListNodeDetails()
		if listErr != nil {
			return false
		}
		for _, detail := range details {
			if detail.Node.ID == "worker-updatable" {
				return detail.Connected &&
					detail.Synced &&
					detail.Node.Version == "0.2.0" &&
					detail.UpdateStatus != nil &&
					detail.UpdateStatus.Status == "succeeded"
			}
		}
		return false
	}, 8*time.Second, 25*time.Millisecond)

	require.Eventually(t, func() bool {
		snapshot, getErr := service.GetThreadContext(thread.ID)
		if getErr != nil {
			return false
		}
		completedCount := 0
		for _, task := range snapshot.Tasks {
			if (task.ID == activeTaskID || task.ID == queuedTaskID) && task.Status == "completed" {
				completedCount++
			}
		}
		return completedCount == 2
	}, 8*time.Second, 25*time.Millisecond)

	inspectedWarm, err := service.GetInstance(caller, GetInstanceInput{InstanceID: spawnedWarm.Instance.ID})
	require.NoError(t, err)
	assert.Contains(t, []string{"ready", "idle"}, inspectedWarm.Instance.Status)

	// Re-running UpdateNodes without force now reports up-to-date.
	repeatResponse, err := service.UpdateNodes(t.Context(), caller, UpdateNodesInput{
		Version: "0.2.0",
		NodeIDs: []string{"worker-updatable"},
	})
	require.NoError(t, err)
	require.Len(t, repeatResponse.Nodes, 1)
	assert.Equal(t, "up-to-date", repeatResponse.Nodes[0].Status)
}

func TestExpireStaleNodeUpdates(t *testing.T) {
	currentTime := time.Date(2026, 10, 31, 14, 0, 0, 0, time.UTC)
	var clockMutex sync.Mutex
	nowFunc := func() time.Time {
		clockMutex.Lock()
		defer clockMutex.Unlock()
		return currentTime
	}

	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store:             openedStore,
		Token:             "tok",
		NodeUpdateTimeout: 2 * time.Minute,
		Now:               nowFunc,
	})
	require.NoError(t, err)
	defer service.Close()

	initialText := currentTime.Format(time.RFC3339Nano)
	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		state.Nodes["worker-stuck"] = protocol.ComputeNode{
			ID:             "worker-stuck",
			Name:           "Worker Stuck",
			Kind:           "local",
			Platform:       "linux-amd64",
			Status:         "offline",
			LastSeen:       initialText,
			Concurrency:    1,
			WorkspaceRoots: []string{"/tmp"},
			Version:        "0.1.0",
		}
		state.NodeUpdates["worker-stuck"] = protocol.NodeUpdateState{
			UpdateID:        "upd-stuck",
			NodeID:          "worker-stuck",
			PreviousVersion: "0.1.0",
			CurrentVersion:  "0.1.0",
			TargetVersion:   "0.2.0",
			Status:          "restarting",
			RequestedAt:     initialText,
			UpdatedAt:       initialText,
		}
		return nil
	})
	require.NoError(t, err)

	service.expireStaleNodeUpdates()
	_ = openedStore.Read(func(state *store.State) error {
		assert.Equal(t, "restarting", state.NodeUpdates["worker-stuck"].Status)
		return nil
	})

	clockMutex.Lock()
	currentTime = currentTime.Add(3 * time.Minute)
	clockMutex.Unlock()

	service.expireStaleNodeUpdates()
	_ = openedStore.Read(func(state *store.State) error {
		assert.Equal(t, "failed", state.NodeUpdates["worker-stuck"].Status)
		assert.Contains(t, state.NodeUpdates["worker-stuck"].Error, "timed out")
		return nil
	})
}
