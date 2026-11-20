package slave

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/workspace"
)

func TestClient_ConnectionLifecycle_ReconnectOnDisconnect(t *testing.T) {
	var connectionAttempts atomic.Int32
	connectionEstablished := make(chan int, 4)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, "Bearer test-reconnect-token", request.Header.Get("Authorization"))

		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")

		attempt := connectionAttempts.Add(1)
		connectionEstablished <- int(attempt)

		for {
			_, payload, readErr := connection.Read(request.Context())
			if readErr != nil {
				return
			}
			var message protocol.SlaveControlMessage
			if err := json.Unmarshal(payload, &message); err != nil {
				continue
			}
			if message.Type == "register" {
				pingBytes, _ := json.Marshal(protocol.MasterControlMessage{
					Type: "ping",
					At:   time.Now().UTC().Format(time.RFC3339Nano),
				})
				_ = connection.Write(request.Context(), websocket.MessageText, pingBytes)

				// For the first connection attempt, simulate the master dropping the connection
				if attempt == 1 {
					time.Sleep(20 * time.Millisecond)
					_ = connection.Close(websocket.StatusPolicyViolation, "simulated disconnect")
					return
				}
			}
		}
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		Endpoint:          server.URL,
		Token:             "test-reconnect-token",
		Node:              protocol.ComputeNode{ID: "worker-reconnect", Concurrency: 1},
		ReconnectMinDelay: 20 * time.Millisecond,
		ReconnectMaxDelay: 50 * time.Millisecond,
	})

	runContext, cancelClient := context.WithCancel(t.Context())
	defer cancelClient()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(runContext)
	}()

	// First connection attempt arrives
	select {
	case attempt := <-connectionEstablished:
		assert.Equal(t, 1, attempt)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for first connection")
	}

	// Client reconnects automatically after master drops the connection
	select {
	case attempt := <-connectionEstablished:
		assert.Equal(t, 2, attempt)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for reconnect connection")
	}

	// Verify client is admitted on the second connection
	require.Eventually(t, func() bool {
		return client.admitted.Load()
	}, 3*time.Second, 20*time.Millisecond)

	cancelClient()
	require.NoError(t, <-done)
}

func TestClient_ConnectionLifecycle_HTTP401Unauthorized(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer valid-token" {
			http.Error(writer, "unauthorized handshake", http.StatusUnauthorized)
			return
		}
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		Endpoint: server.URL,
		Token:    "invalid-token",
		Node:     protocol.ComputeNode{ID: "worker-unauthorized", Concurrency: 1},
	})

	admitted, err := client.runConnection(t.Context())
	require.Error(t, err)
	assert.False(t, admitted)
	assert.Contains(t, err.Error(), "401")
	assert.False(t, client.admitted.Load())
}

func setupLifecycleGitRepository(t *testing.T) (string, string, *workspace.Git) {
	t.Helper()
	gitTool, err := workspace.FindGit()
	require.NoError(t, err)

	rawRoot := t.TempDir()
	rootPath, err := filepath.EvalSymlinks(rawRoot)
	require.NoError(t, err)

	repositoryPath := filepath.Join(rootPath, "project")
	require.NoError(t, os.MkdirAll(repositoryPath, 0o755))

	operationContext := t.Context()
	_, err = gitTool.Run(operationContext, repositoryPath, "init", "-b", "main")
	require.NoError(t, err)
	_, err = gitTool.Run(operationContext, repositoryPath, "config", "user.email", "test@orchestra.local")
	require.NoError(t, err)
	_, err = gitTool.Run(operationContext, repositoryPath, "config", "user.name", "Orchestra Test")
	require.NoError(t, err)
	_, err = gitTool.Run(operationContext, repositoryPath, "remote", "add", "origin", "https://github.com/cafecito-games/Orchestra.git")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(repositoryPath, "README.md"), []byte("# Orchestra Lifecycle\n"), 0o644))
	_, err = gitTool.Run(operationContext, repositoryPath, "add", "README.md")
	require.NoError(t, err)
	_, err = gitTool.Run(operationContext, repositoryPath, "commit", "-m", "initial commit")
	require.NoError(t, err)

	return rootPath, repositoryPath, gitTool
}

func TestLeaseCoordinator_WorktreeProvisioningFailureOnInvalidBaseRevision(t *testing.T) {
	rootPath, repositoryPath, gitTool := setupLifecycleGitRepository(t)
	manager := workspace.NewManager([]string{rootPath}, gitTool)

	client := NewClient(ClientOptions{
		Node:             protocol.ComputeNode{ID: "worker-1", WorkspaceRoots: []string{rootPath}},
		WorkspaceManager: manager,
	})
	coordinator := newLeaseCoordinator(client, manager)

	grant := protocol.WorkspaceLeaseGrant{
		ID:           "lease-invalid-base",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupWhenUnchanged,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("nonexistent-invalid-ref-12345"),
		Branch:       protocol.WorkspaceLeaseBranch("task-invalid", "run-invalid"),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), "lease-invalid-base"),
	}

	taskID := "task-invalid"
	run := protocol.Run{
		ID:     "run-invalid",
		TaskID: &taskID,
	}

	activeLease, workspacePath, err := coordinator.provisionForRun(t.Context(), run, grant)
	require.Error(t, err)
	assert.Nil(t, activeLease)
	assert.Empty(t, workspacePath)

	// Verify the failure was reported back to master in outbox
	client.mutex.Lock()
	messages := client.outbox
	client.mutex.Unlock()

	require.NotEmpty(t, messages)
	lastMessage := messages[len(messages)-1]
	assert.Equal(t, "workspace.lease", lastMessage.Type)
	require.NotNil(t, lastMessage.Lease)
	assert.Equal(t, "failed", lastMessage.Lease.Status)
	assert.Equal(t, "lease-invalid-base", lastMessage.Lease.LeaseID)
	assert.NotEmpty(t, lastMessage.Lease.Detail)
}

func TestLeaseCoordinator_CleanupHonorsDirtyRetainAndCleanDeletePolicies(t *testing.T) {
	rootPath, repositoryPath, gitTool := setupLifecycleGitRepository(t)
	manager := workspace.NewManager([]string{rootPath}, gitTool)

	client := NewClient(ClientOptions{
		Node:             protocol.ComputeNode{ID: "worker-1", WorkspaceRoots: []string{rootPath}},
		WorkspaceManager: manager,
	})
	coordinator := newLeaseCoordinator(client, manager)

	confirmAsync := func(leaseID, status string) {
		go func() {
			key := leaseID + "\x00" + status
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				coordinator.mutex.Lock()
				_, found := coordinator.pending[key]
				coordinator.mutex.Unlock()
				if found {
					coordinator.confirm(leaseID, status)
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	// Case 1: Clean worktree with WorkspaceCleanupWhenUnchanged policy is cleaned (deleted)
	grantClean := protocol.WorkspaceLeaseGrant{
		ID:           "lease-clean",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupWhenUnchanged,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("main"),
		Branch:       protocol.WorkspaceLeaseBranch("task-clean", "run-clean"),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), "lease-clean"),
	}

	taskCleanID := "task-clean"
	runClean := protocol.Run{
		ID:     "run-clean",
		TaskID: &taskCleanID,
	}

	confirmAsync("lease-clean", "active")

	activeLeaseClean, worktreeCleanPath, err := coordinator.provisionForRun(t.Context(), runClean, grantClean)
	require.NoError(t, err)
	require.NotNil(t, activeLeaseClean)
	assert.Equal(t, grantClean.WorktreePath, worktreeCleanPath)
	assert.DirExists(t, grantClean.WorktreePath)

	coordinator.finishForRun(activeLeaseClean, false)

	// Worktree was clean, so it should be deleted
	_, statErr := os.Stat(grantClean.WorktreePath)
	assert.True(t, os.IsNotExist(statErr))

	client.mutex.Lock()
	messagesClean := client.outbox
	client.outbox = nil
	client.mutex.Unlock()
	require.NotEmpty(t, messagesClean)
	lastCleanMessage := messagesClean[len(messagesClean)-1]
	assert.Equal(t, "workspace.lease", lastCleanMessage.Type)
	require.NotNil(t, lastCleanMessage.Lease)
	assert.Equal(t, "cleaned", lastCleanMessage.Lease.Status)

	// Case 2: Dirty worktree with WorkspaceCleanupWhenUnchanged policy is retained
	grantDirty := protocol.WorkspaceLeaseGrant{
		ID:           "lease-dirty",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupWhenUnchanged,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("main"),
		Branch:       protocol.WorkspaceLeaseBranch("task-dirty", "run-dirty"),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), "lease-dirty"),
	}

	taskDirtyID := "task-dirty"
	runDirty := protocol.Run{
		ID:     "run-dirty",
		TaskID: &taskDirtyID,
	}

	confirmAsync("lease-dirty", "active")

	activeLeaseDirty, worktreeDirtyPath, err := coordinator.provisionForRun(t.Context(), runDirty, grantDirty)
	require.NoError(t, err)
	require.NotNil(t, activeLeaseDirty)
	assert.DirExists(t, worktreeDirtyPath)

	// Introduce a dirty change in the worktree
	require.NoError(t, os.WriteFile(filepath.Join(worktreeDirtyPath, "untracked.txt"), []byte("dirty content\n"), 0o644))

	coordinator.finishForRun(activeLeaseDirty, false)

	// Worktree was dirty, so it should be retained
	assert.DirExists(t, grantDirty.WorktreePath)

	client.mutex.Lock()
	messagesDirty := client.outbox
	client.outbox = nil
	client.mutex.Unlock()
	require.NotEmpty(t, messagesDirty)
	lastDirtyMessage := messagesDirty[len(messagesDirty)-1]
	assert.Equal(t, "workspace.lease", lastDirtyMessage.Type)
	require.NotNil(t, lastDirtyMessage.Lease)
	assert.Equal(t, "retained", lastDirtyMessage.Lease.Status)
	assert.NotEmpty(t, lastDirtyMessage.Lease.RetentionReason)

	// Case 3: Clean worktree with WorkspaceCleanupRetain policy is retained
	grantRetain := protocol.WorkspaceLeaseGrant{
		ID:           "lease-retain-policy",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupRetain,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("main"),
		Branch:       protocol.WorkspaceLeaseBranch("task-retain", "run-retain"),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), "lease-retain-policy"),
	}

	taskRetainID := "task-retain"
	runRetain := protocol.Run{
		ID:     "run-retain",
		TaskID: &taskRetainID,
	}

	confirmAsync("lease-retain-policy", "active")

	activeLeaseRetain, worktreeRetainPath, err := coordinator.provisionForRun(t.Context(), runRetain, grantRetain)
	require.NoError(t, err)
	require.NotNil(t, activeLeaseRetain)
	assert.DirExists(t, worktreeRetainPath)

	coordinator.finishForRun(activeLeaseRetain, false)

	// Policy is retain, so it should be retained even though clean
	assert.DirExists(t, grantRetain.WorktreePath)

	client.mutex.Lock()
	messagesRetain := client.outbox
	client.outbox = nil
	client.mutex.Unlock()
	require.NotEmpty(t, messagesRetain)
	lastRetainMessage := messagesRetain[len(messagesRetain)-1]
	assert.Equal(t, "workspace.lease", lastRetainMessage.Type)
	require.NotNil(t, lastRetainMessage.Lease)
	assert.Equal(t, "retained", lastRetainMessage.Lease.Status)
	assert.Equal(t, "policy", lastRetainMessage.Lease.RetentionReason)

	// Case 4: Yield-on-block retains git-worktree (even with when-unchanged) and resumed run adopts it.
	grantYield := protocol.WorkspaceLeaseGrant{
		ID:           "lease-yield-reuse",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupWhenUnchanged,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("main"),
		Branch:       protocol.WorkspaceLeaseBranch("task-yield", "run-yield-1"),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), "lease-yield-reuse"),
	}
	taskYieldID := "task-yield"
	runYield1 := protocol.Run{ID: "run-yield-1", TaskID: &taskYieldID}
	confirmAsync("lease-yield-reuse", "active")

	activeLeaseYield, worktreeYieldPath, err := coordinator.provisionForRun(t.Context(), runYield1, grantYield)
	require.NoError(t, err)
	require.NotNil(t, activeLeaseYield)
	assert.DirExists(t, worktreeYieldPath)

	// Write work-in-progress file before yielding.
	wipFile := filepath.Join(worktreeYieldPath, "wip.txt")
	require.NoError(t, os.WriteFile(wipFile, []byte("in progress\n"), 0o644))

	coordinator.finishForRun(activeLeaseYield, true)
	assert.DirExists(t, worktreeYieldPath)
	assert.FileExists(t, wipFile)

	client.mutex.Lock()
	messagesYield := client.outbox
	client.outbox = nil
	client.mutex.Unlock()
	require.NotEmpty(t, messagesYield)
	lastYieldMessage := messagesYield[len(messagesYield)-1]
	assert.Equal(t, "workspace.lease", lastYieldMessage.Type)
	require.NotNil(t, lastYieldMessage.Lease)
	assert.Equal(t, "retained", lastYieldMessage.Lease.Status)
	assert.Equal(t, "task-blocked", lastYieldMessage.Lease.RetentionReason)

	// Resume on a new run ID adopting the retained lease.
	resumedGrant := activeLeaseYield.grant
	resumedGrant.Status = "retained"
	runYield2 := protocol.Run{ID: "run-yield-2", TaskID: &taskYieldID}
	confirmAsync("lease-yield-reuse", "active")

	resumedLease, resumedPath, err := coordinator.provisionForRun(t.Context(), runYield2, resumedGrant)
	require.NoError(t, err)
	require.NotNil(t, resumedLease)
	assert.Equal(t, worktreeYieldPath, resumedPath)
	wipBytes, readErr := os.ReadFile(wipFile)
	require.NoError(t, readErr)
	assert.Equal(t, "in progress\n", string(wipBytes))

	// Remove wip file so normal cleanup on completion cleans up the worktree cleanly.
	require.NoError(t, os.Remove(wipFile))
	coordinator.finishForRun(resumedLease, false)
	_, statYieldErr := os.Stat(worktreeYieldPath)
	assert.True(t, os.IsNotExist(statYieldErr))
}

func TestClient_InstanceReconfigureInPlaceAndErrorKindReporting(t *testing.T) {
	rawWorkspace := t.TempDir()
	workspacePath, err := filepath.EvalSymlinks(rawWorkspace)
	require.NoError(t, err)

	binaryDirectory := t.TempDir()
	fakeClaude := filepath.Join(binaryDirectory, "claude")
	script := `#!/bin/sh
model=""
resume_id=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --model)
      model="$2"
      shift 2
      ;;
    --resume)
      resume_id="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done
if [ "$model" = "sonnet" ]; then
  printf '%s\n' '{"type":"result","subtype":"success","is_error":true,"result":"You have hit your usage limit"}'
  exit 1
fi
printf '{"type":"assistant","session_id":"sess-warm-42","message":{"content":[{"type":"text","text":"model=%s resume=%s"}]}}\n' "$model" "$resume_id"
printf '{"type":"result","session_id":"sess-warm-42","result":"model=%s resume=%s"}\n' "$model" "$resume_id"
`
	require.NoError(t, os.WriteFile(fakeClaude, []byte(script), 0o755))

	runner := harness.NewRunner([]protocol.HarnessProfile{{
		ID:          protocol.HarnessClaudeCLI,
		Label:       "Claude Code",
		Description: "2.1.231",
		Binary:      fakeClaude,
		Available:   true,
		Models:      []string{"sonnet", "opus", "haiku"},
		Transports:  []string{protocol.TransportNativeCLI},
	}})

	client := NewClient(ClientOptions{
		Node: protocol.ComputeNode{
			ID:             "worker-reconfig",
			Concurrency:    2,
			WorkspaceRoots: []string{workspacePath},
		},
		Runner: runner,
	})
	client.rootContext = t.Context()

	instance := protocol.AgentInstance{
		ID:       "instance-reconfig-1",
		ThreadID: "thread-1",
		Status:   "provisioning",
	}
	allocation := protocol.InstanceAllocation{
		ID:         "alloc-reconfig-1",
		InstanceID: instance.ID,
		NodeID:     "worker-reconfig",
		HarnessID:  protocol.HarnessClaudeCLI,
		Model:      "sonnet",
		Transport:  protocol.TransportNativeCLI,
		Workspace:  workspacePath,
		Status:     "provisioning",
	}

	client.handleInstanceProvision(&instance, &allocation)
	client.mutex.Lock()
	require.Contains(t, client.instances, instance.ID)
	assert.Equal(t, "sonnet", client.instances[instance.ID].allocation.Model)
	client.outbox = nil
	client.mutex.Unlock()

	// Unsupported model reconfigure is rejected without mutating the resident allocation.
	client.handleInstanceReconfigure(protocol.MasterControlMessage{
		Type:         protocol.InstanceReconfigureControlType,
		InstanceID:   instance.ID,
		AllocationID: allocation.ID,
		Model:        "unsupported-model",
	})
	client.mutex.Lock()
	assert.Equal(t, "sonnet", client.instances[instance.ID].allocation.Model)
	client.mutex.Unlock()

	// Run on sonnet fails with quota_exhausted ErrorKind on run.failed.
	client.handleDispatch(protocol.MasterControlMessage{
		Type: "dispatch",
		Run: &protocol.Run{
			ID:           "run-quota-fail",
			ThreadID:     "thread-1",
			InstanceID:   instance.ID,
			AllocationID: allocation.ID,
			NodeID:       "worker-reconfig",
			HarnessID:    protocol.HarnessClaudeCLI,
			Model:        "sonnet",
			Transport:    protocol.TransportNativeCLI,
			Workspace:    workspacePath,
			Prompt:       "Trigger quota limit",
		},
	})
	client.waitGroup.Wait()

	client.mutex.Lock()
	failedOutbox := append([]protocol.SlaveControlMessage(nil), client.outbox...)
	client.outbox = nil
	client.mutex.Unlock()

	var quotaFailure *protocol.SlaveControlMessage
	for index := range failedOutbox {
		if failedOutbox[index].Type == "run.failed" && failedOutbox[index].RunID == "run-quota-fail" {
			quotaFailure = &failedOutbox[index]
			break
		}
	}
	require.NotNil(t, quotaFailure)
	assert.Equal(t, protocol.RunErrorKindQuotaExhausted, quotaFailure.ErrorKind)

	// Reconfigure resident instance in place to opus while idle.
	client.dispatchMasterMessage(protocol.MasterControlMessage{
		Type:         protocol.InstanceReconfigureControlType,
		InstanceID:   instance.ID,
		AllocationID: allocation.ID,
		Model:        "opus",
	})
	client.waitGroup.Wait()

	client.mutex.Lock()
	assert.Equal(t, "opus", client.instances[instance.ID].allocation.Model)
	client.mutex.Unlock()

	// Run on opus establishes warm session, then reconfigure to haiku preserves session resume!
	client.handleDispatch(protocol.MasterControlMessage{
		Type: "dispatch",
		Run: &protocol.Run{
			ID:           "run-opus-1",
			ThreadID:     "thread-1",
			InstanceID:   instance.ID,
			AllocationID: allocation.ID,
			NodeID:       "worker-reconfig",
			HarnessID:    protocol.HarnessClaudeCLI,
			Transport:    protocol.TransportNativeCLI,
			Workspace:    workspacePath,
			Prompt:       "Establish warm session",
		},
	})
	client.waitGroup.Wait()

	client.handleInstanceReconfigure(protocol.MasterControlMessage{
		Type:         protocol.InstanceReconfigureControlType,
		InstanceID:   instance.ID,
		AllocationID: allocation.ID,
		Model:        "haiku",
	})

	client.mutex.Lock()
	client.outbox = nil
	client.mutex.Unlock()

	client.handleDispatch(protocol.MasterControlMessage{
		Type: "dispatch",
		Run: &protocol.Run{
			ID:           "run-haiku-resume",
			ThreadID:     "thread-1",
			InstanceID:   instance.ID,
			AllocationID: allocation.ID,
			NodeID:       "worker-reconfig",
			HarnessID:    protocol.HarnessClaudeCLI,
			Transport:    protocol.TransportNativeCLI,
			Workspace:    workspacePath,
			Prompt:       "Continue warm session",
		},
		SessionBinding: &protocol.DispatchSessionBinding{
			ID:                "binding-warm-42",
			ProviderSessionID: "sess-warm-42",
			Transport:         protocol.TransportNativeCLI,
		},
	})
	client.waitGroup.Wait()

	client.mutex.Lock()
	resumedOutbox := append([]protocol.SlaveControlMessage(nil), client.outbox...)
	client.mutex.Unlock()

	var completedMessage *protocol.SlaveControlMessage
	for index := range resumedOutbox {
		if resumedOutbox[index].Type == "run.completed" && resumedOutbox[index].RunID == "run-haiku-resume" {
			completedMessage = &resumedOutbox[index]
			break
		}
	}
	require.NotNil(t, completedMessage)
	require.NotNil(t, completedMessage.Output)
	assert.Equal(t, "model=haiku resume=sess-warm-42", *completedMessage.Output)
	assert.Equal(t, "sess-warm-42", completedMessage.ProviderSessionID)
}

func TestClient_ConnectionLifecycle_CancelDuringAdmissionHandshake(t *testing.T) {
	rawRoot := t.TempDir()
	rootPath, err := filepath.EvalSymlinks(rawRoot)
	require.NoError(t, err)

	repositoryGitDirectory := filepath.Join(rootPath, "project", ".git")
	require.NoError(t, os.MkdirAll(repositoryGitDirectory, 0o755))

	markerPath := filepath.Join(t.TempDir(), "handshake-entered")
	slowGitBinary := filepath.Join(t.TempDir(), "fake-git")
	slowGitScript := "#!/bin/sh\n: > \"" + markerPath + "\"\nexec sleep 5\n"
	require.NoError(t, os.WriteFile(slowGitBinary, []byte(slowGitScript), 0o755))

	workspaceManager := workspace.NewManager([]string{rootPath}, &workspace.Git{
		Binary:             slowGitBinary,
		Timeout:            5 * time.Second,
		MaximumOutputBytes: 64 * 1024,
	})

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, acceptErr := websocket.Accept(writer, request, nil)
		if acceptErr != nil {
			return
		}
		defer connection.Close(websocket.StatusNormalClosure, "done")

		for {
			_, payload, readErr := connection.Read(request.Context())
			if readErr != nil {
				return
			}
			var message protocol.SlaveControlMessage
			if unmarshalErr := json.Unmarshal(payload, &message); unmarshalErr != nil {
				continue
			}
			if message.Type == "register" {
				pingBytes, _ := json.Marshal(protocol.MasterControlMessage{
					Type: "ping",
					At:   time.Now().UTC().Format(time.RFC3339Nano),
				})
				_ = connection.Write(request.Context(), websocket.MessageText, pingBytes)
			}
		}
	}))
	defer server.Close()

	client := NewClient(ClientOptions{
		Endpoint:          server.URL,
		Token:             "test-token",
		Node:              protocol.ComputeNode{ID: "worker-cancel-handshake", Concurrency: 1, WorkspaceRoots: []string{rootPath}},
		WorkspaceManager:  workspaceManager,
		ReconnectMinDelay: 20 * time.Millisecond,
		ReconnectMaxDelay: 50 * time.Millisecond,
	})

	runContext, cancelClient := context.WithCancel(t.Context())
	defer cancelClient()

	done := make(chan error, 1)
	go func() {
		done <- client.Run(runContext)
	}()

	require.Eventually(t, func() bool {
		_, statErr := os.Stat(markerPath)
		return statErr == nil
	}, 5*time.Second, 10*time.Millisecond)

	cancelClient()

	select {
	case runErr := <-done:
		require.NoError(t, runErr)
	case <-time.After(3 * time.Second):
		require.FailNow(t, "client.Run deadlocked when canceled during admission handshake")
	}
}
