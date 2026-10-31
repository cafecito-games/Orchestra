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

	coordinator.finishForRun(activeLeaseClean)

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

	coordinator.finishForRun(activeLeaseDirty)

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

	coordinator.finishForRun(activeLeaseRetain)

	// Policy is retain, so it should be retained even though clean
	assert.DirExists(t, grantRetain.WorktreePath)

	client.mutex.Lock()
	messagesRetain := client.outbox
	client.mutex.Unlock()
	require.NotEmpty(t, messagesRetain)
	lastRetainMessage := messagesRetain[len(messagesRetain)-1]
	assert.Equal(t, "workspace.lease", lastRetainMessage.Type)
	require.NotNil(t, lastRetainMessage.Lease)
	assert.Equal(t, "retained", lastRetainMessage.Lease.Status)
	assert.Equal(t, "policy", lastRetainMessage.Lease.RetentionReason)
}
