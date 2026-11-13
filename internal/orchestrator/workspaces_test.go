package orchestrator

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/slave"
	"github.com/cafecito-games/Orchestra/internal/store"
	"github.com/cafecito-games/Orchestra/internal/workspace"
)

func TestWorktreeAuthorVerifierAndLandingEndToEnd(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX shell stub and canonical path test")
	}

	gitTool, err := workspace.FindGit()
	require.NoError(t, err)

	temporaryDirectory := t.TempDir()
	fakeCodex := filepath.Join(temporaryDirectory, "codex")
	script := `#!/bin/sh
set -eu
prompt=""
prev=""
for arg in "$@"; do
	prompt="$arg"
done
cwd=$(pwd -P)
case "$prompt" in
	*ROLE=author_feature*)
		printf 'package alpha\n' > feature.go
		git add feature.go
		git -c user.name="Author Agent" -c user.email="author@orchestra.local" commit -q -m "add feature.go"
		text="cwd=$cwd role=author"
		;;
	*ROLE=author_conflict*)
		printf 'conflicting worktree edit\n' > README.md
		git add README.md
		git -c user.name="Author Agent" -c user.email="author@orchestra.local" commit -q -m "edit README on branch"
		text="cwd=$cwd role=conflict"
		;;
	*ROLE=verifier*)
		first_line=$(head -n 1 feature.go)
		text="cwd=$cwd verified=$first_line"
		;;
	*)
		text="cwd=$cwd"
		;;
esac
printf '{"type":"thread.started","thread_id":"thread-codex-ws"}\n'
printf '{"type":"item.completed","item":{"id":"item-1","type":"agent_message","text":"%s"}}\n' "$text"
`
	require.NoError(t, os.WriteFile(fakeCodex, []byte(script), 0o755))

	alphaRoots, err := slave.CanonicalizeWorkspaceRoots([]string{t.TempDir()})
	require.NoError(t, err)
	alphaWorkspaceRoot := alphaRoots[0]

	betaRoots, err := slave.CanonicalizeWorkspaceRoots([]string{t.TempDir()})
	require.NoError(t, err)

	alphaCheckoutPath := filepath.Join(alphaWorkspaceRoot, "alpha-service")
	require.NoError(t, os.MkdirAll(alphaCheckoutPath, 0o755))
	_, err = gitTool.Run(t.Context(), alphaCheckoutPath, "init", "--initial-branch=main")
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), alphaCheckoutPath, "remote", "add", "origin", "https://github.com/cafecito-games/alpha-service.git")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(alphaCheckoutPath, "README.md"), []byte("# alpha-service\n"), 0o644))
	_, err = gitTool.Run(t.Context(), alphaCheckoutPath, "add", "README.md")
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), alphaCheckoutPath, "-c", "user.name=Test", "-c", "user.email=test@orchestra.local", "commit", "-m", "initial")
	require.NoError(t, err)

	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	const bearerToken = "worktree-landing-secret"
	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: bearerToken,
	})
	require.NoError(t, err)
	defer service.Close()

	masterServer := httptest.NewServer(service.Handler())
	defer masterServer.Close()

	profiles := []protocol.HarnessProfile{{
		ID:             protocol.HarnessCodexCLI,
		Label:          "Codex",
		Description:    "0.40.0",
		Binary:         fakeCodex,
		Available:      true,
		AuthMode:       "local-account",
		ApprovalPolicy: protocol.ApprovalPolicyAuto,
		Models:         []string{"gpt-5.5"},
		Transports:     []string{protocol.TransportNativeCLI},
	}}

	startWorker := func(nodeID string, roots []string) func() {
		instanceCapacity := 4
		client := slave.NewClient(slave.ClientOptions{
			Endpoint: masterServer.URL,
			Token:    bearerToken,
			Node: protocol.ComputeNode{
				ID:               nodeID,
				Name:             nodeID,
				Kind:             "home-server",
				Platform:         runtime.GOOS + "-" + runtime.GOARCH,
				Status:           "online",
				Concurrency:      3,
				InstanceCapacity: &instanceCapacity,
				WorkspaceRoots:   roots,
				Version:          "0.3.0",
				Harnesses:        profiles,
			},
			Runner:           harness.NewRunner(profiles),
			WorkspaceManager: workspace.NewManager(roots, gitTool),
			CapabilityReport: &protocol.NodeCapabilityReport{
				NodeID: nodeID,
				At:     protocol.NowTimestamp(),
				Evidence: []protocol.NodeCapabilityEvidence{
					{CapabilityID: "workspace-writable", Success: true, NormalizedValue: "true", ObservedAt: protocol.NowTimestamp()},
					{CapabilityID: "workspace-lease:" + protocol.WorkspaceIsolationGitWorktree, Success: true, NormalizedValue: "true", ObservedAt: protocol.NowTimestamp()},
				},
			},
		})
		workerContext, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			done <- client.Run(workerContext)
		}()
		return func() {
			cancel()
			require.NoError(t, <-done)
		}
	}

	stopAlpha := startWorker("node-alpha", alphaRoots)
	defer stopAlpha()
	stopBeta := startWorker("node-beta", betaRoots)
	defer stopBeta()

	require.Eventually(t, func() bool {
		inventory, invErr := service.GetExecutionInventory()
		if invErr != nil || len(inventory.Nodes) != 2 {
			return false
		}
		for _, node := range inventory.Nodes {
			if !node.Synced {
				return false
			}
		}
		return true
	}, 5*time.Second, 25*time.Millisecond)

	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "test-orchestrator", SessionID: "osess-ws"}
	thread, _, err := service.CreateThread(caller, CreateThreadInput{
		Title:  "Worktree Author, Verifier, and Safe Landing",
		Attach: true,
	})
	require.NoError(t, err)

	otherThread, _, err := service.CreateThread(caller, CreateThreadInput{
		Title:  "Separate Thread",
		Attach: true,
	})
	require.NoError(t, err)

	waitForTaskCompletion := func(threadID string, taskID string) protocol.Run {
		t.Helper()
		var completedRun protocol.Run
		require.Eventually(t, func() bool {
			detail, detailErr := service.GetThreadDetail(threadID)
			if detailErr != nil {
				return false
			}
			taskCompleted := false
			for _, task := range detail.Tasks {
				if task.ID == taskID && task.Status == "completed" {
					taskCompleted = true
					break
				}
			}
			if !taskCompleted {
				return false
			}
			for _, runRecord := range detail.Runs {
				if runRecord.TaskID != nil && *runRecord.TaskID == taskID && runRecord.Status == "completed" {
					completedRun = runRecord
					return true
				}
			}
			return false
		}, 8*time.Second, 25*time.Millisecond)
		return completedRun
	}

	// 1. Submit writable git-worktree author task.
	authorSubmission, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "submit-author-1",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "author-task",
			Title:        "Implement feature.go in isolated worktree",
			Instructions: "ROLE=author_feature",
			Requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{
					Repository: "https://github.com/cafecito-games/alpha-service",
					Writable:   true,
					Isolation:  protocol.WorkspaceIsolationGitWorktree,
					Cleanup:    protocol.WorkspaceCleanupRetain,
					BaseBranch: "main",
				},
			},
		}},
	})
	require.NoError(t, err)
	authorTaskID := authorSubmission.ClientTaskMap["author-task"]
	authorRun := waitForTaskCompletion(thread.ID, authorTaskID)
	require.NotNil(t, authorRun.WorkspaceLeaseID)
	authorLeaseID := *authorRun.WorkspaceLeaseID

	require.Eventually(t, func() bool {
		listed, listErr := service.ListWorkspaceLeases(caller, ListWorkspaceLeasesInput{ThreadID: thread.ID})
		return listErr == nil && len(listed.WorkspaceLeases) == 1 && listed.WorkspaceLeases[0].Status == "retained"
	}, 5*time.Second, 25*time.Millisecond)

	listedLeases, err := service.ListWorkspaceLeases(caller, ListWorkspaceLeasesInput{ThreadID: thread.ID})
	require.NoError(t, err)
	require.Len(t, listedLeases.WorkspaceLeases, 1)
	authorLease := listedLeases.WorkspaceLeases[0]
	assert.Equal(t, authorLeaseID, authorLease.ID)
	assert.Equal(t, "node-alpha", authorLease.NodeID)

	// Cross-thread run caller cannot list leases from another thread.
	_, crossListErr := service.ListWorkspaceLeases(
		CallerIdentity{Kind: "run", RunID: "run_other", ThreadID: otherThread.ID},
		ListWorkspaceLeasesInput{ThreadID: thread.ID},
	)
	require.Error(t, crossListErr)

	// 2. Validate SourceLeaseID rejection cases (table-driven).
	rejectionCases := []struct {
		name         string
		targetThread string
		requirements protocol.ExecutionRequirements
		override     *protocol.PlacementOverride
		expectError  string
	}{
		{
			name:         "rejects writable true with sourceLeaseId",
			targetThread: thread.ID,
			requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{
					SourceLeaseID: authorLease.ID,
					Writable:      true,
				},
			},
			expectError: "writable=false",
		},
		{
			name:         "rejects isolation combined with sourceLeaseId",
			targetThread: thread.ID,
			requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{
					SourceLeaseID: authorLease.ID,
					Isolation:     protocol.WorkspaceIsolationGitWorktree,
				},
			},
			expectError: "cannot be combined with isolation",
		},
		{
			name:         "rejects cross-thread sourceLeaseId",
			targetThread: otherThread.ID,
			requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{
					SourceLeaseID: authorLease.ID,
					Writable:      false,
				},
			},
			expectError: "another thread",
		},
		{
			name:         "rejects mismatched node override",
			targetThread: thread.ID,
			requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{
					SourceLeaseID: authorLease.ID,
					Writable:      false,
				},
			},
			override:    &protocol.PlacementOverride{NodeID: "node-beta"},
			expectError: "does not match sourceLeaseId",
		},
		{
			name:         "rejects mismatched repository",
			targetThread: thread.ID,
			requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{
					SourceLeaseID: authorLease.ID,
					Repository:    "https://github.com/cafecito-games/different-repo",
					Writable:      false,
				},
			},
			expectError: "does not match sourceLeaseId",
		},
	}

	for index, testCase := range rejectionCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, submitErr := service.SubmitTasks(caller, SubmitTasksInput{
				ThreadID:       testCase.targetThread,
				IdempotencyKey: protocol.NewIdentifier("reject") + string(rune('a'+index)),
				Tasks: []SubmittedTaskSpec{{
					Title:             testCase.name,
					Instructions:      "ROLE=verifier",
					Requirements:      testCase.requirements,
					PlacementOverride: testCase.override,
				}},
			})
			require.Error(t, submitErr)
			assert.Contains(t, submitErr.Error(), testCase.expectError)
		})
	}

	// 3. Submit read-only verifier task using SourceLeaseID; verify it schedules onto node-alpha and inspects author's worktree without cloning.
	verifierSubmission, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "submit-verifier-1",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "verifier-task",
			Title:        "Verify feature.go in author worktree",
			Instructions: "ROLE=verifier",
			Requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{
					SourceLeaseID: authorLease.ID,
					Writable:      false,
				},
			},
		}},
	})
	require.NoError(t, err)
	verifierTaskID := verifierSubmission.ClientTaskMap["verifier-task"]
	verifierRun := waitForTaskCompletion(thread.ID, verifierTaskID)
	assert.Equal(t, "node-alpha", verifierRun.NodeID)
	assert.Equal(t, authorLease.WorktreePath, verifierRun.Workspace)
	assert.Contains(t, verifierRun.Output, "cwd="+authorLease.WorktreePath)
	assert.Contains(t, verifierRun.Output, "verified=package alpha")
	require.NotNil(t, verifierRun.WorkspaceLeaseID)

	require.Eventually(t, func() bool {
		leasesResp, listErr := service.ListWorkspaceLeases(caller, ListWorkspaceLeasesInput{
			ThreadID: thread.ID,
			TaskID:   verifierTaskID,
		})
		return listErr == nil && len(leasesResp.WorkspaceLeases) == 1 && leasesResp.WorkspaceLeases[0].Status == "cleaned"
	}, 5*time.Second, 25*time.Millisecond)

	// Managed worktrees directory still has only the single author worktree.
	managedEntries, err := os.ReadDir(filepath.Join(alphaWorkspaceRoot, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory)))
	require.NoError(t, err)
	assert.Len(t, managedEntries, 1)

	// 4. Non-delegating worker run cannot call LandWorkspaceLease.
	_, unauthorizedLandErr := service.LandWorkspaceLease(
		t.Context(),
		CallerIdentity{Kind: "run", RunID: "run_worker", ThreadID: thread.ID, CanDelegate: false},
		LandWorkspaceLeaseInput{LeaseID: authorLease.ID},
	)
	require.Error(t, unauthorizedLandErr)

	// 5. Dirty source checkout refuses landing safely and preserves the retained worktree.
	dirtyMarkerPath := filepath.Join(alphaCheckoutPath, "dirty.tmp")
	require.NoError(t, os.WriteFile(dirtyMarkerPath, []byte("uncommitted\n"), 0o644))

	dirtyLandResp, err := service.LandWorkspaceLease(t.Context(), caller, LandWorkspaceLeaseInput{
		LeaseID:          authorLease.ID,
		CleanupAfterLand: true,
	})
	require.NoError(t, err)
	assert.False(t, dirtyLandResp.Result.Landed)
	assert.Equal(t, protocol.WorkspaceLandingStatusDirty, dirtyLandResp.Result.Status)
	assert.Equal(t, "source_dirty", dirtyLandResp.Result.Reason)
	assert.Contains(t, dirtyLandResp.Result.DirtyFiles, "dirty.tmp")
	assert.True(t, dirtyLandResp.Result.WorktreeRetained)
	_, statErr := os.Stat(authorLease.WorktreePath)
	require.NoError(t, statErr)
	require.NoError(t, os.Remove(dirtyMarkerPath))

	// 6. Advance main so fast-forward refuses cleanly, then land via cherry-pick with cleanupAfterLand=true.
	require.NoError(t, os.WriteFile(filepath.Join(alphaCheckoutPath, "main_advance.txt"), []byte("advance\n"), 0o644))
	_, err = gitTool.Run(t.Context(), alphaCheckoutPath, "add", "main_advance.txt")
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), alphaCheckoutPath, "-c", "user.name=Test", "-c", "user.email=test@orchestra.local", "commit", "-m", "advance main")
	require.NoError(t, err)

	ffRefusedResp, err := service.LandWorkspaceLease(t.Context(), caller, LandWorkspaceLeaseInput{
		LeaseID:          authorLease.ID,
		Mode:             protocol.WorkspaceLandingModeFastForward,
		CleanupAfterLand: true,
	})
	require.NoError(t, err)
	assert.False(t, ffRefusedResp.Result.Landed)
	assert.Equal(t, protocol.WorkspaceLandingStatusConflict, ffRefusedResp.Result.Status)
	assert.Equal(t, "non_fast_forward", ffRefusedResp.Result.Reason)
	assert.True(t, ffRefusedResp.Result.WorktreeRetained)
	_, statErr = os.Stat(authorLease.WorktreePath)
	require.NoError(t, statErr)

	landedResp, err := service.LandWorkspaceLease(t.Context(), caller, LandWorkspaceLeaseInput{
		LeaseID:          authorLease.ID,
		Mode:             protocol.WorkspaceLandingModeCherryPick,
		CleanupAfterLand: true,
	})
	require.NoError(t, err)
	assert.True(t, landedResp.Result.Landed)
	assert.Equal(t, protocol.WorkspaceLandingStatusLanded, landedResp.Result.Status)
	assert.Equal(t, 1, landedResp.Result.CommitsLanded)
	assert.True(t, landedResp.Result.Cleaned)
	assert.False(t, landedResp.Result.WorktreeRetained)
	assert.Equal(t, "cleaned", landedResp.WorkspaceLease.Status)
	assert.Equal(t, "main", landedResp.WorkspaceLease.LandedBranch)
	assert.Equal(t, landedResp.Result.LandedRevision, landedResp.WorkspaceLease.LandedRevision)

	landedFeatureBytes, err := os.ReadFile(filepath.Join(alphaCheckoutPath, "feature.go"))
	require.NoError(t, err)
	assert.Equal(t, "package alpha\n", string(landedFeatureBytes))
	_, statErr = os.Stat(authorLease.WorktreePath)
	assert.True(t, os.IsNotExist(statErr))

	// Calling LandWorkspaceLease again on the landed & cleaned lease is idempotent.
	repeatResp, err := service.LandWorkspaceLease(t.Context(), caller, LandWorkspaceLeaseInput{
		LeaseID:          authorLease.ID,
		Mode:             protocol.WorkspaceLandingModeCherryPick,
		CleanupAfterLand: true,
	})
	require.NoError(t, err)
	assert.True(t, repeatResp.Result.Landed)
	assert.True(t, repeatResp.Result.AlreadyLanded)
	assert.True(t, repeatResp.Result.Cleaned)

	// 7. Conflicting cherry-pick preserves the retained worktree and reports conflicting files.
	conflictSubmission, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "submit-conflict-author",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "conflict-author",
			Title:        "Modify README on branch",
			Instructions: "ROLE=author_conflict",
			Requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{
					Repository: "https://github.com/cafecito-games/alpha-service",
					Writable:   true,
					Isolation:  protocol.WorkspaceIsolationGitWorktree,
					Cleanup:    protocol.WorkspaceCleanupRetain,
					BaseBranch: "main",
				},
			},
		}},
	})
	require.NoError(t, err)
	conflictTaskID := conflictSubmission.ClientTaskMap["conflict-author"]
	conflictRun := waitForTaskCompletion(thread.ID, conflictTaskID)
	require.NotNil(t, conflictRun.WorkspaceLeaseID)
	conflictLeaseID := *conflictRun.WorkspaceLeaseID

	require.Eventually(t, func() bool {
		leasesResp, listErr := service.ListWorkspaceLeases(caller, ListWorkspaceLeasesInput{
			ThreadID: thread.ID,
			TaskID:   conflictTaskID,
		})
		return listErr == nil && len(leasesResp.WorkspaceLeases) == 1 && leasesResp.WorkspaceLeases[0].Status == "retained"
	}, 5*time.Second, 25*time.Millisecond)

	// Create conflicting edit on README.md on main.
	require.NoError(t, os.WriteFile(filepath.Join(alphaCheckoutPath, "README.md"), []byte("# conflicting main README\n"), 0o644))
	_, err = gitTool.Run(t.Context(), alphaCheckoutPath, "add", "README.md")
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), alphaCheckoutPath, "-c", "user.name=Test", "-c", "user.email=test@orchestra.local", "commit", "-m", "main README conflict")
	require.NoError(t, err)

	conflictLandResp, err := service.LandWorkspaceLease(t.Context(), caller, LandWorkspaceLeaseInput{
		LeaseID:          conflictLeaseID,
		Mode:             protocol.WorkspaceLandingModeCherryPick,
		CleanupAfterLand: true,
	})
	require.NoError(t, err)
	assert.False(t, conflictLandResp.Result.Landed)
	assert.Equal(t, protocol.WorkspaceLandingStatusConflict, conflictLandResp.Result.Status)
	assert.Equal(t, "merge_conflict", conflictLandResp.Result.Reason)
	assert.Contains(t, conflictLandResp.Result.Conflicts, "README.md")
	assert.True(t, conflictLandResp.Result.WorktreeRetained)
	assert.Equal(t, "retained", conflictLandResp.WorkspaceLease.Status)

	_, statErr = os.Stat(conflictLandResp.WorkspaceLease.WorktreePath)
	require.NoError(t, statErr, "conflicting cherry-pick must preserve the retained worktree")

	httpConflictRecorder := httptest.NewRecorder()
	httpConflictRequest := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"/api/workspace-leases/"+conflictLeaseID+"/land",
		bytes.NewBufferString(`{"mode":"cherry-pick"}`),
	)
	service.HandleAPIWorkspaceLeaseLand(httpConflictRecorder, httpConflictRequest, conflictLeaseID)
	assert.Equal(t, http.StatusConflict, httpConflictRecorder.Code)
}
