package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/slave"
	"github.com/cafecito-games/Orchestra/internal/store"
	"github.com/cafecito-games/Orchestra/internal/workspace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepositoryDiscoveryCloneRefreshAndAutomaticTaskPlacement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX shell stub and canonical path test")
	}

	gitTool, err := workspace.FindGit()
	require.NoError(t, err)

	temporaryDirectory := t.TempDir()
	fakeCodex := filepath.Join(temporaryDirectory, "codex")
	script := `#!/bin/sh
set -eu
cwd=$(pwd -P)
printf '{"type":"thread.started","thread_id":"thread-codex-1"}\n'
printf '{"type":"item.completed","item":{"id":"item-1","type":"agent_message","text":"cwd=%s"}}\n' "$cwd"
`
	require.NoError(t, os.WriteFile(fakeCodex, []byte(script), 0o755))

	parentRoots, err := slave.CanonicalizeWorkspaceRoots([]string{t.TempDir()})
	require.NoError(t, err)
	workspaceRoot := parentRoots[0]

	createLocalCheckout := func(relativeDirectory string, remoteURL string, branch string) string {
		checkoutPath := filepath.Join(workspaceRoot, filepath.FromSlash(relativeDirectory))
		require.NoError(t, os.MkdirAll(checkoutPath, 0o755))
		_, runErr := gitTool.Run(t.Context(), checkoutPath, "init", "--initial-branch="+branch)
		require.NoError(t, runErr)
		_, runErr = gitTool.Run(t.Context(), checkoutPath, "remote", "add", "origin", remoteURL)
		require.NoError(t, runErr)
		require.NoError(t, os.WriteFile(filepath.Join(checkoutPath, "README.md"), []byte("# "+relativeDirectory+"\n"), 0o644))
		_, runErr = gitTool.Run(t.Context(), checkoutPath, "add", "README.md")
		require.NoError(t, runErr)
		_, runErr = gitTool.Run(t.Context(), checkoutPath, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "initial")
		require.NoError(t, runErr)
		return checkoutPath
	}

	// Pre-populate alpha-service with an SSH remote to verify transport-agnostic matching when tasks specify HTTPS.
	alphaCheckoutPath := createLocalCheckout("alpha-service", "git@github.com:cafecito-games/alpha-service.git", "main")

	// Prepare a remote bare HTTP repository for beta-service that is NOT yet cloned on the slave.
	remoteHostingRoot, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	seedBeta := filepath.Join(remoteHostingRoot, "seed-beta")
	require.NoError(t, os.MkdirAll(seedBeta, 0o755))
	_, err = gitTool.Run(t.Context(), seedBeta, "init", "--initial-branch=main")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(seedBeta, "service.go"), []byte("package beta\n"), 0o644))
	_, err = gitTool.Run(t.Context(), seedBeta, "add", "service.go")
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), seedBeta, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "beta init")
	require.NoError(t, err)

	bareBetaPath := filepath.Join(remoteHostingRoot, "cafecito-games", "beta-service.git")
	require.NoError(t, os.MkdirAll(filepath.Dir(bareBetaPath), 0o755))
	_, err = gitTool.Run(t.Context(), remoteHostingRoot, "clone", "--bare", seedBeta, bareBetaPath)
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), bareBetaPath, "update-server-info")
	require.NoError(t, err)

	gitHTTPServer := httptest.NewServer(http.FileServer(http.Dir(remoteHostingRoot)))
	defer gitHTTPServer.Close()
	betaCloneURL := gitHTTPServer.URL + "/cafecito-games/beta-service.git"

	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	const bearerToken = "repo-discovery-secret"
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
	runner := harness.NewRunner(profiles)
	workspaceManager := workspace.NewManager(parentRoots, gitTool)
	instanceCapacity := 6

	workerClient := slave.NewClient(slave.ClientOptions{
		Endpoint: masterServer.URL,
		Token:    bearerToken,
		Node: protocol.ComputeNode{
			ID:               "fabrica",
			Name:             "Fabrica Remote Worker",
			Kind:             "home-server",
			Platform:         runtime.GOOS + "-" + runtime.GOARCH,
			Status:           "online",
			Concurrency:      3,
			InstanceCapacity: &instanceCapacity,
			WorkspaceRoots:   parentRoots,
			Version:          "0.3.0",
			Harnesses:        profiles,
		},
		Runner:           runner,
		WorkspaceManager: workspaceManager,
		CapabilityReport: &protocol.NodeCapabilityReport{
			NodeID: "fabrica",
			At:     protocol.NowTimestamp(),
			Evidence: []protocol.NodeCapabilityEvidence{
				{CapabilityID: "workspace-writable", Success: true, NormalizedValue: "true", ObservedAt: protocol.NowTimestamp()},
				{CapabilityID: "workspace-lease:" + protocol.WorkspaceIsolationGitWorktree, Success: true, NormalizedValue: "true", ObservedAt: protocol.NowTimestamp()},
			},
		},
	})

	workerContext, cancelWorker := context.WithCancel(t.Context())
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- workerClient.Run(workerContext)
	}()
	defer func() {
		cancelWorker()
		require.NoError(t, <-workerDone)
	}()

	require.Eventually(t, func() bool {
		inventory, invErr := service.GetExecutionInventory()
		if invErr != nil || len(inventory.Nodes) != 1 || !inventory.Nodes[0].Synced {
			return false
		}
		report := inventory.Nodes[0].RepositoryReport
		return report != nil && len(report.Repositories) == 1 && report.Repositories[0].RelativePath == "alpha-service"
	}, 5*time.Second, 25*time.Millisecond)

	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "test-orchestrator", SessionID: "osess-1"}
	thread, _, err := service.CreateThread(caller, CreateThreadInput{
		Title:  "Multi-Repo & Clone Verification",
		Attach: true,
	})
	require.NoError(t, err)

	waitForTaskRunOutput := func(taskID string) protocol.Run {
		t.Helper()
		var completedRun protocol.Run
		require.Eventually(t, func() bool {
			detail, detailErr := service.GetThreadDetail(thread.ID)
			if detailErr != nil {
				return false
			}
			taskDone := false
			for _, task := range detail.Tasks {
				if task.ID == taskID && task.Status == "completed" {
					taskDone = true
					break
				}
			}
			if !taskDone {
				return false
			}
			for _, runRecord := range detail.Runs {
				if runRecord.TaskID != nil && *runRecord.TaskID == taskID && runRecord.Status == "completed" {
					completedRun = runRecord
					return true
				}
			}
			return false
		}, 6*time.Second, 25*time.Millisecond)
		return completedRun
	}

	// 1. Task specifying HTTPS repository URL automatically resolves to SSH-cloned alpha-service directory.
	alphaSubmission, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "alpha-by-repo",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "alpha-task",
			Title:        "Run inside alpha-service via repository identity",
			Instructions: "Inspect alpha-service",
			Requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{
					Repository: "https://github.com/cafecito-games/alpha-service",
				},
			},
		}},
	})
	require.NoError(t, err)
	alphaRun := waitForTaskRunOutput(alphaSubmission.ClientTaskMap["alpha-task"])
	assert.Equal(t, alphaCheckoutPath, alphaRun.Workspace)
	assert.Contains(t, alphaRun.Output, "cwd="+alphaCheckoutPath)

	// 2. Task omitting workspace runs in parent workspace root for cross-repository work.
	crossRepoSubmission, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "cross-repo-root",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "root-task",
			Title:        "Cross-repository inspection at parent root",
			Instructions: "Inspect across repositories",
		}},
	})
	require.NoError(t, err)
	crossRepoRun := waitForTaskRunOutput(crossRepoSubmission.ClientTaskMap["root-task"])
	assert.Equal(t, workspaceRoot, crossRepoRun.Workspace)
	assert.Contains(t, crossRepoRun.Output, "cwd="+workspaceRoot)

	// 3. Task requesting missing beta-service records an actionable placement diagnostic, then automatically schedules once cloned!
	betaSubmission, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "beta-missing-then-cloned",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "beta-task",
			Title:        "Work on beta-service",
			Instructions: "Implement feature in beta-service",
			Requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{
					Repository: betaCloneURL,
					Writable:   true,
					Isolation:  protocol.WorkspaceIsolationGitWorktree,
				},
			},
		}},
	})
	require.NoError(t, err)
	betaTaskID := betaSubmission.ClientTaskMap["beta-task"]

	require.Eventually(t, func() bool {
		detail, detailErr := service.GetThreadDetail(thread.ID)
		if detailErr != nil {
			return false
		}
		for _, task := range detail.Tasks {
			if task.ID == betaTaskID && task.PlacementDiagnostic != nil && len(task.PlacementDiagnostic.Unsatisfied) > 0 {
				return strings.Contains(task.PlacementDiagnostic.Unsatisfied[0].Detail, "clone_repository")
			}
		}
		return false
	}, 3*time.Second, 25*time.Millisecond)

	cloneRawArgs, err := json.Marshal(CloneRepositoryInput{
		NodeID:        "fabrica",
		RepositoryURL: betaCloneURL,
	})
	require.NoError(t, err)
	cloneRawResult, err := service.ExecuteOrchestratorTool(t.Context(), caller, "clone_repository", cloneRawArgs)
	require.NoError(t, err)

	var cloneResponse CloneRepositoryResponse
	require.NoError(t, json.Unmarshal(cloneRawResult, &cloneResponse))
	assert.True(t, cloneResponse.Created)
	expectedBetaPath := filepath.Join(workspaceRoot, "beta-service")
	assert.Equal(t, expectedBetaPath, cloneResponse.Repository.Path)
	assert.Len(t, cloneResponse.Report.Repositories, 2)

	// Waiting beta-task automatically wakes up and runs in an isolated git worktree of the newly cloned repository!
	betaRun := waitForTaskRunOutput(betaTaskID)
	assert.Contains(t, betaRun.Workspace, filepath.Join(workspaceRoot, ".orchestra", "worktrees"))
	assert.Contains(t, betaRun.Output, "cwd="+betaRun.Workspace)

	// 4. Out-of-band checkout created directly on disk is discovered via refresh_repositories and usable by relative path.
	gammaCheckoutPath := createLocalCheckout("nested/gamma-tool", "https://github.com/cafecito-games/gamma-tool.git", "main")

	refreshRawResult, err := service.ExecuteOrchestratorTool(t.Context(), caller, "refresh_repositories", json.RawMessage(`{}`))
	require.NoError(t, err)
	var refreshResponse RefreshRepositoriesResponse
	require.NoError(t, json.Unmarshal(refreshRawResult, &refreshResponse))
	require.Len(t, refreshResponse.Nodes, 1)
	assert.True(t, refreshResponse.Nodes[0].Synced)
	require.NotNil(t, refreshResponse.Nodes[0].Report)
	assert.Len(t, refreshResponse.Nodes[0].Report.Repositories, 3)

	gammaSubmission, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "gamma-by-relative-path",
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID: "gamma-task",
			Title:        "Run inside nested/gamma-tool by relative path",
			Instructions: "Inspect gamma-tool",
			Requirements: protocol.ExecutionRequirements{
				Workspace: &protocol.WorkspaceRequirements{
					Path: "nested/gamma-tool",
				},
			},
		}},
	})
	require.NoError(t, err)
	gammaRun := waitForTaskRunOutput(gammaSubmission.ClientTaskMap["gamma-task"])
	assert.Equal(t, gammaCheckoutPath, gammaRun.Workspace)
	assert.Contains(t, gammaRun.Output, "cwd="+gammaCheckoutPath)
}
