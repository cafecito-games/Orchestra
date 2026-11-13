package workspace_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/workspace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupGitRepository(t *testing.T) (string, string, *workspace.Git) {
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
	require.NoError(t, os.WriteFile(filepath.Join(repositoryPath, "README.md"), []byte("# Orchestra\n"), 0o644))
	_, err = gitTool.Run(operationContext, repositoryPath, "add", "README.md")
	require.NoError(t, err)
	_, err = gitTool.Run(operationContext, repositoryPath, "commit", "-m", "initial commit")
	require.NoError(t, err)

	return rootPath, repositoryPath, gitTool
}

func TestProvisionAndCleanGitWorktree(t *testing.T) {
	rootPath, repositoryPath, gitTool := setupGitRepository(t)
	manager := workspace.NewManager([]string{rootPath}, gitTool)
	operationContext := t.Context()

	grant := protocol.WorkspaceLeaseGrant{
		ID:           "lease-1",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupWhenUnchanged,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("main"),
		Branch:       protocol.WorkspaceLeaseBranch("task-1", "run-1"),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), "lease-1"),
	}

	releaseOwnership, err := manager.Own(grant)
	require.NoError(t, err)

	_, err = manager.Own(grant)
	require.ErrorIs(t, err, workspace.ErrLeaseOwned)

	secondManager := workspace.NewManager([]string{rootPath}, gitTool)
	_, err = secondManager.Own(grant)
	require.ErrorIs(t, err, workspace.ErrLeaseOwned)

	provisionOutcome := manager.Provision(operationContext, grant, "task-1", "run-1")
	assert.Equal(t, "active", provisionOutcome.Status)
	assert.Equal(t, grant.WorktreePath, provisionOutcome.Path)
	assert.True(t, protocol.IsResolvedRevision(provisionOutcome.ResolvedBaseRevision))

	grant.Status = "active"
	grant.ResolvedBaseRevision = provisionOutcome.ResolvedBaseRevision
	adoptedOutcome := manager.Provision(operationContext, grant, "task-1", "run-1")
	assert.Equal(t, "active", adoptedOutcome.Status)

	grant.Status = "released"
	cleanupOutcome := manager.Cleanup(operationContext, grant, "run-1", workspace.ModeRun, nil)
	assert.Equal(t, "cleaned", cleanupOutcome.Status)

	_, statErr := os.Stat(grant.WorktreePath)
	assert.True(t, os.IsNotExist(statErr))
	releaseOwnership()
}

func TestRetainsDirtyGitWorktree(t *testing.T) {
	rootPath, repositoryPath, gitTool := setupGitRepository(t)
	manager := workspace.NewManager([]string{rootPath}, gitTool)
	operationContext := t.Context()

	grant := protocol.WorkspaceLeaseGrant{
		ID:           "lease-dirty",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupWhenUnchanged,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("main"),
		Branch:       protocol.WorkspaceLeaseBranch("task-2", "run-2"),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), "lease-dirty"),
	}

	releaseOwnership, err := manager.Own(grant)
	require.NoError(t, err)
	defer releaseOwnership()

	provisionOutcome := manager.Provision(operationContext, grant, "task-2", "run-2")
	assert.Equal(t, "active", provisionOutcome.Status)

	require.NoError(t, os.WriteFile(filepath.Join(grant.WorktreePath, "README.md"), []byte("modified\n"), 0o644))

	grant.Status = "released"
	cleanupOutcome := manager.Cleanup(operationContext, grant, "run-2", workspace.ModeRun, nil)
	assert.Equal(t, "retained", cleanupOutcome.Status)
	assert.Equal(t, "dirty", cleanupOutcome.RetentionReason)
}

func TestExclusiveExistingLease(t *testing.T) {
	rootPath, repositoryPath, gitTool := setupGitRepository(t)
	manager := workspace.NewManager([]string{rootPath}, gitTool)
	operationContext := t.Context()

	firstGrant := protocol.WorkspaceLeaseGrant{
		ID:           "lease-exclusive-1",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationExclusiveExisting,
		Cleanup:      protocol.WorkspaceCleanupRetain,
		Root:         rootPath,
		SourcePath:   repositoryPath,
		WorktreePath: repositoryPath,
	}
	secondGrant := protocol.WorkspaceLeaseGrant{
		ID:           "lease-exclusive-2",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationExclusiveExisting,
		Cleanup:      protocol.WorkspaceCleanupRetain,
		Root:         rootPath,
		SourcePath:   repositoryPath,
		WorktreePath: repositoryPath,
	}

	firstOutcome := manager.Provision(operationContext, firstGrant, "task-1", "run-1")
	assert.Equal(t, "active", firstOutcome.Status)

	secondOutcome := manager.Provision(operationContext, secondGrant, "task-2", "run-2")
	assert.Equal(t, "failed", secondOutcome.Status)

	firstGrant.Status = "released"
	cleanOutcome := manager.Cleanup(operationContext, firstGrant, "run-1", workspace.ModeRun, nil)
	assert.Equal(t, "cleaned", cleanOutcome.Status)

	secondOutcomeAfterRelease := manager.Provision(operationContext, secondGrant, "task-2", "run-2")
	assert.Equal(t, "active", secondOutcomeAfterRelease.Status)
}

func TestProvision_NonExistentBaseRevision(t *testing.T) {
	rootPath, repositoryPath, gitTool := setupGitRepository(t)
	manager := workspace.NewManager([]string{rootPath}, gitTool)

	grant := protocol.WorkspaceLeaseGrant{
		ID:           "lease-nonexistent-base",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupWhenUnchanged,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("nonexistent-ref"),
		Branch:       protocol.WorkspaceLeaseBranch("task-nonexist", "run-1"),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), "lease-nonexistent-base"),
	}

	releaseOwnership, err := manager.Own(grant)
	require.NoError(t, err)
	defer releaseOwnership()

	provisionOutcome := manager.Provision(t.Context(), grant, "task-nonexist", "run-1")
	assert.Equal(t, "failed", provisionOutcome.Status)
	assert.Contains(t, provisionOutcome.Detail, "does not exist")
}

func TestProvision_ConcurrentSameRepository(t *testing.T) {
	rootPath, repositoryPath, gitTool := setupGitRepository(t)
	manager := workspace.NewManager([]string{rootPath}, gitTool)

	firstGrant := protocol.WorkspaceLeaseGrant{
		ID:           "lease-concurrent-1",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupWhenUnchanged,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("main"),
		Branch:       protocol.WorkspaceLeaseBranch("task-conc-1", "run-1"),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), "lease-concurrent-1"),
	}
	secondGrant := protocol.WorkspaceLeaseGrant{
		ID:           "lease-concurrent-2",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupWhenUnchanged,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("main"),
		Branch:       protocol.WorkspaceLeaseBranch("task-conc-2", "run-2"),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), "lease-concurrent-2"),
	}

	releaseFirst, err := manager.Own(firstGrant)
	require.NoError(t, err)
	defer releaseFirst()

	releaseSecond, err := manager.Own(secondGrant)
	require.NoError(t, err)
	defer releaseSecond()

	var waitGroup sync.WaitGroup
	var firstOutcome, secondOutcome workspace.Outcome
	waitGroup.Add(2)

	go func() {
		defer waitGroup.Done()
		firstOutcome = manager.Provision(t.Context(), firstGrant, "task-conc-1", "run-1")
	}()

	go func() {
		defer waitGroup.Done()
		secondOutcome = manager.Provision(t.Context(), secondGrant, "task-conc-2", "run-2")
	}()

	waitGroup.Wait()

	assert.Equal(t, "active", firstOutcome.Status)
	assert.Equal(t, "active", secondOutcome.Status)
	assert.NotEqual(t, firstOutcome.Path, secondOutcome.Path)

	firstGrant.Status = "released"
	cleanFirst := manager.Cleanup(t.Context(), firstGrant, "run-1", workspace.ModeRun, nil)
	assert.Equal(t, "cleaned", cleanFirst.Status)

	secondGrant.Status = "released"
	cleanSecond := manager.Cleanup(t.Context(), secondGrant, "run-2", workspace.ModeRun, nil)
	assert.Equal(t, "cleaned", cleanSecond.Status)
}

func TestCleanupPolicies_RetainVersusWhenUnchanged(t *testing.T) {
	rootPath, repositoryPath, gitTool := setupGitRepository(t)
	manager := workspace.NewManager([]string{rootPath}, gitTool)

	// 1. WorkspaceCleanupRetain preserves clean worktree under ModeRun
	retainGrant := protocol.WorkspaceLeaseGrant{
		ID:           "lease-policy-retain",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupRetain,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("main"),
		Branch:       protocol.WorkspaceLeaseBranch("task-retain", "run-1"),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), "lease-policy-retain"),
	}

	releaseRetain, err := manager.Own(retainGrant)
	require.NoError(t, err)
	defer releaseRetain()

	provisionOutcome := manager.Provision(t.Context(), retainGrant, "task-retain", "run-1")
	require.Equal(t, "active", provisionOutcome.Status)

	retainGrant.Status = "released"
	cleanupRun := manager.Cleanup(t.Context(), retainGrant, "run-1", workspace.ModeRun, nil)
	assert.Equal(t, "retained", cleanupRun.Status)
	assert.Equal(t, "policy", cleanupRun.RetentionReason)

	// Worktree directory is preserved on disk
	_, statErr := os.Stat(retainGrant.WorktreePath)
	require.NoError(t, statErr)

	// Operator mode allows cleaning even with WorkspaceCleanupRetain policy
	cleanupOperator := manager.Cleanup(t.Context(), retainGrant, "run-1", workspace.ModeOperator, nil)
	assert.Equal(t, "cleaned", cleanupOperator.Status)
	_, statErr = os.Stat(retainGrant.WorktreePath)
	assert.True(t, os.IsNotExist(statErr))
}

func provisionRetainedAuthorWorktree(
	t *testing.T,
	manager *workspace.Manager,
	gitTool *workspace.Git,
	rootPath string,
	repositoryPath string,
	leaseID string,
	taskID string,
	runID string,
	fileName string,
	fileContent string,
) protocol.WorkspaceLeaseGrant {
	t.Helper()
	operationContext := t.Context()
	grant := protocol.WorkspaceLeaseGrant{
		ID:           leaseID,
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupRetain,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("main"),
		Branch:       protocol.WorkspaceLeaseBranch(taskID, runID),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), leaseID),
	}

	releaseOwner, err := manager.Own(grant)
	require.NoError(t, err)
	defer releaseOwner()

	provisionOutcome := manager.Provision(operationContext, grant, taskID, runID)
	require.Equal(t, "active", provisionOutcome.Status)
	grant.ResolvedBaseRevision = provisionOutcome.ResolvedBaseRevision
	grant.Status = "active"

	_, err = gitTool.Run(operationContext, grant.WorktreePath, "config", "user.email", "author@orchestra.local")
	require.NoError(t, err)
	_, err = gitTool.Run(operationContext, grant.WorktreePath, "config", "user.name", "Author Worker")
	require.NoError(t, err)

	targetFile := filepath.Join(grant.WorktreePath, fileName)
	require.NoError(t, os.MkdirAll(filepath.Dir(targetFile), 0o755))
	require.NoError(t, os.WriteFile(targetFile, []byte(fileContent), 0o644))
	_, err = gitTool.Run(operationContext, grant.WorktreePath, "add", fileName)
	require.NoError(t, err)
	_, err = gitTool.Run(operationContext, grant.WorktreePath, "commit", "-m", "author change "+fileName)
	require.NoError(t, err)

	grant.Status = "released"
	cleanupOutcome := manager.Cleanup(operationContext, grant, runID, workspace.ModeRun, nil)
	require.Equal(t, "retained", cleanupOutcome.Status)
	grant.Status = "retained"
	return grant
}

func TestLandLease_Scenarios(t *testing.T) {
	testCases := []struct {
		name                  string
		mode                  string
		targetBranch          string
		cleanupAfterLand      bool
		prepareRepository     func(t *testing.T, repositoryPath string, grant protocol.WorkspaceLeaseGrant, gitTool *workspace.Git)
		expectedStatus        string
		expectedLanded        bool
		expectedCleaned       bool
		expectedRetained      bool
		expectedCommitsLanded int
		expectedReason        string
		expectedConflictFile  string
		expectedDirtyFile     string
	}{
		{
			name:                  "fast-forward default mode preserves worktree when cleanupAfterLand is false",
			mode:                  "",
			cleanupAfterLand:      false,
			expectedStatus:        protocol.WorkspaceLandingStatusLanded,
			expectedLanded:        true,
			expectedCleaned:       false,
			expectedRetained:      true,
			expectedCommitsLanded: 1,
		},
		{
			name:                  "fast-forward with cleanupAfterLand removes worktree and branch",
			mode:                  protocol.WorkspaceLandingModeFastForward,
			cleanupAfterLand:      true,
			expectedStatus:        protocol.WorkspaceLandingStatusLanded,
			expectedLanded:        true,
			expectedCleaned:       true,
			expectedRetained:      false,
			expectedCommitsLanded: 1,
		},
		{
			name:             "merge-ff-only refuses diverged target branch and preserves worktree",
			mode:             protocol.WorkspaceLandingModeMergeFFOnly,
			cleanupAfterLand: true,
			prepareRepository: func(t *testing.T, repositoryPath string, _ protocol.WorkspaceLeaseGrant, gitTool *workspace.Git) {
				require.NoError(t, os.WriteFile(filepath.Join(repositoryPath, "other.txt"), []byte("main advance\n"), 0o644))
				_, err := gitTool.Run(t.Context(), repositoryPath, "add", "other.txt")
				require.NoError(t, err)
				_, err = gitTool.Run(t.Context(), repositoryPath, "commit", "-m", "advance main")
				require.NoError(t, err)
			},
			expectedStatus:   protocol.WorkspaceLandingStatusConflict,
			expectedLanded:   false,
			expectedCleaned:  false,
			expectedRetained: true,
			expectedReason:   "non_fast_forward",
		},
		{
			name:             "cherry-pick replays commit onto diverged main and cleans worktree",
			mode:             protocol.WorkspaceLandingModeCherryPick,
			cleanupAfterLand: true,
			prepareRepository: func(t *testing.T, repositoryPath string, _ protocol.WorkspaceLeaseGrant, gitTool *workspace.Git) {
				require.NoError(t, os.WriteFile(filepath.Join(repositoryPath, "other.txt"), []byte("main advance\n"), 0o644))
				_, err := gitTool.Run(t.Context(), repositoryPath, "add", "other.txt")
				require.NoError(t, err)
				_, err = gitTool.Run(t.Context(), repositoryPath, "commit", "-m", "advance main")
				require.NoError(t, err)
			},
			expectedStatus:        protocol.WorkspaceLandingStatusLanded,
			expectedLanded:        true,
			expectedCleaned:       true,
			expectedRetained:      false,
			expectedCommitsLanded: 1,
		},
		{
			name:             "cherry-pick onto non-checked-out target branch succeeds cleanly",
			mode:             protocol.WorkspaceLandingModeCherryPick,
			targetBranch:     "release",
			cleanupAfterLand: true,
			prepareRepository: func(t *testing.T, repositoryPath string, _ protocol.WorkspaceLeaseGrant, gitTool *workspace.Git) {
				_, err := gitTool.Run(t.Context(), repositoryPath, "branch", "release", "HEAD")
				require.NoError(t, err)
			},
			expectedStatus:        protocol.WorkspaceLandingStatusLanded,
			expectedLanded:        true,
			expectedCleaned:       true,
			expectedRetained:      false,
			expectedCommitsLanded: 1,
		},
		{
			name:             "cherry-pick conflict aborts cleanly and preserves worktree",
			mode:             protocol.WorkspaceLandingModeCherryPick,
			cleanupAfterLand: true,
			prepareRepository: func(t *testing.T, repositoryPath string, _ protocol.WorkspaceLeaseGrant, gitTool *workspace.Git) {
				require.NoError(t, os.WriteFile(filepath.Join(repositoryPath, "feature.txt"), []byte("conflicting main content\n"), 0o644))
				_, err := gitTool.Run(t.Context(), repositoryPath, "add", "feature.txt")
				require.NoError(t, err)
				_, err = gitTool.Run(t.Context(), repositoryPath, "commit", "-m", "conflicting commit on main")
				require.NoError(t, err)
			},
			expectedStatus:       protocol.WorkspaceLandingStatusConflict,
			expectedLanded:       false,
			expectedCleaned:      false,
			expectedRetained:     true,
			expectedReason:       "merge_conflict",
			expectedConflictFile: "feature.txt",
		},
		{
			name:             "dirty source checkout refuses safely and preserves worktree",
			mode:             protocol.WorkspaceLandingModeFastForward,
			cleanupAfterLand: true,
			prepareRepository: func(t *testing.T, repositoryPath string, _ protocol.WorkspaceLeaseGrant, _ *workspace.Git) {
				require.NoError(t, os.WriteFile(filepath.Join(repositoryPath, "uncommitted.txt"), []byte("dirty\n"), 0o644))
			},
			expectedStatus:    protocol.WorkspaceLandingStatusDirty,
			expectedLanded:    false,
			expectedCleaned:   false,
			expectedRetained:  true,
			expectedReason:    "source_dirty",
			expectedDirtyFile: "uncommitted.txt",
		},
		{
			name:             "dirty lease worktree refuses safely and preserves worktree",
			mode:             protocol.WorkspaceLandingModeFastForward,
			cleanupAfterLand: true,
			prepareRepository: func(t *testing.T, _ string, grant protocol.WorkspaceLeaseGrant, _ *workspace.Git) {
				require.NoError(t, os.WriteFile(filepath.Join(grant.WorktreePath, "wip.txt"), []byte("uncommitted wip\n"), 0o644))
			},
			expectedStatus:    protocol.WorkspaceLandingStatusDirty,
			expectedLanded:    false,
			expectedCleaned:   false,
			expectedRetained:  true,
			expectedReason:    "worktree_dirty",
			expectedDirtyFile: "wip.txt",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rootPath, repositoryPath, gitTool := setupGitRepository(t)
			manager := workspace.NewManager([]string{rootPath}, gitTool)
			grant := provisionRetainedAuthorWorktree(
				t,
				manager,
				gitTool,
				rootPath,
				repositoryPath,
				"lease-land-case",
				"task-land",
				"run-land",
				"feature.txt",
				"author feature line\n",
			)

			if testCase.prepareRepository != nil {
				testCase.prepareRepository(t, repositoryPath, grant, gitTool)
			}

			releaseOwner, ownErr := manager.Own(grant)
			require.NoError(t, ownErr)
			defer releaseOwner()

			result, landErr := manager.LandLease(t.Context(), protocol.WorkspaceLeaseLandRequest{
				RequestID:        "land_req_1",
				Lease:            grant,
				Mode:             testCase.mode,
				TargetBranch:     testCase.targetBranch,
				CleanupAfterLand: testCase.cleanupAfterLand,
			})
			require.NoError(t, landErr)
			require.NoError(t, result.Validate())

			assert.Equal(t, testCase.expectedStatus, result.Status)
			assert.Equal(t, testCase.expectedLanded, result.Landed)
			assert.Equal(t, testCase.expectedCleaned, result.Cleaned)
			assert.Equal(t, testCase.expectedRetained, result.WorktreeRetained)
			assert.Equal(t, testCase.expectedCommitsLanded, result.CommitsLanded)

			if testCase.expectedReason != "" {
				assert.Equal(t, testCase.expectedReason, result.Reason)
			}
			if testCase.expectedConflictFile != "" {
				assert.Contains(t, result.Conflicts, testCase.expectedConflictFile)
				statusOut, statusErr := gitTool.Run(t.Context(), repositoryPath, "status", "--porcelain=v1", "-z")
				require.NoError(t, statusErr)
				assert.Empty(t, string(statusOut), "source checkout must be clean after conflict abort")
			}
			if testCase.expectedDirtyFile != "" {
				assert.Contains(t, result.DirtyFiles, testCase.expectedDirtyFile)
			}

			_, worktreeStatErr := os.Stat(grant.WorktreePath)
			if testCase.expectedRetained {
				require.NoError(t, worktreeStatErr, "retained worktree must remain on disk when landing fails or cleanupAfterLand is false")
			} else {
				assert.True(t, os.IsNotExist(worktreeStatErr), "worktree must be removed when cleanupAfterLand succeeds")
			}

			if testCase.expectedLanded && (testCase.targetBranch == "" || testCase.targetBranch == "main") {
				featureBytes, readErr := os.ReadFile(filepath.Join(repositoryPath, "feature.txt"))
				require.NoError(t, readErr)
				assert.Equal(t, "author feature line\n", string(featureBytes))
			}
		})
	}
}

func TestLandLease_AlreadyLandedIdempotentAndConfinement(t *testing.T) {
	rootPath, repositoryPath, gitTool := setupGitRepository(t)
	manager := workspace.NewManager([]string{rootPath}, gitTool)
	grant := provisionRetainedAuthorWorktree(
		t,
		manager,
		gitTool,
		rootPath,
		repositoryPath,
		"lease-idem",
		"task-idem",
		"run-idem",
		"idempotent.txt",
		"hello\n",
	)

	// Advance main independently so cherry-pick produces a distinct commit SHA.
	require.NoError(t, os.WriteFile(filepath.Join(repositoryPath, "main-only.txt"), []byte("main\n"), 0o644))
	_, err := gitTool.Run(t.Context(), repositoryPath, "add", "main-only.txt")
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), repositoryPath, "commit", "-m", "main change")
	require.NoError(t, err)

	releaseOwner, err := manager.Own(grant)
	require.NoError(t, err)
	defer releaseOwner()

	firstResult, err := manager.LandLease(t.Context(), protocol.WorkspaceLeaseLandRequest{
		Lease:            grant,
		Mode:             protocol.WorkspaceLandingModeCherryPick,
		CleanupAfterLand: false,
	})
	require.NoError(t, err)
	assert.True(t, firstResult.Landed)
	assert.False(t, firstResult.AlreadyLanded)
	assert.Equal(t, 1, firstResult.CommitsLanded)
	assert.True(t, firstResult.WorktreeRetained)

	secondResult, err := manager.LandLease(t.Context(), protocol.WorkspaceLeaseLandRequest{
		Lease:            grant,
		Mode:             protocol.WorkspaceLandingModeCherryPick,
		CleanupAfterLand: true,
	})
	require.NoError(t, err)
	assert.True(t, secondResult.Landed)
	assert.True(t, secondResult.AlreadyLanded)
	assert.Equal(t, 0, secondResult.CommitsLanded)
	assert.True(t, secondResult.Cleaned)
	assert.False(t, secondResult.WorktreeRetained)

	// Unauthorized root is rejected.
	otherManager := workspace.NewManager([]string{filepath.Join(rootPath, "other-root")}, gitTool)
	_, unauthorizedErr := otherManager.LandLease(t.Context(), protocol.WorkspaceLeaseLandRequest{
		Lease: grant,
	})
	require.Error(t, unauthorizedErr)
}

func TestSharedReadOnlySourceLease_ConcurrentInspectionAndValidation(t *testing.T) {
	rootPath, repositoryPath, gitTool := setupGitRepository(t)
	manager := workspace.NewManager([]string{rootPath}, gitTool)
	operationContext := t.Context()

	authorGrant := protocol.WorkspaceLeaseGrant{
		ID:           "lease-author",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupRetain,
		Repository:   "https://github.com/cafecito-games/Orchestra",
		Root:         rootPath,
		SourcePath:   repositoryPath,
		BaseRevision: protocol.WorkspaceLeaseBaseRef("main"),
		Branch:       protocol.WorkspaceLeaseBranch("task-author", "run-author"),
		WorktreePath: filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), "lease-author"),
	}

	releaseAuthor, err := manager.Own(authorGrant)
	require.NoError(t, err)
	defer releaseAuthor()

	authorOutcome := manager.Provision(operationContext, authorGrant, "task-author", "run-author")
	require.Equal(t, "active", authorOutcome.Status)
	authorGrant.Status = "active"
	authorGrant.ResolvedBaseRevision = authorOutcome.ResolvedBaseRevision

	require.NoError(t, os.WriteFile(filepath.Join(authorGrant.WorktreePath, "draft.txt"), []byte("author draft\n"), 0o644))

	// ResolveSourceLease validation table.
	validationCases := []struct {
		name         string
		managerRoots []string
		requirements protocol.WorkspaceRequirements
		expectError  string
	}{
		{
			name:         "resolves active source lease for read-only verifier",
			managerRoots: []string{rootPath},
			requirements: protocol.WorkspaceRequirements{
				SourceLeaseID: authorGrant.ID,
				Writable:      false,
			},
		},
		{
			name:         "rejects writable true with sourceLeaseId",
			managerRoots: []string{rootPath},
			requirements: protocol.WorkspaceRequirements{
				SourceLeaseID: authorGrant.ID,
				Writable:      true,
			},
			expectError: "requires writable=false",
		},
		{
			name:         "rejects mismatched root",
			managerRoots: []string{filepath.Join(rootPath, "different")},
			requirements: protocol.WorkspaceRequirements{
				SourceLeaseID: authorGrant.ID,
				Writable:      false,
			},
			expectError: "not authorized",
		},
		{
			name:         "rejects mismatched repository",
			managerRoots: []string{rootPath},
			requirements: protocol.WorkspaceRequirements{
				SourceLeaseID: authorGrant.ID,
				Repository:    "https://github.com/example/other",
				Writable:      false,
			},
			expectError: "does not match source lease repository",
		},
	}

	for _, testCase := range validationCases {
		t.Run(testCase.name, func(t *testing.T) {
			candidateManager := workspace.NewManager(testCase.managerRoots, gitTool)
			resolved, resolveErr := candidateManager.ResolveSourceLease(operationContext, authorGrant, testCase.requirements)
			if testCase.expectError != "" {
				require.Error(t, resolveErr)
				assert.Contains(t, resolveErr.Error(), testCase.expectError)
				return
			}
			require.NoError(t, resolveErr)
			assert.Equal(t, "active", resolved.Status)
			assert.Equal(t, authorGrant.WorktreePath, resolved.Path)
		})
	}

	// Two read-only verifiers concurrently own and provision against the same active author worktree without blocking or cloning.
	firstVerifierGrant := authorGrant
	firstVerifierGrant.ID = "lease-verifier-1"
	firstVerifierGrant.SourceLeaseID = authorGrant.ID
	firstVerifierGrant.ReadOnly = true
	firstVerifierGrant.Status = "requested"
	firstVerifierGrant.Cleanup = protocol.WorkspaceCleanupWhenUnchanged

	secondVerifierGrant := firstVerifierGrant
	secondVerifierGrant.ID = "lease-verifier-2"

	releaseFirstVerifier, err := manager.Own(firstVerifierGrant)
	require.NoError(t, err)
	releaseSecondVerifier, err := manager.Own(secondVerifierGrant)
	require.NoError(t, err)

	firstVerifierOutcome := manager.Provision(operationContext, firstVerifierGrant, "task-verifier-1", "run-verifier-1")
	secondVerifierOutcome := manager.Provision(operationContext, secondVerifierGrant, "task-verifier-2", "run-verifier-2")
	require.Equal(t, "active", firstVerifierOutcome.Status)
	require.Equal(t, "active", secondVerifierOutcome.Status)
	assert.Equal(t, authorGrant.WorktreePath, firstVerifierOutcome.Path)
	assert.Equal(t, authorGrant.WorktreePath, secondVerifierOutcome.Path)

	managedEntries, err := os.ReadDir(filepath.Join(rootPath, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory)))
	require.NoError(t, err)
	assert.Len(t, managedEntries, 1, "shared read-only verifiers must not create duplicate worktrees")

	// Operator cleanup on the source lease refuses to delete the worktree while verifiers are active.
	lockedCleanup := manager.Cleanup(operationContext, authorGrant, "run-author", workspace.ModeOperator, nil)
	assert.Equal(t, "retained", lockedCleanup.Status)
	assert.Equal(t, "locked", lockedCleanup.RetentionReason)

	// Cleaning up verifier leases leaves the author's worktree and files intact.
	firstVerifierGrant.Status = "released"
	assert.Equal(t, "cleaned", manager.Cleanup(operationContext, firstVerifierGrant, "run-verifier-1", workspace.ModeRun, nil).Status)
	releaseFirstVerifier()

	secondVerifierGrant.Status = "released"
	assert.Equal(t, "cleaned", manager.Cleanup(operationContext, secondVerifierGrant, "run-verifier-2", workspace.ModeRun, nil).Status)
	releaseSecondVerifier()

	draftContent, readErr := os.ReadFile(filepath.Join(authorGrant.WorktreePath, "draft.txt"))
	require.NoError(t, readErr)
	assert.Equal(t, "author draft\n", string(draftContent))
}
