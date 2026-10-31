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
