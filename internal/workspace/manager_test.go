package workspace_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/workspace"
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

	operationContext := context.Background()
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
	operationContext := context.Background()

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
	require.Equal(t, "active", provisionOutcome.Status)
	require.Equal(t, grant.WorktreePath, provisionOutcome.Path)
	require.True(t, protocol.IsResolvedRevision(provisionOutcome.ResolvedBaseRevision))

	grant.Status = "active"
	grant.ResolvedBaseRevision = provisionOutcome.ResolvedBaseRevision
	adoptedOutcome := manager.Provision(operationContext, grant, "task-1", "run-1")
	require.Equal(t, "active", adoptedOutcome.Status)

	grant.Status = "released"
	cleanupOutcome := manager.Cleanup(operationContext, grant, "run-1", workspace.ModeRun, nil)
	require.Equal(t, "cleaned", cleanupOutcome.Status)

	_, statErr := os.Stat(grant.WorktreePath)
	require.True(t, os.IsNotExist(statErr))
	releaseOwnership()
}

func TestRetainsDirtyGitWorktree(t *testing.T) {
	rootPath, repositoryPath, gitTool := setupGitRepository(t)
	manager := workspace.NewManager([]string{rootPath}, gitTool)
	operationContext := context.Background()

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
	require.Equal(t, "active", provisionOutcome.Status)

	require.NoError(t, os.WriteFile(filepath.Join(grant.WorktreePath, "README.md"), []byte("modified\n"), 0o644))

	grant.Status = "released"
	cleanupOutcome := manager.Cleanup(operationContext, grant, "run-2", workspace.ModeRun, nil)
	require.Equal(t, "retained", cleanupOutcome.Status)
	require.Equal(t, "dirty", cleanupOutcome.RetentionReason)
}

func TestExclusiveExistingLease(t *testing.T) {
	rootPath, repositoryPath, gitTool := setupGitRepository(t)
	manager := workspace.NewManager([]string{rootPath}, gitTool)
	operationContext := context.Background()

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
	require.Equal(t, "active", firstOutcome.Status)

	secondOutcome := manager.Provision(operationContext, secondGrant, "task-2", "run-2")
	require.Equal(t, "failed", secondOutcome.Status)

	firstGrant.Status = "released"
	cleanOutcome := manager.Cleanup(operationContext, firstGrant, "run-1", workspace.ModeRun, nil)
	require.Equal(t, "cleaned", cleanOutcome.Status)

	secondOutcomeAfterRelease := manager.Provision(operationContext, secondGrant, "task-2", "run-2")
	require.Equal(t, "active", secondOutcomeAfterRelease.Status)
}
