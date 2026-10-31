package workspace_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/workspace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoverRepositories_MultiRepoWorkspaceAndConfinement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("workspace discovery expects Unix-style canonical paths")
	}

	gitTool, err := workspace.FindGit()
	require.NoError(t, err)

	rawRoot := t.TempDir()
	canonicalRoot, err := filepath.EvalSymlinks(rawRoot)
	require.NoError(t, err)

	createRepo := func(relativeDirectory string, remotes map[string]string, branch string) string {
		repositoryPath := filepath.Join(canonicalRoot, filepath.FromSlash(relativeDirectory))
		require.NoError(t, os.MkdirAll(repositoryPath, 0o755))
		_, runErr := gitTool.Run(t.Context(), repositoryPath, "init", "--initial-branch="+branch)
		require.NoError(t, runErr)
		for remoteName, remoteURL := range remotes {
			_, runErr = gitTool.Run(t.Context(), repositoryPath, "remote", "add", remoteName, remoteURL)
			require.NoError(t, runErr)
		}
		require.NoError(t, os.WriteFile(filepath.Join(repositoryPath, "README.md"), []byte("# "+relativeDirectory+"\n"), 0o644))
		_, runErr = gitTool.Run(t.Context(), repositoryPath, "add", "README.md")
		require.NoError(t, runErr)
		_, runErr = gitTool.Run(t.Context(), repositoryPath, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "initial")
		require.NoError(t, runErr)
		return repositoryPath
	}

	orchestraPath := createRepo("Orchestra", map[string]string{
		"origin":   "git@github.com:cafecito-games/Orchestra.git",
		"upstream": "https://github.com/upstream-org/Orchestra.git",
	}, "main")
	uzirPath := createRepo("games/uzir", map[string]string{
		"origin": "https://github.com/cafecito-games/uzir.git",
	}, "develop")

	// Symlinked repository pointing outside workspace root must be ignored.
	outsideRoot := t.TempDir()
	outsideCanonical, err := filepath.EvalSymlinks(outsideRoot)
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), outsideCanonical, "init", "--initial-branch=main")
	require.NoError(t, err)
	require.NoError(t, os.Symlink(outsideCanonical, filepath.Join(canonicalRoot, "escaped-symlink")))

	manager := workspace.NewManager([]string{canonicalRoot}, gitTool)
	discovered, err := manager.DiscoverRepositories(t.Context())
	require.NoError(t, err)
	require.Len(t, discovered, 2)

	assert.Equal(t, canonicalRoot, discovered[0].Root)
	assert.Equal(t, orchestraPath, discovered[0].Path)
	assert.Equal(t, "Orchestra", discovered[0].RelativePath)
	assert.Equal(t, "github.com:cafecito-games/Orchestra", discovered[0].Repository)
	assert.Equal(t, []string{"github.com:cafecito-games/Orchestra", "https://github.com/upstream-org/Orchestra"}, discovered[0].Remotes)
	assert.Equal(t, "main", discovered[0].HeadBranch)
	assert.True(t, discovered[0].MatchesRepository("https://github.com/cafecito-games/Orchestra"))
	assert.True(t, discovered[0].MatchesRepository("https://github.com/upstream-org/Orchestra.git"))
	assert.False(t, discovered[0].MatchesRepository("https://github.com/cafecito-games/uzir"))

	assert.Equal(t, uzirPath, discovered[1].Path)
	assert.Equal(t, "games/uzir", discovered[1].RelativePath)
	assert.Equal(t, "https://github.com/cafecito-games/uzir", discovered[1].Repository)
	assert.Equal(t, "develop", discovered[1].HeadBranch)
}

func TestCloneRepository_HTTPCloneIdempotencyAndSafety(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("workspace clone expects Unix-style canonical paths")
	}

	gitTool, err := workspace.FindGit()
	require.NoError(t, err)

	remoteHostingRoot, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)

	seedWorktree := filepath.Join(remoteHostingRoot, "seed")
	require.NoError(t, os.MkdirAll(seedWorktree, 0o755))
	_, err = gitTool.Run(t.Context(), seedWorktree, "init", "--initial-branch=main")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(seedWorktree, "hello.txt"), []byte("main branch\n"), 0o644))
	_, err = gitTool.Run(t.Context(), seedWorktree, "add", "hello.txt")
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), seedWorktree, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "main commit")
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), seedWorktree, "checkout", "-b", "release")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(seedWorktree, "release.txt"), []byte("release branch\n"), 0o644))
	_, err = gitTool.Run(t.Context(), seedWorktree, "add", "release.txt")
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), seedWorktree, "-c", "user.name=Test", "-c", "user.email=test@example.com", "commit", "-m", "release commit")
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), seedWorktree, "checkout", "main")
	require.NoError(t, err)

	bareRepository := filepath.Join(remoteHostingRoot, "cafecito-games", "sample-project.git")
	require.NoError(t, os.MkdirAll(filepath.Dir(bareRepository), 0o755))
	_, err = gitTool.Run(t.Context(), remoteHostingRoot, "clone", "--bare", seedWorktree, bareRepository)
	require.NoError(t, err)
	_, err = gitTool.Run(t.Context(), bareRepository, "update-server-info")
	require.NoError(t, err)

	httpServer := httptest.NewServer(http.FileServer(http.Dir(remoteHostingRoot)))
	defer httpServer.Close()

	cloneURL := httpServer.URL + "/cafecito-games/sample-project.git"

	workspaceRoot, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	manager := workspace.NewManager([]string{workspaceRoot}, gitTool)

	outcome, err := manager.CloneRepository(t.Context(), workspace.CloneRequest{
		RepositoryURL: cloneURL,
	})
	require.NoError(t, err)
	assert.True(t, outcome.Created)
	assert.Equal(t, "sample-project", outcome.Repository.RelativePath)
	assert.Equal(t, filepath.Join(workspaceRoot, "sample-project"), outcome.Repository.Path)
	assert.Equal(t, "main", outcome.Repository.HeadBranch)
	require.Len(t, outcome.Repositories, 1)

	// Re-cloning the same repository without explicit directory returns existing checkout idempotently.
	secondOutcome, err := manager.CloneRepository(t.Context(), workspace.CloneRequest{
		RepositoryURL: cloneURL,
	})
	require.NoError(t, err)
	assert.False(t, secondOutcome.Created)
	assert.Equal(t, outcome.Repository.Path, secondOutcome.Repository.Path)

	// Cloning a specific branch into a nested directory succeeds.
	branchOutcome, err := manager.CloneRepository(t.Context(), workspace.CloneRequest{
		RepositoryURL: cloneURL,
		Directory:     "nested/sample-release",
		Branch:        "release",
	})
	require.NoError(t, err)
	assert.True(t, branchOutcome.Created)
	assert.Equal(t, "nested/sample-release", branchOutcome.Repository.RelativePath)
	assert.Equal(t, "release", branchOutcome.Repository.HeadBranch)
	assert.FileExists(t, filepath.Join(workspaceRoot, "nested", "sample-release", "release.txt"))
	require.Len(t, branchOutcome.Repositories, 2)

	// Reject unsafe inputs and confinement violations.
	_, err = manager.CloneRepository(t.Context(), workspace.CloneRequest{
		RepositoryURL: "--upload-pack=evil",
	})
	require.Error(t, err)

	_, err = manager.CloneRepository(t.Context(), workspace.CloneRequest{
		RepositoryURL: "ext::sh -c touch%20pwned",
	})
	require.Error(t, err)

	_, err = manager.CloneRepository(t.Context(), workspace.CloneRequest{
		RepositoryURL: "file://" + bareRepository,
	})
	require.Error(t, err)

	_, err = manager.CloneRepository(t.Context(), workspace.CloneRequest{
		RepositoryURL: cloneURL,
		Directory:     "../outside",
	})
	require.Error(t, err)

	_, err = manager.CloneRepository(t.Context(), workspace.CloneRequest{
		RepositoryURL: cloneURL,
		WorkspaceRoot: remoteHostingRoot,
	})
	require.Error(t, err)

	report := protocol.NodeRepositoryReport{
		NodeID:       "node-test",
		Roots:        []string{workspaceRoot},
		Repositories: branchOutcome.Repositories,
		ObservedAt:   "2026-10-31T13:30:00Z",
	}
	require.NoError(t, report.Validate())
}
