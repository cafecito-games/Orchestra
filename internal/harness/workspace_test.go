package harness_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthorizeWorkspace_RejectsRelativePath(t *testing.T) {
	allowedRoot := t.TempDir()
	canonicalRoot, err := filepath.EvalSymlinks(allowedRoot)
	require.NoError(t, err)

	_, err = harness.AuthorizeWorkspace("relative/workspace/path", []string{canonicalRoot})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "workspace must be an absolute path")
}

func TestAuthorizeWorkspace_RejectsPathOutsideAllowedRoots(t *testing.T) {
	allowedRoot := t.TempDir()
	canonicalRoot, err := filepath.EvalSymlinks(allowedRoot)
	require.NoError(t, err)

	outsideDirectory := t.TempDir()
	canonicalOutside, err := filepath.EvalSymlinks(outsideDirectory)
	require.NoError(t, err)

	_, err = harness.AuthorizeWorkspace(canonicalOutside, []string{canonicalRoot})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is outside this slave node's allowed roots")
}

func TestAuthorizeWorkspace_RejectsSymlinkPointingOutsideAllowedRoots(t *testing.T) {
	allowedRoot := t.TempDir()
	canonicalRoot, err := filepath.EvalSymlinks(allowedRoot)
	require.NoError(t, err)

	outsideDirectory := t.TempDir()
	canonicalOutside, err := filepath.EvalSymlinks(outsideDirectory)
	require.NoError(t, err)

	symlinkPath := filepath.Join(canonicalRoot, "escape-link")
	require.NoError(t, os.Symlink(canonicalOutside, symlinkPath))

	_, err = harness.AuthorizeWorkspace(symlinkPath, []string{canonicalRoot})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is outside this slave node's allowed roots")
}

func TestAuthorizeWorkspace_AcceptsValidWorkspaceAndInternalSymlink(t *testing.T) {
	allowedRoot := t.TempDir()
	canonicalRoot, err := filepath.EvalSymlinks(allowedRoot)
	require.NoError(t, err)

	projectDirectory := filepath.Join(canonicalRoot, "project")
	require.NoError(t, os.MkdirAll(projectDirectory, 0o755))

	authorized, err := harness.AuthorizeWorkspace(projectDirectory, []string{canonicalRoot})
	require.NoError(t, err)
	assert.Equal(t, projectDirectory, authorized)

	symlinkPath := filepath.Join(canonicalRoot, "project-link")
	require.NoError(t, os.Symlink(projectDirectory, symlinkPath))

	authorizedFromSymlink, err := harness.AuthorizeWorkspace(symlinkPath, []string{canonicalRoot})
	require.NoError(t, err)
	assert.Equal(t, projectDirectory, authorizedFromSymlink)
}
