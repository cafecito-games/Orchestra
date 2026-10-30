package safepath_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/safepath"
	"github.com/stretchr/testify/require"
)

func canonicalTempDirectory(t *testing.T) string {
	t.Helper()
	raw := t.TempDir()
	resolved, err := filepath.EvalSymlinks(raw)
	require.NoError(t, err)
	return resolved
}

func TestEnsureAndVerifyDirectoryWithinRoot(t *testing.T) {
	root := canonicalTempDirectory(t)
	nested := filepath.Join(root, "alpha", "beta", "gamma")

	created, err := safepath.EnsureDirectoryWithinRoot(root, nested, 0o700)
	require.NoError(t, err)
	require.Equal(t, nested, created)

	verified, err := safepath.VerifyDirectoryWithinRoot(root, nested)
	require.NoError(t, err)
	require.Equal(t, nested, verified)
}

func TestRefusesEscapeAndSymlinkAncestors(t *testing.T) {
	root := canonicalTempDirectory(t)
	outside := canonicalTempDirectory(t)

	_, err := safepath.EnsureDirectoryWithinRoot(root, filepath.Join(outside, "child"), 0o700)
	require.Error(t, err)

	symlinkPath := filepath.Join(root, "escape")
	require.NoError(t, os.Symlink(outside, symlinkPath))

	_, err = safepath.EnsureDirectoryWithinRoot(root, filepath.Join(symlinkPath, "nested"), 0o700)
	require.Error(t, err)

	_, err = safepath.VerifyDirectoryWithinRoot(root, symlinkPath)
	require.Error(t, err)

	regularFile := filepath.Join(root, "notes.txt")
	require.NoError(t, os.WriteFile(regularFile, []byte("hello"), 0o600))
	verifiedPath, fileInfo, err := safepath.VerifyRegularFileWithinRoot(root, regularFile)
	require.NoError(t, err)
	require.Equal(t, regularFile, verifiedPath)
	require.Equal(t, int64(5), fileInfo.Size())

	fileLink := filepath.Join(root, "notes-link.txt")
	require.NoError(t, os.Symlink(regularFile, fileLink))
	_, _, err = safepath.VerifyRegularFileWithinRoot(root, fileLink)
	require.Error(t, err)
}
