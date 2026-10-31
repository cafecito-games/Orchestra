package safepath_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/safepath"
	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, nested, created)

	verified, err := safepath.VerifyDirectoryWithinRoot(root, nested)
	require.NoError(t, err)
	assert.Equal(t, nested, verified)
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
	assert.Equal(t, regularFile, verifiedPath)
	assert.Equal(t, int64(5), fileInfo.Size())

	fileLink := filepath.Join(root, "notes-link.txt")
	require.NoError(t, os.Symlink(regularFile, fileLink))
	_, _, err = safepath.VerifyRegularFileWithinRoot(root, fileLink)
	require.Error(t, err)
}

func TestRejectingRelativePaths(t *testing.T) {
	root := canonicalTempDirectory(t)

	_, err := safepath.EnsureDirectoryWithinRoot("relative/root", filepath.Join(root, "nested"), 0o700)
	require.Error(t, err)

	_, err = safepath.EnsureDirectoryWithinRoot(root, "relative/nested", 0o700)
	require.Error(t, err)

	_, err = safepath.VerifyDirectoryWithinRoot("relative/root", filepath.Join(root, "nested"))
	require.Error(t, err)

	_, err = safepath.VerifyDirectoryWithinRoot(root, "relative/nested")
	require.Error(t, err)

	_, _, err = safepath.VerifyRegularFileWithinRoot("relative/root", filepath.Join(root, "file.txt"))
	require.Error(t, err)

	_, _, err = safepath.VerifyRegularFileWithinRoot(root, "relative/file.txt")
	require.Error(t, err)
}

func TestRejectingDirectoryPassedToVerifyRegularFile(t *testing.T) {
	root := canonicalTempDirectory(t)
	dirPath := filepath.Join(root, "directory-not-file")
	require.NoError(t, os.Mkdir(dirPath, 0o700))

	_, _, err := safepath.VerifyRegularFileWithinRoot(root, dirPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a regular file")
}

func TestRejectingDotDotTraversal(t *testing.T) {
	root := canonicalTempDirectory(t)

	traversalDir := filepath.Join(root, "..", "escaped")
	_, err := safepath.EnsureDirectoryWithinRoot(root, traversalDir, 0o700)
	require.Error(t, err)

	nestedTraversal := filepath.Join(root, "child", "..", "..", "escaped")
	_, err = safepath.VerifyDirectoryWithinRoot(root, nestedTraversal)
	require.Error(t, err)

	fileTraversal := filepath.Join(root, "..", "escaped.txt")
	_, _, err = safepath.VerifyRegularFileWithinRoot(root, fileTraversal)
	require.Error(t, err)
}

func TestRejectingBrokenSymlinks(t *testing.T) {
	root := canonicalTempDirectory(t)
	brokenSymlink := filepath.Join(root, "broken-symlink")
	require.NoError(t, os.Symlink(filepath.Join(root, "nonexistent-target"), brokenSymlink))

	_, err := safepath.VerifyDirectoryWithinRoot(root, brokenSymlink)
	require.Error(t, err)

	_, _, err = safepath.VerifyRegularFileWithinRoot(root, brokenSymlink)
	require.Error(t, err)

	_, err = safepath.EnsureDirectoryWithinRoot(root, filepath.Join(brokenSymlink, "child"), 0o700)
	require.Error(t, err)
}
