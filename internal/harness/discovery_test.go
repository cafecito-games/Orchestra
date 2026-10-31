package harness

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenCodeDiscoveryProfile(t *testing.T) {
	index := slices.IndexFunc(providers, func(candidate provider) bool {
		return candidate.id == protocol.HarnessOpenCodeCLI
	})
	require.NotEqual(t, -1, index)
	assert.Equal(t, "OpenCode", providers[index].label)
	assert.Equal(t, "opencode", providers[index].binary)
	assert.Equal(t, "local-account", providers[index].authMode)
	assert.Empty(t, providers[index].models, "OpenCode accepts any configured provider/model identifier")
}

func TestAntigravityDiscoveryProfile(t *testing.T) {
	index := slices.IndexFunc(providers, func(candidate provider) bool {
		return candidate.id == protocol.HarnessAntigravityCLI
	})
	require.NotEqual(t, -1, index)
	assert.Equal(t, "Antigravity", providers[index].label)
	assert.Equal(t, "agy", providers[index].binary)
	assert.Equal(t, "local-account", providers[index].authMode)
	assert.Empty(t, providers[index].models, "Antigravity discovers models dynamically via ACP or agy models")
}

func TestDiscovery_NonExecutableHarnessBinary(t *testing.T) {
	temporaryDirectory := t.TempDir()
	nonExecutableBinary := filepath.Join(temporaryDirectory, "claude")
	require.NoError(t, os.WriteFile(nonExecutableBinary, []byte("#!/bin/sh\necho '1.0.0'\n"), 0o644))

	t.Setenv("PATH", temporaryDirectory)

	candidate, present := externalCandidate(t.Context(), "claude")
	assert.False(t, present, "non-executable binary (0o644) should not be discovered as an external candidate")
	assert.Empty(t, candidate.Binary)
}

func TestDiscovery_UnparseableVersionOutputGracefullyHandled(t *testing.T) {
	temporaryDirectory := t.TempDir()
	scriptPath := filepath.Join(temporaryDirectory, "codex")
	unparseableOutput := "custom corporate wrapper banner without semver digits"
	scriptContent := "#!/bin/sh\necho '" + unparseableOutput + "'\n"
	require.NoError(t, os.WriteFile(scriptPath, []byte(scriptContent), 0o755))

	t.Setenv("PATH", temporaryDirectory+string(os.PathListSeparator)+os.Getenv("PATH"))

	candidate, present := externalCandidate(t.Context(), "codex")
	require.True(t, present)
	assert.Equal(t, scriptPath, candidate.Binary)
	assert.Equal(t, unparseableOutput, candidate.Description)
	assert.Empty(t, candidate.Version, "unparseable version output should result in an empty normalized version")

	// Verify Resolve integrates the candidate gracefully
	resolutions := Resolve(t.Context(), nil)
	index := slices.IndexFunc(resolutions, func(resolution HarnessResolution) bool {
		return resolution.HarnessID == protocol.HarnessCodexCLI
	})
	require.NotEqual(t, -1, index)
	assert.True(t, resolutions[index].Profile.Available)
	assert.Equal(t, unparseableOutput, resolutions[index].Profile.Description)
}
