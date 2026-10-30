package harness

import (
	"slices"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestOpenCodeDiscoveryProfile(t *testing.T) {
	index := slices.IndexFunc(providers, func(candidate provider) bool {
		return candidate.id == protocol.HarnessOpenCodeCLI
	})
	require.NotEqual(t, -1, index)
	require.Equal(t, "OpenCode", providers[index].label)
	require.Equal(t, "opencode", providers[index].binary)
	require.Equal(t, "local-account", providers[index].authMode)
	require.Empty(t, providers[index].models, "OpenCode accepts any configured provider/model identifier")
}

func TestAntigravityDiscoveryProfile(t *testing.T) {
	index := slices.IndexFunc(providers, func(candidate provider) bool {
		return candidate.id == protocol.HarnessAntigravityCLI
	})
	require.NotEqual(t, -1, index)
	require.Equal(t, "Antigravity", providers[index].label)
	require.Equal(t, "agy", providers[index].binary)
	require.Equal(t, "local-account", providers[index].authMode)
	require.Empty(t, providers[index].models, "Antigravity discovers models dynamically via ACP or agy models")
}
