package acpadapter

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/setup"
	"github.com/stretchr/testify/require"
)

func TestLoadVerifiesPinnedOverride(t *testing.T) {
	manifest, err := setup.LoadDefaultManifest()
	require.NoError(t, err)

	temporaryDirectory := t.TempDir()
	binaryPath := filepath.Join(temporaryDirectory, "codex-acp")
	content := []byte("#!/bin/sh\necho ok\n")
	require.NoError(t, os.WriteFile(binaryPath, content, 0o755))
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])

	result, err := Load(Options{
		DataRoot:   t.TempDir(),
		Manifest:   manifest,
		Platform:   setup.CurrentPlatform(),
		Activation: setup.LoadActivationState(t.TempDir()),
		Overrides: []Override{
			{HarnessID: "codex-cli", SHA256: digest, Path: binaryPath},
		},
	})
	require.NoError(t, err)
	adapter, present := result.Adapters["codex-cli"]
	require.True(t, present)
	require.Equal(t, protocol.ACPAdapterSourceAdministrator, adapter.Source)
	require.NoError(t, adapter.Verify())

	_, err = Load(Options{
		DataRoot:   t.TempDir(),
		Manifest:   manifest,
		Platform:   setup.CurrentPlatform(),
		Activation: setup.LoadActivationState(t.TempDir()),
		Overrides: []Override{
			{HarnessID: "codex-cli", SHA256: "0000000000000000000000000000000000000000000000000000000000000000", Path: binaryPath},
		},
	})
	require.Error(t, err)
}
