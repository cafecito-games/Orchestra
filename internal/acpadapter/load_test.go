package acpadapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/setup"
	"github.com/stretchr/testify/assert"
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
			{HarnessID: protocol.HarnessAntigravityCLI, SHA256: digest, Path: binaryPath},
		},
	})
	require.NoError(t, err)
	adapter, present := result.Adapters["codex-cli"]
	require.True(t, present)
	assert.Equal(t, protocol.ACPAdapterSourceAdministrator, adapter.Source)
	require.NoError(t, adapter.Verify())

	antigravityAdapter, antigravityPresent := result.Adapters[protocol.HarnessAntigravityCLI]
	require.True(t, antigravityPresent)
	assert.Equal(t, protocol.ACPAdapterSourceAdministrator, antigravityAdapter.Source)
	require.NoError(t, antigravityAdapter.Verify())

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

func TestLoadResolvesActivatedComponent(t *testing.T) {
	dataRoot := t.TempDir()
	platform := setup.CurrentPlatform()

	customManifest := setup.Manifest{
		ManifestVersion: setup.ManifestVersion,
		Components: []setup.ComponentManifestEntry{
			{
				ID:        "claude-acp",
				Kind:      setup.ComponentKindACPAdapter,
				HarnessID: "claude-cli",
				Provider:  "anthropic",
				Label:     "Claude ACP Adapter",
				Version:   "0.79.0",
				Platforms: map[string]setup.PlatformDistribution{
					platform: {Kind: setup.DistributionKindManual, ExecutablePath: "bin/claude-agent-acp"},
				},
			},
		},
	}
	manifestBytes, err := json.Marshal(customManifest)
	require.NoError(t, err)

	ledger, err := setup.LoadOwnershipLedger(dataRoot)
	require.NoError(t, err)

	plan, skipped, err := setup.BuildPlan(manifestBytes, customManifest, platform, dataRoot, ledger)
	require.NoError(t, err)
	assert.Empty(t, skipped)
	require.Len(t, plan.Operations, 1)

	sourceBinary := filepath.Join(t.TempDir(), "claude-agent-acp")
	binaryContent := []byte("#!/bin/sh\necho 0.79.0\n")
	require.NoError(t, os.WriteFile(sourceBinary, binaryContent, 0o755))
	sum := sha256.Sum256(binaryContent)
	checksum := hex.EncodeToString(sum[:])

	applyResult, err := setup.Apply(t.Context(), plan, manifestBytes, ledger, dataRoot, setup.ApplyOptions{
		ManualArtifactSources: map[string]string{"claude-acp": sourceBinary},
		ManualChecksums:       map[string]string{"claude-acp": checksum},
	})
	require.NoError(t, err)
	assert.Len(t, applyResult.Applied, 1)

	loadedLedger, err := setup.LoadOwnershipLedger(dataRoot)
	require.NoError(t, err)
	assert.Len(t, loadedLedger.Records, 1)

	activationOutcome, err := setup.Activate(t.Context(), setup.ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifest,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: setup.LoadActivationState(dataRoot),
		Probe:      func(context.Context, setup.InstalledComponent) error { return nil },
		InUse:      func(string) bool { return false },
	}, setup.ComponentSelector{Kind: setup.ComponentKindACPAdapter, ID: "claude-acp", Version: "0.79.0"})
	require.NoError(t, err)
	assert.True(t, activationOutcome.Changed)

	result, err := Load(Options{
		DataRoot:   dataRoot,
		Manifest:   customManifest,
		Platform:   platform,
		Activation: setup.LoadActivationState(dataRoot),
	})
	require.NoError(t, err)
	adapter, present := result.Adapters["claude-cli"]
	require.True(t, present)
	assert.Equal(t, "claude-acp", adapter.ID)
	assert.Equal(t, "0.79.0", adapter.Version)
	assert.Equal(t, protocol.ACPAdapterSourceSetupLedger, adapter.Source)
	require.NoError(t, adapter.Verify())
}

func TestLoadSkipsOnCorruptedLedger(t *testing.T) {
	dataRoot := t.TempDir()
	manifest, err := setup.LoadDefaultManifest()
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(dataRoot, "ownership.json"), []byte("{corrupted json"), 0o600))

	result, err := Load(Options{
		DataRoot:   dataRoot,
		Manifest:   manifest,
		Platform:   setup.CurrentPlatform(),
		Activation: setup.LoadActivationState(t.TempDir()),
	})
	require.NoError(t, err)
	assert.Contains(t, result.Skipped["claude-cli"], "the ownership ledger could not be read")
}

func TestLoadSkipsOnActivationRejection(t *testing.T) {
	manifest, err := setup.LoadDefaultManifest()
	require.NoError(t, err)

	result, err := Load(Options{
		DataRoot: t.TempDir(),
		Manifest: manifest,
		Platform: setup.CurrentPlatform(),
		Activation: setup.ActivationState{
			Rejection: errors.New("activation signature invalid"),
		},
	})
	require.NoError(t, err)
	assert.Contains(t, result.Skipped["claude-cli"], "the activation ledger could not be accepted")
}

func TestLoadSkipsOnAmbiguousMultipleAdapters(t *testing.T) {
	dataRoot := t.TempDir()
	platform := setup.CurrentPlatform()

	customManifest := setup.Manifest{
		ManifestVersion: setup.ManifestVersion,
		Components: []setup.ComponentManifestEntry{
			{
				ID:        "adapter-alpha",
				Kind:      setup.ComponentKindACPAdapter,
				HarnessID: "shared-harness",
				Provider:  "test",
				Label:     "Adapter Alpha",
				Version:   "1.0.0",
				Platforms: map[string]setup.PlatformDistribution{
					platform: {Kind: setup.DistributionKindManual, ExecutablePath: "bin/adapter"},
				},
			},
			{
				ID:        "adapter-beta",
				Kind:      setup.ComponentKindACPAdapter,
				HarnessID: "shared-harness",
				Provider:  "test",
				Label:     "Adapter Beta",
				Version:   "1.0.0",
				Platforms: map[string]setup.PlatformDistribution{
					platform: {Kind: setup.DistributionKindManual, ExecutablePath: "bin/adapter"},
				},
			},
		},
	}
	manifestBytes, err := json.Marshal(customManifest)
	require.NoError(t, err)

	ledger, err := setup.LoadOwnershipLedger(dataRoot)
	require.NoError(t, err)

	plan, _, err := setup.BuildPlan(manifestBytes, customManifest, platform, dataRoot, ledger)
	require.NoError(t, err)

	sourceBinary := filepath.Join(t.TempDir(), "adapter")
	binaryContent := []byte("#!/bin/sh\necho 1.0.0\n")
	require.NoError(t, os.WriteFile(sourceBinary, binaryContent, 0o755))
	sum := sha256.Sum256(binaryContent)
	checksum := hex.EncodeToString(sum[:])

	_, err = setup.Apply(t.Context(), plan, manifestBytes, ledger, dataRoot, setup.ApplyOptions{
		ManualArtifactSources: map[string]string{
			"adapter-alpha": sourceBinary,
			"adapter-beta":  sourceBinary,
		},
		ManualChecksums: map[string]string{
			"adapter-alpha": checksum,
			"adapter-beta":  checksum,
		},
	})
	require.NoError(t, err)

	loadedLedger, err := setup.LoadOwnershipLedger(dataRoot)
	require.NoError(t, err)

	_, err = setup.Activate(t.Context(), setup.ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifest,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: setup.LoadActivationState(dataRoot),
		Probe:      func(context.Context, setup.InstalledComponent) error { return nil },
		InUse:      func(string) bool { return false },
	}, setup.ComponentSelector{Kind: setup.ComponentKindACPAdapter, ID: "adapter-alpha", Version: "1.0.0"})
	require.NoError(t, err)

	_, err = setup.Activate(t.Context(), setup.ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifest,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: setup.LoadActivationState(dataRoot),
		Probe:      func(context.Context, setup.InstalledComponent) error { return nil },
		InUse:      func(string) bool { return false },
	}, setup.ComponentSelector{Kind: setup.ComponentKindACPAdapter, ID: "adapter-beta", Version: "1.0.0"})
	require.NoError(t, err)

	result, err := Load(Options{
		DataRoot:   dataRoot,
		Manifest:   customManifest,
		Platform:   platform,
		Activation: setup.LoadActivationState(dataRoot),
	})
	require.NoError(t, err)
	assert.Contains(t, result.Skipped["shared-harness"], "more than one activated adapter matches the harness")
}

func TestLoadRejectsSymlinkOverride(t *testing.T) {
	manifest, err := setup.LoadDefaultManifest()
	require.NoError(t, err)

	temporaryDirectory := t.TempDir()
	targetPath := filepath.Join(temporaryDirectory, "target-binary")
	content := []byte("#!/bin/sh\necho ok\n")
	require.NoError(t, os.WriteFile(targetPath, content, 0o755))
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])

	symlinkPath := filepath.Join(temporaryDirectory, "symlink-binary")
	require.NoError(t, os.Symlink(targetPath, symlinkPath))

	_, err = Load(Options{
		DataRoot:   t.TempDir(),
		Manifest:   manifest,
		Platform:   setup.CurrentPlatform(),
		Activation: setup.LoadActivationState(t.TempDir()),
		Overrides: []Override{
			{HarnessID: "codex-cli", SHA256: digest, Path: symlinkPath},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the path must name a regular file, not a symlink or directory")
}

func TestLoadRejectsNonExecutableOverrideOnUnix(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("skipping Unix executable permission test on Windows")
	}
	manifest, err := setup.LoadDefaultManifest()
	require.NoError(t, err)

	temporaryDirectory := t.TempDir()
	binaryPath := filepath.Join(temporaryDirectory, "non-exec-binary")
	content := []byte("#!/bin/sh\necho ok\n")
	require.NoError(t, os.WriteFile(binaryPath, content, 0o644))
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])

	_, err = Load(Options{
		DataRoot:   t.TempDir(),
		Manifest:   manifest,
		Platform:   setup.CurrentPlatform(),
		Activation: setup.LoadActivationState(t.TempDir()),
		Overrides: []Override{
			{HarnessID: "codex-cli", SHA256: digest, Path: binaryPath},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the file is not executable")
}
