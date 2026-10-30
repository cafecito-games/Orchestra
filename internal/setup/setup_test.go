package setup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestDefaultManifestAndPlanApplyActivateRollbackPrune(t *testing.T) {
	manifest, err := LoadDefaultManifest()
	require.NoError(t, err)
	require.NotEmpty(t, manifest.Components)

	dataRoot := t.TempDir()
	platform := CurrentPlatform()

	customManifest := Manifest{
		ManifestVersion: ManifestVersion,
		Components: []ComponentManifestEntry{
			{
				ID:        "claude-acp",
				Kind:      ComponentKindACPAdapter,
				HarnessID: "claude-cli",
				Provider:  "anthropic",
				Label:     "Claude ACP Adapter",
				Version:   "0.79.0",
				Platforms: map[string]PlatformDistribution{
					platform: {Kind: DistributionKindManual, ExecutablePath: "bin/claude-agent-acp"},
				},
			},
		},
	}
	manifestBytes, err := json.Marshal(customManifest)
	require.NoError(t, err)

	ledger, err := LoadOwnershipLedger(dataRoot)
	require.NoError(t, err)

	plan, skipped, err := BuildPlan(manifestBytes, customManifest, platform, dataRoot, ledger)
	require.NoError(t, err)
	require.Empty(t, skipped)
	require.Len(t, plan.Operations, 1)

	sourceBinary := filepath.Join(t.TempDir(), "claude-agent-acp")
	binaryContent := []byte("#!/bin/sh\necho 0.79.0\n")
	require.NoError(t, os.WriteFile(sourceBinary, binaryContent, 0o755))
	sum := sha256.Sum256(binaryContent)
	checksum := hex.EncodeToString(sum[:])

	applyResult, err := Apply(context.Background(), plan, manifestBytes, ledger, dataRoot, ApplyOptions{
		ManualArtifactSources: map[string]string{"claude-acp": sourceBinary},
		ManualChecksums:       map[string]string{"claude-acp": checksum},
	})
	require.NoError(t, err)
	require.Len(t, applyResult.Applied, 1)

	loadedLedger, err := LoadOwnershipLedger(dataRoot)
	require.NoError(t, err)
	require.Len(t, loadedLedger.Records, 1)

	activationOutcome, err := Activate(context.Background(), ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifest,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: LoadActivationState(dataRoot),
		Probe:      func(context.Context, InstalledComponent) error { return nil },
		InUse:      func(string) bool { return false },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "claude-acp", Version: "0.79.0"})
	require.NoError(t, err)
	require.True(t, activationOutcome.Changed)
	require.Equal(t, "0.79.0", activationOutcome.Active.Version)

	customManifestV2 := customManifest
	customManifestV2.Components[0].Version = "0.80.0"
	manifestBytesV2, err := json.Marshal(customManifestV2)
	require.NoError(t, err)
	planV2, _, err := BuildPlan(manifestBytesV2, customManifestV2, platform, dataRoot, loadedLedger)
	require.NoError(t, err)
	_, err = Apply(context.Background(), planV2, manifestBytesV2, loadedLedger, dataRoot, ApplyOptions{
		ManualArtifactSources: map[string]string{"claude-acp": sourceBinary},
		ManualChecksums:       map[string]string{"claude-acp": checksum},
	})
	require.NoError(t, err)

	loadedLedger, err = LoadOwnershipLedger(dataRoot)
	require.NoError(t, err)
	require.Len(t, loadedLedger.Records, 2)

	activationOutcomeV2, err := Activate(context.Background(), ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifestV2,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: LoadActivationState(dataRoot),
		Probe:      func(context.Context, InstalledComponent) error { return nil },
		InUse:      func(string) bool { return false },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "claude-acp", Version: "0.80.0"})
	require.NoError(t, err)
	require.Equal(t, "0.80.0", activationOutcomeV2.Active.Version)
	require.NotNil(t, activationOutcomeV2.Previous)
	require.Equal(t, "0.79.0", activationOutcomeV2.Previous.Version)

	rollbackOutcome, err := Rollback(context.Background(), ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifestV2,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: LoadActivationState(dataRoot),
		Probe:      func(context.Context, InstalledComponent) error { return nil },
		InUse:      func(string) bool { return false },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "claude-acp"})
	require.NoError(t, err)
	require.Equal(t, "0.79.0", rollbackOutcome.Active.Version)
	require.Nil(t, rollbackOutcome.Previous)

	pruneResult, prunedLedger, err := Prune(ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifestV2,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: LoadActivationState(dataRoot),
		InUse:      func(string) bool { return false },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "claude-acp"})
	require.NoError(t, err)
	require.Len(t, pruneResult.Removed, 1)
	require.Len(t, prunedLedger.Records, 1)

	report := RunDoctor(
		context.Background(),
		customManifest,
		prunedLedger,
		dataRoot,
		platform,
		LoadActivationState(dataRoot),
		[]protocol.HarnessProfile{{ID: "claude-cli", Binary: sourceBinary, Available: true, Transports: []string{protocol.TransportNativeCLI, protocol.TransportACP}}},
		"ws://user:secret@127.0.0.1:8787/control-agent?token=secret",
		func(context.Context, string) error { return nil },
	)
	require.True(t, report.HubConnectivity.Reachable)
	require.Equal(t, "ws://127.0.0.1:8787/control-agent", report.HubConnectivity.Endpoint)
	require.Len(t, report.Components, 1)
	require.Equal(t, ComponentProvenanceManaged, report.Components[0].Provenance)
}
