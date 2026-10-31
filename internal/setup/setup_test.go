package setup

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

func TestDefaultManifestAndPlanApplyActivateRollbackPrune(t *testing.T) {
	manifest, err := LoadDefaultManifest()
	require.NoError(t, err)
	assert.NotEmpty(t, manifest.Components)

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
	assert.Empty(t, skipped)
	require.Len(t, plan.Operations, 1)

	sourceBinary := filepath.Join(t.TempDir(), "claude-agent-acp")
	binaryContent := []byte("#!/bin/sh\necho 0.79.0\n")
	require.NoError(t, os.WriteFile(sourceBinary, binaryContent, 0o755))
	sum := sha256.Sum256(binaryContent)
	checksum := hex.EncodeToString(sum[:])

	applyResult, err := Apply(t.Context(), plan, manifestBytes, ledger, dataRoot, ApplyOptions{
		ManualArtifactSources: map[string]string{"claude-acp": sourceBinary},
		ManualChecksums:       map[string]string{"claude-acp": checksum},
	})
	require.NoError(t, err)
	assert.Len(t, applyResult.Applied, 1)

	loadedLedger, err := LoadOwnershipLedger(dataRoot)
	require.NoError(t, err)
	assert.Len(t, loadedLedger.Records, 1)

	activationOutcome, err := Activate(t.Context(), ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifest,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: LoadActivationState(dataRoot),
		Probe:      func(context.Context, InstalledComponent) error { return nil },
		InUse:      func(string) bool { return false },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "claude-acp", Version: "0.79.0"})
	require.NoError(t, err)
	assert.True(t, activationOutcome.Changed)
	assert.Equal(t, "0.79.0", activationOutcome.Active.Version)

	customManifestV2 := customManifest
	customManifestV2.Components[0].Version = "0.80.0"
	manifestBytesV2, err := json.Marshal(customManifestV2)
	require.NoError(t, err)
	planV2, _, err := BuildPlan(manifestBytesV2, customManifestV2, platform, dataRoot, loadedLedger)
	require.NoError(t, err)
	_, err = Apply(t.Context(), planV2, manifestBytesV2, loadedLedger, dataRoot, ApplyOptions{
		ManualArtifactSources: map[string]string{"claude-acp": sourceBinary},
		ManualChecksums:       map[string]string{"claude-acp": checksum},
	})
	require.NoError(t, err)

	loadedLedger, err = LoadOwnershipLedger(dataRoot)
	require.NoError(t, err)
	assert.Len(t, loadedLedger.Records, 2)

	activationOutcomeV2, err := Activate(t.Context(), ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifestV2,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: LoadActivationState(dataRoot),
		Probe:      func(context.Context, InstalledComponent) error { return nil },
		InUse:      func(string) bool { return false },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "claude-acp", Version: "0.80.0"})
	require.NoError(t, err)
	assert.Equal(t, "0.80.0", activationOutcomeV2.Active.Version)
	require.NotNil(t, activationOutcomeV2.Previous)
	assert.Equal(t, "0.79.0", activationOutcomeV2.Previous.Version)

	rollbackOutcome, err := Rollback(t.Context(), ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifestV2,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: LoadActivationState(dataRoot),
		Probe:      func(context.Context, InstalledComponent) error { return nil },
		InUse:      func(string) bool { return false },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "claude-acp"})
	require.NoError(t, err)
	assert.Equal(t, "0.79.0", rollbackOutcome.Active.Version)
	assert.Nil(t, rollbackOutcome.Previous)

	pruneResult, prunedLedger, err := Prune(ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifestV2,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: LoadActivationState(dataRoot),
		InUse:      func(string) bool { return false },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "claude-acp"})
	require.NoError(t, err)
	assert.Len(t, pruneResult.Removed, 1)
	assert.Len(t, prunedLedger.Records, 1)

	report := RunDoctor(
		t.Context(),
		customManifest,
		prunedLedger,
		dataRoot,
		platform,
		LoadActivationState(dataRoot),
		[]protocol.HarnessProfile{{ID: "claude-cli", Binary: sourceBinary, Available: true, Transports: []string{protocol.TransportNativeCLI, protocol.TransportACP}}},
		"ws://user:secret@127.0.0.1:8787/control-agent?token=secret",
		func(context.Context, string) error { return nil },
	)
	assert.True(t, report.HubConnectivity.Reachable)
	assert.Equal(t, "ws://127.0.0.1:8787/control-agent", report.HubConnectivity.Endpoint)
	assert.Len(t, report.Components, 1)
	assert.Equal(t, ComponentProvenanceManaged, report.Components[0].Provenance)
}

func TestApplyArchiveExtractionTarGzAndZip(t *testing.T) {
	binaryPayload := []byte("#!/bin/sh\necho archive-extracted-ok\n")

	tarGzBytes := createTestTarGzArchive(t, "bin/adapter-binary", binaryPayload)
	tarGzSum := sha256.Sum256(tarGzBytes)
	tarGzChecksum := hex.EncodeToString(tarGzSum[:])

	zipBytes := createTestZipArchive(t, "bin/adapter-binary", binaryPayload)
	zipSum := sha256.Sum256(zipBytes)
	zipChecksum := hex.EncodeToString(zipSum[:])

	testServer := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/component.tar.gz":
			writer.Header().Set("Content-Type", "application/gzip")
			_, _ = writer.Write(tarGzBytes)
		case "/component.zip":
			writer.Header().Set("Content-Type", "application/zip")
			_, _ = writer.Write(zipBytes)
		case "/corrupted.tar.gz":
			writer.Header().Set("Content-Type", "application/gzip")
			_, _ = writer.Write(tarGzBytes)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer testServer.Close()

	parsedURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	applyOptions := ApplyOptions{
		HTTPClient:   testServer.Client(),
		AllowedHosts: []string{parsedURL.Hostname()},
	}

	platform := CurrentPlatform()

	// 1. TarGz extraction test
	t.Run("TarGz Extraction", func(t *testing.T) {
		dataRoot := t.TempDir()
		tarGzManifest := Manifest{
			ManifestVersion: ManifestVersion,
			Components: []ComponentManifestEntry{
				{
					ID:        "targz-adapter",
					Kind:      ComponentKindACPAdapter,
					HarnessID: "targz-harness",
					Provider:  "test-provider",
					Label:     "TarGz Test Adapter",
					Version:   "1.0.0",
					Platforms: map[string]PlatformDistribution{
						platform: {
							Kind:           DistributionKindArchive,
							URL:            testServer.URL + "/component.tar.gz",
							SHA256:         tarGzChecksum,
							SizeBytes:      int64(len(tarGzBytes)),
							ExecutablePath: "bin/adapter-binary",
						},
					},
				},
			},
		}

		manifestBytes, marshalErr := json.Marshal(tarGzManifest)
		require.NoError(t, marshalErr)

		ledger, ledgerErr := LoadOwnershipLedger(dataRoot)
		require.NoError(t, ledgerErr)

		plan, skipped, buildErr := BuildPlan(manifestBytes, tarGzManifest, platform, dataRoot, ledger)
		require.NoError(t, buildErr)
		assert.Empty(t, skipped)
		require.Len(t, plan.Operations, 1)

		applyResult, applyErr := Apply(t.Context(), plan, manifestBytes, ledger, dataRoot, applyOptions)
		require.NoError(t, applyErr)
		assert.Len(t, applyResult.Applied, 1)

		installedPath := plan.Operations[0].TargetPath
		fileInformation, statErr := os.Lstat(installedPath)
		require.NoError(t, statErr)
		assert.Equal(t, os.FileMode(0o755), fileInformation.Mode().Perm())

		extractedContent, readErr := os.ReadFile(installedPath)
		require.NoError(t, readErr)
		assert.Equal(t, binaryPayload, extractedContent)

		savedLedger, reloadErr := LoadOwnershipLedger(dataRoot)
		require.NoError(t, reloadErr)
		assert.Len(t, savedLedger.Records, 1)
	})

	// 2. Zip extraction test
	t.Run("Zip Extraction", func(t *testing.T) {
		dataRoot := t.TempDir()
		zipManifest := Manifest{
			ManifestVersion: ManifestVersion,
			Components: []ComponentManifestEntry{
				{
					ID:        "zip-adapter",
					Kind:      ComponentKindACPAdapter,
					HarnessID: "zip-harness",
					Provider:  "test-provider",
					Label:     "Zip Test Adapter",
					Version:   "1.0.0",
					Platforms: map[string]PlatformDistribution{
						platform: {
							Kind:           DistributionKindArchive,
							URL:            testServer.URL + "/component.zip",
							SHA256:         zipChecksum,
							SizeBytes:      int64(len(zipBytes)),
							ExecutablePath: "bin/adapter-binary",
						},
					},
				},
			},
		}

		manifestBytes, marshalErr := json.Marshal(zipManifest)
		require.NoError(t, marshalErr)

		ledger, ledgerErr := LoadOwnershipLedger(dataRoot)
		require.NoError(t, ledgerErr)

		plan, skipped, buildErr := BuildPlan(manifestBytes, zipManifest, platform, dataRoot, ledger)
		require.NoError(t, buildErr)
		assert.Empty(t, skipped)
		require.Len(t, plan.Operations, 1)

		applyResult, applyErr := Apply(t.Context(), plan, manifestBytes, ledger, dataRoot, applyOptions)
		require.NoError(t, applyErr)
		assert.Len(t, applyResult.Applied, 1)

		installedPath := plan.Operations[0].TargetPath
		fileInformation, statErr := os.Lstat(installedPath)
		require.NoError(t, statErr)
		assert.Equal(t, os.FileMode(0o755), fileInformation.Mode().Perm())

		extractedContent, readErr := os.ReadFile(installedPath)
		require.NoError(t, readErr)
		assert.Equal(t, binaryPayload, extractedContent)

		savedLedger, reloadErr := LoadOwnershipLedger(dataRoot)
		require.NoError(t, reloadErr)
		assert.Len(t, savedLedger.Records, 1)
	})

	// 3. Corrupted SHA256 digest test
	t.Run("Corrupted Checksum Rollback", func(t *testing.T) {
		dataRoot := t.TempDir()
		corruptedManifest := Manifest{
			ManifestVersion: ManifestVersion,
			Components: []ComponentManifestEntry{
				{
					ID:        "corrupted-adapter",
					Kind:      ComponentKindACPAdapter,
					HarnessID: "corrupted-harness",
					Provider:  "test-provider",
					Label:     "Corrupted Test Adapter",
					Version:   "1.0.0",
					Platforms: map[string]PlatformDistribution{
						platform: {
							Kind:           DistributionKindArchive,
							URL:            testServer.URL + "/corrupted.tar.gz",
							SHA256:         "0000000000000000000000000000000000000000000000000000000000000000",
							SizeBytes:      int64(len(tarGzBytes)),
							ExecutablePath: "bin/adapter-binary",
						},
					},
				},
			},
		}

		manifestBytes, marshalErr := json.Marshal(corruptedManifest)
		require.NoError(t, marshalErr)

		ledger, ledgerErr := LoadOwnershipLedger(dataRoot)
		require.NoError(t, ledgerErr)

		plan, skipped, buildErr := BuildPlan(manifestBytes, corruptedManifest, platform, dataRoot, ledger)
		require.NoError(t, buildErr)
		assert.Empty(t, skipped)
		require.Len(t, plan.Operations, 1)

		_, applyErr := Apply(t.Context(), plan, manifestBytes, ledger, dataRoot, applyOptions)
		require.Error(t, applyErr)
		assert.Contains(t, applyErr.Error(), "checksum mismatch")

		// Verify target binary was not installed and temporary staging was removed
		installedPath := plan.Operations[0].TargetPath
		_, statErr := os.Lstat(installedPath)
		require.ErrorIs(t, statErr, os.ErrNotExist)

		// Verify ledger remains uncorrupted
		savedLedger, reloadErr := LoadOwnershipLedger(dataRoot)
		require.NoError(t, reloadErr)
		assert.Empty(t, savedLedger.Records)
	})
}

func TestInUseAndRollbackInvariants(t *testing.T) {
	dataRoot := t.TempDir()
	platform := CurrentPlatform()

	customManifest := Manifest{
		ManifestVersion: ManifestVersion,
		Components: []ComponentManifestEntry{
			{
				ID:        "inuse-acp",
				Kind:      ComponentKindACPAdapter,
				HarnessID: "claude-cli",
				Provider:  "anthropic",
				Label:     "InUse ACP Adapter",
				Version:   "1.0.0",
				Platforms: map[string]PlatformDistribution{
					platform: {Kind: DistributionKindManual, ExecutablePath: "bin/inuse-agent-acp"},
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
	assert.Empty(t, skipped)
	require.Len(t, plan.Operations, 1)

	sourceBinary := filepath.Join(t.TempDir(), "inuse-agent-acp")
	binaryContent := []byte("#!/bin/sh\necho 1.0.0\n")
	require.NoError(t, os.WriteFile(sourceBinary, binaryContent, 0o755))
	sum := sha256.Sum256(binaryContent)
	checksum := hex.EncodeToString(sum[:])

	applyResult, err := Apply(t.Context(), plan, manifestBytes, ledger, dataRoot, ApplyOptions{
		ManualArtifactSources: map[string]string{"inuse-acp": sourceBinary},
		ManualChecksums:       map[string]string{"inuse-acp": checksum},
	})
	require.NoError(t, err)
	assert.Len(t, applyResult.Applied, 1)

	loadedLedger, err := LoadOwnershipLedger(dataRoot)
	require.NoError(t, err)

	// Activate version 1.0.0
	_, err = Activate(t.Context(), ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifest,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: LoadActivationState(dataRoot),
		Probe:      func(context.Context, InstalledComponent) error { return nil },
		InUse:      func(string) bool { return false },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "inuse-acp", Version: "1.0.0"})
	require.NoError(t, err)

	// InUse invariant verification: Prune refuses to remove an in-use component
	_, _, pruneErr := Prune(ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifest,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: LoadActivationState(dataRoot),
		InUse:      func(string) bool { return true },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "inuse-acp"})
	require.Error(t, pruneErr)
	assert.Contains(t, pruneErr.Error(), "currently using this component")

	// Target file must still exist
	installedPath := plan.Operations[0].TargetPath
	_, statErr := os.Lstat(installedPath)
	require.NoError(t, statErr)

	// InUse invariant verification: Activate refuses to switch when component is currently in use
	customManifestV2 := customManifest
	customManifestV2.Components[0].Version = "2.0.0"
	manifestBytesV2, err := json.Marshal(customManifestV2)
	require.NoError(t, err)

	planV2, _, err := BuildPlan(manifestBytesV2, customManifestV2, platform, dataRoot, loadedLedger)
	require.NoError(t, err)

	_, err = Apply(t.Context(), planV2, manifestBytesV2, loadedLedger, dataRoot, ApplyOptions{
		ManualArtifactSources: map[string]string{"inuse-acp": sourceBinary},
		ManualChecksums:       map[string]string{"inuse-acp": checksum},
	})
	require.NoError(t, err)

	loadedLedgerV2, err := LoadOwnershipLedger(dataRoot)
	require.NoError(t, err)

	_, activateErr := Activate(t.Context(), ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifestV2,
		Platform:   platform,
		Ownership:  loadedLedgerV2,
		Activation: LoadActivationState(dataRoot),
		Probe:      func(context.Context, InstalledComponent) error { return nil },
		InUse:      func(string) bool { return true },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "inuse-acp", Version: "2.0.0"})
	require.Error(t, activateErr)
	assert.Contains(t, activateErr.Error(), "a run is currently using")

	// Rollback invariant verification: Rollback returns an error when no previous version is retained
	_, rollbackErr := Rollback(t.Context(), ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifest,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: LoadActivationState(dataRoot),
		Probe:      func(context.Context, InstalledComponent) error { return nil },
		InUse:      func(string) bool { return false },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "inuse-acp"})
	require.Error(t, rollbackErr)
	assert.Contains(t, rollbackErr.Error(), "no previously verified version is retained")

	// Rollback invariant verification: Rollback returns ErrComponentNotActivated when not activated
	_, notActivatedErr := Rollback(t.Context(), ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   customManifest,
		Platform:   platform,
		Ownership:  loadedLedger,
		Activation: LoadActivationState(dataRoot),
		Probe:      func(context.Context, InstalledComponent) error { return nil },
		InUse:      func(string) bool { return false },
	}, ComponentSelector{Kind: ComponentKindACPAdapter, ID: "unactivated-adapter"})
	require.Error(t, notActivatedErr)
	assert.ErrorIs(t, notActivatedErr, ErrComponentNotActivated)
}

func createTestTarGzArchive(t *testing.T, entryPath string, content []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	tarWriter := tar.NewWriter(gzipWriter)

	header := &tar.Header{
		Name:     entryPath,
		Mode:     0o755,
		Size:     int64(len(content)),
		Typeflag: tar.TypeReg,
	}
	require.NoError(t, tarWriter.WriteHeader(header))
	_, err := tarWriter.Write(content)
	require.NoError(t, err)
	require.NoError(t, tarWriter.Close())
	require.NoError(t, gzipWriter.Close())
	return buffer.Bytes()
}

func createTestZipArchive(t *testing.T, entryPath string, content []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	zipWriter := zip.NewWriter(&buffer)

	writer, err := zipWriter.Create(entryPath)
	require.NoError(t, err)
	_, err = writer.Write(content)
	require.NoError(t, err)
	require.NoError(t, zipWriter.Close())
	return buffer.Bytes()
}
