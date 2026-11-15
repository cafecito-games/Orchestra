package cli_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/cli"
	"github.com/cafecito-games/Orchestra/internal/mcpbridge"
	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/slave"
)

func TestConfigProfiles_AcrossMasterRelaySlaveMCPAndOperatorCLI(t *testing.T) {
	t.Parallel()

	homeDirectory := t.TempDir()
	workspaceDefaultRaw := filepath.Join(homeDirectory, "CafecitoGames")
	workspaceGPURaw := filepath.Join(homeDirectory, "GPUWorkspaces")
	require.NoError(t, os.MkdirAll(workspaceDefaultRaw, 0o755))
	require.NoError(t, os.MkdirAll(workspaceGPURaw, 0o755))

	canonicalDefaultRoots, err := slave.CanonicalizeWorkspaceRoots([]string{workspaceDefaultRaw})
	require.NoError(t, err)
	canonicalGPURoots, err := slave.CanonicalizeWorkspaceRoots([]string{workspaceGPURaw})
	require.NoError(t, err)

	configPath := filepath.Join(homeDirectory, ".orchestra", "config.toml")
	require.NoError(t, os.MkdirAll(filepath.Dir(configPath), 0o700))

	configTOML := `
[default]
auth = "tailscale"
workspace-root = "~/CafecitoGames"
claude-acp-auth-mode = "local-subscription"
approval-policy = "auto"

[default.master]
with-local-worker = true
data-dir = "~/master-state"

[default.slave]
concurrency = 4
label = ["macos", "apple-silicon"]

[default.relay]
data-dir = "~/relay-state"

[gpu]
auth = "token"
token = "gpu-cluster-shared-token"
kind = "home-server"
workspace-root = ["~/GPUWorkspaces"]
accelerator = ["cuda"]
concurrency = 6
`
	require.NoError(t, os.WriteFile(configPath, []byte(configTOML), 0o600))

	lookup := func(name string) (string, bool) {
		if name == "HOME" {
			return homeDirectory, true
		}
		return "", false
	}

	// 1. Bare `orchestra master` picks up [default] + [default.master], while preserving ListenExplicit=false for Tailscale auto-bind.
	masterConfig, err := orchestrator.ParseConfiguration(nil, lookup)
	require.NoError(t, err)
	assert.Equal(t, configPath, masterConfig.ConfigFilePath)
	assert.Equal(t, "default", masterConfig.Profile)
	assert.Equal(t, "tailscale", masterConfig.AuthMode)
	assert.False(t, masterConfig.ListenExplicit)
	assert.True(t, masterConfig.WithLocalWorker)
	assert.Equal(t, filepath.Join(homeDirectory, "master-state"), masterConfig.DataDirectory)
	assert.Equal(t, canonicalDefaultRoots, masterConfig.LocalWorker.WorkspaceRoots)
	assert.Equal(t, "local-subscription", masterConfig.LocalWorker.ClaudeACPAuthMode)
	assert.Equal(t, 4, masterConfig.LocalWorker.Concurrency)
	assert.Equal(t, []string{"macos", "apple-silicon"}, masterConfig.LocalWorker.Labels)

	// 2. `orchestra master --profile gpu` overrides [default] while inheriting unset fields.
	gpuMasterConfig, err := orchestrator.ParseConfiguration([]string{"--profile", "gpu"}, lookup)
	require.NoError(t, err)
	assert.Equal(t, "gpu", gpuMasterConfig.Profile)
	assert.Equal(t, "token", gpuMasterConfig.AuthMode)
	assert.Equal(t, "gpu-cluster-shared-token", gpuMasterConfig.Token)
	assert.True(t, gpuMasterConfig.TokenExplicit)
	assert.True(t, gpuMasterConfig.WithLocalWorker)
	assert.Equal(t, canonicalGPURoots, gpuMasterConfig.LocalWorker.WorkspaceRoots)
	assert.Equal(t, 6, gpuMasterConfig.LocalWorker.Concurrency)
	assert.Equal(t, []string{"cuda"}, gpuMasterConfig.LocalWorker.Accelerators)

	// 3. `orchestra relay` ignores [default] worker-only settings while applying [default] auth and [default.relay] data-dir.
	relayConfig, err := orchestrator.ParseRelayConfiguration(nil, lookup)
	require.NoError(t, err)
	assert.Equal(t, "tailscale", relayConfig.AuthMode)
	assert.False(t, relayConfig.WithLocalWorker)
	assert.Equal(t, filepath.Join(homeDirectory, "relay-state"), relayConfig.DataDirectory)

	// 4. Bare `orchestra slave` picks up [default] + [default.slave] while preserving EndpointExplicit=false for auto-discovery.
	slaveConfig, err := slave.ParseConfiguration(nil, lookup)
	require.NoError(t, err)
	assert.Equal(t, configPath, slaveConfig.ConfigFilePath)
	assert.Equal(t, "default", slaveConfig.Profile)
	assert.False(t, slaveConfig.EndpointExplicit)
	assert.Equal(t, canonicalDefaultRoots, slaveConfig.WorkspaceRoots)
	assert.Equal(t, 4, slaveConfig.Concurrency)
	assert.Equal(t, "local-subscription", slaveConfig.ClaudeACPAuthMode)
	assert.Equal(t, []string{"macos", "apple-silicon"}, slaveConfig.Labels)

	// 5. `orchestra slave --profile gpu` applies [gpu] overrides.
	gpuSlaveConfig, err := slave.ParseConfiguration([]string{"--profile", "gpu"}, lookup)
	require.NoError(t, err)
	assert.Equal(t, "gpu", gpuSlaveConfig.Profile)
	assert.Equal(t, "home-server", gpuSlaveConfig.Kind)
	assert.Equal(t, canonicalGPURoots, gpuSlaveConfig.WorkspaceRoots)
	assert.Equal(t, 6, gpuSlaveConfig.Concurrency)
	assert.Equal(t, []string{"cuda"}, gpuSlaveConfig.Accelerators)
	assert.Equal(t, "gpu-cluster-shared-token", gpuSlaveConfig.Token)
	assert.True(t, gpuSlaveConfig.TokenExplicit)

	// 6. Operator CLI (`orchestra status --profile remote --json`) and `orchestra mcp --profile remote` pick up endpoint + token.
	runningMaster, err := orchestrator.StartMaster(t.Context(), orchestrator.Configuration{
		ListenAddress:      "127.0.0.1:0",
		ListenExplicit:     true,
		AuthMode:           "token",
		Token:              "remote-profile-token",
		TokenExplicit:      true,
		DataDirectory:      filepath.Join(t.TempDir(), "master-live"),
		ConnectionFilePath: "none",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = runningMaster.Shutdown(t.Context())
	})

	remoteConfigPath := filepath.Join(t.TempDir(), "remote-config.toml")
	require.NoError(t, os.WriteFile(remoteConfigPath, []byte(`
[remote]
endpoint = "`+runningMaster.BaseURL+`"
token = "remote-profile-token"
`), 0o600))

	mcpConfig, err := mcpbridge.ParseConfiguration([]string{"--config", remoteConfigPath, "--profile", "remote"}, lookup)
	require.NoError(t, err)
	assert.Equal(t, runningMaster.BaseURL, mcpConfig.Endpoint)
	assert.Equal(t, "remote-profile-token", mcpConfig.Token)
	assert.True(t, mcpConfig.EndpointExplicit)
	assert.True(t, mcpConfig.TokenExplicit)

	var stdout, stderr bytes.Buffer
	exitCode := cli.Run(
		t.Context(),
		[]string{"status", "--config", remoteConfigPath, "--profile", "remote", "--json"},
		nil,
		&stdout,
		&stderr,
		lookup,
	)
	require.Equal(t, 0, exitCode, "stderr: %s", stderr.String())
	var statusPayload protocol.StatusResponse
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &statusPayload))
	assert.Equal(t, protocol.ProtocolVersion, statusPayload.ProtocolVersion)
}
