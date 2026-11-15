package localstate_test

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/localstate"
)

type testListFlag struct {
	values []string
}

func (list *testListFlag) String() string {
	return strings.Join(list.values, ",")
}

func (list *testListFlag) Set(raw string) error {
	list.values = append(list.values, raw)
	return nil
}

func lookupFromMap(environment map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, found := environment[name]
		return value, found
	}
}

func TestResolveConfigFilePath_PrecedenceAndIsolation(t *testing.T) {
	t.Parallel()

	homeDirectory := t.TempDir()
	xdgDirectory := filepath.Join(t.TempDir(), "xdg")
	primaryPath := filepath.Join(homeDirectory, ".orchestra", "config.toml")
	xdgPath := filepath.Join(xdgDirectory, "orchestra", "config.toml")
	explicitPath := filepath.Join(t.TempDir(), "custom.toml")

	require.NoError(t, os.MkdirAll(filepath.Dir(xdgPath), 0o700))
	require.NoError(t, os.WriteFile(xdgPath, []byte("[default]\n"), 0o600))
	require.NoError(t, os.WriteFile(explicitPath, []byte("[default]\n"), 0o600))

	// Isolated lookup without HOME/ORCHESTRA_CONFIG never touches real home directory.
	isolatedPath, explicit, err := localstate.ResolveConfigFilePath("", false, lookupFromMap(nil))
	require.NoError(t, err)
	assert.Empty(t, isolatedPath)
	assert.False(t, explicit)

	// XDG fallback is used when ~/.orchestra/config.toml is absent.
	resolvedXDG, explicit, err := localstate.ResolveConfigFilePath("", false, lookupFromMap(map[string]string{
		"HOME":            homeDirectory,
		"XDG_CONFIG_HOME": xdgDirectory,
	}))
	require.NoError(t, err)
	assert.Equal(t, xdgPath, resolvedXDG)
	assert.False(t, explicit)

	// ~/.orchestra/config.toml wins over XDG fallback once created.
	require.NoError(t, os.MkdirAll(filepath.Dir(primaryPath), 0o700))
	require.NoError(t, os.WriteFile(primaryPath, []byte("[default]\n"), 0o600))
	resolvedPrimary, explicit, err := localstate.ResolveConfigFilePath("", false, lookupFromMap(map[string]string{
		"HOME":            homeDirectory,
		"XDG_CONFIG_HOME": xdgDirectory,
	}))
	require.NoError(t, err)
	assert.Equal(t, primaryPath, resolvedPrimary)
	assert.False(t, explicit)

	// ORCHESTRA_CONFIG overrides ~/.orchestra/config.toml.
	resolvedEnv, explicit, err := localstate.ResolveConfigFilePath("", false, lookupFromMap(map[string]string{
		"HOME":             homeDirectory,
		"ORCHESTRA_CONFIG": explicitPath,
	}))
	require.NoError(t, err)
	assert.Equal(t, explicitPath, resolvedEnv)
	assert.True(t, explicit)

	// ORCHESTRA_CONFIG=none disables loading even when ~/.orchestra/config.toml exists.
	disabledPath, explicit, err := localstate.ResolveConfigFilePath("", false, lookupFromMap(map[string]string{
		"HOME":             homeDirectory,
		"ORCHESTRA_CONFIG": "none",
	}))
	require.NoError(t, err)
	assert.Empty(t, disabledPath)
	assert.True(t, explicit)

	// Missing explicit --config returns an error.
	_, _, missingErr := localstate.ResolveConfigFilePath(filepath.Join(homeDirectory, "missing.toml"), true, lookupFromMap(map[string]string{
		"HOME": homeDirectory,
	}))
	require.Error(t, missingErr)
	assert.Contains(t, missingErr.Error(), "does not exist")
}

func TestApplyProfileToFlagSet_InheritancePrecedenceAndExpansion(t *testing.T) {
	t.Parallel()

	homeDirectory := t.TempDir()
	configPath := filepath.Join(homeDirectory, ".orchestra", "config.toml")
	require.NoError(t, os.MkdirAll(filepath.Dir(configPath), 0o700))

	configContent := `
[default]
auth = "tailscale"
workspace-root = "~/CafecitoGames"
claude_acp_auth_mode = "local-subscription"
concurrency = 3
label = ["shared-a", "shared-b"]
acp-adapter = ["claude-cli=sha256:0123456789abcdef:~/adapters/claude-agent-acp"]

[default.slave]
concurrency = 4

[gpu]
kind = "home-server"
workspace-root = ["~/Projects", "~/Engine"]
accelerator = ["cuda"]

[gpu.slave]
concurrency = 8
`
	require.NoError(t, os.WriteFile(configPath, []byte(configContent), 0o600))

	t.Run("default profile applies to slave and expands tilde paths", func(t *testing.T) {
		t.Parallel()

		flagSet := flag.NewFlagSet("slave", flag.ContinueOnError)
		var kind string
		var concurrency int
		var claudeAuth string
		var roots testListFlag
		var labels testListFlag
		var adapters testListFlag
		flagSet.StringVar(&kind, "kind", "local", "")
		flagSet.IntVar(&concurrency, "concurrency", 2, "")
		flagSet.StringVar(&claudeAuth, "claude-acp-auth-mode", "", "")
		flagSet.Var(&roots, "workspace-root", "")
		flagSet.Var(&labels, "label", "")
		flagSet.Var(&adapters, "acp-adapter", "")
		require.NoError(t, flagSet.Parse(nil))

		applied, err := localstate.ApplyProfileToFlagSet(flagSet, localstate.ProfileOptions{
			Subcommand: localstate.SubcommandSlave,
			LookupEnvironment: lookupFromMap(map[string]string{
				"HOME": homeDirectory,
			}),
		})
		require.NoError(t, err)
		assert.True(t, applied.Loaded)
		assert.Equal(t, configPath, applied.ConfigFilePath)
		assert.Equal(t, "default", applied.Profile)
		assert.Equal(t, "local", kind)
		assert.Equal(t, 4, concurrency)
		assert.Equal(t, "local-subscription", claudeAuth)
		assert.Equal(t, []string{filepath.Join(homeDirectory, "CafecitoGames")}, roots.values)
		assert.Equal(t, []string{"shared-a", "shared-b"}, labels.values)
		assert.Equal(t, []string{"claude-cli=sha256:0123456789abcdef:" + filepath.Join(homeDirectory, "adapters", "claude-agent-acp")}, adapters.values)
	})

	t.Run("named profile inherits from default and CLI/env win over profile", func(t *testing.T) {
		t.Parallel()

		flagSet := flag.NewFlagSet("slave", flag.ContinueOnError)
		var kind string
		var concurrency int
		var claudeAuth string
		var roots testListFlag
		var labels testListFlag
		var accelerators testListFlag
		flagSet.StringVar(&kind, "kind", "local", "")
		flagSet.IntVar(&concurrency, "concurrency", 5, "")
		flagSet.StringVar(&claudeAuth, "claude-acp-auth-mode", "api", "")
		flagSet.Var(&roots, "workspace-root", "")
		flagSet.Var(&labels, "label", "")
		flagSet.Var(&accelerators, "accelerator", "")
		require.NoError(t, flagSet.Parse([]string{"--label", "cli-only"}))

		applied, err := localstate.ApplyProfileToFlagSet(flagSet, localstate.ProfileOptions{
			Subcommand:     localstate.SubcommandSlave,
			ProfileFlag:    "gpu",
			ProfileFlagSet: true,
			LookupEnvironment: lookupFromMap(map[string]string{
				"HOME":                           homeDirectory,
				"ORCHESTRA_CLAUDE_ACP_AUTH_MODE": "api",
				"ORCHESTRA_CONCURRENCY":          "5",
			}),
		})
		require.NoError(t, err)
		assert.Equal(t, "gpu", applied.Profile)
		assert.Equal(t, "home-server", kind)
		// Environment beats [gpu.slave] concurrency = 8 and [default] claude_acp_auth_mode.
		assert.Equal(t, 5, concurrency)
		assert.Equal(t, "api", claudeAuth)
		// CLI --label beats [default] label.
		assert.Equal(t, []string{"cli-only"}, labels.values)
		// [gpu] overrides [default] workspace-root and adds accelerator.
		assert.Equal(t, []string{
			filepath.Join(homeDirectory, "Projects"),
			filepath.Join(homeDirectory, "Engine"),
		}, roots.values)
		assert.Equal(t, []string{"cuda"}, accelerators.values)
	})

	t.Run("mcp ignores shared node name while honoring mcp subtable name", func(t *testing.T) {
		t.Parallel()

		mcpHome := t.TempDir()
		mcpConfigPath := filepath.Join(mcpHome, ".orchestra", "config.toml")
		require.NoError(t, os.MkdirAll(filepath.Dir(mcpConfigPath), 0o700))
		require.NoError(t, os.WriteFile(mcpConfigPath, []byte(`
[default]
name = "Mac Studio Worker"
endpoint = "http://100.64.0.10:8787"

[named-agent.mcp]
name = "planner"
`), 0o600))

		defaultFlags := flag.NewFlagSet("mcp", flag.ContinueOnError)
		var endpoint string
		var mailName string
		defaultFlags.StringVar(&endpoint, "endpoint", "http://127.0.0.1:8787", "")
		defaultFlags.StringVar(&mailName, "name", "", "")
		require.NoError(t, defaultFlags.Parse(nil))
		_, err := localstate.ApplyProfileToFlagSet(defaultFlags, localstate.ProfileOptions{
			Subcommand:        localstate.SubcommandMCP,
			LookupEnvironment: lookupFromMap(map[string]string{"HOME": mcpHome}),
		})
		require.NoError(t, err)
		assert.Equal(t, "http://100.64.0.10:8787", endpoint)
		assert.Empty(t, mailName)

		namedFlags := flag.NewFlagSet("mcp", flag.ContinueOnError)
		namedFlags.StringVar(&endpoint, "endpoint", "http://127.0.0.1:8787", "")
		namedFlags.StringVar(&mailName, "name", "", "")
		require.NoError(t, namedFlags.Parse(nil))
		_, err = localstate.ApplyProfileToFlagSet(namedFlags, localstate.ProfileOptions{
			Subcommand:        localstate.SubcommandMCP,
			ProfileFlag:       "named-agent",
			ProfileFlagSet:    true,
			LookupEnvironment: lookupFromMap(map[string]string{"HOME": mcpHome}),
		})
		require.NoError(t, err)
		assert.Equal(t, "planner", mailName)
	})
}

func TestApplyProfileToFlagSet_ValidationErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		tomlContent     string
		subcommand      string
		profileFlag     string
		profileExplicit bool
		expectedMessage string
	}{
		{
			name: "array of tables rejected with helpful hint",
			tomlContent: `
[[default]]
auth = "tailscale"
`,
			subcommand:      localstate.SubcommandMaster,
			expectedMessage: "must be a TOML table ([default]), not an array of tables ([[default]])",
		},
		{
			name: "unknown key in shared profile rejected",
			tomlContent: `
[default]
workpsace-root = "~/CafecitoGames"
`,
			subcommand:      localstate.SubcommandSlave,
			expectedMessage: `unknown setting "workpsace-root" in [default]`,
		},
		{
			name: "master-only key inside slave subtable rejected",
			tomlContent: `
[default.slave]
listen = "127.0.0.1:9000"
`,
			subcommand:      localstate.SubcommandSlave,
			expectedMessage: `unknown or unsupported slave setting "listen" in [default.slave]`,
		},
		{
			name: "duplicate snake_case and kebab-case key rejected",
			tomlContent: `
[default]
workspace-root = "~/A"
workspace_root = "~/B"
`,
			subcommand:      localstate.SubcommandSlave,
			expectedMessage: "duplicate setting",
		},
		{
			name: "top-level setting outside profile table rejected",
			tomlContent: `
auth = "tailscale"
`,
			subcommand:      localstate.SubcommandMaster,
			expectedMessage: `top-level setting "auth" must be placed inside [default]`,
		},
		{
			name: "explicit missing profile fails with available list",
			tomlContent: `
[default]
auth = "tailscale"
`,
			subcommand:      localstate.SubcommandMaster,
			profileFlag:     "production",
			profileExplicit: true,
			expectedMessage: `profile "production" not found`,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			homeDirectory := t.TempDir()
			configPath := filepath.Join(homeDirectory, ".orchestra", "config.toml")
			require.NoError(t, os.MkdirAll(filepath.Dir(configPath), 0o700))
			require.NoError(t, os.WriteFile(configPath, []byte(testCase.tomlContent), 0o600))

			flagSet := flag.NewFlagSet("test", flag.ContinueOnError)
			flagSet.String("auth", "token", "")
			flagSet.String("listen", "127.0.0.1:8787", "")
			var roots testListFlag
			flagSet.Var(&roots, "workspace-root", "")
			require.NoError(t, flagSet.Parse(nil))

			_, err := localstate.ApplyProfileToFlagSet(flagSet, localstate.ProfileOptions{
				Subcommand:        testCase.subcommand,
				ProfileFlag:       testCase.profileFlag,
				ProfileFlagSet:    testCase.profileExplicit,
				LookupEnvironment: lookupFromMap(map[string]string{"HOME": homeDirectory}),
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), testCase.expectedMessage)
		})
	}
}
