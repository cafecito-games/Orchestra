package localstate_test

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/localstate"
)

func TestEnsureMasterTokenAndConnectionState(t *testing.T) {
	temporaryHome := t.TempDir()
	dataDirectory := filepath.Join(temporaryHome, ".local", "share", "orchestra", "master")

	generatedToken, tokenPath, generated, err := localstate.EnsureMasterToken(dataDirectory, "")
	require.NoError(t, err)
	require.True(t, generated)
	require.Len(t, generatedToken, 64)
	require.Equal(t, filepath.Join(dataDirectory, "token"), tokenPath)

	if runtime.GOOS != "windows" {
		information, statErr := os.Stat(tokenPath)
		require.NoError(t, statErr)
		require.Equal(t, os.FileMode(0o600), information.Mode().Perm())
	}

	reusedToken, _, reusedGenerated, err := localstate.EnsureMasterToken(dataDirectory, "")
	require.NoError(t, err)
	require.False(t, reusedGenerated)
	require.Equal(t, generatedToken, reusedToken)

	overrideToken, _, overrideGenerated, err := localstate.EnsureMasterToken(dataDirectory, "explicit-token-value")
	require.NoError(t, err)
	require.False(t, overrideGenerated)
	require.Equal(t, "explicit-token-value", overrideToken)

	persistedStillSame, _, _, err := localstate.EnsureMasterToken(dataDirectory, "")
	require.NoError(t, err)
	require.Equal(t, generatedToken, persistedStillSame)

	environment := map[string]string{"HOME": temporaryHome}
	lookupEnvironment := func(name string) (string, bool) {
		value, found := environment[name]
		return value, found
	}

	connectionPath := localstate.ResolveConnectionFilePath(lookupEnvironment)
	require.Equal(t, filepath.Join(temporaryHome, ".local", "share", "orchestra", "connection.json"), connectionPath)

	require.Empty(t, localstate.ResolveConnectionFilePath(func(string) (string, bool) {
		return "", false
	}))

	err = localstate.WriteConnectionState(connectionPath, localstate.ConnectionState{
		Endpoint:      "http://100.64.10.20:8787",
		Token:         generatedToken,
		DataDirectory: dataDirectory,
		ProcessID:     4242,
	})
	require.NoError(t, err)

	if runtime.GOOS != "windows" {
		information, statErr := os.Stat(connectionPath)
		require.NoError(t, statErr)
		require.Equal(t, os.FileMode(0o600), information.Mode().Perm())
	}

	loaded, found, err := localstate.ReadConnectionState(connectionPath)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "http://100.64.10.20:8787", loaded.Endpoint)
	require.Equal(t, generatedToken, loaded.Token)
	require.Equal(t, 4242, loaded.ProcessID)
}

func TestLocalDialEndpoint(t *testing.T) {
	cases := []struct {
		scheme   string
		bound    string
		expected string
	}{
		{scheme: "http", bound: "0.0.0.0:8787", expected: "http://127.0.0.1:8787"},
		{scheme: "https", bound: "[::]:8787", expected: "https://127.0.0.1:8787"},
		{scheme: "http", bound: "127.0.0.1:8787", expected: "http://127.0.0.1:8787"},
		{scheme: "http", bound: "100.64.0.9:8787", expected: "http://100.64.0.9:8787"},
		{scheme: "https", bound: "[fd7a:115c:a1e0::1]:8787", expected: "https://[fd7a:115c:a1e0::1]:8787"},
	}
	for _, testCase := range cases {
		require.Equal(t, testCase.expected, localstate.LocalDialEndpoint(testCase.scheme, testCase.bound))
	}
}

func TestInstallInspectAndUninstallHarnessMCPConfigs(t *testing.T) {
	temporaryHome := t.TempDir()

	claudePath := filepath.Join(temporaryHome, ".claude.json")
	require.NoError(t, os.WriteFile(claudePath, []byte(`{
  "numStartups": 12,
  "mcpServers": {
    "github": {
      "command": "gh-mcp"
    },
    "orchestra": {
      "command": "orchestra",
      "args": ["mcp", "--endpoint", "http://100.64.0.1:8787"],
      "env": {
        "ORCHESTRA_TOKEN": "stale-token"
      }
    }
  }
}
`), 0o600))

	codexDirectory := filepath.Join(temporaryHome, ".codex")
	require.NoError(t, os.MkdirAll(codexDirectory, 0o755))
	codexPath := filepath.Join(codexDirectory, "config.toml")
	require.NoError(t, os.WriteFile(codexPath, []byte(`# User codex settings
model = "gpt-5-codex"

[mcp_servers.github]
command = "gh-mcp"

[mcp_servers.orchestra]
command = "orchestra"
args = ["mcp", "--endpoint", "http://100.64.0.1:8787"]

[mcp_servers.orchestra.env]
ORCHESTRA_TOKEN = "stale-token"

[projects."/Users/test/repo"]
trust_level = "trusted"
`), 0o600))

	antigravityDirectory := filepath.Join(temporaryHome, ".gemini")
	require.NoError(t, os.MkdirAll(antigravityDirectory, 0o755))

	openCodeDirectory := filepath.Join(temporaryHome, ".config", "opencode")
	require.NoError(t, os.MkdirAll(openCodeDirectory, 0o755))

	lookupEnvironment := func(name string) (string, bool) {
		if name == "HOME" {
			return temporaryHome, true
		}
		return "", false
	}
	lookPathNone := func(string) (string, error) {
		return "", errors.New("not on path")
	}

	binaryPath := filepath.Join(temporaryHome, "bin", "orchestra")
	installed, err := localstate.InstallHarnessMCPConfigs(localstate.MCPInstallOptions{
		BinaryPath:        binaryPath,
		MailName:          "lead",
		LookupEnvironment: lookupEnvironment,
		LookPath:          lookPathNone,
	})
	require.NoError(t, err)
	require.Len(t, installed, 4)

	inspected, err := localstate.InspectHarnessMCPConfigs(lookupEnvironment, lookPathNone)
	require.NoError(t, err)
	require.Len(t, inspected, 4)
	for _, entry := range inspected {
		require.True(t, entry.Configured, entry.Target)
		require.Equal(t, binaryPath, entry.Command, entry.Target)
		require.Equal(t, []string{"mcp", "--name", "lead"}, entry.Arguments, entry.Target)
	}

	updatedClaudeBytes, err := os.ReadFile(claudePath)
	require.NoError(t, err)
	require.Contains(t, string(updatedClaudeBytes), `"numStartups": 12`)
	require.Contains(t, string(updatedClaudeBytes), `"github"`)
	require.NotContains(t, string(updatedClaudeBytes), "stale-token")

	updatedCodexBytes, err := os.ReadFile(codexPath)
	require.NoError(t, err)
	updatedCodex := string(updatedCodexBytes)
	require.Contains(t, updatedCodex, `# User codex settings`)
	require.Contains(t, updatedCodex, `[mcp_servers.github]`)
	require.Contains(t, updatedCodex, `[projects."/Users/test/repo"]`)
	require.NotContains(t, updatedCodex, "stale-token")
	require.NotContains(t, updatedCodex, `[mcp_servers.orchestra.env]`)

	reinstalled, err := localstate.InstallHarnessMCPConfigs(localstate.MCPInstallOptions{
		BinaryPath:        binaryPath,
		MailName:          "lead",
		LookupEnvironment: lookupEnvironment,
		LookPath:          lookPathNone,
	})
	require.NoError(t, err)
	for _, entry := range reinstalled {
		require.Equal(t, "unchanged", entry.Action, entry.Target)
	}

	removed, err := localstate.UninstallHarnessMCPConfigs(localstate.MCPInstallOptions{
		LookupEnvironment: lookupEnvironment,
		LookPath:          lookPathNone,
	})
	require.NoError(t, err)
	require.Len(t, removed, 4)
	for _, entry := range removed {
		require.Equal(t, "removed", entry.Action, entry.Target)
	}

	afterUninstall, err := localstate.InspectHarnessMCPConfigs(lookupEnvironment, lookPathNone)
	require.NoError(t, err)
	for _, entry := range afterUninstall {
		require.False(t, entry.Configured, entry.Target)
	}
}
