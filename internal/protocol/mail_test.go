package protocol_test

import (
	"strings"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestMailAddressNormalizationAndParsing(t *testing.T) {
	handleCases := []struct {
		name     string
		raw      string
		expected string
		valid    bool
	}{
		{name: "simple handle", raw: "claude1234", expected: "claude1234", valid: true},
		{name: "uppercase trimmed", raw: "  Auth-API_2.0  ", expected: "auth-api_2.0", valid: true},
		{name: "single character", raw: "a", expected: "a", valid: true},
		{name: "empty", raw: "   ", expected: "", valid: false},
		{name: "leading hyphen rejected", raw: "-claude", expected: "", valid: false},
		{name: "trailing dot rejected", raw: "claude.", expected: "", valid: false},
		{name: "contains at symbol rejected", raw: "claude@fabrica", expected: "", valid: false},
		{name: "secret-like handle rejected", raw: "sk-ant-api03-1234567890abcdef", expected: "", valid: false},
		{name: "exceeds max length rejected", raw: strings.Repeat("a", protocol.MailHandleMaximumBytes+1), expected: "", valid: false},
	}

	for _, testCase := range handleCases {
		t.Run(testCase.name, func(t *testing.T) {
			normalized, ok := protocol.NormalizeMailHandle(testCase.raw)
			require.Equal(t, testCase.valid, ok)
			require.Equal(t, testCase.expected, normalized)
		})
	}

	require.Equal(t, "fabrica", protocol.NormalizeHostnameSlug("Fabrica.local"))
	require.Equal(t, "bunker-node-1", protocol.NormalizeHostnameSlug("Bunker Node 1.lan"))
	require.Equal(t, "local", protocol.NormalizeHostnameSlug("..."))

	require.Equal(t, "claude", protocol.NormalizeHarnessMailSlug("claude-code"))
	require.Equal(t, "claude", protocol.NormalizeHarnessMailSlug("claude-cli"))
	require.Equal(t, "codex", protocol.NormalizeHarnessMailSlug("codex-mcp-client"))
	require.Equal(t, "opencode", protocol.NormalizeHarnessMailSlug("opencode-cli"))
	require.Equal(t, "antigravity", protocol.NormalizeHarnessMailSlug("agy"))
	require.Equal(t, "cursor", protocol.NormalizeHarnessMailSlug("Cursor Agent"))
	require.Equal(t, "gemini", protocol.NormalizeHarnessMailSlug("gemini-cli"))
	require.Equal(t, "agent", protocol.NormalizeHarnessMailSlug("orchestra"))
	require.Equal(t, "agent", protocol.NormalizeHarnessMailSlug("mcp-client-a1b2c3d4"))
	require.Equal(t, "custom-runner", protocol.NormalizeHarnessMailSlug("Custom Runner"))

	sanitized, ok := protocol.SanitizeMailHandle("Auth API Reviewer #2")
	require.True(t, ok)
	require.Equal(t, "auth-api-reviewer-2", sanitized)

	formatted, err := protocol.FormatMailAddress("Claude1234", "Fabrica")
	require.NoError(t, err)
	require.Equal(t, "claude1234@fabrica", formatted)

	parsedHandle, parsedHost, parsedAddress, ok := protocol.ParseMailAddress(" Codex345@Bunker ")
	require.True(t, ok)
	require.Equal(t, "codex345", parsedHandle)
	require.Equal(t, "bunker", parsedHost)
	require.Equal(t, "codex345@bunker", parsedAddress)

	_, _, _, ok = protocol.ParseMailAddress("missing-host")
	require.False(t, ok)
	_, _, _, ok = protocol.ParseMailAddress("two@at@signs")
	require.False(t, ok)

	renameHandle, renameHost, renameAddress, err := protocol.NormalizeRenameTarget("Auth-Worker", "Fabrica")
	require.NoError(t, err)
	require.Equal(t, "auth-worker", renameHandle)
	require.Equal(t, "fabrica", renameHost)
	require.Equal(t, "auth-worker@fabrica", renameAddress)

	renameHandle, renameHost, renameAddress, err = protocol.NormalizeRenameTarget("Reviewer@Bunker", "fabrica")
	require.NoError(t, err)
	require.Equal(t, "reviewer", renameHandle)
	require.Equal(t, "bunker", renameHost)
	require.Equal(t, "reviewer@bunker", renameAddress)

	_, _, _, err = protocol.NormalizeRenameTarget("", "fabrica")
	require.Error(t, err)
	_, _, _, err = protocol.NormalizeRenameTarget("bad name!", "fabrica")
	require.Error(t, err)
}
