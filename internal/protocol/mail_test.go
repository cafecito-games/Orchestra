package protocol_test

import (
	"strings"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/assert"
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
			assert.Equal(t, testCase.valid, ok)
			assert.Equal(t, testCase.expected, normalized)
		})
	}

	assert.Equal(t, "fabrica", protocol.NormalizeHostnameSlug("Fabrica.local"))
	assert.Equal(t, "bunker-node-1", protocol.NormalizeHostnameSlug("Bunker Node 1.lan"))
	assert.Equal(t, "local", protocol.NormalizeHostnameSlug("..."))

	assert.Equal(t, "claude", protocol.NormalizeHarnessMailSlug("claude-code"))
	assert.Equal(t, "claude", protocol.NormalizeHarnessMailSlug("claude-cli"))
	assert.Equal(t, "codex", protocol.NormalizeHarnessMailSlug("codex-mcp-client"))
	assert.Equal(t, "opencode", protocol.NormalizeHarnessMailSlug("opencode-cli"))
	assert.Equal(t, "antigravity", protocol.NormalizeHarnessMailSlug("agy"))
	assert.Equal(t, "cursor", protocol.NormalizeHarnessMailSlug("Cursor Agent"))
	assert.Equal(t, "gemini", protocol.NormalizeHarnessMailSlug("gemini-cli"))
	assert.Equal(t, "agent", protocol.NormalizeHarnessMailSlug("orchestra"))
	assert.Equal(t, "agent", protocol.NormalizeHarnessMailSlug("mcp-client-a1b2c3d4"))
	assert.Equal(t, "custom-runner", protocol.NormalizeHarnessMailSlug("Custom Runner"))

	sanitized, ok := protocol.SanitizeMailHandle("Auth API Reviewer #2")
	require.True(t, ok)
	assert.Equal(t, "auth-api-reviewer-2", sanitized)

	formatted, err := protocol.FormatMailAddress("Claude1234", "Fabrica")
	require.NoError(t, err)
	assert.Equal(t, "claude1234@fabrica", formatted)

	parsedHandle, parsedHost, parsedAddress, ok := protocol.ParseMailAddress(" Codex345@Bunker ")
	require.True(t, ok)
	assert.Equal(t, "codex345", parsedHandle)
	assert.Equal(t, "bunker", parsedHost)
	assert.Equal(t, "codex345@bunker", parsedAddress)

	_, _, _, ok = protocol.ParseMailAddress("missing-host")
	assert.False(t, ok)
	_, _, _, ok = protocol.ParseMailAddress("two@at@signs")
	assert.False(t, ok)

	renameHandle, renameHost, renameAddress, err := protocol.NormalizeRenameTarget("Auth-Worker", "Fabrica")
	require.NoError(t, err)
	assert.Equal(t, "auth-worker", renameHandle)
	assert.Equal(t, "fabrica", renameHost)
	assert.Equal(t, "auth-worker@fabrica", renameAddress)

	renameHandle, renameHost, renameAddress, err = protocol.NormalizeRenameTarget("Reviewer@Bunker", "fabrica")
	require.NoError(t, err)
	assert.Equal(t, "reviewer", renameHandle)
	assert.Equal(t, "bunker", renameHost)
	assert.Equal(t, "reviewer@bunker", renameAddress)

	_, _, _, err = protocol.NormalizeRenameTarget("", "fabrica")
	require.Error(t, err)
	_, _, _, err = protocol.NormalizeRenameTarget("bad name!", "fabrica")
	require.Error(t, err)
}
