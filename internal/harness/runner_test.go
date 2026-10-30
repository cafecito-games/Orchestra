package harness

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestClaudeACPAuthGateAndNativeExecution(t *testing.T) {
	_, err := ClaudeACPAuthGate("", nil)
	require.ErrorIs(t, err, ErrClaudeACPAuthModeNotConfigured)

	_, err = ClaudeACPAuthGate(ClaudeACPAuthModeLocalSubscription, []string{"ANTHROPIC_API_KEY=sk-ant-12345678901234567890"})
	require.ErrorIs(t, err, ErrClaudeACPBillingVariablePresent)

	mode, err := ClaudeACPAuthGate(ClaudeACPAuthModeLocalSubscription, []string{"PATH=/usr/bin"})
	require.NoError(t, err)
	require.Equal(t, ClaudeACPAuthModeLocalSubscription, mode)

	mode, err = ClaudeACPAuthGate(ClaudeACPAuthModeAPI, []string{"ANTHROPIC_API_KEY=sk-ant-12345678901234567890"})
	require.NoError(t, err)
	require.Equal(t, ClaudeACPAuthModeAPI, mode)

	temporaryDirectory := t.TempDir()
	fakeClaude := filepath.Join(temporaryDirectory, "claude")
	script := "#!/bin/sh\necho '{\"type\":\"assistant\",\"session_id\":\"sess-123\",\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"hello from claude\"}]}}'\necho '{\"type\":\"result\",\"result\":\"final claude answer\"}'\n"
	require.NoError(t, os.WriteFile(fakeClaude, []byte(script), 0o755))

	workspaceDirectory := t.TempDir()
	authorized, err := AuthorizeWorkspace(workspaceDirectory, []string{workspaceDirectory})
	require.NoError(t, err)

	profiles := []protocol.HarnessProfile{{
		ID:          "claude-cli",
		Label:       "Claude Code",
		Description: "2.1.231",
		Binary:      fakeClaude,
		Available:   true,
		Models:      []string{"sonnet"},
		Transports:  []string{TransportNative},
	}}
	runner := NewRunner(profiles)

	var providerSession string
	var startedSelection protocol.RunTransportSelection
	result, err := runner.Execute(context.Background(), Invocation{
		Run: protocol.Run{
			ID:        "run-1",
			HarnessID: "claude-cli",
			Model:     "sonnet",
			Workspace: authorized,
			Prompt:    "Say hello",
			Transport: TransportNative,
		},
		Workspace: authorized,
		ProviderSession: func(identity string) {
			providerSession = identity
		},
		Started: func(selection protocol.RunTransportSelection) {
			startedSelection = selection
		},
	})
	require.NoError(t, err)
	require.Equal(t, "final claude answer", result)
	require.Equal(t, "sess-123", providerSession)
	require.Equal(t, TransportNative, startedSelection.SelectedTransport)
	require.Equal(t, "2.1.231", startedSelection.HarnessVersion)
}
