package harness

import (
	"context"
	"errors"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestBuiltInOpenCodeACPAdapterAndModelSelection(t *testing.T) {
	verificationErr := errors.New("verification called")
	managed := &ManagedHarness{Verify: func() error { return verificationErr }}
	adapter, available := BuiltInOpenCodeACPAdapter(protocol.HarnessProfile{
		ID:          protocol.HarnessOpenCodeCLI,
		Available:   true,
		Binary:      "/opt/homebrew/bin/opencode",
		Description: "opencode v2.2.0",
	}, managed)
	require.True(t, available)
	require.Equal(t, "/opt/homebrew/bin/opencode", adapter.Binary)
	require.Equal(t, []string{"acp"}, adapter.Arguments)
	require.Equal(t, "2.2.0", adapter.Version)
	require.ErrorIs(t, adapter.Verify(), verificationErr)

	provider, present := DefaultACPProviders()[protocol.HarnessOpenCodeCLI]
	require.True(t, present)
	require.True(t, provider.RequireMCPConnection)
	require.Equal(t, opencodeModelOption, provider.ModelOption)
	require.Equal(t, []acp.ConfigurationSelection{{
		ID:          opencodeModelOption,
		Value:       "deepseek/deepseek-v4-pro",
		Requirement: acp.ConfigurationRequested,
	}}, provider.Configuration(protocol.Run{Model: "deepseek/deepseek-v4-pro"}, protocol.ApprovalPolicyManual))
	require.Empty(t, provider.Configuration(protocol.Run{Model: "default"}, protocol.ApprovalPolicyManual))

	bypass := provider.Permission(protocol.ApprovalPolicyBypass, nil)
	decision, err := bypass(context.Background(), acp.PermissionRequest{Options: []protocol.ApprovalOption{
		{ID: "reject", Kind: "reject-once"},
		{ID: "allow", Kind: "allow-once"},
	}})
	require.NoError(t, err)
	require.Equal(t, "allow", decision.OptionID)
}
