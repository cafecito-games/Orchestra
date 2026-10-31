package harness

import (
	"context"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApprovalPolicies_For(t *testing.T) {
	policies := ApprovalPolicies{
		protocol.HarnessClaudeCLI:   protocol.ApprovalPolicyAuto,
		protocol.HarnessCodexCLI:    protocol.ApprovalPolicyBypass,
		protocol.HarnessOpenCodeCLI: protocol.ApprovalPolicyManual,
	}

	assert.Equal(t, protocol.ApprovalPolicyAuto, policies.For(protocol.HarnessClaudeCLI))
	assert.Equal(t, protocol.ApprovalPolicyBypass, policies.For(protocol.HarnessCodexCLI))
	assert.Equal(t, protocol.ApprovalPolicyManual, policies.For(protocol.HarnessOpenCodeCLI))
	assert.Equal(t, protocol.ApprovalPolicyManual, policies.For(protocol.HarnessAntigravityCLI))
	assert.Equal(t, protocol.ApprovalPolicyManual, policies.For("unknown-harness"))
}

func TestApprovalPolicyEffect(t *testing.T) {
	assert.Contains(t, ApprovalPolicyEffect(protocol.ApprovalPolicyAuto), "decides permission requests itself")
	assert.Contains(t, ApprovalPolicyEffect(protocol.ApprovalPolicyBypass), "not sent to Orchestra")
	assert.Contains(t, ApprovalPolicyEffect(protocol.ApprovalPolicyManual), "every ACP permission request is sent")
	assert.Contains(t, ApprovalPolicyEffect("other"), "every ACP permission request is sent")
}

func TestAdvertiseApprovalPolicies(t *testing.T) {
	profiles := []protocol.HarnessProfile{
		{ID: protocol.HarnessClaudeCLI},
		{ID: protocol.HarnessCodexCLI},
		{ID: protocol.HarnessOpenCodeCLI},
		{ID: protocol.HarnessAntigravityCLI},
	}
	policies := ApprovalPolicies{
		protocol.HarnessClaudeCLI:   protocol.ApprovalPolicyAuto,
		protocol.HarnessCodexCLI:    protocol.ApprovalPolicyBypass,
		protocol.HarnessOpenCodeCLI: protocol.ApprovalPolicyManual,
	}

	advertised := AdvertiseApprovalPolicies(profiles, policies)
	require.Len(t, advertised, 4)
	assert.Equal(t, protocol.ApprovalPolicyAuto, advertised[0].ApprovalPolicy)
	assert.Equal(t, protocol.ApprovalPolicyBypass, advertised[1].ApprovalPolicy)
	assert.Empty(t, advertised[2].ApprovalPolicy)
	assert.Empty(t, advertised[3].ApprovalPolicy)
}

func TestOpenCodePermissionHandler_BypassDecisions(t *testing.T) {
	fallbackCalled := false
	fallbackHandler := func(context.Context, acp.PermissionRequest) (acp.PermissionDecision, error) {
		fallbackCalled = true
		return acp.PermissionDecision{OptionID: "fallback"}, nil
	}

	bypassHandler := opencodePermissionHandler(protocol.ApprovalPolicyBypass, fallbackHandler)
	require.NotNil(t, bypassHandler)

	// Decision with allow-once option
	decision, err := bypassHandler(t.Context(), acp.PermissionRequest{
		Options: []protocol.ApprovalOption{
			{ID: "option-reject", Kind: "reject-once"},
			{ID: "option-allow-once", Kind: "allow-once"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "option-allow-once", decision.OptionID)
	assert.False(t, fallbackCalled)

	// Decision with allow-always option
	decision, err = bypassHandler(t.Context(), acp.PermissionRequest{
		Options: []protocol.ApprovalOption{
			{ID: "option-allow-always", Kind: "allow-always"},
			{ID: "option-reject", Kind: "reject-once"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "option-allow-always", decision.OptionID)

	// Empty permission options rejected
	_, err = bypassHandler(t.Context(), acp.PermissionRequest{Options: []protocol.ApprovalOption{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OpenCode offered no allow option for bypass approval policy")

	// Unmatched permission options (e.g. reject only) rejected
	_, err = bypassHandler(t.Context(), acp.PermissionRequest{
		Options: []protocol.ApprovalOption{
			{ID: "option-reject-only", Kind: "reject-once"},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "OpenCode offered no allow option for bypass approval policy")
}

func TestOpenCodePermissionHandler_AutoAndManualUseFallback(t *testing.T) {
	expectedDecision := acp.PermissionDecision{OptionID: "fallback-option"}
	fallbackHandler := func(context.Context, acp.PermissionRequest) (acp.PermissionDecision, error) {
		return expectedDecision, nil
	}

	autoHandler := opencodePermissionHandler(protocol.ApprovalPolicyAuto, fallbackHandler)
	require.NotNil(t, autoHandler)
	decision, err := autoHandler(t.Context(), acp.PermissionRequest{})
	require.NoError(t, err)
	assert.Equal(t, expectedDecision, decision)

	manualHandler := opencodePermissionHandler(protocol.ApprovalPolicyManual, fallbackHandler)
	require.NotNil(t, manualHandler)
	decision, err = manualHandler(t.Context(), acp.PermissionRequest{})
	require.NoError(t, err)
	assert.Equal(t, expectedDecision, decision)
}

func TestHarnessSpecificPermissionModeMapping(t *testing.T) {
	// Claude modes
	assert.Equal(t, claudeAutoPermissionMode, claudePermissionMode(protocol.ApprovalPolicyAuto))
	assert.Equal(t, claudeBypassPermissionMode, claudePermissionMode(protocol.ApprovalPolicyBypass))
	assert.Equal(t, claudeManualPermissionMode, claudePermissionMode(protocol.ApprovalPolicyManual))
	assert.Equal(t, claudeManualPermissionMode, claudePermissionMode("unknown"))

	// Codex modes
	assert.Equal(t, codexAgentMode, codexAgentModeFor(protocol.ApprovalPolicyAuto))
	assert.Equal(t, codexFullAccessMode, codexAgentModeFor(protocol.ApprovalPolicyBypass))
	assert.Equal(t, codexWorkspaceWriteMode, codexAgentModeFor(protocol.ApprovalPolicyManual))
	assert.Equal(t, codexWorkspaceWriteMode, codexAgentModeFor("unknown"))

	// Antigravity modes
	assert.Equal(t, "bypass", antigravityModeForApprovalPolicy(protocol.ApprovalPolicyBypass))
	assert.Equal(t, "sandbox", antigravityModeForApprovalPolicy(protocol.ApprovalPolicyAuto))
	assert.Equal(t, "sandbox", antigravityModeForApprovalPolicy(protocol.ApprovalPolicyManual))
	assert.Equal(t, "sandbox", antigravityModeForApprovalPolicy("unknown"))
}
