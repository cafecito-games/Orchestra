package harness

import (
	"errors"
	"fmt"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const (
	claudeModeOption           = "mode"
	claudeModelOption          = "model"
	claudeManualPermissionMode = "default"
	claudeAutoPermissionMode   = "auto"
	claudeBypassPermissionMode = "bypassPermissions"
)

func claudePermissionMode(approvalPolicy string) string {
	switch approvalPolicy {
	case protocol.ApprovalPolicyAuto:
		return claudeAutoPermissionMode
	case protocol.ApprovalPolicyBypass:
		return claudeBypassPermissionMode
	default:
		return claudeManualPermissionMode
	}
}

func claudeACPProvider() ACPProvider {
	return ACPProvider{
		Configuration:        claudeSessionConfiguration,
		RequireMCPConnection: true,
		NativeBinaryVariable: "CLAUDE_CODE_EXECUTABLE",
		ModelOption:          claudeModelOption,
	}
}

func claudeSessionConfiguration(run protocol.Run, approvalPolicy string) []acp.ConfigurationSelection {
	selections := []acp.ConfigurationSelection{{ID: claudeModeOption, Value: claudePermissionMode(approvalPolicy), Requirement: acp.ConfigurationPolicy}}
	if run.Model != "" && run.Model != "default" {
		selections = append(selections, acp.ConfigurationSelection{ID: claudeModelOption, Value: run.Model, Requirement: acp.ConfigurationRequested})
	}
	return selections
}

const (
	ClaudeACPAuthModeLocalSubscription = "local-subscription"
	ClaudeACPAuthModeAPI               = "api"
)

var ErrClaudeACPAuthModeNotConfigured = errors.New("claude ACP auth mode is not configured")

var ErrClaudeACPBillingVariablePresent = errors.New("a billing-switching environment variable is present for local-subscription Claude ACP")

var ClaudeACPBillingSwitchingVariables = []string{
	"ANTHROPIC_BASE_URL",
	"ANTHROPIC_BEDROCK_BASE_URL",
	"ANTHROPIC_VERTEX_BASE_URL",
	"CLAUDE_CODE_USE_BEDROCK",
	"CLAUDE_CODE_USE_VERTEX",
	"ANTHROPIC_VERTEX_PROJECT_ID",
	"CLOUD_ML_REGION",
	"AWS_REGION",
	"ANTHROPIC_CUSTOM_HEADERS",
	"ANTHROPIC_API_KEY",
	"ANTHROPIC_AUTH_TOKEN",
	"CLAUDE_CODE_OAUTH_TOKEN",
}

func ClaudeACPAuthGate(mode string, environment []string) (authMode string, err error) {
	switch mode {
	case ClaudeACPAuthModeAPI:
		return ClaudeACPAuthModeAPI, nil
	case ClaudeACPAuthModeLocalSubscription:
		if variable, present := firstPresentEnvironmentVariable(ClaudeACPBillingSwitchingVariables, environment); present {
			return "", fmt.Errorf("%w: %s is set in Orchestra's environment, which would move Claude ACP off its local subscription login; unset it or set --claude-acp-auth-mode=api", ErrClaudeACPBillingVariablePresent, variable)
		}
		return ClaudeACPAuthModeLocalSubscription, nil
	default:
		return "", fmt.Errorf("%w: set --claude-acp-auth-mode (or ORCHESTRA_CLAUDE_ACP_AUTH_MODE) to %q or %q", ErrClaudeACPAuthModeNotConfigured, ClaudeACPAuthModeLocalSubscription, ClaudeACPAuthModeAPI)
	}
}

func firstPresentEnvironmentVariable(names []string, environment []string) (string, bool) {
	present := map[string]bool{}
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		present[strings.ToUpper(name)] = true
	}
	for _, name := range names {
		if present[strings.ToUpper(name)] {
			return name, true
		}
	}
	return "", false
}
