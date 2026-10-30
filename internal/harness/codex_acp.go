package harness

import (
	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const (
	codexModeOption         = "mode"
	codexModelOption        = "model"
	codexWorkspaceWriteMode = "read-only"
	codexAgentMode          = "agent"
	codexFullAccessMode     = "agent-full-access"
)

func codexAgentModeFor(approvalPolicy string) string {
	switch approvalPolicy {
	case protocol.ApprovalPolicyAuto:
		return codexAgentMode
	case protocol.ApprovalPolicyBypass:
		return codexFullAccessMode
	default:
		return codexWorkspaceWriteMode
	}
}

func codexACPProvider() ACPProvider {
	return ACPProvider{
		Environment: func(approvalPolicy string) []string {
			return []string{"INITIAL_AGENT_MODE=" + codexAgentModeFor(approvalPolicy)}
		},
		Configuration:        codexSessionConfiguration,
		RequireMCPConnection: true,
		NativeBinaryVariable: "CODEX_PATH",
		ModelOption:          codexModelOption,
	}
}

func codexSessionConfiguration(run protocol.Run, approvalPolicy string) []acp.ConfigurationSelection {
	selections := []acp.ConfigurationSelection{{ID: codexModeOption, Value: codexAgentModeFor(approvalPolicy), Requirement: acp.ConfigurationPolicy}}
	if run.Model != "" && run.Model != "default" {
		selections = append(selections, acp.ConfigurationSelection{ID: codexModelOption, Value: run.Model, Requirement: acp.ConfigurationRequested})
	}
	return selections
}
