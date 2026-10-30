package harness

import "github.com/cafecito-games/Orchestra/internal/protocol"

type ApprovalPolicies map[string]string

var ApprovalPolicyHarnessIDs = []string{protocol.HarnessClaudeCLI, protocol.HarnessCodexCLI, protocol.HarnessOpenCodeCLI}

func (policies ApprovalPolicies) For(harnessID string) string {
	switch policy := policies[harnessID]; policy {
	case protocol.ApprovalPolicyAuto, protocol.ApprovalPolicyBypass:
		return policy
	default:
		return protocol.ApprovalPolicyManual
	}
}

func ApprovalPolicyEffect(policy string) string {
	switch policy {
	case protocol.ApprovalPolicyAuto:
		return "the harness decides permission requests itself, and only requests it still escalates are sent to Orchestra"
	case protocol.ApprovalPolicyBypass:
		return "permission requests are not sent to Orchestra"
	default:
		return "every ACP permission request is sent to Orchestra"
	}
}

func reportedApprovalPolicy(policy string) string {
	if policy != protocol.ApprovalPolicyAuto && policy != protocol.ApprovalPolicyBypass {
		return ""
	}
	return policy
}

func AdvertiseApprovalPolicies(profiles []protocol.HarnessProfile, policies ApprovalPolicies) []protocol.HarnessProfile {
	for index := range profiles {
		profiles[index].ApprovalPolicy = reportedApprovalPolicy(policies.For(profiles[index].ID))
	}
	return profiles
}
