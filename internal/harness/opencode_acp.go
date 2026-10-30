package harness

import (
	"context"
	"errors"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const opencodeModelOption = "model"

func opencodeACPProvider() ACPProvider {
	return ACPProvider{
		Configuration:        opencodeSessionConfiguration,
		Permission:           opencodePermissionHandler,
		RequireMCPConnection: true,
		ModelOption:          opencodeModelOption,
	}
}

func opencodePermissionHandler(approvalPolicy string, fallback PermissionHandler) PermissionHandler {
	if approvalPolicy != protocol.ApprovalPolicyBypass {
		return fallback
	}
	return func(_ context.Context, request acp.PermissionRequest) (acp.PermissionDecision, error) {
		for _, kind := range []string{"allow-once", "allow-always"} {
			for _, option := range request.Options {
				if option.Kind == kind {
					return acp.PermissionDecision{OptionID: option.ID}, nil
				}
			}
		}
		return acp.PermissionDecision{}, errors.New("OpenCode offered no allow option for bypass approval policy")
	}
}

func opencodeSessionConfiguration(run protocol.Run, _ string) []acp.ConfigurationSelection {
	if run.Model == "" || run.Model == "default" {
		return nil
	}
	return []acp.ConfigurationSelection{{
		ID:          opencodeModelOption,
		Value:       run.Model,
		Requirement: acp.ConfigurationRequested,
	}}
}

// BuiltInOpenCodeACPAdapter exposes OpenCode's own `opencode acp` server as
// an adapter without requiring a separately installed ACP component.
func BuiltInOpenCodeACPAdapter(profile protocol.HarnessProfile, managed *ManagedHarness) (ACPAdapter, bool) {
	if profile.ID != protocol.HarnessOpenCodeCLI || !profile.Available || profile.Binary == "" {
		return ACPAdapter{}, false
	}
	adapter := ACPAdapter{
		Binary:    profile.Binary,
		Arguments: []string{"acp"},
		Version:   protocol.ExtractNormalizedVersion(profile.Description),
	}
	if managed != nil {
		adapter.Verify = managed.Verify
	}
	return adapter, true
}
