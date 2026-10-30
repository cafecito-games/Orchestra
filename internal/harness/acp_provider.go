package harness

import (
	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

type ACPProvider struct {
	Environment          func(approvalPolicy string) []string
	Configuration        func(run protocol.Run, approvalPolicy string) []acp.ConfigurationSelection
	RequireMCPConnection bool
	NativeBinaryVariable string
	ModelOption          string
}

func DefaultACPProviders() map[string]ACPProvider {
	return map[string]ACPProvider{
		"claude-cli": claudeACPProvider(),
		"codex-cli":  codexACPProvider(),
	}
}
