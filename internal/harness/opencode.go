package harness

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/mcpserver"
)

const opencodeConfigEnvironment = "OPENCODE_CONFIG_CONTENT"

func opencodeInlineConfiguration(existing string, mcpConfiguration mcpserver.Configuration) (string, error) {
	configuration := map[string]any{}
	if strings.TrimSpace(existing) != "" {
		if err := json.Unmarshal([]byte(existing), &configuration); err != nil {
			return "", errors.New("OpenCode OPENCODE_CONFIG_CONTENT is not valid JSON")
		}
	}

	mcp, err := objectAt(configuration, "mcp")
	if err != nil {
		return "", err
	}
	server := map[string]any{
		"type": "remote",
		"url":  mcpConfiguration.URL,
		"headers": map[string]string{
			"Authorization": "Bearer {env:ORCHESTRA_MCP_TOKEN}",
		},
	}
	if rawServers, usesV2Shape := mcp["servers"]; usesV2Shape {
		servers, valid := rawServers.(map[string]any)
		if !valid {
			return "", errors.New("OpenCode OPENCODE_CONFIG_CONTENT mcp.servers must be an object")
		}
		servers[mcpServerName] = server
	} else {
		server["enabled"] = true
		mcp[mcpServerName] = server
	}

	encoded, err := json.Marshal(configuration)
	if err != nil {
		return "", errors.New("could not encode OpenCode runtime configuration")
	}
	return string(encoded), nil
}

func objectAt(parent map[string]any, name string) (map[string]any, error) {
	raw, present := parent[name]
	if !present {
		child := map[string]any{}
		parent[name] = child
		return child, nil
	}
	child, valid := raw.(map[string]any)
	if !valid {
		return nil, errors.New("OpenCode OPENCODE_CONFIG_CONTENT " + name + " must be an object")
	}
	return child, nil
}

func environmentValue(environment []string, name string) string {
	prefix := name + "="
	for _, entry := range environment {
		if after, ok := strings.CutPrefix(entry, prefix); ok {
			return after
		}
	}
	return ""
}

func replaceEnvironmentValue(environment []string, name, value string) []string {
	prefix := name + "="
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			result = append(result, entry)
		}
	}
	return append(result, prefix+value)
}
