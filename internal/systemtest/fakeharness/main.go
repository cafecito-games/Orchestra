// Command fakeharness emulates Claude and Codex native CLIs and ACP v1 stdio
// adapters for deterministic multi-node Orchestra system tests.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	recordDirectoryVariable  = "ORCHESTRA_FAKE_RECORD_DIRECTORY"
	sessionDirectoryVariable = "ORCHESTRA_FAKE_SESSION_DIRECTORY"
	gateDirectoryVariable    = "ORCHESTRA_FAKE_GATE_DIRECTORY"
)

var observedEnvironmentVariables = []string{
	"OPENAI_API_KEY",
	"CODEX_API_KEY",
	"ANTHROPIC_API_KEY",
	"CLAUDE_CODE_OAUTH_TOKEN",
	"GITHUB_TOKEN",
	"ORCHESTRA_TOKEN",
	"ORCHESTRA_MCP_TOKEN",
	"INITIAL_AGENT_MODE",
	"CODEX_PATH",
	"CLAUDE_CODE_EXECUTABLE",
	"AGY_EXECUTABLE",
	"ORCHESTRA_ANTIGRAVITY_INITIAL_MODE",
}

func main() {
	role := strings.TrimSuffix(filepath.Base(os.Args[0]), filepath.Ext(os.Args[0]))
	eventRecorder := newEventRecorder(role)
	var exitCode int
	switch role {
	case "codex-acp":
		exitCode = runACP(eventRecorder, codexFlavor)
	case "claude-agent-acp":
		exitCode = runACP(eventRecorder, claudeFlavor)
	case "codex", "claude", "agy":
		exitCode = runNative(eventRecorder, role, os.Args[1:])
	default:
		fmt.Fprintf(os.Stderr, "fakeharness: unknown executable role %q\n", role)
		exitCode = 64
	}
	terminateProcess(exitCode)
}
