package harness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/mcpserver"
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

func TestOpenCodeNativeModelsCanRunConcurrently(t *testing.T) {
	temporaryDirectory := t.TempDir()
	fakeOpenCode := filepath.Join(temporaryDirectory, "opencode")
	script := `#!/bin/sh
case "$*" in
  *zai-coding-plan/glm-5.2*)
    session="ses_glm_52"
    answer="completed with glm"
    ;;
  *deepseek/deepseek-chat*)
    session="ses_deepseek_chat"
    answer="completed with deepseek"
    ;;
  *)
    echo '{"type":"error","error":{"data":{"message":"model was not forwarded"}}}'
    exit 1
    ;;
esac
printf '{"type":"step_start","sessionID":"%s","part":{"type":"step-start"}}\n' "$session"
printf '{"type":"text","sessionID":"%s","part":{"type":"text","text":"%s"}}\n' "$session" "$answer"
printf '{"type":"step_finish","sessionID":"%s","part":{"type":"step-finish","reason":"stop"}}\n' "$session"
`
	require.NoError(t, os.WriteFile(fakeOpenCode, []byte(script), 0o755))

	workspaceDirectory := t.TempDir()
	profiles := []protocol.HarnessProfile{{
		ID:          protocol.HarnessOpenCodeCLI,
		Label:       "OpenCode",
		Description: "opencode v2.2.0",
		Binary:      fakeOpenCode,
		Available:   true,
		AuthMode:    "local-account",
		Transports:  []string{TransportNative},
	}}
	runner := NewRunner(profiles)

	type outcome struct {
		answer  string
		session string
		err     error
	}
	models := []string{"zai-coding-plan/glm-5.2", "deepseek/deepseek-chat"}
	outcomes := make(map[string]outcome, len(models))
	var mutex sync.Mutex
	var waitGroup sync.WaitGroup
	for index, model := range models {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			var session string
			answer, err := runner.Execute(context.Background(), Invocation{
				Run: protocol.Run{
					ID:        "run-" + string(rune('1'+index)),
					HarnessID: protocol.HarnessOpenCodeCLI,
					Model:     model,
					Workspace: workspaceDirectory,
					Prompt:    "Complete this task",
					Transport: TransportNative,
				},
				Workspace: workspaceDirectory,
				ProviderSession: func(identity string) {
					session = identity
				},
			})
			mutex.Lock()
			outcomes[model] = outcome{answer: answer, session: session, err: err}
			mutex.Unlock()
		}()
	}
	waitGroup.Wait()

	require.NoError(t, outcomes[models[0]].err)
	require.Equal(t, "completed with glm", outcomes[models[0]].answer)
	require.Equal(t, "ses_glm_52", outcomes[models[0]].session)
	require.NoError(t, outcomes[models[1]].err)
	require.Equal(t, "completed with deepseek", outcomes[models[1]].answer)
	require.Equal(t, "ses_deepseek_chat", outcomes[models[1]].session)
}

func TestOpenCodeArgumentsAndRuntimeMCPConfiguration(t *testing.T) {
	arguments, err := harnessArguments(protocol.Run{
		HarnessID: protocol.HarnessOpenCodeCLI,
		Model:     "zai-coding-plan/glm-5.2",
		Prompt:    "Implement the change",
	}, "Follow repository instructions.", mcpserver.Configuration{}, protocol.ApprovalPolicyAuto)
	require.NoError(t, err)
	require.Equal(t, []string{"--standalone", "run", "--format", "json", "--auto", "--model", "zai-coding-plan/glm-5.2"}, arguments[:7])
	require.Contains(t, arguments[7], "Follow repository instructions.")
	require.Contains(t, arguments[7], "Implement the change")

	manualArguments, err := harnessArguments(protocol.Run{HarnessID: protocol.HarnessOpenCodeCLI, Prompt: "Inspect the change"}, "", mcpserver.Configuration{}, protocol.ApprovalPolicyManual)
	require.NoError(t, err)
	require.NotContains(t, manualArguments, "--auto")

	configuration, err := opencodeInlineConfiguration(`{"model":"deepseek/deepseek-chat","mcp":{"existing":{"type":"remote","url":"https://example.invalid/mcp"}}}`, mcpserver.Configuration{
		URL:   "http://127.0.0.1:4567/mcp",
		Token: "must-not-appear",
	})
	require.NoError(t, err)
	require.NotContains(t, configuration, "must-not-appear")
	require.Contains(t, configuration, "{env:ORCHESTRA_MCP_TOKEN}")

	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(configuration), &decoded))
	require.Equal(t, "deepseek/deepseek-chat", decoded["model"])
	mcp := decoded["mcp"].(map[string]any)
	require.Contains(t, mcp, "existing")
	require.Contains(t, mcp, mcpServerName)

	v2Configuration, err := opencodeInlineConfiguration(`{"mcp":{"servers":{"existing":{"type":"remote","url":"https://example.invalid/mcp"}}}}`, mcpserver.Configuration{URL: "http://127.0.0.1:4567/mcp"})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(v2Configuration), &decoded))
	mcp = decoded["mcp"].(map[string]any)
	servers := mcp["servers"].(map[string]any)
	require.Contains(t, servers, "existing")
	require.Contains(t, servers, mcpServerName)
}

func TestOpenCodeStreamErrorIsReturned(t *testing.T) {
	_, err := readEvents(strings.NewReader("{\"type\":\"error\",\"error\":{\"data\":{\"message\":\"provider credentials are missing\"}}}\n"), protocol.HarnessOpenCodeCLI, func(string) {}, nil)
	require.EqualError(t, err, "provider credentials are missing")
}

func TestAntigravityNativeExecutionAccumulatesStreamAndCleansEphemeralMCP(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stub uses POSIX sh")
	}

	temporaryDirectory := t.TempDir()
	capturedMCPPathFile := filepath.Join(temporaryDirectory, "mcp-dir.txt")
	capturedArgsFile := filepath.Join(temporaryDirectory, "args.txt")
	fakeAgy := filepath.Join(temporaryDirectory, "agy")
	script := `#!/bin/sh
set -eu
printf '%s\n' "$@" > "` + capturedArgsFile + `"
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--add-dir" ] && [ -f "$2/.agents/mcp_config.json" ]; then
    printf '%s' "$2" > "` + capturedMCPPathFile + `"
  fi
  shift
done
printf '%s\n' '{"event":"init","conversation_id":"agy-conv-777"}'
printf '%s\n' '{"event":"step_update","step_update":{"conversation_id":"agy-conv-777","step_index":1,"state":"ACTIVE","step_type":"thought","text_delta":"Internal reasoning"}}'
printf '%s\n' '{"event":"step_update","step_update":{"conversation_id":"agy-conv-777","step_index":2,"state":"ACTIVE","step_type":"agent_response","text_delta":"Part one"}}'
printf '%s\n' '{"event":"step_update","step_update":{"conversation_id":"agy-conv-777","step_index":2,"state":"DONE","step_type":"agent_response","text_delta":" and part two\n"}}'
printf '%s\n' '{"event":"result","result":{"conversation_id":"agy-conv-777","status":"SUCCESS","response":"Part one and part two\n"}}'
`
	require.NoError(t, os.WriteFile(fakeAgy, []byte(script), 0o755))

	workspaceDirectory := t.TempDir()
	authorized, err := AuthorizeWorkspace(workspaceDirectory, []string{workspaceDirectory})
	require.NoError(t, err)

	profiles := []protocol.HarnessProfile{{
		ID:          protocol.HarnessAntigravityCLI,
		Label:       "Antigravity",
		Description: "1.4.11",
		Binary:      fakeAgy,
		Available:   true,
		Models:      []string{},
		Transports:  []string{TransportNative},
	}}
	runner := NewRunner(profiles)

	var streamed strings.Builder
	var providerSession string
	result, err := runner.Execute(context.Background(), Invocation{
		Run: protocol.Run{
			ID:        "run-agy-1",
			HarnessID: protocol.HarnessAntigravityCLI,
			Model:     "gemini-4-argon-medium",
			Workspace: authorized,
			Prompt:    "Verify Antigravity native runner",
			Transport: TransportNative,
		},
		Workspace: authorized,
		MCP: mcpserver.Configuration{
			URL:         "http://127.0.0.1:8787/mcp",
			Token:       "ephemeral-mcp-secret",
			CanDelegate: false,
		},
		Resume: &SessionResume{
			ProviderSessionID: "agy-conv-prev",
		},
		Output: func(chunk string) {
			streamed.WriteString(chunk)
		},
		ProviderSession: func(identity string) {
			providerSession = identity
		},
	})
	require.NoError(t, err)
	require.Equal(t, "Part one and part two", result)
	require.Equal(t, "Part one and part two\n", streamed.String())
	require.Equal(t, "agy-conv-777", providerSession)

	recordedMCPDirectoryBytes, err := os.ReadFile(capturedMCPPathFile)
	require.NoError(t, err)
	recordedMCPDirectory := string(recordedMCPDirectoryBytes)
	require.NotEmpty(t, recordedMCPDirectory)
	_, statErr := os.Stat(recordedMCPDirectory)
	require.ErrorIs(t, statErr, os.ErrNotExist)

	recordedArgsBytes, err := os.ReadFile(capturedArgsFile)
	require.NoError(t, err)
	recordedArgs := string(recordedArgsBytes)
	require.Contains(t, recordedArgs, "--conversation\nagy-conv-prev")
	require.Contains(t, recordedArgs, "--model\ngemini-4-argon-medium")
	require.Contains(t, recordedArgs, "--sandbox")
	require.NotContains(t, recordedArgs, "ephemeral-mcp-secret")
}
