package antigravityacp

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseModelsOutputSkipsSpinnerAndExtractsModels(t *testing.T) {
	rawOutput := "\r⠋ Fetching available models...\rFetching available models...\n" +
		"gemini-4-argon-medium\tGemini 4 Argon (Medium)\n" +
		"gemini-3.8-flash-medium\tGemini 3.8 Flash (Medium)\n" +
		"claude-sonnet-5-5-medium   Claude Sonnet 5.5 (Medium)\n"

	models := parseModelsOutput(rawOutput)
	require.Equal(t, []discoveredModel{
		{Value: "gemini-4-argon-medium", Name: "Gemini 4 Argon (Medium)"},
		{Value: "gemini-3.8-flash-medium", Name: "Gemini 3.8 Flash (Medium)"},
		{Value: "claude-sonnet-5-5-medium", Name: "Claude Sonnet 5.5 (Medium)"},
	}, models)
}

func TestPrepareMCPDirectoryWritesIsolatedConfigAndCleansUp(t *testing.T) {
	prepared, err := PrepareMCPDirectory([]MCPHTTPServer{
		{
			Name:    "orchestra_hub",
			URL:     "http://127.0.0.1:8787/mcp",
			Headers: map[string]string{"Authorization": "Bearer secret-token-123"},
		},
	}, map[string][]string{
		"orchestra_hub": {"orchestra_get_thread", "orchestra_publish_message"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, prepared.Directory)

	configPath := filepath.Join(prepared.Directory, ".agents", "mcp_config.json")
	rawBytes, err := os.ReadFile(configPath)
	require.NoError(t, err)
	require.Contains(t, string(rawBytes), `"serverUrl":"http://127.0.0.1:8787/mcp"`)
	require.Contains(t, prepared.SettingsJSON, `"mcp(orchestra_hub/orchestra_get_thread)"`)
	require.Contains(t, prepared.SettingsJSON, `"mcp(orchestra_hub/orchestra_publish_message)"`)

	prepared.Cleanup()
	_, statErr := os.Stat(prepared.Directory)
	require.ErrorIs(t, statErr, os.ErrNotExist)
}

func TestBridgeSessionLifecycleResumeStreamingAndCancellation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stub uses POSIX sh")
	}

	stubDirectory := t.TempDir()
	logPath := filepath.Join(stubDirectory, "invocations.log")
	agyStubPath := filepath.Join(stubDirectory, "agy")
	stubScript := `#!/bin/sh
set -eu
if [ "${1:-}" = "--version" ]; then
  echo "1.4.11"
  exit 0
fi
if [ "${1:-}" = "models" ]; then
  printf "Fetching available models...\ngemini-4-argon-medium\tGemini 4 Argon (Medium)\ngemini-3.8-flash-medium\tGemini 3.8 Flash (Medium)\n"
  exit 0
fi

conversation_id=""
model_id=""
mode_id=""
sandbox_flag="false"
prompt_value=""
mcp_seen="false"

while [ "$#" -gt 0 ]; do
  case "$1" in
    --conversation)
      conversation_id="$2"
      shift 2
      ;;
    --model)
      model_id="$2"
      shift 2
      ;;
    --mode)
      mode_id="$2"
      shift 2
      ;;
    --sandbox)
      sandbox_flag="true"
      shift
      ;;
    --add-dir)
      if [ -f "$2/.agents/mcp_config.json" ]; then
        mcp_seen="true"
      fi
      shift 2
      ;;
    -p=*)
      prompt_value="${1#-p=}"
      shift
      ;;
    *)
      shift
      ;;
  esac
done

printf '%s\n' "cid=${conversation_id}|model=${model_id}|mode=${mode_id}|sandbox=${sandbox_flag}|mcp=${mcp_seen}|prompt=${prompt_value}" >> "` + logPath + `"

case "$prompt_value" in
  *BLOCK_UNTIL_CANCELLED*)
    echo '{"event":"init","conversation_id":"conv-cancel-1"}'
    trap 'exit 130' INT TERM
    sleep 30
    exit 0
    ;;
  *TRIGGER_FAIL_NO_RESULT*)
    echo "agy internal engine crashed unexpectedly" >&2
    exit 1
    ;;
esac

if [ -z "$conversation_id" ]; then
  conversation_id="conv-first-123"
fi

printf '%s\n' "{\"event\":\"init\",\"conversation_id\":\"${conversation_id}\"}"
printf '%s\n' "{\"event\":\"step_update\",\"step_update\":{\"conversation_id\":\"${conversation_id}\",\"step_index\":1,\"state\":\"ACTIVE\",\"step_type\":\"thought\",\"text_delta\":\"Planning change\"}}"
printf '%s\n' "{\"event\":\"step_update\",\"step_update\":{\"conversation_id\":\"${conversation_id}\",\"step_index\":2,\"state\":\"ACTIVE\",\"step_type\":\"tool\",\"tool_name\":\"edit_file\",\"tool_info\":{\"name\":\"edit_file\",\"parameters\":{\"Path\":\"` + filepath.ToSlash(filepath.Join(stubDirectory, "note.txt")) + `\",\"Target\":\"old\",\"Replacement\":\"new\",\"toolSummary\":\"Update note.txt\"}}}}"
printf '%s\n' "{\"event\":\"step_update\",\"step_update\":{\"conversation_id\":\"${conversation_id}\",\"step_index\":2,\"state\":\"DONE\",\"step_type\":\"tool\",\"tool_name\":\"edit_file\"}}"
printf '%s\n' "{\"event\":\"step_update\",\"step_update\":{\"conversation_id\":\"${conversation_id}\",\"step_index\":3,\"state\":\"ACTIVE\",\"step_type\":\"agent_response\",\"text_delta\":\"Hello \"}}"
printf '%s\n' "{\"event\":\"step_update\",\"step_update\":{\"conversation_id\":\"${conversation_id}\",\"step_index\":3,\"state\":\"DONE\",\"step_type\":\"agent_response\",\"text_delta\":\"from ${conversation_id}\\n\"}}"
printf '%s\n' "{\"event\":\"result\",\"result\":{\"conversation_id\":\"${conversation_id}\",\"status\":\"SUCCESS\",\"response\":\"Hello from ${conversation_id}\\n\",\"usage\":{\"input_tokens\":120,\"output_tokens\":15,\"cache_read_tokens\":40,\"total_tokens\":135}}}"
`
	require.NoError(t, os.WriteFile(agyStubPath, []byte(stubScript), 0o755))

	var mcpMutex sync.Mutex
	var mcpMethods []string
	mcpServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, "Bearer run-token-xyz", request.Header.Get("Authorization"))
		var envelope struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
		}
		assert.NoError(t, json.NewDecoder(request.Body).Decode(&envelope))
		mcpMutex.Lock()
		mcpMethods = append(mcpMethods, envelope.Method)
		mcpMutex.Unlock()

		if envelope.ID == nil {
			writer.WriteHeader(http.StatusAccepted)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		switch envelope.Method {
		case "initialize":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      envelope.ID,
				"result": map[string]any{
					"protocolVersion": "2025-03-26",
					"capabilities":    map[string]any{"tools": map[string]any{}},
					"serverInfo":      map[string]any{"name": "orchestra", "version": "0.1.0"},
				},
			})
		case "tools/list":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      envelope.ID,
				"result":  map[string]any{"tools": []any{}},
			})
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer mcpServer.Close()

	stateDirectory := t.TempDir()
	workspaceDirectory := t.TempDir()

	startBridgeClient := func(eventSink func(protocol.HarnessEvent)) (*acp.Client, func()) {
		clientToServerReader, clientToServerWriter := io.Pipe()
		serverToClientReader, serverToClientWriter := io.Pipe()

		bridgeContext, cancelBridge := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			defer serverToClientWriter.Close()
			done <- Run(bridgeContext, clientToServerReader, serverToClientWriter, io.Discard, Options{
				Binary:           agyStubPath,
				StateDirectory:   stateDirectory,
				Version:          "0.1.0",
				WorkingDirectory: workspaceDirectory,
			})
		}()

		client := acp.NewClient(serverToClientReader, clientToServerWriter, acp.Options{
			RunID:                "run-test-1",
			ClientVersion:        "0.1.0",
			ExpectedAgentVersion: "0.1.0",
			Events:               eventSink,
		})
		cleanup := func() {
			t.Helper()
			client.Close()
			_ = clientToServerWriter.Close()
			cancelBridge()
			<-done
		}
		return client, cleanup
	}

	probeClient, closeProbe := startBridgeClient(nil)
	capabilities, err := probeClient.Probe(context.Background())
	require.NoError(t, err)
	require.True(t, capabilities.ResumeSession)
	require.True(t, capabilities.Mcp.HTTP)
	offeredModels := probeClient.ProbeOption(context.Background(), workspaceDirectory, ModelOption)
	require.Equal(t, []string{"gemini-4-argon-medium", "gemini-3.8-flash-medium"}, offeredModels)
	closeProbe()

	var recordedEvents []protocol.HarnessEvent
	firstClient, closeFirst := startBridgeClient(func(event protocol.HarnessEvent) {
		recordedEvents = append(recordedEvents, event)
	})
	firstResult, err := firstClient.Run(context.Background(), acp.SessionRequest{
		WorkingDirectory: workspaceDirectory,
		Prompt:           "First turn prompt",
		MCPServer: &acp.MCPServer{
			Name:        "orchestra_hub",
			URL:         mcpServer.URL,
			BearerToken: "run-token-xyz",
		},
		Configuration: []acp.ConfigurationSelection{
			{ID: ModeOption, Value: ModeSandbox, Requirement: acp.ConfigurationPolicy},
			{ID: ModelOption, Value: "gemini-3.8-flash-medium", Requirement: acp.ConfigurationRequested},
		},
	})
	closeFirst()
	require.NoError(t, err)
	require.NotEmpty(t, firstResult.SessionID)
	require.Equal(t, "Hello from conv-first-123\n", firstResult.Text)

	mcpMutex.Lock()
	require.Contains(t, mcpMethods, "initialize")
	require.Contains(t, mcpMethods, "tools/list")
	mcpMutex.Unlock()

	assert.True(t, slices.ContainsFunc(recordedEvents, func(event protocol.HarnessEvent) bool {
		return event.Type == "thought.delta" && event.Text == "Planning change"
	}))
	assert.True(t, slices.ContainsFunc(recordedEvents, func(event protocol.HarnessEvent) bool {
		return event.Type == "tool.call" && event.Status == "completed" && event.Kind == "edit"
	}))
	assert.True(t, slices.ContainsFunc(recordedEvents, func(event protocol.HarnessEvent) bool {
		return event.Type == "diff" && event.NewText == "new"
	}))
	assert.True(t, slices.ContainsFunc(recordedEvents, func(event protocol.HarnessEvent) bool {
		return event.Type == "usage" && event.InputTokens != nil && *event.InputTokens == 120
	}))

	secondClient, closeSecond := startBridgeClient(nil)
	var resumedFlag bool
	secondResult, err := secondClient.Run(context.Background(), acp.SessionRequest{
		WorkingDirectory: workspaceDirectory,
		Prompt:           "Fallback prompt",
		Resume: &acp.ResumeRequest{
			SessionID: firstResult.SessionID,
			Prompt:    "Second turn resumed prompt",
		},
		MCPServer: &acp.MCPServer{
			Name:        "orchestra_hub",
			URL:         mcpServer.URL,
			BearerToken: "run-token-xyz",
		},
		Configuration: []acp.ConfigurationSelection{
			{ID: ModeOption, Value: ModeBypass, Requirement: acp.ConfigurationPolicy},
		},
		BeforePrompt: func(context.Context) error {
			_, resumedFlag = secondClient.Session()
			return nil
		},
	})
	closeSecond()
	require.NoError(t, err)
	require.True(t, resumedFlag)
	require.Equal(t, firstResult.SessionID, secondResult.SessionID)
	require.Equal(t, "Hello from conv-first-123\n", secondResult.Text)

	invocationsRaw, err := os.ReadFile(logPath)
	require.NoError(t, err)
	invocations := string(invocationsRaw)
	require.Contains(t, invocations, "cid=|model=gemini-3.8-flash-medium|mode=accept-edits|sandbox=true|mcp=true|prompt=First turn prompt")
	require.Contains(t, invocations, "cid=conv-first-123|model=gemini-3.8-flash-medium|mode=accept-edits|sandbox=false|mcp=true|prompt=Second turn resumed prompt")

	cancelContext, triggerCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer triggerCancel()
	thirdClient, closeThird := startBridgeClient(nil)
	_, cancelErr := thirdClient.Run(cancelContext, acp.SessionRequest{
		WorkingDirectory: workspaceDirectory,
		Prompt:           "BLOCK_UNTIL_CANCELLED",
		Configuration: []acp.ConfigurationSelection{
			{ID: ModeOption, Value: ModeSandbox, Requirement: acp.ConfigurationPolicy},
		},
	})
	closeThird()
	require.ErrorIs(t, cancelErr, acp.ErrCancelled)
}

func TestBridgeMissingSessionRecoveryAndErrorRedaction(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stub uses POSIX sh")
	}

	stubDirectory := t.TempDir()
	agyStubPath := filepath.Join(stubDirectory, "agy")
	stubScript := `#!/bin/sh
set -eu
prompt_value=""
for arg in "$@"; do
  case "$arg" in
    -p=*) prompt_value="${arg#-p=}" ;;
  esac
done
case "$prompt_value" in
  *TRIGGER_DENIED*)
    printf '%s\n' '{"event":"init","conversation_id":"conv-err-1"}'
    printf '%s\n' '{"event":"result","result":{"conversation_id":"conv-err-1","status":"ERROR","response":"","error":"failed with run-secret-token-999","denied_actions":[{"action":"command","display_name":"Run shell"}]}}'
    exit 0
    ;;
  *TRIGGER_FAIL_NO_RESULT*)
    echo "agy internal engine crashed unexpectedly" >&2
    exit 1
    ;;
esac
printf '%s\n' '{"event":"init","conversation_id":"conv-recovered-1"}'
printf '%s\n' '{"event":"result","result":{"conversation_id":"conv-recovered-1","status":"SUCCESS","response":"Recovered OK"}}'
`
	require.NoError(t, os.WriteFile(agyStubPath, []byte(stubScript), 0o755))

	mcpServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var envelope struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
		}
		_ = json.NewDecoder(request.Body).Decode(&envelope)
		if envelope.ID == nil {
			writer.WriteHeader(http.StatusAccepted)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      envelope.ID,
			"result": map[string]any{
				"protocolVersion": "2025-03-26",
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": "orchestra", "version": "0.1.0"},
				"tools":           []any{},
			},
		})
	}))
	defer mcpServer.Close()

	stateDirectory := t.TempDir()
	workspaceDirectory := t.TempDir()

	runWithRequest := func(request acp.SessionRequest) (acp.Result, []protocol.HarnessEvent, error) {
		clientToServerReader, clientToServerWriter := io.Pipe()
		serverToClientReader, serverToClientWriter := io.Pipe()
		bridgeContext, cancelBridge := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			defer serverToClientWriter.Close()
			done <- Run(bridgeContext, clientToServerReader, serverToClientWriter, io.Discard, Options{
				Binary:           agyStubPath,
				StateDirectory:   stateDirectory,
				Version:          "0.1.0",
				WorkingDirectory: workspaceDirectory,
			})
		}()
		var events []protocol.HarnessEvent
		client := acp.NewClient(serverToClientReader, clientToServerWriter, acp.Options{
			RunID:                "run-edge-1",
			ClientVersion:        "0.1.0",
			ExpectedAgentVersion: "0.1.0",
			Secrets:              []string{"run-secret-token-999"},
			Events: func(event protocol.HarnessEvent) {
				events = append(events, event)
			},
		})
		result, err := client.Run(context.Background(), request)
		client.Close()
		_ = clientToServerWriter.Close()
		cancelBridge()
		<-done
		return result, events, err
	}

	recovered, events, err := runWithRequest(acp.SessionRequest{
		WorkingDirectory: workspaceDirectory,
		Prompt:           "Fallback fresh prompt",
		Resume: &acp.ResumeRequest{
			SessionID: "agysess-nonexistent",
			Prompt:    "Resumed prompt",
		},
		Configuration: []acp.ConfigurationSelection{
			{ID: ModeOption, Value: ModeSandbox, Requirement: acp.ConfigurationPolicy},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "Recovered OK", recovered.Text)
	assert.True(t, slices.ContainsFunc(events, func(event protocol.HarnessEvent) bool {
		return event.Type == "warning" && event.Code == acp.WarningSessionNotResumed
	}))

	_, _, turnErr := runWithRequest(acp.SessionRequest{
		WorkingDirectory: workspaceDirectory,
		Prompt:           "TRIGGER_DENIED",
		MCPServer: &acp.MCPServer{
			Name:        "orchestra_hub",
			URL:         mcpServer.URL,
			BearerToken: "run-secret-token-999",
		},
		Configuration: []acp.ConfigurationSelection{
			{ID: ModeOption, Value: ModeSandbox, Requirement: acp.ConfigurationPolicy},
		},
	})
	require.Error(t, turnErr)
	require.NotContains(t, turnErr.Error(), "run-secret-token-999")

	_, _, failErr := runWithRequest(acp.SessionRequest{
		WorkingDirectory: workspaceDirectory,
		Prompt:           "TRIGGER_FAIL_NO_RESULT",
		Configuration: []acp.ConfigurationSelection{
			{ID: ModeOption, Value: ModeSandbox, Requirement: acp.ConfigurationPolicy},
		},
	})
	require.Error(t, failErr)
	assert.Contains(t, failErr.Error(), "agy internal engine crashed unexpectedly")
}
