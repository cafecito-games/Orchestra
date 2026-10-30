package acp_test

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestACPClientPromptPermissionsAndResume(t *testing.T) {
	clientReader, adapterWriter := io.Pipe()
	adapterReader, clientWriter := io.Pipe()

	var eventsMutex sync.Mutex
	var events []protocol.HarnessEvent

	secretValue := "super-secret-mcp-token-xyz"
	client := acp.NewClient(clientReader, clientWriter, acp.Options{
		RunID:   "run-1",
		Secrets: []string{secretValue},
		Events: func(event protocol.HarnessEvent) {
			eventsMutex.Lock()
			events = append(events, event)
			eventsMutex.Unlock()
		},
		Permission: func(requestContext context.Context, request acp.PermissionRequest) (acp.PermissionDecision, error) {
			require.Equal(t, "Execute tool", request.Title)
			return acp.PermissionDecision{OptionID: "allow-1"}, nil
		},
	})

	go func() {
		defer adapterWriter.Close()
		scanner := bufio.NewScanner(adapterReader)
		writeJSON := func(payload any) {
			encoded, err := json.Marshal(payload)
			if err != nil {
				return
			}
			_, _ = adapterWriter.Write(append(encoded, '\n'))
		}
		for scanner.Scan() {
			var frame struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
				Params json.RawMessage `json:"params"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
				return
			}
			switch frame.Method {
			case "initialize":
				writeJSON(map[string]any{
					"jsonrpc": "2.0",
					"id":      frame.ID,
					"result": map[string]any{
						"protocolVersion": 1,
						"agentCapabilities": map[string]any{
							"loadSession": true,
							"mcpCapabilities": map[string]any{
								"http": true,
							},
							"sessionCapabilities": map[string]any{
								"resume": map[string]any{},
								"close":  map[string]any{},
							},
						},
						"agentInfo": map[string]any{
							"name":    "claude-acp",
							"version": "1.2.3",
						},
					},
				})
			case "session/new":
				writeJSON(map[string]any{
					"jsonrpc": "2.0",
					"id":      frame.ID,
					"result": map[string]any{
						"sessionId": "session-abc",
						"configOptions": []any{
							map[string]any{
								"id":           "model",
								"type":         "select",
								"currentValue": "claude-sonnet-4-6",
								"options": []any{
									map[string]any{"value": "claude-sonnet-4-6"},
									map[string]any{"value": "claude-opus-4-6"},
								},
							},
						},
					},
				})
			case "session/set_config_option":
				writeJSON(map[string]any{
					"jsonrpc": "2.0",
					"id":      frame.ID,
					"result": map[string]any{
						"configOptions": []any{
							map[string]any{
								"id":           "model",
								"type":         "select",
								"currentValue": "claude-opus-4-6",
								"options": []any{
									map[string]any{"value": "claude-sonnet-4-6"},
									map[string]any{"value": "claude-opus-4-6"},
								},
							},
						},
					},
				})
			case "session/prompt":
				writeJSON(map[string]any{
					"jsonrpc": "2.0",
					"method":  "session/update",
					"params": map[string]any{
						"sessionId": "session-abc",
						"update": map[string]any{
							"sessionUpdate": "tool_call",
							"toolCallId":    "tc-1",
							"title":         "Execute tool",
							"kind":          "execute",
							"status":        "in_progress",
						},
					},
				})
				writeJSON(map[string]any{
					"jsonrpc": "2.0",
					"id":      "perm-req-1",
					"method":  "session/request_permission",
					"params": map[string]any{
						"sessionId": "session-abc",
						"toolCall": map[string]any{
							"toolCallId": "tc-1",
							"title":      "Execute tool",
							"kind":       "execute",
						},
						"options": []any{
							map[string]any{"optionId": "allow-1", "name": "Allow once", "kind": "allow_once"},
							map[string]any{"optionId": "reject-1", "name": "Reject", "kind": "reject_once"},
						},
					},
				})
				if !scanner.Scan() {
					return
				}
				writeJSON(map[string]any{
					"jsonrpc": "2.0",
					"method":  "session/update",
					"params": map[string]any{
						"sessionId": "session-abc",
						"update": map[string]any{
							"sessionUpdate": "agent_message_chunk",
							"content": map[string]any{
								"type": "text",
								"text": "Completed with token " + secretValue,
							},
						},
					},
				})
				writeJSON(map[string]any{
					"jsonrpc": "2.0",
					"id":      frame.ID,
					"result": map[string]any{
						"stopReason": "end_turn",
						"usage": map[string]any{
							"inputTokens":  120,
							"outputTokens": 45,
						},
					},
				})
			case "session/close":
				writeJSON(map[string]any{
					"jsonrpc": "2.0",
					"id":      frame.ID,
					"result":  map[string]any{},
				})
				return
			}
		}
	}()

	workingDirectory := t.TempDir()
	result, err := client.Run(context.Background(), acp.SessionRequest{
		WorkingDirectory: workingDirectory,
		Prompt:           "Run tests",
		MCPServer: &acp.MCPServer{
			Name:        "orchestra_hub",
			URL:         "http://127.0.0.1:9999/mcp",
			BearerToken: secretValue,
		},
		Configuration: []acp.ConfigurationSelection{
			{ID: "model", Value: "claude-opus-4-6", Requirement: acp.ConfigurationRequested},
		},
	})
	require.NoError(t, err)
	client.Close()
	client.Wait()

	require.Equal(t, "session-abc", result.SessionID)
	require.Equal(t, "end_turn", result.StopReason)
	require.NotContains(t, result.Text, secretValue)
	require.Contains(t, result.Text, "[redacted]")

	eventsMutex.Lock()
	defer eventsMutex.Unlock()
	require.NotEmpty(t, events)
	for index, event := range events {
		require.Equal(t, int64(index+1), event.Sequence)
		require.NoError(t, event.Validate())
	}
}

func TestStderrTailRedaction(t *testing.T) {
	secret := "orchestra-secret-123456"
	tail := acp.NewStderrTail(64, []string{secret})
	_, err := tail.Write([]byte("error connecting with " + secret + " on port 8787\n"))
	require.NoError(t, err)
	output := tail.String()
	require.NotContains(t, output, secret)
	require.Contains(t, output, "[redacted]")
}
