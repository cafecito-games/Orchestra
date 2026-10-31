package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMCPServerGrantDelegationAndPostArtifact(t *testing.T) {
	operationContext := t.Context()

	workspaceDirectory := t.TempDir()
	reportPath := filepath.Join(workspaceDirectory, "report.txt")
	require.NoError(t, os.WriteFile(reportPath, []byte("hello artifact"), 0o644))

	outsideFile := filepath.Join(t.TempDir(), "secret.txt")
	require.NoError(t, os.WriteFile(outsideFile, []byte("secret"), 0o644))
	_ = os.Symlink(outsideFile, filepath.Join(workspaceDirectory, "escape.txt"))

	var uploadedBytes []byte
	server := New(
		func(_ context.Context, runID string, method string, arguments json.RawMessage) (json.RawMessage, error) {
			if method == "post_artifact" {
				var parsed map[string]any
				require.NoError(t, json.Unmarshal(arguments, &parsed))
				return json.Marshal(map[string]any{
					"artifact": map[string]any{
						"id":           "art-1",
						"runId":        runID,
						"title":        parsed["title"],
						"kind":         parsed["kind"],
						"downloadPath": "/api/artifacts/art-1/content",
						"uploaded":     false,
					},
					"uploadPath": "/api/artifacts/art-1/content",
				})
			}
			return json.Marshal(map[string]any{"runId": runID, "method": method})
		},
		func(_ context.Context, uploadPath string, content io.Reader, _ int64) error {
			require.Equal(t, "/api/artifacts/art-1/content", uploadPath)
			data, err := io.ReadAll(content)
			if err != nil {
				return err
			}
			uploadedBytes = data
			return nil
		},
	)
	require.NoError(t, server.Start(operationContext))

	workerConfiguration, err := server.Grant("run-worker", workspaceDirectory, false)
	require.NoError(t, err)
	defer server.Revoke(workerConfiguration.Token)

	callMCP := func(configuration Configuration, method string, parameters any) (int, map[string]any) {
		t.Helper()
		body, marshalErr := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  method,
			"params":  parameters,
		})
		require.NoError(t, marshalErr)
		request, requestErr := http.NewRequestWithContext(t.Context(), http.MethodPost, configuration.URL, bytes.NewReader(body))
		require.NoError(t, requestErr)
		request.Header.Set("Authorization", "Bearer "+configuration.Token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		request.Header.Set("MCP-Protocol-Version", "2025-06-18")
		response, doErr := http.DefaultClient.Do(request)
		require.NoError(t, doErr)
		defer response.Body.Close()
		var decoded map[string]any
		if response.StatusCode == http.StatusOK {
			require.NoError(t, json.NewDecoder(response.Body).Decode(&decoded))
		}
		return response.StatusCode, decoded
	}

	statusCode, listResult := callMCP(workerConfiguration, "tools/list", map[string]any{})
	assert.Equal(t, http.StatusOK, statusCode)
	<-workerConfiguration.Connected
	listedTools := listResult["result"].(map[string]any)["tools"].([]any)
	assert.Len(t, listedTools, len(ToolNames(false)))
	var leafPostRoomSchema map[string]any
	for _, rawTool := range listedTools {
		toolMap := rawTool.(map[string]any)
		if toolMap["name"] == "post_room_message" {
			leafPostRoomSchema = toolMap["inputSchema"].(map[string]any)
		}
	}
	require.NotNil(t, leafPostRoomSchema)
	assert.NotContains(t, leafPostRoomSchema["properties"].(map[string]any), "nudgeTaskIds")

	_, leafNudgeDenied := callMCP(workerConfiguration, "tools/call", map[string]any{
		"name": "post_room_message",
		"arguments": map[string]any{
			"idempotencyKey": "k-nudge",
			"kind":           "guidance",
			"body":           "attempt nudge",
			"nudgeTaskIds":   []string{"task-1"},
		},
	})
	assert.Equal(t, true, leafNudgeDenied["result"].(map[string]any)["isError"])

	delegatingConfiguration, err := server.Grant("run-delegator", workspaceDirectory, true)
	require.NoError(t, err)
	defer server.Revoke(delegatingConfiguration.Token)
	_, delegatingList := callMCP(delegatingConfiguration, "tools/list", map[string]any{})
	var delegatingPostRoomSchema map[string]any
	for _, rawTool := range delegatingList["result"].(map[string]any)["tools"].([]any) {
		toolMap := rawTool.(map[string]any)
		if toolMap["name"] == "post_room_message" {
			delegatingPostRoomSchema = toolMap["inputSchema"].(map[string]any)
		}
	}
	require.NotNil(t, delegatingPostRoomSchema)
	assert.Contains(t, delegatingPostRoomSchema["properties"].(map[string]any), "nudgeTaskIds")

	_, delegateDenied := callMCP(workerConfiguration, "tools/call", map[string]any{
		"name":      "submit_tasks",
		"arguments": map[string]any{"idempotencyKey": "k1", "tasks": []any{}},
	})
	assert.Equal(t, true, delegateDenied["result"].(map[string]any)["isError"])

	_, symlinkRejected := callMCP(workerConfiguration, "tools/call", map[string]any{
		"name": "post_artifact",
		"arguments": map[string]any{
			"relativePath":   "escape.txt",
			"title":          "Escape",
			"kind":           "report",
			"mediaType":      "text/plain",
			"idempotencyKey": "art-escape",
		},
	})
	assert.Equal(t, true, symlinkRejected["result"].(map[string]any)["isError"])

	_, artifactUploaded := callMCP(workerConfiguration, "tools/call", map[string]any{
		"name": "post_artifact",
		"arguments": map[string]any{
			"relativePath":   "report.txt",
			"title":          "Report",
			"kind":           "report",
			"mediaType":      "text/plain",
			"idempotencyKey": "art-ok",
		},
	})
	assert.NotEqual(t, true, artifactUploaded["result"].(map[string]any)["isError"])
	assert.Equal(t, []byte("hello artifact"), uploadedBytes)

	server.Revoke(workerConfiguration.Token)
	revokedStatus, _ := callMCP(workerConfiguration, "tools/list", map[string]any{})
	assert.Equal(t, http.StatusUnauthorized, revokedStatus)
}

func TestMCPServerPathTraversalRevocationAndUnknownTools(t *testing.T) {
	operationContext := t.Context()
	workspaceDirectory := t.TempDir()

	server := New(
		func(_ context.Context, runID string, method string, arguments json.RawMessage) (json.RawMessage, error) {
			return json.Marshal(map[string]any{"runId": runID, "status": "ok"})
		},
		func(_ context.Context, _ string, _ io.Reader, _ int64) error {
			return nil
		},
	)
	require.NoError(t, server.Start(operationContext))

	configuration, err := server.Grant("run-security", workspaceDirectory, false)
	require.NoError(t, err)

	callMCP := func(token string, method string, parameters any) (int, map[string]any) {
		t.Helper()
		body, marshalErr := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  method,
			"params":  parameters,
		})
		require.NoError(t, marshalErr)
		request, requestErr := http.NewRequestWithContext(t.Context(), http.MethodPost, configuration.URL, bytes.NewReader(body))
		require.NoError(t, requestErr)
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		request.Header.Set("MCP-Protocol-Version", "2025-06-18")
		response, doErr := http.DefaultClient.Do(request)
		require.NoError(t, doErr)
		defer response.Body.Close()
		var decoded map[string]any
		if response.StatusCode == http.StatusOK {
			require.NoError(t, json.NewDecoder(response.Body).Decode(&decoded))
		}
		return response.StatusCode, decoded
	}

	statusCode, pathTraversalResponse := callMCP(configuration.Token, "tools/call", map[string]any{
		"name": "post_artifact",
		"arguments": map[string]any{
			"relativePath": "../../secret.txt",
			"title":        "Traverse",
			"kind":         "report",
		},
	})
	assert.Equal(t, http.StatusOK, statusCode)
	resultMap, _ := pathTraversalResponse["result"].(map[string]any)
	assert.Equal(t, true, resultMap["isError"])

	statusCode, unknownToolResponse := callMCP(configuration.Token, "tools/call", map[string]any{
		"name":      "non_existent_tool",
		"arguments": map[string]any{},
	})
	assert.Equal(t, http.StatusOK, statusCode)
	if errMap, hasError := unknownToolResponse["error"]; hasError {
		assert.NotNil(t, errMap)
	} else if toolResult, hasResult := unknownToolResponse["result"].(map[string]any); hasResult {
		assert.Equal(t, true, toolResult["isError"])
	}

	statusCode, invalidArgsResponse := callMCP(configuration.Token, "tools/call", map[string]any{
		"name": "post_artifact",
		"arguments": map[string]any{
			"relativePath": "",
			"title":        "Empty Path",
		},
	})
	assert.Equal(t, http.StatusOK, statusCode)
	invalidResult, _ := invalidArgsResponse["result"].(map[string]any)
	assert.Equal(t, true, invalidResult["isError"])

	server.Revoke(configuration.Token)
	revokedStatus, _ := callMCP(configuration.Token, "tools/list", map[string]any{})
	assert.Equal(t, http.StatusUnauthorized, revokedStatus)

	revokedToolStatus, _ := callMCP(configuration.Token, "tools/call", map[string]any{
		"name":      "get_thread",
		"arguments": map[string]any{"threadId": "thread-1"},
	})
	assert.Equal(t, http.StatusUnauthorized, revokedToolStatus)
}
