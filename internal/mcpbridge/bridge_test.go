package mcpbridge

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

func TestStdioBridgeInitializeToolsCallArtifactUploadAndDoorbell(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := orchestrator.NewService(orchestrator.ServiceOptions{
		Store:            openedStore,
		Token:            "bridge-token",
		DoorbellDebounce: 25 * time.Millisecond,
	})
	require.NoError(t, err)
	defer service.Close()

	server := httptest.NewServer(service.Handler())
	defer server.Close()

	bridge := NewBridge(Configuration{
		Endpoint: server.URL,
		Token:    "bridge-token",
		ClientID: "claude-desktop",
	})

	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	bridgeContext, cancelBridge := context.WithCancel(context.Background())
	defer cancelBridge()

	bridgeDone := make(chan error, 1)
	go func() {
		defer stdoutWriter.Close()
		bridgeDone <- bridge.Run(bridgeContext, stdinReader, stdoutWriter)
	}()

	lines := make(chan map[string]any, 32)
	go func() {
		scanner := bufio.NewScanner(stdoutReader)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			var parsed map[string]any
			if json.Unmarshal(scanner.Bytes(), &parsed) == nil {
				lines <- parsed
			}
		}
	}()

	sendLine := func(payload string) {
		t.Helper()
		_, writeErr := stdinWriter.Write([]byte(payload + "\n"))
		require.NoError(t, writeErr)
	}

	readResponseByID := func(expectedID float64) map[string]any {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case message := <-lines:
				if idValue, ok := message["id"].(float64); ok && idValue == expectedID {
					return message
				}
			case <-deadline:
				t.Fatalf("timed out waiting for response id %v", expectedID)
			}
		}
	}

	sendLine(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`)
	initializeResponse := readResponseByID(1)
	initializeJSON, _ := json.Marshal(initializeResponse)
	require.Contains(t, string(initializeJSON), "claude/channel")

	sendLine(`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
	listResponse := readResponseByID(2)
	listResult := listResponse["result"].(map[string]any)
	toolsSlice := listResult["tools"].([]any)
	require.Len(t, toolsSlice, len(protocol.OrchestratorToolNames))

	sendLine(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"create_thread","arguments":{"title":"Bridge Test Thread","attach":true}}}`)
	createResponse := readResponseByID(3)
	structured := createResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	threadObject := structured["thread"].(map[string]any)
	threadID := threadObject["id"].(string)
	require.True(t, strings.HasPrefix(threadID, "thread-"))

	localArtifactPath := filepath.Join(t.TempDir(), "notes.md")
	require.NoError(t, os.WriteFile(localArtifactPath, []byte("# Design Notes\nVerified via MCP bridge.\n"), 0o600))

	postCall, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      4,
		"method":  "tools/call",
		"params": map[string]any{
			"name": "post_artifact",
			"arguments": map[string]any{
				"threadId": threadID,
				"filePath": localArtifactPath,
				"summary":  "Design notes markdown",
			},
		},
	})
	sendLine(string(postCall))
	artifactResponse := readResponseByID(4)
	artifactText, _ := json.Marshal(artifactResponse)
	require.Contains(t, string(artifactText), `"uploaded":true`)

	newSummary := "Trigger doorbell from operator"
	_, err = service.UpdateThread(orchestrator.CallerIdentity{Kind: "operator", OperatorID: "cli"}, orchestrator.UpdateThreadInput{
		ThreadID: threadID,
		Summary:  &newSummary,
	})
	require.NoError(t, err)

	deadline := time.After(3 * time.Second)
	foundDoorbell := false
	for !foundDoorbell {
		select {
		case message := <-lines:
			if method, _ := message["method"].(string); method == "notifications/claude/channel" {
				foundDoorbell = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for notifications/claude/channel doorbell")
		}
	}

	_ = stdinWriter.Close()
	require.NoError(t, <-bridgeDone)
}
