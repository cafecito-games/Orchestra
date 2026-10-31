package mcpbridge

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

	bridgeContext, cancelBridge := context.WithCancel(t.Context())
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
				require.Failf(t, "timeout", "timed out waiting for response id %v", expectedID)
			}
		}
	}

	sendLine(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"bridge-test","version":"1.0.0"}}}`)
	initializeResponse := readResponseByID(1)
	initializeJSON, _ := json.Marshal(initializeResponse)
	assert.Contains(t, string(initializeJSON), "claude/channel")

	sendLine(`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
	listResponse := readResponseByID(2)
	listResult := listResponse["result"].(map[string]any)
	toolsSlice := listResult["tools"].([]any)
	assert.Len(t, toolsSlice, len(protocol.OrchestratorToolNames))

	sendLine(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"create_thread","arguments":{"title":"Bridge Test Thread","attach":true}}}`)
	createResponse := readResponseByID(3)
	structured := createResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	threadObject := structured["thread"].(map[string]any)
	threadID := threadObject["id"].(string)
	assert.True(t, strings.HasPrefix(threadID, "thread-"))

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
	assert.Contains(t, string(artifactText), `"uploaded":true`)

	_, err = service.UpdateThread(orchestrator.CallerIdentity{Kind: "operator", OperatorID: "cli"}, orchestrator.UpdateThreadInput{
		ThreadID: threadID,
		Summary:  new("Trigger doorbell from operator"),
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
			require.Fail(t, "timed out waiting for notifications/claude/channel doorbell")
		}
	}

	_ = stdinWriter.Close()
	require.NoError(t, <-bridgeDone)
}

func TestStdioBridgeAgentMailIdentityRenameAndLiveChannelPush(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := orchestrator.NewService(orchestrator.ServiceOptions{
		Store: openedStore,
		Token: "mail-bridge-token",
	})
	require.NoError(t, err)
	defer service.Close()

	server := httptest.NewServer(service.Handler())
	defer server.Close()

	type liveBridgeClient struct {
		stdinWriter *io.PipeWriter
		lines       chan map[string]any
		done        chan error
	}

	startClient := func(configuration Configuration) *liveBridgeClient {
		bridge := NewBridge(configuration)
		stdinReader, stdinWriter := io.Pipe()
		stdoutReader, stdoutWriter := io.Pipe()

		bridgeContext, cancelBridge := context.WithCancel(context.Background())
		doneChannel := make(chan error, 1)
		go func() {
			defer stdoutWriter.Close()
			doneChannel <- bridge.Run(bridgeContext, stdinReader, stdoutWriter)
		}()

		linesChannel := make(chan map[string]any, 32)
		go func() {
			scanner := bufio.NewScanner(stdoutReader)
			scanner.Buffer(make([]byte, 64*1024), 1024*1024)
			for scanner.Scan() {
				var parsed map[string]any
				if json.Unmarshal(scanner.Bytes(), &parsed) == nil {
					linesChannel <- parsed
				}
			}
		}()

		t.Cleanup(func() {
			_ = stdinWriter.Close()
			cancelBridge()
			<-doneChannel
		})

		return &liveBridgeClient{
			stdinWriter: stdinWriter,
			lines:       linesChannel,
			done:        doneChannel,
		}
	}

	sendRPC := func(client *liveBridgeClient, payload any) {
		t.Helper()
		encoded, marshalErr := json.Marshal(payload)
		require.NoError(t, marshalErr)
		_, writeErr := client.stdinWriter.Write(append(encoded, '\n'))
		require.NoError(t, writeErr)
	}

	readByID := func(client *liveBridgeClient, expectedID float64, bufferedNotifications *[]map[string]any) map[string]any {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case message := <-client.lines:
				if idValue, ok := message["id"].(float64); ok && idValue == expectedID {
					return message
				}
				if bufferedNotifications != nil {
					*bufferedNotifications = append(*bufferedNotifications, message)
				}
			case <-deadline:
				require.Failf(t, "timeout", "timed out waiting for response id %v", expectedID)
				return nil
			}
		}
	}

	claudeClient := startClient(Configuration{
		Endpoint:  server.URL,
		Token:     "mail-bridge-token",
		ClientID:  "mcp-client-alpha",
		MailHost:  "fabrica",
		Workspace: "/workspaces/frontend",
		ProcessID: 1234,
	})
	codexClient := startClient(Configuration{
		Endpoint:  server.URL,
		Token:     "mail-bridge-token",
		ClientID:  "mcp-client-beta",
		MailHost:  "bunker",
		Workspace: "/workspaces/backend",
		ProcessID: 345,
	})

	var claudeNotifications []map[string]any
	sendRPC(claudeClient, map[string]any{
		"jsonrpc": "2.0",
		"id":      "discover-1",
		"method":  "server/discover",
		"params": map[string]any{
			"_meta": map[string]any{
				"io.modelcontextprotocol/protocolVersion": "2026-07-28",
				"io.modelcontextprotocol/clientInfo": map[string]string{
					"name":    "claude-code",
					"version": "2.1.314",
				},
			},
		},
	})
	sendRPC(claudeClient, map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2025-06-18",
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]string{"name": "", "version": "1.0.0"},
		},
	})
	_ = readByID(claudeClient, 1, &claudeNotifications)

	sendRPC(codexClient, map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2025-06-18",
			"capabilities":    map[string]any{},
			"clientInfo":      map[string]string{"name": "codex-mcp-client", "version": "1.0.0"},
		},
	})
	_ = readByID(codexClient, 1, nil)

	claudeRequestID := float64(10)
	require.Eventually(t, func() bool {
		claudeRequestID++
		sendRPC(claudeClient, map[string]any{
			"jsonrpc": "2.0",
			"id":      claudeRequestID,
			"method":  "tools/call",
			"params":  map[string]any{"name": "get_mail_identity", "arguments": map[string]any{}},
		})
		response := readByID(claudeClient, claudeRequestID, &claudeNotifications)
		resultMap, _ := response["result"].(map[string]any)
		structured, _ := resultMap["structuredContent"].(map[string]any)
		mailboxObject, _ := structured["mailbox"].(map[string]any)
		return mailboxObject["address"] == "claude1234@fabrica"
	}, 3*time.Second, 50*time.Millisecond)

	codexRequestID := float64(10)
	require.Eventually(t, func() bool {
		codexRequestID++
		sendRPC(codexClient, map[string]any{
			"jsonrpc": "2.0",
			"id":      codexRequestID,
			"method":  "tools/call",
			"params":  map[string]any{"name": "get_mail_identity", "arguments": map[string]any{}},
		})
		response := readByID(codexClient, codexRequestID, nil)
		resultMap, _ := response["result"].(map[string]any)
		structured, _ := resultMap["structuredContent"].(map[string]any)
		mailboxObject, _ := structured["mailbox"].(map[string]any)
		return mailboxObject["address"] == "codex345@bunker"
	}, 3*time.Second, 50*time.Millisecond)

	claudeRequestID++
	sendRPC(claudeClient, map[string]any{
		"jsonrpc": "2.0",
		"id":      claudeRequestID,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      "set_mail_identity",
			"arguments": map[string]any{"name": "ui-lead"},
		},
	})
	renameResponse := readByID(claudeClient, claudeRequestID, &claudeNotifications)
	renameStructured := renameResponse["result"].(map[string]any)["structuredContent"].(map[string]any)
	renamedMailbox := renameStructured["mailbox"].(map[string]any)
	assert.Equal(t, "ui-lead@fabrica", renamedMailbox["address"])

	codexRequestID++
	sendRPC(codexClient, map[string]any{
		"jsonrpc": "2.0",
		"id":      codexRequestID,
		"method":  "tools/call",
		"params": map[string]any{
			"name": "send_mail",
			"arguments": map[string]any{
				"idempotencyKey": "bridge-mail-1",
				"to":             "claude1234@fabrica",
				"subject":        "Schema updated",
				"body":           "Updated GraphQL schema on bunker; ready for UI codegen.",
			},
		},
	})
	sendResponse := readByID(codexClient, codexRequestID, nil)
	sendResult := sendResponse["result"].(map[string]any)
	assert.NotEqual(t, true, sendResult["isError"])

	checkMailNotification := func(candidate map[string]any) bool {
		t.Helper()
		if method, _ := candidate["method"].(string); method != "notifications/claude/channel" {
			return false
		}
		parameters, _ := candidate["params"].(map[string]any)
		metadata, _ := parameters["meta"].(map[string]any)
		if metadata["kind"] != "agent_mail" {
			return false
		}
		assert.Equal(t, "codex345@bunker", metadata["from"])
		assert.Equal(t, "ui-lead@fabrica", metadata["to"])
		contentText, _ := parameters["content"].(string)
		assert.Contains(t, contentText, "Updated GraphQL schema on bunker")
		return true
	}

	foundMailPush := slices.ContainsFunc(claudeNotifications, checkMailNotification)
	deadline := time.After(4 * time.Second)
	for !foundMailPush {
		select {
		case incoming := <-claudeClient.lines:
			if checkMailNotification(incoming) {
				foundMailPush = true
			}
		case <-deadline:
			require.Fail(t, "timed out waiting for notifications/claude/channel agent_mail push")
		}
	}
}

func TestStdioBridgeErrorHandlingAndResilience(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := orchestrator.NewService(orchestrator.ServiceOptions{
		Store: openedStore,
		Token: "test-bridge-token",
	})
	require.NoError(t, err)
	defer service.Close()

	server := httptest.NewServer(service.Handler())
	defer server.Close()

	bridge := NewBridge(Configuration{
		Endpoint: server.URL,
		Token:    "test-bridge-token",
		ClientID: "claude-desktop",
	})

	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	bridgeContext, cancelBridge := context.WithCancel(t.Context())
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
				require.Failf(t, "timeout", "timed out waiting for response id %v", expectedID)
				return nil
			}
		}
	}

	sendLine(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"bridge-test","version":"1.0.0"}}}`)
	initResp := readResponseByID(1)
	require.NotNil(t, initResp)

	// 1. Invalid JSON-RPC stdin frame recovery without crashing
	sendLine(`{"jsonrpc":"2.0","id":99,"method":"unknown/invalid_method","params":{}}`)
	invalidResponse := readResponseByID(99)
	assert.NotNil(t, invalidResponse)
	assert.NotNil(t, invalidResponse["error"])

	sendLine(`{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`)
	listResponse := readResponseByID(2)
	assert.NotNil(t, listResponse)
	assert.NotNil(t, listResponse["result"])

	// 2. post_artifact with missing local file
	sendLine(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"post_artifact","arguments":{"filePath":"/tmp/nonexistent-file-xyz-12345.md"}}}`)
	missingFileResponse := readResponseByID(3)
	assert.NotNil(t, missingFileResponse)
	missingResult, _ := missingFileResponse["result"].(map[string]any)
	assert.Equal(t, true, missingResult["isError"])
	contentList, _ := missingResult["content"].([]any)
	require.NotEmpty(t, contentList)
	firstContent, _ := contentList[0].(map[string]any)
	assert.Contains(t, firstContent["text"], "filePath must name an existing regular file")

	_ = stdinWriter.Close()
	require.NoError(t, <-bridgeDone)
}

type testCustomRoundTripper struct {
	base     http.RoundTripper
	override func(*http.Request) (*http.Response, error)
}

func (tripper testCustomRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if tripper.override != nil {
		if response, err := tripper.override(request); response != nil || err != nil {
			return response, err
		}
	}
	base := tripper.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(request)
}

func TestStdioBridgeMasterHTTPErrorPropagation(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := orchestrator.NewService(orchestrator.ServiceOptions{
		Store: openedStore,
		Token: "test-bridge-token",
	})
	require.NoError(t, err)
	defer service.Close()

	server := httptest.NewServer(service.Handler())
	defer server.Close()

	customHTTPClient := &http.Client{
		Transport: testCustomRoundTripper{
			override: func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodPut && strings.Contains(request.URL.Path, "/content") {
					return &http.Response{
						StatusCode: http.StatusInternalServerError,
						Body:       io.NopCloser(strings.NewReader("internal storage failure")),
						Header:     make(http.Header),
					}, nil
				}
				return nil, nil
			},
		},
	}

	bridge := NewBridge(Configuration{
		Endpoint:   server.URL,
		Token:      "test-bridge-token",
		ClientID:   "claude-desktop",
		HTTPClient: customHTTPClient,
	})

	stdinReader, stdinWriter := io.Pipe()
	stdoutReader, stdoutWriter := io.Pipe()

	bridgeContext, cancelBridge := context.WithCancel(t.Context())
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
				require.Failf(t, "timeout", "timed out waiting for response id %v", expectedID)
				return nil
			}
		}
	}

	sendLine(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"bridge-test","version":"1.0.0"}}}`)
	initResp := readResponseByID(1)
	require.NotNil(t, initResp)

	sendLine(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"create_thread","arguments":{"title":"HTTP Error Thread"}}}`)
	createResp := readResponseByID(2)
	structured := createResp["result"].(map[string]any)["structuredContent"].(map[string]any)
	threadObject := structured["thread"].(map[string]any)
	threadID := threadObject["id"].(string)

	localArtifactPath := filepath.Join(t.TempDir(), "upload_fail.txt")
	require.NoError(t, os.WriteFile(localArtifactPath, []byte("some content"), 0o600))

	postCall, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      3,
		"method":  "tools/call",
		"params": map[string]any{
			"name": "post_artifact",
			"arguments": map[string]any{
				"threadId": threadID,
				"filePath": localArtifactPath,
				"summary":  "test upload",
			},
		},
	})
	sendLine(string(postCall))
	uploadFailResponse := readResponseByID(3)
	require.NotNil(t, uploadFailResponse)
	uploadResult, _ := uploadFailResponse["result"].(map[string]any)
	assert.Equal(t, true, uploadResult["isError"])
	contentList, _ := uploadResult["content"].([]any)
	require.NotEmpty(t, contentList)
	firstContent, _ := contentList[0].(map[string]any)
	assert.Contains(t, firstContent["text"], "artifact upload returned HTTP 500")

	sendLine(`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"send_mail","arguments":{"to":""}}}`)
	rpcErrorResponse := readResponseByID(4)
	require.NotNil(t, rpcErrorResponse)
	rpcResult, _ := rpcErrorResponse["result"].(map[string]any)
	assert.Equal(t, true, rpcResult["isError"])

	_ = stdinWriter.Close()
	require.NoError(t, <-bridgeDone)
}
