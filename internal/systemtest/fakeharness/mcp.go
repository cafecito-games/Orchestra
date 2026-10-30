package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sync/atomic"
	"time"
)

type hubMCPClient struct {
	endpoint   string
	token      string
	httpClient *http.Client
	sequence   atomic.Int64
	tools      []string
}

var errNoMCPServer = errors.New("no Orchestra MCP server was offered to this run")

func newHubMCPClient(endpoint, token string) (*hubMCPClient, error) {
	if endpoint == "" {
		return nil, errNoMCPServer
	}
	parsedURL, err := url.Parse(endpoint)
	if err != nil || (parsedURL.Hostname() != "127.0.0.1" && parsedURL.Hostname() != "localhost" && parsedURL.Hostname() != "::1") {
		return nil, errors.New("the offered MCP endpoint is not a loopback URL")
	}
	return &hubMCPClient{
		endpoint:   endpoint,
		token:      token,
		httpClient: &http.Client{Timeout: 45 * time.Second},
	}, nil
}

type jsonRPCResponseEnvelope struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (client *hubMCPClient) invokeRPC(operationContext context.Context, method string, parameters any) (json.RawMessage, error) {
	payloadBytes, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      client.sequence.Add(1),
		"method":  method,
		"params":  parameters,
	})
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(operationContext, http.MethodPost, client.endpoint, bytes.NewReader(payloadBytes))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+client.token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	request.Header.Set("MCP-Protocol-Version", "2025-06-18")

	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("MCP %s: %w", method, err)
	}
	defer response.Body.Close()

	bodyBytes, err := io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("MCP %s returned HTTP %d", method, response.StatusCode)
	}
	var envelope jsonRPCResponseEnvelope
	if err := json.Unmarshal(bodyBytes, &envelope); err != nil {
		return nil, fmt.Errorf("MCP %s returned undecodable JSON", method)
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("MCP %s failed with code %d: %s", method, envelope.Error.Code, envelope.Error.Message)
	}
	return envelope.Result, nil
}

func (client *hubMCPClient) connect(operationContext context.Context) error {
	_, err := client.invokeRPC(operationContext, "initialize", map[string]any{
		"protocolVersion": "2025-06-18",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]string{"name": "fakeharness", "version": "1.0.0"},
	})
	if err != nil {
		return err
	}
	rawTools, err := client.invokeRPC(operationContext, "tools/list", map[string]any{})
	if err != nil {
		return err
	}
	names, err := decodeToolNames(rawTools)
	if err != nil {
		return err
	}
	client.tools = names
	if activeRecorder != nil {
		activeRecorder.write(map[string]any{"event": "mcp-tools-list", "tools": names})
	}
	return nil
}

func decodeToolNames(raw json.RawMessage) ([]string, error) {
	var decoded struct {
		Tools []struct {
			Name string `json:"name"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("decode tools/list: %w", err)
	}
	names := make([]string, 0, len(decoded.Tools))
	seen := map[string]bool{}
	for _, tool := range decoded.Tools {
		if tool.Name == "" || seen[tool.Name] {
			return nil, errors.New("tools/list contains empty or duplicate tool name")
		}
		seen[tool.Name] = true
		names = append(names, tool.Name)
	}
	slices.Sort(names)
	return names, nil
}

type mcpToolOutcome struct {
	Value   any
	IsError bool
}

func (client *hubMCPClient) callTool(operationContext context.Context, name string, arguments any) (mcpToolOutcome, error) {
	if activeRecorder != nil {
		activeRecorder.write(map[string]any{"event": "mcp-tool-call", "tool": name})
	}
	rawResult, err := client.invokeRPC(operationContext, "tools/call", map[string]any{
		"name":      name,
		"arguments": arguments,
	})
	if err != nil {
		return mcpToolOutcome{}, err
	}
	var result struct {
		Content []struct {
			Text string `json:"text"`
		} `json:"content"`
		StructuredContent json.RawMessage `json:"structuredContent"`
		IsError           bool            `json:"isError"`
	}
	if err := json.Unmarshal(rawResult, &result); err != nil {
		return mcpToolOutcome{}, fmt.Errorf("tool %s returned undecodable result", name)
	}
	rawContent := result.StructuredContent
	if len(rawContent) == 0 && len(result.Content) > 0 {
		rawContent = json.RawMessage(result.Content[0].Text)
	}
	var parsedValue any
	if len(rawContent) > 0 {
		if err := json.Unmarshal(rawContent, &parsedValue); err != nil {
			return mcpToolOutcome{}, fmt.Errorf("tool %s returned undecodable content", name)
		}
	}
	outcome := mcpToolOutcome{Value: parsedValue, IsError: result.IsError}
	if activeRecorder != nil {
		activeRecorder.write(map[string]any{
			"event":     "mcp-tool-result",
			"tool":      name,
			"isError":   outcome.IsError,
			"errorCode": extractToolErrorCode(outcome.Value),
		})
	}
	return outcome, nil
}
