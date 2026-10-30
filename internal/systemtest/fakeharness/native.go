package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

var (
	codexNativeVersion  = "codex-cli 0.46.0"
	claudeNativeVersion = "2.0.14 (Claude Code)"
	agyNativeVersion    = "1.4.11"
)

const (
	FakeAntigravityDefaultModel = "gemini-4-argon-medium"
	FakeAntigravityAltModel     = "gemini-3.8-flash-medium"
)

func nativeVersionForRole(role string) string {
	switch role {
	case "codex":
		return codexNativeVersion
	case "agy":
		return agyNativeVersion
	default:
		return claudeNativeVersion
	}
}

type nativeExecutionHost struct {
	role           string
	conversationID string
	stepIndex      int
	lastOutput     string
	cancelChannel  chan struct{}
}

type parsedNativeArguments struct {
	prompt         string
	mcpEndpoint    string
	mcpBearerToken string
	conversationID string
	modelID        string
	sandbox        bool
}

func runNative(recorder *eventRecorder, role string, arguments []string) int {
	if len(arguments) == 1 && arguments[0] == "--version" {
		version := nativeVersionForRole(role)
		recorder.write(map[string]any{"event": "native-version", "version": version})
		fmt.Println(version)
		return 0
	}
	if role == "agy" && len(arguments) == 1 && arguments[0] == "models" {
		fmt.Printf("Fetching available models...\n%s\tGemini 4 Argon (Medium)\n%s\tGemini 3.8 Flash (Medium)\n", FakeAntigravityDefaultModel, FakeAntigravityAltModel)
		return 0
	}

	parsed, err := parseNativeInvocation(role, arguments)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakeharness: %v\n", err)
		return 64
	}

	var mcpClient *hubMCPClient
	if parsed.mcpEndpoint != "" {
		bearerToken := parsed.mcpBearerToken
		if bearerToken == "" {
			bearerToken = os.Getenv("ORCHESTRA_MCP_TOKEN")
		}
		mcpClient, err = newHubMCPClient(parsed.mcpEndpoint, bearerToken)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fakeharness: %v\n", err)
			return 64
		}
		if connectErr := mcpClient.connect(context.Background()); connectErr != nil {
			fmt.Fprintf(os.Stderr, "fakeharness: connect MCP: %v\n", connectErr)
			return 64
		}
	}

	conversationID := parsed.conversationID
	resumedConversation := false
	if role == "agy" {
		sessionDirectory := os.Getenv(sessionDirectoryVariable)
		if conversationID != "" {
			if sessionDirectory != "" {
				if _, statErr := os.Stat(filepath.Join(sessionDirectory, "agy-"+conversationID+".json")); statErr == nil {
					resumedConversation = true
				}
			}
		} else {
			conversationID = "agy-conv-" + strconv.Itoa(os.Getpid())
			if sessionDirectory != "" {
				_ = os.WriteFile(filepath.Join(sessionDirectory, "agy-"+conversationID+".json"), []byte(`{"active":true}`), 0o600)
			}
		}
	}

	recorder.write(map[string]any{
		"event":          "native-run",
		"mcp":            mcpClient != nil,
		"version":        nativeVersionForRole(role),
		"conversationId": conversationID,
		"resumed":        resumedConversation,
		"model":          parsed.modelID,
		"sandbox":        parsed.sandbox,
	})

	script, err := extractScript(parsed.prompt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakeharness: %v\n", err)
		return 65
	}

	cancelChannel := make(chan struct{})
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChannel
		close(cancelChannel)
	}()

	workingDirectory, _ := os.Getwd()
	host := &nativeExecutionHost{
		role:           role,
		conversationID: conversationID,
		cancelChannel:  cancelChannel,
	}
	switch role {
	case "codex":
		host.emit(map[string]any{"type": "thread.started", "thread_id": "native-codex-" + strconv.Itoa(os.Getpid())})
	case "agy":
		host.emit(map[string]any{"event": "init", "conversation_id": conversationID})
	default:
		host.emit(map[string]any{"type": "system", "session_id": "native-claude-" + strconv.Itoa(os.Getpid())})
	}

	runErr := newScriptEngine(host, mcpClient, parsed.prompt, workingDirectory, conversationID).run(context.Background(), script)
	var crashResult scriptedCrash
	if errors.As(runErr, &crashResult) {
		return crashResult.code
	}
	if errors.Is(runErr, errRunCancelled) {
		return 130
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "fakeharness: %v\n", runErr)
		return 70
	}
	switch role {
	case "claude":
		host.emit(map[string]any{"type": "result", "subtype": "success", "result": host.lastOutput})
	case "agy":
		host.emit(map[string]any{
			"event": "result",
			"result": map[string]any{
				"conversation_id": conversationID,
				"status":          "SUCCESS",
				"response":        host.lastOutput,
				"usage": map[string]any{
					"input_tokens":      84,
					"output_tokens":     12,
					"cache_read_tokens": 20,
					"total_tokens":      96,
				},
			},
		})
	}
	return 0
}

func parseNativeInvocation(role string, arguments []string) (parsedNativeArguments, error) {
	var parsed parsedNativeArguments

	if role == "codex" {
		if len(arguments) < 2 || arguments[0] != "exec" {
			return parsed, errors.New("expected codex exec")
		}
		parsed.prompt = arguments[len(arguments)-1]
		for index := 0; index+1 < len(arguments)-1; index++ {
			if arguments[index] != "-c" {
				continue
			}
			key, value, found := strings.Cut(arguments[index+1], "=")
			if found && key == "mcp_servers.orchestra_hub.url" {
				unquoted, unquoteErr := strconv.Unquote(value)
				if unquoteErr != nil {
					return parsed, errors.New("MCP URL configuration is not quoted")
				}
				parsed.mcpEndpoint = unquoted
			}
		}
		return parsed, nil
	}

	if role == "agy" {
		for index := 0; index < len(arguments); index++ {
			argument := arguments[index]
			switch {
			case strings.HasPrefix(argument, "-p="):
				parsed.prompt = strings.TrimPrefix(argument, "-p=")
			case argument == "-p" && index+1 < len(arguments):
				parsed.prompt = arguments[index+1]
				index++
			case argument == "--conversation" && index+1 < len(arguments):
				parsed.conversationID = arguments[index+1]
				index++
			case argument == "--model" && index+1 < len(arguments):
				parsed.modelID = arguments[index+1]
				index++
			case argument == "--sandbox":
				parsed.sandbox = true
			case argument == "--add-dir" && index+1 < len(arguments):
				addedDirectory := arguments[index+1]
				index++
				mcpConfigPath := filepath.Join(addedDirectory, ".agents", "mcp_config.json")
				if rawBytes, readErr := os.ReadFile(mcpConfigPath); readErr == nil {
					var document struct {
						MCPServers map[string]struct {
							ServerURL string            `json:"serverUrl"`
							Headers   map[string]string `json:"headers"`
						} `json:"mcpServers"`
					}
					if json.Unmarshal(rawBytes, &document) == nil {
						if hub, found := document.MCPServers["orchestra_hub"]; found {
							parsed.mcpEndpoint = hub.ServerURL
							parsed.mcpBearerToken = strings.TrimPrefix(hub.Headers["Authorization"], "Bearer ")
						}
					}
				}
			}
		}
		if parsed.prompt == "" {
			return parsed, errors.New("expected agy -p=<prompt>")
		}
		return parsed, nil
	}

	for index := 0; index+1 < len(arguments); index++ {
		switch arguments[index] {
		case "-p":
			parsed.prompt = arguments[index+1]
		case "--mcp-config":
			var configuration struct {
				MCPServers map[string]struct {
					URL string `json:"url"`
				} `json:"mcpServers"`
			}
			if err := json.Unmarshal([]byte(arguments[index+1]), &configuration); err != nil {
				return parsed, errors.New("MCP configuration flag is not valid JSON")
			}
			parsed.mcpEndpoint = configuration.MCPServers["orchestra_hub"].URL
		}
	}
	if parsed.prompt == "" {
		return parsed, errors.New("expected claude -p <prompt>")
	}
	return parsed, nil
}

func (host *nativeExecutionHost) emit(event map[string]any) {
	encodedBytes, _ := json.Marshal(event)
	fmt.Println(string(encodedBytes))
}

func (host *nativeExecutionHost) message(text string) error {
	host.lastOutput = text
	switch host.role {
	case "codex":
		host.emit(map[string]any{
			"type": "item.completed",
			"item": map[string]any{"type": "agent_message", "text": text},
		})
	case "agy":
		host.stepIndex++
		host.emit(map[string]any{
			"event": "step_update",
			"step_update": map[string]any{
				"conversation_id": host.conversationID,
				"step_index":      host.stepIndex,
				"state":           "DONE",
				"step_type":       "agent_response",
				"text_delta":      text,
			},
		})
	default:
		host.emit(map[string]any{
			"type": "assistant",
			"message": map[string]any{
				"content": []any{map[string]any{"type": "text", "text": text}},
			},
		})
	}
	return nil
}

func (host *nativeExecutionHost) thought(text string) error {
	if host.role == "agy" {
		host.stepIndex++
		host.emit(map[string]any{
			"event": "step_update",
			"step_update": map[string]any{
				"conversation_id": host.conversationID,
				"step_index":      host.stepIndex,
				"state":           "DONE",
				"step_type":       "thought",
				"text_delta":      text,
			},
		})
	}
	return nil
}

func (host *nativeExecutionHost) update(json.RawMessage) error { return nil }

func (host *nativeExecutionHost) raw(string) error {
	return errors.New("native CLI emulation has no ACP stream")
}

func (host *nativeExecutionHost) permission(context.Context, map[string]any) (any, error) {
	return nil, errors.New("native CLI emulation cannot raise ACP permission requests")
}

func (host *nativeExecutionHost) cancelled() <-chan struct{} {
	return host.cancelChannel
}
