package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

var (
	codexNativeVersion  = "codex-cli 0.46.0"
	claudeNativeVersion = "2.0.14 (Claude Code)"
)

func nativeVersionForRole(role string) string {
	if role == "codex" {
		return codexNativeVersion
	}
	return claudeNativeVersion
}

type nativeExecutionHost struct {
	role          string
	lastOutput    string
	cancelChannel chan struct{}
}

func runNative(recorder *eventRecorder, role string, arguments []string) int {
	if len(arguments) == 1 && arguments[0] == "--version" {
		version := nativeVersionForRole(role)
		recorder.write(map[string]any{"event": "native-version", "version": version})
		fmt.Println(version)
		return 0
	}

	prompt, mcpEndpoint, err := parseNativeInvocation(role, arguments)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fakeharness: %v\n", err)
		return 64
	}

	var mcpClient *hubMCPClient
	if mcpEndpoint != "" {
		mcpClient, err = newHubMCPClient(mcpEndpoint, os.Getenv("ORCHESTRA_MCP_TOKEN"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "fakeharness: %v\n", err)
			return 64
		}
		if connectErr := mcpClient.connect(context.Background()); connectErr != nil {
			fmt.Fprintf(os.Stderr, "fakeharness: connect MCP: %v\n", connectErr)
			return 64
		}
	}

	recorder.write(map[string]any{"event": "native-run", "mcp": mcpClient != nil, "version": nativeVersionForRole(role)})

	script, err := extractScript(prompt)
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
	host := &nativeExecutionHost{role: role, cancelChannel: cancelChannel}
	if role == "codex" {
		host.emit(map[string]any{"type": "thread.started", "thread_id": "native-codex-" + strconv.Itoa(os.Getpid())})
	} else {
		host.emit(map[string]any{"type": "system", "session_id": "native-claude-" + strconv.Itoa(os.Getpid())})
	}

	runErr := newScriptEngine(host, mcpClient, prompt, workingDirectory, "").run(context.Background(), script)
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
	if role == "claude" {
		host.emit(map[string]any{"type": "result", "subtype": "success", "result": host.lastOutput})
	}
	return 0
}

func parseNativeInvocation(role string, arguments []string) (string, string, error) {
	var prompt string
	var endpoint string

	if role == "codex" {
		if len(arguments) < 2 || arguments[0] != "exec" {
			return "", "", errors.New("expected codex exec")
		}
		prompt = arguments[len(arguments)-1]
		for index := 0; index+1 < len(arguments)-1; index++ {
			if arguments[index] != "-c" {
				continue
			}
			key, value, found := strings.Cut(arguments[index+1], "=")
			if found && key == "mcp_servers.orchestra_hub.url" {
				unquoted, unquoteErr := strconv.Unquote(value)
				if unquoteErr != nil {
					return "", "", errors.New("MCP URL configuration is not quoted")
				}
				endpoint = unquoted
			}
		}
		return prompt, endpoint, nil
	}

	for index := 0; index+1 < len(arguments); index++ {
		switch arguments[index] {
		case "-p":
			prompt = arguments[index+1]
		case "--mcp-config":
			var configuration struct {
				MCPServers map[string]struct {
					URL string `json:"url"`
				} `json:"mcpServers"`
			}
			if err := json.Unmarshal([]byte(arguments[index+1]), &configuration); err != nil {
				return "", "", errors.New("MCP configuration flag is not valid JSON")
			}
			endpoint = configuration.MCPServers["orchestra_hub"].URL
		}
	}
	if prompt == "" {
		return "", "", errors.New("expected claude -p <prompt>")
	}
	return prompt, endpoint, nil
}

func (host *nativeExecutionHost) emit(event map[string]any) {
	encodedBytes, _ := json.Marshal(event)
	fmt.Println(string(encodedBytes))
}

func (host *nativeExecutionHost) message(text string) error {
	host.lastOutput = text
	if host.role == "codex" {
		host.emit(map[string]any{
			"type": "item.completed",
			"item": map[string]any{"type": "agent_message", "text": text},
		})
	} else {
		host.emit(map[string]any{
			"type": "assistant",
			"message": map[string]any{
				"content": []any{map[string]any{"type": "text", "text": text}},
			},
		})
	}
	return nil
}

func (host *nativeExecutionHost) thought(string) error { return nil }

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
