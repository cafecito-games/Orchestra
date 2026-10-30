package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	FakeCodexAdapterVersion  = "1.12.0"
	FakeClaudeAdapterVersion = "0.79.0"
	FakeCodexDefaultModel    = "gpt-5.5"
	FakeCodexAltModel        = "gpt-5.4"
	FakeClaudeDefaultModel   = "claude-sonnet-4-6"
	FakeClaudeAltModel       = "claude-opus-4-2"
)

type acpAdapterFlavor struct {
	short         string
	agentName     string
	title         string
	version       string
	configOptions func(mode, model string) string
	initialMode   string
	initialModel  string
}

func renderCodexConfigOptions(mode, model string) string {
	modes := `[{"value":"read-only","name":"Ask for approval"},{"value":"agent","name":"Approve for me"},{"value":"agent-full-access","name":"Full access"}]`
	models := `[{"group":"openai","name":"OpenAI","options":[{"value":"` + FakeCodexDefaultModel + `","name":"5.5"},{"value":"` + FakeCodexAltModel + `","name":"5.4"}]}]`
	return `[{"id":"mode","name":"Mode","category":"mode","type":"select","currentValue":"` + mode + `","options":` + modes + `},` +
		`{"id":"model","name":"Model","category":"model","type":"select","currentValue":"` + model + `","options":` + models + `}]`
}

func renderClaudeConfigOptions(mode, model string) string {
	modes := `[{"value":"default","name":"Manual"},{"value":"acceptEdits","name":"Accept edits"},{"value":"plan","name":"Plan"},{"value":"auto","name":"Auto"},{"value":"bypassPermissions","name":"Bypass permissions"}]`
	models := `[{"group":"anthropic","name":"Anthropic","options":[{"value":"` + FakeClaudeDefaultModel + `","name":"Sonnet 4.6"},{"value":"` + FakeClaudeAltModel + `","name":"Opus 4.2"}]}]`
	return `[{"id":"mode","name":"Mode","category":"mode","type":"select","currentValue":"` + mode + `","options":` + modes + `},` +
		`{"id":"model","name":"Model","category":"model","type":"select","currentValue":"` + model + `","options":` + models + `}]`
}

var (
	codexFlavor = acpAdapterFlavor{
		short:         "codex",
		agentName:     "@agentclientprotocol/codex-acp",
		title:         "Codex",
		version:       FakeCodexAdapterVersion,
		configOptions: renderCodexConfigOptions,
		initialMode:   "agent",
		initialModel:  FakeCodexDefaultModel,
	}
	claudeFlavor = acpAdapterFlavor{
		short:         "claude",
		agentName:     "@agentclientprotocol/claude-agent-acp",
		title:         "Claude",
		version:       FakeClaudeAdapterVersion,
		configOptions: renderClaudeConfigOptions,
		initialMode:   "auto",
		initialModel:  FakeClaudeDefaultModel,
	}
)

const advertisedACPCapabilities = `{"loadSession":false,"promptCapabilities":{"image":false,"audio":false,"embeddedContext":false},"mcpCapabilities":{"http":true,"sse":false},"sessionCapabilities":{"close":{},"resume":{}}}`

const rejectACPSessionMarker = ".fake-acp-reject-session"

type jsonRPCFrame struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

type acpAdapterServer struct {
	flavor   acpAdapterFlavor
	recorder *eventRecorder
	requests chan jsonRPCFrame
	stdinEOF chan struct{}

	writeMutex sync.Mutex

	stateMutex      sync.Mutex
	pending         map[string]chan jsonRPCFrame
	promptCancel    chan struct{}
	promptCancelled bool

	sessionID        string
	workingDirectory string
	mode             string
	model            string
	mcpClient        *hubMCPClient
	sessionCounter   int
	permissionCount  int
}

func runACP(recorder *eventRecorder, flavor acpAdapterFlavor) int {
	agent := &acpAdapterServer{
		flavor:   flavor,
		recorder: recorder,
		requests: make(chan jsonRPCFrame),
		stdinEOF: make(chan struct{}),
		pending:  map[string]chan jsonRPCFrame{},
		mode:     flavor.initialMode,
		model:    flavor.initialModel,
	}
	go agent.readStdinLoop()
	for request := range agent.requests {
		if err := agent.handleRequest(request); err != nil {
			var crashResult scriptedCrash
			if errors.As(err, &crashResult) {
				return crashResult.code
			}
			fmt.Fprintf(os.Stderr, "fakeharness: %v\n", err)
			return 70
		}
	}
	return 0
}

func (agent *acpAdapterServer) readStdinLoop() {
	defer close(agent.requests)
	defer close(agent.stdinEOF)
	defer agent.cancelActivePrompt()

	reader := bufio.NewReaderSize(os.Stdin, 1<<20)
	for {
		lineBytes, err := reader.ReadBytes('\n')
		if len(strings.TrimSpace(string(lineBytes))) > 0 {
			var decoded jsonRPCFrame
			if json.Unmarshal(lineBytes, &decoded) == nil {
				agent.routeIncomingFrame(decoded)
			}
		}
		if err != nil {
			return
		}
	}
}

func (agent *acpAdapterServer) routeIncomingFrame(incoming jsonRPCFrame) {
	switch {
	case incoming.Method == "" && len(incoming.ID) > 0:
		agent.stateMutex.Lock()
		waiter := agent.pending[string(incoming.ID)]
		delete(agent.pending, string(incoming.ID))
		agent.stateMutex.Unlock()
		if waiter != nil {
			waiter <- incoming
		}
	case incoming.Method == "session/cancel":
		agent.cancelActivePrompt()
	default:
		if incoming.Method == "session/prompt" {
			agent.stateMutex.Lock()
			agent.promptCancel = make(chan struct{})
			agent.promptCancelled = false
			agent.stateMutex.Unlock()
		}
		agent.requests <- incoming
	}
}

func (agent *acpAdapterServer) cancelActivePrompt() {
	agent.stateMutex.Lock()
	defer agent.stateMutex.Unlock()
	if agent.promptCancel != nil && !agent.promptCancelled {
		agent.promptCancelled = true
		close(agent.promptCancel)
	}
}

func (agent *acpAdapterServer) handleRequest(request jsonRPCFrame) error {
	switch request.Method {
	case "initialize":
		return agent.respond(
			request.ID,
			`{"protocolVersion":1,"agentCapabilities":`+advertisedACPCapabilities+`,"authMethods":[],"agentInfo":{"name":"`+agent.flavor.agentName+`","title":"`+agent.flavor.title+`","version":"`+agent.flavor.version+`"}}`,
		)
	case "session/new":
		return agent.handleNewSession(request)
	case "session/resume":
		return agent.handleResumeSession(request)
	case "session/set_config_option":
		var parameters struct {
			ConfigID string `json:"configId"`
			Value    string `json:"value"`
		}
		if err := json.Unmarshal(request.Params, &parameters); err != nil {
			return agent.respondError(request.ID, -32602, "invalid params")
		}
		switch parameters.ConfigID {
		case "mode":
			agent.mode = parameters.Value
		case "model":
			agent.model = parameters.Value
		default:
			return agent.respondError(request.ID, -32602, "unknown configuration option")
		}
		return agent.respond(request.ID, `{"configOptions":`+agent.flavor.configOptions(agent.mode, agent.model)+`}`)
	case "session/prompt":
		return agent.handlePrompt(request)
	case "session/close":
		agent.recorder.write(map[string]any{"event": "session-closed", "sessionId": agent.sessionID})
		return agent.respond(request.ID, `{}`)
	default:
		if len(request.ID) > 0 {
			return agent.respondError(request.ID, -32601, "method not found")
		}
		return nil
	}
}

type acpSessionParameters struct {
	SessionID  string `json:"sessionId"`
	Cwd        string `json:"cwd"`
	MCPServers []struct {
		URL     string `json:"url"`
		Headers []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"headers"`
	} `json:"mcpServers"`
}

func (agent *acpAdapterServer) configureSession(parameters acpSessionParameters) error {
	agent.workingDirectory = parameters.Cwd
	agent.mcpClient = nil
	for _, server := range parameters.MCPServers {
		bearerToken := ""
		for _, header := range server.Headers {
			if header.Name == "Authorization" {
				bearerToken = strings.TrimPrefix(header.Value, "Bearer ")
			}
		}
		client, err := newHubMCPClient(server.URL, bearerToken)
		if err != nil {
			return err
		}
		agent.mcpClient = client
	}
	return nil
}

func (agent *acpAdapterServer) connectMCP() error {
	if agent.mcpClient == nil {
		return nil
	}
	return agent.mcpClient.connect(context.Background())
}

func (agent *acpAdapterServer) handleNewSession(request jsonRPCFrame) error {
	var parameters acpSessionParameters
	if err := json.Unmarshal(request.Params, &parameters); err != nil {
		return agent.respondError(request.ID, -32602, "invalid params")
	}
	if err := agent.configureSession(parameters); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(parameters.Cwd, rejectACPSessionMarker)); err == nil {
		agent.recorder.write(map[string]any{"event": "session-rejected", "cwd": parameters.Cwd})
		return agent.respond(request.ID, `{"sessionId":""}`)
	}
	agent.sessionCounter++
	agent.sessionID = "fake-" + agent.flavor.short + "-" + strconv.Itoa(os.Getpid()) + "-" + strconv.Itoa(agent.sessionCounter)
	if len(parameters.MCPServers) > 0 {
		agent.rememberSession(agent.sessionID)
	}
	agent.recorder.write(map[string]any{
		"event":     "session-new",
		"sessionId": agent.sessionID,
		"cwd":       parameters.Cwd,
		"mcp":       agent.mcpClient != nil,
	})
	if err := agent.respond(request.ID, `{"sessionId":"`+agent.sessionID+`","configOptions":`+agent.flavor.configOptions(agent.mode, agent.model)+`}`); err != nil {
		return err
	}
	return agent.connectMCP()
}

func (agent *acpAdapterServer) handleResumeSession(request jsonRPCFrame) error {
	var parameters acpSessionParameters
	if err := json.Unmarshal(request.Params, &parameters); err != nil {
		return agent.respondError(request.ID, -32602, "invalid params")
	}
	if !agent.knowsSession(parameters.SessionID) {
		agent.recorder.write(map[string]any{"event": "session-resume-refused", "sessionId": parameters.SessionID})
		return agent.respondError(request.ID, -32002, "Resource not found")
	}
	if err := agent.configureSession(parameters); err != nil {
		return err
	}
	agent.sessionID = parameters.SessionID
	agent.recorder.write(map[string]any{
		"event":     "session-resumed",
		"sessionId": agent.sessionID,
		"cwd":       parameters.Cwd,
	})
	if err := agent.respond(request.ID, `{"configOptions":`+agent.flavor.configOptions(agent.mode, agent.model)+`}`); err != nil {
		return err
	}
	return agent.connectMCP()
}

func (agent *acpAdapterServer) sessionFilePath(sessionID string) string {
	directory := os.Getenv(sessionDirectoryVariable)
	if directory == "" || sessionID == "" || sessionID != filepath.Base(sessionID) {
		return ""
	}
	return filepath.Join(directory, sessionID)
}

func (agent *acpAdapterServer) rememberSession(sessionID string) {
	if path := agent.sessionFilePath(sessionID); path != "" {
		_ = os.WriteFile(path, []byte(agent.flavor.short+"\n"), 0o600)
	}
}

func (agent *acpAdapterServer) knowsSession(sessionID string) bool {
	path := agent.sessionFilePath(sessionID)
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

func (agent *acpAdapterServer) handlePrompt(request jsonRPCFrame) error {
	var parameters struct {
		SessionID string `json:"sessionId"`
		Prompt    []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"prompt"`
	}
	if err := json.Unmarshal(request.Params, &parameters); err != nil || parameters.SessionID != agent.sessionID {
		return agent.respondError(request.ID, -32602, "invalid prompt")
	}
	var promptBuilder strings.Builder
	for _, block := range parameters.Prompt {
		if block.Type == "text" {
			promptBuilder.WriteString(block.Text)
		}
	}
	promptText := promptBuilder.String()
	agent.recorder.write(map[string]any{"event": "prompt", "sessionId": agent.sessionID})
	script, err := extractScript(promptText)
	if err != nil {
		return err
	}
	runErr := newScriptEngine(agent, agent.mcpClient, promptText, agent.workingDirectory, agent.sessionID).run(context.Background(), script)
	switch {
	case runErr == nil:
		return agent.respond(request.ID, `{"stopReason":"end_turn"}`)
	case errors.Is(runErr, errRunCancelled):
		agent.recorder.write(map[string]any{"event": "prompt-cancelled", "sessionId": agent.sessionID})
		return agent.respond(request.ID, `{"stopReason":"cancelled"}`)
	default:
		return runErr
	}
}

func (agent *acpAdapterServer) cancelled() <-chan struct{} {
	agent.stateMutex.Lock()
	defer agent.stateMutex.Unlock()
	if agent.promptCancel == nil {
		return make(chan struct{})
	}
	return agent.promptCancel
}

func (agent *acpAdapterServer) emitSessionUpdate(updatePayload string) error {
	return agent.writeLine(`{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"` + agent.sessionID + `","update":` + updatePayload + `}}` + "\n")
}

func (agent *acpAdapterServer) message(text string) error {
	encodedText, _ := json.Marshal(text)
	return agent.emitSessionUpdate(`{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":` + string(encodedText) + `}}`)
}

func (agent *acpAdapterServer) thought(text string) error {
	encodedText, _ := json.Marshal(text)
	return agent.emitSessionUpdate(`{"sessionUpdate":"agent_thought_chunk","content":{"type":"text","text":` + string(encodedText) + `}}`)
}

func (agent *acpAdapterServer) update(raw json.RawMessage) error {
	return agent.emitSessionUpdate(string(raw))
}

func (agent *acpAdapterServer) raw(text string) error {
	return agent.writeLine(text)
}

func (agent *acpAdapterServer) permission(operationContext context.Context, requestPayload map[string]any) (any, error) {
	agent.stateMutex.Lock()
	agent.permissionCount++
	requestIdentifier := `"permission-` + strconv.Itoa(agent.permissionCount) + `"`
	waiter := make(chan jsonRPCFrame, 1)
	agent.pending[requestIdentifier] = waiter
	agent.stateMutex.Unlock()

	requestPayload["sessionId"] = agent.sessionID
	encodedParams, err := json.Marshal(requestPayload)
	if err != nil {
		return nil, err
	}
	frameText := `{"jsonrpc":"2.0","id":` + requestIdentifier + `,"method":"session/request_permission","params":` + string(encodedParams) + "}\n"
	if err := agent.writeLine(frameText); err != nil {
		return nil, err
	}
	select {
	case response := <-waiter:
		if len(response.Result) > 0 {
			var decoded any
			if unmarshalErr := json.Unmarshal(response.Result, &decoded); unmarshalErr != nil {
				return nil, unmarshalErr
			}
			return decoded, nil
		}
		return nil, errors.New("permission request answered with JSON-RPC error")
	case <-agent.cancelled():
		return nil, errRunCancelled
	case <-agent.stdinEOF:
		terminateProcess(0)
		return nil, nil
	case <-operationContext.Done():
		return nil, operationContext.Err()
	}
}

func (agent *acpAdapterServer) respond(id json.RawMessage, result string) error {
	return agent.writeLine(`{"jsonrpc":"2.0","id":` + string(id) + `,"result":` + result + "}\n")
}

func (agent *acpAdapterServer) respondError(id json.RawMessage, code int, message string) error {
	encodedMessage, _ := json.Marshal(message)
	return agent.writeLine(`{"jsonrpc":"2.0","id":` + string(id) + `,"error":{"code":` + strconv.Itoa(code) + `,"message":` + string(encodedMessage) + "}}\n")
}

func (agent *acpAdapterServer) writeLine(frameText string) error {
	agent.writeMutex.Lock()
	defer agent.writeMutex.Unlock()
	_, err := os.Stdout.WriteString(frameText)
	return err
}
