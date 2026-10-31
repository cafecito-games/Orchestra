// Package antigravityacp implements Orchestra's built-in ACP v1 stdio bridge
// for Google Antigravity's agy CLI.
package antigravityacp

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const (
	AdapterID       = "antigravity-acp"
	AgentInfoName   = "orchestra-antigravity-acp"
	AgentInfoTitle  = "Antigravity"
	FallbackVersion = "0.1.0"

	maximumFrameBytes   = 4 * 1024 * 1024
	mcpHandshakeTimeout = 10 * time.Second

	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603
	codeSessionMissing = -32002
)

// Options configures an Antigravity ACP v1 stdio bridge server.
type Options struct {
	Binary           string
	StateDirectory   string
	Version          string
	WorkingDirectory string
	HTTPClient       *http.Client
	Now              func() time.Time
}

// NormalizedAdapterVersion returns a protocol-valid normalized version string for ACP handshakes.
func NormalizedAdapterVersion(version string) string {
	trimmed := strings.TrimSpace(version)
	if protocol.IsNormalizedVersion(trimmed) {
		return trimmed
	}
	if extracted := protocol.ExtractNormalizedVersion(trimmed); protocol.IsNormalizedVersion(extracted) {
		return extracted
	}
	return FallbackVersion
}

// ResolveBinary locates the agy CLI executable from explicit configuration, environment variables, or PATH.
func ResolveBinary(explicitBinary string, lookupEnvironment func(string) (string, bool)) (string, error) {
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}
	candidates := []string{strings.TrimSpace(explicitBinary)}
	for _, variableName := range []string{NativeBinaryEnvironmentVariable, LegacyBinaryEnvironmentVariable} {
		if value, found := lookupEnvironment(variableName); found && strings.TrimSpace(value) != "" {
			candidates = append(candidates, strings.TrimSpace(value))
		}
	}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		resolved, err := exec.LookPath(candidate)
		if err == nil {
			return resolved, nil
		}
		if filepath.IsAbs(candidate) {
			if information, statErr := os.Stat(candidate); statErr == nil && !information.IsDir() {
				return candidate, nil
			}
		}
		return "", fmt.Errorf("configured agy executable %q is not available", candidate)
	}
	resolved, err := exec.LookPath("agy")
	if err != nil {
		return "", errors.New("agy executable was not found on PATH")
	}
	return resolved, nil
}

// Run serves ACP v1 JSON-RPC over standardInput and standardOutput until EOF or context cancellation.
func Run(
	operationContext context.Context,
	standardInput io.Reader,
	standardOutput io.Writer,
	standardError io.Writer,
	options Options,
) error {
	server, err := newServer(standardOutput, standardError, options)
	if err != nil {
		return err
	}
	return server.serve(operationContext, standardInput)
}

type jsonRPCFrame struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type activeRuntimeSession struct {
	mcpServers []MCPHTTPServer
}

type server struct {
	binaryPath       string
	version          string
	workingDirectory string
	httpClient       *http.Client
	now              func() time.Time
	store            *sessionStore

	outputWriter io.Writer
	errorWriter  io.Writer
	writeMutex   sync.Mutex

	secretMutex sync.Mutex
	secrets     []string

	runtimeMutex    sync.Mutex
	runtimeSessions map[string]activeRuntimeSession
	activeCancels   map[string]chan struct{}
	modelsLoaded    bool
	availableModels []discoveredModel
	turnCounter     int
}

func newServer(standardOutput, standardError io.Writer, options Options) (*server, error) {
	if standardOutput == nil {
		return nil, errors.New("standardOutput is required")
	}
	if standardError == nil {
		standardError = io.Discard
	}
	nowFunc := options.Now
	if nowFunc == nil {
		nowFunc = time.Now
	}
	workingDirectory := strings.TrimSpace(options.WorkingDirectory)
	if workingDirectory == "" {
		if currentDirectory, err := os.Getwd(); err == nil {
			workingDirectory = currentDirectory
		}
	}
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: mcpHandshakeTimeout}
	}
	initialMode := ModeSandbox
	if initialEnvironment := strings.TrimSpace(os.Getenv("ORCHESTRA_ANTIGRAVITY_INITIAL_MODE")); slices.Contains([]string{ModeSandbox, ModeBypass, ModePlan}, initialEnvironment) {
		initialMode = initialEnvironment
	}
	_ = initialMode

	return &server{
		binaryPath:       strings.TrimSpace(options.Binary),
		version:          NormalizedAdapterVersion(options.Version),
		workingDirectory: workingDirectory,
		httpClient:       httpClient,
		now:              nowFunc,
		store:            newSessionStore(strings.TrimSpace(options.StateDirectory)),
		outputWriter:     standardOutput,
		errorWriter:      standardError,
		runtimeSessions:  map[string]activeRuntimeSession{},
		activeCancels:    map[string]chan struct{}{},
	}, nil
}

func (bridge *server) serve(operationContext context.Context, standardInput io.Reader) error {
	requests := make(chan jsonRPCFrame, 16)
	readErrors := make(chan error, 1)

	go func() {
		defer close(requests)
		defer bridge.cancelAllPrompts()

		scanner := bufio.NewScanner(standardInput)
		scanner.Buffer(make([]byte, 64*1024), maximumFrameBytes)
		for scanner.Scan() {
			lineBytes := bytes.TrimSpace(scanner.Bytes())
			if len(lineBytes) == 0 {
				continue
			}
			var incoming jsonRPCFrame
			if err := json.Unmarshal(lineBytes, &incoming); err != nil {
				_ = bridge.writeFrame(jsonRPCFrame{
					JSONRPC: "2.0",
					Error:   &jsonRPCError{Code: codeParseError, Message: "invalid JSON-RPC frame"},
				})
				continue
			}
			if incoming.Method == "session/cancel" {
				bridge.handleCancelNotification(incoming.Params)
				continue
			}
			select {
			case requests <- incoming:
			case <-operationContext.Done():
				return
			}
		}
		if scanErr := scanner.Err(); scanErr != nil {
			readErrors <- scanErr
			return
		}
		readErrors <- nil
	}()

	for {
		select {
		case <-operationContext.Done():
			bridge.cancelAllPrompts()
			return nil
		case request, open := <-requests:
			if !open {
				return <-readErrors
			}
			bridge.handleRequest(operationContext, request)
		}
	}
}

func (bridge *server) handleRequest(operationContext context.Context, request jsonRPCFrame) {
	hasID := len(bytes.TrimSpace(request.ID)) > 0 && !bytes.Equal(bytes.TrimSpace(request.ID), []byte("null"))
	switch request.Method {
	case "initialize":
		if !hasID {
			return
		}
		bridge.handleInitialize(request.ID)
	case "authenticate":
		if hasID {
			_ = bridge.respondResult(request.ID, map[string]any{})
		}
	case "session/new":
		if !hasID {
			return
		}
		bridge.handleSessionNew(operationContext, request.ID, request.Params)
	case "session/resume":
		if !hasID {
			return
		}
		bridge.handleSessionResume(operationContext, request.ID, request.Params)
	case "session/set_config_option":
		if !hasID {
			return
		}
		bridge.handleSetConfigOption(operationContext, request.ID, request.Params)
	case "session/prompt":
		if !hasID {
			return
		}
		bridge.handleSessionPrompt(operationContext, request.ID, request.Params)
	case "session/close":
		bridge.handleSessionClose(request.ID, hasID, request.Params)
	default:
		if hasID {
			_ = bridge.respondError(request.ID, codeMethodNotFound, "unsupported ACP method "+request.Method)
		}
	}
}

func (bridge *server) handleInitialize(requestID json.RawMessage) {
	_ = bridge.respondResult(requestID, map[string]any{
		"protocolVersion": 1,
		"agentCapabilities": map[string]any{
			"loadSession": false,
			"promptCapabilities": map[string]any{
				"image":           false,
				"audio":           false,
				"embeddedContext": true,
			},
			"mcpCapabilities": map[string]any{
				"http": true,
				"sse":  false,
			},
			"sessionCapabilities": map[string]any{
				"close":  map[string]any{},
				"resume": map[string]any{},
			},
		},
		"authMethods": []map[string]any{
			{
				"id":   "agy-account",
				"name": "Google Antigravity Account",
			},
		},
		"agentInfo": map[string]any{
			"name":    AgentInfoName,
			"title":   AgentInfoTitle,
			"version": bridge.version,
		},
	})
}

type wireHTTPHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type wireMCPServer struct {
	Type    string           `json:"type"`
	Name    string           `json:"name"`
	URL     string           `json:"url"`
	Headers []wireHTTPHeader `json:"headers"`
}

type newSessionParameters struct {
	WorkingDirectory string          `json:"cwd"`
	MCPServers       []wireMCPServer `json:"mcpServers"`
}

type resumeSessionParameters struct {
	SessionID        string          `json:"sessionId"`
	WorkingDirectory string          `json:"cwd"`
	MCPServers       []wireMCPServer `json:"mcpServers"`
}

func (bridge *server) handleSessionNew(operationContext context.Context, requestID json.RawMessage, rawParams json.RawMessage) {
	var parameters newSessionParameters
	if len(bytes.TrimSpace(rawParams)) > 0 {
		if err := json.Unmarshal(rawParams, &parameters); err != nil {
			_ = bridge.respondError(requestID, codeInvalidParams, "invalid session/new parameters")
			return
		}
	}
	workingDirectory, err := bridge.resolveSessionDirectory(parameters.WorkingDirectory)
	if err != nil {
		_ = bridge.respondError(requestID, codeInvalidParams, err.Error())
		return
	}
	mcpServers, err := bridge.prepareMCPServers(operationContext, parameters.MCPServers)
	if err != nil {
		_ = bridge.respondError(requestID, codeInternalError, bridge.redact(err.Error()))
		return
	}

	sessionID := protocol.NewIdentifier("agysess")
	defaultMode := ModeSandbox
	if envMode := strings.TrimSpace(os.Getenv("ORCHESTRA_ANTIGRAVITY_INITIAL_MODE")); slices.Contains([]string{ModeSandbox, ModeBypass, ModePlan}, envMode) {
		defaultMode = envMode
	}
	session := storedSession{
		WorkingDirectory: workingDirectory,
		Mode:             defaultMode,
		UpdatedAt:        bridge.now().UTC().Format(time.RFC3339Nano),
	}
	if err := bridge.store.save(sessionID, session); err != nil {
		_ = bridge.respondError(requestID, codeInternalError, "persist session state: "+bridge.redact(err.Error()))
		return
	}

	bridge.runtimeMutex.Lock()
	bridge.runtimeSessions[sessionID] = activeRuntimeSession{mcpServers: mcpServers}
	bridge.runtimeMutex.Unlock()

	models := bridge.models(operationContext)
	_ = bridge.respondResult(requestID, map[string]any{
		"sessionId":     sessionID,
		"configOptions": buildConfigOptions(session, models),
	})
}

func (bridge *server) handleSessionResume(operationContext context.Context, requestID json.RawMessage, rawParams json.RawMessage) {
	var parameters resumeSessionParameters
	if err := json.Unmarshal(rawParams, &parameters); err != nil || strings.TrimSpace(parameters.SessionID) == "" {
		_ = bridge.respondError(requestID, codeInvalidParams, "session/resume requires sessionId")
		return
	}
	sessionID := strings.TrimSpace(parameters.SessionID)
	session, found, err := bridge.store.load(sessionID)
	if err != nil || !found || strings.TrimSpace(session.ConversationID) == "" {
		_ = bridge.respondError(requestID, codeSessionMissing, "session cannot be resumed")
		return
	}

	workingDirectory := session.WorkingDirectory
	if strings.TrimSpace(parameters.WorkingDirectory) != "" {
		resolved, dirErr := bridge.resolveSessionDirectory(parameters.WorkingDirectory)
		if dirErr != nil {
			_ = bridge.respondError(requestID, codeInvalidParams, dirErr.Error())
			return
		}
		workingDirectory = resolved
	}
	mcpServers, err := bridge.prepareMCPServers(operationContext, parameters.MCPServers)
	if err != nil {
		_ = bridge.respondError(requestID, codeInternalError, bridge.redact(err.Error()))
		return
	}

	session.WorkingDirectory = workingDirectory
	session.UpdatedAt = bridge.now().UTC().Format(time.RFC3339Nano)
	if saveErr := bridge.store.save(sessionID, session); saveErr != nil {
		_ = bridge.respondError(requestID, codeInternalError, "persist resumed session: "+bridge.redact(saveErr.Error()))
		return
	}

	bridge.runtimeMutex.Lock()
	bridge.runtimeSessions[sessionID] = activeRuntimeSession{mcpServers: mcpServers}
	bridge.runtimeMutex.Unlock()

	models := bridge.models(operationContext)
	_ = bridge.respondResult(requestID, map[string]any{
		"configOptions": buildConfigOptions(session, models),
	})
}

type setConfigParameters struct {
	SessionID string `json:"sessionId"`
	ConfigID  string `json:"configId"`
	Value     string `json:"value"`
}

func (bridge *server) handleSetConfigOption(operationContext context.Context, requestID json.RawMessage, rawParams json.RawMessage) {
	var parameters setConfigParameters
	if err := json.Unmarshal(rawParams, &parameters); err != nil || parameters.SessionID == "" || parameters.ConfigID == "" || parameters.Value == "" {
		_ = bridge.respondError(requestID, codeInvalidParams, "session/set_config_option requires sessionId, configId, and value")
		return
	}
	session, found, err := bridge.store.load(parameters.SessionID)
	if err != nil || !found {
		_ = bridge.respondError(requestID, codeSessionMissing, "unknown session")
		return
	}
	models := bridge.models(operationContext)

	switch parameters.ConfigID {
	case ModeOption:
		if !slices.Contains([]string{ModeSandbox, ModeBypass, ModePlan}, parameters.Value) {
			_ = bridge.respondError(requestID, codeInvalidParams, "unsupported mode "+parameters.Value)
			return
		}
		session.Mode = parameters.Value
	case ModelOption:
		if len(models) > 0 && !containsModelValue(models, parameters.Value) && session.ModelID != parameters.Value {
			_ = bridge.respondError(requestID, codeInvalidParams, "unsupported model "+parameters.Value)
			return
		}
		session.ModelID = parameters.Value
	default:
		_ = bridge.respondError(requestID, codeInvalidParams, "unknown configuration option "+parameters.ConfigID)
		return
	}

	session.UpdatedAt = bridge.now().UTC().Format(time.RFC3339Nano)
	if err := bridge.store.save(parameters.SessionID, session); err != nil {
		_ = bridge.respondError(requestID, codeInternalError, bridge.redact(err.Error()))
		return
	}
	_ = bridge.respondResult(requestID, map[string]any{
		"configOptions": buildConfigOptions(session, models),
	})
}

type promptParameters struct {
	SessionID string            `json:"sessionId"`
	Prompt    []json.RawMessage `json:"prompt"`
}

func (bridge *server) handleSessionPrompt(operationContext context.Context, requestID json.RawMessage, rawParams json.RawMessage) {
	var parameters promptParameters
	if err := json.Unmarshal(rawParams, &parameters); err != nil || strings.TrimSpace(parameters.SessionID) == "" {
		_ = bridge.respondError(requestID, codeInvalidParams, "session/prompt requires sessionId")
		return
	}
	sessionID := strings.TrimSpace(parameters.SessionID)
	session, found, err := bridge.store.load(sessionID)
	if err != nil || !found {
		_ = bridge.respondError(requestID, codeSessionMissing, "unknown session")
		return
	}
	binaryPath, err := bridge.executablePath()
	if err != nil {
		_ = bridge.respondError(requestID, codeInternalError, bridge.redact(err.Error()))
		return
	}

	promptText := extractPromptText(parameters.Prompt)
	if strings.TrimSpace(promptText) == "" {
		_ = bridge.respondError(requestID, codeInvalidParams, "session/prompt requires non-empty prompt content")
		return
	}

	cancelChannel := make(chan struct{})
	bridge.runtimeMutex.Lock()
	runtimeState := bridge.runtimeSessions[sessionID]
	bridge.activeCancels[sessionID] = cancelChannel
	bridge.turnCounter++
	turnNumber := bridge.turnCounter
	bridge.runtimeMutex.Unlock()

	defer func() {
		bridge.runtimeMutex.Lock()
		if current, exists := bridge.activeCancels[sessionID]; exists && current == cancelChannel {
			delete(bridge.activeCancels, sessionID)
		}
		bridge.runtimeMutex.Unlock()
	}()

	workingDirectory := cmp.Or(session.WorkingDirectory, bridge.workingDirectory)

	outcome, turnErr := executeTurn(
		operationContext,
		binaryPath,
		workingDirectory,
		session,
		runtimeState.mcpServers,
		promptText,
		turnNumber,
		cancelChannel,
		func(conversationID string) {
			session.ConversationID = conversationID
			session.UpdatedAt = bridge.now().UTC().Format(time.RFC3339Nano)
			_ = bridge.store.save(sessionID, session)
		},
		func(updatePayload map[string]any) error {
			return bridge.sendNotification("session/update", map[string]any{
				"sessionId": sessionID,
				"update":    updatePayload,
			})
		},
	)

	if outcome.ConversationID != "" && outcome.ConversationID != session.ConversationID {
		session.ConversationID = outcome.ConversationID
		session.UpdatedAt = bridge.now().UTC().Format(time.RFC3339Nano)
		_ = bridge.store.save(sessionID, session)
	}

	if turnErr != nil {
		_ = bridge.respondError(requestID, codeInternalError, bridge.redact(turnErr.Error()))
		return
	}

	responsePayload := map[string]any{
		"stopReason": outcome.StopReason,
	}
	if outcome.Usage != nil {
		responsePayload["usage"] = outcome.Usage
	}
	_ = bridge.respondResult(requestID, responsePayload)
}

func (bridge *server) handleCancelNotification(rawParams json.RawMessage) {
	var parameters struct {
		SessionID string `json:"sessionId"`
	}
	if json.Unmarshal(rawParams, &parameters) != nil || parameters.SessionID == "" {
		return
	}
	bridge.cancelSessionPrompt(parameters.SessionID)
}

func (bridge *server) handleSessionClose(requestID json.RawMessage, hasID bool, rawParams json.RawMessage) {
	var parameters struct {
		SessionID string `json:"sessionId"`
	}
	if len(bytes.TrimSpace(rawParams)) > 0 {
		_ = json.Unmarshal(rawParams, &parameters)
	}
	if parameters.SessionID != "" {
		bridge.cancelSessionPrompt(parameters.SessionID)
		bridge.runtimeMutex.Lock()
		delete(bridge.runtimeSessions, parameters.SessionID)
		bridge.runtimeMutex.Unlock()
		bridge.store.evictMemory(parameters.SessionID)
	}
	if hasID {
		_ = bridge.respondResult(requestID, map[string]any{})
	}
}

func (bridge *server) cancelSessionPrompt(sessionID string) {
	bridge.runtimeMutex.Lock()
	cancelChannel, exists := bridge.activeCancels[sessionID]
	if exists {
		delete(bridge.activeCancels, sessionID)
		close(cancelChannel)
	}
	bridge.runtimeMutex.Unlock()
}

func (bridge *server) cancelAllPrompts() {
	bridge.runtimeMutex.Lock()
	channels := make([]chan struct{}, 0, len(bridge.activeCancels))
	for sessionID, channel := range bridge.activeCancels {
		delete(bridge.activeCancels, sessionID)
		channels = append(channels, channel)
	}
	bridge.runtimeMutex.Unlock()
	for _, channel := range channels {
		close(channel)
	}
}

func (bridge *server) executablePath() (string, error) {
	if bridge.binaryPath != "" {
		return ResolveBinary(bridge.binaryPath, os.LookupEnv)
	}
	resolved, err := ResolveBinary("", os.LookupEnv)
	if err != nil {
		return "", err
	}
	bridge.binaryPath = resolved
	return resolved, nil
}

func (bridge *server) models(operationContext context.Context) []discoveredModel {
	bridge.runtimeMutex.Lock()
	if bridge.modelsLoaded {
		cached := slices.Clone(bridge.availableModels)
		bridge.runtimeMutex.Unlock()
		return cached
	}
	bridge.runtimeMutex.Unlock()

	binaryPath, err := bridge.executablePath()
	var discovered []discoveredModel
	if err == nil {
		discovered = discoverModels(operationContext, binaryPath, bridge.store, bridge.now())
	}

	bridge.runtimeMutex.Lock()
	bridge.modelsLoaded = true
	bridge.availableModels = discovered
	bridge.runtimeMutex.Unlock()
	return slices.Clone(discovered)
}

func (bridge *server) resolveSessionDirectory(requested string) (string, error) {
	candidate := strings.TrimSpace(requested)
	if candidate == "" {
		candidate = bridge.workingDirectory
	}
	if !filepath.IsAbs(candidate) {
		return "", errors.New("session cwd must be an absolute path")
	}
	information, err := os.Stat(candidate)
	if err != nil || !information.IsDir() {
		return "", errors.New("session cwd does not exist or is not a directory")
	}
	return filepath.Clean(candidate), nil
}

func (bridge *server) prepareMCPServers(operationContext context.Context, wireServers []wireMCPServer) ([]MCPHTTPServer, error) {
	if len(wireServers) == 0 {
		return nil, nil
	}
	prepared := make([]MCPHTTPServer, 0, len(wireServers))
	for _, entry := range wireServers {
		if entry.Type != "" && !strings.EqualFold(entry.Type, "http") {
			return nil, fmt.Errorf("unsupported MCP server transport %q", entry.Type)
		}
		if strings.TrimSpace(entry.Name) == "" || strings.TrimSpace(entry.URL) == "" {
			return nil, errors.New("MCP server requires name and url")
		}
		headers := make(map[string]string, len(entry.Headers))
		for _, header := range entry.Headers {
			if strings.TrimSpace(header.Name) == "" {
				continue
			}
			headers[header.Name] = header.Value
			if strings.EqualFold(header.Name, "Authorization") {
				token := strings.TrimSpace(strings.TrimPrefix(header.Value, "Bearer "))
				bridge.recordSecret(token)
				bridge.recordSecret(header.Value)
			}
		}
		serverEntry := MCPHTTPServer{
			Name:    strings.TrimSpace(entry.Name),
			URL:     strings.TrimSpace(entry.URL),
			Headers: headers,
		}
		if err := bridge.verifyMCPHandshake(operationContext, serverEntry); err != nil {
			return nil, fmt.Errorf("connect MCP server %s: %w", serverEntry.Name, err)
		}
		prepared = append(prepared, serverEntry)
	}
	return prepared, nil
}

func (bridge *server) verifyMCPHandshake(operationContext context.Context, serverEntry MCPHTTPServer) error {
	handshakeContext, cancel := context.WithTimeout(operationContext, mcpHandshakeTimeout)
	defer cancel()

	sessionHeader, err := bridge.postMCPMessage(handshakeContext, serverEntry, "", map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]any{
				"name":    AgentInfoName,
				"version": bridge.version,
			},
		},
	}, true)
	if err != nil {
		return err
	}

	_, _ = bridge.postMCPMessage(handshakeContext, serverEntry, sessionHeader, map[string]any{
		"jsonrpc": "2.0",
		"method":  "notifications/initialized",
	}, false)

	if _, err := bridge.postMCPMessage(handshakeContext, serverEntry, sessionHeader, map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "tools/list",
		"params":  map[string]any{},
	}, true); err != nil {
		return err
	}
	return nil
}

func (bridge *server) postMCPMessage(
	operationContext context.Context,
	serverEntry MCPHTTPServer,
	mcpSessionID string,
	payload map[string]any,
	expectResult bool,
) (string, error) {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	httpRequest, err := http.NewRequestWithContext(operationContext, http.MethodPost, serverEntry.URL, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Accept", "application/json, text/event-stream")
	for headerName, headerValue := range serverEntry.Headers {
		httpRequest.Header.Set(headerName, headerValue)
	}
	if mcpSessionID != "" {
		httpRequest.Header.Set("Mcp-Session-Id", mcpSessionID)
	}

	httpResponse, err := bridge.httpClient.Do(httpRequest)
	if err != nil {
		return "", err
	}
	defer httpResponse.Body.Close()

	returnedSessionID := cmp.Or(httpResponse.Header.Get("Mcp-Session-Id"), mcpSessionID)
	responseBody, _ := io.ReadAll(io.LimitReader(httpResponse.Body, 1<<20))
	if httpResponse.StatusCode < 200 || httpResponse.StatusCode >= 300 {
		return returnedSessionID, fmt.Errorf("MCP HTTP status %d", httpResponse.StatusCode)
	}
	if !expectResult {
		return returnedSessionID, nil
	}
	var rpcEnvelope struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(responseBody, &rpcEnvelope) == nil && rpcEnvelope.Error != nil {
		return returnedSessionID, errors.New(rpcEnvelope.Error.Message)
	}
	return returnedSessionID, nil
}

func buildConfigOptions(session storedSession, models []discoveredModel) []map[string]any {
	currentMode := session.Mode
	if !slices.Contains([]string{ModeSandbox, ModeBypass, ModePlan}, currentMode) {
		currentMode = ModeSandbox
	}
	options := []map[string]any{
		{
			"id":           ModeOption,
			"name":         "Mode",
			"category":     "mode",
			"type":         "select",
			"currentValue": currentMode,
			"options": []map[string]any{
				{"value": ModeSandbox, "name": "Sandbox"},
				{"value": ModeBypass, "name": "Bypass sandbox"},
				{"value": ModePlan, "name": "Plan"},
			},
		},
	}

	modelChoices := slices.Clone(models)
	if session.ModelID != "" && !containsModelValue(modelChoices, session.ModelID) {
		modelChoices = append([]discoveredModel{{Value: session.ModelID, Name: session.ModelID}}, modelChoices...)
	}
	if len(modelChoices) > 0 {
		currentModel := session.ModelID
		if currentModel == "" || !containsModelValue(modelChoices, currentModel) {
			currentModel = modelChoices[0].Value
		}
		renderedModels := make([]map[string]any, 0, len(modelChoices))
		for _, item := range modelChoices {
			renderedModels = append(renderedModels, map[string]any{
				"value": item.Value,
				"name":  item.Name,
			})
		}
		options = append(options, map[string]any{
			"id":           ModelOption,
			"name":         "Model",
			"category":     "model",
			"type":         "select",
			"currentValue": currentModel,
			"options":      renderedModels,
		})
	}
	return options
}

func containsModelValue(models []discoveredModel, target string) bool {
	for _, item := range models {
		if item.Value == target {
			return true
		}
	}
	return false
}

func extractPromptText(blocks []json.RawMessage) string {
	var parts []string
	for _, rawBlock := range blocks {
		var decoded map[string]any
		if json.Unmarshal(rawBlock, &decoded) != nil {
			continue
		}
		blockType, _ := decoded["type"].(string)
		switch blockType {
		case "text":
			if text, ok := decoded["text"].(string); ok && text != "" {
				parts = append(parts, text)
			}
		case "resource":
			if resourceMap, ok := decoded["resource"].(map[string]any); ok {
				uri, _ := resourceMap["uri"].(string)
				body := pickStringParameter(resourceMap, "text", "content")
				if body != "" {
					if uri != "" {
						parts = append(parts, fmt.Sprintf("Resource (%s):\n%s", uri, body))
					} else {
						parts = append(parts, body)
					}
				}
			}
		case "resource_link":
			uri, _ := decoded["uri"].(string)
			label := pickStringParameter(decoded, "title", "name")
			if uri != "" {
				if label != "" && label != uri {
					parts = append(parts, fmt.Sprintf("Referenced resource: %s (%s)", label, uri))
				} else {
					parts = append(parts, "Referenced resource: "+uri)
				}
			}
		default:
			if text, ok := decoded["text"].(string); ok && text != "" {
				parts = append(parts, text)
			}
		}
	}
	return strings.Join(parts, "\n\n")
}

func (bridge *server) recordSecret(secret string) {
	trimmed := strings.TrimSpace(secret)
	if len(trimmed) < 4 {
		return
	}
	bridge.secretMutex.Lock()
	defer bridge.secretMutex.Unlock()
	if !slices.Contains(bridge.secrets, trimmed) {
		bridge.secrets = append(bridge.secrets, trimmed)
	}
}

func (bridge *server) redact(message string) string {
	redacted := protocol.RedactText(message)
	bridge.secretMutex.Lock()
	secrets := slices.Clone(bridge.secrets)
	bridge.secretMutex.Unlock()
	for _, secret := range secrets {
		if secret != "" {
			redacted = strings.ReplaceAll(redacted, secret, "[REDACTED]")
		}
	}
	return protocol.BoundedDiagnostic(redacted)
}

func (bridge *server) respondResult(requestID json.RawMessage, result any) error {
	return bridge.writeFrame(jsonRPCFrame{
		JSONRPC: "2.0",
		ID:      requestID,
		Result:  result,
	})
}

func (bridge *server) respondError(requestID json.RawMessage, code int, message string) error {
	return bridge.writeFrame(jsonRPCFrame{
		JSONRPC: "2.0",
		ID:      requestID,
		Error: &jsonRPCError{
			Code:    code,
			Message: bridge.redact(message),
		},
	})
}

func (bridge *server) sendNotification(method string, parameters any) error {
	encodedParameters, err := json.Marshal(parameters)
	if err != nil {
		return err
	}
	return bridge.writeFrame(jsonRPCFrame{
		JSONRPC: "2.0",
		Method:  method,
		Params:  encodedParameters,
	})
}

func (bridge *server) writeFrame(frame jsonRPCFrame) error {
	encodedBytes, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	encodedBytes = append(encodedBytes, '\n')
	bridge.writeMutex.Lock()
	defer bridge.writeMutex.Unlock()
	_, err = bridge.outputWriter.Write(encodedBytes)
	return err
}
