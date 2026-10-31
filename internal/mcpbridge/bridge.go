// Package mcpbridge implements the `orchestra mcp` stdio Model Context Protocol
// bridge connecting local coding agents to the Orchestra master daemon.
package mcpbridge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/cafecito-games/Orchestra/internal/localstate"
	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/slave"
)

const (
	maximumLineBytes = 4 * 1024 * 1024
	rpcCallTimeout   = 30 * time.Second
)

type Configuration struct {
	Endpoint           string
	Token              string
	ClientID           string
	MailName           string
	MailHost           string
	Workspace          string
	ProcessID          int
	HTTPClient         *http.Client
	ConnectionFilePath string
	EndpointExplicit   bool
	TokenExplicit      bool
	TLSExplicit        bool
}

func ParseConfiguration(arguments []string, lookupEnvironment func(string) (string, bool)) (Configuration, error) {
	defaultConnectionFile := localstate.ResolveConnectionFilePath(lookupEnvironment)
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}

	environmentEndpoint := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_ENDPOINT", "CONTROL_ENDPOINT")
	environmentToken := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_TOKEN")
	environmentTLSCACert := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_TLS_CA")
	environmentTLSInsecureRaw := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_TLS_INSECURE")

	discoveredState, hasDiscoveredState, _ := localstate.ReadConnectionState(defaultConnectionFile)

	defaultEndpoint := environmentEndpoint
	if defaultEndpoint == "" && hasDiscoveredState && discoveredState.Endpoint != "" {
		defaultEndpoint = discoveredState.Endpoint
	}
	if defaultEndpoint == "" {
		defaultEndpoint = slave.DefaultEndpoint
	}

	defaultToken := environmentToken
	if defaultToken == "" && hasDiscoveredState && discoveredState.Token != "" {
		defaultToken = discoveredState.Token
	}

	defaultClientID := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_CLIENT_ID")
	if defaultClientID == "" {
		defaultClientID = protocol.NewIdentifier("mcp-client")
	}
	defaultMailName := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_MAIL_NAME", "ORCHESTRA_AGENT_NAME")
	defaultMailHost := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_MAIL_HOST")

	defaultTLSCACert := environmentTLSCACert
	if defaultTLSCACert == "" && hasDiscoveredState && discoveredState.TLSCACert != "" {
		defaultTLSCACert = discoveredState.TLSCACert
	}
	defaultTLSInsecure := environmentTLSInsecureRaw == "1" || strings.EqualFold(environmentTLSInsecureRaw, "true")
	if environmentTLSInsecureRaw == "" && hasDiscoveredState {
		defaultTLSInsecure = discoveredState.TLSInsecure
	}

	flagSet := flag.NewFlagSet("orchestra mcp", flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)

	var configuration Configuration
	var tlsCACert string
	var tlsInsecure bool
	flagSet.StringVar(&configuration.Endpoint, "endpoint", defaultEndpoint, "Orchestra master HTTP or WebSocket endpoint")
	flagSet.StringVar(&configuration.Endpoint, "control-endpoint", defaultEndpoint, "Orchestra master HTTP or WebSocket endpoint")
	flagSet.StringVar(&configuration.Token, "token", defaultToken, "Bearer token matching ORCHESTRA_TOKEN")
	flagSet.StringVar(&configuration.ConnectionFilePath, "connection-file", defaultConnectionFile, "Local connection state file path")
	flagSet.StringVar(&configuration.ClientID, "client-id", defaultClientID, "Stable orchestrator client identifier")
	flagSet.StringVar(&configuration.MailName, "name", defaultMailName, "Optional agent mail handle (for example \"auth-api\")")
	flagSet.StringVar(&configuration.MailHost, "host", defaultMailHost, "Optional agent mail host override (defaults to local hostname)")
	flagSet.StringVar(&tlsCACert, "tls-ca", defaultTLSCACert, "Optional CA certificate PEM path for wss:// / https://")
	flagSet.BoolVar(&tlsInsecure, "tls-insecure", defaultTLSInsecure, "Skip TLS certificate verification")

	if err := flagSet.Parse(arguments); err != nil {
		return Configuration{}, err
	}
	if flagSet.NArg() > 0 {
		return Configuration{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flagSet.Args(), " "))
	}

	configuration.EndpointExplicit = environmentEndpoint != ""
	configuration.TokenExplicit = environmentToken != ""
	configuration.TLSExplicit = environmentTLSCACert != "" || environmentTLSInsecureRaw != ""
	flagSet.Visit(func(visited *flag.Flag) {
		switch visited.Name {
		case "endpoint", "control-endpoint":
			configuration.EndpointExplicit = true
		case "token":
			configuration.TokenExplicit = true
		case "tls-ca", "tls-insecure":
			configuration.TLSExplicit = true
		}
	})

	if !protocol.IsIdentifier(configuration.ClientID) {
		return Configuration{}, fmt.Errorf("invalid --client-id %q", configuration.ClientID)
	}
	if strings.TrimSpace(configuration.MailHost) != "" {
		if _, ok := protocol.NormalizeMailHost(configuration.MailHost); !ok {
			return Configuration{}, fmt.Errorf("invalid --host %q", configuration.MailHost)
		}
	}
	if strings.TrimSpace(configuration.MailName) != "" {
		hostForValidation := configuration.MailHost
		if strings.TrimSpace(hostForValidation) == "" {
			hostForValidation = "local"
		}
		if _, _, _, err := protocol.NormalizeRenameTarget(configuration.MailName, hostForValidation); err != nil {
			return Configuration{}, fmt.Errorf("invalid --name %q: %w", configuration.MailName, err)
		}
	}
	if tlsCACert != "" || tlsInsecure {
		httpClient, tlsErr := protocol.BuildHTTPClient(tlsCACert, tlsInsecure, 0)
		if tlsErr != nil {
			return Configuration{}, tlsErr
		}
		configuration.HTTPClient = httpClient
	}
	return configuration, nil
}

type Bridge struct {
	configuration Configuration
	sessionID     string

	mcpConnectionMutex sync.RWMutex
	mcpConnection      mcp.Connection

	connectionMutex  sync.Mutex
	connection       *websocket.Conn
	connected        chan struct{}
	activeEndpoint   string
	activeToken      string
	activeHTTPClient *http.Client

	requestSequence atomic.Int64
	pendingMutex    sync.Mutex
	pendingCalls    map[string]chan protocol.OrchestratorServerMessage

	attachedMutex   sync.Mutex
	attachedThreads map[string]bool

	mailMutex        sync.RWMutex
	mailRegistration protocol.ClientMailRegistration
	mailIdentified   bool
	mailbox          *protocol.AgentMailbox
}

func NewBridge(configuration Configuration) *Bridge {
	if configuration.Endpoint == "" {
		configuration.Endpoint = slave.DefaultEndpoint
	}
	if configuration.ClientID == "" {
		configuration.ClientID = protocol.NewIdentifier("mcp-client")
	}
	host := strings.TrimSpace(configuration.MailHost)
	if host == "" {
		if hostname, err := os.Hostname(); err == nil && strings.TrimSpace(hostname) != "" {
			host = protocol.NormalizeHostnameSlug(hostname)
		} else {
			host = "local"
		}
	} else {
		host = protocol.NormalizeHostnameSlug(host)
	}
	workspace := strings.TrimSpace(configuration.Workspace)
	if workspace == "" {
		if currentDirectory, err := os.Getwd(); err == nil {
			workspace = currentDirectory
		}
	}
	processID := configuration.ProcessID
	if processID <= 0 {
		processID = os.Getppid()
	}
	return &Bridge{
		configuration:   configuration,
		sessionID:       protocol.NewIdentifier("mcp-session"),
		connected:       make(chan struct{}),
		pendingCalls:    map[string]chan protocol.OrchestratorServerMessage{},
		attachedThreads: map[string]bool{},
		mailRegistration: protocol.ClientMailRegistration{
			RequestedName: strings.TrimSpace(configuration.MailName),
			Host:          host,
			Workspace:     workspace,
			PushCapable:   true,
			ProcessID:     processID,
		},
	}
}

func (bridge *Bridge) Run(operationContext context.Context, input io.Reader, output io.Writer) error {
	bridgeContext, cancelBridge := context.WithCancel(operationContext)
	defer cancelBridge()

	connectionDone := make(chan struct{})
	go func() {
		defer close(connectionDone)
		bridge.maintainWebSocketLoop(bridgeContext)
	}()

	protocolServer := bridge.newProtocolServer()
	transport := &capturingTransport{
		bridge: bridge,
		delegate: &mcp.IOTransport{
			Reader:        closingReader{Reader: input},
			Writer:        noCloseWriter{Writer: output},
			MaxLineLength: maximumLineBytes,
		},
	}
	runErr := protocolServer.Run(bridgeContext, transport)
	cancelBridge()
	<-connectionDone
	if errors.Is(runErr, io.EOF) || errors.Is(runErr, context.Canceled) {
		return nil
	}
	return runErr
}

type closingReader struct {
	io.Reader
}

func (reader closingReader) Close() error {
	if closer, ok := reader.Reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

type noCloseWriter struct {
	io.Writer
}

func (writer noCloseWriter) Close() error { return nil }

type capturingTransport struct {
	bridge   *Bridge
	delegate mcp.Transport
}

func (transport *capturingTransport) Connect(operationContext context.Context) (mcp.Connection, error) {
	connection, err := transport.delegate.Connect(operationContext)
	if err != nil {
		return nil, err
	}
	wrapped := &capturingConnection{
		bridge:   transport.bridge,
		delegate: connection,
	}
	transport.bridge.mcpConnectionMutex.Lock()
	transport.bridge.mcpConnection = wrapped
	transport.bridge.mcpConnectionMutex.Unlock()
	return wrapped, nil
}

type capturingConnection struct {
	bridge   *Bridge
	delegate mcp.Connection
}

func (connection *capturingConnection) Read(operationContext context.Context) (jsonrpc.Message, error) {
	message, err := connection.delegate.Read(operationContext)
	if err == nil {
		if incomingRequest, ok := message.(*jsonrpc.Request); ok && incomingRequest != nil {
			if clientName := extractMCPClientName(incomingRequest.Method, incomingRequest.Params); clientName != "" {
				connection.bridge.recordDiscoveredHarness(operationContext, clientName)
			}
		}
	}
	return message, err
}

func (connection *capturingConnection) Write(operationContext context.Context, message jsonrpc.Message) error {
	return connection.delegate.Write(operationContext, message)
}

func (connection *capturingConnection) Close() error {
	return connection.delegate.Close()
}

func (connection *capturingConnection) SessionID() string {
	return connection.delegate.SessionID()
}

func extractMCPClientName(method string, rawParameters json.RawMessage) string {
	if method != "initialize" && method != "server/discover" {
		return ""
	}
	if len(rawParameters) == 0 {
		return ""
	}
	var payload struct {
		ClientInfo *struct {
			Name string `json:"name"`
		} `json:"clientInfo"`
		Meta map[string]json.RawMessage `json:"_meta"`
	}
	if json.Unmarshal(rawParameters, &payload) != nil {
		return ""
	}
	if payload.ClientInfo != nil && strings.TrimSpace(payload.ClientInfo.Name) != "" {
		return strings.TrimSpace(payload.ClientInfo.Name)
	}
	if rawMetaClient, exists := payload.Meta["io.modelcontextprotocol/clientInfo"]; exists {
		var metaClient struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(rawMetaClient, &metaClient) == nil && strings.TrimSpace(metaClient.Name) != "" {
			return strings.TrimSpace(metaClient.Name)
		}
	}
	return ""
}

func (bridge *Bridge) recordDiscoveredHarness(operationContext context.Context, clientName string) {
	trimmed := strings.TrimSpace(clientName)
	if trimmed == "" {
		return
	}
	bridge.mailMutex.Lock()
	changed := bridge.mailRegistration.Harness != trimmed || !bridge.mailIdentified
	bridge.mailRegistration.Harness = trimmed
	bridge.mailIdentified = true
	bridge.mailMutex.Unlock()
	if changed {
		bridge.identifyMailbox(operationContext)
	}
}

const bridgeInstructions = "Orchestra connects coding agents across machines. " +
	"Every agent session participates in Orchestra Agent Mail (<handle>@<host>): inspect your address with get_mail_identity, " +
	"rename your handle with set_mail_identity, list active peer agents with list_mailboxes, send prompts or replies with send_mail, " +
	"and read or await incoming mail with read_mail and wait_for_mail."

func (bridge *Bridge) newProtocolServer() *mcp.Server {
	protocolServer := mcp.NewServer(
		&mcp.Implementation{Name: "orchestra", Version: protocol.EffectiveBuildVersion()},
		&mcp.ServerOptions{
			Instructions: bridgeInstructions,
			Capabilities: &mcp.ServerCapabilities{
				Tools:        &mcp.ToolCapabilities{},
				Experimental: map[string]any{"claude/channel": map[string]any{}},
			},
		},
	)
	protocolServer.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(operationContext context.Context, method string, request mcp.Request) (mcp.Result, error) {
			if method == "initialize" && request != nil {
				if initializeParameters, ok := request.GetParams().(*mcp.InitializeParams); ok && initializeParameters != nil && initializeParameters.ClientInfo != nil {
					bridge.recordDiscoveredHarness(operationContext, initializeParameters.ClientInfo.Name)
				}
			}
			return next(operationContext, method, request)
		}
	})
	for _, definition := range orchestrator.OrchestratorToolDefinitions() {
		protocolServer.AddTool(&mcp.Tool{
			Name:        definition.Name,
			Description: definition.Description,
			InputSchema: definition.InputSchema,
		}, func(operationContext context.Context, request *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			resultBytes, rpcErr := bridge.invokeTool(operationContext, definition.Name, request.Params.Arguments)
			if rpcErr != nil {
				errorText, _ := json.Marshal(rpcErr)
				return &mcp.CallToolResult{
					Content: []mcp.Content{&mcp.TextContent{Text: string(errorText)}},
					IsError: true,
				}, nil
			}
			var structured any
			if err := json.Unmarshal(resultBytes, &structured); err != nil {
				errorText, _ := json.Marshal(&protocol.HubRPCError{Code: "internal_error", Message: "malformed tool result"})
				return &mcp.CallToolResult{
					Content: []mcp.Content{&mcp.TextContent{Text: string(errorText)}},
					IsError: true,
				}, nil
			}
			return &mcp.CallToolResult{
				Content:           []mcp.Content{&mcp.TextContent{Text: string(resultBytes)}},
				StructuredContent: structured,
			}, nil
		})
	}
	return protocolServer
}

func (bridge *Bridge) invokeTool(
	operationContext context.Context,
	toolName string,
	rawArguments json.RawMessage,
) (json.RawMessage, *protocol.HubRPCError) {
	if toolName == "post_artifact" {
		return bridge.invokePostArtifact(operationContext, rawArguments)
	}

	resultBytes, rpcErr := bridge.callOverWebSocket(operationContext, toolName, rawArguments)
	if rpcErr != nil {
		return nil, rpcErr
	}

	switch toolName {
	case "attach_thread":
		var parsed struct {
			ThreadID string `json:"threadId"`
		}
		if json.Unmarshal(rawArguments, &parsed) == nil && parsed.ThreadID != "" {
			bridge.rememberAttachedThread(parsed.ThreadID, true)
		}
	case "detach_thread":
		var parsed struct {
			ThreadID string `json:"threadId"`
		}
		if json.Unmarshal(rawArguments, &parsed) == nil && parsed.ThreadID != "" {
			bridge.rememberAttachedThread(parsed.ThreadID, false)
		}
	case "create_thread":
		var parsed struct {
			Attach bool `json:"attach"`
		}
		if json.Unmarshal(rawArguments, &parsed) == nil && parsed.Attach {
			var created struct {
				Thread protocol.Thread `json:"thread"`
			}
			if json.Unmarshal(resultBytes, &created) == nil && created.Thread.ID != "" {
				bridge.rememberAttachedThread(created.Thread.ID, true)
			}
		}
	case "set_mail_identity":
		var parsedArguments struct {
			Address string `json:"address"`
		}
		if json.Unmarshal(rawArguments, &parsedArguments) == nil && strings.TrimSpace(parsedArguments.Address) == "" {
			var updated struct {
				Mailbox protocol.AgentMailbox `json:"mailbox"`
			}
			if json.Unmarshal(resultBytes, &updated) == nil && updated.Mailbox.Handle != "" {
				bridge.mailMutex.Lock()
				bridge.mailRegistration.RequestedName = updated.Mailbox.Handle
				mailboxCopy := updated.Mailbox
				bridge.mailbox = &mailboxCopy
				bridge.mailMutex.Unlock()
			}
		}
	}

	return resultBytes, nil
}

func (bridge *Bridge) invokePostArtifact(
	operationContext context.Context,
	rawArguments json.RawMessage,
) (json.RawMessage, *protocol.HubRPCError) {
	var argumentsMap map[string]any
	if len(rawArguments) > 0 {
		if err := json.Unmarshal(rawArguments, &argumentsMap); err != nil {
			return nil, &protocol.HubRPCError{Code: "invalid_request", Message: "malformed post_artifact arguments"}
		}
	} else {
		argumentsMap = map[string]any{}
	}

	filePathValue, _ := argumentsMap["filePath"].(string)
	filePathValue = strings.TrimSpace(filePathValue)
	if filePathValue == "" {
		return bridge.callOverWebSocket(operationContext, "post_artifact", rawArguments)
	}

	cleanPath, err := filepath.Abs(filePathValue)
	if err != nil {
		return nil, &protocol.HubRPCError{Code: "invalid_request", Message: err.Error()}
	}
	information, statErr := os.Lstat(cleanPath)
	if statErr != nil || !information.Mode().IsRegular() {
		return nil, &protocol.HubRPCError{Code: "invalid_request", Message: "filePath must name an existing regular file"}
	}
	if information.Size() > protocol.MaximumArtifactSizeBytes {
		return nil, &protocol.HubRPCError{Code: "invalid_request", Message: "artifact file exceeds 10 MiB limit"}
	}

	fileBytes, readErr := os.ReadFile(cleanPath)
	if readErr != nil {
		return nil, &protocol.HubRPCError{Code: "invalid_request", Message: readErr.Error()}
	}
	sum := sha256.Sum256(fileBytes)
	digest := hex.EncodeToString(sum[:])

	if name, _ := argumentsMap["name"].(string); strings.TrimSpace(name) == "" {
		argumentsMap["name"] = filepath.Base(cleanPath)
	}
	argumentsMap["sizeBytes"] = int64(len(fileBytes))
	argumentsMap["sha256"] = digest
	delete(argumentsMap, "filePath")

	forwardedArguments, err := json.Marshal(argumentsMap)
	if err != nil {
		return nil, &protocol.HubRPCError{Code: "internal_error", Message: err.Error()}
	}

	grantPayload, rpcErr := bridge.callOverWebSocket(operationContext, "post_artifact", forwardedArguments)
	if rpcErr != nil {
		return nil, rpcErr
	}

	var grantResult orchestrator.PostArtifactResult
	if err := json.Unmarshal(grantPayload, &grantResult); err != nil {
		return nil, &protocol.HubRPCError{Code: "internal_error", Message: "malformed post_artifact response"}
	}

	targetEndpoint, targetToken, httpClient := bridge.activeOrResolvedTarget()
	uploadURL, err := resolveHTTPURLFromEndpoint(targetEndpoint, grantResult.Upload.Path)
	if err != nil {
		return nil, &protocol.HubRPCError{Code: "internal_error", Message: err.Error()}
	}
	putRequest, err := http.NewRequestWithContext(operationContext, http.MethodPut, uploadURL, bytes.NewReader(fileBytes))
	if err != nil {
		return nil, &protocol.HubRPCError{Code: "internal_error", Message: err.Error()}
	}
	putRequest.ContentLength = int64(len(fileBytes))
	putRequest.Header.Set("Content-Type", "application/octet-stream")
	if targetToken != "" {
		putRequest.Header.Set("Authorization", "Bearer "+targetToken)
	}

	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	putResponse, err := httpClient.Do(putRequest)
	if err != nil {
		return nil, &protocol.HubRPCError{Code: "upload_failed", Message: err.Error(), Retryable: true}
	}
	responseBody, _ := io.ReadAll(io.LimitReader(putResponse.Body, 256*1024))
	putResponse.Body.Close()
	if putResponse.StatusCode < 200 || putResponse.StatusCode >= 300 {
		return nil, &protocol.HubRPCError{
			Code:    "upload_failed",
			Message: fmt.Sprintf("artifact upload returned HTTP %d", putResponse.StatusCode),
		}
	}
	return responseBody, nil
}

func (bridge *Bridge) callOverWebSocket(
	operationContext context.Context,
	toolName string,
	rawArguments json.RawMessage,
) (json.RawMessage, *protocol.HubRPCError) {
	waitConnectContext, cancelWait := context.WithTimeout(operationContext, 10*time.Second)
	connection, waitErr := bridge.awaitConnection(waitConnectContext)
	cancelWait()
	if waitErr != nil {
		return nil, &protocol.HubRPCError{
			Code:      "master_unavailable",
			Message:   "not connected to Orchestra master daemon",
			Retryable: true,
		}
	}

	requestID := fmt.Sprintf("mcp-req-%d", bridge.requestSequence.Add(1))
	responseChannel := make(chan protocol.OrchestratorServerMessage, 1)

	bridge.pendingMutex.Lock()
	bridge.pendingCalls[requestID] = responseChannel
	bridge.pendingMutex.Unlock()

	defer func() {
		bridge.pendingMutex.Lock()
		delete(bridge.pendingCalls, requestID)
		bridge.pendingMutex.Unlock()
	}()

	if len(rawArguments) == 0 {
		rawArguments = json.RawMessage(`{}`)
	}
	encodedCall, err := json.Marshal(protocol.OrchestratorClientMessage{
		Type:      "tool.call",
		RequestID: requestID,
		Tool:      toolName,
		Arguments: rawArguments,
		At:        time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, &protocol.HubRPCError{Code: "internal_error", Message: err.Error()}
	}

	writeContext, cancelWrite := context.WithTimeout(operationContext, 10*time.Second)
	writeErr := connection.Write(writeContext, websocket.MessageText, encodedCall)
	cancelWrite()
	if writeErr != nil {
		return nil, &protocol.HubRPCError{
			Code:      "master_unavailable",
			Message:   "failed sending tool call to Orchestra master",
			Retryable: true,
		}
	}

	callContext, cancelCall := context.WithTimeout(operationContext, rpcCallTimeout)
	defer cancelCall()

	select {
	case response := <-responseChannel:
		if response.Error != nil {
			return nil, response.Error
		}
		if len(response.Result) == 0 {
			return json.RawMessage(`{}`), nil
		}
		return response.Result, nil
	case <-callContext.Done():
		return nil, &protocol.HubRPCError{
			Code:      "timeout",
			Message:   "timed out waiting for Orchestra master response",
			Retryable: true,
		}
	}
}

func (bridge *Bridge) maintainWebSocketLoop(operationContext context.Context) {
	delay := 100 * time.Millisecond
	reconnecting := false

	for {
		if operationContext.Err() != nil {
			return
		}
		connectedOnce := bridge.runSingleWebSocketSession(operationContext, reconnecting)
		if operationContext.Err() != nil {
			return
		}
		if connectedOnce {
			delay = 100 * time.Millisecond
			reconnecting = true
		}
		timer := time.NewTimer(delay)
		select {
		case <-operationContext.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = min(delay*2, 3*time.Second)
	}
}

func (bridge *Bridge) runSingleWebSocketSession(operationContext context.Context, reattach bool) bool {
	targetEndpoint, targetToken, targetHTTPClient := bridge.resolveConnectionTarget()
	wsURL, err := orchestratorClientWebSocketURLFromEndpoint(targetEndpoint)
	if err != nil {
		return false
	}
	headers := http.Header{}
	if targetToken != "" {
		headers.Set("Authorization", "Bearer "+targetToken)
	}

	dialContext, cancelDial := context.WithTimeout(operationContext, 10*time.Second)
	connection, httpResponse, err := websocket.Dial(dialContext, wsURL, &websocket.DialOptions{
		HTTPHeader: headers,
		HTTPClient: targetHTTPClient,
	})
	cancelDial()
	if httpResponse != nil && httpResponse.Body != nil {
		_ = httpResponse.Body.Close()
	}
	if err != nil {
		return false
	}
	connection.SetReadLimit(16 * 1024 * 1024)

	defer func() {
		bridge.connectionMutex.Lock()
		if bridge.connection == connection {
			bridge.connection = nil
			bridge.activeEndpoint = ""
			bridge.activeToken = ""
			bridge.activeHTTPClient = nil
			bridge.connected = make(chan struct{})
		}
		bridge.connectionMutex.Unlock()
		bridge.failPendingCalls()
		_ = connection.Close(websocket.StatusNormalClosure, "closing")
	}()

	initialRegistration, wasIdentified := bridge.snapshotMailRegistration()
	registrationBytes, _ := json.Marshal(protocol.OrchestratorClientMessage{
		Type:            "register",
		ProtocolVersion: protocol.ProtocolVersion,
		ClientID:        bridge.configuration.ClientID,
		SessionID:       bridge.sessionID,
		Mail:            &initialRegistration,
		At:              time.Now().UTC().Format(time.RFC3339Nano),
	})
	writeContext, cancelWrite := context.WithTimeout(operationContext, 10*time.Second)
	writeErr := connection.Write(writeContext, websocket.MessageText, registrationBytes)
	cancelWrite()
	if writeErr != nil {
		return false
	}

	welcomeContext, cancelWelcome := context.WithTimeout(operationContext, 10*time.Second)
	_, welcomePayload, readErr := connection.Read(welcomeContext)
	cancelWelcome()
	if readErr != nil {
		return false
	}
	var welcome protocol.OrchestratorServerMessage
	if err := json.Unmarshal(welcomePayload, &welcome); err != nil || welcome.Type != "welcome" {
		return false
	}
	if welcome.Mailbox != nil {
		bridge.storeMailbox(*welcome.Mailbox)
	}

	bridge.connectionMutex.Lock()
	bridge.connection = connection
	bridge.activeEndpoint = targetEndpoint
	bridge.activeToken = targetToken
	bridge.activeHTTPClient = targetHTTPClient
	close(bridge.connected)
	bridge.connectionMutex.Unlock()

	if _, nowIdentified := bridge.snapshotMailRegistration(); nowIdentified && !wasIdentified {
		go bridge.identifyMailbox(operationContext)
	}
	if reattach {
		go bridge.reattachRememberedThreads(operationContext)
	}

	for {
		_, payload, err := connection.Read(operationContext)
		if err != nil {
			return true
		}
		var message protocol.OrchestratorServerMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			continue
		}
		switch message.Type {
		case "tool.result", "mail.identified":
			if message.Mailbox != nil {
				bridge.storeMailbox(*message.Mailbox)
			}
			if message.RequestID != "" {
				bridge.pendingMutex.Lock()
				channel, waiting := bridge.pendingCalls[message.RequestID]
				if waiting {
					delete(bridge.pendingCalls, message.RequestID)
				}
				bridge.pendingMutex.Unlock()
				if waiting {
					channel <- message
				}
			}
		case "doorbell":
			if message.Doorbell != nil {
				bridge.emitDoorbellNotification(*message.Doorbell)
			}
		case "mail.received":
			if message.Mail != nil {
				bridge.emitMailNotification(*message.Mail)
			}
		case "attachment.replaced":
			if message.ThreadID != "" {
				bridge.rememberAttachedThread(message.ThreadID, false)
			}
		}
	}
}

func (bridge *Bridge) Mailbox() *protocol.AgentMailbox {
	bridge.mailMutex.RLock()
	defer bridge.mailMutex.RUnlock()
	if bridge.mailbox == nil {
		return nil
	}
	copyValue := *bridge.mailbox
	return &copyValue
}

func (bridge *Bridge) storeMailbox(mailbox protocol.AgentMailbox) {
	bridge.mailMutex.Lock()
	copyValue := mailbox
	bridge.mailbox = &copyValue
	bridge.mailMutex.Unlock()
}

func (bridge *Bridge) snapshotMailRegistration() (protocol.ClientMailRegistration, bool) {
	bridge.mailMutex.RLock()
	defer bridge.mailMutex.RUnlock()
	return bridge.mailRegistration, bridge.mailIdentified
}

func (bridge *Bridge) identifyMailbox(operationContext context.Context) {
	waitContext, cancelWait := context.WithTimeout(operationContext, 2*time.Second)
	connection, err := bridge.awaitConnection(waitContext)
	cancelWait()
	if err != nil {
		return
	}

	registration, _ := bridge.snapshotMailRegistration()
	requestID := fmt.Sprintf("identify-%d", bridge.requestSequence.Add(1))
	responseChannel := make(chan protocol.OrchestratorServerMessage, 1)

	bridge.pendingMutex.Lock()
	bridge.pendingCalls[requestID] = responseChannel
	bridge.pendingMutex.Unlock()

	defer func() {
		bridge.pendingMutex.Lock()
		delete(bridge.pendingCalls, requestID)
		bridge.pendingMutex.Unlock()
	}()

	payloadBytes, err := json.Marshal(protocol.OrchestratorClientMessage{
		Type:      "mail.identify",
		RequestID: requestID,
		ClientID:  bridge.configuration.ClientID,
		SessionID: bridge.sessionID,
		Mail:      &registration,
		At:        time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return
	}

	writeContext, cancelWrite := context.WithTimeout(operationContext, 2*time.Second)
	writeErr := connection.Write(writeContext, websocket.MessageText, payloadBytes)
	cancelWrite()
	if writeErr != nil {
		return
	}

	awaitContext, cancelAwait := context.WithTimeout(operationContext, 2*time.Second)
	defer cancelAwait()
	select {
	case <-responseChannel:
	case <-awaitContext.Done():
	}
}

func (bridge *Bridge) emitMailNotification(notification protocol.MailNotification) {
	meta := map[string]string{
		"kind":        "agent_mail",
		"message_id":  notification.Message.ID,
		"from":        notification.Message.FromAddress,
		"to":          notification.MailboxAddress,
		"occurred_at": notification.OccurredAt,
	}
	if notification.Message.Subject != "" {
		meta["subject"] = notification.Message.Subject
	}
	if notification.Message.ReplyToID != "" {
		meta["reply_to_id"] = notification.Message.ReplyToID
	}
	if len(notification.Message.ArtifactIDs) > 0 {
		meta["artifact_ids"] = strings.Join(notification.Message.ArtifactIDs, ",")
	}

	var builder strings.Builder
	fmt.Fprintf(&builder,
		"[Orchestra Mail from %s -> %s | id=%s]",
		notification.Message.FromAddress,
		notification.MailboxAddress,
		notification.Message.ID,
	)
	if notification.Message.Subject != "" {
		builder.WriteString(" Subject: " + notification.Message.Subject)
	}
	builder.WriteByte('\n')
	builder.WriteString(notification.Message.Body)
	if !strings.HasSuffix(notification.Message.Body, "\n") {
		builder.WriteByte('\n')
	}
	fmt.Fprintf(&builder,
		"(Reply via `send_mail` with `to: %q` and `replyToId: %q`, or mark handled with `read_mail`.)",
		notification.Message.FromAddress,
		notification.Message.ID,
	)

	parameters, err := json.Marshal(map[string]any{
		"content": builder.String(),
		"meta":    meta,
	})
	if err != nil {
		return
	}
	bridge.mcpConnectionMutex.RLock()
	connection := bridge.mcpConnection
	bridge.mcpConnectionMutex.RUnlock()
	if connection == nil {
		return
	}
	writeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = connection.Write(writeContext, &jsonrpc.Request{
		Method: "notifications/claude/channel",
		Params: parameters,
	})
}

func (bridge *Bridge) emitDoorbellNotification(doorbell protocol.DoorbellNotification) {
	meta := map[string]string{
		"thread_id":        doorbell.ThreadID,
		"thread_title":     doorbell.ThreadTitle,
		"through_sequence": strconv.FormatInt(doorbell.ThroughSequence, 10),
		"urgent":           strconv.FormatBool(doorbell.Urgent),
		"occurred_at":      doorbell.OccurredAt,
	}
	if len(doorbell.ApprovalIDs) > 0 {
		meta["approval_ids"] = strings.Join(doorbell.ApprovalIDs, ",")
	}
	content := fmt.Sprintf("[Orchestra thread %s (%s)] %s", doorbell.ThreadTitle, doorbell.ThreadID, doorbell.Summary)
	parameters, err := json.Marshal(map[string]any{"content": content, "meta": meta})
	if err != nil {
		return
	}
	bridge.mcpConnectionMutex.RLock()
	connection := bridge.mcpConnection
	bridge.mcpConnectionMutex.RUnlock()
	if connection == nil {
		return
	}
	writeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = connection.Write(writeContext, &jsonrpc.Request{
		Method: "notifications/claude/channel",
		Params: parameters,
	})
}

func (bridge *Bridge) rememberAttachedThread(threadID string, attached bool) {
	bridge.attachedMutex.Lock()
	defer bridge.attachedMutex.Unlock()
	if attached {
		bridge.attachedThreads[threadID] = true
	} else {
		delete(bridge.attachedThreads, threadID)
	}
}

func (bridge *Bridge) reattachRememberedThreads(operationContext context.Context) {
	bridge.attachedMutex.Lock()
	threadIDs := make([]string, 0, len(bridge.attachedThreads))
	for threadID := range bridge.attachedThreads {
		threadIDs = append(threadIDs, threadID)
	}
	bridge.attachedMutex.Unlock()

	for _, threadID := range threadIDs {
		arguments, _ := json.Marshal(map[string]string{"threadId": threadID})
		_, _ = bridge.callOverWebSocket(operationContext, "attach_thread", arguments)
	}
}

func (bridge *Bridge) awaitConnection(operationContext context.Context) (*websocket.Conn, error) {
	for {
		bridge.connectionMutex.Lock()
		connection := bridge.connection
		readySignal := bridge.connected
		bridge.connectionMutex.Unlock()
		if connection != nil {
			return connection, nil
		}
		select {
		case <-readySignal:
		case <-operationContext.Done():
			return nil, operationContext.Err()
		}
	}
}

func (bridge *Bridge) failPendingCalls() {
	bridge.pendingMutex.Lock()
	channels := make([]chan protocol.OrchestratorServerMessage, 0, len(bridge.pendingCalls))
	for requestID, channel := range bridge.pendingCalls {
		delete(bridge.pendingCalls, requestID)
		channels = append(channels, channel)
	}
	bridge.pendingMutex.Unlock()

	for _, channel := range channels {
		channel <- protocol.OrchestratorServerMessage{
			Type: "tool.result",
			Error: &protocol.HubRPCError{
				Code:      "master_unavailable",
				Message:   "connection to Orchestra master dropped before call completed",
				Retryable: true,
			},
		}
	}
}

func (bridge *Bridge) resolveConnectionTarget() (string, string, *http.Client) {
	endpoint := bridge.configuration.Endpoint
	token := bridge.configuration.Token
	httpClient := bridge.configuration.HTTPClient

	if (!bridge.configuration.EndpointExplicit || !bridge.configuration.TokenExplicit || !bridge.configuration.TLSExplicit) &&
		strings.TrimSpace(bridge.configuration.ConnectionFilePath) != "" {
		discovered, found, err := localstate.ReadConnectionState(bridge.configuration.ConnectionFilePath)
		if err == nil && found {
			if !bridge.configuration.EndpointExplicit && discovered.Endpoint != "" {
				endpoint = discovered.Endpoint
			}
			if !bridge.configuration.TokenExplicit && (discovered.Token != "" || strings.EqualFold(discovered.AuthMode, "tailscale")) {
				token = discovered.Token
			}
			if !bridge.configuration.TLSExplicit && (discovered.TLSCACert != "" || discovered.TLSInsecure) {
				if discoveredClient, tlsErr := protocol.BuildHTTPClient(discovered.TLSCACert, discovered.TLSInsecure, 0); tlsErr == nil {
					httpClient = discoveredClient
				}
			}
		}
	}

	if strings.TrimSpace(endpoint) == "" {
		endpoint = slave.DefaultEndpoint
	}
	return endpoint, token, httpClient
}

func (bridge *Bridge) activeOrResolvedTarget() (string, string, *http.Client) {
	bridge.connectionMutex.Lock()
	activeEndpoint := bridge.activeEndpoint
	activeToken := bridge.activeToken
	activeHTTPClient := bridge.activeHTTPClient
	hasActiveConnection := bridge.connection != nil
	bridge.connectionMutex.Unlock()

	if hasActiveConnection && activeEndpoint != "" {
		return activeEndpoint, activeToken, activeHTTPClient
	}
	return bridge.resolveConnectionTarget()
}

func orchestratorClientWebSocketURLFromEndpoint(endpoint string) (string, error) {
	parsedURL, err := url.Parse(endpoint)
	if err != nil || parsedURL.Host == "" {
		return "", fmt.Errorf("invalid endpoint %q", endpoint)
	}
	switch parsedURL.Scheme {
	case "http":
		parsedURL.Scheme = "ws"
	case "https":
		parsedURL.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported endpoint scheme %q", parsedURL.Scheme)
	}
	parsedURL.Path = strings.TrimRight(parsedURL.Path, "/") + "/orchestrator-client"
	return parsedURL.String(), nil
}

func resolveHTTPURLFromEndpoint(endpoint string, relativeOrAbsolute string) (string, error) {
	parsedTarget, err := url.Parse(relativeOrAbsolute)
	if err != nil {
		return "", err
	}
	if parsedTarget.Scheme == "http" || parsedTarget.Scheme == "https" {
		return parsedTarget.String(), nil
	}
	parsedBase, err := url.Parse(endpoint)
	if err != nil || parsedBase.Host == "" {
		return "", fmt.Errorf("invalid endpoint %q", endpoint)
	}
	switch parsedBase.Scheme {
	case "ws":
		parsedBase.Scheme = "http"
	case "wss":
		parsedBase.Scheme = "https"
	case "http", "https":
	default:
		return "", fmt.Errorf("unsupported endpoint scheme %q", parsedBase.Scheme)
	}
	return parsedBase.ResolveReference(parsedTarget).String(), nil
}

func firstEnvironmentValue(lookup func(string) (string, bool), names ...string) string {
	for _, name := range names {
		if value, found := lookup(name); found && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
