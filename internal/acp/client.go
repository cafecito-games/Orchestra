package acp

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

var (
	ErrUnsupportedVersion     = errors.New("acp adapter selected an unsupported protocol version")
	ErrMissingCapability      = errors.New("acp adapter lacks a required capability")
	ErrAuthenticationRequired = errors.New("acp adapter requires authentication on this compute node")
	ErrCancelled              = errors.New("acp prompt turn was cancelled")
	ErrCancelGraceExpired     = errors.New("acp adapter did not acknowledge cancellation within its grace period")
	ErrIncompleteTurn         = errors.New("acp prompt turn did not complete")
	ErrAdapterVersionMismatch = errors.New("acp adapter reported a version other than its pinned version")
)

var stopReasons = map[string]bool{
	"end_turn":          true,
	"max_tokens":        true,
	"max_turn_requests": true,
	"refusal":           true,
	"cancelled":         true,
}

type PermissionRequest struct {
	RunID      string
	SessionID  string
	ApprovalID string
	ToolCallID string
	Title      string
	Kind       string
	Options    []protocol.ApprovalOption
}

type PermissionDecision struct {
	OptionID string
}

type PermissionHandler func(requestContext context.Context, request PermissionRequest) (PermissionDecision, error)

type MCPServer struct {
	Name        string
	URL         string
	BearerToken string
}

type Options struct {
	RunID                string
	ClientName           string
	ClientVersion        string
	Secrets              []string
	Events               func(protocol.HarnessEvent)
	Permission           PermissionHandler
	ExpectedAgentVersion string
	RequestTimeout       time.Duration
	PermissionTimeout    time.Duration
	CancelGracePeriod    time.Duration
	Now                  func() time.Time
}

type ResumeRequest struct {
	SessionID string
	Prompt    string
}

type SessionRequest struct {
	WorkingDirectory string
	Prompt           string
	Resume           *ResumeRequest
	MCPServer        *MCPServer
	Configuration    []ConfigurationSelection
	BeforePrompt     func(operationContext context.Context) error
}

type Result struct {
	SessionID  string
	StopReason string
	Text       string
	AgentName  string
}

type Client struct {
	peer          *connection
	options       Options
	redaction     secretRedactor
	normalization *normalizer

	emitMutex sync.Mutex
	sequence  int64

	stateMutex     sync.Mutex
	sessionID      string
	resumed        bool
	loading        bool
	negotiated     *protocol.AcpAgentCapabilities
	closeSupported bool
	completed      bool
	lateReported   bool
	cancelling     chan struct{}
	cancelOnce     sync.Once
	approvalCount  atomic.Int64
	promptSent     atomic.Bool
}

func NewClient(stdout io.Reader, stdin io.WriteCloser, options Options) *Client {
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = DefaultRequestTimeout
	}
	if options.PermissionTimeout <= 0 {
		options.PermissionTimeout = DefaultPermissionTimeout
	}
	if options.CancelGracePeriod <= 0 {
		options.CancelGracePeriod = DefaultCancelGracePeriod
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	options.ClientName = cmp.Or(options.ClientName, "orchestra-slave")
	options.ClientVersion = cmp.Or(options.ClientVersion, protocol.EffectiveBuildVersion())
	client := &Client{
		options:       options,
		redaction:     newSecretRedactor(options.Secrets),
		normalization: newNormalizer(),
		cancelling:    make(chan struct{}),
	}
	client.peer = newConnection(stdout, stdin, client)
	return client
}

func (client *Client) Run(operationContext context.Context, request SessionRequest) (Result, error) {
	client.peer.start()
	result, err := client.run(operationContext, request)
	client.markCompleted()
	if err != nil {
		return Result{}, client.redactError(err)
	}
	result.Text = client.redaction.apply(result.Text)
	return result, nil
}

func (client *Client) Probe(operationContext context.Context) (protocol.AcpAgentCapabilities, error) {
	client.peer.start()
	client.markCompleted()
	if _, err := client.initialize(operationContext); err != nil {
		return protocol.AcpAgentCapabilities{}, client.redactError(err)
	}
	capabilities, _ := client.Negotiated()
	return capabilities, nil
}

func (client *Client) ProbeOption(operationContext context.Context, workingDirectory, optionID string) []string {
	client.stateMutex.Lock()
	negotiated := client.negotiated != nil
	client.stateMutex.Unlock()
	if !negotiated || optionID == "" || !filepath.IsAbs(workingDirectory) {
		return nil
	}
	requestContext, cancel := context.WithTimeout(operationContext, client.options.RequestTimeout)
	defer cancel()
	var response newSessionResponse
	if client.peer.call(requestContext, methodSessionNew, newSessionRequest{WorkingDirectory: workingDirectory, McpServers: []mcpServerHTTP{}}, &response) != nil || response.SessionID == "" {
		return nil
	}
	values := []string{}
	for _, option := range response.ConfigOptions {
		if option.ID == optionID {
			values = orderedOfferedValues(option.Options)
			break
		}
	}
	if client.closeSupported {
		_ = client.peer.call(requestContext, methodSessionClose, sessionReference{SessionID: response.SessionID}, nil)
	}
	return values
}

func (client *Client) Negotiated() (protocol.AcpAgentCapabilities, bool) {
	client.stateMutex.Lock()
	defer client.stateMutex.Unlock()
	if client.negotiated == nil {
		return protocol.AcpAgentCapabilities{}, false
	}
	return *client.negotiated, true
}

func (client *Client) PromptSent() bool {
	return client.promptSent.Load()
}

func (client *Client) Close() {
	client.peer.closeInput()
}

func (client *Client) Wait() {
	client.peer.wait()
}

func (client *Client) Session() (string, bool) {
	client.stateMutex.Lock()
	defer client.stateMutex.Unlock()
	return client.sessionID, client.resumed
}

func (client *Client) run(operationContext context.Context, request SessionRequest) (Result, error) {
	if !filepath.IsAbs(request.WorkingDirectory) {
		return Result{}, errors.New("acp session working directory must be absolute")
	}
	initialized, err := client.initialize(operationContext)
	if err != nil {
		return Result{}, err
	}
	servers := []mcpServerHTTP{}
	if request.MCPServer != nil {
		if !initialized.AgentCapabilities.McpCapabilities.HTTP {
			return Result{}, fmt.Errorf("%w: HTTP MCP servers are required for the Orchestra MCP server", ErrMissingCapability)
		}
		servers = append(servers, mcpServerHTTP{
			Type:    "http",
			Name:    request.MCPServer.Name,
			URL:     request.MCPServer.URL,
			Headers: []httpHeader{{Name: "Authorization", Value: "Bearer " + request.MCPServer.BearerToken}},
		})
	}
	sessionID, configuration, resumed, err := client.establishSession(operationContext, request, servers, initialized)
	if err != nil {
		return Result{}, err
	}
	if err := client.configure(operationContext, sessionID, configuration, request.Configuration); err != nil {
		return Result{}, err
	}
	if request.BeforePrompt != nil {
		if err := request.BeforePrompt(operationContext); err != nil {
			return Result{}, err
		}
	}
	if operationContext.Err() != nil {
		return Result{}, fmt.Errorf("%w before %s was sent", ErrCancelled, methodSessionPrompt)
	}
	promptText := request.Prompt
	if resumed && request.Resume.Prompt != "" {
		promptText = request.Resume.Prompt
	}
	response, err := client.prompt(operationContext, sessionID, promptText)
	if err != nil {
		return Result{}, err
	}
	client.stateMutex.Lock()
	finalText := client.normalization.finalText()
	client.stateMutex.Unlock()
	if initialized.AgentCapabilities.SessionCapabilities.Close != nil && !isNull(initialized.AgentCapabilities.SessionCapabilities.Close) {
		closeContext, cancel := context.WithTimeout(context.Background(), client.options.RequestTimeout)
		if err := client.peer.call(closeContext, methodSessionClose, sessionReference{SessionID: sessionID}, nil); err != nil {
			client.emit(warningEvent(WarningSessionCloseFailed, "session/close failed: "+err.Error()))
		}
		cancel()
	}
	agentName := ""
	if initialized.AgentInfo != nil {
		agentName = truncateBytes(initialized.AgentInfo.Name, eventIdentifierBytes)
	}
	return Result{SessionID: sessionID, StopReason: response.StopReason, Text: finalText, AgentName: agentName}, nil
}

func (client *Client) initialize(operationContext context.Context) (initializeResponse, error) {
	requestContext, cancel := context.WithTimeout(operationContext, client.options.RequestTimeout)
	defer cancel()
	request := initializeRequest{
		ProtocolVersion: ProtocolVersion,
		ClientInfo:      implementation{Name: client.options.ClientName, Version: client.options.ClientVersion},
	}
	var response initializeResponse
	if err := client.peer.call(requestContext, methodInitialize, request, &response); err != nil {
		return response, client.setupError(operationContext, methodInitialize, err)
	}
	if response.ProtocolVersion == nil {
		return response, fmt.Errorf("%w: initialize response has no protocolVersion", ErrProtocolViolation)
	}
	if *response.ProtocolVersion != ProtocolVersion {
		return response, fmt.Errorf("%w: adapter selected %d, Orchestra supports %d", ErrUnsupportedVersion, *response.ProtocolVersion, ProtocolVersion)
	}
	if expected := client.options.ExpectedAgentVersion; expected != "" && (response.AgentInfo == nil || response.AgentInfo.Version != expected) {
		return response, fmt.Errorf("%w %s", ErrAdapterVersionMismatch, expected)
	}
	negotiated := negotiatedCapabilities(response)
	closeCapability := response.AgentCapabilities.SessionCapabilities.Close
	client.stateMutex.Lock()
	client.negotiated = &negotiated
	client.closeSupported = closeCapability != nil && !isNull(closeCapability)
	client.stateMutex.Unlock()
	return response, nil
}

func negotiatedCapabilities(response initializeResponse) protocol.AcpAgentCapabilities {
	capabilities := response.AgentCapabilities
	resume := capabilities.SessionCapabilities.Resume
	negotiated := protocol.AcpAgentCapabilities{
		ProtocolVersion: ProtocolVersion,
		LoadSession:     capabilities.LoadSession,
		ResumeSession:   resume != nil && !isNull(resume),
		Prompt: protocol.AcpPromptCapabilities{
			Image:           capabilities.PromptCapabilities.Image,
			Audio:           capabilities.PromptCapabilities.Audio,
			EmbeddedContext: capabilities.PromptCapabilities.EmbeddedContext,
		},
		Mcp: protocol.AcpMcpCapabilities{
			HTTP: capabilities.McpCapabilities.HTTP,
			SSE:  capabilities.McpCapabilities.SSE,
		},
	}
	if response.AgentInfo != nil {
		if name := truncateBytes(response.AgentInfo.Name, protocol.ACPAdapterNameMaximumBytes); !protocol.LooksSecretLike(name) {
			negotiated.AdapterName = name
		}
		if protocol.IsNormalizedVersion(response.AgentInfo.Version) {
			negotiated.AdapterVersion = response.AgentInfo.Version
		}
	}
	return negotiated
}

func (client *Client) establishSession(operationContext context.Context, request SessionRequest, servers []mcpServerHTTP, initialized initializeResponse) (string, configurationState, bool, error) {
	if resume := request.Resume; resume != nil {
		if resume.SessionID == "" || len(resume.SessionID) > eventIdentifierBytes {
			return "", nil, false, errors.New("acp resume session identity is invalid")
		}
		capabilities := initialized.AgentCapabilities
		method := ""
		switch {
		case capabilities.SessionCapabilities.Resume != nil && !isNull(capabilities.SessionCapabilities.Resume):
			method = methodSessionResume
		case capabilities.LoadSession:
			method = methodSessionLoad
		}
		if method == "" {
			client.emit(warningEvent(WarningSessionNotResumed, "adapter negotiated neither session resume nor load; a new session received the durable context"))
		} else {
			configuration, err := client.resumeSession(operationContext, method, resume.SessionID, request.WorkingDirectory, servers, initialized.AuthMethods)
			if err == nil {
				return resume.SessionID, configuration, true, nil
			}
			if _, ok := errors.AsType[*ResponseError](err); !ok {
				return "", nil, false, err
			}
			client.emit(warningEvent(WarningSessionNotResumed, method+" was refused; a new session received the durable context"))
		}
	}
	sessionID, configuration, err := client.newSession(operationContext, request.WorkingDirectory, servers, initialized.AuthMethods)
	return sessionID, configuration, false, err
}

func (client *Client) resumeSession(operationContext context.Context, method, sessionID, workingDirectory string, servers []mcpServerHTTP, methods []authMethod) (configurationState, error) {
	requestContext, cancel := context.WithTimeout(operationContext, client.options.RequestTimeout)
	defer cancel()
	client.stateMutex.Lock()
	client.sessionID = sessionID
	client.loading = true
	client.stateMutex.Unlock()
	var response resumeSessionResponse
	err := client.peer.call(requestContext, method, resumeSessionRequest{SessionID: sessionID, WorkingDirectory: workingDirectory, McpServers: servers}, &response)
	client.stateMutex.Lock()
	client.loading = false
	client.resumed = err == nil
	if err != nil {
		client.sessionID = ""
	}
	client.stateMutex.Unlock()
	if responseError, ok := errors.AsType[*ResponseError](err); ok {
		if responseError.Code == CodeAuthenticationRequired {
			return nil, authenticationRequired(methods)
		}
		return nil, err
	}
	if err != nil {
		return nil, client.setupError(operationContext, method, err)
	}
	return newConfigurationState(response.ConfigOptions), nil
}

func authenticationRequired(methods []authMethod) error {
	names := make([]string, 0, len(methods))
	for _, method := range methods {
		names = append(names, truncateBytes(method.ID, eventIdentifierBytes))
	}
	return fmt.Errorf("%w (advertised methods: %s)", ErrAuthenticationRequired, truncateBytes(strings.Join(names, ", "), MaximumDiagnosticBytes))
}

func (client *Client) newSession(operationContext context.Context, workingDirectory string, servers []mcpServerHTTP, methods []authMethod) (string, configurationState, error) {
	requestContext, cancel := context.WithTimeout(operationContext, client.options.RequestTimeout)
	defer cancel()
	var response newSessionResponse
	err := client.peer.call(requestContext, methodSessionNew, newSessionRequest{WorkingDirectory: workingDirectory, McpServers: servers}, &response)
	if responseError, ok := errors.AsType[*ResponseError](err); ok && responseError.Code == CodeAuthenticationRequired {
		return "", nil, authenticationRequired(methods)
	}
	if err != nil {
		return "", nil, client.setupError(operationContext, methodSessionNew, err)
	}
	if response.SessionID == "" || len(response.SessionID) > eventIdentifierBytes {
		return "", nil, fmt.Errorf("%w: session/new returned an invalid sessionId", ErrProtocolViolation)
	}
	client.stateMutex.Lock()
	client.sessionID = response.SessionID
	client.stateMutex.Unlock()
	return response.SessionID, newConfigurationState(response.ConfigOptions), nil
}

func (client *Client) setupError(operationContext context.Context, method string, err error) error {
	if operationContext.Err() != nil {
		return fmt.Errorf("%w before %s completed", ErrCancelled, method)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %s did not respond within %s", ErrProtocolViolation, method, client.options.RequestTimeout)
	}
	return fmt.Errorf("%s failed: %w", method, err)
}

type promptOutcome struct {
	response promptResponse
	err      error
}

func (client *Client) prompt(operationContext context.Context, sessionID string, text string) (promptResponse, error) {
	outcome := make(chan promptOutcome, 1)
	client.promptSent.Store(true)
	go func() {
		var response promptResponse
		request := promptRequest{SessionID: sessionID, Prompt: []contentBlock{{Type: "text", Text: text}}}
		err := client.peer.callSettled(context.Background(), methodSessionPrompt, request, &response, client.markCompleted)
		outcome <- promptOutcome{response: response, err: err}
	}()

	var finished promptOutcome
	select {
	case finished = <-outcome:
	case <-operationContext.Done():
		client.requestCancellation(sessionID)
		timer := time.NewTimer(client.options.CancelGracePeriod)
		defer timer.Stop()
		select {
		case finished = <-outcome:
		case <-timer.C:
			return promptResponse{}, ErrCancelGraceExpired
		}
	}
	if finished.err != nil {
		if operationContext.Err() != nil {
			return promptResponse{}, fmt.Errorf("%w: %w", ErrCancelled, finished.err)
		}
		return promptResponse{}, fmt.Errorf("session/prompt failed: %w", finished.err)
	}
	response := finished.response
	if !stopReasons[response.StopReason] {
		failure := fmt.Errorf("%w: unknown stopReason %q", ErrProtocolViolation, truncateBytes(response.StopReason, eventIdentifierBytes))
		client.peer.fail(failure)
		return promptResponse{}, failure
	}
	client.emit(promptUsageEvent(response.Usage)...)
	if operationContext.Err() != nil {
		return promptResponse{}, ErrCancelled
	}
	if response.StopReason != "end_turn" {
		return promptResponse{}, fmt.Errorf("%w: stopReason %s", ErrIncompleteTurn, response.StopReason)
	}
	return response, nil
}

func (client *Client) markCompleted() {
	client.stateMutex.Lock()
	client.completed = true
	client.stateMutex.Unlock()
}

func (client *Client) requestCancellation(sessionID string) {
	client.cancelOnce.Do(func() {
		close(client.cancelling)
		notifyContext, cancel := context.WithTimeout(context.Background(), client.options.CancelGracePeriod)
		defer cancel()
		_ = client.peer.notify(notifyContext, methodSessionCancel, sessionReference{SessionID: sessionID})
	})
}

func (client *Client) handleNotification(method string, parameters json.RawMessage) error {
	if method == methodSessionUpdate {
		return client.sessionUpdate(parameters)
	}
	if strings.HasPrefix(method, "_") {
		return nil
	}
	client.emit(unknownEvent(method, "unsupported ACP notification"))
	return nil
}

func (client *Client) sessionUpdate(parameters json.RawMessage) error {
	var notification sessionNotification
	if err := decodeStrict(parameters, &notification); err != nil || notification.SessionID == "" || notification.Update == nil {
		return fmt.Errorf("%w: session/update is missing sessionId or update", ErrMalformedUpdate)
	}
	client.stateMutex.Lock()
	sessionID, completed := client.sessionID, client.completed
	if sessionID == "" || (client.loading && notification.SessionID == sessionID) {
		client.stateMutex.Unlock()
		return nil
	}
	if notification.SessionID != sessionID {
		client.stateMutex.Unlock()
		return fmt.Errorf("session/update names session %q, not the active session", truncateBytes(notification.SessionID, eventIdentifierBytes))
	}
	if completed {
		reportLate := !client.lateReported
		client.lateReported = true
		client.stateMutex.Unlock()
		if reportLate {
			client.emit(warningEvent(WarningUpdateAfterComplete, "adapter sent session updates after the prompt turn ended; they were ignored"))
		}
		return nil
	}
	events, err := client.normalization.sessionUpdate(notification.Update)
	client.stateMutex.Unlock()
	if err != nil {
		return err
	}
	client.emit(events...)
	return nil
}

func (client *Client) handleRequest(requestContext context.Context, method string, parameters json.RawMessage) (any, *ResponseError) {
	if method == methodRequestPermission {
		return client.requestPermission(requestContext, parameters), nil
	}
	return nil, &ResponseError{Code: CodeMethodNotFound, Message: "method not supported by Orchestra"}
}

var approvalOptionKinds = map[string]string{
	"allow_once":    "allow-once",
	"allow_always":  "allow-always",
	"reject_once":   "reject-once",
	"reject_always": "reject-always",
}

func cancelledPermission() requestPermissionResponse {
	return requestPermissionResponse{Outcome: permissionOutcome{Outcome: "cancelled"}}
}

func (client *Client) requestPermission(requestContext context.Context, parameters json.RawMessage) requestPermissionResponse {
	var request requestPermissionRequest
	if err := decodeStrict(parameters, &request); err != nil {
		client.emit(warningEvent(WarningPermissionMalformed, "permission request is malformed and was cancelled"))
		return cancelledPermission()
	}
	client.stateMutex.Lock()
	sessionID, completed, loading := client.sessionID, client.completed, client.loading
	client.stateMutex.Unlock()
	if sessionID == "" || request.SessionID != sessionID || completed || loading {
		client.emit(warningEvent(WarningPermissionMalformed, "permission request does not belong to the active prompt turn and was cancelled"))
		return cancelledPermission()
	}
	options, offered, valid := approvalOptions(request.Options)
	toolCallID := request.ToolCall.ToolCallID
	if !valid || toolCallID == "" || len(toolCallID) > eventIdentifierBytes {
		client.emit(warningEvent(WarningPermissionMalformed, "permission request has missing, duplicate, or invalid options and was cancelled"))
		return cancelledPermission()
	}

	approvalID := fmt.Sprintf("acp-permission-%d", client.approvalCount.Add(1))
	title := "Permission requested"
	if request.ToolCall.Title != nil && *request.ToolCall.Title != "" {
		title = truncateBytes(*request.ToolCall.Title, eventTitleBytes)
	}
	kind := "other"
	if request.ToolCall.Kind != nil {
		if mapped, known := toolCallKinds[*request.ToolCall.Kind]; known {
			kind = mapped
		}
	}
	client.emit(protocol.HarnessEvent{Type: "permission.requested", ApprovalID: approvalID, ToolCallID: toolCallID, Title: title, Options: options})

	status, selected := client.decidePermission(requestContext, PermissionRequest{
		RunID:      client.options.RunID,
		SessionID:  sessionID,
		ApprovalID: approvalID,
		ToolCallID: toolCallID,
		Title:      client.redaction.apply(title),
		Kind:       kind,
		Options:    client.redactOptions(options),
	}, offered)
	client.emit(protocol.HarnessEvent{Type: "permission.resolved", ApprovalID: approvalID, Status: status, SelectedOptionID: selected})
	if selected == "" {
		return fallbackPermission(options)
	}
	return requestPermissionResponse{Outcome: permissionOutcome{Outcome: "selected", OptionID: selected}}
}

func (client *Client) decidePermission(requestContext context.Context, request PermissionRequest, offered map[string]string) (string, string) {
	if client.options.Permission == nil {
		return "cancelled", ""
	}
	decisionContext, cancel := context.WithTimeout(requestContext, client.options.PermissionTimeout)
	defer cancel()

	type decisionOutcome struct {
		decision PermissionDecision
		err      error
	}
	outcome := make(chan decisionOutcome, 1)
	go func() {
		decision, err := client.options.Permission(decisionContext, request)
		outcome <- decisionOutcome{decision: decision, err: err}
	}()
	select {
	case result := <-outcome:
		if decisionContext.Err() != nil {
			return timeoutStatus(decisionContext), ""
		}
		kind, known := offered[result.decision.OptionID]
		if result.err != nil || result.decision.OptionID == "" || !known {
			return "cancelled", ""
		}
		if strings.HasPrefix(kind, "allow") {
			return "approved", result.decision.OptionID
		}
		return "rejected", result.decision.OptionID
	case <-decisionContext.Done():
		return timeoutStatus(decisionContext), ""
	case <-client.cancelling:
		return "cancelled", ""
	}
}

func timeoutStatus(requestContext context.Context) string {
	if errors.Is(requestContext.Err(), context.DeadlineExceeded) {
		return "expired"
	}
	return "cancelled"
}

func fallbackPermission(options []protocol.ApprovalOption) requestPermissionResponse {
	for _, option := range options {
		if option.Kind == "reject-once" {
			return requestPermissionResponse{Outcome: permissionOutcome{Outcome: "selected", OptionID: option.ID}}
		}
	}
	return cancelledPermission()
}

func approvalOptions(received []permissionOption) ([]protocol.ApprovalOption, map[string]string, bool) {
	if len(received) == 0 || len(received) > eventOptionLimit {
		return nil, nil, false
	}
	options := make([]protocol.ApprovalOption, 0, len(received))
	offered := make(map[string]string, len(received))
	for _, option := range received {
		kind, known := approvalOptionKinds[option.Kind]
		if !known || option.OptionID == "" || len(option.OptionID) > eventIdentifierBytes {
			return nil, nil, false
		}
		if _, duplicate := offered[option.OptionID]; duplicate {
			return nil, nil, false
		}
		offered[option.OptionID] = kind
		options = append(options, protocol.ApprovalOption{ID: option.OptionID, Label: truncateBytes(option.Name, eventTitleBytes), Kind: kind})
	}
	return options, offered, true
}

func (client *Client) redactOptions(options []protocol.ApprovalOption) []protocol.ApprovalOption {
	redacted := make([]protocol.ApprovalOption, len(options))
	for index, option := range options {
		redacted[index] = protocol.ApprovalOption{ID: option.ID, Label: client.redaction.apply(option.Label), Kind: option.Kind}
	}
	return redacted
}

func (client *Client) emit(events ...protocol.HarnessEvent) {
	if len(events) == 0 {
		return
	}
	client.emitMutex.Lock()
	defer client.emitMutex.Unlock()
	for _, event := range events {
		event = client.redactEvent(event)
		client.sequence++
		event.RunID = client.options.RunID
		event.Sequence = client.sequence
		event.At = client.options.Now().UTC().Format(time.RFC3339Nano)
		if err := event.Validate(); err != nil {
			event = protocol.HarnessEvent{
				Type:     "warning",
				RunID:    event.RunID,
				Sequence: event.Sequence,
				At:       event.At,
				Code:     WarningEventInvalid,
				Message:  truncateBytes(client.redaction.apply(err.Error()), MaximumDiagnosticBytes),
			}
		}
		if client.options.Events != nil {
			client.options.Events(event)
		}
	}
}

func (client *Client) redactEvent(event protocol.HarnessEvent) protocol.HarnessEvent {
	apply := client.redaction.apply
	for _, field := range []*string{&event.Text, &event.Title, &event.Detail, &event.Path, &event.OldText, &event.NewText, &event.Message, &event.ToolCallID, &event.SourceType} {
		*field = apply(*field)
	}
	if event.Entries != nil {
		entries := make([]protocol.PlanEntry, len(event.Entries))
		for index, entry := range event.Entries {
			entry.Content = apply(entry.Content)
			entries[index] = entry
		}
		event.Entries = entries
	}
	if event.Options != nil {
		event.Options = client.redactOptions(event.Options)
	}
	return event
}

type redactedError struct {
	message string
	cause   error
}

func (err *redactedError) Error() string { return err.message }
func (err *redactedError) Unwrap() error { return err.cause }

func (client *Client) redactError(err error) error {
	return &redactedError{message: truncateBytes(client.redaction.apply(err.Error()), MaximumDiagnosticBytes), cause: err}
}

func (client *Client) Redact(value string) string {
	return client.redaction.apply(value)
}

func (client *Client) Warn(code, message string) {
	client.emit(warningEvent(code, message))
}

func isNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}
