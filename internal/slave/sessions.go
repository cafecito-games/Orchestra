package slave

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/mcpserver"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const (
	maximumRunBufferedEvents = 2048
	maximumRunBufferedBytes  = 8 * 1024 * 1024
	hubRPCTimeout            = 30 * time.Second
)

type pendingApprovalWaiter struct {
	approvalID string
	decided    chan protocol.ApprovalDecision
	closed     bool
}

type runSession struct {
	client    *Client
	run       protocol.Run
	resident  residentInstance
	cancel    context.CancelFunc
	cancelled atomic.Bool

	sequence atomic.Int64

	mutex                sync.Mutex
	providerSessionID    string
	sessionBindingID     string
	selectedTransport    string
	activeBindingEmitted bool
	pendingApprovals     map[string]*pendingApprovalWaiter
	bufferedDecisions    map[string]protocol.ApprovalDecision
	droppedEvents        int
	bufferedBytes        int
	bufferedEvents       int
}

func (client *Client) handleDispatch(message protocol.MasterControlMessage) {
	if message.Run == nil {
		return
	}
	run := *message.Run
	if !protocol.IsIdentifier(run.ID) {
		return
	}

	client.mutex.Lock()
	if _, alreadyRunning := client.sessions[run.ID]; alreadyRunning {
		client.mutex.Unlock()
		return
	}
	if client.completedRuns[run.ID] {
		client.mutex.Unlock()
		return
	}
	client.mutex.Unlock()

	resident, err := client.acquireResidentForRun(run, message.Instance, message.Allocation)
	if err != nil {
		client.sendOrQueue(protocol.SlaveControlMessage{
			Type:  "run.failed",
			RunID: run.ID,
			Error: new(protocol.BoundedDiagnostic(protocol.RedactText(err.Error()))),
			At:    client.nowUTC(),
		})
		return
	}

	runContext, cancelRun := context.WithCancel(client.rootContext)
	session := &runSession{
		client:            client,
		run:               run,
		resident:          resident,
		cancel:            cancelRun,
		pendingApprovals:  map[string]*pendingApprovalWaiter{},
		bufferedDecisions: map[string]protocol.ApprovalDecision{},
	}
	if message.SessionBinding != nil {
		session.sessionBindingID = message.SessionBinding.ID
	}

	client.mutex.Lock()
	if _, duplicate := client.sessions[run.ID]; duplicate || client.completedRuns[run.ID] {
		client.mutex.Unlock()
		cancelRun()
		client.releaseResidentRun(resident.instance.ID, resident.allocation.ID, run.ID)
		return
	}
	client.sessions[run.ID] = session
	client.mutex.Unlock()

	client.waitGroup.Go(func() {
		session.execute(runContext, message.SessionBinding, message.WorkspaceLease)
	})
}

func (client *Client) cancelRun(runID string) {
	client.mutex.Lock()
	session, exists := client.sessions[runID]
	client.mutex.Unlock()
	if !exists {
		return
	}
	session.cancelled.Store(true)
	session.cancelAllApprovals()
	session.cancel()
}

func (session *runSession) execute(
	runContext context.Context,
	dispatchedBinding *protocol.DispatchSessionBinding,
	dispatchedLease *protocol.WorkspaceLeaseGrant,
) {
	defer func() {
		session.cancelAllApprovals()
		session.cancel()
		session.client.finishSession(session.run.ID)
		session.client.releaseResidentRun(session.resident.instance.ID, session.resident.allocation.ID, session.run.ID)
		session.client.sendHeartbeat()
	}()

	workingDirectory := session.run.Workspace
	if workingDirectory == "" {
		workingDirectory = session.resident.allocation.Workspace
	}

	var activeLease *activeRunLease
	if dispatchedLease != nil {
		leaseHandle, provisionedPath, err := session.client.leases.provisionForRun(runContext, session.run, *dispatchedLease)
		if err != nil {
			session.emitTerminalFailure(err)
			return
		}
		activeLease = leaseHandle
		workingDirectory = provisionedPath
		defer session.client.leases.finishForRun(activeLease)
	} else {
		canonicalWorkspace, err := VerifyWorkspaceWithinRoots(session.client.node.WorkspaceRoots, workingDirectory)
		if err != nil {
			session.emitTerminalFailure(err)
			return
		}
		workingDirectory = canonicalWorkspace
	}

	if session.client.runner == nil {
		session.emitTerminalFailure(errors.New("harness runner is not configured on this slave node"))
		return
	}

	fallbackTransport := ""
	if session.run.FallbackTransport != nil {
		fallbackTransport = *session.run.FallbackTransport
	}
	if err := session.client.runner.AdmitModel(session.run.HarnessID, session.run.Model); err != nil {
		session.emitTerminalFailure(err)
		return
	}
	if err := session.client.runner.Admit(session.run.HarnessID, session.run.Transport, fallbackTransport); err != nil {
		session.emitTerminalFailure(err)
		return
	}

	effectiveTransport := session.run.Transport
	if effectiveTransport == "" {
		effectiveTransport = protocol.TransportNativeCLI
	}

	var resume *harness.SessionResume
	if dispatchedBinding != nil {
		if err := dispatchedBinding.Validate(); err != nil {
			session.emitTerminalFailure(err)
			return
		}
		if dispatchedBinding.Transport != "" && dispatchedBinding.Transport != effectiveTransport {
			session.emitTerminalFailure(errors.New(protocol.SessionResumeUnavailableReason))
			return
		}
		if err := session.client.runner.AdmitResume(session.run.HarnessID, effectiveTransport); err != nil {
			failedBinding := protocol.SessionBindingUpdate{
				BindingID:         dispatchedBinding.ID,
				ProviderSessionID: dispatchedBinding.ProviderSessionID,
				HarnessID:         session.run.HarnessID,
				Transport:         effectiveTransport,
				Status:            "failed",
			}
			session.client.sendOrQueue(protocol.SlaveControlMessage{
				Type:    "session.binding",
				RunID:   session.run.ID,
				Binding: &failedBinding,
				At:      session.client.nowUTC(),
			})
			session.emitTerminalFailure(errors.New(protocol.SessionResumeUnavailableReason))
			return
		}
		resume = &harness.SessionResume{
			ProviderSessionID: dispatchedBinding.ProviderSessionID,
			Prompt:            dispatchedBinding.ResumePrompt,
		}
	}

	var mcpGrant mcpserver.Configuration
	if session.client.mcpServer != nil {
		granted, err := session.client.mcpServer.Grant(session.run.ID, workingDirectory, session.resident.instance.Delegation.CanDelegate)
		if err != nil {
			session.emitTerminalFailure(err)
			return
		}
		mcpGrant = granted
		defer session.client.mcpServer.Revoke(mcpGrant.Token)
	}

	systemPrompt := ""
	if session.resident.instance.Purpose != nil && session.resident.instance.Purpose.Instructions != nil {
		systemPrompt = *session.resident.instance.Purpose.Instructions
	}

	var startedOnce sync.Once
	invocation := harness.Invocation{
		Run:               session.run,
		SystemPrompt:      systemPrompt,
		Workspace:         workingDirectory,
		MCP:               mcpGrant,
		FallbackTransport: fallbackTransport,
		Resume:            resume,
		Started: func(selection protocol.RunTransportSelection) {
			startedOnce.Do(func() {
				session.mutex.Lock()
				session.selectedTransport = selection.SelectedTransport
				providerSessionID := session.providerSessionID
				session.mutex.Unlock()
				selectionCopy := selection
				session.client.sendOrQueue(protocol.SlaveControlMessage{
					Type:              "run.started",
					RunID:             session.run.ID,
					Transport:         &selectionCopy,
					ProviderSessionID: providerSessionID,
					At:                session.client.nowUTC(),
				})
				session.client.sendHeartbeat()
			})
		},
		Session: func(established harness.EstablishedSession) {
			if !protocol.IsIdentifier(established.ProviderSessionID) || protocol.LooksSecretLike(established.ProviderSessionID) {
				return
			}
			session.mutex.Lock()
			session.activeBindingEmitted = true
			session.providerSessionID = established.ProviderSessionID
			if !established.Resumed || dispatchedBinding == nil || established.ProviderSessionID != dispatchedBinding.ProviderSessionID {
				session.sessionBindingID = ""
			}
			bindingID := session.sessionBindingID
			session.mutex.Unlock()

			bindingUpdate := protocol.SessionBindingUpdate{
				BindingID:         bindingID,
				ProviderSessionID: established.ProviderSessionID,
				HarnessID:         session.run.HarnessID,
				Transport:         protocol.TransportACP,
				Status:            "active",
			}
			session.client.sendOrQueue(protocol.SlaveControlMessage{
				Type:    "session.binding",
				RunID:   session.run.ID,
				Binding: &bindingUpdate,
				At:      session.client.nowUTC(),
			})
		},
		ProviderSession: func(providerSessionID string) {
			if !protocol.IsIdentifier(providerSessionID) || protocol.LooksSecretLike(providerSessionID) {
				return
			}
			session.mutex.Lock()
			alreadyEmitted := session.activeBindingEmitted
			session.activeBindingEmitted = true
			session.providerSessionID = providerSessionID
			if dispatchedBinding == nil || providerSessionID != dispatchedBinding.ProviderSessionID {
				session.sessionBindingID = ""
			}
			bindingID := session.sessionBindingID
			selectedTransport := session.selectedTransport
			if selectedTransport == "" {
				selectedTransport = effectiveTransport
			}
			session.mutex.Unlock()

			if !alreadyEmitted && selectedTransport == protocol.TransportNativeCLI {
				bindingUpdate := protocol.SessionBindingUpdate{
					BindingID:         bindingID,
					ProviderSessionID: providerSessionID,
					HarnessID:         session.run.HarnessID,
					Transport:         protocol.TransportNativeCLI,
					Status:            "active",
				}
				session.client.sendOrQueue(protocol.SlaveControlMessage{
					Type:    "session.binding",
					RunID:   session.run.ID,
					Binding: &bindingUpdate,
					At:      session.client.nowUTC(),
				})
			}
		},
		Output: func(chunk string) {
			if chunk == "" {
				return
			}
			redactedChunk := protocol.BoundedTextValue(protocol.RedactText(chunk), protocol.TextMaximumBytes)
			session.client.sendOrQueue(protocol.SlaveControlMessage{
				Type:  "run.output",
				RunID: session.run.ID,
				Chunk: &redactedChunk,
				At:    session.client.nowUTC(),
			})
		},
		Events: func(event protocol.HarnessEvent) {
			session.forwardHarnessEvent(event)
		},
		Permission: func(permissionContext context.Context, request acp.PermissionRequest) (acp.PermissionDecision, error) {
			return session.awaitApprovalDecision(permissionContext, request)
		},
	}

	output, executeErr := session.client.runner.Execute(runContext, invocation)
	session.flushDroppedEventsWarning()

	session.mutex.Lock()
	providerSessionID := session.providerSessionID
	selectedTransport := session.selectedTransport
	bindingID := session.sessionBindingID
	session.mutex.Unlock()

	if selectedTransport == "" {
		selectedTransport = effectiveTransport
	}
	wasCancelled := session.cancelled.Load() || errors.Is(executeErr, context.Canceled)
	if providerSessionID == "" && executeErr != nil && !wasCancelled && dispatchedBinding != nil && bindingID != "" {
		providerSessionID = dispatchedBinding.ProviderSessionID
	}
	if providerSessionID != "" && (selectedTransport == protocol.TransportACP || selectedTransport == protocol.TransportNativeCLI) {
		nextBindingStatus := "idle"
		if executeErr != nil && !wasCancelled {
			nextBindingStatus = "failed"
		}
		bindingUpdate := protocol.SessionBindingUpdate{
			BindingID:         bindingID,
			ProviderSessionID: providerSessionID,
			HarnessID:         session.run.HarnessID,
			Transport:         selectedTransport,
			Status:            nextBindingStatus,
		}
		session.client.sendOrQueue(protocol.SlaveControlMessage{
			Type:    "session.binding",
			RunID:   session.run.ID,
			Binding: &bindingUpdate,
			At:      session.client.nowUTC(),
		})
	}

	if wasCancelled {
		redactedOutput := protocol.BoundedTextValue(protocol.RedactText(output), protocol.TextMaximumBytes)
		session.client.sendOrQueue(protocol.SlaveControlMessage{
			Type:              "run.cancelled",
			RunID:             session.run.ID,
			Output:            &redactedOutput,
			ProviderSessionID: providerSessionID,
			At:                session.client.nowUTC(),
		})
		return
	}
	if executeErr != nil {
		session.emitTerminalFailureWithSession(executeErr, providerSessionID)
		return
	}

	redactedOutput := protocol.BoundedTextValue(protocol.RedactText(output), protocol.TextMaximumBytes)
	session.client.sendOrQueue(protocol.SlaveControlMessage{
		Type:              "run.completed",
		RunID:             session.run.ID,
		Output:            &redactedOutput,
		ProviderSessionID: providerSessionID,
		At:                session.client.nowUTC(),
	})
}

func (session *runSession) emitTerminalFailure(err error) {
	session.emitTerminalFailureWithSession(err, "")
}

func (session *runSession) emitTerminalFailureWithSession(err error, providerSessionID string) {
	if session.cancelled.Load() || errors.Is(err, context.Canceled) {
		empty := ""
		session.client.sendOrQueue(protocol.SlaveControlMessage{
			Type:              "run.cancelled",
			RunID:             session.run.ID,
			Output:            &empty,
			ProviderSessionID: providerSessionID,
			At:                session.client.nowUTC(),
		})
		return
	}
	session.client.sendOrQueue(protocol.SlaveControlMessage{
		Type:              "run.failed",
		RunID:             session.run.ID,
		Error:             new(protocol.BoundedDiagnostic(protocol.RedactText(err.Error()))),
		ProviderSessionID: providerSessionID,
		At:                session.client.nowUTC(),
	})
}

func (session *runSession) scopedApprovalID(rawApprovalID string) string {
	if rawApprovalID == "" {
		return ""
	}
	prefix := session.run.ID + "-"
	if strings.HasPrefix(rawApprovalID, prefix) {
		return rawApprovalID
	}
	candidate := prefix + rawApprovalID
	if protocol.IsIdentifier(candidate) {
		return candidate
	}
	return rawApprovalID
}

func (session *runSession) forwardHarnessEvent(event protocol.HarnessEvent) {
	event.RunID = session.run.ID
	if event.At == "" {
		event.At = session.client.nowUTC()
	}
	if event.ApprovalID != "" {
		event.ApprovalID = session.scopedApprovalID(event.ApprovalID)
	}
	event.Text = protocol.RedactText(event.Text)
	event.Title = protocol.RedactText(event.Title)
	event.Detail = protocol.RedactText(event.Detail)
	event.Message = protocol.RedactText(event.Message)
	event.OldText = protocol.RedactText(event.OldText)
	event.NewText = protocol.RedactText(event.NewText)
	for index := range event.Entries {
		event.Entries[index].Content = protocol.RedactText(event.Entries[index].Content)
	}
	for index := range event.Options {
		event.Options[index].Label = protocol.RedactText(event.Options[index].Label)
	}

	isCritical := event.Type == "permission.requested" || event.Type == "permission.resolved"
	encodedBytes, err := json.Marshal(event)
	if err != nil {
		return
	}

	session.mutex.Lock()
	if !isCritical && !session.client.isConnected() &&
		(session.bufferedEvents >= maximumRunBufferedEvents || session.bufferedBytes+len(encodedBytes) > maximumRunBufferedBytes) {
		session.droppedEvents++
		session.mutex.Unlock()
		return
	}
	if !session.client.isConnected() {
		session.bufferedEvents++
		session.bufferedBytes += len(encodedBytes)
	} else {
		session.bufferedEvents = 0
		session.bufferedBytes = 0
	}
	dropped := 0
	if session.client.isConnected() && session.droppedEvents > 0 {
		dropped = session.droppedEvents
		session.droppedEvents = 0
	}
	session.mutex.Unlock()

	if dropped > 0 {
		warningSequence := session.sequence.Add(1)
		warningEvent := protocol.HarnessEvent{
			Type:     "warning",
			RunID:    session.run.ID,
			Sequence: warningSequence,
			At:       session.client.nowUTC(),
			Code:     "orchestra-events-dropped",
			Message:  fmt.Sprintf("%d harness events were dropped while the slave outbox was full", dropped),
		}
		if warningEvent.Validate() == nil {
			session.client.sendOrQueue(protocol.SlaveControlMessage{
				Type:  "harness.event",
				RunID: session.run.ID,
				Event: &warningEvent,
				At:    warningEvent.At,
			})
		}
	}

	event.Sequence = session.sequence.Add(1)
	if err := event.Validate(); err != nil {
		return
	}
	eventCopy := event
	session.client.sendOrQueue(protocol.SlaveControlMessage{
		Type:  "harness.event",
		RunID: session.run.ID,
		Event: &eventCopy,
		At:    event.At,
	})
}

func (session *runSession) flushDroppedEventsWarning() {
	session.mutex.Lock()
	dropped := session.droppedEvents
	session.droppedEvents = 0
	session.mutex.Unlock()
	if dropped <= 0 {
		return
	}
	warningEvent := protocol.HarnessEvent{
		Type:     "warning",
		RunID:    session.run.ID,
		Sequence: session.sequence.Add(1),
		At:       session.client.nowUTC(),
		Code:     "orchestra-events-dropped",
		Message:  fmt.Sprintf("%d harness events were dropped while the slave outbox was full", dropped),
	}
	if warningEvent.Validate() == nil {
		session.client.sendOrQueue(protocol.SlaveControlMessage{
			Type:  "harness.event",
			RunID: session.run.ID,
			Event: &warningEvent,
			At:    warningEvent.At,
		})
	}
}

func (session *runSession) awaitApprovalDecision(
	permissionContext context.Context,
	request acp.PermissionRequest,
) (acp.PermissionDecision, error) {
	approvalID := session.scopedApprovalID(request.ApprovalID)
	waiter := &pendingApprovalWaiter{
		approvalID: approvalID,
		decided:    make(chan protocol.ApprovalDecision, 1),
	}

	session.mutex.Lock()
	if buffered, exists := session.bufferedDecisions[approvalID]; exists {
		delete(session.bufferedDecisions, approvalID)
		session.mutex.Unlock()
		return translateApprovalDecision(buffered, request.Options)
	}
	session.pendingApprovals[approvalID] = waiter
	session.mutex.Unlock()

	defer func() {
		session.mutex.Lock()
		delete(session.pendingApprovals, approvalID)
		session.mutex.Unlock()
	}()

	select {
	case decision, ok := <-waiter.decided:
		if !ok {
			return acp.PermissionDecision{}, errors.New("approval cancelled")
		}
		return translateApprovalDecision(decision, request.Options)
	case <-permissionContext.Done():
		return acp.PermissionDecision{}, permissionContext.Err()
	}
}

func translateApprovalDecision(
	decision protocol.ApprovalDecision,
	options []protocol.ApprovalOption,
) (acp.PermissionDecision, error) {
	if decision.Status == "cancelled" || decision.Status == "expired" {
		return acp.PermissionDecision{}, fmt.Errorf("approval %s", decision.Status)
	}
	if decision.SelectedOptionID != "" {
		return acp.PermissionDecision{OptionID: decision.SelectedOptionID}, nil
	}
	if decision.Status == "rejected" {
		for _, option := range options {
			if option.Kind == "reject-once" || option.Kind == "reject-always" {
				return acp.PermissionDecision{OptionID: option.ID}, nil
			}
		}
	}
	return acp.PermissionDecision{}, errors.New("approval decision carried no option")
}

func (session *runSession) deliverApprovalDecision(decision protocol.ApprovalDecision) bool {
	session.mutex.Lock()
	waiter, waiting := session.pendingApprovals[decision.ApprovalID]
	if waiting && !waiter.closed {
		waiter.closed = true
		delete(session.pendingApprovals, decision.ApprovalID)
		session.mutex.Unlock()
		waiter.decided <- decision
		close(waiter.decided)
		return true
	}
	if !session.cancelled.Load() {
		session.bufferedDecisions[decision.ApprovalID] = decision
		session.mutex.Unlock()
		return true
	}
	session.mutex.Unlock()
	return false
}

func (session *runSession) cancelAllApprovals() {
	session.mutex.Lock()
	waiters := make([]*pendingApprovalWaiter, 0, len(session.pendingApprovals))
	for key, waiter := range session.pendingApprovals {
		delete(session.pendingApprovals, key)
		if !waiter.closed {
			waiter.closed = true
			waiters = append(waiters, waiter)
		}
	}
	session.mutex.Unlock()
	for _, waiter := range waiters {
		close(waiter.decided)
	}
}

func (client *Client) handleApprovalDecision(decision *protocol.ApprovalDecision) {
	if decision == nil {
		return
	}
	if err := decision.Validate(); err != nil {
		return
	}
	client.mutex.Lock()
	session, exists := client.sessions[decision.RunID]
	client.mutex.Unlock()
	if !exists || !session.deliverApprovalDecision(*decision) {
		errorMessage := "approval request is no longer awaiting a decision on this slave node"
		client.sendOrQueue(protocol.SlaveControlMessage{
			Type:       "approval.undeliverable",
			RunID:      decision.RunID,
			ApprovalID: decision.ApprovalID,
			Error:      &errorMessage,
			At:         client.nowUTC(),
		})
	}
}

func (client *Client) callHubRPC(
	operationContext context.Context,
	runID string,
	method string,
	arguments json.RawMessage,
) (json.RawMessage, error) {
	if !protocol.IsHubToolName(method) {
		return nil, &mcpserver.ToolError{
			Code:    "invalid_tool",
			Message: fmt.Sprintf("unknown hub tool %q", method),
		}
	}
	requestID := fmt.Sprintf("rpc-%s-%d", client.node.ID, client.rpcSequence.Add(1))
	responseChannel := make(chan protocol.MasterControlMessage, 1)

	client.rpcMutex.Lock()
	client.pendingRPC[requestID] = responseChannel
	client.rpcMutex.Unlock()

	defer func() {
		client.rpcMutex.Lock()
		delete(client.pendingRPC, requestID)
		client.rpcMutex.Unlock()
	}()

	if len(arguments) == 0 {
		arguments = json.RawMessage(`{}`)
	}
	if err := client.sendImmediate(operationContext, protocol.SlaveControlMessage{
		Type:      "hub.rpc.request",
		RequestID: requestID,
		RunID:     runID,
		Method:    method,
		Params:    slices.Clone(arguments),
		At:        client.nowUTC(),
	}); err != nil {
		return nil, &mcpserver.ToolError{
			Code:      "hub_unavailable",
			Message:   "orchestrator connection is currently unavailable",
			Retryable: true,
		}
	}

	timeout := hubRPCTimeout
	if method == "wait_for_task_events" {
		timeout = 35 * time.Second
	}
	waitContext, cancel := context.WithTimeout(operationContext, timeout)
	defer cancel()

	select {
	case response := <-responseChannel:
		if response.Error != nil {
			return nil, &mcpserver.ToolError{
				Code:      response.Error.Code,
				Message:   response.Error.Message,
				Retryable: response.Error.Retryable,
			}
		}
		if len(response.Result) == 0 {
			return json.RawMessage(`{}`), nil
		}
		return response.Result, nil
	case <-waitContext.Done():
		return nil, &mcpserver.ToolError{
			Code:      "hub_timeout",
			Message:   "timed out waiting for orchestrator hub RPC response",
			Retryable: true,
		}
	}
}

func (client *Client) handleHubRPCResponse(message protocol.MasterControlMessage) {
	if message.RequestID == "" {
		return
	}
	client.rpcMutex.Lock()
	responseChannel, waiting := client.pendingRPC[message.RequestID]
	if waiting {
		delete(client.pendingRPC, message.RequestID)
	}
	client.rpcMutex.Unlock()
	if waiting {
		responseChannel <- message
	}
}

func (client *Client) uploadArtifactContent(
	operationContext context.Context,
	uploadPath string,
	content io.Reader,
	size int64,
) error {
	targetURL, err := client.resolveHTTPURL(uploadPath)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(operationContext, http.MethodPut, targetURL, content)
	if err != nil {
		return err
	}
	request.ContentLength = size
	request.Header.Set("Content-Type", "application/octet-stream")
	if client.token != "" {
		request.Header.Set("Authorization", "Bearer "+client.token)
	}

	httpClient := client.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return &mcpserver.UploadError{Uncertain: true}
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return nil
	}
	return &mcpserver.UploadError{StatusCode: response.StatusCode}
}
