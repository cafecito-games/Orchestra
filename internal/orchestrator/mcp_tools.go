package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type MCPToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

func OrchestratorToolDefinitions() []MCPToolDefinition {
	return []MCPToolDefinition{
		{
			Name:        "get_execution_inventory",
			Description: "Inspect all enrolled slave machines, live connectivity, harnesses, models, transports, workspace roots, capabilities, and project profiles.",
			InputSchema: objectSchema(nil, nil),
		},
		{
			Name:        "create_thread",
			Description: "Create a durable orchestration thread for coordinating tasks, resident agent instances, messages, approvals, and artifacts.",
			InputSchema: objectSchema(map[string]any{
				"title":   map[string]any{"type": "string"},
				"summary": map[string]any{"type": "string"},
				"attach":  map[string]any{"type": "boolean"},
			}, []string{"title"}),
		},
		{
			Name:        "list_threads",
			Description: "List orchestration threads ordered by recent activity.",
			InputSchema: objectSchema(map[string]any{
				"status": map[string]any{"type": "string", "enum": protocol.ThreadStatuses},
				"limit":  map[string]any{"type": "integer", "minimum": 1, "maximum": 200},
			}, nil),
		},
		{
			Name:        "attach_thread",
			Description: "Attach the current orchestrator session to a thread to receive doorbell notifications and acknowledge events.",
			InputSchema: objectSchema(map[string]any{
				"threadId": map[string]any{"type": "string"},
			}, []string{"threadId"}),
		},
		{
			Name:        "detach_thread",
			Description: "Detach the current orchestrator session from a thread.",
			InputSchema: objectSchema(map[string]any{
				"threadId": map[string]any{"type": "string"},
			}, []string{"threadId"}),
		},
		{
			Name:        "get_thread_context",
			Description: "Retrieve full thread snapshot including task DAG, resident instances, runs, pending approvals, artifacts, and recent messages.",
			InputSchema: objectSchema(map[string]any{
				"threadId": map[string]any{"type": "string"},
			}, []string{"threadId"}),
		},
		{
			Name:        "get_thread_events",
			Description: "Read ordered per-thread journal events after an opaque cursor.",
			InputSchema: objectSchema(map[string]any{
				"threadId": map[string]any{"type": "string"},
				"cursor":   map[string]any{"type": "string"},
				"limit":    map[string]any{"type": "integer", "minimum": 1, "maximum": protocol.MaximumEventsPerWait},
				"types":    map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			}, []string{"threadId"}),
		},
		{
			Name:        "read_room",
			Description: "Read ordered entries from a thread's shared blackboard room, optionally filtered after a sequence or by message kinds.",
			InputSchema: objectSchema(map[string]any{
				"threadId":      map[string]any{"type": "string"},
				"afterSequence": map[string]any{"type": "integer", "minimum": 0},
				"kinds":         map[string]any{"type": "array", "items": map[string]any{"type": "string", "enum": protocol.RoomMessageKinds}},
				"limit":         map[string]any{"type": "integer", "minimum": 1, "maximum": protocol.MaximumRoomReadLimit},
			}, []string{"threadId"}),
		},
		{
			Name:        "post_room_message",
			Description: "Post an append-only blackboard entry to a thread's room, optionally nudging active tasks in that thread to read before finalizing.",
			InputSchema: objectSchema(map[string]any{
				"threadId":           map[string]any{"type": "string"},
				"idempotencyKey":     map[string]any{"type": "string"},
				"kind":               map[string]any{"type": "string", "enum": protocol.RoomMessageKinds},
				"topic":              map[string]any{"type": "string"},
				"body":               map[string]any{"type": "string"},
				"replyToSequence":    map[string]any{"type": "integer", "minimum": 1},
				"supersedesSequence": map[string]any{"type": "integer", "minimum": 1},
				"artifactIds":        map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"nudge":              map[string]any{"type": "boolean"},
				"nudgeTaskIds":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			}, []string{"threadId", "idempotencyKey", "kind", "body"}),
		},
		{
			Name:        "nudge_room",
			Description: "Require active tasks in a thread (or specified taskIds) to read through the room's current head sequence before completing.",
			InputSchema: objectSchema(map[string]any{
				"threadId":       map[string]any{"type": "string"},
				"idempotencyKey": map[string]any{"type": "string"},
				"taskIds":        map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			}, []string{"threadId", "idempotencyKey"}),
		},
		{
			Name:        "submit_tasks",
			Description: "Atomically and idempotently submit a task or DAG of tasks into a thread for capability-matched scheduling across slave nodes.",
			InputSchema: objectSchema(map[string]any{
				"threadId":       map[string]any{"type": "string"},
				"idempotencyKey": map[string]any{"type": "string"},
				"tasks": map[string]any{
					"type":     "array",
					"minItems": 1,
					"items":    map[string]any{"type": "object"},
				},
			}, []string{"threadId", "idempotencyKey", "tasks"}),
		},
		{
			Name:        "update_task",
			Description: "Update task status, summary, blockerReason, or attached artifactIds.",
			InputSchema: objectSchema(map[string]any{
				"taskId":         map[string]any{"type": "string"},
				"idempotencyKey": map[string]any{"type": "string"},
				"status":         map[string]any{"type": "string"},
				"summary":        map[string]any{"type": "string"},
				"blockerReason":  map[string]any{"type": "string"},
				"artifactIds":    map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			}, []string{"taskId"}),
		},
		{
			Name:        "send_task_message",
			Description: "Send a durable message to a task in a thread.",
			InputSchema: objectSchema(map[string]any{
				"threadId":       map[string]any{"type": "string"},
				"toTaskId":       map[string]any{"type": "string"},
				"replyToId":      map[string]any{"type": "string"},
				"idempotencyKey": map[string]any{"type": "string"},
				"kind":           map[string]any{"type": "string", "enum": protocol.TaskMessageKinds},
				"body":           map[string]any{"type": "string"},
				"artifactIds":    map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			}, []string{"toTaskId", "idempotencyKey", "kind", "body"}),
		},
		{
			Name:        "update_thread",
			Description: "Update a thread's title, summary, or status (open or resolved).",
			InputSchema: objectSchema(map[string]any{
				"threadId": map[string]any{"type": "string"},
				"title":    map[string]any{"type": "string"},
				"summary":  map[string]any{"type": "string"},
				"status":   map[string]any{"type": "string"},
			}, []string{"threadId"}),
		},
		{
			Name:        "post_artifact",
			Description: "Register a bounded artifact (up to 10 MiB) verified by SHA-256 and obtain an upload grant.",
			InputSchema: objectSchema(map[string]any{
				"threadId":     map[string]any{"type": "string"},
				"taskId":       map[string]any{"type": "string"},
				"name":         map[string]any{"type": "string"},
				"filePath":     map[string]any{"type": "string"},
				"relativePath": map[string]any{"type": "string"},
				"kind":         map[string]any{"type": "string", "enum": protocol.ArtifactKinds},
				"mediaType":    map[string]any{"type": "string"},
				"sizeBytes":    map[string]any{"type": "integer", "minimum": 0, "maximum": protocol.MaximumArtifactSizeBytes},
				"sha256":       map[string]any{"type": "string"},
				"summary":      map[string]any{"type": "string"},
			}, nil),
		},
		{
			Name:        "spawn_instance",
			Description: "Provision a resident agent instance on a capability-matched slave node.",
			InputSchema: objectSchema(map[string]any{
				"threadId":           map[string]any{"type": "string"},
				"idempotencyKey":     map[string]any{"type": "string"},
				"purpose":            map[string]any{"type": "object"},
				"delegation":         map[string]any{"type": "object"},
				"requirements":       map[string]any{"type": "object"},
				"idleTimeoutSeconds": map[string]any{"type": "integer"},
				"initialTask":        map[string]any{"type": "object"},
			}, []string{"threadId", "idempotencyKey"}),
		},
		{
			Name:        "get_instance",
			Description: "Inspect a resident agent instance, its node allocation, and active runs.",
			InputSchema: objectSchema(map[string]any{
				"instanceId": map[string]any{"type": "string"},
			}, []string{"instanceId"}),
		},
		{
			Name:        "renew_instance",
			Description: "Extend the idle lease of a resident agent instance.",
			InputSchema: objectSchema(map[string]any{
				"instanceId":         map[string]any{"type": "string"},
				"idempotencyKey":     map[string]any{"type": "string"},
				"idleTimeoutSeconds": map[string]any{"type": "integer"},
			}, []string{"instanceId", "idempotencyKey"}),
		},
		{
			Name:        "release_instance",
			Description: "Release a resident agent instance using drain or cancel mode.",
			InputSchema: objectSchema(map[string]any{
				"instanceId":     map[string]any{"type": "string"},
				"idempotencyKey": map[string]any{"type": "string"},
				"mode":           map[string]any{"type": "string", "enum": protocol.InstanceReleaseModes},
			}, []string{"instanceId", "idempotencyKey"}),
		},
		{
			Name:        "list_approvals",
			Description: "List permission approval requests across worker runs.",
			InputSchema: objectSchema(map[string]any{
				"threadId": map[string]any{"type": "string"},
				"status":   map[string]any{"type": "string", "enum": protocol.ApprovalStatuses},
			}, nil),
		},
		{
			Name:        "resolve_approval",
			Description: "Approve or reject a pending worker permission request.",
			InputSchema: objectSchema(map[string]any{
				"approvalId": map[string]any{"type": "string"},
				"status":     map[string]any{"type": "string", "enum": []string{"approved", "rejected", "cancelled"}},
				"optionId":   map[string]any{"type": "string"},
			}, []string{"approvalId"}),
		},
		{
			Name:        "get_mail_identity",
			Description: "Inspect this agent session's Orchestra mail address (<handle>@<host>), aliases, status, and unread message count.",
			InputSchema: objectSchema(nil, nil),
		},
		{
			Name:        "set_mail_identity",
			Description: "Rename this agent's mail handle (for example \"auth-api\" -> \"auth-api@fabrica\") or, if address is specified, rename another mailbox while keeping the old address as an alias.",
			InputSchema: objectSchema(map[string]any{
				"name":    map[string]any{"type": "string"},
				"address": map[string]any{"type": "string"},
			}, []string{"name"}),
		},
		{
			Name:        "list_mailboxes",
			Description: "List registered peer agent mailboxes (<handle>@<host>) across interactive MCP sessions and slave nodes.",
			InputSchema: objectSchema(map[string]any{
				"host":            map[string]any{"type": "string"},
				"onlineOnly":      map[string]any{"type": "boolean"},
				"includeReleased": map[string]any{"type": "boolean"},
				"workspace":       map[string]any{"type": "string"},
			}, nil),
		},
		{
			Name:        "send_mail",
			Description: "Send a peer-to-peer mail prompt or reply to another agent by <handle>@<host> (pushing/waking the target agent immediately when supported).",
			InputSchema: objectSchema(map[string]any{
				"idempotencyKey": map[string]any{"type": "string"},
				"to":             map[string]any{"type": "string"},
				"replyToId":      map[string]any{"type": "string"},
				"subject":        map[string]any{"type": "string"},
				"body":           map[string]any{"type": "string"},
				"artifactIds":    map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"wake":           map[string]any{"type": "boolean"},
			}, []string{"idempotencyKey", "body"}),
		},
		{
			Name:        "read_mail",
			Description: "Read messages in this agent's inbox or sent folder, optionally marking inbox messages read.",
			InputSchema: objectSchema(map[string]any{
				"address":       map[string]any{"type": "string"},
				"box":           map[string]any{"type": "string", "enum": []string{"inbox", "sent"}},
				"unreadOnly":    map[string]any{"type": "boolean"},
				"afterSequence": map[string]any{"type": "integer", "minimum": 0},
				"limit":         map[string]any{"type": "integer", "minimum": 1, "maximum": protocol.MaximumMailReadLimit},
				"markRead":      map[string]any{"type": "boolean"},
			}, nil),
		},
		{
			Name:        "wait_for_mail",
			Description: "Block up to timeoutMilliseconds waiting for unread mail to arrive in this agent's inbox.",
			InputSchema: objectSchema(map[string]any{
				"timeoutMilliseconds": map[string]any{"type": "integer", "minimum": 0, "maximum": protocol.MaximumWaitMilliseconds},
				"afterSequence":       map[string]any{"type": "integer", "minimum": 0},
				"limit":               map[string]any{"type": "integer", "minimum": 1, "maximum": protocol.MaximumMailReadLimit},
				"markRead":            map[string]any{"type": "boolean"},
			}, nil),
		},
	}
}

func (service *Service) newMCPHTTPHandler() (http.Handler, func()) {
	protocolServer := mcp.NewServer(
		&mcp.Implementation{Name: "orchestra", Version: protocol.BuildVersion},
		&mcp.ServerOptions{
			Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}},
		},
	)
	protocolServer.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(operationContext context.Context, method string, request mcp.Request) (mcp.Result, error) {
			result, err := next(operationContext, method, request)
			if method == "initialize" && err == nil && request != nil {
				sessionID := "http-session"
				if session := request.GetSession(); session != nil && protocol.IsIdentifier(session.ID()) {
					sessionID = session.ID()
				}
				clientID := sessionID
				requestedName := ""
				requestedHost := ""
				if extra := request.GetExtra(); extra != nil && extra.Header != nil {
					if headerClient := strings.TrimSpace(extra.Header.Get("X-Orchestra-Client-Id")); protocol.IsIdentifier(headerClient) {
						clientID = headerClient
					}
					requestedName = strings.TrimSpace(extra.Header.Get("X-Orchestra-Mail-Name"))
					requestedHost = strings.TrimSpace(extra.Header.Get("X-Orchestra-Mail-Host"))
				}
				harnessName := ""
				if initializeParameters, ok := request.GetParams().(*mcp.InitializeParams); ok && initializeParameters != nil && initializeParameters.ClientInfo != nil {
					harnessName = initializeParameters.ClientInfo.Name
				}
				_, _ = service.RegisterClientMailbox(clientID, sessionID, &protocol.ClientMailRegistration{
					RequestedName: requestedName,
					Host:          requestedHost,
					Harness:       harnessName,
				})
			}
			return result, err
		}
	})
	for _, definition := range OrchestratorToolDefinitions() {
		protocolServer.AddTool(&mcp.Tool{
			Name:        definition.Name,
			Description: definition.Description,
			InputSchema: definition.InputSchema,
		}, func(operationContext context.Context, request *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			caller := callerFromMCPRequest(request)
			resultBytes, err := service.ExecuteOrchestratorTool(
				operationContext,
				caller,
				definition.Name,
				request.Params.Arguments,
			)
			return orchestratorMCPResult(resultBytes, err), nil
		})
	}
	handler := mcp.NewStreamableHTTPHandler(
		func(*http.Request) *mcp.Server { return protocolServer },
		&mcp.StreamableHTTPOptions{
			JSONResponse:        true,
			MaxRequestBodyBytes: 4 << 20,
			SessionTimeout:      30 * time.Minute,
		},
	)
	closeSessions := func() {
		for session := range protocolServer.Sessions() {
			_ = session.Close()
		}
	}
	return handler, closeSessions
}

func callerFromMCPRequest(request *mcp.CallToolRequest) CallerIdentity {
	sessionID := "http-session"
	if request.Session != nil && protocol.IsIdentifier(request.Session.ID()) {
		sessionID = request.Session.ID()
	}
	clientID := ""
	if request.Extra != nil {
		clientID = strings.TrimSpace(request.Extra.Header.Get("X-Orchestra-Client-Id"))
	}
	if !protocol.IsIdentifier(clientID) {
		clientID = sessionID
	}
	return CallerIdentity{Kind: "orchestrator-client", ClientID: clientID, SessionID: sessionID}
}

func orchestratorMCPResult(resultBytes json.RawMessage, callErr error) *mcp.CallToolResult {
	if callErr != nil {
		errorJSON, _ := json.Marshal(toHubRPCError(callErr))
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(errorJSON)}},
			IsError: true,
		}
	}
	var structured any
	if err := json.Unmarshal(resultBytes, &structured); err != nil {
		errorJSON, _ := json.Marshal(toHubRPCError(fmt.Errorf("encode MCP tool result: %w", err)))
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(errorJSON)}},
			IsError: true,
		}
	}
	return &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: string(resultBytes)}},
		StructuredContent: structured,
	}
}

func objectSchema(properties map[string]any, required []string) map[string]any {
	if properties == nil {
		properties = map[string]any{}
	}
	schema := map[string]any{
		"type":       "object",
		"properties": properties,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}

func decodeToolArguments[Target any](raw json.RawMessage) (Target, error) {
	var target Target
	if len(raw) == 0 || string(raw) == "null" {
		return target, nil
	}
	if err := json.Unmarshal(raw, &target); err != nil {
		return target, invalidRequest("malformed tool arguments: " + err.Error())
	}
	return target, nil
}

func (service *Service) ExecuteOrchestratorTool(
	operationContext context.Context,
	caller CallerIdentity,
	toolName string,
	rawArguments json.RawMessage,
) (json.RawMessage, error) {
	if caller.Kind == "" {
		caller.Kind = "orchestrator-client"
	}

	var responseValue any
	var err error

	switch toolName {
	case "get_execution_inventory":
		responseValue, err = service.GetExecutionInventory()
	case "create_thread":
		input, decodeErr := decodeToolArguments[CreateThreadInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		thread, attachment, createErr := service.CreateThread(caller, input)
		if createErr != nil {
			return nil, createErr
		}
		responseValue = map[string]any{
			"thread":     thread,
			"attachment": attachment,
		}
	case "list_threads":
		input, decodeErr := decodeToolArguments[ListThreadsInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		threads, listErr := service.ListThreads(input)
		if listErr != nil {
			return nil, listErr
		}
		responseValue = map[string]any{"threads": threads}
	case "attach_thread":
		input, decodeErr := decodeToolArguments[AttachThreadInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		attachment, cursor, attachErr := service.AttachThread(caller, input)
		if attachErr != nil {
			return nil, attachErr
		}
		responseValue = map[string]any{
			"attachment": attachment,
			"cursor":     cursor,
		}
	case "detach_thread":
		input, decodeErr := decodeToolArguments[DetachThreadInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		detached, detachErr := service.DetachThread(caller, input)
		if detachErr != nil {
			return nil, detachErr
		}
		responseValue = map[string]any{
			"threadId": input.ThreadID,
			"detached": detached,
		}
	case "get_thread_context":
		input, decodeErr := decodeToolArguments[GetThreadContextInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.GetThreadContext(input.ThreadID)
	case "get_thread_events":
		input, decodeErr := decodeToolArguments[GetThreadEventsInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.GetThreadEvents(caller, input)
	case "read_room":
		input, decodeErr := decodeToolArguments[ReadRoomInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.ReadRoom(caller, input)
	case "post_room_message":
		input, decodeErr := decodeToolArguments[PostRoomMessageInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.PostRoomMessage(caller, input)
	case "nudge_room":
		input, decodeErr := decodeToolArguments[NudgeRoomInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.NudgeRoom(caller, input)
	case "submit_tasks":
		input, decodeErr := decodeToolArguments[SubmitTasksInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.SubmitTasks(caller, input)
	case "update_task":
		input, decodeErr := decodeToolArguments[UpdateTaskInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		task, updateErr := service.UpdateTask(caller, input)
		if updateErr != nil {
			return nil, updateErr
		}
		responseValue = map[string]any{"task": task}
	case "send_task_message":
		input, decodeErr := decodeToolArguments[SendTaskMessageInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.SendTaskMessage(caller, input)
	case "update_thread":
		input, decodeErr := decodeToolArguments[UpdateThreadInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		thread, updateErr := service.UpdateThread(caller, input)
		if updateErr != nil {
			return nil, updateErr
		}
		responseValue = map[string]any{"thread": thread}
	case "post_artifact":
		input, decodeErr := decodeToolArguments[PostArtifactInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.PostArtifact(caller, input)
	case "spawn_instance":
		input, decodeErr := decodeToolArguments[SpawnInstanceInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.SpawnInstance(caller, input)
	case "get_instance":
		input, decodeErr := decodeToolArguments[GetInstanceInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.GetInstance(caller, input)
	case "renew_instance":
		input, decodeErr := decodeToolArguments[RenewInstanceInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.RenewInstance(caller, input)
	case "release_instance":
		input, decodeErr := decodeToolArguments[ReleaseInstanceInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.ReleaseInstance(caller, input)
	case "list_approvals":
		input, decodeErr := decodeToolArguments[ListApprovalsInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		approvals, listErr := service.ListApprovals(input)
		if listErr != nil {
			return nil, listErr
		}
		responseValue = map[string]any{"approvals": approvals}
	case "resolve_approval":
		input, decodeErr := decodeToolArguments[ResolveApprovalInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		approval, resolveErr := service.ResolveApproval(caller, input)
		if resolveErr != nil {
			return nil, resolveErr
		}
		responseValue = map[string]any{"approval": approval}
	case "get_mail_identity":
		mailbox, identityErr := service.GetMailIdentity(caller)
		if identityErr != nil {
			return nil, identityErr
		}
		responseValue = map[string]any{"mailbox": mailbox}
	case "set_mail_identity":
		input, decodeErr := decodeToolArguments[SetMailIdentityInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		mailbox, setErr := service.SetMailIdentity(caller, input)
		if setErr != nil {
			return nil, setErr
		}
		responseValue = map[string]any{"mailbox": mailbox}
	case "list_mailboxes":
		input, decodeErr := decodeToolArguments[ListMailboxesInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.ListMailboxes(input)
	case "send_mail":
		input, decodeErr := decodeToolArguments[SendMailInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.SendMail(caller, input)
	case "read_mail":
		input, decodeErr := decodeToolArguments[ReadMailInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.ReadMail(caller, input)
	case "wait_for_mail":
		input, decodeErr := decodeToolArguments[WaitForMailInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.WaitForMail(operationContext, caller, input)
	default:
		return nil, invalidRequest(fmt.Sprintf("unknown orchestrator tool %q", toolName))
	}

	if err != nil {
		return nil, err
	}
	encodedBytes, marshalErr := json.Marshal(responseValue)
	if marshalErr != nil {
		return nil, marshalErr
	}
	return encodedBytes, nil
}

func (service *Service) handleHubRPCRequest(
	sessionContext context.Context,
	session *slaveSession,
	message protocol.SlaveControlMessage,
) {
	if message.RequestID == "" {
		return
	}
	resultBytes, rpcErr := service.executeHubToolForSlave(sessionContext, session.nodeID, message.RunID, message.Method, message.Params)
	response := protocol.MasterControlMessage{
		Type:      "hub.rpc.response",
		RequestID: message.RequestID,
		At:        service.nowUTC(),
	}
	if rpcErr != nil {
		response.Error = toHubRPCError(rpcErr)
	} else {
		response.Result = resultBytes
	}
	_ = session.send(sessionContext, response)
}

func (service *Service) executeHubToolForSlave(
	operationContext context.Context,
	nodeID string,
	runID string,
	method string,
	rawArguments json.RawMessage,
) (json.RawMessage, error) {
	if !protocol.IsHubToolName(method) {
		return nil, invalidRequest(fmt.Sprintf("unknown hub tool %q", method))
	}

	var caller CallerIdentity
	err := service.store.Read(func(state *store.State) error {
		runRecord, exists := state.Runs[runID]
		if !exists || runRecord.NodeID != nodeID {
			return forbiddenError("run is not active on this slave node")
		}
		if !protocol.IsActiveRunStatus(runRecord.Status) {
			return forbiddenError("run is no longer active")
		}
		taskID := ""
		if runRecord.TaskID != nil {
			taskID = *runRecord.TaskID
		}
		canDelegate := false
		if instance, instanceExists := state.Instances[runRecord.InstanceID]; instanceExists {
			canDelegate = instance.Delegation.CanDelegate
		}
		caller = CallerIdentity{
			Kind:        "run",
			RunID:       runRecord.ID,
			TaskID:      taskID,
			ThreadID:    runRecord.ThreadID,
			InstanceID:  runRecord.InstanceID,
			CanDelegate: canDelegate,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if protocol.IsDelegationHubToolName(method) && !caller.CanDelegate {
		return nil, forbiddenError(fmt.Sprintf("tool %q requires instance delegation permission", method))
	}

	var responseValue any
	switch method {
	case "get_task_context":
		responseValue, err = service.GetTaskContext(caller)
	case "wait_for_task_events":
		input, decodeErr := decodeToolArguments[WaitForTaskEventsInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.WaitForTaskEvents(operationContext, caller, input)
	case "delegate_task":
		input, decodeErr := decodeToolArguments[DelegateTaskInput](rawArguments)
		if decodeErr != nil {
			return nil, decodeErr
		}
		responseValue, err = service.DelegateTask(caller, input)
	default:
		return service.ExecuteOrchestratorTool(operationContext, caller, method, rawArguments)
	}

	if err != nil {
		return nil, err
	}
	return json.Marshal(responseValue)
}
