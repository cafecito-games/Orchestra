package orchestrator

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/selfupdate"
	"github.com/cafecito-games/Orchestra/internal/setup"
	"github.com/cafecito-games/Orchestra/internal/slave"
	"github.com/cafecito-games/Orchestra/internal/store"
)

const (
	DefaultListenAddress = "127.0.0.1:8787"
)

type Configuration struct {
	ListenAddress      string
	Token              string
	DataDirectory      string
	TLSCertPath        string
	TLSKeyPath         string
	ProjectProfilePath string
	WithLocalWorker    bool
	LocalWorker        slave.Configuration
}

type ServiceOptions struct {
	Store                *store.Store
	Token                string
	ProjectProfiles      []protocol.ProjectProfile
	DoorbellDebounce     time.Duration
	PingInterval         time.Duration
	PongTimeout          time.Duration
	SweepInterval        time.Duration
	NodeUpdateTimeout    time.Duration
	ReleaseClient        *selfupdate.ReleaseClient
	UpdateCacheDirectory string
	Now                  func() time.Time
}

type orchestratorClientSession struct {
	clientID   string
	sessionID  string
	connection *websocket.Conn
	writeMutex sync.Mutex
	cancel     context.CancelFunc
}

func (session *orchestratorClientSession) send(operationContext context.Context, message protocol.OrchestratorServerMessage) error {
	encodedBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	session.writeMutex.Lock()
	defer session.writeMutex.Unlock()
	timedContext, cancel := context.WithTimeout(operationContext, socketWriteTimeout)
	defer cancel()
	return session.connection.Write(timedContext, websocket.MessageText, encodedBytes)
}

type replayWindow struct {
	digests map[int64]string
	order   []int64
}

type Service struct {
	baseContext       context.Context
	store             *store.Store
	token             string
	redactor          *protocol.Redactor
	doorbellDebounce  time.Duration
	pingInterval      time.Duration
	pongTimeout       time.Duration
	sweepInterval     time.Duration
	nodeUpdateTimeout time.Duration
	releaseClient     *selfupdate.ReleaseClient
	updateCache       *selfupdate.BundleCache
	now               func() time.Time
	startedAt         string

	scheduleWakeup chan struct{}

	slavesMutex sync.RWMutex
	slaves      map[string]*slaveSession

	clientsMutex sync.RWMutex
	clients      map[string]*orchestratorClientSession

	replayMutex   sync.Mutex
	replayWindows map[string]*replayWindow

	mailSubscriberSequence atomic.Int64
	mailSubscriberMutex    sync.Mutex
	mailSubscribers        map[int64]chan protocol.MailMessage

	backgroundCancel context.CancelFunc
	backgroundGroup  sync.WaitGroup
	mcpHandler       http.Handler
	closeMCP         func()
}

func NewService(options ServiceOptions) (*Service, error) {
	if options.Store == nil {
		return nil, errors.New("orchestrator store is required")
	}
	nowFunction := options.Now
	if nowFunction == nil {
		nowFunction = time.Now
	}
	debounce := options.DoorbellDebounce
	if debounce <= 0 {
		debounce = 350 * time.Millisecond
	}
	pingInterval := options.PingInterval
	if pingInterval <= 0 {
		pingInterval = pingIntervalDuration
	}
	pongTimeout := options.PongTimeout
	if pongTimeout <= 0 {
		pongTimeout = pongTimeoutDuration
	}
	sweepInterval := options.SweepInterval
	if sweepInterval <= 0 {
		sweepInterval = 5 * time.Second
	}
	nodeUpdateTimeout := options.NodeUpdateTimeout
	if nodeUpdateTimeout <= 0 {
		nodeUpdateTimeout = defaultNodeUpdateTimeout
	}
	releaseClient := options.ReleaseClient
	if releaseClient == nil {
		createdClient, clientErr := selfupdate.NewReleaseClient(selfupdate.ReleaseClientOptions{})
		if clientErr != nil {
			return nil, clientErr
		}
		releaseClient = createdClient
	}
	cacheDirectory := strings.TrimSpace(options.UpdateCacheDirectory)
	if cacheDirectory == "" {
		cacheDirectory = filepath.Join(options.Store.DataDirectory(), "updates")
	}
	updateCache, cacheErr := selfupdate.NewBundleCache(cacheDirectory)
	if cacheErr != nil {
		return nil, cacheErr
	}

	service := &Service{
		baseContext:       context.Background(),
		store:             options.Store,
		token:             options.Token,
		redactor:          protocol.NewRedactor(options.Token),
		doorbellDebounce:  debounce,
		pingInterval:      pingInterval,
		pongTimeout:       pongTimeout,
		sweepInterval:     sweepInterval,
		nodeUpdateTimeout: nodeUpdateTimeout,
		releaseClient:     releaseClient,
		updateCache:       updateCache,
		now:               nowFunction,
		startedAt:         nowFunction().UTC().Format(time.RFC3339Nano),
		scheduleWakeup:    make(chan struct{}, 1),
		slaves:            map[string]*slaveSession{},
		clients:           map[string]*orchestratorClientSession{},
		replayWindows:     map[string]*replayWindow{},
		mailSubscribers:   map[int64]chan protocol.MailMessage{},
	}
	service.mcpHandler, service.closeMCP = service.newMCPHTTPHandler()

	if len(options.ProjectProfiles) > 0 {
		_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
			for _, profile := range options.ProjectProfiles {
				state.ProjectProfiles[profile.ID] = profile
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		nowText := service.nowUTC()
		for nodeID, node := range state.Nodes {
			if node.Status != "offline" {
				node.Status = "offline"
				node.LastSeen = nowText
				state.Nodes[nodeID] = node
			}
		}
		for mailboxID, mailbox := range state.Mailboxes {
			if mailbox.Kind == "mcp-session" && mailbox.Status == "online" {
				mailbox.Status = "offline"
				mailbox.UpdatedAt = nowText
				state.Mailboxes[mailboxID] = mailbox
			}
		}
		return nil
	})

	backgroundContext, cancelBackground := context.WithCancel(context.Background())
	service.baseContext = backgroundContext
	service.backgroundCancel = cancelBackground

	service.backgroundGroup.Go(func() {
		service.runSchedulerLoop(backgroundContext)
	})
	service.backgroundGroup.Go(func() {
		service.runDoorbellLoop(backgroundContext)
	})
	service.backgroundGroup.Go(func() {
		service.runSweepLoop(backgroundContext)
	})

	return service, nil
}

func (service *Service) Close() {
	service.closeMailSubscribers()
	if service.closeMCP != nil {
		service.closeMCP()
	}
	if service.backgroundCancel != nil {
		service.backgroundCancel()
	}
	service.slavesMutex.RLock()
	for _, session := range service.slaves {
		session.cancel()
		_ = session.connection.Close(websocket.StatusGoingAway, "shutting down")
	}
	service.slavesMutex.RUnlock()

	service.clientsMutex.RLock()
	for _, session := range service.clients {
		session.cancel()
		_ = session.connection.Close(websocket.StatusGoingAway, "shutting down")
	}
	service.clientsMutex.RUnlock()

	service.backgroundGroup.Wait()
}

func (service *Service) runSweepLoop(operationContext context.Context) {
	ticker := time.NewTicker(service.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-operationContext.Done():
			return
		case <-ticker.C:
			service.expireIdleInstances()
			service.expirePendingApprovals()
			service.expireStaleNodeUpdates()
		}
	}
}

func (service *Service) isReplayHarnessEvent(runID string, sequence int64, digest string) bool {
	service.replayMutex.Lock()
	defer service.replayMutex.Unlock()

	window := service.replayWindows[runID]
	if window == nil {
		window = &replayWindow{digests: map[int64]string{}}
		service.replayWindows[runID] = window
	}
	if previousDigest, seen := window.digests[sequence]; seen {
		return previousDigest == digest
	}
	if len(window.order) >= replayWindowEntries {
		oldest := window.order[0]
		window.order = window.order[1:]
		delete(window.digests, oldest)
	}
	window.digests[sequence] = digest
	window.order = append(window.order, sequence)
	return false
}

func (service *Service) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", service.handleHealth)
	mux.HandleFunc("/control-agent", service.handleControlAgentUpgrade)
	mux.HandleFunc("/orchestrator-client", service.handleOrchestratorClientUpgrade)
	mux.Handle("/mcp", service.requireBearerHandler(service.mcpHandler))
	mux.HandleFunc("/api/status", service.requireBearer(service.handleAPIStatus))
	mux.HandleFunc("/api/nodes", service.requireBearer(service.handleAPINodes))
	mux.HandleFunc("/api/nodes/update", service.requireBearer(service.handleAPINodesUpdate))
	mux.HandleFunc("/api/updates/bundles/", service.requireBearer(service.handleAPIUpdateBundle))
	mux.HandleFunc("/api/threads", service.requireBearer(service.handleAPIThreads))
	mux.HandleFunc("/api/threads/", service.requireBearer(service.handleAPIThreadSubroutes))
	mux.HandleFunc("/api/runs/", service.requireBearer(service.handleAPIRunSubroutes))
	mux.HandleFunc("/api/tasks/", service.requireBearer(service.handleAPITaskSubroutes))
	mux.HandleFunc("/api/cancel/", service.requireBearer(service.handleAPICancelTarget))
	mux.HandleFunc("/api/approvals", service.requireBearer(service.handleAPIApprovals))
	mux.HandleFunc("/api/approvals/", service.requireBearer(service.handleAPIApprovalSubroutes))
	mux.HandleFunc("/api/artifacts/", service.handleAPIArtifactSubroutes)
	mux.HandleFunc("/api/workspace-leases", service.requireBearer(service.handleAPIWorkspaceLeases))
	mux.HandleFunc("/api/workspace-leases/", service.requireBearer(service.handleAPIWorkspaceLeaseSubroutes))
	mux.HandleFunc("/api/mail/agents", service.requireBearer(service.handleAPIMailAgents))
	mux.HandleFunc("/api/mail/messages", service.requireBearer(service.handleAPIMailMessages))
	mux.HandleFunc("/api/mail/rename", service.requireBearer(service.handleAPIMailRename))
	return mux
}

func (service *Service) isBearerAuthorized(request *http.Request) bool {
	if service.token == "" {
		return true
	}
	header := request.Header.Get("Authorization")
	const bearerPrefix = "Bearer "
	provided, ok := strings.CutPrefix(header, bearerPrefix)
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(provided), []byte(service.token)) == 1
}

func (service *Service) requireBearer(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if !service.isBearerAuthorized(request) {
			writeJSONError(writer, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
			return
		}
		next(writer, request)
	}
}

func (service *Service) requireBearerHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !service.isBearerAuthorized(request) {
			writeJSONError(writer, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func (service *Service) handleHealth(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	writeJSONResponse(writer, http.StatusOK, map[string]any{
		"ok":              true,
		"protocolVersion": protocol.ProtocolVersion,
	})
}

func (service *Service) handleControlAgentUpgrade(writer http.ResponseWriter, request *http.Request) {
	if !service.isBearerAuthorized(request) {
		writeJSONError(writer, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
		return
	}
	connection, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	service.backgroundGroup.Add(1)
	defer service.backgroundGroup.Done()
	service.serveSlaveConnection(request.Context(), connection)
}

func (service *Service) handleOrchestratorClientUpgrade(writer http.ResponseWriter, request *http.Request) {
	if !service.isBearerAuthorized(request) {
		writeJSONError(writer, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
		return
	}
	connection, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	service.backgroundGroup.Add(1)
	defer service.backgroundGroup.Done()
	service.serveOrchestratorClientConnection(request.Context(), connection)
}

func (service *Service) serveOrchestratorClientConnection(requestContext context.Context, connection *websocket.Conn) {
	connection.SetReadLimit(16 * 1024 * 1024)

	registrationContext, cancelRegistration := context.WithTimeout(requestContext, 15*time.Second)
	defer cancelRegistration()
	_, firstPayload, err := connection.Read(registrationContext)
	if err != nil {
		_ = connection.Close(websocket.StatusProtocolError, "expected client registration")
		return
	}

	var registration protocol.OrchestratorClientMessage
	if err := json.Unmarshal(firstPayload, &registration); err != nil || registration.Type != "register" {
		_ = connection.Close(websocket.StatusProtocolError, "malformed client registration")
		return
	}
	if registration.ProtocolVersion != "" && registration.ProtocolVersion != protocol.ProtocolVersion {
		_ = connection.Close(websocket.StatusPolicyViolation, "unsupported protocol version")
		return
	}

	clientID := strings.TrimSpace(registration.ClientID)
	if !protocol.IsIdentifier(clientID) {
		clientID = protocol.NewIdentifier("client")
	}
	sessionID := strings.TrimSpace(registration.SessionID)
	if !protocol.IsIdentifier(sessionID) {
		sessionID = protocol.NewIdentifier("osess")
	}

	sessionContext, cancelSession := context.WithCancel(requestContext)
	session := &orchestratorClientSession{
		clientID:   clientID,
		sessionID:  sessionID,
		connection: connection,
		cancel:     cancelSession,
	}

	service.clientsMutex.Lock()
	if previous, exists := service.clients[sessionID]; exists {
		previous.cancel()
		_ = previous.connection.Close(websocket.StatusPolicyViolation, "session replaced")
	}
	service.clients[sessionID] = session
	service.clientsMutex.Unlock()

	defer func() {
		cancelSession()
		removedCurrent := false
		service.clientsMutex.Lock()
		if service.clients[sessionID] == session {
			delete(service.clients, sessionID)
			removedCurrent = true
		}
		service.clientsMutex.Unlock()
		if removedCurrent {
			service.DisconnectClientMailbox(sessionID)
		}
		_ = connection.Close(websocket.StatusNormalClosure, "closed")
	}()

	var mailboxPointer *protocol.AgentMailbox
	if registeredMailbox, regErr := service.RegisterClientMailbox(clientID, sessionID, registration.Mail); regErr == nil {
		mailboxPointer = &registeredMailbox
	}

	if err := session.send(sessionContext, protocol.OrchestratorServerMessage{
		Type:            "welcome",
		ProtocolVersion: protocol.ProtocolVersion,
		ClientID:        clientID,
		SessionID:       sessionID,
		Mailbox:         mailboxPointer,
		Scopes:          slices.Clone(protocol.OrchestratorToolNames),
		At:              service.nowUTC(),
	}); err != nil {
		return
	}

	caller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  clientID,
		SessionID: sessionID,
	}

	for {
		_, payload, readErr := connection.Read(sessionContext)
		if readErr != nil {
			return
		}
		var message protocol.OrchestratorClientMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			continue
		}

		switch message.Type {
		case "ping":
			_ = session.send(sessionContext, protocol.OrchestratorServerMessage{
				Type: "pong",
				At:   service.nowUTC(),
			})
		case "mail.identify":
			identifiedMailbox, identifyErr := service.RegisterClientMailbox(clientID, sessionID, message.Mail)
			response := protocol.OrchestratorServerMessage{
				Type:      "mail.identified",
				RequestID: message.RequestID,
				At:        service.nowUTC(),
			}
			if identifyErr != nil {
				response.Error = toHubRPCError(identifyErr)
			} else {
				response.Mailbox = &identifiedMailbox
			}
			_ = session.send(sessionContext, response)
		case "tool.call":
			requestID := message.RequestID
			toolName := message.Tool
			arguments := slices.Clone(message.Arguments)
			go func() {
				resultBytes, callErr := service.ExecuteOrchestratorTool(sessionContext, caller, toolName, arguments)
				response := protocol.OrchestratorServerMessage{
					Type:      "tool.result",
					RequestID: requestID,
					At:        service.nowUTC(),
				}
				if callErr != nil {
					response.Error = toHubRPCError(callErr)
				} else {
					response.Result = resultBytes
				}
				_ = session.send(sessionContext, response)
			}()
		}
	}
}

func (service *Service) sendToOrchestratorSession(sessionID string, message protocol.OrchestratorServerMessage) {
	service.clientsMutex.RLock()
	session, exists := service.clients[sessionID]
	service.clientsMutex.RUnlock()
	if !exists {
		return
	}
	_ = session.send(context.Background(), message)
}

func (service *Service) handleAPIStatus(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	status, err := service.GetStatus()
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, status)
}

func (service *Service) handleAPINodes(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	nodes, err := service.ListNodeDetails()
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, map[string]any{"nodes": nodes})
}

func (service *Service) handleAPIThreads(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		statusFilter := request.URL.Query().Get("status")
		limit := 100
		if rawLimit := request.URL.Query().Get("limit"); rawLimit != "" {
			if parsed, err := strconv.Atoi(rawLimit); err == nil {
				limit = parsed
			}
		}
		threads, err := service.ListThreads(ListThreadsInput{Status: statusFilter, Limit: limit})
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, map[string]any{"threads": threads})
	case http.MethodPost:
		var input CreateThreadInput
		if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
		thread, attachment, err := service.CreateThread(CallerIdentity{Kind: "operator", OperatorID: "cli"}, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusCreated, map[string]any{"thread": thread, "attachment": attachment})
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (service *Service) handleAPIThreadSubroutes(writer http.ResponseWriter, request *http.Request) {
	remainder := strings.TrimPrefix(request.URL.Path, "/api/threads/")
	remainder = strings.TrimSuffix(remainder, "/")
	threadID, subroute, hasSubroute := strings.Cut(remainder, "/")
	if !hasSubroute {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		detail, err := service.GetThreadDetail(threadID)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, detail)
		return
	}

	operatorCaller := CallerIdentity{Kind: "operator", OperatorID: "cli"}
	switch subroute {
	case "room":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var afterSequence int64
		if rawAfter := strings.TrimSpace(request.URL.Query().Get("after")); rawAfter != "" {
			parsedAfter, parseErr := strconv.ParseInt(rawAfter, 10, 64)
			if parseErr != nil {
				writeOperationError(writer, invalidRequest("after must be an integer"))
				return
			}
			afterSequence = parsedAfter
		}
		var limit int
		if rawLimit := strings.TrimSpace(request.URL.Query().Get("limit")); rawLimit != "" {
			parsedLimit, parseErr := strconv.Atoi(rawLimit)
			if parseErr != nil {
				writeOperationError(writer, invalidRequest("limit must be an integer"))
				return
			}
			limit = parsedLimit
		}
		var kinds []string
		for _, rawKind := range request.URL.Query()["kind"] {
			for part := range strings.SplitSeq(rawKind, ",") {
				trimmed := strings.TrimSpace(part)
				if trimmed != "" {
					kinds = append(kinds, trimmed)
				}
			}
		}
		result, err := service.ReadRoom(operatorCaller, ReadRoomInput{
			ThreadID:      threadID,
			AfterSequence: afterSequence,
			Kinds:         kinds,
			Limit:         limit,
		})
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, result)
	case "room/messages":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var input PostRoomMessageInput
		if err := json.NewDecoder(io.LimitReader(request.Body, 256*1024)).Decode(&input); err != nil {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
		input.ThreadID = threadID
		result, err := service.PostRoomMessage(operatorCaller, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		statusCode := http.StatusCreated
		if result.Replayed {
			statusCode = http.StatusOK
		}
		writeJSONResponse(writer, statusCode, result)
	case "room/nudge":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var input NudgeRoomInput
		if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
		input.ThreadID = threadID
		result, err := service.NudgeRoom(operatorCaller, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, result)
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

func (service *Service) handleAPIRunSubroutes(writer http.ResponseWriter, request *http.Request) {
	remainder := strings.TrimPrefix(request.URL.Path, "/api/runs/")
	runID, action, hasAction := strings.Cut(remainder, "/")
	if !hasAction {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	switch action {
	case "transcript":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var transcript protocol.RunTranscript
		var found bool
		_ = service.store.Read(func(state *store.State) error {
			transcript, found = state.RunTranscripts[runID]
			return nil
		})
		if !found {
			writeOperationError(writer, notFound(fmt.Sprintf("transcript for run %s does not exist", runID)))
			return
		}
		writeJSONResponse(writer, http.StatusOK, transcript)
	case "cancel":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		result, err := service.CancelTarget(runID)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, result)
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

func (service *Service) handleAPITaskSubroutes(writer http.ResponseWriter, request *http.Request) {
	remainder := strings.TrimPrefix(request.URL.Path, "/api/tasks/")
	taskID, action, hasAction := strings.Cut(remainder, "/")
	if !hasAction || action != "cancel" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	result, err := service.CancelTarget(taskID)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, result)
}

func (service *Service) handleAPICancelTarget(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	targetID := strings.TrimPrefix(request.URL.Path, "/api/cancel/")
	targetID = strings.TrimSuffix(targetID, "/")
	result, err := service.CancelTarget(targetID)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, result)
}

func (service *Service) handleAPIApprovals(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	approvals, err := service.ListApprovals(ListApprovalsInput{
		ThreadID: request.URL.Query().Get("threadId"),
		Status:   request.URL.Query().Get("status"),
	})
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, map[string]any{"approvals": approvals})
}

func (service *Service) handleAPIApprovalSubroutes(writer http.ResponseWriter, request *http.Request) {
	remainder := strings.TrimPrefix(request.URL.Path, "/api/approvals/")
	approvalID, action, hasAction := strings.Cut(remainder, "/")
	if !hasAction || action != "resolve" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var body struct {
		Status   string `json:"status"`
		OptionID string `json:"optionId"`
	}
	if err := json.NewDecoder(io.LimitReader(request.Body, 16*1024)).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		writeOperationError(writer, invalidRequest("malformed JSON body"))
		return
	}
	resolved, err := service.ResolveApproval(
		CallerIdentity{Kind: "operator", OperatorID: "cli"},
		ResolveApprovalInput{
			ApprovalID: approvalID,
			Status:     body.Status,
			OptionID:   body.OptionID,
		},
	)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, map[string]any{"approval": resolved})
}

func (service *Service) handleAPIArtifactSubroutes(writer http.ResponseWriter, request *http.Request) {
	remainder := strings.TrimPrefix(request.URL.Path, "/api/artifacts/")
	artifactID, action, hasAction := strings.Cut(remainder, "/")
	if !hasAction || action != "content" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	switch request.Method {
	case http.MethodPut:
		bearerAuthorized := service.isBearerAuthorized(request)
		uploadToken := request.URL.Query().Get("uploadToken")
		if uploadToken == "" {
			uploadToken = request.Header.Get("X-Orchestra-Upload-Token")
		}
		updated, err := service.CompleteArtifactUpload(artifactID, uploadToken, bearerAuthorized, request.Body)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, map[string]any{"artifact": updated})
	case http.MethodGet:
		if !service.isBearerAuthorized(request) {
			writeJSONError(writer, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
			return
		}
		artifact, file, size, err := service.OpenUploadedArtifact(artifactID)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		defer file.Close()
		mediaType := artifact.MediaType
		if mediaType == "" {
			mediaType = "application/octet-stream"
		}
		writer.Header().Set("Content-Type", mediaType)
		writer.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		writer.WriteHeader(http.StatusOK)
		_, _ = io.Copy(writer, file)
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (service *Service) handleAPIWorkspaceLeases(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	threadFilter := request.URL.Query().Get("threadId")
	var leases []protocol.WorkspaceLease
	_ = service.store.Read(func(state *store.State) error {
		for _, lease := range state.WorkspaceLeases {
			if threadFilter != "" && lease.ThreadID != threadFilter {
				continue
			}
			leases = append(leases, lease)
		}
		return nil
	})
	if leases == nil {
		leases = []protocol.WorkspaceLease{}
	}
	writeJSONResponse(writer, http.StatusOK, map[string]any{"workspaceLeases": leases})
}

func (service *Service) handleAPIWorkspaceLeaseSubroutes(writer http.ResponseWriter, request *http.Request) {
	remainder := strings.TrimPrefix(request.URL.Path, "/api/workspace-leases/")
	leaseID, action, hasAction := strings.Cut(remainder, "/")
	if !hasAction || action != "cleanup" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	updated, err := service.CleanupWorkspaceLease(leaseID)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, map[string]any{"workspaceLease": updated})
}

func (service *Service) handleAPIMailAgents(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	query := request.URL.Query()
	onlineOnly := query.Get("onlineOnly") == "1" || strings.EqualFold(query.Get("onlineOnly"), "true")
	includeReleased := query.Get("includeReleased") == "1" || strings.EqualFold(query.Get("includeReleased"), "true")
	result, err := service.ListMailboxes(ListMailboxesInput{
		Host:            query.Get("host"),
		OnlineOnly:      onlineOnly,
		IncludeReleased: includeReleased,
		Workspace:       query.Get("workspace"),
	})
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, result)
}

func (service *Service) handleAPIMailMessages(writer http.ResponseWriter, request *http.Request) {
	operatorCaller := CallerIdentity{Kind: "operator", OperatorID: "cli"}
	switch request.Method {
	case http.MethodGet:
		query := request.URL.Query()
		unreadOnly := query.Get("unreadOnly") == "1" || strings.EqualFold(query.Get("unreadOnly"), "true")
		var afterSequence int64
		if rawAfter := strings.TrimSpace(query.Get("afterSequence")); rawAfter != "" {
			parsedAfter, parseErr := strconv.ParseInt(rawAfter, 10, 64)
			if parseErr != nil {
				writeOperationError(writer, invalidRequest("afterSequence must be an integer"))
				return
			}
			afterSequence = parsedAfter
		}
		var limit int
		if rawLimit := strings.TrimSpace(query.Get("limit")); rawLimit != "" {
			parsedLimit, parseErr := strconv.Atoi(rawLimit)
			if parseErr != nil {
				writeOperationError(writer, invalidRequest("limit must be an integer"))
				return
			}
			limit = parsedLimit
		}
		var markRead *bool
		if rawMarkRead := strings.TrimSpace(query.Get("markRead")); rawMarkRead != "" {
			parsedMarkRead := rawMarkRead == "1" || strings.EqualFold(rawMarkRead, "true")
			markRead = &parsedMarkRead
		} else {
			defaultNoMark := false
			markRead = &defaultNoMark
		}
		result, err := service.ReadMail(operatorCaller, ReadMailInput{
			Address:       query.Get("address"),
			Box:           query.Get("box"),
			UnreadOnly:    unreadOnly,
			AfterSequence: afterSequence,
			Limit:         limit,
			MarkRead:      markRead,
		})
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, result)
	case http.MethodPost:
		var input SendMailInput
		if err := json.NewDecoder(io.LimitReader(request.Body, 256*1024)).Decode(&input); err != nil {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
		if strings.TrimSpace(input.IdempotencyKey) == "" {
			input.IdempotencyKey = protocol.NewIdentifier("cli")
		}
		result, err := service.SendMail(operatorCaller, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		statusCode := http.StatusCreated
		if result.Replayed {
			statusCode = http.StatusOK
		}
		writeJSONResponse(writer, statusCode, result)
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (service *Service) handleAPIMailRename(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var input SetMailIdentityInput
	if err := json.NewDecoder(io.LimitReader(request.Body, 32*1024)).Decode(&input); err != nil {
		writeOperationError(writer, invalidRequest("malformed JSON body"))
		return
	}
	if strings.TrimSpace(input.Address) == "" {
		writeOperationError(writer, invalidRequest("address is required"))
		return
	}
	updated, err := service.SetMailIdentity(CallerIdentity{Kind: "operator", OperatorID: "cli"}, input)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, map[string]any{"mailbox": updated})
}

func (service *Service) nowUTC() string {
	return service.now().UTC().Format(time.RFC3339Nano)
}

func writeJSONResponse(writer http.ResponseWriter, statusCode int, payload any) {
	encodedBytes, err := json.Marshal(payload)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(statusCode)
	_, _ = writer.Write(encodedBytes)
}

func writeJSONError(writer http.ResponseWriter, statusCode int, code, message string) {
	writeJSONResponse(writer, statusCode, map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
		},
	})
}

func writeOperationError(writer http.ResponseWriter, err error) {
	if operationError, ok := errors.AsType[*OperationError](err); ok {
		statusCode := operationError.HTTPStatus
		if statusCode <= 0 {
			statusCode = http.StatusBadRequest
		}
		writeJSONError(writer, statusCode, operationError.Code, operationError.Message)
		return
	}
	writeJSONError(writer, http.StatusInternalServerError, "internal_error", protocol.BoundedDiagnostic(protocol.RedactText(err.Error())))
}

type repeatStringFlag struct {
	values []string
	set    bool
}

func (flagValue *repeatStringFlag) String() string {
	return strings.Join(flagValue.values, ",")
}

func (flagValue *repeatStringFlag) Set(raw string) error {
	flagValue.set = true
	for part := range strings.SplitSeq(raw, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			flagValue.values = append(flagValue.values, trimmed)
		}
	}
	return nil
}

func ParseConfiguration(arguments []string, lookupEnvironment func(string) (string, bool)) (Configuration, error) {
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}

	defaultListen := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_LISTEN")
	if defaultListen == "" {
		defaultListen = DefaultListenAddress
	}
	defaultToken := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_TOKEN")
	defaultDataDirectory := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_DATA_DIR", "ORCHESTRA_DATA_ROOT")
	if defaultDataDirectory == "" {
		defaultDataDirectory = filepath.Join(setup.DefaultDataRoot(), "master")
	}
	defaultTLSCert := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_TLS_CERT")
	defaultTLSKey := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_TLS_KEY")
	defaultProfiles := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_PROJECT_PROFILES")
	defaultWithLocal := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_WITH_LOCAL_WORKER") == "1" ||
		strings.EqualFold(firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_WITH_LOCAL_WORKER"), "true")

	var workspaceRootsFlag repeatStringFlag
	var approvalPolicyFlag repeatStringFlag
	var acpAdapterFlag repeatStringFlag
	var workerLabelsFlag repeatStringFlag

	flagSet := flag.NewFlagSet("orchestra master", flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)

	var configuration Configuration
	var workerName string
	var workerID string
	var workerConcurrency int
	var workerInstanceCapacity int
	var claudeAuthMode string

	flagSet.StringVar(&configuration.ListenAddress, "listen", defaultListen, "HTTP/WebSocket listen address (e.g. 0.0.0.0:8787)")
	flagSet.StringVar(&configuration.Token, "token", defaultToken, "Bearer token matching ORCHESTRA_TOKEN")
	flagSet.StringVar(&configuration.DataDirectory, "data-dir", defaultDataDirectory, "SQLite state and artifact storage directory")
	flagSet.StringVar(&configuration.TLSCertPath, "tls-cert", defaultTLSCert, "Optional TLS certificate PEM path for wss:// / https://")
	flagSet.StringVar(&configuration.TLSKeyPath, "tls-key", defaultTLSKey, "Optional TLS private key PEM path for wss:// / https://")
	flagSet.StringVar(&configuration.ProjectProfilePath, "project-profiles", defaultProfiles, "Optional JSON file containing project profiles")
	flagSet.BoolVar(&configuration.WithLocalWorker, "with-local-worker", defaultWithLocal, "Run an embedded local slave worker connected to loopback")
	flagSet.StringVar(&workerName, "worker-name", firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_NAME"), "Display name for embedded local worker")
	flagSet.StringVar(&workerID, "worker-id", firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_NODE_ID"), "Node ID for embedded local worker")
	flagSet.IntVar(&workerConcurrency, "worker-concurrency", 2, "Concurrency for embedded local worker")
	flagSet.IntVar(&workerInstanceCapacity, "worker-instance-capacity", 4, "Instance capacity for embedded local worker")
	flagSet.StringVar(&claudeAuthMode, "claude-acp-auth-mode", firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_CLAUDE_ACP_AUTH_MODE"), "Claude ACP auth mode for embedded local worker")
	flagSet.Var(&workspaceRootsFlag, "workspace-root", "Workspace root for embedded local worker (repeatable)")
	flagSet.Var(&workerLabelsFlag, "worker-label", "Label for embedded local worker (repeatable)")
	flagSet.Var(&approvalPolicyFlag, "approval-policy", "Approval policy for embedded local worker")
	flagSet.Var(&acpAdapterFlag, "acp-adapter", "ACP adapter override for embedded local worker")

	if err := flagSet.Parse(arguments); err != nil {
		return Configuration{}, err
	}
	if flagSet.NArg() > 0 {
		return Configuration{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flagSet.Args(), " "))
	}

	if (configuration.TLSCertPath == "") != (configuration.TLSKeyPath == "") {
		return Configuration{}, errors.New("--tls-cert and --tls-key must be provided together")
	}
	if configuration.Token == "" {
		return Configuration{}, errors.New("ORCHESTRA_TOKEN or --token is required to start orchestra master")
	}

	if configuration.WithLocalWorker {
		var workerArgs []string
		workerArgs = append(workerArgs, "--token", configuration.Token)
		if workerName != "" {
			workerArgs = append(workerArgs, "--name", workerName)
		}
		if workerID != "" {
			workerArgs = append(workerArgs, "--id", workerID)
		}
		workerArgs = append(workerArgs, "--concurrency", strconv.Itoa(workerConcurrency))
		workerArgs = append(workerArgs, "--instance-capacity", strconv.Itoa(workerInstanceCapacity))
		if claudeAuthMode != "" {
			workerArgs = append(workerArgs, "--claude-acp-auth-mode", claudeAuthMode)
		}
		for _, root := range workspaceRootsFlag.values {
			workerArgs = append(workerArgs, "--workspace-root", root)
		}
		for _, label := range workerLabelsFlag.values {
			workerArgs = append(workerArgs, "--label", label)
		}
		for _, policy := range approvalPolicyFlag.values {
			workerArgs = append(workerArgs, "--approval-policy", policy)
		}
		for _, adapter := range acpAdapterFlag.values {
			workerArgs = append(workerArgs, "--acp-adapter", adapter)
		}
		workerConfig, workerErr := slave.ParseConfiguration(workerArgs, lookupEnvironment)
		if workerErr != nil {
			return Configuration{}, fmt.Errorf("local worker configuration: %w", workerErr)
		}
		if len(workerConfig.WorkspaceRoots) == 0 {
			workingDirectory, wdErr := os.Getwd()
			if wdErr == nil {
				roots, rootErr := slave.CanonicalizeWorkspaceRoots([]string{workingDirectory})
				if rootErr == nil {
					workerConfig.WorkspaceRoots = roots
				}
			}
		}
		configuration.LocalWorker = workerConfig
	}
	return configuration, nil
}

type RunningMaster struct {
	Service  *Service
	Store    *store.Store
	Address  string
	BaseURL  string
	Wait     func() error
	Shutdown func(context.Context) error
}

func StartMaster(operationContext context.Context, configuration Configuration) (*RunningMaster, error) {
	var profiles []protocol.ProjectProfile
	if configuration.ProjectProfilePath != "" {
		profileBytes, readErr := os.ReadFile(configuration.ProjectProfilePath)
		if readErr != nil {
			return nil, fmt.Errorf("read project profiles: %w", readErr)
		}
		if unmarshalErr := json.Unmarshal(profileBytes, &profiles); unmarshalErr != nil {
			return nil, fmt.Errorf("parse project profiles: %w", unmarshalErr)
		}
	}

	openedStore, err := store.Open(configuration.DataDirectory)
	if err != nil {
		return nil, err
	}

	service, err := NewService(ServiceOptions{
		Store:           openedStore,
		Token:           configuration.Token,
		ProjectProfiles: profiles,
	})
	if err != nil {
		_ = openedStore.Close()
		return nil, err
	}

	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(operationContext, "tcp", configuration.ListenAddress)
	if err != nil {
		service.Close()
		_ = openedStore.Close()
		return nil, fmt.Errorf("listen on %s: %w", configuration.ListenAddress, err)
	}

	scheme := "http"
	var tlsConfiguration *tls.Config
	if configuration.TLSCertPath != "" {
		certificate, certErr := tls.LoadX509KeyPair(configuration.TLSCertPath, configuration.TLSKeyPath)
		if certErr != nil {
			_ = listener.Close()
			service.Close()
			_ = openedStore.Close()
			return nil, fmt.Errorf("load TLS key pair: %w", certErr)
		}
		tlsConfiguration = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{certificate},
		}
		listener = tls.NewListener(listener, tlsConfiguration)
		scheme = "https"
	}

	boundAddress := listener.Addr().String()
	_, boundPort, _ := net.SplitHostPort(boundAddress)
	loopbackBaseURL := scheme + "://127.0.0.1:" + boundPort

	serverBaseContext, cancelServerBase := context.WithCancel(operationContext)
	httpServer := &http.Server{
		Handler:           service.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext: func(net.Listener) context.Context {
			return serverBaseContext
		},
	}

	serverErrChannel := make(chan error, 1)
	go func() {
		serveErr := httpServer.Serve(listener)
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			serverErrChannel <- serveErr
			return
		}
		serverErrChannel <- nil
	}()

	workerContext, cancelWorker := context.WithCancel(operationContext)
	workerDone := make(chan struct{})
	if configuration.WithLocalWorker {
		localConfig := configuration.LocalWorker
		localConfig.ControlEndpoint = loopbackBaseURL
		localConfig.Token = configuration.Token
		localConfig.DisableSelfUpdate = true
		if tlsConfiguration != nil {
			localConfig.HTTPClient = &http.Client{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{
						MinVersion:         tls.VersionTLS12,
						InsecureSkipVerify: true,
					},
				},
			}
		}
		runtimeWorker, buildErr := slave.BuildRuntime(workerContext, localConfig)
		if buildErr != nil {
			cancelWorker()
			cancelServerBase()
			shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelShutdown()
			_ = httpServer.Shutdown(shutdownContext)
			service.Close()
			_ = openedStore.Close()
			return nil, fmt.Errorf("build embedded local worker: %w", buildErr)
		}
		go func() {
			defer close(workerDone)
			_ = runtimeWorker.Client.Run(workerContext)
		}()
	} else {
		close(workerDone)
	}

	var shutdownOnce sync.Once
	var shutdownErr error
	shutdownFunction := func(shutdownContext context.Context) error {
		shutdownOnce.Do(func() {
			cancelWorker()
			<-workerDone
			cancelServerBase()
			service.Close()
			shutdownErr = httpServer.Shutdown(shutdownContext)
			if closeErr := openedStore.Close(); shutdownErr == nil {
				shutdownErr = closeErr
			}
		})
		return shutdownErr
	}

	go func() {
		<-operationContext.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownFunction(shutdownContext)
	}()

	return &RunningMaster{
		Service:  service,
		Store:    openedStore,
		Address:  boundAddress,
		BaseURL:  loopbackBaseURL,
		Wait:     func() error { return <-serverErrChannel },
		Shutdown: shutdownFunction,
	}, nil
}

func firstMasterEnvironment(lookup func(string) (string, bool), names ...string) string {
	for _, name := range names {
		if value, found := lookup(name); found && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
