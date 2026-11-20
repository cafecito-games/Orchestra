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

	"github.com/cafecito-games/Orchestra/internal/activitylog"
	"github.com/cafecito-games/Orchestra/internal/localstate"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/selfupdate"
	"github.com/cafecito-games/Orchestra/internal/setup"
	"github.com/cafecito-games/Orchestra/internal/slave"
	"github.com/cafecito-games/Orchestra/internal/store"
	"github.com/cafecito-games/Orchestra/internal/tailscaleauth"
	"github.com/cafecito-games/Orchestra/internal/webui"
)

const (
	DefaultListenAddress = "127.0.0.1:8787"
)

type ServiceMode string

const (
	ServiceModeOrchestrator ServiceMode = "orchestrator"
	ServiceModeRelay        ServiceMode = "relay"
)

type Configuration struct {
	Mode                ServiceMode
	ConfigFilePath      string
	Profile             string
	ListenAddress       string
	ListenExplicit      bool
	AuthMode            string
	Token               string
	TokenExplicit       bool
	TailscaleBinary     string
	TailscaleAllowUsers []string
	TailscaleAllowTags  []string
	TailscaleVerifier   *tailscaleauth.Verifier
	DataDirectory       string
	ConnectionFilePath  string
	TLSCertPath         string
	TLSKeyPath          string
	ProjectProfilePath  string
	WithLocalWorker     bool
	LocalWorker         slave.Configuration
	LogLevel            activitylog.Level
	LogFormat           activitylog.Format
	Logger              *activitylog.Logger
}

type ServiceOptions struct {
	Mode                 ServiceMode
	Store                *store.Store
	Token                string
	TailscaleVerifier    *tailscaleauth.Verifier
	ProjectProfiles      []protocol.ProjectProfile
	DoorbellDebounce     time.Duration
	PingInterval         time.Duration
	PongTimeout          time.Duration
	SweepInterval        time.Duration
	NodeUpdateTimeout    time.Duration
	ReleaseClient        *selfupdate.ReleaseClient
	UpdateCacheDirectory string
	Logger               *activitylog.Logger
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
	mode              ServiceMode
	store             *store.Store
	token             string
	authMode          string
	hostName          string
	tailscaleVerifier *tailscaleauth.Verifier
	redactor          *protocol.Redactor
	logger            *activitylog.Logger
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
	mode := options.Mode
	if mode == "" {
		mode = ServiceModeOrchestrator
	}
	if mode != ServiceModeOrchestrator && mode != ServiceModeRelay {
		return nil, fmt.Errorf("unknown service mode %q", mode)
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
	var releaseClient *selfupdate.ReleaseClient
	var updateCache *selfupdate.BundleCache
	if mode == ServiceModeOrchestrator {
		releaseClient = options.ReleaseClient
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
		var cacheErr error
		updateCache, cacheErr = selfupdate.NewBundleCache(cacheDirectory)
		if cacheErr != nil {
			return nil, cacheErr
		}
	}

	authMode := tailscaleauth.AuthModeToken
	hostName, _ := os.Hostname()
	if options.TailscaleVerifier != nil {
		authMode = tailscaleauth.AuthModeTailscale
		if selfHost := strings.TrimSpace(options.TailscaleVerifier.Self().HostName); selfHost != "" {
			hostName = selfHost
		}
	}

	service := &Service{
		baseContext:       context.Background(),
		mode:              mode,
		store:             options.Store,
		token:             options.Token,
		authMode:          authMode,
		hostName:          strings.TrimSpace(hostName),
		tailscaleVerifier: options.TailscaleVerifier,
		redactor:          protocol.NewRedactor(options.Token),
		logger:            options.Logger,
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

	if mode == ServiceModeOrchestrator && len(options.ProjectProfiles) > 0 {
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
		if mode == ServiceModeOrchestrator {
			for nodeID, node := range state.Nodes {
				if node.Status != "offline" {
					node.Status = "offline"
					node.LastSeen = nowText
					state.Nodes[nodeID] = node
				}
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

	if mode == ServiceModeOrchestrator {
		service.backgroundGroup.Go(func() {
			service.runSchedulerLoop(backgroundContext)
		})
	}
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
			service.expireTaskClaims()
			if service.mode == ServiceModeOrchestrator {
				service.expireIdleInstances()
				service.expirePendingApprovals()
				service.expireStaleNodeUpdates()
			}
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
	mux.HandleFunc("/orchestrator-client", service.handleOrchestratorClientUpgrade)
	mux.Handle("/mcp", service.requireBearerHandler(service.mcpHandler))
	mux.HandleFunc("/api/mail/agents", service.requireBearer(service.handleAPIMailAgents))
	mux.HandleFunc("/api/mail/messages", service.requireBearer(service.handleAPIMailMessages))
	mux.HandleFunc("/api/mail/rename", service.requireClusterAdmin(service.handleAPIMailRename))
	mux.HandleFunc("/api/mail/traffic", service.requireClusterAdmin(service.handleAPIMailTraffic))
	mux.HandleFunc("/api/threads", service.requireBearer(service.handleAPIThreads))
	mux.HandleFunc("/api/threads/", service.requireBearer(service.handleAPIThreadSubroutes))
	mux.HandleFunc("/api/tasks/", service.requireBearer(service.handleAPITaskSubroutes))
	mux.HandleFunc("/api/tokens", service.requireBearer(service.handleAPITokens))
	mux.HandleFunc("/api/tokens/", service.requireBearer(service.handleAPITokenSubroutes))
	if service.mode == ServiceModeOrchestrator {
		mux.HandleFunc("/control-agent", service.handleControlAgentUpgrade)
		mux.HandleFunc("/api/status", service.requireClusterAdmin(service.handleAPIStatus))
		mux.HandleFunc("/api/nodes", service.requireClusterAdmin(service.handleAPINodes))
		mux.HandleFunc("/api/nodes/repositories/refresh", service.requireClusterAdmin(service.handleAPINodesRefresh))
		mux.HandleFunc("/api/nodes/repositories/clone", service.requireClusterAdmin(service.handleAPINodesClone))
		mux.HandleFunc("/api/nodes/update", service.requireClusterAdmin(service.handleAPINodesUpdate))
		mux.HandleFunc("/api/updates/bundles/", service.requireClusterAdmin(service.handleAPIUpdateBundle))
		mux.HandleFunc("/api/runs/", service.requireClusterAdmin(service.handleAPIRunSubroutes))
		mux.HandleFunc("/api/cancel/", service.requireClusterAdmin(service.handleAPICancelTarget))
		mux.HandleFunc("/api/approvals", service.requireClusterAdmin(service.handleAPIApprovals))
		mux.HandleFunc("/api/approvals/", service.requireClusterAdmin(service.handleAPIApprovalSubroutes))
		mux.HandleFunc("/api/artifacts/", service.handleAPIArtifactSubroutes)
		mux.HandleFunc("/api/workspace-leases", service.requireClusterAdmin(service.handleAPIWorkspaceLeases))
		mux.HandleFunc("/api/workspace-leases/", service.requireClusterAdmin(service.handleAPIWorkspaceLeaseSubroutes))
	}
	// The operator console is a static shell with no cluster data; it falls back
	// to "/" so it only serves paths not claimed by the API or WebSocket routes.
	mux.Handle("/", webui.Handler())
	return mux
}

func (service *Service) matchesBearerToken(request *http.Request) bool {
	if service.token == "" || request == nil {
		return false
	}
	header := request.Header.Get("Authorization")
	const bearerPrefix = "Bearer "
	provided, ok := strings.CutPrefix(header, bearerPrefix)
	if !ok {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(provided), []byte(service.token)) == 1
}

func (service *Service) authorizeRequest(request *http.Request) (AuthContext, bool, string) {
	extractBearer := func(req *http.Request) string {
		if req == nil {
			return ""
		}
		provided, ok := strings.CutPrefix(req.Header.Get("Authorization"), "Bearer ")
		if !ok {
			return ""
		}
		return provided
	}

	finalize := func(auth AuthContext) (AuthContext, bool, string) {
		narrowed, narrowErr := service.applyHeaderScopeNarrowing(request, auth)
		if narrowErr != nil {
			return AuthContext{}, false, narrowErr.Error()
		}
		return narrowed, true, ""
	}

	if service.tailscaleVerifier != nil {
		if tailscaleauth.IsBrowserRequest(request) {
			return AuthContext{}, false, "browser-origin requests are not permitted in tailscale auth mode"
		}
		if tailscaleauth.IsProxiedOrFunnelRequest(request) {
			return AuthContext{}, false, "proxied or Tailscale Funnel requests are not permitted in tailscale auth mode"
		}
		if service.matchesBearerToken(request) {
			return finalize(AuthContext{
				PrincipalKind: protocol.PrincipalKindToken,
				PrincipalID:   "master-token",
				FullAuthority: true,
			})
		}
		if provided := extractBearer(request); provided != "" {
			if grant, found := service.lookupAccessGrantByToken(provided); found {
				scopeCopy := grant.Scope
				return finalize(AuthContext{
					PrincipalKind: grant.PrincipalKind,
					PrincipalID:   grant.PrincipalID,
					VerifiedNode:  grant.VerifiedNode,
					GrantID:       grant.ID,
					Scope:         &scopeCopy,
					FullAuthority: false,
				})
			}
			return AuthContext{}, false, "invalid bearer or scoped grant token"
		}
		peer, err := service.tailscaleVerifier.AuthorizeHTTPRequest(request)
		if err != nil {
			service.logger.Warn(
				"auth",
				"tailscale peer rejected",
				activitylog.String("remote", request.RemoteAddr),
				activitylog.String("path", request.URL.Path),
				activitylog.String("error", err.Error()),
			)
			return AuthContext{}, false, err.Error()
		}
		service.logger.Debug(
			"auth",
			"tailscale peer verified",
			activitylog.String("node", peer.NodeName),
			activitylog.String("user", peer.LoginName),
			activitylog.String("remote", request.RemoteAddr),
		)
		return finalize(principalFromTailscalePeer(peer))
	}

	if provided := extractBearer(request); provided != "" {
		if service.matchesBearerToken(request) {
			return finalize(AuthContext{
				PrincipalKind: protocol.PrincipalKindToken,
				PrincipalID:   "master-token",
				FullAuthority: true,
			})
		}
		if grant, found := service.lookupAccessGrantByToken(provided); found {
			scopeCopy := grant.Scope
			return finalize(AuthContext{
				PrincipalKind: grant.PrincipalKind,
				PrincipalID:   grant.PrincipalID,
				VerifiedNode:  grant.VerifiedNode,
				GrantID:       grant.ID,
				Scope:         &scopeCopy,
				FullAuthority: false,
			})
		}
		return AuthContext{}, false, "missing or invalid bearer token"
	}

	if service.token == "" {
		return finalize(AuthContext{
			PrincipalKind: protocol.PrincipalKindLocal,
			PrincipalID:   "local",
			FullAuthority: true,
		})
	}
	return AuthContext{}, false, "missing or invalid bearer token"
}

func (service *Service) requireBearer(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		auth, authorized, reason := service.authorizeRequest(request)
		if !authorized {
			writeJSONError(writer, http.StatusUnauthorized, "unauthorized", reason)
			return
		}
		next(writer, request.WithContext(withAuthContext(request.Context(), auth)))
	}
}

func (service *Service) requireClusterAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		auth, authorized, reason := service.authorizeRequest(request)
		if !authorized {
			writeJSONError(writer, http.StatusUnauthorized, "unauthorized", reason)
			return
		}
		if !auth.FullAuthority {
			writeJSONError(writer, http.StatusForbidden, "forbidden", "scoped access grant cannot access cluster administration endpoints")
			return
		}
		next(writer, request.WithContext(withAuthContext(request.Context(), auth)))
	}
}

func (service *Service) requireBearerHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		auth, authorized, reason := service.authorizeRequest(request)
		if !authorized {
			writeJSONError(writer, http.StatusUnauthorized, "unauthorized", reason)
			return
		}
		next.ServeHTTP(writer, request.WithContext(withAuthContext(request.Context(), auth)))
	})
}

func (service *Service) handleHealth(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	serviceName := "orchestra-master"
	if service.mode == ServiceModeRelay {
		serviceName = "orchestra-relay"
	}
	writeJSONResponse(writer, http.StatusOK, map[string]any{
		"ok":              true,
		"service":         serviceName,
		"mode":            service.mode,
		"version":         protocol.EffectiveBuildVersion(),
		"protocolVersion": protocol.ProtocolVersion,
		"hostName":        service.hostName,
		"authMode":        service.authMode,
	})
}

func (service *Service) handleControlAgentUpgrade(writer http.ResponseWriter, request *http.Request) {
	auth, authorized, reason := service.authorizeRequest(request)
	if !authorized {
		writeJSONError(writer, http.StatusUnauthorized, "unauthorized", reason)
		return
	}
	if !auth.FullAuthority {
		writeJSONError(writer, http.StatusForbidden, "forbidden", "scoped access grants cannot enroll slave nodes")
		return
	}
	connection, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	service.backgroundGroup.Add(1)
	defer service.backgroundGroup.Done()
	service.serveSlaveConnection(withAuthContext(request.Context(), auth), connection)
}

func (service *Service) handleOrchestratorClientUpgrade(writer http.ResponseWriter, request *http.Request) {
	auth, authorized, reason := service.authorizeRequest(request)
	if !authorized {
		writeJSONError(writer, http.StatusUnauthorized, "unauthorized", reason)
		return
	}
	connection, err := websocket.Accept(writer, request, nil)
	if err != nil {
		return
	}
	service.backgroundGroup.Add(1)
	defer service.backgroundGroup.Done()
	service.serveOrchestratorClientConnection(withAuthContext(request.Context(), auth), connection)
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
			service.logger.Info(
				"client",
				"disconnected",
				activitylog.String("client", clientID),
				activitylog.String("session", sessionID),
			)
		}
		_ = connection.Close(websocket.StatusNormalClosure, "closed")
	}()

	var mailboxPointer *protocol.AgentMailbox
	mailboxAddress := ""
	if registeredMailbox, regErr := service.RegisterClientMailbox(clientID, sessionID, registration.Mail); regErr == nil {
		mailboxPointer = &registeredMailbox
		mailboxAddress = registeredMailbox.Address
	}

	if err := session.send(sessionContext, protocol.OrchestratorServerMessage{
		Type:            "welcome",
		ProtocolVersion: protocol.ProtocolVersion,
		ClientID:        clientID,
		SessionID:       sessionID,
		Mailbox:         mailboxPointer,
		Scopes:          service.toolNames(),
		At:              service.nowUTC(),
	}); err != nil {
		return
	}

	harnessLabel := ""
	if registration.Mail != nil {
		harnessLabel = registration.Mail.Harness
	}
	service.logger.Info(
		"client",
		"connected",
		activitylog.String("client", clientID),
		activitylog.String("harness", harnessLabel),
		activitylog.String("mail", mailboxAddress),
	)

	caller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  clientID,
		SessionID: sessionID,
	}
	if auth, ok := authContextFromContext(requestContext); ok {
		caller = callerWithAuthContext(caller, auth)
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
				identifiedHarness := ""
				if message.Mail != nil {
					identifiedHarness = message.Mail.Harness
				}
				service.logger.Info(
					"client",
					"identified",
					activitylog.String("client", clientID),
					activitylog.String("harness", identifiedHarness),
					activitylog.String("mail", identifiedMailbox.Address),
				)
			}
			_ = session.send(sessionContext, response)
		case "tool.call":
			requestID := message.RequestID
			toolName := message.Tool
			arguments := slices.Clone(message.Arguments)
			go func() {
				startedAt := service.now()
				resultBytes, callErr := service.ExecuteOrchestratorTool(sessionContext, caller, toolName, arguments)
				response := protocol.OrchestratorServerMessage{
					Type:      "tool.result",
					RequestID: requestID,
					At:        service.nowUTC(),
				}
				if callErr != nil {
					response.Error = toHubRPCError(callErr)
					service.logger.Debug(
						"client",
						"tool failed",
						activitylog.String("client", clientID),
						activitylog.String("tool", toolName),
						activitylog.String("error", response.Error.Message),
						activitylog.Duration("duration", service.now().Sub(startedAt)),
					)
				} else {
					response.Result = resultBytes
					service.logger.Debug(
						"client",
						"tool completed",
						activitylog.String("client", clientID),
						activitylog.String("tool", toolName),
						activitylog.Duration("duration", service.now().Sub(startedAt)),
					)
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
	if err := requireClusterAdminAuthority(operatorCallerFromRequest(request), "status"); err != nil {
		writeOperationError(writer, err)
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
	if err := requireClusterAdminAuthority(operatorCallerFromRequest(request), "nodes"); err != nil {
		writeOperationError(writer, err)
		return
	}
	nodes, err := service.ListNodeDetails()
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, map[string]any{"nodes": nodes})
}

func (service *Service) handleAPITokens(writer http.ResponseWriter, request *http.Request) {
	caller := operatorCallerFromRequest(request)
	switch request.Method {
	case http.MethodGet:
		grants, err := service.ListAccessGrants(caller)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, map[string]any{"grants": grants})
	case http.MethodPost:
		var input CreateAccessGrantInput
		if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
		result, err := service.CreateAccessGrant(caller, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusCreated, result)
	default:
		writer.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (service *Service) handleAPITokenSubroutes(writer http.ResponseWriter, request *http.Request) {
	remainder := strings.TrimPrefix(request.URL.Path, "/api/tokens/")
	remainder = strings.TrimSuffix(remainder, "/")
	grantID, action, hasAction := strings.Cut(remainder, "/")
	caller := operatorCallerFromRequest(request)
	if (!hasAction && request.Method == http.MethodDelete) || (hasAction && action == "revoke" && request.Method == http.MethodPost) {
		revoked, err := service.RevokeAccessGrant(caller, grantID)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, map[string]any{"grant": revoked})
		return
	}
	writer.WriteHeader(http.StatusNotFound)
}

func (service *Service) handleAPIThreads(writer http.ResponseWriter, request *http.Request) {
	operatorCaller := operatorCallerFromRequest(request)
	switch request.Method {
	case http.MethodGet:
		if err := requireScopeOperation(operatorCaller, protocol.AccessOpRead); err != nil {
			writeOperationError(writer, err)
			return
		}
		statusFilter := request.URL.Query().Get("status")
		limit := 100
		if rawLimit := request.URL.Query().Get("limit"); rawLimit != "" {
			if parsed, err := strconv.Atoi(rawLimit); err == nil {
				limit = parsed
			}
		}
		labels := request.URL.Query()["label"]
		threads, err := service.ListThreads(ListThreadsInput{Status: statusFilter, Labels: labels, Limit: limit})
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		if operatorCaller.Scope != nil && len(operatorCaller.Scope.ThreadIDs) > 0 {
			filtered := make([]protocol.Thread, 0, len(threads))
			for _, thread := range threads {
				if protocol.ScopeAllowsThread(operatorCaller.Scope, thread.ID) {
					filtered = append(filtered, thread)
				}
			}
			threads = filtered
		}
		writeJSONResponse(writer, http.StatusOK, map[string]any{"threads": threads})
	case http.MethodPost:
		var input CreateThreadInput
		if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
		thread, attachment, err := service.CreateThread(operatorCaller, input)
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
	operatorCaller := operatorCallerFromRequest(request)
	if !hasSubroute {
		switch request.Method {
		case http.MethodGet:
			if err := requireScopeOperation(operatorCaller, protocol.AccessOpRead); err != nil {
				writeOperationError(writer, err)
				return
			}
			if err := requireScopeThread(operatorCaller, threadID); err != nil {
				writeOperationError(writer, err)
				return
			}
			detail, err := service.GetThreadDetail(threadID)
			if err != nil {
				writeOperationError(writer, err)
				return
			}
			writeJSONResponse(writer, http.StatusOK, detail)
		case http.MethodPatch:
			var input UpdateThreadInput
			if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil {
				writeOperationError(writer, invalidRequest("malformed JSON body"))
				return
			}
			input.ThreadID = threadID
			updated, err := service.UpdateThread(operatorCaller, input)
			if err != nil {
				writeOperationError(writer, err)
				return
			}
			writeJSONResponse(writer, http.StatusOK, map[string]any{"thread": updated})
		default:
			writer.WriteHeader(http.StatusMethodNotAllowed)
		}
		return
	}

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
			Channel:       strings.TrimSpace(request.URL.Query().Get("channel")),
			AfterSequence: afterSequence,
			Kinds:         kinds,
			Labels:        request.URL.Query()["label"],
			Limit:         limit,
		})
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, result)
	case "room/wait":
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		query := request.URL.Query()
		var afterSequence *int64
		if rawAfter := strings.TrimSpace(query.Get("after")); rawAfter != "" {
			parsedAfter, parseErr := strconv.ParseInt(rawAfter, 10, 64)
			if parseErr != nil {
				writeOperationError(writer, invalidRequest("after must be an integer"))
				return
			}
			afterSequence = &parsedAfter
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
		timeoutMilliseconds := protocol.MaximumWaitMilliseconds
		if rawTimeout := strings.TrimSpace(query.Get("timeoutMs")); rawTimeout != "" {
			parsedTimeout, parseErr := strconv.Atoi(rawTimeout)
			if parseErr != nil {
				writeOperationError(writer, invalidRequest("timeoutMs must be an integer"))
				return
			}
			timeoutMilliseconds = parsedTimeout
		}
		var kinds []string
		for _, rawKind := range query["kind"] {
			for part := range strings.SplitSeq(rawKind, ",") {
				trimmed := strings.TrimSpace(part)
				if trimmed != "" {
					kinds = append(kinds, trimmed)
				}
			}
		}
		result, err := service.WaitRoom(request.Context(), operatorCaller, WaitRoomInput{
			ThreadID:            threadID,
			Channel:             strings.TrimSpace(query.Get("channel")),
			AfterSequence:       afterSequence,
			EventCursor:         strings.TrimSpace(query.Get("cursor")),
			Kinds:               kinds,
			Labels:              query["label"],
			Mentions:            strings.TrimSpace(query.Get("mentions")),
			TimeoutMilliseconds: timeoutMilliseconds,
			Limit:               limit,
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
		if service.mode == ServiceModeRelay {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
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
	case "tasks":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var input SubmitTasksInput
		if err := json.NewDecoder(io.LimitReader(request.Body, 512*1024)).Decode(&input); err != nil {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
		input.ThreadID = threadID
		result, err := service.SubmitTasks(operatorCaller, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		statusCode := http.StatusCreated
		if result.Replayed {
			statusCode = http.StatusOK
		}
		writeJSONResponse(writer, statusCode, result)
	case "tasks/claim":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var input ClaimTaskInput
		if request.Body != nil {
			if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
				writeOperationError(writer, invalidRequest("malformed JSON body"))
				return
			}
		}
		input.ThreadID = threadID
		result, err := service.ClaimTask(request.Context(), operatorCaller, input)
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
	remainder = strings.TrimSuffix(remainder, "/")
	operatorCaller := operatorCallerFromRequest(request)
	if remainder == "claim" {
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var input ClaimTaskInput
		if request.Body != nil {
			if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
				writeOperationError(writer, invalidRequest("malformed JSON body"))
				return
			}
		}
		result, err := service.ClaimTask(request.Context(), operatorCaller, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, result)
		return
	}
	taskID, action, hasAction := strings.Cut(remainder, "/")
	if !hasAction {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	switch action {
	case "claim":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var input ClaimTaskInput
		if request.Body != nil {
			if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
				writeOperationError(writer, invalidRequest("malformed JSON body"))
				return
			}
		}
		if strings.TrimSpace(input.TaskID) != "" && strings.TrimSpace(input.TaskID) != taskID {
			writeOperationError(writer, invalidRequest("taskId in body does not match URL path"))
			return
		}
		input.TaskID = taskID
		result, err := service.ClaimTask(request.Context(), operatorCaller, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, result)
	case "claim/renew":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var input RenewTaskClaimInput
		if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
		if strings.TrimSpace(input.TaskID) != "" && strings.TrimSpace(input.TaskID) != taskID {
			writeOperationError(writer, invalidRequest("taskId in body does not match URL path"))
			return
		}
		input.TaskID = taskID
		result, err := service.RenewTaskClaim(operatorCaller, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, result)
	case "claim/release":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var input ReleaseTaskClaimInput
		if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
		if strings.TrimSpace(input.TaskID) != "" && strings.TrimSpace(input.TaskID) != taskID {
			writeOperationError(writer, invalidRequest("taskId in body does not match URL path"))
			return
		}
		input.TaskID = taskID
		result, err := service.ReleaseTaskClaim(operatorCaller, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, result)
	case "update":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var input UpdateTaskInput
		if err := json.NewDecoder(io.LimitReader(request.Body, 128*1024)).Decode(&input); err != nil {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
		if strings.TrimSpace(input.TaskID) != "" && strings.TrimSpace(input.TaskID) != taskID {
			writeOperationError(writer, invalidRequest("taskId in body does not match URL path"))
			return
		}
		input.TaskID = taskID
		updated, err := service.UpdateTask(operatorCaller, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, map[string]any{"task": updated})
	case "cancel":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if err := requireScopeOperation(operatorCaller, protocol.AccessOpDelegate); err != nil {
			writeOperationError(writer, err)
			return
		}
		if operatorCaller.Scope != nil {
			var taskThreadID string
			var foundTask bool
			_ = service.store.Read(func(state *store.State) error {
				if task, exists := state.Tasks[taskID]; exists {
					taskThreadID = task.ThreadID
					foundTask = true
				}
				return nil
			})
			if foundTask {
				if err := requireScopeThread(operatorCaller, taskThreadID); err != nil {
					writeOperationError(writer, err)
					return
				}
			}
		}
		result, err := service.CancelTarget(taskID)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, result)
	case "unblock":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var input UnblockTaskInput
		if request.Body != nil {
			if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
				writeOperationError(writer, invalidRequest("malformed JSON body"))
				return
			}
		}
		if strings.TrimSpace(input.TaskID) != "" && strings.TrimSpace(input.TaskID) != taskID {
			writeOperationError(writer, invalidRequest("taskId in body does not match URL path"))
			return
		}
		input.TaskID = taskID
		result, err := service.UnblockTask(operatorCaller, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, result)
	case "reopen":
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var input ReopenTaskInput
		if request.Body != nil {
			if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
				writeOperationError(writer, invalidRequest("malformed JSON body"))
				return
			}
		}
		if strings.TrimSpace(input.TaskID) != "" && strings.TrimSpace(input.TaskID) != taskID {
			writeOperationError(writer, invalidRequest("taskId in body does not match URL path"))
			return
		}
		input.TaskID = taskID
		result, err := service.ReopenTask(operatorCaller, input)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSONResponse(writer, http.StatusOK, result)
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
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
	if service.tailscaleVerifier != nil {
		if tailscaleauth.IsBrowserRequest(request) {
			writeJSONError(writer, http.StatusUnauthorized, "unauthorized", "browser-origin requests are not permitted in tailscale auth mode")
			return
		}
		if tailscaleauth.IsProxiedOrFunnelRequest(request) {
			writeJSONError(writer, http.StatusUnauthorized, "unauthorized", "proxied or Tailscale Funnel requests are not permitted in tailscale auth mode")
			return
		}
	}
	switch request.Method {
	case http.MethodPut:
		auth, authorized, _ := service.authorizeRequest(request)
		bearerAuthorized := authorized && auth.FullAuthority
		if authorized && !auth.FullAuthority && protocol.ScopeAllowsOperation(auth.Scope, protocol.AccessOpPost) {
			_ = service.store.Read(func(state *store.State) error {
				if artifact, exists := state.Artifacts[artifactID]; exists && protocol.ScopeAllowsThread(auth.Scope, artifact.ThreadID) {
					bearerAuthorized = true
				}
				return nil
			})
		}
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
		auth, authorized, reason := service.authorizeRequest(request)
		if !authorized {
			writeJSONError(writer, http.StatusUnauthorized, "unauthorized", reason)
			return
		}
		if !auth.FullAuthority {
			if !protocol.ScopeAllowsOperation(auth.Scope, protocol.AccessOpRead) {
				writeJSONError(writer, http.StatusForbidden, "forbidden", "access grant does not permit reading artifacts")
				return
			}
			var allowed bool
			var exists bool
			_ = service.store.Read(func(state *store.State) error {
				if artifact, ok := state.Artifacts[artifactID]; ok {
					exists = true
					allowed = protocol.ScopeAllowsThread(auth.Scope, artifact.ThreadID)
				}
				return nil
			})
			if exists && !allowed {
				writeJSONError(writer, http.StatusForbidden, "forbidden", "access grant does not permit reading artifacts from this thread")
				return
			}
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
	if !hasAction {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	switch action {
	case "cleanup":
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
	case "land":
		service.HandleAPIWorkspaceLeaseLand(writer, request, leaseID)
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

func (service *Service) handleAPIMailAgents(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := requireScopeOperation(operatorCallerFromRequest(request), protocol.AccessOpRead); err != nil {
		writeOperationError(writer, err)
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

func (service *Service) handleAPIMailTraffic(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	limit := 0
	if rawLimit := strings.TrimSpace(request.URL.Query().Get("limit")); rawLimit != "" {
		parsedLimit, err := strconv.Atoi(rawLimit)
		if err != nil {
			writeOperationError(writer, invalidRequest("limit must be an integer"))
			return
		}
		limit = parsedLimit
	}
	result, err := service.ListMailTraffic(ListMailTrafficInput{
		Address: request.URL.Query().Get("address"),
		Limit:   limit,
	})
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, result)
}

func (service *Service) handleAPIMailMessages(writer http.ResponseWriter, request *http.Request) {
	operatorCaller := operatorCallerFromRequest(request)
	switch request.Method {
	case http.MethodGet:
		if err := requireScopeOperation(operatorCaller, protocol.AccessOpRead); err != nil {
			writeOperationError(writer, err)
			return
		}
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
		if err := requireScopeOperation(operatorCaller, protocol.AccessOpPost); err != nil {
			writeOperationError(writer, err)
			return
		}
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
		if len(operationError.Detail) > 0 {
			writeJSONResponse(writer, statusCode, map[string]any{
				"error": map[string]any{
					"code":    operationError.Code,
					"message": operationError.Message,
					"detail":  operationError.Detail,
				},
			})
			return
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
	return parseServerConfiguration(ServiceModeOrchestrator, arguments, lookupEnvironment)
}

func parseServerConfiguration(
	mode ServiceMode,
	arguments []string,
	rawLookupEnvironment func(string) (string, bool),
) (Configuration, error) {
	defaultConnectionFile := localstate.ResolveConnectionFilePath(rawLookupEnvironment)
	lookupEnvironment := rawLookupEnvironment
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}

	environmentListen := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_LISTEN")
	defaultListen := environmentListen
	if defaultListen == "" {
		defaultListen = DefaultListenAddress
	}
	defaultAuthModeRaw := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_AUTH_MODE")
	if defaultAuthModeRaw == "" {
		defaultAuthModeRaw = tailscaleauth.AuthModeToken
	}
	defaultToken := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_TOKEN")
	defaultTailscaleBinary := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_TAILSCALE_BINARY")
	environmentDataDirectory := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_DATA_DIR", "ORCHESTRA_DATA_ROOT")
	defaultDataDirectory := environmentDataDirectory
	if defaultDataDirectory == "" {
		if mode == ServiceModeRelay {
			defaultDataDirectory = filepath.Join(setup.DefaultDataRoot(), "relay")
		} else {
			defaultDataDirectory = filepath.Join(setup.DefaultDataRoot(), "master")
		}
	}
	defaultTLSCert := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_TLS_CERT")
	defaultTLSKey := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_TLS_KEY")
	defaultProfiles := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_PROJECT_PROFILES")
	defaultWithLocal := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_WITH_LOCAL_WORKER") == "1" ||
		strings.EqualFold(firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_WITH_LOCAL_WORKER"), "true")
	defaultVerbose := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_VERBOSE") == "1" ||
		strings.EqualFold(firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_VERBOSE"), "true")
	defaultLogLevelRaw := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_LOG_LEVEL")
	defaultLogFormatRaw := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_LOG_FORMAT")

	var workspaceRootsFlag repeatStringFlag
	var approvalPolicyFlag repeatStringFlag
	var acpAdapterFlag repeatStringFlag
	var workerLabelsFlag repeatStringFlag
	var tailscaleAllowUsersFlag repeatStringFlag
	var tailscaleAllowTagsFlag repeatStringFlag

	flagSetName := "orchestra master"
	subcommand := localstate.SubcommandMaster
	if mode == ServiceModeRelay {
		flagSetName = "orchestra relay"
		subcommand = localstate.SubcommandRelay
	}
	flagSet := flag.NewFlagSet(flagSetName, flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)

	var configuration Configuration
	configuration.Mode = mode
	var configFileFlag string
	var profileFlag string
	var authModeRaw string
	var workerName string
	var workerID string
	var workerConcurrency int
	var workerInstanceCapacity int
	var claudeAuthMode string
	var verbose bool
	var logLevelRaw string
	var logFormatRaw string

	flagSet.StringVar(&configFileFlag, "config", "", "Path to Orchestra TOML config file (default ~/.orchestra/config.toml)")
	flagSet.StringVar(&profileFlag, "profile", "", "Configuration profile name in config.toml (default \"default\")")
	flagSet.StringVar(&configuration.ListenAddress, "listen", defaultListen, "HTTP/WebSocket listen address (e.g. 127.0.0.1:8787)")
	flagSet.StringVar(&authModeRaw, "auth", defaultAuthModeRaw, "Authentication mode: token or tailscale (env ORCHESTRA_AUTH_MODE)")
	flagSet.StringVar(&configuration.Token, "token", defaultToken, "Bearer token matching ORCHESTRA_TOKEN (auto-persisted in <data-dir>/token in token auth mode)")
	flagSet.StringVar(&configuration.TailscaleBinary, "tailscale-binary", defaultTailscaleBinary, "Explicit path to the tailscale CLI binary")
	flagSet.Var(&tailscaleAllowUsersFlag, "tailscale-allow-user", "Allowed Tailscale user login in tailscale auth mode (repeatable)")
	flagSet.Var(&tailscaleAllowTagsFlag, "tailscale-allow-tag", "Allowed Tailscale ACL tag in tailscale auth mode (repeatable, e.g. tag:orchestra)")
	flagSet.StringVar(&configuration.DataDirectory, "data-dir", defaultDataDirectory, "SQLite state and artifact storage directory")
	flagSet.StringVar(&configuration.ConnectionFilePath, "connection-file", defaultConnectionFile, "Local connection discovery state file path")
	flagSet.StringVar(&configuration.TLSCertPath, "tls-cert", defaultTLSCert, "Optional TLS certificate PEM path for wss:// / https://")
	flagSet.StringVar(&configuration.TLSKeyPath, "tls-key", defaultTLSKey, "Optional TLS private key PEM path for wss:// / https://")
	if mode == ServiceModeOrchestrator {
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
	}
	flagSet.BoolVar(&verbose, "verbose", defaultVerbose, "Enable debug-level operational logs")
	flagSet.StringVar(&logLevelRaw, "log-level", defaultLogLevelRaw, "Operational log level: debug, info, warn, error, or quiet")
	flagSet.StringVar(&logFormatRaw, "log-format", defaultLogFormatRaw, "Operational log format: text or json")

	if err := flagSet.Parse(arguments); err != nil {
		return Configuration{}, err
	}
	if flagSet.NArg() > 0 {
		return Configuration{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flagSet.Args(), " "))
	}

	cliFlags := map[string]bool{}
	flagSet.Visit(func(visited *flag.Flag) {
		cliFlags[visited.Name] = true
	})

	appliedProfile, profileErr := localstate.ApplyProfileToFlagSet(flagSet, localstate.ProfileOptions{
		Subcommand:        subcommand,
		ConfigFlag:        configFileFlag,
		ConfigFlagSet:     cliFlags["config"],
		ProfileFlag:       profileFlag,
		ProfileFlagSet:    cliFlags["profile"],
		LookupEnvironment: rawLookupEnvironment,
	})
	if profileErr != nil {
		return Configuration{}, profileErr
	}
	configuration.ConfigFilePath = appliedProfile.ConfigFilePath
	configuration.Profile = appliedProfile.Profile

	explicitFlags := map[string]bool{}
	flagSet.Visit(func(visited *flag.Flag) {
		explicitFlags[visited.Name] = true
	})

	parsedAuthMode, authErr := tailscaleauth.ParseAuthMode(authModeRaw)
	if authErr != nil {
		return Configuration{}, authErr
	}
	configuration.AuthMode = parsedAuthMode
	configuration.ListenExplicit = explicitFlags["listen"] || environmentListen != ""
	configuration.TokenExplicit = explicitFlags["token"] || defaultToken != ""

	if !tailscaleAllowUsersFlag.set {
		if rawUsers := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_TAILSCALE_ALLOW_USERS"); rawUsers != "" {
			_ = tailscaleAllowUsersFlag.Set(rawUsers)
		}
	}
	if !tailscaleAllowTagsFlag.set {
		if rawTags := firstMasterEnvironment(lookupEnvironment, "ORCHESTRA_TAILSCALE_ALLOW_TAGS"); rawTags != "" {
			_ = tailscaleAllowTagsFlag.Set(rawTags)
		}
	}
	configuration.TailscaleAllowUsers = tailscaleAllowUsersFlag.values
	configuration.TailscaleAllowTags = tailscaleAllowTagsFlag.values

	if (configuration.TLSCertPath == "") != (configuration.TLSKeyPath == "") {
		return Configuration{}, errors.New("--tls-cert and --tls-key must be provided together")
	}

	parsedLevel, levelErr := activitylog.ParseLevel(logLevelRaw)
	if levelErr != nil {
		return Configuration{}, levelErr
	}
	if verbose && logLevelRaw == "" {
		parsedLevel = activitylog.LevelDebug
	}
	parsedFormat, formatErr := activitylog.ParseFormat(logFormatRaw)
	if formatErr != nil {
		return Configuration{}, formatErr
	}
	configuration.LogLevel = parsedLevel
	configuration.LogFormat = parsedFormat

	if mode == ServiceModeOrchestrator && configuration.WithLocalWorker {
		var workerArgs []string
		if appliedProfile.ConfigFilePath != "" {
			workerArgs = append(workerArgs, "--config", appliedProfile.ConfigFilePath, "--profile", appliedProfile.Profile)
		} else {
			workerArgs = append(workerArgs, "--config", "none")
		}
		if configuration.Token != "" {
			workerArgs = append(workerArgs, "--token", configuration.Token)
		}
		if workerName != "" {
			workerArgs = append(workerArgs, "--name", workerName)
		}
		if workerID != "" {
			workerArgs = append(workerArgs, "--id", workerID)
		}
		if explicitFlags["worker-concurrency"] {
			workerArgs = append(workerArgs, "--concurrency", strconv.Itoa(workerConcurrency))
		}
		if explicitFlags["worker-instance-capacity"] {
			workerArgs = append(workerArgs, "--instance-capacity", strconv.Itoa(workerInstanceCapacity))
		}
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
		workerConfig, workerErr := slave.ParseConfiguration(workerArgs, rawLookupEnvironment)
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

func ParseRelayConfiguration(arguments []string, lookupEnvironment func(string) (string, bool)) (Configuration, error) {
	for _, disallowed := range []string{
		"project-profiles",
		"with-local-worker",
		"worker-name",
		"worker-id",
		"worker-concurrency",
		"worker-instance-capacity",
		"workspace-root",
		"worker-label",
		"approval-policy",
		"acp-adapter",
		"claude-acp-auth-mode",
	} {
		if commandLineHasFlag(arguments, disallowed) {
			return Configuration{}, fmt.Errorf("--%s is unavailable in relay mode", disallowed)
		}
	}
	effectiveLookup := lookupEnvironment
	if effectiveLookup == nil {
		effectiveLookup = os.LookupEnv
	}
	if firstMasterEnvironment(effectiveLookup, "ORCHESTRA_WITH_LOCAL_WORKER") == "1" ||
		strings.EqualFold(firstMasterEnvironment(effectiveLookup, "ORCHESTRA_WITH_LOCAL_WORKER"), "true") {
		return Configuration{}, errors.New("embedded workers are unavailable in relay mode")
	}
	if firstMasterEnvironment(effectiveLookup, "ORCHESTRA_PROJECT_PROFILES") != "" {
		return Configuration{}, errors.New("project profiles are unavailable in relay mode")
	}
	return parseServerConfiguration(ServiceModeRelay, arguments, lookupEnvironment)
}

func commandLineHasFlag(arguments []string, name string) bool {
	prefix := "--" + name
	for _, argument := range arguments {
		if argument == prefix || strings.HasPrefix(argument, prefix+"=") {
			return true
		}
	}
	return false
}

type RunningMaster struct {
	Service            *Service
	Store              *store.Store
	Address            string
	BaseURL            string
	AuthMode           string
	TailscaleSelf      tailscaleauth.SelfIdentity
	Token              string
	TokenPath          string
	TokenGenerated     bool
	ConnectionFilePath string
	Wait               func() error
	Shutdown           func(context.Context) error
}

func StartMaster(operationContext context.Context, configuration Configuration) (*RunningMaster, error) {
	configuration.Mode = ServiceModeOrchestrator
	return startRuntime(operationContext, configuration)
}

func StartRelay(operationContext context.Context, configuration Configuration) (*RunningMaster, error) {
	if configuration.WithLocalWorker {
		return nil, errors.New("embedded workers are unavailable in relay mode")
	}
	if strings.TrimSpace(configuration.ProjectProfilePath) != "" {
		return nil, errors.New("project profiles are unavailable in relay mode")
	}
	configuration.Mode = ServiceModeRelay
	return startRuntime(operationContext, configuration)
}

func startRuntime(operationContext context.Context, configuration Configuration) (*RunningMaster, error) {
	authMode, authErr := tailscaleauth.ParseAuthMode(configuration.AuthMode)
	if authErr != nil {
		return nil, authErr
	}
	configuration.AuthMode = authMode

	var verifier *tailscaleauth.Verifier
	var tailscaleSelf tailscaleauth.SelfIdentity
	var tokenPath string
	var tokenGenerated bool

	if authMode == tailscaleauth.AuthModeTailscale {
		verifier = configuration.TailscaleVerifier
		if verifier == nil {
			builtVerifier, verifierErr := tailscaleauth.NewVerifier(operationContext, tailscaleauth.Options{
				BinaryPath:   configuration.TailscaleBinary,
				AllowedUsers: configuration.TailscaleAllowUsers,
				AllowedTags:  configuration.TailscaleAllowTags,
			})
			if verifierErr != nil {
				return nil, verifierErr
			}
			verifier = builtVerifier
		}
		tailscaleSelf = verifier.Self()
		if (strings.TrimSpace(configuration.ListenAddress) == "" ||
			(!configuration.ListenExplicit && configuration.ListenAddress == DefaultListenAddress)) &&
			tailscaleSelf.PrimaryIPv4 != "" {
			configuration.ListenAddress = net.JoinHostPort(tailscaleSelf.PrimaryIPv4, "8787")
		}
		if validateErr := tailscaleauth.ValidateListenAddress(configuration.ListenAddress, tailscaleSelf.TailscaleIPs); validateErr != nil {
			return nil, validateErr
		}
		if configuration.TokenExplicit || strings.TrimSpace(configuration.Token) != "" {
			resolvedToken, resolvedPath, generated, tokenErr := localstate.EnsureMasterToken(configuration.DataDirectory, configuration.Token)
			if tokenErr != nil {
				return nil, tokenErr
			}
			configuration.Token = resolvedToken
			tokenPath = resolvedPath
			tokenGenerated = generated
		} else {
			configuration.Token = ""
		}
	} else {
		resolvedToken, resolvedPath, generated, tokenErr := localstate.EnsureMasterToken(configuration.DataDirectory, configuration.Token)
		if tokenErr != nil {
			return nil, tokenErr
		}
		configuration.Token = resolvedToken
		tokenPath = resolvedPath
		tokenGenerated = generated
	}
	if configuration.Logger != nil {
		configuration.Logger = configuration.Logger.WithSecrets(configuration.Token)
	}

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
		Mode:              configuration.Mode,
		Store:             openedStore,
		Token:             configuration.Token,
		TailscaleVerifier: verifier,
		ProjectProfiles:   profiles,
		Logger:            configuration.Logger,
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
	localDialURL := localstate.LocalDialEndpoint(scheme, boundAddress)

	writtenConnectionFile := ""
	if !localstate.IsDisabledConnectionFilePath(configuration.ConnectionFilePath) {
		writeErr := localstate.WriteConnectionState(configuration.ConnectionFilePath, localstate.ConnectionState{
			Endpoint:      localDialURL,
			AuthMode:      authMode,
			Token:         configuration.Token,
			TLSInsecure:   tlsConfiguration != nil,
			DataDirectory: configuration.DataDirectory,
			ProcessID:     os.Getpid(),
		})
		if writeErr != nil {
			service.logger.Warn("config", "failed writing local connection state", activitylog.String("path", configuration.ConnectionFilePath), activitylog.String("error", writeErr.Error()))
		} else {
			writtenConnectionFile = configuration.ConnectionFilePath
		}
	}

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
	if configuration.Mode == ServiceModeOrchestrator && configuration.WithLocalWorker {
		localConfig := configuration.LocalWorker
		localConfig.ControlEndpoint = localDialURL
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
		if configuration.Logger != nil {
			if configuration.Logger.Enabled(activitylog.LevelDebug) {
				localConfig.ActivityLogger = configuration.Logger.WithComponent("worker")
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
		for _, warning := range runtimeWorker.Warnings {
			service.logger.Warn("worker", warning)
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
			component := "master"
			if configuration.Mode == ServiceModeRelay {
				component = "relay"
			}
			service.logger.Info(component, "shutting down")
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
		Service:            service,
		Store:              openedStore,
		Address:            boundAddress,
		BaseURL:            localDialURL,
		AuthMode:           authMode,
		TailscaleSelf:      tailscaleSelf,
		Token:              configuration.Token,
		TokenPath:          tokenPath,
		TokenGenerated:     tokenGenerated,
		ConnectionFilePath: writtenConnectionFile,
		Wait:               func() error { return <-serverErrChannel },
		Shutdown:           shutdownFunction,
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

type WaitRoomInput struct {
	ThreadID            string   `json:"threadId"`
	Channel             string   `json:"channel,omitempty"`
	AfterSequence       *int64   `json:"afterSequence,omitempty"`
	EventCursor         string   `json:"eventCursor,omitempty"`
	Kinds               []string `json:"kinds,omitempty"`
	Labels              []string `json:"labels,omitempty"`
	Mentions            string   `json:"mentions,omitempty"`
	TimeoutMilliseconds int      `json:"timeoutMilliseconds,omitempty"`
	Limit               int      `json:"limit,omitempty"`
}

type WaitRoomResult struct {
	ThreadID        string                    `json:"threadId"`
	Channel         string                    `json:"channel,omitempty"`
	Messages        []protocol.RoomMessage    `json:"messages"`
	Events          []protocol.TaskEventEntry `json:"events"`
	HeadSequence    int64                     `json:"headSequence"`
	ThroughSequence int64                     `json:"throughSequence"`
	EventCursor     string                    `json:"eventCursor,omitempty"`
	CursorExpired   bool                      `json:"cursorExpired"`
	TimedOut        bool                      `json:"timedOut"`
}

type roomMentionFilter struct {
	active  bool
	all     bool
	channel string
	handle  string
}

func parseRoomMentionFilter(raw string) (roomMentionFilter, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return roomMentionFilter{}, nil
	}
	withoutAt := strings.TrimPrefix(trimmed, "@")
	if withoutAt == "" {
		return roomMentionFilter{}, invalidRequest(fmt.Sprintf("invalid room mention filter %q", raw))
	}
	if rawChannel, isChannel := strings.CutPrefix(withoutAt, "#"); isChannel {
		if strings.EqualFold(strings.TrimSpace(rawChannel), protocol.DefaultRoomChannel) {
			return roomMentionFilter{}, invalidRequest(protocol.ErrRoomMainChannelMention.Error())
		}
		normalizedChannel, err := protocol.NormalizeRoomChannel(rawChannel)
		if err != nil {
			return roomMentionFilter{}, invalidRequest(err.Error())
		}
		return roomMentionFilter{active: true, channel: normalizedChannel}, nil
	}
	if strings.EqualFold(withoutAt, "all") {
		return roomMentionFilter{active: true, all: true}, nil
	}
	if strings.Contains(withoutAt, "@") {
		parsedHandle, _, _, ok := protocol.ParseMailAddress(withoutAt)
		if !ok {
			return roomMentionFilter{}, invalidRequest(fmt.Sprintf("invalid room mention filter %q", raw))
		}
		if parsedHandle == "all" {
			return roomMentionFilter{active: true, all: true}, nil
		}
		return roomMentionFilter{active: true, handle: parsedHandle}, nil
	}
	normalizedHandle, ok := protocol.NormalizeMailHandle(withoutAt)
	if !ok {
		return roomMentionFilter{}, invalidRequest(fmt.Sprintf("invalid room mention filter %q", raw))
	}
	return roomMentionFilter{active: true, handle: normalizedHandle}, nil
}

func (filter roomMentionFilter) matches(entry protocol.RoomMessage) bool {
	if !filter.active {
		return true
	}
	parsed, err := protocol.ParseRoomMentions(entry.Body, entry.Mentions)
	if err != nil {
		return false
	}
	if filter.all {
		return parsed.All
	}
	if filter.channel != "" {
		return slices.Contains(parsed.Channels, filter.channel)
	}
	if parsed.All {
		return true
	}
	for _, mentionedHandle := range parsed.Handles {
		if protocol.MatchesHandleMention(filter.handle, mentionedHandle) {
			return true
		}
	}
	return false
}

func (service *Service) redactEventDetail(detail map[string]any) map[string]any {
	if len(detail) == 0 {
		return nil
	}
	redacted := make(map[string]any, len(detail))
	for key, value := range detail {
		if text, isString := value.(string); isString {
			redacted[key] = service.redactor.Redact(text)
			continue
		}
		redacted[key] = value
	}
	return redacted
}

func (service *Service) WaitRoom(
	operationContext context.Context,
	caller CallerIdentity,
	input WaitRoomInput,
) (WaitRoomResult, error) {
	threadID := strings.TrimSpace(input.ThreadID)
	if !protocol.IsIdentifier(threadID) {
		return WaitRoomResult{}, invalidRequest("threadId is required")
	}
	if err := requireScopeOperation(caller, protocol.AccessOpRead); err != nil {
		return WaitRoomResult{}, err
	}
	if err := requireScopeThread(caller, threadID); err != nil {
		return WaitRoomResult{}, err
	}
	readChannel, channelErr := protocol.NormalizeRoomReadChannel(input.Channel)
	if channelErr != nil {
		return WaitRoomResult{}, invalidRequest(channelErr.Error())
	}
	if input.AfterSequence != nil && *input.AfterSequence < 0 {
		return WaitRoomResult{}, invalidRequest("afterSequence must be greater than or equal to 0")
	}
	for _, kind := range input.Kinds {
		if !slices.Contains(protocol.RoomMessageKinds, kind) {
			return WaitRoomResult{}, invalidRequest(fmt.Sprintf("unknown room message kind %q", kind))
		}
	}
	labelFilters, labelFilterErr := protocol.ParseLabelFilters(input.Labels)
	if labelFilterErr != nil {
		return WaitRoomResult{}, invalidRequest(labelFilterErr.Error())
	}
	mentionFilter, mentionErr := parseRoomMentionFilter(input.Mentions)
	if mentionErr != nil {
		return WaitRoomResult{}, mentionErr
	}
	if input.Limit < 0 {
		return WaitRoomResult{}, invalidRequest("limit must be positive")
	}
	limit := input.Limit
	if limit == 0 {
		limit = protocol.DefaultRoomReadLimit
	}
	if limit > protocol.MaximumRoomReadLimit {
		limit = protocol.MaximumRoomReadLimit
	}
	waitMilliseconds := min(max(input.TimeoutMilliseconds, 0), protocol.MaximumWaitMilliseconds)

	readerKey := caller.SourceKey()
	if readerKey == "" {
		readerKey = "operator:cli"
	}
	rawEventCursor := strings.TrimSpace(input.EventCursor)
	includeThreadEvents := len(input.Kinds) == 0 && !mentionFilter.active

	eventChannel, unsubscribeEvents := service.store.SubscribeEvents()
	defer unsubscribeEvents()

	var roomBaselineSequence int64
	var eventBaselineSequence int64
	if err := service.store.Read(func(state *store.State) error {
		if _, exists := state.Threads[threadID]; !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}
		if input.AfterSequence != nil {
			roomBaselineSequence = *input.AfterSequence
		} else {
			roomBaselineSequence = state.RoomHeads[threadID].HeadSequence
		}
		if rawEventCursor != "" {
			decodedSequence, decodeErr := protocol.DecodeEventCursor(rawEventCursor, threadID, readerKey)
			if decodeErr != nil {
				return invalidRequest(decodeErr.Error())
			}
			eventBaselineSequence = decodedSequence
		} else {
			eventBaselineSequence = state.EventStreams[threadID].Head
		}
		return nil
	}); err != nil {
		return WaitRoomResult{}, err
	}

	collectSnapshot := func() (WaitRoomResult, bool, error) {
		snapshot := WaitRoomResult{
			ThreadID: threadID,
			Channel:  readChannel,
			Messages: []protocol.RoomMessage{},
			Events:   []protocol.TaskEventEntry{},
		}
		err := service.store.Read(func(state *store.State) error {
			if _, exists := state.Threads[threadID]; !exists {
				return notFound(fmt.Sprintf("thread %s does not exist", threadID))
			}

			roomHead := state.RoomHeads[threadID]
			snapshot.HeadSequence = roomHead.HeadSequence
			throughSequence := min(roomBaselineSequence, roomHead.HeadSequence)
			hasMoreMessages := false

			for _, entry := range state.RoomMessages[threadID] {
				if entry.Sequence <= roomBaselineSequence {
					continue
				}
				if !roomEntryMatchesReadChannel(entry, readChannel, caller.Kind, nil) {
					throughSequence = entry.Sequence
					continue
				}
				if !protocol.ScopeAllowsRoomChannel(caller.Scope, entry.Channel) {
					throughSequence = entry.Sequence
					continue
				}
				if len(input.Kinds) > 0 && !slices.Contains(input.Kinds, entry.Kind) {
					throughSequence = entry.Sequence
					continue
				}
				if !mentionFilter.matches(entry) {
					throughSequence = entry.Sequence
					continue
				}
				if !protocol.MatchesLabelFilters(entry.Labels, labelFilters) {
					throughSequence = entry.Sequence
					continue
				}
				if len(snapshot.Messages) >= limit {
					hasMoreMessages = true
					break
				}
				throughSequence = entry.Sequence
				entryCopy := entry
				entryCopy.Topic = service.redactor.Redact(entry.Topic)
				entryCopy.Body = service.redactor.Redact(entry.Body)
				entryCopy.Labels = protocol.CloneLabels(entry.Labels)
				entryCopy.ArtifactIDs = slices.Clone(entry.ArtifactIDs)
				entryCopy.Mentions = slices.Clone(entry.Mentions)
				snapshot.Messages = append(snapshot.Messages, entryCopy)
			}
			if !hasMoreMessages && roomHead.HeadSequence > throughSequence && roomBaselineSequence <= roomHead.HeadSequence {
				throughSequence = roomHead.HeadSequence
			}
			snapshot.ThroughSequence = throughSequence

			stream := state.EventStreams[threadID]
			if eventBaselineSequence < stream.Floor {
				nextCursor, cursorErr := protocol.EncodeEventCursor(threadID, readerKey, stream.Floor)
				if cursorErr != nil {
					return cursorErr
				}
				snapshot.EventCursor = nextCursor
				snapshot.CursorExpired = true
				return nil
			}

			lastScanned := eventBaselineSequence
			hasMoreEvents := false
			for _, event := range state.ThreadEvents[threadID] {
				if event.Sequence <= eventBaselineSequence {
					continue
				}
				if event.Type == "room.message_posted" || !includeThreadEvents {
					lastScanned = event.Sequence
					continue
				}
				if len(snapshot.Events) >= limit {
					hasMoreEvents = true
					break
				}
				lastScanned = event.Sequence
				eventCopy := event
				eventCopy.Summary = service.redactor.Redact(event.Summary)
				eventCopy.Detail = service.redactEventDetail(event.Detail)
				snapshot.Events = append(snapshot.Events, eventCopy)
			}
			if !hasMoreEvents && stream.Head > lastScanned {
				lastScanned = stream.Head
			}
			nextCursor, cursorErr := protocol.EncodeEventCursor(threadID, readerKey, lastScanned)
			if cursorErr != nil {
				return cursorErr
			}
			snapshot.EventCursor = nextCursor
			return nil
		})
		if err != nil {
			return WaitRoomResult{}, false, err
		}
		hasActivity := len(snapshot.Messages) > 0 || len(snapshot.Events) > 0 || snapshot.CursorExpired
		return snapshot, hasActivity, nil
	}

	initialSnapshot, hasActivity, err := collectSnapshot()
	if err != nil {
		return WaitRoomResult{}, err
	}
	if hasActivity {
		return initialSnapshot, nil
	}
	if waitMilliseconds == 0 {
		initialSnapshot.TimedOut = true
		return initialSnapshot, nil
	}

	waitTimer := time.NewTimer(time.Duration(waitMilliseconds) * time.Millisecond)
	defer waitTimer.Stop()

	for {
		select {
		case <-operationContext.Done():
			polled, woke, pollErr := collectSnapshot()
			if pollErr != nil {
				return WaitRoomResult{}, pollErr
			}
			polled.TimedOut = !woke
			return polled, nil
		case <-waitTimer.C:
			polled, woke, pollErr := collectSnapshot()
			if pollErr != nil {
				return WaitRoomResult{}, pollErr
			}
			polled.TimedOut = !woke
			return polled, nil
		case eventBatch, open := <-eventChannel:
			if !open {
				polled, woke, pollErr := collectSnapshot()
				if pollErr != nil {
					return WaitRoomResult{}, pollErr
				}
				polled.TimedOut = !woke
				return polled, nil
			}
			relevantThread := false
			for _, event := range eventBatch {
				if event.ThreadID == threadID {
					relevantThread = true
					break
				}
			}
			if !relevantThread {
				continue
			}
			polled, woke, pollErr := collectSnapshot()
			if pollErr != nil || woke {
				return polled, pollErr
			}
		}
	}
}
