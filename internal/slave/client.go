package slave

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/mcpserver"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/workspace"
)

const (
	maximumOutboxMessages = 4096
	heartbeatInterval     = 10 * time.Second
	writeTimeout          = 10 * time.Second
	readLimitBytes        = 16 * 1024 * 1024
)

type ClientOptions struct {
	Endpoint          string
	Token             string
	Node              protocol.ComputeNode
	Runner            *harness.Runner
	WorkspaceManager  *workspace.Manager
	CapabilityReport  *protocol.NodeCapabilityReport
	ComponentReport   *protocol.ComponentInventoryReport
	HTTPClient        *http.Client
	ReconnectMinDelay time.Duration
	ReconnectMaxDelay time.Duration
	Logger            func(string)
	Now               func() time.Time
}

type Client struct {
	endpoint          string
	token             string
	node              protocol.ComputeNode
	runner            *harness.Runner
	capabilityReport  *protocol.NodeCapabilityReport
	componentReport   *protocol.ComponentInventoryReport
	httpClient        *http.Client
	reconnectMinDelay time.Duration
	reconnectMaxDelay time.Duration
	loggerMutex       sync.RWMutex
	logger            func(string)
	now               func() time.Time

	mcpServer *mcpserver.Server
	leases    *leaseCoordinator

	rootContext context.Context
	waitGroup   sync.WaitGroup

	writeMutex sync.Mutex
	connection *websocket.Conn
	admitted   atomic.Bool

	mutex               sync.Mutex
	sessions            map[string]*runSession
	completedRuns       map[string]bool
	completedOrder      []string
	instances           map[string]*residentInstance
	releasedAllocations map[string]bool
	releasedOrder       []string
	outbox              []protocol.SlaveControlMessage

	rpcSequence atomic.Int64
	rpcMutex    sync.Mutex
	pendingRPC  map[string]chan protocol.MasterControlMessage
}

func NewClient(options ClientOptions) *Client {
	nowFunction := options.Now
	if nowFunction == nil {
		nowFunction = time.Now
	}
	minimumDelay := options.ReconnectMinDelay
	if minimumDelay <= 0 {
		minimumDelay = 100 * time.Millisecond
	}
	maximumDelay := options.ReconnectMaxDelay
	if maximumDelay <= 0 {
		maximumDelay = 5 * time.Second
	}
	if options.Node.Concurrency <= 0 {
		options.Node.Concurrency = DefaultConcurrency
	}
	client := &Client{
		endpoint:            strings.TrimSpace(options.Endpoint),
		token:               options.Token,
		node:                options.Node,
		runner:              options.Runner,
		capabilityReport:    options.CapabilityReport,
		componentReport:     options.ComponentReport,
		httpClient:          options.HTTPClient,
		reconnectMinDelay:   minimumDelay,
		reconnectMaxDelay:   maximumDelay,
		logger:              options.Logger,
		now:                 nowFunction,
		rootContext:         context.Background(),
		sessions:            map[string]*runSession{},
		completedRuns:       map[string]bool{},
		instances:           map[string]*residentInstance{},
		releasedAllocations: map[string]bool{},
		pendingRPC:          map[string]chan protocol.MasterControlMessage{},
	}
	client.leases = newLeaseCoordinator(client, options.WorkspaceManager)
	client.mcpServer = mcpserver.New(client.callHubRPC, client.uploadArtifactContent)
	return client
}

func (client *Client) SetLogger(logger func(string)) {
	client.loggerMutex.Lock()
	client.logger = logger
	client.loggerMutex.Unlock()
}

func (client *Client) logf(format string, arguments ...any) {
	client.loggerMutex.RLock()
	logger := client.logger
	client.loggerMutex.RUnlock()
	if logger == nil {
		return
	}
	logger(protocol.RedactText(fmt.Sprintf(format, arguments...)))
}

func (client *Client) Run(operationContext context.Context) error {
	client.mutex.Lock()
	client.rootContext = operationContext
	client.mutex.Unlock()

	if err := client.mcpServer.Start(operationContext); err != nil {
		return err
	}

	delay := client.reconnectMinDelay
	for {
		if operationContext.Err() != nil {
			client.shutdownSessions()
			client.waitGroup.Wait()
			return nil
		}

		admittedOnce, connectionErr := client.runConnection(operationContext)
		if operationContext.Err() != nil {
			client.shutdownSessions()
			client.waitGroup.Wait()
			return nil
		}
		if connectionErr != nil {
			client.logf("control connection closed (node=%s): %v", client.node.ID, connectionErr)
		}
		if admittedOnce {
			delay = client.reconnectMinDelay
		}

		timer := time.NewTimer(delay)
		select {
		case <-operationContext.Done():
			timer.Stop()
			client.shutdownSessions()
			client.waitGroup.Wait()
			return nil
		case <-timer.C:
		}
		delay = min(delay*2, client.reconnectMaxDelay)
	}
}

func (client *Client) runConnection(operationContext context.Context) (bool, error) {
	controlAgentURL, err := client.controlAgentWebSocketURL()
	if err != nil {
		return false, err
	}

	headers := http.Header{}
	if client.token != "" {
		headers.Set("Authorization", "Bearer "+client.token)
	}

	dialContext, cancelDial := context.WithTimeout(operationContext, 10*time.Second)
	connection, response, err := websocket.Dial(dialContext, controlAgentURL, &websocket.DialOptions{
		HTTPHeader: headers,
		HTTPClient: client.httpClient,
	})
	cancelDial()
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return false, err
	}
	connection.SetReadLimit(readLimitBytes)

	client.writeMutex.Lock()
	client.connection = connection
	client.admitted.Store(false)
	client.writeMutex.Unlock()

	connectionContext, cancelConnection := context.WithCancel(operationContext)
	defer func() {
		cancelConnection()
		client.admitted.Store(false)
		client.writeMutex.Lock()
		if client.connection == connection {
			client.connection = nil
		}
		client.writeMutex.Unlock()
		client.failPendingRPC()
		_ = connection.Close(websocket.StatusNormalClosure, "closing")
	}()

	registrationNode := client.snapshotNode()
	if err := client.writeMessage(connectionContext, connection, protocol.SlaveControlMessage{
		Type:            "register",
		ProtocolVersion: protocol.ProtocolVersion,
		Node:            &registrationNode,
		At:              client.nowUTC(),
	}); err != nil {
		return false, err
	}

	admitted := false
	heartbeatDone := make(chan struct{})
	defer func() {
		cancelConnection()
		if admitted {
			<-heartbeatDone
		}
	}()

	for {
		_, payload, readErr := connection.Read(connectionContext)
		if readErr != nil {
			return admitted, readErr
		}

		var message protocol.MasterControlMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			continue
		}

		if message.Type == "ping" {
			_ = client.writeMessage(connectionContext, connection, protocol.SlaveControlMessage{
				Type: "pong",
				At:   client.nowUTC(),
			})
			if !admitted {
				admitted = true
				client.admitted.Store(true)
				if err := client.completeAdmissionHandshake(connectionContext, connection); err != nil {
					return true, err
				}
				client.logf("admitted and synced with master (node=%s)", client.node.ID)
				go func() {
					defer close(heartbeatDone)
					ticker := time.NewTicker(heartbeatInterval)
					defer ticker.Stop()
					for {
						select {
						case <-connectionContext.Done():
							return
						case <-ticker.C:
							client.sendHeartbeat()
						}
					}
				}()
			}
			continue
		}

		if !admitted {
			continue
		}

		client.dispatchMasterMessage(message)
	}
}

func (client *Client) completeAdmissionHandshake(
	connectionContext context.Context,
	connection *websocket.Conn,
) error {
	if client.componentReport != nil {
		reportCopy := *client.componentReport
		reportCopy.ObservedAt = client.nowUTC()
		if err := client.writeMessage(connectionContext, connection, protocol.SlaveControlMessage{
			Type:            "component.inventory",
			ComponentReport: &reportCopy,
			At:              reportCopy.ObservedAt,
		}); err != nil {
			return err
		}
	}

	if err := client.flushOutbox(connectionContext, connection); err != nil {
		return err
	}

	client.leases.replayPending()

	activeRunIDs, activeInstanceIDs := client.activeIdentifiers()
	activeRuns := len(activeRunIDs)
	activeInstances := len(activeInstanceIDs)
	if err := client.writeMessage(connectionContext, connection, protocol.SlaveControlMessage{
		Type:              "sync.complete",
		ActiveRuns:        &activeRuns,
		ActiveInstances:   &activeInstances,
		ActiveRunIDs:      &activeRunIDs,
		ActiveInstanceIDs: &activeInstanceIDs,
		At:                client.nowUTC(),
	}); err != nil {
		return err
	}

	if client.capabilityReport != nil {
		reportCopy := *client.capabilityReport
		observedAt := client.nowUTC()
		reportCopy.At = observedAt
		for index := range reportCopy.Evidence {
			reportCopy.Evidence[index].ObservedAt = observedAt
		}
		if err := client.writeMessage(connectionContext, connection, protocol.SlaveControlMessage{
			Type:             "node.capabilities",
			CapabilityReport: &reportCopy,
			At:               observedAt,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (client *Client) flushOutbox(connectionContext context.Context, connection *websocket.Conn) error {
	for {
		client.mutex.Lock()
		if len(client.outbox) == 0 {
			client.mutex.Unlock()
			return nil
		}
		pending := slices.Clone(client.outbox)
		client.outbox = nil
		client.mutex.Unlock()

		for index, item := range pending {
			if err := client.writeMessage(connectionContext, connection, item); err != nil {
				client.mutex.Lock()
				remaining := append(slices.Clone(pending[index:]), client.outbox...)
				client.outbox = trimOutbox(remaining)
				client.mutex.Unlock()
				return err
			}
		}
	}
}

func (client *Client) dispatchMasterMessage(message protocol.MasterControlMessage) {
	switch message.Type {
	case "instance.provision":
		client.waitGroup.Go(func() {
			client.handleInstanceProvision(message.Instance, message.Allocation)
		})
	case "instance.release":
		client.waitGroup.Go(func() {
			client.handleInstanceRelease(message.InstanceID, message.AllocationID, message.Mode)
		})
	case "dispatch":
		client.handleDispatch(message)
	case "cancel":
		if message.RunID != "" {
			client.cancelRun(message.RunID)
		}
	case "approval.decision":
		client.handleApprovalDecision(message.Decision)
	case "workspace.lease.confirmed":
		client.leases.confirm(message.LeaseID, message.Status)
	case "workspace.cleanup":
		if message.Lease != nil {
			grantCopy := *message.Lease
			mode := message.Mode
			client.waitGroup.Go(func() {
				client.leases.handleCleanupCommand(grantCopy, mode)
			})
		}
	case "hub.rpc.response":
		client.handleHubRPCResponse(message)
	}
}

func (client *Client) sendHeartbeat() {
	if !client.isConnected() {
		return
	}
	activeRunIDs, activeInstanceIDs := client.activeIdentifiers()
	activeRuns := len(activeRunIDs)
	activeInstances := len(activeInstanceIDs)
	_ = client.sendImmediate(context.Background(), protocol.SlaveControlMessage{
		Type:            "heartbeat",
		NodeID:          client.node.ID,
		ActiveRuns:      &activeRuns,
		ActiveInstances: &activeInstances,
		At:              client.nowUTC(),
	})
}

func (client *Client) sendOrQueue(message protocol.SlaveControlMessage) {
	if message.At == "" {
		message.At = client.nowUTC()
	}
	if client.isConnected() {
		if err := client.sendImmediate(context.Background(), message); err == nil {
			return
		}
	}
	if message.Type == "heartbeat" || message.Type == "pong" {
		return
	}
	client.mutex.Lock()
	client.outbox = trimOutbox(append(client.outbox, message))
	client.mutex.Unlock()
}

func trimOutbox(messages []protocol.SlaveControlMessage) []protocol.SlaveControlMessage {
	if len(messages) <= maximumOutboxMessages {
		return messages
	}
	excess := len(messages) - maximumOutboxMessages
	filtered := make([]protocol.SlaveControlMessage, 0, len(messages))
	for _, item := range messages {
		if excess > 0 && (item.Type == "run.output" || item.Type == "heartbeat") {
			excess--
			continue
		}
		filtered = append(filtered, item)
	}
	if len(filtered) > maximumOutboxMessages {
		filtered = filtered[len(filtered)-maximumOutboxMessages:]
	}
	return filtered
}

func (client *Client) sendImmediate(operationContext context.Context, message protocol.SlaveControlMessage) error {
	if !client.admitted.Load() {
		return errors.New("slave control connection is not admitted")
	}
	client.writeMutex.Lock()
	connection := client.connection
	client.writeMutex.Unlock()
	if connection == nil {
		return errors.New("slave control connection is closed")
	}
	return client.writeMessage(operationContext, connection, message)
}

func (client *Client) writeMessage(
	operationContext context.Context,
	connection *websocket.Conn,
	message protocol.SlaveControlMessage,
) error {
	encodedBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	client.writeMutex.Lock()
	defer client.writeMutex.Unlock()
	if client.connection != connection {
		return errors.New("slave control connection was replaced")
	}
	timedContext, cancel := context.WithTimeout(operationContext, writeTimeout)
	defer cancel()
	return connection.Write(timedContext, websocket.MessageText, encodedBytes)
}

func (client *Client) isConnected() bool {
	return client.admitted.Load()
}

func (client *Client) snapshotNode() protocol.ComputeNode {
	client.mutex.Lock()
	activeRuns := len(client.sessions)
	activeInstances := len(client.instances)
	nodeCopy := client.node
	client.mutex.Unlock()

	nodeCopy.LastSeen = client.nowUTC()
	nodeCopy.ActiveRuns = activeRuns
	if activeRuns >= nodeCopy.Concurrency {
		nodeCopy.Status = "busy"
	} else {
		nodeCopy.Status = "online"
	}
	nodeCopy.ActiveInstances = &activeInstances
	return nodeCopy
}

func (client *Client) activeIdentifiers() ([]string, []string) {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	runIDs := make([]string, 0, len(client.sessions))
	for runID := range client.sessions {
		runIDs = append(runIDs, runID)
	}
	slices.Sort(runIDs)
	return runIDs, client.activeInstanceIDsLocked()
}

func (client *Client) finishSession(runID string) {
	client.mutex.Lock()
	delete(client.sessions, runID)
	if !client.completedRuns[runID] {
		if len(client.completedOrder) >= 1024 {
			oldest := client.completedOrder[0]
			client.completedOrder = client.completedOrder[1:]
			delete(client.completedRuns, oldest)
		}
		client.completedRuns[runID] = true
		client.completedOrder = append(client.completedOrder, runID)
	}
	client.mutex.Unlock()
}

func (client *Client) shutdownSessions() {
	client.mutex.Lock()
	sessions := make([]*runSession, 0, len(client.sessions))
	for _, session := range client.sessions {
		sessions = append(sessions, session)
	}
	client.mutex.Unlock()
	for _, session := range sessions {
		session.cancelled.Store(true)
		session.cancelAllApprovals()
		session.cancel()
	}
}

func (client *Client) failPendingRPC() {
	client.rpcMutex.Lock()
	channels := make([]chan protocol.MasterControlMessage, 0, len(client.pendingRPC))
	for requestID, responseChannel := range client.pendingRPC {
		delete(client.pendingRPC, requestID)
		channels = append(channels, responseChannel)
	}
	client.rpcMutex.Unlock()

	for _, responseChannel := range channels {
		responseChannel <- protocol.MasterControlMessage{
			Type: "hub.rpc.response",
			Error: &protocol.HubRPCError{
				Code:      "hub_unavailable",
				Message:   "orchestrator connection dropped before hub RPC completed",
				Retryable: true,
			},
		}
	}
}

func (client *Client) controlAgentWebSocketURL() (string, error) {
	parsedURL, err := url.Parse(client.endpoint)
	if err != nil || parsedURL.Host == "" {
		return "", fmt.Errorf("invalid control endpoint %q", client.endpoint)
	}
	switch parsedURL.Scheme {
	case "http":
		parsedURL.Scheme = "ws"
	case "https":
		parsedURL.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported control endpoint scheme %q", parsedURL.Scheme)
	}
	if parsedURL.Path == "" || parsedURL.Path == "/" {
		parsedURL.Path = "/control-agent"
	} else if !strings.HasSuffix(parsedURL.Path, "/control-agent") {
		parsedURL.Path = strings.TrimRight(parsedURL.Path, "/") + "/control-agent"
	}
	return parsedURL.String(), nil
}

func (client *Client) resolveHTTPURL(relativeOrAbsolute string) (string, error) {
	parsedTarget, err := url.Parse(relativeOrAbsolute)
	if err != nil {
		return "", err
	}
	if parsedTarget.Scheme == "http" || parsedTarget.Scheme == "https" {
		return parsedTarget.String(), nil
	}
	parsedBase, err := url.Parse(client.endpoint)
	if err != nil || parsedBase.Host == "" {
		return "", fmt.Errorf("invalid control endpoint %q", client.endpoint)
	}
	switch parsedBase.Scheme {
	case "ws":
		parsedBase.Scheme = "http"
	case "wss":
		parsedBase.Scheme = "https"
	case "http", "https":
	default:
		return "", fmt.Errorf("unsupported control endpoint scheme %q", parsedBase.Scheme)
	}
	parsedBase.Path = strings.TrimSuffix(parsedBase.Path, "/control-agent")
	return parsedBase.ResolveReference(parsedTarget).String(), nil
}

func (client *Client) nowUTC() string {
	return client.now().UTC().Format(time.RFC3339Nano)
}
