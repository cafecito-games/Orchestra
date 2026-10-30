package orchestrator

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

const (
	pingIntervalDuration = 15 * time.Second
	pongTimeoutDuration  = 45 * time.Second
	socketWriteTimeout   = 10 * time.Second
)

type slaveSession struct {
	nodeID     string
	connection *websocket.Conn
	writeMutex sync.Mutex
	synced     atomic.Bool
	lastPong   atomic.Int64
	cancel     context.CancelFunc
}

func (session *slaveSession) send(operationContext context.Context, message protocol.MasterControlMessage) error {
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

type ExecutionInventoryResponse struct {
	ProtocolVersion string                    `json:"protocolVersion"`
	GeneratedAt     string                    `json:"generatedAt"`
	Nodes           []protocol.NodeDetail     `json:"nodes"`
	ProjectProfiles []protocol.ProjectProfile `json:"projectProfiles"`
}

func (service *Service) GetExecutionInventory() (ExecutionInventoryResponse, error) {
	nodes, err := service.ListNodeDetails()
	if err != nil {
		return ExecutionInventoryResponse{}, err
	}
	var profiles []protocol.ProjectProfile
	if readErr := service.store.Read(func(state *store.State) error {
		for _, profile := range state.ProjectProfiles {
			profiles = append(profiles, profile)
		}
		return nil
	}); readErr != nil {
		return ExecutionInventoryResponse{}, readErr
	}
	slices.SortFunc(profiles, func(left, right protocol.ProjectProfile) int {
		return cmp.Compare(left.ID, right.ID)
	})
	if profiles == nil {
		profiles = []protocol.ProjectProfile{}
	}
	return ExecutionInventoryResponse{
		ProtocolVersion: protocol.ProtocolVersion,
		GeneratedAt:     service.nowUTC(),
		Nodes:           nodes,
		ProjectProfiles: profiles,
	}, nil
}

func (service *Service) ListNodeDetails() ([]protocol.NodeDetail, error) {
	var details []protocol.NodeDetail
	err := service.store.Read(func(state *store.State) error {
		for _, nodeID := range sortedNodeIDs(state) {
			node := state.Nodes[nodeID]
			connected := service.isNodeConnected(nodeID)
			synced := service.isNodeLiveAndSynced(nodeID)

			activeRuns := countActiveRunsOnNode(state, nodeID)
			occupyingInstances := countOccupyingAllocationsOnNode(state, nodeID)
			node.ActiveRuns = activeRuns
			node.ActiveInstances = &occupyingInstances
			if !connected {
				node.Status = "offline"
			} else if activeRuns >= node.Concurrency {
				node.Status = "busy"
			} else {
				node.Status = "online"
			}

			var capReportPtr *protocol.NodeCapabilityReport
			if capReport, exists := state.CapabilityReports[nodeID]; exists {
				copyReport := capReport
				capReportPtr = &copyReport
			}
			var compReportPtr *protocol.ComponentInventoryReport
			if compReport, exists := state.ComponentReports[nodeID]; exists {
				copyReport := compReport
				compReportPtr = &copyReport
			}

			var residentIDs []string
			for _, allocation := range state.Allocations {
				if allocation.NodeID == nodeID && protocol.IsOccupyingAllocationStatus(allocation.Status) {
					residentIDs = append(residentIDs, allocation.InstanceID)
				}
			}
			slices.Sort(residentIDs)

			details = append(details, protocol.NodeDetail{
				Node:              node,
				Connected:         connected,
				Synced:            synced,
				CapabilityReport:  capReportPtr,
				ComponentReport:   compReportPtr,
				ResidentInstances: residentIDs,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if details == nil {
		details = []protocol.NodeDetail{}
	}
	return details, nil
}

func (service *Service) GetStatus() (protocol.StatusResponse, error) {
	nodeDetails, err := service.ListNodeDetails()
	if err != nil {
		return protocol.StatusResponse{}, err
	}
	response := protocol.StatusResponse{
		Version:         protocol.BuildVersion,
		ProtocolVersion: protocol.ProtocolVersion,
		StartedAt:       service.startedAt,
		Now:             service.nowUTC(),
		TotalNodes:      len(nodeDetails),
		Nodes:           make([]protocol.ComputeNode, 0, len(nodeDetails)),
	}
	for _, detail := range nodeDetails {
		if detail.Connected {
			response.ConnectedNodes++
		}
		response.Nodes = append(response.Nodes, detail.Node)
	}

	err = service.store.Read(func(state *store.State) error {
		for _, thread := range state.Threads {
			if thread.Status == "open" {
				response.OpenThreads++
			}
		}
		for _, instance := range state.Instances {
			if !protocol.IsTerminalInstanceStatus(instance.Status) {
				response.ActiveInstances++
			}
		}
		for _, runRecord := range state.Runs {
			if protocol.IsActiveRunStatus(runRecord.Status) {
				response.ActiveRuns++
			}
		}
		for _, task := range state.Tasks {
			switch task.Status {
			case "running":
				response.RunningTasks++
			case "ready":
				response.ReadyTasks++
			}
		}
		for _, approval := range state.Approvals {
			if approval.Status == "pending" {
				response.PendingApprovals++
			}
		}
		return nil
	})
	return response, err
}

func (service *Service) isNodeConnected(nodeID string) bool {
	service.slavesMutex.RLock()
	_, connected := service.slaves[nodeID]
	service.slavesMutex.RUnlock()
	return connected
}

func (service *Service) isNodeLiveAndSynced(nodeID string) bool {
	service.slavesMutex.RLock()
	session, connected := service.slaves[nodeID]
	service.slavesMutex.RUnlock()
	return connected && session.synced.Load()
}

func (service *Service) sendToSlave(nodeID string, message protocol.MasterControlMessage) error {
	service.slavesMutex.RLock()
	session, connected := service.slaves[nodeID]
	service.slavesMutex.RUnlock()
	if !connected {
		return errors.New("slave node is not connected")
	}
	return session.send(context.Background(), message)
}

func (service *Service) serveSlaveConnection(requestContext context.Context, connection *websocket.Conn) {
	connection.SetReadLimit(16 * 1024 * 1024)

	regContext, cancelReg := context.WithTimeout(requestContext, 15*time.Second)
	_, firstPayload, err := connection.Read(regContext)
	cancelReg()
	if err != nil {
		_ = connection.Close(websocket.StatusProtocolError, "expected registration message")
		return
	}

	var registration protocol.SlaveControlMessage
	if err := json.Unmarshal(firstPayload, &registration); err != nil || registration.Type != "register" || registration.Node == nil {
		_ = connection.Close(websocket.StatusProtocolError, "malformed registration message")
		return
	}
	if registration.ProtocolVersion != protocol.ProtocolVersion {
		_ = connection.Close(websocket.StatusPolicyViolation, "unsupported protocol version")
		return
	}

	node := *registration.Node
	nowText := service.nowUTC()
	if node.LastSeen == "" {
		node.LastSeen = nowText
	}
	if err := node.Validate(); err != nil {
		_ = connection.Close(websocket.StatusPolicyViolation, err.Error())
		return
	}

	sessionContext, cancelSession := context.WithCancel(requestContext)
	session := &slaveSession{
		nodeID:     node.ID,
		connection: connection,
		cancel:     cancelSession,
	}
	session.lastPong.Store(service.now().UnixNano())

	service.slavesMutex.Lock()
	if _, alreadyLive := service.slaves[node.ID]; alreadyLive {
		service.slavesMutex.Unlock()
		cancelSession()
		_ = connection.Close(websocket.StatusPolicyViolation, "node already has a live connection")
		return
	}
	service.slaves[node.ID] = session
	service.slavesMutex.Unlock()

	defer func() {
		cancelSession()
		service.slavesMutex.Lock()
		if service.slaves[node.ID] == session {
			delete(service.slaves, node.ID)
		}
		service.slavesMutex.Unlock()
		service.markNodeOffline(node.ID)
		_ = connection.Close(websocket.StatusNormalClosure, "closed")
	}()

	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		node.Status = "online"
		node.LastSeen = service.nowUTC()
		state.Nodes[node.ID] = node
		return nil
	})

	if err := session.send(sessionContext, protocol.MasterControlMessage{
		Type: "ping",
		At:   service.nowUTC(),
	}); err != nil {
		return
	}

	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		ticker := time.NewTicker(service.pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-sessionContext.Done():
				return
			case <-ticker.C:
				lastPongTime := time.Unix(0, session.lastPong.Load())
				if service.now().Sub(lastPongTime) > service.pongTimeout {
					_ = connection.Close(websocket.StatusGoingAway, "ping liveness timeout")
					cancelSession()
					return
				}
				_ = session.send(sessionContext, protocol.MasterControlMessage{
					Type: "ping",
					At:   service.nowUTC(),
				})
			}
		}
	}()
	defer func() {
		cancelSession()
		<-pingDone
	}()

	for {
		_, payload, readErr := connection.Read(sessionContext)
		if readErr != nil {
			return
		}
		session.lastPong.Store(service.now().UnixNano())

		var message protocol.SlaveControlMessage
		if err := json.Unmarshal(payload, &message); err != nil {
			continue
		}
		service.dispatchSlaveMessage(sessionContext, session, message)
	}
}

func (service *Service) markNodeOffline(nodeID string) {
	if service.isNodeConnected(nodeID) {
		return
	}
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		node, exists := state.Nodes[nodeID]
		if !exists {
			return nil
		}
		node.Status = "offline"
		node.LastSeen = service.nowUTC()
		state.Nodes[nodeID] = node
		return nil
	})
}

func (service *Service) dispatchSlaveMessage(
	sessionContext context.Context,
	session *slaveSession,
	message protocol.SlaveControlMessage,
) {
	nodeID := session.nodeID
	switch message.Type {
	case "pong":
		session.lastPong.Store(service.now().UnixNano())
	case "heartbeat":
		service.handleNodeHeartbeat(nodeID, message)
	case "component.inventory":
		service.handleComponentInventory(nodeID, message)
	case "node.capabilities":
		service.handleNodeCapabilities(nodeID, message)
	case "sync.complete":
		service.handleSyncComplete(session, message)
	case "instance.ready":
		service.handleInstanceReady(nodeID, message)
	case "instance.failed":
		service.handleInstanceFailed(nodeID, message)
	case "instance.released":
		service.handleInstanceReleased(nodeID, message)
	case "workspace.lease":
		service.handleWorkspaceLeaseUpdate(nodeID, message)
	case "run.started":
		service.handleRunStarted(nodeID, message)
	case "run.output":
		service.handleRunOutput(nodeID, message)
	case "harness.event":
		service.handleHarnessEvent(nodeID, message)
	case "session.binding":
		service.handleSessionBinding(nodeID, message)
	case "approval.undeliverable":
		service.handleApprovalUndeliverable(nodeID, message)
	case "run.completed":
		service.handleRunCompleted(nodeID, message)
	case "run.failed":
		service.handleRunFailed(nodeID, message, false)
	case "run.cancelled":
		service.handleRunCancelled(nodeID, message)
	case "hub.rpc.request":
		go service.handleHubRPCRequest(sessionContext, session, message)
	}
}

func (service *Service) handleNodeHeartbeat(nodeID string, message protocol.SlaveControlMessage) {
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		node, exists := state.Nodes[nodeID]
		if !exists {
			return nil
		}
		node.LastSeen = service.nowUTC()
		if message.ActiveRuns != nil && *message.ActiveRuns >= 0 {
			node.ActiveRuns = *message.ActiveRuns
		}
		if message.ActiveInstances != nil && *message.ActiveInstances >= 0 {
			value := *message.ActiveInstances
			node.ActiveInstances = &value
		}
		if node.ActiveRuns >= node.Concurrency {
			node.Status = "busy"
		} else {
			node.Status = "online"
		}
		state.Nodes[nodeID] = node
		return nil
	})
}

func (service *Service) handleComponentInventory(nodeID string, message protocol.SlaveControlMessage) {
	if message.ComponentReport == nil || message.ComponentReport.NodeID != nodeID {
		return
	}
	if err := message.ComponentReport.Validate(); err != nil {
		return
	}
	report := *message.ComponentReport
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		state.ComponentReports[nodeID] = report
		return nil
	})
}

func (service *Service) handleNodeCapabilities(nodeID string, message protocol.SlaveControlMessage) {
	if message.CapabilityReport == nil || message.CapabilityReport.NodeID != nodeID {
		return
	}
	if err := message.CapabilityReport.Validate(); err != nil {
		return
	}
	report := *message.CapabilityReport
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		state.CapabilityReports[nodeID] = report
		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
		return nil
	})
}

func (service *Service) handleSyncComplete(session *slaveSession, message protocol.SlaveControlMessage) {
	nodeID := session.nodeID
	reportedRuns := map[string]bool{}
	if message.ActiveRunIDs != nil {
		for _, runID := range *message.ActiveRunIDs {
			reportedRuns[runID] = true
		}
	}
	reportedInstances := map[string]bool{}
	if message.ActiveInstanceIDs != nil {
		for _, instanceID := range *message.ActiveInstanceIDs {
			reportedInstances[instanceID] = true
		}
	}

	var outbound []protocol.MasterControlMessage

	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		nowText := service.nowUTC()
		knownOccupants := map[string]protocol.InstanceAllocation{}

		for _, allocation := range state.Allocations {
			if allocation.NodeID != nodeID || !protocol.IsOccupyingAllocationStatus(allocation.Status) {
				continue
			}
			instance, instanceExists := state.Instances[allocation.InstanceID]
			if !instanceExists {
				continue
			}

			if reportedInstances[instance.ID] {
				knownOccupants[instance.ID] = allocation
				if intent, draining := state.ReleaseIntents[instance.ID]; draining || instance.Status == "draining" {
					mode := "drain"
					if draining && intent.Mode != "" {
						mode = intent.Mode
					}
					outbound = append(outbound, protocol.MasterControlMessage{
						Type:         "instance.release",
						InstanceID:   instance.ID,
						AllocationID: allocation.ID,
						Mode:         mode,
						At:           nowText,
					})
				} else if allocation.Status == "provisioning" {
					allocation.Status = "active"
					allocation.UpdatedAt = nowText
					state.Allocations[allocation.ID] = allocation
					if instance.Status == "provisioning" || instance.Status == "requested" {
						instance.Status = "ready"
						instance.UpdatedAt = nowText
						state.Instances[instance.ID] = instance
					}
				}
				continue
			}

			if allocation.Status == "reserved" || allocation.Status == "provisioning" {
				if intent, draining := state.ReleaseIntents[instance.ID]; draining || instance.Status == "draining" {
					allocation.Status = "released"
					allocation.UpdatedAt = nowText
					state.Allocations[allocation.ID] = allocation
					instance.Status = "released"
					instance.UpdatedAt = nowText
					state.Instances[instance.ID] = instance
					delete(state.ReleaseIntents, instance.ID)
					_ = intent
					continue
				}
				instCopy := instance
				allocCopy := allocation
				outbound = append(outbound, protocol.MasterControlMessage{
					Type:       "instance.provision",
					Instance:   &instCopy,
					Allocation: &allocCopy,
					At:         nowText,
				})
				knownOccupants[instance.ID] = allocation
				continue
			}

			allocation.Status = "failed"
			allocation.UpdatedAt = nowText
			state.Allocations[allocation.ID] = allocation
			if !protocol.IsTerminalInstanceStatus(instance.Status) {
				instance.Status = "failed"
				instance.UpdatedAt = nowText
				state.Instances[instance.ID] = instance
			}
			delete(state.ReleaseIntents, instance.ID)
			for bindingID, binding := range state.SessionBindings {
				if binding.AllocationID == allocation.ID && !protocol.IsTerminalSessionBindingStatus(binding.Status) {
					binding.Status = "failed"
					binding.UpdatedAt = nowText
					state.SessionBindings[bindingID] = binding
				}
			}
		}

		for reportedInstID := range reportedInstances {
			if _, recognized := knownOccupants[reportedInstID]; !recognized {
				outbound = append(outbound, protocol.MasterControlMessage{
					Type:       "instance.release",
					InstanceID: reportedInstID,
					Mode:       "cancel",
					At:         nowText,
				})
			}
		}

		for _, runRecord := range state.Runs {
			if runRecord.NodeID != nodeID || !protocol.IsActiveRunStatus(runRecord.Status) {
				continue
			}
			if reportedRuns[runRecord.ID] {
				continue
			}

			instance := state.Instances[runRecord.InstanceID]
			allocation := state.Allocations[runRecord.AllocationID]
			if runRecord.Status == "queued" && reportedInstances[instance.ID] && allocation.Status == "active" {
				runCopy := runRecord
				instCopy := instance
				allocCopy := allocation
				var grantPtr *protocol.WorkspaceLeaseGrant
				if runRecord.WorkspaceLeaseID != nil {
					if lease, exists := state.WorkspaceLeases[*runRecord.WorkspaceLeaseID]; exists {
						grant := lease.Grant()
						grantPtr = &grant
					}
				}
				outbound = append(outbound, protocol.MasterControlMessage{
					Type:           "dispatch",
					At:             nowText,
					RunID:          runCopy.ID,
					InstanceID:     instCopy.ID,
					AllocationID:   allocCopy.ID,
					Instance:       &instCopy,
					Allocation:     &allocCopy,
					Run:            &runCopy,
					WorkspaceLease: grantPtr,
				})
				continue
			}

			lostError := "slave node reconnected without this active run"
			runRecord.Status = "failed"
			runRecord.Error = &lostError
			runRecord.FinishedAt = &nowText
			state.Runs[runRecord.ID] = runRecord
			service.settleRunApprovalsInState(state, runRecord.ID, "cancelled", nowText)
			service.releaseInstanceAfterRunInState(state, runRecord.InstanceID, nowText)

			if runRecord.TaskID != nil {
				if task, taskExists := state.Tasks[*runRecord.TaskID]; taskExists && !protocol.IsTerminalTaskStatus(task.Status) {
					if task.AttemptCount < task.MaxAttempts {
						task.Status = "ready"
						task.Assignment = nil
						task.Summary = lostError + "; retrying"
						task.UpdatedAt = nowText
					} else {
						task.Status = "failed"
						task.Summary = lostError
						task.UpdatedAt = nowText
						task.FinishedAt = nowText
					}
					state.Tasks[task.ID] = task
					recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
				}
			}
		}

		for _, lease := range state.WorkspaceLeases {
			if lease.NodeID != nodeID || protocol.IsTerminalWorkspaceLeaseStatus(lease.Status) || lease.Status == "retained" {
				continue
			}
			runRecord, runExists := state.Runs[lease.RunID]
			if runExists && protocol.IsActiveRunStatus(runRecord.Status) {
				continue
			}
			grant := lease.Grant()
			outbound = append(outbound, protocol.MasterControlMessage{
				Type:  "workspace.cleanup",
				Lease: &grant,
				Mode:  protocol.WorkspaceCleanupModeReconcile,
				At:    nowText,
			})
		}

		for _, approval := range state.Approvals {
			if approval.NodeID != nodeID || approval.Status == "pending" || approval.DeliveryState == "applied" || approval.DeliveryState == "not-applied" {
				continue
			}
			if !reportedRuns[approval.RunID] {
				continue
			}
			decision := protocol.ApprovalDecision{
				ApprovalID:       approval.ID,
				RunID:            approval.RunID,
				Status:           approval.Status,
				SelectedOptionID: approval.SelectedOptionID,
			}
			outbound = append(outbound, protocol.MasterControlMessage{
				Type:     "approval.decision",
				RunID:    approval.RunID,
				Decision: &decision,
				At:       nowText,
			})
		}
		return nil
	})

	session.synced.Store(true)
	for _, item := range outbound {
		_ = session.send(context.Background(), item)
	}
	service.TriggerSchedule()
}
