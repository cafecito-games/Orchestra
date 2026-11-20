package slave

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/activitylog"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

type residentInstance struct {
	instance    protocol.AgentInstance
	allocation  protocol.InstanceAllocation
	status      string
	releaseMode string
	activeRunID string
}

func (client *Client) handleInstanceProvision(instance *protocol.AgentInstance, allocation *protocol.InstanceAllocation) {
	if instance == nil || allocation == nil {
		return
	}
	if !protocol.IsIdentifier(instance.ID) || !protocol.IsIdentifier(allocation.ID) {
		return
	}

	client.mutex.Lock()
	if client.restartingForUpdate {
		client.mutex.Unlock()
		return
	}
	if existing, exists := client.instances[instance.ID]; exists {
		if existing.allocation.ID == allocation.ID {
			if existing.status == "active" {
				client.mutex.Unlock()
				client.sendOrQueue(protocol.SlaveControlMessage{
					Type:         "instance.ready",
					InstanceID:   instance.ID,
					AllocationID: allocation.ID,
					At:           client.nowUTC(),
				})
				return
			}
			client.mutex.Unlock()
			return
		}
		client.mutex.Unlock()
		errorMessage := "another allocation is already resident for this instance"
		client.activityLogger.Error(
			"instance",
			"provision failed",
			activitylog.String("instance", instance.ID),
			activitylog.String("error", errorMessage),
		)
		client.sendOrQueue(protocol.SlaveControlMessage{
			Type:         "instance.failed",
			InstanceID:   instance.ID,
			AllocationID: allocation.ID,
			Error:        &errorMessage,
			At:           client.nowUTC(),
		})
		return
	}
	if client.releasedAllocations[allocation.ID] {
		client.mutex.Unlock()
		return
	}
	capacity := client.instanceCapacity()
	if capacity >= 0 && len(client.instances) >= capacity {
		client.mutex.Unlock()
		errorMessage := "instance capacity on this slave node is exhausted"
		client.activityLogger.Error(
			"instance",
			"provision failed",
			activitylog.String("instance", instance.ID),
			activitylog.String("error", errorMessage),
		)
		client.sendOrQueue(protocol.SlaveControlMessage{
			Type:         "instance.failed",
			InstanceID:   instance.ID,
			AllocationID: allocation.ID,
			Error:        new(errorMessage),
			At:           client.nowUTC(),
		})
		return
	}
	client.mutex.Unlock()

	if err := client.validateProvision(*instance, *allocation); err != nil {
		errorMessage := protocol.BoundedDiagnostic(protocol.RedactText(err.Error()))
		client.activityLogger.Error(
			"instance",
			"provision failed",
			activitylog.String("instance", instance.ID),
			activitylog.String("error", errorMessage),
		)
		client.sendOrQueue(protocol.SlaveControlMessage{
			Type:         "instance.failed",
			InstanceID:   instance.ID,
			AllocationID: allocation.ID,
			Error:        new(errorMessage),
			At:           client.nowUTC(),
		})
		return
	}

	client.mutex.Lock()
	if client.restartingForUpdate || client.releasedAllocations[allocation.ID] {
		client.mutex.Unlock()
		return
	}
	if existing, exists := client.instances[instance.ID]; exists {
		sameAllocation := existing.allocation.ID == allocation.ID && existing.status == "active"
		client.mutex.Unlock()
		if sameAllocation {
			client.sendOrQueue(protocol.SlaveControlMessage{
				Type:         "instance.ready",
				InstanceID:   instance.ID,
				AllocationID: allocation.ID,
				At:           client.nowUTC(),
			})
		}
		return
	}
	client.instances[instance.ID] = &residentInstance{
		instance:   *instance,
		allocation: *allocation,
		status:     "active",
	}
	client.mutex.Unlock()

	client.activityLogger.Info(
		"instance",
		"ready",
		activitylog.String("instance", instance.ID),
		activitylog.String("harness", allocation.HarnessID),
		activitylog.String("model", allocation.Model),
		activitylog.String("transport", allocation.Transport),
		activitylog.String("workspace", allocation.Workspace),
	)
	client.sendOrQueue(protocol.SlaveControlMessage{
		Type:         "instance.ready",
		InstanceID:   instance.ID,
		AllocationID: allocation.ID,
		At:           client.nowUTC(),
	})
	client.sendHeartbeat()
}

func (client *Client) validateProvision(instance protocol.AgentInstance, allocation protocol.InstanceAllocation) error {
	_ = instance
	if allocation.NodeID != client.node.ID {
		return fmt.Errorf("allocation targets node %s rather than %s", allocation.NodeID, client.node.ID)
	}
	if len(client.node.WorkspaceRoots) == 0 {
		return fmt.Errorf("slave node %s has no configured workspace roots", client.node.ID)
	}
	if _, err := VerifyWorkspaceWithinRoots(client.node.WorkspaceRoots, allocation.Workspace); err != nil {
		return err
	}
	if client.runner != nil {
		if err := client.runner.AdmitModel(allocation.HarnessID, allocation.Model); err != nil {
			return err
		}
		if err := client.runner.Admit(allocation.HarnessID, allocation.Transport, ""); err != nil {
			return err
		}
	}
	return nil
}

func (client *Client) handleInstanceRelease(instanceID, allocationID, mode string) {
	if !protocol.IsIdentifier(instanceID) {
		return
	}
	if mode != "cancel" {
		mode = "drain"
	}

	var runIDToCancel string
	var releasedAllocationID string
	var shouldEmitReleased bool

	client.mutex.Lock()
	resident, exists := client.instances[instanceID]
	if !exists {
		if allocationID != "" {
			client.rememberReleasedAllocationLocked(allocationID)
			releasedAllocationID = allocationID
			shouldEmitReleased = true
		}
		client.mutex.Unlock()
		if shouldEmitReleased {
			client.sendOrQueue(protocol.SlaveControlMessage{
				Type:         "instance.released",
				InstanceID:   instanceID,
				AllocationID: releasedAllocationID,
				Reason:       mode,
				At:           client.nowUTC(),
			})
		}
		return
	}
	if allocationID != "" && resident.allocation.ID != allocationID {
		client.mutex.Unlock()
		return
	}

	if resident.activeRunID == "" {
		releasedAllocationID = resident.allocation.ID
		delete(client.instances, instanceID)
		client.rememberReleasedAllocationLocked(releasedAllocationID)
		shouldEmitReleased = true
	} else {
		resident.status = "draining"
		if resident.releaseMode != "cancel" {
			resident.releaseMode = mode
		}
		if mode == "cancel" {
			runIDToCancel = resident.activeRunID
		}
	}
	client.mutex.Unlock()

	if runIDToCancel != "" {
		client.cancelRun(runIDToCancel, "")
	}
	if shouldEmitReleased {
		client.activityLogger.Info(
			"instance",
			"released",
			activitylog.String("instance", instanceID),
			activitylog.String("mode", mode),
		)
		client.sendOrQueue(protocol.SlaveControlMessage{
			Type:         "instance.released",
			InstanceID:   instanceID,
			AllocationID: releasedAllocationID,
			Reason:       mode,
			At:           client.nowUTC(),
		})
		client.sendHeartbeat()
	}
}

func (client *Client) acquireResidentForRun(
	run protocol.Run,
	dispatchedInstance *protocol.AgentInstance,
	dispatchedAllocation *protocol.InstanceAllocation,
) (residentInstance, error) {
	if !protocol.IsIdentifier(run.InstanceID) || !protocol.IsIdentifier(run.AllocationID) {
		return residentInstance{}, fmt.Errorf("run %s is missing resident instance or allocation authority", run.ID)
	}

	client.mutex.Lock()
	resident, exists := client.instances[run.InstanceID]
	if !exists && dispatchedInstance != nil && dispatchedAllocation != nil &&
		dispatchedInstance.ID == run.InstanceID && dispatchedAllocation.ID == run.AllocationID &&
		!client.releasedAllocations[run.AllocationID] {
		client.mutex.Unlock()
		if err := client.validateProvision(*dispatchedInstance, *dispatchedAllocation); err != nil {
			return residentInstance{}, err
		}
		client.mutex.Lock()
		if current, found := client.instances[run.InstanceID]; found {
			resident = current
			exists = true
		} else if !client.releasedAllocations[run.AllocationID] {
			resident = &residentInstance{
				instance:   *dispatchedInstance,
				allocation: *dispatchedAllocation,
				status:     "active",
			}
			client.instances[run.InstanceID] = resident
			exists = true
		}
	}

	if !exists {
		client.mutex.Unlock()
		return residentInstance{}, fmt.Errorf("instance %s is not resident on slave node %s", run.InstanceID, client.node.ID)
	}
	if resident.allocation.ID != run.AllocationID {
		activeAllocationID := resident.allocation.ID
		client.mutex.Unlock()
		return residentInstance{}, fmt.Errorf("allocation %s does not match active resident allocation %s", run.AllocationID, activeAllocationID)
	}
	if resident.status != "active" {
		residentStatus := resident.status
		client.mutex.Unlock()
		return residentInstance{}, fmt.Errorf("instance %s is %s and cannot accept new runs", run.InstanceID, residentStatus)
	}
	if resident.activeRunID != "" {
		activeRunID := resident.activeRunID
		client.mutex.Unlock()
		return residentInstance{}, fmt.Errorf("instance %s is already executing run %s", run.InstanceID, activeRunID)
	}
	if len(client.sessions) >= client.node.Concurrency {
		client.mutex.Unlock()
		return residentInstance{}, fmt.Errorf("slave node %s is at run concurrency limit (%d)", client.node.ID, client.node.Concurrency)
	}
	if dispatchedInstance != nil && dispatchedInstance.ID == resident.instance.ID {
		resident.instance = *dispatchedInstance
	}
	if dispatchedAllocation != nil && dispatchedAllocation.ID == resident.allocation.ID && strings.TrimSpace(dispatchedAllocation.Model) != "" {
		if client.runner == nil || client.runner.AdmitModel(resident.allocation.HarnessID, dispatchedAllocation.Model) == nil {
			resident.allocation.Model = dispatchedAllocation.Model
			if dispatchedAllocation.UpdatedAt != "" {
				resident.allocation.UpdatedAt = dispatchedAllocation.UpdatedAt
			}
		}
	}
	resident.activeRunID = run.ID
	snapshot := *resident
	client.mutex.Unlock()
	return snapshot, nil
}

func (client *Client) handleInstanceReconfigure(message protocol.MasterControlMessage) {
	instanceID := message.InstanceID
	if instanceID == "" && message.Instance != nil {
		instanceID = message.Instance.ID
	}
	allocationID := message.AllocationID
	if allocationID == "" && message.Allocation != nil {
		allocationID = message.Allocation.ID
	}
	targetModel := strings.TrimSpace(message.Model)
	if targetModel == "" && message.Allocation != nil {
		targetModel = strings.TrimSpace(message.Allocation.Model)
	}
	if !protocol.IsIdentifier(instanceID) || targetModel == "" {
		return
	}

	client.mutex.Lock()
	if client.restartingForUpdate {
		client.mutex.Unlock()
		return
	}
	resident, exists := client.instances[instanceID]
	if !exists {
		client.mutex.Unlock()
		return
	}
	if allocationID != "" && resident.allocation.ID != allocationID {
		client.mutex.Unlock()
		return
	}
	if resident.status != "active" || resident.activeRunID != "" {
		client.mutex.Unlock()
		return
	}
	harnessID := resident.allocation.HarnessID
	client.mutex.Unlock()

	if client.runner != nil {
		if err := client.runner.AdmitModel(harnessID, targetModel); err != nil {
			client.activityLogger.Warn(
				"instance",
				"reconfigure rejected",
				activitylog.String("instance", instanceID),
				activitylog.String("harness", harnessID),
				activitylog.String("model", targetModel),
				activitylog.String("error", protocol.BoundedDiagnostic(protocol.RedactText(err.Error()))),
			)
			return
		}
	}

	client.mutex.Lock()
	resident, exists = client.instances[instanceID]
	if !exists || resident.status != "active" || resident.activeRunID != "" {
		client.mutex.Unlock()
		return
	}
	if allocationID != "" && resident.allocation.ID != allocationID {
		client.mutex.Unlock()
		return
	}
	resident.allocation.Model = targetModel
	if message.Allocation != nil && message.Allocation.ID == resident.allocation.ID && message.Allocation.UpdatedAt != "" {
		resident.allocation.UpdatedAt = message.Allocation.UpdatedAt
	}
	if message.Instance != nil && message.Instance.ID == resident.instance.ID {
		resident.instance = *message.Instance
	}
	client.mutex.Unlock()

	client.activityLogger.Info(
		"instance",
		"reconfigured",
		activitylog.String("instance", instanceID),
		activitylog.String("harness", harnessID),
		activitylog.String("model", targetModel),
	)
}

func (client *Client) completeResidentRun(instanceID, allocationID, runID string) (bool, string) {
	var emitReleased bool
	var releaseReason string

	client.mutex.Lock()
	resident, exists := client.instances[instanceID]
	if exists && resident.allocation.ID == allocationID && resident.activeRunID == runID {
		resident.activeRunID = ""
		if resident.status == "draining" {
			releaseReason = resident.releaseMode
			if releaseReason == "" {
				releaseReason = "drain"
			}
			delete(client.instances, instanceID)
			client.rememberReleasedAllocationLocked(allocationID)
			emitReleased = true
		}
	}
	client.mutex.Unlock()
	return emitReleased, releaseReason
}

func (client *Client) releaseResidentRun(instanceID, allocationID, runID string) {
	emitReleased, releaseReason := client.completeResidentRun(instanceID, allocationID, runID)

	if emitReleased {
		client.activityLogger.Info(
			"instance",
			"released",
			activitylog.String("instance", instanceID),
			activitylog.String("mode", releaseReason),
		)
		client.sendOrQueue(protocol.SlaveControlMessage{
			Type:         "instance.released",
			InstanceID:   instanceID,
			AllocationID: allocationID,
			Reason:       releaseReason,
			At:           client.nowUTC(),
		})
	}
}

func (client *Client) instanceCapacity() int {
	if client.node.InstanceCapacity == nil {
		return -1
	}
	return *client.node.InstanceCapacity
}

func (client *Client) activeInstanceIDsLocked() []string {
	ids := make([]string, 0, len(client.instances))
	for instanceID := range client.instances {
		ids = append(ids, instanceID)
	}
	slices.Sort(ids)
	return ids
}

func (client *Client) rememberReleasedAllocationLocked(allocationID string) {
	if allocationID == "" || client.releasedAllocations[allocationID] {
		return
	}
	if len(client.releasedOrder) >= 1024 {
		oldest := client.releasedOrder[0]
		client.releasedOrder = client.releasedOrder[1:]
		delete(client.releasedAllocations, oldest)
	}
	client.releasedAllocations[allocationID] = true
	client.releasedOrder = append(client.releasedOrder, allocationID)
}
