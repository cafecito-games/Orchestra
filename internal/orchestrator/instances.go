package orchestrator

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type SpawnInstanceInput struct {
	ThreadID           string                                  `json:"threadId,omitempty"`
	IdempotencyKey     string                                  `json:"idempotencyKey"`
	Purpose            *protocol.InstancePurpose               `json:"purpose,omitempty"`
	Delegation         *protocol.InstanceDelegationPolicy      `json:"delegation,omitempty"`
	Requirements       *protocol.InstanceExecutionRequirements `json:"requirements,omitempty"`
	IdleTimeoutSeconds *int                                    `json:"idleTimeoutSeconds,omitempty"`
	InitialTask        *protocol.InstanceInitialTask           `json:"initialTask,omitempty"`
}

type GetInstanceInput struct {
	InstanceID string `json:"instanceId"`
}

type RenewInstanceInput struct {
	InstanceID         string `json:"instanceId"`
	IdempotencyKey     string `json:"idempotencyKey"`
	IdleTimeoutSeconds *int   `json:"idleTimeoutSeconds,omitempty"`
}

type ReleaseInstanceInput struct {
	InstanceID     string `json:"instanceId"`
	IdempotencyKey string `json:"idempotencyKey"`
	Mode           string `json:"mode,omitempty"`
}

type InstanceSnapshot struct {
	Instance   protocol.AgentInstance       `json:"instance"`
	Allocation *protocol.InstanceAllocation `json:"allocation,omitempty"`
	ActiveRuns []protocol.Run               `json:"activeRuns"`
}

func (service *Service) SpawnInstance(
	caller CallerIdentity,
	input SpawnInstanceInput,
) (protocol.InstanceLifecycleResult, error) {
	threadID := strings.TrimSpace(input.ThreadID)
	if threadID == "" && caller.Kind == "run" {
		threadID = caller.ThreadID
	}
	if !protocol.IsIdentifier(threadID) {
		return protocol.InstanceLifecycleResult{}, invalidRequest("threadId is required")
	}
	if caller.Kind == "run" {
		if !caller.CanDelegate {
			return protocol.InstanceLifecycleResult{}, forbiddenError("this worker instance is not authorized to spawn instances")
		}
		if caller.ThreadID != "" && caller.ThreadID != threadID {
			return protocol.InstanceLifecycleResult{}, forbiddenError("worker run cannot spawn instances into another thread")
		}
	}

	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" || !protocol.IsBoundedString(idempotencyKey, protocol.InstanceIdempotencyKeyBytes) {
		return protocol.InstanceLifecycleResult{}, invalidRequest("idempotencyKey is required and must be at most 128 bytes")
	}

	timeoutSeconds := protocol.DefaultInstanceIdleTimeoutSeconds
	if input.IdleTimeoutSeconds != nil {
		timeoutSeconds = *input.IdleTimeoutSeconds
		if timeoutSeconds < protocol.MinimumInstanceIdleTimeoutSeconds || timeoutSeconds > protocol.MaximumInstanceIdleTimeoutSeconds {
			return protocol.InstanceLifecycleResult{}, invalidRequest(fmt.Sprintf(
				"idleTimeoutSeconds must be between %d and %d",
				protocol.MinimumInstanceIdleTimeoutSeconds,
				protocol.MaximumInstanceIdleTimeoutSeconds,
			))
		}
	}

	if err := validatePurpose(input.Purpose); err != nil {
		return protocol.InstanceLifecycleResult{}, err
	}
	if input.InitialTask != nil {
		if strings.TrimSpace(input.InitialTask.Title) == "" || strings.TrimSpace(input.InitialTask.Instructions) == "" {
			return protocol.InstanceLifecycleResult{}, invalidRequest("initialTask requires both title and instructions")
		}
	}

	sourceKey := caller.SourceKey()
	receiptKey := sourceKey + "\x00create\x00" + threadID + "\x00" + idempotencyKey
	digest := canonicalDigest(input)

	var result protocol.InstanceLifecycleResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		if _, exists := state.Threads[threadID]; !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}
		if existingReceipt, found := state.InstanceReceipts[receiptKey]; found {
			if existingReceipt.Digest != digest {
				return conflictError("idempotency_conflict", "idempotencyKey was already used for a different spawn_instance request")
			}
			instance := state.Instances[existingReceipt.InstanceID]
			var allocationPtr *protocol.InstanceAllocation
			if allocation, allocExists := state.LatestAllocationForInstance(instance.ID); allocExists {
				allocationCopy := allocation
				allocationPtr = &allocationCopy
			}
			result = protocol.InstanceLifecycleResult{
				Instance:      instance,
				Allocation:    allocationPtr,
				InitialTaskID: existingReceipt.InitialTaskID,
				Replayed:      true,
			}
			return nil
		}

		if len(state.Instances) >= protocol.InstanceCountMaximum {
			return newRetryableError("capacity_unavailable", 503, "instance table capacity exhausted")
		}

		var instanceReqs protocol.InstanceExecutionRequirements
		if input.Requirements != nil {
			instanceReqs = *input.Requirements
		}
		taskStyleReqs := instanceRequirementsToExecutionRequirements(instanceReqs)
		selection, diagnostic := service.selectOfferingForRequirements(state, taskStyleReqs, nil, true)
		if selection == nil {
			detail := "no connected, synced slave node satisfies instance requirements"
			if len(diagnostic.Unsatisfied) > 0 {
				detail = diagnostic.Unsatisfied[0].Detail
			}
			return newRetryableError("capacity_unavailable", 503, detail)
		}

		nowText := service.nowUTC()
		expiresAt := extendLeaseTimestamp(nowText, timeoutSeconds)

		var delegation protocol.InstanceDelegationPolicy
		if input.Delegation != nil {
			delegation = *input.Delegation
		}

		instanceID := protocol.NewIdentifier("inst")
		allocationID := protocol.NewIdentifier("alloc")

		instance := protocol.AgentInstance{
			ID:           instanceID,
			ThreadID:     threadID,
			Creator:      caller.InstanceCreator(),
			Purpose:      input.Purpose,
			Delegation:   delegation,
			Requirements: instanceReqs,
			Lease: protocol.InstanceLease{
				IdleTimeoutSeconds: timeoutSeconds,
				ExpiresAt:          expiresAt,
			},
			Status:      "provisioning",
			AutoManaged: false,
			CreatedAt:   nowText,
			UpdatedAt:   nowText,
		}
		allocation := protocol.InstanceAllocation{
			ID:         allocationID,
			InstanceID: instanceID,
			NodeID:     selection.node.ID,
			HarnessID:  selection.harnessID,
			Model:      selection.model,
			Transport:  selection.transport,
			Workspace:  selection.workspacePath,
			Lease: protocol.InstanceLease{
				IdleTimeoutSeconds: timeoutSeconds,
				ExpiresAt:          expiresAt,
			},
			Status:    "provisioning",
			CreatedAt: nowText,
			UpdatedAt: nowText,
		}
		state.Instances[instanceID] = instance
		state.Allocations[allocationID] = allocation
		service.ensureInstanceMailboxInState(state, instance, allocation, nowText)

		delivery := store.InstanceDelivery{
			ID:           protocol.NewIdentifier("idlv"),
			NodeID:       selection.node.ID,
			InstanceID:   instanceID,
			AllocationID: allocationID,
			Kind:         "provision",
			CreatedAt:    nowText,
		}
		state.InstanceDeliveries[delivery.ID] = delivery

		var initialTaskID *string
		if input.InitialTask != nil {
			taskID := protocol.NewIdentifier("task")
			parentTaskID := ""
			if caller.Kind == "run" {
				parentTaskID = caller.TaskID
			}
			task := protocol.Task{
				ID:             taskID,
				ThreadID:       threadID,
				ParentTaskID:   parentTaskID,
				CreatedByRunID: caller.RunID,
				Origin: protocol.TaskOrigin{
					Kind:       caller.Kind,
					ClientID:   caller.ClientID,
					RunID:      caller.RunID,
					TaskID:     parentTaskID,
					InstanceID: caller.InstanceID,
				},
				Title:               strings.TrimSpace(input.InitialTask.Title),
				Instructions:        strings.TrimSpace(input.InitialTask.Instructions),
				Status:              "ready",
				AttemptCount:        0,
				MaxAttempts:         protocol.MaximumTaskAttempts,
				Dependencies:        []protocol.TaskDependency{},
				Requirements:        taskStyleReqs,
				PlacementInstanceID: instanceID,
				CreatedAt:           nowText,
				UpdatedAt:           nowText,
			}
			state.Tasks[taskID] = task
			initialTaskID = &taskID
		}

		state.InstanceReceipts[receiptKey] = protocol.InstanceLifecycleReceipt{
			ID:             protocol.NewIdentifier("ircpt"),
			ThreadID:       threadID,
			SourceKey:      sourceKey,
			IdempotencyKey: idempotencyKey,
			Operation:      "create",
			Digest:         digest,
			InstanceID:     instanceID,
			InitialTaskID:  initialTaskID,
			CreatedAt:      nowText,
		}

		allocationCopy := allocation
		result = protocol.InstanceLifecycleResult{
			Instance:      instance,
			Allocation:    &allocationCopy,
			InitialTaskID: initialTaskID,
			Replayed:      false,
		}

		targetNodeID := selection.node.ID
		instanceSnapshot := instance
		allocationSnapshot := allocation
		mutation.OnCommit(func() {
			_ = service.sendToSlave(targetNodeID, protocol.MasterControlMessage{
				Type:       "instance.provision",
				Instance:   &instanceSnapshot,
				Allocation: &allocationSnapshot,
				At:         service.nowUTC(),
			})
		})
		return nil
	})
	return result, err
}

func (service *Service) GetInstance(caller CallerIdentity, input GetInstanceInput) (InstanceSnapshot, error) {
	if !protocol.IsIdentifier(input.InstanceID) {
		return InstanceSnapshot{}, invalidRequest("instanceId is required")
	}
	var snapshot InstanceSnapshot
	err := service.store.Read(func(state *store.State) error {
		instance, exists := state.Instances[input.InstanceID]
		if !exists {
			return notFound(fmt.Sprintf("instance %s does not exist", input.InstanceID))
		}
		if caller.Kind == "run" && caller.ThreadID != "" && instance.ThreadID != caller.ThreadID {
			return forbiddenError("worker run cannot inspect instances in another thread")
		}
		snapshot.Instance = instance
		if allocation, found := state.LatestAllocationForInstance(instance.ID); found {
			allocationCopy := allocation
			snapshot.Allocation = &allocationCopy
		}
		for _, runRecord := range state.Runs {
			if runRecord.InstanceID == instance.ID && protocol.IsActiveRunStatus(runRecord.Status) {
				snapshot.ActiveRuns = append(snapshot.ActiveRuns, runRecord)
			}
		}
		return nil
	})
	if err != nil {
		return InstanceSnapshot{}, err
	}
	if snapshot.ActiveRuns == nil {
		snapshot.ActiveRuns = []protocol.Run{}
	}
	return snapshot, nil
}

func (service *Service) RenewInstance(
	caller CallerIdentity,
	input RenewInstanceInput,
) (protocol.InstanceLifecycleResult, error) {
	if !protocol.IsIdentifier(input.InstanceID) {
		return protocol.InstanceLifecycleResult{}, invalidRequest("instanceId is required")
	}
	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" || !protocol.IsBoundedString(idempotencyKey, protocol.InstanceIdempotencyKeyBytes) {
		return protocol.InstanceLifecycleResult{}, invalidRequest("idempotencyKey is required")
	}

	sourceKey := caller.SourceKey()
	receiptKey := sourceKey + "\x00renew\x00" + input.InstanceID + "\x00" + idempotencyKey
	digest := canonicalDigest(input)

	var result protocol.InstanceLifecycleResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		instance, exists := state.Instances[input.InstanceID]
		if !exists {
			return notFound(fmt.Sprintf("instance %s does not exist", input.InstanceID))
		}
		if caller.Kind == "run" {
			if !caller.CanDelegate {
				return forbiddenError("this worker instance is not authorized to renew instances")
			}
			if caller.ThreadID != "" && instance.ThreadID != caller.ThreadID {
				return forbiddenError("worker run cannot renew instances in another thread")
			}
		}
		if existingReceipt, found := state.InstanceReceipts[receiptKey]; found {
			if existingReceipt.Digest != digest {
				return conflictError("idempotency_conflict", "idempotencyKey was already used for a different renew_instance request")
			}
			var allocationPtr *protocol.InstanceAllocation
			if allocation, allocExists := state.LatestAllocationForInstance(instance.ID); allocExists {
				allocationCopy := allocation
				allocationPtr = &allocationCopy
			}
			result = protocol.InstanceLifecycleResult{
				Instance:   instance,
				Allocation: allocationPtr,
				Replayed:   true,
			}
			return nil
		}

		if protocol.IsTerminalInstanceStatus(instance.Status) || instance.Status == "draining" {
			return conflictError("invalid_transition", fmt.Sprintf("cannot renew instance %s in status %s", instance.ID, instance.Status))
		}

		timeoutSeconds := instance.Lease.IdleTimeoutSeconds
		if input.IdleTimeoutSeconds != nil {
			timeoutSeconds = *input.IdleTimeoutSeconds
			if timeoutSeconds < protocol.MinimumInstanceIdleTimeoutSeconds || timeoutSeconds > protocol.MaximumInstanceIdleTimeoutSeconds {
				return invalidRequest("idleTimeoutSeconds is out of bounds")
			}
		}

		nowText := service.nowUTC()
		expiresAt := extendLeaseTimestamp(nowText, timeoutSeconds)
		instance.Lease.IdleTimeoutSeconds = timeoutSeconds
		instance.Lease.ExpiresAt = expiresAt
		instance.UpdatedAt = nowText
		state.Instances[instance.ID] = instance

		var allocationPtr *protocol.InstanceAllocation
		if allocation, found := state.ActiveAllocationForInstance(instance.ID); found {
			allocation.Lease.IdleTimeoutSeconds = timeoutSeconds
			allocation.Lease.ExpiresAt = expiresAt
			allocation.UpdatedAt = nowText
			state.Allocations[allocation.ID] = allocation
			allocationCopy := allocation
			allocationPtr = &allocationCopy
		}

		state.InstanceReceipts[receiptKey] = protocol.InstanceLifecycleReceipt{
			ID:             protocol.NewIdentifier("ircpt"),
			ThreadID:       instance.ThreadID,
			SourceKey:      sourceKey,
			IdempotencyKey: idempotencyKey,
			Operation:      "renew",
			Digest:         digest,
			InstanceID:     instance.ID,
			CreatedAt:      nowText,
		}

		result = protocol.InstanceLifecycleResult{
			Instance:   instance,
			Allocation: allocationPtr,
			Replayed:   false,
		}
		return nil
	})
	return result, err
}

func (service *Service) ReleaseInstance(
	caller CallerIdentity,
	input ReleaseInstanceInput,
) (protocol.InstanceLifecycleResult, error) {
	if !protocol.IsIdentifier(input.InstanceID) {
		return protocol.InstanceLifecycleResult{}, invalidRequest("instanceId is required")
	}
	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" || !protocol.IsBoundedString(idempotencyKey, protocol.InstanceIdempotencyKeyBytes) {
		return protocol.InstanceLifecycleResult{}, invalidRequest("idempotencyKey is required")
	}
	mode := input.Mode
	if mode == "" {
		mode = "drain"
	}
	if !slices.Contains(protocol.InstanceReleaseModes, mode) {
		return protocol.InstanceLifecycleResult{}, invalidRequest("mode must be drain or cancel")
	}

	sourceKey := caller.SourceKey()
	receiptKey := sourceKey + "\x00release\x00" + input.InstanceID + "\x00" + idempotencyKey
	digest := canonicalDigest(struct {
		InstanceID string `json:"instanceId"`
		Mode       string `json:"mode"`
	}{
		InstanceID: input.InstanceID,
		Mode:       mode,
	})

	var result protocol.InstanceLifecycleResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		instance, exists := state.Instances[input.InstanceID]
		if !exists {
			return notFound(fmt.Sprintf("instance %s does not exist", input.InstanceID))
		}
		if caller.Kind == "run" {
			if !caller.CanDelegate {
				return forbiddenError("this worker instance is not authorized to release instances")
			}
			if caller.ThreadID != "" && instance.ThreadID != caller.ThreadID {
				return forbiddenError("worker run cannot release instances in another thread")
			}
		}

		if existingReceipt, found := state.InstanceReceipts[receiptKey]; found {
			if existingReceipt.Digest != digest {
				return conflictError("idempotency_conflict", "idempotencyKey was already used for a different release_instance request")
			}
			var allocationPtr *protocol.InstanceAllocation
			if allocation, allocExists := state.LatestAllocationForInstance(instance.ID); allocExists {
				allocationCopy := allocation
				allocationPtr = &allocationCopy
			}
			result = protocol.InstanceLifecycleResult{
				Instance:   instance,
				Allocation: allocationPtr,
				Replayed:   true,
			}
			return nil
		}

		nowText := service.nowUTC()
		service.initiateInstanceReleaseInState(state, mutation, &instance, mode, nowText)

		state.InstanceReceipts[receiptKey] = protocol.InstanceLifecycleReceipt{
			ID:             protocol.NewIdentifier("ircpt"),
			ThreadID:       instance.ThreadID,
			SourceKey:      sourceKey,
			IdempotencyKey: idempotencyKey,
			Operation:      "release",
			Digest:         digest,
			InstanceID:     instance.ID,
			CreatedAt:      nowText,
		}

		var allocationPtr *protocol.InstanceAllocation
		if allocation, found := state.LatestAllocationForInstance(instance.ID); found {
			allocationCopy := allocation
			allocationPtr = &allocationCopy
		}
		result = protocol.InstanceLifecycleResult{
			Instance:   state.Instances[instance.ID],
			Allocation: allocationPtr,
			Replayed:   false,
		}
		return nil
	})
	return result, err
}

func (service *Service) initiateInstanceReleaseInState(
	state *store.State,
	mutation *store.Mutation,
	instance *protocol.AgentInstance,
	mode string,
	nowText string,
) {
	if protocol.IsTerminalInstanceStatus(instance.Status) {
		return
	}

	intent, hasIntent := state.ReleaseIntents[instance.ID]
	if !hasIntent {
		intent = protocol.InstanceReleaseIntent{
			InstanceID:  instance.ID,
			ThreadID:    instance.ThreadID,
			Mode:        mode,
			RequestedAt: nowText,
			UpdatedAt:   nowText,
		}
	} else {
		if intent.Mode == "cancel" && mode == "drain" {
			mode = "cancel"
		}
		intent.Mode = mode
		intent.UpdatedAt = nowText
	}
	state.ReleaseIntents[instance.ID] = intent

	if instance.Status != "draining" {
		instance.Status = "draining"
		instance.UpdatedAt = nowText
		state.Instances[instance.ID] = *instance
		service.syncInstanceMailboxInState(state, instance.ID, nowText)
	}

	for deliveryID, delivery := range state.InstanceDeliveries {
		if delivery.InstanceID == instance.ID && delivery.Kind == "provision" {
			delete(state.InstanceDeliveries, deliveryID)
		}
	}

	if mode == "cancel" {
		for _, runRecord := range state.Runs {
			if runRecord.InstanceID == instance.ID && protocol.IsActiveRunStatus(runRecord.Status) {
				runCopy := runRecord
				service.cancelRunInState(state, mutation, &runCopy, nowText)
				state.Runs[runCopy.ID] = runCopy
				if runCopy.TaskID != nil {
					if task, taskExists := state.Tasks[*runCopy.TaskID]; taskExists && !protocol.IsTerminalTaskStatus(task.Status) {
						task.Status = "cancelled"
						task.UpdatedAt = nowText
						task.FinishedAt = nowText
						state.Tasks[task.ID] = task
						recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
					}
				}
			}
		}
		for taskID, task := range state.Tasks {
			if task.PlacementInstanceID == instance.ID && (task.Status == "pending" || task.Status == "ready") {
				task.Status = "cancelled"
				task.UpdatedAt = nowText
				task.FinishedAt = nowText
				state.Tasks[taskID] = task
				recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
			}
		}
	}

	allocation, hasAllocation := state.ActiveAllocationForInstance(instance.ID)
	if !hasAllocation {
		instance.Status = "released"
		instance.UpdatedAt = nowText
		state.Instances[instance.ID] = *instance
		service.syncInstanceMailboxInState(state, instance.ID, nowText)
		delete(state.ReleaseIntents, instance.ID)
		return
	}

	delivery := store.InstanceDelivery{
		ID:           protocol.NewIdentifier("idlv"),
		NodeID:       allocation.NodeID,
		InstanceID:   instance.ID,
		AllocationID: allocation.ID,
		Kind:         "release",
		Mode:         mode,
		CreatedAt:    nowText,
	}
	state.InstanceDeliveries[delivery.ID] = delivery

	nodeID := allocation.NodeID
	instanceID := instance.ID
	allocationID := allocation.ID
	releaseMode := mode
	mutation.OnCommit(func() {
		_ = service.sendToSlave(nodeID, protocol.MasterControlMessage{
			Type:         "instance.release",
			InstanceID:   instanceID,
			AllocationID: allocationID,
			Mode:         releaseMode,
			At:           service.nowUTC(),
		})
	})
}

func (service *Service) handleInstanceReady(nodeID string, message protocol.SlaveControlMessage) {
	if !protocol.IsIdentifier(message.InstanceID) || !protocol.IsIdentifier(message.AllocationID) {
		return
	}
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		instance, instanceExists := state.Instances[message.InstanceID]
		allocation, allocExists := state.Allocations[message.AllocationID]
		if !instanceExists || !allocExists || allocation.InstanceID != instance.ID || allocation.NodeID != nodeID {
			return nil
		}

		for deliveryID, delivery := range state.InstanceDeliveries {
			if delivery.AllocationID == allocation.ID && delivery.Kind == "provision" {
				delete(state.InstanceDeliveries, deliveryID)
			}
		}

		nowText := service.nowUTC()
		if intent, draining := state.ReleaseIntents[instance.ID]; draining || instance.Status == "draining" {
			releaseMode := "drain"
			if draining && intent.Mode != "" {
				releaseMode = intent.Mode
			}
			mutation.OnCommit(func() {
				_ = service.sendToSlave(nodeID, protocol.MasterControlMessage{
					Type:         "instance.release",
					InstanceID:   instance.ID,
					AllocationID: allocation.ID,
					Mode:         releaseMode,
					At:           service.nowUTC(),
				})
			})
			return nil
		}

		if allocation.Status == "reserved" || allocation.Status == "provisioning" {
			allocation.Status = "active"
			allocation.UpdatedAt = nowText
			state.Allocations[allocation.ID] = allocation
		}
		if instance.Status == "requested" || instance.Status == "provisioning" {
			instance.Status = "ready"
			instance.Lease.ExpiresAt = extendLeaseTimestamp(nowText, instance.Lease.IdleTimeoutSeconds)
			instance.UpdatedAt = nowText
			state.Instances[instance.ID] = instance
		}
		service.syncInstanceMailboxInState(state, instance.ID, nowText)
		service.enqueuePendingInstanceMailWakeInState(state, instance.ID, nowText)

		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
		return nil
	})
}

func (service *Service) handleInstanceFailed(nodeID string, message protocol.SlaveControlMessage) {
	if !protocol.IsIdentifier(message.InstanceID) || !protocol.IsIdentifier(message.AllocationID) {
		return
	}
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		instance, instanceExists := state.Instances[message.InstanceID]
		allocation, allocExists := state.Allocations[message.AllocationID]
		if !instanceExists || !allocExists || allocation.InstanceID != instance.ID || allocation.NodeID != nodeID {
			return nil
		}
		for deliveryID, delivery := range state.InstanceDeliveries {
			if delivery.AllocationID == allocation.ID {
				delete(state.InstanceDeliveries, deliveryID)
			}
		}
		delete(state.ReleaseIntents, instance.ID)

		nowText := service.nowUTC()
		allocation.Status = "failed"
		allocation.UpdatedAt = nowText
		state.Allocations[allocation.ID] = allocation

		instance.Status = "failed"
		instance.UpdatedAt = nowText
		state.Instances[instance.ID] = instance
		service.syncInstanceMailboxInState(state, instance.ID, nowText)

		failureReason := "resident instance failed on slave node"
		if message.Error != nil && *message.Error != "" {
			failureReason = protocol.BoundedDiagnostic(service.redactor.Redact(*message.Error))
		}
		for taskID, task := range state.Tasks {
			if task.PlacementInstanceID == instance.ID && !protocol.IsTerminalTaskStatus(task.Status) {
				task.Status = "failed"
				task.Summary = failureReason
				task.UpdatedAt = nowText
				task.FinishedAt = nowText
				state.Tasks[taskID] = task
				recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
			}
		}

		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
		return nil
	})
}

func (service *Service) handleInstanceReleased(nodeID string, message protocol.SlaveControlMessage) {
	if !protocol.IsIdentifier(message.InstanceID) {
		return
	}
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		instance, instanceExists := state.Instances[message.InstanceID]
		if !instanceExists {
			return nil
		}
		nowText := service.nowUTC()
		if message.AllocationID != "" {
			if allocation, allocExists := state.Allocations[message.AllocationID]; allocExists && allocation.NodeID == nodeID {
				allocation.Status = "released"
				allocation.UpdatedAt = nowText
				state.Allocations[allocation.ID] = allocation
			}
		}
		for deliveryID, delivery := range state.InstanceDeliveries {
			if delivery.InstanceID == instance.ID {
				delete(state.InstanceDeliveries, deliveryID)
			}
		}
		delete(state.ReleaseIntents, instance.ID)

		for bindingID, binding := range state.SessionBindings {
			if binding.InstanceID == instance.ID && !protocol.IsTerminalSessionBindingStatus(binding.Status) {
				binding.Status = "closed"
				binding.UpdatedAt = nowText
				state.SessionBindings[bindingID] = binding
			}
		}

		if !protocol.IsTerminalInstanceStatus(instance.Status) {
			instance.Status = "released"
			instance.UpdatedAt = nowText
			state.Instances[instance.ID] = instance
		}
		service.syncInstanceMailboxInState(state, instance.ID, nowText)

		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
		return nil
	})
}

func (service *Service) expireIdleInstances() {
	now := service.now()
	nowText := now.UTC().Format(time.RFC3339Nano)
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		for _, instance := range state.Instances {
			if instance.Status != "ready" && instance.Status != "idle" {
				continue
			}
			expiresAt, parsed := protocol.ParseTimestamp(instance.Lease.ExpiresAt)
			if !parsed || expiresAt.After(now) {
				continue
			}
			instanceCopy := instance
			service.initiateInstanceReleaseInState(state, mutation, &instanceCopy, "drain", nowText)
		}
		return nil
	})
}

func extendLeaseTimestamp(nowText string, timeoutSeconds int) string {
	baseTime, parsed := protocol.ParseTimestamp(nowText)
	if !parsed {
		baseTime = time.Now().UTC()
	}
	if timeoutSeconds <= 0 {
		timeoutSeconds = protocol.DefaultInstanceIdleTimeoutSeconds
	}
	return baseTime.Add(time.Duration(timeoutSeconds) * time.Second).UTC().Format(time.RFC3339Nano)
}

func validatePurpose(purpose *protocol.InstancePurpose) error {
	if purpose == nil {
		return nil
	}
	if purpose.Name != nil && !protocol.IsBoundedString(*purpose.Name, protocol.InstanceNameMaximumBytes) {
		return invalidRequest("purpose.name exceeds maximum length")
	}
	if purpose.Title != nil && !protocol.IsBoundedString(*purpose.Title, protocol.InstanceNameMaximumBytes) {
		return invalidRequest("purpose.title exceeds maximum length")
	}
	if purpose.Summary != nil && !protocol.IsBoundedString(*purpose.Summary, protocol.InstanceSummaryMaximumBytes) {
		return invalidRequest("purpose.summary exceeds maximum length")
	}
	if purpose.Instructions != nil && !protocol.IsBoundedString(*purpose.Instructions, protocol.InstanceInstructionsMaximumBytes) {
		return invalidRequest("purpose.instructions exceeds maximum length")
	}
	return nil
}

func instanceRequirementsToExecutionRequirements(input protocol.InstanceExecutionRequirements) protocol.ExecutionRequirements {
	var converted protocol.ExecutionRequirements
	if input.HarnessIDs != nil {
		converted.HarnessIDs = slices.Clone(*input.HarnessIDs)
	}
	if input.Models != nil {
		converted.Models = slices.Clone(*input.Models)
	}
	if input.Transports != nil {
		converted.Transports = slices.Clone(*input.Transports)
	}
	if input.OperatingSystems != nil {
		converted.OperatingSystems = slices.Clone(*input.OperatingSystems)
	}
	if input.Architectures != nil {
		converted.Architectures = slices.Clone(*input.Architectures)
	}
	if input.Labels != nil {
		converted.Labels = slices.Clone(*input.Labels)
	}
	converted.MinimumConcurrency = input.MinimumConcurrency
	converted.MinimumMemoryMegabytes = input.MinimumMemoryMegabytes
	if input.ProjectProfileID != nil {
		converted.ProjectProfileID = *input.ProjectProfileID
	}
	if input.Workspace != nil {
		var ws protocol.WorkspaceRequirements
		if input.Workspace.Repository != nil {
			ws.Repository = *input.Workspace.Repository
		}
		if input.Workspace.Path != nil {
			ws.Path = *input.Workspace.Path
		}
		ws.Writable = input.Workspace.Writable
		converted.Workspace = &ws
	}
	if input.Preferences != nil {
		var prefs protocol.ExecutionPreferences
		if input.Preferences.NodeIDs != nil {
			prefs.NodeIDs = slices.Clone(*input.Preferences.NodeIDs)
		}
		if input.Preferences.HarnessIDs != nil {
			prefs.HarnessIDs = slices.Clone(*input.Preferences.HarnessIDs)
		}
		if input.Preferences.Models != nil {
			prefs.Models = slices.Clone(*input.Preferences.Models)
		}
		if input.Preferences.Labels != nil {
			prefs.Labels = slices.Clone(*input.Preferences.Labels)
		}
		converted.Preferences = &prefs
	}
	return converted
}
