package orchestrator

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/activitylog"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

const maximumForkWarmContextBytes = 12 * 1024

type SpawnInstanceInput struct {
	ThreadID           string                                  `json:"threadId,omitempty"`
	IdempotencyKey     string                                  `json:"idempotencyKey"`
	Handle             string                                  `json:"handle,omitempty"`
	PublicBrief        bool                                    `json:"publicBrief,omitempty"`
	Brief              string                                  `json:"brief,omitempty"`
	Count              int                                     `json:"count,omitempty"`
	ForkFromInstanceID string                                  `json:"forkFromInstanceId,omitempty"`
	RoomChannels       []string                                `json:"roomChannels,omitempty"`
	Purpose            *protocol.InstancePurpose               `json:"purpose,omitempty"`
	Delegation         *protocol.InstanceDelegationPolicy      `json:"delegation,omitempty"`
	Requirements       *protocol.InstanceExecutionRequirements `json:"requirements,omitempty"`
	IdleTimeoutSeconds *int                                    `json:"idleTimeoutSeconds,omitempty"`
	InitialTask        *protocol.InstanceInitialTask           `json:"initialTask,omitempty"`
	Coordinator        bool                                    `json:"coordinator,omitempty"`
}

type GetInstanceInput struct {
	InstanceID string `json:"instanceId"`
}

type ReconfigureInstanceInput struct {
	InstanceID     string `json:"instanceId"`
	IdempotencyKey string `json:"idempotencyKey"`
	Model          string `json:"model"`
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

	if input.Count < 0 || input.Count > protocol.MaximumSpawnInstanceCount {
		return protocol.InstanceLifecycleResult{}, invalidRequest(fmt.Sprintf(
			"count must be between 1 and %d",
			protocol.MaximumSpawnInstanceCount,
		))
	}
	instanceCount := input.Count
	if instanceCount == 0 {
		instanceCount = 1
	}
	if input.Coordinator {
		if instanceCount != 1 {
			return protocol.InstanceLifecycleResult{}, invalidRequest("coordinator binding requires count=1")
		}
		if input.Delegation == nil || !input.Delegation.CanDelegate {
			return protocol.InstanceLifecycleResult{}, forbiddenError("coordinator instance requires delegation.canDelegate=true")
		}
	}

	fallbackPurposeName := ""
	if input.Purpose != nil && input.Purpose.Name != nil {
		fallbackPurposeName = *input.Purpose.Name
	}
	derivedHandles, handleErr := protocol.DeriveShardedInstanceHandles(input.Handle, fallbackPurposeName, instanceCount)
	if handleErr != nil {
		return protocol.InstanceLifecycleResult{}, invalidRequest(handleErr.Error())
	}

	trimmedBrief := strings.TrimSpace(input.Brief)
	if trimmedBrief != "" && !protocol.IsBoundedString(trimmedBrief, protocol.TextMaximumBytes) {
		return protocol.InstanceLifecycleResult{}, invalidRequest("brief exceeds maximum length")
	}

	forkFromInstanceID := strings.TrimSpace(input.ForkFromInstanceID)
	if forkFromInstanceID != "" && !protocol.IsIdentifier(forkFromInstanceID) {
		return protocol.InstanceLifecycleResult{}, invalidRequest("forkFromInstanceId is malformed")
	}

	normalizedRoomChannels, channelErr := protocol.NormalizeRoomChannels(input.RoomChannels)
	if channelErr != nil {
		return protocol.InstanceLifecycleResult{}, invalidRequest(channelErr.Error())
	}

	sourceKey := caller.SourceKey()
	receiptKey := sourceKey + "\x00create\x00" + threadID + "\x00" + idempotencyKey
	digest := canonicalDigest(input)

	var result protocol.InstanceLifecycleResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		thread, exists := state.Threads[threadID]
		if !exists {
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
			var replayedInstances []protocol.AgentInstance
			var replayedAllocations []protocol.InstanceAllocation
			if len(existingReceipt.InstanceIDs) > 1 {
				replayedInstances = make([]protocol.AgentInstance, 0, len(existingReceipt.InstanceIDs))
				replayedAllocations = make([]protocol.InstanceAllocation, 0, len(existingReceipt.InstanceIDs))
				for _, recordedInstanceID := range existingReceipt.InstanceIDs {
					if recordedInstance, recordedExists := state.Instances[recordedInstanceID]; recordedExists {
						replayedInstances = append(replayedInstances, recordedInstance)
					}
					if recordedAllocation, recordedAllocExists := state.LatestAllocationForInstance(recordedInstanceID); recordedAllocExists {
						replayedAllocations = append(replayedAllocations, recordedAllocation)
					}
				}
			}
			var replayedInitialTaskIDs []string
			if len(existingReceipt.InitialTaskIDs) > 1 {
				replayedInitialTaskIDs = slices.Clone(existingReceipt.InitialTaskIDs)
			}
			var replayedBriefSequence *int64
			if existingReceipt.BriefRoomSequence != nil {
				sequenceCopy := *existingReceipt.BriefRoomSequence
				replayedBriefSequence = &sequenceCopy
			}
			result = protocol.InstanceLifecycleResult{
				Instance:          instance,
				Allocation:        allocationPtr,
				InitialTaskID:     existingReceipt.InitialTaskID,
				Instances:         replayedInstances,
				Allocations:       replayedAllocations,
				InitialTaskIDs:    replayedInitialTaskIDs,
				BriefRoomSequence: replayedBriefSequence,
				Replayed:          true,
			}
			return nil
		}

		if len(state.Instances)+instanceCount > protocol.InstanceCountMaximum {
			return newRetryableError("capacity_unavailable", 503, "instance table capacity exhausted")
		}

		var instanceRequirements protocol.InstanceExecutionRequirements
		if input.Requirements != nil {
			instanceRequirements = cloneInstanceExecutionRequirements(*input.Requirements)
		}

		instanceRoomChannels := slices.Clone(normalizedRoomChannels)
		basePurpose := cloneInstancePurpose(input.Purpose)
		if forkFromInstanceID != "" {
			sourceInstance, sourceExists := state.Instances[forkFromInstanceID]
			if !sourceExists {
				return notFound(fmt.Sprintf("source instance %s does not exist", forkFromInstanceID))
			}
			if sourceInstance.ThreadID != threadID {
				return invalidRequest(fmt.Sprintf("source instance %s does not belong to thread %s", forkFromInstanceID, threadID))
			}
			if input.Requirements == nil {
				instanceRequirements = cloneInstanceExecutionRequirements(sourceInstance.Requirements)
			}
			if len(input.RoomChannels) == 0 && len(sourceInstance.RoomChannels) > 0 {
				instanceRoomChannels = slices.Clone(sourceInstance.RoomChannels)
			}
			warmContextDigest := service.buildInstanceForkWarmContext(state, sourceInstance)
			basePurpose = mergeForkWarmContextIntoPurpose(basePurpose, sourceInstance, warmContextDigest)
		}

		taskStyleRequirements := instanceRequirementsToExecutionRequirements(instanceRequirements)
		nowText := service.nowUTC()
		expiresAt := extendLeaseTimestamp(nowText, timeoutSeconds)

		var delegation protocol.InstanceDelegationPolicy
		if input.Delegation != nil {
			delegation = *input.Delegation
		}

		type pendingShardProvision struct {
			nodeID     string
			instance   protocol.AgentInstance
			allocation protocol.InstanceAllocation
		}

		spawnedInstances := make([]protocol.AgentInstance, 0, instanceCount)
		spawnedAllocations := make([]protocol.InstanceAllocation, 0, instanceCount)
		spawnedMailboxes := make([]protocol.AgentMailbox, 0, instanceCount)
		createdInstanceIDs := make([]string, 0, instanceCount)
		createdTaskIDs := make([]string, 0, instanceCount)
		pendingProvisions := make([]pendingShardProvision, 0, instanceCount)

		for shardIndex := range instanceCount {
			selection, diagnostic := service.selectOfferingForRequirements(state, taskStyleRequirements, nil, true)
			if selection == nil {
				detail := "no connected, synced slave node satisfies instance requirements"
				if len(diagnostic.Unsatisfied) > 0 {
					detail = diagnostic.Unsatisfied[0].Detail
				}
				return newRetryableError("capacity_unavailable", 503, detail)
			}

			instanceID := protocol.NewIdentifier("inst")
			allocationID := protocol.NewIdentifier("alloc")

			shardPurpose := cloneInstancePurpose(basePurpose)
			desiredHandle := derivedHandles[shardIndex]
			if instanceCount > 1 && desiredHandle != "" {
				if shardPurpose == nil {
					shardPurpose = &protocol.InstancePurpose{}
				}
				handleCopy := desiredHandle
				shardPurpose.Name = &handleCopy
			}

			instance := protocol.AgentInstance{
				ID:                 instanceID,
				ThreadID:           threadID,
				ForkFromInstanceID: forkFromInstanceID,
				Creator:            caller.InstanceCreator(),
				Purpose:            shardPurpose,
				Delegation:         delegation,
				Requirements:       cloneInstanceExecutionRequirements(instanceRequirements),
				RoomChannels:       slices.Clone(instanceRoomChannels),
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

			mailbox := service.ensureInstanceMailboxInState(state, instance, allocation, nowText)
			if desiredHandle != "" {
				assignedHandle := allocateUniqueHandle(state, desiredHandle, mailbox.Host, mailbox.ID)
				mailbox.Handle = assignedHandle
				mailbox.Address = assignedHandle + "@" + mailbox.Host
				mailbox.CustomHandle = true
				state.Mailboxes[mailbox.ID] = mailbox
			}
			instance.Handle = mailbox.Handle
			state.Instances[instanceID] = instance

			delivery := store.InstanceDelivery{
				ID:           protocol.NewIdentifier("idlv"),
				NodeID:       selection.node.ID,
				InstanceID:   instanceID,
				AllocationID: allocationID,
				Kind:         "provision",
				CreatedAt:    nowText,
			}
			state.InstanceDeliveries[delivery.ID] = delivery

			if input.InitialTask != nil {
				taskID := protocol.NewIdentifier("task")
				parentTaskID := ""
				if caller.Kind == "run" {
					parentTaskID = caller.TaskID
				}
				taskTitle := strings.TrimSpace(input.InitialTask.Title)
				if instanceCount > 1 {
					taskTitle = fmt.Sprintf("%s (%s)", taskTitle, instance.Handle)
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
					Title:               taskTitle,
					Instructions:        strings.TrimSpace(input.InitialTask.Instructions),
					Status:              "ready",
					AttemptCount:        0,
					MaxAttempts:         protocol.MaximumTaskAttempts,
					Dependencies:        []protocol.TaskDependency{},
					Requirements:        taskStyleRequirements,
					PlacementInstanceID: instanceID,
					CreatedAt:           nowText,
					UpdatedAt:           nowText,
				}
				state.Tasks[taskID] = task
				createdTaskIDs = append(createdTaskIDs, taskID)
			}

			spawnedInstances = append(spawnedInstances, instance)
			spawnedAllocations = append(spawnedAllocations, allocation)
			spawnedMailboxes = append(spawnedMailboxes, mailbox)
			createdInstanceIDs = append(createdInstanceIDs, instanceID)
			pendingProvisions = append(pendingProvisions, pendingShardProvision{
				nodeID:     selection.node.ID,
				instance:   instance,
				allocation: allocation,
			})
		}

		var briefRoomSequence *int64
		if input.PublicBrief || trimmedBrief != "" {
			postedSequence := service.appendRosterBriefToRoomInState(
				state,
				mutation,
				thread,
				caller,
				spawnedInstances,
				spawnedMailboxes,
				trimmedBrief,
				input.InitialTask,
				nowText,
			)
			briefRoomSequence = &postedSequence
		}

		var primaryTaskID *string
		if len(createdTaskIDs) > 0 {
			firstTaskID := createdTaskIDs[0]
			primaryTaskID = &firstTaskID
		}

		if input.Coordinator && len(createdInstanceIDs) == 1 {
			if bindErr := service.bindThreadCoordinatorInState(state, mutation, caller, &thread, createdInstanceIDs[0], nowText); bindErr != nil {
				return bindErr
			}
			thread.UpdatedAt = nowText
			state.Threads[threadID] = thread
		}

		var resultInstances []protocol.AgentInstance
		var resultAllocations []protocol.InstanceAllocation
		var resultInitialTaskIDs []string
		if instanceCount > 1 {
			resultInstances = slices.Clone(spawnedInstances)
			resultAllocations = slices.Clone(spawnedAllocations)
			if len(createdTaskIDs) > 0 {
				resultInitialTaskIDs = slices.Clone(createdTaskIDs)
			}
		}

		var receiptBriefSequence *int64
		if briefRoomSequence != nil {
			sequenceCopy := *briefRoomSequence
			receiptBriefSequence = &sequenceCopy
		}

		state.InstanceReceipts[receiptKey] = protocol.InstanceLifecycleReceipt{
			ID:                protocol.NewIdentifier("ircpt"),
			ThreadID:          threadID,
			SourceKey:         sourceKey,
			IdempotencyKey:    idempotencyKey,
			Operation:         "create",
			Digest:            digest,
			InstanceID:        spawnedInstances[0].ID,
			InitialTaskID:     primaryTaskID,
			InstanceIDs:       slices.Clone(createdInstanceIDs),
			InitialTaskIDs:    slices.Clone(createdTaskIDs),
			BriefRoomSequence: receiptBriefSequence,
			CreatedAt:         nowText,
		}

		primaryAllocationCopy := spawnedAllocations[0]
		result = protocol.InstanceLifecycleResult{
			Instance:          spawnedInstances[0],
			Allocation:        &primaryAllocationCopy,
			InitialTaskID:     primaryTaskID,
			Instances:         resultInstances,
			Allocations:       resultAllocations,
			InitialTaskIDs:    resultInitialTaskIDs,
			BriefRoomSequence: briefRoomSequence,
			Replayed:          false,
		}

		hasInitialTasks := len(createdTaskIDs) > 0
		mutation.OnCommit(func() {
			for _, provisionItem := range pendingProvisions {
				instanceSnapshot := provisionItem.instance
				allocationSnapshot := provisionItem.allocation
				service.logger.Info(
					"instance",
					"spawned",
					activitylog.String("instance", instanceSnapshot.ID),
					activitylog.String("handle", instanceSnapshot.Handle),
					activitylog.String("node", provisionItem.nodeID),
					activitylog.String("harness", allocationSnapshot.HarnessID),
					activitylog.String("model", allocationSnapshot.Model),
					activitylog.String("thread", instanceSnapshot.ThreadID),
				)
				_ = service.sendToSlave(provisionItem.nodeID, protocol.MasterControlMessage{
					Type:       "instance.provision",
					Instance:   &instanceSnapshot,
					Allocation: &allocationSnapshot,
					At:         service.nowUTC(),
				})
			}
			if hasInitialTasks {
				service.TriggerSchedule()
			}
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

func (service *Service) ReconfigureInstance(
	caller CallerIdentity,
	input ReconfigureInstanceInput,
) (protocol.InstanceLifecycleResult, error) {
	if !protocol.IsIdentifier(input.InstanceID) {
		return protocol.InstanceLifecycleResult{}, invalidRequest("instanceId is required")
	}
	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" || !protocol.IsBoundedString(idempotencyKey, protocol.InstanceIdempotencyKeyBytes) {
		return protocol.InstanceLifecycleResult{}, invalidRequest("idempotencyKey is required and must be at most 128 bytes")
	}
	targetModel := strings.TrimSpace(input.Model)
	if targetModel == "" || !protocol.IsBoundedString(targetModel, protocol.InstanceNameMaximumBytes) || strings.ContainsAny(targetModel, "\x00\r\n") {
		return protocol.InstanceLifecycleResult{}, invalidRequest("model is required")
	}

	sourceKey := caller.SourceKey()
	receiptKey := sourceKey + "\x00reconfigure\x00" + input.InstanceID + "\x00" + idempotencyKey
	digest := canonicalDigest(struct {
		InstanceID string `json:"instanceId"`
		Model      string `json:"model"`
	}{
		InstanceID: input.InstanceID,
		Model:      targetModel,
	})

	var result protocol.InstanceLifecycleResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		instance, exists := state.Instances[input.InstanceID]
		if !exists {
			return notFound(fmt.Sprintf("instance %s does not exist", input.InstanceID))
		}
		if caller.Kind == "run" {
			if !caller.CanDelegate {
				return forbiddenError("this worker instance is not authorized to reconfigure instances")
			}
			if caller.ThreadID != "" && instance.ThreadID != caller.ThreadID {
				return forbiddenError("worker run cannot reconfigure instances in another thread")
			}
		}

		if existingReceipt, found := state.InstanceReceipts[receiptKey]; found {
			if existingReceipt.Digest != digest {
				return conflictError("idempotency_conflict", "idempotencyKey was already used for a different reconfigure_instance request")
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

		if instance.Status != "idle" && instance.Status != "ready" {
			return conflictError(
				"invalid_transition",
				fmt.Sprintf("cannot reconfigure instance %s in status %s; instance must be ready or idle", instance.ID, instance.Status),
			)
		}
		for _, runRecord := range state.Runs {
			if runRecord.InstanceID == instance.ID && protocol.IsActiveRunStatus(runRecord.Status) {
				return conflictError(
					"invalid_transition",
					fmt.Sprintf("cannot reconfigure instance %s while run %s is %s", instance.ID, runRecord.ID, runRecord.Status),
				)
			}
		}

		allocation, hasAllocation := state.ActiveAllocationForInstance(instance.ID)
		if !hasAllocation || allocation.Status != "active" {
			return conflictError(
				"invalid_transition",
				fmt.Sprintf("instance %s does not have an active resident allocation", instance.ID),
			)
		}

		node, nodeExists := state.Nodes[allocation.NodeID]
		if !nodeExists {
			return conflictError("node_unavailable", fmt.Sprintf("allocated node %s does not exist", allocation.NodeID))
		}
		var matchedProfile *protocol.HarnessProfile
		for index := range node.Harnesses {
			profile := node.Harnesses[index]
			if profile.ID == allocation.HarnessID && profile.Available {
				matchedProfile = &profile
				break
			}
		}
		if matchedProfile == nil {
			return invalidRequest(fmt.Sprintf("harness %s is not available on node %s", allocation.HarnessID, allocation.NodeID))
		}
		if !isHarnessModelSupported(*matchedProfile, targetModel) {
			return invalidRequest(fmt.Sprintf(
				"model %q is not supported by harness %s on node %s",
				targetModel,
				allocation.HarnessID,
				allocation.NodeID,
			))
		}

		nowText := service.nowUTC()
		previousModel := allocation.Model
		allocation.Model = targetModel
		allocation.UpdatedAt = nowText
		state.Allocations[allocation.ID] = allocation

		if instance.Requirements.Models != nil && len(*instance.Requirements.Models) > 0 && !slices.Contains(*instance.Requirements.Models, targetModel) {
			updatedModels := []string{targetModel}
			instance.Requirements.Models = &updatedModels
		}
		instance.UpdatedAt = nowText
		state.Instances[instance.ID] = instance

		state.InstanceReceipts[receiptKey] = protocol.InstanceLifecycleReceipt{
			ID:             protocol.NewIdentifier("ircpt"),
			ThreadID:       instance.ThreadID,
			SourceKey:      sourceKey,
			IdempotencyKey: idempotencyKey,
			Operation:      "reconfigure",
			Digest:         digest,
			InstanceID:     instance.ID,
			CreatedAt:      nowText,
		}

		mutation.EmitEvent(protocol.TaskEventEntry{
			ThreadID:   instance.ThreadID,
			Type:       "instance.reconfigured",
			InstanceID: instance.ID,
			Status:     instance.Status,
			Summary:    fmt.Sprintf("instance %s switched model from %s to %s", instance.ID, previousModel, targetModel),
			Detail: map[string]any{
				"allocationId":  allocation.ID,
				"nodeId":        allocation.NodeID,
				"harnessId":     allocation.HarnessID,
				"previousModel": previousModel,
				"model":         targetModel,
			},
			OccurredAt: nowText,
		})

		allocationCopy := allocation
		result = protocol.InstanceLifecycleResult{
			Instance:   instance,
			Allocation: &allocationCopy,
			Replayed:   false,
		}

		targetNodeID := allocation.NodeID
		instanceSnapshot := instance
		allocationSnapshot := allocation
		mutation.OnCommit(func() {
			service.logger.Info(
				"instance",
				"reconfigured",
				activitylog.String("instance", instanceSnapshot.ID),
				activitylog.String("node", targetNodeID),
				activitylog.String("harness", allocationSnapshot.HarnessID),
				activitylog.String("previousModel", previousModel),
				activitylog.String("model", targetModel),
			)
			_ = service.sendToSlave(targetNodeID, protocol.MasterControlMessage{
				Type:         protocol.InstanceReconfigureControlType,
				InstanceID:   instanceSnapshot.ID,
				AllocationID: allocationSnapshot.ID,
				Model:        targetModel,
				Instance:     &instanceSnapshot,
				Allocation:   &allocationSnapshot,
				At:           service.nowUTC(),
			})
			service.TriggerSchedule()
		})
		return nil
	})
	return result, err
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
	if err == nil && !result.Replayed {
		service.logger.Info(
			"instance",
			"releasing",
			activitylog.String("instance", result.Instance.ID),
			activitylog.String("mode", mode),
			activitylog.String("status", result.Instance.Status),
		)
	}
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
	service.cleanupCoordinatorOnInstanceUnavailableInState(state, mutation, instance.ID, nowText)

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
		service.cleanupCoordinatorOnInstanceUnavailableInState(state, mutation, instance.ID, service.nowUTC())
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
		service.enqueuePendingCoordinatorWakeInState(state, instance.ID, nowText)

		instanceSnapshot := instance
		allocationSnapshot := allocation
		mutation.OnCommit(func() {
			service.logger.Info(
				"instance",
				"ready",
				activitylog.String("instance", instanceSnapshot.ID),
				activitylog.String("node", nodeID),
				activitylog.String("harness", allocationSnapshot.HarnessID),
				activitylog.String("model", allocationSnapshot.Model),
				activitylog.String("transport", allocationSnapshot.Transport),
			)
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
		service.cleanupCoordinatorOnInstanceUnavailableInState(state, mutation, instance.ID, service.nowUTC())

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

		instanceID := instance.ID
		mutation.OnCommit(func() {
			service.logger.Error(
				"instance",
				"failed",
				activitylog.String("instance", instanceID),
				activitylog.String("node", nodeID),
				activitylog.String("error", failureReason),
			)
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
		service.cleanupCoordinatorOnInstanceUnavailableInState(state, mutation, instance.ID, service.nowUTC())

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

		instanceID := instance.ID
		mutation.OnCommit(func() {
			service.logger.Info(
				"instance",
				"released",
				activitylog.String("instance", instanceID),
				activitylog.String("node", nodeID),
			)
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

func cloneInstancePurpose(purpose *protocol.InstancePurpose) *protocol.InstancePurpose {
	if purpose == nil {
		return nil
	}
	cloned := &protocol.InstancePurpose{}
	if purpose.Name != nil {
		value := *purpose.Name
		cloned.Name = &value
	}
	if purpose.Title != nil {
		value := *purpose.Title
		cloned.Title = &value
	}
	if purpose.Summary != nil {
		value := *purpose.Summary
		cloned.Summary = &value
	}
	if purpose.Instructions != nil {
		value := *purpose.Instructions
		cloned.Instructions = &value
	}
	return cloned
}

func cloneOptionalStringSlice(source *[]string) *[]string {
	if source == nil {
		return nil
	}
	cloned := slices.Clone(*source)
	return &cloned
}

func cloneInstanceExecutionRequirements(input protocol.InstanceExecutionRequirements) protocol.InstanceExecutionRequirements {
	cloned := protocol.InstanceExecutionRequirements{
		HarnessIDs:       cloneOptionalStringSlice(input.HarnessIDs),
		Models:           cloneOptionalStringSlice(input.Models),
		Transports:       cloneOptionalStringSlice(input.Transports),
		OperatingSystems: cloneOptionalStringSlice(input.OperatingSystems),
		Architectures:    cloneOptionalStringSlice(input.Architectures),
		Labels:           cloneOptionalStringSlice(input.Labels),
	}
	if input.MinimumConcurrency != nil {
		value := *input.MinimumConcurrency
		cloned.MinimumConcurrency = &value
	}
	if input.MinimumMemoryMegabytes != nil {
		value := *input.MinimumMemoryMegabytes
		cloned.MinimumMemoryMegabytes = &value
	}
	if input.ProjectProfileID != nil {
		value := *input.ProjectProfileID
		cloned.ProjectProfileID = &value
	}
	if input.Workspace != nil {
		workspaceCopy := protocol.InstanceWorkspaceRequirements{
			Writable: input.Workspace.Writable,
		}
		if input.Workspace.Repository != nil {
			value := *input.Workspace.Repository
			workspaceCopy.Repository = &value
		}
		if input.Workspace.Path != nil {
			value := *input.Workspace.Path
			workspaceCopy.Path = &value
		}
		cloned.Workspace = &workspaceCopy
	}
	if input.Preferences != nil {
		cloned.Preferences = &protocol.InstanceExecutionPreferences{
			NodeIDs:    cloneOptionalStringSlice(input.Preferences.NodeIDs),
			HarnessIDs: cloneOptionalStringSlice(input.Preferences.HarnessIDs),
			Models:     cloneOptionalStringSlice(input.Preferences.Models),
			Labels:     cloneOptionalStringSlice(input.Preferences.Labels),
		}
	}
	return cloned
}

func (service *Service) buildInstanceForkWarmContext(
	state *store.State,
	sourceInstance protocol.AgentInstance,
) string {
	var builder strings.Builder
	sourceLabel := sourceInstance.ID
	if sourceInstance.Handle != "" {
		sourceLabel = "@" + sourceInstance.Handle + " (" + sourceInstance.ID + ")"
	}
	fmt.Fprintf(&builder, "Warm Context Forked From %s:\n", sourceLabel)
	if sourceInstance.Purpose != nil {
		if sourceInstance.Purpose.Title != nil && strings.TrimSpace(*sourceInstance.Purpose.Title) != "" {
			fmt.Fprintf(&builder, "Source Role: %s\n", strings.TrimSpace(*sourceInstance.Purpose.Title))
		}
		if sourceInstance.Purpose.Summary != nil && strings.TrimSpace(*sourceInstance.Purpose.Summary) != "" {
			fmt.Fprintf(&builder, "Source Summary: %s\n", strings.TrimSpace(*sourceInstance.Purpose.Summary))
		}
	}

	var sourceRuns []protocol.Run
	for _, runRecord := range state.Runs {
		if runRecord.InstanceID == sourceInstance.ID && runRecord.ThreadID == sourceInstance.ThreadID {
			sourceRuns = append(sourceRuns, runRecord)
		}
	}
	slices.SortFunc(sourceRuns, func(left, right protocol.Run) int {
		if compared := cmp.Compare(left.CreatedAt, right.CreatedAt); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})

	if len(sourceRuns) == 0 {
		builder.WriteString("No completed prior runs recorded on source instance yet.\n")
		return protocol.TruncateUTF8Head(service.redactor.Redact(builder.String()), maximumForkWarmContextBytes)
	}

	builder.WriteString("\nPrior Run Transcript Digest:\n")
	for _, runRecord := range sourceRuns {
		taskHeader := ""
		if runRecord.TaskID != nil {
			if task, taskExists := state.Tasks[*runRecord.TaskID]; taskExists && task.Title != "" {
				taskHeader = " (" + task.Title + ")"
			}
		}
		fmt.Fprintf(&builder, "- Run %s%s [status=%s, model=%s]:\n", runRecord.ID, taskHeader, runRecord.Status, runRecord.Model)
		if transcript, hasTranscript := state.RunTranscripts[runRecord.ID]; hasTranscript && len(transcript.Entries) > 0 {
			for _, entry := range transcript.Entries {
				switch entry.Kind {
				case "message":
					if text := strings.TrimSpace(entry.Text); text != "" {
						fmt.Fprintf(&builder, "  Assistant: %s\n", protocol.TruncateUTF8Head(text, 1200))
					}
				case "tool", "tool.call":
					if title := strings.TrimSpace(entry.Title); title != "" {
						status := entry.Status
						if status == "" {
							status = "recorded"
						}
						fmt.Fprintf(&builder, "  Tool [%s]: %s\n", status, protocol.TruncateUTF8Head(title, 300))
					}
				case "diff":
					if filePath := strings.TrimSpace(entry.Path); filePath != "" {
						fmt.Fprintf(&builder, "  Edited file: %s\n", filePath)
					}
				case "plan":
					for _, planItem := range entry.Entries {
						if content := strings.TrimSpace(planItem.Content); content != "" {
							fmt.Fprintf(&builder, "  Plan [%s]: %s\n", planItem.Status, protocol.TruncateUTF8Head(content, 300))
						}
					}
				case "warning":
					if warningText := strings.TrimSpace(entry.Message); warningText != "" {
						fmt.Fprintf(&builder, "  Warning: %s\n", protocol.TruncateUTF8Head(warningText, 300))
					}
				}
			}
		}
		if outputText := strings.TrimSpace(runRecord.Output); outputText != "" {
			fmt.Fprintf(&builder, "  Output: %s\n", protocol.TruncateUTF8Head(outputText, 1600))
		}
		if runRecord.Error != nil && strings.TrimSpace(*runRecord.Error) != "" {
			fmt.Fprintf(&builder, "  Error: %s\n", protocol.TruncateUTF8Head(strings.TrimSpace(*runRecord.Error), 600))
		}
	}

	redacted := service.redactor.Redact(builder.String())
	return protocol.TruncateUTF8Head(redacted, maximumForkWarmContextBytes)
}

func mergeForkWarmContextIntoPurpose(
	basePurpose *protocol.InstancePurpose,
	sourceInstance protocol.AgentInstance,
	warmContextDigest string,
) *protocol.InstancePurpose {
	merged := cloneInstancePurpose(basePurpose)
	if merged == nil {
		merged = &protocol.InstancePurpose{}
	}
	if merged.Title == nil && sourceInstance.Purpose != nil && sourceInstance.Purpose.Title != nil {
		titleCopy := *sourceInstance.Purpose.Title
		merged.Title = &titleCopy
	}
	if merged.Summary == nil && sourceInstance.Purpose != nil && sourceInstance.Purpose.Summary != nil {
		summaryCopy := *sourceInstance.Purpose.Summary
		merged.Summary = &summaryCopy
	}

	var combinedInstructions strings.Builder
	if merged.Instructions != nil && strings.TrimSpace(*merged.Instructions) != "" {
		combinedInstructions.WriteString(strings.TrimSpace(*merged.Instructions))
		combinedInstructions.WriteString("\n\n")
	} else if sourceInstance.Purpose != nil && sourceInstance.Purpose.Instructions != nil && strings.TrimSpace(*sourceInstance.Purpose.Instructions) != "" {
		combinedInstructions.WriteString(strings.TrimSpace(*sourceInstance.Purpose.Instructions))
		combinedInstructions.WriteString("\n\n")
	}
	combinedInstructions.WriteString(strings.TrimSpace(warmContextDigest))
	boundedInstructions := protocol.TruncateUTF8Head(combinedInstructions.String(), protocol.InstanceInstructionsMaximumBytes)
	merged.Instructions = &boundedInstructions
	return merged
}

func (service *Service) appendRosterBriefToRoomInState(
	state *store.State,
	mutation *store.Mutation,
	thread protocol.Thread,
	caller CallerIdentity,
	instances []protocol.AgentInstance,
	mailboxes []protocol.AgentMailbox,
	explicitBrief string,
	initialTask *protocol.InstanceInitialTask,
	nowText string,
) int64 {
	head, hasHead := state.RoomHeads[thread.ID]
	if !hasHead {
		head = protocol.ThreadRoomHead{
			ThreadID:      thread.ID,
			DirectoryName: protocol.RoomDirectoryName(thread.ID, thread.CreatedAt),
			HeadSequence:  0,
			CreatedAt:     thread.CreatedAt,
			UpdatedAt:     thread.UpdatedAt,
		}
	}
	nextSequence := head.HeadSequence + 1

	mentionParts := make([]string, 0, len(instances))
	for index, instance := range instances {
		handle := instance.Handle
		address := ""
		if index < len(mailboxes) {
			if handle == "" {
				handle = mailboxes[index].Handle
			}
			address = mailboxes[index].Address
		}
		switch {
		case address != "":
			mentionParts = append(mentionParts, fmt.Sprintf("@%s (%s)", handle, address))
		case handle != "":
			mentionParts = append(mentionParts, "@"+handle)
		default:
			mentionParts = append(mentionParts, instance.ID)
		}
	}

	topic := "roster:" + instances[0].Handle
	if topic == "roster:" {
		topic = "roster:" + instances[0].ID
	}
	if len(instances) > 1 && instances[0].Handle != "" && instances[len(instances)-1].Handle != "" {
		topic = fmt.Sprintf("roster:%s..%s", instances[0].Handle, instances[len(instances)-1].Handle)
	}
	topic = protocol.TruncateUTF8Head(service.redactor.Redact(topic), protocol.RoomTopicMaximumBytes)

	var bodyBuilder strings.Builder
	fmt.Fprintf(&bodyBuilder, "Roster assignment for %s", strings.Join(mentionParts, ", "))
	if instances[0].ForkFromInstanceID != "" {
		fmt.Fprintf(&bodyBuilder, " (forked from %s)", instances[0].ForkFromInstanceID)
	}
	bodyBuilder.WriteString("\n")

	if instances[0].Purpose != nil {
		if instances[0].Purpose.Title != nil && strings.TrimSpace(*instances[0].Purpose.Title) != "" {
			fmt.Fprintf(&bodyBuilder, "Role: %s\n", strings.TrimSpace(*instances[0].Purpose.Title))
		}
		if instances[0].Purpose.Summary != nil && strings.TrimSpace(*instances[0].Purpose.Summary) != "" {
			fmt.Fprintf(&bodyBuilder, "Scope: %s\n", strings.TrimSpace(*instances[0].Purpose.Summary))
		}
	}
	if explicitBrief != "" {
		fmt.Fprintf(&bodyBuilder, "Brief: %s\n", explicitBrief)
	} else if initialTask != nil && strings.TrimSpace(initialTask.Title) != "" {
		fmt.Fprintf(&bodyBuilder, "Initial Task: %s\n", strings.TrimSpace(initialTask.Title))
	}

	redactedBody := protocol.TruncateUTF8Head(service.redactor.Redact(strings.TrimSpace(bodyBuilder.String())), protocol.TextMaximumBytes)
	parsedMentions, mentionErr := protocol.ParseRoomMentions(redactedBody, nil)
	var mentionTokens []string
	if mentionErr == nil {
		mentionTokens = slices.Clone(parsedMentions.Tokens)
	}

	roomMessage := protocol.RoomMessage{
		Sequence:  nextSequence,
		ID:        protocol.NewRoomMessageIdentifier(),
		ThreadID:  thread.ID,
		CreatedAt: nowText,
		Author:    service.roomAuthorForCaller(state, caller),
		Kind:      "contract",
		Channel:   protocol.DefaultRoomChannel,
		Topic:     topic,
		Body:      redactedBody,
		Mentions:  mentionTokens,
	}

	head.HeadSequence = nextSequence
	head.UpdatedAt = nowText
	state.RoomHeads[thread.ID] = head
	state.RoomMessages[thread.ID] = append(state.RoomMessages[thread.ID], roomMessage)

	callerInstanceID := resolveCallerInstanceID(state, caller)
	if caller.Kind == "run" {
		if caller.TaskID != "" {
			if callerTask, taskExists := state.Tasks[caller.TaskID]; taskExists && callerTask.ThreadID == thread.ID {
				if callerTask.LastReadRoomSequence == nextSequence-1 {
					callerTask.LastReadRoomSequence = nextSequence
					callerTask.UpdatedAt = nowText
					state.Tasks[callerTask.ID] = callerTask
				}
			}
		}
		if callerInstanceID != "" {
			if callerInstance, instanceExists := state.Instances[callerInstanceID]; instanceExists && callerInstance.ThreadID == thread.ID {
				if nextSequence > callerInstance.LastPostRoomSequence {
					callerInstance.LastPostRoomSequence = nextSequence
				}
				if callerInstance.LastReadRoomSequence == nextSequence-1 {
					callerInstance.LastReadRoomSequence = nextSequence
				}
				callerInstance.UpdatedAt = nowText
				state.Instances[callerInstance.ID] = callerInstance
			}
		}
	} else if caller.ClientID != "" {
		if attachment, attached := state.ActiveAttachmentForThread(thread.ID); attached && attachment.ClientID == caller.ClientID {
			if attachment.LastReadRoomSequence == nextSequence-1 {
				attachment.LastReadRoomSequence = nextSequence
				attachment.LastHeartbeatAt = nowText
				attachment.UpdatedAt = nowText
				state.Attachments[attachment.ID] = attachment
			}
		}
	}

	mutation.EmitEvent(protocol.TaskEventEntry{
		ThreadID:   thread.ID,
		Type:       "room.message_posted",
		TaskID:     caller.TaskID,
		RunID:      caller.RunID,
		InstanceID: instances[0].ID,
		MessageID:  roomMessage.ID,
		Summary:    fmt.Sprintf("room contract #%d posted", nextSequence),
		Detail: map[string]any{
			"roomSequence": nextSequence,
			"kind":         roomMessage.Kind,
			"topic":        roomMessage.Topic,
		},
		OccurredAt: nowText,
	})

	return nextSequence
}
