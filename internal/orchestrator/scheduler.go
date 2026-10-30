package orchestrator

import (
	"cmp"
	"context"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type matchedOffering struct {
	node          protocol.ComputeNode
	harnessID     string
	model         string
	transport     string
	workspaceRoot string
	workspacePath string
	preferenceHit int
	activeRuns    int
}

func (service *Service) TriggerSchedule() {
	select {
	case service.scheduleWakeup <- struct{}{}:
	default:
	}
}

func (service *Service) runSchedulerLoop(operationContext context.Context) {
	for {
		select {
		case <-operationContext.Done():
			return
		case <-service.scheduleWakeup:
			service.scheduleReadyTasks()
		}
	}
}

func (service *Service) scheduleReadyTasks() {
	type pendingProvision struct {
		nodeID     string
		instance   protocol.AgentInstance
		allocation protocol.InstanceAllocation
	}
	type pendingDispatch struct {
		nodeID  string
		message protocol.MasterControlMessage
	}

	var provisions []pendingProvision
	var dispatches []pendingDispatch
	var drains []struct {
		nodeID       string
		instanceID   string
		allocationID string
	}

	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		readyTasks := make([]protocol.Task, 0)
		for _, task := range state.Tasks {
			if task.Status == "ready" {
				readyTasks = append(readyTasks, task)
			}
		}
		if len(readyTasks) == 0 {
			return nil
		}

		slices.SortFunc(readyTasks, func(left, right protocol.Task) int {
			if compared := cmp.Compare(left.CreatedAt, right.CreatedAt); compared != 0 {
				return compared
			}
			return cmp.Compare(left.ID, right.ID)
		})

		nowText := service.nowUTC()

		for _, readyTask := range readyTasks {
			task := state.Tasks[readyTask.ID]
			if task.Status != "ready" {
				continue
			}

			effectiveReqs := mergeProjectProfileRequirements(state, task.Requirements)

			if resident, allocation, found := service.findReusableResident(state, task, effectiveReqs); found {
				dispatchMessage := service.assignTaskToResident(state, &task, &resident, &allocation, effectiveReqs, nowText)
				dispatches = append(dispatches, pendingDispatch{
					nodeID:  allocation.NodeID,
					message: dispatchMessage,
				})
				continue
			}

			if task.PlacementInstanceID != "" {
				pinnedInstance, exists := state.Instances[task.PlacementInstanceID]
				if !exists || protocol.IsTerminalInstanceStatus(pinnedInstance.Status) || pinnedInstance.Status == "draining" {
					task.Status = "failed"
					task.Summary = fmt.Sprintf("pinned instance %s is no longer available", task.PlacementInstanceID)
					task.UpdatedAt = nowText
					task.FinishedAt = nowText
					state.Tasks[task.ID] = task
					recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
					continue
				}
				task.PlacementDiagnostic = &protocol.PlacementDiagnostic{
					EvaluatedAt:     nowText,
					EligibleNodeIDs: []string{},
					Unsatisfied: []protocol.UnsatisfiedRequirement{{
						Kind:        "instance",
						Requirement: "placementInstanceId=" + pinnedInstance.ID,
						InstanceID:  pinnedInstance.ID,
						Detail:      fmt.Sprintf("pinned instance %s is currently %s", pinnedInstance.ID, pinnedInstance.Status),
					}},
				}
				state.Tasks[task.ID] = task
				continue
			}

			if service.hasProvisioningCompatibleInstance(state, task.ThreadID, effectiveReqs, task.PlacementOverride) {
				continue
			}

			selection, diagnostic := service.selectOfferingForRequirements(state, effectiveReqs, task.PlacementOverride, true)
			if selection == nil {
				if drainedNodeID, drainedInstID, drainedAllocID, didDrain := service.evictIdleAutoManagedInstance(state, effectiveReqs, task.PlacementOverride, nowText); didDrain {
					drains = append(drains, struct {
						nodeID       string
						instanceID   string
						allocationID string
					}{nodeID: drainedNodeID, instanceID: drainedInstID, allocationID: drainedAllocID})
				}
				task.PlacementDiagnostic = &diagnostic
				state.Tasks[task.ID] = task
				continue
			}

			timeoutSeconds := 300
			expiresAt := extendLeaseTimestamp(nowText, timeoutSeconds)
			instanceID := protocol.NewIdentifier("inst")
			allocationID := protocol.NewIdentifier("alloc")

			instance := protocol.AgentInstance{
				ID:       instanceID,
				ThreadID: task.ThreadID,
				Creator:  protocol.InstanceCreator{Kind: "operator", OperatorID: "scheduler"},
				Delegation: protocol.InstanceDelegationPolicy{
					CanDelegate: false,
				},
				Lease: protocol.InstanceLease{
					IdleTimeoutSeconds: timeoutSeconds,
					ExpiresAt:          expiresAt,
				},
				Status:      "provisioning",
				AutoManaged: true,
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
			delivery := store.InstanceDelivery{
				ID:           protocol.NewIdentifier("idlv"),
				NodeID:       selection.node.ID,
				InstanceID:   instanceID,
				AllocationID: allocationID,
				Kind:         "provision",
				CreatedAt:    nowText,
			}
			state.InstanceDeliveries[delivery.ID] = delivery

			provisions = append(provisions, pendingProvision{
				nodeID:     selection.node.ID,
				instance:   instance,
				allocation: allocation,
			})
		}

		if len(provisions) > 0 || len(dispatches) > 0 || len(drains) > 0 {
			mutation.OnCommit(func() {
				for _, drainItem := range drains {
					_ = service.sendToSlave(drainItem.nodeID, protocol.MasterControlMessage{
						Type:         "instance.release",
						InstanceID:   drainItem.instanceID,
						AllocationID: drainItem.allocationID,
						Mode:         "drain",
						At:           service.nowUTC(),
					})
				}
				for _, provisionItem := range provisions {
					instCopy := provisionItem.instance
					allocCopy := provisionItem.allocation
					_ = service.sendToSlave(provisionItem.nodeID, protocol.MasterControlMessage{
						Type:       "instance.provision",
						Instance:   &instCopy,
						Allocation: &allocCopy,
						At:         service.nowUTC(),
					})
				}
				for _, dispatchItem := range dispatches {
					_ = service.sendToSlave(dispatchItem.nodeID, dispatchItem.message)
				}
			})
		}
		return nil
	})
}

func (service *Service) findReusableResident(
	state *store.State,
	task protocol.Task,
	requirements protocol.ExecutionRequirements,
) (protocol.AgentInstance, protocol.InstanceAllocation, bool) {
	type candidate struct {
		instance      protocol.AgentInstance
		allocation    protocol.InstanceAllocation
		hasBinding    bool
		preferenceHit int
		activeRuns    int
	}

	var candidates []candidate

	for _, instance := range state.Instances {
		if instance.ThreadID != task.ThreadID {
			continue
		}
		if instance.Status != "ready" && instance.Status != "idle" {
			continue
		}
		if task.PlacementInstanceID != "" && instance.ID != task.PlacementInstanceID {
			continue
		}
		allocation, hasAlloc := state.ActiveAllocationForInstance(instance.ID)
		if !hasAlloc || allocation.Status != "active" {
			continue
		}
		if task.PlacementOverride != nil && task.PlacementOverride.NodeID != "" && allocation.NodeID != task.PlacementOverride.NodeID {
			continue
		}

		node, nodeExists := state.Nodes[allocation.NodeID]
		if !nodeExists || !service.isNodeLiveAndSynced(node.ID) {
			continue
		}
		activeRuns := countActiveRunsOnNode(state, node.ID)
		if activeRuns >= node.Concurrency {
			continue
		}

		if len(requirements.HarnessIDs) > 0 && !slices.Contains(requirements.HarnessIDs, allocation.HarnessID) {
			continue
		}
		if len(requirements.Models) > 0 && !slices.Contains(requirements.Models, allocation.Model) {
			continue
		}
		if len(requirements.Transports) > 0 && !slices.Contains(requirements.Transports, allocation.Transport) {
			continue
		}
		if requirements.Workspace != nil && requirements.Workspace.Path != "" && allocation.Workspace != requirements.Workspace.Path {
			continue
		}

		report := state.CapabilityReports[node.ID]
		if reason := checkNodeCapabilities(node, report, requirements); reason != "" {
			continue
		}
		if _, _, ok := resolveWorkspaceOnNode(node, requirements); !ok {
			continue
		}

		hasBinding := false
		for _, binding := range state.SessionBindings {
			if binding.InstanceID == instance.ID && binding.AllocationID == allocation.ID && binding.Status == "idle" {
				hasBinding = true
				break
			}
		}

		candidates = append(candidates, candidate{
			instance:      instance,
			allocation:    allocation,
			hasBinding:    hasBinding,
			preferenceHit: scorePreferences(node, report, allocation.HarnessID, allocation.Model, requirements.Preferences),
			activeRuns:    activeRuns,
		})
	}

	if len(candidates) == 0 {
		return protocol.AgentInstance{}, protocol.InstanceAllocation{}, false
	}

	slices.SortFunc(candidates, func(left, right candidate) int {
		if left.instance.AutoManaged != right.instance.AutoManaged {
			if !left.instance.AutoManaged {
				return -1
			}
			return 1
		}
		if left.hasBinding != right.hasBinding {
			if left.hasBinding {
				return -1
			}
			return 1
		}
		if compared := cmp.Compare(right.preferenceHit, left.preferenceHit); compared != 0 {
			return compared
		}
		if compared := cmp.Compare(left.activeRuns, right.activeRuns); compared != 0 {
			return compared
		}
		return cmp.Compare(left.instance.ID, right.instance.ID)
	})

	winner := candidates[0]
	return winner.instance, winner.allocation, true
}

func (service *Service) assignTaskToResident(
	state *store.State,
	task *protocol.Task,
	instance *protocol.AgentInstance,
	allocation *protocol.InstanceAllocation,
	requirements protocol.ExecutionRequirements,
	nowText string,
) protocol.MasterControlMessage {
	runID := protocol.NewIdentifier("run")
	attempt := task.AttemptCount + 1
	taskID := task.ID

	depth := 0
	var parentRunID *string
	if task.CreatedByRunID != "" {
		if parentRun, exists := state.Runs[task.CreatedByRunID]; exists {
			depth = parentRun.Depth + 1
			parentIDCopy := parentRun.ID
			parentRunID = &parentIDCopy
		}
	}

	workspacePath := allocation.Workspace
	node := state.Nodes[allocation.NodeID]
	workspaceRoot, resolvedSourcePath, _ := resolveWorkspaceOnNode(node, requirements)
	if resolvedSourcePath != "" {
		workspacePath = resolvedSourcePath
	}

	var leaseGrantPtr *protocol.WorkspaceLeaseGrant
	var leaseIDPtr *string
	if requirements.Workspace != nil && requirements.Workspace.Isolation != "" {
		lease := buildWorkspaceLease(
			state,
			*task,
			runID,
			node.ID,
			workspaceRoot,
			workspacePath,
			requirements,
			nowText,
		)
		state.WorkspaceLeases[lease.ID] = lease
		leaseIDCopy := lease.ID
		leaseIDPtr = &leaseIDCopy
		grant := lease.Grant()
		leaseGrantPtr = &grant
		workspacePath = lease.WorktreePath
	}

	prompt := composeTaskPrompt(state, *task)

	var dispatchBindingPtr *protocol.DispatchSessionBinding
	var bindingIDPtr *string
	if allocation.Transport == protocol.TransportACP {
		for _, binding := range state.SessionBindings {
			if binding.InstanceID == instance.ID &&
				binding.AllocationID == allocation.ID &&
				binding.HarnessID == allocation.HarnessID &&
				binding.Transport == protocol.TransportACP &&
				binding.Workspace == workspacePath &&
				binding.Status == "idle" {
				bindingIDCopy := binding.ID
				bindingIDPtr = &bindingIDCopy
				dispatchBindingPtr = &protocol.DispatchSessionBinding{
					ID:                binding.ID,
					ProviderSessionID: binding.ProviderSessionID,
					ResumePrompt:      protocol.TruncateUTF8Head(prompt, protocol.SessionResumePromptMaximumBytes),
				}
				break
			}
		}
	}

	runRecord := protocol.Run{
		ID:               runID,
		ThreadID:         task.ThreadID,
		InstanceID:       instance.ID,
		AllocationID:     allocation.ID,
		NodeID:           allocation.NodeID,
		HarnessID:        allocation.HarnessID,
		Model:            allocation.Model,
		Workspace:        workspacePath,
		Prompt:           prompt,
		Status:           "queued",
		Output:           "",
		Depth:            depth,
		ParentRunID:      parentRunID,
		DispatchedAt:     &nowText,
		CreatedAt:        nowText,
		TaskID:           &taskID,
		Attempt:          &attempt,
		Transport:        allocation.Transport,
		SessionBindingID: bindingIDPtr,
		WorkspaceLeaseID: leaseIDPtr,
	}
	state.Runs[runID] = runRecord

	instance.Status = "busy"
	instance.UpdatedAt = nowText
	state.Instances[instance.ID] = *instance

	task.Status = "running"
	task.AttemptCount = attempt
	task.PlacementDiagnostic = nil
	task.Assignment = &protocol.TaskAssignment{
		RunID:        runID,
		InstanceID:   instance.ID,
		AllocationID: allocation.ID,
		NodeID:       allocation.NodeID,
		Attempt:      attempt,
		AssignedAt:   nowText,
	}
	task.UpdatedAt = nowText
	state.Tasks[task.ID] = *task

	instanceSnapshot := *instance
	allocationSnapshot := *allocation
	return protocol.MasterControlMessage{
		Type:           "dispatch",
		At:             nowText,
		RunID:          runID,
		InstanceID:     instance.ID,
		AllocationID:   allocation.ID,
		Instance:       &instanceSnapshot,
		Allocation:     &allocationSnapshot,
		Run:            &runRecord,
		SessionBinding: dispatchBindingPtr,
		WorkspaceLease: leaseGrantPtr,
	}
}

func (service *Service) selectOfferingForRequirements(
	state *store.State,
	requirements protocol.ExecutionRequirements,
	override *protocol.PlacementOverride,
	requireInstanceSlot bool,
) (*matchedOffering, protocol.PlacementDiagnostic) {
	effectiveReqs := mergeProjectProfileRequirements(state, requirements)
	nowText := service.nowUTC()
	diagnostic := protocol.PlacementDiagnostic{
		EvaluatedAt:     nowText,
		EligibleNodeIDs: []string{},
		Unsatisfied:     []protocol.UnsatisfiedRequirement{},
	}

	var offerings []matchedOffering

	for _, nodeID := range sortedNodeIDs(state) {
		node := state.Nodes[nodeID]
		if override != nil && override.NodeID != "" && nodeID != override.NodeID {
			continue
		}
		if !service.isNodeLiveAndSynced(nodeID) {
			diagnostic.Unsatisfied = append(diagnostic.Unsatisfied, protocol.UnsatisfiedRequirement{
				Kind:        "node-liveness",
				Requirement: "online-and-synced",
				NodeID:      nodeID,
				Detail:      fmt.Sprintf("node %s is offline or still synchronizing", nodeID),
			})
			continue
		}

		activeRuns := countActiveRunsOnNode(state, nodeID)
		if !requireInstanceSlot && activeRuns >= node.Concurrency {
			diagnostic.Unsatisfied = append(diagnostic.Unsatisfied, protocol.UnsatisfiedRequirement{
				Kind:        "concurrency",
				Requirement: fmt.Sprintf("activeRuns<%d", node.Concurrency),
				NodeID:      nodeID,
				Detail:      fmt.Sprintf("node %s is at run concurrency (%d/%d)", nodeID, activeRuns, node.Concurrency),
			})
			continue
		}

		if requireInstanceSlot && node.InstanceCapacity != nil {
			occupying := countOccupyingAllocationsOnNode(state, nodeID)
			if occupying >= *node.InstanceCapacity {
				diagnostic.Unsatisfied = append(diagnostic.Unsatisfied, protocol.UnsatisfiedRequirement{
					Kind:        "instance-capacity",
					Requirement: fmt.Sprintf("activeInstances<%d", *node.InstanceCapacity),
					NodeID:      nodeID,
					Detail:      fmt.Sprintf("node %s is at resident instance capacity (%d/%d)", nodeID, occupying, *node.InstanceCapacity),
				})
				continue
			}
		}

		report := state.CapabilityReports[nodeID]
		if capFailure := checkNodeCapabilities(node, report, effectiveReqs); capFailure != "" {
			diagnostic.Unsatisfied = append(diagnostic.Unsatisfied, protocol.UnsatisfiedRequirement{
				Kind:        "capability",
				Requirement: capFailure,
				NodeID:      nodeID,
				Detail:      fmt.Sprintf("node %s does not satisfy %s", nodeID, capFailure),
			})
			continue
		}

		workspaceRoot, workspacePath, workspaceOK := resolveWorkspaceOnNode(node, effectiveReqs)
		if !workspaceOK {
			diagnostic.Unsatisfied = append(diagnostic.Unsatisfied, protocol.UnsatisfiedRequirement{
				Kind:        "workspace",
				Requirement: "workspace-root",
				NodeID:      nodeID,
				Detail:      fmt.Sprintf("node %s has no workspace root matching requested path", nodeID),
			})
			continue
		}

		nodeMatchedOffering := false
		for _, harnessProfile := range node.Harnesses {
			if !harnessProfile.Available {
				continue
			}
			if len(effectiveReqs.HarnessIDs) > 0 && !slices.Contains(effectiveReqs.HarnessIDs, harnessProfile.ID) {
				continue
			}

			selectedModel := chooseModel(harnessProfile, effectiveReqs)
			if selectedModel == "" && len(effectiveReqs.Models) > 0 {
				continue
			}

			selectedTransport := chooseTransport(harnessProfile, effectiveReqs)
			if selectedTransport == "" {
				continue
			}

			nodeMatchedOffering = true
			offerings = append(offerings, matchedOffering{
				node:          node,
				harnessID:     harnessProfile.ID,
				model:         selectedModel,
				transport:     selectedTransport,
				workspaceRoot: workspaceRoot,
				workspacePath: workspacePath,
				preferenceHit: scorePreferences(node, report, harnessProfile.ID, selectedModel, effectiveReqs.Preferences),
				activeRuns:    activeRuns,
			})
		}

		if nodeMatchedOffering {
			diagnostic.EligibleNodeIDs = append(diagnostic.EligibleNodeIDs, nodeID)
		} else {
			diagnostic.Unsatisfied = append(diagnostic.Unsatisfied, protocol.UnsatisfiedRequirement{
				Kind:        "harness",
				Requirement: "harness/model/transport",
				NodeID:      nodeID,
				Detail:      fmt.Sprintf("node %s has no available harness matching requested harness/model/transport", nodeID),
			})
		}
	}

	if len(offerings) == 0 {
		return nil, diagnostic
	}

	slices.SortFunc(offerings, func(left, right matchedOffering) int {
		if compared := cmp.Compare(right.preferenceHit, left.preferenceHit); compared != 0 {
			return compared
		}
		if compared := cmp.Compare(left.activeRuns, right.activeRuns); compared != 0 {
			return compared
		}
		if left.transport != right.transport {
			if left.transport == protocol.TransportACP {
				return -1
			}
			return 1
		}
		if compared := cmp.Compare(left.node.ID, right.node.ID); compared != 0 {
			return compared
		}
		return cmp.Compare(left.harnessID, right.harnessID)
	})

	winner := offerings[0]
	return &winner, diagnostic
}

func (service *Service) hasProvisioningCompatibleInstance(
	state *store.State,
	threadID string,
	requirements protocol.ExecutionRequirements,
	override *protocol.PlacementOverride,
) bool {
	for _, instance := range state.Instances {
		if instance.ThreadID != threadID || (instance.Status != "requested" && instance.Status != "provisioning") {
			continue
		}
		allocation, found := state.ActiveAllocationForInstance(instance.ID)
		if !found {
			continue
		}
		if override != nil && override.NodeID != "" && allocation.NodeID != override.NodeID {
			continue
		}
		if len(requirements.HarnessIDs) > 0 && !slices.Contains(requirements.HarnessIDs, allocation.HarnessID) {
			continue
		}
		if len(requirements.Transports) > 0 && !slices.Contains(requirements.Transports, allocation.Transport) {
			continue
		}
		return true
	}
	return false
}

func (service *Service) evictIdleAutoManagedInstance(
	state *store.State,
	requirements protocol.ExecutionRequirements,
	override *protocol.PlacementOverride,
	nowText string,
) (string, string, string, bool) {
	for _, instance := range state.Instances {
		if !instance.AutoManaged || (instance.Status != "ready" && instance.Status != "idle") {
			continue
		}
		allocation, found := state.ActiveAllocationForInstance(instance.ID)
		if !found {
			continue
		}
		if override != nil && override.NodeID != "" && allocation.NodeID != override.NodeID {
			continue
		}
		node, exists := state.Nodes[allocation.NodeID]
		if !exists || node.InstanceCapacity == nil {
			continue
		}
		if countOccupyingAllocationsOnNode(state, node.ID) < *node.InstanceCapacity {
			continue
		}
		_ = requirements
		instanceCopy := instance
		instanceCopy.Status = "draining"
		instanceCopy.UpdatedAt = nowText
		state.Instances[instance.ID] = instanceCopy
		state.ReleaseIntents[instance.ID] = protocol.InstanceReleaseIntent{
			InstanceID:  instance.ID,
			ThreadID:    instance.ThreadID,
			Mode:        "drain",
			RequestedAt: nowText,
			UpdatedAt:   nowText,
		}
		return node.ID, instance.ID, allocation.ID, true
	}
	return "", "", "", false
}

func mergeProjectProfileRequirements(
	state *store.State,
	requirements protocol.ExecutionRequirements,
) protocol.ExecutionRequirements {
	if requirements.ProjectProfileID == "" {
		return requirements
	}
	profile, exists := state.ProjectProfiles[requirements.ProjectProfileID]
	if !exists {
		return requirements
	}
	merged := requirements
	if len(merged.HarnessIDs) == 0 {
		merged.HarnessIDs = slices.Clone(profile.Requirements.HarnessIDs)
	}
	if len(merged.Models) == 0 {
		merged.Models = slices.Clone(profile.Requirements.Models)
	}
	if len(merged.Transports) == 0 {
		merged.Transports = slices.Clone(profile.Requirements.Transports)
	}
	if len(merged.OperatingSystems) == 0 {
		merged.OperatingSystems = slices.Clone(profile.Requirements.OperatingSystems)
	}
	if len(merged.Architectures) == 0 {
		merged.Architectures = slices.Clone(profile.Requirements.Architectures)
	}
	if len(merged.Labels) == 0 {
		merged.Labels = slices.Clone(profile.Requirements.Labels)
	}
	if len(merged.Accelerators) == 0 {
		merged.Accelerators = slices.Clone(profile.Requirements.Accelerators)
	}
	if merged.Toolchains == nil && len(profile.Requirements.Toolchains) > 0 {
		merged.Toolchains = map[string]string{}
		for key, value := range profile.Requirements.Toolchains {
			merged.Toolchains[key] = value
		}
	}
	if merged.Workspace == nil && (profile.Repository != nil || profile.WorkspacePolicy.Isolation != "") {
		merged.Workspace = &protocol.WorkspaceRequirements{}
	}
	if merged.Workspace != nil {
		if merged.Workspace.Repository == "" && profile.Repository != nil {
			merged.Workspace.Repository = profile.Repository.URL
		}
		if merged.Workspace.BaseBranch == "" && profile.Repository != nil {
			merged.Workspace.BaseBranch = profile.Repository.DefaultBranch
		}
		if merged.Workspace.Isolation == "" {
			merged.Workspace.Isolation = profile.WorkspacePolicy.Isolation
		}
		if merged.Workspace.Cleanup == "" {
			merged.Workspace.Cleanup = profile.WorkspacePolicy.Cleanup
		}
	}
	return merged
}

func checkNodeCapabilities(
	node protocol.ComputeNode,
	report protocol.NodeCapabilityReport,
	requirements protocol.ExecutionRequirements,
) string {
	if requirements.MinimumConcurrency != nil && node.Concurrency < *requirements.MinimumConcurrency {
		return fmt.Sprintf("minimumConcurrency>=%d", *requirements.MinimumConcurrency)
	}
	if requirements.ProjectProfileID != "" && len(report.ProjectAllowlist) > 0 && !slices.Contains(report.ProjectAllowlist, requirements.ProjectProfileID) {
		return "projectAllowlist:" + requirements.ProjectProfileID
	}

	evidenceMap := map[string]protocol.NodeCapabilityEvidence{}
	for _, entry := range report.Evidence {
		if entry.Success {
			evidenceMap[entry.CapabilityID] = entry
		}
	}

	operatingSystem, _, _ := strings.Cut(node.Platform, "-")
	_, architecture, _ := strings.Cut(node.Platform, "-")
	if osEvidence, ok := evidenceMap["os"]; ok && osEvidence.NormalizedValue != "" {
		operatingSystem = osEvidence.NormalizedValue
	}
	if archEvidence, ok := evidenceMap["architecture"]; ok && archEvidence.NormalizedValue != "" {
		architecture = archEvidence.NormalizedValue
	}

	if len(requirements.OperatingSystems) > 0 && !slices.Contains(requirements.OperatingSystems, operatingSystem) {
		return "operatingSystems"
	}
	if len(requirements.Architectures) > 0 && !slices.Contains(requirements.Architectures, architecture) {
		return "architectures"
	}
	for _, label := range requirements.Labels {
		if _, found := evidenceMap["label:"+label]; !found {
			return "label:" + label
		}
	}
	for _, accelerator := range requirements.Accelerators {
		if _, found := evidenceMap["accelerator:"+accelerator]; !found {
			return "accelerator:" + accelerator
		}
	}
	for toolID, constraintRaw := range requirements.Toolchains {
		constraint, err := protocol.ParseVersionConstraint(constraintRaw)
		if err != nil {
			return "toolchain:" + toolID
		}
		evidence, found := evidenceMap["toolchain:"+toolID]
		if !found || !protocol.SatisfiesVersionConstraint(evidence.NormalizedValue, constraint) {
			return fmt.Sprintf("toolchain:%s%s%s", toolID, constraint.Comparator, constraint.Version)
		}
	}
	if requirements.MinimumMemoryMegabytes != nil {
		memEvidence, found := evidenceMap["configured-memory-megabytes"]
		if !found {
			return fmt.Sprintf("minimumMemoryMegabytes>=%d", *requirements.MinimumMemoryMegabytes)
		}
		parsedMem, parseErr := strconv.ParseInt(memEvidence.NormalizedValue, 10, 64)
		if parseErr != nil || parsedMem < *requirements.MinimumMemoryMegabytes {
			return fmt.Sprintf("minimumMemoryMegabytes>=%d", *requirements.MinimumMemoryMegabytes)
		}
	}
	if requirements.Workspace != nil {
		if requirements.Workspace.Writable {
			if _, ok := evidenceMap["workspace-writable"]; !ok && len(report.Evidence) > 0 {
				return "workspace-writable"
			}
		}
		if requirements.Workspace.Isolation != "" {
			if _, ok := evidenceMap["workspace-lease:"+requirements.Workspace.Isolation]; !ok && len(report.Evidence) > 0 {
				return "workspace-lease:" + requirements.Workspace.Isolation
			}
		}
	}
	return ""
}

func resolveWorkspaceOnNode(
	node protocol.ComputeNode,
	requirements protocol.ExecutionRequirements,
) (string, string, bool) {
	if len(node.WorkspaceRoots) == 0 {
		return "", "", false
	}
	if requirements.Workspace == nil || requirements.Workspace.Path == "" {
		return node.WorkspaceRoots[0], node.WorkspaceRoots[0], true
	}
	requestedPath := path.Clean(requirements.Workspace.Path)
	for _, root := range node.WorkspaceRoots {
		cleanRoot := path.Clean(root)
		if requestedPath == cleanRoot || strings.HasPrefix(requestedPath, cleanRoot+"/") {
			return cleanRoot, requestedPath, true
		}
	}
	return "", "", false
}

func chooseModel(profile protocol.HarnessProfile, requirements protocol.ExecutionRequirements) string {
	if len(requirements.Models) > 0 {
		for _, requestedModel := range requirements.Models {
			if len(profile.Models) == 0 || slices.Contains(profile.Models, requestedModel) || requestedModel == "default" {
				return requestedModel
			}
		}
		return ""
	}
	if requirements.Preferences != nil {
		for _, preferred := range requirements.Preferences.Models {
			if slices.Contains(profile.Models, preferred) {
				return preferred
			}
		}
	}
	return "default"
}

func chooseTransport(profile protocol.HarnessProfile, requirements protocol.ExecutionRequirements) string {
	offered := profile.Transports
	if len(offered) == 0 {
		offered = []string{protocol.TransportNativeCLI}
	}
	if len(requirements.Transports) > 0 {
		for _, requested := range requirements.Transports {
			if slices.Contains(offered, requested) {
				return requested
			}
		}
		return ""
	}
	if slices.Contains(offered, protocol.TransportACP) {
		return protocol.TransportACP
	}
	return offered[0]
}

func scorePreferences(
	node protocol.ComputeNode,
	report protocol.NodeCapabilityReport,
	harnessID string,
	model string,
	preferences *protocol.ExecutionPreferences,
) int {
	if preferences == nil {
		return 0
	}
	score := 0
	if slices.Contains(preferences.NodeIDs, node.ID) {
		score += 8
	}
	if slices.Contains(preferences.HarnessIDs, harnessID) {
		score += 4
	}
	if slices.Contains(preferences.Models, model) {
		score += 2
	}
	for _, preferredLabel := range preferences.Labels {
		for _, entry := range report.Evidence {
			if entry.CapabilityID == "label:"+preferredLabel && entry.Success {
				score++
			}
		}
	}
	return score
}

func buildWorkspaceLease(
	state *store.State,
	task protocol.Task,
	runID string,
	nodeID string,
	workspaceRoot string,
	sourcePath string,
	requirements protocol.ExecutionRequirements,
	nowText string,
) protocol.WorkspaceLease {
	_ = state
	leaseID := protocol.NewIdentifier("lease")
	policy := requirements.Workspace.Isolation
	cleanup := requirements.Workspace.Cleanup
	if cleanup == "" {
		cleanup = protocol.WorkspaceCleanupWhenUnchanged
	}

	lease := protocol.WorkspaceLease{
		ID:               leaseID,
		ThreadID:         task.ThreadID,
		TaskID:           task.ID,
		RunID:            runID,
		NodeID:           nodeID,
		ProjectProfileID: requirements.ProjectProfileID,
		Policy:           policy,
		Cleanup:          cleanup,
		Root:             workspaceRoot,
		SourcePath:       sourcePath,
		WorktreePath:     sourcePath,
		Status:           "requested",
		CreatedAt:        nowText,
		UpdatedAt:        nowText,
	}

	if policy == protocol.WorkspaceIsolationGitWorktree {
		if requirements.Workspace.Repository != "" {
			if normalizedRepo, ok := protocol.NormalizeRepositoryIdentity(requirements.Workspace.Repository); ok {
				lease.Repository = normalizedRepo
			}
		}
		baseRevision := "HEAD"
		if requirements.Workspace.BaseBranch != "" && protocol.IsWorkspaceLeaseBaseBranch(requirements.Workspace.BaseBranch) {
			baseRevision = protocol.WorkspaceLeaseBaseRef(requirements.Workspace.BaseBranch)
		}
		lease.BaseRevision = baseRevision
		lease.Branch = protocol.WorkspaceLeaseBranch(task.ID, runID)
		lease.WorktreePath = protocol.WorkspaceLeaseWorktreePath(workspaceRoot, leaseID)
	}
	return lease
}

func composeTaskPrompt(state *store.State, task protocol.Task) string {
	var builder strings.Builder
	if thread, exists := state.Threads[task.ThreadID]; exists {
		builder.WriteString(fmt.Sprintf("Thread: %s (%s)\n", thread.Title, thread.ID))
		if thread.Summary != "" {
			builder.WriteString("Thread Summary: " + thread.Summary + "\n")
		}
	}
	builder.WriteString(fmt.Sprintf("Task: %s (%s)\n", task.Title, task.ID))
	if len(task.Dependencies) > 0 {
		builder.WriteString("\nCompleted Dependencies:\n")
		for _, dependency := range task.Dependencies {
			if depTask, exists := state.Tasks[dependency.TaskID]; exists {
				builder.WriteString(fmt.Sprintf("- %s [%s]: %s\n", depTask.Title, depTask.Status, depTask.Summary))
			}
		}
	}
	builder.WriteString("\nInstructions:\n")
	builder.WriteString(task.Instructions)
	return protocol.TruncateUTF8Head(builder.String(), protocol.PromptMaximumBytes)
}

func countActiveRunsOnNode(state *store.State, nodeID string) int {
	count := 0
	for _, runRecord := range state.Runs {
		if runRecord.NodeID == nodeID && protocol.IsActiveRunStatus(runRecord.Status) {
			count++
		}
	}
	return count
}

func countOccupyingAllocationsOnNode(state *store.State, nodeID string) int {
	count := 0
	for _, allocation := range state.Allocations {
		if allocation.NodeID == nodeID && protocol.IsOccupyingAllocationStatus(allocation.Status) {
			count++
		}
	}
	return count
}

func sortedNodeIDs(state *store.State) []string {
	ids := make([]string, 0, len(state.Nodes))
	for nodeID := range state.Nodes {
		ids = append(ids, nodeID)
	}
	slices.Sort(ids)
	return ids
}
