package orchestrator

import (
	"fmt"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

const coordinatorWakeOriginKind = "coordinator-wake"

func (service *Service) bindThreadCoordinatorInState(
	state *store.State,
	mutation *store.Mutation,
	caller CallerIdentity,
	thread *protocol.Thread,
	instanceID string,
	nowText string,
) error {
	trimmed := strings.TrimSpace(instanceID)
	previousInstanceID := ""
	if thread.Orchestrator != nil && thread.Orchestrator.Kind == "instance" {
		previousInstanceID = thread.Orchestrator.InstanceID
	}

	if trimmed == "" {
		if previousInstanceID != "" {
			service.cancelQueuedCoordinatorWakeTasksInState(state, mutation, thread.ID, previousInstanceID, nowText)
			thread.Orchestrator = nil
		}
		return nil
	}

	if !protocol.IsIdentifier(trimmed) {
		return invalidRequest("coordinatorInstanceId is malformed")
	}
	instance, exists := state.Instances[trimmed]
	if !exists {
		return notFound(fmt.Sprintf("coordinator instance %s does not exist", trimmed))
	}
	if instance.ThreadID != thread.ID {
		return forbiddenError(fmt.Sprintf("coordinator instance %s does not belong to thread %s", trimmed, thread.ID))
	}
	if !instance.Delegation.CanDelegate {
		return forbiddenError(fmt.Sprintf("coordinator instance %s requires delegation.canDelegate=true", trimmed))
	}
	if protocol.IsTerminalInstanceStatus(instance.Status) || instance.Status == "draining" {
		return conflictError("instance_unavailable", fmt.Sprintf("coordinator instance %s is %s", trimmed, instance.Status))
	}
	if caller.Kind == "run" && caller.RunID != "" {
		if runRecord, runExists := state.Runs[caller.RunID]; runExists && runRecord.Depth >= protocol.MaximumDelegationDepth {
			return forbiddenError(fmt.Sprintf("maximum delegation depth (%d) exceeded", protocol.MaximumDelegationDepth))
		}
	}

	if previousInstanceID != "" && previousInstanceID != instance.ID {
		service.cancelQueuedCoordinatorWakeTasksInState(state, mutation, thread.ID, previousInstanceID, nowText)
	}
	if previousInstanceID == instance.ID && thread.Orchestrator != nil {
		return nil
	}

	stream := state.EventStreams[thread.ID]
	thread.Orchestrator = &protocol.ThreadOrchestrator{
		Kind:                     "instance",
		InstanceID:               instance.ID,
		LastHandledEventSequence: stream.Head,
	}
	return nil
}

func (service *Service) cleanupCoordinatorOnInstanceUnavailableInState(
	state *store.State,
	mutation *store.Mutation,
	instanceID string,
	nowText string,
) {
	if instanceID == "" {
		return
	}
	for threadID, thread := range state.Threads {
		if thread.Orchestrator == nil || thread.Orchestrator.Kind != "instance" || thread.Orchestrator.InstanceID != instanceID {
			continue
		}
		service.cancelQueuedCoordinatorWakeTasksInState(state, mutation, threadID, instanceID, nowText)
		thread.Orchestrator = nil
		thread.UpdatedAt = nowText
		state.Threads[threadID] = thread
	}
}

func (service *Service) cancelQueuedCoordinatorWakeTasksInState(
	state *store.State,
	mutation *store.Mutation,
	threadID string,
	instanceID string,
	nowText string,
) {
	for taskID, task := range state.Tasks {
		if task.ThreadID != threadID || task.Origin.Kind != coordinatorWakeOriginKind {
			continue
		}
		if task.PlacementInstanceID != instanceID || protocol.IsTerminalTaskStatus(task.Status) {
			continue
		}
		if task.Status == "ready" || task.Status == "pending" {
			service.cancelTaskInState(state, mutation, &task, nowText)
			state.Tasks[taskID] = task
		}
	}
}

func isCoordinatorOwnedTask(thread protocol.Thread, task protocol.Task) bool {
	if thread.Orchestrator == nil || thread.Orchestrator.Kind != "instance" || thread.Orchestrator.InstanceID == "" {
		return false
	}
	coordID := thread.Orchestrator.InstanceID
	if task.Origin.Kind == coordinatorWakeOriginKind {
		return true
	}
	if task.PlacementInstanceID == coordID {
		return true
	}
	if task.Assignment != nil && task.Assignment.InstanceID == coordID {
		return true
	}
	return false
}

func findCoordinatorActiveAndQueuedWakeTaskInState(
	state *store.State,
	threadID string,
	instanceID string,
) (runningTaskID string, queuedWakeTaskID string) {
	for _, taskID := range sortedTaskIDs(state, threadID) {
		task := state.Tasks[taskID]
		switch task.Status {
		case "running":
			if (task.Assignment != nil && task.Assignment.InstanceID == instanceID) || task.PlacementInstanceID == instanceID {
				runningTaskID = taskID
			}
		case "ready":
			if task.PlacementInstanceID == instanceID && task.Origin.Kind == coordinatorWakeOriginKind && task.AttemptCount == 0 {
				queuedWakeTaskID = taskID
			}
		}
	}
	return runningTaskID, queuedWakeTaskID
}

// scheduleCoordinatorWakeInState dispatches or coalesces an event-driven wake turn for a
// thread's bound autonomous coordinator instance. Returns true if a wake was created or coalesced.
func (service *Service) scheduleCoordinatorWakeInState(
	state *store.State,
	mutation *store.Mutation,
	threadID string,
	triggerReason string,
	triggerSummary string,
	originInstanceID string,
	eventSequence int64,
	nowText string,
) bool {
	thread, exists := state.Threads[threadID]
	if !exists || thread.Status != "open" || thread.Orchestrator == nil || thread.Orchestrator.Kind != "instance" {
		return false
	}
	coordInstanceID := thread.Orchestrator.InstanceID
	if coordInstanceID == "" {
		return false
	}
	// Self-event loop suppression: never wake the coordinator for its own actions.
	if originInstanceID != "" && originInstanceID == coordInstanceID {
		return false
	}

	instance, instanceExists := state.Instances[coordInstanceID]
	if !instanceExists || instance.ThreadID != threadID || !instance.Delegation.CanDelegate {
		service.cleanupCoordinatorOnInstanceUnavailableInState(state, mutation, coordInstanceID, nowText)
		return false
	}
	if protocol.IsTerminalInstanceStatus(instance.Status) || instance.Status == "draining" {
		service.cleanupCoordinatorOnInstanceUnavailableInState(state, mutation, coordInstanceID, nowText)
		return false
	}

	targetSequence := eventSequence
	if targetSequence <= 0 {
		targetSequence = state.EventStreams[threadID].Head + 1
	}

	orchestratorState := *thread.Orchestrator
	// Idempotency under event replay: ignore already-handled sequences.
	if targetSequence <= orchestratorState.LastHandledEventSequence {
		return false
	}
	// Deduplicate exact replay of an already-queued sequence.
	if eventSequence > 0 && targetSequence <= orchestratorState.PendingWakeEventSequence {
		return false
	}

	redactedSummary := protocol.TruncateUTF8Head(service.redactor.Redact(strings.TrimSpace(triggerSummary)), 400)
	if redactedSummary == "" {
		redactedSummary = triggerReason
	}
	triggerLine := fmt.Sprintf("- [%s] (event #%d) %s", triggerReason, targetSequence, redactedSummary)

	runningTaskID, queuedWakeTaskID := findCoordinatorActiveAndQueuedWakeTaskInState(state, threadID, coordInstanceID)

	// Case 1: Coalesce into an already-queued coordinator-wake task so parallel batch settlements
	// trigger only a single coordinator turn.
	if queuedWakeTaskID != "" {
		queuedTask := state.Tasks[queuedWakeTaskID]
		if !strings.Contains(queuedTask.Instructions, triggerLine) {
			queuedTask.Instructions = protocol.TruncateUTF8Head(
				queuedTask.Instructions+"\n"+triggerLine,
				protocol.PromptMaximumBytes,
			)
		}
		queuedTask.UpdatedAt = nowText
		state.Tasks[queuedWakeTaskID] = queuedTask

		if targetSequence > orchestratorState.PendingWakeEventSequence {
			orchestratorState.PendingWakeEventSequence = targetSequence
		}
		orchestratorState.PendingWakeTaskID = queuedWakeTaskID
		orchestratorState.LastWakeSummary = redactedSummary
		orchestratorState.LastWakeAt = nowText
		thread.Orchestrator = &orchestratorState
		state.Threads[threadID] = thread
		return true
	}

	// Case 2: Coordinator is currently busy running a turn. Record the pending event sequence
	// so releaseInstanceAfterRunInState dispatches one coalesced wake turn as soon as it finishes.
	if instance.Status == "busy" || runningTaskID != "" {
		if targetSequence > orchestratorState.PendingWakeEventSequence {
			orchestratorState.PendingWakeEventSequence = targetSequence
		}
		orchestratorState.LastWakeSummary = redactedSummary
		orchestratorState.LastWakeAt = nowText
		thread.Orchestrator = &orchestratorState
		state.Threads[threadID] = thread
		return true
	}

	// Case 3: Coordinator is ready, idle, or provisioning. Create a single wake task pinned to it.
	wakeTask := service.buildCoordinatorWakeTask(thread, instance, targetSequence, triggerLine, nowText)
	state.Tasks[wakeTask.ID] = wakeTask

	orchestratorState.PendingWakeEventSequence = targetSequence
	orchestratorState.PendingWakeTaskID = wakeTask.ID
	orchestratorState.LastWakeSummary = redactedSummary
	orchestratorState.LastWakeAt = nowText
	thread.Orchestrator = &orchestratorState
	state.Threads[threadID] = thread

	if mutation != nil {
		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
	}
	return true
}

func (service *Service) buildCoordinatorWakeTask(
	thread protocol.Thread,
	instance protocol.AgentInstance,
	targetSequence int64,
	triggerLine string,
	nowText string,
) protocol.Task {
	lastHandled := int64(0)
	if thread.Orchestrator != nil {
		lastHandled = thread.Orchestrator.LastHandledEventSequence
	}
	var promptBuilder strings.Builder
	fmt.Fprintf(&promptBuilder, "Autonomous coordinator wakeup for thread %q (%s) through event #%d (last handled #%d).\n\n",
		thread.Title,
		thread.ID,
		targetSequence,
		lastHandled,
	)
	promptBuilder.WriteString("Lifecycle triggers:\n")
	promptBuilder.WriteString(triggerLine)
	promptBuilder.WriteString("\n\nInspect `get_task_context`, `read_room`, or `get_thread_context` as needed, advance or unblock/reopen downstream work with `submit_tasks`/`unblock_task`/`reopen_task`, or mark the thread resolved with `update_thread`, then finalize this turn with `update_task`.")

	return protocol.Task{
		ID:       protocol.NewIdentifier("task"),
		ThreadID: thread.ID,
		Origin: protocol.TaskOrigin{
			Kind:         coordinatorWakeOriginKind,
			SubmissionID: fmt.Sprintf("coord-seq-%d", targetSequence),
			InstanceID:   instance.ID,
		},
		Title:               fmt.Sprintf("Coordinator wake (event #%d)", targetSequence),
		Instructions:        protocol.TruncateUTF8Head(promptBuilder.String(), protocol.PromptMaximumBytes),
		Status:              "ready",
		AttemptCount:        0,
		MaxAttempts:         1,
		Dependencies:        []protocol.TaskDependency{},
		Requirements:        instanceRequirementsToExecutionRequirements(instance.Requirements),
		PlacementInstanceID: instance.ID,
		PreferredInstanceID: instance.ID,
		CreatedAt:           nowText,
		UpdatedAt:           nowText,
	}
}

func (service *Service) markCoordinatorRunDispatchedInState(
	state *store.State,
	threadID string,
	instanceID string,
	dispatchedTaskID string,
	nowText string,
) {
	thread, exists := state.Threads[threadID]
	if !exists || thread.Orchestrator == nil || thread.Orchestrator.Kind != "instance" || thread.Orchestrator.InstanceID != instanceID {
		return
	}
	orchestratorState := *thread.Orchestrator
	streamHead := state.EventStreams[threadID].Head
	if streamHead > orchestratorState.LastHandledEventSequence {
		orchestratorState.LastHandledEventSequence = streamHead
	}
	if dispatchedTaskID != "" && orchestratorState.PendingWakeTaskID == dispatchedTaskID {
		if orchestratorState.PendingWakeEventSequence > orchestratorState.LastHandledEventSequence {
			orchestratorState.LastHandledEventSequence = orchestratorState.PendingWakeEventSequence
		}
		orchestratorState.PendingWakeTaskID = ""
	}
	thread.Orchestrator = &orchestratorState
	thread.UpdatedAt = nowText
	state.Threads[threadID] = thread
}

func (service *Service) enqueuePendingCoordinatorWakeInState(
	state *store.State,
	instanceID string,
	nowText string,
) bool {
	instance, exists := state.Instances[instanceID]
	if !exists || (instance.Status != "ready" && instance.Status != "idle") {
		return false
	}
	if !instance.Delegation.CanDelegate {
		return false
	}
	thread, threadExists := state.Threads[instance.ThreadID]
	if !threadExists || thread.Status != "open" || thread.Orchestrator == nil ||
		thread.Orchestrator.Kind != "instance" || thread.Orchestrator.InstanceID != instanceID {
		return false
	}
	if thread.Orchestrator.PendingWakeEventSequence <= thread.Orchestrator.LastHandledEventSequence {
		return false
	}
	if _, queuedWakeTaskID := findCoordinatorActiveAndQueuedWakeTaskInState(state, thread.ID, instanceID); queuedWakeTaskID != "" {
		return false
	}

	summary := thread.Orchestrator.LastWakeSummary
	if summary == "" {
		summary = "pending thread lifecycle transitions"
	}
	triggerLine := fmt.Sprintf("- [coalesced] (event #%d) %s", thread.Orchestrator.PendingWakeEventSequence, summary)
	wakeTask := service.buildCoordinatorWakeTask(thread, instance, thread.Orchestrator.PendingWakeEventSequence, triggerLine, nowText)
	state.Tasks[wakeTask.ID] = wakeTask

	orchestratorState := *thread.Orchestrator
	orchestratorState.PendingWakeTaskID = wakeTask.ID
	thread.Orchestrator = &orchestratorState
	state.Threads[thread.ID] = thread
	return true
}
