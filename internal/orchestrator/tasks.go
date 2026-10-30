package orchestrator

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type SubmittedDependency struct {
	TaskID       string `json:"taskId,omitempty"`
	ClientTaskID string `json:"clientTaskId,omitempty"`
	Policy       string `json:"policy,omitempty"`
}

type SubmittedTaskSpec struct {
	ClientTaskID      string                         `json:"clientTaskId,omitempty"`
	Title             string                         `json:"title"`
	Instructions      string                         `json:"instructions"`
	MaxAttempts       int                            `json:"maxAttempts,omitempty"`
	Dependencies      []SubmittedDependency          `json:"dependencies,omitempty"`
	Requirements      protocol.ExecutionRequirements `json:"requirements,omitempty"`
	PlacementOverride *protocol.PlacementOverride    `json:"placementOverride,omitempty"`
	InstanceID        string                         `json:"instanceId,omitempty"`
}

type SubmitTasksInput struct {
	ThreadID       string              `json:"threadId,omitempty"`
	IdempotencyKey string              `json:"idempotencyKey"`
	Tasks          []SubmittedTaskSpec `json:"tasks"`
}

type SubmitTasksResult struct {
	ThreadID      string            `json:"threadId"`
	Tasks         []protocol.Task   `json:"tasks"`
	ClientTaskMap map[string]string `json:"clientTaskMap,omitempty"`
	Replayed      bool              `json:"replayed"`
}

type DelegateTaskInput struct {
	IdempotencyKey    string                         `json:"idempotencyKey"`
	ClientTaskID      string                         `json:"clientTaskId,omitempty"`
	Title             string                         `json:"title"`
	Instructions      string                         `json:"instructions"`
	MaxAttempts       int                            `json:"maxAttempts,omitempty"`
	Dependencies      []SubmittedDependency          `json:"dependencies,omitempty"`
	Requirements      protocol.ExecutionRequirements `json:"requirements,omitempty"`
	PlacementOverride *protocol.PlacementOverride    `json:"placementOverride,omitempty"`
	InstanceID        string                         `json:"instanceId,omitempty"`
}

type UpdateTaskInput struct {
	TaskID         string   `json:"taskId,omitempty"`
	IdempotencyKey string   `json:"idempotencyKey,omitempty"`
	Status         string   `json:"status,omitempty"`
	Summary        *string  `json:"summary,omitempty"`
	BlockerReason  *string  `json:"blockerReason,omitempty"`
	ArtifactIDs    []string `json:"artifactIds,omitempty"`
}

type DependencyTaskSummary struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Status      string   `json:"status"`
	Policy      string   `json:"policy"`
	Summary     string   `json:"summary,omitempty"`
	ArtifactIDs []string `json:"artifactIds,omitempty"`
}

type TaskContextResponse struct {
	Thread       protocol.Thread         `json:"thread"`
	Task         protocol.Task           `json:"task"`
	Run          *protocol.Run           `json:"run,omitempty"`
	Dependencies []DependencyTaskSummary `json:"dependencies"`
	Children     []protocol.Task         `json:"children"`
	Messages     []protocol.TaskMessage  `json:"messages"`
	Artifacts    []protocol.Artifact     `json:"artifacts"`
}

func (service *Service) SubmitTasks(caller CallerIdentity, input SubmitTasksInput) (SubmitTasksResult, error) {
	threadID := strings.TrimSpace(input.ThreadID)
	if threadID == "" && caller.Kind == "run" {
		threadID = caller.ThreadID
	}
	if !protocol.IsIdentifier(threadID) {
		return SubmitTasksResult{}, invalidRequest("threadId is required")
	}
	if caller.Kind == "run" {
		if !caller.CanDelegate {
			return SubmitTasksResult{}, forbiddenError("this worker instance is not authorized to delegate tasks")
		}
		if caller.ThreadID != "" && caller.ThreadID != threadID {
			return SubmitTasksResult{}, forbiddenError("worker run cannot submit tasks into another thread")
		}
	}

	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey == "" || !protocol.IsBoundedString(idempotencyKey, 128) {
		return SubmitTasksResult{}, invalidRequest("idempotencyKey is required and must be at most 128 bytes")
	}
	if len(input.Tasks) == 0 || len(input.Tasks) > 64 {
		return SubmitTasksResult{}, invalidRequest("tasks must contain between 1 and 64 task specifications")
	}

	sourceKey := caller.SourceKey()
	receiptKey := sourceKey + "\x00" + threadID + "\x00" + idempotencyKey
	digest := canonicalDigest(struct {
		ThreadID string              `json:"threadId"`
		Tasks    []SubmittedTaskSpec `json:"tasks"`
	}{
		ThreadID: threadID,
		Tasks:    input.Tasks,
	})

	var result SubmitTasksResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		thread, exists := state.Threads[threadID]
		if !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}
		if existingReceipt, found := state.TaskSubmissions[receiptKey]; found {
			if existingReceipt.Digest != digest {
				return conflictError("idempotency_conflict", "idempotencyKey was already used with a different task batch")
			}
			tasks := make([]protocol.Task, 0, len(existingReceipt.TaskIDs))
			for _, taskID := range existingReceipt.TaskIDs {
				if task, taskExists := state.Tasks[taskID]; taskExists {
					tasks = append(tasks, task)
				}
			}
			result = SubmitTasksResult{
				ThreadID:      threadID,
				Tasks:         tasks,
				ClientTaskMap: existingReceipt.ClientTaskMap,
				Replayed:      true,
			}
			return nil
		}

		parentTaskID := ""
		delegationDepth := 0
		if caller.Kind == "run" {
			runRecord, runExists := state.Runs[caller.RunID]
			if !runExists {
				return forbiddenError("calling worker run does not exist")
			}
			delegationDepth = runRecord.Depth + 1
			if delegationDepth > protocol.MaximumDelegationDepth {
				return forbiddenError(fmt.Sprintf("maximum delegation depth (%d) exceeded", protocol.MaximumDelegationDepth))
			}
			if runRecord.TaskID != nil {
				parentTaskID = *runRecord.TaskID
			}
		}

		nowText := service.nowUTC()
		submissionID := protocol.NewIdentifier("sub")
		clientTaskMap := map[string]string{}
		provisionalIDs := make([]string, len(input.Tasks))

		for index, spec := range input.Tasks {
			title := strings.TrimSpace(spec.Title)
			instructions := strings.TrimSpace(spec.Instructions)
			if title == "" || !protocol.IsBoundedString(title, 512) {
				return invalidRequest(fmt.Sprintf("task[%d] title must be between 1 and 512 bytes", index))
			}
			if instructions == "" || !protocol.IsBoundedString(instructions, protocol.PromptMaximumBytes) {
				return invalidRequest(fmt.Sprintf("task[%d] instructions must be non-empty and within %d bytes", index, protocol.PromptMaximumBytes))
			}
			taskID := protocol.NewIdentifier("task")
			provisionalIDs[index] = taskID
			if spec.ClientTaskID != "" {
				if !protocol.IsIdentifier(spec.ClientTaskID) {
					return invalidRequest(fmt.Sprintf("task[%d] clientTaskId is malformed", index))
				}
				if _, duplicate := clientTaskMap[spec.ClientTaskID]; duplicate {
					return invalidRequest(fmt.Sprintf("duplicate clientTaskId %q in batch", spec.ClientTaskID))
				}
				clientTaskMap[spec.ClientTaskID] = taskID
			}
		}

		origin := protocol.TaskOrigin{
			Kind:         caller.Kind,
			SubmissionID: submissionID,
			ClientID:     caller.ClientID,
			RunID:        caller.RunID,
			TaskID:       parentTaskID,
			InstanceID:   caller.InstanceID,
		}
		if origin.Kind == "" {
			origin.Kind = "orchestrator-client"
		}

		proposedTasks := make([]protocol.Task, len(input.Tasks))
		for index, spec := range input.Tasks {
			taskID := provisionalIDs[index]
			dependencies := make([]protocol.TaskDependency, 0, len(spec.Dependencies))
			seenDependencies := map[string]bool{}

			for _, dependencySpec := range spec.Dependencies {
				hasTaskID := dependencySpec.TaskID != ""
				hasClientTaskID := dependencySpec.ClientTaskID != ""
				if hasTaskID == hasClientTaskID {
					return invalidRequest(fmt.Sprintf("task[%d] dependency must specify exactly one of taskId or clientTaskId", index))
				}
				var targetTaskID string
				if hasClientTaskID {
					mappedID, found := clientTaskMap[dependencySpec.ClientTaskID]
					if !found {
						return invalidRequest(fmt.Sprintf("task[%d] references unknown clientTaskId %q", index, dependencySpec.ClientTaskID))
					}
					targetTaskID = mappedID
				} else {
					existingTask, found := state.Tasks[dependencySpec.TaskID]
					if !found || existingTask.ThreadID != threadID {
						return invalidRequest(fmt.Sprintf("task[%d] references unknown taskId %q in thread %s", index, dependencySpec.TaskID, threadID))
					}
					targetTaskID = existingTask.ID
				}
				if targetTaskID == taskID {
					return invalidRequest(fmt.Sprintf("task[%d] cannot depend on itself", index))
				}
				if seenDependencies[targetTaskID] {
					continue
				}
				seenDependencies[targetTaskID] = true

				policy := dependencySpec.Policy
				if policy == "" {
					policy = "require-success"
				}
				if !slices.Contains(protocol.DependencyPolicies, policy) {
					return invalidRequest(fmt.Sprintf("task[%d] dependency policy %q is invalid", index, policy))
				}
				dependencies = append(dependencies, protocol.TaskDependency{
					TaskID: targetTaskID,
					Policy: policy,
				})
			}

			maxAttempts := spec.MaxAttempts
			if maxAttempts <= 0 {
				maxAttempts = protocol.MaximumTaskAttempts
			}
			if maxAttempts > protocol.MaximumTaskAttempts {
				maxAttempts = protocol.MaximumTaskAttempts
			}

			placementInstanceID := spec.InstanceID
			if placementInstanceID == "" && spec.PlacementOverride != nil && spec.PlacementOverride.InstanceID != "" {
				placementInstanceID = spec.PlacementOverride.InstanceID
			}
			if placementInstanceID != "" {
				pinnedInstance, instanceExists := state.Instances[placementInstanceID]
				if !instanceExists || pinnedInstance.ThreadID != threadID {
					return invalidRequest(fmt.Sprintf("task[%d] pins unknown instance %q", index, placementInstanceID))
				}
				if protocol.IsTerminalInstanceStatus(pinnedInstance.Status) || pinnedInstance.Status == "draining" {
					return conflictError("instance_unavailable", fmt.Sprintf("pinned instance %s is %s", placementInstanceID, pinnedInstance.Status))
				}
			}

			if spec.Requirements.ProjectProfileID != "" {
				if _, profileExists := state.ProjectProfiles[spec.Requirements.ProjectProfileID]; !profileExists {
					return invalidRequest(fmt.Sprintf("task[%d] references unknown projectProfileId %q", index, spec.Requirements.ProjectProfileID))
				}
			}

			for toolID, constraintRaw := range spec.Requirements.Toolchains {
				if !protocol.LabelOrAcceleratorPattern.MatchString(toolID) {
					return invalidRequest(fmt.Sprintf("task[%d] toolchain id %q is invalid", index, toolID))
				}
				if _, parseErr := protocol.ParseVersionConstraint(constraintRaw); parseErr != nil {
					return invalidRequest(fmt.Sprintf("task[%d] toolchain constraint %s=%s is invalid: %v", index, toolID, constraintRaw, parseErr))
				}
			}

			proposedTasks[index] = protocol.Task{
				ID:                  taskID,
				ThreadID:            threadID,
				ClientTaskID:        spec.ClientTaskID,
				ParentTaskID:        parentTaskID,
				CreatedByRunID:      caller.RunID,
				Origin:              origin,
				Title:               strings.TrimSpace(spec.Title),
				Instructions:        strings.TrimSpace(spec.Instructions),
				Status:              "pending",
				AttemptCount:        0,
				MaxAttempts:         maxAttempts,
				Dependencies:        dependencies,
				Requirements:        spec.Requirements,
				PlacementOverride:   spec.PlacementOverride,
				PlacementInstanceID: placementInstanceID,
				CreatedAt:           nowText,
				UpdatedAt:           nowText,
			}
		}

		for _, proposed := range proposedTasks {
			state.Tasks[proposed.ID] = proposed
		}
		if hasDependencyCycle(state, threadID) {
			return conflictError("cycle_detected", "submitted task dependencies contain a cycle")
		}

		recomputeThreadTaskReadiness(state, threadID, nowText)

		if thread.Status == "resolved" {
			thread.Status = "open"
		}
		thread.UpdatedAt = nowText
		state.Threads[threadID] = thread

		createdTasks := make([]protocol.Task, len(provisionalIDs))
		for index, taskID := range provisionalIDs {
			createdTasks[index] = state.Tasks[taskID]
		}

		var savedClientMap map[string]string
		if len(clientTaskMap) > 0 {
			savedClientMap = clientTaskMap
		}
		state.TaskSubmissions[receiptKey] = protocol.TaskSubmissionReceipt{
			ID:             submissionID,
			ThreadID:       threadID,
			SourceKey:      sourceKey,
			IdempotencyKey: idempotencyKey,
			Digest:         digest,
			TaskIDs:        provisionalIDs,
			ClientTaskMap:  savedClientMap,
			CreatedAt:      nowText,
		}

		result = SubmitTasksResult{
			ThreadID:      threadID,
			Tasks:         createdTasks,
			ClientTaskMap: savedClientMap,
			Replayed:      false,
		}
		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
		return nil
	})
	return result, err
}

func (service *Service) DelegateTask(caller CallerIdentity, input DelegateTaskInput) (SubmitTasksResult, error) {
	if caller.Kind != "run" {
		return SubmitTasksResult{}, forbiddenError("delegate_task can only be invoked by an active worker run")
	}
	if !caller.CanDelegate {
		return SubmitTasksResult{}, forbiddenError("this worker instance is not authorized to delegate tasks")
	}
	return service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       caller.ThreadID,
		IdempotencyKey: input.IdempotencyKey,
		Tasks: []SubmittedTaskSpec{{
			ClientTaskID:      input.ClientTaskID,
			Title:             input.Title,
			Instructions:      input.Instructions,
			MaxAttempts:       input.MaxAttempts,
			Dependencies:      input.Dependencies,
			Requirements:      input.Requirements,
			PlacementOverride: input.PlacementOverride,
			InstanceID:        input.InstanceID,
		}},
	})
}

func (service *Service) UpdateTask(caller CallerIdentity, input UpdateTaskInput) (protocol.Task, error) {
	taskID := strings.TrimSpace(input.TaskID)
	if taskID == "" && caller.Kind == "run" {
		taskID = caller.TaskID
	}
	if !protocol.IsIdentifier(taskID) {
		return protocol.Task{}, invalidRequest("taskId is required")
	}

	sourceKey := caller.SourceKey()
	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	digest := canonicalDigest(input)

	var updatedTask protocol.Task
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		task, exists := state.Tasks[taskID]
		if !exists {
			return notFound(fmt.Sprintf("task %s does not exist", taskID))
		}
		if caller.Kind == "run" {
			if caller.ThreadID != "" && task.ThreadID != caller.ThreadID {
				return forbiddenError("worker run cannot update a task in another thread")
			}
			if caller.TaskID != "" && task.ID != caller.TaskID && task.ParentTaskID != caller.TaskID {
				return forbiddenError("worker run can only update its assigned task or its delegated child tasks")
			}
		}

		if idempotencyKey != "" {
			receiptKey := sourceKey + "\x00" + taskID + "\x00" + idempotencyKey
			if existingReceipt, found := state.TaskUpdates[receiptKey]; found {
				if existingReceipt.Digest != digest {
					return conflictError("idempotency_conflict", "idempotencyKey was already used for a different task update")
				}
				updatedTask = task
				return nil
			}
			state.TaskUpdates[receiptKey] = protocol.TaskUpdateReceipt{
				ID:             protocol.NewIdentifier("upd"),
				TaskID:         taskID,
				SourceKey:      sourceKey,
				IdempotencyKey: idempotencyKey,
				Digest:         digest,
				CreatedAt:      service.nowUTC(),
			}
		}

		nowText := service.nowUTC()
		if input.Summary != nil {
			trimmed := strings.TrimSpace(*input.Summary)
			if !protocol.IsBoundedString(trimmed, 8192) {
				return invalidRequest("task summary exceeds 8192 bytes")
			}
			task.Summary = trimmed
		}
		if input.BlockerReason != nil {
			trimmed := strings.TrimSpace(*input.BlockerReason)
			if !protocol.IsBoundedString(trimmed, 4096) {
				return invalidRequest("task blockerReason exceeds 4096 bytes")
			}
			task.BlockerReason = trimmed
		}
		for _, artifactID := range input.ArtifactIDs {
			artifact, artifactExists := state.Artifacts[artifactID]
			if !artifactExists || artifact.ThreadID != task.ThreadID {
				return invalidRequest(fmt.Sprintf("unknown artifact %q in thread %s", artifactID, task.ThreadID))
			}
			if !slices.Contains(task.ArtifactIDs, artifactID) {
				task.ArtifactIDs = append(task.ArtifactIDs, artifactID)
			}
		}

		if input.Status != "" && input.Status != task.Status {
			if input.Status == "cancelled" {
				service.cancelTaskInState(state, mutation, &task, nowText)
			} else if protocol.IsTerminalTaskStatus(task.Status) {
				return conflictError("invalid_transition", fmt.Sprintf("task %s is already %s", task.ID, task.Status))
			} else {
				allowed := protocol.CanTransitionTask(task.Status, input.Status) ||
					(task.Status == "running" && input.Status == "blocked")
				if !allowed {
					return conflictError("invalid_transition", fmt.Sprintf("cannot transition task %s from %s to %s", task.ID, task.Status, input.Status))
				}
				task.Status = input.Status
				if protocol.IsTerminalTaskStatus(task.Status) {
					task.FinishedAt = nowText
				}
			}
		}

		task.UpdatedAt = nowText
		state.Tasks[task.ID] = task
		recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
		updatedTask = state.Tasks[task.ID]

		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
		return nil
	})
	return updatedTask, err
}

func (service *Service) GetTaskContext(caller CallerIdentity) (TaskContextResponse, error) {
	if caller.Kind != "run" || caller.RunID == "" {
		return TaskContextResponse{}, forbiddenError("get_task_context is only available to an active worker run")
	}

	var response TaskContextResponse
	err := service.store.Read(func(state *store.State) error {
		runRecord, runExists := state.Runs[caller.RunID]
		if !runExists {
			return notFound(fmt.Sprintf("run %s does not exist", caller.RunID))
		}
		thread, threadExists := state.Threads[runRecord.ThreadID]
		if !threadExists {
			return notFound(fmt.Sprintf("thread %s does not exist", runRecord.ThreadID))
		}
		response.Thread = thread
		runCopy := runRecord
		response.Run = &runCopy

		if runRecord.TaskID != nil {
			if task, taskExists := state.Tasks[*runRecord.TaskID]; taskExists {
				response.Task = task
				for _, dependency := range task.Dependencies {
					if depTask, depExists := state.Tasks[dependency.TaskID]; depExists {
						response.Dependencies = append(response.Dependencies, DependencyTaskSummary{
							ID:          depTask.ID,
							Title:       depTask.Title,
							Status:      depTask.Status,
							Policy:      dependency.Policy,
							Summary:     depTask.Summary,
							ArtifactIDs: depTask.ArtifactIDs,
						})
					}
				}
				for _, candidate := range state.Tasks {
					if candidate.ParentTaskID == task.ID {
						response.Children = append(response.Children, candidate)
					}
				}
			}
		}

		for _, message := range collectThreadMessages(state, runRecord.ThreadID) {
			if isMessageVisibleToTask(message, response.Task.ID, caller.RunID) {
				response.Messages = append(response.Messages, message)
			}
		}
		for _, artifact := range state.Artifacts {
			if artifact.ThreadID == runRecord.ThreadID && artifact.Uploaded {
				response.Artifacts = append(response.Artifacts, artifact)
			}
		}
		return nil
	})
	if err != nil {
		return TaskContextResponse{}, err
	}
	if response.Dependencies == nil {
		response.Dependencies = []DependencyTaskSummary{}
	}
	if response.Children == nil {
		response.Children = []protocol.Task{}
	}
	if response.Messages == nil {
		response.Messages = []protocol.TaskMessage{}
	}
	if response.Artifacts == nil {
		response.Artifacts = []protocol.Artifact{}
	}
	return response, nil
}

type CancelResult struct {
	Kind string         `json:"kind"`
	Run  *protocol.Run  `json:"run,omitempty"`
	Task *protocol.Task `json:"task,omitempty"`
}

func (service *Service) CancelTarget(targetID string) (CancelResult, error) {
	if !protocol.IsIdentifier(targetID) {
		return CancelResult{}, invalidRequest("target identifier is required")
	}

	var result CancelResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		nowText := service.nowUTC()
		if task, isTask := state.Tasks[targetID]; isTask {
			service.cancelTaskInState(state, mutation, &task, nowText)
			state.Tasks[task.ID] = task
			recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
			taskCopy := state.Tasks[task.ID]
			result = CancelResult{Kind: "task", Task: &taskCopy}
			if taskCopy.Assignment != nil {
				if runRecord, runExists := state.Runs[taskCopy.Assignment.RunID]; runExists {
					runCopy := runRecord
					result.Run = &runCopy
				}
			}
			mutation.OnCommit(func() {
				service.TriggerSchedule()
			})
			return nil
		}

		if runRecord, isRun := state.Runs[targetID]; isRun {
			service.cancelRunInState(state, mutation, &runRecord, nowText)
			state.Runs[runRecord.ID] = runRecord
			if runRecord.TaskID != nil {
				if task, taskExists := state.Tasks[*runRecord.TaskID]; taskExists && !protocol.IsTerminalTaskStatus(task.Status) {
					task.Status = "cancelled"
					task.UpdatedAt = nowText
					task.FinishedAt = nowText
					state.Tasks[task.ID] = task
					recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
					taskCopy := state.Tasks[task.ID]
					result.Task = &taskCopy
				}
			}
			runCopy := state.Runs[runRecord.ID]
			result.Kind = "run"
			result.Run = &runCopy
			mutation.OnCommit(func() {
				service.TriggerSchedule()
			})
			return nil
		}

		return notFound(fmt.Sprintf("no run or task %q exists", targetID))
	})
	return result, err
}

func (service *Service) cancelTaskInState(
	state *store.State,
	mutation *store.Mutation,
	task *protocol.Task,
	nowText string,
) {
	if protocol.IsTerminalTaskStatus(task.Status) {
		return
	}
	if task.Assignment != nil {
		if runRecord, exists := state.Runs[task.Assignment.RunID]; exists && protocol.IsActiveRunStatus(runRecord.Status) {
			service.cancelRunInState(state, mutation, &runRecord, nowText)
			state.Runs[runRecord.ID] = runRecord
		}
	}
	task.Status = "cancelled"
	task.UpdatedAt = nowText
	task.FinishedAt = nowText
}

func (service *Service) cancelRunInState(
	state *store.State,
	mutation *store.Mutation,
	runRecord *protocol.Run,
	nowText string,
) {
	if !protocol.IsActiveRunStatus(runRecord.Status) {
		return
	}
	wasQueued := runRecord.Status == "queued"
	runRecord.Status = "cancelled"
	runRecord.FinishedAt = &nowText

	for approvalID, approval := range state.Approvals {
		if approval.RunID == runRecord.ID && approval.Status == "pending" {
			approval.Status = "cancelled"
			approval.DeliveryState = "not-applied"
			approval.ResolvedAt = nowText
			approval.UpdatedAt = nowText
			state.Approvals[approvalID] = approval
		}
	}

	if instance, exists := state.Instances[runRecord.InstanceID]; exists {
		if instance.Status == "busy" {
			instance.Status = "idle"
			instance.Lease.ExpiresAt = extendLeaseTimestamp(nowText, instance.Lease.IdleTimeoutSeconds)
			instance.UpdatedAt = nowText
			state.Instances[instance.ID] = instance
		}
	}

	if !wasQueued {
		nodeID := runRecord.NodeID
		runID := runRecord.ID
		mutation.OnCommit(func() {
			_ = service.sendToSlave(nodeID, protocol.MasterControlMessage{
				Type:  "cancel",
				RunID: runID,
				At:    service.nowUTC(),
			})
		})
	}
}

func hasDependencyCycle(state *store.State, threadID string) bool {
	const (
		unvisited = 0
		visiting  = 1
		visited   = 2
	)
	visitState := map[string]int{}

	var visit func(taskID string) bool
	visit = func(taskID string) bool {
		switch visitState[taskID] {
		case visiting:
			return true
		case visited:
			return false
		}
		visitState[taskID] = visiting
		task, exists := state.Tasks[taskID]
		if exists && task.ThreadID == threadID {
			for _, dependency := range task.Dependencies {
				if visit(dependency.TaskID) {
					return true
				}
			}
		}
		visitState[taskID] = visited
		return false
	}

	for taskID, task := range state.Tasks {
		if task.ThreadID != threadID {
			continue
		}
		if visit(taskID) {
			return true
		}
	}
	return false
}

func recomputeThreadTaskReadiness(state *store.State, threadID string, nowText string) {
	changed := true
	for changed {
		changed = false
		for _, taskID := range sortedTaskIDs(state, threadID) {
			task := state.Tasks[taskID]
			if task.Status != "pending" && task.Status != "ready" {
				continue
			}

			allSatisfied := true
			blockedReason := ""

			for _, dependency := range task.Dependencies {
				depTask, exists := state.Tasks[dependency.TaskID]
				if !exists {
					allSatisfied = false
					blockedReason = fmt.Sprintf("missing dependency %s", dependency.TaskID)
					break
				}
				switch dependency.Policy {
				case "allow-failure":
					if !protocol.IsTerminalTaskStatus(depTask.Status) && depTask.Status != "blocked" {
						allSatisfied = false
					}
				default:
					if depTask.Status == "completed" {
						continue
					}
					allSatisfied = false
					if depTask.Status == "failed" || depTask.Status == "cancelled" || depTask.Status == "blocked" {
						blockedReason = fmt.Sprintf("dependency %s (%s) %s", depTask.ID, depTask.Title, depTask.Status)
					}
				}
				if blockedReason != "" {
					break
				}
			}

			if blockedReason != "" && task.Status != "blocked" {
				task.Status = "blocked"
				task.BlockerReason = blockedReason
				task.UpdatedAt = nowText
				state.Tasks[taskID] = task
				changed = true
				continue
			}
			if blockedReason == "" && allSatisfied && task.Status == "pending" {
				task.Status = "ready"
				task.UpdatedAt = nowText
				state.Tasks[taskID] = task
				changed = true
			}
		}
	}
}

func sortedTaskIDs(state *store.State, threadID string) []string {
	var ids []string
	for taskID, task := range state.Tasks {
		if task.ThreadID == threadID {
			ids = append(ids, taskID)
		}
	}
	slices.Sort(ids)
	return ids
}
