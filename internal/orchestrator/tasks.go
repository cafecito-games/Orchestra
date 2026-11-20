package orchestrator

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/activitylog"
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
	Requirements      protocol.ExecutionRequirements `json:"requirements"`
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
	Requirements      protocol.ExecutionRequirements `json:"requirements"`
	PlacementOverride *protocol.PlacementOverride    `json:"placementOverride,omitempty"`
	InstanceID        string                         `json:"instanceId,omitempty"`
}

type TaskCompletionInput struct {
	Summary     string   `json:"summary"`
	ArtifactIDs []string `json:"artifactIds,omitempty"`
}

type NullableString struct {
	Set   bool
	Value *string
}

func (nullable *NullableString) UnmarshalJSON(raw []byte) error {
	nullable.Set = true
	if string(raw) == "null" {
		nullable.Value = nil
		return nil
	}
	var decoded string
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	nullable.Value = &decoded
	return nil
}

func (nullable NullableString) MarshalJSON() ([]byte, error) {
	if !nullable.Set || nullable.Value == nil {
		return []byte("null"), nil
	}
	return json.Marshal(*nullable.Value)
}

type UpdateTaskInput struct {
	TaskID         string               `json:"taskId,omitempty"`
	IdempotencyKey string               `json:"idempotencyKey,omitempty"`
	Status         string               `json:"status,omitempty"`
	Summary        *string              `json:"summary,omitempty"`
	Progress       *string              `json:"progress,omitempty"`
	BlockerKind    *string              `json:"blockerKind,omitempty"`
	BlockerReason  *string              `json:"blockerReason,omitempty"`
	BlockedReason  NullableString       `json:"blockedReason"`
	ArtifactIDs    []string             `json:"artifactIds,omitempty"`
	Completion     *TaskCompletionInput `json:"completion,omitempty"`
}

type UnblockTaskInput struct {
	TaskID         string `json:"taskId"`
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
	Note           string `json:"note,omitempty"`
}

type UnblockTaskResult struct {
	Task     protocol.Task `json:"task"`
	Replayed bool          `json:"replayed"`
}

type ReopenTaskInput struct {
	TaskID             string `json:"taskId"`
	IdempotencyKey     string `json:"idempotencyKey,omitempty"`
	Note               string `json:"note,omitempty"`
	AdditionalAttempts int    `json:"additionalAttempts,omitempty"`
}

type ReopenTaskResult struct {
	Task     protocol.Task `json:"task"`
	Replayed bool          `json:"replayed"`
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
	Thread       protocol.Thread          `json:"thread"`
	Task         protocol.Task            `json:"task"`
	Room         protocol.TaskRoomSummary `json:"room"`
	Run          *protocol.Run            `json:"run,omitempty"`
	Dependencies []DependencyTaskSummary  `json:"dependencies"`
	Children     []protocol.Task          `json:"children"`
	Messages     []protocol.TaskMessage   `json:"messages"`
	Artifacts    []protocol.Artifact      `json:"artifacts"`
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

		for index, taskSpecification := range input.Tasks {
			title := strings.TrimSpace(taskSpecification.Title)
			instructions := strings.TrimSpace(taskSpecification.Instructions)
			if title == "" || !protocol.IsBoundedString(title, 512) {
				return invalidRequest(fmt.Sprintf("task[%d] title must be between 1 and 512 bytes", index))
			}
			if instructions == "" || !protocol.IsBoundedString(instructions, protocol.PromptMaximumBytes) {
				return invalidRequest(fmt.Sprintf("task[%d] instructions must be non-empty and within %d bytes", index, protocol.PromptMaximumBytes))
			}
			taskID := protocol.NewIdentifier("task")
			provisionalIDs[index] = taskID
			if taskSpecification.ClientTaskID != "" {
				if !protocol.IsIdentifier(taskSpecification.ClientTaskID) {
					return invalidRequest(fmt.Sprintf("task[%d] clientTaskId is malformed", index))
				}
				if _, duplicate := clientTaskMap[taskSpecification.ClientTaskID]; duplicate {
					return invalidRequest(fmt.Sprintf("duplicate clientTaskId %q in batch", taskSpecification.ClientTaskID))
				}
				clientTaskMap[taskSpecification.ClientTaskID] = taskID
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
		for index, taskSpecification := range input.Tasks {
			taskID := provisionalIDs[index]
			dependencies := make([]protocol.TaskDependency, 0, len(taskSpecification.Dependencies))
			seenDependencies := map[string]bool{}

			for _, dependencySpec := range taskSpecification.Dependencies {
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

			maxAttempts := taskSpecification.MaxAttempts
			if maxAttempts <= 0 {
				maxAttempts = protocol.MaximumTaskAttempts
			}
			if maxAttempts > protocol.MaximumTaskAttempts {
				maxAttempts = protocol.MaximumTaskAttempts
			}

			placementInstanceID := taskSpecification.InstanceID
			if placementInstanceID == "" && taskSpecification.PlacementOverride != nil && taskSpecification.PlacementOverride.InstanceID != "" {
				placementInstanceID = taskSpecification.PlacementOverride.InstanceID
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

			if taskSpecification.Requirements.ProjectProfileID != "" {
				if _, profileExists := state.ProjectProfiles[taskSpecification.Requirements.ProjectProfileID]; !profileExists {
					return invalidRequest(fmt.Sprintf("task[%d] references unknown projectProfileId %q", index, taskSpecification.Requirements.ProjectProfileID))
				}
			}

			for toolID, constraintRaw := range taskSpecification.Requirements.Toolchains {
				if !protocol.LabelOrAcceleratorPattern.MatchString(toolID) {
					return invalidRequest(fmt.Sprintf("task[%d] toolchain id %q is invalid", index, toolID))
				}
				if _, parseErr := protocol.ParseVersionConstraint(constraintRaw); parseErr != nil {
					return invalidRequest(fmt.Sprintf("task[%d] toolchain constraint %s=%s is invalid: %v", index, toolID, constraintRaw, parseErr))
				}
			}

			if workspaceErr := validateSubmittedTaskWorkspaceInState(state, threadID, index, taskSpecification, placementInstanceID); workspaceErr != nil {
				return workspaceErr
			}

			proposedTasks[index] = protocol.Task{
				ID:                  taskID,
				ThreadID:            threadID,
				ClientTaskID:        taskSpecification.ClientTaskID,
				ParentTaskID:        parentTaskID,
				CreatedByRunID:      caller.RunID,
				Origin:              origin,
				Title:               strings.TrimSpace(taskSpecification.Title),
				Instructions:        strings.TrimSpace(taskSpecification.Instructions),
				Status:              "pending",
				AttemptCount:        0,
				MaxAttempts:         maxAttempts,
				Dependencies:        dependencies,
				Requirements:        taskSpecification.Requirements,
				PlacementOverride:   taskSpecification.PlacementOverride,
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
	if err == nil && !result.Replayed {
		readyCount := 0
		titles := make([]string, 0, min(len(result.Tasks), 3))
		for index, task := range result.Tasks {
			if task.Status == "ready" {
				readyCount++
			}
			if index < 3 {
				titles = append(titles, task.Title)
			}
		}
		service.logger.Info(
			"task",
			"submitted",
			activitylog.String("thread", result.ThreadID),
			activitylog.Int("count", len(result.Tasks)),
			activitylog.Int("ready", readyCount),
			activitylog.String("titles", strings.Join(titles, "; ")),
		)
	}
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
	isCompletionAttempt := input.Completion != nil || input.Status == "completed"

	var updatedTask protocol.Task
	rejectedByUnreadNudge := false
	var unreadRequiredSequence int64
	var unreadLastReadSequence int64

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

		receiptKey := sourceKey + "\x00" + taskID + "\x00" + idempotencyKey
		if idempotencyKey != "" {
			if existingReceipt, found := state.TaskUpdates[receiptKey]; found {
				if existingReceipt.Digest != digest {
					return conflictError("idempotency_conflict", "idempotencyKey was already used for a different task update")
				}
				updatedTask = task
				return nil
			}
		}

		var completionSummary string
		if input.Completion != nil {
			completionSummary = strings.TrimSpace(input.Completion.Summary)
			if completionSummary == "" || !protocol.IsBoundedString(completionSummary, 8192) {
				return invalidRequest("completion.summary must be a non-empty string up to 8192 bytes")
			}
			for _, artifactID := range input.Completion.ArtifactIDs {
				artifact, artifactExists := state.Artifacts[artifactID]
				if !artifactExists || artifact.ThreadID != task.ThreadID {
					return invalidRequest(fmt.Sprintf("unknown artifact %q in thread %s", artifactID, task.ThreadID))
				}
			}
		}
		if input.Summary != nil {
			trimmed := strings.TrimSpace(*input.Summary)
			if !protocol.IsBoundedString(trimmed, 8192) {
				return invalidRequest("task summary exceeds 8192 bytes")
			}
		}
		if input.Progress != nil {
			trimmed := strings.TrimSpace(*input.Progress)
			if !protocol.IsBoundedString(trimmed, 8192) {
				return invalidRequest("task progress exceeds 8192 bytes")
			}
		}
		if input.BlockerKind != nil {
			trimmed := strings.TrimSpace(*input.BlockerKind)
			if trimmed != "" && !slices.Contains(protocol.TaskBlockerKinds, trimmed) {
				return invalidRequest(fmt.Sprintf("unsupported blockerKind %q", *input.BlockerKind))
			}
		}
		if input.BlockerReason != nil {
			trimmed := strings.TrimSpace(*input.BlockerReason)
			if !protocol.IsBoundedString(trimmed, 4096) {
				return invalidRequest("task blockerReason exceeds 4096 bytes")
			}
		}
		if input.BlockedReason.Set && input.BlockedReason.Value != nil {
			trimmed := strings.TrimSpace(*input.BlockedReason.Value)
			if !protocol.IsBoundedString(trimmed, 4096) {
				return invalidRequest("task blockedReason exceeds 4096 bytes")
			}
		}
		for _, artifactID := range input.ArtifactIDs {
			artifact, artifactExists := state.Artifacts[artifactID]
			if !artifactExists || artifact.ThreadID != task.ThreadID {
				return invalidRequest(fmt.Sprintf("unknown artifact %q in thread %s", artifactID, task.ThreadID))
			}
		}

		nowText := service.nowUTC()
		if caller.Kind == "run" && isCompletionAttempt && task.ID == caller.TaskID &&
			task.RequiredRoomSequence > task.LastReadRoomSequence &&
			task.NudgedAtRoomSequence < task.RequiredRoomSequence {
			task.NudgedAtRoomSequence = task.RequiredRoomSequence
			task.UpdatedAt = nowText
			state.Tasks[task.ID] = task
			rejectedByUnreadNudge = true
			unreadRequiredSequence = task.RequiredRoomSequence
			unreadLastReadSequence = task.LastReadRoomSequence
			return nil
		}

		if idempotencyKey != "" {
			state.TaskUpdates[receiptKey] = protocol.TaskUpdateReceipt{
				ID:             protocol.NewIdentifier("upd"),
				TaskID:         taskID,
				SourceKey:      sourceKey,
				IdempotencyKey: idempotencyKey,
				Digest:         digest,
				CreatedAt:      nowText,
			}
		}

		if input.Progress != nil {
			task.Progress = service.redactor.Redact(strings.TrimSpace(*input.Progress))
		}
		if input.Summary != nil {
			task.Summary = service.redactor.Redact(strings.TrimSpace(*input.Summary))
		}
		if input.Completion != nil {
			task.Summary = service.redactor.Redact(completionSummary)
			for _, artifactID := range input.Completion.ArtifactIDs {
				if !slices.Contains(task.ArtifactIDs, artifactID) {
					task.ArtifactIDs = append(task.ArtifactIDs, artifactID)
				}
			}
		}
		if input.BlockerKind != nil {
			task.BlockerKind = strings.TrimSpace(*input.BlockerKind)
		}
		if input.BlockerReason != nil {
			task.BlockerReason = service.redactor.Redact(strings.TrimSpace(*input.BlockerReason))
		}
		if input.BlockedReason.Set {
			if input.BlockedReason.Value == nil {
				task.BlockerReason = ""
				task.BlockerKind = ""
			} else {
				task.BlockerReason = service.redactor.Redact(strings.TrimSpace(*input.BlockedReason.Value))
			}
		}
		for _, artifactID := range input.ArtifactIDs {
			if !slices.Contains(task.ArtifactIDs, artifactID) {
				task.ArtifactIDs = append(task.ArtifactIDs, artifactID)
			}
		}

		if input.Status != "" && input.Status != task.Status {
			switch {
			case input.Status == "cancelled":
				service.cancelTaskInState(state, mutation, &task, nowText)
			case protocol.IsTerminalTaskStatus(task.Status):
				return conflictError("invalid_transition", fmt.Sprintf("task %s is already %s", task.ID, task.Status))
			case task.Status == "blocked":
				return conflictError("invalid_transition", fmt.Sprintf("task %s is blocked; use unblock_task to resume it", task.ID))
			case task.Status == "running" && input.Status == "blocked":
				service.yieldRunForBlockedTaskInState(state, mutation, &task, nowText)
			default:
				if !protocol.CanTransitionTask(task.Status, input.Status) {
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
	if err != nil {
		return protocol.Task{}, err
	}
	if rejectedByUnreadNudge {
		return protocol.Task{}, conflictError(
			"unread_room_nudge",
			fmt.Sprintf(
				"thread room has unread nudge through sequence %d (lastReadSequence=%d); call read_room before completing this task",
				unreadRequiredSequence,
				unreadLastReadSequence,
			),
		)
	}
	statusOrProgress := updatedTask.Summary
	if statusOrProgress == "" {
		statusOrProgress = updatedTask.Progress
	}
	service.logger.Info(
		"task",
		"updated",
		activitylog.String("task", updatedTask.ID),
		activitylog.String("thread", updatedTask.ThreadID),
		activitylog.String("status", updatedTask.Status),
		activitylog.String("summary", statusOrProgress),
	)
	return updatedTask, nil
}

func (service *Service) UnblockTask(caller CallerIdentity, input UnblockTaskInput) (UnblockTaskResult, error) {
	taskID := strings.TrimSpace(input.TaskID)
	if !protocol.IsIdentifier(taskID) {
		return UnblockTaskResult{}, invalidRequest("taskId is required")
	}
	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey != "" && !protocol.IsBoundedString(idempotencyKey, 128) {
		return UnblockTaskResult{}, invalidRequest("idempotencyKey must be at most 128 bytes")
	}
	note := strings.TrimSpace(input.Note)
	if note != "" && !protocol.IsBoundedString(note, 4096) {
		return UnblockTaskResult{}, invalidRequest("note exceeds 4096 bytes")
	}

	sourceKey := caller.SourceKey()
	receiptKey := sourceKey + "\x00unblock\x00" + taskID + "\x00" + idempotencyKey
	digest := canonicalDigest(struct {
		Action string `json:"action"`
		TaskID string `json:"taskId"`
		Note   string `json:"note,omitempty"`
	}{
		Action: "unblock",
		TaskID: taskID,
		Note:   note,
	})

	var result UnblockTaskResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		task, exists := state.Tasks[taskID]
		if !exists {
			return notFound(fmt.Sprintf("task %s does not exist", taskID))
		}
		if authErr := authorizeTaskRecoveryCaller(caller, task, "unblock"); authErr != nil {
			return authErr
		}

		if idempotencyKey != "" {
			if existingReceipt, found := state.TaskUpdates[receiptKey]; found {
				if existingReceipt.Digest != digest {
					return conflictError("idempotency_conflict", "idempotencyKey was already used with a different unblock_task request")
				}
				result = UnblockTaskResult{Task: task, Replayed: true}
				return nil
			}
		}

		if task.Status != "blocked" {
			return conflictError("invalid_transition", fmt.Sprintf("cannot unblock task %s in status %s (expected blocked)", task.ID, task.Status))
		}
		if depErr := checkTaskDependenciesRecoverable(state, task); depErr != nil {
			return depErr
		}

		nowText := service.nowUTC()
		if idempotencyKey != "" {
			state.TaskUpdates[receiptKey] = protocol.TaskUpdateReceipt{
				ID:             protocol.NewIdentifier("upd"),
				TaskID:         taskID,
				SourceKey:      sourceKey,
				IdempotencyKey: idempotencyKey,
				Digest:         digest,
				CreatedAt:      nowText,
			}
		}

		redactedNote := ""
		if note != "" {
			redactedNote = service.redactor.Redact(note)
			recordRecoveryGuidanceMessage(state, caller, task, redactedNote, nowText)
		}

		task.Status = "pending"
		task.BlockerKind = ""
		task.BlockerReason = ""
		task.PlacementDiagnostic = nil
		task.Assignment = nil
		if task.AttemptCount >= task.MaxAttempts {
			task.MaxAttempts = task.AttemptCount + 1
		}
		task.UpdatedAt = nowText
		state.Tasks[task.ID] = task

		if thread, threadExists := state.Threads[task.ThreadID]; threadExists {
			if thread.Status == "resolved" {
				thread.Status = "open"
			}
			thread.UpdatedAt = nowText
			state.Threads[thread.ID] = thread
		}

		recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
		updatedTask := state.Tasks[task.ID]

		detail := map[string]any{
			"action": "unblock",
			"status": updatedTask.Status,
		}
		if redactedNote != "" {
			detail["note"] = redactedNote
		}
		mutation.EmitEvent(protocol.TaskEventEntry{
			ThreadID:   updatedTask.ThreadID,
			Type:       "task.unblocked",
			TaskID:     updatedTask.ID,
			Status:     updatedTask.Status,
			Summary:    fmt.Sprintf("Task %q unblocked (%s)", updatedTask.Title, updatedTask.Status),
			Detail:     detail,
			OccurredAt: nowText,
		})

		result = UnblockTaskResult{Task: updatedTask, Replayed: false}
		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
		return nil
	})
	if err == nil && !result.Replayed {
		service.logger.Info(
			"task",
			"unblocked",
			activitylog.String("task", result.Task.ID),
			activitylog.String("thread", result.Task.ThreadID),
			activitylog.String("status", result.Task.Status),
		)
	}
	return result, err
}

func (service *Service) ReopenTask(caller CallerIdentity, input ReopenTaskInput) (ReopenTaskResult, error) {
	taskID := strings.TrimSpace(input.TaskID)
	if !protocol.IsIdentifier(taskID) {
		return ReopenTaskResult{}, invalidRequest("taskId is required")
	}
	idempotencyKey := strings.TrimSpace(input.IdempotencyKey)
	if idempotencyKey != "" && !protocol.IsBoundedString(idempotencyKey, 128) {
		return ReopenTaskResult{}, invalidRequest("idempotencyKey must be at most 128 bytes")
	}
	note := strings.TrimSpace(input.Note)
	if note != "" && !protocol.IsBoundedString(note, 4096) {
		return ReopenTaskResult{}, invalidRequest("note exceeds 4096 bytes")
	}
	additionalAttempts := input.AdditionalAttempts
	if additionalAttempts < 0 || additionalAttempts > protocol.MaximumTaskAttempts {
		return ReopenTaskResult{}, invalidRequest(fmt.Sprintf("additionalAttempts must be between 1 and %d", protocol.MaximumTaskAttempts))
	}
	if additionalAttempts == 0 {
		additionalAttempts = protocol.MaximumTaskAttempts
	}

	sourceKey := caller.SourceKey()
	receiptKey := sourceKey + "\x00reopen\x00" + taskID + "\x00" + idempotencyKey
	digest := canonicalDigest(struct {
		Action             string `json:"action"`
		TaskID             string `json:"taskId"`
		Note               string `json:"note,omitempty"`
		AdditionalAttempts int    `json:"additionalAttempts"`
	}{
		Action:             "reopen",
		TaskID:             taskID,
		Note:               note,
		AdditionalAttempts: additionalAttempts,
	})

	var result ReopenTaskResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		task, exists := state.Tasks[taskID]
		if !exists {
			return notFound(fmt.Sprintf("task %s does not exist", taskID))
		}
		if authErr := authorizeTaskRecoveryCaller(caller, task, "reopen"); authErr != nil {
			return authErr
		}

		if idempotencyKey != "" {
			if existingReceipt, found := state.TaskUpdates[receiptKey]; found {
				if existingReceipt.Digest != digest {
					return conflictError("idempotency_conflict", "idempotencyKey was already used with a different reopen_task request")
				}
				result = ReopenTaskResult{Task: task, Replayed: true}
				return nil
			}
		}

		if task.Status != "failed" {
			return conflictError("invalid_transition", fmt.Sprintf("cannot reopen task %s in status %s (expected failed)", task.ID, task.Status))
		}
		if depErr := checkTaskDependenciesRecoverable(state, task); depErr != nil {
			return depErr
		}

		nowText := service.nowUTC()
		if idempotencyKey != "" {
			state.TaskUpdates[receiptKey] = protocol.TaskUpdateReceipt{
				ID:             protocol.NewIdentifier("upd"),
				TaskID:         taskID,
				SourceKey:      sourceKey,
				IdempotencyKey: idempotencyKey,
				Digest:         digest,
				CreatedAt:      nowText,
			}
		}

		redactedNote := ""
		if note != "" {
			redactedNote = service.redactor.Redact(note)
			recordRecoveryGuidanceMessage(state, caller, task, redactedNote, nowText)
		}

		task.Status = "pending"
		task.FinishedAt = ""
		task.BlockerKind = ""
		task.BlockerReason = ""
		task.PlacementDiagnostic = nil
		task.Assignment = nil
		task.MaxAttempts = task.AttemptCount + additionalAttempts
		task.UpdatedAt = nowText
		state.Tasks[task.ID] = task

		if thread, threadExists := state.Threads[task.ThreadID]; threadExists {
			if thread.Status == "resolved" {
				thread.Status = "open"
			}
			thread.UpdatedAt = nowText
			state.Threads[thread.ID] = thread
		}

		recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
		updatedTask := state.Tasks[task.ID]

		detail := map[string]any{
			"action":             "reopen",
			"status":             updatedTask.Status,
			"additionalAttempts": additionalAttempts,
			"maxAttempts":        updatedTask.MaxAttempts,
		}
		if redactedNote != "" {
			detail["note"] = redactedNote
		}
		mutation.EmitEvent(protocol.TaskEventEntry{
			ThreadID:   updatedTask.ThreadID,
			Type:       "task.reopened",
			TaskID:     updatedTask.ID,
			Status:     updatedTask.Status,
			Summary:    fmt.Sprintf("Task %q reopened (%s, maxAttempts=%d)", updatedTask.Title, updatedTask.Status, updatedTask.MaxAttempts),
			Detail:     detail,
			OccurredAt: nowText,
		})

		result = ReopenTaskResult{Task: updatedTask, Replayed: false}
		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
		return nil
	})
	if err == nil && !result.Replayed {
		service.logger.Info(
			"task",
			"reopened",
			activitylog.String("task", result.Task.ID),
			activitylog.String("thread", result.Task.ThreadID),
			activitylog.String("status", result.Task.Status),
			activitylog.Int("maxAttempts", result.Task.MaxAttempts),
		)
	}
	return result, err
}

func authorizeTaskRecoveryCaller(caller CallerIdentity, task protocol.Task, action string) error {
	if caller.Kind != "run" {
		return nil
	}
	if !caller.CanDelegate {
		return forbiddenError(fmt.Sprintf("this worker instance is not authorized to %s tasks", action))
	}
	if caller.ThreadID != "" && task.ThreadID != caller.ThreadID {
		return forbiddenError(fmt.Sprintf("worker run cannot %s a task in another thread", action))
	}
	ownsViaDelegation := (caller.TaskID != "" && task.ParentTaskID == caller.TaskID) ||
		(caller.RunID != "" && task.CreatedByRunID == caller.RunID)
	if !ownsViaDelegation {
		return forbiddenError(fmt.Sprintf("worker run can only %s its delegated child tasks", action))
	}
	return nil
}

func checkTaskDependenciesRecoverable(state *store.State, task protocol.Task) error {
	for _, dependency := range task.Dependencies {
		dependencyTask, exists := state.Tasks[dependency.TaskID]
		if !exists {
			return conflictError("dependency_missing", fmt.Sprintf("task %s depends on missing task %s", task.ID, dependency.TaskID))
		}
		if dependency.Policy == "allow-failure" {
			continue
		}
		if dependencyTask.Status == "failed" || dependencyTask.Status == "cancelled" {
			return conflictError(
				"dependency_failed",
				fmt.Sprintf("task %s depends on %s (%s) which is %s", task.ID, dependencyTask.ID, dependencyTask.Title, dependencyTask.Status),
			)
		}
	}
	return nil
}

func recordRecoveryGuidanceMessage(
	state *store.State,
	caller CallerIdentity,
	task protocol.Task,
	redactedNote string,
	nowText string,
) {
	sender := protocol.MessageEndpoint{
		Kind:     "orchestrator",
		ClientID: caller.ClientID,
	}
	switch caller.Kind {
	case "run":
		sender = protocol.MessageEndpoint{
			Kind:   "task",
			TaskID: caller.TaskID,
			RunID:  caller.RunID,
		}
	case "operator":
		sender = protocol.MessageEndpoint{
			Kind:     "orchestrator",
			ClientID: "operator:" + caller.OperatorID,
		}
	}
	messageID := protocol.NewIdentifier("msg")
	state.Messages[messageID] = protocol.TaskMessage{
		ID:             messageID,
		ThreadID:       task.ThreadID,
		SourceKey:      caller.SourceKey(),
		IdempotencyKey: messageID,
		Digest:         canonicalDigest(redactedNote),
		Sender:         sender,
		Recipient: protocol.MessageEndpoint{
			Kind:   "task",
			TaskID: task.ID,
		},
		Kind:      "guidance",
		Body:      redactedNote,
		CreatedAt: nowText,
	}
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
				response.Room = buildTaskRoomSummary(state, runRecord.ThreadID, task)
				for _, dependency := range task.Dependencies {
					if dependencyTask, depExists := state.Tasks[dependency.TaskID]; depExists {
						response.Dependencies = append(response.Dependencies, DependencyTaskSummary{
							ID:          dependencyTask.ID,
							Title:       dependencyTask.Title,
							Status:      dependencyTask.Status,
							Policy:      dependency.Policy,
							Summary:     dependencyTask.Summary,
							ArtifactIDs: dependencyTask.ArtifactIDs,
						})
					}
				}
				for _, candidate := range state.Tasks {
					if candidate.ParentTaskID == task.ID {
						response.Children = append(response.Children, candidate)
					}
				}
			}
		} else {
			response.Room = buildTaskRoomSummary(state, runRecord.ThreadID, protocol.Task{})
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
	if err == nil {
		runID := ""
		if result.Run != nil {
			runID = result.Run.ID
		}
		taskID := ""
		if result.Task != nil {
			taskID = result.Task.ID
		}
		service.logger.Warn(
			"cancel",
			"requested",
			activitylog.String("kind", result.Kind),
			activitylog.String("task", taskID),
			activitylog.String("run", runID),
		)
	}
	return result, err
}

func (service *Service) yieldRunForBlockedTaskInState(
	state *store.State,
	mutation *store.Mutation,
	task *protocol.Task,
	nowText string,
) {
	if task.Assignment != nil {
		task.PreferredInstanceID = task.Assignment.InstanceID
		if runRecord, exists := state.Runs[task.Assignment.RunID]; exists {
			if runRecord.Output != "" {
				task.LastRunOutput = protocol.TruncateUTF8Head(runRecord.Output, 4000)
			}
			if runRecord.WorkspaceLeaseID != nil {
				if lease, leaseExists := state.WorkspaceLeases[*runRecord.WorkspaceLeaseID]; leaseExists &&
					lease.Policy == protocol.WorkspaceIsolationGitWorktree &&
					!lease.ReadOnly &&
					lease.SourceLeaseID == "" &&
					!protocol.IsTerminalWorkspaceLeaseStatus(lease.Status) {
					task.RetainedLeaseID = lease.ID
					if lease.Status == "active" || lease.Status == "released" {
						lease.Status = "retained"
						lease.RetentionReason = "task-blocked"
						lease.Detail = "retained workspace worktree while task is blocked"
						lease.UpdatedAt = nowText
						state.WorkspaceLeases[lease.ID] = lease
					}
				}
			}
			if protocol.IsActiveRunStatus(runRecord.Status) {
				wasQueued := runRecord.Status == "queued"
				runRecord.Status = "cancelled"
				runRecord.FinishedAt = &nowText
				state.Runs[runRecord.ID] = runRecord

				service.settleRunApprovalsInState(state, runRecord.ID, "cancelled", nowText)
				service.releaseInstanceAfterRunInState(state, runRecord.InstanceID, nowText)

				if !wasQueued {
					nodeID := runRecord.NodeID
					runID := runRecord.ID
					mutation.OnCommit(func() {
						_ = service.sendToSlave(nodeID, protocol.MasterControlMessage{
							Type:  "cancel",
							RunID: runID,
							Mode:  "yield",
							At:    service.nowUTC(),
						})
					})
				}
			}
		}
		task.Assignment = nil
	}
	task.Status = "blocked"
	task.UpdatedAt = nowText
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
			isAutoDependencyBlocked := task.Status == "blocked" && task.BlockerKind == protocol.TaskBlockerKindDependency
			if task.Status != "pending" && task.Status != "ready" && !isAutoDependencyBlocked {
				continue
			}

			allSatisfied := true
			blockedReason := ""
			var waitingBlockedDeps []protocol.Task

			for _, dependency := range task.Dependencies {
				dependencyTask, exists := state.Tasks[dependency.TaskID]
				if !exists {
					allSatisfied = false
					blockedReason = "missing dependency " + dependency.TaskID
					break
				}
				switch dependency.Policy {
				case "allow-failure":
					if !protocol.IsTerminalTaskStatus(dependencyTask.Status) {
						allSatisfied = false
						if dependencyTask.Status == "blocked" {
							waitingBlockedDeps = append(waitingBlockedDeps, dependencyTask)
						}
					}
				default:
					if dependencyTask.Status == "completed" {
						continue
					}
					allSatisfied = false
					switch dependencyTask.Status {
					case "failed", "cancelled":
						blockedReason = fmt.Sprintf("dependency %s (%s) %s", dependencyTask.ID, dependencyTask.Title, dependencyTask.Status)
					case "blocked":
						waitingBlockedDeps = append(waitingBlockedDeps, dependencyTask)
					}
				}
				if blockedReason != "" {
					break
				}
			}

			if blockedReason != "" {
				if task.Status != "blocked" || task.BlockerReason != blockedReason || task.BlockerKind != protocol.TaskBlockerKindDependency {
					task.Status = "blocked"
					task.BlockerKind = protocol.TaskBlockerKindDependency
					task.BlockerReason = blockedReason
					task.UpdatedAt = nowText
					state.Tasks[taskID] = task
					changed = true
				}
				continue
			}

			if isAutoDependencyBlocked {
				task.Status = "pending"
				task.BlockerKind = ""
				task.BlockerReason = ""
				task.UpdatedAt = nowText
				state.Tasks[taskID] = task
				changed = true
			}

			if allSatisfied && task.Status == "pending" {
				task.Status = "ready"
				task.PlacementDiagnostic = clearDependencyReadinessDiagnostics(task.PlacementDiagnostic)
				task.UpdatedAt = nowText
				state.Tasks[taskID] = task
				changed = true
				continue
			}

			if task.Status == "pending" {
				if len(waitingBlockedDeps) > 0 {
					unsatisfied := make([]protocol.UnsatisfiedRequirement, 0, len(waitingBlockedDeps))
					for _, depTask := range waitingBlockedDeps {
						detail := fmt.Sprintf("waiting for blocked dependency %s (%s)", depTask.ID, depTask.Title)
						if depTask.BlockerReason != "" {
							detail = fmt.Sprintf("%s: %s", detail, depTask.BlockerReason)
						}
						unsatisfied = append(unsatisfied, protocol.UnsatisfiedRequirement{
							Kind:        "dependency",
							Requirement: "dependency=" + depTask.ID,
							Detail:      detail,
						})
					}
					nextDiagnostic := &protocol.PlacementDiagnostic{
						EvaluatedAt:     nowText,
						EligibleNodeIDs: []string{},
						Unsatisfied:     unsatisfied,
					}
					if !placementDiagnosticsMatchUnsatisfied(task.PlacementDiagnostic, nextDiagnostic) {
						task.PlacementDiagnostic = nextDiagnostic
						task.UpdatedAt = nowText
						state.Tasks[taskID] = task
					}
				} else if task.PlacementDiagnostic != nil && hasOnlyDependencyReadinessDiagnostics(task.PlacementDiagnostic) {
					task.PlacementDiagnostic = nil
					task.UpdatedAt = nowText
					state.Tasks[taskID] = task
				}
			}
		}
	}
}

func hasOnlyDependencyReadinessDiagnostics(diagnostic *protocol.PlacementDiagnostic) bool {
	if diagnostic == nil || len(diagnostic.Unsatisfied) == 0 {
		return false
	}
	for _, entry := range diagnostic.Unsatisfied {
		if entry.Kind != "dependency" {
			return false
		}
	}
	return true
}

func clearDependencyReadinessDiagnostics(diagnostic *protocol.PlacementDiagnostic) *protocol.PlacementDiagnostic {
	if diagnostic == nil || hasOnlyDependencyReadinessDiagnostics(diagnostic) {
		return nil
	}
	return diagnostic
}

func placementDiagnosticsMatchUnsatisfied(left, right *protocol.PlacementDiagnostic) bool {
	if left == nil || right == nil {
		return left == right
	}
	return slices.Equal(left.Unsatisfied, right.Unsatisfied)
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
