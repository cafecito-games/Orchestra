package orchestrator

import (
	"cmp"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/activitylog"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type ClaimTaskInput struct {
	ThreadID            string   `json:"threadId,omitempty"`
	TaskID              string   `json:"taskId,omitempty"`
	Labels              []string `json:"labels,omitempty"`
	HarnessIDs          []string `json:"harnessIds,omitempty"`
	Models              []string `json:"models,omitempty"`
	OperatingSystems    []string `json:"operatingSystems,omitempty"`
	Architectures       []string `json:"architectures,omitempty"`
	TTLSeconds          int      `json:"ttlSeconds,omitempty"`
	TimeoutMilliseconds int      `json:"timeoutMilliseconds,omitempty"`
	WaitMilliseconds    int      `json:"waitMs,omitempty"`
	ClaimerID           string   `json:"claimerId,omitempty"`
}

type ClaimTaskResult struct {
	Claimed  bool                `json:"claimed"`
	TimedOut bool                `json:"timedOut,omitempty"`
	Task     *protocol.Task      `json:"task,omitempty"`
	Claim    *protocol.TaskClaim `json:"claim,omitempty"`
}

type RenewTaskClaimInput struct {
	TaskID     string `json:"taskId"`
	ClaimEpoch int64  `json:"claimEpoch"`
	ClaimToken string `json:"claimToken"`
	TTLSeconds int    `json:"ttlSeconds,omitempty"`
}

type RenewTaskClaimResult struct {
	Task  protocol.Task      `json:"task"`
	Claim protocol.TaskClaim `json:"claim"`
}

type ReleaseTaskClaimInput struct {
	TaskID     string `json:"taskId"`
	ClaimEpoch int64  `json:"claimEpoch"`
	ClaimToken string `json:"claimToken"`
	Reason     string `json:"reason,omitempty"`
}

type ReleaseTaskClaimResult struct {
	Task protocol.Task `json:"task"`
}

func resolveTaskClaimTTLSeconds(raw int) (int, error) {
	if raw == 0 {
		return protocol.DefaultTaskClaimTTLSeconds, nil
	}
	if raw < protocol.MinimumTaskClaimTTLSeconds || raw > protocol.MaximumTaskClaimTTLSeconds {
		return 0, invalidRequest(fmt.Sprintf(
			"ttlSeconds must be between %d and %d",
			protocol.MinimumTaskClaimTTLSeconds,
			protocol.MaximumTaskClaimTTLSeconds,
		))
	}
	return raw, nil
}

func resolveClaimerIdentity(caller CallerIdentity, explicitClaimerID string) (string, string) {
	claimerKind := strings.TrimSpace(caller.Kind)
	if claimerKind == "" {
		claimerKind = "orchestrator-client"
	}
	claimerID := strings.TrimSpace(explicitClaimerID)
	if claimerID == "" {
		switch claimerKind {
		case "operator":
			claimerID = cmp.Or(strings.TrimSpace(caller.OperatorID), "cli")
		case "run":
			claimerID = cmp.Or(strings.TrimSpace(caller.RunID), strings.TrimSpace(caller.InstanceID), "run")
		default:
			claimerID = cmp.Or(strings.TrimSpace(caller.ClientID), strings.TrimSpace(caller.SessionID), "mcp-client")
		}
	}
	return claimerKind, claimerID
}

func normalizedStringSet(values []string) map[string]bool {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]bool, len(values))
	for _, value := range values {
		trimmed := strings.ToLower(strings.TrimSpace(value))
		if trimmed != "" {
			set[trimmed] = true
		}
	}
	return set
}

func hasIntersection(required []string, offered map[string]bool) bool {
	if len(required) == 0 {
		return true
	}
	if len(offered) == 0 {
		return false
	}
	for _, entry := range required {
		if offered[strings.ToLower(strings.TrimSpace(entry))] {
			return true
		}
	}
	return false
}

func claimMatchesTaskRequirements(input ClaimTaskInput, requirements protocol.ExecutionRequirements) bool {
	if len(requirements.Labels) > 0 {
		offeredLabels := normalizedStringSet(input.Labels)
		for _, requiredLabel := range requirements.Labels {
			if !offeredLabels[strings.ToLower(strings.TrimSpace(requiredLabel))] {
				return false
			}
		}
	}
	if !hasIntersection(requirements.HarnessIDs, normalizedStringSet(input.HarnessIDs)) {
		return false
	}
	if !hasIntersection(requirements.Models, normalizedStringSet(input.Models)) {
		return false
	}
	if !hasIntersection(requirements.OperatingSystems, normalizedStringSet(input.OperatingSystems)) {
		return false
	}
	if !hasIntersection(requirements.Architectures, normalizedStringSet(input.Architectures)) {
		return false
	}
	return true
}

func matchesClaimFencing(claim *protocol.TaskClaim, epoch int64, token string) bool {
	if claim == nil || epoch <= 0 || token == "" {
		return false
	}
	if claim.Epoch != epoch {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(claim.Token), []byte(token)) == 1
}

func (service *Service) ClaimTask(
	operationContext context.Context,
	caller CallerIdentity,
	input ClaimTaskInput,
) (ClaimTaskResult, error) {
	taskID := strings.TrimSpace(input.TaskID)
	threadID := strings.TrimSpace(input.ThreadID)
	if taskID != "" && !protocol.IsIdentifier(taskID) {
		return ClaimTaskResult{}, invalidRequest("taskId is malformed")
	}
	if threadID != "" && !protocol.IsIdentifier(threadID) {
		return ClaimTaskResult{}, invalidRequest("threadId is malformed")
	}
	if err := requireScopeOperation(caller, protocol.AccessOpClaim); err != nil {
		return ClaimTaskResult{}, err
	}
	if threadID != "" {
		if err := requireScopeThread(caller, threadID); err != nil {
			return ClaimTaskResult{}, err
		}
	}
	if explicit := strings.TrimSpace(input.ClaimerID); explicit != "" && !protocol.IsBoundedString(explicit, 128) {
		return ClaimTaskResult{}, invalidRequest("claimerId must be at most 128 bytes")
	}
	ttlSeconds, err := resolveTaskClaimTTLSeconds(input.TTLSeconds)
	if err != nil {
		return ClaimTaskResult{}, err
	}

	waitMilliseconds := input.TimeoutMilliseconds
	if waitMilliseconds == 0 && input.WaitMilliseconds > 0 {
		waitMilliseconds = input.WaitMilliseconds
	}
	if waitMilliseconds < 0 {
		waitMilliseconds = 0
	}
	if waitMilliseconds > protocol.MaximumWaitMilliseconds {
		waitMilliseconds = protocol.MaximumWaitMilliseconds
	}

	var eventChannel <-chan []protocol.TaskEventEntry
	var unsubscribeEvents func()
	if waitMilliseconds > 0 {
		eventChannel, unsubscribeEvents = service.store.SubscribeEvents()
		defer unsubscribeEvents()
	}

	attemptClaim := func(allowWaiting bool) (ClaimTaskResult, bool, error) {
		var result ClaimTaskResult
		var canRetryLater bool
		_, mutateErr := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
			now := service.now()
			nowText := now.UTC().Format(time.RFC3339Nano)
			service.expireTaskClaimsInState(state, mutation, now)

			claimerKind, claimerID := resolveClaimerIdentity(caller, input.ClaimerID)

			var candidate *protocol.Task
			if taskID != "" {
				task, exists := state.Tasks[taskID]
				if !exists || (threadID != "" && task.ThreadID != threadID) {
					return notFound(fmt.Sprintf("task %s does not exist", taskID))
				}
				if err := requireScopeThread(caller, task.ThreadID); err != nil {
					return err
				}
				if task.EffectiveExecutionMode() != protocol.TaskExecutionModeClaimable {
					return conflictError("not_claimable", fmt.Sprintf("task %s has executionMode %q and cannot be claimed", task.ID, task.EffectiveExecutionMode()))
				}
				switch {
				case task.Status == "ready":
					taskCopy := task
					candidate = &taskCopy
				case protocol.IsTerminalTaskStatus(task.Status) || !allowWaiting:
					return conflictError("task_not_ready", fmt.Sprintf("task %s is currently %s", task.ID, task.Status))
				default:
					canRetryLater = true
					return nil
				}
			} else {
				effectiveThreadID := threadID
				if effectiveThreadID != "" {
					if _, exists := state.Threads[effectiveThreadID]; !exists {
						return notFound(fmt.Sprintf("thread %s does not exist", effectiveThreadID))
					}
				}
				readyCandidates := make([]protocol.Task, 0)
				for _, task := range state.Tasks {
					if task.Status != "ready" {
						continue
					}
					if task.EffectiveExecutionMode() != protocol.TaskExecutionModeClaimable {
						continue
					}
					if effectiveThreadID != "" {
						if task.ThreadID != effectiveThreadID {
							continue
						}
					} else if thread, threadExists := state.Threads[task.ThreadID]; !threadExists || thread.Status != "open" {
						continue
					}
					if !protocol.ScopeAllowsThread(caller.Scope, task.ThreadID) {
						continue
					}
					if !claimMatchesTaskRequirements(input, task.Requirements) {
						continue
					}
					readyCandidates = append(readyCandidates, task)
				}
				if len(readyCandidates) == 0 {
					canRetryLater = allowWaiting
					return nil
				}
				slices.SortFunc(readyCandidates, func(left, right protocol.Task) int {
					if compared := cmp.Compare(left.CreatedAt, right.CreatedAt); compared != 0 {
						return compared
					}
					return cmp.Compare(left.ID, right.ID)
				})
				chosen := readyCandidates[0]
				candidate = &chosen
			}

			candidate.ClaimEpoch++
			claimToken := protocol.NewIdentifier("claim")
			expiresAt := now.Add(time.Duration(ttlSeconds) * time.Second).UTC().Format(time.RFC3339Nano)
			claim := protocol.TaskClaim{
				Epoch:            candidate.ClaimEpoch,
				Token:            claimToken,
				ClaimerKind:      claimerKind,
				ClaimerID:        claimerID,
				ClaimerSessionID: caller.SessionID,
				TTLSeconds:       ttlSeconds,
				ClaimedAt:        nowText,
				RenewedAt:        nowText,
				ExpiresAt:        expiresAt,
			}
			candidate.Claim = &claim
			candidate.Status = "running"
			candidate.AttemptCount++
			candidate.UpdatedAt = nowText
			state.Tasks[candidate.ID] = *candidate

			mutation.EmitEvent(protocol.TaskEventEntry{
				ThreadID: candidate.ThreadID,
				Type:     "task.claimed",
				TaskID:   candidate.ID,
				Status:   candidate.Status,
				Summary:  fmt.Sprintf("Task %q claimed by %s (epoch %d)", candidate.Title, claimerID, claim.Epoch),
				Detail: map[string]any{
					"claimEpoch": claim.Epoch,
					"claimerId":  claim.ClaimerID,
					"expiresAt":  claim.ExpiresAt,
				},
				OccurredAt: nowText,
			})

			claimedTaskCopy := *candidate
			claimCopy := claim
			result = ClaimTaskResult{
				Claimed: true,
				Task:    &claimedTaskCopy,
				Claim:   &claimCopy,
			}
			return nil
		})
		return result, canRetryLater, mutateErr
	}

	result, canRetryLater, err := attemptClaim(waitMilliseconds > 0)
	if err != nil {
		return ClaimTaskResult{}, err
	}
	if result.Claimed {
		service.logger.Info(
			"task",
			"claimed",
			activitylog.String("task", result.Task.ID),
			activitylog.String("thread", result.Task.ThreadID),
			activitylog.String("claimer", result.Claim.ClaimerID),
			activitylog.Int("epoch", int(result.Claim.Epoch)),
		)
		return result, nil
	}
	if !canRetryLater || waitMilliseconds == 0 {
		return ClaimTaskResult{Claimed: false, TimedOut: false}, nil
	}

	deadline := time.NewTimer(time.Duration(waitMilliseconds) * time.Millisecond)
	defer deadline.Stop()
	pollTicker := time.NewTicker(100 * time.Millisecond)
	defer pollTicker.Stop()

	for {
		select {
		case <-operationContext.Done():
			return ClaimTaskResult{}, operationContext.Err()
		case <-deadline.C:
			finalResult, _, finalErr := attemptClaim(false)
			if finalErr != nil {
				var opErr *OperationError
				if errors.As(finalErr, &opErr) && opErr.Code == "task_not_ready" {
					return ClaimTaskResult{Claimed: false, TimedOut: true}, nil
				}
				return ClaimTaskResult{}, finalErr
			}
			if finalResult.Claimed {
				return finalResult, nil
			}
			return ClaimTaskResult{Claimed: false, TimedOut: true}, nil
		case <-eventChannel:
		case <-pollTicker.C:
		}

		nextResult, retryAgain, nextErr := attemptClaim(true)
		if nextErr != nil {
			return ClaimTaskResult{}, nextErr
		}
		if nextResult.Claimed {
			service.logger.Info(
				"task",
				"claimed",
				activitylog.String("task", nextResult.Task.ID),
				activitylog.String("thread", nextResult.Task.ThreadID),
				activitylog.String("claimer", nextResult.Claim.ClaimerID),
				activitylog.Int("epoch", int(nextResult.Claim.Epoch)),
			)
			return nextResult, nil
		}
		if !retryAgain {
			return ClaimTaskResult{Claimed: false, TimedOut: true}, nil
		}
	}
}

func (service *Service) RenewTaskClaim(caller CallerIdentity, input RenewTaskClaimInput) (RenewTaskClaimResult, error) {
	taskID := strings.TrimSpace(input.TaskID)
	if !protocol.IsIdentifier(taskID) {
		return RenewTaskClaimResult{}, invalidRequest("taskId is required")
	}
	if input.ClaimEpoch <= 0 || strings.TrimSpace(input.ClaimToken) == "" {
		return RenewTaskClaimResult{}, invalidRequest("claimEpoch and claimToken are required")
	}
	ttlSeconds, err := resolveTaskClaimTTLSeconds(input.TTLSeconds)
	if err != nil {
		return RenewTaskClaimResult{}, err
	}

	var result RenewTaskClaimResult
	_, err = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		now := service.now()
		nowText := now.UTC().Format(time.RFC3339Nano)
		service.expireTaskClaimsInState(state, mutation, now)

		task, exists := state.Tasks[taskID]
		if !exists {
			return notFound(fmt.Sprintf("task %s does not exist", taskID))
		}
		if task.Status != "running" || !matchesClaimFencing(task.Claim, input.ClaimEpoch, input.ClaimToken) {
			return conflictError("claim_lost", fmt.Sprintf("task %s claim epoch %d is no longer active", task.ID, input.ClaimEpoch))
		}

		renewedClaim := *task.Claim
		renewedClaim.TTLSeconds = ttlSeconds
		renewedClaim.RenewedAt = nowText
		renewedClaim.ExpiresAt = now.Add(time.Duration(ttlSeconds) * time.Second).UTC().Format(time.RFC3339Nano)
		task.Claim = &renewedClaim
		task.UpdatedAt = nowText
		state.Tasks[task.ID] = task

		result = RenewTaskClaimResult{
			Task:  task,
			Claim: renewedClaim,
		}
		return nil
	})
	return result, err
}

func (service *Service) ReleaseTaskClaim(caller CallerIdentity, input ReleaseTaskClaimInput) (ReleaseTaskClaimResult, error) {
	taskID := strings.TrimSpace(input.TaskID)
	if !protocol.IsIdentifier(taskID) {
		return ReleaseTaskClaimResult{}, invalidRequest("taskId is required")
	}
	if input.ClaimEpoch <= 0 || strings.TrimSpace(input.ClaimToken) == "" {
		return ReleaseTaskClaimResult{}, invalidRequest("claimEpoch and claimToken are required")
	}
	reason := strings.TrimSpace(input.Reason)
	if reason != "" && !protocol.IsBoundedString(reason, 1024) {
		return ReleaseTaskClaimResult{}, invalidRequest("reason exceeds 1024 bytes")
	}

	var result ReleaseTaskClaimResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		now := service.now()
		nowText := now.UTC().Format(time.RFC3339Nano)
		service.expireTaskClaimsInState(state, mutation, now)

		task, exists := state.Tasks[taskID]
		if !exists {
			return notFound(fmt.Sprintf("task %s does not exist", taskID))
		}
		if task.Status != "running" || !matchesClaimFencing(task.Claim, input.ClaimEpoch, input.ClaimToken) {
			return conflictError("claim_lost", fmt.Sprintf("task %s claim epoch %d is no longer active", task.ID, input.ClaimEpoch))
		}

		releasedEpoch := task.Claim.Epoch
		releasedClaimerID := task.Claim.ClaimerID
		task.Claim = nil
		task.Status = "pending"
		task.UpdatedAt = nowText
		state.Tasks[task.ID] = task

		recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
		updatedTask := state.Tasks[task.ID]

		detail := map[string]any{
			"claimEpoch": releasedEpoch,
			"claimerId":  releasedClaimerID,
		}
		if reason != "" {
			detail["reason"] = service.redactor.Redact(reason)
		}
		mutation.EmitEvent(protocol.TaskEventEntry{
			ThreadID:   updatedTask.ThreadID,
			Type:       "task.claim_released",
			TaskID:     updatedTask.ID,
			Status:     updatedTask.Status,
			Summary:    fmt.Sprintf("Task %q claim released by %s (epoch %d)", updatedTask.Title, releasedClaimerID, releasedEpoch),
			Detail:     detail,
			OccurredAt: nowText,
		})

		result = ReleaseTaskClaimResult{Task: updatedTask}
		return nil
	})
	if err == nil {
		service.logger.Info(
			"task",
			"claim released",
			activitylog.String("task", result.Task.ID),
			activitylog.String("thread", result.Task.ThreadID),
			activitylog.String("status", result.Task.Status),
		)
	}
	return result, err
}

func (service *Service) expireTaskClaims() {
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		service.expireTaskClaimsInState(state, mutation, service.now())
		return nil
	})
}

func (service *Service) expireTaskClaimsInState(state *store.State, mutation *store.Mutation, now time.Time) int {
	nowText := now.UTC().Format(time.RFC3339Nano)
	taskIDs := make([]string, 0, len(state.Tasks))
	for taskID := range state.Tasks {
		taskIDs = append(taskIDs, taskID)
	}
	slices.Sort(taskIDs)

	expiredCount := 0
	affectedThreads := map[string]bool{}
	for _, taskID := range taskIDs {
		task := state.Tasks[taskID]
		if task.Status != "running" || task.Claim == nil {
			continue
		}
		expiresAt, valid := protocol.ParseTimestamp(task.Claim.ExpiresAt)
		if valid && now.Before(expiresAt) {
			continue
		}
		expiredEpoch := task.Claim.Epoch
		expiredClaimerID := task.Claim.ClaimerID
		task.Claim = nil
		task.Status = "pending"
		task.UpdatedAt = nowText
		state.Tasks[task.ID] = task
		affectedThreads[task.ThreadID] = true
		expiredCount++

		recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
		updatedTask := state.Tasks[task.ID]
		mutation.EmitEvent(protocol.TaskEventEntry{
			ThreadID: updatedTask.ThreadID,
			Type:     "task.claim_expired",
			TaskID:   updatedTask.ID,
			Status:   updatedTask.Status,
			Summary:  fmt.Sprintf("Task %q claim expired (epoch %d, claimer %s)", updatedTask.Title, expiredEpoch, expiredClaimerID),
			Detail: map[string]any{
				"expiredEpoch": expiredEpoch,
				"claimerId":    expiredClaimerID,
			},
			OccurredAt: nowText,
		})
	}
	for threadID := range affectedThreads {
		recomputeThreadTaskReadiness(state, threadID, nowText)
	}
	return expiredCount
}
