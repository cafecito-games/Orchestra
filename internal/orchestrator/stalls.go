package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/activitylog"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

// EvaluateThreadStall deterministically inspects state for threadID and returns a non-nil
// ThreadStallReport when an open thread is quiescent (zero active runs, zero ready/running
// tasks, all non-terminal instances idle or ready) and at least one stall condition holds.
func EvaluateThreadStall(state *store.State, threadID string, now time.Time) *protocol.ThreadStallReport {
	if state == nil || !protocol.IsIdentifier(threadID) {
		return nil
	}
	thread, exists := state.Threads[threadID]
	if !exists || thread.Status != "open" {
		return nil
	}

	for _, runRecord := range state.Runs {
		if runRecord.ThreadID == threadID && protocol.IsActiveRunStatus(runRecord.Status) {
			return nil
		}
	}
	for _, task := range state.Tasks {
		if task.ThreadID == threadID && (task.Status == "ready" || task.Status == "running") {
			return nil
		}
	}
	for _, instance := range state.Instances {
		if instance.ThreadID != threadID {
			continue
		}
		if slices.Contains(protocol.NonTerminalInstanceStatuses, instance.Status) {
			if instance.Status != "idle" && instance.Status != "ready" {
				return nil
			}
		}
	}

	var conditions []protocol.ThreadStallCondition

	conditions = append(conditions, evaluateUnansweredMentionConditions(state, threadID)...)
	if blockedCondition, hasBlocked := evaluateBlockedTasksCondition(state, threadID); hasBlocked {
		conditions = append(conditions, blockedCondition)
	}
	conditions = append(conditions, evaluateCoordinatorUnreadConditions(state, threadID)...)

	if len(conditions) == 0 {
		return nil
	}

	evaluatedAt := protocol.NowTimestamp()
	if !now.IsZero() {
		evaluatedAt = now.UTC().Format(time.RFC3339Nano)
	}

	var reasons []string
	var primaryRoomSequence int64
	var blockedTaskIDs []string
	for _, condition := range conditions {
		if !slices.Contains(reasons, condition.Kind) {
			reasons = append(reasons, condition.Kind)
		}
		if primaryRoomSequence == 0 && condition.RoomSequence > 0 {
			primaryRoomSequence = condition.RoomSequence
		}
		if condition.Kind == protocol.StallKindBlockedTasks {
			blockedTaskIDs = slices.Clone(condition.TaskIDs)
		}
	}

	primaryCondition := conditions[0]
	summary := primaryCondition.Summary
	if len(conditions) > 1 {
		summary = fmt.Sprintf("%s (+%d more stall condition(s))", primaryCondition.Summary, len(conditions)-1)
	}

	roomHeadSequence := state.RoomHeads[threadID].HeadSequence
	eventHeadSequence := state.EventStreams[threadID].Head
	fingerprint := computeThreadStallFingerprint(state, threadID)

	return &protocol.ThreadStallReport{
		ThreadID:          threadID,
		Stalled:           true,
		Kind:              primaryCondition.Kind,
		Reasons:           reasons,
		Summary:           summary,
		Fingerprint:       fingerprint,
		EvaluatedAt:       evaluatedAt,
		RoomHeadSequence:  roomHeadSequence,
		EventHeadSequence: eventHeadSequence,
		RoomSequence:      primaryRoomSequence,
		BlockedTaskIDs:    blockedTaskIDs,
		Conditions:        conditions,
	}
}

// CheckThreadStall evaluates whether threadID is stalled and emits a deduplicated
// thread.stalled event plus an urgent coordinator doorbell when room/task/run activity
// changed since any previous thread.stalled event on that thread.
func (service *Service) CheckThreadStall(threadID string) (*protocol.ThreadStallReport, bool, error) {
	if !protocol.IsIdentifier(threadID) {
		return nil, false, invalidRequest("threadId is required")
	}

	var fastReport *protocol.ThreadStallReport
	alreadyEmitted := false
	readErr := service.store.Read(func(state *store.State) error {
		if _, exists := state.Threads[threadID]; !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}
		fastReport = service.redactStallReport(EvaluateThreadStall(state, threadID, service.now()))
		if fastReport != nil && hasMatchingLastStallEvent(state, threadID, fastReport.Fingerprint) {
			alreadyEmitted = true
		}
		return nil
	})
	if readErr != nil {
		return nil, false, readErr
	}
	if fastReport == nil || alreadyEmitted {
		return fastReport, false, nil
	}

	var committedReport *protocol.ThreadStallReport
	emitted := false
	_, mutateErr := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		if _, exists := state.Threads[threadID]; !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}
		evaluated := service.redactStallReport(EvaluateThreadStall(state, threadID, service.now()))
		if evaluated == nil {
			committedReport = nil
			return nil
		}
		committedReport = evaluated
		if hasMatchingLastStallEvent(state, threadID, evaluated.Fingerprint) {
			return nil
		}

		activitySequence := lastNonStallEventSequence(state, threadID)
		detail := map[string]any{
			"kind":                  evaluated.Kind,
			"reasons":               slices.Clone(evaluated.Reasons),
			"fingerprint":           evaluated.Fingerprint,
			"roomHeadSequence":      evaluated.RoomHeadSequence,
			"activityEventSequence": activitySequence,
			"conditions":            slices.Clone(evaluated.Conditions),
		}
		if evaluated.RoomSequence > 0 {
			detail["roomSequence"] = evaluated.RoomSequence
		}
		if len(evaluated.BlockedTaskIDs) > 0 {
			detail["blockedTaskIds"] = slices.Clone(evaluated.BlockedTaskIDs)
		}

		mutation.EmitEvent(protocol.TaskEventEntry{
			ThreadID:   threadID,
			Type:       "thread.stalled",
			Status:     evaluated.Kind,
			Summary:    evaluated.Summary,
			Detail:     detail,
			OccurredAt: evaluated.EvaluatedAt,
		})
		emitted = true

		summary := evaluated.Summary
		mutation.OnCommit(func() {
			_ = service.emitDoorbellWithOverride(threadID, summary, true)
		})
		return nil
	})
	if mutateErr != nil {
		return nil, false, mutateErr
	}

	if emitted && committedReport != nil {
		service.logger.Warn(
			"thread",
			"stalled",
			activitylog.String("thread", threadID),
			activitylog.String("kind", committedReport.Kind),
			activitylog.String("summary", committedReport.Summary),
		)
	}

	return committedReport, emitted, nil
}

func (service *Service) redactStallReport(report *protocol.ThreadStallReport) *protocol.ThreadStallReport {
	if report == nil || service == nil || service.redactor == nil {
		return report
	}
	cloned := *report
	cloned.Summary = service.redactor.Redact(cloned.Summary)
	cloned.Reasons = slices.Clone(report.Reasons)
	cloned.BlockedTaskIDs = slices.Clone(report.BlockedTaskIDs)
	cloned.Conditions = make([]protocol.ThreadStallCondition, len(report.Conditions))
	for index, condition := range report.Conditions {
		conditionCopy := condition
		conditionCopy.Summary = service.redactor.Redact(conditionCopy.Summary)
		conditionCopy.Handle = service.redactor.Redact(conditionCopy.Handle)
		conditionCopy.TaskIDs = slices.Clone(conditionCopy.TaskIDs)
		cloned.Conditions[index] = conditionCopy
	}
	return &cloned
}

func evaluateUnansweredMentionConditions(state *store.State, threadID string) []protocol.ThreadStallCondition {
	roomMessages := state.RoomMessages[threadID]
	if len(roomMessages) == 0 {
		return nil
	}

	var instanceIDs []string
	for instanceID, instance := range state.Instances {
		if instance.ThreadID == threadID && instance.Status != "released" {
			instanceIDs = append(instanceIDs, instanceID)
		}
	}
	slices.Sort(instanceIDs)

	var conditions []protocol.ThreadStallCondition
	for _, instanceID := range instanceIDs {
		instance := state.Instances[instanceID]
		lastPostSequence := effectiveInstanceLastPostSequence(state, roomMessages, instance)
		primaryHandle, candidateHandles := instanceHandlesInState(state, instance)

		var latestMentionMessage *protocol.RoomMessage
		var wakeTaskID string
		var wakeRunID string
		var wakeOutcome string

		for messageIndex := range roomMessages {
			message := roomMessages[messageIndex]
			if message.Sequence <= lastPostSequence {
				continue
			}
			if authorInstanceIDForRoomMessage(state, message) == instanceID {
				continue
			}
			parsedMentions := parsedMentionsForRoomMessage(message)
			matchedHandle := matchesAnyHandle(candidateHandles, parsedMentions.Handles)
			matchedChannel := false
			for _, channel := range parsedMentions.Channels {
				if protocol.InstanceSubscribesToRoomChannel(instance.RoomChannels, channel) {
					matchedChannel = true
					break
				}
			}
			if !matchedHandle && !matchedChannel && !parsedMentions.All {
				continue
			}
			taskID, runID, outcome, hasTerminalAttempt := findTerminalWakeAttemptForMention(
				state,
				threadID,
				instanceID,
				message,
				matchedHandle,
			)
			if !hasTerminalAttempt {
				continue
			}
			messageCopy := message
			latestMentionMessage = &messageCopy
			wakeTaskID = taskID
			wakeRunID = runID
			wakeOutcome = outcome
		}

		if latestMentionMessage == nil {
			continue
		}

		displayHandle := primaryHandle
		if displayHandle == "" {
			displayHandle = instanceID
		}
		summary := fmt.Sprintf(
			"@%s was mentioned in room #%d and wake run %s without posting a room reply",
			displayHandle,
			latestMentionMessage.Sequence,
			wakeOutcome,
		)
		condition := protocol.ThreadStallCondition{
			Kind:         protocol.StallKindUnansweredMention,
			Summary:      summary,
			RoomSequence: latestMentionMessage.Sequence,
			Handle:       primaryHandle,
			InstanceID:   instanceID,
			TaskID:       wakeTaskID,
			RunID:        wakeRunID,
		}
		if wakeTaskID != "" {
			condition.TaskIDs = []string{wakeTaskID}
		}
		conditions = append(conditions, condition)
	}

	return conditions
}

func evaluateBlockedTasksCondition(state *store.State, threadID string) (protocol.ThreadStallCondition, bool) {
	var blockedTaskIDs []string
	var explicitBlockerReason string
	for taskID, task := range state.Tasks {
		if task.ThreadID != threadID {
			continue
		}
		if task.Status == "pending" || task.Status == "blocked" {
			blockedTaskIDs = append(blockedTaskIDs, taskID)
			if explicitBlockerReason == "" && task.BlockerReason != "" {
				explicitBlockerReason = task.BlockerReason
			}
		}
	}
	if len(blockedTaskIDs) == 0 {
		return protocol.ThreadStallCondition{}, false
	}
	slices.Sort(blockedTaskIDs)
	if explicitBlockerReason == "" {
		firstTask := state.Tasks[blockedTaskIDs[0]]
		explicitBlockerReason = firstTask.BlockerReason
	}

	summary := fmt.Sprintf("%d task(s) remain pending or blocked with no runnable upstream tasks", len(blockedTaskIDs))
	if explicitBlockerReason != "" {
		summary = fmt.Sprintf(
			"%d task(s) blocked with no runnable upstream tasks (%s)",
			len(blockedTaskIDs),
			protocol.TruncateUTF8Head(explicitBlockerReason, 160),
		)
	}

	return protocol.ThreadStallCondition{
		Kind:    protocol.StallKindBlockedTasks,
		Summary: summary,
		TaskID:  blockedTaskIDs[0],
		TaskIDs: blockedTaskIDs,
	}, true
}

func evaluateCoordinatorUnreadConditions(state *store.State, threadID string) []protocol.ThreadStallCondition {
	attachment, hasAttachment := state.ActiveAttachmentForThread(threadID)
	if !hasAttachment {
		return nil
	}

	var conditions []protocol.ThreadStallCondition

	var terminalTaskIDs []string
	events := state.ThreadEvents[threadID]
	for _, entry := range events {
		if entry.Sequence <= attachment.AcknowledgedSeq {
			continue
		}
		if entry.Type == "task.updated" && entry.TaskID != "" && protocol.IsTerminalTaskStatus(entry.Status) {
			if currentTask, exists := state.Tasks[entry.TaskID]; exists && protocol.IsTerminalTaskStatus(currentTask.Status) {
				if !slices.Contains(terminalTaskIDs, entry.TaskID) {
					terminalTaskIDs = append(terminalTaskIDs, entry.TaskID)
				}
			}
		}
	}
	if len(events) == 0 && attachment.AcknowledgedSeq == 0 {
		for taskID, task := range state.Tasks {
			if task.ThreadID != threadID || !protocol.IsTerminalTaskStatus(task.Status) {
				continue
			}
			if attachment.CreatedAt == "" || task.UpdatedAt >= attachment.CreatedAt || task.FinishedAt >= attachment.CreatedAt {
				terminalTaskIDs = append(terminalTaskIDs, taskID)
			}
		}
	}
	if len(terminalTaskIDs) > 0 {
		slices.Sort(terminalTaskIDs)
		conditions = append(conditions, protocol.ThreadStallCondition{
			Kind: protocol.StallKindUnaddressedResult,
			Summary: fmt.Sprintf(
				"%d terminal task update(s) awaiting coordinator acknowledgment since event #%d",
				len(terminalTaskIDs),
				attachment.AcknowledgedSeq,
			),
			TaskID:  terminalTaskIDs[0],
			TaskIDs: terminalTaskIDs,
		})
	}

	roomMessages := state.RoomMessages[threadID]
	var unreadCount int
	var latestUnreadSequence int64
	for _, entry := range roomMessages {
		if entry.Sequence <= attachment.LastReadRoomSequence {
			continue
		}
		if entry.Author.Kind == "orchestrator" && attachment.ClientID != "" && entry.Author.ClientID == attachment.ClientID {
			continue
		}
		unreadCount++
		latestUnreadSequence = entry.Sequence
	}
	if unreadCount > 0 {
		conditions = append(conditions, protocol.ThreadStallCondition{
			Kind:         protocol.StallKindUnreadByCoordinator,
			RoomSequence: latestUnreadSequence,
			Summary: fmt.Sprintf(
				"%d unread room message(s) after coordinator cursor #%d (through #%d)",
				unreadCount,
				attachment.LastReadRoomSequence,
				latestUnreadSequence,
			),
		})
	}

	return conditions
}

func effectiveInstanceLastPostSequence(
	state *store.State,
	roomMessages []protocol.RoomMessage,
	instance protocol.AgentInstance,
) int64 {
	lastPost := instance.LastPostRoomSequence
	for _, entry := range roomMessages {
		if entry.Sequence <= lastPost {
			continue
		}
		if authorInstanceIDForRoomMessage(state, entry) == instance.ID {
			lastPost = entry.Sequence
		}
	}
	return lastPost
}

func authorInstanceIDForRoomMessage(state *store.State, message protocol.RoomMessage) string {
	if message.Author.InstanceID != "" {
		return message.Author.InstanceID
	}
	if message.Author.RunID != "" {
		if runRecord, exists := state.Runs[message.Author.RunID]; exists {
			return runRecord.InstanceID
		}
	}
	if message.Author.TaskID != "" {
		if task, exists := state.Tasks[message.Author.TaskID]; exists && task.Assignment != nil {
			return task.Assignment.InstanceID
		}
	}
	return ""
}

func parsedMentionsForRoomMessage(message protocol.RoomMessage) protocol.ParsedRoomMentions {
	parsed, err := protocol.ParseRoomMentions(message.Body, message.Mentions)
	if err != nil {
		parsed, _ = protocol.ParseRoomMentions("", message.Mentions)
	}
	return parsed
}

func matchesAnyHandle(candidateHandles []string, mentionedHandles []string) bool {
	for _, candidateHandle := range candidateHandles {
		for _, mentionedHandle := range mentionedHandles {
			if protocol.MatchesHandleMention(candidateHandle, mentionedHandle) {
				return true
			}
		}
	}
	return false
}

func findTerminalWakeAttemptForMention(
	state *store.State,
	threadID string,
	instanceID string,
	message protocol.RoomMessage,
	matchedHandle bool,
) (taskID string, runID string, outcome string, found bool) {
	expectedSubmissionID := fmt.Sprintf("room-seq-%d", message.Sequence)

	var matchedTasks []protocol.Task
	for _, task := range state.Tasks {
		if task.ThreadID != threadID || !protocol.IsTerminalTaskStatus(task.Status) {
			continue
		}
		targetsInstance := task.PlacementInstanceID == instanceID ||
			task.PreferredInstanceID == instanceID ||
			(task.Assignment != nil && task.Assignment.InstanceID == instanceID)
		if !targetsInstance {
			continue
		}
		isExplicitRoomWake := task.Origin.Kind == "room-wake" &&
			(task.Origin.SubmissionID == expectedSubmissionID || (message.CreatedAt != "" && task.CreatedAt >= message.CreatedAt))
		isNudgedForMessage := task.RequiredRoomSequence >= message.Sequence
		if isExplicitRoomWake || isNudgedForMessage {
			matchedTasks = append(matchedTasks, task)
		}
	}
	slices.SortFunc(matchedTasks, func(left, right protocol.Task) int {
		if left.UpdatedAt != right.UpdatedAt {
			return strings.Compare(left.UpdatedAt, right.UpdatedAt)
		}
		return strings.Compare(left.ID, right.ID)
	})

	if len(matchedTasks) > 0 {
		chosenTask := matchedTasks[len(matchedTasks)-1]
		taskID = chosenTask.ID
		outcome = chosenTask.Status
		for candidateRunID, runRecord := range state.Runs {
			if runRecord.ThreadID == threadID && runRecord.TaskID != nil && *runRecord.TaskID == chosenTask.ID &&
				protocol.IsTerminalRunStatus(runRecord.Status) {
				if runID == "" || candidateRunID > runID {
					runID = candidateRunID
					outcome = runRecord.Status
				}
			}
		}
		return taskID, runID, outcome, true
	}

	if !matchedHandle {
		return "", "", "", false
	}

	var matchedRuns []protocol.Run
	for _, runRecord := range state.Runs {
		if runRecord.ThreadID != threadID || runRecord.InstanceID != instanceID || !protocol.IsTerminalRunStatus(runRecord.Status) {
			continue
		}
		startedOrCreatedAfter := message.CreatedAt == "" || runRecord.CreatedAt >= message.CreatedAt ||
			(runRecord.FinishedAt != nil && *runRecord.FinishedAt >= message.CreatedAt)
		if startedOrCreatedAfter {
			matchedRuns = append(matchedRuns, runRecord)
		}
	}
	slices.SortFunc(matchedRuns, func(left, right protocol.Run) int {
		if left.CreatedAt != right.CreatedAt {
			return strings.Compare(left.CreatedAt, right.CreatedAt)
		}
		return strings.Compare(left.ID, right.ID)
	})
	if len(matchedRuns) > 0 {
		chosenRun := matchedRuns[len(matchedRuns)-1]
		linkedTaskID := ""
		if chosenRun.TaskID != nil {
			linkedTaskID = *chosenRun.TaskID
		}
		return linkedTaskID, chosenRun.ID, chosenRun.Status, true
	}

	return "", "", "", false
}

func lastNonStallEventSequence(state *store.State, threadID string) int64 {
	events := state.ThreadEvents[threadID]
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type != "thread.stalled" {
			return events[index].Sequence
		}
	}
	return 0
}

func computeThreadStallFingerprint(state *store.State, threadID string) string {
	roomHeadSequence := state.RoomHeads[threadID].HeadSequence
	activitySequence := lastNonStallEventSequence(state, threadID)

	var taskTokens []string
	for taskID, task := range state.Tasks {
		if task.ThreadID != threadID {
			continue
		}
		taskTokens = append(
			taskTokens,
			fmt.Sprintf("%s:%s:%d:%d:%s", taskID, task.Status, task.AttemptCount, task.RequiredRoomSequence, task.FinishedAt),
		)
	}
	slices.Sort(taskTokens)

	var runTokens []string
	for runID, runRecord := range state.Runs {
		if runRecord.ThreadID != threadID {
			continue
		}
		finishedAt := ""
		if runRecord.FinishedAt != nil {
			finishedAt = *runRecord.FinishedAt
		}
		runTokens = append(runTokens, fmt.Sprintf("%s:%s:%s", runID, runRecord.Status, finishedAt))
	}
	slices.Sort(runTokens)

	var builder strings.Builder
	fmt.Fprintf(&builder, "room=%d;event=%d;tasks=%s;runs=%s",
		roomHeadSequence,
		activitySequence,
		strings.Join(taskTokens, ","),
		strings.Join(runTokens, ","),
	)
	digest := sha256.Sum256([]byte(builder.String()))
	return hex.EncodeToString(digest[:12])
}

func hasMatchingLastStallEvent(state *store.State, threadID string, fingerprint string) bool {
	events := state.ThreadEvents[threadID]
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Type != "thread.stalled" {
			continue
		}
		if events[index].Detail == nil {
			return false
		}
		previousFingerprint, ok := events[index].Detail["fingerprint"].(string)
		return ok && previousFingerprint == fingerprint
	}
	return false
}
