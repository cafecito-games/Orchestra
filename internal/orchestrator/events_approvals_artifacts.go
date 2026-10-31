package orchestrator

import (
	"cmp"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

const (
	approvalExpiryDuration = 9 * time.Minute
	artifactGrantTTL       = 15 * time.Minute
	maximumProjectedTools  = 64
	maximumProjectedDiffs  = 32
	maximumProjectedTerms  = 16
	maximumProjectedWarns  = 32
	maximumTranscriptItems = 400
	boundedProjectionBytes = 64 * 1024
	replayWindowEntries    = 256
)

type ListApprovalsInput struct {
	ThreadID string `json:"threadId,omitempty"`
	Status   string `json:"status,omitempty"`
}

type ResolveApprovalInput struct {
	ApprovalID string `json:"approvalId"`
	Status     string `json:"status,omitempty"`
	OptionID   string `json:"optionId,omitempty"`
}

type PostArtifactInput struct {
	ThreadID     string `json:"threadId,omitempty"`
	TaskID       string `json:"taskId,omitempty"`
	Name         string `json:"name"`
	RelativePath string `json:"relativePath,omitempty"`
	Kind         string `json:"kind,omitempty"`
	MediaType    string `json:"mediaType,omitempty"`
	SizeBytes    int64  `json:"sizeBytes"`
	SHA256       string `json:"sha256"`
	Summary      string `json:"summary,omitempty"`
}

type PostArtifactResult struct {
	Artifact protocol.Artifact            `json:"artifact"`
	Upload   protocol.ArtifactUploadGrant `json:"upload"`
}

func (service *Service) handleRunStarted(nodeID string, message protocol.SlaveControlMessage) {
	if !protocol.IsIdentifier(message.RunID) {
		return
	}
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		runRecord, exists := state.Runs[message.RunID]
		if !exists || runRecord.NodeID != nodeID {
			return nil
		}
		nowText := service.nowUTC()
		if runRecord.Status == "queued" {
			runRecord.Status = "running"
			runRecord.StartedAt = &nowText
		}
		if message.Transport != nil && message.Transport.Validate() == nil {
			selectionCopy := *message.Transport
			runRecord.TransportSelection = &selectionCopy
			runRecord.Transport = selectionCopy.SelectedTransport
		}
		if message.ProviderSessionID != "" && protocol.IsIdentifier(message.ProviderSessionID) && !protocol.LooksSecretLike(message.ProviderSessionID) {
			sessionID := message.ProviderSessionID
			runRecord.ProviderSessionID = &sessionID
		}
		state.Runs[runRecord.ID] = runRecord
		return nil
	})
}

func (service *Service) handleRunOutput(nodeID string, message protocol.SlaveControlMessage) {
	if !protocol.IsIdentifier(message.RunID) || message.Chunk == nil || *message.Chunk == "" {
		return
	}
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		runRecord, exists := state.Runs[message.RunID]
		if !exists || runRecord.NodeID != nodeID || protocol.IsTerminalRunStatus(runRecord.Status) {
			return nil
		}
		redacted := service.redactor.Redact(*message.Chunk)
		combined := runRecord.Output + redacted
		runRecord.Output = protocol.TruncateUTF8Tail(combined, protocol.TextMaximumBytes)
		state.Runs[runRecord.ID] = runRecord
		return nil
	})
}

func (service *Service) handleRunCompleted(nodeID string, message protocol.SlaveControlMessage) {
	if !protocol.IsIdentifier(message.RunID) {
		return
	}
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		runRecord, exists := state.Runs[message.RunID]
		if !exists || runRecord.NodeID != nodeID || protocol.IsTerminalRunStatus(runRecord.Status) {
			return nil
		}
		nowText := service.nowUTC()
		runRecord.Status = "completed"
		if runRecord.StartedAt == nil {
			runRecord.StartedAt = &nowText
		}
		runRecord.FinishedAt = &nowText
		if message.Output != nil {
			runRecord.Output = protocol.TruncateUTF8Head(service.redactor.Redact(*message.Output), protocol.TextMaximumBytes)
		}
		if message.ProviderSessionID != "" && protocol.IsIdentifier(message.ProviderSessionID) && !protocol.LooksSecretLike(message.ProviderSessionID) {
			sessionID := message.ProviderSessionID
			runRecord.ProviderSessionID = &sessionID
		}
		state.Runs[runRecord.ID] = runRecord

		if activity, hasActivity := state.RunActivities[runRecord.ID]; hasActivity {
			activity.StreamStatus = "completed"
			activity.UpdatedAt = nowText
			state.RunActivities[runRecord.ID] = activity
		}

		service.settleRunApprovalsInState(state, runRecord.ID, "cancelled", nowText)
		service.releaseInstanceAfterRunInState(state, runRecord.InstanceID, nowText)

		if runRecord.TaskID != nil {
			if task, taskExists := state.Tasks[*runRecord.TaskID]; taskExists && !protocol.IsTerminalTaskStatus(task.Status) {
				if task.Status != "blocked" {
					task.Status = "completed"
					if task.Summary == "" && runRecord.Output != "" {
						task.Summary = protocol.TruncateUTF8Head(runRecord.Output, 2000)
					}
					task.FinishedAt = nowText
				}
				task.UpdatedAt = nowText
				state.Tasks[task.ID] = task
				recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
			}
		}

		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
		return nil
	})
}

func (service *Service) handleRunFailed(nodeID string, message protocol.SlaveControlMessage, retryEligible bool) {
	if !protocol.IsIdentifier(message.RunID) {
		return
	}
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		runRecord, exists := state.Runs[message.RunID]
		if !exists || runRecord.NodeID != nodeID || protocol.IsTerminalRunStatus(runRecord.Status) {
			return nil
		}
		nowText := service.nowUTC()
		errorMessage := "worker run failed"
		if message.Error != nil && *message.Error != "" {
			errorMessage = protocol.BoundedDiagnostic(service.redactor.Redact(*message.Error))
		}
		runRecord.Status = "failed"
		runRecord.Error = &errorMessage
		runRecord.FinishedAt = &nowText
		if message.ProviderSessionID != "" && protocol.IsIdentifier(message.ProviderSessionID) && !protocol.LooksSecretLike(message.ProviderSessionID) {
			sessionID := message.ProviderSessionID
			runRecord.ProviderSessionID = &sessionID
		}
		state.Runs[runRecord.ID] = runRecord

		if activity, hasActivity := state.RunActivities[runRecord.ID]; hasActivity {
			activity.StreamStatus = "failed"
			activity.StreamFailure = errorMessage
			activity.UpdatedAt = nowText
			state.RunActivities[runRecord.ID] = activity
		}

		service.settleRunApprovalsInState(state, runRecord.ID, "cancelled", nowText)
		service.releaseInstanceAfterRunInState(state, runRecord.InstanceID, nowText)

		if runRecord.TaskID != nil {
			if task, taskExists := state.Tasks[*runRecord.TaskID]; taskExists && !protocol.IsTerminalTaskStatus(task.Status) {
				if retryEligible && task.AttemptCount < task.MaxAttempts {
					task.Status = "ready"
					task.Assignment = nil
					task.Summary = fmt.Sprintf("retrying after attempt %d failed: %s", task.AttemptCount, errorMessage)
					task.UpdatedAt = nowText
				} else {
					task.Status = "failed"
					task.Summary = errorMessage
					task.UpdatedAt = nowText
					task.FinishedAt = nowText
				}
				state.Tasks[task.ID] = task
				recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
			}
		}

		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
		return nil
	})
}

func (service *Service) handleRunCancelled(nodeID string, message protocol.SlaveControlMessage) {
	if !protocol.IsIdentifier(message.RunID) {
		return
	}
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		runRecord, exists := state.Runs[message.RunID]
		if !exists || runRecord.NodeID != nodeID {
			return nil
		}
		nowText := service.nowUTC()
		if !protocol.IsTerminalRunStatus(runRecord.Status) {
			runRecord.Status = "cancelled"
			runRecord.FinishedAt = &nowText
			if message.Output != nil && *message.Output != "" {
				runRecord.Output = protocol.TruncateUTF8Head(service.redactor.Redact(*message.Output), protocol.TextMaximumBytes)
			}
			state.Runs[runRecord.ID] = runRecord
		}
		service.settleRunApprovalsInState(state, runRecord.ID, "cancelled", nowText)
		service.releaseInstanceAfterRunInState(state, runRecord.InstanceID, nowText)

		if runRecord.TaskID != nil {
			if task, taskExists := state.Tasks[*runRecord.TaskID]; taskExists && !protocol.IsTerminalTaskStatus(task.Status) {
				task.Status = "cancelled"
				task.UpdatedAt = nowText
				task.FinishedAt = nowText
				state.Tasks[task.ID] = task
				recomputeThreadTaskReadiness(state, task.ThreadID, nowText)
			}
		}

		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
		return nil
	})
}

func (service *Service) settleRunApprovalsInState(
	state *store.State,
	runID string,
	terminalStatus string,
	nowText string,
) {
	for approvalID, approval := range state.Approvals {
		if approval.RunID == runID && approval.Status == "pending" {
			approval.Status = terminalStatus
			approval.DeliveryState = "not-applied"
			approval.ResolvedAt = nowText
			approval.UpdatedAt = nowText
			state.Approvals[approvalID] = approval
		}
	}
}

func (service *Service) releaseInstanceAfterRunInState(
	state *store.State,
	instanceID string,
	nowText string,
) {
	instance, exists := state.Instances[instanceID]
	if !exists {
		return
	}
	if instance.Status == "busy" {
		instance.Status = "idle"
		instance.Lease.ExpiresAt = extendLeaseTimestamp(nowText, instance.Lease.IdleTimeoutSeconds)
		instance.UpdatedAt = nowText
		state.Instances[instanceID] = instance
	}
}

func (service *Service) handleHarnessEvent(nodeID string, message protocol.SlaveControlMessage) {
	if message.Event == nil {
		return
	}
	event := *message.Event
	if event.RunID == "" {
		event.RunID = message.RunID
	}
	if err := event.Validate(); err != nil {
		return
	}

	eventDigest := canonicalDigest(event)
	if service.isReplayHarnessEvent(event.RunID, event.Sequence, eventDigest) {
		return
	}

	event = service.redactHarnessEvent(event)

	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		runRecord, exists := state.Runs[event.RunID]
		if !exists || runRecord.NodeID != nodeID {
			return nil
		}

		nowText := service.nowUTC()
		activity, hasActivity := state.RunActivities[runRecord.ID]
		if !hasActivity {
			activity = protocol.RunActivity{
				RunID:        runRecord.ID,
				NodeID:       nodeID,
				ThreadID:     runRecord.ThreadID,
				StreamStatus: "streaming",
				Plan:         []protocol.PlanEntry{},
				ToolCalls:    []protocol.ProjectedToolCall{},
				Diffs:        []protocol.ProjectedDiff{},
				Terminals:    []protocol.ProjectedTerminal{},
				Warnings:     []protocol.ProjectedWarning{},
				UpdatedAt:    nowText,
			}
		}
		if event.Sequence <= activity.LastSequence {
			return nil
		}
		activity.LastSequence = event.Sequence
		activity.AcceptedEvents++
		activity.UpdatedAt = event.At

		transcript, hasTranscript := state.RunTranscripts[runRecord.ID]
		if !hasTranscript {
			transcript = protocol.RunTranscript{
				RunID:     runRecord.ID,
				ThreadID:  runRecord.ThreadID,
				Entries:   []protocol.RunTranscriptEntry{},
				UpdatedAt: nowText,
			}
		}
		transcript.LastSequence = event.Sequence
		transcript.UpdatedAt = event.At

		projectHarnessEventIntoActivityAndTranscript(&activity, &transcript, event)
		state.RunActivities[runRecord.ID] = activity
		state.RunTranscripts[runRecord.ID] = transcript

		switch event.Type {
		case "permission.requested":
			node := state.Nodes[nodeID]
			if nodeHasBypassPolicy(node, runRecord.HarnessID) {
				return nil
			}
			if _, alreadyExists := state.Approvals[event.ApprovalID]; !alreadyExists {
				taskID := ""
				if runRecord.TaskID != nil {
					taskID = *runRecord.TaskID
				}
				expiresAt := service.now().Add(approvalExpiryDuration).UTC().Format(time.RFC3339Nano)
				state.Approvals[event.ApprovalID] = protocol.ApprovalRequest{
					ID:            event.ApprovalID,
					RunID:         runRecord.ID,
					ThreadID:      runRecord.ThreadID,
					TaskID:        taskID,
					InstanceID:    runRecord.InstanceID,
					AllocationID:  runRecord.AllocationID,
					NodeID:        nodeID,
					ToolCallID:    event.ToolCallID,
					Title:         event.Title,
					Detail:        event.Detail,
					Options:       slices.Clone(event.Options),
					Status:        "pending",
					DeliveryState: "pending",
					RequestedAt:   event.At,
					ExpiresAt:     expiresAt,
					UpdatedAt:     event.At,
				}
			}
		case "permission.resolved":
			if approval, found := state.Approvals[event.ApprovalID]; found {
				if approval.Status == "pending" && event.Status != "" && event.Status != "pending" {
					approval.Status = event.Status
					approval.SelectedOptionID = event.SelectedOptionID
					approval.ResolvedAt = event.At
				}
				approval.DeliveryState = "applied"
				approval.UpdatedAt = event.At
				state.Approvals[event.ApprovalID] = approval
			}
		}
		return nil
	})
}

func nodeHasBypassPolicy(node protocol.ComputeNode, harnessID string) bool {
	for _, profile := range node.Harnesses {
		if profile.ID == harnessID && profile.ApprovalPolicy == protocol.ApprovalPolicyBypass {
			return true
		}
	}
	return false
}

func (service *Service) redactHarnessEvent(event protocol.HarnessEvent) protocol.HarnessEvent {
	event.Text = service.redactor.Redact(event.Text)
	event.Title = service.redactor.Redact(event.Title)
	event.Detail = service.redactor.Redact(event.Detail)
	event.Message = service.redactor.Redact(event.Message)
	event.OldText = service.redactor.Redact(event.OldText)
	event.NewText = service.redactor.Redact(event.NewText)
	for index := range event.Entries {
		event.Entries[index].Content = service.redactor.Redact(event.Entries[index].Content)
	}
	for index := range event.Options {
		event.Options[index].Label = service.redactor.Redact(event.Options[index].Label)
	}
	return event
}

func projectHarnessEventIntoActivityAndTranscript(
	activity *protocol.RunActivity,
	transcript *protocol.RunTranscript,
	event protocol.HarnessEvent,
) {
	switch event.Type {
	case "message.delta":
		appendBoundedText(&activity.Message, event.Text, boundedProjectionBytes)
		activity.Summary = protocol.TruncateUTF8Tail(activity.Message.Text, 240)
		appendOrCoalesceTextTranscript(transcript, "message", event)
	case "thought.delta":
		appendBoundedText(&activity.Thought, event.Text, boundedProjectionBytes)
		appendOrCoalesceTextTranscript(transcript, "thought", event)
	case "plan.updated":
		activity.Plan = slices.Clone(event.Entries)
		appendTranscriptEntry(transcript, protocol.RunTranscriptEntry{
			Kind:      "plan",
			ID:        event.Sequence,
			At:        event.At,
			UpdatedAt: event.At,
			Entries:   slices.Clone(event.Entries),
		})
	case "tool.call":
		updatedExisting := false
		for index := range activity.ToolCalls {
			if activity.ToolCalls[index].ToolCallID == event.ToolCallID {
				activity.ToolCalls[index] = protocol.ProjectedToolCall{
					ToolCallID: event.ToolCallID,
					Status:     event.Status,
					Kind:       event.Kind,
					Title:      event.Title,
					Detail:     event.Detail,
					UpdatedAt:  event.At,
				}
				updatedExisting = true
				break
			}
		}
		if !updatedExisting {
			if len(activity.ToolCalls) >= maximumProjectedTools {
				activity.Omitted.ToolCalls++
				activity.ToolCalls = activity.ToolCalls[1:]
			}
			activity.ToolCalls = append(activity.ToolCalls, protocol.ProjectedToolCall{
				ToolCallID: event.ToolCallID,
				Status:     event.Status,
				Kind:       event.Kind,
				Title:      event.Title,
				Detail:     event.Detail,
				UpdatedAt:  event.At,
			})
		}
		activity.Summary = fmt.Sprintf("Tool %s (%s)", event.Title, event.Status)
		appendTranscriptEntry(transcript, protocol.RunTranscriptEntry{
			Kind:       "tool",
			ID:         event.Sequence,
			At:         event.At,
			UpdatedAt:  event.At,
			ToolCallID: event.ToolCallID,
			Status:     event.Status,
			ToolKind:   event.Kind,
			Title:      event.Title,
			Detail:     event.Detail,
		})
	case "diff":
		if len(activity.Diffs) >= maximumProjectedDiffs {
			activity.Omitted.Diffs++
			activity.Diffs = activity.Diffs[1:]
		}
		activity.Diffs = append(activity.Diffs, protocol.ProjectedDiff{
			Path:       event.Path,
			ToolCallID: event.ToolCallID,
			OldText:    event.OldText,
			NewText:    event.NewText,
			Sequence:   event.Sequence,
			At:         event.At,
		})
		appendTranscriptEntry(transcript, protocol.RunTranscriptEntry{
			Kind:       "diff",
			ID:         event.Sequence,
			At:         event.At,
			UpdatedAt:  event.At,
			ToolCallID: event.ToolCallID,
			Path:       event.Path,
			OldText:    event.OldText,
			NewText:    event.NewText,
		})
	case "terminal.output":
		foundTerm := false
		for index := range activity.Terminals {
			if activity.Terminals[index].TerminalID == event.TerminalID {
				if event.Stream == "stderr" {
					appendBoundedText(&activity.Terminals[index].Stderr, event.Text, boundedProjectionBytes)
				} else {
					appendBoundedText(&activity.Terminals[index].Stdout, event.Text, boundedProjectionBytes)
				}
				activity.Terminals[index].UpdatedAt = event.At
				foundTerm = true
				break
			}
		}
		if !foundTerm {
			if len(activity.Terminals) >= maximumProjectedTerms {
				activity.Omitted.Terminals++
				activity.Terminals = activity.Terminals[1:]
			}
			term := protocol.ProjectedTerminal{
				TerminalID: event.TerminalID,
				UpdatedAt:  event.At,
			}
			if event.Stream == "stderr" {
				appendBoundedText(&term.Stderr, event.Text, boundedProjectionBytes)
			} else {
				appendBoundedText(&term.Stdout, event.Text, boundedProjectionBytes)
			}
			activity.Terminals = append(activity.Terminals, term)
		}
		appendTranscriptEntry(transcript, protocol.RunTranscriptEntry{
			Kind:       "terminal",
			ID:         event.Sequence,
			At:         event.At,
			UpdatedAt:  event.At,
			TerminalID: event.TerminalID,
			Stream:     event.Stream,
			Text:       event.Text,
		})
	case "usage":
		activity.Usage = &protocol.ProjectedUsage{
			InputTokens:       event.InputTokens,
			OutputTokens:      event.OutputTokens,
			CachedInputTokens: event.CachedInputTokens,
			CostUSD:           event.CostUSD,
			UpdatedAt:         event.At,
		}
	case "permission.requested":
		activity.Summary = "Awaiting approval: " + event.Title
		appendTranscriptEntry(transcript, protocol.RunTranscriptEntry{
			Kind:       "approval",
			ID:         event.Sequence,
			At:         event.At,
			UpdatedAt:  event.At,
			ApprovalID: event.ApprovalID,
			Status:     "pending",
			Title:      event.Title,
			Detail:     event.Detail,
		})
	case "permission.resolved":
		appendTranscriptEntry(transcript, protocol.RunTranscriptEntry{
			Kind:       "approval",
			ID:         event.Sequence,
			At:         event.At,
			UpdatedAt:  event.At,
			ApprovalID: event.ApprovalID,
			Status:     event.Status,
		})
	case "warning":
		if len(activity.Warnings) >= maximumProjectedWarns {
			activity.Omitted.Warnings++
			activity.Warnings = activity.Warnings[1:]
		}
		activity.Warnings = append(activity.Warnings, protocol.ProjectedWarning{
			Code:     event.Code,
			Message:  event.Message,
			Sequence: event.Sequence,
			At:       event.At,
		})
		appendTranscriptEntry(transcript, protocol.RunTranscriptEntry{
			Kind:      "warning",
			ID:        event.Sequence,
			At:        event.At,
			UpdatedAt: event.At,
			Code:      event.Code,
			Message:   event.Message,
		})
	case "unknown":
		activity.UnknownEvents++
	}
}

func appendBoundedText(target *protocol.BoundedText, delta string, limit int) {
	if delta == "" {
		return
	}
	combined := target.Text + delta
	if len(combined) <= limit {
		target.Text = combined
		return
	}
	excess := len(combined) - limit
	trimmed := protocol.TruncateUTF8Tail(combined, limit)
	target.TruncatedBytes += len(combined) - len(trimmed)
	_ = excess
	target.Text = trimmed
}

func appendOrCoalesceTextTranscript(
	transcript *protocol.RunTranscript,
	kind string,
	event protocol.HarnessEvent,
) {
	if len(transcript.Entries) > 0 {
		lastIndex := len(transcript.Entries) - 1
		if transcript.Entries[lastIndex].Kind == kind {
			combined := transcript.Entries[lastIndex].Text + event.Text
			if len(combined) <= boundedProjectionBytes {
				transcript.Entries[lastIndex].Text = combined
			} else {
				trimmed := protocol.TruncateUTF8Tail(combined, boundedProjectionBytes)
				transcript.Entries[lastIndex].TruncatedBytes += len(combined) - len(trimmed)
				transcript.Entries[lastIndex].Text = trimmed
			}
			transcript.Entries[lastIndex].UpdatedAt = event.At
			return
		}
	}
	appendTranscriptEntry(transcript, protocol.RunTranscriptEntry{
		Kind:      kind,
		ID:        event.Sequence,
		At:        event.At,
		UpdatedAt: event.At,
		Text:      event.Text,
	})
}

func appendTranscriptEntry(transcript *protocol.RunTranscript, entry protocol.RunTranscriptEntry) {
	if len(transcript.Entries) >= maximumTranscriptItems {
		transcript.OmittedEntries++
		transcript.Entries = transcript.Entries[1:]
	}
	transcript.Entries = append(transcript.Entries, entry)
}

func (service *Service) handleSessionBinding(nodeID string, message protocol.SlaveControlMessage) {
	if message.Binding == nil || message.Binding.Validate() != nil {
		return
	}
	update := *message.Binding
	if protocol.LooksSecretLike(update.ProviderSessionID) {
		return
	}

	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		runRecord, exists := state.Runs[message.RunID]
		if !exists || runRecord.NodeID != nodeID {
			return nil
		}
		nowText := service.nowUTC()

		var existingID string
		if update.BindingID != "" {
			if candidate, found := state.SessionBindings[update.BindingID]; found &&
				candidate.NodeID == nodeID &&
				candidate.Transport == update.Transport {
				existingID = candidate.ID
			}
		}
		if existingID == "" {
			for bindingID, candidate := range state.SessionBindings {
				if candidate.NodeID == nodeID &&
					candidate.InstanceID == runRecord.InstanceID &&
					candidate.AllocationID == runRecord.AllocationID &&
					candidate.Transport == update.Transport &&
					candidate.ProviderSessionID == update.ProviderSessionID &&
					!protocol.IsTerminalSessionBindingStatus(candidate.Status) {
					existingID = bindingID
					break
				}
			}
		}

		if existingID != "" {
			binding := state.SessionBindings[existingID]
			if protocol.CanTransitionSessionBinding(binding.Status, update.Status) || binding.Status == update.Status {
				binding.Status = update.Status
				binding.LastRunID = runRecord.ID
				binding.UpdatedAt = nowText
				state.SessionBindings[existingID] = binding
			}
			if runRecord.ProviderSessionID == nil && update.ProviderSessionID != "" {
				sessionIDCopy := update.ProviderSessionID
				runRecord.ProviderSessionID = &sessionIDCopy
				state.Runs[runRecord.ID] = runRecord
			}
			return nil
		}

		newBindingID := protocol.NewIdentifier("sbind")
		for otherID, other := range state.SessionBindings {
			if other.InstanceID == runRecord.InstanceID &&
				other.AllocationID == runRecord.AllocationID &&
				other.Transport == update.Transport &&
				!protocol.IsTerminalSessionBindingStatus(other.Status) {
				other.Status = "replaced"
				other.ReplacedByBindingID = newBindingID
				other.UpdatedAt = nowText
				state.SessionBindings[otherID] = other
			}
		}

		leaseID := ""
		if runRecord.WorkspaceLeaseID != nil {
			leaseID = *runRecord.WorkspaceLeaseID
		}
		var capabilities *protocol.AcpAgentCapabilities
		if runRecord.TransportSelection != nil && runRecord.TransportSelection.ACP != nil {
			capCopy := *runRecord.TransportSelection.ACP
			capabilities = &capCopy
		}

		state.SessionBindings[newBindingID] = protocol.HarnessSessionBinding{
			ID:                newBindingID,
			ThreadID:          runRecord.ThreadID,
			InstanceID:        runRecord.InstanceID,
			AllocationID:      runRecord.AllocationID,
			NodeID:            nodeID,
			HarnessID:         update.HarnessID,
			Transport:         update.Transport,
			Workspace:         runRecord.Workspace,
			WorkspaceLeaseID:  leaseID,
			ProviderSessionID: update.ProviderSessionID,
			Capabilities:      capabilities,
			Status:            update.Status,
			CreatedByRunID:    runRecord.ID,
			LastRunID:         runRecord.ID,
			CreatedAt:         nowText,
			UpdatedAt:         nowText,
		}
		runRecord.SessionBindingID = &newBindingID
		sessionIDCopy := update.ProviderSessionID
		runRecord.ProviderSessionID = &sessionIDCopy
		state.Runs[runRecord.ID] = runRecord
		return nil
	})
}

func (service *Service) ListApprovals(input ListApprovalsInput) ([]protocol.ApprovalRequest, error) {
	if input.Status != "" && !slices.Contains(protocol.ApprovalStatuses, input.Status) {
		return nil, invalidRequest("invalid approval status filter")
	}
	var approvals []protocol.ApprovalRequest
	err := service.store.Read(func(state *store.State) error {
		for _, approval := range state.Approvals {
			if input.ThreadID != "" && approval.ThreadID != input.ThreadID {
				continue
			}
			if input.Status != "" && approval.Status != input.Status {
				continue
			}
			approvals = append(approvals, approval)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(approvals, func(left, right protocol.ApprovalRequest) int {
		if compared := cmp.Compare(left.RequestedAt, right.RequestedAt); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})
	if approvals == nil {
		approvals = []protocol.ApprovalRequest{}
	}
	return approvals, nil
}

func (service *Service) ResolveApproval(
	caller CallerIdentity,
	input ResolveApprovalInput,
) (protocol.ApprovalRequest, error) {
	if !protocol.IsIdentifier(input.ApprovalID) {
		return protocol.ApprovalRequest{}, invalidRequest("approvalId is required")
	}

	var resolved protocol.ApprovalRequest
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		approval, exists := state.Approvals[input.ApprovalID]
		if !exists {
			return notFound(fmt.Sprintf("approval %s does not exist", input.ApprovalID))
		}
		if approval.Status != "pending" {
			resolved = approval
			return nil
		}

		selectedOptionID := strings.TrimSpace(input.OptionID)
		targetStatus := strings.TrimSpace(input.Status)

		if selectedOptionID != "" {
			var matchedOption *protocol.ApprovalOption
			for index := range approval.Options {
				if approval.Options[index].ID == selectedOptionID {
					matchedOption = &approval.Options[index]
					break
				}
			}
			if matchedOption == nil {
				return invalidRequest(fmt.Sprintf("optionId %q is not offered on approval %s", selectedOptionID, approval.ID))
			}
			derivedStatus := "approved"
			if strings.HasPrefix(matchedOption.Kind, "reject") {
				derivedStatus = "rejected"
			}
			if targetStatus != "" && targetStatus != derivedStatus {
				return invalidRequest(fmt.Sprintf("status %q conflicts with selected option kind %q", targetStatus, matchedOption.Kind))
			}
			targetStatus = derivedStatus
		} else {
			if targetStatus != "approved" && targetStatus != "rejected" && targetStatus != "cancelled" {
				return invalidRequest("must specify optionId or status (approved, rejected, cancelled)")
			}
			if targetStatus == "approved" || targetStatus == "rejected" {
				wantedPrefix := "allow"
				if targetStatus == "rejected" {
					wantedPrefix = "reject"
				}
				for _, option := range approval.Options {
					if strings.HasPrefix(option.Kind, wantedPrefix) {
						selectedOptionID = option.ID
						break
					}
				}
				if selectedOptionID == "" && len(approval.Options) > 0 {
					selectedOptionID = approval.Options[0].ID
				}
			}
		}

		nowText := service.nowUTC()
		actor := protocol.ApprovalActor{
			Kind:       caller.Kind,
			OperatorID: caller.OperatorID,
			ClientID:   caller.ClientID,
			RunID:      caller.RunID,
		}
		if actor.Kind == "" {
			actor.Kind = "operator"
		}

		approval.Status = targetStatus
		approval.SelectedOptionID = selectedOptionID
		approval.ResolvedBy = &actor
		approval.ResolvedAt = nowText
		approval.UpdatedAt = nowText

		decision := protocol.ApprovalDecision{
			ApprovalID:       approval.ID,
			RunID:            approval.RunID,
			Status:           approval.Status,
			SelectedOptionID: approval.SelectedOptionID,
		}
		nodeID := approval.NodeID
		if service.isNodeConnected(nodeID) {
			approval.DeliveryState = "sent"
			mutation.OnCommit(func() {
				decisionCopy := decision
				_ = service.sendToSlave(nodeID, protocol.MasterControlMessage{
					Type:     "approval.decision",
					RunID:    decisionCopy.RunID,
					Decision: &decisionCopy,
					At:       service.nowUTC(),
				})
			})
		} else {
			approval.DeliveryState = "pending"
		}

		state.Approvals[approval.ID] = approval
		resolved = approval
		return nil
	})
	return resolved, err
}

func (service *Service) handleApprovalUndeliverable(nodeID string, message protocol.SlaveControlMessage) {
	if !protocol.IsIdentifier(message.ApprovalID) {
		return
	}
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		approval, exists := state.Approvals[message.ApprovalID]
		if !exists || approval.NodeID != nodeID {
			return nil
		}
		approval.DeliveryState = "not-applied"
		if message.Error != nil {
			approval.UndeliverableError = protocol.BoundedDiagnostic(service.redactor.Redact(*message.Error))
		}
		approval.UpdatedAt = service.nowUTC()
		state.Approvals[approval.ID] = approval
		return nil
	})
}

func (service *Service) expirePendingApprovals() {
	now := service.now()
	nowText := now.UTC().Format(time.RFC3339Nano)
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		for approvalID, approval := range state.Approvals {
			if approval.Status != "pending" {
				continue
			}
			expiresAt, parsed := protocol.ParseTimestamp(approval.ExpiresAt)
			if !parsed || expiresAt.After(now) {
				continue
			}
			approval.Status = "expired"
			approval.DeliveryState = "pending"
			approval.ResolvedAt = nowText
			approval.UpdatedAt = nowText
			if service.isNodeConnected(approval.NodeID) {
				approval.DeliveryState = "sent"
				nodeID := approval.NodeID
				decision := protocol.ApprovalDecision{
					ApprovalID: approval.ID,
					RunID:      approval.RunID,
					Status:     "expired",
				}
				mutation.OnCommit(func() {
					_ = service.sendToSlave(nodeID, protocol.MasterControlMessage{
						Type:     "approval.decision",
						RunID:    decision.RunID,
						Decision: &decision,
						At:       service.nowUTC(),
					})
				})
			}
			state.Approvals[approvalID] = approval
		}
		return nil
	})
}

func (service *Service) handleWorkspaceLeaseUpdate(nodeID string, message protocol.SlaveControlMessage) {
	if message.Lease == nil || message.Lease.Validate() != nil {
		return
	}
	update := *message.Lease
	var confirmStatus string

	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		lease, exists := state.WorkspaceLeases[update.LeaseID]
		if !exists || lease.NodeID != nodeID {
			return nil
		}
		if lease.Status != update.Status {
			steps := protocol.WorkspaceLeasePath(lease.Status, update.Status)
			if len(steps) == 0 && !protocol.CanTransitionWorkspaceLease(lease.Status, update.Status) {
				return nil
			}
			lease.Status = update.Status
		}
		if update.ResolvedBaseRevision != "" {
			lease.ResolvedBaseRevision = update.ResolvedBaseRevision
		}
		lease.RetentionReason = update.RetentionReason
		lease.Detail = service.redactor.Redact(update.Detail)
		lease.UpdatedAt = service.nowUTC()
		state.WorkspaceLeases[lease.ID] = lease

		if update.Status == "active" || update.Status == "cleaned" || update.Status == "retained" || update.Status == "failed" {
			confirmStatus = update.Status
		}
		return nil
	})

	if confirmStatus != "" {
		_ = service.sendToSlave(nodeID, protocol.MasterControlMessage{
			Type:    "workspace.lease.confirmed",
			LeaseID: update.LeaseID,
			Status:  confirmStatus,
			At:      service.nowUTC(),
		})
	}
}

func (service *Service) CleanupWorkspaceLease(leaseID string) (protocol.WorkspaceLease, error) {
	if !protocol.IsIdentifier(leaseID) {
		return protocol.WorkspaceLease{}, invalidRequest("leaseId is required")
	}
	var updated protocol.WorkspaceLease
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		lease, exists := state.WorkspaceLeases[leaseID]
		if !exists {
			return notFound(fmt.Sprintf("workspace lease %s does not exist", leaseID))
		}
		if protocol.IsTerminalWorkspaceLeaseStatus(lease.Status) {
			updated = lease
			return nil
		}
		nowText := service.nowUTC()
		lease.CleanupRequestedAt = nowText
		lease.UpdatedAt = nowText
		state.WorkspaceLeases[lease.ID] = lease
		updated = lease

		grant := lease.Grant()
		nodeID := lease.NodeID
		mutation.OnCommit(func() {
			_ = service.sendToSlave(nodeID, protocol.MasterControlMessage{
				Type:  "workspace.cleanup",
				Lease: &grant,
				Mode:  protocol.WorkspaceCleanupModeOperator,
				At:    service.nowUTC(),
			})
		})
		return nil
	})
	return updated, err
}

func (service *Service) PostArtifact(
	caller CallerIdentity,
	input PostArtifactInput,
) (PostArtifactResult, error) {
	threadID := strings.TrimSpace(input.ThreadID)
	taskID := strings.TrimSpace(input.TaskID)
	if caller.Kind == "run" {
		if threadID == "" {
			threadID = caller.ThreadID
		} else if caller.ThreadID != "" && threadID != caller.ThreadID {
			return PostArtifactResult{}, forbiddenError("worker run cannot post artifacts into another thread")
		}
		if taskID == "" {
			taskID = caller.TaskID
		}
	}
	if !protocol.IsIdentifier(threadID) {
		return PostArtifactResult{}, invalidRequest("threadId is required")
	}
	name := strings.TrimSpace(input.Name)
	if name == "" && input.RelativePath != "" {
		name = input.RelativePath
	}
	if name == "" || !protocol.IsBoundedString(name, 512) {
		return PostArtifactResult{}, invalidRequest("artifact name is required")
	}
	if input.SizeBytes < 0 || input.SizeBytes > protocol.MaximumArtifactSizeBytes {
		return PostArtifactResult{}, invalidRequest(fmt.Sprintf("sizeBytes must be between 0 and %d", protocol.MaximumArtifactSizeBytes))
	}
	normalizedDigest := strings.ToLower(strings.TrimSpace(input.SHA256))
	if len(normalizedDigest) != 64 {
		return PostArtifactResult{}, invalidRequest("sha256 must be a 64-character lowercase hex digest")
	}
	for _, character := range normalizedDigest {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return PostArtifactResult{}, invalidRequest("sha256 must be a valid hex digest")
		}
	}
	mediaType := strings.TrimSpace(input.MediaType)
	if mediaType == "" {
		mediaType = "application/octet-stream"
	}

	var tokenBytes [24]byte
	_, _ = rand.Read(tokenBytes[:])
	grantToken := base64.RawURLEncoding.EncodeToString(tokenBytes[:])
	tokenDigestBytes := sha256.Sum256([]byte(grantToken))
	tokenDigest := hex.EncodeToString(tokenDigestBytes[:])

	var result PostArtifactResult
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		if _, exists := state.Threads[threadID]; !exists {
			return notFound(fmt.Sprintf("thread %s does not exist", threadID))
		}
		if taskID != "" {
			if targetTask, exists := state.Tasks[taskID]; !exists || targetTask.ThreadID != threadID {
				return invalidRequest(fmt.Sprintf("task %s does not exist in thread %s", taskID, threadID))
			}
		}

		now := service.now()
		nowText := now.UTC().Format(time.RFC3339Nano)
		expiresAt := now.Add(artifactGrantTTL).UTC().Format(time.RFC3339Nano)
		artifactID := protocol.NewIdentifier("art")
		uploadPath := "/api/artifacts/" + artifactID + "/content"

		artifact := protocol.Artifact{
			ID:           artifactID,
			ThreadID:     threadID,
			TaskID:       taskID,
			RunID:        caller.RunID,
			ClientID:     caller.ClientID,
			SourceKey:    caller.SourceKey(),
			Name:         name,
			RelativePath: input.RelativePath,
			MediaType:    mediaType,
			SizeBytes:    input.SizeBytes,
			SHA256:       normalizedDigest,
			Summary:      service.redactor.Redact(strings.TrimSpace(input.Summary)),
			Uploaded:     false,
			DownloadPath: uploadPath,
			CreatedAt:    nowText,
		}
		state.Artifacts[artifactID] = artifact
		state.ArtifactGrants[artifactID] = protocol.ArtifactUploadGrantRecord{
			TokenDigest: tokenDigest,
			ArtifactID:  artifactID,
			SourceKey:   caller.SourceKey(),
			ThreadID:    threadID,
			CreatedAt:   nowText,
			ExpiresAt:   expiresAt,
		}

		result = PostArtifactResult{
			Artifact: artifact,
			Upload: protocol.ArtifactUploadGrant{
				Path:      uploadPath + "?uploadToken=" + grantToken,
				Token:     grantToken,
				ExpiresAt: expiresAt,
			},
		}
		return nil
	})
	return result, err
}

func (service *Service) CompleteArtifactUpload(
	artifactID string,
	uploadToken string,
	bearerAuthorized bool,
	reader io.Reader,
) (protocol.Artifact, error) {
	if !protocol.IsIdentifier(artifactID) {
		return protocol.Artifact{}, invalidRequest("invalid artifact id")
	}

	var target protocol.Artifact
	var grantRecord protocol.ArtifactUploadGrantRecord
	var hasGrant bool

	err := service.store.Read(func(state *store.State) error {
		artifact, exists := state.Artifacts[artifactID]
		if !exists {
			return notFound(fmt.Sprintf("artifact %s does not exist", artifactID))
		}
		target = artifact
		grantRecord, hasGrant = state.ArtifactGrants[artifactID]
		return nil
	})
	if err != nil {
		return protocol.Artifact{}, err
	}

	if !bearerAuthorized {
		if !hasGrant || uploadToken == "" {
			return protocol.Artifact{}, newError("unauthorized", 401, "missing authorization or upload token")
		}
		providedDigestBytes := sha256.Sum256([]byte(uploadToken))
		providedDigest := hex.EncodeToString(providedDigestBytes[:])
		if subtle.ConstantTimeCompare([]byte(providedDigest), []byte(grantRecord.TokenDigest)) != 1 {
			return protocol.Artifact{}, newError("unauthorized", 401, "invalid artifact upload token")
		}
		if expiresAt, parsed := protocol.ParseTimestamp(grantRecord.ExpiresAt); !parsed || expiresAt.Before(service.now()) {
			return protocol.Artifact{}, forbiddenError("artifact upload token has expired")
		}
	}

	if target.Uploaded {
		return target, nil
	}

	if writeErr := service.store.WriteArtifactBlob(artifactID, target.SizeBytes, target.SHA256, reader); writeErr != nil {
		return protocol.Artifact{}, invalidRequest(writeErr.Error())
	}

	var updated protocol.Artifact
	_, err = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		artifact, exists := state.Artifacts[artifactID]
		if !exists {
			return notFound(fmt.Sprintf("artifact %s does not exist", artifactID))
		}
		nowText := service.nowUTC()
		artifact.Uploaded = true
		artifact.UploadedAt = nowText
		state.Artifacts[artifactID] = artifact
		if grant, found := state.ArtifactGrants[artifactID]; found {
			grant.ConsumedAt = nowText
			state.ArtifactGrants[artifactID] = grant
		}
		if artifact.TaskID != "" {
			if task, taskExists := state.Tasks[artifact.TaskID]; taskExists && !slices.Contains(task.ArtifactIDs, artifactID) {
				task.ArtifactIDs = append(task.ArtifactIDs, artifactID)
				task.UpdatedAt = nowText
				state.Tasks[task.ID] = task
			}
		}
		updated = artifact
		return nil
	})
	return updated, err
}

func (service *Service) OpenUploadedArtifact(artifactID string) (protocol.Artifact, *os.File, int64, error) {
	if !protocol.IsIdentifier(artifactID) {
		return protocol.Artifact{}, nil, 0, invalidRequest("invalid artifact id")
	}
	var artifact protocol.Artifact
	err := service.store.Read(func(state *store.State) error {
		found, exists := state.Artifacts[artifactID]
		if !exists || !found.Uploaded {
			return notFound(fmt.Sprintf("uploaded artifact %s does not exist", artifactID))
		}
		artifact = found
		return nil
	})
	if err != nil {
		return protocol.Artifact{}, nil, 0, err
	}
	file, size, openErr := service.store.OpenArtifactBlob(artifactID)
	if openErr != nil {
		return protocol.Artifact{}, nil, 0, notFound(openErr.Error())
	}
	return artifact, file, size, nil
}
