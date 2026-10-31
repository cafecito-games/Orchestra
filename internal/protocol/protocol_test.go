package protocol_test

import (
	"encoding/json"
	"runtime/debug"
	"slices"
	"strings"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStateTransitions(t *testing.T) {
	t.Run("task transitions and terminal checks", func(t *testing.T) {
		expectedTaskTransitions := map[string][]string{
			"pending":   {"ready", "blocked", "cancelled"},
			"ready":     {"running", "blocked", "cancelled"},
			"running":   {"ready", "completed", "failed", "cancelled"},
			"blocked":   {"cancelled"},
			"completed": {},
			"failed":    {},
			"cancelled": {},
		}

		for _, sourceStatus := range protocol.TaskStatuses {
			for _, targetStatus := range protocol.TaskStatuses {
				allowed := slices.Contains(expectedTaskTransitions[sourceStatus], targetStatus)
				assert.Equal(t, allowed, protocol.CanTransitionTask(sourceStatus, targetStatus),
					"transition task from %s to %s", sourceStatus, targetStatus)
			}
		}

		for _, terminalStatus := range protocol.TerminalTaskStatuses {
			assert.True(t, protocol.IsTerminalTaskStatus(terminalStatus))
			for _, activeStatus := range []string{"pending", "ready", "running", "blocked"} {
				assert.False(t, protocol.CanTransitionTask(terminalStatus, activeStatus),
					"terminal task %s should not transition to active %s", terminalStatus, activeStatus)
			}
		}

		assert.False(t, protocol.CanTransitionTask("unknown", "ready"))
		assert.False(t, protocol.CanTransitionTask("pending", "unknown"))
		assert.False(t, protocol.IsTerminalTaskStatus("running"))
	})

	t.Run("run transitions and active/terminal checks", func(t *testing.T) {
		expectedRunTransitions := map[string][]string{
			"queued":    {"running", "failed", "cancelled"},
			"running":   {"completed", "failed", "cancelled"},
			"completed": {},
			"failed":    {},
			"cancelled": {},
		}

		for _, sourceStatus := range protocol.RunStatuses {
			for _, targetStatus := range protocol.RunStatuses {
				allowed := slices.Contains(expectedRunTransitions[sourceStatus], targetStatus)
				assert.Equal(t, allowed, protocol.CanTransitionRun(sourceStatus, targetStatus),
					"transition run from %s to %s", sourceStatus, targetStatus)
			}
		}

		terminalStatuses := []string{"completed", "failed", "cancelled"}
		for _, terminalStatus := range terminalStatuses {
			assert.True(t, protocol.IsTerminalRunStatus(terminalStatus))
			assert.False(t, protocol.IsActiveRunStatus(terminalStatus))
			for _, activeStatus := range []string{"queued", "running"} {
				assert.False(t, protocol.CanTransitionRun(terminalStatus, activeStatus),
					"terminal run %s should not transition to active %s", terminalStatus, activeStatus)
			}
		}

		assert.True(t, protocol.IsActiveRunStatus("queued"))
		assert.True(t, protocol.IsActiveRunStatus("running"))
		assert.False(t, protocol.IsTerminalRunStatus("queued"))
		assert.False(t, protocol.IsTerminalRunStatus("running"))
		assert.False(t, protocol.CanTransitionRun("unknown", "running"))
		assert.False(t, protocol.CanTransitionRun("queued", "unknown"))
	})

	t.Run("instance transitions and terminal checks", func(t *testing.T) {
		expectedInstanceTransitions := map[string][]string{
			"requested":    {"provisioning", "draining", "failed"},
			"provisioning": {"ready", "draining", "failed"},
			"ready":        {"busy", "idle", "provisioning", "draining", "failed"},
			"busy":         {"idle", "provisioning", "draining", "failed"},
			"idle":         {"busy", "provisioning", "draining", "failed"},
			"draining":     {"released", "failed"},
			"released":     {},
			"failed":       {},
		}

		for _, sourceStatus := range protocol.InstanceStatuses {
			for _, targetStatus := range protocol.InstanceStatuses {
				allowed := slices.Contains(expectedInstanceTransitions[sourceStatus], targetStatus)
				assert.Equal(t, allowed, protocol.CanTransitionInstance(sourceStatus, targetStatus),
					"transition instance from %s to %s", sourceStatus, targetStatus)
			}
		}

		for _, terminalStatus := range protocol.TerminalInstanceStatuses {
			assert.True(t, protocol.IsTerminalInstanceStatus(terminalStatus))
			for _, nonTerminalStatus := range []string{"requested", "provisioning", "ready", "busy", "idle", "draining"} {
				assert.False(t, protocol.CanTransitionInstance(terminalStatus, nonTerminalStatus),
					"terminal instance %s should not transition to %s", terminalStatus, nonTerminalStatus)
			}
		}

		assert.False(t, protocol.IsTerminalInstanceStatus("ready"))
		assert.False(t, protocol.CanTransitionInstance("unknown", "ready"))
	})

	t.Run("allocation transitions and occupancy checks", func(t *testing.T) {
		expectedAllocationTransitions := map[string][]string{
			"reserved":     {"provisioning", "released", "failed"},
			"provisioning": {"active", "lost", "released", "failed"},
			"active":       {"lost", "released", "failed"},
			"lost":         {"released", "failed"},
			"released":     {},
			"failed":       {},
		}

		for _, sourceStatus := range protocol.AllocationStatuses {
			for _, targetStatus := range protocol.AllocationStatuses {
				allowed := slices.Contains(expectedAllocationTransitions[sourceStatus], targetStatus)
				assert.Equal(t, allowed, protocol.CanTransitionAllocation(sourceStatus, targetStatus),
					"transition allocation from %s to %s", sourceStatus, targetStatus)
			}
		}

		for _, occupyingStatus := range protocol.OccupyingAllocationStatuses {
			assert.True(t, protocol.IsOccupyingAllocationStatus(occupyingStatus))
		}
		assert.False(t, protocol.IsOccupyingAllocationStatus("released"))
		assert.False(t, protocol.IsOccupyingAllocationStatus("failed"))
		assert.False(t, protocol.IsOccupyingAllocationStatus("lost"))

		for _, terminalStatus := range []string{"released", "failed"} {
			for _, activeStatus := range []string{"reserved", "provisioning", "active"} {
				assert.False(t, protocol.CanTransitionAllocation(terminalStatus, activeStatus),
					"terminal allocation %s should not transition to %s", terminalStatus, activeStatus)
			}
		}
	})

	t.Run("workspace lease transitions and terminal checks", func(t *testing.T) {
		expectedLeaseTransitions := map[string][]string{
			"requested":    {"provisioning", "failed"},
			"provisioning": {"active", "released", "retained", "failed"},
			"active":       {"released", "retained"},
			"released":     {"cleaning", "retained"},
			"cleaning":     {"cleaned", "retained", "failed"},
			"retained":     {"cleaning"},
			"cleaned":      {},
			"failed":       {},
		}

		for _, sourceStatus := range protocol.WorkspaceLeaseStatuses {
			for _, targetStatus := range protocol.WorkspaceLeaseStatuses {
				allowed := slices.Contains(expectedLeaseTransitions[sourceStatus], targetStatus)
				assert.Equal(t, allowed, protocol.CanTransitionWorkspaceLease(sourceStatus, targetStatus),
					"transition lease from %s to %s", sourceStatus, targetStatus)
			}
		}

		assert.True(t, protocol.IsTerminalWorkspaceLeaseStatus("cleaned"))
		assert.True(t, protocol.IsTerminalWorkspaceLeaseStatus("failed"))
		assert.False(t, protocol.IsTerminalWorkspaceLeaseStatus("active"))
		assert.False(t, protocol.IsTerminalWorkspaceLeaseStatus("retained"))

		for _, terminalStatus := range []string{"cleaned", "failed"} {
			for _, activeStatus := range []string{"requested", "provisioning", "active"} {
				assert.False(t, protocol.CanTransitionWorkspaceLease(terminalStatus, activeStatus),
					"terminal lease %s should not transition to %s", terminalStatus, activeStatus)
			}
		}
	})

	t.Run("session binding transitions and terminal checks", func(t *testing.T) {
		expectedBindingTransitions := map[string][]string{
			"active":   {"idle", "closed", "replaced", "failed"},
			"idle":     {"active", "closed", "replaced", "failed"},
			"closed":   {},
			"replaced": {},
			"failed":   {},
		}

		for _, sourceStatus := range protocol.SessionBindingStatuses {
			for _, targetStatus := range protocol.SessionBindingStatuses {
				allowed := slices.Contains(expectedBindingTransitions[sourceStatus], targetStatus)
				assert.Equal(t, allowed, protocol.CanTransitionSessionBinding(sourceStatus, targetStatus),
					"transition binding from %s to %s", sourceStatus, targetStatus)
			}
		}

		for _, terminalStatus := range []string{"closed", "replaced", "failed"} {
			assert.True(t, protocol.IsTerminalSessionBindingStatus(terminalStatus))
			assert.False(t, protocol.CanTransitionSessionBinding(terminalStatus, "active"))
			assert.False(t, protocol.CanTransitionSessionBinding(terminalStatus, "idle"))
		}
		assert.False(t, protocol.IsTerminalSessionBindingStatus("active"))
		assert.False(t, protocol.IsTerminalSessionBindingStatus("idle"))
	})

	t.Run("approval transitions and terminal checks", func(t *testing.T) {
		expectedApprovalTransitions := map[string][]string{
			"pending":   {"approved", "rejected", "cancelled", "expired"},
			"approved":  {},
			"rejected":  {},
			"cancelled": {},
			"expired":   {},
		}

		for _, sourceStatus := range protocol.ApprovalStatuses {
			for _, targetStatus := range protocol.ApprovalStatuses {
				allowed := slices.Contains(expectedApprovalTransitions[sourceStatus], targetStatus)
				assert.Equal(t, allowed, protocol.CanTransitionApproval(sourceStatus, targetStatus),
					"transition approval from %s to %s", sourceStatus, targetStatus)
			}
		}

		for _, terminalStatus := range []string{"approved", "rejected", "cancelled", "expired"} {
			assert.True(t, protocol.IsTerminalApprovalStatus(terminalStatus))
			assert.False(t, protocol.CanTransitionApproval(terminalStatus, "pending"))
		}
		assert.False(t, protocol.IsTerminalApprovalStatus("pending"))
	})

	t.Run("approval decision validation", func(t *testing.T) {
		validApproved := protocol.ApprovalDecision{
			ApprovalID:       "approval-1",
			RunID:            "run-1",
			Status:           "approved",
			SelectedOptionID: "option-1",
		}
		require.NoError(t, validApproved.Validate())

		validCancelled := protocol.ApprovalDecision{
			ApprovalID: "approval-1",
			RunID:      "run-1",
			Status:     "cancelled",
		}
		require.NoError(t, validCancelled.Validate())

		missingID := validApproved
		missingID.ApprovalID = ""
		require.Error(t, missingID.Validate())

		missingRun := validApproved
		missingRun.RunID = ""
		require.Error(t, missingRun.Validate())

		pendingStatus := validApproved
		pendingStatus.Status = "pending"
		require.Error(t, pendingStatus.Validate())

		approvedMissingOption := validApproved
		approvedMissingOption.SelectedOptionID = ""
		require.Error(t, approvedMissingOption.Validate())

		cancelledWithOption := validCancelled
		cancelledWithOption.SelectedOptionID = "option-1"
		require.Error(t, cancelledWithOption.Validate())
	})
}

func TestHarnessEventValidation_AllTypes(t *testing.T) {
	timestamp := protocol.NowTimestamp()
	inputTokens := int64(1500)
	outputTokens := int64(250)
	cachedTokens := int64(400)
	costUSD := 0.042

	cases := []struct {
		name  string
		event protocol.HarnessEvent
	}{
		{
			name: "message.delta event",
			event: protocol.HarnessEvent{
				Type:     "message.delta",
				RunID:    "run-1",
				Sequence: 1,
				At:       timestamp,
				Text:     "Thinking through the solution",
			},
		},
		{
			name: "thought.delta event",
			event: protocol.HarnessEvent{
				Type:     "thought.delta",
				RunID:    "run-1",
				Sequence: 2,
				At:       timestamp,
				Text:     "Internal agent chain of thought",
			},
		},
		{
			name: "plan.updated event",
			event: protocol.HarnessEvent{
				Type:     "plan.updated",
				RunID:    "run-1",
				Sequence: 3,
				At:       timestamp,
				Entries: []protocol.PlanEntry{
					{Content: "Explore codebase", Status: "completed", Priority: "high"},
					{Content: "Implement changes", Status: "in-progress", Priority: "medium"},
					{Content: "Run test suite", Status: "pending", Priority: "low"},
				},
			},
		},
		{
			name: "tool.call event",
			event: protocol.HarnessEvent{
				Type:       "tool.call",
				RunID:      "run-1",
				Sequence:   4,
				At:         timestamp,
				ToolCallID: "call-1",
				Status:     "in-progress",
				Kind:       "read",
				Title:      "Read capability file",
				Detail:     "Reading internal/protocol/capability.go",
			},
		},
		{
			name: "diff event",
			event: protocol.HarnessEvent{
				Type:       "diff",
				RunID:      "run-1",
				Sequence:   5,
				At:         timestamp,
				ToolCallID: "call-1",
				Path:       "internal/protocol/domain.go",
				OldText:    "original code",
				NewText:    "updated code",
			},
		},
		{
			name: "terminal.output stdout event",
			event: protocol.HarnessEvent{
				Type:       "terminal.output",
				RunID:      "run-1",
				Sequence:   6,
				At:         timestamp,
				TerminalID: "term-1",
				Stream:     "stdout",
				Text:       "PASS: TestFeature",
			},
		},
		{
			name: "terminal.output stderr event",
			event: protocol.HarnessEvent{
				Type:       "terminal.output",
				RunID:      "run-1",
				Sequence:   7,
				At:         timestamp,
				TerminalID: "term-1",
				Stream:     "stderr",
				Text:       "warning: deprecated package",
			},
		},
		{
			name: "usage event",
			event: protocol.HarnessEvent{
				Type:              "usage",
				RunID:             "run-1",
				Sequence:          8,
				At:                timestamp,
				InputTokens:       &inputTokens,
				OutputTokens:      &outputTokens,
				CachedInputTokens: &cachedTokens,
				CostUSD:           &costUSD,
			},
		},
		{
			name: "permission.requested event",
			event: protocol.HarnessEvent{
				Type:       "permission.requested",
				RunID:      "run-1",
				Sequence:   9,
				At:         timestamp,
				ApprovalID: "approval-1",
				ToolCallID: "call-1",
				Title:      "File modification request",
				Detail:     "Request to write file",
				Options: []protocol.ApprovalOption{
					{ID: "allow-once", Label: "Allow once", Kind: "allow-once"},
					{ID: "reject", Label: "Reject", Kind: "reject-once"},
				},
			},
		},
		{
			name: "permission.resolved event",
			event: protocol.HarnessEvent{
				Type:             "permission.resolved",
				RunID:            "run-1",
				Sequence:         10,
				At:               timestamp,
				ApprovalID:       "approval-1",
				Status:           "approved",
				SelectedOptionID: "allow-once",
			},
		},
		{
			name: "warning event",
			event: protocol.HarnessEvent{
				Type:     "warning",
				RunID:    "run-1",
				Sequence: 11,
				At:       timestamp,
				Code:     "rate-limit-approaching",
				Message:  "Provider rate limit at 85%",
			},
		},
		{
			name: "unknown event",
			event: protocol.HarnessEvent{
				Type:       "unknown",
				RunID:      "run-1",
				Sequence:   12,
				At:         timestamp,
				SourceType: "custom-extension-event",
				Detail:     "Additional payload data",
			},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			require.NoError(t, testCase.event.Validate())
			encodedBytes, err := json.Marshal(testCase.event)
			require.NoError(t, err)

			var decoded map[string]any
			require.NoError(t, json.Unmarshal(encodedBytes, &decoded))
			assert.Equal(t, testCase.event.Type, decoded["type"])
			assert.Equal(t, testCase.event.RunID, decoded["runId"])
		})
	}
}

func TestHarnessEventValidation_PayloadLimitsAndRejections(t *testing.T) {
	timestamp := protocol.NowTimestamp()
	baseEvent := protocol.HarnessEvent{
		Type:     "message.delta",
		RunID:    "run-1",
		Sequence: 1,
		At:       timestamp,
		Text:     "valid message",
	}

	t.Run("common field validations", func(t *testing.T) {
		unknownType := baseEvent
		unknownType.Type = "not.a.type"
		require.Error(t, unknownType.Validate())

		missingRun := baseEvent
		missingRun.RunID = ""
		require.Error(t, missingRun.Validate())

		invalidRun := baseEvent
		invalidRun.RunID = strings.Repeat("r", protocol.IdentifierMaximumBytes+1)
		require.Error(t, invalidRun.Validate())

		negativeSequence := baseEvent
		negativeSequence.Sequence = -1
		require.Error(t, negativeSequence.Validate())

		invalidTimestamp := baseEvent
		invalidTimestamp.At = "not-a-timestamp"
		require.Error(t, invalidTimestamp.Validate())
	})

	t.Run("message and thought text bounds", func(t *testing.T) {
		oversizedText := strings.Repeat("x", protocol.TextMaximumBytes+1)

		oversizedMessage := baseEvent
		oversizedMessage.Text = oversizedText
		require.Error(t, oversizedMessage.Validate())

		oversizedThought := baseEvent
		oversizedThought.Type = "thought.delta"
		oversizedThought.Text = oversizedText
		require.Error(t, oversizedThought.Validate())
	})

	t.Run("plan updated entries limit and validation", func(t *testing.T) {
		planEvent := baseEvent
		planEvent.Type = "plan.updated"
		planEvent.Text = ""

		oversizedEntries := make([]protocol.PlanEntry, protocol.PlanEntryLimit+1)
		for index := range protocol.PlanEntryLimit + 1 {
			oversizedEntries[index] = protocol.PlanEntry{Content: "task", Status: "pending", Priority: "medium"}
		}
		planEvent.Entries = oversizedEntries
		require.Error(t, planEvent.Validate())

		invalidStatusEntry := baseEvent
		invalidStatusEntry.Type = "plan.updated"
		invalidStatusEntry.Text = ""
		invalidStatusEntry.Entries = []protocol.PlanEntry{
			{Content: "task", Status: "not_a_status", Priority: "medium"},
		}
		require.Error(t, invalidStatusEntry.Validate())

		invalidPriorityEntry := baseEvent
		invalidPriorityEntry.Type = "plan.updated"
		invalidPriorityEntry.Text = ""
		invalidPriorityEntry.Entries = []protocol.PlanEntry{
			{Content: "task", Status: "pending", Priority: "urgent"},
		}
		require.Error(t, invalidPriorityEntry.Validate())

		oversizedContentEntry := baseEvent
		oversizedContentEntry.Type = "plan.updated"
		oversizedContentEntry.Text = ""
		oversizedContentEntry.Entries = []protocol.PlanEntry{
			{Content: strings.Repeat("a", protocol.TextMaximumBytes+1), Status: "pending", Priority: "low"},
		}
		require.Error(t, oversizedContentEntry.Validate())
	})

	t.Run("tool call payload limits", func(t *testing.T) {
		validToolCall := protocol.HarnessEvent{
			Type:       "tool.call",
			RunID:      "run-1",
			Sequence:   1,
			At:         timestamp,
			ToolCallID: "call-1",
			Status:     "completed",
			Kind:       "read",
			Title:      "Read file",
			Detail:     "Reading file content",
		}
		require.NoError(t, validToolCall.Validate())

		invalidID := validToolCall
		invalidID.ToolCallID = strings.Repeat("c", protocol.IdentifierMaximumBytes+1)
		require.Error(t, invalidID.Validate())

		invalidStatus := validToolCall
		invalidStatus.Status = "running"
		require.Error(t, invalidStatus.Validate())

		invalidKind := validToolCall
		invalidKind.Kind = "destroy"
		require.Error(t, invalidKind.Validate())

		oversizedTitle := validToolCall
		oversizedTitle.Title = strings.Repeat("t", protocol.DiagnosticMaximumBytes+1)
		require.Error(t, oversizedTitle.Validate())

		oversizedDetail := validToolCall
		oversizedDetail.Detail = strings.Repeat("d", protocol.TextMaximumBytes+1)
		require.Error(t, oversizedDetail.Validate())
	})

	t.Run("diff payload limits", func(t *testing.T) {
		validDiff := protocol.HarnessEvent{
			Type:     "diff",
			RunID:    "run-1",
			Sequence: 1,
			At:       timestamp,
			Path:     "file.txt",
			OldText:  "old",
			NewText:  "new",
		}
		require.NoError(t, validDiff.Validate())

		oversizedPath := validDiff
		oversizedPath.Path = strings.Repeat("p", protocol.TextMaximumBytes+1)
		require.Error(t, oversizedPath.Validate())

		oversizedOld := validDiff
		oversizedOld.OldText = strings.Repeat("o", protocol.DiffMaximumBytes+1)
		require.Error(t, oversizedOld.Validate())

		oversizedNew := validDiff
		oversizedNew.NewText = strings.Repeat("n", protocol.DiffMaximumBytes+1)
		require.Error(t, oversizedNew.Validate())

		invalidToolCall := validDiff
		invalidToolCall.ToolCallID = strings.Repeat("t", protocol.IdentifierMaximumBytes+1)
		require.Error(t, invalidToolCall.Validate())
	})

	t.Run("terminal output stream and bounds", func(t *testing.T) {
		validTerminal := protocol.HarnessEvent{
			Type:       "terminal.output",
			RunID:      "run-1",
			Sequence:   1,
			At:         timestamp,
			TerminalID: "term-1",
			Stream:     "stdout",
			Text:       "output",
		}
		require.NoError(t, validTerminal.Validate())

		invalidStream := validTerminal
		invalidStream.Stream = "stdlog"
		require.Error(t, invalidStream.Validate())

		invalidTerminalID := validTerminal
		invalidTerminalID.TerminalID = strings.Repeat("t", protocol.IdentifierMaximumBytes+1)
		require.Error(t, invalidTerminalID.Validate())

		oversizedText := validTerminal
		oversizedText.Text = strings.Repeat("x", protocol.TextMaximumBytes+1)
		require.Error(t, oversizedText.Validate())
	})

	t.Run("usage non-negative constraints", func(t *testing.T) {
		negativeTokens := int64(-5)
		negativeCost := -0.01
		validCount := int64(10)
		validCost := 0.05

		validUsage := protocol.HarnessEvent{
			Type:         "usage",
			RunID:        "run-1",
			Sequence:     1,
			At:           timestamp,
			InputTokens:  &validCount,
			OutputTokens: &validCount,
			CostUSD:      &validCost,
		}
		require.NoError(t, validUsage.Validate())

		badInput := validUsage
		badInput.InputTokens = &negativeTokens
		require.Error(t, badInput.Validate())

		badOutput := validUsage
		badOutput.OutputTokens = &negativeTokens
		require.Error(t, badOutput.Validate())

		badCached := validUsage
		badCached.CachedInputTokens = &negativeTokens
		require.Error(t, badCached.Validate())

		badCost := validUsage
		badCost.CostUSD = &negativeCost
		require.Error(t, badCost.Validate())
	})

	t.Run("permission requested options bounds and duplicates", func(t *testing.T) {
		validPermission := protocol.HarnessEvent{
			Type:       "permission.requested",
			RunID:      "run-1",
			Sequence:   1,
			At:         timestamp,
			ApprovalID: "approval-1",
			Title:      "Approval required",
			Options: []protocol.ApprovalOption{
				{ID: "opt-1", Label: "Allow", Kind: "allow-once"},
			},
		}
		require.NoError(t, validPermission.Validate())

		emptyOptions := validPermission
		emptyOptions.Options = []protocol.ApprovalOption{}
		require.Error(t, emptyOptions.Validate())

		oversizedOptions := make([]protocol.ApprovalOption, protocol.ApprovalOptionLimit+1)
		for index := range protocol.ApprovalOptionLimit + 1 {
			oversizedOptions[index] = protocol.ApprovalOption{
				ID:    "opt-" + strings.Repeat("a", index+1),
				Label: "Option",
				Kind:  "allow-once",
			}
		}
		tooManyOptions := validPermission
		tooManyOptions.Options = oversizedOptions
		require.Error(t, tooManyOptions.Validate())

		duplicateOptions := validPermission
		duplicateOptions.Options = []protocol.ApprovalOption{
			{ID: "opt-1", Label: "Option 1", Kind: "allow-once"},
			{ID: "opt-1", Label: "Option 1 duplicate", Kind: "allow-once"},
		}
		require.Error(t, duplicateOptions.Validate())

		invalidOptionKind := validPermission
		invalidOptionKind.Options = []protocol.ApprovalOption{
			{ID: "opt-1", Label: "Option 1", Kind: "skip-once"},
		}
		require.Error(t, invalidOptionKind.Validate())

		invalidOptionID := validPermission
		invalidOptionID.Options = []protocol.ApprovalOption{
			{ID: strings.Repeat("o", protocol.IdentifierMaximumBytes+1), Label: "Option 1", Kind: "allow-once"},
		}
		require.Error(t, invalidOptionID.Validate())

		oversizedOptionLabel := validPermission
		oversizedOptionLabel.Options = []protocol.ApprovalOption{
			{ID: "opt-1", Label: strings.Repeat("l", protocol.DiagnosticMaximumBytes+1), Kind: "allow-once"},
		}
		require.Error(t, oversizedOptionLabel.Validate())
	})

	t.Run("permission resolved terminal requirement", func(t *testing.T) {
		validResolved := protocol.HarnessEvent{
			Type:             "permission.resolved",
			RunID:            "run-1",
			Sequence:         1,
			At:               timestamp,
			ApprovalID:       "approval-1",
			Status:           "approved",
			SelectedOptionID: "opt-1",
		}
		require.NoError(t, validResolved.Validate())

		pendingResolved := validResolved
		pendingResolved.Status = "pending"
		require.Error(t, pendingResolved.Validate())

		unknownStatus := validResolved
		unknownStatus.Status = "unknown"
		require.Error(t, unknownStatus.Validate())

		invalidApprovalID := validResolved
		invalidApprovalID.ApprovalID = strings.Repeat("a", protocol.IdentifierMaximumBytes+1)
		require.Error(t, invalidApprovalID.Validate())

		invalidSelectedOption := validResolved
		invalidSelectedOption.SelectedOptionID = strings.Repeat("s", protocol.IdentifierMaximumBytes+1)
		require.Error(t, invalidSelectedOption.Validate())
	})

	t.Run("warning and unknown bounds", func(t *testing.T) {
		validWarning := protocol.HarnessEvent{
			Type:     "warning",
			RunID:    "run-1",
			Sequence: 1,
			At:       timestamp,
			Code:     "memory-pressure",
			Message:  "Memory pressure detected",
		}
		require.NoError(t, validWarning.Validate())

		invalidWarningCode := validWarning
		invalidWarningCode.Code = strings.Repeat("w", protocol.IdentifierMaximumBytes+1)
		require.Error(t, invalidWarningCode.Validate())

		oversizedWarningMessage := validWarning
		oversizedWarningMessage.Message = strings.Repeat("m", protocol.DiagnosticMaximumBytes+1)
		require.Error(t, oversizedWarningMessage.Validate())

		validUnknown := protocol.HarnessEvent{
			Type:       "unknown",
			RunID:      "run-1",
			Sequence:   1,
			At:         timestamp,
			SourceType: "custom-extension",
			Detail:     "details",
		}
		require.NoError(t, validUnknown.Validate())

		invalidUnknownSource := validUnknown
		invalidUnknownSource.SourceType = strings.Repeat("s", protocol.IdentifierMaximumBytes+1)
		require.Error(t, invalidUnknownSource.Validate())

		oversizedUnknownDetail := validUnknown
		oversizedUnknownDetail.Detail = strings.Repeat("u", protocol.DiagnosticMaximumBytes+1)
		require.Error(t, oversizedUnknownDetail.Validate())
	})

	t.Run("session binding update validation", func(t *testing.T) {
		validUpdate := protocol.SessionBindingUpdate{
			BindingID:         "sbind-1",
			ProviderSessionID: "sess-1",
			HarnessID:         protocol.HarnessClaudeCLI,
			Transport:         protocol.TransportNativeCLI,
			Status:            "active",
		}
		require.NoError(t, validUpdate.Validate())

		badID := validUpdate
		badID.ProviderSessionID = strings.Repeat("s", protocol.IdentifierMaximumBytes+1)
		require.Error(t, badID.Validate())

		badHarness := validUpdate
		badHarness.HarnessID = "unknown-harness"
		require.Error(t, badHarness.Validate())

		badTransport := validUpdate
		badTransport.Transport = "websocket"
		require.Error(t, badTransport.Validate())

		badStatus := validUpdate
		badStatus.Status = "paused"
		require.Error(t, badStatus.Validate())
	})
}

func TestHarnessEvent_JSONMarshalSchema(t *testing.T) {
	timestamp := protocol.NowTimestamp()
	event := protocol.HarnessEvent{
		Type:       "diff",
		RunID:      "run-1",
		Sequence:   5,
		At:         timestamp,
		Path:       "main.go",
		OldText:    "original",
		NewText:    "replacement",
		ToolCallID: "call-1",
		Text:       "irrelevant-variant-text",
		Title:      "irrelevant-variant-title",
	}

	encodedBytes, err := json.Marshal(event)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encodedBytes, &decoded))
	assert.Equal(t, "diff", decoded["type"])
	assert.Equal(t, "main.go", decoded["path"])
	assert.Equal(t, "replacement", decoded["newText"])
	assert.Equal(t, "call-1", decoded["toolCallId"])
	assert.NotContains(t, decoded, "text")
	assert.NotContains(t, decoded, "title")

	unknownEvent := protocol.HarnessEvent{
		Type:     "nonexistent.type",
		RunID:    "run-1",
		Sequence: 1,
		At:       timestamp,
	}
	_, err = json.Marshal(unknownEvent)
	require.Error(t, err)
}

func TestCapabilityAndVersionConstraints(t *testing.T) {
	t.Run("version normalization", func(t *testing.T) {
		validVersions := []string{"1.26.2", "0.1.0", "1.0", "2", "3.4.5.6", "100.200.300"}
		for _, version := range validVersions {
			assert.True(t, protocol.IsNormalizedVersion(version), "expected valid: %s", version)
		}

		invalidVersions := []string{
			"01.2.3",
			"1.02.3",
			"20261030",
			"1.2.3.4.5",
			"v1.2.3",
			"1.2.x",
			"1..2",
			"",
			strings.Repeat("1", 33),
		}
		for _, version := range invalidVersions {
			assert.False(t, protocol.IsNormalizedVersion(version), "expected invalid: %s", version)
		}
	})

	t.Run("extract normalized version", func(t *testing.T) {
		assert.Equal(t, "1.26.2", protocol.ExtractNormalizedVersion("go version go1.26.2 darwin/arm64"))
		assert.Equal(t, "2.40.1", protocol.ExtractNormalizedVersion("git version 2.40.1"))
		assert.Empty(t, protocol.ExtractNormalizedVersion("unknown tool without version digits"))
		assert.Empty(t, protocol.ExtractNormalizedVersion("version 01.02.03"))
	})

	t.Run("compare versions", func(t *testing.T) {
		assert.Equal(t, 0, protocol.CompareVersions("1.26.0", "1.26.0"))
		assert.Equal(t, 0, protocol.CompareVersions("1.2", "1.2.0.0"))
		assert.Equal(t, -1, protocol.CompareVersions("1.25.9", "1.26.0"))
		assert.Equal(t, 1, protocol.CompareVersions("1.26.1", "1.26.0"))
		assert.Equal(t, 1, protocol.CompareVersions("1.10.0", "1.2.0"))
		assert.Equal(t, -1, protocol.CompareVersions("1.2.0", "1.10.0"))
	})

	t.Run("parse version constraints and matching across comparators", func(t *testing.T) {
		equalConstraint, err := protocol.ParseVersionConstraint("=1.26.2")
		require.NoError(t, err)
		assert.Equal(t, "=", equalConstraint.Comparator)
		assert.Equal(t, "1.26.2", equalConstraint.Version)
		assert.True(t, protocol.SatisfiesVersionConstraint("1.26.2", equalConstraint))
		assert.False(t, protocol.SatisfiesVersionConstraint("1.26.1", equalConstraint))
		assert.False(t, protocol.SatisfiesVersionConstraint("1.26.3", equalConstraint))

		bareConstraint, err := protocol.ParseVersionConstraint("1.26.2")
		require.NoError(t, err)
		assert.Equal(t, "=", bareConstraint.Comparator)
		assert.Equal(t, "1.26.2", bareConstraint.Version)
		assert.True(t, protocol.SatisfiesVersionConstraint("1.26.2", bareConstraint))
		assert.False(t, protocol.SatisfiesVersionConstraint("1.26.1", bareConstraint))

		greaterOrEqualConstraint, err := protocol.ParseVersionConstraint(">=1.26")
		require.NoError(t, err)
		assert.Equal(t, ">=", greaterOrEqualConstraint.Comparator)
		assert.Equal(t, "1.26", greaterOrEqualConstraint.Version)
		assert.True(t, protocol.SatisfiesVersionConstraint("1.26.0", greaterOrEqualConstraint))
		assert.True(t, protocol.SatisfiesVersionConstraint("1.26.2", greaterOrEqualConstraint))
		assert.False(t, protocol.SatisfiesVersionConstraint("1.25.9", greaterOrEqualConstraint))

		greaterConstraint, err := protocol.ParseVersionConstraint(">1.26")
		require.NoError(t, err)
		assert.Equal(t, ">", greaterConstraint.Comparator)
		assert.Equal(t, "1.26", greaterConstraint.Version)
		assert.True(t, protocol.SatisfiesVersionConstraint("1.26.1", greaterConstraint))
		assert.False(t, protocol.SatisfiesVersionConstraint("1.26.0", greaterConstraint))
		assert.False(t, protocol.SatisfiesVersionConstraint("1.25.0", greaterConstraint))

		lessOrEqualConstraint, err := protocol.ParseVersionConstraint("<=2.0")
		require.NoError(t, err)
		assert.Equal(t, "<=", lessOrEqualConstraint.Comparator)
		assert.Equal(t, "2.0", lessOrEqualConstraint.Version)
		assert.True(t, protocol.SatisfiesVersionConstraint("2.0.0", lessOrEqualConstraint))
		assert.True(t, protocol.SatisfiesVersionConstraint("1.9.9", lessOrEqualConstraint))
		assert.False(t, protocol.SatisfiesVersionConstraint("2.0.1", lessOrEqualConstraint))

		lessConstraint, err := protocol.ParseVersionConstraint("<2.0")
		require.NoError(t, err)
		assert.Equal(t, "<", lessConstraint.Comparator)
		assert.Equal(t, "2.0", lessConstraint.Version)
		assert.True(t, protocol.SatisfiesVersionConstraint("1.9.9", lessConstraint))
		assert.False(t, protocol.SatisfiesVersionConstraint("2.0.0", lessConstraint))
		assert.False(t, protocol.SatisfiesVersionConstraint("2.1.0", lessConstraint))

		_, err = protocol.ParseVersionConstraint("~1.26.0")
		require.Error(t, err, "tilde prefix constraint should be rejected")

		_, err = protocol.ParseVersionConstraint("^1.26.0")
		require.Error(t, err, "caret prefix constraint should be rejected")

		malformedConstraints := []string{
			"",
			">",
			">=",
			"<",
			"<=",
			"=",
			"invalid-constraint",
			">=01.0",
			">=1.2.3.4.5",
			strings.Repeat("1", 41),
		}
		for _, rawConstraint := range malformedConstraints {
			_, err = protocol.ParseVersionConstraint(rawConstraint)
			require.Error(t, err, "expected error for constraint: %q", rawConstraint)
		}

		assert.False(t, protocol.SatisfiesVersionConstraint("not-a-version", greaterOrEqualConstraint))
		unknownComparatorConstraint := protocol.VersionConstraint{Comparator: "!=", Version: "1.0"}
		assert.False(t, protocol.SatisfiesVersionConstraint("1.0", unknownComparatorConstraint))
	})
}

func TestNodeCapabilityReportValidation(t *testing.T) {
	timestamp := protocol.NowTimestamp()

	validRuntimeEvidence := protocol.NodeCapabilityEvidence{
		CapabilityID:    "git",
		Source:          protocol.CapabilityEvidenceSourceRuntime,
		Success:         true,
		RawValue:        "git version 2.40.1",
		NormalizedValue: "2.40.1",
		ObservedAt:      timestamp,
	}

	validProbeEvidence := protocol.NodeCapabilityEvidence{
		CapabilityID:           "go-compiler",
		Source:                 protocol.CapabilityEvidenceSourceProbe,
		Success:                true,
		RawValue:               "go version go1.26.2 linux/amd64",
		NormalizedValue:        "1.26.2",
		ProbeDefinitionVersion: "1.0.0",
		ObservedAt:             timestamp,
	}

	require.NoError(t, validRuntimeEvidence.Validate())
	require.NoError(t, validProbeEvidence.Validate())

	t.Run("evidence validation errors", func(t *testing.T) {
		badCapabilityID := validRuntimeEvidence
		badCapabilityID.CapabilityID = "GIT_TOOL"
		require.Error(t, badCapabilityID.Validate())

		emptyCapabilityID := validRuntimeEvidence
		emptyCapabilityID.CapabilityID = ""
		require.Error(t, emptyCapabilityID.Validate())

		unknownSource := validRuntimeEvidence
		unknownSource.Source = "manual"
		require.Error(t, unknownSource.Validate())

		probeMissingVersion := validProbeEvidence
		probeMissingVersion.ProbeDefinitionVersion = ""
		require.Error(t, probeMissingVersion.Validate())

		runtimeWithProbeVersion := validRuntimeEvidence
		runtimeWithProbeVersion.ProbeDefinitionVersion = "1.0.0"
		require.Error(t, runtimeWithProbeVersion.Validate())

		oversizedRawValue := validRuntimeEvidence
		oversizedRawValue.RawValue = strings.Repeat("r", 257)
		require.Error(t, oversizedRawValue.Validate())

		oversizedDiagnostic := validRuntimeEvidence
		oversizedDiagnostic.Diagnostic = strings.Repeat("d", 513)
		require.Error(t, oversizedDiagnostic.Validate())

		badObservedAt := validRuntimeEvidence
		badObservedAt.ObservedAt = "bad-timestamp"
		require.Error(t, badObservedAt.Validate())
	})

	t.Run("report validation", func(t *testing.T) {
		validReport := protocol.NodeCapabilityReport{
			NodeID:           "node-1",
			ProjectAllowlist: []string{"project-alpha", "project-beta"},
			Evidence:         []protocol.NodeCapabilityEvidence{validRuntimeEvidence, validProbeEvidence},
			At:               timestamp,
		}
		require.NoError(t, validReport.Validate())

		missingNodeID := validReport
		missingNodeID.NodeID = ""
		require.Error(t, missingNodeID.Validate())

		badProjectID := validReport
		badProjectID.ProjectAllowlist = []string{"Project_Alpha"}
		require.Error(t, badProjectID.Validate())

		duplicateProjectID := validReport
		duplicateProjectID.ProjectAllowlist = []string{"project-alpha", "project-alpha"}
		require.Error(t, duplicateProjectID.Validate())

		duplicateEvidenceReport := validReport
		duplicateEvidenceReport.Evidence = []protocol.NodeCapabilityEvidence{
			validRuntimeEvidence,
			validRuntimeEvidence,
		}
		require.Error(t, duplicateEvidenceReport.Validate(), "duplicate capability evidence for same id and source")

		differentSourceReport := validReport
		configuredEvidence := validRuntimeEvidence
		configuredEvidence.Source = protocol.CapabilityEvidenceSourceConfigured
		differentSourceReport.Evidence = []protocol.NodeCapabilityEvidence{
			validRuntimeEvidence,
			configuredEvidence,
		}
		require.NoError(t, differentSourceReport.Validate(), "same capability via different sources is allowed")

		oversizedEvidence := make([]protocol.NodeCapabilityEvidence, 257)
		for index := range 257 {
			oversizedEvidence[index] = validRuntimeEvidence
		}
		tooManyEvidenceReport := validReport
		tooManyEvidenceReport.Evidence = oversizedEvidence
		require.Error(t, tooManyEvidenceReport.Validate())

		badTimestampReport := validReport
		badTimestampReport.At = "invalid-at"
		require.Error(t, badTimestampReport.Validate())
	})
}

func TestSecretDetectionAndRedaction(t *testing.T) {
	customToken := "orchestra-super-secret-token-999"
	longerCustomToken := "orchestra-super-secret-token-999-extended-privileges"
	redactor := protocol.NewRedactor(customToken, longerCustomToken)

	t.Run("secret like detection", func(t *testing.T) {
		assert.True(t, protocol.LooksSecretLike("sk-ant-api03-1234567890abcdef"))
		assert.True(t, protocol.LooksSecretLike("rk-live-1234567890abcdef"))
		assert.True(t, protocol.LooksSecretLike("pk_test_1234567890abcdef"))
		assert.True(t, protocol.LooksSecretLike("pk_live_1234567890abcdef"))
		assert.True(t, protocol.LooksSecretLike("ghp_1234567890abcdef"))
		assert.True(t, protocol.LooksSecretLike("github_pat_12345678901234567890"))
		assert.True(t, protocol.LooksSecretLike("xoxb-1234567890-abcdef"))
		assert.True(t, protocol.LooksSecretLike("AKIAIOSFODNN7EXAMPLE"))
		assert.True(t, protocol.LooksSecretLike("ASIAIOSFODNN7EXAMPLE"))
		assert.True(t, protocol.LooksSecretLike("glpat-1234567890abcdef"))
		assert.True(t, protocol.LooksSecretLike("AIzaSyA12345678901234567"))
		assert.True(t, protocol.LooksSecretLike("Authorization: Bearer abcdefghijklmnop"))
		assert.True(t, protocol.LooksSecretLike("-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----"))

		assert.False(t, protocol.LooksSecretLike(""))
		assert.False(t, protocol.LooksSecretLike("normal-label-123"))
		assert.False(t, protocol.LooksSecretLike("standard sentence with ordinary words"))
	})

	t.Run("redaction with overlapping secrets and bearer tokens", func(t *testing.T) {
		input := "access=" + longerCustomToken + " and fallback=" + customToken +
			" with Authorization: Bearer mysecrettokenvalue123 and Authorization: Basic dXNlcjpwYXNzMTIzNDU2"
		redacted := redactor.Redact(input)

		assert.NotContains(t, redacted, longerCustomToken)
		assert.NotContains(t, redacted, customToken)
		assert.NotContains(t, redacted, "mysecrettokenvalue123")
		assert.NotContains(t, redacted, "dXNlcjpwYXNzMTIzNDU2")
		assert.Contains(t, redacted, protocol.RedactionMarker)
		assert.Contains(t, redacted, "Authorization: Bearer "+protocol.RedactionMarker)
		assert.Contains(t, redacted, "Authorization: Basic "+protocol.RedactionMarker)
	})

	t.Run("empty and whitespace secret handling", func(t *testing.T) {
		emptyRedactor := protocol.NewRedactor("", "   ", "\t\n", "short")
		assert.Equal(t, "regular text", emptyRedactor.Redact("regular text"))
		assert.Empty(t, emptyRedactor.Redact(""))
		assert.False(t, emptyRedactor.ContainsSecret(""))
		assert.False(t, emptyRedactor.ContainsSecret("clean text without credentials"))
	})

	t.Run("contains secret check", func(t *testing.T) {
		assert.True(t, redactor.ContainsSecret("key: "+customToken))
		assert.True(t, redactor.ContainsSecret("sk-ant-api03-1234567890abcdef"))
		assert.False(t, redactor.ContainsSecret("just a regular string"))
	})

	t.Run("redact text convenience and scrub child environment", func(t *testing.T) {
		textWithSecret := "Token is ghp_1234567890abcdef"
		assert.NotContains(t, protocol.RedactText(textWithSecret), "ghp_1234567890abcdef")
		assert.Contains(t, protocol.RedactText(textWithSecret), protocol.RedactionMarker)

		cleanEnvironment := protocol.ScrubChildEnvironment([]string{
			"ORCHESTRA_TOKEN=secret-123",
			"orchestra_mcp_token=secret-456",
			"PATH=/usr/bin:/bin",
			"USER=worker",
		})
		assert.Equal(t, []string{"PATH=/usr/bin:/bin", "USER=worker"}, cleanEnvironment)
	})
}

func TestEventCursorChecksumsAndOwnership(t *testing.T) {
	cursor, err := protocol.EncodeEventCursor("thread-1", "orchestrator-client:main", 42)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(cursor, "tev1."))

	sequence, err := protocol.DecodeEventCursor(cursor, "thread-1", "orchestrator-client:main")
	require.NoError(t, err)
	assert.Equal(t, int64(42), sequence)

	_, err = protocol.DecodeEventCursor(cursor, "thread-2", "orchestrator-client:main")
	require.Error(t, err, "wrong thread must be rejected")

	_, err = protocol.DecodeEventCursor(cursor, "thread-1", "run:run-1")
	require.Error(t, err, "wrong reader key must be rejected")

	_, err = protocol.DecodeEventCursor(cursor+"bad", "thread-1", "orchestrator-client:main")
	require.Error(t, err, "tampered checksum must be rejected")

	tamperedPrefix := "tev2." + strings.TrimPrefix(cursor, "tev1.")
	_, err = protocol.DecodeEventCursor(tamperedPrefix, "thread-1", "orchestrator-client:main")
	require.Error(t, err, "tampered prefix must be rejected")

	_, err = protocol.DecodeEventCursor("tev1malformedcursorwithoutdot", "thread-1", "orchestrator-client:main")
	require.Error(t, err, "cursor without dot separator must be rejected")

	_, err = protocol.DecodeEventCursor("tev1.invalid-base64!."+"0123456789abcdef", "thread-1", "orchestrator-client:main")
	require.Error(t, err, "malformed base64 payload must be rejected")

	_, err = protocol.EncodeEventCursor(strings.Repeat("t", protocol.IdentifierMaximumBytes+1), "reader", 1)
	require.Error(t, err, "malformed thread ID must be rejected")

	_, err = protocol.EncodeEventCursor("thread-1", "", 1)
	require.Error(t, err, "empty reader key must be rejected")

	_, err = protocol.EncodeEventCursor("thread-1", "reader", -1)
	require.Error(t, err, "negative sequence must be rejected")
}

func TestRepositoryIdentityAndWorkspaceLeases(t *testing.T) {
	t.Run("repository identity normalization", func(t *testing.T) {
		identity, ok := protocol.NormalizeRepositoryIdentity("https://user:p@ss@github.com/cafecito-games/Orchestra.git/")
		require.True(t, ok)
		assert.Equal(t, "https://github.com/cafecito-games/Orchestra", identity)

		scpIdentity, ok := protocol.NormalizeRepositoryIdentity("git@github.com:cafecito-games/Orchestra.git")
		require.True(t, ok)
		assert.Equal(t, "github.com:cafecito-games/Orchestra", scpIdentity)

		_, ok = protocol.NormalizeRepositoryIdentity("https://sk-1234567890abcdef@github.com/sk-1234567890abcdef/repo")
		assert.False(t, ok, "secret-like username in URL must be rejected")
	})

	t.Run("valid git worktree grant", func(t *testing.T) {
		grant := protocol.WorkspaceLeaseGrant{
			ID:                   "lease-1",
			Status:               "requested",
			Policy:               protocol.WorkspaceIsolationGitWorktree,
			Cleanup:              protocol.WorkspaceCleanupRetain,
			Repository:           "https://github.com/cafecito-games/Orchestra",
			Root:                 "/workspaces",
			SourcePath:           "/workspaces/repo",
			BaseRevision:         protocol.WorkspaceLeaseBaseRef("main"),
			Branch:               protocol.WorkspaceLeaseBranch("task-1", "run-1"),
			WorktreePath:         protocol.WorkspaceLeaseWorktreePath("/workspaces", "lease-1"),
			ResolvedBaseRevision: strings.Repeat("a", 40),
		}
		require.NoError(t, grant.Validate())
		assert.Equal(t, []string{"provisioning", "active"}, protocol.WorkspaceLeasePath("requested", "active"))
		assert.Equal(t, []string{"released", "cleaning", "cleaned"}, protocol.WorkspaceLeasePath("active", "cleaned"))
		assert.Empty(t, protocol.WorkspaceLeasePath("active", "active"))
		assert.Empty(t, protocol.WorkspaceLeasePath("cleaned", "requested"))
	})

	t.Run("valid exclusive existing grant", func(t *testing.T) {
		exclusiveGrant := protocol.WorkspaceLeaseGrant{
			ID:           "lease-2",
			Status:       "active",
			Policy:       protocol.WorkspaceIsolationExclusiveExisting,
			Cleanup:      protocol.WorkspaceCleanupWhenUnchanged,
			Root:         "/workspaces",
			SourcePath:   "/workspaces/repo",
			WorktreePath: "/workspaces/repo",
		}
		require.NoError(t, exclusiveGrant.Validate())

		grantWithRepo := exclusiveGrant
		grantWithRepo.Repository = "https://github.com/cafecito-games/Orchestra"
		require.Error(t, grantWithRepo.Validate())

		grantWithBranch := exclusiveGrant
		grantWithBranch.Branch = "orchestra/task-1-run-1"
		require.Error(t, grantWithBranch.Validate())

		grantWithMismatchedPath := exclusiveGrant
		grantWithMismatchedPath.WorktreePath = "/workspaces/other"
		require.Error(t, grantWithMismatchedPath.Validate())
	})

	t.Run("invalid workspace lease grant fields", func(t *testing.T) {
		baseGrant := protocol.WorkspaceLeaseGrant{
			ID:                   "lease-1",
			Status:               "requested",
			Policy:               protocol.WorkspaceIsolationGitWorktree,
			Cleanup:              protocol.WorkspaceCleanupRetain,
			Repository:           "https://github.com/cafecito-games/Orchestra",
			Root:                 "/workspaces",
			SourcePath:           "/workspaces/repo",
			BaseRevision:         "refs/heads/main",
			Branch:               "orchestra/branch-1",
			WorktreePath:         "/workspaces/worktrees/lease-1",
			ResolvedBaseRevision: strings.Repeat("b", 40),
		}
		require.NoError(t, baseGrant.Validate())

		badID := baseGrant
		badID.ID = "Bad Lease ID!"
		require.Error(t, badID.Validate())

		badStatus := baseGrant
		badStatus.Status = "unknown"
		require.Error(t, badStatus.Validate())

		badPolicy := baseGrant
		badPolicy.Policy = "docker"
		require.Error(t, badPolicy.Validate())

		badCleanup := baseGrant
		badCleanup.Cleanup = "destroy"
		require.Error(t, badCleanup.Validate())

		relativeRoot := baseGrant
		relativeRoot.Root = "relative/workspaces"
		require.Error(t, relativeRoot.Validate())

		nullByteInPath := baseGrant
		nullByteInPath.WorktreePath = "/workspaces/\x00/lease"
		require.Error(t, nullByteInPath.Validate())

		badResolvedBaseRevision := baseGrant
		badResolvedBaseRevision.ResolvedBaseRevision = "not-a-40-or-64-character-hash"
		require.Error(t, badResolvedBaseRevision.Validate())

		unnormalizedRepo := baseGrant
		unnormalizedRepo.Repository = "https://github.com/cafecito-games/Orchestra/"
		require.Error(t, unnormalizedRepo.Validate())

		badBaseRevision := baseGrant
		badBaseRevision.BaseRevision = "main"
		require.Error(t, badBaseRevision.Validate())

		badBranchPrefix := baseGrant
		badBranchPrefix.Branch = "feature/mybranch"
		require.Error(t, badBranchPrefix.Validate())
	})

	t.Run("workspace lease update validation", func(t *testing.T) {
		validUpdate := protocol.WorkspaceLeaseUpdate{
			LeaseID: "lease-1",
			Status:  "active",
		}
		require.NoError(t, validUpdate.Validate())

		validRetained := protocol.WorkspaceLeaseUpdate{
			LeaseID:         "lease-1",
			Status:          "retained",
			RetentionReason: "policy",
		}
		require.NoError(t, validRetained.Validate())

		missingID := validUpdate
		missingID.LeaseID = ""
		require.Error(t, missingID.Validate())

		badStatus := validUpdate
		badStatus.Status = "unknown"
		require.Error(t, badStatus.Validate())

		retainedMissingReason := validRetained
		retainedMissingReason.RetentionReason = ""
		require.Error(t, retainedMissingReason.Validate())

		retainedBadReason := validRetained
		retainedBadReason.RetentionReason = "unknown-reason"
		require.Error(t, retainedBadReason.Validate())

		activeWithReason := validUpdate
		activeWithReason.RetentionReason = "policy"
		require.Error(t, activeWithReason.Validate())

		badResolvedRevision := validUpdate
		badResolvedRevision.ResolvedBaseRevision = "bad-revision"
		require.Error(t, badResolvedRevision.Validate())

		oversizedDetail := validUpdate
		oversizedDetail.Detail = strings.Repeat("x", protocol.DiagnosticMaximumBytes+1)
		require.Error(t, oversizedDetail.Validate())
	})
}

func TestDispatchSessionBinding_TransportValidation(t *testing.T) {
	cases := []struct {
		name      string
		binding   protocol.DispatchSessionBinding
		expectErr bool
	}{
		{
			name: "empty transport allowed for compatibility",
			binding: protocol.DispatchSessionBinding{
				ID:                "sbind-1",
				ProviderSessionID: "sess-123",
			},
			expectErr: false,
		},
		{
			name: "acp transport valid",
			binding: protocol.DispatchSessionBinding{
				ID:                "sbind-1",
				ProviderSessionID: "sess-acp-1",
				Transport:         protocol.TransportACP,
			},
			expectErr: false,
		},
		{
			name: "native transport valid",
			binding: protocol.DispatchSessionBinding{
				ID:                "sbind-1",
				ProviderSessionID: "sess-native-1",
				Transport:         protocol.TransportNativeCLI,
			},
			expectErr: false,
		},
		{
			name: "unknown transport rejected",
			binding: protocol.DispatchSessionBinding{
				ID:                "sbind-1",
				ProviderSessionID: "sess-1",
				Transport:         "stdio-raw",
			},
			expectErr: true,
		},
		{
			name: "missing id rejected",
			binding: protocol.DispatchSessionBinding{
				ID:                "",
				ProviderSessionID: "sess-1",
			},
			expectErr: true,
		},
		{
			name: "secret like id rejected",
			binding: protocol.DispatchSessionBinding{
				ID:                "sk-ant-api03-1234567890abcdef",
				ProviderSessionID: "sess-1",
			},
			expectErr: true,
		},
		{
			name: "oversized resume prompt rejected",
			binding: protocol.DispatchSessionBinding{
				ID:                "sbind-1",
				ProviderSessionID: "sess-1",
				ResumePrompt:      strings.Repeat("p", protocol.SessionResumePromptMaximumBytes+1),
			},
			expectErr: true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.binding.Validate()
			if testCase.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestComponentInventoryValidation(t *testing.T) {
	timestamp := protocol.NowTimestamp()

	validEntry := protocol.ComponentInventoryEntry{
		Kind:              "harness",
		ID:                "claude-cli",
		HarnessID:         "claude-cli",
		DeclaredVersion:   "1.0.0",
		InstalledVersions: []string{"1.0.0", "1.1.0"},
		ActiveVersion:     "1.1.0",
		RollbackVersion:   "1.0.0",
		RollbackAvailable: true,
		Provenance:        "managed",
		Readiness:         "ready",
		DiagnosticCodes:   []string{"not-activated"},
	}
	require.NoError(t, validEntry.Validate())

	badKind := validEntry
	badKind.Kind = "unknown-kind"
	require.Error(t, badKind.Validate())

	badID := validEntry
	badID.ID = "Invalid ID!"
	require.Error(t, badID.Validate())

	badDeclaredVersion := validEntry
	badDeclaredVersion.DeclaredVersion = "01.0.0"
	require.Error(t, badDeclaredVersion.Validate())

	unsortedInstalled := validEntry
	unsortedInstalled.InstalledVersions = []string{"1.1.0", "1.0.0"}
	require.Error(t, unsortedInstalled.Validate())

	rollbackConflict := validEntry
	rollbackConflict.RollbackAvailable = false
	require.Error(t, rollbackConflict.Validate())

	validReport := protocol.ComponentInventoryReport{
		NodeID:     "node-1",
		ObservedAt: timestamp,
		Components: []protocol.ComponentInventoryEntry{validEntry},
	}
	require.NoError(t, validReport.Validate())

	missingNode := validReport
	missingNode.NodeID = ""
	require.Error(t, missingNode.Validate())

	badObservedAt := validReport
	badObservedAt.ObservedAt = "bad-timestamp"
	require.Error(t, badObservedAt.Validate())

	duplicateEntryReport := validReport
	duplicateEntryReport.Components = []protocol.ComponentInventoryEntry{validEntry, validEntry}
	require.Error(t, duplicateEntryReport.Validate())
}

func TestTransportValidation(t *testing.T) {
	t.Run("ACP agent capabilities validation and resume check", func(t *testing.T) {
		validACP := protocol.AcpAgentCapabilities{
			ProtocolVersion: 1,
			LoadSession:     true,
			ResumeSession:   false,
			AdapterName:     "adapter-standard",
			AdapterVersion:  "1.0.0",
		}
		require.NoError(t, validACP.Validate())
		assert.True(t, validACP.SupportsResume())

		var nilACP *protocol.AcpAgentCapabilities
		assert.False(t, nilACP.SupportsResume())

		unsupportedVersion := validACP
		unsupportedVersion.ProtocolVersion = 2
		require.Error(t, unsupportedVersion.Validate())

		secretLikeAdapter := validACP
		secretLikeAdapter.AdapterName = "sk-ant-api03-1234567890abcdef"
		require.Error(t, secretLikeAdapter.Validate())

		badAdapterVersion := validACP
		badAdapterVersion.AdapterVersion = "bad.version"
		require.Error(t, badAdapterVersion.Validate())
	})

	t.Run("run transport selection validation", func(t *testing.T) {
		validSelection := protocol.RunTransportSelection{
			RequestedTransport: protocol.TransportACP,
			SelectedTransport:  protocol.TransportACP,
			HarnessVersion:     "1.0.0",
		}
		require.NoError(t, validSelection.Validate())

		fallbackSelection := protocol.RunTransportSelection{
			RequestedTransport: protocol.TransportACP,
			SelectedTransport:  protocol.TransportNativeCLI,
			FallbackReason:     protocol.FallbackACPAdapterUnavailable,
			HarnessVersion:     "1.0.0",
		}
		require.NoError(t, fallbackSelection.Validate())

		invalidFallbackReason := fallbackSelection
		invalidFallbackReason.FallbackReason = "unsupported-reason"
		require.Error(t, invalidFallbackReason.Validate())

		sameWithFallbackReason := validSelection
		sameWithFallbackReason.FallbackReason = protocol.FallbackACPAdapterUnavailable
		require.Error(t, sameWithFallbackReason.Validate())

		unknownTransport := validSelection
		unknownTransport.SelectedTransport = "stdio-transport"
		require.Error(t, unknownTransport.Validate())
	})

	t.Run("compute node validation", func(t *testing.T) {
		timestamp := protocol.NowTimestamp()
		capacity := 4
		activeInstances := 2

		validNode := protocol.ComputeNode{
			ID:               "node-1",
			Name:             "Node Primary",
			Kind:             "local",
			Platform:         "linux-amd64",
			Status:           "online",
			LastSeen:         timestamp,
			ActiveRuns:       1,
			Concurrency:      8,
			InstanceCapacity: &capacity,
			ActiveInstances:  &activeInstances,
			WorkspaceRoots:   []string{"/workspaces"},
			Version:          "0.1.0",
			Harnesses: []protocol.HarnessProfile{
				{
					ID:          protocol.HarnessClaudeCLI,
					Label:       "Claude CLI",
					Description: "Claude Code CLI harness",
					Available:   true,
					AuthMode:    "local-subscription",
					Models:      []string{"claude-3-5-sonnet"},
					Transports:  []string{protocol.TransportNativeCLI},
				},
			},
		}
		require.NoError(t, validNode.Validate())

		missingID := validNode
		missingID.ID = ""
		require.Error(t, missingID.Validate())

		unknownKind := validNode
		unknownKind.Kind = "quantum"
		require.Error(t, unknownKind.Validate())

		unknownStatus := validNode
		unknownStatus.Status = "sleeping"
		require.Error(t, unknownStatus.Validate())

		excessInstances := validNode
		overactive := 5
		excessInstances.ActiveInstances = &overactive
		require.Error(t, excessInstances.Validate())

		nonCanonicalRoot := validNode
		nonCanonicalRoot.WorkspaceRoots = []string{"/workspaces/../workspaces"}
		require.Error(t, nonCanonicalRoot.Validate())
	})
}

func TestVersionAndPathUtilities(t *testing.T) {
	t.Run("identifiers and timestamps", func(t *testing.T) {
		id := protocol.NewIdentifier("task")
		assert.True(t, strings.HasPrefix(id, "task-"))
		assert.True(t, protocol.IsIdentifier(id))

		roomMsgID := protocol.NewRoomMessageIdentifier()
		assert.True(t, strings.HasPrefix(roomMsgID, "rmsg_"))
		assert.True(t, protocol.IsIdentifier(roomMsgID))

		timestamp := protocol.NowTimestamp()
		assert.True(t, protocol.IsTimestamp(timestamp))
		parsedTime, ok := protocol.ParseTimestamp(timestamp)
		assert.True(t, ok)
		assert.False(t, parsedTime.IsZero())

		_, ok = protocol.ParseTimestamp("not-a-timestamp")
		assert.False(t, ok)
	})

	t.Run("string truncation utilities", func(t *testing.T) {
		assert.Equal(t, "hello", protocol.TruncateUTF8Head("hello world", 5))
		assert.Equal(t, "world", protocol.TruncateUTF8Tail("hello world", 5))
		assert.Equal(t, "short", protocol.TruncateUTF8Head("short", 10))
		assert.Equal(t, "short", protocol.TruncateUTF8Tail("short", 10))
		assert.Equal(t, "long", protocol.TruncateField("long value", 4))
		assert.Equal(t, "diag", protocol.TruncateDiagnostic("diag"))
		assert.Equal(t, "bounded text", protocol.BoundedTextValue("bounded text", 20))
		assert.Equal(t, "diag info", protocol.BoundedDiagnostic("diag info"))
	})

	t.Run("workspace path containment", func(t *testing.T) {
		assert.True(t, protocol.IsCanonicalWorkspacePath("/workspaces/repo"))
		assert.False(t, protocol.IsCanonicalWorkspacePath("/workspaces/repo/"))
		assert.False(t, protocol.IsCanonicalWorkspacePath("workspaces/repo"))
		assert.False(t, protocol.IsCanonicalWorkspacePath("/workspaces/../repo"))

		assert.True(t, protocol.IsWorkspaceWithinRoot("/workspaces/repo", "/workspaces"))
		assert.True(t, protocol.IsWorkspaceWithinRoot("/workspaces", "/workspaces"))
		assert.False(t, protocol.IsWorkspaceWithinRoot("/other/repo", "/workspaces"))
		assert.False(t, protocol.IsWorkspaceWithinRoot("relative", "/workspaces"))
	})

	t.Run("sorted unique strings", func(t *testing.T) {
		input := []string{"charlie", "alpha", "bravo", "alpha", "charlie"}
		unique := protocol.SortedUniqueStrings(input)
		assert.Equal(t, []string{"alpha", "bravo", "charlie"}, unique)
	})

	t.Run("tool names recognition", func(t *testing.T) {
		assert.True(t, protocol.IsHubToolName("get_task_context"))
		assert.False(t, protocol.IsHubToolName("unknown_tool"))

		assert.True(t, protocol.IsDelegationHubToolName("submit_tasks"))
		assert.False(t, protocol.IsDelegationHubToolName("get_task_context"))

		assert.True(t, protocol.IsOrchestratorToolName("create_thread"))
		assert.False(t, protocol.IsOrchestratorToolName("unknown_tool"))
	})

	t.Run("domain room and source key helpers", func(t *testing.T) {
		roomDir := protocol.RoomDirectoryName("thread-alpha", "2026-10-31T12:00:00Z")
		assert.Equal(t, "room_20261031_alpha", roomDir)

		runCreator := protocol.InstanceCreator{Kind: "run", RunID: "run-1"}
		assert.Equal(t, "run:run-1", runCreator.SourceKey())

		operatorCreator := protocol.InstanceCreator{Kind: "operator", OperatorID: "op-1"}
		assert.Equal(t, "operator:op-1", operatorCreator.SourceKey())

		clientCreator := protocol.InstanceCreator{Kind: "orchestrator-client", ClientID: "client-1"}
		assert.Equal(t, "orchestrator-client:client-1", clientCreator.SourceKey())
	})
}

func TestResolveBuildInfo(t *testing.T) {
	testCases := []struct {
		name              string
		rawVersion        string
		rawCommit         string
		rawDate           string
		rawTreeState      string
		goVersion         string
		embeddedBuildInfo *debug.BuildInfo
		expected          protocol.BuildInfo
		expectedText      string
	}{
		{
			name:         "linker injected clean release metadata",
			rawVersion:   "v1.4.2",
			rawCommit:    "0123456789abcdef0123456789abcdef01234567",
			rawDate:      "2026-10-31T13:30:00Z",
			rawTreeState: "clean",
			goVersion:    "go1.26.9",
			embeddedBuildInfo: &debug.BuildInfo{
				Main: debug.Module{Version: "(devel)"},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "ignored"},
					{Key: "vcs.modified", Value: "true"},
				},
			},
			expected: protocol.BuildInfo{
				Version:         "1.4.2",
				ProtocolVersion: protocol.ProtocolVersion,
				Commit:          "0123456789abcdef0123456789abcdef01234567",
				Date:            "2026-10-31T13:30:00Z",
				TreeState:       "clean",
				GoVersion:       "go1.26.9",
			},
			expectedText: "orchestra 1.4.2 (protocol v1, commit 0123456789ab, built 2026-10-31T13:30:00Z)",
		},
		{
			name:       "vcs fallback for go install module version and dirty tree",
			rawVersion: "dev",
			goVersion:  "go1.26.9",
			embeddedBuildInfo: &debug.BuildInfo{
				Main: debug.Module{Version: "v0.3.1"},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "fedcba9876543210"},
					{Key: "vcs.time", Value: "2026-10-31T10:00:00Z"},
					{Key: "vcs.modified", Value: "true"},
				},
			},
			expected: protocol.BuildInfo{
				Version:         "0.3.1",
				ProtocolVersion: protocol.ProtocolVersion,
				Commit:          "fedcba9876543210",
				Date:            "2026-10-31T10:00:00Z",
				TreeState:       "dirty",
				GoVersion:       "go1.26.9",
			},
			expectedText: "orchestra 0.3.1 (protocol v1, commit fedcba987654, dirty, built 2026-10-31T10:00:00Z)",
		},
		{
			name:       "minimal fallback without vcs metadata",
			rawVersion: "(devel)",
			goVersion:  "go1.26.9",
			expected: protocol.BuildInfo{
				Version:         "dev",
				ProtocolVersion: protocol.ProtocolVersion,
				GoVersion:       "go1.26.9",
			},
			expectedText: "orchestra dev (protocol v1)",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			resolved := protocol.ResolveBuildInfo(
				testCase.rawVersion,
				testCase.rawCommit,
				testCase.rawDate,
				testCase.rawTreeState,
				testCase.goVersion,
				testCase.embeddedBuildInfo,
			)
			assert.Equal(t, testCase.expected, resolved)
			assert.Equal(t, testCase.expectedText, resolved.String())
		})
	}
}
