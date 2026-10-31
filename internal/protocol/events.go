package protocol

import (
	"encoding/json"
	"fmt"
	"slices"
	"unicode/utf8"
)

const (
	SessionResumePromptMaximumBytes = 64 * 1024
	SessionResumeUnavailableReason  = "unsupported execution: session resume not available for this harness on this node"

	PlanEntryLimit      = 200
	ApprovalOptionLimit = 8
)

var (
	HarnessEventTypes = []string{
		"message.delta",
		"thought.delta",
		"plan.updated",
		"tool.call",
		"diff",
		"terminal.output",
		"usage",
		"permission.requested",
		"permission.resolved",
		"warning",
		"unknown",
	}
	ApprovalStatuses       = []string{"pending", "approved", "rejected", "cancelled", "expired"}
	ApprovalOptionKinds    = []string{"allow-once", "allow-always", "reject-once", "reject-always"}
	ApprovalDeliveryStates = []string{"pending", "sent", "applied", "not-applied"}
	SessionBindingStatuses = []string{"active", "idle", "closed", "replaced", "failed"}
	PlanEntryStatuses      = []string{"pending", "in-progress", "completed"}
	PlanEntryPriorities    = []string{"high", "medium", "low"}
	ToolCallStatuses       = []string{"pending", "in-progress", "completed", "failed"}
	ToolCallKinds          = []string{"read", "edit", "delete", "move", "search", "execute", "think", "fetch", "other"}
)

var sessionBindingTransitions = map[string][]string{
	"active":   {"idle", "closed", "replaced", "failed"},
	"idle":     {"active", "closed", "replaced", "failed"},
	"closed":   {},
	"replaced": {},
	"failed":   {},
}

func CanTransitionSessionBinding(fromStatus, toStatus string) bool {
	return slices.Contains(sessionBindingTransitions[fromStatus], toStatus)
}

func IsTerminalSessionBindingStatus(status string) bool {
	return status == "closed" || status == "replaced" || status == "failed"
}

var approvalTransitions = map[string][]string{
	"pending":   {"approved", "rejected", "cancelled", "expired"},
	"approved":  {},
	"rejected":  {},
	"cancelled": {},
	"expired":   {},
}

func CanTransitionApproval(fromStatus, toStatus string) bool {
	return slices.Contains(approvalTransitions[fromStatus], toStatus)
}

func IsTerminalApprovalStatus(status string) bool {
	return status != "pending" && slices.Contains(ApprovalStatuses, status)
}

type DispatchSessionBinding struct {
	ID                string `json:"id"`
	ProviderSessionID string `json:"providerSessionId"`
	Transport         string `json:"transport,omitempty"`
	ResumePrompt      string `json:"resumePrompt,omitempty"`
}

func (binding DispatchSessionBinding) Validate() error {
	if !IsIdentifier(binding.ID) || !IsIdentifier(binding.ProviderSessionID) {
		return fmt.Errorf("session binding is missing identity")
	}
	if LooksSecretLike(binding.ID) || LooksSecretLike(binding.ProviderSessionID) {
		return fmt.Errorf("session binding identity looks like a credential")
	}
	if binding.Transport != "" && !slices.Contains(HarnessTransports, binding.Transport) {
		return fmt.Errorf("session binding has an unknown transport")
	}
	if len(binding.ResumePrompt) > SessionResumePromptMaximumBytes || !utf8.ValidString(binding.ResumePrompt) {
		return fmt.Errorf("session binding resume prompt exceeds its bound")
	}
	return nil
}

type SessionBindingUpdate struct {
	BindingID         string `json:"bindingId,omitempty"`
	ProviderSessionID string `json:"providerSessionId"`
	HarnessID         string `json:"harnessId"`
	Transport         string `json:"transport"`
	Status            string `json:"status"`
}

func (binding SessionBindingUpdate) Validate() error {
	if (binding.BindingID != "" && !IsIdentifier(binding.BindingID)) || !IsIdentifier(binding.ProviderSessionID) {
		return fmt.Errorf("session binding is missing identity")
	}
	if !slices.Contains(HarnessIDs, binding.HarnessID) || !slices.Contains(HarnessTransports, binding.Transport) || !slices.Contains(SessionBindingStatuses, binding.Status) {
		return fmt.Errorf("session binding has an unknown harness, transport, or status")
	}
	return nil
}

type HarnessSessionBinding struct {
	ID                  string                `json:"id"`
	ThreadID            string                `json:"threadId"`
	InstanceID          string                `json:"instanceId"`
	AllocationID        string                `json:"allocationId"`
	NodeID              string                `json:"nodeId"`
	HarnessID           string                `json:"harnessId"`
	Transport           string                `json:"transport"`
	Workspace           string                `json:"workspace"`
	WorkspaceLeaseID    string                `json:"workspaceLeaseId,omitempty"`
	ProviderSessionID   string                `json:"providerSessionId"`
	Capabilities        *AcpAgentCapabilities `json:"capabilities,omitempty"`
	Status              string                `json:"status"`
	CreatedByRunID      string                `json:"createdByRunId"`
	LastRunID           string                `json:"lastRunId"`
	ReplacedByBindingID string                `json:"replacedByBindingId,omitempty"`
	CreatedAt           string                `json:"createdAt"`
	UpdatedAt           string                `json:"updatedAt"`
}

type PlanEntry struct {
	Content  string `json:"content"`
	Status   string `json:"status"`
	Priority string `json:"priority"`
}

type ApprovalOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Kind  string `json:"kind"`
}

type ApprovalActor struct {
	Kind       string `json:"kind"`
	OperatorID string `json:"operatorId,omitempty"`
	ClientID   string `json:"clientId,omitempty"`
	RunID      string `json:"runId,omitempty"`
}

type ApprovalRequest struct {
	ID                 string           `json:"id"`
	RunID              string           `json:"runId"`
	ThreadID           string           `json:"threadId,omitempty"`
	TaskID             string           `json:"taskId,omitempty"`
	InstanceID         string           `json:"instanceId"`
	AllocationID       string           `json:"allocationId"`
	NodeID             string           `json:"nodeId"`
	ToolCallID         string           `json:"toolCallId,omitempty"`
	Title              string           `json:"title"`
	Detail             string           `json:"detail,omitempty"`
	Options            []ApprovalOption `json:"options"`
	Status             string           `json:"status"`
	SelectedOptionID   string           `json:"selectedOptionId,omitempty"`
	ResolvedBy         *ApprovalActor   `json:"resolvedBy,omitempty"`
	DeliveryState      string           `json:"deliveryState"`
	UndeliverableError string           `json:"undeliverableError,omitempty"`
	RequestedAt        string           `json:"requestedAt"`
	ExpiresAt          string           `json:"expiresAt"`
	ResolvedAt         string           `json:"resolvedAt,omitempty"`
	UpdatedAt          string           `json:"updatedAt"`
}

type ApprovalDecision struct {
	ApprovalID       string `json:"approvalId"`
	RunID            string `json:"runId"`
	Status           string `json:"status"`
	SelectedOptionID string `json:"selectedOptionId,omitempty"`
}

func (decision ApprovalDecision) Validate() error {
	if !IsIdentifier(decision.ApprovalID) || !IsIdentifier(decision.RunID) {
		return fmt.Errorf("approval decision is missing identity")
	}
	if !slices.Contains(ApprovalStatuses, decision.Status) || decision.Status == "pending" {
		return fmt.Errorf("approval decision status must be terminal")
	}
	selectsOption := decision.Status == "approved" || decision.Status == "rejected"
	if selectsOption && !IsIdentifier(decision.SelectedOptionID) {
		return fmt.Errorf("approval decision option does not match its status")
	}
	if !selectsOption && decision.SelectedOptionID != "" {
		return fmt.Errorf("approval decision option does not match its status")
	}
	return nil
}

type HarnessEvent struct {
	Type              string           `json:"type"`
	RunID             string           `json:"runId"`
	Sequence          int64            `json:"sequence"`
	At                string           `json:"at"`
	Text              string           `json:"text,omitempty"`
	Entries           []PlanEntry      `json:"entries,omitempty"`
	ToolCallID        string           `json:"toolCallId,omitempty"`
	Status            string           `json:"status,omitempty"`
	Kind              string           `json:"kind,omitempty"`
	Title             string           `json:"title,omitempty"`
	Detail            string           `json:"detail,omitempty"`
	Path              string           `json:"path,omitempty"`
	OldText           string           `json:"oldText,omitempty"`
	NewText           string           `json:"newText,omitempty"`
	TerminalID        string           `json:"terminalId,omitempty"`
	Stream            string           `json:"stream,omitempty"`
	InputTokens       *int64           `json:"inputTokens,omitempty"`
	OutputTokens      *int64           `json:"outputTokens,omitempty"`
	CachedInputTokens *int64           `json:"cachedInputTokens,omitempty"`
	CostUSD           *float64         `json:"costUsd,omitempty"`
	ApprovalID        string           `json:"approvalId,omitempty"`
	Options           []ApprovalOption `json:"options,omitempty"`
	SelectedOptionID  string           `json:"selectedOptionId,omitempty"`
	Code              string           `json:"code,omitempty"`
	Message           string           `json:"message,omitempty"`
	SourceType        string           `json:"sourceType,omitempty"`
}

func isNonNegativeCount(value *int64) bool {
	return value == nil || *value >= 0
}

func isNonNegativeAmount(value *float64) bool {
	return value == nil || *value >= 0
}

func (event HarnessEvent) Validate() error {
	if !slices.Contains(HarnessEventTypes, event.Type) {
		return fmt.Errorf("harness event type is missing or unknown: %q", event.Type)
	}
	if !IsIdentifier(event.RunID) || event.Sequence < 0 || !IsTimestamp(event.At) {
		return fmt.Errorf("%s is missing run identity, sequence, or timestamp", event.Type)
	}
	switch event.Type {
	case "message.delta", "thought.delta":
		if !IsBoundedString(event.Text, TextMaximumBytes) {
			return fmt.Errorf("%s text exceeds its bound", event.Type)
		}
	case "plan.updated":
		if len(event.Entries) > PlanEntryLimit {
			return fmt.Errorf("plan.updated entries exceed their bound")
		}
		for _, entry := range event.Entries {
			if !IsBoundedString(entry.Content, TextMaximumBytes) || !slices.Contains(PlanEntryStatuses, entry.Status) || !slices.Contains(PlanEntryPriorities, entry.Priority) {
				return fmt.Errorf("plan.updated entry is malformed")
			}
		}
	case "tool.call":
		if !IsIdentifier(event.ToolCallID) || !slices.Contains(ToolCallStatuses, event.Status) || !slices.Contains(ToolCallKinds, event.Kind) || !IsBoundedString(event.Title, DiagnosticMaximumBytes) || !IsBoundedString(event.Detail, TextMaximumBytes) {
			return fmt.Errorf("tool.call payload is malformed or exceeds its bounds")
		}
	case "diff":
		if !IsBoundedString(event.Path, TextMaximumBytes) || !IsBoundedString(event.OldText, DiffMaximumBytes) || !IsBoundedString(event.NewText, DiffMaximumBytes) || (event.ToolCallID != "" && !IsIdentifier(event.ToolCallID)) {
			return fmt.Errorf("diff payload is malformed or exceeds its bounds")
		}
	case "terminal.output":
		if !IsIdentifier(event.TerminalID) || (event.Stream != "stdout" && event.Stream != "stderr") || !IsBoundedString(event.Text, TextMaximumBytes) {
			return fmt.Errorf("terminal.output payload is malformed or exceeds its bounds")
		}
	case "usage":
		if !isNonNegativeCount(event.InputTokens) || !isNonNegativeCount(event.OutputTokens) || !isNonNegativeCount(event.CachedInputTokens) || !isNonNegativeAmount(event.CostUSD) {
			return fmt.Errorf("usage payload is malformed")
		}
	case "permission.requested":
		if len(event.Options) == 0 || len(event.Options) > ApprovalOptionLimit {
			return fmt.Errorf("permission.requested options are missing or exceed their bound")
		}
		seenIdentifiers := make(map[string]bool, len(event.Options))
		for _, option := range event.Options {
			if !IsIdentifier(option.ID) || !IsBoundedString(option.Label, DiagnosticMaximumBytes) || !slices.Contains(ApprovalOptionKinds, option.Kind) {
				return fmt.Errorf("permission.requested option is malformed")
			}
			if seenIdentifiers[option.ID] {
				return fmt.Errorf("permission.requested options contain a duplicate identifier")
			}
			seenIdentifiers[option.ID] = true
		}
		if !IsIdentifier(event.ApprovalID) || (event.ToolCallID != "" && !IsIdentifier(event.ToolCallID)) || !IsBoundedString(event.Title, DiagnosticMaximumBytes) || !IsBoundedString(event.Detail, TextMaximumBytes) {
			return fmt.Errorf("permission.requested payload is malformed or exceeds its bounds")
		}
	case "permission.resolved":
		if !IsIdentifier(event.ApprovalID) || !slices.Contains(ApprovalStatuses, event.Status) || event.Status == "pending" || (event.SelectedOptionID != "" && !IsIdentifier(event.SelectedOptionID)) {
			return fmt.Errorf("permission.resolved payload is malformed")
		}
	case "warning":
		if !IsIdentifier(event.Code) || !IsBoundedString(event.Message, DiagnosticMaximumBytes) {
			return fmt.Errorf("warning payload is malformed or exceeds its bounds")
		}
	case "unknown":
		if !IsIdentifier(event.SourceType) || !IsBoundedString(event.Detail, DiagnosticMaximumBytes) {
			return fmt.Errorf("unknown payload is malformed or exceeds its bounds")
		}
	}
	return nil
}

var harnessEventPayloadKeys = map[string]struct{ required, optional []string }{
	"message.delta":        {required: []string{"text"}},
	"thought.delta":        {required: []string{"text"}},
	"plan.updated":         {required: []string{"entries"}},
	"tool.call":            {required: []string{"toolCallId", "status", "kind", "title"}, optional: []string{"detail"}},
	"diff":                 {required: []string{"path", "newText"}, optional: []string{"toolCallId", "oldText"}},
	"terminal.output":      {required: []string{"terminalId", "stream", "text"}},
	"usage":                {optional: []string{"inputTokens", "outputTokens", "cachedInputTokens", "costUsd"}},
	"permission.requested": {required: []string{"approvalId", "title", "options"}, optional: []string{"toolCallId", "detail"}},
	"permission.resolved":  {required: []string{"approvalId", "status"}, optional: []string{"selectedOptionId"}},
	"warning":              {required: []string{"code", "message"}},
	"unknown":              {required: []string{"sourceType"}, optional: []string{"detail"}},
}

func (event HarnessEvent) payloadValues() map[string]any {
	entries := event.Entries
	if entries == nil {
		entries = []PlanEntry{}
	}
	options := event.Options
	if options == nil {
		options = []ApprovalOption{}
	}
	values := map[string]any{
		"text":             event.Text,
		"entries":          entries,
		"toolCallId":       event.ToolCallID,
		"status":           event.Status,
		"kind":             event.Kind,
		"title":            event.Title,
		"detail":           event.Detail,
		"path":             event.Path,
		"oldText":          event.OldText,
		"newText":          event.NewText,
		"terminalId":       event.TerminalID,
		"stream":           event.Stream,
		"approvalId":       event.ApprovalID,
		"options":          options,
		"selectedOptionId": event.SelectedOptionID,
		"code":             event.Code,
		"message":          event.Message,
		"sourceType":       event.SourceType,
	}
	if event.InputTokens != nil {
		values["inputTokens"] = *event.InputTokens
	}
	if event.OutputTokens != nil {
		values["outputTokens"] = *event.OutputTokens
	}
	if event.CachedInputTokens != nil {
		values["cachedInputTokens"] = *event.CachedInputTokens
	}
	if event.CostUSD != nil {
		values["costUsd"] = *event.CostUSD
	}
	return values
}

func (event HarnessEvent) MarshalJSON() ([]byte, error) {
	keys, known := harnessEventPayloadKeys[event.Type]
	if !known {
		return nil, fmt.Errorf("harness event type is missing or unknown: %q", event.Type)
	}
	values := event.payloadValues()
	encoded := map[string]any{
		"type":     event.Type,
		"runId":    event.RunID,
		"sequence": event.Sequence,
		"at":       event.At,
	}
	for _, key := range keys.required {
		encoded[key] = values[key]
	}
	for _, key := range keys.optional {
		if value, present := values[key]; present && value != "" {
			encoded[key] = value
		}
	}
	return json.Marshal(encoded)
}

type BoundedText struct {
	Text           string `json:"text"`
	TruncatedBytes int    `json:"truncatedBytes"`
}

type ProjectedToolCall struct {
	ToolCallID string `json:"toolCallId"`
	Status     string `json:"status"`
	Kind       string `json:"kind"`
	Title      string `json:"title"`
	Detail     string `json:"detail,omitempty"`
	UpdatedAt  string `json:"updatedAt"`
}

type ProjectedDiff struct {
	Path       string `json:"path"`
	ToolCallID string `json:"toolCallId,omitempty"`
	OldText    string `json:"oldText,omitempty"`
	NewText    string `json:"newText"`
	Truncated  bool   `json:"truncated"`
	Sequence   int64  `json:"sequence"`
	At         string `json:"at"`
}

type ProjectedTerminal struct {
	TerminalID string      `json:"terminalId"`
	Stdout     BoundedText `json:"stdout"`
	Stderr     BoundedText `json:"stderr"`
	UpdatedAt  string      `json:"updatedAt"`
}

type ProjectedUsage struct {
	InputTokens       *int64   `json:"inputTokens,omitempty"`
	OutputTokens      *int64   `json:"outputTokens,omitempty"`
	CachedInputTokens *int64   `json:"cachedInputTokens,omitempty"`
	CostUSD           *float64 `json:"costUsd,omitempty"`
	UpdatedAt         string   `json:"updatedAt"`
}

type ProjectedWarning struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	Sequence int64  `json:"sequence"`
	At       string `json:"at"`
}

type OmittedCounts struct {
	ToolCalls int `json:"toolCalls"`
	Diffs     int `json:"diffs"`
	Terminals int `json:"terminals"`
	Warnings  int `json:"warnings"`
}

type RunActivity struct {
	RunID          string              `json:"runId"`
	NodeID         string              `json:"nodeId"`
	ThreadID       string              `json:"threadId,omitempty"`
	StreamStatus   string              `json:"streamStatus"`
	StreamFailure  string              `json:"streamFailure,omitempty"`
	LastSequence   int64               `json:"lastSequence"`
	AcceptedEvents int                 `json:"acceptedEvents"`
	Message        BoundedText         `json:"message"`
	Thought        BoundedText         `json:"thought"`
	Plan           []PlanEntry         `json:"plan"`
	ToolCalls      []ProjectedToolCall `json:"toolCalls"`
	Diffs          []ProjectedDiff     `json:"diffs"`
	Terminals      []ProjectedTerminal `json:"terminals"`
	Usage          *ProjectedUsage     `json:"usage,omitempty"`
	Warnings       []ProjectedWarning  `json:"warnings"`
	UnknownEvents  int                 `json:"unknownEvents"`
	Omitted        OmittedCounts       `json:"omitted"`
	Summary        string              `json:"summary"`
	UpdatedAt      string              `json:"updatedAt"`
}

type RunTranscriptEntry struct {
	Kind           string      `json:"kind"`
	ID             int64       `json:"id"`
	At             string      `json:"at"`
	UpdatedAt      string      `json:"updatedAt"`
	Text           string      `json:"text,omitempty"`
	TruncatedBytes int         `json:"truncatedBytes,omitempty"`
	TerminalID     string      `json:"terminalId,omitempty"`
	Stream         string      `json:"stream,omitempty"`
	Entries        []PlanEntry `json:"entries,omitempty"`
	ToolCallID     string      `json:"toolCallId,omitempty"`
	Status         string      `json:"status,omitempty"`
	ToolKind       string      `json:"toolKind,omitempty"`
	Title          string      `json:"title,omitempty"`
	Detail         string      `json:"detail,omitempty"`
	Path           string      `json:"path,omitempty"`
	OldText        string      `json:"oldText,omitempty"`
	NewText        string      `json:"newText,omitempty"`
	Truncated      bool        `json:"truncated,omitempty"`
	ApprovalID     string      `json:"approvalId,omitempty"`
	Code           string      `json:"code,omitempty"`
	Message        string      `json:"message,omitempty"`
}

type RunTranscript struct {
	RunID          string               `json:"runId"`
	ThreadID       string               `json:"threadId,omitempty"`
	LastSequence   int64                `json:"lastSequence"`
	Entries        []RunTranscriptEntry `json:"entries"`
	OmittedEntries int                  `json:"omittedEntries"`
	UpdatedAt      string               `json:"updatedAt"`
}
