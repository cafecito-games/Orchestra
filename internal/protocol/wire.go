package protocol

import (
	"encoding/json"
	"slices"
)

// HubToolNames are the worker MCP tools exposed on the slave's ephemeral `orchestra_hub` server.
var HubToolNames = []string{
	"get_task_context",
	"send_task_message",
	"wait_for_task_events",
	"update_task",
	"post_artifact",
	"update_thread",
	"get_execution_inventory",
	"submit_tasks",
	"delegate_task",
	"spawn_instance",
	"get_instance",
	"renew_instance",
	"release_instance",
}

// DelegationHubToolNames are the subset of HubToolNames gated on CanDelegate == true.
var DelegationHubToolNames = []string{
	"get_execution_inventory",
	"submit_tasks",
	"delegate_task",
	"spawn_instance",
	"get_instance",
	"renew_instance",
	"release_instance",
}

// OrchestratorToolNames are the 17 tools exposed on the master's orchestrator MCP surface.
var OrchestratorToolNames = []string{
	"get_execution_inventory",
	"create_thread",
	"list_threads",
	"attach_thread",
	"detach_thread",
	"get_thread_context",
	"get_thread_events",
	"submit_tasks",
	"update_task",
	"send_task_message",
	"update_thread",
	"post_artifact",
	"spawn_instance",
	"get_instance",
	"renew_instance",
	"release_instance",
	"list_approvals",
	"resolve_approval",
}

func IsHubToolName(name string) bool {
	return slices.Contains(HubToolNames, name)
}

func IsDelegationHubToolName(name string) bool {
	return slices.Contains(DelegationHubToolNames, name)
}

func IsOrchestratorToolName(name string) bool {
	return slices.Contains(OrchestratorToolNames, name)
}

// Control-plane messages sent from a slave node to the master `/control-agent` endpoint.
type SlaveControlMessage struct {
	Type              string                    `json:"type"`
	ProtocolVersion   string                    `json:"protocolVersion,omitempty"`
	Node              *ComputeNode              `json:"node,omitempty"`
	NodeID            string                    `json:"nodeId,omitempty"`
	RunID             string                    `json:"runId,omitempty"`
	InstanceID        string                    `json:"instanceId,omitempty"`
	AllocationID      string                    `json:"allocationId,omitempty"`
	ApprovalID        string                    `json:"approvalId,omitempty"`
	ActiveRuns        *int                      `json:"activeRuns,omitempty"`
	ActiveInstances   *int                      `json:"activeInstances,omitempty"`
	ActiveRunIDs      *[]string                 `json:"activeRunIds,omitempty"`
	ActiveInstanceIDs *[]string                 `json:"activeInstanceIds,omitempty"`
	Chunk             *string                   `json:"chunk,omitempty"`
	Output            *string                   `json:"output,omitempty"`
	Error             *string                   `json:"error,omitempty"`
	Reason            string                    `json:"reason,omitempty"`
	ProviderSessionID string                    `json:"providerSessionId,omitempty"`
	Transport         *RunTransportSelection    `json:"transport,omitempty"`
	Event             *HarnessEvent             `json:"event,omitempty"`
	Binding           *SessionBindingUpdate     `json:"binding,omitempty"`
	Lease             *WorkspaceLeaseUpdate     `json:"lease,omitempty"`
	CapabilityReport  *NodeCapabilityReport     `json:"report,omitempty"`
	ComponentReport   *ComponentInventoryReport `json:"componentReport,omitempty"`
	RequestID         string                    `json:"requestId,omitempty"`
	Method            string                    `json:"method,omitempty"`
	Params            json.RawMessage           `json:"params,omitempty"`
	At                string                    `json:"at,omitempty"`
}

// Control-plane messages sent from the master `/control-agent` endpoint to a slave node.
type MasterControlMessage struct {
	Type           string                  `json:"type"`
	At             string                  `json:"at,omitempty"`
	RunID          string                  `json:"runId,omitempty"`
	LeaseID        string                  `json:"leaseId,omitempty"`
	Status         string                  `json:"status,omitempty"`
	Mode           string                  `json:"mode,omitempty"`
	InstanceID     string                  `json:"instanceId,omitempty"`
	AllocationID   string                  `json:"allocationId,omitempty"`
	Instance       *AgentInstance          `json:"instance,omitempty"`
	Allocation     *InstanceAllocation     `json:"allocation,omitempty"`
	Run            *Run                    `json:"run,omitempty"`
	SessionBinding *DispatchSessionBinding `json:"sessionBinding,omitempty"`
	WorkspaceLease *WorkspaceLeaseGrant    `json:"workspaceLease,omitempty"`
	Lease          *WorkspaceLeaseGrant    `json:"lease,omitempty"`
	Decision       *ApprovalDecision       `json:"decision,omitempty"`
	RequestID      string                  `json:"requestId,omitempty"`
	Result         json.RawMessage         `json:"result,omitempty"`
	Error          *HubRPCError            `json:"error,omitempty"`
}

type HubRPCError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable,omitempty"`
}

type HubRPCRequest struct {
	Type      string          `json:"type"`
	RequestID string          `json:"requestId"`
	RunID     string          `json:"runId"`
	Method    string          `json:"method"`
	Params    json.RawMessage `json:"params"`
}

type HubRPCResponse struct {
	Type      string          `json:"type"`
	RequestID string          `json:"requestId"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     *HubRPCError    `json:"error,omitempty"`
}

// Orchestrator-client WebSocket messages exchanged between `orchestra mcp` and `/orchestrator-client`.
type OrchestratorClientMessage struct {
	Type            string          `json:"type"`
	ProtocolVersion string          `json:"protocolVersion,omitempty"`
	ClientID        string          `json:"clientId,omitempty"`
	SessionID       string          `json:"sessionId,omitempty"`
	RequestID       string          `json:"requestId,omitempty"`
	Tool            string          `json:"tool,omitempty"`
	Arguments       json.RawMessage `json:"arguments,omitempty"`
	At              string          `json:"at,omitempty"`
}

type DoorbellNotification struct {
	ThreadID        string   `json:"threadId"`
	ThreadTitle     string   `json:"threadTitle"`
	ThroughSequence int64    `json:"throughSequence"`
	Summary         string   `json:"summary"`
	Urgent          bool     `json:"urgent"`
	ApprovalIDs     []string `json:"approvalIds,omitempty"`
	OccurredAt      string   `json:"occurredAt"`
}

type OrchestratorServerMessage struct {
	Type            string                `json:"type"`
	ProtocolVersion string                `json:"protocolVersion,omitempty"`
	ClientID        string                `json:"clientId,omitempty"`
	SessionID       string                `json:"sessionId,omitempty"`
	Scopes          []string              `json:"scopes,omitempty"`
	RequestID       string                `json:"requestId,omitempty"`
	Result          json.RawMessage       `json:"result,omitempty"`
	Error           *HubRPCError          `json:"error,omitempty"`
	Doorbell        *DoorbellNotification `json:"doorbell,omitempty"`
	ThreadID        string                `json:"threadId,omitempty"`
	Reason          string                `json:"reason,omitempty"`
	At              string                `json:"at,omitempty"`
}

// Operator REST API response payloads.
type StatusResponse struct {
	Version          string        `json:"version"`
	ProtocolVersion  string        `json:"protocolVersion"`
	StartedAt        string        `json:"startedAt"`
	Now              string        `json:"now"`
	ConnectedNodes   int           `json:"connectedNodes"`
	TotalNodes       int           `json:"totalNodes"`
	OpenThreads      int           `json:"openThreads"`
	ActiveInstances  int           `json:"activeInstances"`
	ActiveRuns       int           `json:"activeRuns"`
	RunningTasks     int           `json:"runningTasks"`
	ReadyTasks       int           `json:"readyTasks"`
	PendingApprovals int           `json:"pendingApprovals"`
	Nodes            []ComputeNode `json:"nodes"`
}

type NodeDetail struct {
	Node              ComputeNode               `json:"node"`
	Connected         bool                      `json:"connected"`
	Synced            bool                      `json:"synced"`
	CapabilityReport  *NodeCapabilityReport     `json:"capabilityReport,omitempty"`
	ComponentReport   *ComponentInventoryReport `json:"componentReport,omitempty"`
	ResidentInstances []string                  `json:"residentInstances,omitempty"`
}

type ThreadDetailResponse struct {
	Thread          Thread                  `json:"thread"`
	Attachment      *ThreadAttachment       `json:"attachment,omitempty"`
	Tasks           []Task                  `json:"tasks"`
	Instances       []AgentInstance         `json:"instances"`
	Allocations     []InstanceAllocation    `json:"allocations"`
	Runs            []Run                   `json:"runs"`
	Approvals       []ApprovalRequest       `json:"approvals"`
	Artifacts       []Artifact              `json:"artifacts"`
	WorkspaceLeases []WorkspaceLease        `json:"workspaceLeases"`
	SessionBindings []HarnessSessionBinding `json:"sessionBindings"`
	Messages        []TaskMessage           `json:"messages"`
	Activities      []RunActivity           `json:"activities"`
	Transcripts     []RunTranscript         `json:"transcripts"`
	EventStream     ThreadEventStream       `json:"eventStream"`
}
