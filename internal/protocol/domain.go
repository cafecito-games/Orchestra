package protocol

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

const (
	MaximumTaskAttempts       = 3
	MaximumDelegationDepth    = 6
	ThreadEventRetentionLimit = 2000
	MaximumArtifactSizeBytes  = 10 * 1024 * 1024
	MaximumWaitMilliseconds   = 20000
	MaximumEventsPerWait      = 100
	RoomTopicMaximumBytes     = 256
	DefaultRoomReadLimit      = 100
	MaximumRoomReadLimit      = 200

	MinimumInstanceIdleTimeoutSeconds = 60
	DefaultInstanceIdleTimeoutSeconds = 1800
	MaximumInstanceIdleTimeoutSeconds = 86400
	InstanceNameMaximumBytes          = 256
	InstanceSummaryMaximumBytes       = 2000
	InstanceInstructionsMaximumBytes  = 65536
	InstanceIdempotencyKeyBytes       = 128
	InstanceWorkspaceMaximumBytes     = 4096
	InstanceCollectionEntries         = 1024
	InstanceRequirementEntries        = 64
	InstanceCountMaximum              = 65535
)

var (
	ThreadStatuses              = []string{"open", "resolved"}
	TaskStatuses                = []string{"pending", "ready", "running", "blocked", "completed", "failed", "cancelled"}
	TerminalTaskStatuses        = []string{"completed", "failed", "cancelled"}
	DependencyPolicies          = []string{"require-success", "allow-failure"}
	RunStatuses                 = []string{"queued", "running", "completed", "failed", "cancelled"}
	InstanceStatuses            = []string{"requested", "provisioning", "ready", "busy", "idle", "draining", "released", "failed"}
	NonTerminalInstanceStatuses = []string{"requested", "provisioning", "ready", "busy", "idle"}
	TerminalInstanceStatuses    = []string{"released", "failed"}
	AllocationStatuses          = []string{"reserved", "provisioning", "active", "lost", "released", "failed"}
	OccupyingAllocationStatuses = []string{"reserved", "provisioning", "active"}
	InstanceReleaseModes        = []string{"drain", "cancel"}
	InstanceCreatorKinds        = []string{"operator", "run", "orchestrator-client"}
	InstanceLifecycleOperations = []string{"create", "release", "renew"}
	TaskMessageKinds            = []string{"progress", "question", "guidance", "blocker", "result"}
	RoomMessageKinds            = []string{"finding", "contract", "decision", "gotcha", "guidance", "question", "answer"}
	RoomAuthorKinds             = []string{"task", "orchestrator", "operator"}
	ArtifactKinds               = []string{"patch", "report", "test-results", "log", "image", "other"}
)

var runTransitions = map[string][]string{
	"queued":    {"running", "failed", "cancelled"},
	"running":   {"completed", "failed", "cancelled"},
	"completed": {},
	"failed":    {},
	"cancelled": {},
}

func CanTransitionRun(fromStatus, toStatus string) bool {
	return slices.Contains(runTransitions[fromStatus], toStatus)
}

func IsActiveRunStatus(status string) bool {
	return status == "queued" || status == "running"
}

func IsTerminalRunStatus(status string) bool {
	return status == "completed" || status == "failed" || status == "cancelled"
}

var taskTransitions = map[string][]string{
	"pending":   {"ready", "blocked", "cancelled"},
	"ready":     {"running", "blocked", "cancelled"},
	"running":   {"ready", "completed", "failed", "cancelled"},
	"blocked":   {"cancelled"},
	"completed": {},
	"failed":    {},
	"cancelled": {},
}

func CanTransitionTask(fromStatus, toStatus string) bool {
	return slices.Contains(taskTransitions[fromStatus], toStatus)
}

func IsTerminalTaskStatus(status string) bool {
	return slices.Contains(TerminalTaskStatuses, status)
}

var instanceTransitions = map[string][]string{
	"requested":    {"provisioning", "draining", "failed"},
	"provisioning": {"ready", "draining", "failed"},
	"ready":        {"busy", "idle", "provisioning", "draining", "failed"},
	"busy":         {"idle", "provisioning", "draining", "failed"},
	"idle":         {"busy", "provisioning", "draining", "failed"},
	"draining":     {"released", "failed"},
	"released":     {},
	"failed":       {},
}

func CanTransitionInstance(fromStatus, toStatus string) bool {
	return slices.Contains(instanceTransitions[fromStatus], toStatus)
}

func IsTerminalInstanceStatus(status string) bool {
	return slices.Contains(TerminalInstanceStatuses, status)
}

var allocationTransitions = map[string][]string{
	"reserved":     {"provisioning", "released", "failed"},
	"provisioning": {"active", "lost", "released", "failed"},
	"active":       {"lost", "released", "failed"},
	"lost":         {"released", "failed"},
	"released":     {},
	"failed":       {},
}

func CanTransitionAllocation(fromStatus, toStatus string) bool {
	return slices.Contains(allocationTransitions[fromStatus], toStatus)
}

func IsOccupyingAllocationStatus(status string) bool {
	return slices.Contains(OccupyingAllocationStatuses, status)
}

type ThreadOrchestrator struct {
	Kind       string `json:"kind"`
	ClientID   string `json:"clientId,omitempty"`
	InstanceID string `json:"instanceId,omitempty"`
}

type Thread struct {
	ID           string              `json:"id"`
	Title        string              `json:"title"`
	Summary      string              `json:"summary,omitempty"`
	Status       string              `json:"status"`
	Orchestrator *ThreadOrchestrator `json:"orchestrator,omitempty"`
	CreatedAt    string              `json:"createdAt"`
	UpdatedAt    string              `json:"updatedAt"`
}

type ThreadAttachment struct {
	ID              string `json:"id"`
	ThreadID        string `json:"threadId"`
	ClientID        string `json:"clientId"`
	SessionID       string `json:"sessionId"`
	Status          string `json:"status"`
	LastHeartbeatAt string `json:"lastHeartbeatAt"`
	AcknowledgedSeq int64  `json:"acknowledgedSequence"`
	LastDoorbellSeq int64  `json:"lastDoorbellSequence"`
	CreatedAt       string `json:"createdAt"`
	UpdatedAt       string `json:"updatedAt"`
	DetachedReason  string `json:"detachedReason,omitempty"`
}

type TaskDependency struct {
	TaskID string `json:"taskId"`
	Policy string `json:"policy"`
}

type ExecutionPreferences struct {
	NodeIDs    []string `json:"nodeIds,omitempty"`
	HarnessIDs []string `json:"harnessIds,omitempty"`
	Models     []string `json:"models,omitempty"`
	Labels     []string `json:"labels,omitempty"`
}

type WorkspaceRequirements struct {
	Repository string `json:"repository,omitempty"`
	Path       string `json:"path,omitempty"`
	Writable   bool   `json:"writable,omitempty"`
	Isolation  string `json:"isolation,omitempty"`
	Cleanup    string `json:"cleanup,omitempty"`
	BaseBranch string `json:"baseBranch,omitempty"`
}

type ExecutionRequirements struct {
	HarnessIDs             []string               `json:"harnessIds,omitempty"`
	Models                 []string               `json:"models,omitempty"`
	Transports             []string               `json:"transports,omitempty"`
	OperatingSystems       []string               `json:"operatingSystems,omitempty"`
	Architectures          []string               `json:"architectures,omitempty"`
	Labels                 []string               `json:"labels,omitempty"`
	Accelerators           []string               `json:"accelerators,omitempty"`
	Toolchains             map[string]string      `json:"toolchains,omitempty"`
	MinimumConcurrency     *int                   `json:"minimumConcurrency,omitempty"`
	MinimumMemoryMegabytes *int64                 `json:"minimumMemoryMegabytes,omitempty"`
	ProjectProfileID       string                 `json:"projectProfileId,omitempty"`
	Workspace              *WorkspaceRequirements `json:"workspace,omitempty"`
	Preferences            *ExecutionPreferences  `json:"preferences,omitempty"`
}

type PlacementOverride struct {
	NodeID     string `json:"nodeId,omitempty"`
	InstanceID string `json:"instanceId,omitempty"`
}

type UnsatisfiedRequirement struct {
	Kind        string `json:"kind"`
	Requirement string `json:"requirement"`
	NodeID      string `json:"nodeId,omitempty"`
	InstanceID  string `json:"instanceId,omitempty"`
	Detail      string `json:"detail"`
}

type PlacementDiagnostic struct {
	EvaluatedAt     string                   `json:"evaluatedAt"`
	EligibleNodeIDs []string                 `json:"eligibleNodeIds"`
	Unsatisfied     []UnsatisfiedRequirement `json:"unsatisfied"`
}

type TaskAssignment struct {
	RunID        string `json:"runId"`
	InstanceID   string `json:"instanceId"`
	AllocationID string `json:"allocationId"`
	NodeID       string `json:"nodeId"`
	Attempt      int    `json:"attempt"`
	AssignedAt   string `json:"assignedAt"`
}

type TaskOrigin struct {
	Kind         string `json:"kind"`
	SubmissionID string `json:"submissionId,omitempty"`
	ClientID     string `json:"clientId,omitempty"`
	RunID        string `json:"runId,omitempty"`
	TaskID       string `json:"taskId,omitempty"`
	InstanceID   string `json:"instanceId,omitempty"`
}

type Task struct {
	ID                   string                `json:"id"`
	ThreadID             string                `json:"threadId"`
	ClientTaskID         string                `json:"clientTaskId,omitempty"`
	ParentTaskID         string                `json:"parentTaskId,omitempty"`
	CreatedByRunID       string                `json:"createdByRunId,omitempty"`
	Origin               TaskOrigin            `json:"origin"`
	Title                string                `json:"title"`
	Instructions         string                `json:"instructions"`
	Status               string                `json:"status"`
	AttemptCount         int                   `json:"attemptCount"`
	MaxAttempts          int                   `json:"maxAttempts"`
	Dependencies         []TaskDependency      `json:"dependencies"`
	Requirements         ExecutionRequirements `json:"requirements"`
	PlacementOverride    *PlacementOverride    `json:"placementOverride,omitempty"`
	PlacementInstanceID  string                `json:"placementInstanceId,omitempty"`
	PreferredInstanceID  string                `json:"preferredInstanceId,omitempty"`
	PlacementDiagnostic  *PlacementDiagnostic  `json:"placementDiagnostic,omitempty"`
	Assignment           *TaskAssignment       `json:"assignment,omitempty"`
	Summary              string                `json:"summary,omitempty"`
	Progress             string                `json:"progress,omitempty"`
	BlockerReason        string                `json:"blockerReason,omitempty"`
	ArtifactIDs          []string              `json:"artifactIds,omitempty"`
	LastRunOutput        string                `json:"lastRunOutput,omitempty"`
	LastReadRoomSequence int64                 `json:"lastReadRoomSequence"`
	RequiredRoomSequence int64                 `json:"requiredRoomSequence"`
	NudgedAtRoomSequence int64                 `json:"nudgedAtRoomSequence"`
	CreatedAt            string                `json:"createdAt"`
	UpdatedAt            string                `json:"updatedAt"`
	FinishedAt           string                `json:"finishedAt,omitempty"`
}

type TaskSubmissionReceipt struct {
	ID             string            `json:"id"`
	ThreadID       string            `json:"threadId"`
	SourceKey      string            `json:"sourceKey"`
	IdempotencyKey string            `json:"idempotencyKey"`
	Digest         string            `json:"digest"`
	TaskIDs        []string          `json:"taskIds"`
	ClientTaskMap  map[string]string `json:"clientTaskMap,omitempty"`
	CreatedAt      string            `json:"createdAt"`
}

type TaskUpdateReceipt struct {
	ID             string `json:"id"`
	TaskID         string `json:"taskId"`
	SourceKey      string `json:"sourceKey"`
	IdempotencyKey string `json:"idempotencyKey"`
	Digest         string `json:"digest"`
	CreatedAt      string `json:"createdAt"`
}

type InstancePurpose struct {
	Name         *string `json:"name,omitempty"`
	Title        *string `json:"title,omitempty"`
	Summary      *string `json:"summary,omitempty"`
	Instructions *string `json:"instructions,omitempty"`
}

type InstanceCreator struct {
	Kind       string `json:"kind"`
	OperatorID string `json:"operatorId,omitempty"`
	RunID      string `json:"runId,omitempty"`
	InstanceID string `json:"instanceId,omitempty"`
	ClientID   string `json:"clientId,omitempty"`
}

func (creator InstanceCreator) SourceKey() string {
	switch creator.Kind {
	case "operator":
		return "operator:" + creator.OperatorID
	case "run":
		return "run:" + creator.RunID
	case "orchestrator-client":
		return "orchestrator-client:" + creator.ClientID
	default:
		return creator.Kind
	}
}

type InstanceDelegationPolicy struct {
	CanDelegate bool `json:"canDelegate"`
}

type InstanceLease struct {
	IdleTimeoutSeconds int    `json:"idleTimeoutSeconds"`
	ExpiresAt          string `json:"expiresAt"`
}

type InstanceExecutionPreferences struct {
	NodeIDs    *[]string `json:"nodeIds,omitempty"`
	HarnessIDs *[]string `json:"harnessIds,omitempty"`
	Models     *[]string `json:"models,omitempty"`
	Labels     *[]string `json:"labels,omitempty"`
}

type InstanceWorkspaceRequirements struct {
	Repository *string `json:"repository,omitempty"`
	Path       *string `json:"path,omitempty"`
	Writable   bool    `json:"writable"`
}

type InstanceExecutionRequirements struct {
	HarnessIDs             *[]string                      `json:"harnessIds,omitempty"`
	Models                 *[]string                      `json:"models,omitempty"`
	Transports             *[]string                      `json:"transports,omitempty"`
	OperatingSystems       *[]string                      `json:"operatingSystems,omitempty"`
	Architectures          *[]string                      `json:"architectures,omitempty"`
	Labels                 *[]string                      `json:"labels,omitempty"`
	MinimumConcurrency     *int                           `json:"minimumConcurrency,omitempty"`
	MinimumMemoryMegabytes *int64                         `json:"minimumMemoryMegabytes,omitempty"`
	ProjectProfileID       *string                        `json:"projectProfileId,omitempty"`
	Workspace              *InstanceWorkspaceRequirements `json:"workspace,omitempty"`
	Preferences            *InstanceExecutionPreferences  `json:"preferences,omitempty"`
}

type AgentInstance struct {
	ID           string                        `json:"id"`
	ThreadID     string                        `json:"threadId"`
	Creator      InstanceCreator               `json:"creator"`
	Purpose      *InstancePurpose              `json:"purpose,omitempty"`
	Delegation   InstanceDelegationPolicy      `json:"delegation"`
	Requirements InstanceExecutionRequirements `json:"requirements"`
	Lease        InstanceLease                 `json:"lease"`
	Status       string                        `json:"status"`
	AutoManaged  bool                          `json:"autoManaged,omitempty"`
	CreatedAt    string                        `json:"createdAt"`
	UpdatedAt    string                        `json:"updatedAt"`
}

type InstanceAllocation struct {
	ID         string        `json:"id"`
	InstanceID string        `json:"instanceId"`
	NodeID     string        `json:"nodeId"`
	HarnessID  string        `json:"harnessId"`
	Model      string        `json:"model"`
	Transport  string        `json:"transport"`
	Workspace  string        `json:"workspace"`
	Lease      InstanceLease `json:"lease"`
	Status     string        `json:"status"`
	CreatedAt  string        `json:"createdAt"`
	UpdatedAt  string        `json:"updatedAt"`
}

type InstanceInitialTask struct {
	Title        string `json:"title"`
	Instructions string `json:"instructions"`
}

type InstanceIdempotency struct {
	Caller InstanceCreator `json:"caller"`
	Key    string          `json:"key"`
}

type InstanceLifecycleRequest struct {
	Operation          string                         `json:"operation"`
	ThreadID           string                         `json:"threadId"`
	InstanceID         string                         `json:"instanceId,omitempty"`
	Idempotency        InstanceIdempotency            `json:"idempotency"`
	Purpose            *InstancePurpose               `json:"purpose,omitempty"`
	Delegation         *InstanceDelegationPolicy      `json:"delegation,omitempty"`
	Requirements       *InstanceExecutionRequirements `json:"requirements,omitempty"`
	IdleTimeoutSeconds *int                           `json:"idleTimeoutSeconds,omitempty"`
	InitialTask        *InstanceInitialTask           `json:"initialTask,omitempty"`
	Mode               string                         `json:"mode,omitempty"`
}

type InstanceLifecycleResult struct {
	Instance      AgentInstance       `json:"instance"`
	Allocation    *InstanceAllocation `json:"allocation,omitempty"`
	InitialTaskID *string             `json:"initialTaskId,omitempty"`
	Replayed      bool                `json:"replayed"`
}

type InstanceLifecycleReceipt struct {
	ID             string  `json:"id"`
	ThreadID       string  `json:"threadId"`
	SourceKey      string  `json:"sourceKey"`
	IdempotencyKey string  `json:"idempotencyKey"`
	Operation      string  `json:"operation"`
	Digest         string  `json:"digest"`
	InstanceID     string  `json:"instanceId"`
	InitialTaskID  *string `json:"initialTaskId,omitempty"`
	CreatedAt      string  `json:"createdAt"`
}

type InstanceReleaseIntent struct {
	InstanceID  string `json:"instanceId"`
	ThreadID    string `json:"threadId"`
	Mode        string `json:"mode"`
	RequestedAt string `json:"requestedAt"`
	UpdatedAt   string `json:"updatedAt"`
}

type Run struct {
	ID                 string                 `json:"id"`
	ThreadID           string                 `json:"threadId"`
	InstanceID         string                 `json:"instanceId"`
	AllocationID       string                 `json:"allocationId"`
	NodeID             string                 `json:"nodeId"`
	HarnessID          string                 `json:"harnessId"`
	Model              string                 `json:"model"`
	Workspace          string                 `json:"workspace"`
	Prompt             string                 `json:"prompt"`
	Status             string                 `json:"status"`
	Output             string                 `json:"output"`
	Error              *string                `json:"error,omitempty"`
	Depth              int                    `json:"depth"`
	ParentRunID        *string                `json:"parentRunId,omitempty"`
	DispatchedAt       *string                `json:"dispatchedAt,omitempty"`
	StartedAt          *string                `json:"startedAt,omitempty"`
	FinishedAt         *string                `json:"finishedAt,omitempty"`
	CreatedAt          string                 `json:"createdAt"`
	TaskID             *string                `json:"taskId,omitempty"`
	Attempt            *int                   `json:"attempt,omitempty"`
	Transport          string                 `json:"transport"`
	FallbackTransport  *string                `json:"fallbackTransport,omitempty"`
	TransportSelection *RunTransportSelection `json:"transportSelection,omitempty"`
	SessionBindingID   *string                `json:"sessionBindingId,omitempty"`
	WorkspaceLeaseID   *string                `json:"workspaceLeaseId,omitempty"`
	ProviderSessionID  *string                `json:"providerSessionId,omitempty"`
}

type Artifact struct {
	ID           string `json:"id"`
	ThreadID     string `json:"threadId"`
	TaskID       string `json:"taskId,omitempty"`
	RunID        string `json:"runId,omitempty"`
	ClientID     string `json:"clientId,omitempty"`
	SourceKey    string `json:"sourceKey"`
	Name         string `json:"name"`
	RelativePath string `json:"relativePath,omitempty"`
	MediaType    string `json:"mediaType"`
	SizeBytes    int64  `json:"sizeBytes"`
	SHA256       string `json:"sha256"`
	Summary      string `json:"summary,omitempty"`
	Uploaded     bool   `json:"uploaded"`
	DownloadPath string `json:"downloadPath"`
	CreatedAt    string `json:"createdAt"`
	UploadedAt   string `json:"uploadedAt,omitempty"`
}

type ArtifactUploadGrant struct {
	Path      string `json:"path"`
	Token     string `json:"token"`
	ExpiresAt string `json:"expiresAt"`
}

type ArtifactUploadGrantRecord struct {
	TokenDigest string `json:"tokenDigest"`
	ArtifactID  string `json:"artifactId"`
	SourceKey   string `json:"sourceKey"`
	ThreadID    string `json:"threadId"`
	CreatedAt   string `json:"createdAt"`
	ExpiresAt   string `json:"expiresAt"`
	ConsumedAt  string `json:"consumedAt,omitempty"`
}

type MessageEndpoint struct {
	Kind     string `json:"kind"`
	TaskID   string `json:"taskId,omitempty"`
	ClientID string `json:"clientId,omitempty"`
	RunID    string `json:"runId,omitempty"`
}

type TaskMessage struct {
	ID             string          `json:"id"`
	ThreadID       string          `json:"threadId"`
	SourceKey      string          `json:"sourceKey"`
	IdempotencyKey string          `json:"idempotencyKey"`
	Digest         string          `json:"digest"`
	Sender         MessageEndpoint `json:"sender"`
	Recipient      MessageEndpoint `json:"recipient"`
	ReplyToID      string          `json:"replyToId,omitempty"`
	Kind           string          `json:"kind"`
	Body           string          `json:"body"`
	ArtifactIDs    []string        `json:"artifactIds,omitempty"`
	CreatedAt      string          `json:"createdAt"`
}

type TaskMessageAck struct {
	MessageID      string `json:"messageId"`
	ReaderKey      string `json:"readerKey"`
	AcknowledgedAt string `json:"acknowledgedAt"`
}

type RoomAuthor struct {
	Kind       string `json:"kind"`
	TaskID     string `json:"taskId,omitempty"`
	RunID      string `json:"runId,omitempty"`
	Title      string `json:"title,omitempty"`
	ClientID   string `json:"clientId,omitempty"`
	OperatorID string `json:"operatorId,omitempty"`
}

type RoomMessage struct {
	Sequence           int64      `json:"seq"`
	ID                 string     `json:"id"`
	ThreadID           string     `json:"threadId"`
	CreatedAt          string     `json:"createdAt"`
	Author             RoomAuthor `json:"author"`
	Kind               string     `json:"kind"`
	Topic              string     `json:"topic,omitempty"`
	Body               string     `json:"body"`
	ReplyToSequence    *int64     `json:"replyToSequence,omitempty"`
	SupersedesSequence *int64     `json:"supersedesSequence,omitempty"`
	ArtifactIDs        []string   `json:"artifactIds,omitempty"`
}

type ThreadRoomHead struct {
	ThreadID      string `json:"threadId"`
	DirectoryName string `json:"directoryName"`
	HeadSequence  int64  `json:"headSequence"`
	CreatedAt     string `json:"createdAt"`
	UpdatedAt     string `json:"updatedAt"`
}

type TaskRoomSummary struct {
	HeadSequence     int64 `json:"headSequence"`
	LastReadSequence int64 `json:"lastReadSequence"`
	RequiredSequence int64 `json:"requiredSequence"`
	UnreadNudge      bool  `json:"unreadNudge"`
}

type RoomMessageReceipt struct {
	ID             string   `json:"id"`
	ThreadID       string   `json:"threadId"`
	SourceKey      string   `json:"sourceKey"`
	IdempotencyKey string   `json:"idempotencyKey"`
	Digest         string   `json:"digest"`
	Sequence       int64    `json:"sequence"`
	NudgedTaskIDs  []string `json:"nudgedTaskIds,omitempty"`
	CreatedAt      string   `json:"createdAt"`
}

type RoomNudgeReceipt struct {
	ID               string   `json:"id"`
	ThreadID         string   `json:"threadId"`
	SourceKey        string   `json:"sourceKey"`
	IdempotencyKey   string   `json:"idempotencyKey"`
	Digest           string   `json:"digest"`
	RequiredSequence int64    `json:"requiredSequence"`
	NudgedTaskIDs    []string `json:"nudgedTaskIds,omitempty"`
	CreatedAt        string   `json:"createdAt"`
}

func RoomDirectoryName(threadID string, createdAt string) string {
	dateSegment := "19700101"
	if parsed, ok := ParseTimestamp(createdAt); ok {
		dateSegment = parsed.UTC().Format("20060102")
	}
	rawSuffix := strings.TrimPrefix(strings.TrimSpace(threadID), "thread-")
	var suffixBuilder strings.Builder
	for _, character := range rawSuffix {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '-' || character == '_' {
			suffixBuilder.WriteRune(character)
		}
	}
	suffix := suffixBuilder.String()
	if suffix == "" {
		suffix = "unknown"
	}
	return "room_" + dateSegment + "_" + suffix
}

type TaskEventEntry struct {
	Sequence   int64          `json:"sequence"`
	ThreadID   string         `json:"threadId"`
	Type       string         `json:"type"`
	TaskID     string         `json:"taskId,omitempty"`
	RunID      string         `json:"runId,omitempty"`
	InstanceID string         `json:"instanceId,omitempty"`
	MessageID  string         `json:"messageId,omitempty"`
	ApprovalID string         `json:"approvalId,omitempty"`
	ArtifactID string         `json:"artifactId,omitempty"`
	Status     string         `json:"status,omitempty"`
	Summary    string         `json:"summary,omitempty"`
	Message    *TaskMessage   `json:"message,omitempty"`
	Detail     map[string]any `json:"detail,omitempty"`
	OccurredAt string         `json:"occurredAt"`
}

type ThreadEventStream struct {
	ThreadID  string `json:"threadId"`
	Head      int64  `json:"head"`
	Floor     int64  `json:"floor"`
	UpdatedAt string `json:"updatedAt"`
}

type ProjectRepository struct {
	URL           string `json:"url"`
	DefaultBranch string `json:"defaultBranch"`
}

type ProjectWorkspacePolicy struct {
	Isolation string `json:"isolation,omitempty"`
	Cleanup   string `json:"cleanup,omitempty"`
}

type ProjectProfile struct {
	ID              string                 `json:"id"`
	Name            string                 `json:"name"`
	Repository      *ProjectRepository     `json:"repository,omitempty"`
	WorkspacePolicy ProjectWorkspacePolicy `json:"workspacePolicy,omitempty"`
	Requirements    ExecutionRequirements  `json:"requirements,omitempty"`
}

type EventCursorPayload struct {
	Version   int    `json:"v"`
	ThreadID  string `json:"threadId"`
	ReaderKey string `json:"readerKey"`
	Sequence  int64  `json:"sequence"`
}

const cursorPrefix = "tev1."

func EncodeEventCursor(threadID, readerKey string, sequence int64) (string, error) {
	if !IsIdentifier(threadID) || readerKey == "" || sequence < 0 {
		return "", errors.New("cursor parameters are malformed")
	}
	payload := EventCursorPayload{
		Version:   1,
		ThreadID:  threadID,
		ReaderKey: readerKey,
		Sequence:  sequence,
	}
	encodedBytes, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(encodedBytes)
	checksum := sha256.Sum256([]byte(encodedPayload))
	return fmt.Sprintf("%s%s.%s", cursorPrefix, encodedPayload, hex.EncodeToString(checksum[:8])), nil
}

func DecodeEventCursor(cursor string, expectedThreadID, expectedReaderKey string) (int64, error) {
	if !strings.HasPrefix(cursor, cursorPrefix) {
		return 0, errors.New("event cursor has an invalid prefix")
	}
	remainder := strings.TrimPrefix(cursor, cursorPrefix)
	encodedPayload, providedChecksum, found := strings.Cut(remainder, ".")
	if !found || encodedPayload == "" || providedChecksum == "" {
		return 0, errors.New("event cursor is malformed")
	}
	expectedSum := sha256.Sum256([]byte(encodedPayload))
	if providedChecksum != hex.EncodeToString(expectedSum[:8]) {
		return 0, errors.New("event cursor checksum mismatch")
	}
	decodedBytes, err := base64.RawURLEncoding.DecodeString(encodedPayload)
	if err != nil {
		return 0, errors.New("event cursor base64 payload is malformed")
	}
	var payload EventCursorPayload
	if err := json.Unmarshal(decodedBytes, &payload); err != nil {
		return 0, errors.New("event cursor JSON payload is malformed")
	}
	if payload.Version != 1 || payload.Sequence < 0 {
		return 0, errors.New("event cursor version or sequence is invalid")
	}
	if expectedThreadID != "" && payload.ThreadID != expectedThreadID {
		return 0, errors.New("event cursor belongs to another thread")
	}
	if expectedReaderKey != "" && payload.ReaderKey != expectedReaderKey {
		return 0, errors.New("event cursor belongs to another caller")
	}
	return payload.Sequence, nil
}
