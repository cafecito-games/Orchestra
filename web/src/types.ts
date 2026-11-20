// JSON shapes returned by the master's operator REST API. Fields mirror the
// Go protocol structs; only the fields the console renders are declared.

export interface AcpAgentCapabilities {
  protocolVersion: number;
  loadSession: boolean;
  resumeSession: boolean;
  adapterName?: string;
  adapterVersion?: string;
}

export interface HarnessProfile {
  id: string;
  label: string;
  description: string;
  available: boolean;
  binary?: string;
  authMode: string;
  models: string[];
  transports?: string[];
  acp?: AcpAgentCapabilities;
  approvalPolicy?: string;
}

export interface ComputeNode {
  id: string;
  name: string;
  kind: string;
  platform: string;
  status: string;
  lastSeen: string;
  activeRuns: number;
  concurrency: number;
  instanceCapacity?: number;
  activeInstances?: number;
  workspaceRoots: string[];
  version: string;
  selfUpdateDisabled?: boolean;
  harnesses: HarnessProfile[];
}

export interface NodeCapabilityEvidence {
  capabilityId: string;
  source: string;
  success: boolean;
  rawValue?: string;
  normalizedValue?: string;
}

export interface NodeCapabilityReport {
  nodeId: string;
  projectAllowlist?: string[];
  evidence: NodeCapabilityEvidence[];
  at: string;
}

export interface DiscoveredRepository {
  root: string;
  path: string;
  relativePath: string;
  repository?: string;
  remotes?: string[];
  defaultBranch?: string;
  headBranch?: string;
}

export interface NodeRepositoryReport {
  nodeId: string;
  roots: string[];
  repositories: DiscoveredRepository[];
  observedAt: string;
}

export interface NodeDetail {
  node: ComputeNode;
  connected: boolean;
  synced: boolean;
  capabilityReport?: NodeCapabilityReport;
  repositoryReport?: NodeRepositoryReport;
  updateStatus?: { status: string; targetVersion: string; message?: string; error?: string };
  residentInstances?: string[];
}

export interface StatusResponse {
  version: string;
  protocolVersion: string;
  startedAt: string;
  now: string;
  connectedNodes: number;
  totalNodes: number;
  openThreads: number;
  activeInstances: number;
  activeRuns: number;
  runningTasks: number;
  readyTasks: number;
  pendingApprovals: number;
  nodes: ComputeNode[];
}

export interface Health {
  ok: boolean;
  service: string;
  mode: "orchestrator" | "relay";
  version: string;
  protocolVersion: string;
  hostName: string;
  authMode: string;
}

export interface ThreadOrchestrator {
  kind: string;
  clientId?: string;
  instanceId?: string;
  lastHandledEventSequence?: number;
  pendingWakeEventSequence?: number;
  pendingWakeTaskId?: string;
  lastWakeSummary?: string;
  lastWakeAt?: string;
}

export interface Thread {
  id: string;
  title: string;
  summary?: string;
  status: string;
  labels?: Record<string, string>;
  orchestrator?: ThreadOrchestrator;
  createdAt: string;
  updatedAt: string;
}

export interface TaskDependency {
  taskId: string;
  policy: string;
}

export interface TaskAssignment {
  runId: string;
  instanceId: string;
  allocationId: string;
  nodeId: string;
  attempt: number;
  assignedAt: string;
}

export interface TaskClaim {
  epoch: number;
  token: string;
  claimerKind: string;
  claimerId: string;
  claimerSessionId?: string;
  ttlSeconds: number;
  claimedAt: string;
  renewedAt: string;
  expiresAt: string;
}

export interface Task {
  id: string;
  threadId: string;
  title: string;
  instructions: string;
  executionMode?: "dispatched" | "claimable";
  status: string;
  attemptCount: number;
  maxAttempts: number;
  dependencies: TaskDependency[];
  assignment?: TaskAssignment;
  claimEpoch?: number;
  claim?: TaskClaim;
  summary?: string;
  blockerKind?: string;
  blockerReason?: string;
  lastReadRoomSequence?: number;
  requiredRoomSequence?: number;
}

export interface InstancePurpose {
  name?: string;
  title?: string;
  summary?: string;
}

export interface InstanceLease {
  idleTimeoutSeconds: number;
  expiresAt: string;
}

export interface AgentInstance {
  id: string;
  threadId: string;
  handle?: string;
  forkFromInstanceId?: string;
  roomChannels?: string[];
  purpose?: InstancePurpose;
  status: string;
  autoManaged?: boolean;
  lease: InstanceLease;
}

export interface InstanceAllocation {
  id: string;
  instanceId: string;
  nodeId: string;
  harnessId: string;
  model: string;
  transport: string;
  status: string;
}

export interface Run {
  id: string;
  threadId: string;
  instanceId: string;
  nodeId: string;
  harnessId: string;
  model: string;
  transport: string;
  status: string;
  output: string;
  error?: string;
  errorKind?: string;
  taskId?: string;
  createdAt: string;
  finishedAt?: string;
}

export interface ApprovalOption {
  id: string;
  label: string;
  kind: string;
}

export interface ApprovalRequest {
  id: string;
  runId: string;
  threadId?: string;
  taskId?: string;
  nodeId: string;
  title: string;
  detail?: string;
  options: ApprovalOption[];
  status: string;
  selectedOptionId?: string;
  deliveryState: string;
  requestedAt: string;
  expiresAt: string;
  resolvedAt?: string;
  updatedAt: string;
}

export interface Artifact {
  id: string;
  threadId: string;
  taskId?: string;
  runId?: string;
  name: string;
  relativePath?: string;
  kind?: string;
  mediaType: string;
  sizeBytes: number;
  sha256?: string;
  summary?: string;
  uploaded: boolean;
  downloadPath: string;
  createdAt: string;
  uploadedAt?: string;
}

export interface TaskMessage {
  id: string;
  threadId: string;
  kind: string;
  body: string;
  createdAt: string;
}

export type RoomMessageKind =
  | "finding"
  | "contract"
  | "decision"
  | "gotcha"
  | "guidance"
  | "question"
  | "answer";

export interface RoomAuthor {
  kind: string;
  taskId?: string;
  runId?: string;
  instanceId?: string;
  handle?: string;
  title?: string;
  clientId?: string;
  operatorId?: string;
  principalKind?: string;
  principalId?: string;
  verifiedNode?: string;
}

export interface RoomMessage {
  seq: number;
  id: string;
  threadId: string;
  createdAt: string;
  author: RoomAuthor;
  kind: RoomMessageKind | string;
  channel?: string;
  topic?: string;
  body: string;
  labels?: Record<string, string>;
  mentions?: string[];
  replyToSequence?: number;
  supersedesSequence?: number;
  artifactIds?: string[];
}

export interface ThreadRoomHead {
  threadId: string;
  directoryName: string;
  headSequence: number;
  createdAt: string;
  updatedAt: string;
}

export interface ReadRoomResult {
  threadId: string;
  room: ThreadRoomHead;
  path?: string;
  messages: RoomMessage[];
  headSequence: number;
  lastReadSequence: number;
  requiredSequence: number;
  unreadNudge: boolean;
  throughSequence: number;
  hasMore: boolean;
}

export interface PostRoomMessageRequest {
  idempotencyKey: string;
  kind: RoomMessageKind;
  channel?: string;
  topic?: string;
  body: string;
  replyToSequence?: number;
  supersedesSequence?: number;
  artifactIds?: string[];
  nudge?: boolean;
  nudgeTaskIds?: string[];
}

export interface PostRoomMessageResult {
  threadId: string;
  room: ThreadRoomHead;
  message: RoomMessage;
  nudgedTaskIds: string[];
  replayed: boolean;
}

export interface NudgeRoomResult {
  threadId: string;
  room: ThreadRoomHead;
  requiredSequence: number;
  nudgedTaskIds: string[];
  replayed: boolean;
}

export interface ThreadStallCondition {
  kind?: string;
  reason?: string;
  summary?: string;
  detail?: string;
  handle?: string;
  instanceId?: string;
  taskId?: string;
  taskIds?: string[];
  runId?: string;
  seq?: number;
  sequence?: number;
  roomSequence?: number;
}

export interface ThreadStallReport {
  threadId?: string;
  stalled?: boolean;
  kind?: string;
  reason?: string;
  reasons?: string[];
  summary?: string;
  conditions?: Array<string | ThreadStallCondition>;
  detectedAt?: string;
  evaluatedAt?: string;
  occurredAt?: string;
  roomSequence?: number;
  headRoomSequence?: number;
  throughRoomSequence?: number;
  lastReadRoomSequence?: number;
  throughSequence?: number;
  blockedTaskIds?: string[];
  unansweredMentions?: Array<
    | string
    | {
        handle?: string;
        instanceId?: string;
        seq?: number;
        sequence?: number;
        roomSequence?: number;
        taskId?: string;
        runId?: string;
      }
  >;
}

export interface ThreadAttachment {
  id: string;
  threadId: string;
  clientId: string;
  sessionId: string;
  status: string;
  lastReadRoomSequence?: number;
}

export interface ThreadDetail {
  thread: Thread;
  attachment?: ThreadAttachment;
  tasks: Task[];
  instances: AgentInstance[];
  allocations: InstanceAllocation[];
  runs: Run[];
  approvals: ApprovalRequest[];
  artifacts: Artifact[];
  messages: TaskMessage[];
  room?: ThreadRoomHead;
  stall?: ThreadStallReport | null;
  eventStream: { threadId: string; head: number; floor: number; updatedAt: string };
}

export interface AgentMailbox {
  id: string;
  address: string;
  handle: string;
  host: string;
  aliases?: string[];
  kind: string;
  status: string;
  harness?: string;
  workspace?: string;
  unreadCount: number;
  createdAt: string;
  updatedAt: string;
}

export interface MailMessage {
  id: string;
  sequence: number;
  from: string;
  to: string;
  replyToId?: string;
  subject?: string;
  body: string;
  read: boolean;
  createdAt: string;
}

export interface SendMailRequest {
  idempotencyKey: string;
  to: string;
  replyToId?: string;
  subject?: string;
  body: string;
  wake?: boolean;
}

export interface SendMailResult {
  message: MailMessage;
  recipient: AgentMailbox;
  replayed: boolean;
}

export interface ListMailboxesResult {
  mailboxes: AgentMailbox[];
}

export interface ReadMailResult {
  mailbox: AgentMailbox;
  messages: MailMessage[];
  hasMore: boolean;
}

export interface ListMailTrafficResult {
  messages: MailMessage[];
  hasMore: boolean;
}
