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
  version: string;
  protocolVersion: string;
  hostName: string;
  authMode: string;
}

export interface Thread {
  id: string;
  title: string;
  summary?: string;
  status: string;
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

export interface Task {
  id: string;
  threadId: string;
  title: string;
  instructions: string;
  status: string;
  attemptCount: number;
  maxAttempts: number;
  dependencies: TaskDependency[];
  assignment?: TaskAssignment;
  summary?: string;
  blockerReason?: string;
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
  taskId?: string;
  createdAt: string;
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
  name: string;
  mediaType: string;
  sizeBytes: number;
  uploaded: boolean;
  downloadPath: string;
  createdAt: string;
}

export interface TaskMessage {
  id: string;
  threadId: string;
  kind: string;
  body: string;
  createdAt: string;
}

export interface ThreadAttachment {
  id: string;
  threadId: string;
  clientId: string;
  sessionId: string;
  status: string;
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
  subject?: string;
  body: string;
  read: boolean;
  createdAt: string;
}

export interface ListMailboxesResult {
  mailboxes: AgentMailbox[];
}

export interface ReadMailResult {
  mailbox: AgentMailbox;
  messages: MailMessage[];
  hasMore: boolean;
}
