import { api } from "../api";
import { useLive, usePolledFetch, navigate } from "../hooks";
import type { ReactNode } from "react";
import type { ApprovalRequest, NodeDetail, StatusResponse, Thread } from "../types";
import { Badge, Card, Empty, ErrorCard, SectionHead } from "../components/ui";
import { CpuIcon, PlayIcon, ServerIcon, ShieldIcon, ThreadIcon } from "../components/Icons";
import { timeAgo } from "../format";

interface OverviewData {
  status: StatusResponse;
  nodes: NodeDetail[];
  threads: Thread[];
  pending: ApprovalRequest[];
}

function Metric({
  label,
  value,
  hint,
  icon,
  tone,
}: {
  label: string;
  value: string;
  hint: string;
  icon: ReactNode;
  tone?: string;
}) {
  return (
    <Card className="metric">
      <div className="metric-top">
        <span className="metric-label">{label}</span>
        <span className={`metric-icon ${tone ?? ""}`}>{icon}</span>
      </div>
      <div className="metric-value">{value}</div>
      <div className="metric-hint">{hint}</div>
    </Card>
  );
}

export function Overview() {
  const { pollMs, refreshNonce } = useLive();
  const { data, error } = usePolledFetch<OverviewData>(
    async () => {
      const [status, nodes, threads, approvals] = await Promise.all([
        api<StatusResponse>("/api/status"),
        api<{ nodes: NodeDetail[] }>("/api/nodes"),
        api<{ threads: Thread[] }>("/api/threads?limit=8"),
        api<{ approvals: ApprovalRequest[] }>("/api/approvals?status=pending"),
      ]);
      return {
        status,
        nodes: nodes.nodes,
        threads: threads.threads,
        pending: approvals.approvals,
      };
    },
    [refreshNonce],
    pollMs,
  );

  if (error) return <ErrorCard message={error} />;
  if (!data) return <div className="loading"><div className="spinner" /></div>;

  const { status, nodes, threads, pending } = data;

  return (
    <>
      <div className="section">
        <div className="grid cols-4">
          <Metric
            label="Nodes online"
            value={`${status.connectedNodes}/${status.totalNodes}`}
            hint={`${status.totalNodes} registered`}
            icon={<ServerIcon />}
            tone={status.connectedNodes > 0 ? "accent" : ""}
          />
          <Metric
            label="Active runs"
            value={String(status.activeRuns)}
            hint={`${status.runningTasks} tasks running`}
            icon={<PlayIcon />}
            tone={status.activeRuns > 0 ? "accent" : ""}
          />
          <Metric
            label="Resident instances"
            value={String(status.activeInstances)}
            hint="warm agent sessions"
            icon={<CpuIcon />}
          />
          <Metric
            label="Open threads"
            value={String(status.openThreads)}
            hint={`${status.readyTasks} tasks ready`}
            icon={<ThreadIcon />}
          />
          <Metric
            label="Pending approvals"
            value={String(status.pendingApprovals)}
            hint="awaiting decision"
            icon={<ShieldIcon />}
            tone={status.pendingApprovals > 0 ? "warn" : ""}
          />
        </div>
      </div>

      <div className="section">
        <SectionHead title="Nodes" action={<a className="section-count" href="#nodes">view all →</a>} />
        {nodes.length === 0 ? (
          <Card>
            <Empty
              title="No worker nodes registered"
              sub={<>Start <code>orchestra slave</code> on a machine to bring it online.</>}
            />
          </Card>
        ) : (
          <Card>
            <div className="list">
              {nodes.slice(0, 6).map((detail) => {
                const node = detail.node;
                const harnesses = node.harnesses.filter((h) => h.available);
                return (
                  <div className="row click" key={node.id} onClick={() => navigate("/nodes")}>
                    <div className="row-main">
                      <div className="row-title">{node.name || node.id}</div>
                      <div className="row-sub">
                        {node.platform} · {harnesses.map((h) => h.id).join(", ")}
                      </div>
                    </div>
                    <div className="row-side">
                      <div style={{ textAlign: "right" }}>
                        <div className="row-sub" style={{ marginBottom: 3 }}>
                          <span className="mono">{node.activeRuns}/{node.concurrency}</span> runs
                        </div>
                        <div className={`bar ${node.activeRuns >= node.concurrency ? "warn" : ""}`}>
                          <span
                            style={{
                              width: `${Math.min(100, Math.round((node.activeRuns / Math.max(1, node.concurrency)) * 100))}%`,
                            }}
                          />
                        </div>
                      </div>
                      <Badge status={node.status} />
                    </div>
                  </div>
                );
              })}
            </div>
          </Card>
        )}
      </div>

      <div className="grid cols-2">
        <div className="section" style={{ marginBottom: 0 }}>
          <SectionHead title="Recent threads" action={<a className="section-count" href="#threads">view all →</a>} />
          <Card>
            {threads.length === 0 ? (
              <Empty title="No threads yet" />
            ) : (
              <div className="list">
                {threads.map((thread) => (
                  <div className="row click" key={thread.id} onClick={() => navigate(`/thread/${thread.id}`)}>
                    <div className="row-main">
                      <div className="row-title">{thread.title}</div>
                      <div className="row-sub">
                        <span className="mono">{thread.id}</span> · updated {timeAgo(thread.updatedAt)}
                      </div>
                    </div>
                    <div className="row-side">
                      <Badge status={thread.status} />
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Card>
        </div>

        <div className="section" style={{ marginBottom: 0 }}>
          <SectionHead title="Pending approvals" action={<a className="section-count" href="#approvals">view all →</a>} />
          <Card>
            {pending.length === 0 ? (
              <Empty title="Nothing awaiting approval" />
            ) : (
              <div className="list">
                {pending.slice(0, 6).map((approval) => (
                  <div className="row click" key={approval.id} onClick={() => navigate("/approvals")}>
                    <div className="row-main">
                      <div className="row-title">{approval.title}</div>
                      <div className="row-sub">
                        {approval.nodeId} · {timeAgo(approval.requestedAt)}
                      </div>
                    </div>
                    <div className="row-side">
                      <Badge status="pending" />
                    </div>
                  </div>
                ))}
              </div>
            )}
          </Card>
        </div>
      </div>
    </>
  );
}
