import { api, download } from "../api";
import { useLive, usePolledFetch, navigate } from "../hooks";
import type { AgentInstance, InstanceAllocation, ThreadDetail as ThreadDetailType } from "../types";
import { Badge, Card, Empty, ErrorCard, LoadingCard, PlainBadge, SectionHead } from "../components/ui";
import { BackIcon, ThreadIcon, XIcon } from "../components/Icons";
import { fmtBytes, shortId, timeAgo } from "../format";
import { toast } from "../toast";
import { ApprovalCard } from "../components/ApprovalCard";

const OCCUPYING = new Set(["reserved", "provisioning", "active"]);

export function ThreadDetail({ id }: { id: string }) {
  const { pollMs, refreshNonce } = useLive();
  const { data, error, reload } = usePolledFetch<ThreadDetailType>(
    () => api<ThreadDetailType>(`/api/threads/${encodeURIComponent(id)}`),
    [id, refreshNonce],
    pollMs,
  );

  async function cancel(runId: string) {
    try {
      await api(`/api/cancel/${encodeURIComponent(runId)}`, { method: "POST" });
      toast(`Cancelled ${shortId(runId)}`, "ok");
      reload();
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), "err");
    }
  }

  async function downloadArtifact(name: string, path: string) {
    try {
      await download(path, name);
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), "err");
    }
  }

  if (error) return <ErrorCard message={error} />;
  if (!data) return <LoadingCard />;

  const { thread, tasks, runs, instances, allocations, approvals, artifacts, messages } = data;

  const hasContent =
    tasks.length || runs.length || instances.length || artifacts.length || messages.length;

  return (
    <>
      <div className="detail-head">
        <button className="btn ghost" onClick={() => navigate("/threads")} style={{ marginBottom: 12, paddingLeft: 8 }}>
          <BackIcon />
          <span>Threads</span>
        </button>
        <div className="detail-title">
          {thread.title}
          <Badge status={thread.status} />
        </div>
        <div className="detail-meta">
          <span className="kv"><b>id</b> {thread.id}</span>
          <span className="kv"><b>created</b> {timeAgo(thread.createdAt)}</span>
          <span className="kv"><b>updated</b> {timeAgo(thread.updatedAt)}</span>
          {data.attachment ? <span className="kv"><b>attached</b> {data.attachment.clientId}</span> : null}
        </div>
        {thread.summary ? <div className="muted" style={{ marginTop: 8, fontSize: 13 }}>{thread.summary}</div> : null}
      </div>

      {tasks.length > 0 ? (
        <div className="section">
          <SectionHead title="Tasks" count={tasks.length} />
          <Card>
            <table className="table">
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Status</th>
                  <th className="num">Attempt</th>
                  <th>Depends on</th>
                  <th>Node</th>
                </tr>
              </thead>
              <tbody>
                {tasks.map((task) => {
                  const deps = task.dependencies.map((d) => shortId(d.taskId));
                  return (
                    <tr key={task.id}>
                      <td style={{ maxWidth: 320 }}>
                        <div className="cell-title">{task.title || task.id}</div>
                        {task.instructions ? (
                          <div className="muted" style={{ fontSize: 11.5, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: 320 }}>
                            {task.instructions.slice(0, 160)}
                          </div>
                        ) : null}
                      </td>
                      <td>
                        <Badge status={task.status} />
                        {task.blockerReason ? <div className="faint" style={{ fontSize: 11, marginTop: 2 }}>{task.blockerReason}</div> : null}
                      </td>
                      <td className="num">{task.attemptCount}/{task.maxAttempts}</td>
                      <td className="muted mono" style={{ fontSize: 11.5 }}>{deps.length ? deps.join(", ") : "—"}</td>
                      <td className="muted mono" style={{ fontSize: 11.5 }}>{task.assignment ? shortId(task.assignment.nodeId) : "—"}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </Card>
        </div>
      ) : null}

      {runs.length > 0 ? (
        <div className="section">
          <SectionHead title="Runs" count={runs.length} />
          <Card>
            <div className="list">
              {[...runs].reverse().map((run) => {
                const active = run.status === "running" || run.status === "queued";
                return (
                  <div key={run.id} style={{ padding: "10px 4px", borderBottom: "1px solid var(--border)", display: "flex", alignItems: "flex-start", gap: 10 }}>
                    <div style={{ minWidth: 0, flex: 1 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <span className="mono" style={{ fontSize: 12, color: "var(--text-2)" }}>{run.id}</span>
                        <Badge status={run.status} />
                        {run.taskId ? <PlainBadge>{shortId(run.taskId)}</PlainBadge> : null}
                      </div>
                      <div className="muted" style={{ fontSize: 11.5, marginTop: 4 }}>
                        {run.harnessId} / {run.transport} · {run.model} · {run.nodeId} · {timeAgo(run.createdAt)}
                      </div>
                      {run.output ? (
                        <div className="muted" style={{ fontSize: 12, marginTop: 6, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                          {run.output.slice(0, 220)}
                        </div>
                      ) : null}
                      {run.error ? <div style={{ fontSize: 12, marginTop: 6, color: "var(--err)" }}>{run.error}</div> : null}
                    </div>
                    {active ? (
                      <button className="btn danger" style={{ flex: "none" }} onClick={() => cancel(run.id)}>
                        <XIcon />
                      </button>
                    ) : null}
                  </div>
                );
              })}
            </div>
          </Card>
        </div>
      ) : null}

      {instances.length > 0 ? (
        <div className="section">
          <SectionHead title="Resident instances" count={instances.length} />
          <Card>
            <div className="list">
              {instances.map((instance) => (
                <InstanceRow key={instance.id} instance={instance} allocations={allocations} />
              ))}
            </div>
          </Card>
        </div>
      ) : null}

      {approvals.length > 0 ? (
        <div className="section">
          <SectionHead title="Approvals" count={approvals.length} />
          <div className="grid cols-2">
            {approvals.map((approval) => (
              <ApprovalCard key={approval.id} approval={approval} onResolved={reload} />
            ))}
          </div>
        </div>
      ) : null}

      {artifacts.length > 0 ? (
        <div className="section">
          <SectionHead title="Artifacts" count={artifacts.length} />
          <Card>
            <div className="list">
              {artifacts.map((artifact) => (
                <div key={artifact.id} style={{ padding: "8px 4px", borderBottom: "1px solid var(--border)", display: "flex", alignItems: "center", gap: 10 }}>
                  <div style={{ minWidth: 0, flex: 1 }}>
                    <span style={{ fontWeight: 500 }}>{artifact.name}</span>
                    <div className="muted mono" style={{ fontSize: 11 }}>
                      {shortId(artifact.id)} · {fmtBytes(artifact.sizeBytes)}
                      {artifact.uploaded ? "" : " · pending upload"}
                    </div>
                  </div>
                  {artifact.uploaded ? (
                    <button className="btn" onClick={() => downloadArtifact(artifact.name, artifact.downloadPath)}>Download</button>
                  ) : null}
                </div>
              ))}
            </div>
          </Card>
        </div>
      ) : null}

      {messages.length > 0 ? (
        <div className="section">
          <SectionHead title="Messages" count={messages.length} />
          <Card>
            <div className="list">
              {messages.slice(-20).map((message) => (
                <div className="msg" key={message.id}>
                  <div className="msg-head">
                    <PlainBadge>{message.kind}</PlainBadge>
                    <span className="mono">{shortId(message.id)}</span>
                    <span className="muted">{timeAgo(message.createdAt)}</span>
                  </div>
                  <div className="msg-body">{message.body}</div>
                </div>
              ))}
            </div>
          </Card>
        </div>
      ) : null}

      {!hasContent ? (
        <Card>
          <Empty icon={<ThreadIcon />} title="No activity in this thread yet" />
        </Card>
      ) : null}
    </>
  );
}

function InstanceRow({ instance, allocations }: { instance: AgentInstance; allocations: InstanceAllocation[] }) {
  const allocation = allocations.find((a) => a.instanceId === instance.id && OCCUPYING.has(a.status));
  const purpose = instance.purpose ? (instance.purpose.name || instance.purpose.title || "") : "";
  return (
    <div style={{ padding: "10px 4px", borderBottom: "1px solid var(--border)", display: "flex", alignItems: "center", gap: 10 }}>
      <div style={{ minWidth: 0, flex: 1 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span className="mono" style={{ fontSize: 12, color: "var(--text-2)" }}>{instance.id}</span>
          <Badge status={instance.status} />
          {purpose ? <span className="muted" style={{ fontSize: 12 }}>{purpose}</span> : null}
        </div>
        <div className="muted" style={{ fontSize: 11.5, marginTop: 4 }}>
          {allocation ? `${allocation.harnessId} / ${allocation.model} · ${allocation.nodeId}` : ""}
          {instance.autoManaged ? " · auto-managed" : ""}
        </div>
      </div>
      {instance.lease?.expiresAt ? (
        <div className="kv" style={{ flex: "none" }}>expires {timeAgo(instance.lease.expiresAt)}</div>
      ) : null}
    </div>
  );
}
