import { useEffect, useMemo, useRef } from "react";
import { api, AuthError, download, DownloadAbortedError, fetchThreadRoom } from "../api";
import { navigate, scrollToAnchor, useLive, usePolledFetch, useRoute } from "../hooks";
import type {
  AgentInstance,
  InstanceAllocation,
  ReadRoomResult,
  ThreadDetail as ThreadDetailType,
} from "../types";
import {
  Badge,
  Card,
  ErrorCard,
  LoadingCard,
  PlainBadge,
  SectionHead,
} from "../components/ui";
import { BackIcon, XIcon } from "../components/Icons";
import { shortId, timeAgo } from "../format";
import { toast } from "../toast";
import { ApprovalCard } from "../components/ApprovalCard";
import { ArtifactList } from "../components/ArtifactList";
import { BlackboardRoom } from "../components/BlackboardRoom";
import {
  NeedsAttentionSection,
  synthesizeThreadAttention,
} from "../components/NeedsAttention";
import { isUploadedImageArtifact, ProgressReel } from "../components/ProgressReel";

const OCCUPYING_STATUSES = new Set(["reserved", "provisioning", "active"]);

export function ThreadDetail({ id }: { id: string }) {
  const { pollMs, refreshNonce } = useLive();
  const route = useRoute();

  const detailState = usePolledFetch<ThreadDetailType>(
    () => api<ThreadDetailType>(`/api/threads/${encodeURIComponent(id)}`),
    [id, refreshNonce],
    pollMs,
  );
  const roomState = usePolledFetch<ReadRoomResult>(
    () => fetchThreadRoom(id),
    [id, refreshNonce],
    pollMs,
  );

  const lastScrolledAnchorRef = useRef<string>("");

  function reloadAll() {
    detailState.reload();
    roomState.reload();
  }

  const roomMessages = useMemo(
    () => roomState.data?.messages ?? [],
    [roomState.data],
  );

  const attentionItems = useMemo(
    () =>
      detailState.data
        ? synthesizeThreadAttention(detailState.data, roomMessages)
        : [],
    [detailState.data, roomMessages],
  );

  const imageArtifacts = useMemo(
    () =>
      detailState.data
        ? detailState.data.artifacts.filter(isUploadedImageArtifact)
        : [],
    [detailState.data],
  );

  useEffect(() => {
    const targetAnchor = route.anchor;
    if (!targetAnchor || !detailState.data) {
      return;
    }
    if (targetAnchor.startsWith("room-seq-") && !roomState.data) {
      return;
    }
    const anchorKey = `${id}:${targetAnchor}`;
    if (lastScrolledAnchorRef.current === anchorKey) {
      return;
    }
    const animationFrame = window.requestAnimationFrame(() => {
      if (scrollToAnchor(targetAnchor)) {
        lastScrolledAnchorRef.current = anchorKey;
      }
    });
    return () => window.cancelAnimationFrame(animationFrame);
  }, [id, route.anchor, detailState.data, roomState.data]);

  async function cancelRun(runId: string) {
    try {
      await api(`/api/cancel/${encodeURIComponent(runId)}`, { method: "POST" });
      toast(`Cancelled ${shortId(runId)}`, "ok");
      reloadAll();
    } catch (cancelError) {
      toast(
        cancelError instanceof Error ? cancelError.message : String(cancelError),
        "err",
      );
    }
  }

  async function downloadArtifact(name: string, path: string) {
    try {
      await download(path, name);
    } catch (downloadError) {
      if (downloadError instanceof DownloadAbortedError || downloadError instanceof AuthError) {
        return;
      }
      const message =
        downloadError instanceof Error ? downloadError.message : String(downloadError);
      toast(`Failed to download ${name}: ${message}`, "err");
    }
  }

  if (detailState.error) return <ErrorCard message={detailState.error} />;
  if (!detailState.data) return <LoadingCard />;

  const {
    thread,
    tasks,
    runs,
    instances,
    allocations,
    approvals,
    artifacts,
    messages,
  } = detailState.data;
  const isStalled = Boolean(detailState.data.stall) && detailState.data.stall?.stalled !== false;

  return (
    <>
      <div className="detail-head">
        <button
          className="btn ghost"
          onClick={() => navigate("/threads")}
          style={{ marginBottom: 12, paddingLeft: 8 }}
        >
          <BackIcon />
          <span>Threads</span>
        </button>
        <div className="detail-title">
          {thread.title}
          <Badge status={thread.status} />
          {isStalled ? <Badge status="stalled" label="stalled" /> : null}
        </div>
        <div className="detail-meta">
          <span className="kv">
            <b>id</b> {thread.id}
          </span>
          <span className="kv">
            <b>created</b> {timeAgo(thread.createdAt)}
          </span>
          <span className="kv">
            <b>updated</b> {timeAgo(thread.updatedAt)}
          </span>
          {detailState.data.attachment ? (
            <span className="kv">
              <b>attached</b> {detailState.data.attachment.clientId}
            </span>
          ) : null}
          {thread.orchestrator?.kind === "instance" && thread.orchestrator.instanceId ? (
            <span className="kv">
              <b>coordinator</b> {shortId(thread.orchestrator.instanceId)} · handled #
              {thread.orchestrator.lastHandledEventSequence ?? 0}
              {thread.orchestrator.pendingWakeTaskId
                ? ` · wake ${shortId(thread.orchestrator.pendingWakeTaskId)}`
                : (thread.orchestrator.pendingWakeEventSequence ?? 0) >
                    (thread.orchestrator.lastHandledEventSequence ?? 0)
                  ? ` · wake pending #${thread.orchestrator.pendingWakeEventSequence}`
                  : ""}
            </span>
          ) : null}
        </div>
        {thread.summary ? (
          <div className="muted" style={{ marginTop: 8, fontSize: 13 }}>
            {thread.summary}
          </div>
        ) : null}
      </div>

      <NeedsAttentionSection
        items={attentionItems}
        currentThreadId={thread.id}
        onReload={reloadAll}
      />

      <ProgressReel artifacts={imageArtifacts} onDownload={downloadArtifact} />

      {roomState.error ? (
        <div className="section">
          <ErrorCard message={roomState.error} />
        </div>
      ) : (
        <BlackboardRoom
          threadId={thread.id}
          roomHead={roomState.data?.room ?? detailState.data.room}
          messages={roomMessages}
          artifacts={artifacts}
          onReload={reloadAll}
          onDownloadArtifact={downloadArtifact}
        />
      )}

      {approvals.length > 0 ? (
        <div className="section">
          <SectionHead title="Approvals" count={approvals.length} />
          <div className="grid cols-2">
            {approvals.map((approval) => (
              <div key={approval.id} id={`approval-${approval.id}`} className="anchor-target">
                <ApprovalCard approval={approval} onResolved={reloadAll} />
              </div>
            ))}
          </div>
        </div>
      ) : null}

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
                  const dependencyIds = task.dependencies.map((dependency) =>
                    shortId(dependency.taskId),
                  );
                  return (
                    <tr key={task.id} id={`task-${task.id}`} className="anchor-target">
                      <td style={{ maxWidth: 320 }}>
                        <div className="cell-title">{task.title || task.id}</div>
                        {task.instructions ? (
                          <div
                            className="muted"
                            style={{
                              fontSize: 11.5,
                              overflow: "hidden",
                              textOverflow: "ellipsis",
                              whiteSpace: "nowrap",
                              maxWidth: 320,
                            }}
                          >
                            {task.instructions.slice(0, 160)}
                          </div>
                        ) : null}
                      </td>
                      <td>
                        <Badge status={task.status} />
                        {task.blockerReason ? (
                          <div className="faint" style={{ fontSize: 11, marginTop: 2 }}>
                            {task.blockerReason}
                          </div>
                        ) : null}
                      </td>
                      <td className="num">
                        {task.attemptCount}/{task.maxAttempts}
                      </td>
                      <td className="muted mono" style={{ fontSize: 11.5 }}>
                        {dependencyIds.length ? dependencyIds.join(", ") : "—"}
                      </td>
                      <td className="muted mono" style={{ fontSize: 11.5 }}>
                        {task.assignment ? shortId(task.assignment.nodeId) : "—"}
                      </td>
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
                  <div
                    key={run.id}
                    id={`run-${run.id}`}
                    className="anchor-target"
                    style={{
                      padding: "10px 4px",
                      borderBottom: "1px solid var(--border)",
                      display: "flex",
                      alignItems: "flex-start",
                      gap: 10,
                    }}
                  >
                    <div style={{ minWidth: 0, flex: 1 }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
                        <span className="mono" style={{ fontSize: 12, color: "var(--text-2)" }}>
                          {run.id}
                        </span>
                        <Badge status={run.status} />
                        {run.errorKind ? (
                          <Badge
                            status={run.errorKind}
                            label={run.errorKind.replace(/_/g, " ")}
                          />
                        ) : null}
                        {run.taskId ? <PlainBadge>{shortId(run.taskId)}</PlainBadge> : null}
                      </div>
                      <div className="muted" style={{ fontSize: 11.5, marginTop: 4 }}>
                        {run.harnessId} / {run.transport} · {run.model} · {run.nodeId} ·{" "}
                        {timeAgo(run.createdAt)}
                      </div>
                      {run.output ? (
                        <div
                          className="muted"
                          style={{
                            fontSize: 12,
                            marginTop: 6,
                            overflow: "hidden",
                            textOverflow: "ellipsis",
                            whiteSpace: "nowrap",
                          }}
                        >
                          {run.output.slice(0, 220)}
                        </div>
                      ) : null}
                      {run.error ? (
                        <div style={{ fontSize: 12, marginTop: 6, color: "var(--err)" }}>
                          {run.error}
                        </div>
                      ) : null}
                    </div>
                    {active ? (
                      <button
                        className="btn danger"
                        style={{ flex: "none" }}
                        onClick={() => cancelRun(run.id)}
                      >
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
                <InstanceRow
                  key={instance.id}
                  instance={instance}
                  allocations={allocations}
                />
              ))}
            </div>
          </Card>
        </div>
      ) : null}

      <ArtifactList artifacts={artifacts} />

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
    </>
  );
}

function InstanceRow({
  instance,
  allocations,
}: {
  instance: AgentInstance;
  allocations: InstanceAllocation[];
}) {
  const allocation = allocations.find(
    (candidate) =>
      candidate.instanceId === instance.id && OCCUPYING_STATUSES.has(candidate.status),
  );
  const purpose = instance.purpose
    ? instance.purpose.name || instance.purpose.title || ""
    : "";
  return (
    <div
      style={{
        padding: "10px 4px",
        borderBottom: "1px solid var(--border)",
        display: "flex",
        alignItems: "center",
        gap: 10,
      }}
    >
      <div style={{ minWidth: 0, flex: 1 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
          <span className="mono" style={{ fontSize: 12, color: "var(--text-2)" }}>
            {instance.id}
          </span>
          {instance.handle ? (
            <span className="room-mention">
              {instance.handle.startsWith("@") ? instance.handle : `@${instance.handle}`}
            </span>
          ) : null}
          <Badge status={instance.status} />
          {purpose ? (
            <span className="muted" style={{ fontSize: 12 }}>
              {purpose}
            </span>
          ) : null}
        </div>
        <div className="muted" style={{ fontSize: 11.5, marginTop: 4 }}>
          {allocation
            ? `${allocation.harnessId} / ${allocation.model} · ${allocation.nodeId}`
            : ""}
          {instance.autoManaged ? " · auto-managed" : ""}
        </div>
      </div>
      {instance.lease?.expiresAt ? (
        <div className="kv" style={{ flex: "none" }}>
          expires {timeAgo(instance.lease.expiresAt)}
        </div>
      ) : null}
    </div>
  );
}
