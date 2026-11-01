import { api } from "../api";
import { useLive, usePolledFetch } from "../hooks";
import type { NodeDetail } from "../types";
import { Badge, Card, Empty, ErrorCard, LoadingCard, PlainBadge, SectionHead } from "../components/ui";
import { ServerIcon } from "../components/Icons";
import { timeAgo } from "../format";

export function Nodes() {
  const { pollMs, refreshNonce } = useLive();
  const { data, error } = usePolledFetch<{ nodes: NodeDetail[] }>(
    () => api<{ nodes: NodeDetail[] }>("/api/nodes"),
    [refreshNonce],
    pollMs,
  );

  if (error) return <ErrorCard message={error} />;
  if (!data) return <LoadingCard />;

  const nodes = data.nodes;
  if (nodes.length === 0) {
    return (
      <Card>
        <Empty
          icon={<ServerIcon />}
          title="No worker nodes registered"
          sub={<>Start <code>orchestra slave</code> on a machine to bring it online.</>}
        />
      </Card>
    );
  }

  return (
    <div className="grid cols-2">
      {nodes.map((detail) => (
        <NodeCard key={detail.node.id} detail={detail} />
      ))}
    </div>
  );
}

function NodeCard({ detail }: { detail: NodeDetail }) {
  const node = detail.node;
  const cap = node.concurrency || 0;
  const runsPct = cap > 0 ? Math.min(100, Math.round((node.activeRuns / cap) * 100)) : 0;

  const harnesses = node.harnesses.filter((h) => h.available);
  const labels: string[] = [];
  const accelerators: string[] = [];
  const others: string[] = [];
  for (const evidence of detail.capabilityReport?.evidence ?? []) {
    if (!evidence.success) continue;
    if (evidence.capabilityId.startsWith("label:")) labels.push(evidence.capabilityId.slice(6));
    else if (evidence.capabilityId.startsWith("accelerator:")) accelerators.push(evidence.capabilityId.slice(12));
    else others.push(evidence.normalizedValue ? `${evidence.capabilityId} ${evidence.normalizedValue}` : evidence.capabilityId);
  }
  const repositories = detail.repositoryReport?.repositories ?? [];

  return (
    <Card>
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 12, marginBottom: 12 }}>
        <div style={{ minWidth: 0 }}>
          <div style={{ fontSize: 15, fontWeight: 600, letterSpacing: "-0.01em", display: "flex", alignItems: "center", gap: 8 }}>
            {node.name || node.id}
            <Badge status={node.status} />
          </div>
          <div className="muted mono" style={{ fontSize: 12, marginTop: 2 }}>{node.id}</div>
        </div>
        {detail.connected ? (
          <div style={{ textAlign: "right", flex: "none" }}>
            <div className="mono" style={{ fontSize: 12, color: "var(--text-2)" }}>
              {node.activeRuns}/{cap} runs
            </div>
            <div className={`bar ${runsPct >= 100 ? "warn" : ""}`} style={{ width: 88, marginTop: 5 }}>
              <span style={{ width: `${runsPct}%` }} />
            </div>
          </div>
        ) : null}
      </div>

      <div className="chips" style={{ marginBottom: 12 }}>
        <span className="chip">{node.platform || "unknown"}</span>
        <span className="chip">{node.kind}</span>
        <span className="chip">v{node.version || "?"}</span>
        {node.instanceCapacity != null ? (
          <span className="chip">{node.activeInstances ?? 0}/{node.instanceCapacity} instances</span>
        ) : null}
        {detail.synced === false ? <span className="chip" style={{ color: "var(--warn)" }}>not synced</span> : null}
      </div>

      {harnesses.length > 0 ? (
        <>
          <SectionHead title="Harnesses" />
          <div className="list" style={{ marginBottom: 12 }}>
            {harnesses.map((h) => {
              const models = (h.models ?? []).slice(0, 4);
              return (
                <div key={h.id} style={{ display: "flex", alignItems: "center", gap: 8, padding: "6px 0", borderBottom: "1px solid var(--border)" }}>
                  <span style={{ fontWeight: 500 }}>{h.label || h.id}</span>
                  <span className="muted" style={{ fontSize: 11 }}>{(h.transports ?? []).join(" · ")}</span>
                  <span className="chips" style={{ marginLeft: "auto" }}>
                    {models.map((m) => (
                      <span className="chip" key={m}>{m}</span>
                    ))}
                  </span>
                  {h.acp ? <PlainBadge>acp</PlainBadge> : null}
                </div>
              );
            })}
          </div>
        </>
      ) : null}

      {labels.length || accelerators.length || others.length ? (
        <>
          <SectionHead title="Capabilities" />
          <div className="chips" style={{ marginBottom: 12 }}>
            {labels.map((l) => (
              <span className="chip" key={l} style={{ color: "var(--accent-strong)" }}>{l}</span>
            ))}
            {accelerators.map((a) => (
              <span className="chip" key={a} style={{ color: "var(--info)" }}>{a}</span>
            ))}
            {others.map((o) => (
              <span className="chip" key={o}>{o}</span>
            ))}
          </div>
        </>
      ) : null}

      {repositories.length > 0 ? (
        <>
          <SectionHead title="Repositories" count={repositories.length} />
          <div className="list" style={{ marginBottom: 4 }}>
            {repositories.slice(0, 6).map((repo) => (
              <div key={repo.path} style={{ padding: "6px 0", borderBottom: "1px solid var(--border)", display: "flex", alignItems: "center", gap: 8 }}>
                <span className="mono" style={{ fontSize: 12, color: "var(--text-2)" }}>{repo.relativePath || "."}</span>
                <span className="muted" style={{ fontSize: 11, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                  {repo.repository ?? ""}
                </span>
                {repo.headBranch ? (
                  <span className="badge plain" style={{ marginLeft: "auto" }}>{repo.headBranch}</span>
                ) : null}
              </div>
            ))}
            {repositories.length > 6 ? (
              <div className="muted" style={{ fontSize: 11, padding: "6px 0" }}>+ {repositories.length - 6} more</div>
            ) : null}
          </div>
        </>
      ) : null}

      <div className="muted" style={{ fontSize: 11, marginTop: 10 }}>Last seen {timeAgo(node.lastSeen)}</div>
    </Card>
  );
}
