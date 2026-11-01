import { api } from "../api";
import { useLive, usePolledFetch } from "../hooks";
import type { ApprovalRequest } from "../types";
import { Badge, Card, Empty, ErrorCard, LoadingCard, SectionHead } from "../components/ui";
import { ShieldIcon } from "../components/Icons";
import { timeAgo } from "../format";
import { ApprovalCard } from "../components/ApprovalCard";

export function Approvals() {
  const { pollMs, refreshNonce } = useLive();
  const { data, error, reload } = usePolledFetch<{ approvals: ApprovalRequest[] }>(
    () => api<{ approvals: ApprovalRequest[] }>("/api/approvals"),
    [refreshNonce],
    pollMs,
  );

  if (error) return <ErrorCard message={error} />;
  if (!data) return <LoadingCard />;

  const all = [...data.approvals].reverse();
  const pending = all.filter((a) => a.status === "pending");
  const resolved = all.filter((a) => a.status !== "pending");

  if (all.length === 0) {
    return (
      <Card>
        <Empty
          icon={<ShieldIcon />}
          title="No permission requests"
          sub={<>When an agent needs permission, the request appears here for approval.</>}
        />
      </Card>
    );
  }

  return (
    <>
      {pending.length > 0 ? (
        <div className="section">
          <SectionHead title="Pending" count={pending.length} />
          <div className="grid cols-2">
            {pending.map((approval) => (
              <ApprovalCard key={approval.id} approval={approval} onResolved={reload} />
            ))}
          </div>
        </div>
      ) : null}

      {resolved.length > 0 ? (
        <div className="section">
          <SectionHead title="Resolved" count={resolved.length} />
          <Card>
            <div className="list">
              {resolved.slice(0, 40).map((approval) => (
                <div className="row" key={approval.id}>
                  <div className="row-main">
                    <div className="row-title">{approval.title}</div>
                    <div className="row-sub">
                      {approval.nodeId} · {approval.runId}
                    </div>
                  </div>
                  <div className="row-side">
                    <span className="muted mono" style={{ fontSize: 11 }}>
                      {timeAgo(approval.resolvedAt ?? approval.updatedAt)}
                    </span>
                    <Badge status={approval.status} />
                  </div>
                </div>
              ))}
            </div>
          </Card>
        </div>
      ) : null}
    </>
  );
}
