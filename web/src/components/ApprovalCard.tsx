import { useState } from "react";
import { api } from "../api";
import type { ApprovalRequest } from "../types";
import { Badge, Card } from "./ui";
import { CheckIcon, XIcon } from "./Icons";
import { shortId, timeAgo } from "../format";
import { toast } from "../toast";

export function ApprovalCard({
  approval,
  onResolved,
}: {
  approval: ApprovalRequest;
  onResolved?: () => void;
}) {
  const [busy, setBusy] = useState<string | null>(null);
  const pending = approval.status === "pending";
  const allowOptions = approval.options.filter((o) => o.kind.startsWith("allow"));
  const rejectOptions = approval.options.filter((o) => o.kind.startsWith("reject"));

  async function resolve(status: "approved" | "rejected", optionId: string) {
    setBusy(status);
    try {
      await api(`/api/approvals/${encodeURIComponent(approval.id)}/resolve`, {
        method: "POST",
        body: { status, optionId },
      });
      toast(status === "approved" ? "Approval approved" : "Approval rejected", "ok");
      onResolved?.();
    } catch (err) {
      toast(err instanceof Error ? err.message : String(err), "err");
      setBusy(null);
    }
  }

  return (
    <Card>
      <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 10, marginBottom: 8 }}>
        <div style={{ fontSize: 14, fontWeight: 600, letterSpacing: "-0.01em" }}>{approval.title}</div>
        <Badge status={approval.status} />
      </div>
      {approval.detail ? <div className="pre" style={{ marginBottom: 10 }}>{approval.detail}</div> : null}
      <div className="chips" style={{ marginBottom: 12 }}>
        <span className="chip">{approval.nodeId}</span>
        <span className="chip">{shortId(approval.runId)}</span>
        {approval.taskId ? <span className="chip">{shortId(approval.taskId)}</span> : null}
        <span className="chip">{timeAgo(approval.requestedAt)}</span>
      </div>

      {pending ? (
        <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
          {allowOptions.map((option) => (
            <button
              key={option.id}
              className="btn primary"
              disabled={busy !== null}
              onClick={() => resolve("approved", option.id)}
            >
              <CheckIcon />
              <span>{option.label}</span>
            </button>
          ))}
          {allowOptions.length === 0 ? (
            <button className="btn primary" disabled={busy !== null} onClick={() => resolve("approved", "")}>
              <CheckIcon />
              <span>Approve</span>
            </button>
          ) : null}
          {rejectOptions.map((option) => (
            <button
              key={option.id}
              className="btn"
              disabled={busy !== null}
              onClick={() => resolve("rejected", option.id)}
            >
              <XIcon />
              <span>{option.label}</span>
            </button>
          ))}
          {rejectOptions.length === 0 ? (
            <button className="btn danger" disabled={busy !== null} onClick={() => resolve("rejected", "")}>
              <XIcon />
              <span>Reject</span>
            </button>
          ) : null}
        </div>
      ) : null}
    </Card>
  );
}
