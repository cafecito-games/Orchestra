import type { ReactNode } from "react";

export type Tone = "ok" | "warn" | "err" | "info" | "muted" | "faint" | "accent" | "plain";

const STATUS_TONE: Record<string, Tone> = {
  online: "ok",
  busy: "warn",
  updating: "info",
  offline: "faint",
  queued: "muted",
  running: "info",
  completed: "ok",
  failed: "err",
  cancelled: "faint",
  pending: "muted",
  ready: "info",
  blocked: "warn",
  requested: "muted",
  provisioning: "info",
  idle: "muted",
  draining: "warn",
  released: "faint",
  approved: "ok",
  rejected: "err",
  expired: "faint",
  open: "info",
  resolved: "ok",
  active: "info",
  closed: "faint",
  streaming: "info",
  retained: "warn",
  cleaned: "faint",
};

export function toneForStatus(status: string): Tone {
  return STATUS_TONE[status] ?? "muted";
}

export function Badge({ status, label }: { status: string; label?: string }) {
  const tone = toneForStatus(status);
  return <span className={`badge ${tone}`}>{label ?? status}</span>;
}

export function PlainBadge({ children }: { children: ReactNode }) {
  return <span className="badge plain">{children}</span>;
}

export function Spinner() {
  return (
    <div className="loading">
      <div className="spinner" />
    </div>
  );
}

export function Empty({ title, sub, icon }: { title: string; sub?: ReactNode; icon?: ReactNode }) {
  return (
    <div className="empty">
      {icon}
      <div className="empty-title">{title}</div>
      {sub ? <div className="empty-sub">{sub}</div> : null}
    </div>
  );
}

export function SectionHead({
  title,
  count,
  action,
}: {
  title: string;
  count?: number;
  action?: ReactNode;
}) {
  return (
    <div className="section-head">
      <div className="section-title">{title}</div>
      {count != null ? <div className="section-count">{count}</div> : null}
      {action ? <div style={{ marginLeft: "auto" }}>{action}</div> : null}
    </div>
  );
}

export function Card({ children, className = "" }: { children: ReactNode; className?: string }) {
  return <div className={`card ${className}`}>{children}</div>;
}

export function CapacityBar({ used, total, tone }: { used: number; total: number; tone?: "warn" }) {
  const pct = total > 0 ? Math.min(100, Math.round((used / total) * 100)) : 0;
  return (
    <div className={`bar ${tone ?? ""}`} style={{ width: 72 }}>
      <span style={{ width: `${pct}%` }} />
    </div>
  );
}

export function ErrorCard({ message }: { message: string }) {
  return (
    <Card>
      <Empty
        title="Something went wrong"
        sub={message}
        icon={
          <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.4">
            <circle cx="8" cy="8" r="6" />
            <path d="M8 5v3.5M8 11h.01" />
          </svg>
        }
      />
    </Card>
  );
}

export function LoadingCard() {
  return (
    <div className="loading">
      <div className="spinner" />
    </div>
  );
}
