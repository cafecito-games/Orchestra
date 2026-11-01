import { useMemo, useState } from "react";
import type { ReactNode } from "react";
import { api, setToken } from "./api";
import { LiveContext, navigate, useFetch, useLive, usePolledFetch, useRoute, useTokenState } from "./hooks";
import type { LiveControls } from "./hooks";
import { DEFAULT_POLL_MS } from "./hooks";
import type { ApprovalRequest, Health } from "./types";
import {
  GridIcon,
  MailIcon,
  RefreshIcon,
  ServerIcon,
  ShieldIcon,
  SignOutIcon,
  SparkIcon,
  ThreadIcon,
} from "./components/Icons";
import { Login } from "./views/Login";
import { Overview } from "./views/Overview";
import { Nodes } from "./views/Nodes";
import { Threads } from "./views/Threads";
import { ThreadDetail } from "./views/ThreadDetail";
import { Approvals } from "./views/Approvals";
import { Mail, Mailbox } from "./views/Mail";

interface NavItem {
  name: string;
  label: string;
  icon: ReactNode;
}

const NAV: NavItem[] = [
  { name: "overview", label: "Overview", icon: <GridIcon /> },
  { name: "nodes", label: "Nodes", icon: <ServerIcon /> },
  { name: "threads", label: "Threads", icon: <ThreadIcon /> },
  { name: "approvals", label: "Approvals", icon: <ShieldIcon /> },
  { name: "mail", label: "Mail", icon: <MailIcon /> },
];

const TITLES: Record<string, [string, string]> = {
  overview: ["Overview", "Fleet health and activity at a glance"],
  nodes: ["Nodes", "Worker machines, harnesses, and capabilities"],
  threads: ["Threads", "Coordination threads and their task graphs"],
  thread: ["Thread", "Task graph, runs, and activity"],
  approvals: ["Approvals", "Pending worker permission requests"],
  mail: ["Mail", "Agent mailboxes across the fleet"],
  mailbox: ["Mail", "Agent mailbox"],
};

function activeFor(routeName: string, item: string): boolean {
  if (routeName === item) return true;
  if (routeName === "thread" && item === "threads") return true;
  if (routeName === "mailbox" && item === "mail") return true;
  return false;
}

export default function App() {
  const [token] = useTokenState();
  const route = useRoute();
  const health = useFetch<Health>(() => api<Health>("/healthz"), []).data;

  const [live, setLive] = useState(true);
  const [refreshNonce, setRefreshNonce] = useState(0);
  const liveControls = useMemo<LiveControls>(
    () => ({
      live,
      pollMs: live ? DEFAULT_POLL_MS : 0,
      refreshNonce,
      toggleLive: () => setLive((value) => !value),
      refresh: () => setRefreshNonce((value) => value + 1),
    }),
    [live, refreshNonce],
  );

  if (!token) {
    return <Login health={health} />;
  }

  return (
    <LiveContext.Provider value={liveControls}>
      <Shell route={route} health={health} />
    </LiveContext.Provider>
  );
}

function Shell({ route, health }: { route: ReturnType<typeof useRoute>; health: Health | null }) {
  const live = useLive();
  const pending = usePolledFetch<{ approvals: ApprovalRequest[] }>(
    () => api<{ approvals: ApprovalRequest[] }>("/api/approvals?status=pending"),
    [live.refreshNonce],
    live.pollMs,
  ).data;
  const pendingCount = pending?.approvals.length ?? 0;

  const [title, subtitle] = TITLES[route.name] ?? TITLES.overview!;
  const connected = health?.authMode === "token";

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">
            <SparkIcon />
          </div>
          <div>
            <div className="brand-name">Orchestra</div>
            <div className="brand-sub">Agent control plane</div>
          </div>
        </div>
        <nav className="nav">
          <div className="nav-label">Fleet</div>
          {NAV.map((item) => (
            <button
              key={item.name}
              className={`nav-item${activeFor(route.name, item.name) ? " active" : ""}`}
              onClick={() => navigate(`/${item.name}`)}
            >
              <span className="nav-icon">{item.icon}</span>
              <span>{item.label}</span>
              {item.name === "approvals" && pendingCount > 0 ? (
                <span className="nav-badge hot">{pendingCount}</span>
              ) : null}
            </button>
          ))}
        </nav>
        <div className="sidebar-foot">
          <div className="conn">
            <span className={`dot ${connected ? "ok" : "muted"}`} />
            <span>{connected ? "Connected" : "Unauthenticated"}</span>
          </div>
          {health ? (
            <div className="conn-meta">
              {health.hostName || "master"} · v{health.version}
            </div>
          ) : null}
          <button className="btn-signout" onClick={() => setToken("")}>
            <SignOutIcon />
            <span>Sign out</span>
          </button>
        </div>
      </aside>

      <div className="main">
        <div className="topbar">
          <div>
            <div className="topbar-title">{title}</div>
            <div className="topbar-sub">{subtitle}</div>
          </div>
          <div className="topbar-actions">
            <button
              className={`live${live.live ? " on" : ""}`}
              onClick={live.toggleLive}
              title={live.live ? "Pause auto-refresh" : "Resume auto-refresh"}
            >
              <span className="dot" />
              {live.live ? "Live" : "Paused"}
            </button>
            <button className="btn" onClick={live.refresh}>
              <RefreshIcon />
              <span>Refresh</span>
            </button>
          </div>
        </div>
        <div className="content">
          <View route={route} />
        </div>
      </div>
    </div>
  );
}

function View({ route }: { route: ReturnType<typeof useRoute> }) {
  switch (route.name) {
    case "nodes":
      return <Nodes />;
    case "threads":
      return <Threads />;
    case "thread":
      return <ThreadDetail id={route.id ?? ""} />;
    case "approvals":
      return <Approvals />;
    case "mail":
      return <Mail />;
    case "mailbox":
      return <Mailbox address={route.id ?? ""} />;
    case "overview":
    default:
      return <Overview />;
  }
}
