import { api } from "../api";
import { useLive, usePolledFetch, navigate } from "../hooks";
import type { Thread } from "../types";
import { Badge, Card, Empty, ErrorCard, LoadingCard } from "../components/ui";
import { ThreadIcon } from "../components/Icons";
import { timeAgo } from "../format";

export function Threads() {
  const { pollMs, refreshNonce } = useLive();
  const { data, error } = usePolledFetch<{ threads: Thread[] }>(
    () => api<{ threads: Thread[] }>("/api/threads?limit=100"),
    [refreshNonce],
    pollMs,
  );

  if (error) return <ErrorCard message={error} />;
  if (!data) return <LoadingCard />;

  const threads = data.threads;
  if (threads.length === 0) {
    return (
      <Card>
        <Empty
          icon={<ThreadIcon />}
          title="No threads yet"
          sub={<>Threads appear when an agent calls <code>create_thread</code>.</>}
        />
      </Card>
    );
  }

  return (
    <Card>
      <div className="list">
        {threads.map((thread) => (
          <div className="row click" key={thread.id} onClick={() => navigate(`/thread/${thread.id}`)}>
            <div className="row-main">
              <div className="row-title">{thread.title}</div>
              <div className="row-sub">
                <span className="mono">{thread.id}</span>
                {thread.summary ? ` · ${thread.summary.slice(0, 120)}` : ""}
              </div>
            </div>
            <div className="row-side">
              <span className="muted mono" style={{ fontSize: 11 }}>{timeAgo(thread.updatedAt)}</span>
              <Badge status={thread.status} />
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
}
