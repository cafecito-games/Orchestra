import { api } from "../api";
import { useLive, usePolledFetch, navigate } from "../hooks";
import type { ListMailboxesResult, ListMailTrafficResult, ReadMailResult } from "../types";
import { Badge, Card, Empty, ErrorCard, LoadingCard, SectionHead } from "../components/ui";
import { BackIcon, MailIcon } from "../components/Icons";
import { timeAgo } from "../format";

export function Mail() {
  const { pollMs, refreshNonce } = useLive();
  const mailboxes = usePolledFetch<ListMailboxesResult>(
    () => api<ListMailboxesResult>("/api/mail/agents"),
    [refreshNonce],
    pollMs,
  );
  const traffic = usePolledFetch<ListMailTrafficResult>(
    () => api<ListMailTrafficResult>("/api/mail/traffic?limit=200"),
    [refreshNonce],
    pollMs,
  );

  if (mailboxes.error || traffic.error) return <ErrorCard message={mailboxes.error ?? traffic.error ?? "Unable to load mail"} />;
  if (!mailboxes.data || !traffic.data) return <LoadingCard />;

  const boxes = [...mailboxes.data.mailboxes].sort(
    (a, b) => (b.unreadCount ?? 0) - (a.unreadCount ?? 0) || a.address.localeCompare(b.address),
  );
  const messages = traffic.data.messages;

  if (boxes.length === 0 && messages.length === 0) {
    return (
      <Card>
        <Empty
          icon={<MailIcon />}
          title="No agent mailboxes"
          sub={<>Mailboxes appear as agents connect to Orchestra.</>}
        />
      </Card>
    );
  }

  return (
    <>
      <section className="section">
        <SectionHead title="Traffic" count={messages.length} />
        <Card>
          {messages.length === 0 ? (
            <Empty icon={<MailIcon />} title="No mail traffic yet" />
          ) : (
            <div className="list">
              {messages.map((message) => (
                <div className="msg" key={message.id}>
                  <div className="msg-head">
                    <span className="mono">{message.from} → {message.to}</span>
                    <span className="muted">{timeAgo(message.createdAt)}</span>
                    {!message.read ? <span className="msg-unread">unread</span> : null}
                  </div>
                  {message.subject ? <div className="msg-subject">{message.subject}</div> : null}
                  <div className="msg-body">{message.body}</div>
                </div>
              ))}
            </div>
          )}
        </Card>
      </section>

      <section className="section">
        <SectionHead title="Mailboxes" count={boxes.length} />
        <Card>
          <div className="list">
            {boxes.map((mailbox) => (
              <div className="row click" key={mailbox.id} onClick={() => navigate(`/mail/${encodeURIComponent(mailbox.address)}`)}>
                <div className="row-main">
                  <div className="row-title mono">{mailbox.address}</div>
                  <div className="row-sub">
                    {mailbox.kind}
                    {mailbox.harness ? ` · ${mailbox.harness}` : ""}
                    {mailbox.workspace ? ` · ${mailbox.workspace}` : ""}
                  </div>
                </div>
                <div className="row-side">
                  {mailbox.unreadCount > 0 ? (
                    <span className="badge warn">{mailbox.unreadCount} unread</span>
                  ) : null}
                  <Badge status={mailbox.status} />
                </div>
              </div>
            ))}
          </div>
        </Card>
      </section>
    </>
  );
}

export function Mailbox({ address }: { address: string }) {
  const { pollMs, refreshNonce } = useLive();
  const { data, error } = usePolledFetch<ReadMailResult>(
    () => api<ReadMailResult>(`/api/mail/messages?address=${encodeURIComponent(address)}&limit=100&markRead=false`),
    [address, refreshNonce],
    pollMs,
  );

  if (error) return <ErrorCard message={error} />;
  if (!data) return <LoadingCard />;

  const mailbox = data.mailbox;
  const messages = data.messages;

  return (
    <>
      <div className="detail-head">
        <button className="btn ghost" onClick={() => navigate("/mail")} style={{ marginBottom: 12, paddingLeft: 8 }}>
          <BackIcon />
          <span>Mailboxes</span>
        </button>
        <div className="detail-title mono">
          {mailbox.address}
          <Badge status={mailbox.status} />
        </div>
        <div className="detail-meta">
          <span className="kv"><b>kind</b> {mailbox.kind}</span>
          {mailbox.harness ? <span className="kv"><b>harness</b> {mailbox.harness}</span> : null}
          {mailbox.workspace ? <span className="kv"><b>workspace</b> {mailbox.workspace}</span> : null}
          {mailbox.aliases?.length ? <span className="kv"><b>aliases</b> {mailbox.aliases.join(", ")}</span> : null}
        </div>
      </div>

      {messages.length === 0 ? (
        <Card>
          <Empty icon={<MailIcon />} title="No messages" />
        </Card>
      ) : (
        <Card>
          <div className="list">
            {[...messages].reverse().map((message) => (
              <div className="msg" key={message.id}>
                <div className="msg-head">
                  <span className="mono">{message.from} → {message.to}</span>
                  <span className="muted">{timeAgo(message.createdAt)}</span>
                  {!message.read ? <span className="msg-unread">unread</span> : null}
                </div>
                {message.subject ? <div className="msg-subject">{message.subject}</div> : null}
                <div className="msg-body">{message.body}</div>
              </div>
            ))}
          </div>
        </Card>
      )}
    </>
  );
}
