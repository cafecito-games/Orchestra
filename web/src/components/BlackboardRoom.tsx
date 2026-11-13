import { useMemo, useRef, useState } from "react";
import type { FormEvent, KeyboardEvent, ReactNode } from "react";
import { api, generateIdempotencyKey } from "../api";
import { fullTime, shortId, timeAgo } from "../format";
import { replaceThreadAnchor, scrollToAnchor } from "../hooks";
import { toast } from "../toast";
import type {
  Artifact,
  NudgeRoomResult,
  PostRoomMessageRequest,
  PostRoomMessageResult,
  RoomAuthor,
  RoomMessage,
  RoomMessageKind,
  ThreadRoomHead,
} from "../types";
import { BellIcon, SendIcon, ThreadIcon } from "./Icons";
import { Card, Empty, SectionHead } from "./ui";

export const ROOM_MESSAGE_KINDS: readonly RoomMessageKind[] = [
  "guidance",
  "decision",
  "contract",
  "question",
  "answer",
  "finding",
  "gotcha",
];

const INLINE_TOKEN_PATTERN =
  /(^|[^a-zA-Z0-9._+\-&#])(@#?[a-zA-Z0-9](?:[a-zA-Z0-9._-]*[a-zA-Z0-9_-])?|#[1-9]\d*\b)/g;

export function computeSupersededByMap(messages: RoomMessage[]): Map<number, number> {
  const supersededBy = new Map<number, number>();
  for (const entry of messages) {
    if (entry.supersedesSequence != null && entry.supersedesSequence > 0) {
      const existingReplacement = supersededBy.get(entry.supersedesSequence);
      if (existingReplacement == null || entry.seq > existingReplacement) {
        supersededBy.set(entry.supersedesSequence, entry.seq);
      }
    }
  }
  return supersededBy;
}

export function resolveActiveSequence(
  sequenceNumber: number,
  supersededBy: ReadonlyMap<number, number>,
): number {
  let currentSequence = sequenceNumber;
  const visitedSequences = new Set<number>();
  while (supersededBy.has(currentSequence) && !visitedSequences.has(currentSequence)) {
    visitedSequences.add(currentSequence);
    currentSequence = supersededBy.get(currentSequence)!;
  }
  return currentSequence;
}

export function computeAnsweredByMap(
  messages: RoomMessage[],
  supersededBy: ReadonlyMap<number, number>,
): Map<number, number> {
  const answeredBy = new Map<number, number>();
  for (const entry of messages) {
    if (
      entry.kind === "answer" &&
      entry.replyToSequence != null &&
      entry.seq > entry.replyToSequence
    ) {
      const effectiveAnswerSequence = resolveActiveSequence(entry.seq, supersededBy);
      const previousAnswerSequence = answeredBy.get(entry.replyToSequence);
      if (
        previousAnswerSequence == null ||
        effectiveAnswerSequence > previousAnswerSequence
      ) {
        answeredBy.set(entry.replyToSequence, effectiveAnswerSequence);
      }
    }
  }
  return answeredBy;
}

export function formatRoomAuthor(author: RoomAuthor): string {
  if (author.handle) {
    return author.handle.startsWith("@") ? author.handle : `@${author.handle}`;
  }
  if (author.kind === "task") {
    if (author.title && author.taskId) {
      return `${author.title} · ${shortId(author.taskId)}`;
    }
    return author.title || shortId(author.taskId) || shortId(author.runId) || "task";
  }
  if (author.kind === "orchestrator") {
    return author.clientId ? `orchestrator · ${author.clientId}` : "orchestrator";
  }
  if (author.kind === "operator") {
    return author.operatorId ? `operator · ${author.operatorId}` : "operator";
  }
  return author.kind || "agent";
}

function normalizeChannel(channel?: string): string {
  const trimmed = channel?.trim() ?? "";
  return trimmed || "main";
}

function renderRoomBody(
  body: string,
  onSelectSequence: (sequence: number) => void,
): ReactNode[] {
  const nodes: ReactNode[] = [];
  let cursor = 0;
  let tokenKey = 0;

  INLINE_TOKEN_PATTERN.lastIndex = 0;
  let match = INLINE_TOKEN_PATTERN.exec(body);
  while (match !== null) {
    const prefix = match[1] ?? "";
    const token = match[2] ?? "";
    const tokenStart = match.index + prefix.length;

    if (tokenStart > cursor) {
      nodes.push(body.slice(cursor, tokenStart));
    }

    if (token.startsWith("@")) {
      nodes.push(
        <span key={`mention-${tokenKey}`} className="room-mention">
          {token}
        </span>,
      );
    } else if (token.startsWith("#")) {
      const sequenceNumber = Number.parseInt(token.slice(1), 10);
      if (Number.isFinite(sequenceNumber) && sequenceNumber > 0) {
        nodes.push(
          <button
            key={`seq-${tokenKey}`}
            type="button"
            className="room-inline-seq"
            onClick={() => onSelectSequence(sequenceNumber)}
            title={`Jump to room entry #${sequenceNumber}`}
          >
            {token}
          </button>,
        );
      } else {
        nodes.push(token);
      }
    } else {
      nodes.push(token);
    }

    tokenKey += 1;
    cursor = tokenStart + token.length;
    match = INLINE_TOKEN_PATTERN.exec(body);
  }

  if (cursor < body.length) {
    nodes.push(body.slice(cursor));
  }
  return nodes;
}

export interface BlackboardRoomProps {
  threadId: string;
  roomHead?: ThreadRoomHead;
  messages: RoomMessage[];
  artifacts: Artifact[];
  onReload: () => void;
  onDownloadArtifact?: (name: string, path: string) => void;
}

export function BlackboardRoom({
  threadId,
  roomHead,
  messages,
  artifacts,
  onReload,
  onDownloadArtifact,
}: BlackboardRoomProps) {
  const [selectedChannel, setSelectedChannel] = useState<string>("all");
  const [kind, setKind] = useState<RoomMessageKind>("guidance");
  const [topic, setTopic] = useState<string>("");
  const [body, setBody] = useState<string>("");
  const [nudgeActiveTasks, setNudgeActiveTasks] = useState<boolean>(false);
  const [replyToSequence, setReplyToSequence] = useState<number | null>(null);
  const [supersedesSequence, setSupersedesSequence] = useState<number | null>(null);
  const [submittingMessage, setSubmittingMessage] = useState<boolean>(false);
  const [nudgingAll, setNudgingAll] = useState<boolean>(false);
  const composerTextareaRef = useRef<HTMLTextAreaElement | null>(null);
  const pendingPostKeyRef = useRef<{ signature: string; key: string } | null>(null);

  const supersededBy = useMemo(() => computeSupersededByMap(messages), [messages]);
  const answeredBy = useMemo(
    () => computeAnsweredByMap(messages, supersededBy),
    [messages, supersededBy],
  );

  const artifactById = useMemo(() => {
    const lookup = new Map<string, Artifact>();
    for (const artifact of artifacts) {
      lookup.set(artifact.id, artifact);
    }
    return lookup;
  }, [artifacts]);

  const channels = useMemo(() => {
    const discovered = new Set<string>();
    for (const entry of messages) {
      discovered.add(normalizeChannel(entry.channel));
    }
    return Array.from(discovered);
  }, [messages]);

  const visibleMessages = useMemo(() => {
    if (selectedChannel === "all") {
      return messages;
    }
    return messages.filter((entry) => normalizeChannel(entry.channel) === selectedChannel);
  }, [messages, selectedChannel]);

  const headSequence = roomHead?.headSequence ?? messages[messages.length - 1]?.seq ?? 0;
  const canNudgeAll = headSequence > 0 && !nudgingAll;

  function jumpToSequence(sequenceNumber: number) {
    const anchorId = `room-seq-${sequenceNumber}`;
    if (selectedChannel !== "all") {
      const targetMessage = messages.find((entry) => entry.seq === sequenceNumber);
      if (targetMessage && normalizeChannel(targetMessage.channel) !== selectedChannel) {
        setSelectedChannel("all");
        window.requestAnimationFrame(() => {
          scrollToAnchor(anchorId);
        });
        replaceThreadAnchor(threadId, anchorId);
        return;
      }
    }
    scrollToAnchor(anchorId);
    replaceThreadAnchor(threadId, anchorId);
  }

  function startReply(targetMessage: RoomMessage) {
    setReplyToSequence(targetMessage.seq);
    if (targetMessage.kind === "question") {
      setKind("answer");
    }
    if (targetMessage.topic && !topic) {
      setTopic(targetMessage.topic);
    }
    composerTextareaRef.current?.focus();
  }

  function startSupersede(targetMessage: RoomMessage) {
    setSupersedesSequence(targetMessage.seq);
    const matchingKind = ROOM_MESSAGE_KINDS.find((candidate) => candidate === targetMessage.kind);
    if (matchingKind) {
      setKind(matchingKind);
    }
    if (targetMessage.topic && !topic) {
      setTopic(targetMessage.topic);
    }
    composerTextareaRef.current?.focus();
  }

  async function submitMessage(event?: FormEvent) {
    event?.preventDefault();
    const trimmedBody = body.trim();
    const trimmedTopic = topic.trim();
    if (!trimmedBody || submittingMessage) {
      return;
    }

    const draftSignature = JSON.stringify([
      threadId,
      kind,
      trimmedTopic,
      trimmedBody,
      replyToSequence,
      supersedesSequence,
      nudgeActiveTasks,
    ]);
    if (pendingPostKeyRef.current?.signature !== draftSignature) {
      pendingPostKeyRef.current = {
        signature: draftSignature,
        key: generateIdempotencyKey(),
      };
    }
    const idempotencyKey = pendingPostKeyRef.current.key;

    const payload: PostRoomMessageRequest = {
      idempotencyKey,
      kind,
      topic: trimmedTopic || undefined,
      body: trimmedBody,
      replyToSequence: replyToSequence ?? undefined,
      supersedesSequence: supersedesSequence ?? undefined,
      nudge: nudgeActiveTasks || undefined,
    };

    setSubmittingMessage(true);
    try {
      const response = await api<PostRoomMessageResult>(
        `/api/threads/${encodeURIComponent(threadId)}/room/messages`,
        {
          method: "POST",
          body: payload,
        },
      );
      pendingPostKeyRef.current = null;
      setBody("");
      setTopic("");
      setReplyToSequence(null);
      setSupersedesSequence(null);
      const nudgedSuffix =
        response.nudgedTaskIds.length > 0
          ? ` · nudged ${response.nudgedTaskIds.length} task${response.nudgedTaskIds.length === 1 ? "" : "s"}`
          : "";
      toast(`Posted #${response.message.seq} (${response.message.kind})${nudgedSuffix}`, "ok");
      onReload();
    } catch (submitError) {
      toast(submitError instanceof Error ? submitError.message : String(submitError), "err");
    } finally {
      setSubmittingMessage(false);
    }
  }

  async function nudgeAllTasks() {
    if (!canNudgeAll) {
      return;
    }
    setNudgingAll(true);
    try {
      const response = await api<NudgeRoomResult>(
        `/api/threads/${encodeURIComponent(threadId)}/room/nudge`,
        {
          method: "POST",
          body: { idempotencyKey: generateIdempotencyKey() },
        },
      );
      const count = response.nudgedTaskIds.length;
      toast(
        count > 0
          ? `Nudged ${count} active task${count === 1 ? "" : "s"} through #${response.requiredSequence}`
          : `Room required sequence set to #${response.requiredSequence} (no active tasks)`,
        "ok",
      );
      onReload();
    } catch (nudgeError) {
      toast(nudgeError instanceof Error ? nudgeError.message : String(nudgeError), "err");
    } finally {
      setNudgingAll(false);
    }
  }

  function handleComposerKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
      event.preventDefault();
      void submitMessage();
    }
  }

  return (
    <div className="section">
      <SectionHead
        title="Blackboard room"
        count={messages.length}
        action={
          <div className="room-head-actions">
            {headSequence > 0 ? (
              <span className="kv">
                <b>head</b> #{headSequence}
              </span>
            ) : null}
            <button
              type="button"
              className="btn"
              disabled={!canNudgeAll}
              onClick={nudgeAllTasks}
              title={
                headSequence > 0
                  ? "Require all active tasks in this thread to read through the latest room sequence"
                  : "Post a room message before nudging tasks"
              }
            >
              <BellIcon />
              <span>{nudgingAll ? "Nudging…" : "Nudge all"}</span>
            </button>
          </div>
        }
      />
      <Card className="room-card">
        {channels.length > 1 ? (
          <div className="room-channels-bar">
            <button
              type="button"
              className={`room-channel-tab${selectedChannel === "all" ? " active" : ""}`}
              onClick={() => setSelectedChannel("all")}
            >
              All channels
            </button>
            {channels.map((channelName) => (
              <button
                key={channelName}
                type="button"
                className={`room-channel-tab${selectedChannel === channelName ? " active" : ""}`}
                onClick={() => setSelectedChannel(channelName)}
              >
                #{channelName}
              </button>
            ))}
          </div>
        ) : null}

        {visibleMessages.length === 0 ? (
          <div className="room-empty-wrap">
            <Empty
              icon={<ThreadIcon />}
              title="No blackboard entries yet"
              sub="Post contracts, decisions, findings, questions, or operator guidance for tasks in this thread."
            />
          </div>
        ) : (
          <div className="room-feed">
            {visibleMessages.map((entry) => {
              const directReplacementSequence = supersededBy.get(entry.seq);
              const isSuperseded = directReplacementSequence != null;
              const replacementSequence = isSuperseded
                ? resolveActiveSequence(entry.seq, supersededBy)
                : undefined;
              const answerSequence =
                entry.kind === "question" ? answeredBy.get(entry.seq) : undefined;
              const entryChannel = entry.channel?.trim();

              return (
                <div
                  key={entry.id}
                  id={`room-seq-${entry.seq}`}
                  className={`room-entry${isSuperseded ? " superseded" : ""}`}
                >
                  <div className="room-entry-head">
                    <a
                      href={`#/thread/${encodeURIComponent(threadId)}?anchor=room-seq-${entry.seq}`}
                      className="room-seq-badge"
                      onClick={(event) => {
                        event.preventDefault();
                        jumpToSequence(entry.seq);
                      }}
                      title={`Anchor #room-seq-${entry.seq}`}
                    >
                      #{entry.seq}
                    </a>
                    <span className={`room-kind ${entry.kind}`}>{entry.kind}</span>
                    {entryChannel ? (
                      <span className="room-pill channel">#{entryChannel}</span>
                    ) : null}
                    {entry.topic ? <span className="room-pill topic">{entry.topic}</span> : null}

                    {entry.replyToSequence != null ? (
                      <button
                        type="button"
                        className="room-thread-link"
                        onClick={() => jumpToSequence(entry.replyToSequence!)}
                        title={`In reply to #${entry.replyToSequence}`}
                      >
                        ↳ #{entry.replyToSequence}
                      </button>
                    ) : null}

                    {entry.supersedesSequence != null ? (
                      <button
                        type="button"
                        className="room-thread-link supersedes"
                        onClick={() => jumpToSequence(entry.supersedesSequence!)}
                        title={`Supersedes #${entry.supersedesSequence}`}
                      >
                        supersedes #{entry.supersedesSequence}
                      </button>
                    ) : null}

                    {replacementSequence != null ? (
                      <button
                        type="button"
                        className="room-thread-link superseded-by"
                        onClick={() => jumpToSequence(replacementSequence)}
                        title={`Superseded by #${replacementSequence}`}
                      >
                        superseded by #{replacementSequence}
                      </button>
                    ) : null}

                    {answerSequence != null ? (
                      <button
                        type="button"
                        className="room-thread-link answered"
                        onClick={() => jumpToSequence(answerSequence)}
                        title={`Answered in #${answerSequence}`}
                      >
                        answered in #{answerSequence}
                      </button>
                    ) : null}

                    <span className="room-author">{formatRoomAuthor(entry.author)}</span>
                    <span className="room-time" title={fullTime(entry.createdAt)}>
                      {timeAgo(entry.createdAt)}
                    </span>

                    <div className="room-entry-actions">
                      <button
                        type="button"
                        className="room-action-btn"
                        onClick={() => startReply(entry)}
                        title={`Reply to #${entry.seq}`}
                      >
                        Reply
                      </button>
                      {!isSuperseded && (entry.kind === "decision" || entry.kind === "contract" || entry.kind === "guidance") ? (
                        <button
                          type="button"
                          className="room-action-btn"
                          onClick={() => startSupersede(entry)}
                          title={`Supersede #${entry.seq}`}
                        >
                          Supersede
                        </button>
                      ) : null}
                    </div>
                  </div>

                  <div className="room-entry-body">
                    {renderRoomBody(entry.body, jumpToSequence)}
                  </div>

                  {entry.artifactIds && entry.artifactIds.length > 0 ? (
                    <div className="room-entry-artifacts">
                      {entry.artifactIds.map((artifactId) => {
                        const linkedArtifact = artifactById.get(artifactId);
                        if (!linkedArtifact) {
                          return (
                            <span key={artifactId} className="chip">
                              {shortId(artifactId)}
                            </span>
                          );
                        }
                        return linkedArtifact.uploaded && onDownloadArtifact ? (
                          <button
                            key={artifactId}
                            type="button"
                            className="room-artifact-chip"
                            onClick={() =>
                              onDownloadArtifact(linkedArtifact.name, linkedArtifact.downloadPath)
                            }
                          >
                            <span>{linkedArtifact.name}</span>
                            <span className="muted">{shortId(linkedArtifact.id)}</span>
                          </button>
                        ) : (
                          <span key={artifactId} className="chip">
                            {linkedArtifact.name}
                          </span>
                        );
                      })}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        )}

        <form className="room-composer" onSubmit={submitMessage}>
          {replyToSequence != null || supersedesSequence != null ? (
            <div className="room-composer-context">
              {replyToSequence != null ? (
                <span className="room-context-pill">
                  <span>Replying to #{replyToSequence}</span>
                  <button
                    type="button"
                    onClick={() => setReplyToSequence(null)}
                    aria-label="Clear reply target"
                  >
                    ×
                  </button>
                </span>
              ) : null}
              {supersedesSequence != null ? (
                <span className="room-context-pill warn">
                  <span>Superseding #{supersedesSequence}</span>
                  <button
                    type="button"
                    onClick={() => setSupersedesSequence(null)}
                    aria-label="Clear supersedes target"
                  >
                    ×
                  </button>
                </span>
              ) : null}
            </div>
          ) : null}

          <div className="room-composer-top">
            <label className="room-select-wrap">
              <span className="sr-only">Kind</span>
              <select
                className="room-select"
                value={kind}
                onChange={(event) => setKind(event.target.value as RoomMessageKind)}
                aria-label="Message kind"
              >
                {ROOM_MESSAGE_KINDS.map((optionKind) => (
                  <option key={optionKind} value={optionKind}>
                    {optionKind}
                  </option>
                ))}
              </select>
            </label>
            <input
              type="text"
              className="room-topic-input"
              placeholder="Topic (optional)"
              maxLength={256}
              value={topic}
              onChange={(event) => setTopic(event.target.value)}
              aria-label="Message topic"
            />
          </div>

          <textarea
            ref={composerTextareaRef}
            className="room-body-input"
            rows={2}
            placeholder="Write to the blackboard room… Reference @handle or #seq (⌘/Ctrl+Enter to send)"
            value={body}
            onChange={(event) => setBody(event.target.value)}
            onKeyDown={handleComposerKeyDown}
            aria-label="Message body"
          />

          <div className="room-composer-foot">
            <label className="room-nudge-toggle">
              <input
                type="checkbox"
                checked={nudgeActiveTasks}
                onChange={(event) => setNudgeActiveTasks(event.target.checked)}
              />
              <span>Nudge active tasks</span>
            </label>
            <div className="room-composer-actions">
              <button
                type="submit"
                className="btn primary"
                disabled={submittingMessage || body.trim().length === 0}
              >
                <SendIcon />
                <span>{submittingMessage ? "Posting…" : "Post to room"}</span>
              </button>
            </div>
          </div>
        </form>
      </Card>
    </div>
  );
}
