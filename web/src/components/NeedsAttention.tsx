import { useState } from "react";
import { shortId, timeAgo } from "../format";
import { navigate, navigateToThread, replaceThreadAnchor, scrollToAnchor } from "../hooks";
import type {
  ApprovalRequest,
  RoomMessage,
  Run,
  ThreadDetail,
  ThreadStallCondition,
  ThreadStallReport,
} from "../types";
import { computeAnsweredByMap, computeSupersededByMap, formatRoomAuthor } from "./BlackboardRoom";
import { AlertIcon, ChevronRightIcon } from "./Icons";
import { Card, SectionHead } from "./ui";
import type { Tone } from "./ui";

export type AttentionRank = 1 | 2 | 3 | 4;

export interface AttentionItem {
  id: string;
  rank: AttentionRank;
  category:
    | "approval"
    | "stall"
    | "unanswered_question"
    | "quota_exhausted"
    | "rate_limited"
    | "run_failed"
    | "decision"
    | "contract";
  badgeLabel: string;
  tone: Tone;
  title: string;
  detail?: string;
  citations: string[];
  timestamp?: string;
  threadId?: string;
  threadTitle?: string;
  anchor?: string;
  fallbackRoute?: string;
}

export interface ThreadAttentionBundle {
  detail: ThreadDetail;
  roomMessages: RoomMessage[];
}

function truncateLine(value: string, maximumLength = 150): string {
  const singleLine = value.replace(/\s+/g, " ").trim();
  if (singleLine.length <= maximumLength) {
    return singleLine;
  }
  return `${singleLine.slice(0, maximumLength - 1)}…`;
}

function humanizeCode(code: string): string {
  return code.replace(/[_-]+/g, " ").trim();
}

function isStallActive(stall: ThreadStallReport | null | undefined): stall is ThreadStallReport {
  if (!stall) {
    return false;
  }
  if (stall.stalled === false) {
    return false;
  }
  return true;
}

function buildStallAttentionItem(
  detail: ThreadDetail,
  stall: ThreadStallReport,
): AttentionItem {
  const citations: string[] = [];
  let anchor: string | undefined;

  const conditionLabels: string[] = [];
  if (stall.kind) {
    conditionLabels.push(humanizeCode(stall.kind));
  }
  if (stall.reason && stall.reason !== stall.kind) {
    conditionLabels.push(humanizeCode(stall.reason));
  }
  if (stall.reasons) {
    for (const reason of stall.reasons) {
      const formatted = humanizeCode(reason);
      if (formatted && !conditionLabels.includes(formatted)) {
        conditionLabels.push(formatted);
      }
    }
  }

  let conditionDetail: string | undefined;
  if (stall.conditions) {
    for (const rawCondition of stall.conditions) {
      if (typeof rawCondition === "string") {
        const formatted = humanizeCode(rawCondition);
        if (formatted && !conditionLabels.includes(formatted)) {
          conditionLabels.push(formatted);
        }
        continue;
      }
      const condition: ThreadStallCondition = rawCondition;
      const code = condition.kind ?? condition.reason;
      if (code) {
        const formatted = humanizeCode(code);
        if (formatted && !conditionLabels.includes(formatted)) {
          conditionLabels.push(formatted);
        }
      }
      if (!conditionDetail && (condition.summary || condition.detail)) {
        conditionDetail = condition.summary ?? condition.detail;
      }
      const conditionSequence =
        condition.roomSequence ?? condition.seq ?? condition.sequence;
      if (conditionSequence != null && conditionSequence > 0) {
        const sequenceCitation = `#${conditionSequence}`;
        if (!citations.includes(sequenceCitation)) {
          citations.push(sequenceCitation);
        }
        if (!anchor) {
          anchor = `room-seq-${conditionSequence}`;
        }
      }
      if (condition.handle) {
        const handleCitation = condition.handle.startsWith("@")
          ? condition.handle
          : `@${condition.handle}`;
        if (!citations.includes(handleCitation)) {
          citations.push(handleCitation);
        }
      }
      const firstTaskId = condition.taskId ?? condition.taskIds?.[0];
      if (firstTaskId) {
        const taskCitation = `task ${shortId(firstTaskId)}`;
        if (!citations.includes(taskCitation)) {
          citations.push(taskCitation);
        }
        if (!anchor) {
          anchor = `task-${firstTaskId}`;
        }
      }
      if (condition.runId) {
        const runCitation = `run ${shortId(condition.runId)}`;
        if (!citations.includes(runCitation)) {
          citations.push(runCitation);
        }
        if (!anchor) {
          anchor = `run-${condition.runId}`;
        }
      }
    }
  }

  if (stall.unansweredMentions) {
    for (const mention of stall.unansweredMentions) {
      if (typeof mention === "string") {
        const handleLabel = mention.startsWith("@") ? mention : `@${mention}`;
        if (!citations.includes(handleLabel)) {
          citations.push(handleLabel);
        }
      } else {
        if (mention.handle) {
          const handleLabel = mention.handle.startsWith("@") ? mention.handle : `@${mention.handle}`;
          if (!citations.includes(handleLabel)) {
            citations.push(handleLabel);
          }
        }
        const mentionSequence = mention.roomSequence ?? mention.seq ?? mention.sequence;
        if (mentionSequence != null && mentionSequence > 0) {
          const sequenceLabel = `#${mentionSequence}`;
          if (!citations.includes(sequenceLabel)) {
            citations.push(sequenceLabel);
          }
          if (!anchor) {
            anchor = `room-seq-${mentionSequence}`;
          }
        }
      }
    }
  }

  const topLevelSequence =
    stall.roomSequence ?? stall.throughRoomSequence ?? stall.headRoomSequence;
  if (topLevelSequence != null && topLevelSequence > 0) {
    const sequenceCitation = `#${topLevelSequence}`;
    if (!citations.includes(sequenceCitation)) {
      citations.push(sequenceCitation);
    }
    if (!anchor) {
      anchor = `room-seq-${topLevelSequence}`;
    }
  }

  const firstBlockedTask = stall.blockedTaskIds?.[0];
  if (firstBlockedTask) {
    const taskCitation = `task ${shortId(firstBlockedTask)}`;
    if (!citations.includes(taskCitation)) {
      citations.push(taskCitation);
    }
    if (!anchor) {
      anchor = `task-${firstBlockedTask}`;
    }
  }

  if (citations.length === 0) {
    const fallbackTask = detail.tasks.find(
      (task) => task.status === "blocked" || task.status === "pending",
    );
    if (fallbackTask) {
      citations.push(`task ${shortId(fallbackTask.id)}`);
      anchor = `task-${fallbackTask.id}`;
    } else if ((detail.room?.headSequence ?? 0) > 0) {
      citations.push(`#${detail.room!.headSequence}`);
      anchor = `room-seq-${detail.room!.headSequence}`;
    } else {
      citations.push(`thread ${shortId(detail.thread.id)}`);
    }
  }

  const title =
    stall.summary?.trim() ||
    (conditionLabels.length > 0
      ? `Thread stalled: ${conditionLabels.join(", ")}`
      : "Thread stalled with no runnable tasks or active runs");

  return {
    id: `stall-${detail.thread.id}`,
    rank: 2,
    category: "stall",
    badgeLabel: "stalled",
    tone: "err",
    title: truncateLine(title),
    detail: conditionDetail ? truncateLine(conditionDetail) : undefined,
    citations: citations.slice(0, 3),
    timestamp: stall.detectedAt ?? stall.occurredAt ?? stall.evaluatedAt ?? detail.thread.updatedAt,
    threadId: detail.thread.id,
    threadTitle: detail.thread.title,
    anchor,
  };
}

function buildApprovalAttentionItem(
  approval: ApprovalRequest,
  threadTitle?: string,
): AttentionItem {
  const citations: string[] = [];
  if (approval.taskId) {
    citations.push(`task ${shortId(approval.taskId)}`);
  }
  citations.push(`run ${shortId(approval.runId)}`);

  return {
    id: `approval-${approval.id}`,
    rank: 1,
    category: "approval",
    badgeLabel: "approval pending",
    tone: "warn",
    title: approval.title || "Worker permission request awaiting decision",
    detail: approval.detail ? truncateLine(approval.detail, 130) : `Node ${approval.nodeId}`,
    citations,
    timestamp: approval.requestedAt,
    threadId: approval.threadId,
    threadTitle,
    anchor: approval.threadId ? `approval-${approval.id}` : undefined,
    fallbackRoute: "/approvals",
  };
}

function buildRunAttentionItem(run: Run, threadTitle?: string): AttentionItem | null {
  const isQuotaExhausted = run.errorKind === "quota_exhausted";
  const isRateLimited = run.errorKind === "rate_limited";
  const isFailed = run.status === "failed";

  if (!isQuotaExhausted && !isRateLimited && !isFailed) {
    return null;
  }

  const citations: string[] = [`run ${shortId(run.id)}`];
  if (run.taskId) {
    citations.push(`task ${shortId(run.taskId)}`);
  }

  const category = isQuotaExhausted
    ? "quota_exhausted"
    : isRateLimited
      ? "rate_limited"
      : "run_failed";
  const badgeLabel = isQuotaExhausted
    ? "quota exhausted"
    : isRateLimited
      ? "rate limited"
      : "run failed";
  const tone: Tone = isRateLimited && !isQuotaExhausted ? "warn" : "err";

  const summary =
    isQuotaExhausted || isRateLimited
      ? `${run.harnessId} / ${run.model} on ${run.nodeId} hit ${badgeLabel}`
      : `Run failed on ${run.harnessId} / ${run.model} (${run.nodeId})`;

  return {
    id: `run-${run.id}`,
    rank: 3,
    category,
    badgeLabel,
    tone,
    title: summary,
    detail: run.error ? truncateLine(run.error, 140) : undefined,
    citations,
    timestamp: run.finishedAt ?? run.createdAt,
    threadId: run.threadId,
    threadTitle,
    anchor: `run-${run.id}`,
  };
}

function timestampValue(iso?: string): number {
  if (!iso) {
    return 0;
  }
  const parsed = new Date(iso).getTime();
  return Number.isNaN(parsed) ? 0 : parsed;
}

function sortAttentionItems(items: AttentionItem[]): AttentionItem[] {
  return [...items].sort((left, right) => {
    if (left.rank !== right.rank) {
      return left.rank - right.rank;
    }
    if (left.rank === 2 && left.category !== right.category) {
      return left.category === "stall" ? -1 : 1;
    }
    if (left.rank === 3 && left.category !== right.category) {
      const categoryOrder = { quota_exhausted: 0, rate_limited: 1, run_failed: 2 } as const;
      const leftOrder = categoryOrder[left.category as keyof typeof categoryOrder] ?? 3;
      const rightOrder = categoryOrder[right.category as keyof typeof categoryOrder] ?? 3;
      if (leftOrder !== rightOrder) {
        return leftOrder - rightOrder;
      }
    }
    return timestampValue(right.timestamp) - timestampValue(left.timestamp);
  });
}

export function synthesizeThreadAttention(
  detail: ThreadDetail,
  roomMessages: RoomMessage[],
): AttentionItem[] {
  const items: AttentionItem[] = [];
  const threadId = detail.thread.id;
  const threadTitle = detail.thread.title;

  for (const approval of detail.approvals) {
    if (approval.status === "pending") {
      items.push(buildApprovalAttentionItem(approval, threadTitle));
    }
  }

  if (isStallActive(detail.stall)) {
    items.push(buildStallAttentionItem(detail, detail.stall));
  }

  const supersededBy = computeSupersededByMap(roomMessages);
  const answeredBy = computeAnsweredByMap(roomMessages, supersededBy);

  for (const message of roomMessages) {
    if (
      message.kind === "question" &&
      !supersededBy.has(message.seq) &&
      !answeredBy.has(message.seq)
    ) {
      const citations = [`#${message.seq}`];
      if (message.author.taskId) {
        citations.push(`task ${shortId(message.author.taskId)}`);
      } else if (message.author.runId) {
        citations.push(`run ${shortId(message.author.runId)}`);
      }
      items.push({
        id: `question-${threadId}-${message.seq}`,
        rank: 2,
        category: "unanswered_question",
        badgeLabel: "unanswered question",
        tone: "warn",
        title: message.topic
          ? `${message.topic}: ${truncateLine(message.body, 110)}`
          : truncateLine(message.body, 140),
        detail: `Asked by ${formatRoomAuthor(message.author)}`,
        citations,
        timestamp: message.createdAt,
        threadId,
        threadTitle,
        anchor: `room-seq-${message.seq}`,
      });
    }
  }

  for (const run of detail.runs) {
    const runItem = buildRunAttentionItem(run, threadTitle);
    if (runItem) {
      items.push(runItem);
    }
  }

  for (const message of roomMessages) {
    if (
      (message.kind === "decision" || message.kind === "contract") &&
      !supersededBy.has(message.seq)
    ) {
      const citations = [`#${message.seq}`];
      if (message.author.taskId) {
        citations.push(`task ${shortId(message.author.taskId)}`);
      }
      items.push({
        id: `${message.kind}-${threadId}-${message.seq}`,
        rank: 4,
        category: message.kind,
        badgeLabel: `active ${message.kind}`,
        tone: message.kind === "decision" ? "ok" : "accent",
        title: message.topic
          ? `${message.topic} — ${truncateLine(message.body, 110)}`
          : truncateLine(message.body, 140),
        detail: `Posted by ${formatRoomAuthor(message.author)}`,
        citations,
        timestamp: message.createdAt,
        threadId,
        threadTitle,
        anchor: `room-seq-${message.seq}`,
      });
    }
  }

  return sortAttentionItems(items);
}

export function synthesizeFleetAttention(
  bundles: ThreadAttentionBundle[],
  pendingApprovals: ApprovalRequest[],
): AttentionItem[] {
  const combined: AttentionItem[] = [];
  const seenApprovalIds = new Set<string>();

  for (const bundle of bundles) {
    for (const approval of bundle.detail.approvals) {
      if (approval.status === "pending") {
        seenApprovalIds.add(approval.id);
      }
    }
    combined.push(...synthesizeThreadAttention(bundle.detail, bundle.roomMessages));
  }

  for (const approval of pendingApprovals) {
    if (approval.status === "pending" && !seenApprovalIds.has(approval.id)) {
      combined.push(buildApprovalAttentionItem(approval));
    }
  }

  return sortAttentionItems(combined);
}

export function NeedsAttentionSection({
  items,
  currentThreadId,
  initialVisibleCount = 6,
}: {
  items: AttentionItem[];
  currentThreadId?: string;
  initialVisibleCount?: number;
}) {
  const [expanded, setExpanded] = useState(false);

  if (items.length === 0) {
    return null;
  }

  const displayedItems = expanded ? items : items.slice(0, initialVisibleCount);
  const hiddenCount = items.length - displayedItems.length;

  function activateItem(item: AttentionItem) {
    if (item.threadId) {
      if (currentThreadId === item.threadId && item.anchor) {
        scrollToAnchor(item.anchor);
        replaceThreadAnchor(item.threadId, item.anchor);
        return;
      }
      navigateToThread(item.threadId, item.anchor);
      return;
    }
    if (item.fallbackRoute) {
      navigate(item.fallbackRoute);
    }
  }

  return (
    <div className="section">
      <SectionHead
        title="Needs attention"
        count={items.length}
        action={
          items.length > initialVisibleCount ? (
            <button
              type="button"
              className="attention-expand-btn"
              onClick={() => setExpanded((previous) => !previous)}
            >
              {expanded ? "Show fewer" : `Show all ${items.length} (${hiddenCount} more)`}
            </button>
          ) : null
        }
      />
      <Card className="attention-card">
        <div className="attention-list">
          {displayedItems.map((item) => (
            <button
              key={item.id}
              type="button"
              className={`attention-item tone-${item.tone}`}
              onClick={() => activateItem(item)}
            >
              <span className={`attention-icon ${item.tone}`}>
                <AlertIcon />
              </span>
              <div className="attention-main">
                <div className="attention-top">
                  <span className={`badge ${item.tone}`}>{item.badgeLabel}</span>
                  {!currentThreadId && item.threadTitle ? (
                    <span className="attention-thread">{item.threadTitle}</span>
                  ) : null}
                  <div className="attention-citations">
                    {item.citations.map((citation) => (
                      <span key={citation} className="attention-cite">
                        {citation}
                      </span>
                    ))}
                  </div>
                  {item.timestamp ? (
                    <span className="attention-time">{timeAgo(item.timestamp)}</span>
                  ) : null}
                </div>
                <div className="attention-title">{item.title}</div>
                {item.detail ? <div className="attention-detail">{item.detail}</div> : null}
              </div>
              <span className="attention-arrow">
                <ChevronRightIcon />
              </span>
            </button>
          ))}
        </div>
      </Card>
    </div>
  );
}
