import {
  type FormEvent,
  type KeyboardEvent,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from "react";
import { api, AuthError, generateIdempotencyKey } from "../api";
import { shortId } from "../format";
import { toast } from "../toast";
import type {
  AgentMailbox,
  MailMessage,
  SendMailRequest,
  SendMailResult,
} from "../types";
import { SendIcon } from "./Icons";
import { Badge, Card } from "./ui";

interface MailComposerProps {
  mailboxes: AgentMailbox[];
  defaultTo?: string;
  replyTo?: MailMessage | null;
  onClearReply?: () => void;
  onSent: () => void;
}

function formatReplySubject(subject?: string): string {
  const trimmed = subject?.trim() ?? "";
  if (!trimmed) return "";
  if (/^re:/i.test(trimmed)) return trimmed;
  return `Re: ${trimmed}`;
}

export function MailComposer({
  mailboxes,
  defaultTo = "",
  replyTo = null,
  onClearReply,
  onSent,
}: MailComposerProps) {
  const [to, setTo] = useState(defaultTo);
  const [subject, setSubject] = useState("");
  const [body, setBody] = useState("");
  const [wake, setWake] = useState(true);
  const [sending, setSending] = useState(false);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [highlightedIndex, setHighlightedIndex] = useState(0);

  const containerRef = useRef<HTMLDivElement | null>(null);
  const bodyRef = useRef<HTMLTextAreaElement | null>(null);
  const autocompleteRef = useRef<HTMLDivElement | null>(null);
  const idempotencyKeyRef = useRef<string | null>(null);
  const listboxId = useId();
  const toInputId = useId();
  const subjectInputId = useId();
  const bodyInputId = useId();

  useEffect(() => {
    if (!replyTo) {
      setTo(defaultTo);
    }
  }, [defaultTo, replyTo]);

  useEffect(() => {
    if (!replyTo) {
      return;
    }
    setTo(replyTo.from);
    setSubject(formatReplySubject(replyTo.subject));
    setDropdownOpen(false);
    idempotencyKeyRef.current = null;

    containerRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
    window.requestAnimationFrame(() => {
      bodyRef.current?.focus();
    });
  }, [replyTo]);

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      if (
        autocompleteRef.current &&
        !autocompleteRef.current.contains(event.target as Node)
      ) {
        setDropdownOpen(false);
      }
    }
    document.addEventListener("mousedown", handlePointerDown);
    return () => document.removeEventListener("mousedown", handlePointerDown);
  }, []);

  const suggestions = useMemo(() => {
    const query = to.trim().toLowerCase();
    const sorted = [...mailboxes].sort((left, right) => {
      const leftOnline = left.status === "online" ? 0 : 1;
      const rightOnline = right.status === "online" ? 0 : 1;
      if (leftOnline !== rightOnline) return leftOnline - rightOnline;
      return left.address.localeCompare(right.address);
    });
    if (!query) {
      return sorted;
    }
    return sorted.filter((mailbox) => {
      if (mailbox.address.toLowerCase().includes(query)) return true;
      if (mailbox.handle.toLowerCase().includes(query)) return true;
      if (mailbox.host.toLowerCase().includes(query)) return true;
      if (mailbox.harness?.toLowerCase().includes(query)) return true;
      if (mailbox.workspace?.toLowerCase().includes(query)) return true;
      if (mailbox.aliases?.some((alias) => alias.toLowerCase().includes(query))) {
        return true;
      }
      return false;
    });
  }, [mailboxes, to]);

  useEffect(() => {
    if (highlightedIndex >= suggestions.length) {
      setHighlightedIndex(0);
    }
  }, [highlightedIndex, suggestions.length]);

  function resetIdempotencyKey() {
    idempotencyKeyRef.current = null;
  }

  function selectMailbox(mailbox: AgentMailbox) {
    setTo(mailbox.address);
    setDropdownOpen(false);
    resetIdempotencyKey();
  }

  function handleToKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (!dropdownOpen) {
      if (event.key === "ArrowDown" && suggestions.length > 0) {
        event.preventDefault();
        setDropdownOpen(true);
        setHighlightedIndex(0);
      }
      return;
    }
    if (event.key === "ArrowDown") {
      event.preventDefault();
      if (suggestions.length > 0) {
        setHighlightedIndex((current) => (current + 1) % suggestions.length);
      }
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      if (suggestions.length > 0) {
        setHighlightedIndex(
          (current) => (current - 1 + suggestions.length) % suggestions.length,
        );
      }
    } else if (event.key === "Enter" && suggestions.length > 0) {
      const chosen = suggestions[highlightedIndex];
      if (chosen) {
        event.preventDefault();
        selectMailbox(chosen);
      }
    } else if (event.key === "Escape") {
      event.preventDefault();
      setDropdownOpen(false);
    }
  }

  function handleBodyKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
      event.preventDefault();
      void submitMessage();
    }
  }

  async function submitMessage() {
    if (sending) return;
    const trimmedTo = to.trim();
    const trimmedBody = body.trim();
    const trimmedSubject = subject.trim();

    if (!trimmedTo && !replyTo) {
      toast("Recipient address is required", "err");
      return;
    }
    if (!trimmedBody) {
      toast("Message body cannot be empty", "err");
      return;
    }

    if (!idempotencyKeyRef.current) {
      idempotencyKeyRef.current = generateIdempotencyKey();
    }

    const payload: SendMailRequest = {
      idempotencyKey: idempotencyKeyRef.current,
      to: trimmedTo,
      body: trimmedBody,
      wake,
    };
    if (trimmedSubject) {
      payload.subject = trimmedSubject;
    }
    if (replyTo) {
      payload.replyToId = replyTo.id;
    }

    setSending(true);
    try {
      const result = await api<SendMailResult>("/api/mail/messages", {
        method: "POST",
        body: payload,
      });
      toast(`Sent mail to ${result.recipient.address}`, "ok");
      idempotencyKeyRef.current = null;
      setBody("");
      setSubject("");
      setTo(defaultTo);
      setDropdownOpen(false);
      onClearReply?.();
      onSent();
    } catch (sendError) {
      if (!(sendError instanceof AuthError)) {
        const message =
          sendError instanceof Error ? sendError.message : String(sendError);
        toast(`Failed to send mail: ${message}`, "err");
      }
    } finally {
      setSending(false);
    }
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    void submitMessage();
  }

  const canSubmit = Boolean((to.trim() || replyTo) && body.trim()) && !sending;
  const activeOptionId =
    dropdownOpen && suggestions[highlightedIndex]
      ? `${listboxId}-opt-${highlightedIndex}`
      : undefined;

  return (
    <div ref={containerRef}>
      <Card className="mail-composer-card">
        <form className="mail-composer" onSubmit={handleSubmit}>
          {replyTo ? (
            <div className="room-composer-context">
              <span className="room-context-pill">
                Replying to {replyTo.from} ({shortId(replyTo.id)})
                {onClearReply ? (
                  <button
                    type="button"
                    aria-label="Cancel reply"
                    onClick={() => {
                      onClearReply();
                      setTo(defaultTo);
                      setSubject("");
                      resetIdempotencyKey();
                    }}
                  >
                    ×
                  </button>
                ) : null}
              </span>
            </div>
          ) : null}

          <div className="mail-composer-grid">
            <div className="mail-field" ref={autocompleteRef}>
              <label htmlFor={toInputId} className="mail-field-label">
                To
              </label>
              <div className="mail-autocomplete">
                <input
                  id={toInputId}
                  type="text"
                  role="combobox"
                  aria-autocomplete="list"
                  aria-expanded={dropdownOpen}
                  aria-controls={listboxId}
                  aria-activedescendant={activeOptionId}
                  className="room-topic-input mono mail-input"
                  placeholder="handle@host"
                  autoComplete="off"
                  value={to}
                  disabled={sending}
                  onFocus={() => setDropdownOpen(true)}
                  onChange={(event) => {
                    setTo(event.target.value);
                    setDropdownOpen(true);
                    setHighlightedIndex(0);
                    resetIdempotencyKey();
                  }}
                  onKeyDown={handleToKeyDown}
                />
                {dropdownOpen ? (
                  <div id={listboxId} role="listbox" className="mail-suggestions">
                    {suggestions.length === 0 ? (
                      <div className="mail-suggestion-empty">
                        {mailboxes.length === 0
                          ? "No registered mailboxes yet"
                          : "No matching mailboxes"}
                      </div>
                    ) : (
                      suggestions.map((mailbox, index) => (
                        <button
                          key={mailbox.id}
                          id={`${listboxId}-opt-${index}`}
                          type="button"
                          role="option"
                          aria-selected={index === highlightedIndex}
                          className={`mail-suggestion${
                            index === highlightedIndex ? " active" : ""
                          }`}
                          onMouseEnter={() => setHighlightedIndex(index)}
                          onMouseDown={(event) => {
                            event.preventDefault();
                            selectMailbox(mailbox);
                          }}
                        >
                          <div className="mail-suggestion-main">
                            <span className="mono mail-suggestion-addr">
                              {mailbox.address}
                            </span>
                            <span className="mail-suggestion-sub">
                              {mailbox.kind}
                              {mailbox.harness ? ` · ${mailbox.harness}` : ""}
                              {mailbox.workspace ? ` · ${mailbox.workspace}` : ""}
                            </span>
                          </div>
                          <Badge status={mailbox.status} />
                        </button>
                      ))
                    )}
                  </div>
                ) : null}
              </div>
            </div>

            <div className="mail-field">
              <label htmlFor={subjectInputId} className="mail-field-label">
                Subject
              </label>
              <input
                id={subjectInputId}
                type="text"
                className="room-topic-input mail-input"
                placeholder="Optional subject"
                value={subject}
                disabled={sending}
                onChange={(event) => {
                  setSubject(event.target.value);
                  resetIdempotencyKey();
                }}
              />
            </div>
          </div>

          <div className="mail-field">
            <label htmlFor={bodyInputId} className="sr-only">
              Message body
            </label>
            <textarea
              ref={bodyRef}
              id={bodyInputId}
              className="room-body-input"
              placeholder="Write a message to an agent… (Ctrl/Cmd + Enter to send)"
              rows={3}
              value={body}
              disabled={sending}
              onChange={(event) => {
                setBody(event.target.value);
                resetIdempotencyKey();
              }}
              onKeyDown={handleBodyKeyDown}
            />
          </div>

          <div className="room-composer-foot">
            <label className="room-nudge-toggle">
              <input
                type="checkbox"
                checked={wake}
                disabled={sending}
                onChange={(event) => {
                  setWake(event.target.checked);
                  resetIdempotencyKey();
                }}
              />
              <span>Wake recipient immediately</span>
            </label>

            <div className="room-composer-actions">
              {replyTo && onClearReply ? (
                <button
                  type="button"
                  className="btn ghost"
                  disabled={sending}
                  onClick={() => {
                    onClearReply();
                    setTo(defaultTo);
                    setSubject("");
                    resetIdempotencyKey();
                  }}
                >
                  Cancel reply
                </button>
              ) : null}
              <button type="submit" className="btn primary" disabled={!canSubmit}>
                <SendIcon />
                <span>{sending ? "Sending…" : replyTo ? "Send reply" : "Send mail"}</span>
              </button>
            </div>
          </div>
        </form>
      </Card>
    </div>
  );
}
