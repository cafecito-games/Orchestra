// Thin fetch client for the master's bearer-authenticated operator API.

import { useEffect, useState } from "react";
import type { ReadRoomResult, RoomMessage } from "./types";

const TOKEN_KEY = "orchestra.token";

let token = sessionStorage.getItem(TOKEN_KEY) ?? "";
const listeners = new Set<(value: string) => void>();

export class AuthError extends Error {
  constructor() {
    super("unauthorized");
    this.name = "AuthError";
  }
}

export function getToken(): string {
  return token;
}

export function setToken(value: string): void {
  token = value;
  if (value) {
    sessionStorage.setItem(TOKEN_KEY, value);
  } else {
    sessionStorage.removeItem(TOKEN_KEY);
  }
  listeners.forEach((listener) => listener(token));
}

export function subscribeToken(listener: (value: string) => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function authHeaders(): HeadersInit {
  return token ? { Authorization: `Bearer ${token}` } : {};
}

interface ApiOptions extends Omit<RequestInit, "body"> {
  body?: unknown;
}

export async function api<T>(path: string, options: ApiOptions = {}): Promise<T> {
  const headers = new Headers(options.headers);
  if (token) headers.set("Authorization", `Bearer ${token}`);
  const { body, ...rest } = options;
  const init: RequestInit = { ...rest, headers };
  if (body != null) {
    if (typeof body === "string") {
      init.body = body;
    } else {
      headers.set("Content-Type", "application/json");
      init.body = JSON.stringify(body);
    }
  }

  let response: Response;
  try {
    response = await fetch(path, init);
  } catch {
    throw new Error("Network error — is Orchestra reachable?");
  }

  if (response.status === 401) {
    setToken("");
    throw new AuthError();
  }

  const text = await response.text();
  let data: unknown = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }

  if (!response.ok) {
    const message =
      (data as { error?: { message?: string } })?.error?.message ?? `HTTP ${response.status}`;
    throw new Error(message);
  }
  return data as T;
}

// Fetches an authenticated binary artifact blob using the active bearer token.
export async function fetchArtifactBlob(path: string, signal?: AbortSignal): Promise<Blob> {
  let response: Response;
  try {
    response = await fetch(path, { headers: authHeaders(), signal });
  } catch (fetchError) {
    if (signal?.aborted) {
      throw fetchError;
    }
    throw new Error("Network error — is Orchestra reachable?");
  }
  if (response.status === 401) {
    setToken("");
    throw new AuthError();
  }
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return response.blob();
}

// Downloads an authenticated artifact as a client-side blob, which lets us send
// the bearer token that a plain anchor link cannot.
export async function download(path: string, filename: string): Promise<void> {
  const blob = await fetchArtifactBlob(path);
  const objectUrl = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = objectUrl;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(objectUrl);
}

// Resolves a bearer-authenticated endpoint into a temporary object URL and
// revokes the URL when the path changes or the component unmounts.
export function useAuthenticatedBlobUrl(path: string | null | undefined): {
  blobUrl: string | null;
  loading: boolean;
  error: string | null;
} {
  const [activeToken, setActiveToken] = useState<string>(() => getToken());
  const [blobUrl, setBlobUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(Boolean(path));
  const [error, setError] = useState<string | null>(null);

  useEffect(() => subscribeToken(setActiveToken), []);

  useEffect(() => {
    if (!path) {
      setBlobUrl(null);
      setLoading(false);
      setError(null);
      return;
    }

    const abortController = new AbortController();
    let activeObjectUrl: string | null = null;
    let disposed = false;

    setLoading(true);
    setError(null);
    setBlobUrl(null);

    fetchArtifactBlob(path, abortController.signal)
      .then((blob) => {
        if (disposed) return;
        activeObjectUrl = URL.createObjectURL(blob);
        setBlobUrl(activeObjectUrl);
        setLoading(false);
      })
      .catch((fetchError: unknown) => {
        if (disposed || abortController.signal.aborted) return;
        if (fetchError instanceof AuthError) return;
        setError(fetchError instanceof Error ? fetchError.message : String(fetchError));
        setLoading(false);
      });

    return () => {
      disposed = true;
      abortController.abort();
      if (activeObjectUrl !== null) {
        URL.revokeObjectURL(activeObjectUrl);
        activeObjectUrl = null;
      }
    };
  }, [path, activeToken]);

  return { blobUrl, loading, error };
}

export function generateIdempotencyKey(): string {
  if (typeof globalThis.crypto?.randomUUID === "function") {
    return globalThis.crypto.randomUUID();
  }
  const randomBytes = new Uint8Array(16);
  if (typeof globalThis.crypto?.getRandomValues === "function") {
    globalThis.crypto.getRandomValues(randomBytes);
  } else {
    for (let index = 0; index < randomBytes.length; index += 1) {
      randomBytes[index] = Math.floor(Math.random() * 256);
    }
  }
  randomBytes[6] = ((randomBytes[6] ?? 0) & 0x0f) | 0x40;
  randomBytes[8] = ((randomBytes[8] ?? 0) & 0x3f) | 0x80;
  const hex = Array.from(randomBytes, (byteValue) => byteValue.toString(16).padStart(2, "0"));
  return `${hex.slice(0, 4).join("")}-${hex.slice(4, 6).join("")}-${hex.slice(6, 8).join("")}-${hex.slice(8, 10).join("")}-${hex.slice(10, 16).join("")}`;
}

export async function fetchThreadRoom(threadId: string): Promise<ReadRoomResult> {
  const encodedThreadId = encodeURIComponent(threadId);
  const firstPage = await api<ReadRoomResult>(`/api/threads/${encodedThreadId}/room?limit=200`);
  if (!firstPage.hasMore || firstPage.throughSequence <= 0) {
    return firstPage;
  }

  const combinedMessages: RoomMessage[] = [...firstPage.messages];
  let currentResult = firstPage;
  const maximumPages = 5;

  for (let pageIndex = 1; pageIndex < maximumPages && currentResult.hasMore; pageIndex += 1) {
    const nextPage = await api<ReadRoomResult>(
      `/api/threads/${encodedThreadId}/room?after=${currentResult.throughSequence}&limit=200`,
    );
    if (nextPage.messages.length === 0 || nextPage.throughSequence <= currentResult.throughSequence) {
      currentResult = nextPage;
      break;
    }
    combinedMessages.push(...nextPage.messages);
    currentResult = nextPage;
  }

  return {
    ...currentResult,
    messages: combinedMessages,
  };
}
