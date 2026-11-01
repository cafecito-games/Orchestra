// Thin fetch client for the master's bearer-authenticated operator API.

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
    throw new Error("Network error — is the master reachable?");
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

// Downloads an authenticated artifact as a client-side blob, which lets us send
// the bearer token that a plain anchor link cannot.
export async function download(path: string, filename: string): Promise<void> {
  const response = await fetch(path, { headers: authHeaders() });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}
