import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { DependencyList } from "react";
import { AuthError, getToken, setToken, subscribeToken } from "./api";

export const DEFAULT_POLL_MS = 5000;

export interface LiveControls {
  live: boolean;
  pollMs: number;
  refreshNonce: number;
  toggleLive: () => void;
  refresh: () => void;
}

export const LiveContext = createContext<LiveControls>({
  live: true,
  pollMs: DEFAULT_POLL_MS,
  refreshNonce: 0,
  toggleLive: () => {},
  refresh: () => {},
});

export function useLive(): LiveControls {
  return useContext(LiveContext);
}

export interface Route {
  name: string;
  id?: string;
  anchor?: string;
}

export function parseRoute(hash: string): Route {
  const withoutLeadingHash = hash.replace(/^#\/?/, "");
  const [beforeSecondHash, inlineFragment] = withoutLeadingHash.split("#");
  const [pathPart, queryPart] = (beforeSecondHash ?? "").split("?");
  const parts = (pathPart ?? "").split("/").filter(Boolean);

  let anchor: string | undefined;
  if (queryPart) {
    const parameters = new URLSearchParams(queryPart);
    const anchorParameter = parameters.get("anchor");
    if (anchorParameter) {
      anchor = anchorParameter;
    }
  }
  if (!anchor && inlineFragment) {
    anchor = decodeURIComponent(inlineFragment);
  }

  if (parts.length === 0) return { name: "overview", anchor };
  const name = parts[0] ?? "overview";
  const rest = parts[1];
  if (name === "thread" && rest) {
    return { name: "thread", id: decodeURIComponent(rest), anchor };
  }
  if (name === "mail" && rest) {
    return { name: "mailbox", id: decodeURIComponent(rest), anchor };
  }
  return { name, anchor };
}

export function navigate(hash: string): void {
  window.location.hash = hash;
}

export function navigateToThread(threadId: string, anchor?: string): void {
  const basePath = `/thread/${encodeURIComponent(threadId)}`;
  if (anchor) {
    navigate(`${basePath}?anchor=${encodeURIComponent(anchor)}`);
    return;
  }
  navigate(basePath);
}

export function replaceThreadAnchor(threadId: string, anchor: string): void {
  const nextHash = `#/thread/${encodeURIComponent(threadId)}?anchor=${encodeURIComponent(anchor)}`;
  if (typeof window.history?.replaceState === "function") {
    window.history.replaceState(null, "", nextHash);
  }
}

export function scrollToAnchor(anchorId: string): boolean {
  const targetElement = document.getElementById(anchorId);
  if (!targetElement) {
    return false;
  }
  targetElement.scrollIntoView({ behavior: "smooth", block: "center" });
  targetElement.classList.remove("anchor-flash");
  void targetElement.offsetWidth;
  targetElement.classList.add("anchor-flash");
  return true;
}

export function useRoute(): Route {
  const [route, setRoute] = useState<Route>(() => parseRoute(window.location.hash));
  useEffect(() => {
    const onHashChange = () => setRoute(parseRoute(window.location.hash));
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);
  return route;
}

interface PolledState<T> {
  data: T | null;
  error: string | null;
  reload: () => void;
}

// Fetches on mount and then re-fetches on a fixed interval, cancelling stale
// requests and ignoring errors that resolve after unmount. A 401 clears the
// token (which the App observes) and is surfaced as an AuthError.
export function usePolledFetch<T>(
  fetcher: () => Promise<T>,
  dependencies: DependencyList,
  intervalMs = 5000,
): PolledState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [nonce, setNonce] = useState(0);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  useEffect(() => {
    let alive = true;
    const load = () => {
      fetcherRef
        .current()
        .then((result) => {
          if (alive) {
            setData(result);
            setError(null);
          }
        })
        .catch((err: unknown) => {
          if (!alive) return;
          if (err instanceof AuthError) return; // App handles sign-out.
          setError(err instanceof Error ? err.message : String(err));
        });
    };
    load();
    const timer = Number.isFinite(intervalMs) && intervalMs > 0
      ? window.setInterval(load, intervalMs)
      : null;
    return () => {
      alive = false;
      if (timer !== null) window.clearInterval(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...dependencies, nonce, intervalMs]);

  const reload = useCallback(() => setNonce((value) => value + 1), []);
  return useMemo(() => ({ data, error, reload }), [data, error, reload]);
}

// One-shot fetch for values loaded once per mount.
export function useFetch<T>(fetcher: () => Promise<T>, dependencies: DependencyList) {
  return usePolledFetch<T>(fetcher, dependencies, 0);
}

// Keeps the API client's token in sync with a React state value so the App can
// react to sign-in and sign-out.
export function useTokenState(): [string, (value: string) => void] {
  const [token, setLocalToken] = useState<string>(() => getToken());
  useEffect(() => subscribeToken(setLocalToken), []);
  return [token, setToken];
}
