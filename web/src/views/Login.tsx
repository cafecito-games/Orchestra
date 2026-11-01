import { useState } from "react";
import type { FormEvent } from "react";
import { setToken } from "../api";
import type { Health } from "../types";
import { SparkIcon } from "../components/Icons";

export function Login({ health }: { health: Health | null }) {
  const [value, setValue] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const tailscale = health?.authMode === "tailscale";

  async function submit(event: FormEvent) {
    event.preventDefault();
    const token = value.trim();
    if (!token || busy) return;
    setBusy(true);
    setError(null);
    try {
      const response = await fetch("/api/status", {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (response.status === 401) throw new Error("That token was rejected by the master.");
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      setToken(token);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setBusy(false);
    }
  }

  return (
    <div className="login-wrap">
      <div className="card login-card">
        <div className="login-head">
          <div className="login-mark">
            <SparkIcon />
          </div>
          <div>
            <div className="login-title">Orchestra Console</div>
            <div className="login-sub">{health?.hostName || "control plane"}</div>
          </div>
        </div>

        {tailscale ? (
          <div className="login-error">
            This master runs in Tailscale auth mode. The browser console requires token auth — restart
            with <code>--auth token</code> to use it.
          </div>
        ) : (
          <form className="login-body" onSubmit={submit}>
            <div className="field">
              <label htmlFor="token">Bearer token</label>
              <input
                id="token"
                type="password"
                autoComplete="off"
                autoFocus
                placeholder="Paste your ORCHESTRA_TOKEN"
                value={value}
                onChange={(event) => setValue(event.target.value)}
              />
            </div>
            {error ? <div className="login-error">{error}</div> : null}
            <button className="btn primary" type="submit" disabled={busy} style={{ justifyContent: "center" }}>
              {busy ? "Connecting…" : "Connect"}
            </button>
            <div className="login-hint">
              Find your token at <code>&lt;data-dir&gt;/token</code>, or pass <code>--token</code> when
              starting <code>orchestra master</code>. The token is held in this tab only.
            </div>
          </form>
        )}
      </div>
    </div>
  );
}
