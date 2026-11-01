// Lightweight imperative toast, independent of React state so it can be called
// from anywhere (event handlers, async actions).

export function toast(message: string, kind: "ok" | "err" = "ok"): void {
  let wrap = document.getElementById("toasts");
  if (!wrap) {
    wrap = document.createElement("div");
    wrap.id = "toasts";
    wrap.className = "toast-wrap";
    document.body.appendChild(wrap);
  }
  const el = document.createElement("div");
  el.className = `toast ${kind}`;
  el.textContent = message;
  wrap.appendChild(el);
  window.setTimeout(() => {
    el.style.opacity = "0";
    el.style.transition = "opacity 200ms";
  }, 3200);
  window.setTimeout(() => el.remove(), 3500);
}
