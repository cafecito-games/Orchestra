# Orchestra

`orchestra` is a self-contained, headless multi-machine coding agent orchestrator written in pure Go 1.26 (`CGO_ENABLED=0`, zero UI/Node/TypeScript dependencies). A single `orchestra` binary runs the SQLite-backed control plane (`orchestra master`), remote worker agents (`orchestra slave`), the stdio Model Context Protocol bridge (`orchestra mcp`), and operator inspection/management commands.

---

## Architecture

```
Main Machine                                       Remote Worker Machines (over Tailscale)
┌──────────────────────────────────────────┐       ┌──────────────────────────────────────────┐
│ Coding Agent (Claude / Codex / AGY)      │       │ orchestra slave (macOS / Linux)          │
│    │                                     │       │  ├─ Reverse WebSocket (/control-agent)   │
│    ▼ stdio JSON-RPC (or HTTP /mcp)       │       │  ├─ Workspace Leases (git-worktree /     │
│ orchestra mcp                            │       │  │   exclusive-existing + flock)         │
│    │ HTTP + SSE Doorbell                 │       │  ├─ Ephemeral 127.0.0.1 Run MCP Server   │
│    ▼                                     │  ws/  │  │   (orchestra_hub worker tools)        │
│ orchestra master                         │◄─────►│  └─ Harnesses (Claude, Codex, OpenCode & │
│  ├─ Pure-Go SQLite (WAL, event journal)  │  wss  │      Antigravity via acp-v1 & native-cli)│
│  ├─ Capability & Readiness Scheduler     │       └──────────────────────────────────────────┘
│  ├─ Resident Agent Instances & Sessions  │
│  ├─ Task DAGs, Mailboxes & Approvals     │
│  └─ Bounded SHA-256 Artifact Store       │
└──────────────────────────────────────────┘
```

### Security Invariants
- **Constant-time bearer authentication** (`ORCHESTRA_TOKEN`) on all REST, MCP, and `/control-agent` endpoints.
- **Vendor credentials stay local**: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, OAuth tokens, and CLI subscriptions never leave `orchestra slave` machines; `ORCHESTRA_TOKEN` is stripped from child harness environments.
- **No shell execution**: Harnesses and `git` are invoked directly via `exec.CommandContext` with process-group isolation and cancellation tombstones.
- **Canonical workspace confinement**: Every workspace path on a slave must resolve (symlink-free) beneath an operator-configured `WORKSPACE_ROOTS` directory.
- **Claude ACP auth safety gate**: `claude-cli` over `acp-v1` requires explicit `--claude-acp-auth-mode` (`local-subscription` or `api`) so subscription sessions never silently bill API keys.
- **Secret redaction**: Harness events, transcripts, and diagnostics pass through automatic secret scrubbing before storage or display.

---

## Building & Cross-Compiling

Requires **Go 1.26+**, **golangci-lint (v2)**, and optionally [`task`](https://taskfile.dev). Because SQLite uses `modernc.org/sqlite`, cross-compilation requires no C toolchain.

```bash
# Build for current machine (./bin/orchestra)
task build
# or directly:
CGO_ENABLED=0 go build -trimpath -o bin/orchestra ./cmd/orchestra

# Install atomically to ~/.local/bin/orchestra
task install

# Cross-compile darwin/arm64, darwin/amd64, linux/arm64, linux/amd64 into ./dist/
task build:all
```

---

## Tailscale Setup

Orchestra is designed to run over a Tailscale tailnet where `orchestra master` listens on your main workstation or home server and remote `orchestra slave` machines dial outbound to `/control-agent`.

### 1. Generate a Shared Bearer Token
```bash
export ORCHESTRA_TOKEN="$(openssl rand -hex 32)"
```

### 2. Option A: Plain Tailnet WireGuard Transport (`http://` / `ws://`)
Bind `orchestra master` strictly to your machine's Tailscale IPv4 address (and optionally loopback for local MCP/CLI use by binding `0.0.0.0:8787` with host firewall rules or Tailscale interface IP):

```bash
TAILSCALE_IP="$(tailscale ip -4)"
orchestra master \
  --listen "${TAILSCALE_IP}:8787" \
  --token "${ORCHESTRA_TOKEN}" \
  --data-dir ~/.local/share/orchestra/master
```

### 3. Option B: End-to-End TLS over Tailscale (`https://` / `wss://`)
Provision a Tailscale MagicDNS TLS certificate and pass `--tls-cert` and `--tls-key`:

```bash
tailscale cert my-main-mac.tailnet-name.ts.net
orchestra master \
  --listen 0.0.0.0:8787 \
  --tls-cert ./my-main-mac.tailnet-name.ts.net.crt \
  --tls-key ./my-main-mac.tailnet-name.ts.net.key \
  --token "${ORCHESTRA_TOKEN}" \
  --data-dir ~/.local/share/orchestra/master
```

When using a private CA in custom setups, pass `--tls-ca /path/to/ca.pem` (or `ORCHESTRA_TLS_CA`) to `orchestra slave`, `orchestra mcp`, and CLI commands.

---

## Running `orchestra master`

```bash
# Master only
ORCHESTRA_TOKEN="..." orchestra master \
  --listen 0.0.0.0:8787 \
  --data-dir ~/.local/share/orchestra/master

# Master + embedded local worker so the main machine can also execute tasks
ORCHESTRA_TOKEN="..." orchestra master \
  --listen 0.0.0.0:8787 \
  --data-dir ~/.local/share/orchestra/master \
  --with-local-worker \
  --worker-id main-mac \
  --worker-name "Main MacBook Pro" \
  --workspace-root ~/Projects \
  --worker-label macos \
  --worker-label apple-silicon \
  --claude-acp-auth-mode local-subscription
```

### Optional Project Profiles (`--project-profiles`)
You can supply a JSON file mapping logical `projectId` values to readiness capabilities so tasks targeting a project automatically verify slave prerequisites:

```json
[
  {
    "id": "game-engine",
    "title": "Game Engine",
    "requiredCapabilities": ["git", "label:linux"],
    "preferredCapabilities": ["accelerator:cuda"]
  }
]
```

---

## Setting Up & Running `orchestra slave`

Copy the cross-compiled `orchestra` binary to each worker machine where Claude Code (`claude`), Codex (`codex`), OpenCode (`opencode`), and/or Antigravity (`agy`) are installed and authenticated locally.

Antigravity (`antigravity-cli`, executable `agy`) uses Orchestra's built-in pure-Go ACP v1 stdio bridge (`orchestra acp-bridge antigravity`) automatically whenever `agy` is installed on `PATH`, requiring no external adapter download, while also supporting `native-cli` execution and optional pinned `--acp-adapter antigravity-cli=sha256:<hex>:/absolute/path/to/adapter` overrides.

### 1. Provision Managed ACP Adapters (`orchestra setup` & `orchestra doctor`)
```bash
# Inspect planned component installations
orchestra setup plan --out plan.json

# Apply archive downloads and/or local manual ACP adapter artifacts
orchestra setup apply --plan plan.json \
  --manual-artifact claude-acp=/path/to/claude-agent-acp \
  --manual-checksum claude-acp=<sha256> \
  --manual-artifact codex-acp=/path/to/codex-acp \
  --manual-checksum codex-acp=<sha256>

# Activate specific adapter versions
orchestra setup activate --kind acp-adapter --id claude-acp --version 0.79.0
orchestra setup activate --kind acp-adapter --id codex-acp --version 1.12.0

# Verify harnesses, active ACP adapters, auth safety gate, and master connectivity
ORCHESTRA_TOKEN="..." orchestra doctor \
  --endpoint https://my-main-mac.tailnet-name.ts.net:8787 \
  --claude-acp-auth-mode local-subscription
```

You can also pin explicit ACP adapter binaries directly via `--acp-adapter <harness-id>=sha256:<hex>:/absolute/path/to/adapter`.

### 2. Start the Worker Daemon
```bash
export ORCHESTRA_TOKEN="..."
orchestra slave \
  --endpoint https://my-main-mac.tailnet-name.ts.net:8787 \
  --id build-linux-01 \
  --name "Linux Build Server" \
  --kind home-server \
  --workspace-root /srv/workspaces \
  --label linux \
  --label x86-64 \
  --accelerator cuda \
  --concurrency 4 \
  --instance-capacity 6 \
  --approval-policy claude-cli=manual \
  --approval-policy codex-cli=manual \
  --approval-policy opencode-cli=manual \
  --approval-policy antigravity-cli=manual \
  --claude-acp-auth-mode local-subscription
```

### OpenCode with GLM and DeepSeek

An installed `opencode` binary is discovered as the `opencode-cli` harness. Orchestra uses OpenCode's built-in ACP server by default, including model discovery and resumable resident sessions, and retains `native-cli` as a fallback transport. The selected model is passed through unchanged using OpenCode's `provider/model` syntax, so one slave can run GLM and DeepSeek agents concurrently when `--concurrency` and `--instance-capacity` are at least `2`.

Use the exact model IDs shown by `opencode models`. For example, two task or instance requirements can target the same node and harness while selecting different models:

```json
{
  "harnessIds": ["opencode-cli"],
  "models": ["zai-coding-plan/glm-5.3"],
  "transports": ["acp-v1"]
}
```

```json
{
  "harnessIds": ["opencode-cli"],
  "models": ["deepseek/deepseek-v4-pro"],
  "transports": ["acp-v1"]
}
```

ACP runs receive the run-scoped `orchestra_hub` connection directly in the session request. Native runs use OpenCode's standalone headless mode and an in-memory MCP configuration override. Orchestra does not rewrite the user's global or project OpenCode configuration, and provider credentials remain on the slave machine.

---

## Configuring Coding Agents (MCP)

Your main machine's coding agent can connect to `orchestra master` either via the **`orchestra mcp` stdio bridge** (which also subscribes to `/api/mcp/events` SSE doorbells and forwards `notifications/orchestra/thread_event` push notifications) or directly over **Streamable HTTP (`POST /mcp`)**.

### Claude Code (`~/.claude.json` or `.mcp.json`)
```json
{
  "mcpServers": {
    "orchestra": {
      "command": "orchestra",
      "args": ["mcp", "--endpoint", "http://127.0.0.1:8787"],
      "env": {
        "ORCHESTRA_TOKEN": "your-orchestra-bearer-token"
      }
    }
  }
}
```

### Codex (`~/.codex/config.toml`)
```toml
[mcp_servers.orchestra]
command = "orchestra"
args = ["mcp", "--endpoint", "http://127.0.0.1:8787"]
env = { "ORCHESTRA_TOKEN" = "your-orchestra-bearer-token" }
```

### Antigravity (`mcp_config.json`)
```json
{
  "mcpServers": {
    "orchestra": {
      "command": "orchestra",
      "args": ["mcp", "--endpoint", "http://127.0.0.1:8787"],
      "env": {
        "ORCHESTRA_TOKEN": "your-orchestra-bearer-token"
      }
    }
  }
}
```

### Orchestrator MCP Toolset (18 Tools)

| Category | Tools |
| --- | --- |
| **Inventory** | `get_execution_inventory` |
| **Threads & Events** | `create_thread`, `list_threads`, `attach_thread`, `detach_thread`, `get_thread_context`, `get_thread_events`, `update_thread` |
| **Task DAG & Mailbox** | `submit_tasks`, `update_task`, `send_task_message`, `post_artifact` |
| **Resident Instances** | `spawn_instance`, `get_instance`, `renew_instance`, `release_instance` |
| **Approvals** | `list_approvals`, `resolve_approval` |

Worker runs receive an isolated loopback `orchestra_hub` Streamable HTTP MCP server exposing run-scoped tools (`get_task_context`, `send_task_message`, `wait_for_task_events`, `update_task`, `post_artifact`, `update_thread`, plus `delegate_task`, `spawn_instance`, `get_instance`, `renew_instance`, and `release_instance` when `allowTaskDelegation` is enabled on the task).

---

## Operator CLI Reference

All operator commands read `ORCHESTRA_ENDPOINT` (default `http://127.0.0.1:8787`) and `ORCHESTRA_TOKEN`, and support `--json` for machine-readable output:

```bash
# Cluster health, node connectivity, open threads, active runs, pending approvals
orchestra status

# Connected & enrolled slave nodes, harnesses, transports, labels, workspace roots, capacity
orchestra nodes

# List threads (optionally filter via --status open|resolved|archived)
orchestra threads

# Inspect full thread state: task DAG, resident instances, runs, leases, approvals, messages, transcript
orchestra thread <thread-id>

# Inspect and resolve pending worker permission requests
orchestra approvals --status pending
orchestra approve <approval-id> --option allow_once
orchestra reject <approval-id> --reason "Do not modify production config"

# Cancel a running or queued task or run
orchestra cancel <run-id|task-id>

# Manage local slave components, built-in ACP bridges & diagnostics
orchestra setup <plan|apply|activate|rollback|prune|uninstall> [flags]
orchestra acp-bridge antigravity [--agy-binary agy] [--state-dir <path>]
orchestra doctor
orchestra version
```

---

## Development & Verification

```bash
# Format, vet, unit tests (-race), multi-node real-process system tests (-race), and cross-builds
task ci

# Run static checks (gofmt, go vet, go fix, golangci-lint, and whitespace checks)
task lint

# Run unit tests only
task unit:test

# Run multi-process E2E system tests (1 master, 2+ slave nodes, stdio MCP bridge, fake harnesses)
task system:test
```
