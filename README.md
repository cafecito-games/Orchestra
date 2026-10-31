<div align="center">

# Orchestra

**One binary to coordinate coding agents across all your machines.**

[![Go 1.26+](https://img.shields.io/badge/Go-1.26+-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![CGo-Free](https://img.shields.io/badge/CGO__ENABLED-0-success)](#1-build--install)
[![SQLite WAL](https://img.shields.io/badge/State-Pure--Go_SQLite-003B57?logo=sqlite&logoColor=white)](#how-it-works)
[![Protocols](https://img.shields.io/badge/Protocols-MCP_%2B_ACP_v1-6E56CF)](#3-connect-your-coding-agent)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[Quickstart](#quickstart-local-in-60-seconds) · [How It Works](#how-it-works) · [Multi-Machine Setup](#scaling-to-multiple-machines) · [Operator CLI](#operator-cli) · [Architecture & Security](#reference--deep-dives)

</div>

---

**Orchestra** turns your laptop, home server, and cloud boxes into a unified fleet of coding agents. Keep working inside your favorite interactive agent on your main machine while delegating parallel builds, reviews, GPU workloads, and multi-repo changes to remote workers.

- **Single zero-dependency binary** — Pure Go (`CGO_ENABLED=0`) with embedded SQLite. No Node.js, Python, Docker, or web UI runtime required. Cross-compiles cleanly across macOS and Linux (`arm64` / `amd64`).
- **Bring every harness** — Orchestrate **Claude Code** (`claude`), **Codex** (`codex`), **OpenCode** (`opencode`, including GLM & DeepSeek), and **Antigravity** (`agy`) side by side.
- **Credentials never leave worker machines** — Anthropic/OpenAI keys, OAuth tokens, and local CLI subscriptions stay on each worker; only prompts, structured events, and explicit artifacts cross the wire.
- **Safe parallel workspaces** — Tasks run inside isolated `git-worktree` checkouts or flock-guarded directories confined to explicit workspace roots.
- **Rich agent coordination** — Go beyond fire-and-forget subtasks with capability-based **Task DAGs**, shared **Blackboard Rooms**, resumable **Resident Instances**, and `<handle>@<host>` **Agent Mail**.

---

## Quickstart (Local in 60 Seconds)

Try Orchestra on a single machine first—no network setup or adapter downloads needed if you already have `claude`, `codex`, `opencode`, or `agy` on your `PATH`.

### 1. Build & Install

Install prebuilt binaries on macOS or Linux via Homebrew:

```bash
brew install cafecito-games/tap/orchestra
orchestra version
```

Or build from source with **Go 1.26+** (and optionally [`task`](https://taskfile.dev)):

```bash
# Build with injected git version metadata and install to ~/.local/bin/orchestra
task install

# Or build directly with standard Go tools:
CGO_ENABLED=0 go build -trimpath -o bin/orchestra ./cmd/orchestra
```

### 2. Start the Control Plane + Local Worker

Launch `orchestra master` with an embedded worker pointed at your projects directory (when `--token` is omitted, Orchestra automatically generates and persists a `0600` bearer token at `<data-dir>/token` and writes `~/.local/share/orchestra/connection.json` for local discovery):

```bash
orchestra master \
  --with-local-worker \
  --workspace-root ~/Projects \
  --claude-acp-auth-mode local-subscription
```

### 3. Connect Your Coding Agent

Register `orchestra mcp` once across all harnesses installed on your machine (**Antigravity**, **Claude Code**, **Codex**, and **OpenCode**):

```bash
orchestra mcp install
orchestra mcp status
```

`orchestra mcp install` writes zero-secret `orchestra mcp` entries into each harness configuration (`~/.gemini/config/mcp_config.json`, `~/.claude.json`, `~/.codex/config.toml`, `~/.config/opencode/opencode.json`). At runtime, `orchestra mcp` reads `~/.local/share/orchestra/connection.json` on demand—starting immediately even if `orchestra master` is stopped and hot-reconnecting whenever the master restarts on a new port, IP, or token.

<details>
<summary><strong>Manual MCP Configs (Remote Machines & Streamable HTTP)</strong></summary>

When connecting from a machine where `orchestra master` is not running locally, pass `--endpoint` (and `ORCHESTRA_TOKEN` when the master uses `--auth token`):

#### Claude Code (`~/.claude.json`) & Antigravity (`~/.gemini/config/mcp_config.json`)

```json
{
  "mcpServers": {
    "orchestra": {
      "command": "orchestra",
      "args": ["mcp", "--endpoint", "http://100.x.y.z:8787"],
      "env": {
        "ORCHESTRA_TOKEN": "paste-your-token-here"
      }
    }
  }
}
```

#### Codex (`~/.codex/config.toml`)

```toml
[mcp_servers.orchestra]
command = "orchestra"
args = ["mcp", "--endpoint", "http://100.x.y.z:8787"]
env = { "ORCHESTRA_TOKEN" = "paste-your-token-here" }
```

#### OpenCode (`~/.config/opencode/opencode.json`)

```json
{
  "mcp": {
    "orchestra": {
      "type": "local",
      "command": ["orchestra", "mcp", "--endpoint", "http://100.x.y.z:8787"],
      "environment": {
        "ORCHESTRA_TOKEN": "paste-your-token-here"
      },
      "enabled": true
    }
  }
}
```

#### Direct Streamable HTTP

Agents that support remote MCP natively can connect directly to `POST http://127.0.0.1:8787/mcp` (with header `Authorization: Bearer <ORCHESTRA_TOKEN>` when `--auth token` is active). Using the `orchestra mcp` stdio bridge additionally subscribes to `/api/mcp/events` SSE notifications so idle sessions wake on thread and mail events.

</details>

### 4. Verify Cluster Health

In another terminal (auto-discovered via `~/.local/share/orchestra/connection.json`):

```bash
orchestra status
orchestra nodes
```

---

## How It Works

```text
Main Workstation                                   Worker Machines (Local or Tailscale)
┌──────────────────────────────────────────┐       ┌──────────────────────────────────────────┐
│ Your Coding Agent (Claude / Codex / AGY) │       │ orchestra slave (macOS / Linux)          │
│    │                                     │       │  ├─ Outbound WebSocket (/control-agent)  │
│    ▼ stdio MCP (or HTTP /mcp)            │       │  ├─ Isolated Git Worktrees & File Locks  │
│ orchestra mcp                            │       │  ├─ Ephemeral 127.0.0.1 orchestra_hub    │
│    │ HTTP + SSE Doorbell                 │  ws/  │  └─ Local Agent Harnesses:               │
│    ▼                                     │  wss  │      Claude · Codex · OpenCode · AGY     │
│ orchestra master                         │◄─────►│      (via ACP v1 or native headless CLI) │
│  ├─ Pure-Go SQLite (WAL + event journal) │       └──────────────────────────────────────────┘
│  ├─ Capability & Readiness Scheduler     │
│  ├─ Task DAGs, Rooms & Agent Mailboxes   │
│  └─ SHA-256 Content-Addressed Artifacts  │
└──────────────────────────────────────────┘
```

### Core Coordination Primitives

Orchestra gives agents four ways to collaborate, ranging from structured batch execution to free-form peer messaging:

| Primitive | What It Solves | How Agents Use It |
| :--- | :--- | :--- |
| **Threads & Task DAGs** | Parallel execution with dependencies and capability matching | Describe *what* a task needs (`linux`, `accelerator:cuda`, `opencode-cli`, writable `git-worktree`) rather than hardcoding machine names. |
| **Blackboard Rooms** | Keeping parallel sibling tasks aligned on shared contracts | Post structured `finding`, `contract`, `decision`, `gotcha`, or `question` entries to the thread room—and **nudge** active tasks so they reconcile changes before completing. |
| **Resident Instances** | Stateful, multi-turn specialist sessions | Spawn a warm ACP session on a worker, delegate follow-up tasks to the same live context, and release it when finished. |
| **Agent Mail (`handle@host`)** | Direct peer-to-peer messaging across machines and interactive sessions | Every MCP session and resident worker gets a `<handle>@<host>` mailbox (`auth-api@macbook`, `gpu-bench@linux-01`). Sending mail wakes idle agents immediately. |

---

## Scaling to Multiple Machines

Orchestra is designed around **Tailscale** (or any private WireGuard network): `orchestra master` runs on your main workstation or home server, and remote `orchestra slave` daemons dial outbound to `/control-agent`—so workers never need inbound firewall ports.

### 1. Cross-Compile for Your Fleet

```bash
# Produces darwin/arm64, darwin/amd64, linux/arm64, and linux/amd64 binaries in ./dist/
task build:all
```

Copy the matching binary to `/usr/local/bin/orchestra` or `~/.local/bin/orchestra` on each worker machine.

### 2. Start `orchestra master` on Tailscale

Pass `--auth tailscale` so neither `orchestra master`, remote `orchestra slave` machines, `orchestra mcp`, nor CLI commands need `ORCHESTRA_TOKEN`:

```bash
# Auto-binds <your-tailscale-ipv4>:8787, verifies peers via `tailscale whois`,
# refuses wildcard/LAN binds, and rejects browser Origin / Sec-Fetch-* / Funnel requests:
orchestra master \
  --auth tailscale \
  --data-dir ~/.local/share/orchestra/master
```

By default, `--auth tailscale` authorizes devices owned by the same Tailscale user as `orchestra master`, plus ACL-tagged devices (`tag:...`) belonging to the same tailnet. To authorize additional tailnet users or restrict to specific ACL tags, pass repeatable `--tailscale-allow-user <login>` and/or `--tailscale-allow-tag tag:<name>` flags.

> Prefer explicit bearer tokens instead? Omit `--auth tailscale` (default `--auth token`) and bind `--listen "$(tailscale ip -4):8787"` with `ORCHESTRA_TOKEN`.

<details>
<summary><strong>Optional: Enable End-to-End TLS (`https://` & `wss://`)</strong></summary>

Provision a Tailscale MagicDNS certificate (or use your own internal CA) and pass `--tls-cert` and `--tls-key`:

```bash
tailscale cert my-main-mac.tailnet-name.ts.net

orchestra master \
  --auth tailscale \
  --tls-cert ./my-main-mac.tailnet-name.ts.net.crt \
  --tls-key ./my-main-mac.tailnet-name.ts.net.key \
  --data-dir ~/.local/share/orchestra/master
```

If you use a private CA instead of MagicDNS public certificates, pass `--tls-ca /path/to/ca.pem` (or `ORCHESTRA_TLS_CA`) to `orchestra slave`, `orchestra mcp`, and CLI commands.

</details>

### 3. Check Readiness & Start `orchestra slave`

On any remote worker machine on your tailnet, start `orchestra slave` with **no `--endpoint` and no `--token`**—it automatically probes online Tailscale peers for active Orchestra masters on port `8787`, connects immediately if one is found, or prompts you interactively if multiple masters are online:

```bash
orchestra slave \
  --workspace-root /srv/workspaces \
  --claude-acp-auth-mode local-subscription
```

Point `--workspace-root` at your **parent projects directory** (for example `~/Projects` or `/srv/workspaces`; if omitted, `orchestra slave` defaults to the current working directory). On startup and reconnect, each slave automatically discovers Git repositories beneath its workspace roots and advertises them to `orchestra master`:
- **Single-repository tasks** can set `requirements.workspace.repository` (for example `https://github.com/org/repo`) or a relative `requirements.workspace.path` (for example `repo`), and the scheduler automatically places and runs the task inside that repository checkout (or provisions a `git-worktree` lease from it).
- **Cross-repository tasks** can omit `requirements.workspace`, running in the parent `--workspace-root` with access to sibling repositories beneath it.
- **Missing repositories** can be cloned remotely onto a slave via the `clone_repository` MCP tool or `orchestra nodes clone`, and repository inventories can be re-scanned anytime via `refresh_repositories` or `orchestra nodes refresh`.

You can also run `orchestra doctor` first or pin explicit node metadata, capabilities, and endpoints:

```bash
orchestra doctor \
  --endpoint http://100.x.y.z:8787 \
  --claude-acp-auth-mode local-subscription

orchestra slave \
  --endpoint http://100.x.y.z:8787 \
  --id build-linux-01 \
  --name "Linux Build Server" \
  --kind home-server \
  --workspace-root /srv/workspaces \
  --label linux \
  --label x86-64 \
  --accelerator cuda \
  --concurrency 4 \
  --instance-capacity 6 \
  --approval-policy manual \
  --claude-acp-auth-mode local-subscription
```

Both `orchestra master` and `orchestra slave` emit structured, secret-redacted operational logs to `stderr` (`info` by default; customize with `--verbose`, `--log-level debug|info|warn|error|quiet`, and `--log-format text|json`).

### Supported Harnesses at a Glance

| Harness ID | CLI Binary | Default Transport | Zero-Download Ready? | Notes |
| :--- | :--- | :--- | :--- | :--- |
| `claude-cli` | `claude` | `acp-v1` (with adapter) or `native-cli` | Yes (`native-cli`) | Auto-discovers `claude-agent-acp` on `PATH`; pass `--claude-acp-auth-mode local-subscription\|api` to enable `acp-v1`. |
| `codex-cli` | `codex` | `acp-v1` (with adapter) or `native-cli` | Yes (`native-cli`) | Auto-discovers `codex-acp` on `PATH` (Homebrew/Linuxbrew/npm) for stateful ACP sessions. |
| `opencode-cli` | `opencode` | `acp-v1` (built-in) | **Yes (`acp-v1`)** | Built-in ACP server with full `provider/model` pass-through (e.g., GLM + DeepSeek concurrently). |
| `antigravity-cli` | `agy` | `acp-v1` (built-in bridge) | **Yes (`acp-v1`)** | Uses Orchestra's built-in Go bridge (`orchestra acp-bridge antigravity`) automatically whenever `agy` is on `PATH`. |

#### Installing ACP Adapters for Codex & Claude (`codex-acp` & `claude-agent-acp`)

While `opencode` and `agy` include built-in `acp-v1` support, `codex` and `claude` use companion ACP stdio adapters when you want stateful, resumable ACP resident sessions (and real-time MCP/permission callbacks) instead of headless `native-cli` execution.

Install them directly onto `PATH` on any worker machine—`orchestra doctor` and `orchestra slave` automatically discover them at startup (including Homebrew, Linuxbrew, and global `npm` symlinks):

```bash
# Codex ACP adapter (via Homebrew/Linuxbrew or npm)
brew install codex-acp
# or: npm install -g @agentclientprotocol/codex-acp

# Claude ACP adapter (via npm)
npm install -g @agentclientprotocol/claude-agent-acp
```

Verify readiness with `orchestra doctor` (and **restart `orchestra slave`** if it was already running when you installed the adapter, since adapters are probed at worker startup):

```bash
orchestra doctor --claude-acp-auth-mode local-subscription
```

<details>
<summary><strong>Optional: Checksum-Pinned ACP Adapters (`orchestra setup` or `--acp-adapter`)</strong></summary>

For environments that require strict SHA-256 binary pinning instead of `PATH` auto-discovery, Orchestra includes a checksum-verified component manager to install, activate, and roll back standalone adapter binaries:

```bash
# 1. Generate an installation plan
orchestra setup plan --out plan.json

# 2. Apply downloads or register standalone checksummed adapter binaries
orchestra setup apply --plan plan.json \
  --manual-artifact claude-acp=/path/to/claude-agent-acp \
  --manual-checksum claude-acp=<sha256> \
  --manual-artifact codex-acp=/path/to/codex-acp \
  --manual-checksum codex-acp=<sha256>

# 3. Activate specific versions
orchestra setup activate --kind acp-adapter --id claude-acp --version 0.79.0
orchestra setup activate --kind acp-adapter --id codex-acp --version 1.12.0
```

Alternatively, pin a resolved regular-file executable directly on startup without `orchestra setup` (use `readlink -f "$(command -v codex-acp)"` if resolving a Homebrew/npm symlink):

```bash
--acp-adapter codex-cli=sha256:<hex>:/absolute/path/to/resolved/entrypoint
```

</details>

<details>
<summary><strong>Running Multiple Models Concurrently with OpenCode (GLM, DeepSeek, etc.)</strong></summary>

`opencode-cli` discovers models reported by `opencode models` and passes `provider/model` IDs straight through without modifying your global or project OpenCode config files. A single slave with `--concurrency 2+` can run multiple providers in parallel:

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

</details>

<details>
<summary><strong>Defining Reusable Project Readiness Profiles (`--project-profiles`)</strong></summary>

Pass `--project-profiles profiles.json` to `orchestra master` so tasks referencing a `projectProfileId` automatically enforce worker capability prerequisites:

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

</details>

---

## Operator CLI

Every inspection and control command auto-discovers `~/.local/share/orchestra/connection.json` when omitted, supports `ORCHESTRA_ENDPOINT` and `ORCHESTRA_TOKEN` overrides, and accepts `--json` for scripting.

```bash
# Register, inspect, or remove Orchestra MCP across local harnesses
orchestra mcp install
orchestra mcp status
orchestra mcp uninstall
# Cluster overview, discovered repositories & remote repository management
orchestra status
orchestra nodes
orchestra nodes refresh
orchestra nodes clone --node build-linux-01 --repo git@github.com:org/service.git

# Self-update local CLI/master binary or roll out a release to connected slaves
orchestra update --check
orchestra update
orchestra nodes update --wait

# Inspect coordination threads, task DAGs, and transcripts
orchestra threads --status open
orchestra thread <thread-id>

# Read, post guidance to, or nudge a thread's shared blackboard room
orchestra room read <thread-id>
orchestra room post <thread-id> "Use PostgreSQL 17 UUIDv7 keys" --kind decision --nudge

# Inspect and message <handle>@<host> agent mailboxes
orchestra mail agents --online
orchestra mail send auth-api@linux-01 "Please rebase onto main and re-run integration tests"
orchestra mail inbox

# Review & resolve worker permission requests
orchestra approvals --status pending
orchestra approve <approval-id> --option allow_once
orchestra reject <approval-id> --reason "Do not modify production config"

# Cancel a running or queued run/task
orchestra cancel <run-id|task-id>
```

---

## Reference & Deep Dives

<details>
<summary><strong>Security Model & Invariants</strong></summary>

- **Flexible authentication**: Constant-time bearer authentication (`--auth token`, default, with automatic `0600` `<data-dir>/token` generation) or zero-token Tailscale peer verification (`--auth tailscale`) using `tailscale whois`, strict Tailscale/loopback bind confinement, and browser `Origin` / `Sec-Fetch-*` / Tailscale Funnel request rejection.
- **Vendor credentials remain on workers**: `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, OAuth credentials, and CLI subscriptions never leave `orchestra slave` machines. `ORCHESTRA_TOKEN` is stripped from child harness environments; workers talk only to an ephemeral loopback `orchestra_hub` server scoped to their active run.
- **No shell execution**: Harnesses and `git` are spawned directly via `exec.CommandContext` with process-group isolation and cancellation tombstones—never `sh -c`.
- **Canonical workspace confinement**: Every workspace path on a slave is resolved symlink-free and verified to live beneath an operator-configured `--workspace-root`.
- **Claude ACP billing gate**: `claude-cli` over `acp-v1` requires an explicit `--claude-acp-auth-mode` (`local-subscription` or `api`) so local Max/Pro subscription users never accidentally incur API key charges.
- **Automatic secret redaction**: Harness events, transcripts, operational logs, and error diagnostics are scrubbed before SQLite persistence or CLI/MCP output.

</details>

<details>
<summary><strong>MCP Tool Reference (Orchestrator & Worker Surfaces)</strong></summary>

#### Orchestrator Surface (`orchestra mcp` & `POST /mcp` — 28 Tools)

| Area | Tools |
| :--- | :--- |
| **Inventory & Updates** | `get_execution_inventory`, `update_nodes` |
| **Threads & Events** | `create_thread`, `list_threads`, `attach_thread`, `detach_thread`, `get_thread_context`, `get_thread_events`, `update_thread` |
| **Blackboard Room** | `read_room`, `post_room_message`, `nudge_room` |
| **Tasks & Artifacts** | `submit_tasks`, `update_task`, `send_task_message`, `post_artifact` |
| **Resident Instances** | `spawn_instance`, `get_instance`, `renew_instance`, `release_instance` |
| **Agent Mail** | `get_mail_identity`, `set_mail_identity`, `list_mailboxes`, `send_mail`, `read_mail`, `wait_for_mail` |
| **Approvals** | `list_approvals`, `resolve_approval` |

#### Worker Run Surface (`orchestra_hub` — up to 21 Tools)

Each worker run receives an isolated `127.0.0.1` Streamable HTTP MCP server exposing:
- **Standard Worker Tools (14):** `get_task_context`, `read_room`, `post_room_message`, `send_task_message`, `wait_for_task_events`, `update_task`, `post_artifact`, `update_thread`, `get_mail_identity`, `set_mail_identity`, `list_mailboxes`, `send_mail`, `read_mail`, `wait_for_mail`.
- **Delegation Tools (+7 when delegation is permitted):** `get_execution_inventory`, `submit_tasks`, `delegate_task`, `spawn_instance`, `get_instance`, `renew_instance`, `release_instance`.

</details>

<details>
<summary><strong>Full Daemon Flag Reference (<code>master</code>, <code>slave</code>, <code>mcp</code>)</strong></summary>

#### `orchestra master`

| Flag | Environment Variable | Description |
| :--- | :--- | :--- |
| `--listen` | `ORCHESTRA_LISTEN` | HTTP/WebSocket listen address (default `127.0.0.1:8787`, or `<tailscale-ipv4>:8787` with `--auth tailscale`) |
| `--auth` | `ORCHESTRA_AUTH_MODE` | Authentication mode: `token` (default) or `tailscale` |
| `--token` | `ORCHESTRA_TOKEN` | Shared bearer token in `token` mode (auto-generated/persisted at `<data-dir>/token` if omitted) |
| `--tailscale-allow-user` | `ORCHESTRA_TAILSCALE_ALLOW_USERS` | Authorized Tailscale login in `tailscale` mode (repeatable; defaults to master's Tailscale user) |
| `--tailscale-allow-tag` | `ORCHESTRA_TAILSCALE_ALLOW_TAGS` | Authorized Tailscale ACL `tag:<name>` in `tailscale` mode (repeatable) |
| `--tailscale-binary` | `ORCHESTRA_TAILSCALE_BINARY` | Explicit path to the `tailscale` CLI binary |
| `--connection-file` | `ORCHESTRA_CONNECTION_FILE` | Local discovery file path (default `~/.local/share/orchestra/connection.json`; `none` to disable) |
| `--data-dir` | `ORCHESTRA_DATA_DIR` | Directory for SQLite WAL database and SHA-256 artifact store |
| `--tls-cert`, `--tls-key` | `ORCHESTRA_TLS_CERT`, `ORCHESTRA_TLS_KEY` | PEM certificate and private key for `https://` and `wss://` |
| `--project-profiles` | `ORCHESTRA_PROJECT_PROFILES` | Optional JSON file defining project capability profiles |
| `--with-local-worker` | `ORCHESTRA_WITH_LOCAL_WORKER` | Run an embedded worker connected over loopback |
| `--workspace-root` | — | Canonical workspace root for the embedded worker (repeatable) |
| `--worker-id`, `--worker-name` | — | Stable ID and display name for the embedded worker |
| `--worker-concurrency`, `--worker-instance-capacity` | — | Max concurrent runs (default `2`) and resident instances (default `4`) |
| `--worker-label` | — | Capability label for the embedded worker (repeatable) |
| `--verbose`, `--log-level`, `--log-format` | `ORCHESTRA_VERBOSE`, `ORCHESTRA_LOG_LEVEL`, `ORCHESTRA_LOG_FORMAT` | Operational logging (`debug\|info\|warn\|error\|quiet`, `text\|json`) |

#### `orchestra slave`

| Flag | Environment Variable | Description |
| :--- | :--- | :--- |
| `--endpoint` | `ORCHESTRA_ENDPOINT` | Master URL (auto-discovered via `connection.json` or Tailscale peers if omitted) |
| `--master-port` | `ORCHESTRA_MASTER_PORT` | Master port probed during Tailscale auto-discovery (default `8787`) |
| `--tailscale-binary` | `ORCHESTRA_TAILSCALE_BINARY` | Explicit path to the `tailscale` CLI binary |
| `--token` | `ORCHESTRA_TOKEN` | Shared bearer token when connecting to a `token`-mode master |
| `--connection-file` | `ORCHESTRA_CONNECTION_FILE` | Local discovery file fallback path (`none` to disable) |
| `--id`, `--name`, `--kind` | `ORCHESTRA_NODE_ID`, `ORCHESTRA_NAME`, `ORCHESTRA_KIND` | Node identity and kind (`local`, `home-server`, `cloud`) |
| `--workspace-root` | `WORKSPACE_ROOTS` | Allowed parent directory for task workspaces (defaults to current working directory; repeatable) |
| `--concurrency`, `--instance-capacity` | `ORCHESTRA_CONCURRENCY`, `ORCHESTRA_INSTANCE_CAPACITY` | Max concurrent runs (`2`) and resident instances (`4`) |
| `--label`, `--accelerator`, `--toolchain` | `ORCHESTRA_LABELS`, `ORCHESTRA_ACCELERATORS`, `ORCHESTRA_TOOLCHAINS` | Advertised scheduling capabilities (repeatable) |
| `--approval-policy` | `ORCHESTRA_APPROVAL_POLICY` | `manual`, `auto`, `bypass`, or `<harness>=<policy>` (repeatable) |
| `--claude-acp-auth-mode` | `ORCHESTRA_CLAUDE_ACP_AUTH_MODE` | `local-subscription` or `api` safety gate for Claude ACP |
| `--acp-adapter` | `ORCHESTRA_ACP_ADAPTERS` | Pin `<harness>=sha256:<hex>:/abs/path` (repeatable) |
| `--require-acp` | `ORCHESTRA_REQUIRE_ACP` | Withhold harnesses that only have `native-cli` transport |
| `--disable-self-update` | `ORCHESTRA_DISABLE_SELF_UPDATE` | Reject remote self-update directives on this slave node |
| `--tls-ca`, `--tls-insecure` | `ORCHESTRA_TLS_CA`, `ORCHESTRA_TLS_INSECURE` | Custom CA bundle path or TLS verification skip |
| `--verbose`, `--log-level`, `--log-format` | `ORCHESTRA_VERBOSE`, `ORCHESTRA_LOG_LEVEL`, `ORCHESTRA_LOG_FORMAT` | Operational logging (`debug\|info\|warn\|error\|quiet`, `text\|json`) |

#### `orchestra mcp`

| Flag | Environment Variable | Description |
| :--- | :--- | :--- |
| `--endpoint` | `ORCHESTRA_ENDPOINT` | Master HTTP/HTTPS URL (auto-discovered via `connection.json` if omitted) |
| `--token` | `ORCHESTRA_TOKEN` | Shared bearer token override (auto-discovered via `connection.json` if omitted) |
| `--connection-file` | `ORCHESTRA_CONNECTION_FILE` | Local discovery file path (`none` to disable) |
| `--name`, `--host` | `ORCHESTRA_MAIL_NAME`, `ORCHESTRA_MAIL_HOST` | Optional Agent Mail handle and host override (`<name>@<host>`) |
| `--client-id` | `ORCHESTRA_CLIENT_ID` | Stable client identifier across bridge restarts |

</details>

---

## Development & Verification

```bash
# Run static analysis (gofmt, go vet, go fix, golangci-lint, whitespace check)
task lint

# Run package unit tests with the race detector
task unit:test

# Run multi-process E2E system tests (master + multiple slaves + MCP bridge + harnesses)
task system:test

# Full CI pipeline: lint + unit & system tests (-race) + cross-platform release builds
task ci
```

### Releasing

Releases are published through the **Release** GitHub Actions workflow (`.github/workflows/release.yml`):

1. Trigger **Release** manually (`workflow_dispatch`) on `main` and select a SemVer bump (`patch`, `minor`, or `major`), or push an annotated `v*` tag directly.
2. CI runs static analysis, unit tests, and multi-process system tests against the target commit before tagging `vMAJOR.MINOR.PATCH`.
3. [GoReleaser](.goreleaser.yaml) cross-compiles `darwin`/`linux` (`amd64`/`arm64`) archives with version, commit, and build timestamp metadata injected via `-ldflags`, publishes SHA-256 checksums to GitHub Releases, and updates the `orchestra` cask in [`cafecito-games/homebrew-tap`](https://github.com/cafecito-games/homebrew-tap) using `HOMEBREW_TAP_TOKEN`.

---

## License

MIT License — Copyright (c) 2026 Cafecito Games LLC. See [LICENSE](LICENSE) for details.
