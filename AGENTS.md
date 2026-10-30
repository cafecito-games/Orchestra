# Repository Guidelines

## Project Structure & Module Organization

`cmd/orchestra/main.go` is the executable entry point. Most code lives in `internal/`: `orchestrator/` implements the control plane, `slave/` and `harness/` run workers and agent processes, `mcpserver/` and `mcpbridge/` expose MCP, and `store/` owns persistence. Supporting packages cover protocol types, ACP integration, setup, workspace safety, and readiness. Unit tests sit beside their packages as `*_test.go`; multi-process coverage is in `internal/systemtest/`. Generated binaries belong in `bin/` or `dist/`.

## Build, Test, and Development Commands

The project requires Go 1.26+ and optionally [Task](https://taskfile.dev/).

- `task build`: compile the local, CGo-free binary to `bin/orchestra`.
- `task run:master -- --help`: run the master from source and pass CLI flags after `--`.
- `task unit:test`: run all package tests with the race detector.
- `task system:test`: run build-tagged, real-process system tests (timeout: 10 minutes).
- `task lint`: check `gofmt`, `go vet`, and whitespace errors.
- `task ci`: run the complete verification and cross-platform build pipeline.

Use `go test -race ./internal/store/...` for a focused package test.

## Coding Style & Naming Conventions

Format Go code with `gofmt` (`task fmt`) and keep `task lint` clean. Follow standard Go conventions: tabs as emitted by `gofmt`, short lowercase package names, exported identifiers in `PascalCase`, and unexported identifiers in `camelCase`. Keep platform-specific implementations in suffixed files such as `process_unix.go` or `lock_other.go`. Prefer small, responsibility-focused internal packages and preserve the project’s direct `exec.CommandContext` approach rather than introducing shell execution.

## Testing Guidelines

Write table-driven tests where multiple inputs share behavior, and name tests `TestFeature` or `TestFeature_Scenario`. Use `testify` only where it improves clarity. Changes involving concurrency, persistence, transport, process lifecycle, or authentication should include regression coverage and pass the race detector. Run `task test` before submitting; use `task ci` for release-sensitive changes.

## Commit & Pull Request Guidelines

The current history contains a single imperative, sentence-style subject (`Bootstrap headless Go control plane and worker agent`). Continue with concise subjects that describe the outcome, and keep each commit focused. Pull requests should explain the motivation and behavior change, link relevant issues, list verification commands, and call out protocol, security, or compatibility effects. Include terminal output or screenshots only when they clarify operator-visible behavior.

## Security & Configuration

Never commit tokens, vendor credentials, local `.env` files, TLS keys, or runtime data. Preserve bearer-token authentication, secret redaction, canonical workspace confinement, and the rule that vendor credentials remain on worker machines.
