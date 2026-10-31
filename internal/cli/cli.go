// Package cli implements the unified command-line entrypoint for Orchestra,
// covering master and slave daemons, the stdio MCP bridge, headless operator
// inspection and control commands, and local component setup and diagnostics.
package cli

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/cafecito-games/Orchestra/internal/mcpbridge"
	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/slave"
)

func Run(
	operationContext context.Context,
	arguments []string,
	standardInput io.Reader,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}
	standardInput = cmp.Or[io.Reader](standardInput, os.Stdin)
	standardOutput = cmp.Or[io.Writer](standardOutput, os.Stdout)
	standardError = cmp.Or[io.Writer](standardError, os.Stderr)

	if len(arguments) == 0 {
		printTopLevelUsage(standardError)
		return 2
	}

	subcommand := arguments[0]
	remaining := arguments[1:]

	switch subcommand {
	case "-h", "--help", "help":
		printTopLevelUsage(standardOutput)
		return 0
	case "-v", "--version", "version":
		return runVersion(remaining, standardOutput, standardError)
	case "master":
		return runMaster(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "slave":
		return runSlave(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "mcp":
		return runMCPBridge(operationContext, remaining, standardInput, standardOutput, standardError, lookupEnvironment)
	case "acp-bridge":
		return runACPBridge(operationContext, remaining, standardInput, standardOutput, standardError, lookupEnvironment)
	case "status":
		return runStatus(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "nodes":
		return runNodes(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "threads":
		return runThreads(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "thread":
		return runThreadDetail(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "room":
		return runRoom(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "mail":
		return runMail(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "approvals":
		return runApprovals(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "approve":
		return runResolveApproval(operationContext, "approve", "approved", remaining, standardOutput, standardError, lookupEnvironment)
	case "reject":
		return runResolveApproval(operationContext, "reject", "rejected", remaining, standardOutput, standardError, lookupEnvironment)
	case "cancel":
		return runCancel(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "setup":
		return runSetup(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "doctor":
		return runDoctor(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	default:
		fmt.Fprintf(standardError, "orchestra: unknown subcommand %q\n\n", subcommand)
		printTopLevelUsage(standardError)
		return 2
	}
}

func printTopLevelUsage(output io.Writer) {
	fmt.Fprintf(output, `Usage: orchestra <command> [flags]

Runtime Modes:
  master      Start the Orchestra control-plane server (HTTP, WebSocket, SQLite, Streamable HTTP MCP)
  slave       Start an Orchestra remote worker daemon connecting to a master
  mcp         Run the stdio Model Context Protocol bridge for local coding agents
  acp-bridge  Run a built-in stdio Agent Client Protocol bridge (antigravity)

Operator Commands:
  status      Inspect orchestrator health, connected nodes, and active workloads
  nodes       List registered worker machines, harnesses, capabilities, and capacity
  threads     List coordination threads on the orchestrator
  thread      Inspect a thread's task DAG, resident instances, runs, messages, and transcripts
  room        Read, post, nudge, or locate a thread's shared blackboard room
  mail        List <handle>@<host> agent mailboxes, send or read agent mail, and rename handles
  approvals   List worker permission requests
  approve     Approve a pending worker permission request
  reject      Reject a pending worker permission request
  cancel      Cancel an active run or task by identifier

Node Setup & Diagnostics:
  setup       Manage pinned ACP adapters and harness components (plan, apply, activate, rollback, prune, uninstall)
  doctor      Inspect local harness readiness, managed components, auth gates, and master connectivity
  version     Print the Orchestra build and protocol version
`)
}

func runVersion(arguments []string, standardOutput, standardError io.Writer) int {
	flagSet := flag.NewFlagSet("orchestra version", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	asJSON := flagSet.Bool("json", false, "Format version information as JSON")
	if err := flagSet.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra version: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}
	if *asJSON {
		return writePrettyJSON(standardOutput, standardError, map[string]any{
			"version":         protocol.BuildVersion,
			"protocolVersion": protocol.ProtocolVersion,
		})
	}
	fmt.Fprintf(standardOutput, "orchestra %s (protocol v%s)\n", protocol.BuildVersion, protocol.ProtocolVersion)
	return 0
}

func runMaster(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if hasHelpFlag(arguments) {
		printMasterUsage(standardOutput)
		return 0
	}
	configuration, err := orchestrator.ParseConfiguration(arguments, lookupEnvironment)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra master: %v\n", err)
		return 2
	}
	running, err := orchestrator.StartMaster(operationContext, configuration)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra master: %v\n", err)
		return 1
	}
	scheme := "http"
	wsScheme := "ws"
	if configuration.TLSCertPath != "" {
		scheme = "https"
		wsScheme = "wss"
	}
	fmt.Fprintf(
		standardError,
		"orchestra master %s listening on %s (%s://%s, %s://%s/control-agent, local worker=%t)\n",
		protocol.BuildVersion,
		running.Address,
		scheme,
		running.Address,
		wsScheme,
		running.Address,
		configuration.WithLocalWorker,
	)
	if waitErr := running.Wait(); waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		fmt.Fprintf(standardError, "orchestra master stopped with error: %v\n", waitErr)
		return 1
	}
	return 0
}

func printMasterUsage(output io.Writer) {
	fmt.Fprintf(output, `Usage: orchestra master [flags]

Flags:
  --listen string               HTTP/WebSocket listen address (default 127.0.0.1:8787, env ORCHESTRA_LISTEN)
  --token string                Bearer token required for API, MCP, and slave connections (env ORCHESTRA_TOKEN)
  --data-dir string             SQLite state and artifact storage directory (env ORCHESTRA_DATA_DIR)
  --tls-cert string             TLS certificate PEM path for https:// and wss:// (env ORCHESTRA_TLS_CERT)
  --tls-key string              TLS private key PEM path for https:// and wss:// (env ORCHESTRA_TLS_KEY)
  --project-profiles string     Optional JSON file containing project profiles (env ORCHESTRA_PROJECT_PROFILES)
  --with-local-worker           Run an embedded slave worker connected to loopback (env ORCHESTRA_WITH_LOCAL_WORKER)
  --worker-name string          Display name for embedded local worker
  --worker-id string            Node ID for embedded local worker
  --worker-concurrency int      Maximum concurrent runs for embedded local worker (default 2)
  --worker-instance-capacity int Maximum resident instances for embedded local worker (default 4)
  --workspace-root value        Canonical workspace root for embedded local worker (repeatable)
  --worker-label value          Capability label for embedded local worker (repeatable)
  --approval-policy value       Approval policy for embedded local worker (repeatable)
  --acp-adapter value           Pinned ACP adapter override for embedded local worker (repeatable)
  --claude-acp-auth-mode string Claude ACP auth mode (local-subscription or api)
`)
}

func runSlave(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if hasHelpFlag(arguments) {
		printSlaveUsage(standardOutput)
		return 0
	}
	configuration, err := slave.ParseConfiguration(arguments, lookupEnvironment)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra slave: %v\n", err)
		return 2
	}
	runtimeWorker, err := slave.BuildRuntime(operationContext, configuration)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra slave: %v\n", err)
		return 1
	}
	for _, warning := range runtimeWorker.Warnings {
		fmt.Fprintf(standardError, "orchestra slave warning: %s\n", warning)
	}
	var availableLabels []string
	for _, profile := range runtimeWorker.Node.Harnesses {
		if profile.Available {
			availableLabels = append(availableLabels, fmt.Sprintf("%s (%s)", profile.ID, strings.Join(profile.Transports, ",")))
		}
	}
	if len(availableLabels) == 0 {
		if configuration.RequireACP {
			fmt.Fprintln(standardError, "orchestra slave: no ACP-ready harnesses discovered (--require-acp is enabled); install and configure an ACP adapter or ACP-native harness")
		} else {
			fmt.Fprintln(standardError, "orchestra slave: no available harnesses discovered; install and authenticate Claude Code, Codex, OpenCode, or Antigravity")
		}
		return 1
	}
	fmt.Fprintf(
		standardError,
		"orchestra slave %s (node=%s, harnesses=%s, roots=%s) connecting to %s\n",
		protocol.BuildVersion,
		runtimeWorker.Node.ID,
		strings.Join(availableLabels, "; "),
		strings.Join(runtimeWorker.Node.WorkspaceRoots, ","),
		configuration.ControlEndpoint,
	)
	var logMutex sync.Mutex
	runtimeWorker.Client.SetLogger(func(message string) {
		logMutex.Lock()
		defer logMutex.Unlock()
		fmt.Fprintf(standardError, "orchestra slave: %s\n", message)
	})
	if runErr := runtimeWorker.Client.Run(operationContext); runErr != nil && !errors.Is(runErr, context.Canceled) {
		fmt.Fprintf(standardError, "orchestra slave stopped: %v\n", runErr)
		return 1
	}
	return 0
}

func printSlaveUsage(output io.Writer) {
	fmt.Fprintf(output, `Usage: orchestra slave [flags]

Flags:
  --endpoint string             Orchestrator HTTP/WebSocket endpoint (default http://127.0.0.1:8787, env ORCHESTRA_ENDPOINT)
  --token string                Bearer token matching ORCHESTRA_TOKEN (env ORCHESTRA_TOKEN)
  --id string                   Stable node identifier (env ORCHESTRA_NODE_ID)
  --name string                 Display name for this slave node (env ORCHESTRA_NAME)
  --kind string                 Node kind: local, home-server, or cloud (default local, env ORCHESTRA_KIND)
  --workspace-root value        Canonical workspace root directory (repeatable, env WORKSPACE_ROOTS)
  --concurrency int             Maximum concurrent runs (default 2, env ORCHESTRA_CONCURRENCY)
  --instance-capacity int       Maximum resident instances (default 4, env ORCHESTRA_INSTANCE_CAPACITY)
  --label value                 Capability label (repeatable, env ORCHESTRA_LABELS)
  --accelerator value           Accelerator identifier (repeatable, env ORCHESTRA_ACCELERATORS)
  --toolchain value             Configured toolchain id=version (repeatable, env ORCHESTRA_TOOLCHAINS)
  --project value               Allowed project profile ID (repeatable, env ORCHESTRA_PROJECT_ALLOWLIST)
  --approval-policy value       Approval policy manual|auto|bypass or harness=policy (repeatable, env ORCHESTRA_APPROVAL_POLICY)
  --acp-adapter value           ACP adapter override harness=sha256:digest:/abs/path (repeatable, env ORCHESTRA_ACP_ADAPTERS)
  --acp-native-fallback value   Harness ID allowed to fall back from acp-v1 to native-cli (repeatable)
  --require-acp                 Require acp-v1 transport and withhold native-cli harnesses (env ORCHESTRA_REQUIRE_ACP)
  --claude-acp-auth-mode string Claude ACP auth mode: local-subscription or api (env ORCHESTRA_CLAUDE_ACP_AUTH_MODE)
  --data-root string            Managed component data root (env ORCHESTRA_DATA_ROOT)
  --adapter-manifest string     Optional component manifest override path (env ORCHESTRA_ADAPTER_MANIFEST)
  --tls-ca string               CA certificate PEM path for https:// and wss:// (env ORCHESTRA_TLS_CA)
  --tls-insecure                Skip TLS certificate verification (env ORCHESTRA_TLS_INSECURE)
`)
}

func runMCPBridge(
	operationContext context.Context,
	arguments []string,
	standardInput io.Reader,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if hasHelpFlag(arguments) {
		fmt.Fprintf(standardOutput, `Usage: orchestra mcp [flags]

Flags:
  --endpoint string    Orchestra master HTTP or WebSocket endpoint (default http://127.0.0.1:8787, env ORCHESTRA_ENDPOINT)
  --token string       Bearer token matching ORCHESTRA_TOKEN (env ORCHESTRA_TOKEN)
  --client-id string   Stable orchestrator client identifier (env ORCHESTRA_CLIENT_ID)
  --name string        Optional agent mail handle, e.g. "auth-api" (env ORCHESTRA_MAIL_NAME)
  --host string        Optional agent mail host override (defaults to local hostname, env ORCHESTRA_MAIL_HOST)
  --tls-ca string      CA certificate PEM path for https:// and wss:// (env ORCHESTRA_TLS_CA)
  --tls-insecure       Skip TLS certificate verification (env ORCHESTRA_TLS_INSECURE)
`)
		return 0
	}
	configuration, err := mcpbridge.ParseConfiguration(arguments, lookupEnvironment)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra mcp: %v\n", err)
		return 2
	}
	bridge := mcpbridge.NewBridge(configuration)
	if runErr := bridge.Run(operationContext, standardInput, standardOutput); runErr != nil && !errors.Is(runErr, context.Canceled) {
		fmt.Fprintf(standardError, "orchestra mcp: %v\n", runErr)
		return 1
	}
	return 0
}

func hasHelpFlag(arguments []string) bool {
	for _, argument := range arguments {
		if argument == "--" {
			return false
		}
		if argument == "-h" || argument == "--help" || argument == "-help" {
			return true
		}
	}
	return false
}

func writePrettyJSON(output io.Writer, standardError io.Writer, value any) int {
	encodedBytes, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fmt.Fprintf(standardError, "encode JSON: %v\n", err)
		return 1
	}
	_, _ = output.Write(append(encodedBytes, '\n'))
	return 0
}

func firstEnvironmentValue(lookup func(string) (string, bool), names ...string) string {
	for _, name := range names {
		if rawValue, found := lookup(name); found && strings.TrimSpace(rawValue) != "" {
			return strings.TrimSpace(rawValue)
		}
	}
	return ""
}
