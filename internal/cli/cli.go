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

	"github.com/cafecito-games/Orchestra/internal/activitylog"
	"github.com/cafecito-games/Orchestra/internal/mcpbridge"
	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/slave"
	"github.com/cafecito-games/Orchestra/internal/tailscaleauth"
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
		return runSlave(operationContext, remaining, standardInput, standardOutput, standardError, lookupEnvironment)
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
	case "update":
		return runUpdate(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
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
  mcp         Run the stdio MCP bridge or configure local harnesses (install, uninstall, status)
  acp-bridge  Run a built-in stdio Agent Client Protocol bridge (antigravity)

Operator Commands:
  status      Inspect orchestrator health, connected nodes, and active workloads
  nodes       List worker machines, refresh/clone repositories, or trigger self-updates (nodes refresh|clone|update)
  threads     List coordination threads on the orchestrator
  thread      Inspect a thread's task DAG, resident instances, runs, messages, and transcripts
  room        Read, post, nudge, or locate a thread's shared blackboard room
  mail        List <handle>@<host> agent mailboxes, send or read agent mail, and rename handles
  approvals   List worker permission requests
  approve     Approve a pending worker permission request
  reject      Reject a pending worker permission request
  cancel      Cancel an active run or task by identifier

Node Setup, Updates & Diagnostics:
  update      Self-update the local Orchestra binary (and optionally slave nodes) from GitHub Releases
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
		return writePrettyJSON(standardOutput, standardError, protocol.CurrentBuildInfo())
	}
	fmt.Fprintln(standardOutput, protocol.CurrentBuildInfo().String())
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
	logger := activitylog.New(activitylog.Options{
		Writer:            standardError,
		Level:             configuration.LogLevel,
		Format:            configuration.LogFormat,
		Redactor:          protocol.NewRedactor(configuration.Token),
		LookupEnvironment: lookupEnvironment,
	})
	configuration.Logger = logger

	running, err := orchestrator.StartMaster(operationContext, configuration)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra master: %v\n", err)
		return 1
	}
	logger = logger.WithSecrets(running.Token)
	logger.Info(
		"master",
		fmt.Sprintf("orchestra master %s listening", protocol.EffectiveBuildVersion()),
		activitylog.String("listen", running.Address),
		activitylog.String("endpoint", running.BaseURL),
		activitylog.String("auth", running.AuthMode),
		activitylog.Bool("localWorker", configuration.WithLocalWorker),
	)
	if running.AuthMode == tailscaleauth.AuthModeTailscale {
		nodeLabel := running.TailscaleSelf.DNSName
		if nodeLabel == "" {
			nodeLabel = running.TailscaleSelf.HostName
		}
		logger.Info(
			"auth",
			"tailscale peer identity verification enabled",
			activitylog.String("node", nodeLabel),
			activitylog.String("owner", running.TailscaleSelf.LoginName),
			activitylog.String("tailnet", running.TailscaleSelf.MagicDNSSuffix),
		)
	}
	if running.TokenGenerated {
		logger.Info(
			"auth",
			"generated persistent bearer token",
			activitylog.String("file", running.TokenPath),
		)
	} else if running.TokenPath != "" {
		logger.Info(
			"auth",
			"bearer token ready",
			activitylog.String("file", running.TokenPath),
		)
	}
	if running.ConnectionFilePath != "" {
		logger.Info(
			"config",
			"wrote local discovery state",
			activitylog.String("file", running.ConnectionFilePath),
		)
	}
	if waitErr := running.Wait(); waitErr != nil && !errors.Is(waitErr, context.Canceled) {
		logger.Error("master", "stopped with error", activitylog.String("error", waitErr.Error()))
		return 1
	}
	return 0
}

func printMasterUsage(output io.Writer) {
	fmt.Fprintf(output, `Usage: orchestra master [flags]

Flags:
  --listen string                HTTP/WebSocket listen address (default 127.0.0.1:8787, or <tailscale-ip>:8787 with --auth tailscale, env ORCHESTRA_LISTEN)
  --auth string                  Authentication mode: token or tailscale (default token, env ORCHESTRA_AUTH_MODE)
  --token string                 Bearer token (auto-generated in <data-dir>/token in token mode, env ORCHESTRA_TOKEN)
  --tailscale-allow-user value   Allowed Tailscale user login in tailscale auth mode (repeatable, env ORCHESTRA_TAILSCALE_ALLOW_USERS)
  --tailscale-allow-tag value    Allowed Tailscale ACL tag in tailscale auth mode (repeatable, env ORCHESTRA_TAILSCALE_ALLOW_TAGS)
  --tailscale-binary string      Explicit path to tailscale CLI binary (env ORCHESTRA_TAILSCALE_BINARY)
  --data-dir string              SQLite state and artifact storage directory (env ORCHESTRA_DATA_DIR)
  --connection-file string       Local discovery state file written on startup (env ORCHESTRA_CONNECTION_FILE)
  --tls-cert string              TLS certificate PEM path for https:// and wss:// (env ORCHESTRA_TLS_CERT)
  --tls-key string               TLS private key PEM path for https:// and wss:// (env ORCHESTRA_TLS_KEY)
  --project-profiles string      Optional JSON file containing project profiles (env ORCHESTRA_PROJECT_PROFILES)
  --verbose                      Enable debug operational logs (env ORCHESTRA_VERBOSE)
  --log-level string             Operational log level: debug, info, warn, error, quiet (default info, env ORCHESTRA_LOG_LEVEL)
  --log-format string            Operational log format: text or json (default text, env ORCHESTRA_LOG_FORMAT)
  --with-local-worker            Run an embedded slave worker connected to this master (env ORCHESTRA_WITH_LOCAL_WORKER)
  --worker-name string           Display name for embedded local worker
  --worker-id string             Node ID for embedded local worker
  --worker-concurrency int       Maximum concurrent runs for embedded local worker (default 2)
  --worker-instance-capacity int Maximum resident instances for embedded local worker (default 4)
  --workspace-root value         Canonical workspace root for embedded local worker (repeatable)
  --worker-label value           Capability label for embedded local worker (repeatable)
  --approval-policy value        Approval policy for embedded local worker (repeatable)
  --acp-adapter value            Pinned ACP adapter override for embedded local worker (repeatable)
  --claude-acp-auth-mode string  Claude ACP auth mode (local-subscription or api)
`)
}

func runSlave(
	operationContext context.Context,
	arguments []string,
	standardInput io.Reader,
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
	logger := activitylog.New(activitylog.Options{
		Writer:            standardError,
		Level:             configuration.LogLevel,
		Format:            configuration.LogFormat,
		Redactor:          protocol.NewRedactor(configuration.Token),
		LookupEnvironment: lookupEnvironment,
	})
	configuration.ActivityLogger = logger

	runtimeWorker, err := slave.BuildRuntime(operationContext, configuration)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra slave: %v\n", err)
		return 1
	}
	for _, warning := range runtimeWorker.Warnings {
		logger.Warn("startup", warning)
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
	if !configuration.EndpointExplicit {
		if discoverErr := resolveSlaveMasterEndpoint(operationContext, &configuration, standardInput, standardError, logger); discoverErr != nil {
			if errors.Is(discoverErr, context.Canceled) {
				return 0
			}
			fmt.Fprintf(standardError, "orchestra slave: %v\n", discoverErr)
			return 1
		}
		runtimeWorker.Client.SetTarget(configuration.ControlEndpoint, configuration.Token)
		logger = logger.WithSecrets(configuration.Token)
	}

	logger.Info(
		"slave",
		fmt.Sprintf("orchestra slave %s starting", protocol.EffectiveBuildVersion()),
		activitylog.String("node", runtimeWorker.Node.ID),
		activitylog.String("endpoint", configuration.ControlEndpoint),
		activitylog.String("harnesses", strings.Join(availableLabels, "; ")),
		activitylog.String("roots", strings.Join(runtimeWorker.Node.WorkspaceRoots, ",")),
		activitylog.Int("repositories", len(runtimeWorker.RepositoryReport.Repositories)),
	)
	runtimeWorker.Client.SetLogger(func(message string) {
		logger.Info("update", message)
	})
	if runErr := runtimeWorker.Client.Run(operationContext); runErr != nil && !errors.Is(runErr, context.Canceled) {
		logger.Error("slave", "stopped with error", activitylog.String("error", runErr.Error()))
		return 1
	}
	return 0
}

func printSlaveUsage(output io.Writer) {
	fmt.Fprintf(output, `Usage: orchestra slave [flags]

Flags:
  --endpoint string             Orchestrator HTTP/WebSocket endpoint (auto-discovered via localhost/Tailscale if omitted, env ORCHESTRA_ENDPOINT)
  --master-port int             Master port probed during Tailscale auto-discovery (default 8787, env ORCHESTRA_MASTER_PORT)
  --tailscale-binary string     Optional explicit path to tailscale CLI binary (env ORCHESTRA_TAILSCALE_BINARY)
  --token string                Bearer token matching ORCHESTRA_TOKEN (env ORCHESTRA_TOKEN)
  --connection-file string      Local discovery state file fallback (env ORCHESTRA_CONNECTION_FILE)
  --id string                   Stable node identifier (env ORCHESTRA_NODE_ID)
  --name string                 Display name for this slave node (env ORCHESTRA_NAME)
  --kind string                 Node kind: local, home-server, or cloud (default local, env ORCHESTRA_KIND)
  --workspace-root value        Canonical workspace root directory (defaults to current working directory; repeatable, env WORKSPACE_ROOTS)
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
  --disable-self-update         Reject remote self-update directives on this slave (env ORCHESTRA_DISABLE_SELF_UPDATE)
  --tls-ca string               CA certificate PEM path for https:// and wss:// (env ORCHESTRA_TLS_CA)
  --tls-insecure                Skip TLS certificate verification (env ORCHESTRA_TLS_INSECURE)
  --verbose                     Enable debug operational logs (env ORCHESTRA_VERBOSE)
  --log-level string            Operational log level: debug, info, warn, error, quiet (default info, env ORCHESTRA_LOG_LEVEL)
  --log-format string           Operational log format: text or json (default text, env ORCHESTRA_LOG_FORMAT)
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
	if len(arguments) > 0 {
		switch arguments[0] {
		case "install":
			return runMCPInstall(arguments[1:], standardOutput, standardError, lookupEnvironment)
		case "uninstall":
			return runMCPUninstall(arguments[1:], standardOutput, standardError, lookupEnvironment)
		case "status":
			return runMCPStatus(operationContext, arguments[1:], standardOutput, standardError, lookupEnvironment)
		}
	}
	if hasHelpFlag(arguments) {
		fmt.Fprintf(standardOutput, `Usage:
  orchestra mcp [flags]
  orchestra mcp <install|uninstall|status> [flags]

Subcommands:
  install     Register Orchestra MCP once across local harnesses (Antigravity, Claude Code, Codex, OpenCode)
  uninstall   Remove Orchestra MCP entries from local harness config files
  status      Inspect local master discovery state and per-harness MCP registration

Bridge Flags (stdio MCP server):
  --endpoint string         Explicit Orchestra master endpoint override (auto-discovered from connection.json if omitted)
  --token string            Explicit bearer token override (auto-discovered from connection.json if omitted)
  --connection-file string  Local connection discovery state file path (env ORCHESTRA_CONNECTION_FILE)
  --client-id string        Stable orchestrator client identifier (env ORCHESTRA_CLIENT_ID)
  --name string             Optional agent mail handle, e.g. "auth-api" (env ORCHESTRA_MAIL_NAME)
  --host string             Optional agent mail host override (defaults to local hostname, env ORCHESTRA_MAIL_HOST)
  --tls-ca string           CA certificate PEM path for https:// and wss:// (env ORCHESTRA_TLS_CA)
  --tls-insecure            Skip TLS certificate verification (env ORCHESTRA_TLS_INSECURE)
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
