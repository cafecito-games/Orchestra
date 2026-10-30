package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/antigravityacp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

func runACPBridge(
	operationContext context.Context,
	arguments []string,
	standardInput io.Reader,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if len(arguments) == 0 || hasHelpFlag(arguments[:1]) {
		targetWriter := standardError
		exitCode := 2
		if len(arguments) > 0 && hasHelpFlag(arguments[:1]) {
			targetWriter = standardOutput
			exitCode = 0
		}
		fmt.Fprintln(targetWriter, "Usage: orchestra acp-bridge <antigravity> [flags]")
		return exitCode
	}
	switch arguments[0] {
	case "antigravity":
		return runAntigravityACPBridge(operationContext, arguments[1:], standardInput, standardOutput, standardError, lookupEnvironment)
	default:
		fmt.Fprintf(standardError, "orchestra acp-bridge: unknown target %q; valid targets: antigravity\n", arguments[0])
		return 2
	}
}

func runAntigravityACPBridge(
	operationContext context.Context,
	arguments []string,
	standardInput io.Reader,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	defaultStateDirectory := firstEnvironmentValue(lookupEnvironment, antigravityacp.StateDirectoryEnvironmentVariable)
	if defaultStateDirectory == "" {
		defaultStateDirectory = filepath.Join(defaultSetupDataRoot(lookupEnvironment), "state", "antigravity-acp")
	}
	defaultBinary := firstEnvironmentValue(
		lookupEnvironment,
		antigravityacp.NativeBinaryEnvironmentVariable,
		antigravityacp.LegacyBinaryEnvironmentVariable,
	)

	flagSet := flag.NewFlagSet("orchestra acp-bridge antigravity", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	stateDirectory := flagSet.String("state-dir", defaultStateDirectory, "Directory for persistent Antigravity ACP session bindings")
	agyBinary := flagSet.String("agy-binary", defaultBinary, "Optional path to the agy executable")
	if err := flagSet.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra acp-bridge antigravity: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}

	resolvedBinary, err := antigravityacp.ResolveBinary(*agyBinary, lookupEnvironment)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra acp-bridge antigravity: %v\n", err)
		return 1
	}
	if runErr := antigravityacp.Run(operationContext, standardInput, standardOutput, standardError, antigravityacp.Options{
		Binary:         resolvedBinary,
		StateDirectory: *stateDirectory,
		Version:        protocol.BuildVersion,
	}); runErr != nil && !errors.Is(runErr, context.Canceled) {
		fmt.Fprintf(standardError, "orchestra acp-bridge antigravity: %v\n", runErr)
		return 1
	}
	return 0
}
