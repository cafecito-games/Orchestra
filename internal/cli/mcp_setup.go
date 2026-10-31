package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"text/tabwriter"

	"github.com/cafecito-games/Orchestra/internal/localstate"
)

type mcpConnectionSummary struct {
	ConnectionFile string `json:"connectionFile"`
	Discovered     bool   `json:"discovered"`
	Endpoint       string `json:"endpoint,omitempty"`
	AuthMode       string `json:"authMode,omitempty"`
	HasToken       bool   `json:"hasToken"`
	MasterPID      int    `json:"masterPid,omitempty"`
	UpdatedAt      string `json:"updatedAt,omitempty"`
	Reachable      bool   `json:"reachable"`
	ReachableError string `json:"reachableError,omitempty"`
}

type mcpStatusReport struct {
	Connection mcpConnectionSummary          `json:"connection"`
	Harnesses  []localstate.HarnessMCPStatus `json:"harnesses"`
}

func runMCPInstall(
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra mcp install", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	var harnessTargets repeatedStringValues
	flagSet.Var(&harnessTargets, "harness", "Target harness: all, antigravity, claude, codex, or opencode (repeatable)")
	binaryPath := flagSet.String("binary", "", "Explicit path to the orchestra binary (defaults to current executable)")
	mailName := flagSet.String("name", "", "Optional fixed agent mail handle passed to `orchestra mcp --name`")
	dryRun := flagSet.Bool("dry-run", false, "Preview configuration changes without modifying files")
	asJSON := flagSet.Bool("json", false, "Format result as JSON")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra mcp install: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}

	results, err := localstate.InstallHarnessMCPConfigs(localstate.MCPInstallOptions{
		Targets:           harnessTargets,
		BinaryPath:        *binaryPath,
		MailName:          *mailName,
		DryRun:            *dryRun,
		LookupEnvironment: lookupEnvironment,
		LookPath:          exec.LookPath,
	})
	if err != nil {
		fmt.Fprintf(standardError, "orchestra mcp install: %v\n", err)
		return 1
	}

	connectionFile := localstate.ResolveConnectionFilePath(lookupEnvironment)
	if *asJSON {
		return writePrettyJSON(standardOutput, standardError, map[string]any{
			"dryRun":         *dryRun,
			"connectionFile": connectionFile,
			"harnesses":      results,
		})
	}

	table := tabwriter.NewWriter(standardOutput, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "HARNESS\tACTION\tCONFIG")
	configuredCount := 0
	for _, entry := range results {
		if entry.Action == "configured" || entry.Action == "unchanged" || entry.Action == "would-configure" {
			configuredCount++
		}
		fmt.Fprintf(table, "%s\t%s\t%s\n", entry.Label, entry.Action, entry.ConfigPath)
	}
	_ = table.Flush()

	if configuredCount == 0 {
		fmt.Fprintln(standardOutput, "\nNo installed coding harnesses were auto-detected. Pass --harness <antigravity|claude|codex|opencode> to create a config file explicitly.")
	} else {
		if connectionFile != "" {
			fmt.Fprintf(standardOutput, "\nAuto-discovery state file: %s\n", connectionFile)
		}
		fmt.Fprintln(standardOutput, "Harness configs contain no static endpoints or tokens; starting or restarting `orchestra master` updates discovery automatically.")
	}
	return 0
}

func runMCPUninstall(
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra mcp uninstall", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	var harnessTargets repeatedStringValues
	flagSet.Var(&harnessTargets, "harness", "Target harness: all, antigravity, claude, codex, or opencode (repeatable)")
	dryRun := flagSet.Bool("dry-run", false, "Preview configuration removals without modifying files")
	asJSON := flagSet.Bool("json", false, "Format result as JSON")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra mcp uninstall: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}

	results, err := localstate.UninstallHarnessMCPConfigs(localstate.MCPInstallOptions{
		Targets:           harnessTargets,
		DryRun:            *dryRun,
		LookupEnvironment: lookupEnvironment,
		LookPath:          exec.LookPath,
	})
	if err != nil {
		fmt.Fprintf(standardError, "orchestra mcp uninstall: %v\n", err)
		return 1
	}

	if *asJSON {
		return writePrettyJSON(standardOutput, standardError, map[string]any{
			"dryRun":    *dryRun,
			"harnesses": results,
		})
	}

	if len(results) == 0 {
		fmt.Fprintln(standardOutput, "No Orchestra MCP entries were present in local harness configs.")
		return 0
	}

	table := tabwriter.NewWriter(standardOutput, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "HARNESS\tACTION\tCONFIG")
	for _, entry := range results {
		fmt.Fprintf(table, "%s\t%s\t%s\n", entry.Label, entry.Action, entry.ConfigPath)
	}
	_ = table.Flush()
	return 0
}

func runMCPStatus(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra mcp status", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	asJSON := flagSet.Bool("json", false, "Format status report as JSON")

	if err := flagSet.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra mcp status: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}

	statuses, err := localstate.InspectHarnessMCPConfigs(lookupEnvironment, exec.LookPath)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra mcp status: %v\n", err)
		return 1
	}

	connectionFile := localstate.ResolveConnectionFilePath(lookupEnvironment)
	summary := mcpConnectionSummary{
		ConnectionFile: connectionFile,
	}
	if connectionFile != "" {
		if discovered, found, readErr := localstate.ReadConnectionState(connectionFile); readErr == nil && found {
			summary.Discovered = true
			summary.Endpoint = discovered.Endpoint
			summary.AuthMode = discovered.AuthMode
			summary.HasToken = strings.TrimSpace(discovered.Token) != ""
			summary.MasterPID = discovered.ProcessID
			summary.UpdatedAt = discovered.UpdatedAt
			if strings.TrimSpace(discovered.Endpoint) != "" {
				if dialErr := dialMasterEndpoint(operationContext, discovered.Endpoint); dialErr == nil {
					summary.Reachable = true
				} else {
					summary.ReachableError = dialErr.Error()
				}
			}
		}
	}

	report := mcpStatusReport{
		Connection: summary,
		Harnesses:  statuses,
	}
	if *asJSON {
		return writePrettyJSON(standardOutput, standardError, report)
	}

	if summary.ConnectionFile != "" {
		if summary.Discovered {
			reachability := "offline"
			if summary.Reachable {
				reachability = "online"
			}
			authSummary := "token=none"
			if strings.EqualFold(summary.AuthMode, "tailscale") && !summary.HasToken {
				authSummary = "auth=tailscale"
			} else if summary.HasToken {
				authSummary = "token=present"
			}
			fmt.Fprintf(
				standardOutput,
				"Master discovery: %s (endpoint=%s, %s, state=%s, file=%s)\n\n",
				reachability,
				summary.Endpoint,
				authSummary,
				reachability,
				summary.ConnectionFile,
			)
		} else {
			fmt.Fprintf(standardOutput, "Master discovery: not initialized yet (will be written to %s when `orchestra master` starts)\n\n", summary.ConnectionFile)
		}
	}

	table := tabwriter.NewWriter(standardOutput, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "HARNESS\tDETECTED\tMCP CONFIGURED\tINVOCATION\tCONFIG")
	for _, entry := range statuses {
		detectedLabel := "no"
		if entry.Detected {
			detectedLabel = "yes"
		}
		configuredLabel := "no"
		invocation := "-"
		if entry.Configured {
			configuredLabel = "yes"
			invocation = strings.TrimSpace(entry.Command + " " + strings.Join(entry.Arguments, " "))
		}
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n", entry.Label, detectedLabel, configuredLabel, invocation, entry.ConfigPath)
	}
	_ = table.Flush()
	return 0
}
