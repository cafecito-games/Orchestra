package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/selfupdate"
)

const (
	defaultNodeUpdateWaitTimeout = 2 * time.Minute
	nodeUpdatePollInterval       = 200 * time.Millisecond
)

type combinedUpdateOutput struct {
	Local selfupdate.LocalUpdateResult     `json:"local"`
	Nodes orchestrator.UpdateNodesResponse `json:"nodes"`
}

func runUpdate(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if hasHelpFlag(arguments) {
		printUpdateUsage(standardOutput)
		return 0
	}

	flagSet := flag.NewFlagSet("orchestra update", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	targetVersion := flagSet.String("version", "latest", "Target Orchestra release version or 'latest'")
	checkOnly := flagSet.Bool("check", false, "Check whether an update is available without modifying the binary")
	force := flagSet.Bool("force", false, "Reinstall the target release even if already running that version")
	executablePath := flagSet.String("executable", "", "Optional path to local Orchestra executable to update (defaults to running binary)")
	includeNodes := flagSet.Bool("nodes", false, "Also trigger self-update on connected slave nodes via the master API")
	var selectedNodes cliStringSliceFlag
	flagSet.Var(&selectedNodes, "node", "Specific slave node ID to update when --nodes is set (repeatable)")
	waitForNodes := flagSet.Bool("wait", false, "Wait for targeted slave nodes to finish updating and reconnect (requires --nodes)")
	waitTimeout := flagSet.Duration("timeout", defaultNodeUpdateWaitTimeout, "Maximum wait duration for --nodes")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra update: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}
	if *checkOnly && *includeNodes {
		fmt.Fprintln(standardError, "orchestra update: --check cannot be combined with --nodes")
		return 2
	}
	if !*includeNodes && (len(selectedNodes.values) > 0 || *waitForNodes) {
		fmt.Fprintln(standardError, "orchestra update: --node and --wait require --nodes (or use 'orchestra nodes update')")
		return 2
	}
	if *waitTimeout <= 0 {
		fmt.Fprintln(standardError, "orchestra update: --timeout must be positive")
		return 2
	}

	releaseClient, err := selfupdate.NewReleaseClient(selfupdate.ReleaseClientOptions{
		LookupEnvironment: lookupEnvironment,
	})
	if err != nil {
		fmt.Fprintf(standardError, "orchestra update: %v\n", err)
		return 1
	}

	localResult, err := selfupdate.UpdateLocalExecutable(operationContext, selfupdate.LocalUpdateOptions{
		ReleaseClient:  releaseClient,
		TargetVersion:  *targetVersion,
		ExecutablePath: *executablePath,
		CheckOnly:      *checkOnly,
		Force:          *force,
	})
	if err != nil {
		fmt.Fprintf(standardError, "orchestra update: %v\n", err)
		return 1
	}

	if !*includeNodes {
		if *connection.asJSON {
			return writePrettyJSON(standardOutput, standardError, localResult)
		}
		writeLocalUpdateSummary(standardOutput, localResult, *checkOnly)
		return 0
	}

	nodesResponse, waitErr := triggerAndMaybeWaitNodeUpdates(
		operationContext,
		connection,
		*targetVersion,
		selectedNodes.values,
		*force,
		*waitForNodes,
		*waitTimeout,
	)
	if waitErr != nil && nodesResponse.TargetVersion == "" {
		fmt.Fprintf(standardError, "orchestra update: %v\n", waitErr)
		return 1
	}

	exitCode := 0
	if waitErr != nil || hasFailedNodeUpdate(nodesResponse.Nodes) {
		exitCode = 1
	}

	if *connection.asJSON {
		if jsonCode := writePrettyJSON(standardOutput, standardError, combinedUpdateOutput{
			Local: localResult,
			Nodes: nodesResponse,
		}); jsonCode != 0 {
			return jsonCode
		}
		if waitErr != nil {
			fmt.Fprintf(standardError, "orchestra update: %v\n", waitErr)
		}
		return exitCode
	}

	writeLocalUpdateSummary(standardOutput, localResult, false)
	fmt.Fprintln(standardOutput)
	writeNodeUpdatesSummary(standardOutput, nodesResponse)
	if waitErr != nil {
		fmt.Fprintf(standardError, "orchestra update: %v\n", waitErr)
	}
	return exitCode
}

func printUpdateUsage(output io.Writer) {
	fmt.Fprintf(output, `Usage: orchestra update [flags]

Check or install an Orchestra release from GitHub Releases for the local binary,
and optionally roll the same release out to connected slave nodes.

Flags:
  --version string        Target Orchestra release version or 'latest' (default "latest")
  --check                 Check whether a newer release is available without modifying the binary
  --force                 Reinstall the target release even if already running that version
  --executable string     Optional path to local Orchestra executable to update
  --nodes                 Also trigger self-update on connected slave nodes via the master API
  --node value            Specific slave node ID to update when --nodes is set (repeatable)
  --wait                  Wait for targeted slave nodes to finish updating and reconnect (with --nodes)
  --timeout duration      Maximum wait duration for --nodes (default 2m0s)
  --endpoint string       Orchestra master HTTP or WebSocket endpoint (env ORCHESTRA_ENDPOINT)
  --token string          Bearer token matching ORCHESTRA_TOKEN (env ORCHESTRA_TOKEN)
  --tls-ca string         CA certificate PEM path for https:// (env ORCHESTRA_TLS_CA)
  --tls-insecure          Skip TLS certificate verification (env ORCHESTRA_TLS_INSECURE)
  --json                  Format output as JSON
`)
}

func runNodesUpdate(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if hasHelpFlag(arguments) {
		printNodesUpdateUsage(standardOutput)
		return 0
	}

	flagSet := flag.NewFlagSet("orchestra nodes update", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	targetVersion := flagSet.String("version", "latest", "Target Orchestra release version or 'latest'")
	var selectedNodes cliStringSliceFlag
	flagSet.Var(&selectedNodes, "node", "Specific slave node ID to update (repeatable)")
	force := flagSet.Bool("force", false, "Reinstall the target release even if a slave already reports that version")
	waitForNodes := flagSet.Bool("wait", false, "Wait until targeted slave nodes finish updating and reconnect")
	waitTimeout := flagSet.Duration("timeout", defaultNodeUpdateWaitTimeout, "Maximum wait duration when --wait is enabled")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *waitTimeout <= 0 {
		fmt.Fprintln(standardError, "orchestra nodes update: --timeout must be positive")
		return 2
	}

	nodeIDs := make([]string, 0, len(selectedNodes.values)+flagSet.NArg())
	nodeIDs = append(nodeIDs, selectedNodes.values...)
	for _, positionalID := range flagSet.Args() {
		trimmed := strings.TrimSpace(positionalID)
		if trimmed != "" {
			nodeIDs = append(nodeIDs, trimmed)
		}
	}

	response, waitErr := triggerAndMaybeWaitNodeUpdates(
		operationContext,
		connection,
		*targetVersion,
		nodeIDs,
		*force,
		*waitForNodes,
		*waitTimeout,
	)
	if waitErr != nil && response.TargetVersion == "" {
		fmt.Fprintf(standardError, "orchestra nodes update: %v\n", waitErr)
		return 1
	}

	exitCode := 0
	if waitErr != nil || hasFailedNodeUpdate(response.Nodes) {
		exitCode = 1
	}

	if *connection.asJSON {
		if jsonCode := writePrettyJSON(standardOutput, standardError, response); jsonCode != 0 {
			return jsonCode
		}
		if waitErr != nil {
			fmt.Fprintf(standardError, "orchestra nodes update: %v\n", waitErr)
		}
		return exitCode
	}

	writeNodeUpdatesSummary(standardOutput, response)
	if waitErr != nil {
		fmt.Fprintf(standardError, "orchestra nodes update: %v\n", waitErr)
	}
	return exitCode
}

func printNodesUpdateUsage(output io.Writer) {
	fmt.Fprintf(output, `Usage: orchestra nodes update [node-id...] [flags]

Instruct connected slave nodes (or specified node IDs) to download, verify,
drain active runs, and self-update in place to a GitHub release version.

Flags:
  --version string        Target Orchestra release version or 'latest' (default "latest")
  --node value            Specific slave node ID to update (repeatable; positional IDs also accepted)
  --force                 Reinstall the target release even if a slave already reports that version
  --wait                  Wait until targeted slave nodes finish updating and reconnect
  --timeout duration      Maximum wait duration when --wait is enabled (default 2m0s)
  --endpoint string       Orchestra master HTTP or WebSocket endpoint (env ORCHESTRA_ENDPOINT)
  --token string          Bearer token matching ORCHESTRA_TOKEN (env ORCHESTRA_TOKEN)
  --tls-ca string         CA certificate PEM path for https:// (env ORCHESTRA_TLS_CA)
  --tls-insecure          Skip TLS certificate verification (env ORCHESTRA_TLS_INSECURE)
  --json                  Format output as JSON
`)
}

func triggerAndMaybeWaitNodeUpdates(
	operationContext context.Context,
	connection operatorConnectionFlags,
	version string,
	nodeIDs []string,
	force bool,
	waitForCompletion bool,
	timeout time.Duration,
) (orchestrator.UpdateNodesResponse, error) {
	requestTimeout := max(operatorRequestTimeout, timeout)
	var response orchestrator.UpdateNodesResponse
	err := connection.requestJSONWithTimeout(
		operationContext,
		http.MethodPost,
		"/api/nodes/update",
		nil,
		orchestrator.UpdateNodesInput{
			Version: version,
			NodeIDs: nodeIDs,
			Force:   force,
		},
		&response,
		requestTimeout,
	)
	if err != nil {
		return orchestrator.UpdateNodesResponse{}, err
	}

	if !waitForCompletion || !hasActiveNodeUpdate(response.Nodes) {
		return response, nil
	}

	waitContext, cancel := context.WithTimeout(operationContext, timeout)
	defer cancel()

	ticker := time.NewTicker(nodeUpdatePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-waitContext.Done():
			var pendingIDs []string
			for _, entry := range response.Nodes {
				if protocol.IsActiveNodeUpdateStatus(entry.Status) {
					pendingIDs = append(pendingIDs, fmt.Sprintf("%s (%s)", entry.NodeID, entry.Status))
				}
			}
			if len(pendingIDs) == 0 {
				return response, waitContext.Err()
			}
			return response, fmt.Errorf("timed out waiting for node update completion: %s", strings.Join(pendingIDs, ", "))
		case <-ticker.C:
			var nodesPayload struct {
				Nodes []protocol.NodeDetail `json:"nodes"`
			}
			pollErr := connection.requestJSON(waitContext, http.MethodGet, "/api/nodes", nil, nil, &nodesPayload)
			if pollErr != nil {
				if waitContext.Err() != nil {
					continue
				}
				continue
			}

			detailsByID := make(map[string]protocol.NodeDetail, len(nodesPayload.Nodes))
			for _, detail := range nodesPayload.Nodes {
				detailsByID[detail.Node.ID] = detail
			}

			stillWaiting := false
			for index, entry := range response.Nodes {
				detail, exists := detailsByID[entry.NodeID]
				if !exists {
					continue
				}
				if detail.UpdateStatus != nil && detail.UpdateStatus.UpdateID == entry.UpdateID {
					response.Nodes[index] = *detail.UpdateStatus
					entry = *detail.UpdateStatus
				}
				if protocol.IsActiveNodeUpdateStatus(entry.Status) {
					stillWaiting = true
					continue
				}
				if entry.Status == "succeeded" && !detail.Synced {
					stillWaiting = true
				}
			}
			if !stillWaiting {
				return response, nil
			}
		}
	}
}

func hasActiveNodeUpdate(entries []protocol.NodeUpdateState) bool {
	for _, entry := range entries {
		if protocol.IsActiveNodeUpdateStatus(entry.Status) {
			return true
		}
	}
	return false
}

func hasFailedNodeUpdate(entries []protocol.NodeUpdateState) bool {
	for _, entry := range entries {
		if entry.Status == "failed" {
			return true
		}
	}
	return false
}

func writeLocalUpdateSummary(output io.Writer, result selfupdate.LocalUpdateResult, checkOnly bool) {
	if checkOnly {
		if result.UpdateAvailable {
			fmt.Fprintf(
				output,
				"Update available: %s -> %s (%s, platform=%s)\n",
				result.CurrentVersion,
				result.TargetVersion,
				result.ReleaseTag,
				result.Platform,
			)
			return
		}
		fmt.Fprintf(
			output,
			"Orchestra %s (%s) is up to date (release %s).\n",
			result.CurrentVersion,
			result.Platform,
			result.ReleaseTag,
		)
		return
	}
	if result.Updated {
		fmt.Fprintf(
			output,
			"Updated Orchestra %s -> %s (%s)\n  Executable: %s\n  SHA-256:    %s\n",
			result.CurrentVersion,
			result.TargetVersion,
			result.ReleaseTag,
			result.ExecutablePath,
			result.SHA256,
		)
		return
	}
	fmt.Fprintf(
		output,
		"Orchestra %s (%s) is already up to date.\n",
		result.CurrentVersion,
		result.Platform,
	)
}

func writeNodeUpdatesSummary(output io.Writer, response orchestrator.UpdateNodesResponse) {
	fmt.Fprintf(output, "Target Release: %s (%s)\n", response.TargetVersion, response.ReleaseTag)
	if len(response.Nodes) == 0 {
		fmt.Fprintln(output, "No connected worker nodes matched the update request.")
		return
	}
	writer := tabwriter.NewWriter(output, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "NODE ID\tSTATUS\tPREVIOUS\tCURRENT\tTARGET\tDETAIL")
	for _, entry := range response.Nodes {
		detailText := entry.Message
		if entry.Error != "" {
			detailText = entry.Error
		}
		fmt.Fprintf(
			writer,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			entry.NodeID,
			entry.Status,
			defaultIfEmpty(entry.PreviousVersion, "unknown"),
			defaultIfEmpty(entry.CurrentVersion, "unknown"),
			entry.TargetVersion,
			defaultIfEmpty(detailText, "-"),
		)
	}
	_ = writer.Flush()
}
