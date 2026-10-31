package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cafecito-games/Orchestra/internal/localstate"
	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/slave"
)

const operatorRequestTimeout = 30 * time.Second

type operatorConnectionFlags struct {
	flagSet                 *flag.FlagSet
	connectionFilePath      string
	endpointFromEnvironment string
	tokenFromEnvironment    string
	tlsExplicitEnvironment  bool
	endpoint                *string
	token                   *string
	asJSON                  *bool
	tlsCACert               *string
	tlsInsecure             *bool
}

func registerOperatorFlags(flagSet *flag.FlagSet, lookupEnvironment func(string) (string, bool)) operatorConnectionFlags {
	connectionFilePath := localstate.ResolveConnectionFilePath(lookupEnvironment)
	endpointFromEnvironment := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_ENDPOINT", "CONTROL_ENDPOINT")
	defaultEndpoint := endpointFromEnvironment
	if defaultEndpoint == "" {
		defaultEndpoint = slave.DefaultEndpoint
	}
	tokenFromEnvironment := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_TOKEN")
	defaultToken := tokenFromEnvironment
	defaultTLSCACert := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_TLS_CA")
	defaultTLSInsecure := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_TLS_INSECURE") == "1" ||
		strings.EqualFold(firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_TLS_INSECURE"), "true")

	endpoint := flagSet.String("endpoint", defaultEndpoint, "Orchestra master HTTP or WebSocket endpoint")
	flagSet.StringVar(endpoint, "control-endpoint", defaultEndpoint, "Orchestra master HTTP or WebSocket endpoint")
	token := flagSet.String("token", defaultToken, "Bearer token matching ORCHESTRA_TOKEN")
	asJSON := flagSet.Bool("json", false, "Format output as JSON")
	tlsCACert := flagSet.String("tls-ca", defaultTLSCACert, "CA certificate PEM path for https://")
	tlsInsecure := flagSet.Bool("tls-insecure", defaultTLSInsecure, "Skip TLS certificate verification")

	return operatorConnectionFlags{
		flagSet:                 flagSet,
		connectionFilePath:      connectionFilePath,
		endpointFromEnvironment: endpointFromEnvironment,
		tokenFromEnvironment:    tokenFromEnvironment,
		tlsExplicitEnvironment:  defaultTLSCACert != "" || defaultTLSInsecure,
		endpoint:                endpoint,
		token:                   token,
		asJSON:                  asJSON,
		tlsCACert:               tlsCACert,
		tlsInsecure:             tlsInsecure,
	}
}

func (connection operatorConnectionFlags) resolvedTarget() (string, string, string, bool) {
	explicitFlags := map[string]bool{}
	if connection.flagSet != nil {
		connection.flagSet.Visit(func(visited *flag.Flag) {
			explicitFlags[visited.Name] = true
		})
	}
	endpoint := *connection.endpoint
	token := *connection.token
	tlsCACert := *connection.tlsCACert
	tlsInsecure := *connection.tlsInsecure

	endpointExplicit := explicitFlags["endpoint"] || explicitFlags["control-endpoint"] || connection.endpointFromEnvironment != ""
	tokenExplicit := explicitFlags["token"] || connection.tokenFromEnvironment != ""
	tlsExplicit := explicitFlags["tls-ca"] || explicitFlags["tls-insecure"] || connection.tlsExplicitEnvironment

	if connection.connectionFilePath != "" && (!endpointExplicit || !tokenExplicit || !tlsExplicit) {
		if discovered, found, readErr := localstate.ReadConnectionState(connection.connectionFilePath); readErr == nil && found {
			if !endpointExplicit && strings.TrimSpace(discovered.Endpoint) != "" {
				endpoint = strings.TrimSpace(discovered.Endpoint)
			}
			if !tokenExplicit && (strings.TrimSpace(discovered.Token) != "" || strings.EqualFold(discovered.AuthMode, "tailscale")) {
				token = strings.TrimSpace(discovered.Token)
			}
			if !tlsExplicit {
				if strings.TrimSpace(discovered.TLSCACert) != "" {
					tlsCACert = strings.TrimSpace(discovered.TLSCACert)
				}
				if discovered.TLSInsecure {
					tlsInsecure = true
				}
			}
		}
	}
	return endpoint, token, tlsCACert, tlsInsecure
}

func normalizeFlagOrder(flagSet *flag.FlagSet, arguments []string) []string {
	var flagArguments []string
	var positionalArguments []string
	for index := 0; index < len(arguments); index++ {
		current := arguments[index]
		if current == "--" {
			positionalArguments = append(positionalArguments, arguments[index+1:]...)
			break
		}
		if !strings.HasPrefix(current, "-") || current == "-" {
			positionalArguments = append(positionalArguments, current)
			continue
		}
		flagArguments = append(flagArguments, current)
		trimmed := strings.TrimLeft(current, "-")
		if strings.Contains(trimmed, "=") {
			continue
		}
		defined := flagSet.Lookup(trimmed)
		if defined == nil {
			continue
		}
		if booleanFlag, isBool := defined.Value.(interface{ IsBoolFlag() bool }); isBool && booleanFlag.IsBoolFlag() {
			continue
		}
		if index+1 < len(arguments) {
			index++
			flagArguments = append(flagArguments, arguments[index])
		}
	}
	return append(flagArguments, positionalArguments...)
}

func (connection operatorConnectionFlags) requestJSON(
	operationContext context.Context,
	method string,
	apiPath string,
	query url.Values,
	requestBody any,
	destination any,
) error {
	return connection.requestJSONWithTimeout(
		operationContext,
		method,
		apiPath,
		query,
		requestBody,
		destination,
		operatorRequestTimeout,
	)
}

func (connection operatorConnectionFlags) requestJSONWithTimeout(
	operationContext context.Context,
	method string,
	apiPath string,
	query url.Values,
	requestBody any,
	destination any,
	timeout time.Duration,
) error {
	resolvedEndpoint, resolvedToken, resolvedTLSCACert, resolvedTLSInsecure := connection.resolvedTarget()
	baseURL, err := resolveHTTPBaseURL(resolvedEndpoint)
	if err != nil {
		return err
	}
	targetURL := baseURL + apiPath
	if len(query) > 0 {
		targetURL += "?" + query.Encode()
	}

	var bodyReader io.Reader
	if requestBody != nil {
		encodedBytes, marshalErr := json.Marshal(requestBody)
		if marshalErr != nil {
			return marshalErr
		}
		bodyReader = bytes.NewReader(encodedBytes)
	}

	httpClient, err := protocol.BuildHTTPClient(resolvedTLSCACert, resolvedTLSInsecure, timeout)
	if err != nil {
		return err
	}

	request, err := http.NewRequestWithContext(operationContext, method, targetURL, bodyReader)
	if err != nil {
		return err
	}
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if resolvedToken != "" {
		request.Header.Set("Authorization", "Bearer "+resolvedToken)
	}

	response, err := httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("request %s %s: %w", method, apiPath, err)
	}
	defer response.Body.Close()

	responseBytes, err := io.ReadAll(io.LimitReader(response.Body, 8*1024*1024))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var errorEnvelope struct {
			Error struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if unmarshalErr := json.Unmarshal(responseBytes, &errorEnvelope); unmarshalErr == nil && errorEnvelope.Error.Message != "" {
			return fmt.Errorf("%s (%s)", errorEnvelope.Error.Message, errorEnvelope.Error.Code)
		}
		return fmt.Errorf("HTTP %d from %s", response.StatusCode, apiPath)
	}
	if destination != nil && len(responseBytes) > 0 {
		if err := json.Unmarshal(responseBytes, destination); err != nil {
			return fmt.Errorf("decode response from %s: %w", apiPath, err)
		}
	}
	return nil
}

func resolveHTTPBaseURL(rawEndpoint string) (string, error) {
	trimmed := strings.TrimSpace(rawEndpoint)
	if trimmed == "" {
		return "", errors.New("endpoint is empty")
	}
	if !strings.Contains(trimmed, "://") {
		trimmed = "http://" + trimmed
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.Host == "" {
		return "", errors.New("endpoint is not a valid URL")
	}
	switch parsed.Scheme {
	case "ws":
		parsed.Scheme = "http"
	case "wss":
		parsed.Scheme = "https"
	case "http", "https":
	default:
		return "", fmt.Errorf("unsupported endpoint scheme %q", parsed.Scheme)
	}
	parsed.Path = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/"), nil
}

func runStatus(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra status", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra status: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}

	var status protocol.StatusResponse
	if err := connection.requestJSON(operationContext, http.MethodGet, "/api/status", nil, nil, &status); err != nil {
		fmt.Fprintf(standardError, "orchestra status: %v\n", err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, status)
	}

	fmt.Fprintf(standardOutput, "Orchestra Master %s (protocol v%s)\n", status.Version, status.ProtocolVersion)
	fmt.Fprintf(standardOutput, "Started:           %s\n", status.StartedAt)
	fmt.Fprintf(standardOutput, "Nodes:             %d connected / %d total\n", status.ConnectedNodes, status.TotalNodes)
	fmt.Fprintf(standardOutput, "Open Threads:      %d\n", status.OpenThreads)
	fmt.Fprintf(standardOutput, "Active Instances:  %d\n", status.ActiveInstances)
	fmt.Fprintf(standardOutput, "Active Runs:       %d\n", status.ActiveRuns)
	fmt.Fprintf(standardOutput, "Tasks:             %d running, %d ready\n", status.RunningTasks, status.ReadyTasks)
	fmt.Fprintf(standardOutput, "Pending Approvals: %d\n", status.PendingApprovals)
	if len(status.Nodes) > 0 {
		fmt.Fprintln(standardOutput)
		writer := tabwriter.NewWriter(standardOutput, 0, 0, 2, ' ', 0)
		fmt.Fprintln(writer, "NODE ID\tNAME\tSTATUS\tRUNS\tHARNESSES")
		for _, node := range status.Nodes {
			var harnessNames []string
			for _, profile := range node.Harnesses {
				if profile.Available {
					harnessNames = append(harnessNames, profile.ID)
				}
			}
			fmt.Fprintf(
				writer,
				"%s\t%s\t%s\t%d/%d\t%s\n",
				node.ID,
				node.Name,
				node.Status,
				node.ActiveRuns,
				node.Concurrency,
				defaultIfEmpty(strings.Join(harnessNames, ","), "none"),
			)
		}
		_ = writer.Flush()
	}
	return 0
}

func runNodes(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if len(arguments) > 0 {
		switch arguments[0] {
		case "update":
			return runNodesUpdate(operationContext, arguments[1:], standardOutput, standardError, lookupEnvironment)
		case "refresh":
			return runNodesRefresh(operationContext, arguments[1:], standardOutput, standardError, lookupEnvironment)
		case "clone":
			return runNodesClone(operationContext, arguments[1:], standardOutput, standardError, lookupEnvironment)
		}
	}
	if hasHelpFlag(arguments) {
		printNodesUsage(standardOutput)
		return 0
	}
	flagSet := flag.NewFlagSet("orchestra nodes", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra nodes: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}

	var payload struct {
		Nodes []protocol.NodeDetail `json:"nodes"`
	}
	if err := connection.requestJSON(operationContext, http.MethodGet, "/api/nodes", nil, nil, &payload); err != nil {
		fmt.Fprintf(standardError, "orchestra nodes: %v\n", err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, payload)
	}
	if len(payload.Nodes) == 0 {
		fmt.Fprintln(standardOutput, "No worker nodes registered.")
		return 0
	}

	for index, detail := range payload.Nodes {
		if index > 0 {
			fmt.Fprintln(standardOutput)
		}
		node := detail.Node
		instanceUsage := "disabled"
		if node.InstanceCapacity != nil && *node.InstanceCapacity > 0 {
			activeCount := 0
			if node.ActiveInstances != nil {
				activeCount = *node.ActiveInstances
			}
			instanceUsage = fmt.Sprintf("%d/%d", activeCount, *node.InstanceCapacity)
		}
		fmt.Fprintf(
			standardOutput,
			"%s (%s) — status=%s version=%s connected=%t synced=%t kind=%s platform=%s runs=%d/%d instances=%s\n",
			node.ID,
			node.Name,
			node.Status,
			defaultIfEmpty(node.Version, "unknown"),
			detail.Connected,
			detail.Synced,
			node.Kind,
			node.Platform,
			node.ActiveRuns,
			node.Concurrency,
			instanceUsage,
		)
		if detail.UpdateStatus != nil {
			updateNote := detail.UpdateStatus.Message
			if detail.UpdateStatus.Error != "" {
				updateNote = detail.UpdateStatus.Error
			}
			fmt.Fprintf(
				standardOutput,
				"  Update:          %s (target=%s, previous=%s, detail=%s)\n",
				detail.UpdateStatus.Status,
				detail.UpdateStatus.TargetVersion,
				defaultIfEmpty(detail.UpdateStatus.PreviousVersion, "unknown"),
				defaultIfEmpty(updateNote, "-"),
			)
		}
		fmt.Fprintf(standardOutput, "  Workspace Roots: %s\n", defaultIfEmpty(strings.Join(node.WorkspaceRoots, ", "), "none"))
		if detail.RepositoryReport != nil && len(detail.RepositoryReport.Repositories) > 0 {
			formattedRepositories := make([]string, 0, len(detail.RepositoryReport.Repositories))
			for _, repository := range detail.RepositoryReport.Repositories {
				branchLabel := defaultIfEmpty(repository.HeadBranch, repository.DefaultBranch)
				switch {
				case repository.Repository != "" && branchLabel != "":
					formattedRepositories = append(formattedRepositories, fmt.Sprintf("%s (%s, branch=%s)", repository.RelativePath, repository.Repository, branchLabel))
				case repository.Repository != "":
					formattedRepositories = append(formattedRepositories, fmt.Sprintf("%s (%s)", repository.RelativePath, repository.Repository))
				default:
					formattedRepositories = append(formattedRepositories, repository.RelativePath)
				}
			}
			fmt.Fprintf(standardOutput, "  Repositories:    %s\n", strings.Join(formattedRepositories, "; "))
		} else if detail.RepositoryReport != nil {
			fmt.Fprintln(standardOutput, "  Repositories:    none")
		}
		for _, harnessProfile := range node.Harnesses {
			if !harnessProfile.Available {
				continue
			}
			fmt.Fprintf(
				standardOutput,
				"  Harness %s: transports=%s models=%s auth=%s approval=%s\n",
				harnessProfile.ID,
				defaultIfEmpty(strings.Join(harnessProfile.Transports, ","), "native-cli"),
				defaultIfEmpty(strings.Join(harnessProfile.Models, ","), "default"),
				harnessProfile.AuthMode,
				defaultIfEmpty(harnessProfile.ApprovalPolicy, "manual"),
			)
		}
		if detail.CapabilityReport != nil {
			var labels []string
			for _, evidence := range detail.CapabilityReport.Evidence {
				if evidence.Success && strings.HasPrefix(evidence.CapabilityID, "label:") {
					labels = append(labels, strings.TrimPrefix(evidence.CapabilityID, "label:"))
				}
			}
			if len(labels) > 0 {
				fmt.Fprintf(standardOutput, "  Labels:          %s\n", strings.Join(labels, ", "))
			}
		}
	}
	return 0
}

func printNodesUsage(output io.Writer) {
	fmt.Fprintf(output, `Usage:
  orchestra nodes [flags]
  orchestra nodes <refresh|clone|update> [flags]

Subcommands:
  refresh     Re-scan and refresh discovered Git repositories on connected slave nodes
  clone       Git clone a repository beneath an authorized workspace root on a slave node
  update      Roll out an Orchestra release to connected slave nodes

Flags:
  --endpoint string       Orchestra master HTTP or WebSocket endpoint (env ORCHESTRA_ENDPOINT)
  --token string          Bearer token matching ORCHESTRA_TOKEN (env ORCHESTRA_TOKEN)
  --tls-ca string         CA certificate PEM path for https:// (env ORCHESTRA_TLS_CA)
  --tls-insecure          Skip TLS certificate verification (env ORCHESTRA_TLS_INSECURE)
  --json                  Format output as JSON
`)
}

func runNodesRefresh(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if hasHelpFlag(arguments) {
		fmt.Fprintf(standardOutput, `Usage: orchestra nodes refresh [flags]

Re-scan and refresh discovered Git repositories under workspace roots on connected slave nodes.

Flags:
  --node value            Specific slave node ID to refresh (repeatable; defaults to all connected nodes)
  --endpoint string       Orchestra master HTTP or WebSocket endpoint (env ORCHESTRA_ENDPOINT)
  --token string          Bearer token matching ORCHESTRA_TOKEN (env ORCHESTRA_TOKEN)
  --tls-ca string         CA certificate PEM path for https:// (env ORCHESTRA_TLS_CA)
  --tls-insecure          Skip TLS certificate verification (env ORCHESTRA_TLS_INSECURE)
  --json                  Format output as JSON
`)
		return 0
	}

	flagSet := flag.NewFlagSet("orchestra nodes refresh", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	var selectedNodes cliStringSliceFlag
	flagSet.Var(&selectedNodes, "node", "Specific slave node ID to refresh (repeatable)")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra nodes refresh: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}

	var response orchestrator.RefreshRepositoriesResponse
	requestBody := orchestrator.RefreshRepositoriesInput{NodeIDs: selectedNodes.values}
	if err := connection.requestJSON(operationContext, http.MethodPost, "/api/nodes/repositories/refresh", nil, requestBody, &response); err != nil {
		fmt.Fprintf(standardError, "orchestra nodes refresh: %v\n", err)
		return 1
	}

	exitCode := 0
	for _, nodeResult := range response.Nodes {
		if !nodeResult.Synced {
			exitCode = 1
			break
		}
	}

	if *connection.asJSON {
		if jsonCode := writePrettyJSON(standardOutput, standardError, response); jsonCode != 0 {
			return jsonCode
		}
		return exitCode
	}

	if len(response.Nodes) == 0 {
		fmt.Fprintln(standardOutput, "No connected worker nodes to refresh.")
		return 0
	}
	for _, nodeResult := range response.Nodes {
		if !nodeResult.Synced {
			fmt.Fprintf(standardOutput, "%s: refresh failed (%s)\n", nodeResult.NodeID, defaultIfEmpty(nodeResult.Error, "unknown error"))
			continue
		}
		repositoryCount := 0
		if nodeResult.Report != nil {
			repositoryCount = len(nodeResult.Report.Repositories)
		}
		fmt.Fprintf(standardOutput, "%s: synced %d repositories\n", nodeResult.NodeID, repositoryCount)
		if nodeResult.Report != nil {
			for _, repository := range nodeResult.Report.Repositories {
				fmt.Fprintf(
					standardOutput,
					"  - %s -> %s (%s)\n",
					repository.RelativePath,
					repository.Path,
					defaultIfEmpty(repository.Repository, "local-only"),
				)
			}
		}
	}
	return exitCode
}

func runNodesClone(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if hasHelpFlag(arguments) {
		fmt.Fprintf(standardOutput, `Usage: orchestra nodes clone --node <node-id> --repo <repository-url> [flags]

Instruct a connected slave node to git clone a repository beneath an authorized workspace root.

Flags:
  --node string           Target slave node ID (required)
  --repo string           Git clone URL (https://, ssh://, git://, or git@host:org/repo.git) (required)
  --workspace-root string Optional authorized workspace root on the target node (defaults to primary root)
  --directory string      Optional relative target directory beneath workspace root (defaults to repository name)
  --branch string         Optional branch to check out during clone
  --endpoint string       Orchestra master HTTP or WebSocket endpoint (env ORCHESTRA_ENDPOINT)
  --token string          Bearer token matching ORCHESTRA_TOKEN (env ORCHESTRA_TOKEN)
  --tls-ca string         CA certificate PEM path for https:// (env ORCHESTRA_TLS_CA)
  --tls-insecure          Skip TLS certificate verification (env ORCHESTRA_TLS_INSECURE)
  --json                  Format output as JSON
`)
		return 0
	}

	flagSet := flag.NewFlagSet("orchestra nodes clone", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	nodeID := flagSet.String("node", "", "Target slave node ID")
	repositoryURL := flagSet.String("repo", "", "Git clone URL")
	workspaceRoot := flagSet.String("workspace-root", "", "Authorized workspace root on target node")
	directory := flagSet.String("directory", "", "Relative directory beneath workspace root")
	branch := flagSet.String("branch", "", "Optional branch to check out")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra nodes clone: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}
	if strings.TrimSpace(*nodeID) == "" || strings.TrimSpace(*repositoryURL) == "" {
		fmt.Fprintln(standardError, "orchestra nodes clone: --node and --repo are required")
		return 2
	}

	var response orchestrator.CloneRepositoryResponse
	requestBody := orchestrator.CloneRepositoryInput{
		NodeID:        strings.TrimSpace(*nodeID),
		RepositoryURL: strings.TrimSpace(*repositoryURL),
		WorkspaceRoot: strings.TrimSpace(*workspaceRoot),
		Directory:     strings.TrimSpace(*directory),
		Branch:        strings.TrimSpace(*branch),
	}
	if err := connection.requestJSON(operationContext, http.MethodPost, "/api/nodes/repositories/clone", nil, requestBody, &response); err != nil {
		fmt.Fprintf(standardError, "orchestra nodes clone: %v\n", err)
		return 1
	}

	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, response)
	}

	actionLabel := "already present"
	if response.Created {
		actionLabel = "cloned"
	}
	fmt.Fprintf(
		standardOutput,
		"%s: %s %s at %s (branch=%s)\n",
		response.NodeID,
		actionLabel,
		defaultIfEmpty(response.Repository.Repository, requestBody.RepositoryURL),
		response.Repository.Path,
		defaultIfEmpty(response.Repository.HeadBranch, defaultIfEmpty(response.Repository.DefaultBranch, "HEAD")),
	)
	return 0
}

func runThreads(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra threads", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	statusFilter := flagSet.String("status", "", "Filter threads by status (open or resolved)")
	limit := flagSet.Int("limit", 50, "Maximum number of threads to display")
	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra threads: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}

	query := url.Values{}
	if *statusFilter != "" {
		query.Set("status", *statusFilter)
	}
	if *limit > 0 {
		query.Set("limit", strconv.Itoa(*limit))
	}

	var payload struct {
		Threads []protocol.Thread `json:"threads"`
	}
	if err := connection.requestJSON(operationContext, http.MethodGet, "/api/threads", query, nil, &payload); err != nil {
		fmt.Fprintf(standardError, "orchestra threads: %v\n", err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, payload)
	}
	if len(payload.Threads) == 0 {
		fmt.Fprintln(standardOutput, "No threads found.")
		return 0
	}

	writer := tabwriter.NewWriter(standardOutput, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "THREAD ID\tSTATUS\tUPDATED AT\tTITLE")
	for _, thread := range payload.Threads {
		fmt.Fprintf(writer, "%s\t%s\t%s\t%s\n", thread.ID, thread.Status, thread.UpdatedAt, thread.Title)
	}
	_ = writer.Flush()
	return 0
}

func runThreadDetail(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra thread", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 1 {
		fmt.Fprintln(standardError, "orchestra thread: expected exactly one <thread-id> argument")
		return 2
	}
	threadID := flagSet.Arg(0)

	var detail protocol.ThreadDetailResponse
	if err := connection.requestJSON(operationContext, http.MethodGet, "/api/threads/"+url.PathEscape(threadID), nil, nil, &detail); err != nil {
		fmt.Fprintf(standardError, "orchestra thread: %v\n", err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, detail)
	}

	fmt.Fprintf(standardOutput, "Thread %s [%s] — %s\n", detail.Thread.ID, detail.Thread.Status, detail.Thread.Title)
	if detail.Thread.Summary != "" {
		fmt.Fprintf(standardOutput, "Summary: %s\n", detail.Thread.Summary)
	}
	if detail.Attachment != nil {
		fmt.Fprintf(standardOutput, "Attached Orchestrator: client=%s session=%s\n", detail.Attachment.ClientID, detail.Attachment.SessionID)
	}
	fmt.Fprintf(standardOutput, "Event Journal: head=%d floor=%d\n", detail.EventStream.Head, detail.EventStream.Floor)

	if len(detail.Tasks) > 0 {
		fmt.Fprintln(standardOutput, "\nTasks:")
		writer := tabwriter.NewWriter(standardOutput, 0, 0, 2, ' ', 0)
		fmt.Fprintln(writer, "  TASK ID\tSTATUS\tATTEMPTS\tDEPENDENCIES\tTITLE")
		for _, task := range detail.Tasks {
			var dependencyIDs []string
			for _, dependency := range task.Dependencies {
				dependencyIDs = append(dependencyIDs, dependency.TaskID)
			}
			fmt.Fprintf(
				writer,
				"  %s\t%s\t%d/%d\t%s\t%s\n",
				task.ID,
				task.Status,
				task.AttemptCount,
				task.MaxAttempts,
				defaultIfEmpty(strings.Join(dependencyIDs, ","), "-"),
				task.Title,
			)
		}
		_ = writer.Flush()
	}

	if len(detail.Instances) > 0 {
		fmt.Fprintln(standardOutput, "\nResident Instances:")
		for _, instance := range detail.Instances {
			purposeLabel := "worker"
			if instance.Purpose != nil {
				if instance.Purpose.Name != nil && *instance.Purpose.Name != "" {
					purposeLabel = *instance.Purpose.Name
				} else if instance.Purpose.Title != nil && *instance.Purpose.Title != "" {
					purposeLabel = *instance.Purpose.Title
				}
			}
			activeAllocation := "none"
			for _, allocation := range detail.Allocations {
				if allocation.InstanceID == instance.ID && protocol.IsOccupyingAllocationStatus(allocation.Status) {
					activeAllocation = allocation.ID + "@" + allocation.NodeID
					break
				}
			}
			fmt.Fprintf(
				standardOutput,
				"  %s [%s] purpose=%s autoManaged=%t allocation=%s\n",
				instance.ID,
				instance.Status,
				purposeLabel,
				instance.AutoManaged,
				activeAllocation,
			)
		}
	}

	if len(detail.Runs) > 0 {
		fmt.Fprintln(standardOutput, "\nRuns:")
		for _, runRecord := range detail.Runs {
			taskLabel := "none"
			if runRecord.TaskID != nil {
				taskLabel = *runRecord.TaskID
			}
			fmt.Fprintf(
				standardOutput,
				"  %s [%s] node=%s harness=%s/%s task=%s\n",
				runRecord.ID,
				runRecord.Status,
				runRecord.NodeID,
				runRecord.HarnessID,
				runRecord.Transport,
				taskLabel,
			)
			if runRecord.Output != "" {
				fmt.Fprintf(standardOutput, "    output: %s\n", compactLine(runRecord.Output, 140))
			}
			if runRecord.Error != nil && *runRecord.Error != "" {
				fmt.Fprintf(standardOutput, "    error:  %s\n", compactLine(*runRecord.Error, 140))
			}
		}
	}

	if len(detail.Approvals) > 0 {
		fmt.Fprintln(standardOutput, "\nApprovals:")
		for _, approval := range detail.Approvals {
			fmt.Fprintf(
				standardOutput,
				"  %s [%s] run=%s title=%s\n",
				approval.ID,
				approval.Status,
				approval.RunID,
				compactLine(approval.Title, 100),
			)
		}
	}

	if len(detail.Artifacts) > 0 {
		fmt.Fprintln(standardOutput, "\nArtifacts:")
		for _, artifact := range detail.Artifacts {
			fmt.Fprintf(
				standardOutput,
				"  %s name=%s size=%d uploaded=%t sha256=%s\n",
				artifact.ID,
				artifact.Name,
				artifact.SizeBytes,
				artifact.Uploaded,
				artifact.SHA256,
			)
		}
	}

	if len(detail.Messages) > 0 {
		fmt.Fprintln(standardOutput, "\nMessages:")
		start := 0
		if len(detail.Messages) > 10 {
			start = len(detail.Messages) - 10
		}
		for _, message := range detail.Messages[start:] {
			fmt.Fprintf(
				standardOutput,
				"  [%s] %s -> %s: %s\n",
				message.Kind,
				formatMessageEndpoint(message.Sender),
				formatMessageEndpoint(message.Recipient),
				compactLine(message.Body, 120),
			)
		}
	}

	if len(detail.Transcripts) > 0 {
		fmt.Fprintln(standardOutput, "\nRecent Transcripts:")
		for _, transcript := range detail.Transcripts {
			if len(transcript.Entries) > 0 {
				lastEntry := transcript.Entries[len(transcript.Entries)-1]
				summaryText := lastEntry.Text
				if summaryText == "" {
					summaryText = lastEntry.Title
				}
				fmt.Fprintf(standardOutput, "  run %s (%s): %s\n", transcript.RunID, lastEntry.Kind, compactLine(summaryText, 120))
			}
		}
	}
	return 0
}

func runApprovals(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra approvals", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	threadFilter := flagSet.String("thread", "", "Filter approvals by thread ID")
	statusFilter := flagSet.String("status", "pending", "Filter approvals by status (pending, approved, rejected, expired, cancelled, or all)")
	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra approvals: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}

	query := url.Values{}
	if *threadFilter != "" {
		query.Set("threadId", *threadFilter)
	}
	if *statusFilter != "" && *statusFilter != "all" {
		query.Set("status", *statusFilter)
	}

	var payload struct {
		Approvals []protocol.ApprovalRequest `json:"approvals"`
	}
	if err := connection.requestJSON(operationContext, http.MethodGet, "/api/approvals", query, nil, &payload); err != nil {
		fmt.Fprintf(standardError, "orchestra approvals: %v\n", err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, payload)
	}
	if len(payload.Approvals) == 0 {
		fmt.Fprintln(standardOutput, "No matching approvals.")
		return 0
	}

	for _, approval := range payload.Approvals {
		var optionParts []string
		for _, option := range approval.Options {
			optionParts = append(optionParts, fmt.Sprintf("%s(%s)", option.ID, option.Kind))
		}
		fmt.Fprintf(
			standardOutput,
			"%s [%s] thread=%s run=%s options=%s title=%s\n",
			approval.ID,
			approval.Status,
			approval.ThreadID,
			approval.RunID,
			defaultIfEmpty(strings.Join(optionParts, ","), "default"),
			compactLine(approval.Title, 100),
		)
	}
	return 0
}

func runResolveApproval(
	operationContext context.Context,
	commandName string,
	resolutionStatus string,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra "+commandName, flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	optionID := flagSet.String("option", "", "Specific ACP permission option ID to select")
	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 1 {
		fmt.Fprintf(standardError, "orchestra %s: expected exactly one <approval-id> argument\n", commandName)
		return 2
	}
	approvalID := flagSet.Arg(0)

	var payload struct {
		Approval protocol.ApprovalRequest `json:"approval"`
	}
	err := connection.requestJSON(
		operationContext,
		http.MethodPost,
		"/api/approvals/"+url.PathEscape(approvalID)+"/resolve",
		nil,
		map[string]string{
			"status":   resolutionStatus,
			"optionId": *optionID,
		},
		&payload,
	)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra %s: %v\n", commandName, err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, payload)
	}
	fmt.Fprintf(
		standardOutput,
		"Approval %s %s (option=%s, delivery=%s)\n",
		payload.Approval.ID,
		payload.Approval.Status,
		defaultIfEmpty(payload.Approval.SelectedOptionID, "-"),
		payload.Approval.DeliveryState,
	)
	return 0
}

func runCancel(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra cancel", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 1 {
		fmt.Fprintln(standardError, "orchestra cancel: expected exactly one <run-id|task-id> argument")
		return 2
	}
	targetID := flagSet.Arg(0)

	var result orchestrator.CancelResult
	if err := connection.requestJSON(operationContext, http.MethodPost, "/api/cancel/"+url.PathEscape(targetID), nil, nil, &result); err != nil {
		fmt.Fprintf(standardError, "orchestra cancel: %v\n", err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, result)
	}
	switch result.Kind {
	case "task":
		fmt.Fprintf(standardOutput, "Cancelled task %s (status=%s)\n", result.Task.ID, result.Task.Status)
		if result.Run != nil {
			fmt.Fprintf(standardOutput, "Cancelled associated run %s (status=%s)\n", result.Run.ID, result.Run.Status)
		}
	case "run":
		fmt.Fprintf(standardOutput, "Cancelled run %s (status=%s)\n", result.Run.ID, result.Run.Status)
		if result.Task != nil {
			fmt.Fprintf(standardOutput, "Cancelled associated task %s (status=%s)\n", result.Task.ID, result.Task.Status)
		}
	default:
		fmt.Fprintf(standardOutput, "Cancelled %s\n", targetID)
	}
	return 0
}

func formatMessageEndpoint(endpoint protocol.MessageEndpoint) string {
	switch endpoint.Kind {
	case "orchestrator":
		if endpoint.ClientID != "" {
			return "orchestrator:" + endpoint.ClientID
		}
		return "orchestrator"
	case "task":
		return "task:" + endpoint.TaskID
	default:
		return endpoint.Kind
	}
}

func compactLine(raw string, maximumLength int) string {
	collapsed := strings.Join(strings.Fields(raw), " ")
	if len(collapsed) <= maximumLength {
		return collapsed
	}
	return collapsed[:maximumLength-3] + "..."
}

func defaultIfEmpty(value string, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
