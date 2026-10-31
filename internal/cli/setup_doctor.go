package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/localstate"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/setup"
	"github.com/cafecito-games/Orchestra/internal/slave"
)

const (
	doctorDialTimeout       = 3 * time.Second
	applyDownloadTimeout    = 2 * time.Minute
	activationProbeDuration = 30 * time.Second
)

var validSetupSubcommands = []string{"plan", "apply", "activate", "rollback", "prune", "uninstall"}

type repeatedStringValues []string

func (values *repeatedStringValues) String() string {
	return strings.Join(*values, ",")
}

func (values *repeatedStringValues) Set(raw string) error {
	*values = append(*values, raw)
	return nil
}

func parseNameValueFlags(flagName string, values []string) (map[string]string, error) {
	pairs := make(map[string]string, len(values))
	for _, value := range values {
		name, item, found := strings.Cut(value, "=")
		if !found || name == "" || item == "" {
			return nil, fmt.Errorf("%s must be NAME=VALUE", flagName)
		}
		pairs[name] = item
	}
	return pairs, nil
}

func currentPlatformKey() string {
	return runtime.GOOS + "-" + runtime.GOARCH
}

func loadManifestFromFlag(manifestPath string) ([]byte, setup.Manifest, error) {
	if manifestPath == "" {
		manifest, err := setup.LoadDefaultManifest()
		if err != nil {
			return nil, setup.Manifest{}, err
		}
		return setup.DefaultManifestBytes(), manifest, nil
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, setup.Manifest{}, fmt.Errorf("read component manifest: %w", err)
	}
	manifest, err := setup.ParseManifest(manifestBytes)
	if err != nil {
		return nil, setup.Manifest{}, fmt.Errorf("component manifest at %s: %w", manifestPath, err)
	}
	return manifestBytes, manifest, nil
}

func runSetup(
	operationContext context.Context,
	arguments []string,
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
		fmt.Fprintf(targetWriter, "Usage: orchestra setup <%s> [flags]\n", strings.Join(validSetupSubcommands, "|"))
		return exitCode
	}
	switch arguments[0] {
	case "plan":
		return runSetupPlan(arguments[1:], standardOutput, standardError, lookupEnvironment)
	case "apply":
		return runSetupApply(operationContext, arguments[1:], standardOutput, standardError, lookupEnvironment)
	case "activate":
		return runSetupActivate(operationContext, arguments[1:], standardOutput, standardError, lookupEnvironment)
	case "rollback":
		return runSetupRollback(operationContext, arguments[1:], standardOutput, standardError, lookupEnvironment)
	case "prune":
		return runSetupPrune(operationContext, arguments[1:], standardOutput, standardError, lookupEnvironment)
	case "uninstall":
		return runSetupUninstall(arguments[1:], standardOutput, standardError, lookupEnvironment)
	default:
		fmt.Fprintf(
			standardError,
			"orchestra setup: unknown subcommand %q; valid subcommands are %s\n",
			arguments[0],
			strings.Join(validSetupSubcommands, ", "),
		)
		return 2
	}
}

func defaultSetupDataRoot(lookupEnvironment func(string) (string, bool)) string {
	if configured := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_DATA_ROOT"); configured != "" {
		return configured
	}
	return setup.DefaultDataRoot()
}

func runSetupPlan(
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra setup plan", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	dataRoot := flagSet.String("data-root", defaultSetupDataRoot(lookupEnvironment), "Orchestra-owned data root")
	manifestPath := flagSet.String("manifest", firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_ADAPTER_MANIFEST"), "Optional path to component manifest JSON file")
	outputPath := flagSet.String("out", "", "Write plan JSON to this path instead of stdout")
	if err := flagSet.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra setup plan: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}

	manifestBytes, manifest, err := loadManifestFromFlag(*manifestPath)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup plan: %v\n", err)
		return 2
	}
	ledger, err := setup.LoadOwnershipLedger(*dataRoot)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup plan: %v\n", err)
		return 2
	}
	plan, skipped, err := setup.BuildPlan(manifestBytes, manifest, currentPlatformKey(), *dataRoot, ledger)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup plan: %v\n", err)
		return 2
	}
	encodedBytes, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup plan: encode plan: %v\n", err)
		return 2
	}

	destination := standardOutput
	if *outputPath != "" {
		file, createErr := os.Create(*outputPath)
		if createErr != nil {
			fmt.Fprintf(standardError, "orchestra setup plan: create --out file: %v\n", createErr)
			return 2
		}
		defer file.Close()
		destination = file
	}
	if _, err := destination.Write(append(encodedBytes, '\n')); err != nil {
		fmt.Fprintf(standardError, "orchestra setup plan: write plan: %v\n", err)
		return 2
	}
	for _, skippedComponent := range skipped {
		fmt.Fprintf(standardError, "skipped: %s has no platform distribution for %s\n", skippedComponent, currentPlatformKey())
	}
	return 0
}

func runSetupApply(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra setup apply", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	dataRoot := flagSet.String("data-root", defaultSetupDataRoot(lookupEnvironment), "Orchestra-owned data root")
	manifestPath := flagSet.String("manifest", firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_ADAPTER_MANIFEST"), "Optional path to component manifest JSON file")
	planPath := flagSet.String("plan", "", "Path to the plan JSON file produced by `orchestra setup plan`")
	var allowedHosts repeatedStringValues
	var manualArtifacts repeatedStringValues
	var manualChecksums repeatedStringValues
	flagSet.Var(&allowedHosts, "allowed-host", "Download redirect host allowlist entry (repeatable)")
	flagSet.Var(&manualArtifacts, "manual-artifact", "componentID=PATH local artifact for a manual component (repeatable)")
	flagSet.Var(&manualChecksums, "manual-checksum", "componentID=SHA256 operator-asserted checksum for a manual component (repeatable)")
	if err := flagSet.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra setup apply: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}
	if *planPath == "" {
		fmt.Fprintln(standardError, "orchestra setup apply: --plan is required")
		return 2
	}

	parsedArtifacts, err := parseNameValueFlags("--manual-artifact", manualArtifacts)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup apply: %v\n", err)
		return 2
	}
	parsedChecksums, err := parseNameValueFlags("--manual-checksum", manualChecksums)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup apply: %v\n", err)
		return 2
	}

	planBytes, err := os.ReadFile(*planPath)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup apply: read plan: %v\n", err)
		return 1
	}
	var plan setup.Plan
	if err := json.Unmarshal(planBytes, &plan); err != nil {
		fmt.Fprintf(standardError, "orchestra setup apply: parse plan: %v\n", err)
		return 1
	}
	manifestBytes, _, err := loadManifestFromFlag(*manifestPath)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup apply: %v\n", err)
		return 1
	}
	ledger, err := setup.LoadOwnershipLedger(*dataRoot)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup apply: %v\n", err)
		return 1
	}
	if err := os.MkdirAll(*dataRoot, 0o755); err != nil {
		fmt.Fprintf(standardError, "orchestra setup apply: create data root: %v\n", err)
		return 1
	}
	result, err := setup.Apply(operationContext, plan, manifestBytes, ledger, *dataRoot, setup.ApplyOptions{
		HTTPClient:            &http.Client{Timeout: applyDownloadTimeout},
		AllowedHosts:          allowedHosts,
		ManualArtifactSources: parsedArtifacts,
		ManualChecksums:       parsedChecksums,
	})
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup apply: %v\n", err)
		return 1
	}
	for _, operation := range result.Applied {
		fmt.Fprintf(standardOutput, "installed: %s\n", operation.Component)
	}
	for _, operation := range result.Skipped {
		fmt.Fprintf(standardOutput, "already installed: %s\n", operation.Component)
	}
	return 0
}

type componentTargetFlags struct {
	kind    *string
	id      *string
	version *string
}

func registerComponentTargetFlags(flagSet *flag.FlagSet, requireVersion bool, optionalVersion bool) componentTargetFlags {
	kindNames := make([]string, 0, len(setup.ComponentKinds))
	for _, kind := range setup.ComponentKinds {
		kindNames = append(kindNames, string(kind))
	}
	target := componentTargetFlags{
		kind: flagSet.String("kind", "", "Component kind ("+strings.Join(kindNames, " or ")+")"),
		id:   flagSet.String("id", "", "Component ID declared in the component manifest"),
	}
	if requireVersion || optionalVersion {
		target.version = flagSet.String("version", "", "Component version")
	}
	return target
}

func (target componentTargetFlags) toSelector(requireVersion bool) (setup.ComponentSelector, error) {
	selector := setup.ComponentSelector{
		Kind: setup.ComponentKind(*target.kind),
		ID:   *target.id,
	}
	if target.version != nil {
		selector.Version = *target.version
	}
	if err := selector.Validate(); err != nil {
		return setup.ComponentSelector{}, err
	}
	if requireVersion && selector.Version == "" {
		return setup.ComponentSelector{}, errors.New("--version is required")
	}
	return selector, nil
}

func buildActivationContext(
	operationContext context.Context,
	subcommand string,
	dataRoot string,
	manifestPath string,
	standardError io.Writer,
) (setup.ActivationContext, int) {
	if _, err := os.Stat(dataRoot); err != nil {
		fmt.Fprintf(standardError, "orchestra setup %s: data root does not exist; run `orchestra setup apply` first\n", subcommand)
		return setup.ActivationContext{}, 1
	}
	_, manifest, err := loadManifestFromFlag(manifestPath)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup %s: %v\n", subcommand, err)
		return setup.ActivationContext{}, 2
	}
	ownership, err := setup.LoadOwnershipLedger(dataRoot)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup %s: %v\n", subcommand, err)
		return setup.ActivationContext{}, 1
	}
	activation := setup.LoadActivationState(dataRoot)
	if activation.Rejection != nil {
		fmt.Fprintf(standardError, "orchestra setup %s: %v\n", subcommand, activation.Rejection)
		fmt.Fprintf(standardError, "orchestra setup %s: %s\n", subcommand, setup.ActivationRepairGuidance)
		return setup.ActivationContext{}, 1
	}

	nativeBinaries := map[string]string{}
	for _, profile := range harness.Discover(operationContext) {
		if profile.Available {
			nativeBinaries[profile.ID] = profile.Binary
		}
	}

	probe := func(probeContext context.Context, installed setup.InstalledComponent) error {
		switch installed.Reference().Kind {
		case setup.ComponentKindHarness:
			return setup.ProbeHarnessVersion(probeContext, installed)
		case setup.ComponentKindACPAdapter:
			harnessID := installed.Entry.HarnessID
			driver := harness.NewACPDriver(harness.ACPDriverOptions{
				Adapters: map[string]harness.ACPAdapter{
					harnessID: {
						Binary:      installed.Path,
						Arguments:   installed.Entry.Launch.Arguments,
						Environment: installed.Entry.Launch.Environment,
						ID:          installed.Entry.ID,
						Version:     installed.Entry.Version,
						Source:      protocol.ACPAdapterSourceSetupLedger,
						Verify:      installed.Verify,
					},
				},
				NativeBinaries: nativeBinaries,
				ClientVersion:  protocol.EffectiveBuildVersion(),
			})
			timedContext, cancel := context.WithTimeout(probeContext, activationProbeDuration)
			defer cancel()
			if _, probeErr := driver.Probe(timedContext, harnessID); probeErr != nil {
				return fmt.Errorf("%s did not complete its ACP startup handshake", installed.Reference())
			}
			return nil
		default:
			return fmt.Errorf("%s has no activation probe", installed.Reference())
		}
	}

	return setup.ActivationContext{
		DataRoot:   dataRoot,
		Manifest:   manifest,
		Platform:   currentPlatformKey(),
		Ownership:  ownership,
		Activation: activation,
		Probe:      probe,
		InUse:      func(string) bool { return false },
	}, 0
}

func runSetupActivate(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra setup activate", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	dataRoot := flagSet.String("data-root", defaultSetupDataRoot(lookupEnvironment), "Orchestra-owned data root")
	manifestPath := flagSet.String("manifest", firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_ADAPTER_MANIFEST"), "Optional path to component manifest JSON file")
	target := registerComponentTargetFlags(flagSet, true, false)
	if err := flagSet.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra setup activate: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}
	selector, err := target.toSelector(true)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup activate: %v\n", err)
		return 2
	}
	activationContext, code := buildActivationContext(operationContext, "activate", *dataRoot, *manifestPath, standardError)
	if code != 0 {
		return code
	}
	outcome, err := setup.Activate(operationContext, activationContext, selector)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup activate: %v\n", err)
		return 1
	}
	writeActivationOutcome(standardOutput, "activated", outcome)
	return 0
}

func runSetupRollback(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra setup rollback", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	dataRoot := flagSet.String("data-root", defaultSetupDataRoot(lookupEnvironment), "Orchestra-owned data root")
	manifestPath := flagSet.String("manifest", firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_ADAPTER_MANIFEST"), "Optional path to component manifest JSON file")
	target := registerComponentTargetFlags(flagSet, false, false)
	if err := flagSet.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra setup rollback: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}
	selector, err := target.toSelector(false)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup rollback: %v\n", err)
		return 2
	}
	activationContext, code := buildActivationContext(operationContext, "rollback", *dataRoot, *manifestPath, standardError)
	if code != 0 {
		return code
	}
	outcome, err := setup.Rollback(operationContext, activationContext, selector)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup rollback: %v\n", err)
		return 1
	}
	writeActivationOutcome(standardOutput, "rolled back to", outcome)
	return 0
}

func runSetupPrune(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra setup prune", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	dataRoot := flagSet.String("data-root", defaultSetupDataRoot(lookupEnvironment), "Orchestra-owned data root")
	manifestPath := flagSet.String("manifest", firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_ADAPTER_MANIFEST"), "Optional path to component manifest JSON file")
	target := registerComponentTargetFlags(flagSet, false, false)
	if err := flagSet.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra setup prune: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}
	selector, err := target.toSelector(false)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup prune: %v\n", err)
		return 2
	}
	activationContext, code := buildActivationContext(operationContext, "prune", *dataRoot, *manifestPath, standardError)
	if code != 0 {
		return code
	}
	result, _, err := setup.Prune(activationContext, selector)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup prune: %v\n", err)
		return 1
	}
	for _, removed := range result.Removed {
		fmt.Fprintf(standardOutput, "pruned: %s\n", removed.Component)
	}
	for _, retained := range result.Retained {
		fmt.Fprintf(standardOutput, "retained: %s (%s)\n", retained.Record.Component, retained.Reason)
	}
	return 0
}

func runSetupUninstall(
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra setup uninstall", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	dataRoot := flagSet.String("data-root", defaultSetupDataRoot(lookupEnvironment), "Orchestra-owned data root")
	target := registerComponentTargetFlags(flagSet, false, true)
	if err := flagSet.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra setup uninstall: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}
	selector, err := target.toSelector(false)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup uninstall: %v\n", err)
		return 2
	}
	ledger, err := setup.LoadOwnershipLedger(*dataRoot)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup uninstall: %v\n", err)
		return 1
	}
	result, _, err := setup.Uninstall(*dataRoot, ledger, selector)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra setup uninstall: %v\n", err)
		return 1
	}
	for _, removed := range result.Removed {
		fmt.Fprintf(standardOutput, "uninstalled: %s\n", removed.Component)
	}
	for _, retained := range result.Retained {
		fmt.Fprintf(standardOutput, "retained: %s\n", retained.Component)
	}
	return 0
}

func writeActivationOutcome(output io.Writer, verb string, outcome setup.ActivationOutcome) {
	if !outcome.Changed {
		fmt.Fprintf(output, "already active: %s\n", outcome.Active)
	} else {
		fmt.Fprintf(output, "%s: %s\n", verb, outcome.Active)
	}
	if outcome.Previous != nil {
		fmt.Fprintf(output, "rollback target: %s\n", *outcome.Previous)
	} else {
		fmt.Fprintln(output, "rollback target: none")
	}
	fmt.Fprintln(output, "restart any running orchestra slave daemon to adopt the updated component selection")
}

func runDoctor(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	connectionFilePath := localstate.ResolveConnectionFilePath(lookupEnvironment)
	endpointFromEnvironment := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_ENDPOINT", "CONTROL_ENDPOINT")
	defaultEndpoint := endpointFromEnvironment
	if defaultEndpoint == "" {
		defaultEndpoint = slave.DefaultEndpoint
	}
	defaultClaudeAuthMode := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_CLAUDE_ACP_AUTH_MODE")

	flagSet := flag.NewFlagSet("orchestra doctor", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	dataRoot := flagSet.String("data-root", defaultSetupDataRoot(lookupEnvironment), "Orchestra-owned data root")
	manifestPath := flagSet.String("manifest", firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_ADAPTER_MANIFEST"), "Optional path to component manifest JSON file")
	controlEndpoint := flagSet.String("endpoint", defaultEndpoint, "Orchestra master endpoint to test for TCP reachability")
	flagSet.StringVar(controlEndpoint, "control-endpoint", defaultEndpoint, "Orchestra master endpoint to test for TCP reachability")
	claudeACPAuthMode := flagSet.String("claude-acp-auth-mode", defaultClaudeAuthMode, "Claude ACP auth mode policy (local-subscription or api)")
	instanceCapacity := flagSet.Int("instance-capacity", slave.DefaultInstanceCapacity, "Resident instance capacity")
	asJSON := flagSet.Bool("json", false, "Format readiness report as JSON")
	var approvalPolicyValues repeatedStringValues
	flagSet.Var(&approvalPolicyValues, "approval-policy", "Harness approval policy manual|auto|bypass or harness=policy (repeatable)")

	if err := flagSet.Parse(arguments); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra doctor: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}

	explicitFlags := map[string]bool{}
	flagSet.Visit(func(visited *flag.Flag) {
		explicitFlags[visited.Name] = true
	})
	if !explicitFlags["endpoint"] && !explicitFlags["control-endpoint"] && endpointFromEnvironment == "" && connectionFilePath != "" {
		if discovered, found, readErr := localstate.ReadConnectionState(connectionFilePath); readErr == nil && found && strings.TrimSpace(discovered.Endpoint) != "" {
			*controlEndpoint = strings.TrimSpace(discovered.Endpoint)
		}
	}

	rawPolicies := []string(approvalPolicyValues)
	if len(rawPolicies) == 0 {
		if envPolicy := firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_APPROVAL_POLICY"); envPolicy != "" {
			for part := range strings.SplitSeq(envPolicy, ",") {
				if trimmed := strings.TrimSpace(part); trimmed != "" {
					rawPolicies = append(rawPolicies, trimmed)
				}
			}
		}
	}
	approvalPolicies, err := parseDoctorApprovalPolicies(rawPolicies)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra doctor: %v\n", err)
		return 2
	}

	_, manifest, err := loadManifestFromFlag(*manifestPath)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra doctor: %v\n", err)
		return 1
	}
	ledger, err := setup.LoadOwnershipLedger(*dataRoot)
	if err != nil {
		fmt.Fprintf(standardError, "orchestra doctor: %v\n", err)
		return 1
	}
	activation := setup.LoadActivationState(*dataRoot)

	managed := map[string]harness.ManagedHarness{}
	if activation.Rejection == nil {
		for _, entry := range manifest.ComponentsOfKind(setup.ComponentKindHarness) {
			installed, installErr := setup.ActiveInstalledComponent(
				*dataRoot,
				manifest,
				currentPlatformKey(),
				ledger,
				activation,
				entry.Reference().Identity(),
			)
			if installErr == nil {
				managed[entry.HarnessID] = harness.ManagedHarness{
					Binary:  installed.Path,
					Version: installed.Reference().Version,
					Verify:  installed.Verify,
				}
			}
		}
	}
	resolutions := harness.Resolve(operationContext, managed)
	profiles := harness.Profiles(resolutions)

	report := setup.RunDoctor(
		operationContext,
		manifest,
		ledger,
		*dataRoot,
		currentPlatformKey(),
		activation,
		profiles,
		*controlEndpoint,
		dialMasterEndpoint,
	)
	appendClaudeACPAuthModeNote(report.Components, strings.TrimSpace(*claudeACPAuthMode))
	report.ApprovalPolicies = map[string]string{}
	for _, harnessID := range harness.ApprovalPolicyHarnessIDs {
		report.ApprovalPolicies[harnessID] = approvalPolicies.For(harnessID)
	}

	if *asJSON {
		return writePrettyJSON(standardOutput, standardError, report)
	}

	for _, entry := range report.Components {
		harnessState := "not-applicable"
		if entry.HarnessApplicable {
			if entry.HarnessInstalled {
				harnessState = "installed"
			} else {
				harnessState = "missing"
			}
		}
		componentState := "missing"
		if entry.ComponentInstalled {
			componentState = "installed"
		}
		launchState := "not-ready"
		if entry.ACPLaunchReady {
			launchState = "ready"
		}
		fmt.Fprintf(
			standardOutput,
			"%s (%s): harness=%s component=%s auth=%s launch=%s provenance=%s active=%s rollback=%s\n",
			entry.Component,
			entry.HarnessID,
			harnessState,
			componentState,
			entry.AuthReadiness,
			launchState,
			entry.Provenance,
			defaultIfEmpty(entry.ActiveVersion, "none"),
			defaultIfEmpty(entry.RollbackVersion, "none"),
		)
		for _, note := range entry.Notes {
			fmt.Fprintf(standardOutput, "  note: %s\n", note)
		}
	}
	fmt.Fprintf(
		standardOutput,
		"activation ledger: generation=%s accepted=%t\n",
		defaultIfEmpty(report.Activation.Generation, "none"),
		report.Activation.Accepted,
	)
	if !report.Activation.Accepted {
		fmt.Fprintf(standardOutput, "  rejection: %s\n", report.Activation.Rejection)
		fmt.Fprintf(standardOutput, "  repair: %s\n", report.Activation.Repair)
	}
	for _, harnessID := range harness.ApprovalPolicyHarnessIDs {
		policy := report.ApprovalPolicies[harnessID]
		fmt.Fprintf(standardOutput, "approval policy for %s: %s (%s)\n", harnessID, policy, harness.ApprovalPolicyEffect(policy))
	}
	if *instanceCapacity <= 0 {
		fmt.Fprintln(standardOutput, "resident instance hosting: disabled (instance capacity 0)")
	} else {
		fmt.Fprintf(standardOutput, "resident instance capacity: %d\n", *instanceCapacity)
	}
	reachability := "unreachable"
	if report.HubConnectivity.Reachable {
		reachability = "reachable"
	} else if report.HubConnectivity.Detail != "" {
		reachability = "unreachable (" + report.HubConnectivity.Detail + ")"
	}
	fmt.Fprintf(standardOutput, "master %s: %s\n", report.HubConnectivity.Endpoint, reachability)
	return 0
}

func parseDoctorApprovalPolicies(entries []string) (harness.ApprovalPolicies, error) {
	policies := harness.ApprovalPolicies{}
	for _, rawEntry := range entries {
		if strings.Contains(rawEntry, "=") {
			harnessID, policy, found := strings.Cut(rawEntry, "=")
			harnessID = strings.TrimSpace(harnessID)
			policy = strings.TrimSpace(policy)
			if !found || !slices.Contains(harness.ApprovalPolicyHarnessIDs, harnessID) || !slices.Contains(protocol.ApprovalPolicies, policy) {
				return nil, errors.New("approval-policy must be manual|auto|bypass or harness=manual|auto|bypass")
			}
			policies[harnessID] = policy
		} else {
			policy := strings.TrimSpace(rawEntry)
			if !slices.Contains(protocol.ApprovalPolicies, policy) {
				return nil, errors.New("approval-policy must be manual|auto|bypass or harness=manual|auto|bypass")
			}
			for _, harnessID := range harness.ApprovalPolicyHarnessIDs {
				policies[harnessID] = policy
			}
		}
	}
	return policies, nil
}

func dialMasterEndpoint(operationContext context.Context, rawEndpoint string) error {
	hostPort, err := hostPortFromEndpoint(rawEndpoint)
	if err != nil {
		return err
	}
	dialer := &net.Dialer{Timeout: doctorDialTimeout}
	connection, err := dialer.DialContext(operationContext, "tcp", hostPort)
	if err != nil {
		return err
	}
	return connection.Close()
}

func hostPortFromEndpoint(rawEndpoint string) (string, error) {
	value := strings.TrimSpace(rawEndpoint)
	if value == "" {
		return "", errors.New("control endpoint is empty")
	}
	parseable := value
	if !strings.Contains(parseable, "://") {
		parseable = "https://" + parseable
	}
	parsed, err := url.Parse(parseable)
	if err != nil {
		return "", errors.New("control endpoint could not be parsed as a URL")
	}
	hostname := parsed.Hostname()
	if hostname == "" {
		return "", errors.New("control endpoint has no host")
	}
	port := parsed.Port()
	if port == "" {
		switch parsed.Scheme {
		case "http", "ws":
			port = "80"
		default:
			port = "443"
		}
	}
	return net.JoinHostPort(hostname, port), nil
}

func appendClaudeACPAuthModeNote(entries []setup.ComponentDoctorEntry, claudeACPAuthMode string) {
	for index := range entries {
		if entries[index].HarnessID != "claude-cli" || entries[index].Component.Kind != setup.ComponentKindACPAdapter {
			continue
		}
		if resolvedMode, err := harness.ClaudeACPAuthGate(claudeACPAuthMode, os.Environ()); err != nil {
			entries[index].Notes = append(entries[index].Notes, "claude ACP: "+err.Error())
		} else {
			entries[index].Notes = append(entries[index].Notes, "claude ACP: auth mode "+resolvedMode)
		}
	}
}
