package slave

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/acpadapter"
	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/readiness"
	"github.com/cafecito-games/Orchestra/internal/safepath"
	"github.com/cafecito-games/Orchestra/internal/setup"
	"github.com/cafecito-games/Orchestra/internal/workspace"
)

const (
	DefaultEndpoint         = "http://127.0.0.1:8787"
	DefaultKind             = "local"
	DefaultConcurrency      = 2
	DefaultInstanceCapacity = 4
	DefaultVersion          = "1.0.0"
)

var nonSlugCharacters = regexp.MustCompile(`[^a-z0-9-]+`)

type Configuration struct {
	ControlEndpoint      string
	NodeID               string
	Name                 string
	Kind                 string
	WorkspaceRoots       []string
	ProjectAllowlist     []string
	Labels               []string
	Accelerators         []string
	Toolchains           []readiness.Toolchain
	MemoryMegabytes      int
	Concurrency          int
	InstanceCapacity     int
	Token                string
	DataRoot             string
	AdapterManifest      string
	ACPAdapters          []acpadapter.Override
	ACPNativeFallback    []string
	RequireACP           bool
	ClaudeACPAuthMode    string
	ApprovalPolicies     harness.ApprovalPolicies
	BuiltInACPExecutable string
	HTTPClient           *http.Client
	Version              string
}

type Runtime struct {
	Configuration    Configuration
	Node             protocol.ComputeNode
	Runner           *harness.Runner
	WorkspaceManager *workspace.Manager
	Client           *Client
	CapabilityReport protocol.NodeCapabilityReport
	ComponentReport  protocol.ComponentInventoryReport
	Warnings         []string
}

type repeatFlag struct {
	values []string
	set    bool
}

func (flagValue *repeatFlag) String() string {
	return strings.Join(flagValue.values, ",")
}

func (flagValue *repeatFlag) Set(raw string) error {
	flagValue.set = true
	for part := range strings.SplitSeq(raw, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			flagValue.values = append(flagValue.values, trimmed)
		}
	}
	return nil
}

func ParseConfiguration(arguments []string, lookupEnvironment func(string) (string, bool)) (Configuration, error) {
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}
	hostname, _ := os.Hostname()
	if strings.TrimSpace(hostname) == "" {
		hostname = "orchestra-node"
	}

	defaultEndpoint := firstEnvironment(lookupEnvironment, "ORCHESTRA_ENDPOINT", "CONTROL_ENDPOINT")
	if defaultEndpoint == "" {
		defaultEndpoint = DefaultEndpoint
	}
	defaultName := firstEnvironment(lookupEnvironment, "ORCHESTRA_NAME")
	if defaultName == "" {
		defaultName = hostname
	}
	defaultNodeID := firstEnvironment(lookupEnvironment, "ORCHESTRA_NODE_ID")
	defaultKind := firstEnvironment(lookupEnvironment, "ORCHESTRA_KIND")
	if defaultKind == "" {
		defaultKind = DefaultKind
	}
	defaultToken := firstEnvironment(lookupEnvironment, "ORCHESTRA_TOKEN")
	defaultDataRoot := firstEnvironment(lookupEnvironment, "ORCHESTRA_DATA_ROOT")
	defaultManifest := firstEnvironment(lookupEnvironment, "ORCHESTRA_ADAPTER_MANIFEST")
	defaultClaudeAuthMode := firstEnvironment(lookupEnvironment, "ORCHESTRA_CLAUDE_ACP_AUTH_MODE")
	defaultTLSCACert := firstEnvironment(lookupEnvironment, "ORCHESTRA_TLS_CA")
	defaultTLSInsecure := firstEnvironment(lookupEnvironment, "ORCHESTRA_TLS_INSECURE") == "1" ||
		strings.EqualFold(firstEnvironment(lookupEnvironment, "ORCHESTRA_TLS_INSECURE"), "true")
	defaultRequireACP := false
	if rawRequireACP := firstEnvironment(lookupEnvironment, "ORCHESTRA_REQUIRE_ACP"); rawRequireACP != "" {
		parsedRequireACP, err := parseStrictBool(rawRequireACP)
		if err != nil {
			return Configuration{}, fmt.Errorf("invalid ORCHESTRA_REQUIRE_ACP %q: %w", rawRequireACP, err)
		}
		defaultRequireACP = parsedRequireACP
	}

	defaultConcurrency := DefaultConcurrency
	if raw := firstEnvironment(lookupEnvironment, "ORCHESTRA_CONCURRENCY"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return Configuration{}, fmt.Errorf("invalid ORCHESTRA_CONCURRENCY %q", raw)
		}
		defaultConcurrency = parsed
	}
	defaultInstanceCapacity := DefaultInstanceCapacity
	if raw := firstEnvironment(lookupEnvironment, "ORCHESTRA_INSTANCE_CAPACITY"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return Configuration{}, fmt.Errorf("invalid ORCHESTRA_INSTANCE_CAPACITY %q", raw)
		}
		defaultInstanceCapacity = parsed
	}
	defaultMemoryMegabytes := 0
	if raw := firstEnvironment(lookupEnvironment, "ORCHESTRA_MEMORY_MEGABYTES"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return Configuration{}, fmt.Errorf("invalid ORCHESTRA_MEMORY_MEGABYTES %q", raw)
		}
		defaultMemoryMegabytes = parsed
	}

	var workspaceRootFlag repeatFlag
	var projectFlag repeatFlag
	var labelFlag repeatFlag
	var acceleratorFlag repeatFlag
	var toolchainFlag repeatFlag
	var acpAdapterFlag repeatFlag
	var acpFallbackFlag repeatFlag
	var approvalPolicyFlag repeatFlag

	flagSet := flag.NewFlagSet("orchestra slave", flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)

	var configuration Configuration
	var tlsCACert string
	var tlsInsecure bool
	flagSet.StringVar(&configuration.ControlEndpoint, "control-endpoint", defaultEndpoint, "Orchestrator HTTP or WebSocket base endpoint")
	flagSet.StringVar(&configuration.ControlEndpoint, "endpoint", defaultEndpoint, "Orchestrator HTTP or WebSocket base endpoint")
	flagSet.StringVar(&configuration.Name, "name", defaultName, "Display name for this slave node")
	flagSet.StringVar(&configuration.NodeID, "id", defaultNodeID, "Stable node identifier")
	flagSet.StringVar(&configuration.Kind, "kind", defaultKind, "Node kind (local, home-server, cloud)")
	flagSet.IntVar(&configuration.Concurrency, "concurrency", defaultConcurrency, "Maximum concurrent runs")
	flagSet.IntVar(&configuration.InstanceCapacity, "instance-capacity", defaultInstanceCapacity, "Maximum resident agent instances")
	flagSet.IntVar(&configuration.MemoryMegabytes, "memory-megabytes", defaultMemoryMegabytes, "Configured memory in megabytes")
	flagSet.StringVar(&configuration.Token, "token", defaultToken, "Bearer token matching ORCHESTRA_TOKEN")
	flagSet.StringVar(&configuration.DataRoot, "data-root", defaultDataRoot, "Managed component data root")
	flagSet.StringVar(&configuration.AdapterManifest, "adapter-manifest", defaultManifest, "Optional path to component manifest override")
	flagSet.StringVar(&configuration.ClaudeACPAuthMode, "claude-acp-auth-mode", defaultClaudeAuthMode, "Claude ACP auth mode (local-subscription or api)")
	flagSet.StringVar(&tlsCACert, "tls-ca", defaultTLSCACert, "Optional CA certificate PEM path for wss:// / https://")
	flagSet.BoolVar(&tlsInsecure, "tls-insecure", defaultTLSInsecure, "Skip TLS certificate verification")
	flagSet.Var(&workspaceRootFlag, "workspace-root", "Canonical workspace root (repeatable)")
	flagSet.Var(&projectFlag, "project", "Allowed project profile identifier (repeatable)")
	flagSet.Var(&labelFlag, "label", "Node capability label (repeatable)")
	flagSet.Var(&acceleratorFlag, "accelerator", "Node accelerator identifier (repeatable)")
	flagSet.Var(&toolchainFlag, "toolchain", "Configured toolchain id=version (repeatable)")
	flagSet.Var(&acpAdapterFlag, "acp-adapter", "Pinned ACP adapter override harness=sha256:digest:/abs/path (repeatable)")
	flagSet.Var(&acpFallbackFlag, "acp-native-fallback", "Harness ID allowed to fall back from acp-v1 to native-cli (repeatable)")
	flagSet.BoolVar(&configuration.RequireACP, "require-acp", defaultRequireACP, "Require verified acp-v1 readiness and disable native-cli advertisement")
	flagSet.Var(&approvalPolicyFlag, "approval-policy", "Harness approval policy manual|auto|bypass or harness=policy (repeatable)")

	if err := flagSet.Parse(arguments); err != nil {
		return Configuration{}, err
	}
	if flagSet.NArg() > 0 {
		return Configuration{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flagSet.Args(), " "))
	}

	if configuration.NodeID == "" {
		configuration.NodeID = slugifyIdentifier(configuration.Name)
	}
	if !protocol.IsIdentifier(configuration.NodeID) {
		return Configuration{}, fmt.Errorf("invalid node id %q", configuration.NodeID)
	}
	if !slices.Contains(protocol.NodeKinds, configuration.Kind) {
		return Configuration{}, fmt.Errorf("invalid node kind %q (expected one of %s)", configuration.Kind, strings.Join(protocol.NodeKinds, ", "))
	}
	if configuration.Concurrency <= 0 || configuration.Concurrency > 65535 {
		return Configuration{}, errors.New("concurrency must be between 1 and 65535")
	}
	if configuration.InstanceCapacity < 0 || configuration.InstanceCapacity > 65535 {
		return Configuration{}, errors.New("instance-capacity must be between 0 and 65535")
	}

	rawWorkspaceRoots := workspaceRootFlag.values
	if !workspaceRootFlag.set {
		if raw := firstEnvironment(lookupEnvironment, "WORKSPACE_ROOTS", "ORCHESTRA_WORKSPACE_ROOTS"); raw != "" {
			rawWorkspaceRoots = splitEnvironmentList(raw, string(os.PathListSeparator))
		}
	}
	canonicalRoots, err := canonicalizeWorkspaceRoots(rawWorkspaceRoots)
	if err != nil {
		return Configuration{}, err
	}
	configuration.WorkspaceRoots = canonicalRoots

	rawProjects := chooseList(projectFlag, firstEnvironment(lookupEnvironment, "ORCHESTRA_PROJECT_ALLOWLIST"))
	projects, err := parseKebabIdentifiers("project", rawProjects)
	if err != nil {
		return Configuration{}, err
	}
	configuration.ProjectAllowlist = projects

	rawLabels := chooseList(labelFlag, firstEnvironment(lookupEnvironment, "ORCHESTRA_LABELS"))
	labels, err := parseKebabIdentifiers("label", rawLabels)
	if err != nil {
		return Configuration{}, err
	}
	configuration.Labels = labels

	rawAccelerators := chooseList(acceleratorFlag, firstEnvironment(lookupEnvironment, "ORCHESTRA_ACCELERATORS"))
	accelerators, err := parseKebabIdentifiers("accelerator", rawAccelerators)
	if err != nil {
		return Configuration{}, err
	}
	configuration.Accelerators = accelerators

	rawToolchains := chooseList(toolchainFlag, firstEnvironment(lookupEnvironment, "ORCHESTRA_TOOLCHAINS"))
	toolchains, err := parseConfiguredToolchains(rawToolchains)
	if err != nil {
		return Configuration{}, err
	}
	configuration.Toolchains = toolchains

	rawOverrides := chooseList(acpAdapterFlag, firstEnvironment(lookupEnvironment, "ORCHESTRA_ACP_ADAPTERS"))
	overrides, err := parseACPAdapterOverrides(rawOverrides)
	if err != nil {
		return Configuration{}, err
	}
	configuration.ACPAdapters = overrides

	rawFallbacks := chooseList(acpFallbackFlag, firstEnvironment(lookupEnvironment, "ORCHESTRA_ACP_NATIVE_FALLBACK"))
	fallbacks, err := parseHarnessList(rawFallbacks)
	if err != nil {
		return Configuration{}, err
	}
	configuration.ACPNativeFallback = fallbacks
	if configuration.RequireACP && len(configuration.ACPNativeFallback) > 0 {
		return Configuration{}, errors.New("--require-acp cannot be combined with --acp-native-fallback")
	}

	rawPolicies := chooseList(approvalPolicyFlag, firstEnvironment(lookupEnvironment, "ORCHESTRA_APPROVAL_POLICY"))
	policies, err := parseApprovalPolicies(rawPolicies)
	if err != nil {
		return Configuration{}, err
	}
	configuration.ApprovalPolicies = policies

	resolvedDataRoot, err := resolveDataRootPath(configuration.DataRoot)
	if err != nil {
		return Configuration{}, err
	}
	configuration.DataRoot = resolvedDataRoot
	if tlsCACert != "" || tlsInsecure {
		httpClient, tlsErr := protocol.BuildHTTPClient(tlsCACert, tlsInsecure, 0)
		if tlsErr != nil {
			return Configuration{}, tlsErr
		}
		configuration.HTTPClient = httpClient
	}
	configuration.Version = protocol.BuildVersion
	return configuration, nil
}

func BuildRuntime(operationContext context.Context, configuration Configuration) (*Runtime, error) {
	if configuration.RequireACP && len(configuration.ACPNativeFallback) > 0 {
		return nil, errors.New("--require-acp cannot be combined with --acp-native-fallback")
	}
	if configuration.Version == "" {
		configuration.Version = DefaultVersion
	}
	if configuration.Concurrency <= 0 {
		configuration.Concurrency = DefaultConcurrency
	}
	if configuration.InstanceCapacity <= 0 {
		configuration.InstanceCapacity = max(DefaultInstanceCapacity, configuration.Concurrency)
	}
	if configuration.NodeID == "" {
		configuration.NodeID = slugifyIdentifier(configuration.Name)
	}
	if configuration.Kind == "" {
		configuration.Kind = DefaultKind
	}
	if configuration.DataRoot == "" {
		resolvedRoot, err := resolveDataRootPath("")
		if err != nil {
			return nil, err
		}
		configuration.DataRoot = resolvedRoot
	}

	var manifest setup.Manifest
	var err error
	if configuration.AdapterManifest != "" {
		manifestBytes, readErr := os.ReadFile(configuration.AdapterManifest)
		if readErr != nil {
			return nil, fmt.Errorf("read adapter manifest: %w", readErr)
		}
		manifest, err = setup.ParseManifest(manifestBytes)
	} else {
		manifest, err = setup.LoadDefaultManifest()
	}
	if err != nil {
		return nil, err
	}

	platform := runtime.GOOS + "-" + runtime.GOARCH
	ledger, _ := setup.LoadOwnershipLedger(configuration.DataRoot)
	activation := setup.LoadActivationState(configuration.DataRoot)

	managedHarnesses := map[string]harness.ManagedHarness{}
	for _, entry := range manifest.ComponentsOfKind(setup.ComponentKindHarness) {
		installed, installErr := setup.ActiveInstalledComponent(
			configuration.DataRoot,
			manifest,
			platform,
			ledger,
			activation,
			entry.Reference().Identity(),
		)
		if installErr == nil {
			managedHarnesses[entry.HarnessID] = harness.ManagedHarness{
				Binary:  installed.Path,
				Version: installed.Entry.Version,
				Verify:  installed.Verify,
			}
		}
	}

	resolutions := harness.Resolve(operationContext, managedHarnesses)
	nativeProfiles := harness.Profiles(resolutions)
	verifiedManaged := harness.ManagedHarnesses(resolutions)

	loadedAdapters, err := acpadapter.Load(acpadapter.Options{
		DataRoot:   configuration.DataRoot,
		Manifest:   manifest,
		Platform:   platform,
		Overrides:  configuration.ACPAdapters,
		Activation: activation,
	})
	if err != nil {
		return nil, err
	}

	var warnings []string
	adapters := map[string]harness.ACPAdapter{}
	nativeBinaries := map[string]string{}
	for _, resolution := range resolutions {
		if resolution.Profile.Available && resolution.Profile.Binary != "" {
			nativeBinaries[resolution.HarnessID] = resolution.Profile.Binary
		}
	}
	for harnessID, adapter := range loadedAdapters.Adapters {
		if harnessID == protocol.HarnessClaudeCLI {
			if _, gateErr := harness.ClaudeACPAuthGate(configuration.ClaudeACPAuthMode, os.Environ()); gateErr != nil {
				warnings = append(warnings, gateErr.Error())
				continue
			}
		}
		adapters[harnessID] = adapter
	}
	if _, overridden := adapters[protocol.HarnessOpenCodeCLI]; !overridden {
		for _, resolution := range resolutions {
			if resolution.HarnessID != protocol.HarnessOpenCodeCLI {
				continue
			}
			var managed *harness.ManagedHarness
			if entry, present := verifiedManaged[resolution.HarnessID]; present {
				managed = &entry
			}
			if adapter, available := harness.BuiltInOpenCodeACPAdapter(resolution.Profile, managed); available {
				adapters[resolution.HarnessID] = adapter
			}
			break
		}
	}
	if _, overridden := adapters[protocol.HarnessAntigravityCLI]; !overridden && nativeBinaries[protocol.HarnessAntigravityCLI] != "" {
		if builtInAdapter, available := harness.BuiltInAntigravityACPAdapter(configuration.BuiltInACPExecutable, configuration.DataRoot, configuration.Version); available {
			adapters[protocol.HarnessAntigravityCLI] = builtInAdapter
		}
	}

	var acpDriver *harness.ACPDriver
	if len(adapters) > 0 {
		acpDriver = harness.NewACPDriver(harness.ACPDriverOptions{
			Adapters:       adapters,
			NativeBinaries: nativeBinaries,
			ClientVersion:  configuration.Version,
		})
	}
	probedProfiles, probeFailures := harness.AdvertiseACP(operationContext, nativeProfiles, acpDriver)
	for harnessID, probeErr := range probeFailures {
		warnings = append(warnings, fmt.Sprintf("ACP probe failed for %s: %v", harnessID, probeErr))
	}
	if configuration.ClaudeACPAuthMode == harness.ClaudeACPAuthModeAPI {
		for index := range probedProfiles {
			if probedProfiles[index].ID == protocol.HarnessClaudeCLI {
				probedProfiles[index].AuthMode = "api"
			}
		}
	}
	probedProfiles = harness.AdvertiseApprovalPolicies(probedProfiles, configuration.ApprovalPolicies)
	advertisedProfiles := probedProfiles
	if configuration.RequireACP {
		var requireACPWarnings []string
		advertisedProfiles, requireACPWarnings = enforceRequireACP(probedProfiles)
		warnings = append(warnings, requireACPWarnings...)
	}

	runner := harness.NewRunner(nativeProfiles).
		WithManagedHarnesses(verifiedManaged).
		WithAdvertisedProfiles(advertisedProfiles).
		WithApprovalPolicies(configuration.ApprovalPolicies).
		WithNativeFallback(configuration.ACPNativeFallback...)
	if acpDriver != nil {
		runner.WithACP(acpDriver)
	}

	gitExecutable, _ := workspace.FindGit()
	workspaceManager := workspace.NewManager(configuration.WorkspaceRoots, gitExecutable)

	observedAt := time.Now().UTC().Format(time.RFC3339Nano)
	activeInstances := 0
	instanceCapacity := configuration.InstanceCapacity
	node := protocol.ComputeNode{
		ID:               configuration.NodeID,
		Name:             configuration.Name,
		Kind:             configuration.Kind,
		Platform:         platform,
		Status:           "online",
		LastSeen:         observedAt,
		ActiveRuns:       0,
		Concurrency:      configuration.Concurrency,
		InstanceCapacity: &instanceCapacity,
		ActiveInstances:  &activeInstances,
		WorkspaceRoots:   slices.Clone(configuration.WorkspaceRoots),
		Version:          configuration.Version,
		Harnesses:        advertisedProfiles,
	}

	capabilityReport := readiness.BuildCapabilityReport(operationContext, readiness.NodeSettings{
		NodeID:           configuration.NodeID,
		WorkspaceRoots:   configuration.WorkspaceRoots,
		ProjectAllowlist: configuration.ProjectAllowlist,
		Labels:           configuration.Labels,
		Accelerators:     configuration.Accelerators,
		Toolchains:       configuration.Toolchains,
		MemoryMegabytes:  configuration.MemoryMegabytes,
	}, advertisedProfiles)

	assessments := setup.AssessComponents(
		operationContext,
		manifest,
		ledger,
		configuration.DataRoot,
		platform,
		activation,
		probedProfiles,
		setup.ComponentAssessmentOptions{ACPStartupEvidenceKnown: true},
	)
	componentReport := setup.ComponentInventoryReport(configuration.NodeID, observedAt, assessments)

	client := NewClient(ClientOptions{
		Endpoint:         configuration.ControlEndpoint,
		Token:            configuration.Token,
		Node:             node,
		Runner:           runner,
		WorkspaceManager: workspaceManager,
		CapabilityReport: &capabilityReport,
		ComponentReport:  &componentReport,
		HTTPClient:       configuration.HTTPClient,
	})

	return &Runtime{
		Configuration:    configuration,
		Node:             node,
		Runner:           runner,
		WorkspaceManager: workspaceManager,
		Client:           client,
		CapabilityReport: capabilityReport,
		ComponentReport:  componentReport,
		Warnings:         warnings,
	}, nil
}

func firstEnvironment(lookupEnvironment func(string) (string, bool), names ...string) string {
	for _, name := range names {
		if value, found := lookupEnvironment(name); found && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func parseStrictBool(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true":
		return true, nil
	case "0", "false":
		return false, nil
	default:
		return false, errors.New("expected true, false, 1, or 0")
	}
}

func enforceRequireACP(profiles []protocol.HarnessProfile) ([]protocol.HarnessProfile, []string) {
	filtered := make([]protocol.HarnessProfile, len(profiles))
	var warnings []string
	for index, profile := range profiles {
		updated := profile
		if profile.Available && slices.Contains(profile.Transports, protocol.TransportACP) {
			updated.Transports = []string{protocol.TransportACP}
		} else {
			if profile.Available {
				warnings = append(warnings, fmt.Sprintf("harness %s withheld because --require-acp is enabled and acp-v1 is not ready", profile.ID))
			}
			updated.Available = false
			updated.Transports = []string{}
			updated.ACP = nil
		}
		filtered[index] = updated
	}
	return filtered, warnings
}

func chooseList(flagValue repeatFlag, environmentValue string) []string {
	if flagValue.set {
		return flagValue.values
	}
	if environmentValue == "" {
		return nil
	}
	return splitEnvironmentList(environmentValue, ",")
}

func splitEnvironmentList(raw string, separator string) []string {
	var values []string
	for part := range strings.SplitSeq(raw, separator) {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}

func slugifyIdentifier(raw string) string {
	lower := strings.ToLower(strings.TrimSpace(raw))
	slug := strings.Trim(nonSlugCharacters.ReplaceAllString(lower, "-"), "-")
	if slug == "" {
		return "orchestra-node"
	}
	return slug
}

func parseACPAdapterOverrides(entries []string) ([]acpadapter.Override, error) {
	seen := map[string]bool{}
	overrides := make([]acpadapter.Override, 0, len(entries))
	for _, entry := range entries {
		harnessID, remainder, found := strings.Cut(entry, "=")
		if !found {
			return nil, fmt.Errorf("invalid --acp-adapter %q (expected <harness>=sha256:<digest>:<absolute-path>)", entry)
		}
		if !slices.Contains(protocol.HarnessIDs, harnessID) {
			return nil, fmt.Errorf("unknown harness %q in --acp-adapter", harnessID)
		}
		if seen[harnessID] {
			return nil, fmt.Errorf("duplicate --acp-adapter for %s", harnessID)
		}
		seen[harnessID] = true
		afterPrefix, ok := strings.CutPrefix(remainder, "sha256:")
		if !ok {
			return nil, fmt.Errorf("invalid --acp-adapter %q (missing sha256: prefix)", entry)
		}
		digest, adapterPath, found := strings.Cut(afterPrefix, ":")
		if !found || len(digest) != 64 || !filepath.IsAbs(adapterPath) {
			return nil, fmt.Errorf("invalid --acp-adapter %q (expected sha256:<64-hex>:<absolute-path>)", entry)
		}
		overrides = append(overrides, acpadapter.Override{
			HarnessID: harnessID,
			SHA256:    strings.ToLower(digest),
			Path:      adapterPath,
		})
	}
	return overrides, nil
}

func parseHarnessList(entries []string) ([]string, error) {
	seen := map[string]bool{}
	var result []string
	for _, entry := range entries {
		if !slices.Contains(protocol.HarnessIDs, entry) {
			return nil, fmt.Errorf("unknown harness %q", entry)
		}
		if !seen[entry] {
			seen[entry] = true
			result = append(result, entry)
		}
	}
	return result, nil
}

func parseApprovalPolicies(entries []string) (harness.ApprovalPolicies, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	policies := harness.ApprovalPolicies{}
	for _, entry := range entries {
		if harnessID, policy, hasEquals := strings.Cut(entry, "="); hasEquals {
			if !slices.Contains(harness.ApprovalPolicyHarnessIDs, harnessID) {
				return nil, fmt.Errorf("unknown harness %q in --approval-policy", harnessID)
			}
			if !slices.Contains(protocol.ApprovalPolicies, policy) {
				return nil, fmt.Errorf("unknown approval policy %q", policy)
			}
			policies[harnessID] = policy
			continue
		}
		if !slices.Contains(protocol.ApprovalPolicies, entry) {
			return nil, fmt.Errorf("invalid approval policy %q (expected manual, auto, bypass, or <harness>=<policy>)", entry)
		}
		for _, harnessID := range harness.ApprovalPolicyHarnessIDs {
			if _, exists := policies[harnessID]; !exists {
				policies[harnessID] = entry
			}
		}
	}
	return policies, nil
}

var ErrMissingWorkspaceRoot = errors.New("at least one --workspace-root or WORKSPACE_ROOTS entry is required")

func CanonicalizeWorkspaceRoots(entries []string) ([]string, error) {
	return canonicalizeWorkspaceRoots(entries)
}

func canonicalizeWorkspaceRoots(entries []string) ([]string, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	seen := map[string]bool{}
	roots := make([]string, 0, len(entries))
	for _, entry := range entries {
		trimmed := strings.TrimSpace(entry)
		if trimmed == "" {
			continue
		}
		absolutePath, err := filepath.Abs(trimmed)
		if err != nil {
			return nil, fmt.Errorf("resolve workspace root %q: %w", trimmed, err)
		}
		canonicalPath, err := filepath.EvalSymlinks(absolutePath)
		if err != nil {
			return nil, fmt.Errorf("evaluate workspace root %q: %w", absolutePath, err)
		}
		information, err := os.Stat(canonicalPath)
		if err != nil {
			return nil, fmt.Errorf("stat workspace root %q: %w", canonicalPath, err)
		}
		if !information.IsDir() {
			return nil, fmt.Errorf("workspace root %q is not a directory", canonicalPath)
		}
		if !seen[canonicalPath] {
			seen[canonicalPath] = true
			roots = append(roots, canonicalPath)
		}
	}
	return roots, nil
}

func VerifyWorkspaceWithinRoots(roots []string, targetDirectory string) (string, error) {
	if !filepath.IsAbs(targetDirectory) {
		return "", fmt.Errorf("workspace %q must be an absolute path", targetDirectory)
	}
	cleanTarget := filepath.Clean(targetDirectory)
	for _, root := range roots {
		cleanRoot := filepath.Clean(root)
		if cleanTarget == cleanRoot || strings.HasPrefix(cleanTarget, cleanRoot+string(filepath.Separator)) {
			verified, err := safepath.VerifyDirectoryWithinRoot(cleanRoot, cleanTarget)
			if err != nil {
				return "", err
			}
			return verified, nil
		}
	}
	return "", fmt.Errorf("workspace %s is outside this slave node's allowed roots", cleanTarget)
}

func resolveDataRootPath(configured string) (string, error) {
	target := strings.TrimSpace(configured)
	if target == "" {
		target = setup.DefaultDataRoot()
	}
	return filepath.Abs(target)
}

func parseKebabIdentifiers(kind string, entries []string) ([]string, error) {
	seen := map[string]bool{}
	var result []string
	for _, entry := range entries {
		if len(entry) > protocol.LabelOrAcceleratorMaximumBytes || !protocol.LabelOrAcceleratorPattern.MatchString(entry) {
			return nil, fmt.Errorf("invalid %s %q (expected lowercase kebab-case)", kind, entry)
		}
		if protocol.LooksSecretLike(entry) {
			return nil, fmt.Errorf("%s %q looks secret-like", kind, entry)
		}
		if !seen[entry] {
			seen[entry] = true
			result = append(result, entry)
		}
	}
	return result, nil
}

func parseConfiguredToolchains(entries []string) ([]readiness.Toolchain, error) {
	seen := map[string]bool{}
	var result []readiness.Toolchain
	for _, entry := range entries {
		toolchainID, version, found := strings.Cut(entry, "=")
		if !found || !protocol.LabelOrAcceleratorPattern.MatchString(toolchainID) || !protocol.IsNormalizedVersion(version) {
			return nil, fmt.Errorf("invalid --toolchain %q (expected <kebab-id>=<normalized-version>)", entry)
		}
		if seen[toolchainID] {
			return nil, fmt.Errorf("duplicate --toolchain %q", toolchainID)
		}
		seen[toolchainID] = true
		result = append(result, readiness.Toolchain{ID: toolchainID, Version: version})
	}
	return result, nil
}
