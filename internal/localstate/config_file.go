package localstate

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

const (
	DefaultProfileName = "default"

	SubcommandMaster   = "master"
	SubcommandRelay    = "relay"
	SubcommandSlave    = "slave"
	SubcommandMCP      = "mcp"
	SubcommandOperator = "operator"
	SubcommandDoctor   = "doctor"
)

var validSubcommands = []string{
	SubcommandMaster,
	SubcommandRelay,
	SubcommandSlave,
	SubcommandMCP,
	SubcommandOperator,
	SubcommandDoctor,
}

type valueKind int

const (
	valueKindString valueKind = iota
	valueKindInt
	valueKindBool
	valueKindStringList
)

type pathExpansionMode int

const (
	pathExpansionNone pathExpansionMode = iota
	pathExpansionStandard
	pathExpansionConnectionFile
	pathExpansionACPAdapter
)

type flagSpecification struct {
	targetFlag           string
	kind                 valueKind
	pathMode             pathExpansionMode
	environmentVariables []string
	cliAliases           []string
	isFallbackAlias      bool
}

type ProfileOptions struct {
	Subcommand        string
	ConfigFlag        string
	ConfigFlagSet     bool
	ProfileFlag       string
	ProfileFlagSet    bool
	LookupEnvironment func(string) (string, bool)
}

type AppliedProfile struct {
	ConfigFilePath string
	Profile        string
	Loaded         bool
	AppliedFlags   []string
}

func IsDisabledConfigFilePath(value string) bool {
	trimmed := strings.TrimSpace(value)
	return strings.EqualFold(trimmed, "none") ||
		strings.EqualFold(trimmed, "off") ||
		strings.EqualFold(trimmed, "false")
}

// ResolveConfigFilePath locates the active Orchestra TOML configuration file.
// When lookupEnvironment omits HOME, USERPROFILE, XDG_CONFIG_HOME, and ORCHESTRA_CONFIG
// (such as isolated unit tests), it returns "" so tests never read the operator's real home directory.
func ResolveConfigFilePath(
	explicitPath string,
	explicitSet bool,
	lookupEnvironment func(string) (string, bool),
) (string, bool, error) {
	useOSFallback := lookupEnvironment == nil
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}
	homeDirectory := resolveHomeDirectory(lookupEnvironment, useOSFallback)

	if explicitSet {
		trimmed := strings.TrimSpace(explicitPath)
		if trimmed == "" || IsDisabledConfigFilePath(trimmed) {
			return "", true, nil
		}
		expanded := expandHomePrefix(trimmed, homeDirectory)
		if err := requireExistingRegularFile(expanded); err != nil {
			return "", true, err
		}
		return expanded, true, nil
	}

	if environmentConfig, found := lookupEnvironment("ORCHESTRA_CONFIG"); found {
		trimmed := strings.TrimSpace(environmentConfig)
		if trimmed == "" || IsDisabledConfigFilePath(trimmed) {
			return "", true, nil
		}
		expanded := expandHomePrefix(trimmed, homeDirectory)
		if err := requireExistingRegularFile(expanded); err != nil {
			return "", true, err
		}
		return expanded, true, nil
	}

	var candidates []string
	if homeDirectory != "" {
		candidates = append(candidates, filepath.Join(homeDirectory, ".orchestra", "config.toml"))
	}
	if xdgConfigHome, found := lookupEnvironment("XDG_CONFIG_HOME"); found && strings.TrimSpace(xdgConfigHome) != "" {
		candidates = append(candidates, filepath.Join(expandHomePrefix(strings.TrimSpace(xdgConfigHome), homeDirectory), "orchestra", "config.toml"))
	} else if homeDirectory != "" {
		candidates = append(candidates, filepath.Join(homeDirectory, ".config", "orchestra", "config.toml"))
	}
	if useOSFallback {
		if userConfigDirectory, err := os.UserConfigDir(); err == nil && strings.TrimSpace(userConfigDirectory) != "" {
			candidate := filepath.Join(strings.TrimSpace(userConfigDirectory), "orchestra", "config.toml")
			if !slices.Contains(candidates, candidate) {
				candidates = append(candidates, candidate)
			}
		}
	}

	for _, candidate := range candidates {
		information, err := os.Stat(candidate)
		if err == nil && !information.IsDir() {
			return candidate, false, nil
		}
	}
	return "", false, nil
}

// DefaultConfigFilePath returns the primary `~/.orchestra/config.toml` path for display and error reporting.
func DefaultConfigFilePath(lookupEnvironment func(string) (string, bool)) string {
	useOSFallback := lookupEnvironment == nil
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}
	homeDirectory := resolveHomeDirectory(lookupEnvironment, useOSFallback)
	if homeDirectory == "" {
		return "~/.orchestra/config.toml"
	}
	return filepath.Join(homeDirectory, ".orchestra", "config.toml")
}

// ApplyProfileToFlagSet loads the active profile from config.toml (when present) and applies
// any settings not already overridden by explicit CLI flags or environment variables.
// Callers must run flagSet.Parse before invoking ApplyProfileToFlagSet.
func ApplyProfileToFlagSet(flagSet *flag.FlagSet, options ProfileOptions) (AppliedProfile, error) {
	if flagSet == nil {
		return AppliedProfile{}, errors.New("flag set is required")
	}
	if !slices.Contains(validSubcommands, options.Subcommand) {
		return AppliedProfile{}, fmt.Errorf("unknown subcommand %q", options.Subcommand)
	}

	lookupEnvironment := options.LookupEnvironment
	useOSFallback := lookupEnvironment == nil
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}
	homeDirectory := resolveHomeDirectory(lookupEnvironment, useOSFallback)

	cliVisited := map[string]bool{}
	flagSet.Visit(func(visited *flag.Flag) {
		cliVisited[visited.Name] = true
	})

	environmentProfile := ""
	if rawProfile, found := lookupEnvironment("ORCHESTRA_PROFILE"); found {
		environmentProfile = strings.TrimSpace(rawProfile)
	}

	requestedProfile := ""
	profileExplicit := false
	switch {
	case options.ProfileFlagSet:
		requestedProfile = strings.TrimSpace(options.ProfileFlag)
		profileExplicit = true
	case environmentProfile != "":
		requestedProfile = environmentProfile
		profileExplicit = true
	}
	if profileExplicit && requestedProfile == "" {
		return AppliedProfile{}, errors.New("profile name cannot be empty")
	}

	configPath, configDisabledOrExplicit, err := ResolveConfigFilePath(options.ConfigFlag, options.ConfigFlagSet, options.LookupEnvironment)
	if err != nil {
		return AppliedProfile{}, err
	}
	if configPath == "" {
		if profileExplicit {
			if configDisabledOrExplicit {
				return AppliedProfile{}, fmt.Errorf("profile %q requested, but config file loading is disabled", requestedProfile)
			}
			return AppliedProfile{}, fmt.Errorf(
				"profile %q requested, but no config file found at %s (create it or pass --config <path>)",
				requestedProfile,
				DefaultConfigFilePath(options.LookupEnvironment),
			)
		}
		return AppliedProfile{Profile: DefaultProfileName}, nil
	}

	parsedFile, err := loadAndValidateConfigFile(configPath)
	if err != nil {
		return AppliedProfile{}, err
	}

	activeProfile := requestedProfile
	if activeProfile == "" {
		if parsedFile.defaultProfile != "" {
			activeProfile = parsedFile.defaultProfile
			profileExplicit = true
		} else {
			activeProfile = DefaultProfileName
		}
	}

	profileData, exists := parsedFile.profiles[activeProfile]
	if !exists {
		if profileExplicit {
			available := sortedProfileNames(parsedFile.profiles)
			if len(available) == 0 {
				return AppliedProfile{}, fmt.Errorf("profile %q not found in %s (no profiles defined)", activeProfile, configPath)
			}
			return AppliedProfile{}, fmt.Errorf(
				"profile %q not found in %s (available profiles: %s)",
				activeProfile,
				configPath,
				strings.Join(available, ", "),
			)
		}
		return AppliedProfile{
			ConfigFilePath: configPath,
			Profile:        activeProfile,
			Loaded:         true,
		}, nil
	}

	resolvedValues, resolvedOrder, err := resolveSubcommandFlags(
		configPath,
		parsedFile,
		activeProfile,
		profileData,
		options.Subcommand,
		homeDirectory,
	)
	if err != nil {
		return AppliedProfile{}, err
	}

	specifications := subcommandSpecifications(options.Subcommand)
	specificationByTarget := map[string]flagSpecification{}
	for _, specification := range specifications {
		if !specification.isFallbackAlias {
			specificationByTarget[specification.targetFlag] = specification
		}
	}

	var appliedFlags []string
	for _, targetFlag := range resolvedOrder {
		values := resolvedValues[targetFlag]
		specification, ok := specificationByTarget[targetFlag]
		if !ok {
			continue
		}
		if flagSet.Lookup(targetFlag) == nil {
			continue
		}
		if isFlagSetOnCLI(cliVisited, targetFlag, specification.cliAliases) {
			continue
		}
		if isFlagSetInEnvironment(lookupEnvironment, specification.environmentVariables) {
			continue
		}
		for _, value := range values {
			if setErr := flagSet.Set(targetFlag, value); setErr != nil {
				return AppliedProfile{}, fmt.Errorf("%s [%s] %s: %w", configPath, activeProfile, targetFlag, setErr)
			}
		}
		appliedFlags = append(appliedFlags, targetFlag)
	}

	return AppliedProfile{
		ConfigFilePath: configPath,
		Profile:        activeProfile,
		Loaded:         true,
		AppliedFlags:   appliedFlags,
	}, nil
}

type parsedConfigFile struct {
	defaultProfile string
	profiles       map[string]parsedProfile
}

type parsedProfile struct {
	shared      map[string]rawSetting
	subcommands map[string]map[string]rawSetting
}

type rawSetting struct {
	originalKey string
	value       any
}

func loadAndValidateConfigFile(configPath string) (parsedConfigFile, error) {
	rawBytes, err := os.ReadFile(configPath)
	if err != nil {
		return parsedConfigFile{}, fmt.Errorf("read config file %s: %w", configPath, err)
	}

	var rawRoot map[string]any
	if _, err := toml.Decode(string(rawBytes), &rawRoot); err != nil {
		return parsedConfigFile{}, fmt.Errorf("parse config file %s: %w", configPath, err)
	}

	result := parsedConfigFile{
		profiles: map[string]parsedProfile{},
	}

	allKnownSharedKeys := buildKnownSharedKeySet()

	parseSingleProfile := func(profileName string, rawTable map[string]any) error {
		normalizedProfileName := strings.TrimSpace(profileName)
		if normalizedProfileName == "" {
			return fmt.Errorf("%s: profile name cannot be empty", configPath)
		}
		if _, duplicate := result.profiles[normalizedProfileName]; duplicate {
			return fmt.Errorf("%s: duplicate profile %q", configPath, normalizedProfileName)
		}
		entry := parsedProfile{
			shared:      map[string]rawSetting{},
			subcommands: map[string]map[string]rawSetting{},
		}
		for key, value := range rawTable {
			canonicalKey := normalizeKeyName(key)
			if slices.Contains(validSubcommands, canonicalKey) {
				subtable, isTable := value.(map[string]any)
				if !isTable {
					if _, isSlice := value.([]map[string]any); isSlice {
						return fmt.Errorf("%s: [%s.%s] must be a table, not [[%s.%s]]", configPath, normalizedProfileName, key, normalizedProfileName, key)
					}
					return fmt.Errorf("%s: [%s.%s] must be a table", configPath, normalizedProfileName, key)
				}
				subcommandSettings, subErr := parseSubcommandTable(configPath, normalizedProfileName, canonicalKey, subtable)
				if subErr != nil {
					return subErr
				}
				if _, exists := entry.subcommands[canonicalKey]; exists {
					return fmt.Errorf("%s: duplicate subcommand table [%s.%s]", configPath, normalizedProfileName, canonicalKey)
				}
				entry.subcommands[canonicalKey] = subcommandSettings
				continue
			}

			if !allKnownSharedKeys[canonicalKey] {
				if _, isTable := value.(map[string]any); isTable {
					return fmt.Errorf(
						"%s: unknown subcommand table [%s.%s] (expected one of %s)",
						configPath,
						normalizedProfileName,
						key,
						strings.Join(validSubcommands, ", "),
					)
				}
				return fmt.Errorf("%s: unknown setting %q in [%s]", configPath, key, normalizedProfileName)
			}
			if previous, duplicate := entry.shared[canonicalKey]; duplicate {
				return fmt.Errorf("%s: duplicate setting %q and %q in [%s]", configPath, previous.originalKey, key, normalizedProfileName)
			}
			entry.shared[canonicalKey] = rawSetting{originalKey: key, value: value}
		}
		result.profiles[normalizedProfileName] = entry
		return nil
	}

	if rawProfilesSection, hasProfilesSection := rawRoot["profiles"]; hasProfilesSection {
		profilesMap, isTable := rawProfilesSection.(map[string]any)
		if !isTable {
			return parsedConfigFile{}, fmt.Errorf("%s: [profiles] must be a table of named profiles", configPath)
		}
		for profileName, rawProfileValue := range profilesMap {
			profileTable, ok := rawProfileValue.(map[string]any)
			if !ok {
				if _, isArrayTable := rawProfileValue.([]map[string]any); isArrayTable {
					return parsedConfigFile{}, fmt.Errorf("%s: profile %q must be a table ([profiles.%s]), not [[profiles.%s]]", configPath, profileName, profileName, profileName)
				}
				return parsedConfigFile{}, fmt.Errorf("%s: [profiles.%s] must be a table", configPath, profileName)
			}
			if err := parseSingleProfile(profileName, profileTable); err != nil {
				return parsedConfigFile{}, err
			}
		}
	}

	for rootKey, rootValue := range rawRoot {
		if rootKey == "profiles" {
			continue
		}
		canonicalRootKey := normalizeKeyName(rootKey)
		if canonicalRootKey == "profile" {
			profileText, isString := rootValue.(string)
			if !isString || strings.TrimSpace(profileText) == "" {
				return parsedConfigFile{}, fmt.Errorf("%s: top-level \"profile\" must be a non-empty string", configPath)
			}
			result.defaultProfile = strings.TrimSpace(profileText)
			continue
		}

		profileTable, isTable := rootValue.(map[string]any)
		if !isTable {
			if _, isArrayTable := rootValue.([]map[string]any); isArrayTable {
				return parsedConfigFile{}, fmt.Errorf("%s: profile %q must be a TOML table ([%s]), not an array of tables ([[%s]])", configPath, rootKey, rootKey, rootKey)
			}
			if allKnownSharedKeys[canonicalRootKey] {
				return parsedConfigFile{}, fmt.Errorf("%s: top-level setting %q must be placed inside [default] or a [<profile>] table", configPath, rootKey)
			}
			return parsedConfigFile{}, fmt.Errorf("%s: unexpected top-level key %q (define a [<profile>] table such as [default])", configPath, rootKey)
		}
		if err := parseSingleProfile(rootKey, profileTable); err != nil {
			return parsedConfigFile{}, err
		}
	}

	return result, nil
}

func parseSubcommandTable(
	configPath string,
	profileName string,
	subcommand string,
	rawTable map[string]any,
) (map[string]rawSetting, error) {
	allowed := subcommandKeyLookup(subcommand)
	settings := make(map[string]rawSetting, len(rawTable))
	for key, value := range rawTable {
		canonicalKey := normalizeKeyName(key)
		if _, ok := allowed[canonicalKey]; !ok {
			return nil, fmt.Errorf("%s: unknown or unsupported %s setting %q in [%s.%s]", configPath, subcommand, key, profileName, subcommand)
		}
		if previous, duplicate := settings[canonicalKey]; duplicate {
			return nil, fmt.Errorf("%s: duplicate setting %q and %q in [%s.%s]", configPath, previous.originalKey, key, profileName, subcommand)
		}
		settings[canonicalKey] = rawSetting{originalKey: key, value: value}
	}
	return settings, nil
}

func resolveSubcommandFlags(
	configPath string,
	file parsedConfigFile,
	profileName string,
	selected parsedProfile,
	subcommand string,
	homeDirectory string,
) (map[string][]string, []string, error) {
	type namedLayer struct {
		sectionLabel string
		entries      map[string]rawSetting
	}

	var layers []namedLayer
	if profileName != DefaultProfileName {
		if defaultProfile, hasDefault := file.profiles[DefaultProfileName]; hasDefault {
			layers = append(layers, namedLayer{
				sectionLabel: DefaultProfileName,
				entries:      defaultProfile.shared,
			})
			if defaultSubcommand, hasSub := defaultProfile.subcommands[subcommand]; hasSub {
				layers = append(layers, namedLayer{
					sectionLabel: DefaultProfileName + "." + subcommand,
					entries:      defaultSubcommand,
				})
			}
		}
	}
	layers = append(layers, namedLayer{
		sectionLabel: profileName,
		entries:      selected.shared,
	})
	if selectedSubcommand, hasSub := selected.subcommands[subcommand]; hasSub {
		layers = append(layers, namedLayer{
			sectionLabel: profileName + "." + subcommand,
			entries:      selectedSubcommand,
		})
	}

	keyMap := subcommandKeyLookup(subcommand)
	resolved := map[string][]string{}
	var order []string

	recordTarget := func(targetFlag string, values []string) {
		if !slices.Contains(order, targetFlag) {
			order = append(order, targetFlag)
		}
		resolved[targetFlag] = values
	}

	for _, layer := range layers {
		if len(layer.entries) == 0 {
			continue
		}
		// Apply fallback aliases (e.g. shared "concurrency" -> master "worker-concurrency")
		// before primary keys in the same table so an explicit primary key in the same table wins.
		for _, fallbackPass := range []bool{true, false} {
			keys := make([]string, 0, len(layer.entries))
			for key := range layer.entries {
				keys = append(keys, key)
			}
			slices.Sort(keys)
			for _, key := range keys {
				specification, supported := keyMap[key]
				if !supported || specification.isFallbackAlias != fallbackPass {
					continue
				}
				// Shared "name" and "host" in [<profile>] describe the worker node, not an MCP mail handle;
				// only [<profile>.mcp] (or explicit "mail-name" / "mail-host") configures MCP mail identity.
				if subcommand == SubcommandMCP && (key == "name" || key == "host") && !strings.HasSuffix(layer.sectionLabel, "."+SubcommandMCP) {
					continue
				}
				setting := layer.entries[key]
				coerced, err := coerceSettingValue(setting.value, specification, homeDirectory)
				if err != nil {
					return nil, nil, fmt.Errorf("%s [%s] %s: %w", configPath, layer.sectionLabel, setting.originalKey, err)
				}
				recordTarget(specification.targetFlag, coerced)
			}
		}
	}

	return resolved, order, nil
}

func coerceSettingValue(raw any, specification flagSpecification, homeDirectory string) ([]string, error) {
	switch specification.kind {
	case valueKindString:
		text, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("expected a string, got %T", raw)
		}
		return []string{expandSettingPath(text, specification.pathMode, homeDirectory)}, nil
	case valueKindInt:
		number, ok := raw.(int64)
		if !ok {
			return nil, fmt.Errorf("expected an integer, got %T", raw)
		}
		return []string{strconv.FormatInt(number, 10)}, nil
	case valueKindBool:
		boolean, ok := raw.(bool)
		if !ok {
			return nil, fmt.Errorf("expected a boolean, got %T", raw)
		}
		return []string{strconv.FormatBool(boolean)}, nil
	case valueKindStringList:
		if single, isString := raw.(string); isString {
			return []string{expandSettingPath(single, specification.pathMode, homeDirectory)}, nil
		}
		rawSlice, isSlice := raw.([]any)
		if !isSlice {
			if stringSlice, ok := raw.([]string); ok {
				expanded := make([]string, 0, len(stringSlice))
				for _, item := range stringSlice {
					expanded = append(expanded, expandSettingPath(item, specification.pathMode, homeDirectory))
				}
				return expanded, nil
			}
			return nil, fmt.Errorf("expected a string or array of strings, got %T", raw)
		}
		expanded := make([]string, 0, len(rawSlice))
		for index, item := range rawSlice {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("array element %d must be a string, got %T", index, item)
			}
			expanded = append(expanded, expandSettingPath(text, specification.pathMode, homeDirectory))
		}
		return expanded, nil
	default:
		return nil, fmt.Errorf("unsupported setting kind %d", specification.kind)
	}
}

func expandSettingPath(rawValue string, mode pathExpansionMode, homeDirectory string) string {
	trimmed := strings.TrimSpace(rawValue)
	switch mode {
	case pathExpansionStandard:
		return expandHomePrefix(trimmed, homeDirectory)
	case pathExpansionConnectionFile:
		if IsDisabledConnectionFilePath(trimmed) {
			return trimmed
		}
		return expandHomePrefix(trimmed, homeDirectory)
	case pathExpansionACPAdapter:
		// Format: <harness>=sha256:<hex>:<path>
		harnessPart, remainder, hasEquals := strings.Cut(trimmed, "=")
		if !hasEquals {
			return trimmed
		}
		const shaPrefix = "sha256:"
		if !strings.HasPrefix(remainder, shaPrefix) {
			return trimmed
		}
		afterPrefix := strings.TrimPrefix(remainder, shaPrefix)
		digest, adapterPath, hasColon := strings.Cut(afterPrefix, ":")
		if !hasColon {
			return trimmed
		}
		return harnessPart + "=" + shaPrefix + digest + ":" + expandHomePrefix(strings.TrimSpace(adapterPath), homeDirectory)
	default:
		return trimmed
	}
}

func subcommandSpecifications(subcommand string) []flagSpecification {
	switch subcommand {
	case SubcommandMaster:
		return []flagSpecification{
			{targetFlag: "listen", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_LISTEN"}},
			{targetFlag: "auth", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_AUTH_MODE"}},
			{targetFlag: "token", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_TOKEN"}},
			{targetFlag: "tailscale-binary", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_TAILSCALE_BINARY"}},
			{targetFlag: "tailscale-allow-user", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_TAILSCALE_ALLOW_USERS"}},
			{targetFlag: "tailscale-allow-tag", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_TAILSCALE_ALLOW_TAGS"}},
			{targetFlag: "data-dir", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_DATA_DIR", "ORCHESTRA_DATA_ROOT"}},
			{targetFlag: "connection-file", kind: valueKindString, pathMode: pathExpansionConnectionFile, environmentVariables: []string{"ORCHESTRA_CONNECTION_FILE"}},
			{targetFlag: "tls-cert", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_TLS_CERT"}},
			{targetFlag: "tls-key", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_TLS_KEY"}},
			{targetFlag: "project-profiles", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_PROJECT_PROFILES"}},
			{targetFlag: "with-local-worker", kind: valueKindBool, environmentVariables: []string{"ORCHESTRA_WITH_LOCAL_WORKER"}},
			{targetFlag: "worker-name", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_NAME"}},
			{targetFlag: "worker-id", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_NODE_ID"}},
			{targetFlag: "worker-concurrency", kind: valueKindInt, environmentVariables: []string{"ORCHESTRA_CONCURRENCY"}},
			{targetFlag: "worker-instance-capacity", kind: valueKindInt, environmentVariables: []string{"ORCHESTRA_INSTANCE_CAPACITY"}},
			{targetFlag: "claude-acp-auth-mode", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_CLAUDE_ACP_AUTH_MODE"}},
			{targetFlag: "workspace-root", kind: valueKindStringList, pathMode: pathExpansionStandard, environmentVariables: []string{"WORKSPACE_ROOTS", "ORCHESTRA_WORKSPACE_ROOTS"}},
			{targetFlag: "worker-label", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_LABELS"}},
			{targetFlag: "approval-policy", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_APPROVAL_POLICY"}},
			{targetFlag: "acp-adapter", kind: valueKindStringList, pathMode: pathExpansionACPAdapter, environmentVariables: []string{"ORCHESTRA_ACP_ADAPTERS"}},
			{targetFlag: "verbose", kind: valueKindBool, environmentVariables: []string{"ORCHESTRA_VERBOSE"}, cliAliases: []string{"log-level"}},
			{targetFlag: "log-level", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_LOG_LEVEL"}},
			{targetFlag: "log-format", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_LOG_FORMAT"}},
		}
	case SubcommandRelay:
		return []flagSpecification{
			{targetFlag: "listen", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_LISTEN"}},
			{targetFlag: "auth", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_AUTH_MODE"}},
			{targetFlag: "token", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_TOKEN"}},
			{targetFlag: "tailscale-binary", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_TAILSCALE_BINARY"}},
			{targetFlag: "tailscale-allow-user", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_TAILSCALE_ALLOW_USERS"}},
			{targetFlag: "tailscale-allow-tag", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_TAILSCALE_ALLOW_TAGS"}},
			{targetFlag: "data-dir", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_DATA_DIR", "ORCHESTRA_DATA_ROOT"}},
			{targetFlag: "connection-file", kind: valueKindString, pathMode: pathExpansionConnectionFile, environmentVariables: []string{"ORCHESTRA_CONNECTION_FILE"}},
			{targetFlag: "tls-cert", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_TLS_CERT"}},
			{targetFlag: "tls-key", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_TLS_KEY"}},
			{targetFlag: "verbose", kind: valueKindBool, environmentVariables: []string{"ORCHESTRA_VERBOSE"}, cliAliases: []string{"log-level"}},
			{targetFlag: "log-level", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_LOG_LEVEL"}},
			{targetFlag: "log-format", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_LOG_FORMAT"}},
		}
	case SubcommandSlave:
		return []flagSpecification{
			{targetFlag: "endpoint", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_ENDPOINT", "CONTROL_ENDPOINT"}, cliAliases: []string{"control-endpoint"}},
			{targetFlag: "master-port", kind: valueKindInt, environmentVariables: []string{"ORCHESTRA_MASTER_PORT"}},
			{targetFlag: "tailscale-binary", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_TAILSCALE_BINARY"}},
			{targetFlag: "connection-file", kind: valueKindString, pathMode: pathExpansionConnectionFile, environmentVariables: []string{"ORCHESTRA_CONNECTION_FILE"}},
			{targetFlag: "name", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_NAME"}},
			{targetFlag: "id", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_NODE_ID"}},
			{targetFlag: "kind", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_KIND"}},
			{targetFlag: "concurrency", kind: valueKindInt, environmentVariables: []string{"ORCHESTRA_CONCURRENCY"}},
			{targetFlag: "instance-capacity", kind: valueKindInt, environmentVariables: []string{"ORCHESTRA_INSTANCE_CAPACITY"}},
			{targetFlag: "memory-megabytes", kind: valueKindInt, environmentVariables: []string{"ORCHESTRA_MEMORY_MEGABYTES"}},
			{targetFlag: "token", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_TOKEN"}},
			{targetFlag: "data-root", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_DATA_ROOT"}},
			{targetFlag: "adapter-manifest", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_ADAPTER_MANIFEST"}},
			{targetFlag: "claude-acp-auth-mode", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_CLAUDE_ACP_AUTH_MODE"}},
			{targetFlag: "tls-ca", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_TLS_CA"}},
			{targetFlag: "tls-insecure", kind: valueKindBool, environmentVariables: []string{"ORCHESTRA_TLS_INSECURE"}},
			{targetFlag: "verbose", kind: valueKindBool, environmentVariables: []string{"ORCHESTRA_VERBOSE"}, cliAliases: []string{"log-level"}},
			{targetFlag: "log-level", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_LOG_LEVEL"}},
			{targetFlag: "log-format", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_LOG_FORMAT"}},
			{targetFlag: "workspace-root", kind: valueKindStringList, pathMode: pathExpansionStandard, environmentVariables: []string{"WORKSPACE_ROOTS", "ORCHESTRA_WORKSPACE_ROOTS"}},
			{targetFlag: "project", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_PROJECT_ALLOWLIST"}},
			{targetFlag: "label", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_LABELS"}},
			{targetFlag: "accelerator", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_ACCELERATORS"}},
			{targetFlag: "toolchain", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_TOOLCHAINS"}},
			{targetFlag: "acp-adapter", kind: valueKindStringList, pathMode: pathExpansionACPAdapter, environmentVariables: []string{"ORCHESTRA_ACP_ADAPTERS"}},
			{targetFlag: "acp-native-fallback", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_ACP_NATIVE_FALLBACK"}},
			{targetFlag: "require-acp", kind: valueKindBool, environmentVariables: []string{"ORCHESTRA_REQUIRE_ACP"}},
			{targetFlag: "disable-self-update", kind: valueKindBool, environmentVariables: []string{"ORCHESTRA_DISABLE_SELF_UPDATE"}},
			{targetFlag: "approval-policy", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_APPROVAL_POLICY"}},
		}
	case SubcommandMCP:
		return []flagSpecification{
			{targetFlag: "endpoint", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_ENDPOINT", "CONTROL_ENDPOINT"}, cliAliases: []string{"control-endpoint"}},
			{targetFlag: "token", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_TOKEN"}},
			{targetFlag: "connection-file", kind: valueKindString, pathMode: pathExpansionConnectionFile, environmentVariables: []string{"ORCHESTRA_CONNECTION_FILE"}},
			{targetFlag: "client-id", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_CLIENT_ID"}},
			{targetFlag: "name", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_MAIL_NAME", "ORCHESTRA_AGENT_NAME"}},
			{targetFlag: "host", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_MAIL_HOST"}},
			{targetFlag: "tls-ca", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_TLS_CA"}},
			{targetFlag: "tls-insecure", kind: valueKindBool, environmentVariables: []string{"ORCHESTRA_TLS_INSECURE"}},
		}
	case SubcommandOperator:
		return []flagSpecification{
			{targetFlag: "endpoint", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_ENDPOINT", "CONTROL_ENDPOINT"}, cliAliases: []string{"control-endpoint"}},
			{targetFlag: "token", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_TOKEN"}},
			{targetFlag: "connection-file", kind: valueKindString, pathMode: pathExpansionConnectionFile, environmentVariables: []string{"ORCHESTRA_CONNECTION_FILE"}},
			{targetFlag: "tls-ca", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_TLS_CA"}},
			{targetFlag: "tls-insecure", kind: valueKindBool, environmentVariables: []string{"ORCHESTRA_TLS_INSECURE"}},
		}
	case SubcommandDoctor:
		return []flagSpecification{
			{targetFlag: "data-root", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_DATA_ROOT"}},
			{targetFlag: "manifest", kind: valueKindString, pathMode: pathExpansionStandard, environmentVariables: []string{"ORCHESTRA_ADAPTER_MANIFEST"}},
			{targetFlag: "endpoint", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_ENDPOINT", "CONTROL_ENDPOINT"}, cliAliases: []string{"control-endpoint"}},
			{targetFlag: "connection-file", kind: valueKindString, pathMode: pathExpansionConnectionFile, environmentVariables: []string{"ORCHESTRA_CONNECTION_FILE"}},
			{targetFlag: "claude-acp-auth-mode", kind: valueKindString, environmentVariables: []string{"ORCHESTRA_CLAUDE_ACP_AUTH_MODE"}},
			{targetFlag: "instance-capacity", kind: valueKindInt, environmentVariables: []string{"ORCHESTRA_INSTANCE_CAPACITY"}},
			{targetFlag: "approval-policy", kind: valueKindStringList, environmentVariables: []string{"ORCHESTRA_APPROVAL_POLICY"}},
		}
	default:
		return nil
	}
}

func subcommandKeyLookup(subcommand string) map[string]flagSpecification {
	lookup := map[string]flagSpecification{}
	for _, specification := range subcommandSpecifications(subcommand) {
		lookup[specification.targetFlag] = specification
	}
	makeAlias := func(targetFlag string) flagSpecification {
		base := lookup[targetFlag]
		base.isFallbackAlias = true
		return base
	}
	switch subcommand {
	case SubcommandMaster:
		lookup["name"] = makeAlias("worker-name")
		lookup["id"] = makeAlias("worker-id")
		lookup["concurrency"] = makeAlias("worker-concurrency")
		lookup["instance-capacity"] = makeAlias("worker-instance-capacity")
		lookup["label"] = makeAlias("worker-label")
	case SubcommandSlave:
		lookup["control-endpoint"] = makeAlias("endpoint")
		lookup["manifest"] = makeAlias("adapter-manifest")
	case SubcommandMCP:
		lookup["control-endpoint"] = makeAlias("endpoint")
		lookup["mail-name"] = makeAlias("name")
		lookup["mail-host"] = makeAlias("host")
	case SubcommandOperator:
		lookup["control-endpoint"] = makeAlias("endpoint")
	case SubcommandDoctor:
		lookup["control-endpoint"] = makeAlias("endpoint")
		lookup["adapter-manifest"] = makeAlias("manifest")
	}
	return lookup
}

func buildKnownSharedKeySet() map[string]bool {
	known := map[string]bool{}
	for _, subcommand := range validSubcommands {
		for key := range subcommandKeyLookup(subcommand) {
			known[key] = true
		}
	}
	return known
}

func normalizeKeyName(rawKey string) string {
	lowered := strings.ToLower(strings.TrimSpace(rawKey))
	return strings.ReplaceAll(lowered, "_", "-")
}

func isFlagSetOnCLI(cliVisited map[string]bool, targetFlag string, aliases []string) bool {
	if cliVisited[targetFlag] {
		return true
	}
	for _, alias := range aliases {
		if cliVisited[alias] {
			return true
		}
	}
	return false
}

func isFlagSetInEnvironment(lookupEnvironment func(string) (string, bool), names []string) bool {
	for _, name := range names {
		value, found := lookupEnvironment(name)
		if !found {
			continue
		}
		if name == "ORCHESTRA_CONNECTION_FILE" || strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func requireExistingRegularFile(filePath string) error {
	information, err := os.Stat(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("config file %s does not exist", filePath)
		}
		return fmt.Errorf("stat config file %s: %w", filePath, err)
	}
	if information.IsDir() {
		return fmt.Errorf("config file %s is a directory", filePath)
	}
	return nil
}

func sortedProfileNames(profiles map[string]parsedProfile) []string {
	names := make([]string, 0, len(profiles))
	for name := range profiles {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}
