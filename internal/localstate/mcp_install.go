package localstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

const (
	HarnessTargetAntigravity = "antigravity"
	HarnessTargetClaude      = "claude"
	HarnessTargetCodex       = "codex"
	HarnessTargetOpenCode    = "opencode"
)

var SupportedHarnessTargets = []string{
	HarnessTargetAntigravity,
	HarnessTargetClaude,
	HarnessTargetCodex,
	HarnessTargetOpenCode,
}

type HarnessMCPStatus struct {
	Target     string   `json:"target"`
	Label      string   `json:"label"`
	ConfigPath string   `json:"configPath"`
	Detected   bool     `json:"detected"`
	Configured bool     `json:"configured"`
	Command    string   `json:"command,omitempty"`
	Arguments  []string `json:"args,omitempty"`
	Action     string   `json:"action,omitempty"`
}

type MCPInstallOptions struct {
	Targets           []string
	BinaryPath        string
	MailName          string
	DryRun            bool
	LookupEnvironment func(string) (string, bool)
	LookPath          func(string) (string, error)
}

func NormalizeHarnessTarget(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "all", "":
		return "all", nil
	case "antigravity", "agy", "antigravity-cli":
		return HarnessTargetAntigravity, nil
	case "claude", "claude-code", "claude-cli":
		return HarnessTargetClaude, nil
	case "codex", "codex-cli":
		return HarnessTargetCodex, nil
	case "opencode", "opencode-cli":
		return HarnessTargetOpenCode, nil
	default:
		return "", fmt.Errorf("unknown harness %q (expected all, antigravity, claude, codex, or opencode)", raw)
	}
}

func InspectHarnessMCPConfigs(
	lookupEnvironment func(string) (string, bool),
	lookPath func(string) (string, error),
) ([]HarnessMCPStatus, error) {
	descriptors, err := buildHarnessDescriptors(lookupEnvironment, lookPath)
	if err != nil {
		return nil, err
	}
	statuses := make([]HarnessMCPStatus, 0, len(descriptors))
	for _, descriptor := range descriptors {
		configured, command, arguments, inspectErr := descriptor.inspect()
		if inspectErr != nil {
			return nil, fmt.Errorf("%s (%s): %w", descriptor.label, descriptor.configPath, inspectErr)
		}
		statuses = append(statuses, HarnessMCPStatus{
			Target:     descriptor.target,
			Label:      descriptor.label,
			ConfigPath: descriptor.configPath,
			Detected:   descriptor.detected,
			Configured: configured,
			Command:    command,
			Arguments:  arguments,
		})
	}
	return statuses, nil
}

func InstallHarnessMCPConfigs(options MCPInstallOptions) ([]HarnessMCPStatus, error) {
	binaryPath := strings.TrimSpace(options.BinaryPath)
	if binaryPath == "" {
		executablePath, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("resolve orchestra executable path: %w", err)
		}
		absoluteExecutable, absoluteErr := filepath.Abs(executablePath)
		if absoluteErr == nil {
			binaryPath = absoluteExecutable
		} else {
			binaryPath = executablePath
		}
	} else {
		absoluteBinary, absoluteErr := filepath.Abs(binaryPath)
		if absoluteErr == nil {
			binaryPath = absoluteBinary
		}
	}

	bridgeArguments := []string{"mcp"}
	if trimmedName := strings.TrimSpace(options.MailName); trimmedName != "" {
		bridgeArguments = append(bridgeArguments, "--name", trimmedName)
	}

	descriptors, err := buildHarnessDescriptors(options.LookupEnvironment, options.LookPath)
	if err != nil {
		return nil, err
	}

	selectedTargets, explicitSelection, err := resolveSelectedTargets(options.Targets)
	if err != nil {
		return nil, err
	}

	var results []HarnessMCPStatus
	for _, descriptor := range descriptors {
		if explicitSelection {
			if !slices.Contains(selectedTargets, descriptor.target) {
				continue
			}
		} else if !descriptor.detected {
			continue
		}

		changed, applyErr := descriptor.install(binaryPath, bridgeArguments, options.DryRun)
		if applyErr != nil {
			return nil, fmt.Errorf("configure %s (%s): %w", descriptor.label, descriptor.configPath, applyErr)
		}
		action := "unchanged"
		if changed {
			if options.DryRun {
				action = "would-configure"
			} else {
				action = "configured"
			}
		}
		results = append(results, HarnessMCPStatus{
			Target:     descriptor.target,
			Label:      descriptor.label,
			ConfigPath: descriptor.configPath,
			Detected:   descriptor.detected,
			Configured: true,
			Command:    binaryPath,
			Arguments:  slices.Clone(bridgeArguments),
			Action:     action,
		})
	}
	return results, nil
}

func UninstallHarnessMCPConfigs(options MCPInstallOptions) ([]HarnessMCPStatus, error) {
	descriptors, err := buildHarnessDescriptors(options.LookupEnvironment, options.LookPath)
	if err != nil {
		return nil, err
	}

	selectedTargets, explicitSelection, err := resolveSelectedTargets(options.Targets)
	if err != nil {
		return nil, err
	}

	var results []HarnessMCPStatus
	for _, descriptor := range descriptors {
		if explicitSelection && !slices.Contains(selectedTargets, descriptor.target) {
			continue
		}
		configuredBefore, _, _, inspectErr := descriptor.inspect()
		if inspectErr != nil {
			return nil, fmt.Errorf("inspect %s (%s): %w", descriptor.label, descriptor.configPath, inspectErr)
		}
		if !explicitSelection && !configuredBefore && !descriptor.detected {
			continue
		}

		changed, removeErr := descriptor.uninstall(options.DryRun)
		if removeErr != nil {
			return nil, fmt.Errorf("remove from %s (%s): %w", descriptor.label, descriptor.configPath, removeErr)
		}
		action := "not-present"
		if changed {
			if options.DryRun {
				action = "would-remove"
			} else {
				action = "removed"
			}
		}
		results = append(results, HarnessMCPStatus{
			Target:     descriptor.target,
			Label:      descriptor.label,
			ConfigPath: descriptor.configPath,
			Detected:   descriptor.detected,
			Configured: options.DryRun && configuredBefore,
			Action:     action,
		})
	}
	return results, nil
}

func resolveSelectedTargets(rawTargets []string) ([]string, bool, error) {
	if len(rawTargets) == 0 {
		return SupportedHarnessTargets, false, nil
	}
	var resolved []string
	explicit := false
	for _, raw := range rawTargets {
		for item := range strings.SplitSeq(raw, ",") {
			if strings.TrimSpace(item) == "" {
				continue
			}
			normalized, err := NormalizeHarnessTarget(item)
			if err != nil {
				return nil, false, err
			}
			if normalized == "all" {
				return SupportedHarnessTargets, false, nil
			}
			explicit = true
			if !slices.Contains(resolved, normalized) {
				resolved = append(resolved, normalized)
			}
		}
	}
	if len(resolved) == 0 {
		return SupportedHarnessTargets, false, nil
	}
	return resolved, explicit, nil
}

type harnessDescriptor struct {
	target     string
	label      string
	configPath string
	detected   bool
	inspect    func() (bool, string, []string, error)
	install    func(binaryPath string, arguments []string, dryRun bool) (bool, error)
	uninstall  func(dryRun bool) (bool, error)
}

func buildHarnessDescriptors(
	lookupEnvironment func(string) (string, bool),
	lookPath func(string) (string, error),
) ([]harnessDescriptor, error) {
	useOSFallback := lookupEnvironment == nil
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}
	if lookPath == nil {
		lookPath = exec.LookPath
	}

	homeDirectory := resolveHomeDirectory(lookupEnvironment, useOSFallback)
	if homeDirectory == "" {
		return nil, errors.New("cannot resolve user home directory for harness configuration")
	}

	xdgConfigHome, hasXDGConfig := lookupEnvironment("XDG_CONFIG_HOME")
	openCodeDirectory := filepath.Join(homeDirectory, ".config", "opencode")
	if hasXDGConfig && strings.TrimSpace(xdgConfigHome) != "" {
		openCodeDirectory = filepath.Join(expandHomePrefix(strings.TrimSpace(xdgConfigHome), homeDirectory), "opencode")
	}

	antigravityPath := filepath.Join(homeDirectory, ".gemini", "config", "mcp_config.json")
	claudePath := filepath.Join(homeDirectory, ".claude.json")
	codexPath := filepath.Join(homeDirectory, ".codex", "config.toml")
	openCodePath := filepath.Join(openCodeDirectory, "opencode.json")

	hasExecutable := func(binaryName string) bool {
		_, err := lookPath(binaryName)
		return err == nil
	}
	pathExists := func(targetPath string) bool {
		_, err := os.Stat(targetPath)
		return err == nil
	}

	return []harnessDescriptor{
		{
			target:     HarnessTargetAntigravity,
			label:      "Antigravity",
			configPath: antigravityPath,
			detected:   pathExists(filepath.Join(homeDirectory, ".gemini")) || hasExecutable("agy"),
			inspect:    func() (bool, string, []string, error) { return inspectStandardJSONMCP(antigravityPath) },
			install: func(binaryPath string, arguments []string, dryRun bool) (bool, error) {
				return installStandardJSONMCP(antigravityPath, binaryPath, arguments, dryRun)
			},
			uninstall: func(dryRun bool) (bool, error) {
				return uninstallStandardJSONMCP(antigravityPath, dryRun)
			},
		},
		{
			target:     HarnessTargetClaude,
			label:      "Claude Code",
			configPath: claudePath,
			detected:   pathExists(claudePath) || pathExists(filepath.Join(homeDirectory, ".claude")) || hasExecutable("claude"),
			inspect:    func() (bool, string, []string, error) { return inspectStandardJSONMCP(claudePath) },
			install: func(binaryPath string, arguments []string, dryRun bool) (bool, error) {
				return installStandardJSONMCP(claudePath, binaryPath, arguments, dryRun)
			},
			uninstall: func(dryRun bool) (bool, error) {
				return uninstallStandardJSONMCP(claudePath, dryRun)
			},
		},
		{
			target:     HarnessTargetCodex,
			label:      "Codex",
			configPath: codexPath,
			detected:   pathExists(filepath.Join(homeDirectory, ".codex")) || hasExecutable("codex"),
			inspect:    func() (bool, string, []string, error) { return inspectCodexTOMLMCP(codexPath) },
			install: func(binaryPath string, arguments []string, dryRun bool) (bool, error) {
				return installCodexTOMLMCP(codexPath, binaryPath, arguments, dryRun)
			},
			uninstall: func(dryRun bool) (bool, error) {
				return uninstallCodexTOMLMCP(codexPath, dryRun)
			},
		},
		{
			target:     HarnessTargetOpenCode,
			label:      "OpenCode",
			configPath: openCodePath,
			detected:   pathExists(openCodeDirectory) || hasExecutable("opencode"),
			inspect:    func() (bool, string, []string, error) { return inspectOpenCodeJSONMCP(openCodePath) },
			install: func(binaryPath string, arguments []string, dryRun bool) (bool, error) {
				return installOpenCodeJSONMCP(openCodePath, binaryPath, arguments, dryRun)
			},
			uninstall: func(dryRun bool) (bool, error) {
				return uninstallOpenCodeJSONMCP(openCodePath, dryRun)
			},
		},
	}, nil
}

func inspectStandardJSONMCP(configPath string) (bool, string, []string, error) {
	document, exists, _, err := readJSONObjectFile(configPath)
	if err != nil || !exists {
		return false, "", nil, err
	}
	servers, ok := document["mcpServers"].(map[string]any)
	if !ok {
		return false, "", nil, nil
	}
	rawEntry, exists := servers["orchestra"]
	if !exists {
		return false, "", nil, nil
	}
	entry, ok := rawEntry.(map[string]any)
	if !ok {
		return true, "", nil, nil
	}
	command, _ := entry["command"].(string)
	arguments := extractStringSlice(entry["args"])
	return true, command, arguments, nil
}

func installStandardJSONMCP(configPath string, binaryPath string, arguments []string, dryRun bool) (bool, error) {
	document, exists, originalBytes, err := readJSONObjectFile(configPath)
	if err != nil {
		return false, err
	}
	if !exists {
		document = map[string]any{}
	}
	servers, err := ensureChildMap(document, "mcpServers")
	if err != nil {
		return false, err
	}
	servers["orchestra"] = map[string]any{
		"command": binaryPath,
		"args":    arguments,
	}
	encodedBytes, err := marshalIndentedJSON(document)
	if err != nil {
		return false, err
	}
	if exists && bytes.Equal(bytes.TrimSpace(originalBytes), bytes.TrimSpace(encodedBytes)) {
		return false, nil
	}
	if dryRun {
		return true, nil
	}
	return true, writeConfigFilePreservingMode(configPath, encodedBytes)
}

func uninstallStandardJSONMCP(configPath string, dryRun bool) (bool, error) {
	document, exists, _, err := readJSONObjectFile(configPath)
	if err != nil || !exists {
		return false, err
	}
	servers, ok := document["mcpServers"].(map[string]any)
	if !ok {
		return false, nil
	}
	if _, present := servers["orchestra"]; !present {
		return false, nil
	}
	delete(servers, "orchestra")
	if dryRun {
		return true, nil
	}
	encodedBytes, err := marshalIndentedJSON(document)
	if err != nil {
		return false, err
	}
	return true, writeConfigFilePreservingMode(configPath, encodedBytes)
}

func inspectOpenCodeJSONMCP(configPath string) (bool, string, []string, error) {
	document, exists, _, err := readJSONObjectFile(configPath)
	if err != nil || !exists {
		return false, "", nil, err
	}
	mcpObject, ok := document["mcp"].(map[string]any)
	if !ok {
		return false, "", nil, nil
	}
	targetMap := mcpObject
	if serversMap, usesServers := mcpObject["servers"].(map[string]any); usesServers {
		targetMap = serversMap
	}
	rawEntry, present := targetMap["orchestra"]
	if !present {
		return false, "", nil, nil
	}
	entry, ok := rawEntry.(map[string]any)
	if !ok {
		return true, "", nil, nil
	}
	commandList := extractStringSlice(entry["command"])
	if len(commandList) > 0 {
		return true, commandList[0], commandList[1:], nil
	}
	urlValue, _ := entry["url"].(string)
	return true, urlValue, nil, nil
}

func installOpenCodeJSONMCP(configPath string, binaryPath string, arguments []string, dryRun bool) (bool, error) {
	document, exists, originalBytes, err := readJSONObjectFile(configPath)
	if err != nil {
		return false, err
	}
	if !exists {
		document = map[string]any{
			"$schema": "https://opencode.ai/config.json",
		}
	}
	mcpObject, err := ensureChildMap(document, "mcp")
	if err != nil {
		return false, err
	}
	fullCommand := make([]string, 0, 1+len(arguments))
	fullCommand = append(fullCommand, binaryPath)
	fullCommand = append(fullCommand, arguments...)

	entry := map[string]any{
		"type":    "local",
		"command": fullCommand,
		"enabled": true,
	}
	if rawServers, usesServers := mcpObject["servers"]; usesServers {
		serversMap, valid := rawServers.(map[string]any)
		if !valid {
			return false, errors.New("mcp.servers must be a JSON object")
		}
		delete(entry, "enabled")
		serversMap["orchestra"] = entry
	} else {
		mcpObject["orchestra"] = entry
	}

	encodedBytes, err := marshalIndentedJSON(document)
	if err != nil {
		return false, err
	}
	if exists && bytes.Equal(bytes.TrimSpace(originalBytes), bytes.TrimSpace(encodedBytes)) {
		return false, nil
	}
	if dryRun {
		return true, nil
	}
	return true, writeConfigFilePreservingMode(configPath, encodedBytes)
}

func uninstallOpenCodeJSONMCP(configPath string, dryRun bool) (bool, error) {
	document, exists, _, err := readJSONObjectFile(configPath)
	if err != nil || !exists {
		return false, err
	}
	mcpObject, ok := document["mcp"].(map[string]any)
	if !ok {
		return false, nil
	}
	targetMap := mcpObject
	if serversMap, usesServers := mcpObject["servers"].(map[string]any); usesServers {
		targetMap = serversMap
	}
	if _, present := targetMap["orchestra"]; !present {
		return false, nil
	}
	delete(targetMap, "orchestra")
	if dryRun {
		return true, nil
	}
	encodedBytes, err := marshalIndentedJSON(document)
	if err != nil {
		return false, err
	}
	return true, writeConfigFilePreservingMode(configPath, encodedBytes)
}

func inspectCodexTOMLMCP(configPath string) (bool, string, []string, error) {
	rawBytes, err := os.ReadFile(configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, "", nil, nil
		}
		return false, "", nil, err
	}
	lines := splitPreservingLines(string(rawBytes))
	spans := findOrchestraTOMLTableSpans(lines)
	if len(spans) == 0 {
		return false, "", nil, nil
	}
	var command string
	var arguments []string
	firstSpan := spans[0]
	for lineIndex := firstSpan.start + 1; lineIndex < firstSpan.end; lineIndex++ {
		stripped := stripTOMLComment(lines[lineIndex])
		key, value, hasEquals := strings.Cut(stripped, "=")
		if !hasEquals {
			continue
		}
		cleanKey := strings.TrimSpace(key)
		cleanValue := strings.TrimSpace(value)
		switch cleanKey {
		case "command":
			if unquoted, unquoteErr := unquoteTOMLString(cleanValue); unquoteErr == nil {
				command = unquoted
			}
		case "args":
			arguments = parseTOMLStringArray(cleanValue)
		}
	}
	return true, command, arguments, nil
}

func installCodexTOMLMCP(configPath string, binaryPath string, arguments []string, dryRun bool) (bool, error) {
	var originalContent string
	exists := false
	if rawBytes, err := os.ReadFile(configPath); err == nil {
		exists = true
		originalContent = string(rawBytes)
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}

	renderedSection := formatCodexOrchestraTOMLSection(binaryPath, arguments)
	var updatedContent string
	if !exists || strings.TrimSpace(originalContent) == "" {
		updatedContent = renderedSection
	} else {
		lines := splitPreservingLines(originalContent)
		spans := findOrchestraTOMLTableSpans(lines)
		if len(spans) == 0 {
			trimmedRight := strings.TrimRight(originalContent, "\r\n")
			updatedContent = trimmedRight + "\n\n" + renderedSection
		} else {
			var outputLines []string
			inserted := false
			for index := 0; index < len(lines); {
				matchedSpan, isSpanStart := spanStartingAt(spans, index)
				if isSpanStart {
					if !inserted {
						if len(outputLines) > 0 && strings.TrimSpace(outputLines[len(outputLines)-1]) != "" {
							outputLines = append(outputLines, "")
						}
						for sectionLine := range strings.SplitSeq(strings.TrimSuffix(renderedSection, "\n"), "\n") {
							outputLines = append(outputLines, sectionLine)
						}
						if matchedSpan.end < len(lines) && strings.TrimSpace(lines[matchedSpan.end]) != "" {
							outputLines = append(outputLines, "")
						}
						inserted = true
					}
					index = matchedSpan.end
					continue
				}
				outputLines = append(outputLines, lines[index])
				index++
			}
			updatedContent = collapseExcessBlankLines(strings.Join(outputLines, "\n"))
		}
	}

	if exists && originalContent == updatedContent {
		return false, nil
	}
	if dryRun {
		return true, nil
	}
	return true, writeConfigFilePreservingMode(configPath, []byte(updatedContent))
}

func uninstallCodexTOMLMCP(configPath string, dryRun bool) (bool, error) {
	rawBytes, err := os.ReadFile(configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	originalContent := string(rawBytes)
	lines := splitPreservingLines(originalContent)
	spans := findOrchestraTOMLTableSpans(lines)
	if len(spans) == 0 {
		return false, nil
	}
	if dryRun {
		return true, nil
	}

	var outputLines []string
	for index := 0; index < len(lines); {
		if matchedSpan, isSpanStart := spanStartingAt(spans, index); isSpanStart {
			index = matchedSpan.end
			continue
		}
		outputLines = append(outputLines, lines[index])
		index++
	}
	updatedContent := collapseExcessBlankLines(strings.Join(outputLines, "\n"))
	return true, writeConfigFilePreservingMode(configPath, []byte(updatedContent))
}

type lineSpan struct {
	start int
	end   int
}

func spanStartingAt(spans []lineSpan, lineIndex int) (lineSpan, bool) {
	for _, span := range spans {
		if span.start == lineIndex {
			return span, true
		}
	}
	return lineSpan{}, false
}

func findOrchestraTOMLTableSpans(lines []string) []lineSpan {
	type tableHeader struct {
		lineIndex   int
		isOrchestra bool
	}

	var headers []tableHeader
	multilineDelimiter := ""

	for index, line := range lines {
		if multilineDelimiter != "" {
			if strings.Contains(line, multilineDelimiter) {
				multilineDelimiter = ""
			}
			continue
		}

		stripped := strings.TrimSpace(stripTOMLComment(line))
		if isTOMLHeader, keyPath := parseTOMLTableHeader(stripped); isTOMLHeader {
			isOrchestra := len(keyPath) >= 2 && keyPath[0] == "mcp_servers" && keyPath[1] == "orchestra"
			headers = append(headers, tableHeader{
				lineIndex:   index,
				isOrchestra: isOrchestra,
			})
			continue
		}

		multilineDelimiter = detectUnclosedMultilineDelimiter(stripped)
	}

	var spans []lineSpan
	for headerIndex, header := range headers {
		if !header.isOrchestra {
			continue
		}
		endLine := len(lines)
		if headerIndex+1 < len(headers) {
			endLine = headers[headerIndex+1].lineIndex
		}
		spans = append(spans, lineSpan{start: header.lineIndex, end: endLine})
	}
	return spans
}

func parseTOMLTableHeader(trimmedLine string) (bool, []string) {
	if len(trimmedLine) < 3 || trimmedLine[0] != '[' || trimmedLine[len(trimmedLine)-1] != ']' {
		return false, nil
	}
	interior := trimmedLine[1 : len(trimmedLine)-1]
	if strings.HasPrefix(interior, "[") && strings.HasSuffix(interior, "]") {
		interior = interior[1 : len(interior)-1]
	}
	segments := splitTOMLDottedKey(interior)
	if len(segments) == 0 {
		return false, nil
	}
	return true, segments
}

func splitTOMLDottedKey(rawKey string) []string {
	var segments []string
	var current strings.Builder
	quoteChar := byte(0)
	escaped := false

	for index := range len(rawKey) {
		character := rawKey[index]
		if quoteChar != 0 {
			if escaped {
				current.WriteByte(character)
				escaped = false
				continue
			}
			if character == '\\' && quoteChar == '"' {
				escaped = true
				continue
			}
			if character == quoteChar {
				quoteChar = 0
				continue
			}
			current.WriteByte(character)
			continue
		}
		if character == '"' || character == '\'' {
			quoteChar = character
			continue
		}
		if character == '.' {
			part := strings.TrimSpace(current.String())
			if part == "" {
				return nil
			}
			segments = append(segments, part)
			current.Reset()
			continue
		}
		current.WriteByte(character)
	}
	if quoteChar != 0 {
		return nil
	}
	lastPart := strings.TrimSpace(current.String())
	if lastPart == "" {
		return nil
	}
	segments = append(segments, lastPart)
	return segments
}

func stripTOMLComment(line string) string {
	quoteChar := byte(0)
	escaped := false
	for index := range len(line) {
		character := line[index]
		if quoteChar != 0 {
			if escaped {
				escaped = false
				continue
			}
			if character == '\\' && quoteChar == '"' {
				escaped = true
				continue
			}
			if character == quoteChar {
				quoteChar = 0
			}
			continue
		}
		if character == '"' || character == '\'' {
			quoteChar = character
			continue
		}
		if character == '#' {
			return line[:index]
		}
	}
	return line
}

func detectUnclosedMultilineDelimiter(line string) string {
	for _, delimiter := range []string{`"""`, `'''`} {
		count := strings.Count(line, delimiter)
		if count%2 == 1 {
			return delimiter
		}
	}
	return ""
}

func formatCodexOrchestraTOMLSection(binaryPath string, arguments []string) string {
	quotedArguments := make([]string, 0, len(arguments))
	for _, argument := range arguments {
		quotedArguments = append(quotedArguments, strconv.Quote(argument))
	}
	return fmt.Sprintf(
		"[mcp_servers.orchestra]\ncommand = %s\nargs = [%s]\n",
		strconv.Quote(binaryPath),
		strings.Join(quotedArguments, ", "),
	)
}

func unquoteTOMLString(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if len(trimmed) >= 2 && trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'' {
		return trimmed[1 : len(trimmed)-1], nil
	}
	return strconv.Unquote(trimmed)
}

func parseTOMLStringArray(raw string) []string {
	trimmed := strings.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '[' || trimmed[len(trimmed)-1] != ']' {
		return nil
	}
	interior := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	if interior == "" {
		return []string{}
	}
	var values []string
	var current strings.Builder
	quoteChar := byte(0)
	escaped := false
	for index := range len(interior) {
		character := interior[index]
		if quoteChar != 0 {
			current.WriteByte(character)
			if escaped {
				escaped = false
				continue
			}
			if character == '\\' && quoteChar == '"' {
				escaped = true
				continue
			}
			if character == quoteChar {
				quoteChar = 0
			}
			continue
		}
		if character == '"' || character == '\'' {
			quoteChar = character
			current.WriteByte(character)
			continue
		}
		if character == ',' {
			if parsed, err := unquoteTOMLString(current.String()); err == nil {
				values = append(values, parsed)
			}
			current.Reset()
			continue
		}
		current.WriteByte(character)
	}
	if strings.TrimSpace(current.String()) != "" {
		if parsed, err := unquoteTOMLString(current.String()); err == nil {
			values = append(values, parsed)
		}
	}
	return values
}

func splitPreservingLines(content string) []string {
	normalized := strings.ReplaceAll(content, "\r\n", "\n")
	normalized = strings.TrimSuffix(normalized, "\n")
	if normalized == "" {
		return nil
	}
	return strings.Split(normalized, "\n")
}

func collapseExcessBlankLines(content string) string {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	var cleaned []string
	blankRun := 0
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			blankRun++
			if blankRun <= 1 {
				cleaned = append(cleaned, "")
			}
			continue
		}
		blankRun = 0
		cleaned = append(cleaned, line)
	}
	trimmed := strings.TrimSpace(strings.Join(cleaned, "\n"))
	if trimmed == "" {
		return ""
	}
	return trimmed + "\n"
}

func readJSONObjectFile(filePath string) (map[string]any, bool, []byte, error) {
	rawBytes, err := os.ReadFile(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil, nil
		}
		return nil, false, nil, err
	}
	if len(bytes.TrimSpace(rawBytes)) == 0 {
		return map[string]any{}, true, rawBytes, nil
	}
	var document map[string]any
	if unmarshalErr := json.Unmarshal(rawBytes, &document); unmarshalErr != nil {
		return nil, true, rawBytes, fmt.Errorf("invalid JSON: %w", unmarshalErr)
	}
	if document == nil {
		document = map[string]any{}
	}
	return document, true, rawBytes, nil
}

func ensureChildMap(parent map[string]any, key string) (map[string]any, error) {
	raw, exists := parent[key]
	if !exists || raw == nil {
		child := map[string]any{}
		parent[key] = child
		return child, nil
	}
	child, valid := raw.(map[string]any)
	if !valid {
		return nil, fmt.Errorf("%q must be a JSON object", key)
	}
	return child, nil
}

func extractStringSlice(raw any) []string {
	items, valid := raw.([]any)
	if !valid {
		return nil
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		if textValue, isString := item.(string); isString {
			result = append(result, textValue)
		}
	}
	return result
}

func marshalIndentedJSON(document map[string]any) ([]byte, error) {
	encoded, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func writeConfigFilePreservingMode(filePath string, content []byte) error {
	fileMode := os.FileMode(0o600)
	if information, err := os.Stat(filePath); err == nil {
		fileMode = information.Mode().Perm()
	}
	parentDirectory := filepath.Dir(filePath)
	if err := os.MkdirAll(parentDirectory, 0o755); err != nil {
		return err
	}
	temporaryFile, err := os.CreateTemp(parentDirectory, ".orchestra-cfg-*")
	if err != nil {
		return err
	}
	temporaryPath := temporaryFile.Name()
	cleanupTemporary := true
	defer func() {
		if cleanupTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()

	if chmodErr := temporaryFile.Chmod(fileMode); chmodErr != nil {
		_ = temporaryFile.Close()
		return chmodErr
	}
	if _, writeErr := temporaryFile.Write(content); writeErr != nil {
		_ = temporaryFile.Close()
		return writeErr
	}
	if syncErr := temporaryFile.Sync(); syncErr != nil {
		_ = temporaryFile.Close()
		return syncErr
	}
	if closeErr := temporaryFile.Close(); closeErr != nil {
		return closeErr
	}
	if renameErr := os.Rename(temporaryPath, filePath); renameErr != nil {
		return renameErr
	}
	cleanupTemporary = false
	return nil
}
