package antigravityacp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const (
	ModeOption  = "mode"
	ModelOption = "model"

	ModeSandbox = "sandbox"
	ModeBypass  = "bypass"
	ModePlan    = "plan"

	NativeBinaryEnvironmentVariable   = "AGY_EXECUTABLE"
	LegacyBinaryEnvironmentVariable   = "AGY_BIN"
	StateDirectoryEnvironmentVariable = "ORCHESTRA_ANTIGRAVITY_ACP_STATE_DIR"

	modelDiscoveryTimeout    = 4 * time.Second
	interruptEscalationDelay = 750 * time.Millisecond
)

var (
	discoveredModelPattern      = regexp.MustCompile(`^[a-z0-9][a-z0-9._:/@+-]{0,127}$`)
	conversationIdentityPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
	safeMCPServerNamePattern    = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
)

// MCPHTTPServer describes a Streamable HTTP MCP server endpoint wired into an Antigravity turn.
type MCPHTTPServer struct {
	Name    string
	URL     string
	Headers map[string]string
}

// PreparedMCPDirectory holds the ephemeral --add-dir configuration directory and --settings payload for agy.
type PreparedMCPDirectory struct {
	Directory    string
	SettingsJSON string
	Cleanup      func()
}

// PrepareMCPDirectory writes an isolated 0700 directory containing .agents/mcp_config.json (0600)
// and returns the matching --settings permissions JSON for agy.
func PrepareMCPDirectory(servers []MCPHTTPServer, allowedToolsByServer map[string][]string) (PreparedMCPDirectory, error) {
	if len(servers) == 0 {
		return PreparedMCPDirectory{Cleanup: func() {}}, nil
	}
	temporaryDirectory, err := os.MkdirTemp("", "orchestra-agy-mcp-")
	if err != nil {
		return PreparedMCPDirectory{}, fmt.Errorf("create ephemeral Antigravity MCP directory: %w", err)
	}
	cleanup := func() {
		_ = os.RemoveAll(temporaryDirectory)
	}
	if err := os.Chmod(temporaryDirectory, 0o700); err != nil {
		cleanup()
		return PreparedMCPDirectory{}, err
	}
	agentsDirectory := filepath.Join(temporaryDirectory, ".agents")
	if err := os.MkdirAll(agentsDirectory, 0o700); err != nil {
		cleanup()
		return PreparedMCPDirectory{}, err
	}

	serverEntries := make(map[string]any, len(servers))
	allowedRules := make([]string, 0, len(servers))
	for _, server := range servers {
		if !safeMCPServerNamePattern.MatchString(server.Name) {
			cleanup()
			return PreparedMCPDirectory{}, fmt.Errorf("unsafe MCP server name %q", server.Name)
		}
		entry := map[string]any{
			"serverUrl": server.URL,
		}
		if len(server.Headers) > 0 {
			entry["headers"] = server.Headers
		}
		serverEntries[server.Name] = entry

		if specificTools := allowedToolsByServer[server.Name]; len(specificTools) > 0 {
			for _, toolName := range specificTools {
				if safeMCPServerNamePattern.MatchString(toolName) {
					allowedRules = append(allowedRules, fmt.Sprintf("mcp(%s/%s)", server.Name, toolName))
				}
			}
		} else {
			allowedRules = append(allowedRules, fmt.Sprintf("mcp(%s/*)", server.Name))
		}
	}

	mcpConfigBytes, err := json.Marshal(map[string]any{
		"mcpServers": serverEntries,
	})
	if err != nil {
		cleanup()
		return PreparedMCPDirectory{}, err
	}
	configFilePath := filepath.Join(agentsDirectory, "mcp_config.json")
	if err := os.WriteFile(configFilePath, mcpConfigBytes, 0o600); err != nil {
		cleanup()
		return PreparedMCPDirectory{}, err
	}

	settingsBytes, err := json.Marshal(map[string]any{
		"permissions": map[string]any{
			"allow": allowedRules,
		},
	})
	if err != nil {
		cleanup()
		return PreparedMCPDirectory{}, err
	}

	return PreparedMCPDirectory{
		Directory:    temporaryDirectory,
		SettingsJSON: string(settingsBytes),
		Cleanup:      cleanup,
	}, nil
}

// BuildCLIArguments constructs the headless agy CLI invocation arguments for a single turn.
func BuildCLIArguments(
	workingDirectory string,
	mcpDirectory PreparedMCPDirectory,
	conversationID string,
	modelID string,
	mode string,
	prompt string,
) []string {
	arguments := []string{
		"--output-format", "stream-json",
		"--disable-slash-commands",
		"--print-timeout", "0",
	}
	if workingDirectory != "" {
		arguments = append(arguments, "--add-dir", workingDirectory)
	}
	if mcpDirectory.Directory != "" {
		arguments = append(arguments, "--add-dir", mcpDirectory.Directory)
		if mcpDirectory.SettingsJSON != "" {
			arguments = append(arguments, "--settings", mcpDirectory.SettingsJSON)
		}
	}
	if conversationID != "" {
		arguments = append(arguments, "--conversation", conversationID)
	}
	if modelID != "" && modelID != "default" {
		arguments = append(arguments, "--model", modelID)
	}
	if mode == ModePlan {
		arguments = append(arguments, "--mode", "plan")
	} else {
		arguments = append(arguments, "--mode", "accept-edits")
	}
	arguments = append(arguments, "--dangerously-skip-permissions")
	if mode != ModeBypass {
		arguments = append(arguments, "--sandbox")
	}
	arguments = append(arguments, "-p="+prompt)
	return arguments
}

func discoverModels(operationContext context.Context, binaryPath string, store *sessionStore, now time.Time) []discoveredModel {
	if cached, hit := store.loadCachedModels(now); hit {
		return cached
	}
	if binaryPath == "" {
		return nil
	}
	timeoutContext, cancel := context.WithTimeout(operationContext, modelDiscoveryTimeout)
	defer cancel()

	command := exec.CommandContext(timeoutContext, binaryPath, "models")
	configureProcessCancellation(command)
	command.Env = scrubEnvironment(os.Environ())
	outputBytes, err := command.Output()
	if err != nil {
		return nil
	}
	parsed := parseModelsOutput(string(outputBytes))
	if len(parsed) > 0 {
		store.saveCachedModels(parsed, now)
	}
	return parsed
}

func parseModelsOutput(rawOutput string) []discoveredModel {
	var models []discoveredModel
	seen := map[string]bool{}
	for rawLine := range strings.SplitSeq(rawOutput, "\n") {
		if carriageIndex := strings.LastIndexByte(rawLine, '\r'); carriageIndex >= 0 {
			rawLine = rawLine[carriageIndex+1:]
		}
		trimmed := strings.TrimSpace(rawLine)
		if trimmed == "" || strings.HasSuffix(trimmed, "...") {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) == 0 {
			continue
		}
		identifier := fields[0]
		if seen[identifier] || !discoveredModelPattern.MatchString(identifier) || protocol.LooksSecretLike(identifier) {
			continue
		}
		displayName := identifier
		if tabIndex := strings.IndexByte(trimmed, '\t'); tabIndex > 0 {
			if candidate := strings.TrimSpace(trimmed[tabIndex+1:]); candidate != "" {
				displayName = candidate
			}
		} else if len(fields) > 1 {
			displayName = strings.Join(fields[1:], " ")
		}
		seen[identifier] = true
		models = append(models, discoveredModel{Value: identifier, Name: displayName})
		if len(models) >= 64 {
			break
		}
	}
	return models
}

type agyStreamEnvelope struct {
	Event          string         `json:"event"`
	ConversationID string         `json:"conversation_id"`
	StepUpdate     *agyStepUpdate `json:"step_update"`
	Result         *agyTurnResult `json:"result"`
}

type agyStepUpdate struct {
	ConversationID string         `json:"conversation_id"`
	StepIndex      int            `json:"step_index"`
	State          string         `json:"state"`
	StepType       string         `json:"step_type"`
	TextDelta      string         `json:"text_delta"`
	ToolName       string         `json:"tool_name"`
	ToolInfo       *agyToolInfo   `json:"tool_info"`
	Usage          *agyTokenUsage `json:"usage"`
}

type agyToolInfo struct {
	Name       string         `json:"name"`
	Parameters map[string]any `json:"parameters"`
}

type agyDeniedAction struct {
	Action      string `json:"action"`
	DisplayName string `json:"display_name"`
}

type agyTurnResult struct {
	ConversationID string            `json:"conversation_id"`
	Status         string            `json:"status"`
	Response       string            `json:"response"`
	Error          string            `json:"error"`
	DeniedActions  []agyDeniedAction `json:"denied_actions"`
	Usage          *agyTokenUsage    `json:"usage"`
}

type agyTokenUsage struct {
	InputTokens     *int64 `json:"input_tokens"`
	OutputTokens    *int64 `json:"output_tokens"`
	ThinkingTokens  *int64 `json:"thinking_tokens"`
	CacheReadTokens *int64 `json:"cache_read_tokens"`
	TotalTokens     *int64 `json:"total_tokens"`
}

type turnUsage struct {
	InputTokens      *int64 `json:"inputTokens,omitempty"`
	OutputTokens     *int64 `json:"outputTokens,omitempty"`
	CachedReadTokens *int64 `json:"cachedReadTokens,omitempty"`
}

type turnOutcome struct {
	StopReason          string
	ConversationID      string
	Usage               *turnUsage
	ConversationMissing bool
}

type trackedToolStep struct {
	toolCallID string
	status     string
}

func executeTurn(
	operationContext context.Context,
	binaryPath string,
	workingDirectory string,
	session storedSession,
	mcpServers []MCPHTTPServer,
	promptText string,
	turnNumber int,
	cancelSignal <-chan struct{},
	onConversationID func(string),
	emitUpdate func(map[string]any) error,
) (turnOutcome, error) {
	mcpDirectory, err := PrepareMCPDirectory(mcpServers, nil)
	if err != nil {
		return turnOutcome{}, err
	}
	defer mcpDirectory.Cleanup()

	arguments := BuildCLIArguments(
		workingDirectory,
		mcpDirectory,
		session.ConversationID,
		session.ModelID,
		session.Mode,
		promptText,
	)

	killContext, killProcessTree := context.WithCancel(context.Background())
	defer killProcessTree()

	command := exec.CommandContext(killContext, binaryPath, arguments...)
	configureProcessCancellation(command)
	command.Dir = workingDirectory
	command.Env = scrubEnvironment(os.Environ())

	stdoutPipe, err := command.StdoutPipe()
	if err != nil {
		return turnOutcome{}, err
	}
	var stderrBuffer bytes.Buffer
	command.Stderr = &stderrBuffer

	if err := command.Start(); err != nil {
		return turnOutcome{}, fmt.Errorf("start agy: %w", err)
	}

	stopWatcher := make(chan struct{})
	cancelled := make(chan struct{})
	go func() {
		select {
		case <-stopWatcher:
			return
		case <-operationContext.Done():
		case <-cancelSignal:
		}
		close(cancelled)
		interruptProcessGroup(command)
		timer := time.NewTimer(interruptEscalationDelay)
		defer timer.Stop()
		select {
		case <-stopWatcher:
		case <-timer.C:
			killProcessTree()
		}
	}()

	outcome, streamErr := translateStreamOutput(
		stdoutPipe,
		workingDirectory,
		turnNumber,
		onConversationID,
		emitUpdate,
	)
	waitErr := command.Wait()
	close(stopWatcher)

	select {
	case <-cancelled:
		outcome.StopReason = "cancelled"
		return outcome, nil
	default:
	}

	stderrText := strings.TrimSpace(stderrBuffer.String())
	if streamErr != nil {
		if stderrText != "" {
			return outcome, fmt.Errorf("%w (%s)", streamErr, stderrText)
		}
		return outcome, streamErr
	}
	if waitErr != nil {
		if stderrText != "" {
			return outcome, fmt.Errorf("agy failed: %s", stderrText)
		}
		return outcome, fmt.Errorf("agy exited with error: %w", waitErr)
	}
	outcome.StopReason = "end_turn"
	return outcome, nil
}

func translateStreamOutput(
	reader io.Reader,
	workingDirectory string,
	turnNumber int,
	onConversationID func(string),
	emitUpdate func(map[string]any) error,
) (turnOutcome, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)

	var outcome turnOutcome
	var turnError error
	streamedMessage := false
	toolSteps := map[int]trackedToolStep{}

	recordConversation := func(candidate string) {
		if candidate == "" || !conversationIdentityPattern.MatchString(candidate) || protocol.LooksSecretLike(candidate) {
			return
		}
		if outcome.ConversationID == "" {
			outcome.ConversationID = candidate
			if onConversationID != nil {
				onConversationID(candidate)
			}
		}
	}

	for scanner.Scan() {
		lineBytes := bytes.TrimSpace(scanner.Bytes())
		if len(lineBytes) == 0 {
			continue
		}
		var envelope agyStreamEnvelope
		if json.Unmarshal(lineBytes, &envelope) != nil {
			continue
		}
		recordConversation(envelope.ConversationID)

		switch envelope.Event {
		case "init":
			continue

		case "step_update":
			if envelope.StepUpdate == nil {
				continue
			}
			step := envelope.StepUpdate
			recordConversation(step.ConversationID)
			if step.Usage != nil {
				outcome.Usage = convertUsage(step.Usage)
			}

			isToolStep := strings.EqualFold(step.StepType, "tool") || step.ToolName != "" || step.ToolInfo != nil
			if isToolStep {
				if err := emitToolStepUpdate(step, workingDirectory, turnNumber, toolSteps, emitUpdate); err != nil {
					return outcome, err
				}
			}

			if step.TextDelta != "" {
				lowerType := strings.ToLower(step.StepType)
				updateKind := "agent_message_chunk"
				if strings.Contains(lowerType, "thought") || strings.Contains(lowerType, "think") {
					updateKind = "agent_thought_chunk"
				} else {
					streamedMessage = true
				}
				if err := emitUpdate(map[string]any{
					"sessionUpdate": updateKind,
					"content": map[string]any{
						"type": "text",
						"text": step.TextDelta,
					},
				}); err != nil {
					return outcome, err
				}
			}

		case "result":
			if envelope.Result == nil {
				continue
			}
			result := envelope.Result
			recordConversation(result.ConversationID)
			if result.Usage != nil {
				outcome.Usage = convertUsage(result.Usage)
			}
			if !streamedMessage && result.Response != "" {
				streamedMessage = true
				if err := emitUpdate(map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"content": map[string]any{
						"type": "text",
						"text": result.Response,
					},
				}); err != nil {
					return outcome, err
				}
			}
			if result.Status != "" && !strings.EqualFold(result.Status, "SUCCESS") {
				message := strings.TrimSpace(result.Error)
				if message == "" && len(result.DeniedActions) > 0 {
					message = formatDeniedActions(result.DeniedActions)
				}
				if message == "" {
					message = "agy turn finished with status " + result.Status
				}
				turnError = errors.New(message)
			} else if len(result.DeniedActions) > 0 && !streamedMessage {
				turnError = errors.New(formatDeniedActions(result.DeniedActions))
			}
		}
	}

	if scanErr := scanner.Err(); scanErr != nil {
		return outcome, scanErr
	}
	return outcome, turnError
}

func emitToolStepUpdate(
	step *agyStepUpdate,
	workingDirectory string,
	turnNumber int,
	toolSteps map[int]trackedToolStep,
	emitUpdate func(map[string]any) error,
) error {
	status := mapStepStateToToolStatus(step.State)
	existing, alreadySeen := toolSteps[step.StepIndex]
	if alreadySeen {
		if existing.status == status || existing.status == "completed" || existing.status == "failed" {
			return nil
		}
		existing.status = status
		toolSteps[step.StepIndex] = existing
		return emitUpdate(map[string]any{
			"sessionUpdate": "tool_call_update",
			"toolCallId":    existing.toolCallID,
			"status":        status,
		})
	}

	toolName := step.ToolName
	var parameters map[string]any
	if step.ToolInfo != nil {
		if toolName == "" {
			toolName = step.ToolInfo.Name
		}
		parameters = step.ToolInfo.Parameters
	}
	toolCallID := fmt.Sprintf("agy-%d-%d", turnNumber, step.StepIndex)
	title := buildToolTitle(toolName, parameters, workingDirectory)
	kind := classifyToolKind(toolName)
	content := buildToolContent(toolName, parameters)

	payload := map[string]any{
		"sessionUpdate": "tool_call",
		"toolCallId":    toolCallID,
		"title":         title,
		"kind":          kind,
		"status":        status,
	}
	if len(content) > 0 {
		payload["content"] = content
	}
	toolSteps[step.StepIndex] = trackedToolStep{toolCallID: toolCallID, status: status}
	return emitUpdate(payload)
}

func mapStepStateToToolStatus(state string) string {
	switch strings.ToUpper(state) {
	case "ACTIVE", "PENDING", "RUNNING":
		return "in_progress"
	case "ERROR", "FAILED", "CANCELLED", "ABORTED":
		return "failed"
	default:
		return "completed"
	}
}

func classifyToolKind(toolName string) string {
	lower := strings.ToLower(toolName)
	switch {
	case strings.Contains(lower, "write") || strings.Contains(lower, "edit") || strings.Contains(lower, "replace") || strings.Contains(lower, "sed"):
		return "edit"
	case strings.Contains(lower, "delete") || strings.Contains(lower, "remove"):
		return "delete"
	case strings.Contains(lower, "move") || strings.Contains(lower, "rename"):
		return "move"
	case strings.Contains(lower, "read") || strings.Contains(lower, "view") || strings.Contains(lower, "list"):
		return "read"
	case strings.Contains(lower, "grep") || strings.Contains(lower, "search") || strings.Contains(lower, "find"):
		return "search"
	case strings.Contains(lower, "command") || strings.Contains(lower, "execute") || strings.Contains(lower, "terminal"):
		return "execute"
	case strings.Contains(lower, "think") || strings.Contains(lower, "thought") || strings.Contains(lower, "plan"):
		return "think"
	case strings.Contains(lower, "url") || strings.Contains(lower, "fetch"):
		return "fetch"
	default:
		return "other"
	}
}

func buildToolTitle(toolName string, parameters map[string]any, workingDirectory string) string {
	if summary := pickStringParameter(parameters, "toolSummary", "ToolSummary"); summary != "" {
		return summary
	}
	switch toolName {
	case "call_mcp_tool":
		serverName := pickStringParameter(parameters, "ServerName", "serverName")
		memberName := pickStringParameter(parameters, "ToolName", "toolName")
		if serverName != "" && memberName != "" {
			return fmt.Sprintf("MCP %s/%s", serverName, memberName)
		}
		if memberName != "" {
			return "MCP " + memberName
		}
	case "run_command":
		if commandLine := pickStringParameter(parameters, "CommandLine", "commandLine", "command"); commandLine != "" {
			return "Run " + commandLine
		}
	case "view_file", "read_file":
		if targetPath := pickStringParameter(parameters, "AbsolutePath", "Path", "path"); targetPath != "" {
			return "Read " + relativeDisplayPath(targetPath, workingDirectory)
		}
	case "write_to_file", "write_file":
		if targetPath := pickStringParameter(parameters, "TargetFile", "Path", "path"); targetPath != "" {
			return "Write " + relativeDisplayPath(targetPath, workingDirectory)
		}
	case "edit_file", "replace_file_content", "multi_replace_file_content":
		if targetPath := pickStringParameter(parameters, "TargetFile", "Path", "path"); targetPath != "" {
			return "Edit " + relativeDisplayPath(targetPath, workingDirectory)
		}
	}
	if strings.TrimSpace(toolName) != "" {
		return toolName
	}
	return "Tool"
}

func buildToolContent(toolName string, parameters map[string]any) []map[string]any {
	if len(parameters) == 0 {
		return nil
	}
	lower := strings.ToLower(toolName)
	if strings.Contains(lower, "edit") || strings.Contains(lower, "replace") || strings.Contains(lower, "write") {
		targetPath := pickStringParameter(parameters, "TargetFile", "Path", "path")
		newText := pickStringParameter(parameters, "Replacement", "ReplacementContent", "CodeContent", "Content")
		oldText := pickStringParameter(parameters, "Target", "TargetContent")
		if targetPath != "" && newText != "" {
			return []map[string]any{{
				"type":    "diff",
				"path":    targetPath,
				"oldText": oldText,
				"newText": newText,
			}}
		}
	}
	return nil
}

func pickStringParameter(parameters map[string]any, keys ...string) string {
	for _, key := range keys {
		if rawValue, exists := parameters[key]; exists {
			if textValue, ok := rawValue.(string); ok && strings.TrimSpace(textValue) != "" {
				return strings.TrimSpace(textValue)
			}
		}
	}
	return ""
}

func relativeDisplayPath(targetPath, workingDirectory string) string {
	if workingDirectory == "" || !filepath.IsAbs(targetPath) {
		return targetPath
	}
	relativePath, err := filepath.Rel(workingDirectory, targetPath)
	if err != nil || strings.HasPrefix(relativePath, "..") {
		return targetPath
	}
	return relativePath
}

func convertUsage(usage *agyTokenUsage) *turnUsage {
	if usage == nil || (usage.InputTokens == nil && usage.OutputTokens == nil && usage.CacheReadTokens == nil) {
		return nil
	}
	return &turnUsage{
		InputTokens:      usage.InputTokens,
		OutputTokens:     usage.OutputTokens,
		CachedReadTokens: usage.CacheReadTokens,
	}
}

func formatDeniedActions(denied []agyDeniedAction) string {
	descriptions := make([]string, 0, len(denied))
	for _, item := range denied {
		label := strings.TrimSpace(item.DisplayName)
		if label == "" {
			label = strings.TrimSpace(item.Action)
		}
		if label != "" {
			descriptions = append(descriptions, label)
		}
	}
	if len(descriptions) == 0 {
		return "agy denied a required tool action in headless mode"
	}
	return "agy denied tool action(s): " + strings.Join(descriptions, ", ")
}

func scrubEnvironment(inherited []string) []string {
	blockedVariables := []string{"ORCHESTRA_TOKEN", "ORCHESTRA_MCP_TOKEN"}
	cleaned := make([]string, 0, len(inherited))
	for _, entry := range inherited {
		variableName, _, _ := strings.Cut(entry, "=")
		blocked := false
		for _, secretName := range blockedVariables {
			if strings.EqualFold(variableName, secretName) {
				blocked = true
				break
			}
		}
		if !blocked {
			cleaned = append(cleaned, entry)
		}
	}
	return cleaned
}
