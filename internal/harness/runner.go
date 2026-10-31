package harness

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
	"regexp"
	"strconv"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/antigravityacp"
	"github.com/cafecito-games/Orchestra/internal/mcpserver"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const emptyResponse = "Run completed without a text response."

const coordinationContract = `

You are running inside Orchestra. Use the Orchestra hub tools (` + "`orchestra_hub`" + `) for durable thread and task context, task messages, artifacts, and bounded delegation when they are available. Refine the thread title, objective, and summary when that improves the shared record, and mark it completed only when the overall objective is satisfied. Keep returned task and artifact ids.`

type nativeDriver struct {
	runner *Runner
}

func (driver nativeDriver) Execute(operationContext context.Context, invocation Invocation) (string, error) {
	run, mcpConfiguration, output := invocation.Run, invocation.MCP, invocation.Output
	profile, available := driver.runner.profile(run.HarnessID)
	if !available {
		return "", fmt.Errorf("harness %s is not installed or did not pass its version check", run.HarnessID)
	}
	if err := driver.runner.verifyManagedHarness(run.HarnessID); err != nil {
		return "", err
	}
	binary := profile.Binary
	if binary == "" {
		return "", fmt.Errorf("harness %s has no resolved executable", run.HarnessID)
	}
	releaseExecutable := driver.runner.holdExecutable(binary)
	defer releaseExecutable()
	arguments, cleanupInvocation, err := prepareNativeArguments(invocation)
	if err != nil {
		return "", err
	}
	defer cleanupInvocation()
	command := exec.CommandContext(operationContext, binary, arguments...)
	configureProcessCancellation(command)
	command.Dir = invocation.Workspace
	command.Env = adapterEnvironment(os.Environ(), nil)
	if run.HarnessID == protocol.HarnessOpenCodeCLI && mcpConfiguration.URL != "" {
		configuration, configurationErr := opencodeInlineConfiguration(environmentValue(command.Env, opencodeConfigEnvironment), mcpConfiguration)
		if configurationErr != nil {
			return "", configurationErr
		}
		command.Env = replaceEnvironmentValue(command.Env, opencodeConfigEnvironment, configuration)
	}
	if mcpConfiguration.URL != "" && run.HarnessID != protocol.HarnessAntigravityCLI {
		command.Env = append(command.Env, "ORCHESTRA_MCP_TOKEN="+mcpConfiguration.Token)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		return "", err
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	invocation.announce(transportDetails{})
	if err := command.Start(); err != nil {
		return "", err
	}

	final, readErr := readEvents(stdout, run.HarnessID, output, invocation.ProviderSession)
	waitErr := command.Wait()
	if readErr != nil {
		return "", redactHarnessDiagnostic(readErr, mcpConfiguration.Token)
	}
	if waitErr != nil {
		if message := strings.TrimSpace(stderr.String()); message != "" {
			return "", redactHarnessDiagnostic(errors.New(message), mcpConfiguration.Token)
		}
		return "", waitErr
	}
	if strings.TrimSpace(final) == "" {
		return emptyResponse, nil
	}
	return strings.TrimSpace(final), nil
}

func redactHarnessDiagnostic(err error, mcpToken string) error {
	if err == nil {
		return nil
	}
	redacted := protocol.RedactText(err.Error())
	if strings.TrimSpace(mcpToken) != "" {
		redacted = strings.ReplaceAll(redacted, mcpToken, "[REDACTED]")
	}
	return errors.New(redacted)
}

func (runner *Runner) profile(id string) (protocol.HarnessProfile, bool) {
	for _, profile := range runner.nativeProfiles {
		if profile.ID == id && profile.Available {
			return profile, true
		}
	}
	return protocol.HarnessProfile{}, false
}

func (runner *Runner) advertisedProfile(id string) (protocol.HarnessProfile, bool) {
	advertised := runner.advertisedProfiles
	if advertised == nil {
		advertised = runner.nativeProfiles
	}
	for _, profile := range advertised {
		if profile.ID == id && profile.Available {
			return profile, true
		}
	}
	return protocol.HarnessProfile{}, false
}

func composePrompt(run protocol.Run, systemPrompt string) string {
	prefix := strings.TrimSpace(systemPrompt)
	if prefix != "" {
		prefix += "\n"
	}
	return prefix + coordinationContract + "\n\nUser task:\n" + run.Prompt
}

func nativeClaudePermissionMode(approvalPolicy string) string {
	if approvalPolicy == protocol.ApprovalPolicyBypass {
		return "bypassPermissions"
	}
	return "auto"
}

func nativeCodexSandboxArguments(approvalPolicy string) []string {
	if approvalPolicy == protocol.ApprovalPolicyBypass {
		return []string{"--dangerously-bypass-approvals-and-sandbox"}
	}
	return []string{"--sandbox", "workspace-write"}
}

func isValidNativeSessionID(sessionID string) bool {
	return providerSessionPattern.MatchString(sessionID) &&
		protocol.IsIdentifier(sessionID) &&
		!protocol.LooksSecretLike(sessionID)
}

func prepareNativeArguments(invocation Invocation) ([]string, func(), error) {
	noopCleanup := func() {}
	effectiveRun := invocation.Run
	conversationID := ""
	if invocation.Resume != nil {
		conversationID = invocation.Resume.ProviderSessionID
		if !isValidNativeSessionID(conversationID) {
			return nil, noopCleanup, errors.New("native session resume identity is missing or invalid")
		}
		if invocation.Resume.Prompt != "" {
			effectiveRun.Prompt = invocation.Resume.Prompt
		}
	}
	if effectiveRun.HarnessID == protocol.HarnessAntigravityCLI {
		prompt := composePrompt(effectiveRun, invocation.SystemPrompt)
		var preparedMCP antigravityacp.PreparedMCPDirectory
		if invocation.MCP.URL != "" {
			var err error
			preparedMCP, err = antigravityacp.PrepareMCPDirectory(
				[]antigravityacp.MCPHTTPServer{{
					Name:    mcpServerName,
					URL:     invocation.MCP.URL,
					Headers: map[string]string{"Authorization": "Bearer " + invocation.MCP.Token},
				}},
				map[string][]string{
					mcpServerName: mcpserver.ToolNames(invocation.MCP.CanDelegate),
				},
			)
			if err != nil {
				return nil, noopCleanup, err
			}
		} else {
			preparedMCP = antigravityacp.PreparedMCPDirectory{Cleanup: noopCleanup}
		}
		arguments := antigravityacp.BuildCLIArguments(
			invocation.Workspace,
			preparedMCP,
			conversationID,
			effectiveRun.Model,
			antigravityModeForApprovalPolicy(invocation.approvalPolicy),
			prompt,
		)
		return arguments, preparedMCP.Cleanup, nil
	}
	arguments, err := harnessArgumentsForSession(effectiveRun, invocation.SystemPrompt, invocation.MCP, invocation.approvalPolicy, conversationID)
	return arguments, noopCleanup, err
}

func harnessArguments(run protocol.Run, systemPrompt string, mcpConfiguration mcpserver.Configuration, approvalPolicy string) ([]string, error) {
	return harnessArgumentsForSession(run, systemPrompt, mcpConfiguration, approvalPolicy, "")
}

func harnessArgumentsForSession(
	run protocol.Run,
	systemPrompt string,
	mcpConfiguration mcpserver.Configuration,
	approvalPolicy string,
	conversationID string,
) ([]string, error) {
	if conversationID != "" && !isValidNativeSessionID(conversationID) {
		return nil, errors.New("native session resume identity is invalid")
	}
	prompt := composePrompt(run, systemPrompt)
	switch run.HarnessID {
	case protocol.HarnessClaudeCLI:
		arguments := make([]string, 0, 14)
		if conversationID != "" {
			arguments = append(arguments, "--resume", conversationID)
		}
		arguments = append(arguments,
			"-p", prompt,
			"--output-format", "stream-json",
			"--verbose",
			"--permission-mode", nativeClaudePermissionMode(approvalPolicy),
			"--permission-prompts", "none",
		)
		if run.Model != "" && run.Model != "default" {
			arguments = append(arguments, "--model", run.Model)
		}
		if mcpConfiguration.URL != "" {
			configuration, err := json.Marshal(map[string]any{
				"mcpServers": map[string]any{
					mcpServerName: map[string]any{
						"type":    "http",
						"url":     mcpConfiguration.URL,
						"headers": map[string]string{"Authorization": "Bearer ${ORCHESTRA_MCP_TOKEN}"},
					},
				},
			})
			if err != nil {
				return nil, err
			}
			allowed := make([]string, 0, len(protocol.HubToolNames))
			for _, name := range mcpserver.ToolNames(mcpConfiguration.CanDelegate) {
				allowed = append(allowed, "mcp__"+mcpServerName+"__"+name)
			}
			arguments = append(arguments, "--mcp-config", string(configuration), "--allowedTools", strings.Join(allowed, ","))
		}
		return arguments, nil
	case protocol.HarnessCodexCLI:
		arguments := append([]string{"exec", "--json"}, nativeCodexSandboxArguments(approvalPolicy)...)
		if run.Model != "" && run.Model != "default" {
			arguments = append(arguments, "--model", run.Model)
		}
		if mcpConfiguration.URL != "" {
			arguments = append(arguments,
				"-c", "mcp_servers."+mcpServerName+".url="+strconv.Quote(mcpConfiguration.URL),
				"-c", `mcp_servers.`+mcpServerName+`.bearer_token_env_var="ORCHESTRA_MCP_TOKEN"`,
				"-c", "mcp_servers."+mcpServerName+".required=true",
				"-c", `mcp_servers.`+mcpServerName+`.default_tools_approval_mode="approve"`,
			)
		}
		if conversationID != "" {
			return append(arguments, "resume", conversationID, prompt), nil
		}
		return append(arguments, prompt), nil
	case protocol.HarnessOpenCodeCLI:
		// A private server makes each Orchestra run consume the model, MCP, and
		// permission configuration supplied to this process instead of inheriting
		// configuration from an already-running OpenCode background service.
		arguments := []string{"--standalone", "run", "--format", "json"}
		if conversationID != "" {
			arguments = append(arguments, "--session", conversationID)
		}
		if approvalPolicy == protocol.ApprovalPolicyAuto || approvalPolicy == protocol.ApprovalPolicyBypass {
			arguments = append(arguments, "--auto")
		}
		if run.Model != "" && run.Model != "default" {
			arguments = append(arguments, "--model", run.Model)
		}
		return append(arguments, prompt), nil
	default:
		return nil, fmt.Errorf("harness %s is not executable by Orchestra", run.HarnessID)
	}
}

func readEvents(reader io.Reader, harnessID string, output func(string), session func(string)) (string, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	final := ""
	var antigravityAccumulator strings.Builder
	var antigravityErr error
	sessionReported := false
	var eventErr error
	for scanner.Scan() {
		lineBytes := scanner.Bytes()
		if !sessionReported && session != nil {
			if identity := providerSessionIdentity(harnessID, lineBytes); identity != "" {
				sessionReported = true
				session(identity)
			}
		}
		if harnessID == protocol.HarnessAntigravityCLI {
			delta, fallbackResponse, eventErr := parseAntigravityNativeEvent(lineBytes, antigravityAccumulator.Len() > 0)
			if delta != "" {
				antigravityAccumulator.WriteString(delta)
				final = antigravityAccumulator.String()
				output(delta)
			} else if fallbackResponse != "" {
				antigravityAccumulator.WriteString(fallbackResponse)
				final = antigravityAccumulator.String()
				output(fallbackResponse)
			}
			if eventErr != nil {
				antigravityErr = eventErr
			}
			continue
		}
		readable := readableEvent(harnessID, lineBytes)
		if readable == "" {
			if eventErr == nil {
				eventErr = harnessEventError(harnessID, scanner.Bytes())
			}
			continue
		}
		final = readable
		output(readable)
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return final, scanErr
	}
	if antigravityErr != nil {
		return final, antigravityErr
	}
	return final, eventErr
}

var providerSessionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func providerSessionIdentity(harnessID string, line []byte) string {
	var event struct {
		Type              string `json:"type"`
		Event             string `json:"event"`
		SessionID         string `json:"session_id"`
		OpenCodeSessionID string `json:"sessionID"`
		ThreadID          string `json:"thread_id"`
		ConversationID    string `json:"conversation_id"`
		Result            *struct {
			ConversationID string `json:"conversation_id"`
		} `json:"result"`
	}
	if json.Unmarshal(line, &event) != nil {
		return ""
	}
	identity := ""
	switch harnessID {
	case protocol.HarnessClaudeCLI:
		identity = event.SessionID
	case protocol.HarnessCodexCLI:
		if event.Type == "thread.started" {
			identity = event.ThreadID
		}
	case protocol.HarnessOpenCodeCLI:
		identity = event.OpenCodeSessionID
	case protocol.HarnessAntigravityCLI:
		identity = event.ConversationID
		if identity == "" && event.Result != nil {
			identity = event.Result.ConversationID
		}
	}
	if !providerSessionPattern.MatchString(identity) {
		return ""
	}
	return identity
}

func parseAntigravityNativeEvent(line []byte, alreadyStreamed bool) (string, string, error) {
	var envelope struct {
		Event      string `json:"event"`
		StepUpdate *struct {
			StepType  string `json:"step_type"`
			TextDelta string `json:"text_delta"`
		} `json:"step_update"`
		Result *struct {
			Status        string `json:"status"`
			Response      string `json:"response"`
			Error         string `json:"error"`
			DeniedActions []struct {
				Action      string `json:"action"`
				DisplayName string `json:"display_name"`
			} `json:"denied_actions"`
		} `json:"result"`
	}
	if json.Unmarshal(line, &envelope) != nil {
		return "", "", nil
	}
	switch envelope.Event {
	case "step_update":
		if envelope.StepUpdate == nil || envelope.StepUpdate.TextDelta == "" {
			return "", "", nil
		}
		lowerType := strings.ToLower(envelope.StepUpdate.StepType)
		if strings.Contains(lowerType, "thought") || strings.Contains(lowerType, "think") {
			return "", "", nil
		}
		return envelope.StepUpdate.TextDelta, "", nil
	case "result":
		if envelope.Result == nil {
			return "", "", nil
		}
		fallback := ""
		if !alreadyStreamed && envelope.Result.Response != "" {
			fallback = envelope.Result.Response
		}
		if envelope.Result.Status != "" && !strings.EqualFold(envelope.Result.Status, "SUCCESS") {
			message := strings.TrimSpace(envelope.Result.Error)
			if message == "" {
				message = "agy turn finished with status " + envelope.Result.Status
			}
			return "", fallback, errors.New(message)
		}
		if len(envelope.Result.DeniedActions) > 0 && !alreadyStreamed && strings.TrimSpace(fallback) == "" {
			return "", "", errors.New("agy denied a required tool action in headless mode")
		}
		return "", fallback, nil
	default:
		return "", "", nil
	}
}

func readableEvent(harnessID string, line []byte) string {
	var event map[string]any
	if json.Unmarshal(line, &event) != nil {
		return ""
	}
	if harnessID == protocol.HarnessClaudeCLI {
		if event["type"] == "result" {
			result, _ := event["result"].(string)
			return result
		}
		if event["type"] == "assistant" {
			message, _ := event["message"].(map[string]any)
			content, _ := message["content"].([]any)
			var text strings.Builder
			for _, rawPart := range content {
				part, _ := rawPart.(map[string]any)
				if part["type"] == "text" {
					value, _ := part["text"].(string)
					text.WriteString(value)
				}
			}
			return text.String()
		}
	}
	if harnessID == protocol.HarnessCodexCLI && event["type"] == "item.completed" {
		item, _ := event["item"].(map[string]any)
		switch item["type"] {
		case "agent_message":
			text, _ := item["text"].(string)
			return text
		case "command_execution":
			command, _ := item["command"].(string)
			if command == "" {
				command = "command"
			}
			return "Ran " + command + "\n"
		}
	}
	if harnessID == protocol.HarnessOpenCodeCLI && event["type"] == "text" {
		part, _ := event["part"].(map[string]any)
		text, _ := part["text"].(string)
		return text
	}
	return ""
}

func harnessEventError(harnessID string, line []byte) error {
	if harnessID != protocol.HarnessOpenCodeCLI {
		return nil
	}
	var event struct {
		Type  string `json:"type"`
		Error struct {
			Data struct {
				Message string `json:"message"`
			} `json:"data"`
		} `json:"error"`
	}
	if json.Unmarshal(line, &event) != nil || event.Type != "error" || strings.TrimSpace(event.Error.Data.Message) == "" {
		return nil
	}
	return errors.New(strings.TrimSpace(event.Error.Data.Message))
}
