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
	arguments, err := harnessArguments(run, invocation.SystemPrompt, mcpConfiguration, invocation.approvalPolicy)
	if err != nil {
		return "", err
	}
	command := exec.CommandContext(operationContext, binary, arguments...)
	configureProcessCancellation(command)
	command.Dir = invocation.Workspace
	command.Env = adapterEnvironment(os.Environ(), nil)
	if mcpConfiguration.URL != "" {
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
		return "", readErr
	}
	if waitErr != nil {
		if message := strings.TrimSpace(stderr.String()); message != "" {
			return "", errors.New(message)
		}
		return "", waitErr
	}
	if strings.TrimSpace(final) == "" {
		return emptyResponse, nil
	}
	return strings.TrimSpace(final), nil
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

func harnessArguments(run protocol.Run, systemPrompt string, mcpConfiguration mcpserver.Configuration, approvalPolicy string) ([]string, error) {
	prompt := composePrompt(run, systemPrompt)
	switch run.HarnessID {
	case "claude-cli":
		arguments := []string{"-p", prompt, "--output-format", "stream-json", "--verbose", "--permission-mode", nativeClaudePermissionMode(approvalPolicy), "--permission-prompts", "none"}
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
	case "codex-cli":
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
		return append(arguments, prompt), nil
	default:
		return nil, fmt.Errorf("harness %s is not executable by Orchestra", run.HarnessID)
	}
}

func readEvents(reader io.Reader, harnessID string, output func(string), session func(string)) (string, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	final := ""
	sessionReported := false
	for scanner.Scan() {
		if !sessionReported && session != nil {
			if identity := providerSessionIdentity(harnessID, scanner.Bytes()); identity != "" {
				sessionReported = true
				session(identity)
			}
		}
		readable := readableEvent(harnessID, scanner.Bytes())
		if readable == "" {
			continue
		}
		final = readable
		output(readable)
	}
	return final, scanner.Err()
}

var providerSessionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func providerSessionIdentity(harnessID string, line []byte) string {
	var event struct {
		Type      string `json:"type"`
		SessionID string `json:"session_id"`
		ThreadID  string `json:"thread_id"`
	}
	if json.Unmarshal(line, &event) != nil {
		return ""
	}
	identity := ""
	switch harnessID {
	case "claude-cli":
		identity = event.SessionID
	case "codex-cli":
		if event.Type == "thread.started" {
			identity = event.ThreadID
		}
	}
	if !providerSessionPattern.MatchString(identity) {
		return ""
	}
	return identity
}

func readableEvent(harnessID string, line []byte) string {
	var event map[string]any
	if json.Unmarshal(line, &event) != nil {
		return ""
	}
	if harnessID == "claude-cli" {
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
	if harnessID == "codex-cli" && event["type"] == "item.completed" {
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
	return ""
}
