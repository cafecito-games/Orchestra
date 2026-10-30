package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Script struct {
	Steps []Step `json:"steps"`
}

type Step struct {
	When            *Condition      `json:"when,omitempty"`
	Message         string          `json:"message,omitempty"`
	Thought         string          `json:"thought,omitempty"`
	Update          json.RawMessage `json:"update,omitempty"`
	Call            string          `json:"call,omitempty"`
	Arguments       json.RawMessage `json:"arguments,omitempty"`
	As              string          `json:"as,omitempty"`
	AllowError      bool            `json:"allowError,omitempty"`
	ExpectErrorCode string          `json:"expectErrorCode,omitempty"`
	WaitFor         json.RawMessage `json:"waitFor,omitempty"`
	Permission      *Permission     `json:"permission,omitempty"`
	Gate            string          `json:"gate,omitempty"`
	WriteFile       *WriteFile      `json:"writeFile,omitempty"`
	RemoveFile      string          `json:"removeFile,omitempty"`
	Raw             string          `json:"raw,omitempty"`
	Crash           int             `json:"crash,omitempty"`
	Hang            bool            `json:"hang,omitempty"`
}

type Condition struct {
	Path   string `json:"path"`
	Equals any    `json:"equals"`
}

type Permission struct {
	ToolCallID string             `json:"toolCallId"`
	Title      string             `json:"title"`
	Kind       string             `json:"kind"`
	Options    []PermissionOption `json:"options"`
	As         string             `json:"as"`
}

type PermissionOption struct {
	OptionID string `json:"optionId"`
	Name     string `json:"name"`
	Kind     string `json:"kind"`
}

type WriteFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

var (
	errRunCancelled  = errors.New("the run was cancelled")
	scriptBlockRegex = regexp.MustCompile(`(?s)<fake-script>(.*?)</fake-script>`)
	referenceRegex   = regexp.MustCompile(`\{\{([A-Za-z0-9_.*-]+)\}\}`)
)

type scriptedCrash struct {
	code int
}

func (crashError scriptedCrash) Error() string {
	return "scripted crash with exit code " + strconv.Itoa(crashError.code)
}

func extractScript(promptText string) (Script, error) {
	blocks := scriptBlockRegex.FindAllStringSubmatch(promptText, -1)
	if len(blocks) == 0 {
		return Script{Steps: []Step{{Message: "no script was provided"}}}, nil
	}
	var script Script
	if err := json.Unmarshal([]byte(blocks[len(blocks)-1][1]), &script); err != nil {
		return Script{}, fmt.Errorf("decode fake-script: %w", err)
	}
	return script, nil
}

type executionHost interface {
	message(text string) error
	thought(text string) error
	update(raw json.RawMessage) error
	permission(operationContext context.Context, request map[string]any) (any, error)
	raw(text string) error
	cancelled() <-chan struct{}
}

type scriptEngine struct {
	host             executionHost
	mcpClient        *hubMCPClient
	prompt           string
	workingDirectory string
	variables        map[string]any
	unconsumedEvents []any
}

func newScriptEngine(
	host executionHost,
	mcpClient *hubMCPClient,
	prompt string,
	workingDirectory string,
	sessionID string,
) *scriptEngine {
	return &scriptEngine{
		host:             host,
		mcpClient:        mcpClient,
		prompt:           prompt,
		workingDirectory: workingDirectory,
		variables: map[string]any{
			"cwd":     workingDirectory,
			"session": sessionID,
		},
	}
}

func (engine *scriptEngine) run(operationContext context.Context, script Script) error {
	for index, step := range script.Steps {
		select {
		case <-engine.host.cancelled():
			return errRunCancelled
		default:
		}
		if step.When != nil {
			holds, err := engine.evaluateCondition(*step.When)
			if err != nil {
				return fmt.Errorf("step %d condition: %w", index, err)
			}
			if !holds {
				continue
			}
		}
		if err := engine.executeStep(operationContext, step); err != nil {
			if errors.Is(err, errRunCancelled) {
				return err
			}
			var crashError scriptedCrash
			if errors.As(err, &crashError) {
				return err
			}
			return fmt.Errorf("step %d: %w", index, err)
		}
	}
	return nil
}

func (engine *scriptEngine) executeStep(operationContext context.Context, step Step) error {
	switch {
	case step.Message != "":
		renderedText, err := engine.renderText(step.Message)
		if err != nil {
			return err
		}
		return engine.host.message(renderedText)
	case step.Thought != "":
		renderedText, err := engine.renderText(step.Thought)
		if err != nil {
			return err
		}
		return engine.host.thought(renderedText)
	case len(step.Update) > 0:
		renderedJSON, err := engine.renderJSON(step.Update)
		if err != nil {
			return err
		}
		return engine.host.update(renderedJSON)
	case step.Call != "":
		return engine.executeCall(operationContext, step)
	case len(step.WaitFor) > 0:
		return engine.executeWaitFor(operationContext, step)
	case step.Permission != nil:
		return engine.executePermission(operationContext, *step.Permission)
	case step.Gate != "":
		return engine.waitForGate(step.Gate)
	case step.WriteFile != nil:
		relativePath, err := engine.renderText(step.WriteFile.Path)
		if err != nil {
			return err
		}
		localPath, err := engine.resolveLocalPath(relativePath)
		if err != nil {
			return err
		}
		contentText, err := engine.renderText(step.WriteFile.Content)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
			return err
		}
		return os.WriteFile(localPath, []byte(contentText), 0o644)
	case step.RemoveFile != "":
		localPath, err := engine.resolveLocalPath(step.RemoveFile)
		if err != nil {
			return err
		}
		return os.Remove(localPath)
	case step.Raw != "":
		return engine.host.raw(step.Raw)
	case step.Crash != 0:
		fmt.Fprintln(os.Stderr, "fakeharness: scripted crash")
		return scriptedCrash{code: step.Crash}
	case step.Hang:
		<-engine.host.cancelled()
		return errRunCancelled
	default:
		return errors.New("script step specifies no action")
	}
}

func (engine *scriptEngine) executeCall(operationContext context.Context, step Step) error {
	if engine.mcpClient == nil {
		return errNoMCPServer
	}
	arguments := any(map[string]any{})
	if len(step.Arguments) > 0 {
		var decoded any
		if err := json.Unmarshal(step.Arguments, &decoded); err != nil {
			return fmt.Errorf("decode %s arguments: %w", step.Call, err)
		}
		rendered, err := engine.renderValue(decoded)
		if err != nil {
			return err
		}
		arguments = rendered
	}
	outcome, err := engine.mcpClient.callTool(operationContext, step.Call, arguments)
	if err != nil {
		return err
	}
	if step.ExpectErrorCode != "" {
		code := extractToolErrorCode(outcome.Value)
		if !outcome.IsError || code != step.ExpectErrorCode {
			return fmt.Errorf("tool %s error code=%q, expected %q", step.Call, code, step.ExpectErrorCode)
		}
	} else if outcome.IsError && !step.AllowError {
		encodedBytes, _ := json.Marshal(outcome.Value)
		return fmt.Errorf("tool %s returned error: %s", step.Call, encodedBytes)
	}
	if step.As != "" {
		engine.variables[step.As] = outcome.Value
	}
	return nil
}

func extractToolErrorCode(value any) string {
	object, _ := value.(map[string]any)
	if code, ok := object["code"].(string); ok && code != "" {
		return code
	}
	nestedError, _ := object["error"].(map[string]any)
	code, _ := nestedError["code"].(string)
	return code
}

const waitStepDeadline = 2 * time.Minute

func (engine *scriptEngine) executeWaitFor(operationContext context.Context, step Step) error {
	if engine.mcpClient == nil {
		return errNoMCPServer
	}
	var rawPattern any
	if err := json.Unmarshal(step.WaitFor, &rawPattern); err != nil {
		return fmt.Errorf("decode waitFor: %w", err)
	}
	pattern, err := engine.renderValue(rawPattern)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(waitStepDeadline)
	for {
		for index, event := range engine.unconsumedEvents {
			if valueContains(event, pattern) {
				engine.unconsumedEvents = append(engine.unconsumedEvents[:index:index], engine.unconsumedEvents[index+1:]...)
				if step.As != "" {
					engine.variables[step.As] = event
				}
				return nil
			}
		}
		engine.unconsumedEvents = nil
		if time.Now().After(deadline) {
			return errors.New("waitFor deadline exceeded without matching event")
		}
		arguments := map[string]any{
			"timeoutMilliseconds": 1500,
			"maximumEvents":       50,
		}
		if cursor, hasCursor := engine.variables["_cursor"].(string); hasCursor && cursor != "" {
			arguments["cursor"] = cursor
		}
		outcome, err := engine.mcpClient.callTool(operationContext, "wait_for_task_events", arguments)
		if err != nil {
			return err
		}
		if outcome.IsError {
			encodedBytes, _ := json.Marshal(outcome.Value)
			return fmt.Errorf("wait_for_task_events failed: %s", encodedBytes)
		}
		page, _ := outcome.Value.(map[string]any)
		if nextCursor, ok := page["cursor"].(string); ok && nextCursor != "" {
			engine.variables["_cursor"] = nextCursor
		}
		events, _ := page["events"].([]any)
		engine.unconsumedEvents = events

		select {
		case <-engine.host.cancelled():
			return errRunCancelled
		default:
		}
	}
}

func (engine *scriptEngine) executePermission(operationContext context.Context, permissionRequest Permission) error {
	options := make([]any, 0, len(permissionRequest.Options))
	for _, option := range permissionRequest.Options {
		options = append(options, map[string]any{
			"optionId": option.OptionID,
			"name":     option.Name,
			"kind":     option.Kind,
		})
	}
	payload := map[string]any{
		"toolCall": map[string]any{
			"toolCallId": permissionRequest.ToolCallID,
			"title":      permissionRequest.Title,
			"kind":       permissionRequest.Kind,
		},
		"options": options,
	}
	response, err := engine.host.permission(operationContext, payload)
	if err != nil {
		return err
	}
	if permissionRequest.As != "" {
		engine.variables[permissionRequest.As] = response
	}
	return nil
}

func (engine *scriptEngine) waitForGate(gateName string) error {
	gateDirectory := os.Getenv(gateDirectoryVariable)
	if gateDirectory == "" || gateName != filepath.Base(gateName) {
		return errors.New("gate requires ORCHESTRA_FAKE_GATE_DIRECTORY and a plain gate name")
	}
	gatePath := filepath.Join(gateDirectory, gateName)
	deadline := time.Now().Add(waitStepDeadline)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(gatePath); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("gate %s timed out", gateName)
		}
		select {
		case <-engine.host.cancelled():
			return errRunCancelled
		case <-ticker.C:
		}
	}
}

func (engine *scriptEngine) resolveLocalPath(relativePath string) (string, error) {
	if relativePath == "" || filepath.IsAbs(relativePath) || filepath.Clean(relativePath) != relativePath || strings.HasPrefix(relativePath, "..") {
		return "", errors.New("file path must be clean and relative to working directory")
	}
	return filepath.Join(engine.workingDirectory, relativePath), nil
}

func (engine *scriptEngine) renderText(templateText string) (string, error) {
	rendered, err := engine.renderValue(templateText)
	if err != nil {
		return "", err
	}
	if textValue, isString := rendered.(string); isString {
		return textValue, nil
	}
	encodedBytes, err := json.Marshal(rendered)
	return string(encodedBytes), err
}

func (engine *scriptEngine) renderJSON(raw json.RawMessage) (json.RawMessage, error) {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, err
	}
	rendered, err := engine.renderValue(decoded)
	if err != nil {
		return nil, err
	}
	return json.Marshal(rendered)
}

func (engine *scriptEngine) renderValue(value any) (any, error) {
	switch typed := value.(type) {
	case string:
		if strings.Contains(typed, "<fake-script>") {
			return typed, nil
		}
		if match := referenceRegex.FindStringSubmatch(typed); match != nil && match[0] == typed {
			return engine.lookupPath(match[1])
		}
		var lookupFailure error
		interpolated := referenceRegex.ReplaceAllStringFunc(typed, func(token string) string {
			resolved, err := engine.lookupPath(strings.Trim(token, "{}"))
			if err != nil {
				lookupFailure = err
				return ""
			}
			if textValue, isString := resolved.(string); isString {
				return textValue
			}
			encodedBytes, _ := json.Marshal(resolved)
			return string(encodedBytes)
		})
		return interpolated, lookupFailure
	case []any:
		renderedSlice := make([]any, len(typed))
		for index, item := range typed {
			renderedItem, err := engine.renderValue(item)
			if err != nil {
				return nil, err
			}
			renderedSlice[index] = renderedItem
		}
		return renderedSlice, nil
	case map[string]any:
		renderedMap := make(map[string]any, len(typed))
		for key, item := range typed {
			renderedItem, err := engine.renderValue(item)
			if err != nil {
				return nil, err
			}
			renderedMap[key] = renderedItem
		}
		return renderedMap, nil
	default:
		return value, nil
	}
}

func (engine *scriptEngine) lookupPath(dottedPath string) (any, error) {
	values, err := resolveSegments(engine.variables, strings.Split(dottedPath, "."))
	if err != nil {
		return nil, fmt.Errorf("reference %s: %w", dottedPath, err)
	}
	if len(values) != 1 {
		return nil, fmt.Errorf("reference %s matched %d values", dottedPath, len(values))
	}
	return values[0], nil
}

func (engine *scriptEngine) evaluateCondition(condition Condition) (bool, error) {
	expected, err := engine.renderValue(condition.Equals)
	if err != nil {
		return false, err
	}
	values, err := resolveSegments(engine.variables, strings.Split(condition.Path, "."))
	if err != nil || len(values) == 0 {
		return false, nil
	}
	for _, actual := range values {
		if !valuesEqualJSON(actual, expected) {
			return false, nil
		}
	}
	return true, nil
}

func resolveSegments(value any, segments []string) ([]any, error) {
	if len(segments) == 0 {
		return []any{value}, nil
	}
	head, rest := segments[0], segments[1:]
	switch typed := value.(type) {
	case map[string]any:
		child, exists := typed[head]
		if !exists {
			return nil, fmt.Errorf("missing field %s", head)
		}
		return resolveSegments(child, rest)
	case []any:
		if head == "*" {
			var combined []any
			for _, element := range typed {
				resolved, err := resolveSegments(element, rest)
				if err != nil {
					return nil, err
				}
				combined = append(combined, resolved...)
			}
			return combined, nil
		}
		index, err := strconv.Atoi(head)
		if err != nil || index < 0 || index >= len(typed) {
			return nil, fmt.Errorf("missing index %s", head)
		}
		return resolveSegments(typed[index], rest)
	default:
		return nil, fmt.Errorf("cannot traverse %s", head)
	}
}

func valueContains(actual, pattern any) bool {
	expectedMap, isObject := pattern.(map[string]any)
	if !isObject {
		return valuesEqualJSON(actual, pattern)
	}
	actualMap, isActualObject := actual.(map[string]any)
	if !isActualObject {
		return false
	}
	for key, expectedChild := range expectedMap {
		if !valueContains(actualMap[key], expectedChild) {
			return false
		}
	}
	return true
}

func valuesEqualJSON(left, right any) bool {
	leftBytes, leftErr := json.Marshal(left)
	rightBytes, rightErr := json.Marshal(right)
	if leftErr != nil || rightErr != nil {
		return reflect.DeepEqual(left, right)
	}
	return string(leftBytes) == string(rightBytes)
}
