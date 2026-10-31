package readiness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const (
	probeTimeout           = 5 * time.Second
	probeMaximumOutput     = 4096
	probeDefinitionVersion = "1"
	rawValueMaximumBytes   = 256
	normalizedMaximumBytes = 64
	diagnosticMaximumBytes = 512
)

func looksSecretLike(text string) bool {
	return protocol.LooksSecretLike(text)
}

type Probe struct {
	CapabilityID       string
	Label              string
	Binary             string
	Arguments          []string
	Timeout            time.Duration
	MaximumOutputBytes int
	Parse              func(rawOutput string) (normalized string, ok bool)
}

var AllowlistedProbes = []Probe{
	{CapabilityID: "go", Label: "Go", Binary: "go", Arguments: []string{"version"}, Timeout: probeTimeout, MaximumOutputBytes: probeMaximumOutput, Parse: parseGoVersion},
	{CapabilityID: "git", Label: "Git", Binary: "git", Arguments: []string{"--version"}, Timeout: probeTimeout, MaximumOutputBytes: probeMaximumOutput, Parse: GenericVersionParser},
	{CapabilityID: "node", Label: "Node.js", Binary: "node", Arguments: []string{"--version"}, Timeout: probeTimeout, MaximumOutputBytes: probeMaximumOutput, Parse: parseNodeVersion},
	{CapabilityID: "pnpm", Label: "pnpm", Binary: "pnpm", Arguments: []string{"--version"}, Timeout: probeTimeout, MaximumOutputBytes: probeMaximumOutput, Parse: GenericVersionParser},
	{CapabilityID: "gcc", Label: "GCC", Binary: "gcc", Arguments: []string{"--version"}, Timeout: probeTimeout, MaximumOutputBytes: probeMaximumOutput, Parse: parseGCCVersion},
	{CapabilityID: "clang", Label: "Clang", Binary: "clang", Arguments: []string{"--version"}, Timeout: probeTimeout, MaximumOutputBytes: probeMaximumOutput, Parse: parseClangVersion},
	{CapabilityID: "xcodebuild", Label: "Xcode", Binary: "xcodebuild", Arguments: []string{"-version"}, Timeout: probeTimeout, MaximumOutputBytes: probeMaximumOutput, Parse: parseXcodeVersion},
}

type probeExecution struct {
	Output    string
	Err       error
	TimedOut  bool
	Oversized bool
}

func executeProbe(operationContext context.Context, binary string, arguments []string, timeout time.Duration, maximumOutputBytes int) probeExecution {
	path, err := exec.LookPath(binary)
	if err != nil {
		return probeExecution{Err: err}
	}
	timeoutContext, cancel := context.WithTimeout(operationContext, timeout)
	defer cancel()
	command := exec.CommandContext(timeoutContext, path, arguments...)
	command.Env = protocol.ScrubChildEnvironment(os.Environ())
	reader, writer := io.Pipe()
	command.Stdout = writer
	command.Stderr = writer
	if err := command.Start(); err != nil {
		reader.Close()
		writer.Close()
		return probeExecution{Err: err, TimedOut: timeoutContext.Err() == context.DeadlineExceeded}
	}
	var captured bytes.Buffer
	copyDone := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(&captured, io.LimitReader(reader, int64(maximumOutputBytes)+1))
		reader.Close()
		copyDone <- copyErr
	}()
	runErr := command.Wait()
	writer.Close()
	if copyErr := <-copyDone; runErr == nil {
		runErr = copyErr
	}
	return probeExecution{
		Output:    captured.String()[:min(captured.Len(), maximumOutputBytes)],
		Err:       runErr,
		TimedOut:  timeoutContext.Err() == context.DeadlineExceeded,
		Oversized: captured.Len() > maximumOutputBytes,
	}
}

func RunProbe(operationContext context.Context, probe Probe) protocol.NodeCapabilityEvidence {
	execution := executeProbe(operationContext, probe.Binary, probe.Arguments, probe.Timeout, probe.MaximumOutputBytes)
	evidence := protocol.NodeCapabilityEvidence{
		CapabilityID:           probe.CapabilityID,
		Source:                 protocol.CapabilityEvidenceSourceProbe,
		ProbeDefinitionVersion: probeDefinitionVersion,
	}
	switch {
	case errors.Is(execution.Err, exec.ErrNotFound):
		evidence.Diagnostic = "executable not found on PATH"
	case execution.TimedOut:
		evidence.Diagnostic = fmt.Sprintf("probe timed out after %s", probe.Timeout)
	case execution.Oversized:
		evidence.Diagnostic = fmt.Sprintf("probe output exceeded %d bytes", probe.MaximumOutputBytes)
	case execution.Err != nil:
		evidence.Diagnostic = boundedString(execution.Output, diagnosticMaximumBytes)
		if looksSecretLike(evidence.Diagnostic) {
			evidence.Diagnostic = "probe diagnostic withheld: output looked secret-like"
		}
	default:
		evidence.Success = true
		evidence.RawValue = boundedString(execution.Output, rawValueMaximumBytes)
		if normalized, ok := probe.Parse(execution.Output); ok {
			evidence.NormalizedValue = boundedString(normalized, normalizedMaximumBytes)
		}
		if looksSecretLike(evidence.RawValue) || looksSecretLike(evidence.NormalizedValue) {
			evidence.Success = false
			evidence.RawValue = ""
			evidence.NormalizedValue = ""
			evidence.Diagnostic = "probe output withheld: output looked secret-like"
		}
	}
	return evidence
}

func boundedString(value string, maximumBytes int) string {
	if len(value) <= maximumBytes {
		return value
	}
	truncated := value[:maximumBytes]
	for len(truncated) > 0 {
		character, size := utf8.DecodeLastRuneInString(truncated)
		if character != utf8.RuneError || size > 1 {
			break
		}
		truncated = truncated[:len(truncated)-1]
	}
	return truncated
}

var dottedNumberPattern = regexp.MustCompile(`\d+(\.\d+)*`)

func firstNormalizedVersion(text string) (string, bool) {
	match := dottedNumberPattern.FindString(text)
	if match == "" || !protocol.IsNormalizedVersion(match) {
		return "", false
	}
	return match, true
}

var GenericVersionParser = firstNormalizedVersion

func parseGoVersion(rawOutput string) (string, bool) {
	return firstNormalizedVersion(strings.TrimPrefix(rawOutput, "go version go"))
}

func parseNodeVersion(rawOutput string) (string, bool) {
	return firstNormalizedVersion(strings.TrimPrefix(strings.TrimSpace(rawOutput), "v"))
}

func parseGCCVersion(rawOutput string) (string, bool) {
	firstLine, _, _ := strings.Cut(rawOutput, "\n")
	afterFirstParen := firstLine
	if _, after, ok := strings.Cut(firstLine, ")"); ok {
		afterFirstParen = after
	}
	return firstNormalizedVersion(afterFirstParen)
}

func parseClangVersion(rawOutput string) (string, bool) {
	lowered := strings.ToLower(rawOutput)
	text := rawOutput
	if index := strings.Index(lowered, "version"); index >= 0 {
		text = rawOutput[index+len("version"):]
	} else if index := strings.Index(lowered, "clang"); index >= 0 {
		text = rawOutput[index+len("clang"):]
	}
	return firstNormalizedVersion(text)
}

func parseXcodeVersion(rawOutput string) (string, bool) {
	firstLine, _, _ := strings.Cut(rawOutput, "\n")
	return firstNormalizedVersion(firstLine)
}
