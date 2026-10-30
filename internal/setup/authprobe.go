package setup

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const authProbeTimeout = 5 * time.Second

const authProbeMaximumOutputBytes = 4096

type AuthReadiness string

const (
	AuthReadinessReady    AuthReadiness = "ready"
	AuthReadinessNotReady AuthReadiness = "not-ready"
	AuthReadinessUnknown  AuthReadiness = "unknown"
)

type AuthProbeSpecification struct {
	Arguments       []string
	SuccessExitCode int
}

var AuthProbeAllowlist = map[string]AuthProbeSpecification{
	protocol.HarnessClaudeCLI:   {Arguments: []string{"--version"}, SuccessExitCode: 0},
	protocol.HarnessCodexCLI:    {Arguments: []string{"--version"}, SuccessExitCode: 0},
	protocol.HarnessOpenCodeCLI: {Arguments: []string{"--version"}, SuccessExitCode: 0},
}

func RunAuthProbe(operationContext context.Context, binaryPath string, arguments []string, successExitCode int) AuthReadiness {
	readiness, _ := runProbeCapturingOutput(operationContext, binaryPath, arguments, successExitCode)
	return readiness
}

func runProbeCapturingOutput(operationContext context.Context, binaryPath string, arguments []string, successExitCode int) (AuthReadiness, string) {
	if !filepath.IsAbs(binaryPath) {
		return AuthReadinessUnknown, ""
	}
	timeoutContext, cancel := context.WithTimeout(operationContext, authProbeTimeout)
	defer cancel()
	command := exec.CommandContext(timeoutContext, binaryPath, arguments...)
	command.Env = protocol.ScrubChildEnvironment(os.Environ())
	var captured authProbeOutput
	command.Stdout = &captured
	command.Stderr = &captured
	runErr := command.Run()
	if timeoutContext.Err() == context.DeadlineExceeded {
		return AuthReadinessUnknown, ""
	}
	output := captured.String()
	if protocol.LooksSecretLike(output) {
		return AuthReadinessUnknown, ""
	}
	exitCode := 0
	if runErr != nil {
		var exitErr *exec.ExitError
		if !errors.As(runErr, &exitErr) {
			return AuthReadinessUnknown, ""
		}
		exitCode = exitErr.ExitCode()
	}
	if exitCode == successExitCode {
		return AuthReadinessReady, output
	}
	return AuthReadinessNotReady, ""
}

type authProbeOutput struct {
	data []byte
}

func (output *authProbeOutput) Write(chunk []byte) (int, error) {
	if remaining := authProbeMaximumOutputBytes - len(output.data); remaining > 0 {
		held := min(len(chunk), remaining)
		output.data = append(output.data, chunk[:held]...)
	}
	return len(chunk), nil
}

func (output *authProbeOutput) String() string {
	return string(output.data)
}
