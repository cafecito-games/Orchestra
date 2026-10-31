package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunMain_Help(t *testing.T) {
	originalArgs := os.Args
	t.Cleanup(func() {
		os.Args = originalArgs
	})
	os.Args = []string{"orchestra", "--help"}

	discardWriter := io.Discard
	exitCode := runWithIO(os.Args[1:], nil, discardWriter, discardWriter, nil)
	assert.Equal(t, 0, exitCode)

	// Direct invocation of runMain with restored/mocked stdio
	originalStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writePipe
	t.Cleanup(func() {
		os.Stdout = originalStdout
		_ = readPipe.Close()
	})

	exitCode = runMain()
	_ = writePipe.Close()
	assert.Equal(t, 0, exitCode)
}

func TestRunMain_Version(t *testing.T) {
	originalArgs := os.Args
	t.Cleanup(func() {
		os.Args = originalArgs
	})
	os.Args = []string{"orchestra", "version"}

	originalStdout := os.Stdout
	readPipe, writePipe, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = writePipe
	t.Cleanup(func() {
		os.Stdout = originalStdout
		_ = readPipe.Close()
	})

	exitCode := runMain()
	_ = writePipe.Close()
	assert.Equal(t, 0, exitCode)
}

func TestRunMain_UnknownSubcommand(t *testing.T) {
	originalArgs := os.Args
	t.Cleanup(func() {
		os.Args = originalArgs
	})
	os.Args = []string{"orchestra", "unknown-subcommand-xyz"}

	originalStderr := os.Stderr
	readPipe, writePipe, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = writePipe
	t.Cleanup(func() {
		os.Stderr = originalStderr
		_ = readPipe.Close()
	})

	exitCode := runMain()
	_ = writePipe.Close()
	assert.NotEqual(t, 0, exitCode)
}

func TestRunWithIO_HelpAndVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := runWithIO([]string{"--help"}, nil, &stdout, &stderr, nil)
	assert.Equal(t, 0, exitCode)
	assert.Contains(t, stdout.String(), "Usage: orchestra <command> [flags]")

	stdout.Reset()
	stderr.Reset()
	exitCode = runWithIO([]string{"version"}, nil, &stdout, &stderr, nil)
	assert.Equal(t, 0, exitCode)
	assert.Contains(t, stdout.String(), "orchestra ")

	stdout.Reset()
	stderr.Reset()
	exitCode = runWithIO([]string{"unknown-subcommand"}, nil, &stdout, &stderr, nil)
	assert.Equal(t, 2, exitCode)
	assert.Contains(t, stderr.String(), "unknown subcommand")
}

func TestBuildVersionInjectionViaLinkerFlags(t *testing.T) {
	binaryName := "orchestra"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	binaryPath := filepath.Join(t.TempDir(), binaryName)

	linkerFlags := "-X github.com/cafecito-games/Orchestra/internal/protocol.BuildVersion=2.5.1 " +
		"-X github.com/cafecito-games/Orchestra/internal/protocol.BuildCommit=abcdef0123456789 " +
		"-X github.com/cafecito-games/Orchestra/internal/protocol.BuildDate=2026-10-31T13:30:00Z " +
		"-X github.com/cafecito-games/Orchestra/internal/protocol.BuildTreeState=clean"

	buildCommand := exec.CommandContext(t.Context(), "go", "build", "-ldflags", linkerFlags, "-o", binaryPath, ".")
	buildOutput, err := buildCommand.CombinedOutput()
	require.NoError(t, err, "go build failed: %s", string(buildOutput))

	textCommand := exec.CommandContext(t.Context(), binaryPath, "version")
	textOutput, err := textCommand.CombinedOutput()
	require.NoError(t, err, "orchestra version failed: %s", string(textOutput))
	assert.Equal(t, "orchestra 2.5.1 (protocol v1, commit abcdef012345, built 2026-10-31T13:30:00Z)\n", string(textOutput))

	jsonCommand := exec.CommandContext(t.Context(), binaryPath, "version", "--json")
	jsonOutput, err := jsonCommand.CombinedOutput()
	require.NoError(t, err, "orchestra version --json failed: %s", string(jsonOutput))

	var payload map[string]string
	require.NoError(t, json.Unmarshal(jsonOutput, &payload))
	assert.Equal(t, "2.5.1", payload["version"])
	assert.Equal(t, "1", payload["protocolVersion"])
	assert.Equal(t, "abcdef0123456789", payload["commit"])
	assert.Equal(t, "2026-10-31T13:30:00Z", payload["date"])
	assert.Equal(t, "clean", payload["treeState"])
}
