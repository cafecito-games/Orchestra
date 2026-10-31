package main

import (
	"bytes"
	"io"
	"os"
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
