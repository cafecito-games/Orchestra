package harness

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestACPDriver_AdapterVerificationFailureBeforeLaunch(t *testing.T) {
	temporaryDirectory := t.TempDir()
	fakeBinary := filepath.Join(temporaryDirectory, "fake-adapter")
	require.NoError(t, os.WriteFile(fakeBinary, []byte("#!/bin/sh\nexit 0\n"), 0o755))

	verificationError := errors.New("cryptographic ledger verification failed")
	driver := NewACPDriver(ACPDriverOptions{
		Adapters: map[string]ACPAdapter{
			protocol.HarnessClaudeCLI: {
				Binary: fakeBinary,
				Verify: func() error {
					return verificationError
				},
			},
		},
		Providers: DefaultACPProviders(),
	})

	err := driver.Available(protocol.HarnessClaudeCLI)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDriverUnavailable)
	require.ErrorIs(t, err, verificationError)

	_, err = driver.Execute(t.Context(), Invocation{
		Run: protocol.Run{
			ID:        "run-verification-fail",
			HarnessID: protocol.HarnessClaudeCLI,
		},
		Workspace: temporaryDirectory,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrDriverUnavailable)
	require.ErrorIs(t, err, verificationError)
}

func TestACPDriver_MCPGrantTimeout(t *testing.T) {
	driver := NewACPDriver(ACPDriverOptions{
		MCPConnectTimeout: 20 * time.Millisecond,
	})

	// When connected channel is nil
	err := driver.awaitMCPConnection(t.Context(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMCPUnavailable)
	assert.Contains(t, err.Error(), "the run's MCP grant cannot report a connection")

	// When connected channel does not receive before timeout
	neverConnected := make(chan struct{})
	err = driver.awaitMCPConnection(t.Context(), neverConnected)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMCPUnavailable)
	assert.Contains(t, err.Error(), "within 20ms")

	// When operation context is cancelled before connection
	cancelledContext, cancel := context.WithCancel(t.Context())
	cancel()
	err = driver.awaitMCPConnection(cancelledContext, neverConnected)
	require.Error(t, err)
	require.ErrorIs(t, err, acp.ErrCancelled)

	// When connected channel receives successfully
	alreadyConnected := make(chan struct{})
	close(alreadyConnected)
	err = driver.awaitMCPConnection(t.Context(), alreadyConnected)
	require.NoError(t, err)
}

func TestACPDriver_SupportsResumeHandling(t *testing.T) {
	driver := NewACPDriver(ACPDriverOptions{})

	// Unprobed harness fails resume
	err := driver.SupportsResume(protocol.HarnessClaudeCLI)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrResumeUnsupported)

	// Probed without ResumeSession or LoadSession fails resume
	driver.mutex.Lock()
	driver.probed[protocol.HarnessClaudeCLI] = protocol.AcpAgentCapabilities{
		ResumeSession: false,
		LoadSession:   false,
	}
	driver.mutex.Unlock()

	err = driver.SupportsResume(protocol.HarnessClaudeCLI)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrResumeUnsupported)

	// Probed with ResumeSession succeeds
	driver.mutex.Lock()
	driver.probed[protocol.HarnessClaudeCLI] = protocol.AcpAgentCapabilities{
		ResumeSession: true,
	}
	driver.mutex.Unlock()

	err = driver.SupportsResume(protocol.HarnessClaudeCLI)
	require.NoError(t, err)

	// Probed with LoadSession succeeds
	driver.mutex.Lock()
	driver.probed[protocol.HarnessClaudeCLI] = protocol.AcpAgentCapabilities{
		LoadSession: true,
	}
	driver.mutex.Unlock()

	err = driver.SupportsResume(protocol.HarnessClaudeCLI)
	require.NoError(t, err)
}

func TestACPDriver_BoundedStderrDiagnosticCaptureOnCrash(t *testing.T) {
	temporaryDirectory := t.TempDir()
	crashBinary := filepath.Join(temporaryDirectory, "crashing-adapter")

	// Adapter script that writes to stderr and exits with an error status
	script := `#!/bin/sh
cat >&2 << 'EOF'
adapter initialization failed: missing required runtime dependency
traceback details line 2
EOF
exit 1
`
	require.NoError(t, os.WriteFile(crashBinary, []byte(script), 0o755))

	driver := NewACPDriver(ACPDriverOptions{
		Adapters: map[string]ACPAdapter{
			protocol.HarnessClaudeCLI: {
				Binary: crashBinary,
			},
		},
		Providers: DefaultACPProviders(),
	})

	_, err := driver.Execute(t.Context(), Invocation{
		Run: protocol.Run{
			ID:        "run-crash",
			HarnessID: protocol.HarnessClaudeCLI,
		},
		Workspace: temporaryDirectory,
		Output:    func(string) {},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), adapterStderrSeparator)
	assert.Contains(t, err.Error(), "adapter initialization failed: missing required runtime dependency")
	assert.Contains(t, err.Error(), "traceback details line 2")
}

func TestACPDriver_BoundedStderrDiagnosticLargeOutput(t *testing.T) {
	temporaryDirectory := t.TempDir()
	largeStderrBinary := filepath.Join(temporaryDirectory, "large-stderr-adapter")

	// Emit large output that exceeds acp.MaximumDiagnosticBytes
	largeStderrScript := `#!/bin/sh
for i in $(seq 1 500); do
  printf 'diagnostic-line-%04d-overflowing-the-tail-buffer-with-many-bytes\n' "$i" >&2
done
exit 1
`
	require.NoError(t, os.WriteFile(largeStderrBinary, []byte(largeStderrScript), 0o755))

	driver := NewACPDriver(ACPDriverOptions{
		Adapters: map[string]ACPAdapter{
			protocol.HarnessClaudeCLI: {
				Binary: largeStderrBinary,
			},
		},
		Providers: DefaultACPProviders(),
	})

	_, err := driver.Execute(t.Context(), Invocation{
		Run: protocol.Run{
			ID:        "run-large-stderr",
			HarnessID: protocol.HarnessClaudeCLI,
		},
		Workspace: temporaryDirectory,
		Output:    func(string) {},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), adapterStderrSeparator)
	separatorIndex := strings.Index(err.Error(), adapterStderrSeparator)
	require.Positive(t, separatorIndex)
	capturedDiagnostic := err.Error()[separatorIndex+len(adapterStderrSeparator):]
	assert.LessOrEqual(t, len(capturedDiagnostic), acp.MaximumDiagnosticBytes)
}
