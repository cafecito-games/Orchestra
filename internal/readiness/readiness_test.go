package readiness

import (
	"context"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestBuildCapabilityReportValidatesAndParsersWork(t *testing.T) {
	version, ok := parseGoVersion("go version go1.26.2 darwin/arm64")
	require.True(t, ok)
	require.Equal(t, "1.26.2", version)

	gccVersion, ok := parseGCCVersion("gcc (Ubuntu 13.2.0-4ubuntu3) 13.2.0")
	require.True(t, ok)
	require.Equal(t, "13.2.0", gccVersion)

	clangVersion, ok := parseClangVersion("Apple clang version 15.0.0 (clang-1500.3.9.4)")
	require.True(t, ok)
	require.Equal(t, "15.0.0", clangVersion)

	report := BuildCapabilityReport(context.Background(), NodeSettings{
		NodeID:          "node-mac",
		WorkspaceRoots:  []string{t.TempDir()},
		Labels:          []string{"gpu-workstation"},
		Accelerators:    []string{"apple-metal"},
		Toolchains:      []Toolchain{{ID: "godot", Version: "4.4.1"}},
		MemoryMegabytes: 32768,
	}, nil)
	require.NoError(t, report.Validate())

	foundLabel := false
	foundGitWorktree := false
	for _, entry := range report.Evidence {
		if entry.CapabilityID == "label:gpu-workstation" && entry.Success {
			foundLabel = true
		}
		if entry.CapabilityID == "workspace-lease:"+protocol.WorkspaceIsolationGitWorktree && entry.Success {
			foundGitWorktree = true
		}
	}
	require.True(t, foundLabel)
	require.True(t, foundGitWorktree)
}
