package readiness

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildCapabilityReportValidatesAndParsersWork(t *testing.T) {
	version, ok := parseGoVersion("go version go1.26.2 darwin/arm64")
	assert.True(t, ok)
	assert.Equal(t, "1.26.2", version)

	gccVersion, ok := parseGCCVersion("gcc (Ubuntu 13.2.0-4ubuntu3) 13.2.0")
	assert.True(t, ok)
	assert.Equal(t, "13.2.0", gccVersion)

	clangVersion, ok := parseClangVersion("Apple clang version 15.0.0 (clang-1500.3.9.4)")
	assert.True(t, ok)
	assert.Equal(t, "15.0.0", clangVersion)

	report := BuildCapabilityReport(t.Context(), NodeSettings{
		NodeID:          "node-mac",
		WorkspaceRoots:  []string{t.TempDir()},
		Labels:          []string{"gpu-workstation"},
		Accelerators:    []string{"apple-metal"},
		Toolchains:      []Toolchain{{ID: "godot", Version: "4.4.1"}},
		MemoryMegabytes: 32768,
	}, nil)
	require.NoError(t, report.Validate())

	foundLabel := slices.ContainsFunc(report.Evidence, func(entry protocol.NodeCapabilityEvidence) bool {
		return entry.CapabilityID == "label:gpu-workstation" && entry.Success
	})
	assert.True(t, foundLabel)

	foundGitWorktree := slices.ContainsFunc(report.Evidence, func(entry protocol.NodeCapabilityEvidence) bool {
		return entry.CapabilityID == "workspace-lease:"+protocol.WorkspaceIsolationGitWorktree && entry.Success
	})
	assert.True(t, foundGitWorktree)
}

func TestToolchainVersionParsers_Comprehensive(t *testing.T) {
	t.Run("parseGoVersion", func(t *testing.T) {
		tests := []struct {
			name     string
			input    string
			expected string
			ok       bool
		}{
			{"valid standard", "go version go1.26.2 darwin/arm64", "1.26.2", true},
			{"valid rc", "go version go1.22.1rc1 linux/amd64", "1.22.1", true},
			{"noisy output", "warning: test\ngo version go1.24.0 linux/amd64\n", "1.24.0", true},
			{"empty input", "", "", false},
			{"malformed without version", "go version none", "", false},
			{"malformed random text", "not a go toolchain", "", false},
		}
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				version, ok := parseGoVersion(testCase.input)
				assert.Equal(t, testCase.ok, ok)
				assert.Equal(t, testCase.expected, version)
			})
		}
	})

	t.Run("parseGCCVersion", func(t *testing.T) {
		tests := []struct {
			name     string
			input    string
			expected string
			ok       bool
		}{
			{"valid standard", "gcc (Ubuntu 13.2.0-4ubuntu3) 13.2.0", "13.2.0", true},
			{"valid with header", "gcc (Debian 12.2.0-14) 12.2.0\nCopyright (C) 2022", "12.2.0", true},
			{"noisy output", "gcc (GCC) 11.4.0 20230524 (Red Hat 11.4.0-1)\nCopyright (C) 2021 Free Software Foundation, Inc.\n", "11.4.0", true},
			{"empty input", "", "", false},
			{"malformed without version", "gcc: fatal error: no input files", "", false},
			{"malformed text", "gcc something without numbers", "", false},
		}
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				version, ok := parseGCCVersion(testCase.input)
				assert.Equal(t, testCase.ok, ok)
				assert.Equal(t, testCase.expected, version)
			})
		}
	})

	t.Run("parseClangVersion", func(t *testing.T) {
		tests := []struct {
			name     string
			input    string
			expected string
			ok       bool
		}{
			{"valid apple clang", "Apple clang version 15.0.0 (clang-1500.3.9.4)", "15.0.0", true},
			{"valid llvm clang", "clang version 18.1.3", "18.1.3", true},
			{"noisy output", "Ubuntu clang version 14.0.0-1ubuntu1.1\nTarget: x86_64\n", "14.0.0", true},
			{"empty input", "", "", false},
			{"malformed without version", "clang: error: no input files", "", false},
			{"malformed text", "clang version none", "", false},
		}
		for _, testCase := range tests {
			t.Run(testCase.name, func(t *testing.T) {
				version, ok := parseClangVersion(testCase.input)
				assert.Equal(t, testCase.ok, ok)
				assert.Equal(t, testCase.expected, version)
			})
		}
	})
}

func TestWorkspaceRootProbe_NonExistentAndUnwritable(t *testing.T) {
	t.Run("non-existent workspace root", func(t *testing.T) {
		nonExistentRoot := filepath.Join(t.TempDir(), "does-not-exist")
		report := BuildCapabilityReport(t.Context(), NodeSettings{
			NodeID:         "node-test",
			WorkspaceRoots: []string{nonExistentRoot},
		}, nil)

		index := slices.IndexFunc(report.Evidence, func(entry protocol.NodeCapabilityEvidence) bool {
			return entry.CapabilityID == "workspace-writable"
		})
		require.NotEqual(t, -1, index)
		assert.False(t, report.Evidence[index].Success)
		assert.Contains(t, report.Evidence[index].Diagnostic, "0 of 1 workspace roots are writable")
	})

	t.Run("unwritable workspace root", func(t *testing.T) {
		readOnlyRoot := filepath.Join(t.TempDir(), "read-only-root")
		require.NoError(t, os.Mkdir(readOnlyRoot, 0o500))
		t.Cleanup(func() {
			_ = os.Chmod(readOnlyRoot, 0o700)
		})

		report := BuildCapabilityReport(t.Context(), NodeSettings{
			NodeID:         "node-test",
			WorkspaceRoots: []string{readOnlyRoot},
		}, nil)

		index := slices.IndexFunc(report.Evidence, func(entry protocol.NodeCapabilityEvidence) bool {
			return entry.CapabilityID == "workspace-writable"
		})
		require.NotEqual(t, -1, index)
		assert.False(t, report.Evidence[index].Success)
		assert.Contains(t, report.Evidence[index].Diagnostic, "0 of 1 workspace roots are writable")
	})
}
