package harness

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/antigravityacp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

var (
	orchestraExecutableMutex sync.RWMutex
	orchestraExecutablePath  string
)

// RegisterOrchestraExecutableFromOS records the running orchestra CLI binary path so
// built-in ACP bridges can spawn `orchestra acp-bridge ...` without confusing `go test` binaries.
func RegisterOrchestraExecutableFromOS() {
	executable, err := os.Executable()
	if err != nil {
		return
	}
	if resolved, evalErr := filepath.EvalSymlinks(executable); evalErr == nil {
		executable = resolved
	}
	if !filepath.IsAbs(executable) {
		return
	}
	orchestraExecutableMutex.Lock()
	orchestraExecutablePath = filepath.Clean(executable)
	orchestraExecutableMutex.Unlock()
}

// OrchestraExecutable returns the registered orchestra CLI binary path, if available.
func OrchestraExecutable() string {
	orchestraExecutableMutex.RLock()
	defer orchestraExecutableMutex.RUnlock()
	return orchestraExecutablePath
}

// BuiltInAntigravityACPAdapter constructs the built-in ACPAdapter that invokes
// `orchestra acp-bridge antigravity` when an executable orchestra binary is available.
func BuiltInAntigravityACPAdapter(executablePath, dataRoot, version string) (ACPAdapter, bool) {
	candidate := strings.TrimSpace(executablePath)
	if candidate == "" {
		candidate = OrchestraExecutable()
	}
	if candidate == "" {
		return ACPAdapter{}, false
	}
	if resolved, err := filepath.EvalSymlinks(candidate); err == nil {
		candidate = resolved
	}
	if !filepath.IsAbs(candidate) {
		return ACPAdapter{}, false
	}
	information, err := os.Stat(candidate)
	if err != nil || !information.Mode().IsRegular() || (runtime.GOOS != "windows" && information.Mode().Perm()&0o111 == 0) {
		return ACPAdapter{}, false
	}
	arguments := []string{"acp-bridge", "antigravity"}
	if strings.TrimSpace(dataRoot) != "" {
		arguments = append(arguments, "--state-dir", filepath.Join(dataRoot, "state", "antigravity-acp"))
	}
	return ACPAdapter{
		Binary:    filepath.Clean(candidate),
		Arguments: arguments,
		ID:        antigravityacp.AdapterID,
		Version:   antigravityacp.NormalizedAdapterVersion(version),
		Source:    protocol.ACPAdapterSourceBuiltIn,
	}, true
}

func antigravityModeForApprovalPolicy(approvalPolicy string) string {
	if approvalPolicy == protocol.ApprovalPolicyBypass {
		return antigravityacp.ModeBypass
	}
	return antigravityacp.ModeSandbox
}

func antigravityACPProvider() ACPProvider {
	return ACPProvider{
		Environment: func(approvalPolicy string) []string {
			return []string{"ORCHESTRA_ANTIGRAVITY_INITIAL_MODE=" + antigravityModeForApprovalPolicy(approvalPolicy)}
		},
		Configuration:        antigravitySessionConfiguration,
		RequireMCPConnection: true,
		NativeBinaryVariable: antigravityacp.NativeBinaryEnvironmentVariable,
		ModelOption:          antigravityacp.ModelOption,
	}
}

func antigravitySessionConfiguration(run protocol.Run, approvalPolicy string) []acp.ConfigurationSelection {
	selections := []acp.ConfigurationSelection{
		{
			ID:          antigravityacp.ModeOption,
			Value:       antigravityModeForApprovalPolicy(approvalPolicy),
			Requirement: acp.ConfigurationPolicy,
		},
	}
	if run.Model != "" && run.Model != "default" {
		selections = append(selections, acp.ConfigurationSelection{
			ID:          antigravityacp.ModelOption,
			Value:       run.Model,
			Requirement: acp.ConfigurationRequested,
		})
	}
	return selections
}
