package readiness

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/workspace"
)

type Toolchain struct {
	ID      string
	Version string
}

type NodeSettings struct {
	NodeID           string
	WorkspaceRoots   []string
	ProjectAllowlist []string
	Labels           []string
	Accelerators     []string
	Toolchains       []Toolchain
	MemoryMegabytes  int
}

func BuildCapabilityReport(operationContext context.Context, settings NodeSettings, harnesses []protocol.HarnessProfile) protocol.NodeCapabilityReport {
	_ = harnesses
	observedAt := time.Now().UTC().Format(time.RFC3339Nano)
	evidence := make([]protocol.NodeCapabilityEvidence, 0, 5+len(settings.Labels)+len(settings.Accelerators)+len(settings.Toolchains)+len(AllowlistedProbes))
	evidence = append(evidence,
		runtimeEvidence("os", runtime.GOOS, observedAt),
		runtimeEvidence("architecture", runtime.GOARCH, observedAt),
		runtimeEvidence("logical-cpu-count", strconv.Itoa(runtime.NumCPU()), observedAt),
		workspaceWritableEvidence(settings.WorkspaceRoots, observedAt),
	)
	evidence = append(evidence, workspaceLeaseEvidence(len(settings.WorkspaceRoots) > 0, observedAt)...)
	for _, label := range settings.Labels {
		if looksSecretLike(label) {
			continue
		}
		evidence = append(evidence, protocol.NodeCapabilityEvidence{
			CapabilityID:    "label:" + label,
			Source:          protocol.CapabilityEvidenceSourceConfigured,
			Success:         true,
			NormalizedValue: label,
			ObservedAt:      observedAt,
		})
	}
	for _, accelerator := range settings.Accelerators {
		if looksSecretLike(accelerator) {
			continue
		}
		evidence = append(evidence, protocol.NodeCapabilityEvidence{
			CapabilityID:    "accelerator:" + accelerator,
			Source:          protocol.CapabilityEvidenceSourceConfigured,
			Success:         true,
			NormalizedValue: accelerator,
			ObservedAt:      observedAt,
		})
	}
	for _, toolchain := range settings.Toolchains {
		if looksSecretLike(toolchain.ID) || looksSecretLike(toolchain.Version) {
			continue
		}
		evidence = append(evidence, protocol.NodeCapabilityEvidence{
			CapabilityID:    "toolchain:" + toolchain.ID,
			Source:          protocol.CapabilityEvidenceSourceConfigured,
			Success:         true,
			NormalizedValue: toolchain.Version,
			ObservedAt:      observedAt,
		})
	}
	if settings.MemoryMegabytes > 0 {
		evidence = append(evidence, protocol.NodeCapabilityEvidence{
			CapabilityID:    "configured-memory-megabytes",
			Source:          protocol.CapabilityEvidenceSourceConfigured,
			Success:         true,
			NormalizedValue: strconv.Itoa(settings.MemoryMegabytes),
			ObservedAt:      observedAt,
		})
	}

	probeEvidence := make([]protocol.NodeCapabilityEvidence, len(AllowlistedProbes))
	var waitGroup sync.WaitGroup
	for index, probe := range AllowlistedProbes {
		waitGroup.Go(func() {
			entry := RunProbe(operationContext, probe)
			entry.ObservedAt = observedAt
			probeEvidence[index] = entry
		})
	}
	waitGroup.Wait()
	evidence = append(evidence, probeEvidence...)

	var projectAllowlist []string
	if len(settings.ProjectAllowlist) > 0 {
		projectAllowlist = settings.ProjectAllowlist
	}
	return protocol.NodeCapabilityReport{
		NodeID:           settings.NodeID,
		ProjectAllowlist: projectAllowlist,
		Evidence:         evidence,
		At:               observedAt,
	}
}

func runtimeEvidence(capabilityID, normalizedValue, observedAt string) protocol.NodeCapabilityEvidence {
	return protocol.NodeCapabilityEvidence{
		CapabilityID:    capabilityID,
		Source:          protocol.CapabilityEvidenceSourceRuntime,
		Success:         true,
		NormalizedValue: normalizedValue,
		ObservedAt:      observedAt,
	}
}

func workspaceWritableEvidence(roots []string, observedAt string) protocol.NodeCapabilityEvidence {
	writable := 0
	for _, root := range roots {
		file, err := os.CreateTemp(root, ".orchestra-writable-*")
		if err != nil {
			continue
		}
		path := file.Name()
		file.Close()
		os.Remove(path)
		writable++
	}
	evidence := protocol.NodeCapabilityEvidence{
		CapabilityID: "workspace-writable",
		Source:       protocol.CapabilityEvidenceSourceRuntime,
		ObservedAt:   observedAt,
	}
	if writable > 0 {
		evidence.Success = true
		evidence.NormalizedValue = "true"
		return evidence
	}
	evidence.Diagnostic = fmt.Sprintf("%d of %d workspace roots are writable", writable, len(roots))
	return evidence
}

func workspaceLeaseEvidence(hasRoots bool, observedAt string) []protocol.NodeCapabilityEvidence {
	_, gitErr := workspace.FindGit()
	posixPaths := runtime.GOOS != "windows"
	supported := map[string]bool{
		protocol.WorkspaceIsolationGitWorktree:       posixPaths && hasRoots && gitErr == nil,
		protocol.WorkspaceIsolationExclusiveExisting: posixPaths && hasRoots,
	}
	evidence := make([]protocol.NodeCapabilityEvidence, 0, len(protocol.WorkspaceIsolationPolicies))
	for _, policy := range protocol.WorkspaceIsolationPolicies {
		entry := protocol.NodeCapabilityEvidence{
			CapabilityID: "workspace-lease:" + policy,
			Source:       protocol.CapabilityEvidenceSourceRuntime,
			ObservedAt:   observedAt,
		}
		if supported[policy] {
			entry.Success = true
			entry.NormalizedValue = "true"
		} else {
			entry.Diagnostic = "this workspace lease policy is not available on this slave node"
		}
		evidence = append(evidence, entry)
	}
	return evidence
}
