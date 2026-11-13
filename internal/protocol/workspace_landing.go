package protocol

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

const (
	WorkspaceLandingModeFastForward = "fast-forward"
	WorkspaceLandingModeMergeFFOnly = "merge-ff-only"
	WorkspaceLandingModeCherryPick  = "cherry-pick"

	WorkspaceLandingStatusLanded   = "landed"
	WorkspaceLandingStatusConflict = "conflict"
	WorkspaceLandingStatusDirty    = "dirty"
	WorkspaceLandingStatusFailed   = "failed"

	MaximumLandingDiagnosticPaths = 128
)

var (
	WorkspaceLandingModes    = []string{WorkspaceLandingModeFastForward, WorkspaceLandingModeMergeFFOnly, WorkspaceLandingModeCherryPick}
	WorkspaceLandingStatuses = []string{WorkspaceLandingStatusLanded, WorkspaceLandingStatusConflict, WorkspaceLandingStatusDirty, WorkspaceLandingStatusFailed}
)

type WorkspaceLeaseLandRequest struct {
	RequestID        string              `json:"requestId,omitempty"`
	Lease            WorkspaceLeaseGrant `json:"lease"`
	Mode             string              `json:"mode,omitempty"`
	TargetBranch     string              `json:"targetBranch,omitempty"`
	CleanupAfterLand bool                `json:"cleanupAfterLand,omitempty"`
}

func (request WorkspaceLeaseLandRequest) EffectiveMode() string {
	trimmed := strings.TrimSpace(request.Mode)
	if trimmed == "" {
		return WorkspaceLandingModeFastForward
	}
	return trimmed
}

func (request WorkspaceLeaseLandRequest) NormalizedTargetBranch() (string, error) {
	trimmed := strings.TrimSpace(request.TargetBranch)
	if trimmed == "" {
		if after, ok := strings.CutPrefix(request.Lease.BaseRevision, "refs/heads/"); ok {
			if !IsWorkspaceLeaseBaseBranch(after) {
				return "", errors.New("workspace lease base branch is malformed")
			}
			return after, nil
		}
		return "", nil
	}
	branchName := strings.TrimPrefix(trimmed, "refs/heads/")
	if !IsWorkspaceLeaseBaseBranch(branchName) {
		return "", errors.New("workspace landing targetBranch is invalid")
	}
	return branchName, nil
}

func (request WorkspaceLeaseLandRequest) Validate() error {
	if request.RequestID != "" && !IsIdentifier(request.RequestID) {
		return errors.New("workspace landing requestId is invalid")
	}
	if err := request.Lease.Validate(); err != nil {
		return err
	}
	if request.Lease.Policy != WorkspaceIsolationGitWorktree {
		return errors.New("only git-worktree workspace leases can be landed")
	}
	if request.Lease.ReadOnly || request.Lease.SourceLeaseID != "" {
		return errors.New("read-only shared workspace leases cannot be landed")
	}
	mode := request.EffectiveMode()
	if !slices.Contains(WorkspaceLandingModes, mode) {
		return fmt.Errorf("workspace landing mode %q is unsupported", request.Mode)
	}
	if _, err := request.NormalizedTargetBranch(); err != nil {
		return err
	}
	return nil
}

type WorkspaceLeaseLandResult struct {
	RequestID              string                `json:"requestId,omitempty"`
	NodeID                 string                `json:"nodeId,omitempty"`
	LeaseID                string                `json:"leaseId"`
	Status                 string                `json:"status"`
	Mode                   string                `json:"mode"`
	Landed                 bool                  `json:"landed"`
	AlreadyLanded          bool                  `json:"alreadyLanded,omitempty"`
	Cleaned                bool                  `json:"cleaned,omitempty"`
	WorktreeRetained       bool                  `json:"worktreeRetained"`
	TargetBranch           string                `json:"targetBranch,omitempty"`
	BaseRevision           string                `json:"baseRevision,omitempty"`
	SourceRevision         string                `json:"sourceRevision,omitempty"`
	PreviousTargetRevision string                `json:"previousTargetRevision,omitempty"`
	LandedRevision         string                `json:"landedRevision,omitempty"`
	CommitsLanded          int                   `json:"commitsLanded,omitempty"`
	Reason                 string                `json:"reason,omitempty"`
	Conflicts              []string              `json:"conflicts,omitempty"`
	DirtyFiles             []string              `json:"dirtyFiles,omitempty"`
	Detail                 string                `json:"detail,omitempty"`
	Error                  string                `json:"error,omitempty"`
	LeaseUpdate            *WorkspaceLeaseUpdate `json:"leaseUpdate,omitempty"`
	ObservedAt             string                `json:"observedAt,omitempty"`
}

func (result WorkspaceLeaseLandResult) Validate() error {
	if result.RequestID != "" && !IsIdentifier(result.RequestID) {
		return errors.New("workspace landing result requestId is invalid")
	}
	if result.NodeID != "" && !IsIdentifier(result.NodeID) {
		return errors.New("workspace landing result nodeId is invalid")
	}
	if !IsWorkspaceLeaseIdentity(result.LeaseID) {
		return errors.New("workspace landing result leaseId is malformed")
	}
	if !slices.Contains(WorkspaceLandingStatuses, result.Status) {
		return errors.New("workspace landing result status is missing or unknown")
	}
	if !slices.Contains(WorkspaceLandingModes, result.Mode) {
		return errors.New("workspace landing result mode is missing or unknown")
	}
	if result.Landed != (result.Status == WorkspaceLandingStatusLanded) {
		return errors.New("workspace landing result landed flag does not match status")
	}
	if result.Cleaned && !result.Landed {
		return errors.New("workspace landing result cannot mark cleaned when landing did not succeed")
	}
	if result.Cleaned && result.WorktreeRetained {
		return errors.New("workspace landing result cannot mark both cleaned and worktreeRetained")
	}
	if result.TargetBranch != "" && !IsWorkspaceLeaseBaseBranch(result.TargetBranch) {
		return errors.New("workspace landing result targetBranch is malformed")
	}
	for label, revision := range map[string]string{
		"baseRevision":           result.BaseRevision,
		"sourceRevision":         result.SourceRevision,
		"previousTargetRevision": result.PreviousTargetRevision,
		"landedRevision":         result.LandedRevision,
	} {
		if revision != "" && !IsResolvedRevision(revision) {
			return fmt.Errorf("workspace landing result %s is malformed", label)
		}
	}
	if result.CommitsLanded < 0 {
		return errors.New("workspace landing result commitsLanded must not be negative")
	}
	if len(result.Conflicts) > MaximumLandingDiagnosticPaths {
		return errors.New("workspace landing result conflicts exceeds maximum path count")
	}
	for _, conflictPath := range result.Conflicts {
		if conflictPath == "" || !IsBoundedString(conflictPath, MaximumWorkspaceSubpathBytes*2) || strings.ContainsRune(conflictPath, 0) {
			return errors.New("workspace landing result conflict path is invalid")
		}
	}
	if len(result.DirtyFiles) > MaximumLandingDiagnosticPaths {
		return errors.New("workspace landing result dirtyFiles exceeds maximum path count")
	}
	for _, dirtyPath := range result.DirtyFiles {
		if dirtyPath == "" || !IsBoundedString(dirtyPath, MaximumWorkspaceSubpathBytes*2) || strings.ContainsRune(dirtyPath, 0) {
			return errors.New("workspace landing result dirty file path is invalid")
		}
	}
	if !IsBoundedString(result.Reason, 128) {
		return errors.New("workspace landing result reason exceeds bound")
	}
	if !IsBoundedString(result.Detail, DiagnosticMaximumBytes) {
		return errors.New("workspace landing result detail exceeds bound")
	}
	if !IsBoundedString(result.Error, DiagnosticMaximumBytes) {
		return errors.New("workspace landing result error exceeds bound")
	}
	if result.LeaseUpdate != nil {
		if err := result.LeaseUpdate.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func ValidateWorkspaceRequirements(requirements *WorkspaceRequirements) error {
	if requirements == nil {
		return nil
	}
	sourceLeaseID := strings.TrimSpace(requirements.SourceLeaseID)
	if sourceLeaseID != "" {
		if !IsWorkspaceLeaseIdentity(sourceLeaseID) {
			return errors.New("workspace sourceLeaseId is malformed")
		}
		if requirements.Writable {
			return errors.New("workspace sourceLeaseId requires writable=false")
		}
		if requirements.Isolation != "" {
			return errors.New("workspace sourceLeaseId cannot be combined with isolation")
		}
		if requirements.Cleanup != "" && requirements.Cleanup != WorkspaceCleanupWhenUnchanged {
			return errors.New("workspace sourceLeaseId only supports when-unchanged cleanup")
		}
	}
	if requirements.Isolation != "" && !slices.Contains(WorkspaceIsolationPolicies, requirements.Isolation) {
		return fmt.Errorf("workspace isolation policy %q is unsupported", requirements.Isolation)
	}
	if requirements.Cleanup != "" && !slices.Contains(WorkspaceCleanupPolicies, requirements.Cleanup) {
		return fmt.Errorf("workspace cleanup policy %q is unsupported", requirements.Cleanup)
	}
	if requirements.BaseBranch != "" && !IsWorkspaceLeaseBaseBranch(requirements.BaseBranch) {
		return fmt.Errorf("workspace baseBranch %q is invalid", requirements.BaseBranch)
	}
	return nil
}
