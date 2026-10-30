package protocol

import (
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"
)

const (
	WorkspaceLeaseManagedDirectory       = ".orchestra/worktrees"
	WorkspaceLeaseBranchPrefix           = "orchestra/"
	WorkspaceLeaseBaseBranchMaximumBytes = 200
	WorkspaceIsolationGitWorktree        = "git-worktree"
	WorkspaceIsolationExclusiveExisting  = "exclusive-existing"
	WorkspaceCleanupRetain               = "retain"
	WorkspaceCleanupWhenUnchanged        = "when-unchanged"
	WorkspaceCleanupModeReconcile        = "reconcile"
	WorkspaceCleanupModeOperator         = "operator"
)

var (
	WorkspaceIsolationPolicies = []string{WorkspaceIsolationGitWorktree, WorkspaceIsolationExclusiveExisting}
	WorkspaceCleanupPolicies   = []string{WorkspaceCleanupRetain, WorkspaceCleanupWhenUnchanged}
	WorkspaceCleanupModes      = []string{WorkspaceCleanupModeReconcile, WorkspaceCleanupModeOperator}
	WorkspaceLeaseStatuses     = []string{"requested", "provisioning", "active", "released", "cleaning", "retained", "cleaned", "failed"}
	WorkspaceRetentionReasons  = []string{"dirty", "untracked", "diverged", "locked", "unregistered", "identity-mismatch", "ambiguous", "operator-hold", "policy"}

	workspaceLeaseIdentityPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,95}$`)
	workspaceLeaseBaseBranchPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*$`)
	resolvedRevisionPattern         = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)
	schemeRepositoryURLPattern      = regexp.MustCompile(`^([A-Za-z][A-Za-z0-9+.-]*://)([^/]*)((?s).*)$`)
)

var WorkspaceLeaseTransitions = map[string][]string{
	"requested":    {"provisioning", "failed"},
	"provisioning": {"active", "released", "retained", "failed"},
	"active":       {"released", "retained"},
	"released":     {"cleaning", "retained"},
	"cleaning":     {"cleaned", "retained", "failed"},
	"retained":     {"cleaning"},
	"cleaned":      {},
	"failed":       {},
}

func CanTransitionWorkspaceLease(fromStatus, toStatus string) bool {
	return slices.Contains(WorkspaceLeaseTransitions[fromStatus], toStatus)
}

func IsTerminalWorkspaceLeaseStatus(status string) bool {
	return status == "cleaned" || status == "failed"
}

func WorkspaceLeasePath(fromStatus, toStatus string) []string {
	if fromStatus == toStatus {
		return nil
	}
	previous := map[string]string{fromStatus: ""}
	queue := []string{fromStatus}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, next := range WorkspaceLeaseTransitions[current] {
			if _, seen := previous[next]; seen {
				continue
			}
			previous[next] = current
			if next == toStatus {
				resultPath := []string{toStatus}
				for step := current; step != fromStatus; step = previous[step] {
					resultPath = append([]string{step}, resultPath...)
				}
				return resultPath
			}
			queue = append(queue, next)
		}
	}
	return nil
}

func IsWorkspaceLeaseIdentity(value string) bool {
	return workspaceLeaseIdentityPattern.MatchString(value)
}

func IsWorkspaceLeaseBaseBranch(value string) bool {
	if len(value) > WorkspaceLeaseBaseBranchMaximumBytes || !workspaceLeaseBaseBranchPattern.MatchString(value) || strings.Contains(value, "..") {
		return false
	}
	for component := range strings.SplitSeq(value, "/") {
		if strings.HasSuffix(component, ".") || strings.HasSuffix(component, ".lock") {
			return false
		}
	}
	return true
}

func IsResolvedRevision(value string) bool {
	return resolvedRevisionPattern.MatchString(value)
}

func WorkspaceLeaseBaseRef(defaultBranch string) string {
	return "refs/heads/" + defaultBranch
}

func WorkspaceLeaseBranch(taskID, runID string) string {
	return WorkspaceLeaseBranchPrefix + taskID + "/" + runID
}

func WorkspaceLeaseWorktreePath(rootPath, leaseID string) string {
	return path.Join(rootPath, WorkspaceLeaseManagedDirectory, leaseID)
}

// NormalizeRepositoryIdentity strips userinfo, a trailing slash, and a trailing ".git"
// from a repository URL, rejecting secret-like or hostless URLs.
func NormalizeRepositoryIdentity(repositoryURL string) (string, bool) {
	var authority, identity string
	var requiresHost bool
	if match := schemeRepositoryURLPattern.FindStringSubmatch(repositoryURL); match != nil {
		authority = match[2][strings.LastIndex(match[2], "@")+1:]
		identity = match[1] + authority + match[3]
		requiresHost = !strings.HasPrefix(strings.ToLower(match[1]), "file:")
	} else {
		prefix, _, _ := strings.Cut(repositoryURL, "/")
		identity = repositoryURL[strings.LastIndex(prefix, "@")+1:]
		authority = identity
		if index := strings.IndexAny(identity, "/:"); index >= 0 {
			authority = identity[:index]
		}
		requiresHost = !strings.HasPrefix(repositoryURL, "/")
	}
	identity = strings.TrimSuffix(identity, "/")
	identity = strings.TrimSuffix(identity, ".git")
	if identity == "" || strings.Contains(authority, "@") || (requiresHost && authority == "") || LooksSecretLike(identity) {
		return "", false
	}
	return identity, true
}

type WorkspaceLease struct {
	ID                   string `json:"id"`
	ThreadID             string `json:"threadId"`
	TaskID               string `json:"taskId"`
	RunID                string `json:"runId"`
	NodeID               string `json:"nodeId"`
	ProjectProfileID     string `json:"projectProfileId,omitempty"`
	Policy               string `json:"policy"`
	Cleanup              string `json:"cleanup"`
	Repository           string `json:"repository,omitempty"`
	Root                 string `json:"root"`
	SourcePath           string `json:"sourcePath"`
	BaseRevision         string `json:"baseRevision,omitempty"`
	ResolvedBaseRevision string `json:"resolvedBaseRevision,omitempty"`
	Branch               string `json:"branch,omitempty"`
	WorktreePath         string `json:"worktreePath"`
	Status               string `json:"status"`
	RetentionReason      string `json:"retentionReason,omitempty"`
	Detail               string `json:"detail,omitempty"`
	CleanupRequestedAt   string `json:"cleanupRequestedAt,omitempty"`
	CreatedAt            string `json:"createdAt"`
	UpdatedAt            string `json:"updatedAt"`
}

func (lease WorkspaceLease) Grant() WorkspaceLeaseGrant {
	return WorkspaceLeaseGrant{
		ID:                   lease.ID,
		Status:               lease.Status,
		Policy:               lease.Policy,
		Cleanup:              lease.Cleanup,
		Repository:           lease.Repository,
		Root:                 lease.Root,
		SourcePath:           lease.SourcePath,
		BaseRevision:         lease.BaseRevision,
		ResolvedBaseRevision: lease.ResolvedBaseRevision,
		Branch:               lease.Branch,
		WorktreePath:         lease.WorktreePath,
	}
}

type WorkspaceLeaseGrant struct {
	ID                   string `json:"id"`
	Status               string `json:"status"`
	Policy               string `json:"policy"`
	Cleanup              string `json:"cleanup"`
	Repository           string `json:"repository,omitempty"`
	Root                 string `json:"root"`
	SourcePath           string `json:"sourcePath"`
	BaseRevision         string `json:"baseRevision,omitempty"`
	ResolvedBaseRevision string `json:"resolvedBaseRevision,omitempty"`
	Branch               string `json:"branch,omitempty"`
	WorktreePath         string `json:"worktreePath"`
}

func (grant WorkspaceLeaseGrant) Validate() error {
	if !IsWorkspaceLeaseIdentity(grant.ID) {
		return fmt.Errorf("workspace lease id is malformed")
	}
	if !slices.Contains(WorkspaceLeaseStatuses, grant.Status) {
		return fmt.Errorf("workspace lease status is missing or unknown")
	}
	if !slices.Contains(WorkspaceIsolationPolicies, grant.Policy) {
		return fmt.Errorf("workspace lease policy is missing or unknown")
	}
	if !slices.Contains(WorkspaceCleanupPolicies, grant.Cleanup) {
		return fmt.Errorf("workspace lease cleanup policy is missing or unknown")
	}
	for name, targetPath := range map[string]string{
		"root":         grant.Root,
		"sourcePath":   grant.SourcePath,
		"worktreePath": grant.WorktreePath,
	} {
		if !strings.HasPrefix(targetPath, "/") || !IsBoundedString(targetPath, TextMaximumBytes) || strings.ContainsRune(targetPath, 0) {
			return fmt.Errorf("workspace lease %s must be an absolute path", name)
		}
	}
	if grant.ResolvedBaseRevision != "" && !IsResolvedRevision(grant.ResolvedBaseRevision) {
		return fmt.Errorf("workspace lease resolved base revision is malformed")
	}
	if grant.Policy == WorkspaceIsolationExclusiveExisting {
		if grant.Repository != "" || grant.BaseRevision != "" || grant.ResolvedBaseRevision != "" || grant.Branch != "" {
			return fmt.Errorf("exclusive-existing workspace lease must not name a repository, base, or branch")
		}
		if grant.WorktreePath != grant.SourcePath {
			return fmt.Errorf("exclusive-existing workspace lease must use its source path")
		}
		return nil
	}
	if grant.Repository != "" {
		if identity, ok := NormalizeRepositoryIdentity(grant.Repository); !ok || !IsBoundedString(grant.Repository, 512) || identity != grant.Repository {
			return fmt.Errorf("workspace lease repository identity is not normalized")
		}
	}
	if grant.BaseRevision != "HEAD" {
		if !strings.HasPrefix(grant.BaseRevision, "refs/heads/") || !IsWorkspaceLeaseBaseBranch(strings.TrimPrefix(grant.BaseRevision, "refs/heads/")) {
			return fmt.Errorf("workspace lease base revision is malformed")
		}
	}
	if !strings.HasPrefix(grant.Branch, WorkspaceLeaseBranchPrefix) {
		return fmt.Errorf("workspace lease branch is malformed")
	}
	return nil
}

type WorkspaceLeaseUpdate struct {
	LeaseID              string `json:"leaseId"`
	Status               string `json:"status"`
	RetentionReason      string `json:"retentionReason,omitempty"`
	ResolvedBaseRevision string `json:"resolvedBaseRevision,omitempty"`
	Detail               string `json:"detail,omitempty"`
}

func (update WorkspaceLeaseUpdate) Validate() error {
	if !IsIdentifier(update.LeaseID) {
		return fmt.Errorf("workspace lease update is missing identity")
	}
	if !slices.Contains(WorkspaceLeaseStatuses, update.Status) {
		return fmt.Errorf("workspace lease status is missing or unknown")
	}
	if update.Status == "retained" {
		if !slices.Contains(WorkspaceRetentionReasons, update.RetentionReason) {
			return fmt.Errorf("workspace lease retention reason does not match its status")
		}
	} else if update.RetentionReason != "" {
		return fmt.Errorf("workspace lease retention reason does not match its status")
	}
	if update.ResolvedBaseRevision != "" && !IsResolvedRevision(update.ResolvedBaseRevision) {
		return fmt.Errorf("workspace lease resolved base revision is malformed")
	}
	if !IsBoundedString(update.Detail, DiagnosticMaximumBytes) {
		return fmt.Errorf("workspace lease detail exceeds its bound")
	}
	return nil
}
