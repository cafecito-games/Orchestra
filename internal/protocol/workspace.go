package protocol

import (
	"errors"
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
	MaximumDiscoveredRepositories        = 256
	MaximumRemotesPerRepository          = 16
	MaximumWorkspaceSubpathBytes         = 256
	RepositoryURLMaximumBytes            = 512
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
		return errors.New("workspace lease id is malformed")
	}
	if !slices.Contains(WorkspaceLeaseStatuses, grant.Status) {
		return errors.New("workspace lease status is missing or unknown")
	}
	if !slices.Contains(WorkspaceIsolationPolicies, grant.Policy) {
		return errors.New("workspace lease policy is missing or unknown")
	}
	if !slices.Contains(WorkspaceCleanupPolicies, grant.Cleanup) {
		return errors.New("workspace lease cleanup policy is missing or unknown")
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
		return errors.New("workspace lease resolved base revision is malformed")
	}
	if grant.Policy == WorkspaceIsolationExclusiveExisting {
		if grant.Repository != "" || grant.BaseRevision != "" || grant.ResolvedBaseRevision != "" || grant.Branch != "" {
			return errors.New("exclusive-existing workspace lease must not name a repository, base, or branch")
		}
		if grant.WorktreePath != grant.SourcePath {
			return errors.New("exclusive-existing workspace lease must use its source path")
		}
		return nil
	}
	if grant.Repository != "" {
		if identity, ok := NormalizeRepositoryIdentity(grant.Repository); !ok || !IsBoundedString(grant.Repository, 512) || identity != grant.Repository {
			return errors.New("workspace lease repository identity is not normalized")
		}
	}
	if grant.BaseRevision != "HEAD" {
		branchName, ok := strings.CutPrefix(grant.BaseRevision, "refs/heads/")
		if !ok || !IsWorkspaceLeaseBaseBranch(branchName) {
			return errors.New("workspace lease base revision is malformed")
		}
	}
	if !strings.HasPrefix(grant.Branch, WorkspaceLeaseBranchPrefix) {
		return errors.New("workspace lease branch is malformed")
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
		return errors.New("workspace lease update is missing identity")
	}
	if !slices.Contains(WorkspaceLeaseStatuses, update.Status) {
		return errors.New("workspace lease status is missing or unknown")
	}
	if update.Status == "retained" {
		if !slices.Contains(WorkspaceRetentionReasons, update.RetentionReason) {
			return errors.New("workspace lease retention reason does not match its status")
		}
	} else if update.RetentionReason != "" {
		return errors.New("workspace lease retention reason does not match its status")
	}
	if update.ResolvedBaseRevision != "" && !IsResolvedRevision(update.ResolvedBaseRevision) {
		return errors.New("workspace lease resolved base revision is malformed")
	}
	if !IsBoundedString(update.Detail, DiagnosticMaximumBytes) {
		return errors.New("workspace lease detail exceeds its bound")
	}
	return nil
}

var scpRepositoryURLPattern = regexp.MustCompile(`^(?:([A-Za-z0-9._-]+)@)?([A-Za-z0-9.-]+):([A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*)$`)

// CanonicalRepositoryKey produces a transport-agnostic `<host>/<path>` key so SSH, HTTPS,
// and bare `<host>/<org>/<repo>` identifiers resolve to the same repository checkout.
func CanonicalRepositoryKey(repositoryURL string) (string, bool) {
	normalized, ok := NormalizeRepositoryIdentity(strings.TrimSpace(repositoryURL))
	if !ok {
		return "", false
	}
	var hostPart, repositoryPath string
	if match := schemeRepositoryURLPattern.FindStringSubmatch(normalized); match != nil {
		scheme := strings.ToLower(match[1])
		if scheme == "file://" {
			cleaned := path.Clean("/" + strings.TrimPrefix(match[3], "/"))
			return "file://" + cleaned, true
		}
		hostPart = strings.ToLower(match[2])
		repositoryPath = strings.TrimPrefix(match[3], "/")
	} else if colonIndex := strings.IndexByte(normalized, ':'); colonIndex > 0 && !strings.Contains(normalized[:colonIndex], "/") {
		hostPart = strings.ToLower(normalized[:colonIndex])
		repositoryPath = strings.TrimPrefix(normalized[colonIndex+1:], "/")
	} else {
		slashIndex := strings.IndexByte(normalized, '/')
		if slashIndex <= 0 {
			return "", false
		}
		hostPart = strings.ToLower(normalized[:slashIndex])
		repositoryPath = strings.TrimPrefix(normalized[slashIndex+1:], "/")
	}
	for _, defaultPort := range []string{":22", ":80", ":443"} {
		hostPart = strings.TrimSuffix(hostPart, defaultPort)
	}
	cleanedPath := path.Clean(repositoryPath)
	if hostPart == "" || cleanedPath == "" || cleanedPath == "." || strings.HasPrefix(cleanedPath, "../") || cleanedPath == ".." {
		return "", false
	}
	return hostPart + "/" + strings.ToLower(cleanedPath), true
}

func RepositoriesMatch(firstRepository, secondRepository string) bool {
	firstNormalized, firstOK := NormalizeRepositoryIdentity(strings.TrimSpace(firstRepository))
	secondNormalized, secondOK := NormalizeRepositoryIdentity(strings.TrimSpace(secondRepository))
	if !firstOK || !secondOK {
		return false
	}
	if firstNormalized == secondNormalized {
		return true
	}
	firstKey, firstKeyOK := CanonicalRepositoryKey(firstNormalized)
	secondKey, secondKeyOK := CanonicalRepositoryKey(secondNormalized)
	return firstKeyOK && secondKeyOK && firstKey == secondKey
}

// ValidateCloneRepositoryURL validates a remote Git clone URL, rejecting option-injection
// prefixes, unsafe remote-helper transports, local file URLs, and embedded credentials.
func ValidateCloneRepositoryURL(rawURL string) (string, string, error) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" || !IsBoundedString(trimmed, RepositoryURLMaximumBytes) {
		return "", "", errors.New("repository URL must be between 1 and 512 bytes")
	}
	if strings.HasPrefix(trimmed, "-") {
		return "", "", errors.New("repository URL must not start with '-'")
	}
	for _, character := range trimmed {
		if character <= 0x20 || character == 0x7f {
			return "", "", errors.New("repository URL must not contain whitespace or control characters")
		}
	}
	if strings.Contains(trimmed, "::") {
		return "", "", errors.New("git remote helper transports are not allowed")
	}
	if LooksSecretLike(trimmed) {
		return "", "", errors.New("repository URL appears to contain secret material")
	}

	if match := schemeRepositoryURLPattern.FindStringSubmatch(trimmed); match != nil {
		scheme := strings.ToLower(match[1])
		switch scheme {
		case "https://", "http://", "ssh://", "git://":
		default:
			return "", "", fmt.Errorf("unsupported repository URL scheme %q (expected https, http, ssh, or git)", strings.TrimSuffix(scheme, "://"))
		}
		authority := match[2]
		if atIndex := strings.LastIndexByte(authority, '@'); atIndex >= 0 {
			userInfo := authority[:atIndex]
			if userInfo == "" || strings.Contains(userInfo, ":") || strings.Contains(userInfo, "@") {
				return "", "", errors.New("repository URL must not embed password or token credentials")
			}
		}
		repositoryPath := strings.Trim(match[3], "/")
		if repositoryPath == "" || strings.Contains(repositoryPath, "..") {
			return "", "", errors.New("repository URL is missing a valid repository path")
		}
	} else {
		match := scpRepositoryURLPattern.FindStringSubmatch(trimmed)
		if match == nil {
			return "", "", errors.New("repository URL must use https://, http://, ssh://, git://, or [user@]host:org/repo syntax")
		}
		if strings.Contains(match[3], "..") {
			return "", "", errors.New("repository URL contains invalid path traversal")
		}
	}

	normalizedIdentity, ok := NormalizeRepositoryIdentity(trimmed)
	if !ok {
		return "", "", errors.New("repository URL could not be normalized")
	}
	return trimmed, normalizedIdentity, nil
}

func ValidateRelativeWorkspaceSubpath(rawSubpath string) (string, error) {
	trimmed := strings.TrimSpace(rawSubpath)
	if trimmed == "" || !IsBoundedString(trimmed, MaximumWorkspaceSubpathBytes) {
		return "", errors.New("directory must be between 1 and 256 bytes")
	}
	if strings.HasPrefix(trimmed, "/") || strings.HasPrefix(trimmed, "\\") || strings.ContainsRune(trimmed, 0) || strings.ContainsRune(trimmed, '\\') || strings.ContainsRune(trimmed, ':') {
		return "", errors.New("directory must be a clean relative path beneath the workspace root")
	}
	for segment := range strings.SplitSeq(trimmed, "/") {
		if segment == "" || segment == "." || segment == ".." || segment == ".git" || segment == ".orchestra" || strings.HasPrefix(segment, "-") {
			return "", fmt.Errorf("directory contains disallowed path segment %q", segment)
		}
	}
	cleaned := path.Clean(trimmed)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", errors.New("directory escapes workspace root")
	}
	return cleaned, nil
}

func DefaultCloneDirectoryName(repositoryURL string) (string, error) {
	normalized, ok := NormalizeRepositoryIdentity(strings.TrimSpace(repositoryURL))
	if !ok {
		return "", errors.New("repository URL is invalid")
	}
	tail := normalized
	if slashIndex := strings.LastIndexByte(tail, '/'); slashIndex >= 0 {
		tail = tail[slashIndex+1:]
	}
	if colonIndex := strings.LastIndexByte(tail, ':'); colonIndex >= 0 {
		tail = tail[colonIndex+1:]
	}
	tail = strings.TrimSuffix(tail, ".git")
	return ValidateRelativeWorkspaceSubpath(tail)
}

type DiscoveredRepository struct {
	Root          string   `json:"root"`
	Path          string   `json:"path"`
	RelativePath  string   `json:"relativePath"`
	Repository    string   `json:"repository,omitempty"`
	Remotes       []string `json:"remotes,omitempty"`
	DefaultBranch string   `json:"defaultBranch,omitempty"`
	HeadBranch    string   `json:"headBranch,omitempty"`
}

func (discovered DiscoveredRepository) MatchesRepository(requestedRepository string) bool {
	if discovered.Repository != "" && RepositoriesMatch(discovered.Repository, requestedRepository) {
		return true
	}
	for _, remote := range discovered.Remotes {
		if RepositoriesMatch(remote, requestedRepository) {
			return true
		}
	}
	return false
}

func (discovered DiscoveredRepository) Validate() error {
	for name, targetPath := range map[string]string{
		"root": discovered.Root,
		"path": discovered.Path,
	} {
		if !strings.HasPrefix(targetPath, "/") || !IsBoundedString(targetPath, TextMaximumBytes) || strings.ContainsRune(targetPath, 0) {
			return fmt.Errorf("discovered repository %s must be an absolute path", name)
		}
	}
	cleanRoot := path.Clean(discovered.Root)
	cleanPath := path.Clean(discovered.Path)
	if cleanPath == cleanRoot {
		if discovered.RelativePath != "." {
			return errors.New("discovered repository at workspace root must have relativePath '.'")
		}
	} else {
		if !strings.HasPrefix(cleanPath, cleanRoot+"/") {
			return errors.New("discovered repository path must reside within its workspace root")
		}
		validatedRelative, err := ValidateRelativeWorkspaceSubpath(discovered.RelativePath)
		if err != nil || path.Join(cleanRoot, validatedRelative) != cleanPath {
			return errors.New("discovered repository relativePath does not match path")
		}
	}
	if discovered.Repository != "" {
		normalized, ok := NormalizeRepositoryIdentity(discovered.Repository)
		if !ok || !IsBoundedString(discovered.Repository, RepositoryURLMaximumBytes) || normalized != discovered.Repository {
			return errors.New("discovered repository identity is not normalized")
		}
	}
	if len(discovered.Remotes) > MaximumRemotesPerRepository {
		return errors.New("discovered repository remotes exceed maximum count")
	}
	for _, remote := range discovered.Remotes {
		normalized, ok := NormalizeRepositoryIdentity(remote)
		if !ok || !IsBoundedString(remote, RepositoryURLMaximumBytes) || normalized != remote {
			return errors.New("discovered repository remote is not normalized")
		}
	}
	if discovered.DefaultBranch != "" && !IsWorkspaceLeaseBaseBranch(discovered.DefaultBranch) {
		return errors.New("discovered repository defaultBranch is malformed")
	}
	if discovered.HeadBranch != "" && !IsWorkspaceLeaseBaseBranch(discovered.HeadBranch) {
		return errors.New("discovered repository headBranch is malformed")
	}
	return nil
}

type NodeRepositoryReport struct {
	NodeID       string                 `json:"nodeId"`
	Roots        []string               `json:"roots"`
	Repositories []DiscoveredRepository `json:"repositories"`
	ObservedAt   string                 `json:"observedAt"`
}

func (report NodeRepositoryReport) Validate() error {
	if !IsIdentifier(report.NodeID) {
		return errors.New("repository report nodeId is invalid")
	}
	if len(report.Roots) > 32 {
		return errors.New("repository report roots exceed maximum count")
	}
	cleanRoots := make([]string, 0, len(report.Roots))
	for _, root := range report.Roots {
		if !strings.HasPrefix(root, "/") || !IsBoundedString(root, TextMaximumBytes) || strings.ContainsRune(root, 0) {
			return errors.New("repository report root must be an absolute path")
		}
		cleanRoots = append(cleanRoots, path.Clean(root))
	}
	if len(report.Repositories) > MaximumDiscoveredRepositories {
		return errors.New("repository report exceeds maximum discovered repositories")
	}
	for _, repository := range report.Repositories {
		if err := repository.Validate(); err != nil {
			return err
		}
		if len(cleanRoots) > 0 && !slices.Contains(cleanRoots, path.Clean(repository.Root)) {
			return errors.New("discovered repository root is not listed in report roots")
		}
	}
	if strings.TrimSpace(report.ObservedAt) == "" || !IsBoundedString(report.ObservedAt, 64) {
		return errors.New("repository report observedAt is required")
	}
	return nil
}

type RepositoryCloneDirective struct {
	RequestID     string `json:"requestId"`
	RepositoryURL string `json:"repositoryUrl"`
	WorkspaceRoot string `json:"workspaceRoot,omitempty"`
	Directory     string `json:"directory,omitempty"`
	Branch        string `json:"branch,omitempty"`
}

func (directive RepositoryCloneDirective) Validate() error {
	if !IsIdentifier(directive.RequestID) {
		return errors.New("clone directive requestId is invalid")
	}
	if _, _, err := ValidateCloneRepositoryURL(directive.RepositoryURL); err != nil {
		return err
	}
	if directive.WorkspaceRoot != "" {
		if !strings.HasPrefix(directive.WorkspaceRoot, "/") || !IsBoundedString(directive.WorkspaceRoot, TextMaximumBytes) || strings.ContainsRune(directive.WorkspaceRoot, 0) {
			return errors.New("clone directive workspaceRoot must be an absolute path")
		}
	}
	if directive.Directory != "" {
		if _, err := ValidateRelativeWorkspaceSubpath(directive.Directory); err != nil {
			return err
		}
	}
	if directive.Branch != "" && !IsWorkspaceLeaseBaseBranch(directive.Branch) {
		return errors.New("clone directive branch is invalid")
	}
	return nil
}

type RepositoryCloneResult struct {
	RequestID  string                `json:"requestId"`
	NodeID     string                `json:"nodeId"`
	Repository *DiscoveredRepository `json:"repository,omitempty"`
	Report     *NodeRepositoryReport `json:"report,omitempty"`
	Created    bool                  `json:"created,omitempty"`
	Error      string                `json:"error,omitempty"`
	ObservedAt string                `json:"observedAt"`
}

func (result RepositoryCloneResult) Validate() error {
	if !IsIdentifier(result.RequestID) {
		return errors.New("clone result requestId is invalid")
	}
	if !IsIdentifier(result.NodeID) {
		return errors.New("clone result nodeId is invalid")
	}
	if result.Repository != nil {
		if err := result.Repository.Validate(); err != nil {
			return err
		}
	}
	if result.Report != nil {
		if result.Report.NodeID != result.NodeID {
			return errors.New("clone result report nodeId mismatch")
		}
		if err := result.Report.Validate(); err != nil {
			return err
		}
	}
	if !IsBoundedString(result.Error, DiagnosticMaximumBytes) || LooksSecretLike(result.Error) {
		return errors.New("clone result error is invalid")
	}
	if strings.TrimSpace(result.ObservedAt) == "" || !IsBoundedString(result.ObservedAt, 64) {
		return errors.New("clone result observedAt is required")
	}
	return nil
}
