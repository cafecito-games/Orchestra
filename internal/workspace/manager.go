// Package workspace provisions and cleans up orchestrator-granted workspace leases
// on a slave worker node.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/safepath"
)

const (
	tombstoneDirectory       = ".orchestra/tombstones"
	leaseLockDirectory       = ".orchestra/leases"
	landingWorktreeDirectory = ".orchestra/landing"
	leaseConfigName          = "orchestraLease"
	baseConfigName           = "orchestraBase"
)

const (
	ModeRun       = "run"
	ModeReconcile = protocol.WorkspaceCleanupModeReconcile
	ModeOperator  = protocol.WorkspaceCleanupModeOperator
)

type Outcome struct {
	Status               string
	RetentionReason      string
	ResolvedBaseRevision string
	Detail               string
	Path                 string
}

func (outcome Outcome) Update(leaseID string) protocol.WorkspaceLeaseUpdate {
	return protocol.WorkspaceLeaseUpdate{
		LeaseID:              leaseID,
		Status:               outcome.Status,
		RetentionReason:      outcome.RetentionReason,
		ResolvedBaseRevision: outcome.ResolvedBaseRevision,
		Detail:               outcome.Detail,
	}
}

func failed(detail string) Outcome {
	return Outcome{Status: "failed", Detail: detail}
}

func retained(reason, detail string) Outcome {
	return Outcome{Status: "retained", RetentionReason: reason, Detail: detail}
}

type Manager struct {
	roots []string
	git   *Git

	mutex          sync.Mutex
	repositories   map[string]*sync.Mutex
	owners         map[string]struct{}
	sharedReaders  map[string]int
	mutatingLeases map[string]struct{}
	exclusive      map[string]string
	tombstones     map[string]struct{}
}

func NewManager(roots []string, git *Git) *Manager {
	return &Manager{
		roots:          slices.Clone(roots),
		git:            git,
		repositories:   map[string]*sync.Mutex{},
		owners:         map[string]struct{}{},
		sharedReaders:  map[string]int{},
		mutatingLeases: map[string]struct{}{},
		exclusive:      map[string]string{},
		tombstones:     map[string]struct{}{},
	}
}

func (manager *Manager) Supports(policy string) bool {
	switch policy {
	case protocol.WorkspaceIsolationExclusiveExisting:
		return true
	case protocol.WorkspaceIsolationGitWorktree:
		return manager.git != nil
	default:
		return false
	}
}

var ErrLeaseOwned = errors.New("workspace lease is owned by another operation")

func (manager *Manager) Own(grant protocol.WorkspaceLeaseGrant) (func(), error) {
	if !protocol.IsWorkspaceLeaseIdentity(grant.ID) || !slices.Contains(manager.roots, grant.Root) {
		return nil, errors.New("the workspace lease cannot be owned on this node")
	}
	if (grant.SourceLeaseID != "") != grant.ReadOnly {
		return nil, errors.New("workspace lease readOnly flag must match sourceLeaseId presence")
	}
	if grant.SourceLeaseID != "" {
		if !protocol.IsWorkspaceLeaseIdentity(grant.SourceLeaseID) || grant.SourceLeaseID == grant.ID {
			return nil, errors.New("the shared source workspace lease id is malformed")
		}
		manager.mutex.Lock()
		if _, held := manager.owners[grant.ID]; held {
			manager.mutex.Unlock()
			return nil, ErrLeaseOwned
		}
		if _, mutating := manager.mutatingLeases[grant.SourceLeaseID]; mutating {
			manager.mutex.Unlock()
			return nil, ErrLeaseOwned
		}
		manager.owners[grant.ID] = struct{}{}
		manager.sharedReaders[grant.SourceLeaseID]++
		manager.mutex.Unlock()

		var once sync.Once
		return func() {
			once.Do(func() {
				manager.mutex.Lock()
				delete(manager.owners, grant.ID)
				if remaining := manager.sharedReaders[grant.SourceLeaseID] - 1; remaining > 0 {
					manager.sharedReaders[grant.SourceLeaseID] = remaining
				} else {
					delete(manager.sharedReaders, grant.SourceLeaseID)
				}
				manager.mutex.Unlock()
			})
		}, nil
	}

	manager.mutex.Lock()
	if _, held := manager.owners[grant.ID]; held {
		manager.mutex.Unlock()
		return nil, ErrLeaseOwned
	}
	manager.owners[grant.ID] = struct{}{}
	manager.mutex.Unlock()
	forget := func() {
		manager.mutex.Lock()
		delete(manager.owners, grant.ID)
		manager.mutex.Unlock()
	}
	var lock *os.File
	if grant.Policy == protocol.WorkspaceIsolationGitWorktree {
		file, err := openLeaseLock(grant)
		if err != nil {
			forget()
			return nil, err
		}
		lock = file
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			if lock != nil {
				_ = unlockFile(lock)
				_ = lock.Close()
			}
			forget()
		})
	}, nil
}

func (manager *Manager) beginSourceMutation(leaseID string) bool {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if manager.sharedReaders[leaseID] > 0 {
		return false
	}
	if _, mutating := manager.mutatingLeases[leaseID]; mutating {
		return false
	}
	manager.mutatingLeases[leaseID] = struct{}{}
	return true
}

func (manager *Manager) endSourceMutation(leaseID string) {
	manager.mutex.Lock()
	delete(manager.mutatingLeases, leaseID)
	manager.mutex.Unlock()
}

func openLeaseLock(grant protocol.WorkspaceLeaseGrant) (*os.File, error) {
	directory, err := safepath.EnsureDirectoryWithinRoot(grant.Root, filepath.Join(grant.Root, filepath.FromSlash(leaseLockDirectory)), 0o700)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filepath.Join(directory, grant.ID+".lock"), openLockFlags, 0o600)
	if err != nil {
		return nil, err
	}
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() {
		_ = file.Close()
		return nil, errors.New("the lease lock is not a regular file")
	}
	if err := lockFile(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func (manager *Manager) repositoryLock(sourcePath string) *sync.Mutex {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	lock := manager.repositories[sourcePath]
	if lock == nil {
		lock = &sync.Mutex{}
		manager.repositories[sourcePath] = lock
	}
	return lock
}

func (manager *Manager) Provision(operationContext context.Context, grant protocol.WorkspaceLeaseGrant, taskID, runID string) Outcome {
	if problem := manager.checkGrant(grant, runID); problem != "" {
		return failed(problem)
	}
	if !slices.Contains([]string{"requested", "provisioning", "active", "retained"}, grant.Status) {
		return failed("the workspace lease is not in a provisionable status")
	}
	if manager.tombstoned(grant) {
		return failed("workspace lease was already settled on this node")
	}
	if grant.SourceLeaseID != "" {
		return manager.provisionSharedReadOnly(operationContext, grant)
	}
	if grant.Policy == protocol.WorkspaceIsolationExclusiveExisting {
		return manager.provisionExclusive(grant)
	}
	if grant.Status == "retained" {
		branchTaskID, branchRunID, found := strings.Cut(strings.TrimPrefix(grant.Branch, protocol.WorkspaceLeaseBranchPrefix), "/")
		if !found || !protocol.IsWorkspaceLeaseIdentity(taskID) || branchTaskID != taskID || !protocol.IsWorkspaceLeaseIdentity(branchRunID) {
			return failed("workspace lease branch does not match its task")
		}
	} else if !protocol.IsWorkspaceLeaseIdentity(taskID) || grant.Branch != protocol.WorkspaceLeaseBranch(taskID, runID) {
		return failed("workspace lease branch does not match its task and run")
	}
	if manager.git == nil {
		return failed("git is not available on this node")
	}
	lock := manager.repositoryLock(grant.SourcePath)
	lock.Lock()
	defer lock.Unlock()

	if problem := manager.verifyRepository(operationContext, grant); problem != "" {
		return failed(problem)
	}
	state, err := manager.inspect(operationContext, grant)
	if err != nil {
		return failed("local worktree state could not be read")
	}
	switch {
	case state.target != nil:
		return manager.adopt(operationContext, grant, state)
	case state.pathExists:
		return retained("unregistered", "the lease's worktree path exists but is not a registered worktree")
	case state.branchHead != "":
		return retained("identity-mismatch", "the lease's branch already exists without its worktree")
	case state.recordedLease != "" && state.recordedLease != grant.ID:
		return failed("the lease's branch name is recorded for a different lease")
	}

	base, problem := manager.resolveBase(operationContext, grant)
	if problem != "" {
		return failed(problem)
	}
	if grant.ResolvedBaseRevision != "" && grant.ResolvedBaseRevision != base {
		return failed("the base ref no longer resolves to the lease's recorded base revision")
	}
	managedDirectory := filepath.Dir(grant.WorktreePath)
	if _, err := safepath.EnsureDirectoryWithinRoot(grant.Root, managedDirectory, 0o700); err != nil {
		return failed("the managed worktree directory could not be created safely")
	}
	if err := createLeaf(grant.Root, grant.WorktreePath); err != nil {
		return retained("ambiguous", "the lease's worktree directory could not be created exclusively")
	}
	if err := manager.recordIdentity(operationContext, grant, base); err != nil {
		return manager.reconcileFailedProvision(operationContext, grant, "the lease identity could not be recorded")
	}
	if _, err := safepath.VerifyDirectoryWithinRoot(grant.Root, grant.WorktreePath); err != nil {
		return retained("identity-mismatch", "the lease's worktree directory was replaced before checkout")
	}
	if _, err := manager.git.Run(operationContext, grant.SourcePath, "worktree", "add", "--quiet", "-b", grant.Branch, "--", grant.WorktreePath, base); err != nil {
		return manager.reconcileFailedProvision(operationContext, grant, "git worktree add failed")
	}
	state, err = manager.inspect(operationContext, grant)
	if err != nil || state.target == nil {
		return manager.reconcileFailedProvision(operationContext, grant, "the new worktree is not registered")
	}
	return manager.adopt(operationContext, grant, state)
}

func (manager *Manager) reconcileFailedProvision(operationContext context.Context, grant protocol.WorkspaceLeaseGrant, detail string) Outcome {
	state, err := manager.inspect(operationContext, grant)
	if err != nil {
		return retained("ambiguous", detail+"; local state could not be re-read")
	}
	if state.target != nil {
		if outcome := manager.adopt(operationContext, grant, state); outcome.Status == "active" {
			return outcome
		}
		return retained("ambiguous", detail+"; a partial worktree remains")
	}
	if state.pathExists && removeEmptyLeaf(grant.Root, grant.WorktreePath) {
		state.pathExists = false
	}
	if state.recordedLease == grant.ID {
		manager.forgetIdentity(operationContext, grant)
	}
	if state.pathExists || state.branchHead != "" {
		return retained("ambiguous", detail+"; a partial worktree remains")
	}
	return failed(detail)
}

func (manager *Manager) provisionExclusive(grant protocol.WorkspaceLeaseGrant) Outcome {
	if err := canonicalDirectory(grant.SourcePath); err != nil {
		return failed("the exclusive workspace is missing, not a directory, or reached through a symbolic link")
	}
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if holder, held := manager.exclusive[grant.SourcePath]; held && holder != grant.ID {
		return failed("the exclusive workspace is held by another lease")
	}
	manager.exclusive[grant.SourcePath] = grant.ID
	return Outcome{Status: "active", Path: grant.SourcePath}
}

func (manager *Manager) releaseExclusive(grant protocol.WorkspaceLeaseGrant) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	if manager.exclusive[grant.SourcePath] == grant.ID {
		delete(manager.exclusive, grant.SourcePath)
	}
}

func (manager *Manager) adopt(operationContext context.Context, grant protocol.WorkspaceLeaseGrant, state localState) Outcome {
	if reason, detail := manager.ownership(operationContext, grant, state); reason != "" {
		return retained(reason, detail)
	}
	if state.target.Head != state.recordedBase {
		_, err := manager.git.Run(operationContext, grant.SourcePath, "merge-base", "--is-ancestor", state.recordedBase, state.target.Head)
		if err != nil {
			return retained("identity-mismatch", "the existing worktree does not descend from the lease's base revision")
		}
	}
	return Outcome{Status: "active", Path: grant.WorktreePath, ResolvedBaseRevision: state.recordedBase}
}

func (manager *Manager) ownership(operationContext context.Context, grant protocol.WorkspaceLeaseGrant, state localState) (string, string) {
	target := state.target
	switch {
	case target.Locked:
		return "locked", "the worktree is locked"
	case target.Prunable || target.Unrecognized || target.Bare || target.Detached:
		return "ambiguous", "the worktree is prunable, detached, or reported with unknown attributes"
	case target.Branch != "refs/heads/"+grant.Branch:
		return "identity-mismatch", "the worktree has a different branch checked out"
	case state.branchCheckouts != 1:
		return "ambiguous", "the lease's branch is checked out in more than one worktree"
	case state.recordedLease != grant.ID:
		return "identity-mismatch", "the branch is not recorded as owned by this lease"
	case !protocol.IsResolvedRevision(state.recordedBase):
		return "identity-mismatch", "the branch has no recorded base revision"
	case grant.ResolvedBaseRevision != "" && grant.ResolvedBaseRevision != state.recordedBase:
		return "identity-mismatch", "the recorded base revision differs from the lease"
	case !protocol.IsResolvedRevision(target.Head):
		return "ambiguous", "the worktree HEAD is not a commit"
	}
	if _, err := safepath.VerifyDirectoryWithinRoot(grant.Root, grant.WorktreePath); err != nil {
		return "ambiguous", "the worktree path is missing or reached through a symbolic link"
	}
	if err := canonicalDirectory(grant.WorktreePath); err != nil {
		return "ambiguous", "the worktree path is missing or reached through a symbolic link"
	}
	if !linkedToRepository(grant) {
		return "identity-mismatch", "the worktree's .git file does not point into its repository"
	}
	output, err := manager.git.Run(operationContext, grant.WorktreePath, "rev-parse", "--path-format=absolute", "--show-toplevel", "--git-common-dir")
	if err != nil {
		return "ambiguous", "the worktree's repository could not be read"
	}
	lines := strings.Split(strings.TrimSuffix(string(output), "\n"), "\n")
	if len(lines) != 2 || lines[0] != grant.WorktreePath || !sameCanonicalPath(lines[1], filepath.Join(grant.SourcePath, ".git")) {
		return "identity-mismatch", "the worktree belongs to a different repository"
	}
	return "", ""
}

func (manager *Manager) Cleanup(operationContext context.Context, grant protocol.WorkspaceLeaseGrant, runID, mode string, beforeMutation func()) Outcome {
	if problem := manager.checkGrant(grant, runID); problem != "" {
		return retained("identity-mismatch", problem)
	}
	if grant.SourceLeaseID != "" {
		if grant.Status != "requested" && grant.Status != "provisioning" && beforeMutation != nil {
			beforeMutation()
		}
		return manager.settle(grant)
	}
	if grant.Policy == protocol.WorkspaceIsolationExclusiveExisting {
		manager.releaseExclusive(grant)
		return manager.settle(grant)
	}
	if manager.git == nil {
		return retained("ambiguous", "git is not available to verify the workspace")
	}
	lock := manager.repositoryLock(grant.SourcePath)
	lock.Lock()
	defer lock.Unlock()

	if problem := manager.verifyRepository(operationContext, grant); problem != "" {
		return retained("identity-mismatch", problem)
	}
	state, err := manager.inspect(operationContext, grant)
	if err != nil {
		return retained("ambiguous", "local worktree state could not be read")
	}
	mutating := false
	mutate := func() {
		if !mutating && beforeMutation != nil {
			beforeMutation()
		}
		mutating = true
	}
	if state.target == nil && !state.pathExists && state.branchHead == "" {
		if state.recordedLease == grant.ID {
			mutate()
			manager.forgetIdentity(operationContext, grant)
		}
		return manager.settle(grant)
	}
	if mode != ModeOperator && grant.Cleanup != protocol.WorkspaceCleanupWhenUnchanged {
		return retained("policy", "the workspace cleanup policy retains finished workspaces")
	}
	if !manager.beginSourceMutation(grant.ID) {
		return retained("locked", "the worktree has active read-only inspectors")
	}
	defer manager.endSourceMutation(grant.ID)
	if state.target == nil {
		if state.pathExists {
			return retained("unregistered", "the lease's worktree path exists but is not a registered worktree")
		}
		if state.recordedLease != grant.ID || state.branchCheckouts != 0 {
			return retained("identity-mismatch", "the lease's branch is not provably owned by this lease")
		}
		if state.branchHead != state.recordedBase {
			return retained("diverged", "the lease's branch has commits beyond its base revision")
		}
		mutate()
		return manager.removeBranch(operationContext, grant, state)
	}
	if reason, detail := manager.ownership(operationContext, grant, state); reason != "" {
		return retained(reason, detail)
	}
	output, err := manager.git.Run(operationContext, grant.WorktreePath, "status", "--porcelain=v1", "-z", "--untracked-files=all", "--ignored=matching", "--ignore-submodules=none")
	if err != nil {
		return retained("ambiguous", "the worktree status could not be read")
	}
	summary, err := ParseStatus(output)
	switch {
	case err != nil:
		return retained("ambiguous", "the worktree status could not be parsed")
	case summary.Modified > 0:
		return retained("dirty", "the worktree has uncommitted changes")
	case summary.Untracked > 0 || summary.Ignored > 0:
		return retained("untracked", "the worktree has untracked or ignored files")
	case state.target.Head != state.recordedBase:
		return retained("diverged", "the lease's branch has commits beyond its base revision")
	}
	mutate()
	if _, err := manager.git.Run(operationContext, grant.SourcePath, "worktree", "remove", "--", grant.WorktreePath); err != nil {
		return retained("ambiguous", "git refused to remove the worktree")
	}
	state, err = manager.inspect(operationContext, grant)
	if err != nil || state.target != nil || state.pathExists {
		return retained("ambiguous", "the worktree is still present after removal")
	}
	return manager.removeBranch(operationContext, grant, state)
}

func (manager *Manager) removeBranch(operationContext context.Context, grant protocol.WorkspaceLeaseGrant, state localState) Outcome {
	if state.branchHead != "" {
		if _, err := manager.git.Run(operationContext, grant.SourcePath, "update-ref", "-d", "refs/heads/"+grant.Branch, state.recordedBase); err != nil {
			return retained("ambiguous", "the lease's branch could not be removed")
		}
	}
	manager.forgetIdentity(operationContext, grant)
	state, err := manager.inspect(operationContext, grant)
	if err != nil || state.target != nil || state.pathExists || state.branchHead != "" {
		return retained("ambiguous", "the lease's workspace is still present after removal")
	}
	return manager.settle(grant)
}

func (manager *Manager) settle(grant protocol.WorkspaceLeaseGrant) Outcome {
	manager.writeTombstone(grant)
	if grant.Status == "requested" || grant.Status == "provisioning" {
		return failed("no workspace exists for this lease")
	}
	return Outcome{Status: "cleaned"}
}

func (manager *Manager) tombstonePath(grant protocol.WorkspaceLeaseGrant) string {
	return filepath.Join(grant.Root, filepath.FromSlash(tombstoneDirectory), grant.ID)
}

func (manager *Manager) tombstoned(grant protocol.WorkspaceLeaseGrant) bool {
	manager.mutex.Lock()
	_, remembered := manager.tombstones[grant.ID]
	manager.mutex.Unlock()
	if remembered || grant.Policy != protocol.WorkspaceIsolationGitWorktree {
		return remembered
	}
	_, err := os.Lstat(manager.tombstonePath(grant))
	return !errors.Is(err, fs.ErrNotExist)
}

func (manager *Manager) writeTombstone(grant protocol.WorkspaceLeaseGrant) {
	manager.mutex.Lock()
	manager.tombstones[grant.ID] = struct{}{}
	manager.mutex.Unlock()
	if grant.Policy != protocol.WorkspaceIsolationGitWorktree {
		return
	}
	if _, err := safepath.EnsureDirectoryWithinRoot(grant.Root, filepath.Join(grant.Root, filepath.FromSlash(tombstoneDirectory)), 0o700); err != nil {
		return
	}
	file, err := os.OpenFile(manager.tombstonePath(grant), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err == nil {
		_ = file.Close()
	}
}

func (manager *Manager) checkGrant(grant protocol.WorkspaceLeaseGrant, runID string) string {
	if err := grant.Validate(); err != nil {
		return "the workspace lease grant is malformed"
	}
	if !slices.Contains(manager.roots, grant.Root) {
		return "the workspace lease root is not an authorized root on this node"
	}
	if filepath.Clean(grant.SourcePath) != grant.SourcePath || !within(grant.Root, grant.SourcePath) {
		return "the workspace lease source is not beneath its authorized root"
	}
	if grant.Policy == protocol.WorkspaceIsolationExclusiveExisting {
		return ""
	}
	taskID, runIdentifier, found := strings.Cut(strings.TrimPrefix(grant.Branch, protocol.WorkspaceLeaseBranchPrefix), "/")
	if grant.SourceLeaseID != "" {
		if !found || !protocol.IsWorkspaceLeaseIdentity(taskID) || !protocol.IsWorkspaceLeaseIdentity(runIdentifier) {
			return "workspace lease branch does not match its source task and run"
		}
		if runID != "" && !protocol.IsWorkspaceLeaseIdentity(runID) {
			return "workspace lease run identifier is malformed"
		}
		if grant.WorktreePath != filepath.Join(grant.Root, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), grant.SourceLeaseID) {
			return "the shared workspace lease worktree path does not match its source lease"
		}
		return ""
	}
	if grant.Status == "retained" {
		if !found || !protocol.IsWorkspaceLeaseIdentity(taskID) || !protocol.IsWorkspaceLeaseIdentity(runIdentifier) || !protocol.IsWorkspaceLeaseIdentity(runID) {
			return "workspace lease branch does not match its task and run"
		}
	} else if !found || !protocol.IsWorkspaceLeaseIdentity(taskID) || runIdentifier != runID || !protocol.IsWorkspaceLeaseIdentity(runID) {
		return "workspace lease branch does not match its task and run"
	}
	if grant.WorktreePath != filepath.Join(grant.Root, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), grant.ID) {
		return "the workspace lease worktree path is not its managed path"
	}
	return ""
}

func (manager *Manager) verifyRepository(operationContext context.Context, grant protocol.WorkspaceLeaseGrant) string {
	if err := canonicalDirectory(grant.SourcePath); err != nil {
		return "the source checkout is missing, not a directory, or reached through a symbolic link"
	}
	commonDirectory := filepath.Join(grant.SourcePath, ".git")
	if information, err := os.Lstat(commonDirectory); err != nil || !information.IsDir() {
		return "the source checkout is not the main worktree of a repository"
	}
	output, err := manager.git.Run(operationContext, grant.SourcePath, "rev-parse", "--path-format=absolute", "--show-toplevel", "--git-common-dir", "--is-bare-repository")
	if err != nil {
		return "the source checkout is not a readable Git repository"
	}
	lines := strings.Split(strings.TrimSuffix(string(output), "\n"), "\n")
	if len(lines) != 3 || lines[0] != grant.SourcePath || !sameCanonicalPath(lines[1], commonDirectory) || lines[2] != "false" {
		return "the source checkout is not the top level of its own repository"
	}
	if grant.Repository == "" {
		return ""
	}
	output, err = manager.git.Run(operationContext, grant.SourcePath, "config", "--null", "--get-regexp", `^remote\..*\.url$`)
	if err != nil {
		return "the source checkout has no remote with the lease's repository identity"
	}
	for entry := range strings.SplitSeq(strings.TrimSuffix(string(output), "\x00"), "\x00") {
		_, remoteURL, found := strings.Cut(entry, "\n")
		if identity, ok := protocol.NormalizeRepositoryIdentity(remoteURL); found && ok && protocol.RepositoriesMatch(identity, grant.Repository) {
			return ""
		}
	}
	return "the source checkout has no remote with the lease's repository identity"
}

func (manager *Manager) resolveBase(operationContext context.Context, grant protocol.WorkspaceLeaseGrant) (string, string) {
	if grant.BaseRevision != "HEAD" {
		if _, err := manager.git.Run(operationContext, grant.SourcePath, "check-ref-format", grant.BaseRevision); err != nil {
			return "", "the base ref is not a valid ref name"
		}
	}
	if _, err := manager.git.Run(operationContext, grant.SourcePath, "check-ref-format", "refs/heads/"+grant.Branch); err != nil {
		return "", "the lease branch is not a valid ref name"
	}
	output, err := manager.git.Run(operationContext, grant.SourcePath, "rev-parse", "--verify", "--quiet", grant.BaseRevision+"^{commit}")
	if exitedWith(err, 1) {
		return "", "the base ref does not exist"
	}
	if err != nil {
		return "", "the base ref could not be resolved"
	}
	base := strings.TrimSuffix(string(output), "\n")
	if !protocol.IsResolvedRevision(base) {
		return "", "the base ref did not resolve to one commit"
	}
	return base, ""
}

type localState struct {
	target          *Worktree
	branchCheckouts int
	branchHead      string
	pathExists      bool
	recordedLease   string
	recordedBase    string
}

func (manager *Manager) inspect(operationContext context.Context, grant protocol.WorkspaceLeaseGrant) (localState, error) {
	var state localState
	output, err := manager.git.Run(operationContext, grant.SourcePath, "worktree", "list", "--porcelain", "-z")
	if err != nil {
		return state, err
	}
	worktrees, err := ParseWorktreeList(output)
	if err != nil {
		return state, err
	}
	branchRef := "refs/heads/" + grant.Branch
	for index := range worktrees {
		if worktrees[index].Path == grant.WorktreePath {
			if state.target != nil {
				return state, errMalformedInventory
			}
			state.target = &worktrees[index]
		}
		if worktrees[index].Branch == branchRef {
			state.branchCheckouts++
		}
	}
	output, err = manager.git.Run(operationContext, grant.SourcePath, "rev-parse", "--verify", "--quiet", branchRef)
	switch {
	case err == nil:
		state.branchHead = strings.TrimSuffix(string(output), "\n")
		if !protocol.IsResolvedRevision(state.branchHead) {
			return state, errMalformedInventory
		}
	case !exitedWith(err, 1):
		return state, err
	}
	if state.recordedLease, err = manager.configValue(operationContext, grant, leaseConfigName); err != nil {
		return state, err
	}
	if state.recordedBase, err = manager.configValue(operationContext, grant, baseConfigName); err != nil {
		return state, err
	}
	_, err = os.Lstat(grant.WorktreePath)
	switch {
	case err == nil:
		state.pathExists = true
	case !errors.Is(err, fs.ErrNotExist):
		return state, err
	}
	return state, nil
}

func (manager *Manager) configKey(grant protocol.WorkspaceLeaseGrant, name string) string {
	return "branch." + grant.Branch + "." + name
}

func (manager *Manager) configValue(operationContext context.Context, grant protocol.WorkspaceLeaseGrant, name string) (string, error) {
	output, err := manager.git.Run(operationContext, grant.SourcePath, "config", "--local", "--get", manager.configKey(grant, name))
	if exitedWith(err, 1) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(output), "\n"), nil
}

func (manager *Manager) recordIdentity(operationContext context.Context, grant protocol.WorkspaceLeaseGrant, base string) error {
	if _, err := manager.git.Run(operationContext, grant.SourcePath, "config", "--local", manager.configKey(grant, leaseConfigName), grant.ID); err != nil {
		return err
	}
	_, err := manager.git.Run(operationContext, grant.SourcePath, "config", "--local", manager.configKey(grant, baseConfigName), base)
	return err
}

func (manager *Manager) forgetIdentity(operationContext context.Context, grant protocol.WorkspaceLeaseGrant) {
	for _, name := range []string{leaseConfigName, baseConfigName} {
		_, _ = manager.git.Run(operationContext, grant.SourcePath, "config", "--local", "--unset-all", manager.configKey(grant, name))
	}
}

func canonicalDirectory(targetPath string) error {
	resolved, err := filepath.EvalSymlinks(targetPath)
	if err != nil {
		return err
	}
	if resolved != targetPath {
		return errors.New("path is reached through a symbolic link")
	}
	information, err := os.Lstat(targetPath)
	if err != nil {
		return err
	}
	if !information.IsDir() {
		return errors.New("path is not a directory")
	}
	return nil
}

func sameCanonicalPath(reported, expected string) bool {
	resolved, err := filepath.EvalSymlinks(reported)
	return err == nil && resolved == expected
}

func within(root, targetPath string) bool {
	relative, err := filepath.Rel(root, targetPath)
	if err != nil || filepath.IsAbs(relative) {
		return false
	}
	return relative == "." || (relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}

func createLeaf(root, targetPath string) error {
	if _, err := safepath.VerifyDirectoryWithinRoot(root, filepath.Dir(targetPath)); err != nil {
		return err
	}
	if err := os.Mkdir(targetPath, 0o700); err != nil {
		return err
	}
	information, err := os.Lstat(targetPath)
	if err != nil {
		return err
	}
	return safepath.RejectUnsafeAncestor(targetPath, information)
}

func removeEmptyLeaf(root, targetPath string) bool {
	if _, err := safepath.VerifyDirectoryWithinRoot(root, targetPath); err != nil {
		return false
	}
	return os.Remove(targetPath) == nil
}

const maximumGitFileBytes = 4096

func linkedToRepository(grant protocol.WorkspaceLeaseGrant) bool {
	gitFile := filepath.Join(grant.WorktreePath, ".git")
	information, err := os.Lstat(gitFile)
	if err != nil || !information.Mode().IsRegular() || information.Size() > maximumGitFileBytes {
		return false
	}
	content, err := os.ReadFile(gitFile)
	if err != nil {
		return false
	}
	reported, found := strings.CutPrefix(strings.TrimSuffix(string(content), "\n"), "gitdir: ")
	if !found || !filepath.IsAbs(reported) {
		return false
	}
	administrative, err := filepath.EvalSymlinks(reported)
	administrativeRoot := filepath.Join(grant.SourcePath, ".git", "worktrees")
	if err != nil || filepath.Dir(administrative) != administrativeRoot {
		return false
	}
	backLink, err := os.ReadFile(filepath.Join(administrative, "gitdir"))
	if err != nil || len(backLink) > maximumGitFileBytes {
		return false
	}
	return strings.TrimSuffix(string(backLink), "\n") == gitFile
}

func (manager *Manager) provisionSharedReadOnly(operationContext context.Context, grant protocol.WorkspaceLeaseGrant) Outcome {
	if !grant.ReadOnly || grant.SourceLeaseID == "" {
		return failed("shared workspace lease must be marked readOnly with a sourceLeaseId")
	}
	if _, err := safepath.VerifyDirectoryWithinRoot(grant.Root, grant.SourcePath); err != nil {
		return failed("the source checkout is missing or reached through a symbolic link")
	}
	if err := canonicalDirectory(grant.SourcePath); err != nil {
		return failed("the source checkout is missing, not a directory, or reached through a symbolic link")
	}
	if _, err := safepath.VerifyDirectoryWithinRoot(grant.Root, grant.WorktreePath); err != nil {
		return failed("the source lease worktree is missing or reached through a symbolic link")
	}
	if err := canonicalDirectory(grant.WorktreePath); err != nil {
		return failed("the source lease worktree is missing, not a directory, or reached through a symbolic link")
	}
	if grant.Policy == protocol.WorkspaceIsolationExclusiveExisting {
		return Outcome{Status: "active", Path: grant.WorktreePath}
	}
	if manager.git == nil {
		return failed("git is not available on this node")
	}

	repositoryMutex := manager.repositoryLock(grant.SourcePath)
	repositoryMutex.Lock()
	defer repositoryMutex.Unlock()

	sourceView := grant
	sourceView.ID = grant.SourceLeaseID
	sourceView.SourceLeaseID = ""
	sourceView.ReadOnly = false
	if problem := manager.verifyRepository(operationContext, sourceView); problem != "" {
		return failed(problem)
	}
	state, err := manager.inspect(operationContext, sourceView)
	if err != nil {
		return failed("local worktree state could not be read")
	}
	if state.target == nil {
		return failed("the source lease worktree does not exist on this node")
	}
	return manager.adopt(operationContext, sourceView, state)
}

func (manager *Manager) ResolveSourceLease(
	operationContext context.Context,
	sourceGrant protocol.WorkspaceLeaseGrant,
	requirements protocol.WorkspaceRequirements,
) (Outcome, error) {
	if err := protocol.ValidateWorkspaceRequirements(&requirements); err != nil {
		return failed(err.Error()), err
	}
	requestedSourceID := strings.TrimSpace(requirements.SourceLeaseID)
	if requestedSourceID == "" {
		err := errors.New("workspace sourceLeaseId is required")
		return failed(err.Error()), err
	}
	if err := sourceGrant.Validate(); err != nil {
		return failed("source workspace lease grant is malformed"), err
	}
	if sourceGrant.ReadOnly || sourceGrant.SourceLeaseID != "" {
		err := errors.New("cannot resolve a read-only lease as a writable source lease")
		return failed(err.Error()), err
	}
	if sourceGrant.ID != requestedSourceID {
		err := fmt.Errorf("sourceLeaseId %q does not match grant %q", requestedSourceID, sourceGrant.ID)
		return failed(err.Error()), err
	}
	if !slices.Contains([]string{"active", "released", "retained"}, sourceGrant.Status) {
		err := fmt.Errorf("source workspace lease %s is in non-inspectable status %q", sourceGrant.ID, sourceGrant.Status)
		return failed(err.Error()), err
	}
	if !slices.Contains(manager.roots, sourceGrant.Root) {
		err := errors.New("source workspace lease root is not authorized on this node")
		return failed(err.Error()), err
	}
	if filepath.Clean(sourceGrant.SourcePath) != sourceGrant.SourcePath || !within(sourceGrant.Root, sourceGrant.SourcePath) {
		err := errors.New("source workspace lease checkout is outside its authorized root")
		return failed(err.Error()), err
	}
	if trimmedRepo := strings.TrimSpace(requirements.Repository); trimmedRepo != "" {
		normalizedRepo, ok := protocol.NormalizeRepositoryIdentity(trimmedRepo)
		if !ok {
			err := fmt.Errorf("requested repository %q is invalid", trimmedRepo)
			return failed(err.Error()), err
		}
		if sourceGrant.Repository == "" || !protocol.RepositoriesMatch(sourceGrant.Repository, normalizedRepo) {
			err := fmt.Errorf("requested repository %s does not match source lease repository %s", normalizedRepo, sourceGrant.Repository)
			return failed(err.Error()), err
		}
	}
	if trimmedPath := strings.TrimSpace(requirements.Path); trimmedPath != "" {
		var candidatePath string
		if strings.HasPrefix(trimmedPath, "/") {
			candidatePath = filepath.Clean(trimmedPath)
		} else {
			validatedSubpath, subpathErr := protocol.ValidateRelativeWorkspaceSubpath(trimmedPath)
			if subpathErr != nil {
				return failed(subpathErr.Error()), subpathErr
			}
			candidatePath = filepath.Join(sourceGrant.Root, filepath.FromSlash(validatedSubpath))
		}
		if candidatePath != sourceGrant.SourcePath && candidatePath != sourceGrant.WorktreePath {
			err := fmt.Errorf("requested workspace path %s does not match source lease %s", trimmedPath, sourceGrant.ID)
			return failed(err.Error()), err
		}
	}
	if trimmedBranch := strings.TrimSpace(requirements.BaseBranch); trimmedBranch != "" && sourceGrant.BaseRevision != "" {
		if sourceGrant.BaseRevision != protocol.WorkspaceLeaseBaseRef(trimmedBranch) {
			err := fmt.Errorf("requested baseBranch %s does not match source lease base %s", trimmedBranch, sourceGrant.BaseRevision)
			return failed(err.Error()), err
		}
	}

	sharedGrant := sourceGrant
	sharedGrant.ID = "shared-" + sourceGrant.ID
	if !protocol.IsWorkspaceLeaseIdentity(sharedGrant.ID) {
		sharedGrant.ID = "shared_inspector"
	}
	sharedGrant.SourceLeaseID = sourceGrant.ID
	sharedGrant.ReadOnly = true
	sharedGrant.Status = "requested"
	sharedGrant.Cleanup = protocol.WorkspaceCleanupWhenUnchanged

	outcome := manager.provisionSharedReadOnly(operationContext, sharedGrant)
	if outcome.Status != "active" {
		detail := outcome.Detail
		if detail == "" {
			detail = "source workspace lease could not be verified"
		}
		return outcome, errors.New(detail)
	}
	return outcome, nil
}

func (manager *Manager) LandLease(
	operationContext context.Context,
	request protocol.WorkspaceLeaseLandRequest,
) (protocol.WorkspaceLeaseLandResult, error) {
	mode := request.EffectiveMode()
	if !slices.Contains(protocol.WorkspaceLandingModes, mode) {
		mode = protocol.WorkspaceLandingModeFastForward
	}
	grant := request.Lease
	failWithError := func(retainedWorktree bool, err error) (protocol.WorkspaceLeaseLandResult, error) {
		sanitized := protocol.BoundedDiagnostic(protocol.RedactText(err.Error()))
		return protocol.WorkspaceLeaseLandResult{
			RequestID:        request.RequestID,
			LeaseID:          grant.ID,
			Status:           protocol.WorkspaceLandingStatusFailed,
			Mode:             mode,
			Landed:           false,
			WorktreeRetained: retainedWorktree,
			Error:            sanitized,
			Detail:           sanitized,
		}, err
	}

	if err := request.Validate(); err != nil {
		return failWithError(true, err)
	}
	if !slices.Contains(manager.roots, grant.Root) {
		return failWithError(true, errors.New("the workspace lease root is not an authorized root on this node"))
	}
	if filepath.Clean(grant.SourcePath) != grant.SourcePath || !within(grant.Root, grant.SourcePath) {
		return failWithError(true, errors.New("the workspace lease source is not beneath its authorized root"))
	}
	expectedWorktreePath := filepath.Join(grant.Root, filepath.FromSlash(protocol.WorkspaceLeaseManagedDirectory), grant.ID)
	if grant.WorktreePath != expectedWorktreePath {
		return failWithError(true, errors.New("the workspace lease worktree path is not its managed path"))
	}
	if _, err := safepath.VerifyDirectoryWithinRoot(grant.Root, grant.SourcePath); err != nil {
		return failWithError(true, fmt.Errorf("verify source checkout confinement: %w", err))
	}
	if err := canonicalDirectory(grant.SourcePath); err != nil {
		return failWithError(true, errors.New("the source checkout is missing, not a directory, or reached through a symbolic link"))
	}
	if manager.git == nil {
		return failWithError(true, errors.New("git is not available on this node"))
	}

	repositoryMutex := manager.repositoryLock(grant.SourcePath)
	repositoryMutex.Lock()
	defer repositoryMutex.Unlock()

	if problem := manager.verifyRepository(operationContext, grant); problem != "" {
		return failWithError(true, errors.New(problem))
	}
	state, err := manager.inspect(operationContext, grant)
	if err != nil {
		return failWithError(true, errors.New("local worktree state could not be read"))
	}
	hasWorktree := state.target != nil
	if state.recordedLease != grant.ID || !protocol.IsResolvedRevision(state.recordedBase) {
		return failWithError(hasWorktree, errors.New("the lease branch is not recorded as owned by this workspace lease"))
	}
	if grant.ResolvedBaseRevision != "" && grant.ResolvedBaseRevision != state.recordedBase {
		return failWithError(hasWorktree, errors.New("the recorded base revision differs from the workspace lease"))
	}
	if state.branchHead == "" {
		return failWithError(hasWorktree, errors.New("the workspace lease branch does not exist"))
	}
	if state.target == nil && state.pathExists {
		return failWithError(true, errors.New("the workspace lease path exists without a registered git worktree"))
	}
	if state.target != nil {
		if reason, detail := manager.ownership(operationContext, grant, state); reason != "" {
			return failWithError(true, fmt.Errorf("%s (%s)", detail, reason))
		}
	}
	if state.branchHead != state.recordedBase {
		if _, ancestorErr := manager.git.Run(operationContext, grant.SourcePath, "merge-base", "--is-ancestor", state.recordedBase, state.branchHead); ancestorErr != nil {
			return failWithError(hasWorktree, errors.New("the workspace lease branch does not descend from its recorded base revision"))
		}
	}

	targetBranch, err := request.NormalizedTargetBranch()
	if err != nil {
		return failWithError(hasWorktree, err)
	}
	if targetBranch == "" {
		targetBranch = readHeadBranch(operationContext, manager.git, grant.SourcePath)
		if !protocol.IsWorkspaceLeaseBaseBranch(targetBranch) {
			return failWithError(hasWorktree, errors.New("targetBranch is required when lease baseRevision is HEAD and source checkout is detached"))
		}
	}
	if targetBranch == grant.Branch {
		return failWithError(hasWorktree, errors.New("targetBranch must not be the workspace lease branch"))
	}
	targetRef := "refs/heads/" + targetBranch
	if _, err := manager.git.Run(operationContext, grant.SourcePath, "check-ref-format", targetRef); err != nil {
		return failWithError(hasWorktree, fmt.Errorf("target branch %q is not a valid git ref", targetBranch))
	}
	targetOutput, err := manager.git.Run(operationContext, grant.SourcePath, "rev-parse", "--verify", "--quiet", targetRef+"^{commit}")
	if exitedWith(err, 1) {
		return failWithError(hasWorktree, fmt.Errorf("target branch %q does not exist", targetBranch))
	}
	if err != nil {
		return failWithError(hasWorktree, fmt.Errorf("resolve target branch %q: %w", targetBranch, err))
	}
	targetBefore := strings.TrimSuffix(string(targetOutput), "\n")
	if !protocol.IsResolvedRevision(targetBefore) {
		return failWithError(hasWorktree, fmt.Errorf("target branch %q did not resolve to a commit", targetBranch))
	}

	leaseHead := state.branchHead
	baseResult := protocol.WorkspaceLeaseLandResult{
		RequestID:              request.RequestID,
		LeaseID:                grant.ID,
		Mode:                   mode,
		WorktreeRetained:       hasWorktree,
		TargetBranch:           targetBranch,
		BaseRevision:           state.recordedBase,
		SourceRevision:         leaseHead,
		PreviousTargetRevision: targetBefore,
	}

	if state.target != nil {
		worktreeStatusOutput, statusErr := manager.git.Run(
			operationContext,
			grant.WorktreePath,
			"status",
			"--porcelain=v1",
			"-z",
			"--untracked-files=all",
			"--ignore-submodules=none",
		)
		if statusErr != nil {
			return failWithError(true, errors.New("the lease worktree status could not be read"))
		}
		worktreeDirtyFiles, parseErr := parseDirtyPaths(worktreeStatusOutput, false)
		if parseErr != nil {
			return failWithError(true, errors.New("the lease worktree status could not be parsed"))
		}
		if len(worktreeDirtyFiles) > 0 {
			dirtyResult := baseResult
			dirtyResult.Status = protocol.WorkspaceLandingStatusDirty
			dirtyResult.Reason = "worktree_dirty"
			dirtyResult.DirtyFiles = worktreeDirtyFiles
			dirtyResult.Detail = "workspace lease worktree has uncommitted or untracked changes"
			return dirtyResult, nil
		}
	}

	worktreeListOutput, listErr := manager.git.Run(operationContext, grant.SourcePath, "worktree", "list", "--porcelain", "-z")
	if listErr != nil {
		return failWithError(hasWorktree, errors.New("repository worktree list could not be read"))
	}
	worktrees, parseListErr := ParseWorktreeList(worktreeListOutput)
	if parseListErr != nil {
		return failWithError(hasWorktree, errors.New("repository worktree list could not be parsed"))
	}
	targetCheckedOutInSource := false
	for _, entry := range worktrees {
		if entry.Branch != targetRef {
			continue
		}
		if entry.Path == grant.SourcePath {
			targetCheckedOutInSource = true
			continue
		}
		conflictResult := baseResult
		conflictResult.Status = protocol.WorkspaceLandingStatusConflict
		conflictResult.Reason = "target_checked_out_elsewhere"
		conflictResult.Detail = fmt.Sprintf("target branch %s is checked out in another worktree", targetBranch)
		return conflictResult, nil
	}

	if hasInProgressGitOperation(filepath.Join(grant.SourcePath, ".git")) {
		dirtyResult := baseResult
		dirtyResult.Status = protocol.WorkspaceLandingStatusDirty
		dirtyResult.Reason = "source_operation_in_progress"
		dirtyResult.Detail = "source checkout has an unfinished git merge, rebase, or cherry-pick"
		return dirtyResult, nil
	}

	sourceStatusOutput, sourceStatusErr := manager.git.Run(
		operationContext,
		grant.SourcePath,
		"status",
		"--porcelain=v1",
		"-z",
		"--untracked-files=all",
		"--ignore-submodules=none",
	)
	if sourceStatusErr != nil {
		return failWithError(hasWorktree, errors.New("source checkout status could not be read"))
	}
	sourceDirtyFiles, parseSourceErr := parseDirtyPaths(sourceStatusOutput, grant.SourcePath == grant.Root)
	if parseSourceErr != nil {
		return failWithError(hasWorktree, errors.New("source checkout status could not be parsed"))
	}
	if len(sourceDirtyFiles) > 0 {
		dirtyResult := baseResult
		dirtyResult.Status = protocol.WorkspaceLandingStatusDirty
		dirtyResult.Reason = "source_dirty"
		dirtyResult.DirtyFiles = sourceDirtyFiles
		dirtyResult.Detail = "source checkout has uncommitted or untracked changes"
		return dirtyResult, nil
	}

	alreadyLanded := false
	commitsLanded := 0

	if _, err := manager.git.Run(operationContext, grant.SourcePath, "merge-base", "--is-ancestor", leaseHead, targetBefore); err == nil {
		alreadyLanded = true
	} else if !exitedWith(err, 1) {
		return failWithError(hasWorktree, errors.New("could not check ancestry between lease branch and target branch"))
	}

	if !alreadyLanded {
		switch mode {
		case protocol.WorkspaceLandingModeFastForward, protocol.WorkspaceLandingModeMergeFFOnly:
			if _, err := manager.git.Run(operationContext, grant.SourcePath, "merge-base", "--is-ancestor", targetBefore, leaseHead); err != nil {
				if exitedWith(err, 1) {
					conflictResult := baseResult
					conflictResult.Status = protocol.WorkspaceLandingStatusConflict
					conflictResult.Reason = "non_fast_forward"
					conflictResult.Detail = fmt.Sprintf("target branch %s has diverged from lease base; fast-forward is not possible", targetBranch)
					return conflictResult, nil
				}
				return failWithError(hasWorktree, errors.New("could not check fast-forward eligibility"))
			}

			commitCount, countErr := manager.countCommitsBetween(operationContext, grant.SourcePath, targetBefore, leaseHead)
			if countErr != nil {
				return failWithError(hasWorktree, countErr)
			}
			commitsLanded = commitCount

			if targetCheckedOutInSource {
				if _, mergeErr := manager.git.Run(operationContext, grant.SourcePath, "merge", "--ff-only", "--quiet", leaseHead); mergeErr != nil {
					conflictResult := baseResult
					conflictResult.Status = protocol.WorkspaceLandingStatusConflict
					conflictResult.Reason = "fast_forward_failed"
					conflictResult.Detail = formatGitOperationDiagnostic("git merge --ff-only failed", mergeErr)
					return conflictResult, nil
				}
			} else {
				if _, updateErr := manager.git.Run(operationContext, grant.SourcePath, "update-ref", targetRef, leaseHead, targetBefore); updateErr != nil {
					return failWithError(hasWorktree, fmt.Errorf("advance target ref %s: %w", targetBranch, updateErr))
				}
			}

		case protocol.WorkspaceLandingModeCherryPick:
			cherryOutcome, cherryErr := manager.cherryPickOntoTarget(
				operationContext,
				grant,
				targetBranch,
				targetRef,
				targetBefore,
				leaseHead,
				targetCheckedOutInSource,
				baseResult,
			)
			if cherryErr != nil {
				return failWithError(hasWorktree, cherryErr)
			}
			if !cherryOutcome.Landed {
				return cherryOutcome, nil
			}
			alreadyLanded = cherryOutcome.AlreadyLanded
			commitsLanded = cherryOutcome.CommitsLanded
		}
	}

	verifiedTargetOutput, verifyTargetErr := manager.git.Run(operationContext, grant.SourcePath, "rev-parse", "--verify", "--quiet", targetRef+"^{commit}")
	if verifyTargetErr != nil {
		return failWithError(hasWorktree, fmt.Errorf("verify landed revision on %s: %w", targetBranch, verifyTargetErr))
	}
	landedRevision := strings.TrimSuffix(string(verifiedTargetOutput), "\n")
	if !protocol.IsResolvedRevision(landedRevision) {
		return failWithError(hasWorktree, errors.New("verified target branch revision is malformed"))
	}
	if !manager.verifyCommitsIntegrated(operationContext, grant.SourcePath, targetRef, landedRevision, leaseHead, mode) {
		return failWithError(hasWorktree, errors.New("landed revision could not be verified as containing lease commits"))
	}

	result := baseResult
	result.Status = protocol.WorkspaceLandingStatusLanded
	result.Landed = true
	result.AlreadyLanded = alreadyLanded
	result.LandedRevision = landedRevision
	result.CommitsLanded = commitsLanded
	result.Detail = fmt.Sprintf("landed %d commit(s) onto %s", commitsLanded, targetBranch)
	if alreadyLanded {
		result.Detail = "lease commits are already present on " + targetBranch
	}

	if request.CleanupAfterLand {
		cleaned, cleanupDetail := manager.cleanupLandedWorktree(operationContext, grant, leaseHead)
		if cleaned {
			result.Cleaned = true
			result.WorktreeRetained = false
			result.LeaseUpdate = &protocol.WorkspaceLeaseUpdate{
				LeaseID:              grant.ID,
				Status:               "cleaned",
				ResolvedBaseRevision: state.recordedBase,
				Detail:               result.Detail,
			}
		} else if cleanupDetail != "" {
			result.Detail = protocol.BoundedDiagnostic(result.Detail + "; " + cleanupDetail)
		}
	}

	return result, nil
}

func (manager *Manager) cherryPickOntoTarget(
	operationContext context.Context,
	grant protocol.WorkspaceLeaseGrant,
	targetBranch string,
	targetRef string,
	targetBefore string,
	leaseHead string,
	targetCheckedOutInSource bool,
	baseResult protocol.WorkspaceLeaseLandResult,
) (protocol.WorkspaceLeaseLandResult, error) {
	mergeCommitsOutput, mergeCommitsErr := manager.git.Run(
		operationContext,
		grant.SourcePath,
		"rev-list",
		"--min-parents=2",
		targetBefore+".."+leaseHead,
	)
	if mergeCommitsErr != nil {
		return protocol.WorkspaceLeaseLandResult{}, errors.New("could not inspect lease commits for merge commits")
	}
	if strings.TrimSpace(string(mergeCommitsOutput)) != "" {
		conflictResult := baseResult
		conflictResult.Status = protocol.WorkspaceLandingStatusConflict
		conflictResult.Reason = "merge_commits_unsupported"
		conflictResult.Detail = "cherry-pick landing cannot replay merge commits"
		return conflictResult, nil
	}

	commitsToApply, err := manager.unappliedCherryCommits(operationContext, grant.SourcePath, targetBefore, leaseHead)
	if err != nil {
		return protocol.WorkspaceLeaseLandResult{}, err
	}
	if len(commitsToApply) == 0 {
		landed := baseResult
		landed.Status = protocol.WorkspaceLandingStatusLanded
		landed.Landed = true
		landed.AlreadyLanded = true
		landed.LandedRevision = targetBefore
		landed.CommitsLanded = 0
		return landed, nil
	}

	committerArguments := manager.git.CommitterArguments(operationContext, grant.SourcePath)

	if targetCheckedOutInSource {
		cherryArguments := make([]string, 0, len(committerArguments)+1+len(commitsToApply))
		cherryArguments = append(cherryArguments, committerArguments...)
		cherryArguments = append(cherryArguments, "cherry-pick")
		cherryArguments = append(cherryArguments, commitsToApply...)

		if _, pickErr := manager.git.Run(operationContext, grant.SourcePath, cherryArguments...); pickErr != nil {
			conflicts := manager.collectUnmergedPaths(operationContext, grant.SourcePath)
			_, _ = manager.git.Run(operationContext, grant.SourcePath, "cherry-pick", "--abort")
			if hasInProgressGitOperation(filepath.Join(grant.SourcePath, ".git")) {
				_, _ = manager.git.Run(operationContext, grant.SourcePath, "reset", "--hard", targetBefore)
			}
			conflictResult := baseResult
			conflictResult.Status = protocol.WorkspaceLandingStatusConflict
			conflictResult.Reason = "merge_conflict"
			conflictResult.Conflicts = conflicts
			conflictResult.Detail = formatCherryPickConflictDetail(targetBranch, conflicts, pickErr)
			return conflictResult, nil
		}

		headOutput, headErr := manager.git.Run(operationContext, grant.SourcePath, "rev-parse", "--verify", "--quiet", "HEAD^{commit}")
		if headErr != nil {
			return protocol.WorkspaceLeaseLandResult{}, fmt.Errorf("read landed HEAD commit: %w", headErr)
		}
		landed := baseResult
		landed.Status = protocol.WorkspaceLandingStatusLanded
		landed.Landed = true
		landed.LandedRevision = strings.TrimSuffix(string(headOutput), "\n")
		landed.CommitsLanded = len(commitsToApply)
		return landed, nil
	}

	landingRoot, err := safepath.EnsureDirectoryWithinRoot(
		grant.Root,
		filepath.Join(grant.Root, filepath.FromSlash(landingWorktreeDirectory)),
		0o700,
	)
	if err != nil {
		return protocol.WorkspaceLeaseLandResult{}, fmt.Errorf("prepare temporary landing directory: %w", err)
	}
	temporaryWorktreePath := filepath.Join(landingRoot, grant.ID)
	if _, statErr := os.Lstat(temporaryWorktreePath); statErr == nil {
		_, _ = manager.git.Run(operationContext, grant.SourcePath, "worktree", "remove", "--force", "--", temporaryWorktreePath)
		_ = removeEmptyLeaf(grant.Root, temporaryWorktreePath)
	}
	if err := createLeaf(grant.Root, temporaryWorktreePath); err != nil {
		return protocol.WorkspaceLeaseLandResult{}, fmt.Errorf("create temporary landing worktree leaf: %w", err)
	}
	defer func() {
		_, _ = manager.git.Run(context.Background(), grant.SourcePath, "worktree", "remove", "--force", "--", temporaryWorktreePath)
		_ = removeEmptyLeaf(grant.Root, temporaryWorktreePath)
	}()

	if _, addErr := manager.git.Run(operationContext, grant.SourcePath, "worktree", "add", "--detach", "--quiet", "--", temporaryWorktreePath, targetBefore); addErr != nil {
		return protocol.WorkspaceLeaseLandResult{}, fmt.Errorf("create temporary landing worktree: %w", addErr)
	}
	if _, verifyErr := safepath.VerifyDirectoryWithinRoot(grant.Root, temporaryWorktreePath); verifyErr != nil {
		return protocol.WorkspaceLeaseLandResult{}, fmt.Errorf("verify temporary landing worktree confinement: %w", verifyErr)
	}

	cherryArguments := make([]string, 0, len(committerArguments)+1+len(commitsToApply))
	cherryArguments = append(cherryArguments, committerArguments...)
	cherryArguments = append(cherryArguments, "cherry-pick")
	cherryArguments = append(cherryArguments, commitsToApply...)

	if _, pickErr := manager.git.Run(operationContext, temporaryWorktreePath, cherryArguments...); pickErr != nil {
		conflicts := manager.collectUnmergedPaths(operationContext, temporaryWorktreePath)
		_, _ = manager.git.Run(operationContext, temporaryWorktreePath, "cherry-pick", "--abort")
		conflictResult := baseResult
		conflictResult.Status = protocol.WorkspaceLandingStatusConflict
		conflictResult.Reason = "merge_conflict"
		conflictResult.Conflicts = conflicts
		conflictResult.Detail = formatCherryPickConflictDetail(targetBranch, conflicts, pickErr)
		return conflictResult, nil
	}

	newHeadOutput, headErr := manager.git.Run(operationContext, temporaryWorktreePath, "rev-parse", "--verify", "--quiet", "HEAD^{commit}")
	if headErr != nil {
		return protocol.WorkspaceLeaseLandResult{}, fmt.Errorf("resolve cherry-picked commit: %w", headErr)
	}
	newHead := strings.TrimSuffix(string(newHeadOutput), "\n")
	if _, updateErr := manager.git.Run(operationContext, grant.SourcePath, "update-ref", targetRef, newHead, targetBefore); updateErr != nil {
		return protocol.WorkspaceLeaseLandResult{}, fmt.Errorf("advance target ref %s: %w", targetBranch, updateErr)
	}

	landed := baseResult
	landed.Status = protocol.WorkspaceLandingStatusLanded
	landed.Landed = true
	landed.LandedRevision = newHead
	landed.CommitsLanded = len(commitsToApply)
	return landed, nil
}

func (manager *Manager) unappliedCherryCommits(
	operationContext context.Context,
	sourcePath string,
	upstreamRevision string,
	leaseHead string,
) ([]string, error) {
	output, err := manager.git.Run(operationContext, sourcePath, "cherry", upstreamRevision, leaseHead)
	if err != nil {
		return nil, fmt.Errorf("compute unapplied commits with git cherry: %w", err)
	}
	var commits []string
	for line := range strings.SplitSeq(strings.TrimSuffix(string(output), "\n"), "\n") {
		if line == "" {
			continue
		}
		if after, ok := strings.CutPrefix(line, "+ "); ok {
			revision := strings.TrimSpace(after)
			if !protocol.IsResolvedRevision(revision) {
				return nil, errors.New("git cherry returned a malformed commit revision")
			}
			commits = append(commits, revision)
		}
	}
	return commits, nil
}

func (manager *Manager) countCommitsBetween(
	operationContext context.Context,
	sourcePath string,
	fromRevision string,
	toRevision string,
) (int, error) {
	output, err := manager.git.Run(operationContext, sourcePath, "rev-list", "--count", fromRevision+".."+toRevision)
	if err != nil {
		return 0, fmt.Errorf("count commits between %s and %s: %w", fromRevision, toRevision, err)
	}
	count, parseErr := strconv.Atoi(strings.TrimSpace(string(output)))
	if parseErr != nil || count < 0 {
		return 0, errors.New("git rev-list --count returned an invalid integer")
	}
	return count, nil
}

func (manager *Manager) verifyCommitsIntegrated(
	operationContext context.Context,
	sourcePath string,
	targetRef string,
	landedRevision string,
	leaseHead string,
	mode string,
) bool {
	if _, err := manager.git.Run(operationContext, sourcePath, "merge-base", "--is-ancestor", leaseHead, landedRevision); err == nil {
		return true
	}
	if mode != protocol.WorkspaceLandingModeCherryPick {
		return false
	}
	unapplied, err := manager.unappliedCherryCommits(operationContext, sourcePath, targetRef, leaseHead)
	return err == nil && len(unapplied) == 0
}

func (manager *Manager) cleanupLandedWorktree(
	operationContext context.Context,
	grant protocol.WorkspaceLeaseGrant,
	leaseHead string,
) (bool, string) {
	if !manager.beginSourceMutation(grant.ID) {
		return false, "retained worktree because read-only inspectors are active"
	}
	defer manager.endSourceMutation(grant.ID)

	state, err := manager.inspect(operationContext, grant)
	if err != nil {
		return false, "retained worktree because local worktree state could not be re-read"
	}
	if state.target != nil {
		if _, removeErr := manager.git.Run(operationContext, grant.SourcePath, "worktree", "remove", "--", grant.WorktreePath); removeErr != nil {
			return false, "retained worktree because git refused to remove the worktree"
		}
	}
	if state.branchHead != "" {
		if _, deleteRefErr := manager.git.Run(operationContext, grant.SourcePath, "update-ref", "-d", "refs/heads/"+grant.Branch, leaseHead); deleteRefErr != nil {
			return false, "retained branch metadata because the lease branch ref could not be removed"
		}
	}
	manager.forgetIdentity(operationContext, grant)
	state, err = manager.inspect(operationContext, grant)
	if err != nil || state.target != nil || state.pathExists || state.branchHead != "" {
		return false, "workspace lease artifacts remain on disk after cleanup attempt"
	}
	manager.writeTombstone(grant)
	return true, ""
}

func (manager *Manager) collectUnmergedPaths(operationContext context.Context, directory string) []string {
	output, err := manager.git.Run(operationContext, directory, "diff", "--name-only", "--diff-filter=U", "-z")
	if err != nil || len(output) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	var paths []string
	for rawEntry := range strings.SplitSeq(strings.TrimSuffix(string(output), "\x00"), "\x00") {
		cleaned := strings.TrimSpace(rawEntry)
		if cleaned == "" {
			continue
		}
		if _, exists := seen[cleaned]; exists {
			continue
		}
		seen[cleaned] = struct{}{}
		paths = append(paths, protocol.TruncateUTF8Head(cleaned, protocol.MaximumWorkspaceSubpathBytes*2))
		if len(paths) >= protocol.MaximumLandingDiagnosticPaths {
			break
		}
	}
	slices.Sort(paths)
	return paths
}

func parseDirtyPaths(output []byte, ignoreManagedOrchestraDirectory bool) ([]string, error) {
	text := string(output)
	if text == "" {
		return nil, nil
	}
	if !strings.HasSuffix(text, "\x00") {
		return nil, errMalformedInventory
	}
	fields := strings.Split(strings.TrimSuffix(text, "\x00"), "\x00")
	seen := map[string]struct{}{}
	var paths []string
	for index := 0; index < len(fields); index++ {
		entry := fields[index]
		if len(entry) < 4 || entry[2] != ' ' {
			return nil, errMalformedInventory
		}
		code := entry[:2]
		if code == "!!" {
			continue
		}
		filePath := entry[3:]
		if strings.ContainsAny(code, "RC") {
			index++
			if index >= len(fields) {
				return nil, errMalformedInventory
			}
		}
		if ignoreManagedOrchestraDirectory && code == "??" && (filePath == ".orchestra" || strings.HasPrefix(filePath, ".orchestra/")) {
			continue
		}
		if filePath == "" {
			continue
		}
		if _, exists := seen[filePath]; !exists {
			seen[filePath] = struct{}{}
			paths = append(paths, protocol.TruncateUTF8Head(filePath, protocol.MaximumWorkspaceSubpathBytes*2))
			if len(paths) >= protocol.MaximumLandingDiagnosticPaths {
				break
			}
		}
	}
	slices.Sort(paths)
	return paths, nil
}

func hasInProgressGitOperation(gitDirectory string) bool {
	for _, marker := range []string{
		"MERGE_HEAD",
		"CHERRY_PICK_HEAD",
		"REBASE_HEAD",
		"REVERT_HEAD",
		"rebase-merge",
		"rebase-apply",
	} {
		if _, err := os.Lstat(filepath.Join(gitDirectory, marker)); err == nil {
			return true
		}
	}
	return false
}

func formatGitOperationDiagnostic(prefix string, err error) string {
	gitError, ok := errors.AsType[*GitError](err)
	if ok && gitError.Stderr != "" {
		return protocol.BoundedDiagnostic(prefix + ": " + protocol.RedactText(gitError.Stderr))
	}
	return protocol.BoundedDiagnostic(prefix + ": " + protocol.RedactText(err.Error()))
}

func formatCherryPickConflictDetail(targetBranch string, conflicts []string, err error) string {
	if len(conflicts) > 0 {
		return protocol.BoundedDiagnostic(
			fmt.Sprintf("cherry-pick onto %s conflicted on %s", targetBranch, strings.Join(conflicts, ", ")),
		)
	}
	return formatGitOperationDiagnostic("cherry-pick onto "+targetBranch+" failed", err)
}
