package workspace

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/safepath"
)

const maximumDiscoveryDepth = 4

var skippedDiscoveryDirectories = []string{
	"node_modules",
	"target",
	"dist",
	"build",
	"__pycache__",
}

type CloneRequest struct {
	RepositoryURL string
	WorkspaceRoot string
	Directory     string
	Branch        string
}

type CloneOutcome struct {
	Repository   protocol.DiscoveredRepository
	Repositories []protocol.DiscoveredRepository
	Created      bool
}

func (manager *Manager) Roots() []string {
	if manager == nil {
		return nil
	}
	return slices.Clone(manager.roots)
}

func (manager *Manager) DiscoverRepositories(operationContext context.Context) ([]protocol.DiscoveredRepository, error) {
	if manager == nil {
		return []protocol.DiscoveredRepository{}, nil
	}
	return DiscoverRepositories(operationContext, manager.roots, manager.git)
}

func DiscoverRepositories(operationContext context.Context, roots []string, git *Git) ([]protocol.DiscoveredRepository, error) {
	if git == nil || len(roots) == 0 {
		return []protocol.DiscoveredRepository{}, nil
	}

	discovered := make([]protocol.DiscoveredRepository, 0)
	seenPaths := map[string]bool{}

	var scanDirectory func(root string, directory string, depth int) error
	scanDirectory = func(root string, directory string, depth int) error {
		if operationContext.Err() != nil {
			return operationContext.Err()
		}
		if len(discovered) >= protocol.MaximumDiscoveredRepositories {
			return nil
		}

		repository, isRepository := inspectRepositoryCheckout(operationContext, git, root, directory)
		if isRepository {
			if !seenPaths[repository.Path] {
				seenPaths[repository.Path] = true
				discovered = append(discovered, repository)
			}
			return nil
		}

		if depth >= maximumDiscoveryDepth {
			return nil
		}

		entries, err := os.ReadDir(directory)
		if err != nil {
			return nil
		}
		for _, entry := range entries {
			if len(discovered) >= protocol.MaximumDiscoveredRepositories {
				return nil
			}
			entryName := entry.Name()
			if strings.HasPrefix(entryName, ".") || slices.Contains(skippedDiscoveryDirectories, entryName) {
				continue
			}
			if entry.Type()&fs.ModeSymlink != 0 || !entry.IsDir() {
				continue
			}
			childDirectory := filepath.Join(directory, entryName)
			information, statErr := os.Lstat(childDirectory)
			if statErr != nil || safepath.RejectUnsafeAncestor(childDirectory, information) != nil {
				continue
			}
			if err := scanDirectory(root, childDirectory, depth+1); err != nil {
				return err
			}
		}
		return nil
	}

	for _, rawRoot := range roots {
		cleanRoot := filepath.Clean(rawRoot)
		if _, err := safepath.VerifyDirectoryWithinRoot(cleanRoot, cleanRoot); err != nil {
			continue
		}
		if err := scanDirectory(cleanRoot, cleanRoot, 0); err != nil {
			return nil, err
		}
	}

	slices.SortFunc(discovered, func(left, right protocol.DiscoveredRepository) int {
		if compared := cmp.Compare(left.Root, right.Root); compared != 0 {
			return compared
		}
		return cmp.Compare(left.RelativePath, right.RelativePath)
	})
	return discovered, nil
}

func (manager *Manager) CloneRepository(operationContext context.Context, request CloneRequest) (CloneOutcome, error) {
	if manager == nil || manager.git == nil {
		return CloneOutcome{}, errors.New("git executable is not available on this worker node")
	}
	if len(manager.roots) == 0 {
		return CloneOutcome{}, errors.New("worker node has no configured workspace roots")
	}

	cleanURL, normalizedIdentity, err := protocol.ValidateCloneRepositoryURL(request.RepositoryURL)
	if err != nil {
		return CloneOutcome{}, err
	}

	workspaceRoot := manager.roots[0]
	if strings.TrimSpace(request.WorkspaceRoot) != "" {
		candidateRoot := filepath.Clean(strings.TrimSpace(request.WorkspaceRoot))
		if !slices.Contains(manager.roots, candidateRoot) {
			return CloneOutcome{}, fmt.Errorf("workspace root %s is not authorized on this worker node", candidateRoot)
		}
		workspaceRoot = candidateRoot
	}
	if _, err := safepath.VerifyDirectoryWithinRoot(workspaceRoot, workspaceRoot); err != nil {
		return CloneOutcome{}, fmt.Errorf("workspace root %s is not accessible: %w", workspaceRoot, err)
	}

	branch := strings.TrimSpace(request.Branch)
	if branch != "" && !protocol.IsWorkspaceLeaseBaseBranch(branch) {
		return CloneOutcome{}, errors.New("clone branch name is invalid")
	}

	explicitDirectory := strings.TrimSpace(request.Directory)
	var relativeDirectory string
	if explicitDirectory != "" {
		validatedSubpath, subpathErr := protocol.ValidateRelativeWorkspaceSubpath(explicitDirectory)
		if subpathErr != nil {
			return CloneOutcome{}, subpathErr
		}
		relativeDirectory = validatedSubpath
	} else {
		existingRepositories, discoverErr := DiscoverRepositories(operationContext, []string{workspaceRoot}, manager.git)
		if discoverErr == nil {
			for _, existing := range existingRepositories {
				if existing.MatchesRepository(cleanURL) {
					allRepositories, _ := manager.DiscoverRepositories(operationContext)
					return CloneOutcome{
						Repository:   existing,
						Repositories: allRepositories,
						Created:      false,
					}, nil
				}
			}
		}
		defaultDirectory, defaultErr := protocol.DefaultCloneDirectoryName(cleanURL)
		if defaultErr != nil {
			return CloneOutcome{}, defaultErr
		}
		relativeDirectory = defaultDirectory
	}

	targetPath := filepath.Join(workspaceRoot, filepath.FromSlash(relativeDirectory))
	lock := manager.repositoryLock(targetPath)
	lock.Lock()
	defer lock.Unlock()

	parentDirectory := filepath.Dir(targetPath)
	if _, err := safepath.EnsureDirectoryWithinRoot(workspaceRoot, parentDirectory, 0o755); err != nil {
		return CloneOutcome{}, fmt.Errorf("prepare clone parent directory: %w", err)
	}

	targetInformation, statErr := os.Lstat(targetPath)
	switch {
	case statErr == nil:
		if ancestorErr := safepath.RejectUnsafeAncestor(targetPath, targetInformation); ancestorErr != nil {
			return CloneOutcome{}, fmt.Errorf("target path %s is unsafe: %w", relativeDirectory, ancestorErr)
		}
		if existing, isRepository := inspectRepositoryCheckout(operationContext, manager.git, workspaceRoot, targetPath); isRepository && existing.MatchesRepository(cleanURL) {
			allRepositories, _ := manager.DiscoverRepositories(operationContext)
			return CloneOutcome{
				Repository:   existing,
				Repositories: allRepositories,
				Created:      false,
			}, nil
		}
		return CloneOutcome{}, fmt.Errorf("target directory %s already exists and does not match %s", relativeDirectory, normalizedIdentity)
	case !errors.Is(statErr, fs.ErrNotExist):
		return CloneOutcome{}, fmt.Errorf("inspect target directory %s: %w", relativeDirectory, statErr)
	}

	cloneArguments := []string{"-c", "protocol.file.allow=never", "clone", "--quiet"}
	if branch != "" {
		cloneArguments = append(cloneArguments, "--branch", branch)
	}
	cloneArguments = append(cloneArguments, "--", cleanURL, targetPath)

	if _, cloneErr := manager.git.RunWithTimeout(operationContext, DefaultGitCloneTimeout, workspaceRoot, cloneArguments...); cloneErr != nil {
		return CloneOutcome{}, formatGitCloneError(cloneErr)
	}

	if _, verifyErr := safepath.VerifyDirectoryWithinRoot(workspaceRoot, targetPath); verifyErr != nil {
		return CloneOutcome{}, fmt.Errorf("verify cloned repository directory: %w", verifyErr)
	}
	clonedRepository, ok := inspectRepositoryCheckout(operationContext, manager.git, workspaceRoot, targetPath)
	if !ok {
		return CloneOutcome{}, errors.New("cloned directory is not a readable Git repository")
	}

	allRepositories, discoverErr := manager.DiscoverRepositories(operationContext)
	if discoverErr != nil {
		allRepositories = []protocol.DiscoveredRepository{clonedRepository}
	}

	return CloneOutcome{
		Repository:   clonedRepository,
		Repositories: allRepositories,
		Created:      true,
	}, nil
}

func formatGitCloneError(err error) error {
	gitError, ok := errors.AsType[*GitError](err)
	if !ok {
		return fmt.Errorf("git clone failed: %s", protocol.BoundedDiagnostic(protocol.RedactText(err.Error())))
	}
	if gitError.Stderr != "" {
		sanitized := protocol.BoundedDiagnostic(protocol.RedactText(gitError.Stderr))
		return fmt.Errorf("git clone failed: %s", sanitized)
	}
	return fmt.Errorf("git clone failed: %s", gitError.Error())
}

func inspectRepositoryCheckout(
	operationContext context.Context,
	git *Git,
	root string,
	directory string,
) (protocol.DiscoveredRepository, bool) {
	gitDirectory := filepath.Join(directory, ".git")
	information, err := os.Lstat(gitDirectory)
	if err != nil || information.Mode()&fs.ModeSymlink != 0 || !information.IsDir() {
		return protocol.DiscoveredRepository{}, false
	}

	output, err := git.Run(operationContext, directory, "rev-parse", "--path-format=absolute", "--show-toplevel", "--git-common-dir", "--is-bare-repository")
	if err != nil {
		return protocol.DiscoveredRepository{}, false
	}
	lines := strings.Split(strings.TrimSuffix(string(output), "\n"), "\n")
	if len(lines) != 3 || lines[0] != directory || !sameCanonicalPath(lines[1], gitDirectory) || lines[2] != "false" {
		return protocol.DiscoveredRepository{}, false
	}

	relativePath := "."
	if directory != root {
		computedRelative, relErr := filepath.Rel(root, directory)
		if relErr != nil {
			return protocol.DiscoveredRepository{}, false
		}
		validatedRelative, validErr := protocol.ValidateRelativeWorkspaceSubpath(filepath.ToSlash(computedRelative))
		if validErr != nil {
			return protocol.DiscoveredRepository{}, false
		}
		relativePath = validatedRelative
	}

	primaryRepository, remotes := readNormalizedRemotes(operationContext, git, directory)
	defaultBranch := readOriginDefaultBranch(operationContext, git, directory)
	headBranch := readHeadBranch(operationContext, git, directory)
	if defaultBranch == "" && headBranch != "" && len(remotes) == 0 {
		defaultBranch = headBranch
	}

	discovered := protocol.DiscoveredRepository{
		Root:          filepath.ToSlash(root),
		Path:          filepath.ToSlash(directory),
		RelativePath:  relativePath,
		Repository:    primaryRepository,
		Remotes:       remotes,
		DefaultBranch: defaultBranch,
		HeadBranch:    headBranch,
	}
	if discovered.Validate() != nil {
		return protocol.DiscoveredRepository{}, false
	}
	return discovered, true
}

func readNormalizedRemotes(operationContext context.Context, git *Git, directory string) (string, []string) {
	output, err := git.Run(operationContext, directory, "config", "--local", "--null", "--get-regexp", `^remote\.[^.]+\.url$`)
	if err != nil || len(output) == 0 {
		return "", nil
	}

	type namedRemote struct {
		name     string
		identity string
	}
	var parsed []namedRemote
	for entry := range strings.SplitSeq(strings.TrimSuffix(string(output), "\x00"), "\x00") {
		if entry == "" {
			continue
		}
		key, rawURL, found := strings.Cut(entry, "\n")
		if !found {
			continue
		}
		remoteName := strings.TrimPrefix(key, "remote.")
		remoteName = strings.TrimSuffix(remoteName, ".url")
		normalized, ok := protocol.NormalizeRepositoryIdentity(strings.TrimSpace(rawURL))
		if !ok || !protocol.IsBoundedString(normalized, protocol.RepositoryURLMaximumBytes) {
			continue
		}
		parsed = append(parsed, namedRemote{name: remoteName, identity: normalized})
	}
	if len(parsed) == 0 {
		return "", nil
	}

	slices.SortFunc(parsed, func(left, right namedRemote) int {
		if (left.name == "origin") != (right.name == "origin") {
			if left.name == "origin" {
				return -1
			}
			return 1
		}
		if compared := cmp.Compare(left.name, right.name); compared != 0 {
			return compared
		}
		return cmp.Compare(left.identity, right.identity)
	})

	primary := parsed[0].identity
	uniqueRemotes := make([]string, 0, min(len(parsed), protocol.MaximumRemotesPerRepository))
	for _, candidate := range parsed {
		if !slices.Contains(uniqueRemotes, candidate.identity) {
			uniqueRemotes = append(uniqueRemotes, candidate.identity)
			if len(uniqueRemotes) >= protocol.MaximumRemotesPerRepository {
				break
			}
		}
	}
	slices.Sort(uniqueRemotes)
	return primary, uniqueRemotes
}

func readOriginDefaultBranch(operationContext context.Context, git *Git, directory string) string {
	output, err := git.Run(operationContext, directory, "symbolic-ref", "--quiet", "refs/remotes/origin/HEAD")
	if err != nil {
		return ""
	}
	reference := strings.TrimSpace(string(output))
	branchName, ok := strings.CutPrefix(reference, "refs/remotes/origin/")
	if !ok || !protocol.IsWorkspaceLeaseBaseBranch(branchName) {
		return ""
	}
	return branchName
}

func readHeadBranch(operationContext context.Context, git *Git, directory string) string {
	output, err := git.Run(operationContext, directory, "symbolic-ref", "--quiet", "--short", "HEAD")
	if err != nil {
		return ""
	}
	branchName := strings.TrimSpace(string(output))
	if !protocol.IsWorkspaceLeaseBaseBranch(branchName) {
		return ""
	}
	return branchName
}
