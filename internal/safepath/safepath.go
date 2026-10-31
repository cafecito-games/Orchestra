// Package safepath creates and verifies directories beneath a trusted root one path component at
// a time, refusing any symbolic link or non-directory at every level instead of following it.
package safepath

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// EnsureDirectoryWithinRoot walks from root down to directory one component at a time using
// Lstat before trusting or creating any component. Existing symbolic links are refused at every
// level, and missing components are created with plain Mkdir and re-inspected with Lstat.
func EnsureDirectoryWithinRoot(root string, directory string, mode fs.FileMode) (string, error) {
	return walk(root, directory, func(current string) error {
		information, err := os.Lstat(current)
		switch {
		case err == nil:
			return RejectUnsafeAncestor(current, information)
		case errors.Is(err, fs.ErrNotExist):
			if mkdirErr := os.Mkdir(current, mode); mkdirErr != nil && !errors.Is(mkdirErr, fs.ErrExist) {
				return fmt.Errorf("create directory ancestor %s: %w", current, mkdirErr)
			}
			information, err := os.Lstat(current)
			if err != nil {
				return fmt.Errorf("inspect directory ancestor %s after creating it: %w", current, err)
			}
			return RejectUnsafeAncestor(current, information)
		default:
			return fmt.Errorf("inspect directory ancestor %s: %w", current, err)
		}
	})
}

// VerifyDirectoryWithinRoot performs the same symlink-refusing walk as EnsureDirectoryWithinRoot
// without creating missing components.
func VerifyDirectoryWithinRoot(root string, directory string) (string, error) {
	return walk(root, directory, func(current string) error {
		information, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("inspect directory ancestor %s: %w", current, err)
		}
		return RejectUnsafeAncestor(current, information)
	})
}

// VerifyRegularFileWithinRoot verifies every ancestor directory from root down to filePath's
// parent without following symbolic links, then verifies filePath itself is a regular file.
func VerifyRegularFileWithinRoot(root string, filePath string) (string, fs.FileInfo, error) {
	if !filepath.IsAbs(root) || !filepath.IsAbs(filePath) {
		return "", nil, errors.New("root and target file must be absolute paths")
	}
	cleanRoot := filepath.Clean(root)
	cleanFile := filepath.Clean(filePath)
	if !strings.HasPrefix(cleanFile, cleanRoot+string(filepath.Separator)) {
		return "", nil, errors.New("target file escapes the root")
	}
	parentDirectory := filepath.Dir(cleanFile)
	if _, err := VerifyDirectoryWithinRoot(cleanRoot, parentDirectory); err != nil {
		return "", nil, err
	}
	information, err := os.Lstat(cleanFile)
	if err != nil {
		return "", nil, fmt.Errorf("inspect file %s: %w", cleanFile, err)
	}
	if information.Mode()&fs.ModeSymlink != 0 {
		return "", nil, fmt.Errorf("file %s is a symlink", cleanFile)
	}
	if !information.Mode().IsRegular() {
		return "", nil, fmt.Errorf("file %s is not a regular file", cleanFile)
	}
	return cleanFile, information, nil
}

// RejectUnsafeAncestor refuses a symbolic link or anything that is not a directory.
func RejectUnsafeAncestor(current string, information fs.FileInfo) error {
	if information.Mode()&fs.ModeSymlink != 0 {
		return fmt.Errorf("directory ancestor %s is a symlink", current)
	}
	if !information.IsDir() {
		return fmt.Errorf("directory ancestor %s is not a directory", current)
	}
	return nil
}

func walk(root string, directory string, visit func(string) error) (string, error) {
	if !filepath.IsAbs(root) || !filepath.IsAbs(directory) {
		return "", errors.New("root and target directory must be absolute paths")
	}
	cleanRoot := filepath.Clean(root)
	cleanDirectory := filepath.Clean(directory)
	if cleanDirectory != cleanRoot && !strings.HasPrefix(cleanDirectory, cleanRoot+string(filepath.Separator)) {
		return "", errors.New("target directory escapes the root")
	}
	rootInformation, err := os.Lstat(cleanRoot)
	if err != nil {
		return "", fmt.Errorf("inspect root: %w", err)
	}
	if err := RejectUnsafeAncestor(cleanRoot, rootInformation); err != nil {
		return "", err
	}
	suffix, _ := strings.CutPrefix(cleanDirectory, cleanRoot)
	relative := strings.TrimPrefix(suffix, string(filepath.Separator))
	current := cleanRoot
	if relative == "" {
		return current, nil
	}
	for component := range strings.SplitSeq(relative, string(filepath.Separator)) {
		if component == "" || component == "." || component == ".." {
			return "", errors.New("target directory path is malformed")
		}
		current = filepath.Join(current, component)
		if err := visit(current); err != nil {
			return "", err
		}
	}
	return current, nil
}
