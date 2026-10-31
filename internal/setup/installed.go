package setup

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

var ErrComponentNotInstalled = errors.New("component is not installed")

type InstalledComponent struct {
	Entry         ComponentManifestEntry
	Path          string
	ContentSHA256 string
	dataRoot      string
}

func (installed InstalledComponent) Reference() ComponentReference {
	return installed.Entry.Reference()
}

func VerifyInstalledComponent(dataRoot string, entry ComponentManifestEntry, platform string, ledger OwnershipLedger) (InstalledComponent, error) {
	distribution, supported := entry.Platforms[platform]
	if !supported {
		return InstalledComponent{}, fmt.Errorf("%w: no platform distribution for %s", ErrComponentNotInstalled, platform)
	}
	if !filepath.IsAbs(dataRoot) {
		return InstalledComponent{}, errors.New("data root must be an absolute path")
	}
	target, err := ComponentTargetPath(dataRoot, entry, distribution)
	if err != nil {
		return InstalledComponent{}, err
	}
	if _, err := os.Lstat(target); errors.Is(err, fs.ErrNotExist) {
		return InstalledComponent{}, ErrComponentNotInstalled
	}
	record, owned := ledger.RecordFor(target)
	if !owned {
		return InstalledComponent{}, errors.New("component executable is not recorded in the ownership ledger")
	}
	if record.Component != entry.Reference() {
		return InstalledComponent{}, errors.New("ownership ledger records a different component, kind, or version at the target path")
	}
	installed := InstalledComponent{Entry: entry, Path: target, ContentSHA256: strings.ToLower(record.ContentSHA256), dataRoot: dataRoot}
	if err := installed.Verify(); err != nil {
		return InstalledComponent{}, err
	}
	return installed, nil
}

func (installed InstalledComponent) Verify() error {
	if _, err := verifyDirectoryWithinRoot(installed.dataRoot, filepath.Dir(installed.Path)); err != nil {
		return fmt.Errorf("component install directory is not safely contained in the data root: %w", err)
	}
	information, err := os.Lstat(installed.Path)
	if err != nil {
		return fmt.Errorf("inspect component executable: %w", err)
	}
	if !information.Mode().IsRegular() {
		return errors.New("component executable is not a regular file")
	}
	digest, err := fileChecksum(installed.Path)
	if err != nil {
		return fmt.Errorf("read component executable: %w", err)
	}
	if digest != installed.ContentSHA256 {
		return errors.New("component executable content no longer matches the ownership ledger")
	}
	return nil
}

// DiscoverExternalACPAdapter searches PATH for a package-manager-installed ACP
// adapter executable matching the manifest entry, following symlinks created by
// Homebrew, Linuxbrew, or npm global shims to verify an executable target file.
func DiscoverExternalACPAdapter(entry ComponentManifestEntry, platform string) (string, bool) {
	if entry.Kind != ComponentKindACPAdapter {
		return "", false
	}
	var candidateNames []string
	if distribution, supported := entry.Platforms[platform]; supported && distribution.ExecutablePath != "" {
		baseName := filepath.Base(distribution.ExecutablePath)
		if baseName != "" && baseName != "." && baseName != "/" {
			candidateNames = append(candidateNames, baseName)
		}
	}
	if entry.ID != "" && !slices.Contains(candidateNames, entry.ID) {
		candidateNames = append(candidateNames, entry.ID)
	}
	for _, candidateName := range candidateNames {
		lookedUpPath, err := exec.LookPath(candidateName)
		if err != nil {
			continue
		}
		absolutePath, err := filepath.Abs(lookedUpPath)
		if err != nil {
			continue
		}
		information, err := os.Stat(absolutePath)
		if err != nil || !information.Mode().IsRegular() || (runtime.GOOS != "windows" && information.Mode().Perm()&0o111 == 0) {
			continue
		}
		return filepath.Clean(absolutePath), true
	}
	return "", false
}
