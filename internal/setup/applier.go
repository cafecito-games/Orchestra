package setup

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/safepath"
)

type ApplyOptions struct {
	HTTPClient            *http.Client
	AllowedHosts          []string
	ManualArtifactSources map[string]string
	ManualChecksums       map[string]string
}

type ApplyResult struct {
	Applied []Operation
	Skipped []Operation
}

func Apply(operationContext context.Context, plan Plan, manifestBytes []byte, ledger OwnershipLedger, dataRoot string, options ApplyOptions) (ApplyResult, error) {
	result := ApplyResult{}
	if ComputePlanDigest(plan) != plan.Digest {
		return result, errors.New("supplied plan digest does not match its own contents")
	}
	manifest, err := ParseManifest(manifestBytes)
	if err != nil {
		return result, fmt.Errorf("apply manifest: %w", err)
	}
	nodePlatform := CurrentPlatform()
	if plan.Platform != nodePlatform {
		return result, fmt.Errorf("plan was built for platform %s, this node is %s; run setup plan again on this node", plan.Platform, nodePlatform)
	}
	derivedPlan, _, err := BuildPlan(manifestBytes, manifest, nodePlatform, dataRoot, ledger)
	if err != nil {
		return result, fmt.Errorf("derive plan from manifest and current state: %w", err)
	}
	if !plansEqual(plan, derivedPlan) {
		return result, errors.New("supplied plan does not match the manifest and current node state; run setup plan again")
	}
	for _, operation := range derivedPlan.Operations {
		switch operation.ExpectedCurrentState {
		case ExpectedUnownedExists:
			return result, fmt.Errorf("component %s target %s holds a file this tool did not create", operation.Component, operation.TargetPath)
		case ExpectedOwnedMatch:
			result.Skipped = append(result.Skipped, operation)
			continue
		case ExpectedAbsent:
		default:
			return result, fmt.Errorf("component %s target state %q is unknown; run setup plan again", operation.Component, operation.ExpectedCurrentState)
		}
		record, err := installOperation(operationContext, operation, dataRoot, options)
		if err != nil {
			return result, err
		}
		ledger = ledger.WithRecord(record)
		if err := ledger.Save(dataRoot); err != nil {
			return result, fmt.Errorf("persist ownership ledger after installing component %s: %w", operation.Component, err)
		}
		result.Applied = append(result.Applied, operation)
	}
	return result, nil
}

func plansEqual(first Plan, second Plan) bool {
	return reflect.DeepEqual(first, second)
}

func installOperation(operationContext context.Context, operation Operation, dataRoot string, options ApplyOptions) (OwnershipRecord, error) {
	switch operation.Kind {
	case OperationInstallArchive:
		return installArchive(operationContext, operation, dataRoot, options)
	case OperationManualPlacementCheck:
		return installManualArtifact(operation, dataRoot, options)
	default:
		return OwnershipRecord{}, fmt.Errorf("component %s operation kind %q is unknown", operation.Component, operation.Kind)
	}
}

func installArchive(operationContext context.Context, operation Operation, dataRoot string, options ApplyOptions) (OwnershipRecord, error) {
	parsedURL, err := url.Parse(operation.Source.URL)
	if err != nil {
		return OwnershipRecord{}, fmt.Errorf("component %s source url cannot be parsed", operation.Component)
	}
	lowercasePath := strings.ToLower(parsedURL.Path)
	isZip := strings.HasSuffix(lowercasePath, ".zip")
	if !isZip && !strings.HasSuffix(lowercasePath, ".tar.gz") {
		return OwnershipRecord{}, fmt.Errorf("component %s archive must be a .tar.gz or .zip distribution", operation.Component)
	}
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		return OwnershipRecord{}, fmt.Errorf("create data root %s: %w", dataRoot, err)
	}
	stagingDirectory, err := os.MkdirTemp(dataRoot, "setup-staging-*")
	if err != nil {
		return OwnershipRecord{}, fmt.Errorf("create staging directory under %s: %w", dataRoot, err)
	}
	defer os.RemoveAll(stagingDirectory)
	archivePath, err := DownloadVerified(operationContext, options.HTTPClient, stagingDirectory, DownloadOptions{
		URL:            operation.Source.URL,
		ExpectedSHA256: operation.ExpectedChecksum,
		MaximumBytes:   operation.Source.SizeBytes,
		AllowedHosts:   options.AllowedHosts,
	})
	if err != nil {
		return OwnershipRecord{}, fmt.Errorf("component %s archive download failed: %w", operation.Component, err)
	}
	entryReader, closeEntry, err := openArchiveEntry(archivePath, operation.Source.ExecutablePath, isZip)
	if err != nil {
		return OwnershipRecord{}, fmt.Errorf("component %s archive extraction failed: %w", operation.Component, err)
	}
	defer closeEntry()
	return stageAndInstall(dataRoot, operation, entryReader, "")
}

func installManualArtifact(operation Operation, dataRoot string, options ApplyOptions) (OwnershipRecord, error) {
	source, hasSource := options.ManualArtifactSources[operation.Component.ID]
	assertedChecksum, hasChecksum := options.ManualChecksums[operation.Component.ID]
	if !hasSource || source == "" || !hasChecksum || assertedChecksum == "" {
		return OwnershipRecord{}, fmt.Errorf("component %s requires both a manual artifact source and a manual checksum", operation.Component)
	}
	sourceFile, err := os.Open(source)
	if err != nil {
		return OwnershipRecord{}, fmt.Errorf("component %s manual artifact could not be opened", operation.Component)
	}
	defer sourceFile.Close()
	return stageAndInstall(dataRoot, operation, sourceFile, assertedChecksum)
}

func stageAndInstall(dataRoot string, operation Operation, reader io.Reader, expectedChecksum string) (OwnershipRecord, error) {
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		return OwnershipRecord{}, fmt.Errorf("create data root %s: %w", dataRoot, err)
	}
	targetDirectory := filepath.Dir(operation.TargetPath)
	resolvedDirectory, err := ensureDirectoryWithinRoot(dataRoot, targetDirectory)
	if err != nil {
		return OwnershipRecord{}, fmt.Errorf("component %s install directory is not safely usable: %w", operation.Component, err)
	}
	temporaryFile, err := os.CreateTemp(resolvedDirectory, "setup-tmp-*")
	if err != nil {
		return OwnershipRecord{}, fmt.Errorf("component %s could not stage a temporary file: %w", operation.Component, err)
	}
	temporaryPath := temporaryFile.Name()
	defer func() {
		temporaryFile.Close()
		os.Remove(temporaryPath)
	}()

	digest := sha256.New()
	written, err := io.Copy(io.MultiWriter(temporaryFile, digest), reader)
	if err != nil {
		return OwnershipRecord{}, fmt.Errorf("component %s artifact could not be staged: %w", operation.Component, err)
	}
	contentSHA256 := hex.EncodeToString(digest.Sum(nil))
	if expectedChecksum != "" && !strings.EqualFold(contentSHA256, expectedChecksum) {
		return OwnershipRecord{}, fmt.Errorf("component %s artifact failed checksum verification", operation.Component)
	}
	if runtime.GOOS != "windows" {
		if err := temporaryFile.Chmod(0o755); err != nil {
			return OwnershipRecord{}, fmt.Errorf("component %s artifact could not be marked executable: %w", operation.Component, err)
		}
	}
	if err := temporaryFile.Sync(); err != nil {
		return OwnershipRecord{}, fmt.Errorf("component %s artifact could not be synced: %w", operation.Component, err)
	}
	if err := temporaryFile.Close(); err != nil {
		return OwnershipRecord{}, fmt.Errorf("component %s artifact could not be finalized: %w", operation.Component, err)
	}

	if _, err := ensureDirectoryWithinRoot(dataRoot, targetDirectory); err != nil {
		return OwnershipRecord{}, fmt.Errorf("component %s install directory changed unsafely during install: %w", operation.Component, err)
	}
	if _, err := os.Lstat(operation.TargetPath); err == nil {
		return OwnershipRecord{}, fmt.Errorf("component %s target %s appeared unexpectedly during install", operation.Component, operation.TargetPath)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return OwnershipRecord{}, fmt.Errorf("component %s target could not be inspected: %w", operation.Component, err)
	}
	if err := os.Link(temporaryPath, operation.TargetPath); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return OwnershipRecord{}, fmt.Errorf("component %s target %s appeared unexpectedly during install", operation.Component, operation.TargetPath)
		}
		return OwnershipRecord{}, fmt.Errorf("component %s could not be installed without replacement: %w", operation.Component, err)
	}
	return OwnershipRecord{
		Path:          operation.TargetPath,
		Component:     operation.Component,
		HarnessID:     operation.HarnessID,
		ContentSHA256: contentSHA256,
		SizeBytes:     written,
		InstalledAt:   time.Now().UTC().Format(time.RFC3339Nano),
	}, nil
}

func ensureDirectoryWithinRoot(dataRoot string, directory string) (string, error) {
	return safepath.EnsureDirectoryWithinRoot(dataRoot, directory, 0o755)
}

func verifyDirectoryWithinRoot(dataRoot string, directory string) (string, error) {
	return safepath.VerifyDirectoryWithinRoot(dataRoot, directory)
}

func openArchiveEntry(archivePath string, executablePath string, isZip bool) (io.Reader, func(), error) {
	if err := validateArchiveEntryName(executablePath); err != nil {
		return nil, nil, err
	}
	if isZip {
		return openZipEntry(archivePath, executablePath)
	}
	return openTarGzipEntry(archivePath, executablePath)
}

func openTarGzipEntry(archivePath string, executablePath string) (io.Reader, func(), error) {
	archiveFile, err := os.Open(archivePath)
	if err != nil {
		return nil, nil, fmt.Errorf("open downloaded archive: %w", err)
	}
	gzipReader, err := gzip.NewReader(archiveFile)
	if err != nil {
		archiveFile.Close()
		return nil, nil, fmt.Errorf("open gzip stream: %w", err)
	}
	reader := tar.NewReader(gzipReader)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			gzipReader.Close()
			archiveFile.Close()
			return nil, nil, errors.New("archive does not contain the declared executable entry")
		}
		if err != nil {
			gzipReader.Close()
			archiveFile.Close()
			return nil, nil, fmt.Errorf("read archive entry: %w", err)
		}
		if header.Typeflag != tar.TypeReg || path.Clean(header.Name) != path.Clean(executablePath) {
			continue
		}
		if err := validateArchiveEntryName(header.Name); err != nil {
			gzipReader.Close()
			archiveFile.Close()
			return nil, nil, err
		}
		return reader, func() { gzipReader.Close(); archiveFile.Close() }, nil
	}
}

func openZipEntry(archivePath string, executablePath string) (io.Reader, func(), error) {
	zipReader, err := zip.OpenReader(archivePath)
	if err != nil {
		return nil, nil, fmt.Errorf("open zip archive: %w", err)
	}
	for _, file := range zipReader.File {
		if file.FileInfo().IsDir() || path.Clean(file.Name) != path.Clean(executablePath) {
			continue
		}
		if err := validateArchiveEntryName(file.Name); err != nil {
			zipReader.Close()
			return nil, nil, err
		}
		opened, err := file.Open()
		if err != nil {
			zipReader.Close()
			return nil, nil, fmt.Errorf("open zip entry: %w", err)
		}
		return opened, func() { opened.Close(); zipReader.Close() }, nil
	}
	zipReader.Close()
	return nil, nil, errors.New("archive does not contain the declared executable entry")
}

func validateArchiveEntryName(entryName string) error {
	if entryName == "" || strings.ContainsRune(entryName, '\\') {
		return errors.New("archive entry name is empty or uses backslashes")
	}
	cleaned := path.Clean(entryName)
	if strings.HasPrefix(cleaned, "/") || path.IsAbs(cleaned) {
		return errors.New("archive entry name must be relative")
	}
	if slices.Contains(strings.Split(cleaned, "/"), "..") {
		return errors.New("archive entry name must not traverse upward")
	}
	return nil
}
