package selfupdate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

type InstallOutcome struct {
	InstalledPath        string
	UsedDataRootFallback bool
}

type LocalUpdateOptions struct {
	ReleaseClient  *ReleaseClient
	ExecutablePath string
	CurrentVersion string
	Platform       string
	TargetVersion  string
	CheckOnly      bool
	Force          bool
}

type LocalUpdateResult struct {
	CurrentVersion  string `json:"currentVersion"`
	TargetVersion   string `json:"targetVersion"`
	ReleaseTag      string `json:"releaseTag"`
	Platform        string `json:"platform"`
	ExecutablePath  string `json:"executablePath,omitempty"`
	UpdateAvailable bool   `json:"updateAvailable"`
	Updated         bool   `json:"updated"`
	SHA256          string `json:"sha256,omitempty"`
}

func ProbeBinary(
	operationContext context.Context,
	binaryPath string,
	expectedVersion string,
	expectedProtocolVersion string,
) (protocol.BuildInfo, error) {
	probeContext, cancel := context.WithTimeout(operationContext, 10*time.Second)
	defer cancel()

	command := exec.CommandContext(probeContext, binaryPath, "version", "--json")
	command.Env = minimalProbeEnvironment()
	output, err := command.CombinedOutput()
	if err != nil {
		snippet := strings.TrimSpace(protocol.RedactText(string(output)))
		if snippet != "" {
			return protocol.BuildInfo{}, fmt.Errorf("candidate binary probe failed: %w (%s)", err, protocol.TruncateDiagnostic(snippet))
		}
		return protocol.BuildInfo{}, fmt.Errorf("candidate binary probe failed: %w", err)
	}

	var buildInfo protocol.BuildInfo
	if err := json.Unmarshal(output, &buildInfo); err != nil {
		return protocol.BuildInfo{}, fmt.Errorf("candidate binary reported malformed version JSON: %w", err)
	}
	if expectedVersion != "" && buildInfo.Version != expectedVersion {
		return protocol.BuildInfo{}, fmt.Errorf("candidate binary reported version %q rather than %q", buildInfo.Version, expectedVersion)
	}
	if expectedProtocolVersion != "" && buildInfo.ProtocolVersion != expectedProtocolVersion {
		return protocol.BuildInfo{}, fmt.Errorf(
			"candidate binary uses protocol v%s which is incompatible with v%s",
			buildInfo.ProtocolVersion,
			expectedProtocolVersion,
		)
	}
	return buildInfo, nil
}

func DownloadVerifiedBinary(
	operationContext context.Context,
	httpClient *http.Client,
	downloadURL string,
	bearerToken string,
	expectedSHA256 string,
	expectedSizeBytes int64,
	stagingDirectory string,
) (string, error) {
	if !checksumHexPattern.MatchString(strings.ToLower(expectedSHA256)) {
		return "", errors.New("expectedSha256 must be 64 hex characters")
	}
	if expectedSizeBytes <= 0 || expectedSizeBytes > MaximumBinaryBytes {
		return "", errors.New("expectedSizeBytes is out of bounds")
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Minute}
	}
	if err := os.MkdirAll(stagingDirectory, 0o700); err != nil {
		return "", fmt.Errorf("prepare download staging directory: %w", err)
	}

	temporaryFile, err := os.CreateTemp(stagingDirectory, ".orchestra-download-*")
	if err != nil {
		return "", fmt.Errorf("create staged binary file: %w", err)
	}
	temporaryPath := temporaryFile.Name()
	fail := func(failure error) (string, error) {
		_ = temporaryFile.Close()
		_ = os.Remove(temporaryPath)
		return "", failure
	}

	request, err := http.NewRequestWithContext(operationContext, http.MethodGet, downloadURL, nil)
	if err != nil {
		return fail(fmt.Errorf("build bundle download request: %w", err))
	}
	if bearerToken != "" {
		request.Header.Set("Authorization", "Bearer "+bearerToken)
	}

	response, err := httpClient.Do(request)
	if err != nil {
		return fail(errors.New(protocol.RedactText(err.Error())))
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fail(fmt.Errorf("bundle download returned HTTP %d", response.StatusCode))
	}

	digest := sha256.New()
	written, err := io.Copy(temporaryFile, io.TeeReader(io.LimitReader(response.Body, expectedSizeBytes+1), digest))
	if err != nil {
		return fail(fmt.Errorf("stream bundle binary: %w", err))
	}
	if written != expectedSizeBytes {
		return fail(fmt.Errorf("bundle byte size mismatch: expected %d, got %d", expectedSizeBytes, written))
	}
	if runtime.GOOS != "windows" {
		if chmodErr := temporaryFile.Chmod(0o755); chmodErr != nil {
			return fail(fmt.Errorf("mark bundle binary executable: %w", chmodErr))
		}
	}
	if syncErr := temporaryFile.Sync(); syncErr != nil {
		return fail(fmt.Errorf("sync bundle binary: %w", syncErr))
	}
	if closeErr := temporaryFile.Close(); closeErr != nil {
		return fail(fmt.Errorf("close bundle binary: %w", closeErr))
	}

	actualSHA256 := hex.EncodeToString(digest.Sum(nil))
	if !strings.EqualFold(actualSHA256, expectedSHA256) {
		_ = os.Remove(temporaryPath)
		return "", fmt.Errorf("bundle checksum mismatch: expected %s, got %s", strings.ToLower(expectedSHA256), actualSHA256)
	}
	return temporaryPath, nil
}

func InstallBinary(
	stagedPath string,
	targetExecutablePath string,
	fallbackDataRoot string,
) (InstallOutcome, error) {
	resolvedTarget := strings.TrimSpace(targetExecutablePath)
	if resolvedTarget == "" {
		executable, err := os.Executable()
		if err != nil {
			return InstallOutcome{}, fmt.Errorf("locate running executable: %w", err)
		}
		resolvedTarget = executable
	}
	if evaluated, err := filepath.EvalSymlinks(resolvedTarget); err == nil {
		resolvedTarget = evaluated
	}
	resolvedTarget = filepath.Clean(resolvedTarget)

	if err := atomicCopyReplace(stagedPath, resolvedTarget); err != nil {
		if fallbackDataRoot != "" && isPermissionOrReadOnlyError(err) {
			fallbackDirectory := filepath.Join(fallbackDataRoot, "self", "bin")
			if mkdirErr := os.MkdirAll(fallbackDirectory, 0o755); mkdirErr != nil {
				return InstallOutcome{}, fmt.Errorf("install to %s failed (%w) and fallback directory creation failed (%w)", resolvedTarget, err, mkdirErr)
			}
			binaryName := "orchestra"
			if runtime.GOOS == "windows" {
				binaryName = "orchestra.exe"
			}
			fallbackTarget := filepath.Join(fallbackDirectory, binaryName)
			if fallbackErr := atomicCopyReplace(stagedPath, fallbackTarget); fallbackErr != nil {
				return InstallOutcome{}, fmt.Errorf("install to %s failed (%w) and fallback %s failed (%w)", resolvedTarget, err, fallbackTarget, fallbackErr)
			}
			return InstallOutcome{
				InstalledPath:        fallbackTarget,
				UsedDataRootFallback: true,
			}, nil
		}
		return InstallOutcome{}, fmt.Errorf("replace executable %s: %w", resolvedTarget, err)
	}
	return InstallOutcome{InstalledPath: resolvedTarget}, nil
}

func UpdateLocalExecutable(
	operationContext context.Context,
	options LocalUpdateOptions,
) (LocalUpdateResult, error) {
	releaseClient := options.ReleaseClient
	if releaseClient == nil {
		createdClient, err := NewReleaseClient(ReleaseClientOptions{})
		if err != nil {
			return LocalUpdateResult{}, err
		}
		releaseClient = createdClient
	}

	currentVersion := strings.TrimSpace(options.CurrentVersion)
	if currentVersion == "" {
		currentVersion = protocol.EffectiveBuildVersion()
	}
	platform := strings.TrimSpace(options.Platform)
	if platform == "" {
		platform = runtime.GOOS + "-" + runtime.GOARCH
	}

	release, err := releaseClient.ResolveRelease(operationContext, options.TargetVersion)
	if err != nil {
		return LocalUpdateResult{}, err
	}

	archive, hasPlatform := release.Platforms[platform]
	if !hasPlatform {
		return LocalUpdateResult{}, fmt.Errorf("release %s does not include a binary archive for %s", release.TagName, platform)
	}

	updateAvailable := isUpdateNeeded(currentVersion, release.Version, strings.TrimSpace(options.TargetVersion))
	result := LocalUpdateResult{
		CurrentVersion:  currentVersion,
		TargetVersion:   release.Version,
		ReleaseTag:      release.TagName,
		Platform:        platform,
		UpdateAvailable: updateAvailable,
	}

	executablePath := strings.TrimSpace(options.ExecutablePath)
	if executablePath == "" {
		detectedExecutable, execErr := os.Executable()
		if execErr == nil {
			if evaluated, evalErr := filepath.EvalSymlinks(detectedExecutable); evalErr == nil {
				detectedExecutable = evaluated
			}
			executablePath = filepath.Clean(detectedExecutable)
		}
	}
	result.ExecutablePath = executablePath

	if options.CheckOnly {
		return result, nil
	}
	if !updateAvailable && !options.Force {
		return result, nil
	}
	if executablePath == "" {
		return LocalUpdateResult{}, errors.New("could not determine local executable path to update")
	}

	stagingDirectory, err := os.MkdirTemp("", "orchestra-selfupdate-*")
	if err != nil {
		return LocalUpdateResult{}, fmt.Errorf("create temporary update directory: %w", err)
	}
	defer os.RemoveAll(stagingDirectory)

	extracted, err := releaseClient.DownloadAndExtractBinary(operationContext, archive, stagingDirectory)
	if err != nil {
		return LocalUpdateResult{}, err
	}
	if _, err := ProbeBinary(operationContext, extracted.Path, release.Version, ""); err != nil {
		return LocalUpdateResult{}, err
	}
	outcome, err := InstallBinary(extracted.Path, executablePath, "")
	if err != nil {
		return LocalUpdateResult{}, err
	}

	result.ExecutablePath = outcome.InstalledPath
	result.Updated = true
	result.SHA256 = extracted.SHA256
	return result, nil
}

func isUpdateNeeded(currentVersion string, targetVersion string, requestedRaw string) bool {
	if currentVersion == targetVersion {
		return false
	}
	requestedTrimmed := strings.TrimSpace(requestedRaw)
	explicitPin := requestedTrimmed != "" && !strings.EqualFold(requestedTrimmed, "latest")
	if explicitPin {
		return true
	}
	if compared, bothSemver := protocol.CompareSemanticVersions(currentVersion, targetVersion); bothSemver {
		return compared < 0
	}
	return true
}

func atomicCopyReplace(sourcePath string, destinationPath string) error {
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	parentDirectory := filepath.Dir(destinationPath)
	temporaryFile, err := os.CreateTemp(parentDirectory, ".orchestra-new-*")
	if err != nil {
		return err
	}
	temporaryPath := temporaryFile.Name()
	cleanup := func(failure error) error {
		_ = temporaryFile.Close()
		_ = os.Remove(temporaryPath)
		return failure
	}

	if _, err := io.Copy(temporaryFile, sourceFile); err != nil {
		return cleanup(err)
	}
	if runtime.GOOS != "windows" {
		if err := temporaryFile.Chmod(0o755); err != nil {
			return cleanup(err)
		}
	}
	if err := temporaryFile.Sync(); err != nil {
		return cleanup(err)
	}
	if err := temporaryFile.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	if err := os.Rename(temporaryPath, destinationPath); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	if directoryHandle, openErr := os.Open(parentDirectory); openErr == nil {
		_ = directoryHandle.Sync()
		_ = directoryHandle.Close()
	}
	return nil
}

func minimalProbeEnvironment() []string {
	var environment []string
	for _, key := range []string{"PATH", "HOME", "TMPDIR", "SYSTEMROOT", "TEMP", "TMP"} {
		if value, found := os.LookupEnv(key); found {
			environment = append(environment, key+"="+value)
		}
	}
	return environment
}
