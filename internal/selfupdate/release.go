// Package selfupdate implements verified GitHub release discovery, master-side
// platform bundle caching, pre-flight binary probing, atomic executable
// installation, and in-place daemon re-execution.
package selfupdate

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const (
	DefaultRepository = "cafecito-games/Orchestra"
	DefaultAPIBaseURL = "https://api.github.com"

	MaximumMetadataBytes = 4 * 1024 * 1024
	MaximumArchiveBytes  = 256 * 1024 * 1024
	MaximumBinaryBytes   = 256 * 1024 * 1024
)

var (
	AllowedGitHubHosts = []string{
		"api.github.com",
		"github.com",
		"objects.githubusercontent.com",
		"release-assets.githubusercontent.com",
	}

	checksumHexPattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	repositorySlugCheck = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
)

type ReleaseClientOptions struct {
	APIBaseURL        string
	Repository        string
	GitHubToken       string
	HTTPClient        *http.Client
	AllowedHosts      []string
	LookupEnvironment func(string) (string, bool)
	DisableGHFallback bool
}

type ReleaseClient struct {
	apiBaseURL        string
	apiHost           string
	apiHostAuthority  string
	repository        string
	explicitToken     string
	httpClient        *http.Client
	allowedHosts      []string
	allowLoopbackHTTP bool
	lookupEnvironment func(string) (string, bool)
	disableGHFallback bool
}

type PlatformArchive struct {
	Platform    string `json:"platform"`
	ArchiveName string `json:"archiveName"`
	DownloadURL string `json:"downloadUrl"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"sizeBytes"`
}

type ReleaseInfo struct {
	TagName     string                     `json:"tagName"`
	Version     string                     `json:"version"`
	PublishedAt string                     `json:"publishedAt,omitempty"`
	HTMLURL     string                     `json:"htmlUrl,omitempty"`
	Platforms   map[string]PlatformArchive `json:"platforms"`
}

type ExtractedBinary struct {
	Path      string
	SHA256    string
	SizeBytes int64
}

func NewReleaseClient(options ReleaseClientOptions) (*ReleaseClient, error) {
	lookupEnvironment := options.LookupEnvironment
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}

	rawBaseURL := strings.TrimSpace(options.APIBaseURL)
	if rawBaseURL == "" {
		rawBaseURL = firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_RELEASE_API_URL")
	}
	if rawBaseURL == "" {
		rawBaseURL = DefaultAPIBaseURL
	}
	parsedBase, err := url.Parse(strings.TrimRight(rawBaseURL, "/"))
	if err != nil || parsedBase.Host == "" {
		return nil, fmt.Errorf("invalid release API URL %q", rawBaseURL)
	}
	hostname := parsedBase.Hostname()
	allowLoopbackHTTP := isLoopbackHost(hostname)
	switch parsedBase.Scheme {
	case "https":
	case "http":
		if !allowLoopbackHTTP {
			return nil, errors.New("release API URL must use https unless targeting a loopback test server")
		}
	default:
		return nil, fmt.Errorf("unsupported release API URL scheme %q", parsedBase.Scheme)
	}

	repository := strings.TrimSpace(options.Repository)
	if repository == "" {
		repository = firstEnvironmentValue(lookupEnvironment, "ORCHESTRA_RELEASE_REPO")
	}
	if repository == "" {
		repository = DefaultRepository
	}
	if !repositorySlugCheck.MatchString(repository) {
		return nil, fmt.Errorf("invalid release repository %q (expected owner/repo)", repository)
	}

	allowedHosts := slices.Clone(AllowedGitHubHosts)
	if !slices.Contains(allowedHosts, hostname) {
		allowedHosts = append(allowedHosts, hostname)
	}
	for _, extraHost := range options.AllowedHosts {
		trimmed := strings.TrimSpace(extraHost)
		if trimmed != "" && !slices.Contains(allowedHosts, trimmed) {
			allowedHosts = append(allowedHosts, trimmed)
		}
	}

	baseHTTPClient := options.HTTPClient
	if baseHTTPClient == nil {
		baseHTTPClient = &http.Client{Timeout: 5 * time.Minute}
	}

	return &ReleaseClient{
		apiBaseURL:        strings.TrimRight(parsedBase.String(), "/"),
		apiHost:           hostname,
		apiHostAuthority:  parsedBase.Host,
		repository:        repository,
		explicitToken:     strings.TrimSpace(options.GitHubToken),
		httpClient:        baseHTTPClient,
		allowedHosts:      allowedHosts,
		allowLoopbackHTTP: allowLoopbackHTTP,
		lookupEnvironment: lookupEnvironment,
		disableGHFallback: options.DisableGHFallback,
	}, nil
}

func (client *ReleaseClient) ResolveRelease(operationContext context.Context, requestedVersion string) (ReleaseInfo, error) {
	normalizedTarget, err := protocol.NormalizeReleaseTarget(requestedVersion)
	if err != nil {
		return ReleaseInfo{}, err
	}

	var releaseEndpoint string
	if normalizedTarget == "latest" {
		releaseEndpoint = fmt.Sprintf("%s/repos/%s/releases/latest", client.apiBaseURL, client.repository)
	} else {
		releaseEndpoint = fmt.Sprintf("%s/repos/%s/releases/tags/v%s", client.apiBaseURL, client.repository, normalizedTarget)
	}

	token := client.resolveGitHubToken(operationContext)
	metadataBytes, err := client.getBytes(operationContext, releaseEndpoint, "application/vnd.github+json", token, MaximumMetadataBytes)
	if err != nil {
		return ReleaseInfo{}, fmt.Errorf("query GitHub release (%s): %w", normalizedTarget, err)
	}

	var githubRelease githubReleaseResponse
	if err := json.Unmarshal(metadataBytes, &githubRelease); err != nil {
		return ReleaseInfo{}, fmt.Errorf("decode GitHub release metadata: %w", err)
	}

	versionValue, err := protocol.NormalizeReleaseTarget(githubRelease.TagName)
	if err != nil || versionValue == "latest" {
		return ReleaseInfo{}, fmt.Errorf("GitHub release tag %q is not a valid release version", githubRelease.TagName)
	}

	var checksumAsset *githubReleaseAsset
	for index := range githubRelease.Assets {
		if githubRelease.Assets[index].Name == "checksums.txt" {
			checksumAsset = &githubRelease.Assets[index]
			break
		}
	}
	if checksumAsset == nil {
		return ReleaseInfo{}, fmt.Errorf("release %s does not include checksums.txt", githubRelease.TagName)
	}

	checksumURL := client.assetDownloadURL(*checksumAsset)
	if checksumURL == "" {
		return ReleaseInfo{}, fmt.Errorf("release %s checksums.txt has no download URL", githubRelease.TagName)
	}
	checksumBytes, err := client.getBytes(operationContext, checksumURL, "application/octet-stream", token, MaximumMetadataBytes)
	if err != nil {
		return ReleaseInfo{}, fmt.Errorf("download checksums.txt for %s: %w", githubRelease.TagName, err)
	}
	checksumsByName, err := parseChecksumsFile(string(checksumBytes))
	if err != nil {
		return ReleaseInfo{}, fmt.Errorf("parse checksums.txt for %s: %w", githubRelease.TagName, err)
	}

	platforms := map[string]PlatformArchive{}
	archivePrefix := "orchestra_" + versionValue + "_"
	for _, asset := range githubRelease.Assets {
		if !strings.HasPrefix(asset.Name, archivePrefix) || !strings.HasSuffix(asset.Name, ".tar.gz") {
			continue
		}
		suffix := strings.TrimSuffix(strings.TrimPrefix(asset.Name, archivePrefix), ".tar.gz")
		operatingSystem, architecture, found := strings.Cut(suffix, "_")
		if !found || operatingSystem == "" || architecture == "" {
			continue
		}
		platformKey := operatingSystem + "-" + architecture
		if !protocol.LabelOrAcceleratorPattern.MatchString(platformKey) {
			continue
		}
		digest, hasDigest := checksumsByName[asset.Name]
		if !hasDigest {
			return ReleaseInfo{}, fmt.Errorf("checksums.txt for %s is missing digest for %s", githubRelease.TagName, asset.Name)
		}
		downloadURL := client.assetDownloadURL(asset)
		if downloadURL == "" || asset.Size <= 0 {
			continue
		}
		platforms[platformKey] = PlatformArchive{
			Platform:    platformKey,
			ArchiveName: asset.Name,
			DownloadURL: downloadURL,
			SHA256:      digest,
			SizeBytes:   asset.Size,
		}
	}

	if len(platforms) == 0 {
		return ReleaseInfo{}, fmt.Errorf("release %s contains no verified platform archives", githubRelease.TagName)
	}

	return ReleaseInfo{
		TagName:     githubRelease.TagName,
		Version:     versionValue,
		PublishedAt: githubRelease.PublishedAt,
		HTMLURL:     githubRelease.HTMLURL,
		Platforms:   platforms,
	}, nil
}

func (client *ReleaseClient) DownloadAndExtractBinary(
	operationContext context.Context,
	archive PlatformArchive,
	destinationDirectory string,
) (ExtractedBinary, error) {
	if !checksumHexPattern.MatchString(strings.ToLower(archive.SHA256)) {
		return ExtractedBinary{}, errors.New("platform archive sha256 must be 64 hex characters")
	}
	if archive.SizeBytes <= 0 || archive.SizeBytes > MaximumArchiveBytes {
		return ExtractedBinary{}, errors.New("platform archive sizeBytes is out of bounds")
	}
	if err := os.MkdirAll(destinationDirectory, 0o755); err != nil {
		return ExtractedBinary{}, fmt.Errorf("prepare staging directory: %w", err)
	}

	token := client.resolveGitHubToken(operationContext)
	archivePath, err := client.downloadVerifiedToTemp(
		operationContext,
		archive.DownloadURL,
		"application/octet-stream",
		token,
		archive.SHA256,
		archive.SizeBytes,
		destinationDirectory,
	)
	if err != nil {
		return ExtractedBinary{}, err
	}
	defer os.Remove(archivePath)

	targetBinaryName := "orchestra"
	if strings.HasPrefix(archive.Platform, "windows-") {
		targetBinaryName = "orchestra.exe"
	}
	return extractBinaryFromTarGz(archivePath, targetBinaryName, destinationDirectory)
}

type githubReleaseResponse struct {
	TagName     string               `json:"tag_name"`
	HTMLURL     string               `json:"html_url"`
	PublishedAt string               `json:"published_at"`
	Assets      []githubReleaseAsset `json:"assets"`
}

type githubReleaseAsset struct {
	ID                 int64  `json:"id"`
	Name               string `json:"name"`
	URL                string `json:"url"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Size               int64  `json:"size"`
}

func (client *ReleaseClient) assetDownloadURL(asset githubReleaseAsset) string {
	if strings.TrimSpace(asset.URL) != "" {
		return strings.TrimSpace(asset.URL)
	}
	if asset.ID > 0 {
		return fmt.Sprintf("%s/repos/%s/releases/assets/%d", client.apiBaseURL, client.repository, asset.ID)
	}
	return strings.TrimSpace(asset.BrowserDownloadURL)
}

func (client *ReleaseClient) httpClientWithRedirectPolicy() *http.Client {
	configured := *client.httpClient
	configured.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		if err := client.validateTargetURL(request.URL); err != nil {
			return err
		}
		if len(via) > 0 && !strings.EqualFold(request.URL.Host, via[0].URL.Host) {
			request.Header.Del("Authorization")
		}
		return nil
	}
	return &configured
}

func (client *ReleaseClient) validateTargetURL(parsedURL *url.URL) error {
	if parsedURL == nil || parsedURL.Host == "" {
		return errors.New("download URL is malformed")
	}
	hostname := parsedURL.Hostname()
	switch parsedURL.Scheme {
	case "https":
	case "http":
		if !client.allowLoopbackHTTP || !isLoopbackHost(hostname) {
			return errors.New("download URL must use https")
		}
	default:
		return fmt.Errorf("unsupported download URL scheme %q", parsedURL.Scheme)
	}
	if !slices.Contains(client.allowedHosts, hostname) {
		return fmt.Errorf("host %q is outside the release download allowlist", hostname)
	}
	return nil
}

func (client *ReleaseClient) getBytes(
	operationContext context.Context,
	targetURL string,
	acceptHeader string,
	token string,
	maximumBytes int64,
) ([]byte, error) {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return nil, errors.New("request URL is malformed")
	}
	if err := client.validateTargetURL(parsedURL); err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(operationContext, http.MethodGet, targetURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if acceptHeader != "" {
		request.Header.Set("Accept", acceptHeader)
	}
	request.Header.Set("User-Agent", "orchestra/"+protocol.EffectiveBuildVersion())
	if token != "" && strings.EqualFold(parsedURL.Host, client.apiHostAuthority) {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := client.httpClientWithRedirectPolicy().Do(request)
	if err != nil {
		return nil, errors.New(protocol.RedactText(err.Error()))
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		bodySnippet, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		detail := strings.TrimSpace(protocol.RedactText(string(bodySnippet)))
		if response.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("HTTP 404 not found (if %s is private, set GITHUB_TOKEN or authenticate with 'gh auth login')", client.repository)
		}
		if detail != "" {
			return nil, fmt.Errorf("HTTP %d: %s", response.StatusCode, protocol.TruncateDiagnostic(detail))
		}
		return nil, fmt.Errorf("HTTP %d", response.StatusCode)
	}

	payload, err := io.ReadAll(io.LimitReader(response.Body, maximumBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}
	if int64(len(payload)) > maximumBytes {
		return nil, fmt.Errorf("response exceeded bound of %d bytes", maximumBytes)
	}
	return payload, nil
}

func (client *ReleaseClient) downloadVerifiedToTemp(
	operationContext context.Context,
	targetURL string,
	acceptHeader string,
	token string,
	expectedSHA256 string,
	maximumBytes int64,
	destinationDirectory string,
) (string, error) {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return "", errors.New("download URL is malformed")
	}
	if err := client.validateTargetURL(parsedURL); err != nil {
		return "", err
	}

	temporaryFile, err := os.CreateTemp(destinationDirectory, ".orchestra-archive-*")
	if err != nil {
		return "", fmt.Errorf("create archive temporary file: %w", err)
	}
	temporaryPath := temporaryFile.Name()
	fail := func(failure error) (string, error) {
		_ = temporaryFile.Close()
		_ = os.Remove(temporaryPath)
		return "", failure
	}

	request, err := http.NewRequestWithContext(operationContext, http.MethodGet, targetURL, nil)
	if err != nil {
		return fail(fmt.Errorf("build download request: %w", err))
	}
	if acceptHeader != "" {
		request.Header.Set("Accept", acceptHeader)
	}
	request.Header.Set("User-Agent", "orchestra/"+protocol.EffectiveBuildVersion())
	if token != "" && strings.EqualFold(parsedURL.Host, client.apiHostAuthority) {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := client.httpClientWithRedirectPolicy().Do(request)
	if err != nil {
		return fail(errors.New(protocol.RedactText(err.Error())))
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fail(fmt.Errorf("archive download returned HTTP %d", response.StatusCode))
	}

	digest := sha256.New()
	written, err := io.Copy(temporaryFile, io.TeeReader(io.LimitReader(response.Body, maximumBytes+1), digest))
	if err != nil {
		return fail(fmt.Errorf("read archive stream: %w", err))
	}
	if written > maximumBytes {
		return fail(fmt.Errorf("archive download exceeded expected bound of %d bytes", maximumBytes))
	}
	if err := temporaryFile.Sync(); err != nil {
		return fail(fmt.Errorf("sync archive temporary file: %w", err))
	}
	if err := temporaryFile.Close(); err != nil {
		return fail(fmt.Errorf("close archive temporary file: %w", err))
	}

	actualSHA256 := hex.EncodeToString(digest.Sum(nil))
	if !strings.EqualFold(actualSHA256, expectedSHA256) {
		_ = os.Remove(temporaryPath)
		return "", fmt.Errorf("archive checksum mismatch: expected %s, got %s", strings.ToLower(expectedSHA256), actualSHA256)
	}
	return temporaryPath, nil
}

func (client *ReleaseClient) resolveGitHubToken(operationContext context.Context) string {
	if client.explicitToken != "" {
		return client.explicitToken
	}
	fromEnvironment := firstEnvironmentValue(
		client.lookupEnvironment,
		"ORCHESTRA_GITHUB_TOKEN",
		"GITHUB_TOKEN",
		"GH_TOKEN",
	)
	if fromEnvironment != "" {
		return fromEnvironment
	}
	if client.disableGHFallback || !strings.EqualFold(client.apiHost, "api.github.com") {
		return ""
	}
	ghBinary, err := exec.LookPath("gh")
	if err != nil {
		return ""
	}
	probeContext, cancel := context.WithTimeout(operationContext, 3*time.Second)
	defer cancel()
	command := exec.CommandContext(probeContext, ghBinary, "auth", "token")
	output, err := command.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(output))
}

func parseChecksumsFile(contents string) (map[string]string, error) {
	checksums := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(contents))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		digest := strings.ToLower(fields[0])
		fileName := strings.TrimPrefix(fields[len(fields)-1], "*")
		fileName = path.Base(fileName)
		if !checksumHexPattern.MatchString(digest) || fileName == "" || fileName == "." || fileName == "/" {
			continue
		}
		checksums[fileName] = digest
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(checksums) == 0 {
		return nil, errors.New("checksums.txt contained no valid SHA-256 entries")
	}
	return checksums, nil
}

func extractBinaryFromTarGz(archivePath string, binaryName string, destinationDirectory string) (ExtractedBinary, error) {
	archiveFile, err := os.Open(archivePath)
	if err != nil {
		return ExtractedBinary{}, fmt.Errorf("open verified archive: %w", err)
	}
	defer archiveFile.Close()

	gzipReader, err := gzip.NewReader(archiveFile)
	if err != nil {
		return ExtractedBinary{}, fmt.Errorf("decompress gzip archive: %w", err)
	}
	defer gzipReader.Close()

	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return ExtractedBinary{}, fmt.Errorf("read tar entry: %w", err)
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		cleanedName := path.Clean(strings.TrimPrefix(header.Name, "./"))
		if path.Base(cleanedName) != binaryName || strings.Contains(cleanedName, "..") {
			continue
		}
		if header.Size <= 0 || header.Size > MaximumBinaryBytes {
			return ExtractedBinary{}, fmt.Errorf("archive binary %s size %d is out of bounds", binaryName, header.Size)
		}

		stagedFile, err := os.CreateTemp(destinationDirectory, ".orchestra-extracted-*")
		if err != nil {
			return ExtractedBinary{}, fmt.Errorf("create extracted binary file: %w", err)
		}
		stagedPath := stagedFile.Name()
		fail := func(failure error) (ExtractedBinary, error) {
			_ = stagedFile.Close()
			_ = os.Remove(stagedPath)
			return ExtractedBinary{}, failure
		}

		digest := sha256.New()
		written, copyErr := io.Copy(stagedFile, io.TeeReader(io.LimitReader(tarReader, MaximumBinaryBytes+1), digest))
		if copyErr != nil {
			return fail(fmt.Errorf("extract binary %s: %w", binaryName, copyErr))
		}
		if written <= 0 || written > MaximumBinaryBytes {
			return fail(fmt.Errorf("extracted binary %s size %d is out of bounds", binaryName, written))
		}
		if runtime.GOOS != "windows" {
			if chmodErr := stagedFile.Chmod(0o755); chmodErr != nil {
				return fail(fmt.Errorf("mark extracted binary executable: %w", chmodErr))
			}
		}
		if syncErr := stagedFile.Sync(); syncErr != nil {
			return fail(fmt.Errorf("sync extracted binary: %w", syncErr))
		}
		if closeErr := stagedFile.Close(); closeErr != nil {
			return fail(fmt.Errorf("close extracted binary: %w", closeErr))
		}

		return ExtractedBinary{
			Path:      stagedPath,
			SHA256:    hex.EncodeToString(digest.Sum(nil)),
			SizeBytes: written,
		}, nil
	}
	return ExtractedBinary{}, fmt.Errorf("archive did not contain executable %q", binaryName)
}

func isLoopbackHost(hostname string) bool {
	if strings.EqualFold(hostname, "localhost") {
		return true
	}
	parsedIP := net.ParseIP(hostname)
	return parsedIP != nil && parsedIP.IsLoopback()
}

func firstEnvironmentValue(lookup func(string) (string, bool), names ...string) string {
	for _, name := range names {
		if value, found := lookup(name); found && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
