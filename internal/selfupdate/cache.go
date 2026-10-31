package selfupdate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/safepath"
)

type CachedBundle struct {
	Version   string `json:"version"`
	Platform  string `json:"platform"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"sizeBytes"`
	Path      string `json:"-"`
}

type BundleCache struct {
	rootDirectory string
	mutex         sync.Mutex
	keyLocks      map[string]*sync.Mutex
}

func NewBundleCache(rootDirectory string) (*BundleCache, error) {
	if strings.TrimSpace(rootDirectory) == "" {
		return nil, errors.New("bundle cache root directory must not be empty")
	}
	absoluteRoot, err := filepath.Abs(rootDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve bundle cache directory: %w", err)
	}
	if err := os.MkdirAll(absoluteRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create bundle cache directory: %w", err)
	}
	canonicalRoot, err := filepath.EvalSymlinks(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("canonicalize bundle cache directory: %w", err)
	}
	return &BundleCache{
		rootDirectory: canonicalRoot,
		keyLocks:      map[string]*sync.Mutex{},
	}, nil
}

func (cache *BundleCache) lockForKey(key string) *sync.Mutex {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()
	lock, exists := cache.keyLocks[key]
	if !exists {
		lock = &sync.Mutex{}
		cache.keyLocks[key] = lock
	}
	return lock
}

func (cache *BundleCache) EnsurePlatformBundle(
	operationContext context.Context,
	releaseClient *ReleaseClient,
	release ReleaseInfo,
	platform string,
) (CachedBundle, error) {
	if !protocol.IsSafeReleaseVersion(release.Version) {
		return CachedBundle{}, fmt.Errorf("unsafe release version %q", release.Version)
	}
	if !protocol.LabelOrAcceleratorPattern.MatchString(platform) {
		return CachedBundle{}, fmt.Errorf("malformed platform identifier %q", platform)
	}
	archive, supported := release.Platforms[platform]
	if !supported {
		return CachedBundle{}, fmt.Errorf("release v%s does not publish an archive for platform %s", release.Version, platform)
	}

	keyLock := cache.lockForKey(release.Version + "\x00" + platform)
	keyLock.Lock()
	defer keyLock.Unlock()

	if existing, err := cache.verifyCachedBundle(release.Version, platform); err == nil {
		return existing, nil
	}

	platformDirectory, err := safepath.EnsureDirectoryWithinRoot(
		cache.rootDirectory,
		filepath.Join(cache.rootDirectory, release.Version, platform),
		0o700,
	)
	if err != nil {
		return CachedBundle{}, fmt.Errorf("prepare platform cache directory: %w", err)
	}

	stagingDirectory, err := os.MkdirTemp(platformDirectory, ".staging-*")
	if err != nil {
		return CachedBundle{}, fmt.Errorf("create bundle staging directory: %w", err)
	}
	defer os.RemoveAll(stagingDirectory)

	extracted, err := releaseClient.DownloadAndExtractBinary(operationContext, archive, stagingDirectory)
	if err != nil {
		return CachedBundle{}, err
	}

	binaryDestination := filepath.Join(platformDirectory, cachedBinaryFilename(platform))
	temporaryBinary := filepath.Join(platformDirectory, ".bundle-bin-tmp")
	_ = os.Remove(temporaryBinary)
	if err := os.Rename(extracted.Path, temporaryBinary); err != nil {
		return CachedBundle{}, fmt.Errorf("stage extracted binary: %w", err)
	}
	if err := os.Rename(temporaryBinary, binaryDestination); err != nil {
		_ = os.Remove(temporaryBinary)
		return CachedBundle{}, fmt.Errorf("publish cached binary: %w", err)
	}

	bundle := CachedBundle{
		Version:   release.Version,
		Platform:  platform,
		SHA256:    extracted.SHA256,
		SizeBytes: extracted.SizeBytes,
		Path:      binaryDestination,
	}
	encodedMetadata, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return CachedBundle{}, fmt.Errorf("encode bundle metadata: %w", err)
	}
	metadataPath := filepath.Join(platformDirectory, "metadata.json")
	temporaryMetadata := filepath.Join(platformDirectory, ".metadata-tmp")
	if err := os.WriteFile(temporaryMetadata, encodedMetadata, 0o600); err != nil {
		return CachedBundle{}, fmt.Errorf("write bundle metadata: %w", err)
	}
	if err := os.Rename(temporaryMetadata, metadataPath); err != nil {
		_ = os.Remove(temporaryMetadata)
		return CachedBundle{}, fmt.Errorf("publish bundle metadata: %w", err)
	}

	return bundle, nil
}

func (cache *BundleCache) OpenVerifiedBundle(version string, platform string) (*os.File, CachedBundle, error) {
	if !protocol.IsSafeReleaseVersion(version) {
		return nil, CachedBundle{}, errors.New("invalid bundle version")
	}
	if !protocol.LabelOrAcceleratorPattern.MatchString(platform) {
		return nil, CachedBundle{}, errors.New("invalid bundle platform")
	}

	keyLock := cache.lockForKey(version + "\x00" + platform)
	keyLock.Lock()
	defer keyLock.Unlock()

	bundle, err := cache.verifyCachedBundle(version, platform)
	if err != nil {
		return nil, CachedBundle{}, err
	}
	openedFile, err := os.Open(bundle.Path)
	if err != nil {
		return nil, CachedBundle{}, fmt.Errorf("open verified bundle: %w", err)
	}
	return openedFile, bundle, nil
}

var ErrBundleNotCached = errors.New("platform update bundle is not cached")

func (cache *BundleCache) verifyCachedBundle(version string, platform string) (CachedBundle, error) {
	platformDirectory := filepath.Join(cache.rootDirectory, version, platform)
	metadataPath := filepath.Join(platformDirectory, "metadata.json")
	verifiedMetadataPath, _, err := safepath.VerifyRegularFileWithinRoot(cache.rootDirectory, metadataPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return CachedBundle{}, ErrBundleNotCached
		}
		return CachedBundle{}, err
	}
	metadataBytes, err := os.ReadFile(verifiedMetadataPath)
	if err != nil {
		return CachedBundle{}, err
	}
	var metadata CachedBundle
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return CachedBundle{}, err
	}
	if metadata.Version != version || metadata.Platform != platform || !checksumHexPattern.MatchString(metadata.SHA256) || metadata.SizeBytes <= 0 {
		return CachedBundle{}, errors.New("cached bundle metadata is malformed")
	}

	binaryPath := filepath.Join(platformDirectory, cachedBinaryFilename(platform))
	verifiedBinaryPath, info, err := safepath.VerifyRegularFileWithinRoot(cache.rootDirectory, binaryPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return CachedBundle{}, ErrBundleNotCached
		}
		return CachedBundle{}, err
	}
	if info.Size() != metadata.SizeBytes {
		return CachedBundle{}, errors.New("cached bundle size drifted from metadata")
	}
	actualDigest, err := computeFileSHA256(verifiedBinaryPath)
	if err != nil {
		return CachedBundle{}, err
	}
	if !strings.EqualFold(actualDigest, metadata.SHA256) {
		return CachedBundle{}, errors.New("cached bundle digest drifted from metadata")
	}
	metadata.Path = verifiedBinaryPath
	return metadata, nil
}

func cachedBinaryFilename(platform string) string {
	if strings.HasPrefix(platform, "windows-") {
		return "orchestra.exe"
	}
	return "orchestra"
}

func computeFileSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
