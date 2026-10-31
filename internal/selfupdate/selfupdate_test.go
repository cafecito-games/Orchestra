package selfupdate_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/selfupdate"
)

func buildFakeReleaseArchive(t *testing.T, scriptContent string) ([]byte, string) {
	t.Helper()
	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	tarWriter := tar.NewWriter(gzipWriter)

	entries := []struct {
		name    string
		content []byte
		mode    int64
	}{
		{name: "README.md", content: []byte("# Orchestra\n"), mode: 0o644},
		{name: "orchestra", content: []byte(scriptContent), mode: 0o755},
	}
	for _, entry := range entries {
		require.NoError(t, tarWriter.WriteHeader(&tar.Header{
			Name:     entry.name,
			Mode:     entry.mode,
			Size:     int64(len(entry.content)),
			Typeflag: tar.TypeReg,
		}))
		_, err := tarWriter.Write(entry.content)
		require.NoError(t, err)
	}
	require.NoError(t, tarWriter.Close())
	require.NoError(t, gzipWriter.Close())

	archiveBytes := buffer.Bytes()
	digest := sha256.Sum256(archiveBytes)
	return archiveBytes, hex.EncodeToString(digest[:])
}

func fakeVersionScript(version string, protocolVersion string) string {
	return fmt.Sprintf("#!/bin/sh\nif [ \"$1\" = \"version\" ] && [ \"$2\" = \"--json\" ]; then\n  printf '{\"version\":\"%s\",\"protocolVersion\":\"%s\",\"goVersion\":\"go1.26.0\"}\\n'\n  exit 0\nfi\nexit 1\n", version, protocolVersion)
}

func TestReleaseClient_PrivateAuthRedirectStrippingAndChecksumVerification(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script test binary requires unix")
	}

	platform := runtime.GOOS + "-" + runtime.GOARCH
	archiveFilename := fmt.Sprintf("orchestra_0.2.0_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
	archiveBytes, archiveSHA := buildFakeReleaseArchive(t, fakeVersionScript("0.2.0", protocol.ProtocolVersion))
	checksumsText := fmt.Sprintf("%s  %s\n", archiveSHA, archiveFilename)

	var storageSawAuth atomic.Bool
	storageServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "" {
			storageSawAuth.Store(true)
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		switch request.URL.Path {
		case "/blob/checksums.txt":
			_, _ = writer.Write([]byte(checksumsText))
		case "/blob/" + archiveFilename:
			_, _ = writer.Write(archiveBytes)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer storageServer.Close()

	storageParsed, err := url.Parse(storageServer.URL)
	require.NoError(t, err)

	const expectedGitHubToken = "gh-private-release-token"
	var apiServer *httptest.Server
	apiServer = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+expectedGitHubToken {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch request.URL.Path {
		case "/repos/cafecito-games/Orchestra/releases/latest", "/repos/cafecito-games/Orchestra/releases/tags/v0.2.0":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"tag_name":     "v0.2.0",
				"html_url":     "https://github.com/cafecito-games/Orchestra/releases/tag/v0.2.0",
				"published_at": "2026-10-31T12:00:00Z",
				"assets": []map[string]any{
					{
						"id":   101,
						"name": "checksums.txt",
						"url":  apiServer.URL + "/repos/cafecito-games/Orchestra/releases/assets/101",
						"size": len(checksumsText),
					},
					{
						"id":   102,
						"name": archiveFilename,
						"url":  apiServer.URL + "/repos/cafecito-games/Orchestra/releases/assets/102",
						"size": len(archiveBytes),
					},
				},
			})
		case "/repos/cafecito-games/Orchestra/releases/assets/101":
			http.Redirect(writer, request, storageServer.URL+"/blob/checksums.txt", http.StatusFound)
		case "/repos/cafecito-games/Orchestra/releases/assets/102":
			http.Redirect(writer, request, storageServer.URL+"/blob/"+archiveFilename, http.StatusFound)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer apiServer.Close()

	client, err := selfupdate.NewReleaseClient(selfupdate.ReleaseClientOptions{
		APIBaseURL:        apiServer.URL,
		Repository:        "cafecito-games/Orchestra",
		GitHubToken:       expectedGitHubToken,
		AllowedHosts:      []string{storageParsed.Hostname()},
		DisableGHFallback: true,
	})
	require.NoError(t, err)

	release, err := client.ResolveRelease(t.Context(), "latest")
	require.NoError(t, err)
	assert.Equal(t, "0.2.0", release.Version)
	assert.Equal(t, "v0.2.0", release.TagName)
	require.Contains(t, release.Platforms, platform)
	assert.False(t, storageSawAuth.Load(), "Authorization header must be stripped on cross-host redirect")

	cache, err := selfupdate.NewBundleCache(filepath.Join(t.TempDir(), "bundle-cache"))
	require.NoError(t, err)

	bundle, err := cache.EnsurePlatformBundle(t.Context(), client, release, platform)
	require.NoError(t, err)
	assert.Equal(t, "0.2.0", bundle.Version)
	assert.Equal(t, platform, bundle.Platform)
	assert.NotEmpty(t, bundle.SHA256)

	openedFile, verifiedBundle, err := cache.OpenVerifiedBundle("0.2.0", platform)
	require.NoError(t, err)
	_ = openedFile.Close()
	assert.Equal(t, bundle.SHA256, verifiedBundle.SHA256)

	// Tamper with cached file and ensure OpenVerifiedBundle rejects it.
	require.NoError(t, os.WriteFile(verifiedBundle.Path, []byte("corrupted"), 0o755))
	_, _, err = cache.OpenVerifiedBundle("0.2.0", platform)
	require.Error(t, err)
}

func TestUpdateLocalExecutable_CheckUpgradeAndReadOnlyFallback(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell script test binary requires unix")
	}

	platform := runtime.GOOS + "-" + runtime.GOARCH
	archiveFilename := fmt.Sprintf("orchestra_0.3.0_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
	archiveBytes, archiveSHA := buildFakeReleaseArchive(t, fakeVersionScript("0.3.0", protocol.ProtocolVersion))
	checksumsText := fmt.Sprintf("%s  %s\n", archiveSHA, archiveFilename)

	var apiServer *httptest.Server
	apiServer = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/repos/cafecito-games/Orchestra/releases/latest":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"tag_name": "v0.3.0",
				"assets": []map[string]any{
					{
						"id":   1,
						"name": "checksums.txt",
						"url":  apiServer.URL + "/assets/checksums.txt",
						"size": len(checksumsText),
					},
					{
						"id":   2,
						"name": archiveFilename,
						"url":  apiServer.URL + "/assets/" + archiveFilename,
						"size": len(archiveBytes),
					},
				},
			})
		case "/assets/checksums.txt":
			_, _ = writer.Write([]byte(checksumsText))
		case "/assets/" + archiveFilename:
			_, _ = writer.Write(archiveBytes)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer apiServer.Close()

	client, err := selfupdate.NewReleaseClient(selfupdate.ReleaseClientOptions{
		APIBaseURL:        apiServer.URL,
		DisableGHFallback: true,
	})
	require.NoError(t, err)

	installDirectory := filepath.Join(t.TempDir(), "bin")
	require.NoError(t, os.MkdirAll(installDirectory, 0o755))
	targetBinary := filepath.Join(installDirectory, "orchestra")
	require.NoError(t, os.WriteFile(targetBinary, []byte(fakeVersionScript("0.1.0", protocol.ProtocolVersion)), 0o755))

	// 1. Check only does not replace executable.
	checkResult, err := selfupdate.UpdateLocalExecutable(t.Context(), selfupdate.LocalUpdateOptions{
		ReleaseClient:  client,
		ExecutablePath: targetBinary,
		CurrentVersion: "0.1.0",
		Platform:       platform,
		CheckOnly:      true,
	})
	require.NoError(t, err)
	assert.True(t, checkResult.UpdateAvailable)
	assert.False(t, checkResult.Updated)

	probedBefore, err := selfupdate.ProbeBinary(t.Context(), targetBinary, "0.1.0", protocol.ProtocolVersion)
	require.NoError(t, err)
	assert.Equal(t, "0.1.0", probedBefore.Version)

	// 2. Apply update atomically replaces executable.
	updateResult, err := selfupdate.UpdateLocalExecutable(t.Context(), selfupdate.LocalUpdateOptions{
		ReleaseClient:  client,
		ExecutablePath: targetBinary,
		CurrentVersion: "0.1.0",
		Platform:       platform,
	})
	require.NoError(t, err)
	assert.True(t, updateResult.Updated)
	assert.Equal(t, "0.3.0", updateResult.TargetVersion)
	assert.NotEmpty(t, updateResult.SHA256)

	probedAfter, err := selfupdate.ProbeBinary(t.Context(), targetBinary, "0.3.0", protocol.ProtocolVersion)
	require.NoError(t, err)
	assert.Equal(t, "0.3.0", probedAfter.Version)

	// 3. Re-running when already up to date is a no-op without Force.
	noopResult, err := selfupdate.UpdateLocalExecutable(t.Context(), selfupdate.LocalUpdateOptions{
		ReleaseClient:  client,
		ExecutablePath: targetBinary,
		CurrentVersion: "0.3.0",
		Platform:       platform,
	})
	require.NoError(t, err)
	assert.False(t, noopResult.UpdateAvailable)
	assert.False(t, noopResult.Updated)

	// 4. Read-only directory falls back to dataRoot/self/bin/orchestra when fallbackDataRoot is provided.
	if os.Geteuid() != 0 {
		readOnlyDirectory := filepath.Join(t.TempDir(), "readonly-bin")
		require.NoError(t, os.MkdirAll(readOnlyDirectory, 0o755))
		readOnlyTarget := filepath.Join(readOnlyDirectory, "orchestra")
		require.NoError(t, os.WriteFile(readOnlyTarget, []byte(fakeVersionScript("0.1.0", protocol.ProtocolVersion)), 0o755))
		require.NoError(t, os.Chmod(readOnlyDirectory, 0o555))
		defer func() { _ = os.Chmod(readOnlyDirectory, 0o755) }()

		fallbackRoot := filepath.Join(t.TempDir(), "slave-data")
		outcome, installErr := selfupdate.InstallBinary(targetBinary, readOnlyTarget, fallbackRoot)
		require.NoError(t, installErr)
		assert.True(t, outcome.UsedDataRootFallback)
		assert.True(t, strings.HasPrefix(outcome.InstalledPath, fallbackRoot))
	}
}
