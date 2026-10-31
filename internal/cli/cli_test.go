package cli_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/cli"
	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/selfupdate"
	"github.com/cafecito-games/Orchestra/internal/slave"
	"github.com/cafecito-games/Orchestra/internal/store"
	"github.com/cafecito-games/Orchestra/internal/workspace"
)

func TestCLIVersionAndHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := cli.Run(t.Context(), []string{"version"}, nil, &stdout, &stderr, nil)
	assert.Equal(t, 0, exitCode)
	assert.Contains(t, stdout.String(), "orchestra ")
	assert.Contains(t, stdout.String(), "protocol v1")

	stdout.Reset()
	stderr.Reset()
	exitCode = cli.Run(t.Context(), []string{"version", "--json"}, nil, &stdout, &stderr, nil)
	assert.Equal(t, 0, exitCode)
	var versionPayload map[string]string
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &versionPayload))
	assert.Equal(t, protocol.EffectiveBuildVersion(), versionPayload["version"])
	assert.Equal(t, protocol.ProtocolVersion, versionPayload["protocolVersion"])
	assert.NotEmpty(t, versionPayload["goVersion"])

	stdout.Reset()
	stderr.Reset()
	exitCode = cli.Run(t.Context(), []string{"--help"}, nil, &stdout, &stderr, nil)
	assert.Equal(t, 0, exitCode)
	assert.Contains(t, stdout.String(), "Runtime Modes:")

	stdout.Reset()
	stderr.Reset()
	exitCode = cli.Run(t.Context(), []string{"slave", "--help"}, nil, &stdout, &stderr, nil)
	assert.Equal(t, 0, exitCode)
	assert.Contains(t, stdout.String(), "--require-acp")

	stdout.Reset()
	stderr.Reset()
	exitCode = cli.Run(t.Context(), []string{"slave", "--workspace-root", t.TempDir(), "--require-acp", "--acp-native-fallback", "claude-cli"}, nil, &stdout, &stderr, nil)
	assert.Equal(t, 2, exitCode)
	assert.Contains(t, stderr.String(), "--require-acp cannot be combined with --acp-native-fallback")

	stdout.Reset()
	stderr.Reset()
	exitCode = cli.Run(t.Context(), []string{"nonexistent"}, nil, &stdout, &stderr, nil)
	assert.Equal(t, 2, exitCode)
	assert.Contains(t, stderr.String(), "unknown subcommand")
}

func TestCLIOperatorCommandsAndSetupDoctor(t *testing.T) {
	const bearerToken = "cli-operator-secret"

	openedStore, err := store.Open(filepath.Join(t.TempDir(), "master-state"))
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := orchestrator.NewService(orchestrator.ServiceOptions{
		Store: openedStore,
		Token: bearerToken,
	})
	require.NoError(t, err)
	defer service.Close()

	server := httptest.NewServer(service.Handler())
	defer server.Close()

	canonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{t.TempDir()})
	require.NoError(t, err)
	instanceCapacity := 4
	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		state.Nodes["worker-alpha"] = protocol.ComputeNode{
			ID:               "worker-alpha",
			Name:             "Worker Alpha",
			Kind:             "home-server",
			Platform:         "linux-amd64",
			Status:           "offline",
			LastSeen:         protocol.NowTimestamp(),
			Concurrency:      2,
			InstanceCapacity: &instanceCapacity,
			WorkspaceRoots:   canonicalRoots,
			Version:          "0.1.0",
			Harnesses: []protocol.HarnessProfile{{
				ID:         "claude-cli",
				Label:      "Claude Code",
				Available:  true,
				AuthMode:   "local-subscription",
				Models:     []string{"sonnet"},
				Transports: []string{protocol.TransportNativeCLI, protocol.TransportACP},
			}},
		}
		return nil
	})
	require.NoError(t, err)

	caller := orchestrator.CallerIdentity{Kind: "orchestrator-client", ClientID: "test-agent", SessionID: "sess-1"}
	createdThread, _, err := service.CreateThread(caller, orchestrator.CreateThreadInput{
		Title:   "CLI Inspection Thread",
		Summary: "Testing operator CLI subcommands",
		Attach:  true,
	})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(caller, orchestrator.SubmitTasksInput{
		ThreadID:       createdThread.ID,
		IdempotencyKey: "submit-cli-1",
		Tasks: []orchestrator.SubmittedTaskSpec{
			{
				ClientTaskID: "first",
				Title:        "Build module",
				Instructions: "Compile packages",
				Requirements: protocol.ExecutionRequirements{HarnessIDs: []string{"claude-cli"}},
			},
			{
				ClientTaskID: "second",
				Title:        "Verify tests",
				Instructions: "Execute test suite",
				Dependencies: []orchestrator.SubmittedDependency{{ClientTaskID: "first"}},
				Requirements: protocol.ExecutionRequirements{HarnessIDs: []string{"claude-cli"}},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, submitted.Tasks, 2)

	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		nowText := protocol.NowTimestamp()
		expiresAt := time.Now().UTC().Add(15 * time.Minute).Format(time.RFC3339Nano)
		state.Approvals["approval-test-1"] = protocol.ApprovalRequest{
			ID:            "approval-test-1",
			ThreadID:      createdThread.ID,
			TaskID:        submitted.Tasks[0].ID,
			RunID:         "run-test-1",
			InstanceID:    "instance-test-1",
			AllocationID:  "allocation-test-1",
			NodeID:        "worker-alpha",
			Status:        "pending",
			Title:         "Execute integration check",
			Options:       []protocol.ApprovalOption{{ID: "allow_once", Label: "Allow once", Kind: "allow-once"}, {ID: "reject_once", Label: "Reject", Kind: "reject-once"}},
			DeliveryState: "queued",
			RequestedAt:   nowText,
			ExpiresAt:     expiresAt,
			UpdatedAt:     nowText,
		}
		state.Approvals["approval-test-2"] = protocol.ApprovalRequest{
			ID:            "approval-test-2",
			ThreadID:      createdThread.ID,
			TaskID:        submitted.Tasks[0].ID,
			RunID:         "run-test-1",
			InstanceID:    "instance-test-1",
			AllocationID:  "allocation-test-1",
			NodeID:        "worker-alpha",
			Status:        "pending",
			Title:         "Modify configuration file",
			Options:       []protocol.ApprovalOption{{ID: "allow_once", Label: "Allow once", Kind: "allow-once"}, {ID: "reject_once", Label: "Reject", Kind: "reject-once"}},
			DeliveryState: "queued",
			RequestedAt:   nowText,
			ExpiresAt:     expiresAt,
			UpdatedAt:     nowText,
		}
		return nil
	})
	require.NoError(t, err)

	environment := map[string]string{
		"ORCHESTRA_ENDPOINT":  server.URL,
		"ORCHESTRA_TOKEN":     bearerToken,
		"ORCHESTRA_DATA_ROOT": filepath.Join(t.TempDir(), "setup-data"),
	}
	lookupEnvironment := func(name string) (string, bool) {
		value, found := environment[name]
		return value, found
	}

	runCLI := func(arguments ...string) (int, string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		code := cli.Run(t.Context(), arguments, nil, &stdout, &stderr, lookupEnvironment)
		return code, stdout.String(), stderr.String()
	}

	code, stdout, stderr := runCLI("status")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "Orchestra Master")
	assert.Contains(t, stdout, "worker-alpha")

	code, stdout, stderr = runCLI("nodes")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "worker-alpha (Worker Alpha)")
	assert.Contains(t, stdout, "Harness claude-cli")

	code, stdout, stderr = runCLI("threads")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, createdThread.ID)
	assert.Contains(t, stdout, "CLI Inspection Thread")

	code, stdout, stderr = runCLI("thread", createdThread.ID)
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "Build module")
	assert.Contains(t, stdout, "Verify tests")
	assert.Contains(t, stdout, "approval-test-1")

	code, stdout, stderr = runCLI("thread", createdThread.ID, "--json")
	assert.Equal(t, 0, code, stderr)
	var detail protocol.ThreadDetailResponse
	require.NoError(t, json.Unmarshal([]byte(stdout), &detail))
	assert.Equal(t, createdThread.ID, detail.Thread.ID)
	assert.Len(t, detail.Tasks, 2)

	code, stdout, stderr = runCLI("approvals")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "approval-test-1")
	assert.Contains(t, stdout, "approval-test-2")

	code, stdout, stderr = runCLI("approve", "approval-test-1", "--option", "allow_once")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "Approval approval-test-1 approved")

	code, stdout, stderr = runCLI("reject", "--option", "reject_once", "approval-test-2")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "Approval approval-test-2 rejected")

	code, stdout, stderr = runCLI("cancel", submitted.Tasks[0].ID)
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "Cancelled task "+submitted.Tasks[0].ID)

	code, stdout, stderr = runCLI(
		"room", "post", createdThread.ID, "Switch integration target to staging",
		"--kind", "guidance",
		"--topic", "staging-target",
		"--nudge",
		"--idempotency-key", "cli-room-post-1",
	)
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "Posted room message #1")

	code, stdout, stderr = runCLI("room", "read", createdThread.ID, "--kind", "guidance")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "staging-target")
	assert.Contains(t, stdout, "Switch integration target to staging")

	code, stdout, stderr = runCLI("room", "nudge", createdThread.ID, "--task", submitted.Tasks[1].ID, "--idempotency-key", "cli-room-nudge-1")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "Nudged 1 task(s)")

	code, stdout, stderr = runCLI("room", "path", createdThread.ID)
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, strings.TrimSpace(stdout), "messages.jsonl")

	_, err = service.RegisterClientMailbox("claude-cli-agent", "sess-cli-mail", &protocol.ClientMailRegistration{
		RequestedName: "claude1234",
		Host:          "fabrica",
		Harness:       "claude-code",
		Workspace:     canonicalRoots[0],
		PushCapable:   true,
	})
	require.NoError(t, err)

	code, stdout, stderr = runCLI("mail", "--help")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "orchestra mail")

	code, stdout, stderr = runCLI("mail", "agents")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "claude1234@fabrica")

	code, stdout, stderr = runCLI(
		"mail", "send", "claude1234@fabrica", "Run full integration suite on main",
		"--subject", "Nightly verification",
		"--from", "operator@fabrica",
		"--idempotency-key", "cli-mail-send-1",
	)
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "Sent mail")
	assert.Contains(t, stdout, "claude1234@fabrica")

	code, stdout, stderr = runCLI("mail", "inbox", "claude1234@fabrica", "--unread")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "Nightly verification")
	assert.Contains(t, stdout, "Run full integration suite on main")

	code, stdout, stderr = runCLI("mail", "inbox", "claude1234@fabrica", "--unread", "--mark-read", "--json")
	assert.Equal(t, 0, code, stderr)
	var inboxPayload orchestrator.ReadMailResult
	require.NoError(t, json.Unmarshal([]byte(stdout), &inboxPayload))
	require.Len(t, inboxPayload.Messages, 1)
	assert.True(t, inboxPayload.Messages[0].Read)
	assert.Equal(t, 0, inboxPayload.Mailbox.UnreadCount)

	code, stdout, stderr = runCLI("mail", "rename", "claude1234@fabrica", "release-captain")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "release-captain@fabrica")

	code, stdout, stderr = runCLI("mail", "agents", "--json")
	assert.Equal(t, 0, code, stderr)
	var agentsPayload orchestrator.ListMailboxesResult
	require.NoError(t, json.Unmarshal([]byte(stdout), &agentsPayload))
	foundRenamed := slices.ContainsFunc(agentsPayload.Mailboxes, func(candidate protocol.AgentMailbox) bool {
		return candidate.Address == "release-captain@fabrica" && slices.Contains(candidate.Aliases, "claude1234@fabrica")
	})
	assert.True(t, foundRenamed)

	code, stdout, stderr = runCLI("setup", "plan")
	assert.Equal(t, 0, code, stderr)
	var planPayload map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &planPayload))
	assert.Equal(t, environment["ORCHESTRA_DATA_ROOT"], planPayload["dataRoot"])

	code, stdout, stderr = runCLI("doctor")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "activation ledger:")
	assert.Contains(t, stdout, "reachable")

	code, stdout, stderr = runCLI("doctor", "--json")
	assert.Equal(t, 0, code, stderr)
	var doctorPayload map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &doctorPayload))
	connectivity, _ := doctorPayload["hubConnectivity"].(map[string]any)
	assert.Equal(t, true, connectivity["reachable"])
}

func TestCLISetupApplyAndRollback(t *testing.T) {
	dataRoot := filepath.Join(t.TempDir(), "setup-root")
	lookupEnvironment := func(name string) (string, bool) {
		if name == "ORCHESTRA_DATA_ROOT" {
			return dataRoot, true
		}
		return "", false
	}

	runCLI := func(arguments ...string) (int, string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		code := cli.Run(t.Context(), arguments, nil, &stdout, &stderr, lookupEnvironment)
		return code, stdout.String(), stderr.String()
	}

	// Setup apply --help
	code, _, stderr := runCLI("setup", "apply", "--help")
	assert.Equal(t, 0, code)
	assert.Contains(t, stderr, "orchestra setup apply")

	// Setup apply without --plan
	code, _, stderr = runCLI("setup", "apply")
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr, "--plan is required")

	// Setup apply with non-existent --plan
	code, _, stderr = runCLI("setup", "apply", "--plan", filepath.Join(t.TempDir(), "nonexistent.json"))
	assert.Equal(t, 1, code)
	assert.Contains(t, stderr, "read plan")

	// Setup apply with valid custom manifest and artifact
	sourceBinary := filepath.Join(t.TempDir(), "claude-agent-acp")
	binaryContent := []byte("#!/bin/sh\necho 0.79.0\n")
	require.NoError(t, os.WriteFile(sourceBinary, binaryContent, 0o755))
	checksum := hex.EncodeToString(func() []byte {
		h := sha256.Sum256(binaryContent)
		return h[:]
	}())

	platformKey := runtime.GOOS + "-" + runtime.GOARCH
	manifestJSON := fmt.Sprintf(`{
		"manifestVersion": "2",
		"components": [{
			"id": "claude-acp",
			"kind": "acp-adapter",
			"harnessId": "claude-cli",
			"provider": "anthropic",
			"label": "Claude ACP adapter",
			"version": "0.79.0",
			"platforms": {
				"%s": { "kind": "manual", "executablePath": "bin/claude-agent-acp" }
			},
			"launch": {},
			"authDocsUrl": "https://example.com"
		}]
	}`, platformKey)

	manifestFile := filepath.Join(t.TempDir(), "manifest.json")
	require.NoError(t, os.WriteFile(manifestFile, []byte(manifestJSON), 0o644))

	planFile := filepath.Join(t.TempDir(), "plan.json")
	code, _, stderr = runCLI("setup", "plan", "--manifest", manifestFile, "--out", planFile)
	assert.Equal(t, 0, code, stderr)

	code, stdout, stderr := runCLI(
		"setup", "apply",
		"--manifest", manifestFile,
		"--plan", planFile,
		"--manual-artifact", "claude-acp="+sourceBinary,
		"--manual-checksum", "claude-acp="+checksum,
	)
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "installed: acp-adapter/claude-acp")

	// Setup rollback --help
	code, _, stderr = runCLI("setup", "rollback", "--help")
	assert.Equal(t, 0, code)
	assert.Contains(t, stderr, "orchestra setup rollback")

	// Setup rollback with applied component having no previous version
	code, _, stderr = runCLI("setup", "rollback", "--manifest", manifestFile, "--component", "claude-acp")
	assert.NotEqual(t, 0, code)
	assert.Contains(t, stderr, "orchestra setup rollback")

	// Setup rollback with unknown component
	code, _, stderr = runCLI("setup", "rollback", "--component", "unknown-component")
	assert.NotEqual(t, 0, code)
	assert.Contains(t, stderr, "orchestra setup rollback")
}

func TestCLISlaveValidation(t *testing.T) {
	runCLI := func(arguments ...string) (int, string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		code := cli.Run(t.Context(), arguments, nil, &stdout, &stderr, nil)
		return code, stdout.String(), stderr.String()
	}

	// Invalid node kind
	code, _, stderr := runCLI("slave", "--kind", "unsupported-kind")
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr, "invalid node kind")

	// Invalid concurrency
	code, _, stderr = runCLI("slave", "--concurrency", "0")
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr, "concurrency must be between 1 and 65535")

	// Invalid instance capacity
	code, _, stderr = runCLI("slave", "--instance-capacity", "-1")
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr, "instance-capacity must be between 0 and 65535")

	// Invalid toolchain format (missing =)
	code, _, stderr = runCLI("slave", "--toolchain", "invalid-format")
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr, "toolchain")

	// Conflicting require-acp and fallback
	code, _, stderr = runCLI("slave", "--require-acp", "--acp-native-fallback", "claude-cli")
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr, "--require-acp cannot be combined with --acp-native-fallback")

	// Unexpected positional arguments
	code, _, stderr = runCLI("slave", "extra-arg")
	assert.Equal(t, 2, code)
	assert.Contains(t, stderr, "unexpected positional arguments")
}

func TestCLIOperatorErrors_UnauthorizedAndUnreachable(t *testing.T) {
	const validToken = "valid-token-secret"

	openedStore, err := store.Open(filepath.Join(t.TempDir(), "operator-error-state"))
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := orchestrator.NewService(orchestrator.ServiceOptions{
		Store: openedStore,
		Token: validToken,
	})
	require.NoError(t, err)
	defer service.Close()

	server := httptest.NewServer(service.Handler())
	defer server.Close()

	// 1. Unauthorized: wrong token
	unauthorizedEnv := func(name string) (string, bool) {
		switch name {
		case "ORCHESTRA_ENDPOINT":
			return server.URL, true
		case "ORCHESTRA_TOKEN":
			return "wrong-token", true
		default:
			return "", false
		}
	}

	runUnauthorized := func(arguments ...string) (int, string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		code := cli.Run(t.Context(), arguments, nil, &stdout, &stderr, unauthorizedEnv)
		return code, stdout.String(), stderr.String()
	}

	for _, command := range [][]string{
		{"threads"},
		{"mail", "agents"},
		{"room", "read", "thread-123"},
		{"approvals"},
	} {
		code, _, stderr := runUnauthorized(command...)
		assert.Equal(t, 1, code, "command %v should fail with 1", command)
		assert.Contains(t, stderr, "unauthorized", "command %v should indicate unauthorized", command)
	}

	// 2. Unreachable master: no listener
	unreachableEnv := func(name string) (string, bool) {
		switch name {
		case "ORCHESTRA_ENDPOINT":
			return "http://127.0.0.1:59999", true
		case "ORCHESTRA_TOKEN":
			return validToken, true
		default:
			return "", false
		}
	}

	runUnreachable := func(arguments ...string) (int, string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		code := cli.Run(t.Context(), arguments, nil, &stdout, &stderr, unreachableEnv)
		return code, stdout.String(), stderr.String()
	}

	for _, command := range [][]string{
		{"threads"},
		{"mail", "agents"},
		{"room", "read", "thread-123"},
		{"approvals"},
	} {
		code, _, stderr := runUnreachable(command...)
		assert.Equal(t, 1, code, "command %v should fail with 1", command)
		assert.NotEmpty(t, stderr, "command %v should output error message", command)
	}
}

func TestCLIMasterStartAndShutdown(t *testing.T) {
	masterContext, cancelMaster := context.WithCancel(t.Context())
	defer cancelMaster()
	var stdout, stderr bytes.Buffer

	doneChannel := make(chan int, 1)
	go func() {
		doneChannel <- cli.Run(
			masterContext,
			[]string{
				"master",
				"--listen", "127.0.0.1:0",
				"--token", "master-test-token",
				"--data-dir", filepath.Join(t.TempDir(), "master"),
			},
			nil,
			&stdout,
			&stderr,
			func(string) (string, bool) { return "", false },
		)
	}()

	time.Sleep(150 * time.Millisecond)
	cancelMaster()

	select {
	case exitCode := <-doneChannel:
		assert.Equal(t, 0, exitCode, stderr.String())
		assert.Contains(t, stderr.String(), "orchestra master")
	case <-time.After(5 * time.Second):
		require.Fail(t, "timed out waiting for orchestra master to shut down")
	}
}

func TestCLIUpdateAndNodesUpdate(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX shell script fixture")
	}

	platform := runtime.GOOS + "-" + runtime.GOARCH
	archiveName := fmt.Sprintf("orchestra_9.9.9_%s_%s.tar.gz", runtime.GOOS, runtime.GOARCH)
	newScript := fmt.Appendf(nil, "#!/bin/sh\nif [ \"$1\" = \"version\" ]; then\n  echo '{\"version\":\"9.9.9\",\"protocolVersion\":%q}'\n  exit 0\nfi\nexit 0\n", protocol.ProtocolVersion)

	var tarBuffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&tarBuffer)
	tarWriter := tar.NewWriter(gzipWriter)
	require.NoError(t, tarWriter.WriteHeader(&tar.Header{
		Name:     "orchestra",
		Mode:     0o755,
		Size:     int64(len(newScript)),
		Typeflag: tar.TypeReg,
	}))
	_, err := tarWriter.Write(newScript)
	require.NoError(t, err)
	require.NoError(t, tarWriter.Close())
	require.NoError(t, gzipWriter.Close())

	archiveBytes := tarBuffer.Bytes()
	archiveDigest := sha256.Sum256(archiveBytes)
	checksumContent := fmt.Sprintf("%s  %s\n", hex.EncodeToString(archiveDigest[:]), archiveName)

	var releaseServer *httptest.Server
	releaseMux := http.NewServeMux()
	writeRelease := func(writer http.ResponseWriter) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"tag_name":     "v9.9.9",
			"published_at": "2026-10-31T12:00:00Z",
			"assets": []map[string]any{
				{
					"name":                 "checksums.txt",
					"url":                  releaseServer.URL + "/assets/checksums.txt",
					"browser_download_url": releaseServer.URL + "/assets/checksums.txt",
					"size":                 len(checksumContent),
				},
				{
					"name":                 archiveName,
					"url":                  releaseServer.URL + "/assets/" + archiveName,
					"browser_download_url": releaseServer.URL + "/assets/" + archiveName,
					"size":                 len(archiveBytes),
				},
			},
		})
	}
	releaseMux.HandleFunc("/repos/cafecito-games/Orchestra/releases/latest", func(writer http.ResponseWriter, request *http.Request) {
		writeRelease(writer)
	})
	releaseMux.HandleFunc("/repos/cafecito-games/Orchestra/releases/tags/v9.9.9", func(writer http.ResponseWriter, request *http.Request) {
		writeRelease(writer)
	})
	releaseMux.HandleFunc("/assets/checksums.txt", func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(checksumContent))
	})
	releaseMux.HandleFunc("/assets/"+archiveName, func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write(archiveBytes)
	})
	releaseServer = httptest.NewServer(releaseMux)
	defer releaseServer.Close()

	releaseClient, err := selfupdate.NewReleaseClient(selfupdate.ReleaseClientOptions{
		APIBaseURL:        releaseServer.URL,
		Repository:        "cafecito-games/Orchestra",
		DisableGHFallback: true,
	})
	require.NoError(t, err)

	openedStore, err := store.Open(filepath.Join(t.TempDir(), "master-state"))
	require.NoError(t, err)
	defer openedStore.Close()

	const bearerToken = "cli-update-token"
	service, err := orchestrator.NewService(orchestrator.ServiceOptions{
		Store:         openedStore,
		Token:         bearerToken,
		ReleaseClient: releaseClient,
	})
	require.NoError(t, err)
	defer service.Close()

	masterServer := httptest.NewServer(service.Handler())
	defer masterServer.Close()

	canonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{t.TempDir()})
	require.NoError(t, err)
	workerBinaryPath := filepath.Join(t.TempDir(), "worker-orchestra")
	require.NoError(t, os.WriteFile(workerBinaryPath, []byte("#!/bin/sh\nexit 0\n"), 0o755))

	var reconnectOnce sync.Once
	replacementContext, cancelReplacement := context.WithCancel(t.Context())
	defer cancelReplacement()

	newWorker := func(version string, onExec func(string, []string, []string) error) *slave.Client {
		return slave.NewClient(slave.ClientOptions{
			Endpoint:       masterServer.URL,
			Token:          bearerToken,
			ExecutablePath: workerBinaryPath,
			Node: protocol.ComputeNode{
				ID:             "worker-cli-upd",
				Name:           "Worker CLI Update",
				Kind:           "local",
				Platform:       platform,
				Status:         "online",
				LastSeen:       protocol.NowTimestamp(),
				Concurrency:    1,
				WorkspaceRoots: canonicalRoots,
				Version:        version,
			},
			Runner:           harness.NewRunner(nil),
			WorkspaceManager: workspace.NewManager(canonicalRoots, nil),
			ExecReplacement:  onExec,
		})
	}

	workerV1 := newWorker("0.1.0", func(string, []string, []string) error {
		reconnectOnce.Do(func() {
			workerV2 := newWorker("9.9.9", nil)
			go func() {
				_ = workerV2.Run(replacementContext)
			}()
		})
		return nil
	})
	go func() {
		_ = workerV1.Run(t.Context())
	}()

	require.Eventually(t, func() bool {
		details, listErr := service.ListNodeDetails()
		return listErr == nil && len(details) == 1 && details[0].Synced
	}, 5*time.Second, 25*time.Millisecond)

	environment := map[string]string{
		"ORCHESTRA_ENDPOINT":        masterServer.URL,
		"ORCHESTRA_TOKEN":           bearerToken,
		"ORCHESTRA_RELEASE_API_URL": releaseServer.URL,
		"ORCHESTRA_RELEASE_REPO":    "cafecito-games/Orchestra",
	}
	lookupEnvironment := func(name string) (string, bool) {
		value, exists := environment[name]
		return value, exists
	}
	runCLI := func(arguments ...string) (int, string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		exitCode := cli.Run(t.Context(), arguments, nil, &stdout, &stderr, lookupEnvironment)
		return exitCode, stdout.String(), stderr.String()
	}

	code, stdout, stderr := runCLI("update", "--help")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "orchestra update")

	code, stdout, stderr = runCLI("nodes", "update", "--help")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "orchestra nodes update")

	localExecutable := filepath.Join(t.TempDir(), "local-orchestra")
	require.NoError(t, os.WriteFile(localExecutable, []byte("#!/bin/sh\nexit 0\n"), 0o755))

	code, stdout, stderr = runCLI("update", "--check", "--executable", localExecutable, "--json")
	assert.Equal(t, 0, code, stderr)
	var checkResult selfupdate.LocalUpdateResult
	require.NoError(t, json.Unmarshal([]byte(stdout), &checkResult))
	assert.Equal(t, "9.9.9", checkResult.TargetVersion)
	assert.True(t, checkResult.UpdateAvailable)
	assert.False(t, checkResult.Updated)

	code, stdout, stderr = runCLI("update", "--version", "9.9.9", "--executable", localExecutable, "--json")
	assert.Equal(t, 0, code, stderr)
	var updateResult selfupdate.LocalUpdateResult
	require.NoError(t, json.Unmarshal([]byte(stdout), &updateResult))
	assert.True(t, updateResult.Updated)
	assert.Equal(t, "9.9.9", updateResult.TargetVersion)

	installedBytes, err := os.ReadFile(localExecutable)
	require.NoError(t, err)
	assert.Equal(t, newScript, installedBytes)

	code, stdout, stderr = runCLI("nodes", "update", "worker-cli-upd", "--version", "9.9.9", "--wait", "--json")
	assert.Equal(t, 0, code, stderr)
	var nodesResponse orchestrator.UpdateNodesResponse
	require.NoError(t, json.Unmarshal([]byte(stdout), &nodesResponse))
	require.Len(t, nodesResponse.Nodes, 1)
	assert.Equal(t, "succeeded", nodesResponse.Nodes[0].Status)
	assert.Equal(t, "0.1.0", nodesResponse.Nodes[0].PreviousVersion)
	assert.Equal(t, "9.9.9", nodesResponse.Nodes[0].CurrentVersion)
}
