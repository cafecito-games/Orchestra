package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/cli"
	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/slave"
	"github.com/cafecito-games/Orchestra/internal/store"
)

func TestCLIVersionAndHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := cli.Run(context.Background(), []string{"version"}, nil, &stdout, &stderr, nil)
	require.Equal(t, 0, exitCode)
	require.Contains(t, stdout.String(), "orchestra ")
	require.Contains(t, stdout.String(), "protocol v1")

	stdout.Reset()
	stderr.Reset()
	exitCode = cli.Run(context.Background(), []string{"version", "--json"}, nil, &stdout, &stderr, nil)
	require.Equal(t, 0, exitCode)
	var versionPayload map[string]string
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &versionPayload))
	require.Equal(t, protocol.ProtocolVersion, versionPayload["protocolVersion"])

	stdout.Reset()
	stderr.Reset()
	exitCode = cli.Run(context.Background(), []string{"--help"}, nil, &stdout, &stderr, nil)
	require.Equal(t, 0, exitCode)
	require.Contains(t, stdout.String(), "Runtime Modes:")

	stdout.Reset()
	stderr.Reset()
	exitCode = cli.Run(context.Background(), []string{"slave", "--help"}, nil, &stdout, &stderr, nil)
	require.Equal(t, 0, exitCode)
	require.Contains(t, stdout.String(), "--require-acp")

	stdout.Reset()
	stderr.Reset()
	exitCode = cli.Run(context.Background(), []string{"slave", "--workspace-root", t.TempDir(), "--require-acp", "--acp-native-fallback", "claude-cli"}, nil, &stdout, &stderr, nil)
	require.Equal(t, 2, exitCode)
	require.Contains(t, stderr.String(), "--require-acp cannot be combined with --acp-native-fallback")

	stdout.Reset()
	stderr.Reset()
	exitCode = cli.Run(context.Background(), []string{"nonexistent"}, nil, &stdout, &stderr, nil)
	require.Equal(t, 2, exitCode)
	require.Contains(t, stderr.String(), "unknown subcommand")
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
		var stdout, stderr bytes.Buffer
		code := cli.Run(context.Background(), arguments, nil, &stdout, &stderr, lookupEnvironment)
		return code, stdout.String(), stderr.String()
	}

	code, stdout, stderr := runCLI("status")
	require.Equal(t, 0, code, stderr)
	require.Contains(t, stdout, "Orchestra Master")
	require.Contains(t, stdout, "worker-alpha")

	code, stdout, stderr = runCLI("nodes")
	require.Equal(t, 0, code, stderr)
	require.Contains(t, stdout, "worker-alpha (Worker Alpha)")
	require.Contains(t, stdout, "Harness claude-cli")

	code, stdout, stderr = runCLI("threads")
	require.Equal(t, 0, code, stderr)
	require.Contains(t, stdout, createdThread.ID)
	require.Contains(t, stdout, "CLI Inspection Thread")

	code, stdout, stderr = runCLI("thread", createdThread.ID)
	require.Equal(t, 0, code, stderr)
	require.Contains(t, stdout, "Build module")
	require.Contains(t, stdout, "Verify tests")
	require.Contains(t, stdout, "approval-test-1")

	code, stdout, stderr = runCLI("thread", createdThread.ID, "--json")
	require.Equal(t, 0, code, stderr)
	var detail protocol.ThreadDetailResponse
	require.NoError(t, json.Unmarshal([]byte(stdout), &detail))
	require.Equal(t, createdThread.ID, detail.Thread.ID)
	require.Len(t, detail.Tasks, 2)

	code, stdout, stderr = runCLI("approvals")
	require.Equal(t, 0, code, stderr)
	require.Contains(t, stdout, "approval-test-1")
	require.Contains(t, stdout, "approval-test-2")

	code, stdout, stderr = runCLI("approve", "approval-test-1", "--option", "allow_once")
	require.Equal(t, 0, code, stderr)
	require.Contains(t, stdout, "Approval approval-test-1 approved")

	code, stdout, stderr = runCLI("reject", "--option", "reject_once", "approval-test-2")
	require.Equal(t, 0, code, stderr)
	require.Contains(t, stdout, "Approval approval-test-2 rejected")

	code, stdout, stderr = runCLI("cancel", submitted.Tasks[0].ID)
	require.Equal(t, 0, code, stderr)
	require.Contains(t, stdout, "Cancelled task "+submitted.Tasks[0].ID)

	code, stdout, stderr = runCLI("setup", "plan")
	require.Equal(t, 0, code, stderr)
	var planPayload map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &planPayload))
	require.Equal(t, environment["ORCHESTRA_DATA_ROOT"], planPayload["dataRoot"])

	code, stdout, stderr = runCLI("doctor")
	require.Equal(t, 0, code, stderr)
	require.Contains(t, stdout, "activation ledger:")
	require.Contains(t, stdout, "reachable")

	code, stdout, stderr = runCLI("doctor", "--json")
	require.Equal(t, 0, code, stderr)
	var doctorPayload map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &doctorPayload))
	connectivity, _ := doctorPayload["hubConnectivity"].(map[string]any)
	require.Equal(t, true, connectivity["reachable"])
}

func TestCLIMasterStartAndShutdown(t *testing.T) {
	masterContext, cancelMaster := context.WithCancel(context.Background())
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
		require.Equal(t, 0, exitCode, stderr.String())
		require.Contains(t, stderr.String(), "orchestra master")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for orchestra master to shut down")
	}
}
