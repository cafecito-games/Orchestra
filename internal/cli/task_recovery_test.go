package cli_test

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/cli"
	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

func TestCLITaskUnblockAndReopen(t *testing.T) {
	openedStore, err := store.Open(filepath.Join(t.TempDir(), "state"))
	require.NoError(t, err)
	defer openedStore.Close()

	const bearerToken = "cli-task-recovery-token"
	service, err := orchestrator.NewService(orchestrator.ServiceOptions{
		Store: openedStore,
		Token: bearerToken,
	})
	require.NoError(t, err)
	defer service.Close()

	server := httptest.NewServer(service.Handler())
	defer server.Close()

	caller := orchestrator.CallerIdentity{Kind: "orchestrator-client", ClientID: "coord-cli", SessionID: "sess-cli"}
	thread, _, err := service.CreateThread(caller, orchestrator.CreateThreadInput{Title: "CLI Task Recovery"})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(caller, orchestrator.SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "sub-cli-task",
		Tasks: []orchestrator.SubmittedTaskSpec{
			{ClientTaskID: "blocked-one", Title: "Blocked task", Instructions: "Work"},
			{ClientTaskID: "failed-one", Title: "Failed task", Instructions: "Work"},
		},
	})
	require.NoError(t, err)
	blockedID := submitted.ClientTaskMap["blocked-one"]
	failedID := submitted.ClientTaskMap["failed-one"]

	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		nowText := protocol.NowTimestamp()
		blocked := state.Tasks[blockedID]
		blocked.Status = "blocked"
		blocked.BlockerKind = protocol.TaskBlockerKindNeedsInput
		blocked.BlockerReason = "Waiting for clarification"
		state.Tasks[blockedID] = blocked

		failed := state.Tasks[failedID]
		failed.Status = "failed"
		failed.AttemptCount = 3
		failed.MaxAttempts = 3
		failed.FinishedAt = nowText
		state.Tasks[failedID] = failed
		return nil
	})
	require.NoError(t, err)

	environment := map[string]string{
		"ORCHESTRA_ENDPOINT": server.URL,
		"ORCHESTRA_TOKEN":    bearerToken,
	}
	lookupEnv := func(key string) (string, bool) {
		val, ok := environment[key]
		return val, ok
	}
	runCLI := func(args ...string) (int, string, string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		code := cli.Run(t.Context(), args, nil, &stdout, &stderr, lookupEnv)
		return code, stdout.String(), stderr.String()
	}

	code, stdout, stderr := runCLI("task", "--help")
	assert.Equal(t, 0, code, stderr)
	assert.Contains(t, stdout, "orchestra task unblock")
	assert.Contains(t, stdout, "orchestra task reopen")

	code, stdout, stderr = runCLI("task", "unblock", blockedID, "--note", "Use v2", "--idempotency-key", "cli-unblock-1", "--json")
	assert.Equal(t, 0, code, stderr)
	var unblockResult orchestrator.UnblockTaskResult
	require.NoError(t, json.Unmarshal([]byte(stdout), &unblockResult))
	assert.Equal(t, blockedID, unblockResult.Task.ID)
	assert.Equal(t, "ready", unblockResult.Task.Status)
	assert.False(t, unblockResult.Replayed)

	code, stdout, stderr = runCLI("task", "reopen", failedID, "--note", "Retry now", "--attempts", "2", "--idempotency-key", "cli-reopen-1", "--json")
	assert.Equal(t, 0, code, stderr)
	var reopenResult orchestrator.ReopenTaskResult
	require.NoError(t, json.Unmarshal([]byte(stdout), &reopenResult))
	assert.Equal(t, failedID, reopenResult.Task.ID)
	assert.Equal(t, "ready", reopenResult.Task.Status)
	assert.Equal(t, 5, reopenResult.Task.MaxAttempts)
	assert.False(t, reopenResult.Replayed)
}
