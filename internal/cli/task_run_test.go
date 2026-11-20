package cli

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

func TestTaskRunHelperProcess(t *testing.T) {
	if os.Getenv("ORCHESTRA_TASK_RUN_HELPER") != "1" {
		return
	}
	mode := os.Getenv("ORCHESTRA_TASK_RUN_HELPER_MODE")
	threadID := os.Getenv("ORCHESTRA_THREAD_ID")
	taskID := os.Getenv("ORCHESTRA_TASK_ID")
	token := os.Getenv("ORCHESTRA_TOKEN")
	_, _ = os.Stdout.WriteString("thread=" + threadID + " task=" + taskID + " token_leaked=" + token + "\n")

	switch mode {
	case "fail":
		os.Exit(7)
	case "sleep":
		time.Sleep(1500 * time.Millisecond)
		os.Exit(0)
	case "wait-cancel":
		time.Sleep(30 * time.Second)
		os.Exit(0)
	default:
		os.Exit(0)
	}
}

func startTaskRunTestServer(t *testing.T, mode orchestrator.ServiceMode) (*orchestrator.Service, *httptest.Server, string) {
	t.Helper()
	openedStore, err := store.Open(filepath.Join(t.TempDir(), "state"))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = openedStore.Close()
	})
	token := "cli-task-run-secret"
	service, err := orchestrator.NewService(orchestrator.ServiceOptions{
		Mode:          mode,
		Store:         openedStore,
		Token:         token,
		SweepInterval: 200 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(service.Close)
	server := httptest.NewServer(service.Handler())
	t.Cleanup(server.Close)
	return service, server, token
}

func TestCLITaskRunLifecycleAndSignalRelease(t *testing.T) {
	testBinary, err := os.Executable()
	require.NoError(t, err)

	service, server, token := startTaskRunTestServer(t, orchestrator.ServiceModeRelay)
	caller := orchestrator.CallerIdentity{Kind: "operator", OperatorID: "test"}
	thread, _, err := service.CreateThread(caller, orchestrator.CreateThreadInput{Title: "CLI task run"})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(caller, orchestrator.SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "cli-run-tasks",
		Tasks: []orchestrator.SubmittedTaskSpec{
			{ClientTaskID: "ok", Title: "Succeed", Instructions: "exit 0"},
			{ClientTaskID: "fail", Title: "Fail", Instructions: "exit 7"},
			{ClientTaskID: "renew", Title: "Renew lease", Instructions: "sleep past 1s ttl"},
			{ClientTaskID: "cancel", Title: "Release on signal", Instructions: "wait until cancelled"},
		},
	})
	require.NoError(t, err)

	lookupEnv := func(key string) (string, bool) {
		switch key {
		case "ORCHESTRA_ENDPOINT":
			return server.URL, true
		case "ORCHESTRA_TOKEN":
			return token, true
		default:
			return "", false
		}
	}

	// 1. Success path: sets ORCHESTRA_THREAD_ID & ORCHESTRA_TASK_ID, does not leak --token, settles completed.
	t.Setenv("ORCHESTRA_TASK_RUN_HELPER", "1")
	t.Setenv("ORCHESTRA_TASK_RUN_HELPER_MODE", "ok")
	var stdout, stderr bytes.Buffer
	exitCode := Run(t.Context(), []string{
		"task", "run",
		"--connection-file", "none",
		"--task", submitted.ClientTaskMap["ok"],
		"--ttl", "30s",
		"--", testBinary, "-test.run=TestTaskRunHelperProcess",
	}, nil, &stdout, &stderr, lookupEnv)
	require.Equal(t, 0, exitCode, stderr.String())
	assert.Contains(t, stdout.String(), "thread="+thread.ID)
	assert.Contains(t, stdout.String(), "task="+submitted.ClientTaskMap["ok"])
	assert.Contains(t, stdout.String(), "token_leaked=")
	assert.NotContains(t, stdout.String(), token)

	ctxSnapshot, err := service.GetThreadContext(thread.ID)
	require.NoError(t, err)
	assert.Equal(t, "completed", findTaskByID(t, ctxSnapshot.Tasks, submitted.ClientTaskMap["ok"]).Status)

	// 2. Failure path: settles failed and returns child exit code.
	t.Setenv("ORCHESTRA_TASK_RUN_HELPER_MODE", "fail")
	stdout.Reset()
	stderr.Reset()
	exitCode = Run(t.Context(), []string{
		"task", "run",
		"--connection-file", "none",
		"--task", submitted.ClientTaskMap["fail"],
		"--ttl", "30s",
		"--", testBinary, "-test.run=TestTaskRunHelperProcess",
	}, nil, &stdout, &stderr, lookupEnv)
	assert.Equal(t, 7, exitCode, stderr.String())
	ctxSnapshot, err = service.GetThreadContext(thread.ID)
	require.NoError(t, err)
	failedTask := findTaskByID(t, ctxSnapshot.Tasks, submitted.ClientTaskMap["fail"])
	assert.Equal(t, "failed", failedTask.Status)
	assert.Contains(t, failedTask.Summary, "code 7")

	// 3. Background renewal path: 1s TTL, child sleeps 1.5s -> renewed at ~333ms and settles completed!
	t.Setenv("ORCHESTRA_TASK_RUN_HELPER_MODE", "sleep")
	stdout.Reset()
	stderr.Reset()
	exitCode = Run(t.Context(), []string{
		"task", "run",
		"--connection-file", "none",
		"--task", submitted.ClientTaskMap["renew"],
		"--ttl", "1s",
		"--", testBinary, "-test.run=TestTaskRunHelperProcess",
	}, nil, &stdout, &stderr, lookupEnv)
	require.Equal(t, 0, exitCode, stderr.String())
	ctxSnapshot, err = service.GetThreadContext(thread.ID)
	require.NoError(t, err)
	assert.Equal(t, "completed", findTaskByID(t, ctxSnapshot.Tasks, submitted.ClientTaskMap["renew"]).Status)

	// 4. Signal / context cancellation path: releases claim cleanly back to ready!
	t.Setenv("ORCHESTRA_TASK_RUN_HELPER_MODE", "wait-cancel")
	cancelCtx, cancel := context.WithCancel(t.Context())
	go func() {
		time.Sleep(350 * time.Millisecond)
		cancel()
	}()
	stdout.Reset()
	stderr.Reset()
	exitCode = Run(cancelCtx, []string{
		"task", "run",
		"--connection-file", "none",
		"--task", submitted.ClientTaskMap["cancel"],
		"--ttl", "30s",
		"--", testBinary, "-test.run=TestTaskRunHelperProcess",
	}, nil, &stdout, &stderr, lookupEnv)
	assert.Equal(t, 130, exitCode, stderr.String())

	ctxSnapshot, err = service.GetThreadContext(thread.ID)
	require.NoError(t, err)
	releasedTask := findTaskByID(t, ctxSnapshot.Tasks, submitted.ClientTaskMap["cancel"])
	assert.Equal(t, "ready", releasedTask.Status)
	assert.Nil(t, releasedTask.Claim)
	assert.Equal(t, int64(1), releasedTask.ClaimEpoch)
	assert.Empty(t, strings.TrimSpace(stderr.String()), "clean release should not log release error: %s", stderr.String())
}

func findTaskByID(t *testing.T, tasks []protocol.Task, taskID string) protocol.Task {
	t.Helper()
	for _, task := range tasks {
		if task.ID == taskID {
			return task
		}
	}
	t.Fatalf("task %s not found", taskID)
	return protocol.Task{}
}
