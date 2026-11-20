package orchestrator

import (
	"context"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type claimTestClock struct {
	mutex sync.Mutex
	now   time.Time
}

func newClaimTestClock(start time.Time) *claimTestClock {
	return &claimTestClock{now: start.UTC()}
}

func (clock *claimTestClock) Now() time.Time {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	return clock.now
}

func (clock *claimTestClock) Advance(delta time.Duration) {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	clock.now = clock.now.Add(delta)
}

func newTaskClaimTestService(t *testing.T, mode ServiceMode, clock *claimTestClock) *Service {
	t.Helper()
	openedStore, err := store.Open(filepath.Join(t.TempDir(), "state"))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = openedStore.Close()
	})
	options := ServiceOptions{
		Mode:          mode,
		Store:         openedStore,
		Token:         "claim-secret",
		SweepInterval: time.Hour,
	}
	if clock != nil {
		options.Now = clock.Now
	}
	service, err := NewService(options)
	require.NoError(t, err)
	t.Cleanup(service.Close)
	return service
}

func TestTaskClaimAtomicSingleWinnerUnderContention(t *testing.T) {
	service := newTaskClaimTestService(t, ServiceModeOrchestrator, nil)
	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "coord-1", SessionID: "sess-1"}

	thread, _, err := service.CreateThread(caller, CreateThreadInput{Title: "Contention thread"})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "sub-contention-1",
		Tasks: []SubmittedTaskSpec{{
			Title:         "Single claimable task",
			Instructions:  "Only one worker may claim me.",
			ExecutionMode: protocol.TaskExecutionModeClaimable,
		}},
	})
	require.NoError(t, err)
	require.Len(t, submitted.Tasks, 1)
	targetTaskID := submitted.Tasks[0].ID

	const workers = 16
	var winners atomic.Int32
	var winningEpoch atomic.Int64
	startGate := make(chan struct{})
	var group sync.WaitGroup
	for index := range workers {
		group.Add(1)
		go func(workerIndex int) {
			defer group.Done()
			<-startGate
			workerCaller := CallerIdentity{
				Kind:      "orchestrator-client",
				ClientID:  protocol.NewIdentifier("worker"),
				SessionID: protocol.NewIdentifier("sess"),
			}
			result, claimErr := service.ClaimTask(t.Context(), workerCaller, ClaimTaskInput{
				ThreadID:   thread.ID,
				TTLSeconds: 60,
			})
			if claimErr == nil && result.Claimed {
				winners.Add(1)
				winningEpoch.Store(result.Claim.Epoch)
				assert.Equal(t, targetTaskID, result.Task.ID)
			}
		}(index)
	}
	close(startGate)
	group.Wait()

	assert.Equal(t, int32(1), winners.Load(), "exactly one worker must win the claim")
	assert.Equal(t, int64(1), winningEpoch.Load())
}

func TestTaskClaimLeaseExpiryAndEpochFencing(t *testing.T) {
	clock := newClaimTestClock(time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC))
	service := newTaskClaimTestService(t, ServiceModeOrchestrator, clock)
	callerA := CallerIdentity{Kind: "orchestrator-client", ClientID: "worker-a", SessionID: "sess-a"}
	callerB := CallerIdentity{Kind: "orchestrator-client", ClientID: "worker-b", SessionID: "sess-b"}

	thread, _, err := service.CreateThread(callerA, CreateThreadInput{Title: "Fencing thread"})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(callerA, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "sub-fencing",
		Tasks: []SubmittedTaskSpec{
			{
				ClientTaskID:  "step1",
				Title:         "First step",
				Instructions:  "Run step 1",
				ExecutionMode: protocol.TaskExecutionModeClaimable,
			},
			{
				ClientTaskID:  "step2",
				Title:         "Second step",
				Instructions:  "Run step 2 after step 1",
				ExecutionMode: protocol.TaskExecutionModeClaimable,
				Dependencies:  []SubmittedDependency{{ClientTaskID: "step1", Policy: "require-success"}},
			},
		},
	})
	require.NoError(t, err)
	step1ID := submitted.ClientTaskMap["step1"]
	step2ID := submitted.ClientTaskMap["step2"]

	claim1, err := service.ClaimTask(t.Context(), callerA, ClaimTaskInput{
		ThreadID:   thread.ID,
		TTLSeconds: 30,
	})
	require.NoError(t, err)
	require.True(t, claim1.Claimed)
	require.Equal(t, step1ID, claim1.Task.ID)
	assert.Equal(t, int64(1), claim1.Claim.Epoch)
	assert.Equal(t, "running", claim1.Task.Status)

	// Renew at +10s succeeds and extends lease.
	clock.Advance(10 * time.Second)
	renewed, err := service.RenewTaskClaim(callerA, RenewTaskClaimInput{
		TaskID:     step1ID,
		ClaimEpoch: claim1.Claim.Epoch,
		ClaimToken: claim1.Claim.Token,
		TTLSeconds: 30,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), renewed.Claim.Epoch)
	assert.NotEqual(t, claim1.Claim.ExpiresAt, renewed.Claim.ExpiresAt)

	// Advance past renewed expiry (+31s) and trigger expiration sweep.
	clock.Advance(31 * time.Second)
	service.expireTaskClaims()

	// Verify task returned to ready and task.claim_expired event was emitted.
	contextAfterExpiry, err := service.GetThreadContext(thread.ID)
	require.NoError(t, err)
	var step1AfterExpiry protocol.Task
	for _, task := range contextAfterExpiry.Tasks {
		if task.ID == step1ID {
			step1AfterExpiry = task
		}
	}
	assert.Equal(t, "ready", step1AfterExpiry.Status)
	assert.Nil(t, step1AfterExpiry.Claim)
	assert.Equal(t, int64(1), step1AfterExpiry.ClaimEpoch)

	events, err := service.GetThreadEvents(callerA, GetThreadEventsInput{
		ThreadID: thread.ID,
		Types:    []string{"task.claim_expired"},
	})
	require.NoError(t, err)
	require.Len(t, events.Events, 1)
	assert.Equal(t, step1ID, events.Events[0].TaskID)

	// Worker B claims the expired task and receives monotonically higher epoch 2.
	claim2, err := service.ClaimTask(t.Context(), callerB, ClaimTaskInput{
		ThreadID:   thread.ID,
		TTLSeconds: 60,
	})
	require.NoError(t, err)
	require.True(t, claim2.Claimed)
	assert.Equal(t, step1ID, claim2.Task.ID)
	assert.Equal(t, int64(2), claim2.Claim.Epoch)
	assert.NotEqual(t, claim1.Claim.Token, claim2.Claim.Token)

	// Stale Worker A mutations MUST fail with 409 claim_lost.
	_, err = service.RenewTaskClaim(callerA, RenewTaskClaimInput{
		TaskID:     step1ID,
		ClaimEpoch: claim1.Claim.Epoch,
		ClaimToken: claim1.Claim.Token,
		TTLSeconds: 30,
	})
	require.Error(t, err)
	var opErr *OperationError
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "claim_lost", opErr.Code)
	assert.Equal(t, http.StatusConflict, opErr.HTTPStatus)

	_, err = service.ReleaseTaskClaim(callerA, ReleaseTaskClaimInput{
		TaskID:     step1ID,
		ClaimEpoch: claim1.Claim.Epoch,
		ClaimToken: claim1.Claim.Token,
	})
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "claim_lost", opErr.Code)
	assert.Equal(t, http.StatusConflict, opErr.HTTPStatus)

	staleEpoch := claim1.Claim.Epoch
	summary := "stale completion"
	_, err = service.UpdateTask(callerA, UpdateTaskInput{
		TaskID:     step1ID,
		ClaimEpoch: &staleEpoch,
		ClaimToken: claim1.Claim.Token,
		Status:     "completed",
		Summary:    &summary,
	})
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "claim_lost", opErr.Code)
	assert.Equal(t, http.StatusConflict, opErr.HTTPStatus)

	// Unfenced completion against an actively claimed task is also rejected with 409 claim_lost.
	_, err = service.UpdateTask(callerA, UpdateTaskInput{
		TaskID:  step1ID,
		Status:  "completed",
		Summary: &summary,
	})
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "claim_lost", opErr.Code)
	assert.Equal(t, http.StatusConflict, opErr.HTTPStatus)

	// Worker B completes step1 with matching epoch 2 + token, unlocking step2.
	validEpoch := claim2.Claim.Epoch
	doneSummary := "completed by worker B"
	updated, err := service.UpdateTask(callerB, UpdateTaskInput{
		TaskID:     step1ID,
		ClaimEpoch: &validEpoch,
		ClaimToken: claim2.Claim.Token,
		Status:     "completed",
		Summary:    &doneSummary,
	})
	require.NoError(t, err)
	assert.Equal(t, "completed", updated.Status)
	assert.Nil(t, updated.Claim)

	claimStep2, err := service.ClaimTask(t.Context(), callerB, ClaimTaskInput{
		ThreadID:   thread.ID,
		TTLSeconds: 60,
	})
	require.NoError(t, err)
	require.True(t, claimStep2.Claimed)
	assert.Equal(t, step2ID, claimStep2.Task.ID)
	assert.Equal(t, int64(1), claimStep2.Claim.Epoch)
}

func TestTaskClaimFilteringReleaseAndBoundedWait(t *testing.T) {
	service := newTaskClaimTestService(t, ServiceModeOrchestrator, nil)
	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "coord", SessionID: "sess"}

	thread, _, err := service.CreateThread(caller, CreateThreadInput{Title: "Filter & Wait"})
	require.NoError(t, err)

	submitted, err := service.SubmitTasks(caller, SubmitTasksInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "sub-filters",
		Tasks: []SubmittedTaskSpec{
			{
				ClientTaskID:  "gpu",
				Title:         "GPU task",
				Instructions:  "Needs gpu",
				ExecutionMode: protocol.TaskExecutionModeClaimable,
				Requirements:  protocol.ExecutionRequirements{Labels: []string{"gpu", "linux"}},
			},
			{
				ClientTaskID:  "cpu",
				Title:         "CPU task",
				Instructions:  "Needs cpu",
				ExecutionMode: protocol.TaskExecutionModeClaimable,
				Requirements:  protocol.ExecutionRequirements{Labels: []string{"cpu"}},
			},
		},
	})
	require.NoError(t, err)

	// Worker with only "cpu" label skips GPU task and claims CPU task.
	claimed, err := service.ClaimTask(t.Context(), caller, ClaimTaskInput{
		ThreadID:   thread.ID,
		Labels:     []string{"cpu"},
		TTLSeconds: 30,
	})
	require.NoError(t, err)
	require.True(t, claimed.Claimed)
	assert.Equal(t, submitted.ClientTaskMap["cpu"], claimed.Task.ID)

	// Cleanly release the claim while another goroutine waits for it.
	waitDone := make(chan ClaimTaskResult, 1)
	go func() {
		waited, waitErr := service.ClaimTask(context.Background(), CallerIdentity{
			Kind:      "orchestrator-client",
			ClientID:  "waiter",
			SessionID: "sess-waiter",
		}, ClaimTaskInput{
			TaskID:              submitted.ClientTaskMap["cpu"],
			TTLSeconds:          30,
			TimeoutMilliseconds: 3000,
		})
		if waitErr == nil {
			waitDone <- waited
		}
	}()

	time.Sleep(100 * time.Millisecond)
	released, err := service.ReleaseTaskClaim(caller, ReleaseTaskClaimInput{
		TaskID:     claimed.Task.ID,
		ClaimEpoch: claimed.Claim.Epoch,
		ClaimToken: claimed.Claim.Token,
		Reason:     "graceful shutdown",
	})
	require.NoError(t, err)
	assert.Equal(t, "ready", released.Task.Status)

	select {
	case waited := <-waitDone:
		require.True(t, waited.Claimed)
		assert.Equal(t, submitted.ClientTaskMap["cpu"], waited.Task.ID)
		assert.Equal(t, int64(2), waited.Claim.Epoch)
	case <-time.After(3 * time.Second):
		t.Fatal("waiting claimer did not wake up after release")
	}

	// Bounded wait with no matching tasks times out cleanly.
	timedOut, err := service.ClaimTask(t.Context(), caller, ClaimTaskInput{
		ThreadID:            thread.ID,
		Labels:              []string{"nonexistent"},
		TTLSeconds:          30,
		TimeoutMilliseconds: 150,
	})
	require.NoError(t, err)
	assert.False(t, timedOut.Claimed)
	assert.True(t, timedOut.TimedOut)
}
