package orchestrator

import (
	"fmt"
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

func newLabelTestService(t *testing.T) *Service {
	t.Helper()
	openedStore, err := store.Open(filepath.Join(t.TempDir(), "state"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = openedStore.Close() })
	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "label-secret-token",
	})
	require.NoError(t, err)
	t.Cleanup(service.Close)
	return service
}

func TestThreadLabelCASConcurrencyAndIdempotency(t *testing.T) {
	service := newLabelTestService(t)
	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "coord", SessionID: "sess"}

	thread, _, err := service.CreateThread(caller, CreateThreadInput{
		Title:  "PR review",
		Labels: map[string]string{"stage": "open"},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"stage": "open"}, thread.Labels)

	// Reserved orchestra.* namespace is rejected.
	_, err = service.UpdateThread(caller, UpdateThreadInput{
		ThreadID:  thread.ID,
		SetLabels: map[string]string{"orchestra.owner": "bad"},
	})
	var opErr *OperationError
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "reserved_label", opErr.Code)

	// 12 contenders race to claim reviewer when unset; exactly 1 wins.
	const contenders = 12
	var winners atomic.Int32
	var conflicts atomic.Int32
	var winningReviewer string
	var winnerMu sync.Mutex
	startGate := make(chan struct{})
	var group sync.WaitGroup
	for i := range contenders {
		group.Add(1)
		go func(idx int) {
			defer group.Done()
			<-startGate
			reviewer := fmt.Sprintf("verifier-%d", idx)
			updated, updateErr := service.UpdateThread(
				CallerIdentity{Kind: "orchestrator-client", ClientID: reviewer, SessionID: reviewer},
				UpdateThreadInput{
					ThreadID:       thread.ID,
					IdempotencyKey: fmt.Sprintf("claim-reviewer-%d", idx),
					IfLabels:       map[string]string{"stage": "open"},
					IfUnsetLabels:  []string{"reviewer"},
					SetLabels:      map[string]string{"reviewer": reviewer, "stage": "review"},
				},
			)
			if updateErr == nil {
				winners.Add(1)
				winnerMu.Lock()
				winningReviewer = updated.Labels["reviewer"]
				winnerMu.Unlock()
				return
			}
			var conflictErr *OperationError
			if assert.ErrorAs(t, updateErr, &conflictErr) {
				assert.Equal(t, "label_conflict", conflictErr.Code)
				assert.Equal(t, http.StatusConflict, conflictErr.HTTPStatus)
				assert.NotNil(t, conflictErr.Detail["currentLabels"])
				conflicts.Add(1)
			}
		}(i)
	}
	close(startGate)
	group.Wait()

	assert.Equal(t, int32(1), winners.Load())
	assert.Equal(t, int32(contenders-1), conflicts.Load())
	require.NotEmpty(t, winningReviewer)

	// Replaying the winner's identical idempotent CAS mutation succeeds without conflict.
	winnerCaller := CallerIdentity{Kind: "orchestrator-client", ClientID: winningReviewer, SessionID: winningReviewer}
	var winnerIdx int
	_, scanErr := fmt.Sscanf(winningReviewer, "verifier-%d", &winnerIdx)
	require.NoError(t, scanErr)
	replayed, err := service.UpdateThread(winnerCaller, UpdateThreadInput{
		ThreadID:       thread.ID,
		IdempotencyKey: fmt.Sprintf("claim-reviewer-%d", winnerIdx),
		IfLabels:       map[string]string{"stage": "open"},
		IfUnsetLabels:  []string{"reviewer"},
		SetLabels:      map[string]string{"reviewer": winningReviewer, "stage": "review"},
	})
	require.NoError(t, err)
	assert.Equal(t, winningReviewer, replayed.Labels["reviewer"])
	assert.Equal(t, "review", replayed.Labels["stage"])

	// Unsetting a label with matching ifLabels succeeds.
	unassigned, err := service.UpdateThread(winnerCaller, UpdateThreadInput{
		ThreadID:    thread.ID,
		IfLabels:    map[string]string{"reviewer": winningReviewer},
		UnsetLabels: []string{"reviewer"},
		SetLabels:   map[string]string{"stage": "approved"},
	})
	require.NoError(t, err)
	assert.NotContains(t, unassigned.Labels, "reviewer")
	assert.Equal(t, "approved", unassigned.Labels["stage"])
}

func TestRoomOptimisticSequenceConcurrencyAndIdempotency(t *testing.T) {
	service := newLabelTestService(t)
	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "coord", SessionID: "sess"}

	thread, _, err := service.CreateThread(caller, CreateThreadInput{Title: "Room CAS"})
	require.NoError(t, err)

	first, err := service.PostRoomMessage(caller, PostRoomMessageInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "seed-1",
		Kind:           "contract",
		Body:           "Initial contract",
		Labels:         map[string]string{"stage": "draft"},
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), first.Message.Sequence)

	// 10 contenders race to post assuming headSequence == 1; exactly 1 wins.
	const contenders = 10
	expectedHead := int64(1)
	var winners atomic.Int32
	var conflicts atomic.Int32
	var winnerKey string
	var winnerBody string
	var winnerMu sync.Mutex

	startGate := make(chan struct{})
	var group sync.WaitGroup
	for i := range contenders {
		group.Add(1)
		go func(idx int) {
			defer group.Done()
			<-startGate
			key := fmt.Sprintf("decision-%d", idx)
			body := fmt.Sprintf("Decision from agent %d", idx)
			res, postErr := service.PostRoomMessage(caller, PostRoomMessageInput{
				ThreadID:             thread.ID,
				IdempotencyKey:       key,
				Kind:                 "decision",
				Body:                 body,
				ExpectedHeadSequence: &expectedHead,
				Labels:               map[string]string{"stage": "final"},
			})
			if postErr == nil {
				winners.Add(1)
				winnerMu.Lock()
				winnerKey = key
				winnerBody = body
				winnerMu.Unlock()
				assert.Equal(t, int64(2), res.Message.Sequence)
				return
			}
			var conflictErr *OperationError
			if assert.ErrorAs(t, postErr, &conflictErr) {
				assert.Equal(t, "newer_room_message", conflictErr.Code)
				assert.Equal(t, http.StatusConflict, conflictErr.HTTPStatus)
				conflicts.Add(1)
			}
		}(i)
	}
	close(startGate)
	group.Wait()

	assert.Equal(t, int32(1), winners.Load())
	assert.Equal(t, int32(contenders-1), conflicts.Load())

	// Replaying the winner's idempotent conditional post succeeds cleanly!
	replayed, err := service.PostRoomMessage(caller, PostRoomMessageInput{
		ThreadID:             thread.ID,
		IdempotencyKey:       winnerKey,
		Kind:                 "decision",
		Body:                 winnerBody,
		ExpectedHeadSequence: &expectedHead,
		Labels:               map[string]string{"stage": "final"},
	})
	require.NoError(t, err)
	assert.True(t, replayed.Replayed)
	assert.Equal(t, int64(2), replayed.Message.Sequence)

	// Channel sequence guard allows posting on a different channel ("design") while main is at seq 2.
	expectedDesignSeq := int64(0)
	designPost, err := service.PostRoomMessage(caller, PostRoomMessageInput{
		ThreadID:                thread.ID,
		IdempotencyKey:          "design-1",
		Channel:                 "design",
		Kind:                    "finding",
		Body:                    "Design note",
		ExpectedChannelSequence: &expectedDesignSeq,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), designPost.Message.Sequence)

	// Stale expectedChannelSequence on "design" fails with 409 newer_room_message.
	_, err = service.PostRoomMessage(caller, PostRoomMessageInput{
		ThreadID:                thread.ID,
		IdempotencyKey:          "design-2",
		Channel:                 "design",
		Kind:                    "finding",
		Body:                    "Stale design note",
		ExpectedChannelSequence: &expectedDesignSeq,
	})
	var channelConflict *OperationError
	require.ErrorAs(t, err, &channelConflict)
	assert.Equal(t, "newer_room_message", channelConflict.Code)
	assert.Equal(t, http.StatusConflict, channelConflict.HTTPStatus)
}

func TestLabelFilteringAcrossThreadsRoomsAndWaitForActivity(t *testing.T) {
	service := newLabelTestService(t)
	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "coord", SessionID: "sess"}
	_, err := service.RegisterClientMailbox(caller.ClientID, caller.SessionID, &protocol.ClientMailRegistration{
		RequestedName: "coord",
		Host:          "localhost",
	})
	require.NoError(t, err)

	threadA, _, err := service.CreateThread(caller, CreateThreadInput{
		Title:  "Thread A",
		Labels: map[string]string{"stage": "draft", "team": "core"},
	})
	require.NoError(t, err)
	threadB, _, err := service.CreateThread(caller, CreateThreadInput{
		Title:  "Thread B",
		Labels: map[string]string{"stage": "review", "team": "core"},
	})
	require.NoError(t, err)
	_, _, err = service.CreateThread(caller, CreateThreadInput{
		Title:  "Thread C",
		Labels: map[string]string{"stage": "done", "team": "ui"},
	})
	require.NoError(t, err)

	// ListThreads filter: alternation + conjunction.
	matchedThreads, err := service.ListThreads(ListThreadsInput{
		Labels: []string{"team=core", "stage=draft|review"},
	})
	require.NoError(t, err)
	require.Len(t, matchedThreads, 2)
	matchedIDs := []string{matchedThreads[0].ID, matchedThreads[1].ID}
	assert.ElementsMatch(t, []string{threadA.ID, threadB.ID}, matchedIDs)

	// ReadRoom label filtering.
	_, err = service.PostRoomMessage(caller, PostRoomMessageInput{
		ThreadID:       threadA.ID,
		IdempotencyKey: "rm-1",
		Kind:           "finding",
		Body:           "Draft finding",
		Labels:         map[string]string{"stage": "draft", "verdict": "pending"},
	})
	require.NoError(t, err)
	_, err = service.PostRoomMessage(caller, PostRoomMessageInput{
		ThreadID:       threadA.ID,
		IdempotencyKey: "rm-2",
		Kind:           "decision",
		Body:           "Approved decision",
		Labels:         map[string]string{"stage": "review", "verdict": "approved"},
	})
	require.NoError(t, err)

	roomFiltered, err := service.ReadRoom(caller, ReadRoomInput{
		ThreadID: threadA.ID,
		Labels:   []string{"verdict=approved|rejected", "stage"},
	})
	require.NoError(t, err)
	require.Len(t, roomFiltered.Messages, 1)
	assert.Equal(t, "Approved decision", roomFiltered.Messages[0].Body)

	// WaitForActivity label filtering ignores non-matching room message and wakes on matching one.
	afterSeq := int64(2)
	waitDone := make(chan WaitForActivityResult, 1)
	go func() {
		res, waitErr := service.WaitForActivity(t.Context(), caller, WaitForActivityInput{
			ThreadID:            threadA.ID,
			AfterRoomSequence:   &afterSeq,
			Labels:              []string{"verdict=approved"},
			TimeoutMilliseconds: 3000,
		})
		if waitErr == nil {
			waitDone <- res
		}
	}()

	time.Sleep(80 * time.Millisecond)
	_, err = service.PostRoomMessage(caller, PostRoomMessageInput{
		ThreadID:       threadA.ID,
		IdempotencyKey: "rm-3",
		Kind:           "finding",
		Body:           "Not matching",
		Labels:         map[string]string{"verdict": "pending"},
	})
	require.NoError(t, err)
	time.Sleep(80 * time.Millisecond)
	_, err = service.PostRoomMessage(caller, PostRoomMessageInput{
		ThreadID:       threadA.ID,
		IdempotencyKey: "rm-4",
		Kind:           "decision",
		Body:           "Matching approved message",
		Labels:         map[string]string{"verdict": "approved"},
	})
	require.NoError(t, err)

	select {
	case res := <-waitDone:
		require.Len(t, res.RoomMessages, 1)
		assert.Equal(t, "Matching approved message", res.RoomMessages[0].Body)
	case <-time.After(3 * time.Second):
		t.Fatal("wait_for_activity did not wake on matching labeled room message")
	}
}
