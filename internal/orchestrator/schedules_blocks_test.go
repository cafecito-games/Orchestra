package orchestrator

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

func TestStructuredBlocksOnRoomAndMailAndStoreRoundTrip(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "state")
	openedStore, err := store.Open(dataDir)
	require.NoError(t, err)

	service, err := NewService(ServiceOptions{
		Store:         openedStore,
		Token:         "block-secret-token",
		SweepInterval: time.Hour,
	})
	require.NoError(t, err)

	caller := CallerIdentity{Kind: "orchestrator-client", ClientID: "verifier-1", SessionID: "sess-1"}
	_, err = service.RegisterClientMailbox(caller.ClientID, caller.SessionID, &protocol.ClientMailRegistration{RequestedName: "verifier", Host: "mac"})
	require.NoError(t, err)
	peerBox, err := service.RegisterClientMailbox("author-1", "sess-2", &protocol.ClientMailRegistration{RequestedName: "author", Host: "mac"})
	require.NoError(t, err)

	thread, _, err := service.CreateThread(caller, CreateThreadInput{Title: "Handoff blocks"})
	require.NoError(t, err)

	// Invalid block returns 400 invalid_block.
	_, err = service.PostRoomMessage(caller, PostRoomMessageInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "bad-block-1",
		Kind:           "finding",
		Body:           "Bad block",
		Blocks:         []protocol.StructuredBlock{{Schema: "test-report", Version: 1, Data: map[string]any{"passed": -5, "failed": 0}}},
	})
	var opErr *OperationError
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "invalid_block", opErr.Code)
	assert.Equal(t, http.StatusBadRequest, opErr.HTTPStatus)

	// Valid room block with secret is redacted and persisted across restart.
	posted, err := service.PostRoomMessage(caller, PostRoomMessageInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "good-block-1",
		Kind:           "decision",
		Body:           "Review passed",
		Blocks: []protocol.StructuredBlock{{
			Schema:  "review-verdict",
			Version: 1,
			Data: map[string]any{
				"verdict": "approved",
				"sha":     "deadbeef1234",
				"summary": "Clean with block-secret-token scrubbed",
			},
		}},
	})
	require.NoError(t, err)
	require.Len(t, posted.Message.Blocks, 1)
	assert.NotContains(t, posted.Message.Blocks[0].Data["summary"], "block-secret-token")

	// Valid mail block persists.
	sentMail, err := service.SendMail(caller, SendMailInput{
		IdempotencyKey: "mail-block-1",
		To:             peerBox.Address,
		Body:           "Commit ready",
		Blocks: []protocol.StructuredBlock{{
			Schema:  "git-commit",
			Version: 1,
			Data:    map[string]any{"sha": "deadbeef1234", "branch": "main"},
		}},
	})
	require.NoError(t, err)
	require.Len(t, sentMail.Message.Blocks, 1)
	assert.Equal(t, "git-commit", sentMail.Message.Blocks[0].Schema)

	service.Close()
	require.NoError(t, openedStore.Close())

	// Reopen store and verify structured blocks round-tripped cleanly from SQLite.
	reopenedStore, err := store.Open(dataDir)
	require.NoError(t, err)
	defer reopenedStore.Close()
	reopenedService, err := NewService(ServiceOptions{Store: reopenedStore, Token: "block-secret-token", SweepInterval: time.Hour})
	require.NoError(t, err)
	defer reopenedService.Close()

	roomRead, err := reopenedService.ReadRoom(caller, ReadRoomInput{ThreadID: thread.ID})
	require.NoError(t, err)
	require.Len(t, roomRead.Messages, 1)
	require.Len(t, roomRead.Messages[0].Blocks, 1)
	assert.Equal(t, "approved", roomRead.Messages[0].Blocks[0].Data["verdict"])
}

func TestDurableScheduledMessagesFakeClockRestartAndCancellation(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "state")
	clock := newClaimTestClock(time.Date(2026, 11, 20, 14, 0, 0, 0, time.UTC))

	openedStore, err := store.Open(dataDir)
	require.NoError(t, err)
	service, err := NewService(ServiceOptions{
		Mode:          ServiceModeRelay,
		Store:         openedStore,
		Token:         "sched-secret",
		SweepInterval: time.Hour,
		Now:           clock.Now,
	})
	require.NoError(t, err)

	creator := CallerIdentity{Kind: "orchestrator-client", ClientID: "agent-a", SessionID: "sess-a"}
	other := CallerIdentity{Kind: "orchestrator-client", ClientID: "agent-b", SessionID: "sess-b"}
	operator := CallerIdentity{Kind: "operator", OperatorID: "ops"}

	_, err = service.RegisterClientMailbox(creator.ClientID, creator.SessionID, &protocol.ClientMailRegistration{RequestedName: "alpha", Host: "box"})
	require.NoError(t, err)
	betaBox, err := service.RegisterClientMailbox(other.ClientID, other.SessionID, &protocol.ClientMailRegistration{RequestedName: "beta", Host: "box"})
	require.NoError(t, err)

	thread, _, err := service.CreateThread(creator, CreateThreadInput{Title: "Scheduled thread"})
	require.NoError(t, err)

	// Reject recurring interval < 60s.
	_, err = service.ScheduleMessage(creator, ScheduleMessageInput{
		IdempotencyKey: "too-fast",
		TargetKind:     protocol.ScheduleTargetKindRoom,
		ThreadID:       thread.ID,
		Body:           "Too fast",
		EverySeconds:   30,
	})
	require.Error(t, err)

	// Schedule one-shot room reminder in 120s with idempotency replay.
	oneShot, err := service.ScheduleMessage(creator, ScheduleMessageInput{
		IdempotencyKey: "oneshot-1",
		TargetKind:     protocol.ScheduleTargetKindRoom,
		ThreadID:       thread.ID,
		Kind:           "guidance",
		Body:           "Follow up on PR",
		InSeconds:      120,
	})
	require.NoError(t, err)
	assert.False(t, oneShot.Replayed)
	assert.Equal(t, protocol.ScheduleStatusActive, oneShot.Schedule.Status)

	replayed, err := service.ScheduleMessage(creator, ScheduleMessageInput{
		IdempotencyKey: "oneshot-1",
		TargetKind:     protocol.ScheduleTargetKindRoom,
		ThreadID:       thread.ID,
		Kind:           "guidance",
		Body:           "Follow up on PR",
		InSeconds:      120,
	})
	require.NoError(t, err)
	assert.True(t, replayed.Replayed)
	assert.Equal(t, oneShot.Schedule.ID, replayed.Schedule.ID)

	// Schedule recurring mail tick every 60s for max 2 occurrences.
	recurring, err := service.ScheduleMessage(creator, ScheduleMessageInput{
		IdempotencyKey: "recurring-mail-1",
		TargetKind:     protocol.ScheduleTargetKindMail,
		To:             betaBox.Address,
		Subject:        "Heartbeat",
		Body:           "Periodic check",
		EverySeconds:   60,
		MaxOccurrences: 2,
	})
	require.NoError(t, err)

	// Non-creator non-operator cannot cancel.
	_, err = service.CancelSchedule(other, CancelScheduleInput{ScheduleID: oneShot.Schedule.ID})
	var opErr *OperationError
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "forbidden", opErr.Code)

	// Simulate relay restart before schedules fire!
	service.Close()
	require.NoError(t, openedStore.Close())

	reopenedStore, err := store.Open(dataDir)
	require.NoError(t, err)
	defer reopenedStore.Close()
	restarted, err := NewService(ServiceOptions{
		Mode:          ServiceModeRelay,
		Store:         reopenedStore,
		Token:         "sched-secret",
		SweepInterval: time.Hour,
		Now:           clock.Now,
	})
	require.NoError(t, err)
	defer restarted.Close()

	// Advance +60s: recurring mail fires occurrence 1; one-shot not due yet.
	clock.Advance(60 * time.Second)
	assert.Equal(t, 1, restarted.processDueSchedules())

	// Advance +60s (+120s total): recurring mail fires occurrence 2 (completes) AND one-shot room fires (completes)!
	clock.Advance(60 * time.Second)
	assert.Equal(t, 2, restarted.processDueSchedules())

	// No further deliveries when advancing another 120s.
	clock.Advance(120 * time.Second)
	assert.Equal(t, 0, restarted.processDueSchedules())

	roomMessages, err := restarted.ReadRoom(creator, ReadRoomInput{ThreadID: thread.ID})
	require.NoError(t, err)
	require.Len(t, roomMessages.Messages, 1)
	assert.Equal(t, "Follow up on PR", roomMessages.Messages[0].Body)

	betaInbox, err := restarted.ReadMail(other, ReadMailInput{Box: "inbox"})
	require.NoError(t, err)
	require.Len(t, betaInbox.Messages, 2)

	listed, err := restarted.ListSchedules(operator, ListSchedulesInput{Status: protocol.ScheduleStatusCompleted})
	require.NoError(t, err)
	assert.Len(t, listed, 2)

	// Schedule another room message and verify resolving the thread auto-cancels it.
	autoCancelSched, err := restarted.ScheduleMessage(creator, ScheduleMessageInput{
		IdempotencyKey: "auto-cancel-1",
		TargetKind:     protocol.ScheduleTargetKindRoom,
		ThreadID:       thread.ID,
		Body:           "Should be auto-cancelled",
		InSeconds:      300,
	})
	require.NoError(t, err)
	resolved := "resolved"
	_, err = restarted.UpdateThread(operator, UpdateThreadInput{ThreadID: thread.ID, Status: &resolved})
	require.NoError(t, err)

	cancelledList, err := restarted.ListSchedules(operator, ListSchedulesInput{ThreadID: thread.ID, Status: protocol.ScheduleStatusCancelled})
	require.NoError(t, err)
	require.Len(t, cancelledList, 1)
	assert.Equal(t, autoCancelSched.Schedule.ID, cancelledList[0].ID)

	// Creator can cancel another active schedule explicitly.
	cancellable, err := restarted.ScheduleMessage(creator, ScheduleMessageInput{
		IdempotencyKey: "manual-cancel-1",
		TargetKind:     protocol.ScheduleTargetKindMail,
		To:             betaBox.Address,
		Body:           "Cancel me",
		InSeconds:      300,
	})
	require.NoError(t, err)
	cancelled, err := restarted.CancelSchedule(creator, CancelScheduleInput{ScheduleID: cancellable.Schedule.ID})
	require.NoError(t, err)
	assert.Equal(t, protocol.ScheduleStatusCancelled, cancelled.Status)
	assert.NotEqual(t, recurring.Schedule.ID, cancelled.ID)

	// Scoped grant (read-only OR post-scoped) cannot cancel an operator-created schedule.
	opSched, err := restarted.ScheduleMessage(operator, ScheduleMessageInput{
		IdempotencyKey: "op-sched-1",
		TargetKind:     protocol.ScheduleTargetKindMail,
		To:             betaBox.Address,
		Body:           "Operator schedule",
		InSeconds:      300,
	})
	require.NoError(t, err)

	readOnlyGrantCaller := CallerIdentity{
		Kind:       "operator",
		OperatorID: "cli",
		GrantID:    "grant_readonly",
		Scope: &protocol.AccessGrantScope{
			ThreadIDs:  []string{thread.ID},
			Operations: []string{protocol.AccessOpRead},
		},
	}
	_, err = restarted.CancelSchedule(readOnlyGrantCaller, CancelScheduleInput{ScheduleID: opSched.Schedule.ID})
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "forbidden", opErr.Code)

	postGrantCaller := CallerIdentity{
		Kind:       "operator",
		OperatorID: "cli",
		GrantID:    "grant_poster",
		Scope: &protocol.AccessGrantScope{
			ThreadIDs:  []string{thread.ID},
			Operations: []string{protocol.AccessOpRead, protocol.AccessOpPost},
		},
	}
	_, err = restarted.CancelSchedule(postGrantCaller, CancelScheduleInput{ScheduleID: opSched.Schedule.ID})
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "forbidden", opErr.Code)
}
