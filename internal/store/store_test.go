package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

func TestStoreWALDurabilityRollbackEventPruningAndArtifactBlobs(t *testing.T) {
	dataDirectory := t.TempDir()
	openedStore, err := Open(dataDirectory)
	require.NoError(t, err)

	journalMode, err := openedStore.JournalMode()
	require.NoError(t, err)
	require.Equal(t, "wal", journalMode)

	subscriberChannel, unsubscribe := openedStore.SubscribeEvents()
	defer unsubscribe()

	nowText := time.Now().UTC().Format(time.RFC3339Nano)
	commitCalled := false
	events, err := openedStore.Mutate(func(state *State, mutation *Mutation) error {
		state.Threads["thread-1"] = protocol.Thread{
			ID:        "thread-1",
			Title:     "Investigate renderer",
			Status:    "open",
			CreatedAt: nowText,
			UpdatedAt: nowText,
		}
		state.Tasks["task-1"] = protocol.Task{
			ID:           "task-1",
			ThreadID:     "thread-1",
			Title:        "Profile frame pacing",
			Instructions: "Check swapchain timings",
			Status:       "ready",
			MaxAttempts:  3,
			Origin:       protocol.TaskOrigin{Kind: "orchestrator-client", ClientID: "client-1"},
			CreatedAt:    nowText,
			UpdatedAt:    nowText,
		}
		state.Messages["msg-1"] = protocol.TaskMessage{
			ID:        "msg-1",
			ThreadID:  "thread-1",
			Sender:    protocol.MessageEndpoint{Kind: "orchestrator", ClientID: "client-1"},
			Recipient: protocol.MessageEndpoint{Kind: "task", TaskID: "task-1"},
			Kind:      "guidance",
			Body:      "Capture 120 frames",
			CreatedAt: nowText,
		}
		mutation.OnCommit(func() {
			commitCalled = true
		})
		return nil
	})
	require.NoError(t, err)
	require.True(t, commitCalled)
	require.Len(t, events, 3)
	require.Equal(t, int64(1), events[0].Sequence)
	require.Equal(t, "thread.updated", events[0].Type)
	require.Equal(t, int64(2), events[1].Sequence)
	require.Equal(t, "task.created", events[1].Type)
	require.Equal(t, int64(3), events[2].Sequence)
	require.Equal(t, "message.posted", events[2].Type)

	select {
	case published := <-subscriberChannel:
		require.Len(t, published, 3)
	case <-time.After(time.Second):
		t.Fatal("expected subscriber event broadcast")
	}

	_, err = openedStore.Mutate(func(state *State, mutation *Mutation) error {
		delete(state.Threads, "thread-1")
		return errors.New("intentional rollback")
	})
	require.Error(t, err)

	require.NoError(t, openedStore.Read(func(state *State) error {
		_, exists := state.Threads["thread-1"]
		require.True(t, exists)
		return nil
	}))

	_, err = openedStore.Mutate(func(state *State, mutation *Mutation) error {
		for index := range protocol.ThreadEventRetentionLimit + 25 {
			mutation.EmitEvent(protocol.TaskEventEntry{
				ThreadID:   "thread-1",
				Type:       "task.updated",
				TaskID:     "task-1",
				Status:     "running",
				Summary:    fmt.Sprintf("progress tick %d", index),
				OccurredAt: nowText,
			})
		}
		return nil
	})
	require.NoError(t, err)

	payload := []byte("frame-capture-report-content")
	sum := sha256.Sum256(payload)
	digest := hex.EncodeToString(sum[:])

	require.ErrorIs(t, openedStore.WriteArtifactBlob("art-1", int64(len(payload)+1), digest, bytes.NewReader(payload)), ErrArtifactSizeMismatch)
	require.ErrorIs(t, openedStore.WriteArtifactBlob("art-1", int64(len(payload)), "0000000000000000000000000000000000000000000000000000000000000000", bytes.NewReader(payload)), ErrArtifactDigestMismatch)
	require.NoError(t, openedStore.WriteArtifactBlob("art-1", int64(len(payload)), digest, bytes.NewReader(payload)))

	require.NoError(t, openedStore.Close())

	reopenedStore, err := Open(dataDirectory)
	require.NoError(t, err)
	defer reopenedStore.Close()

	require.NoError(t, reopenedStore.Read(func(state *State) error {
		require.Equal(t, "Investigate renderer", state.Threads["thread-1"].Title)
		require.Equal(t, "ready", state.Tasks["task-1"].Status)
		stream := state.EventStreams["thread-1"]
		require.Equal(t, int64(3+protocol.ThreadEventRetentionLimit+25), stream.Head)
		require.Equal(t, stream.Head-protocol.ThreadEventRetentionLimit, stream.Floor)
		require.Len(t, state.ThreadEvents["thread-1"], protocol.ThreadEventRetentionLimit)
		require.Equal(t, stream.Floor+1, state.ThreadEvents["thread-1"][0].Sequence)
		return nil
	}))

	file, size, err := reopenedStore.OpenArtifactBlob("art-1")
	require.NoError(t, err)
	defer file.Close()
	require.Equal(t, int64(len(payload)), size)
	readBytes, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, payload, readBytes)
}
