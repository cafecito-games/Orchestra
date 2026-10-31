package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
		require.FailNow(t, "expected subscriber event broadcast")
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

func TestStoreRoomSQLiteDurabilityJSONLMirrorConcurrencyAndStartupReconciliation(t *testing.T) {
	dataDirectory := t.TempDir()
	openedStore, err := Open(dataDirectory)
	require.NoError(t, err)

	createdAt := "2026-10-30T14:00:00Z"
	threadID := "thread-abcd1234ef567890"
	_, err = openedStore.Mutate(func(state *State, mutation *Mutation) error {
		state.Threads[threadID] = protocol.Thread{
			ID:        threadID,
			Title:     "Room blackboard thread",
			Status:    "open",
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		}
		return nil
	})
	require.NoError(t, err)

	jsonlPath, exists, err := openedStore.RoomJSONLPath(threadID)
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, strings.HasSuffix(jsonlPath, filepath.Join("rooms", "room_20261030_abcd1234ef567890", "messages.jsonl")))

	roomDirectory := filepath.Dir(jsonlPath)
	roomMetadataPath := filepath.Join(roomDirectory, "room.json")
	directoryInfo, err := os.Lstat(roomDirectory)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), directoryInfo.Mode().Perm())
	metadataInfo, err := os.Lstat(roomMetadataPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), metadataInfo.Mode().Perm())
	jsonlInfo, err := os.Lstat(jsonlPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), jsonlInfo.Mode().Perm())

	const writerCount = 12
	var waitGroup sync.WaitGroup
	for writerIndex := range writerCount {
		index := writerIndex
		waitGroup.Go(func() {
			_, mutateErr := openedStore.Mutate(func(state *State, mutation *Mutation) error {
				head := state.RoomHeads[threadID]
				nextSequence := head.HeadSequence + 1
				head.HeadSequence = nextSequence
				head.UpdatedAt = fmt.Sprintf("2026-10-30T14:00:%02dZ", index)
				state.RoomHeads[threadID] = head
				state.RoomMessages[threadID] = append(state.RoomMessages[threadID], protocol.RoomMessage{
					Sequence:  nextSequence,
					ID:        fmt.Sprintf("rmsg_%016x", nextSequence),
					ThreadID:  threadID,
					CreatedAt: head.UpdatedAt,
					Author: protocol.RoomAuthor{
						Kind:   "task",
						TaskID: fmt.Sprintf("task-%d", index),
						RunID:  fmt.Sprintf("run-%d", index),
						Title:  "Worker task",
					},
					Kind:  "finding",
					Topic: fmt.Sprintf("topic-%d", index),
					Body:  fmt.Sprintf("concurrent finding payload %d", index),
				})
				return nil
			})
			assert.NoError(t, mutateErr)
		})
	}
	waitGroup.Wait()

	verifyMirroredLines := func(targetPath string, expectedCount int) {
		t.Helper()
		rawBytes, readErr := os.ReadFile(targetPath)
		require.NoError(t, readErr)
		if expectedCount == 0 {
			require.Empty(t, rawBytes)
			return
		}
		require.True(t, bytes.HasSuffix(rawBytes, []byte("\n")))
		lines := strings.Split(strings.TrimSuffix(string(rawBytes), "\n"), "\n")
		require.Len(t, lines, expectedCount)
		for lineIndex, line := range lines {
			var decoded protocol.RoomMessage
			require.NoError(t, json.Unmarshal([]byte(line), &decoded))
			require.Equal(t, int64(lineIndex+1), decoded.Sequence)
			require.Equal(t, threadID, decoded.ThreadID)
		}
	}

	verifyMirroredLines(jsonlPath, writerCount)
	require.NoError(t, openedStore.Close())

	rawExisting, err := os.ReadFile(jsonlPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(jsonlPath, rawExisting[:len(rawExisting)/2], 0o600))
	require.NoError(t, os.Remove(roomMetadataPath))

	reopenedStore, err := Open(dataDirectory)
	require.NoError(t, err)
	verifyMirroredLines(jsonlPath, writerCount)

	rawMetadata, err := os.ReadFile(roomMetadataPath)
	require.NoError(t, err)
	var healedHead protocol.ThreadRoomHead
	require.NoError(t, json.Unmarshal(rawMetadata, &healedHead))
	require.Equal(t, int64(writerCount), healedHead.HeadSequence)
	require.NoError(t, reopenedStore.Close())

	outsideFile := filepath.Join(t.TempDir(), "escaped.jsonl")
	require.NoError(t, os.WriteFile(outsideFile, []byte("secret"), 0o600))
	require.NoError(t, os.Remove(jsonlPath))
	require.NoError(t, os.Symlink(outsideFile, jsonlPath))

	_, err = Open(dataDirectory)
	require.Error(t, err)
	escapedBytes, err := os.ReadFile(outsideFile)
	require.NoError(t, err)
	require.Equal(t, "secret", string(escapedBytes))
}

func TestStoreRoomMirrorFailureKeepsCommittedStateAndSelfHeals(t *testing.T) {
	dataDirectory := t.TempDir()
	openedStore, err := Open(dataDirectory)
	require.NoError(t, err)
	defer openedStore.Close()

	createdAt := "2026-10-30T15:00:00Z"
	threadID := "thread-heal1234ef567890"
	expectedDirectoryName := protocol.RoomDirectoryName(threadID, createdAt)
	roomsRoot := filepath.Join(openedStore.DataDirectory(), "rooms")
	roomDirectoryPath := filepath.Join(roomsRoot, expectedDirectoryName)

	outsideTarget := filepath.Join(t.TempDir(), "untouched.txt")
	require.NoError(t, os.WriteFile(outsideTarget, []byte("original"), 0o600))
	require.NoError(t, os.Symlink(outsideTarget, roomDirectoryPath))

	_, err = openedStore.Mutate(func(state *State, mutation *Mutation) error {
		state.Threads[threadID] = protocol.Thread{
			ID:        threadID,
			Title:     "Authoritative thread",
			Status:    "open",
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
		}
		head := protocol.ThreadRoomHead{
			ThreadID:      threadID,
			DirectoryName: expectedDirectoryName,
			HeadSequence:  1,
			CreatedAt:     createdAt,
			UpdatedAt:     createdAt,
		}
		state.RoomHeads[threadID] = head
		state.RoomMessages[threadID] = []protocol.RoomMessage{{
			Sequence:  1,
			ID:        "rmsg_0000000000000001",
			ThreadID:  threadID,
			CreatedAt: createdAt,
			Author:    protocol.RoomAuthor{Kind: "operator", OperatorID: "cli"},
			Kind:      "guidance",
			Body:      "Committed while mirror path was blocked",
		}}
		return nil
	})
	require.NoError(t, err)

	untouchedBytes, err := os.ReadFile(outsideTarget)
	require.NoError(t, err)
	require.Equal(t, "original", string(untouchedBytes))

	_, _, err = openedStore.RoomJSONLPath(threadID)
	require.Error(t, err)

	require.NoError(t, os.Remove(roomDirectoryPath))

	healedPath, exists, err := openedStore.RoomJSONLPath(threadID)
	require.NoError(t, err)
	require.True(t, exists)
	healedBytes, err := os.ReadFile(healedPath)
	require.NoError(t, err)
	require.Contains(t, string(healedBytes), "Committed while mirror path was blocked")

	roomMetadataPath := filepath.Join(roomDirectoryPath, "room.json")
	require.NoError(t, os.Remove(roomMetadataPath))
	require.NoError(t, os.Mkdir(roomMetadataPath, 0o700))

	_, err = openedStore.Mutate(func(state *State, mutation *Mutation) error {
		head := state.RoomHeads[threadID]
		head.HeadSequence = 2
		head.UpdatedAt = "2026-10-30T15:01:00Z"
		state.RoomHeads[threadID] = head
		state.RoomMessages[threadID] = append(state.RoomMessages[threadID], protocol.RoomMessage{
			Sequence:  2,
			ID:        "rmsg_0000000000000002",
			ThreadID:  threadID,
			CreatedAt: head.UpdatedAt,
			Author:    protocol.RoomAuthor{Kind: "operator", OperatorID: "cli"},
			Kind:      "decision",
			Body:      "Sequence two committed while old messages.jsonl remained regular",
		})
		return nil
	})
	require.NoError(t, err)

	_, _, err = openedStore.RoomJSONLPath(threadID)
	require.Error(t, err, "RoomJSONLPath must not advertise a stale regular messages.jsonl when repair fails")

	require.NoError(t, os.Remove(roomMetadataPath))
	repairedPath, exists, err := openedStore.RoomJSONLPath(threadID)
	require.NoError(t, err)
	require.True(t, exists)
	repairedBytes, err := os.ReadFile(repairedPath)
	require.NoError(t, err)
	require.Contains(t, string(repairedBytes), "Sequence two committed while old messages.jsonl remained regular")
}

func TestStoreMailboxesMessagesAndReceiptsDurability(t *testing.T) {
	dataDirectory := t.TempDir()
	openedStore, err := Open(dataDirectory)
	require.NoError(t, err)

	createdAt := "2026-10-30T16:00:00Z"
	_, err = openedStore.Mutate(func(state *State, mutation *Mutation) error {
		state.Mailboxes["mbox-claude"] = protocol.AgentMailbox{
			ID:           "mbox-claude",
			Address:      "claude1234@fabrica",
			Handle:       "claude1234",
			Host:         "fabrica",
			Aliases:      []string{"claude0001@fabrica"},
			CustomHandle: true,
			Kind:         "mcp-session",
			Status:       "online",
			Harness:      "claude",
			Workspace:    "/workspaces/orchestra",
			PushCapable:  true,
			ClientID:     "claude-desktop",
			SessionID:    "sess-1",
			UnreadCount:  1,
			CreatedAt:    createdAt,
			UpdatedAt:    createdAt,
			LastSeenAt:   createdAt,
		}
		state.Mailboxes["mbox-codex"] = protocol.AgentMailbox{
			ID:          "mbox-codex",
			Address:     "codex345@bunker",
			Handle:      "codex345",
			Host:        "bunker",
			Kind:        "instance",
			Status:      "idle",
			Harness:     "codex",
			InstanceID:  "inst-codex-1",
			NodeID:      "bunker",
			UnreadCount: 0,
			CreatedAt:   createdAt,
			UpdatedAt:   createdAt,
			LastSeenAt:  createdAt,
		}
		state.MailMessages["mail-1"] = protocol.MailMessage{
			ID:             "mail-1",
			Sequence:       1,
			SourceKey:      "mbox:mbox-codex",
			IdempotencyKey: "send-1",
			Digest:         "digest-1",
			FromMailboxID:  "mbox-codex",
			FromAddress:    "codex345@bunker",
			ToMailboxID:    "mbox-claude",
			ToAddress:      "claude1234@fabrica",
			Subject:        "API migration ready",
			Body:           "Please review commit abc1234.",
			WakeRequested:  true,
			Read:           false,
			CreatedAt:      createdAt,
		}
		state.MailReceipts["mrec-1"] = protocol.MailReceipt{
			ID:             "mrec-1",
			SourceKey:      "mbox:mbox-codex",
			IdempotencyKey: "send-1",
			Digest:         "digest-1",
			MessageID:      "mail-1",
			CreatedAt:      createdAt,
		}
		return nil
	})
	require.NoError(t, err)
	require.NoError(t, openedStore.Close())

	reopenedStore, err := Open(dataDirectory)
	require.NoError(t, err)
	defer reopenedStore.Close()

	require.NoError(t, reopenedStore.Read(func(state *State) error {
		mailbox, exists := state.Mailboxes["mbox-claude"]
		require.True(t, exists)
		require.Equal(t, "claude1234@fabrica", mailbox.Address)
		require.Equal(t, []string{"claude0001@fabrica"}, mailbox.Aliases)
		require.Equal(t, 1, mailbox.UnreadCount)

		message, exists := state.MailMessages["mail-1"]
		require.True(t, exists)
		require.Equal(t, int64(1), message.Sequence)
		require.Equal(t, "codex345@bunker", message.FromAddress)
		require.Equal(t, "claude1234@fabrica", message.ToAddress)
		require.Equal(t, "Please review commit abc1234.", message.Body)

		receipt, exists := state.MailReceipts["mrec-1"]
		require.True(t, exists)
		require.Equal(t, "mail-1", receipt.MessageID)
		return nil
	}))
}

func TestStoreMutationRollbackIntegrity(t *testing.T) {
	dataDirectory := t.TempDir()
	openedStore, err := Open(dataDirectory)
	require.NoError(t, err)
	defer openedStore.Close()

	initialTimestamp := "2026-10-31T12:00:00Z"
	_, err = openedStore.Mutate(func(state *State, mutation *Mutation) error {
		state.Threads["thread-initial"] = protocol.Thread{
			ID:        "thread-initial",
			Title:     "Initial Thread Title",
			Status:    "open",
			CreatedAt: initialTimestamp,
			UpdatedAt: initialTimestamp,
		}
		state.Tasks["task-initial"] = protocol.Task{
			ID:        "task-initial",
			ThreadID:  "thread-initial",
			Title:     "Initial Task",
			Status:    "ready",
			CreatedAt: initialTimestamp,
			UpdatedAt: initialTimestamp,
		}
		return nil
	})
	require.NoError(t, err)

	commitCallbackInvoked := false
	simulatedError := errors.New("simulated mutator failure")

	_, err = openedStore.Mutate(func(state *State, mutation *Mutation) error {
		state.Threads["thread-initial"] = protocol.Thread{
			ID:        "thread-initial",
			Title:     "Corrupted Thread Title",
			Status:    "closed",
			CreatedAt: initialTimestamp,
			UpdatedAt: initialTimestamp,
		}
		delete(state.Tasks, "task-initial")
		state.Tasks["task-corrupted"] = protocol.Task{
			ID:        "task-corrupted",
			ThreadID:  "thread-initial",
			Title:     "Should Not Exist",
			Status:    "running",
			CreatedAt: initialTimestamp,
			UpdatedAt: initialTimestamp,
		}
		mutation.OnCommit(func() {
			commitCallbackInvoked = true
		})
		return simulatedError
	})
	require.ErrorIs(t, err, simulatedError)
	assert.False(t, commitCallbackInvoked, "mutation.OnCommit callback must not be invoked on failure")

	require.NoError(t, openedStore.Read(func(state *State) error {
		thread, threadFound := state.Threads["thread-initial"]
		assert.True(t, threadFound)
		assert.Equal(t, "Initial Thread Title", thread.Title)
		assert.Equal(t, "open", thread.Status)

		task, taskFound := state.Tasks["task-initial"]
		assert.True(t, taskFound)
		assert.Equal(t, "Initial Task", task.Title)

		_, corruptedFound := state.Tasks["task-corrupted"]
		assert.False(t, corruptedFound)
		return nil
	}))

	require.NoError(t, openedStore.Close())

	reopenedStore, err := Open(dataDirectory)
	require.NoError(t, err)
	defer reopenedStore.Close()

	require.NoError(t, reopenedStore.Read(func(state *State) error {
		thread, threadFound := state.Threads["thread-initial"]
		assert.True(t, threadFound)
		assert.Equal(t, "Initial Thread Title", thread.Title)

		task, taskFound := state.Tasks["task-initial"]
		assert.True(t, taskFound)
		assert.Equal(t, "Initial Task", task.Title)

		_, corruptedFound := state.Tasks["task-corrupted"]
		assert.False(t, corruptedFound)
		return nil
	}))
}

func TestStoreEntityDeletionPersistence(t *testing.T) {
	dataDirectory := t.TempDir()
	openedStore, err := Open(dataDirectory)
	require.NoError(t, err)

	timestamp := "2026-10-31T12:00:00Z"
	taskID := "task-to-delete"
	runID := "run-to-delete"
	leaseID := "lease-to-delete"

	_, err = openedStore.Mutate(func(state *State, mutation *Mutation) error {
		state.Threads["thread-1"] = protocol.Thread{
			ID:        "thread-1",
			Title:     "Thread",
			Status:    "open",
			CreatedAt: timestamp,
			UpdatedAt: timestamp,
		}
		state.Tasks[taskID] = protocol.Task{
			ID:        taskID,
			ThreadID:  "thread-1",
			Title:     "Task To Delete",
			Status:    "ready",
			CreatedAt: timestamp,
			UpdatedAt: timestamp,
		}
		state.Runs[runID] = protocol.Run{
			ID:        runID,
			TaskID:    &taskID,
			ThreadID:  "thread-1",
			Status:    "running",
			NodeID:    "node-1",
			HarnessID: "harness-1",
			CreatedAt: timestamp,
		}
		state.WorkspaceLeases[leaseID] = protocol.WorkspaceLease{
			ID:           leaseID,
			ThreadID:     "thread-1",
			TaskID:       taskID,
			RunID:        runID,
			NodeID:       "node-1",
			Status:       "active",
			Root:         "/workspaces/project",
			WorktreePath: "/workspaces/project/worktree",
		}
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, openedStore.Read(func(state *State) error {
		_, taskExists := state.Tasks[taskID]
		assert.True(t, taskExists)
		_, runExists := state.Runs[runID]
		assert.True(t, runExists)
		_, leaseExists := state.WorkspaceLeases[leaseID]
		assert.True(t, leaseExists)
		return nil
	}))

	_, err = openedStore.Mutate(func(state *State, mutation *Mutation) error {
		delete(state.Tasks, taskID)
		delete(state.Runs, runID)
		delete(state.WorkspaceLeases, leaseID)
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, openedStore.Read(func(state *State) error {
		_, taskExists := state.Tasks[taskID]
		assert.False(t, taskExists)
		_, runExists := state.Runs[runID]
		assert.False(t, runExists)
		_, leaseExists := state.WorkspaceLeases[leaseID]
		assert.False(t, leaseExists)
		return nil
	}))

	require.NoError(t, openedStore.Close())

	reopenedStore, err := Open(dataDirectory)
	require.NoError(t, err)
	defer reopenedStore.Close()

	require.NoError(t, reopenedStore.Read(func(state *State) error {
		_, taskExists := state.Tasks[taskID]
		assert.False(t, taskExists, "deleted task must not reappear after reopen")
		_, runExists := state.Runs[runID]
		assert.False(t, runExists, "deleted run must not reappear after reopen")
		_, leaseExists := state.WorkspaceLeases[leaseID]
		assert.False(t, leaseExists, "deleted workspace lease must not reappear after reopen")
		return nil
	}))
}

func TestStorePaginationAndSubscriberBackpressure(t *testing.T) {
	dataDirectory := t.TempDir()
	openedStore, err := Open(dataDirectory)
	require.NoError(t, err)
	defer openedStore.Close()

	threadID := "thread-pagination-1"
	timestamp := "2026-10-31T12:00:00Z"

	_, err = openedStore.Mutate(func(state *State, mutation *Mutation) error {
		state.Threads[threadID] = protocol.Thread{
			ID:        threadID,
			Title:     "Pagination Thread",
			Status:    "open",
			CreatedAt: timestamp,
			UpdatedAt: timestamp,
		}
		return nil
	})
	require.NoError(t, err)

	_, err = openedStore.Mutate(func(state *State, mutation *Mutation) error {
		for eventIndex := 1; eventIndex <= 20; eventIndex++ {
			mutation.EmitEvent(protocol.TaskEventEntry{
				ThreadID:   threadID,
				Type:       "progress.tick",
				Summary:    fmt.Sprintf("Event %d", eventIndex),
				OccurredAt: timestamp,
			})
		}
		return nil
	})
	require.NoError(t, err)

	firstPage, hasMore, err := openedStore.ReadThreadEvents(threadID, 1, 5)
	require.NoError(t, err)
	assert.True(t, hasMore)
	require.Len(t, firstPage, 5)
	for index, entry := range firstPage {
		assert.Equal(t, int64(index+2), entry.Sequence)
		assert.Equal(t, fmt.Sprintf("Event %d", index+1), entry.Summary)
	}

	secondPage, hasMore, err := openedStore.ReadThreadEvents(threadID, 6, 5)
	require.NoError(t, err)
	assert.True(t, hasMore)
	require.Len(t, secondPage, 5)
	for index, entry := range secondPage {
		assert.Equal(t, int64(index+7), entry.Sequence)
		assert.Equal(t, fmt.Sprintf("Event %d", index+6), entry.Summary)
	}

	fourthPage, hasMore, err := openedStore.ReadThreadEvents(threadID, 16, 5)
	require.NoError(t, err)
	assert.False(t, hasMore)
	require.Len(t, fourthPage, 5)
	for index, entry := range fourthPage {
		assert.Equal(t, int64(index+17), entry.Sequence)
		assert.Equal(t, fmt.Sprintf("Event %d", index+16), entry.Summary)
	}

	emptyPage, hasMore, err := openedStore.ReadThreadEvents(threadID, 21, 5)
	require.NoError(t, err)
	assert.False(t, hasMore)
	assert.Empty(t, emptyPage)

	subscriberChannel, unsubscribe := openedStore.SubscribeEvents()
	defer unsubscribe()

	for iteration := 1; iteration <= 100; iteration++ {
		_, mutateErr := openedStore.Mutate(func(state *State, mutation *Mutation) error {
			mutation.EmitEvent(protocol.TaskEventEntry{
				ThreadID:   threadID,
				Type:       "backpressure.test",
				Summary:    fmt.Sprintf("Backpressure message %d", iteration),
				OccurredAt: timestamp,
			})
			return nil
		})
		require.NoError(t, mutateErr)
	}

	assert.NotEmpty(t, subscriberChannel)
}
