package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/safepath"
)

type roomMirrorUpdate struct {
	head          protocol.ThreadRoomHead
	allMessages   []protocol.RoomMessage
	previousCount int
}

func isSafeRoomDirectoryName(directoryName string) bool {
	if !strings.HasPrefix(directoryName, "room_") || !protocol.IsIdentifier(directoryName) {
		return false
	}
	return !strings.Contains(directoryName, "/") &&
		!strings.Contains(directoryName, "\\") &&
		!strings.Contains(directoryName, "..")
}

func collectRoomDiff(
	previous *State,
	next *State,
) (map[string]protocol.ThreadRoomHead, map[string][]protocol.RoomMessage, []roomMirrorUpdate, error) {
	changedHeads := map[string]protocol.ThreadRoomHead{}
	newMessages := map[string][]protocol.RoomMessage{}
	var mirrorUpdates []roomMirrorUpdate

	for _, threadID := range sortedKeys(next.RoomHeads) {
		nextHead := next.RoomHeads[threadID]
		if !isSafeRoomDirectoryName(nextHead.DirectoryName) {
			return nil, nil, nil, fmt.Errorf("unsafe room directory name %q", nextHead.DirectoryName)
		}
		previousHead, hadPreviousHead := previous.RoomHeads[threadID]
		previousSlice := previous.RoomMessages[threadID]
		nextSlice := next.RoomMessages[threadID]
		if len(nextSlice) < len(previousSlice) {
			return nil, nil, nil, fmt.Errorf("room messages for thread %s cannot shrink (%d < %d)", threadID, len(nextSlice), len(previousSlice))
		}

		appended := nextSlice[len(previousSlice):]
		for offset, entry := range appended {
			expectedSequence := int64(len(previousSlice) + offset + 1)
			if entry.Sequence != expectedSequence {
				return nil, nil, nil, fmt.Errorf("room message sequence mismatch in thread %s: got %d, want %d", threadID, entry.Sequence, expectedSequence)
			}
		}

		if !hadPreviousHead || previousHead != nextHead || len(appended) > 0 {
			changedHeads[threadID] = nextHead
			if len(appended) > 0 {
				clonedAppended := make([]protocol.RoomMessage, len(appended))
				copy(clonedAppended, appended)
				newMessages[threadID] = clonedAppended
			}
			clonedAll := make([]protocol.RoomMessage, len(nextSlice))
			copy(clonedAll, nextSlice)
			mirrorUpdates = append(mirrorUpdates, roomMirrorUpdate{
				head:          nextHead,
				allMessages:   clonedAll,
				previousCount: len(previousSlice),
			})
		}
	}

	return changedHeads, newMessages, mirrorUpdates, nil
}

func (store *Store) verifyRoomMirrorTarget(head protocol.ThreadRoomHead) error {
	if !isSafeRoomDirectoryName(head.DirectoryName) {
		return fmt.Errorf("unsafe room directory name %q", head.DirectoryName)
	}
	if _, err := safepath.VerifyDirectoryWithinRoot(store.dataDirectory, store.roomsDirectory); err != nil {
		return fmt.Errorf("verify rooms root: %w", err)
	}
	roomDirectory := filepath.Join(store.roomsDirectory, head.DirectoryName)
	if information, err := os.Lstat(roomDirectory); err == nil {
		if err := safepath.RejectUnsafeAncestor(roomDirectory, information); err != nil {
			return err
		}
		for _, fileName := range []string{"room.json", "messages.jsonl"} {
			candidatePath := filepath.Join(roomDirectory, fileName)
			if _, statErr := os.Lstat(candidatePath); statErr == nil {
				if _, _, verifyErr := safepath.VerifyRegularFileWithinRoot(store.roomsDirectory, candidatePath); verifyErr != nil {
					return verifyErr
				}
			} else if !errors.Is(statErr, fs.ErrNotExist) {
				return statErr
			}
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func (store *Store) reconcileAllRooms() error {
	var updates []roomMirrorUpdate
	for _, threadID := range sortedKeys(store.state.RoomHeads) {
		head := store.state.RoomHeads[threadID]
		messages := store.state.RoomMessages[threadID]
		clonedMessages := make([]protocol.RoomMessage, len(messages))
		copy(clonedMessages, messages)
		updates = append(updates, roomMirrorUpdate{
			head:          head,
			allMessages:   clonedMessages,
			previousCount: 0,
		})
	}
	for _, update := range updates {
		if err := store.syncRoomMirror(update, true); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) expandDirtyRoomMirrorUpdates(initialUpdates []roomMirrorUpdate) []roomMirrorUpdate {
	queuedThreadIDs := make(map[string]bool, len(initialUpdates))
	for _, update := range initialUpdates {
		queuedThreadIDs[update.head.ThreadID] = true
	}

	store.roomMirrorMutex.Lock()
	dirtyThreadIDs := sortedKeys(store.dirtyRoomMirrors)
	store.roomMirrorMutex.Unlock()

	if len(dirtyThreadIDs) == 0 {
		return initialUpdates
	}

	expanded := make([]roomMirrorUpdate, 0, len(initialUpdates)+len(dirtyThreadIDs))
	expanded = append(expanded, initialUpdates...)
	for _, threadID := range dirtyThreadIDs {
		if queuedThreadIDs[threadID] {
			continue
		}
		head, exists := store.state.RoomHeads[threadID]
		if !exists {
			continue
		}
		messages := store.state.RoomMessages[threadID]
		clonedMessages := make([]protocol.RoomMessage, len(messages))
		copy(clonedMessages, messages)
		expanded = append(expanded, roomMirrorUpdate{
			head:          head,
			allMessages:   clonedMessages,
			previousCount: 0,
		})
	}
	return expanded
}

func (store *Store) syncRoomMirror(update roomMirrorUpdate, forceReconcile bool) (syncError error) {
	store.roomMirrorMutex.Lock()
	defer func() {
		if syncError != nil {
			delete(store.mirroredRoomCounts, update.head.ThreadID)
			delete(store.mirroredRoomSizes, update.head.ThreadID)
			store.dirtyRoomMirrors[update.head.ThreadID] = true
		} else {
			delete(store.dirtyRoomMirrors, update.head.ThreadID)
		}
		store.roomMirrorMutex.Unlock()
	}()

	if store.dirtyRoomMirrors[update.head.ThreadID] {
		forceReconcile = true
	}

	if err := store.verifyRoomMirrorTarget(update.head); err != nil {
		return err
	}

	roomDirectory, err := safepath.EnsureDirectoryWithinRoot(
		store.roomsDirectory,
		filepath.Join(store.roomsDirectory, update.head.DirectoryName),
		0o700,
	)
	if err != nil {
		return fmt.Errorf("prepare room directory: %w", err)
	}
	if err := os.Chmod(roomDirectory, 0o700); err != nil {
		return fmt.Errorf("chmod room directory: %w", err)
	}

	metadataBytes, err := json.MarshalIndent(update.head, "", "  ")
	if err != nil {
		return fmt.Errorf("encode room metadata: %w", err)
	}
	metadataBytes = append(metadataBytes, '\n')
	roomMetadataPath := filepath.Join(roomDirectory, "room.json")
	if err := writeAtomicRegularFile(store.roomsDirectory, roomDirectory, roomMetadataPath, metadataBytes); err != nil {
		return fmt.Errorf("write room metadata: %w", err)
	}

	messagesPath := filepath.Join(roomDirectory, "messages.jsonl")
	if !forceReconcile {
		expectedCount, hasCount := store.mirroredRoomCounts[update.head.ThreadID]
		expectedSize := store.mirroredRoomSizes[update.head.ThreadID]
		if hasCount && expectedCount == update.previousCount {
			verifiedPath, information, verifyErr := safepath.VerifyRegularFileWithinRoot(store.roomsDirectory, messagesPath)
			if verifyErr == nil && information.Size() == expectedSize {
				appended := update.allMessages[update.previousCount:]
				if len(appended) == 0 {
					if information.Mode().Perm() != 0o600 {
						if chmodErr := os.Chmod(verifiedPath, 0o600); chmodErr != nil {
							return chmodErr
						}
					}
					return nil
				}
				encodedDelta, encodeErr := encodeRoomMessagesJSONL(appended)
				if encodeErr != nil {
					return encodeErr
				}
				if appendErr := appendVerifiedRegularFile(verifiedPath, information, encodedDelta); appendErr == nil {
					store.mirroredRoomCounts[update.head.ThreadID] = len(update.allMessages)
					store.mirroredRoomSizes[update.head.ThreadID] = expectedSize + int64(len(encodedDelta))
					return nil
				}
			}
		}
	}

	expectedBytes, err := encodeRoomMessagesJSONL(update.allMessages)
	if err != nil {
		return err
	}

	if _, statErr := os.Lstat(messagesPath); statErr == nil {
		verifiedPath, information, verifyErr := safepath.VerifyRegularFileWithinRoot(store.roomsDirectory, messagesPath)
		if verifyErr != nil {
			return verifyErr
		}
		if information.Size() == int64(len(expectedBytes)) {
			existingBytes, readErr := os.ReadFile(verifiedPath)
			if readErr == nil && bytes.Equal(existingBytes, expectedBytes) {
				if information.Mode().Perm() != 0o600 {
					if chmodErr := os.Chmod(verifiedPath, 0o600); chmodErr != nil {
						return chmodErr
					}
				}
				store.mirroredRoomCounts[update.head.ThreadID] = len(update.allMessages)
				store.mirroredRoomSizes[update.head.ThreadID] = int64(len(expectedBytes))
				return nil
			}
		}
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return statErr
	}

	if err := writeAtomicRegularFile(store.roomsDirectory, roomDirectory, messagesPath, expectedBytes); err != nil {
		return fmt.Errorf("write room messages jsonl: %w", err)
	}
	store.mirroredRoomCounts[update.head.ThreadID] = len(update.allMessages)
	store.mirroredRoomSizes[update.head.ThreadID] = int64(len(expectedBytes))
	return nil
}

func encodeRoomMessagesJSONL(messages []protocol.RoomMessage) ([]byte, error) {
	if len(messages) == 0 {
		return []byte{}, nil
	}
	var buffer bytes.Buffer
	for _, message := range messages {
		lineBytes, err := json.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("encode room message %s: %w", message.ID, err)
		}
		buffer.Write(lineBytes)
		buffer.WriteByte('\n')
	}
	return buffer.Bytes(), nil
}

func appendVerifiedRegularFile(verifiedPath string, expectedInfo fs.FileInfo, delta []byte) error {
	file, err := os.OpenFile(verifiedPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	cleanupClose := true
	defer func() {
		if cleanupClose {
			_ = file.Close()
		}
	}()
	openedInfo, err := file.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(expectedInfo, openedInfo) || !openedInfo.Mode().IsRegular() {
		return errors.New("room messages file changed unexpectedly before append")
	}
	if openedInfo.Mode().Perm() != 0o600 {
		if err := file.Chmod(0o600); err != nil {
			return err
		}
	}
	written, err := file.Write(delta)
	if err != nil {
		return err
	}
	if written != len(delta) {
		return errors.New("short write appending room messages")
	}
	if err := file.Sync(); err != nil {
		return err
	}
	cleanupClose = false
	return file.Close()
}

func writeAtomicRegularFile(rootDirectory string, parentDirectory string, destinationPath string, content []byte) error {
	if _, statErr := os.Lstat(destinationPath); statErr == nil {
		if _, _, verifyErr := safepath.VerifyRegularFileWithinRoot(rootDirectory, destinationPath); verifyErr != nil {
			return verifyErr
		}
	} else if !errors.Is(statErr, fs.ErrNotExist) {
		return statErr
	}

	temporaryFile, err := os.CreateTemp(parentDirectory, ".room-*")
	if err != nil {
		return err
	}
	temporaryPath := temporaryFile.Name()
	cleanupTemporary := true
	defer func() {
		if cleanupTemporary {
			_ = temporaryFile.Close()
			_ = os.Remove(temporaryPath)
		}
	}()

	if len(content) > 0 {
		written, writeErr := temporaryFile.Write(content)
		if writeErr != nil {
			return writeErr
		}
		if written != len(content) {
			return errors.New("short write staging room file")
		}
	}
	if err := temporaryFile.Chmod(0o600); err != nil {
		return err
	}
	if err := temporaryFile.Sync(); err != nil {
		return err
	}
	if err := temporaryFile.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, destinationPath); err != nil {
		return err
	}
	cleanupTemporary = false
	return nil
}

func (store *Store) ReconcileRoomMirror(threadID string) error {
	store.mutex.RLock()
	defer store.mutex.RUnlock()
	if store.database == nil {
		return ErrStoreClosed
	}
	head, exists := store.state.RoomHeads[threadID]
	if !exists {
		return nil
	}
	messages := store.state.RoomMessages[threadID]
	clonedMessages := make([]protocol.RoomMessage, len(messages))
	copy(clonedMessages, messages)
	return store.syncRoomMirror(roomMirrorUpdate{
		head:          head,
		allMessages:   clonedMessages,
		previousCount: 0,
	}, true)
}

func (store *Store) RoomJSONLPath(threadID string) (string, bool, error) {
	store.mutex.RLock()
	if store.database == nil {
		store.mutex.RUnlock()
		return "", false, ErrStoreClosed
	}
	head, exists := store.state.RoomHeads[threadID]
	if !exists {
		store.mutex.RUnlock()
		return "", false, nil
	}
	if !isSafeRoomDirectoryName(head.DirectoryName) {
		store.mutex.RUnlock()
		return "", false, fmt.Errorf("unsafe room directory name %q", head.DirectoryName)
	}
	targetPath := filepath.Join(store.roomsDirectory, head.DirectoryName, "messages.jsonl")
	metadataPath := filepath.Join(store.roomsDirectory, head.DirectoryName, "room.json")
	messages := store.state.RoomMessages[threadID]

	store.roomMirrorMutex.Lock()
	dirty := store.dirtyRoomMirrors[threadID]
	expectedCount, hasCount := store.mirroredRoomCounts[threadID]
	expectedSize := store.mirroredRoomSizes[threadID]
	store.roomMirrorMutex.Unlock()

	needsReconcile := dirty || !hasCount || expectedCount != len(messages)
	if !needsReconcile {
		if targetInfo, err := os.Lstat(targetPath); err != nil || !targetInfo.Mode().IsRegular() || targetInfo.Size() != expectedSize {
			needsReconcile = true
		} else if metadataInfo, metadataErr := os.Lstat(metadataPath); metadataErr != nil || !metadataInfo.Mode().IsRegular() {
			needsReconcile = true
		}
	}
	if needsReconcile {
		clonedMessages := make([]protocol.RoomMessage, len(messages))
		copy(clonedMessages, messages)
		if reconcileErr := store.syncRoomMirror(roomMirrorUpdate{
			head:          head,
			allMessages:   clonedMessages,
			previousCount: 0,
		}, true); reconcileErr != nil {
			store.mutex.RUnlock()
			return "", false, reconcileErr
		}
	}
	store.mutex.RUnlock()

	if _, _, err := safepath.VerifyRegularFileWithinRoot(store.roomsDirectory, metadataPath); err != nil {
		return "", false, err
	}
	verifiedPath, _, err := safepath.VerifyRegularFileWithinRoot(store.roomsDirectory, targetPath)
	if err != nil {
		return "", false, err
	}
	return verifiedPath, true, nil
}
