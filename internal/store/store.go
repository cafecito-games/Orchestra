package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/safepath"
)

var (
	ErrStoreClosed            = errors.New("store is closed")
	ErrArtifactSizeMismatch   = errors.New("artifact upload byte size did not match declared sizeBytes")
	ErrArtifactDigestMismatch = errors.New("artifact upload SHA-256 digest did not match declared sha256")
)

const (
	collectionNodes               = "nodes"
	collectionNodeUpdates         = "node_updates"
	collectionCapabilityReports   = "capability_reports"
	collectionComponentReports    = "component_reports"
	collectionRepositoryReports   = "repository_reports"
	collectionProjectProfiles     = "project_profiles"
	collectionThreads             = "threads"
	collectionAttachments         = "attachments"
	collectionTasks               = "tasks"
	collectionTaskSubmissions     = "task_submissions"
	collectionTaskUpdates         = "task_updates"
	collectionInstances           = "instances"
	collectionAllocations         = "allocations"
	collectionInstanceReceipts    = "instance_receipts"
	collectionReleaseIntents      = "release_intents"
	collectionInstanceDeliveries  = "instance_deliveries"
	collectionRuns                = "runs"
	collectionSessionBindings     = "session_bindings"
	collectionWorkspaceLeases     = "workspace_leases"
	collectionApprovals           = "approvals"
	collectionArtifacts           = "artifacts"
	collectionArtifactGrants      = "artifact_grants"
	collectionMessages            = "messages"
	collectionMessageAcks         = "message_acks"
	collectionRunActivities       = "run_activities"
	collectionRunTranscripts      = "run_transcripts"
	collectionRoomMessageReceipts = "room_message_receipts"
	collectionRoomNudgeReceipts   = "room_nudge_receipts"
	collectionMailboxes           = "mailboxes"
	collectionMailMessages        = "mail_messages"
	collectionMailReceipts        = "mail_receipts"
)

type Mutation struct {
	explicitEvents []protocol.TaskEventEntry
	commitHooks    []func()
}

func (mutation *Mutation) EmitEvent(entry protocol.TaskEventEntry) {
	mutation.explicitEvents = append(mutation.explicitEvents, entry)
}

func (mutation *Mutation) OnCommit(callback func()) {
	if callback != nil {
		mutation.commitHooks = append(mutation.commitHooks, callback)
	}
}

type Store struct {
	dataDirectory      string
	artifactsDirectory string
	roomsDirectory     string
	database           *sql.DB

	mutex      sync.RWMutex
	state      *State
	serialized map[string]map[string]string
	now        func() time.Time

	roomMirrorMutex    sync.Mutex
	mirroredRoomCounts map[string]int
	mirroredRoomSizes  map[string]int64
	dirtyRoomMirrors   map[string]bool

	subscriberSequence atomic.Int64
	subscriberMutex    sync.Mutex
	subscribers        map[int64]chan []protocol.TaskEventEntry
}

func Open(dataDirectory string) (*Store, error) {
	if strings.TrimSpace(dataDirectory) == "" {
		return nil, errors.New("store data directory must not be empty")
	}
	absoluteRoot, err := filepath.Abs(dataDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory: %w", err)
	}
	if err := os.MkdirAll(absoluteRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	canonicalRoot, err := filepath.EvalSymlinks(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("canonicalize data directory: %w", err)
	}
	artifactsDirectory, err := safepath.EnsureDirectoryWithinRoot(canonicalRoot, filepath.Join(canonicalRoot, "artifacts"), 0o700)
	if err != nil {
		return nil, fmt.Errorf("prepare artifacts directory: %w", err)
	}
	roomsDirectory, err := safepath.EnsureDirectoryWithinRoot(canonicalRoot, filepath.Join(canonicalRoot, "rooms"), 0o700)
	if err != nil {
		return nil, fmt.Errorf("prepare rooms directory: %w", err)
	}
	if err := os.Chmod(roomsDirectory, 0o700); err != nil {
		return nil, fmt.Errorf("chmod rooms directory: %w", err)
	}

	databasePath := filepath.Join(canonicalRoot, "orchestra.db")
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)

	if err := initializeDatabase(database); err != nil {
		_ = database.Close()
		return nil, err
	}

	store := &Store{
		dataDirectory:      canonicalRoot,
		artifactsDirectory: artifactsDirectory,
		roomsDirectory:     roomsDirectory,
		database:           database,
		state:              NewState(),
		serialized:         map[string]map[string]string{},
		now:                time.Now,
		mirroredRoomCounts: map[string]int{},
		mirroredRoomSizes:  map[string]int64{},
		dirtyRoomMirrors:   map[string]bool{},
		subscribers:        map[int64]chan []protocol.TaskEventEntry{},
	}
	if err := store.loadFromDatabase(); err != nil {
		_ = database.Close()
		return nil, err
	}
	if err := store.reconcileAllRooms(); err != nil {
		_ = database.Close()
		return nil, err
	}
	return store, nil
}

func (store *Store) Close() error {
	store.subscriberMutex.Lock()
	for subscriberID, channel := range store.subscribers {
		delete(store.subscribers, subscriberID)
		close(channel)
	}
	store.subscriberMutex.Unlock()

	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.database == nil {
		return nil
	}
	err := store.database.Close()
	store.database = nil
	return err
}

func (store *Store) DataDirectory() string {
	return store.dataDirectory
}

func (store *Store) JournalMode() (string, error) {
	store.mutex.RLock()
	defer store.mutex.RUnlock()
	var mode string
	err := store.database.QueryRowContext(context.Background(), `PRAGMA journal_mode;`).Scan(&mode)
	return strings.ToLower(strings.TrimSpace(mode)), err
}

func (store *Store) Read(reader func(state *State) error) error {
	store.mutex.RLock()
	defer store.mutex.RUnlock()
	if store.database == nil {
		return ErrStoreClosed
	}
	return reader(store.state)
}

func (store *Store) Mutate(mutator func(state *State, mutation *Mutation) error) ([]protocol.TaskEventEntry, error) {
	store.mutex.Lock()
	if store.database == nil {
		store.mutex.Unlock()
		return nil, ErrStoreClosed
	}
	working := store.state.Clone()
	mutation := &Mutation{}
	if err := mutator(working, mutation); err != nil {
		store.mutex.Unlock()
		return nil, err
	}
	working.ensureInitialized()
	working.ensureRoomHeads()

	changedRoomHeads, newRoomMessages, mirrorUpdates, err := collectRoomDiff(store.state, working)
	if err != nil {
		store.mutex.Unlock()
		return nil, err
	}

	occurredAt := store.now().UTC().Format(time.RFC3339Nano)
	committedEvents, prunedBelow := synthesizeThreadEvents(store.state, working, mutation.explicitEvents, occurredAt)

	nextSerialized, err := serializeCollections(working)
	if err != nil {
		store.mutex.Unlock()
		return nil, err
	}

	if err := store.commitDiff(nextSerialized, working.EventStreams, committedEvents, prunedBelow, changedRoomHeads, newRoomMessages); err != nil {
		store.mutex.Unlock()
		return nil, err
	}

	store.state = working
	store.serialized = nextSerialized
	updatesToSync := store.expandDirtyRoomMirrorUpdates(mirrorUpdates)
	for _, mirrorUpdate := range updatesToSync {
		_ = store.syncRoomMirror(mirrorUpdate, false)
	}
	hooks := mutation.commitHooks
	store.mutex.Unlock()

	if len(committedEvents) > 0 {
		store.broadcastEvents(committedEvents)
	}
	for _, hook := range hooks {
		hook()
	}
	return committedEvents, nil
}

// ReadThreadEvents returns a slice of thread events with sequence strictly greater than
// cursorSequence, up to limit items, and a boolean indicating whether more events exist.
func (store *Store) ReadThreadEvents(threadID string, cursorSequence int64, limit int) ([]protocol.TaskEventEntry, bool, error) {
	store.mutex.RLock()
	defer store.mutex.RUnlock()
	if store.database == nil {
		return nil, false, ErrStoreClosed
	}
	allEvents := store.state.ThreadEvents[threadID]
	if limit <= 0 {
		return nil, false, nil
	}
	filtered := make([]protocol.TaskEventEntry, 0, min(limit, len(allEvents)))
	hasMore := false
	for _, entry := range allEvents {
		if entry.Sequence <= cursorSequence {
			continue
		}
		if len(filtered) >= limit {
			hasMore = true
			break
		}
		filtered = append(filtered, entry)
	}
	return filtered, hasMore, nil
}

func (store *Store) SubscribeEvents() (<-chan []protocol.TaskEventEntry, func()) {
	subscriberID := store.subscriberSequence.Add(1)
	channel := make(chan []protocol.TaskEventEntry, 64)
	store.subscriberMutex.Lock()
	store.subscribers[subscriberID] = channel
	store.subscriberMutex.Unlock()

	unsubscribe := func() {
		store.subscriberMutex.Lock()
		if existing, found := store.subscribers[subscriberID]; found {
			delete(store.subscribers, subscriberID)
			close(existing)
		}
		store.subscriberMutex.Unlock()
	}
	return channel, unsubscribe
}

func (store *Store) broadcastEvents(events []protocol.TaskEventEntry) {
	store.subscriberMutex.Lock()
	defer store.subscriberMutex.Unlock()
	for _, channel := range store.subscribers {
		select {
		case channel <- events:
		default:
		}
	}
}

func (store *Store) WriteArtifactBlob(
	artifactID string,
	expectedSize int64,
	expectedSHA256 string,
	reader io.Reader,
) error {
	if !isSafeBlobIdentifier(artifactID) {
		return fmt.Errorf("invalid artifact id %q", artifactID)
	}
	if expectedSize < 0 || expectedSize > protocol.MaximumArtifactSizeBytes {
		return fmt.Errorf("artifact size %d is out of bounds", expectedSize)
	}

	temporaryFile, err := os.CreateTemp(store.artifactsDirectory, ".upload-"+artifactID+"-*")
	if err != nil {
		return fmt.Errorf("create artifact staging file: %w", err)
	}
	temporaryPath := temporaryFile.Name()
	cleanupTemporary := true
	defer func() {
		if cleanupTemporary {
			_ = temporaryFile.Close()
			_ = os.Remove(temporaryPath)
		}
	}()

	hasher := sha256.New()
	limitedReader := io.LimitReader(reader, expectedSize+1)
	writtenBytes, copyErr := io.Copy(io.MultiWriter(temporaryFile, hasher), limitedReader)
	if copyErr != nil {
		return fmt.Errorf("write artifact staging file: %w", copyErr)
	}
	if writtenBytes != expectedSize {
		return ErrArtifactSizeMismatch
	}
	actualDigest := hex.EncodeToString(hasher.Sum(nil))
	if actualDigest != strings.ToLower(strings.TrimSpace(expectedSHA256)) {
		return ErrArtifactDigestMismatch
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

	finalPath := filepath.Join(store.artifactsDirectory, artifactID+".blob")
	if err := os.Rename(temporaryPath, finalPath); err != nil {
		return fmt.Errorf("commit artifact blob: %w", err)
	}
	cleanupTemporary = false
	return nil
}

func (store *Store) OpenArtifactBlob(artifactID string) (*os.File, int64, error) {
	if !isSafeBlobIdentifier(artifactID) {
		return nil, 0, fmt.Errorf("invalid artifact id %q", artifactID)
	}
	blobPath := filepath.Join(store.artifactsDirectory, artifactID+".blob")
	verifiedPath, information, err := safepath.VerifyRegularFileWithinRoot(store.artifactsDirectory, blobPath)
	if err != nil {
		return nil, 0, err
	}
	file, err := os.Open(verifiedPath)
	if err != nil {
		return nil, 0, err
	}
	return file, information.Size(), nil
}

func isSafeBlobIdentifier(value string) bool {
	if !protocol.IsIdentifier(value) {
		return false
	}
	return !strings.Contains(value, "/") && !strings.Contains(value, "\\") && !strings.Contains(value, "..")
}

func initializeDatabase(database *sql.DB) error {
	pragmas := []string{
		`PRAGMA journal_mode = WAL;`,
		`PRAGMA synchronous = FULL;`,
		`PRAGMA foreign_keys = ON;`,
		`PRAGMA busy_timeout = 5000;`,
	}
	for _, statement := range pragmas {
		if _, err := database.ExecContext(context.Background(), statement); err != nil {
			return fmt.Errorf("configure sqlite (%s): %w", statement, err)
		}
	}

	schema := `
CREATE TABLE IF NOT EXISTS kv_records (
	collection TEXT NOT NULL,
	record_id TEXT NOT NULL,
	payload_json TEXT NOT NULL,
	PRIMARY KEY (collection, record_id)
);

CREATE TABLE IF NOT EXISTS thread_event_heads (
	thread_id TEXT PRIMARY KEY,
	head_sequence INTEGER NOT NULL,
	floor_sequence INTEGER NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS thread_events (
	thread_id TEXT NOT NULL,
	sequence INTEGER NOT NULL,
	event_type TEXT NOT NULL,
	occurred_at TEXT NOT NULL,
	payload_json TEXT NOT NULL,
	PRIMARY KEY (thread_id, sequence)
);

CREATE TABLE IF NOT EXISTS thread_room_heads (
	thread_id TEXT PRIMARY KEY,
	directory_name TEXT NOT NULL,
	head_sequence INTEGER NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS thread_room_messages (
	thread_id TEXT NOT NULL,
	sequence INTEGER NOT NULL,
	message_id TEXT NOT NULL,
	kind TEXT NOT NULL,
	created_at TEXT NOT NULL,
	payload_json TEXT NOT NULL,
	PRIMARY KEY (thread_id, sequence)
);
`
	if _, err := database.ExecContext(context.Background(), schema); err != nil {
		return fmt.Errorf("initialize sqlite schema: %w", err)
	}
	return nil
}

func (store *Store) loadFromDatabase() error {
	loaded := NewState()
	serialized := map[string]map[string]string{}

	rows, err := store.database.QueryContext(context.Background(), `SELECT collection, record_id, payload_json FROM kv_records ORDER BY collection, record_id`)
	if err != nil {
		return fmt.Errorf("query kv_records: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var collection, recordID, payload string
		if scanErr := rows.Scan(&collection, &recordID, &payload); scanErr != nil {
			return scanErr
		}
		if serialized[collection] == nil {
			serialized[collection] = map[string]string{}
		}
		serialized[collection][recordID] = payload
		if decodeErr := decodeRecordIntoState(loaded, collection, recordID, []byte(payload)); decodeErr != nil {
			return fmt.Errorf("decode %s/%s: %w", collection, recordID, decodeErr)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	headRows, err := store.database.QueryContext(context.Background(), `SELECT thread_id, head_sequence, floor_sequence, updated_at FROM thread_event_heads`)
	if err != nil {
		return fmt.Errorf("query thread_event_heads: %w", err)
	}
	defer headRows.Close()
	for headRows.Next() {
		var stream protocol.ThreadEventStream
		if scanErr := headRows.Scan(&stream.ThreadID, &stream.Head, &stream.Floor, &stream.UpdatedAt); scanErr != nil {
			return scanErr
		}
		loaded.EventStreams[stream.ThreadID] = stream
	}
	if err := headRows.Err(); err != nil {
		return err
	}

	eventRows, err := store.database.QueryContext(context.Background(), `SELECT thread_id, payload_json FROM thread_events ORDER BY thread_id, sequence ASC`)
	if err != nil {
		return fmt.Errorf("query thread_events: %w", err)
	}
	defer eventRows.Close()
	for eventRows.Next() {
		var threadID, payload string
		if scanErr := eventRows.Scan(&threadID, &payload); scanErr != nil {
			return scanErr
		}
		var entry protocol.TaskEventEntry
		if decodeErr := json.Unmarshal([]byte(payload), &entry); decodeErr != nil {
			return fmt.Errorf("decode thread event in %s: %w", threadID, decodeErr)
		}
		loaded.ThreadEvents[threadID] = append(loaded.ThreadEvents[threadID], entry)
	}
	if err := eventRows.Err(); err != nil {
		return err
	}

	roomHeadRows, err := store.database.QueryContext(context.Background(), `SELECT thread_id, directory_name, head_sequence, created_at, updated_at FROM thread_room_heads`)
	if err != nil {
		return fmt.Errorf("query thread_room_heads: %w", err)
	}
	defer roomHeadRows.Close()
	for roomHeadRows.Next() {
		var head protocol.ThreadRoomHead
		if scanErr := roomHeadRows.Scan(&head.ThreadID, &head.DirectoryName, &head.HeadSequence, &head.CreatedAt, &head.UpdatedAt); scanErr != nil {
			return scanErr
		}
		loaded.RoomHeads[head.ThreadID] = head
	}
	if err := roomHeadRows.Err(); err != nil {
		return err
	}

	roomMessageRows, err := store.database.QueryContext(context.Background(), `SELECT thread_id, payload_json FROM thread_room_messages ORDER BY thread_id, sequence ASC`)
	if err != nil {
		return fmt.Errorf("query thread_room_messages: %w", err)
	}
	defer roomMessageRows.Close()
	for roomMessageRows.Next() {
		var threadID, payload string
		if scanErr := roomMessageRows.Scan(&threadID, &payload); scanErr != nil {
			return scanErr
		}
		var message protocol.RoomMessage
		if decodeErr := json.Unmarshal([]byte(payload), &message); decodeErr != nil {
			return fmt.Errorf("decode room message in %s: %w", threadID, decodeErr)
		}
		loaded.RoomMessages[threadID] = append(loaded.RoomMessages[threadID], message)
	}
	if err := roomMessageRows.Err(); err != nil {
		return err
	}

	loaded.ensureRoomHeads()
	for _, threadID := range sortedKeys(loaded.RoomHeads) {
		head := loaded.RoomHeads[threadID]
		if _, execErr := store.database.ExecContext(context.Background(), `
INSERT INTO thread_room_heads (thread_id, directory_name, head_sequence, created_at, updated_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
	directory_name = excluded.directory_name,
	head_sequence = excluded.head_sequence,
	created_at = excluded.created_at,
	updated_at = excluded.updated_at
`, head.ThreadID, head.DirectoryName, head.HeadSequence, head.CreatedAt, head.UpdatedAt); execErr != nil {
			return fmt.Errorf("backfill thread_room_heads for %s: %w", threadID, execErr)
		}
	}

	store.state = loaded
	store.serialized = serialized
	return nil
}

func (store *Store) commitDiff(
	nextSerialized map[string]map[string]string,
	eventStreams map[string]protocol.ThreadEventStream,
	newEvents []protocol.TaskEventEntry,
	prunedBelow map[string]int64,
	changedRoomHeads map[string]protocol.ThreadRoomHead,
	newRoomMessages map[string][]protocol.RoomMessage,
) error {
	operationContext := context.Background()
	transaction, err := store.database.BeginTx(operationContext, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite transaction: %w", err)
	}
	defer func() {
		_ = transaction.Rollback()
	}()

	upsertStatement, err := transaction.PrepareContext(operationContext, `
INSERT INTO kv_records (collection, record_id, payload_json)
VALUES (?, ?, ?)
ON CONFLICT(collection, record_id) DO UPDATE SET payload_json = excluded.payload_json
`)
	if err != nil {
		return err
	}
	defer upsertStatement.Close()

	deleteStatement, err := transaction.PrepareContext(operationContext, `DELETE FROM kv_records WHERE collection = ? AND record_id = ?`)
	if err != nil {
		return err
	}
	defer deleteStatement.Close()

	allCollections := map[string]bool{}
	for collection := range store.serialized {
		allCollections[collection] = true
	}
	for collection := range nextSerialized {
		allCollections[collection] = true
	}

	for collection := range allCollections {
		previousRecords := store.serialized[collection]
		currentRecords := nextSerialized[collection]

		for recordID := range previousRecords {
			if _, exists := currentRecords[recordID]; !exists {
				if _, executeError := deleteStatement.ExecContext(operationContext, collection, recordID); executeError != nil {
					return executeError
				}
			}
		}
		for recordID, payload := range currentRecords {
			if previousPayload, exists := previousRecords[recordID]; exists && previousPayload == payload {
				continue
			}
			if _, executeError := upsertStatement.ExecContext(operationContext, collection, recordID, payload); executeError != nil {
				return executeError
			}
		}
	}

	if len(newEvents) > 0 {
		insertEventStatement, prepareError := transaction.PrepareContext(operationContext, `
INSERT INTO thread_events (thread_id, sequence, event_type, occurred_at, payload_json)
VALUES (?, ?, ?, ?, ?)
`)
		if prepareError != nil {
			return prepareError
		}
		defer insertEventStatement.Close()

		upsertHeadStatement, prepareError := transaction.PrepareContext(operationContext, `
INSERT INTO thread_event_heads (thread_id, head_sequence, floor_sequence, updated_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
	head_sequence = excluded.head_sequence,
	floor_sequence = excluded.floor_sequence,
	updated_at = excluded.updated_at
`)
		if prepareError != nil {
			return prepareError
		}
		defer upsertHeadStatement.Close()

		touchedThreads := map[string]bool{}
		for _, event := range newEvents {
			encodedEvent, marshalErr := json.Marshal(event)
			if marshalErr != nil {
				return marshalErr
			}
			if _, executeError := insertEventStatement.ExecContext(
				operationContext,
				event.ThreadID,
				event.Sequence,
				event.Type,
				event.OccurredAt,
				string(encodedEvent),
			); executeError != nil {
				return executeError
			}
			touchedThreads[event.ThreadID] = true
		}

		for threadID := range touchedThreads {
			if floor, hasPrune := prunedBelow[threadID]; hasPrune && floor > 0 {
				if _, executeError := transaction.ExecContext(operationContext, `DELETE FROM thread_events WHERE thread_id = ? AND sequence <= ?`, threadID, floor); executeError != nil {
					return executeError
				}
			}
			if stream, exists := eventStreams[threadID]; exists {
				if _, executeError := upsertHeadStatement.ExecContext(operationContext, stream.ThreadID, stream.Head, stream.Floor, stream.UpdatedAt); executeError != nil {
					return executeError
				}
			}
		}
	}

	if len(changedRoomHeads) > 0 || len(newRoomMessages) > 0 {
		upsertRoomHeadStatement, prepareError := transaction.PrepareContext(operationContext, `
INSERT INTO thread_room_heads (thread_id, directory_name, head_sequence, created_at, updated_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(thread_id) DO UPDATE SET
	directory_name = excluded.directory_name,
	head_sequence = excluded.head_sequence,
	created_at = excluded.created_at,
	updated_at = excluded.updated_at
`)
		if prepareError != nil {
			return prepareError
		}
		defer upsertRoomHeadStatement.Close()

		insertRoomMessageStatement, prepareError := transaction.PrepareContext(operationContext, `
INSERT INTO thread_room_messages (thread_id, sequence, message_id, kind, created_at, payload_json)
VALUES (?, ?, ?, ?, ?, ?)
`)
		if prepareError != nil {
			return prepareError
		}
		defer insertRoomMessageStatement.Close()

		for _, threadID := range sortedKeys(newRoomMessages) {
			for _, message := range newRoomMessages[threadID] {
				encodedMessage, marshalErr := json.Marshal(message)
				if marshalErr != nil {
					return marshalErr
				}
				if _, executeError := insertRoomMessageStatement.ExecContext(
					operationContext,
					message.ThreadID,
					message.Sequence,
					message.ID,
					message.Kind,
					message.CreatedAt,
					string(encodedMessage),
				); executeError != nil {
					return executeError
				}
			}
		}

		for _, threadID := range sortedKeys(changedRoomHeads) {
			head := changedRoomHeads[threadID]
			if _, executeError := upsertRoomHeadStatement.ExecContext(
				operationContext,
				head.ThreadID,
				head.DirectoryName,
				head.HeadSequence,
				head.CreatedAt,
				head.UpdatedAt,
			); executeError != nil {
				return executeError
			}
		}
	}

	return transaction.Commit()
}

func serializeMap[Value any](records map[string]Value) (map[string]string, error) {
	encoded := make(map[string]string, len(records))
	for recordID, record := range records {
		payloadBytes, err := json.Marshal(record)
		if err != nil {
			return nil, err
		}
		encoded[recordID] = string(payloadBytes)
	}
	return encoded, nil
}

func serializeCollections(state *State) (map[string]map[string]string, error) {
	result := map[string]map[string]string{}
	var err error
	if result[collectionNodes], err = serializeMap(state.Nodes); err != nil {
		return nil, err
	}
	if result[collectionNodeUpdates], err = serializeMap(state.NodeUpdates); err != nil {
		return nil, err
	}
	if result[collectionCapabilityReports], err = serializeMap(state.CapabilityReports); err != nil {
		return nil, err
	}
	if result[collectionComponentReports], err = serializeMap(state.ComponentReports); err != nil {
		return nil, err
	}
	if result[collectionRepositoryReports], err = serializeMap(state.RepositoryReports); err != nil {
		return nil, err
	}
	if result[collectionProjectProfiles], err = serializeMap(state.ProjectProfiles); err != nil {
		return nil, err
	}
	if result[collectionThreads], err = serializeMap(state.Threads); err != nil {
		return nil, err
	}
	if result[collectionAttachments], err = serializeMap(state.Attachments); err != nil {
		return nil, err
	}
	if result[collectionTasks], err = serializeMap(state.Tasks); err != nil {
		return nil, err
	}
	if result[collectionTaskSubmissions], err = serializeMap(state.TaskSubmissions); err != nil {
		return nil, err
	}
	if result[collectionTaskUpdates], err = serializeMap(state.TaskUpdates); err != nil {
		return nil, err
	}
	if result[collectionInstances], err = serializeMap(state.Instances); err != nil {
		return nil, err
	}
	if result[collectionAllocations], err = serializeMap(state.Allocations); err != nil {
		return nil, err
	}
	if result[collectionInstanceReceipts], err = serializeMap(state.InstanceReceipts); err != nil {
		return nil, err
	}
	if result[collectionReleaseIntents], err = serializeMap(state.ReleaseIntents); err != nil {
		return nil, err
	}
	if result[collectionInstanceDeliveries], err = serializeMap(state.InstanceDeliveries); err != nil {
		return nil, err
	}
	if result[collectionRuns], err = serializeMap(state.Runs); err != nil {
		return nil, err
	}
	if result[collectionSessionBindings], err = serializeMap(state.SessionBindings); err != nil {
		return nil, err
	}
	if result[collectionWorkspaceLeases], err = serializeMap(state.WorkspaceLeases); err != nil {
		return nil, err
	}
	if result[collectionApprovals], err = serializeMap(state.Approvals); err != nil {
		return nil, err
	}
	if result[collectionArtifacts], err = serializeMap(state.Artifacts); err != nil {
		return nil, err
	}
	if result[collectionArtifactGrants], err = serializeMap(state.ArtifactGrants); err != nil {
		return nil, err
	}
	if result[collectionMessages], err = serializeMap(state.Messages); err != nil {
		return nil, err
	}
	if result[collectionMessageAcks], err = serializeMap(state.MessageAcks); err != nil {
		return nil, err
	}
	if result[collectionRunActivities], err = serializeMap(state.RunActivities); err != nil {
		return nil, err
	}
	if result[collectionRunTranscripts], err = serializeMap(state.RunTranscripts); err != nil {
		return nil, err
	}
	if result[collectionRoomMessageReceipts], err = serializeMap(state.RoomMessageReceipts); err != nil {
		return nil, err
	}
	if result[collectionRoomNudgeReceipts], err = serializeMap(state.RoomNudgeReceipts); err != nil {
		return nil, err
	}
	if result[collectionMailboxes], err = serializeMap(state.Mailboxes); err != nil {
		return nil, err
	}
	if result[collectionMailMessages], err = serializeMap(state.MailMessages); err != nil {
		return nil, err
	}
	if result[collectionMailReceipts], err = serializeMap(state.MailReceipts); err != nil {
		return nil, err
	}
	return result, nil
}

func decodeInto[Value any](target map[string]Value, recordID string, data []byte) error {
	var record Value
	if err := json.Unmarshal(data, &record); err != nil {
		return err
	}
	target[recordID] = record
	return nil
}

func decodeRecordIntoState(state *State, collection, recordID string, data []byte) error {
	switch collection {
	case collectionNodes:
		return decodeInto(state.Nodes, recordID, data)
	case collectionNodeUpdates:
		return decodeInto(state.NodeUpdates, recordID, data)
	case collectionCapabilityReports:
		return decodeInto(state.CapabilityReports, recordID, data)
	case collectionComponentReports:
		return decodeInto(state.ComponentReports, recordID, data)
	case collectionRepositoryReports:
		return decodeInto(state.RepositoryReports, recordID, data)
	case collectionProjectProfiles:
		return decodeInto(state.ProjectProfiles, recordID, data)
	case collectionThreads:
		return decodeInto(state.Threads, recordID, data)
	case collectionAttachments:
		return decodeInto(state.Attachments, recordID, data)
	case collectionTasks:
		return decodeInto(state.Tasks, recordID, data)
	case collectionTaskSubmissions:
		return decodeInto(state.TaskSubmissions, recordID, data)
	case collectionTaskUpdates:
		return decodeInto(state.TaskUpdates, recordID, data)
	case collectionInstances:
		return decodeInto(state.Instances, recordID, data)
	case collectionAllocations:
		return decodeInto(state.Allocations, recordID, data)
	case collectionInstanceReceipts:
		return decodeInto(state.InstanceReceipts, recordID, data)
	case collectionReleaseIntents:
		return decodeInto(state.ReleaseIntents, recordID, data)
	case collectionInstanceDeliveries:
		return decodeInto(state.InstanceDeliveries, recordID, data)
	case collectionRuns:
		return decodeInto(state.Runs, recordID, data)
	case collectionSessionBindings:
		return decodeInto(state.SessionBindings, recordID, data)
	case collectionWorkspaceLeases:
		return decodeInto(state.WorkspaceLeases, recordID, data)
	case collectionApprovals:
		return decodeInto(state.Approvals, recordID, data)
	case collectionArtifacts:
		return decodeInto(state.Artifacts, recordID, data)
	case collectionArtifactGrants:
		return decodeInto(state.ArtifactGrants, recordID, data)
	case collectionMessages:
		return decodeInto(state.Messages, recordID, data)
	case collectionMessageAcks:
		return decodeInto(state.MessageAcks, recordID, data)
	case collectionRunActivities:
		return decodeInto(state.RunActivities, recordID, data)
	case collectionRunTranscripts:
		return decodeInto(state.RunTranscripts, recordID, data)
	case collectionRoomMessageReceipts:
		return decodeInto(state.RoomMessageReceipts, recordID, data)
	case collectionRoomNudgeReceipts:
		return decodeInto(state.RoomNudgeReceipts, recordID, data)
	case collectionMailboxes:
		return decodeInto(state.Mailboxes, recordID, data)
	case collectionMailMessages:
		return decodeInto(state.MailMessages, recordID, data)
	case collectionMailReceipts:
		return decodeInto(state.MailReceipts, recordID, data)
	default:
		return nil
	}
}
