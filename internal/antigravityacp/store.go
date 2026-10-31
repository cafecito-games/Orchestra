package antigravityacp

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const (
	maximumStoredSessions = 256
	modelCacheTTL         = time.Hour
)

var sessionIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type storedSession struct {
	ConversationID   string `json:"conversationId,omitempty"`
	WorkingDirectory string `json:"cwd"`
	ModelID          string `json:"modelId,omitempty"`
	Mode             string `json:"mode,omitempty"`
	UpdatedAt        string `json:"updatedAt"`
}

type discoveredModel struct {
	Value string `json:"value"`
	Name  string `json:"name"`
}

type cachedModelsDocument struct {
	ObservedAt string            `json:"observedAt"`
	Models     []discoveredModel `json:"models"`
}

type sessionStore struct {
	stateDirectory string
	mutex          sync.Mutex
	memory         map[string]storedSession
}

func newSessionStore(stateDirectory string) *sessionStore {
	return &sessionStore{
		stateDirectory: stateDirectory,
		memory:         map[string]storedSession{},
	}
}

func validSessionIdentifier(sessionID string) bool {
	return sessionIdentifierPattern.MatchString(sessionID) && !strings.Contains(sessionID, "..")
}

func (store *sessionStore) load(sessionID string) (storedSession, bool, error) {
	if !validSessionIdentifier(sessionID) {
		return storedSession{}, false, errors.New("invalid session identifier")
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if cached, exists := store.memory[sessionID]; exists {
		return cached, true, nil
	}
	if store.stateDirectory == "" {
		return storedSession{}, false, nil
	}
	targetPath := store.sessionFilePath(sessionID)
	rawBytes, err := os.ReadFile(targetPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return storedSession{}, false, nil
		}
		return storedSession{}, false, err
	}
	var session storedSession
	if err := json.Unmarshal(rawBytes, &session); err != nil {
		return storedSession{}, false, err
	}
	store.memory[sessionID] = session
	return session, true, nil
}

func (store *sessionStore) save(sessionID string, session storedSession) error {
	if !validSessionIdentifier(sessionID) {
		return errors.New("invalid session identifier")
	}
	if session.UpdatedAt == "" {
		session.UpdatedAt = protocol.NowTimestamp()
	}
	store.mutex.Lock()
	defer store.mutex.Unlock()

	store.memory[sessionID] = session
	if store.stateDirectory == "" {
		return nil
	}
	sessionsDirectory := filepath.Join(store.stateDirectory, "sessions")
	if err := os.MkdirAll(sessionsDirectory, 0o700); err != nil {
		return fmt.Errorf("create antigravity session directory: %w", err)
	}
	encodedBytes, err := json.Marshal(session)
	if err != nil {
		return err
	}
	if err := writeFileAtomically(store.sessionFilePath(sessionID), encodedBytes); err != nil {
		return err
	}
	store.pruneExcessSessionsLocked(sessionsDirectory)
	return nil
}

func (store *sessionStore) evictMemory(sessionID string) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	delete(store.memory, sessionID)
}

func (store *sessionStore) sessionFilePath(sessionID string) string {
	return filepath.Join(store.stateDirectory, "sessions", sessionID+".json")
}

func (store *sessionStore) pruneExcessSessionsLocked(sessionsDirectory string) {
	entries, err := os.ReadDir(sessionsDirectory)
	if err != nil || len(entries) <= maximumStoredSessions {
		return
	}
	type fileEntry struct {
		path         string
		modifiedUnix int64
	}
	candidates := make([]fileEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		information, infoErr := entry.Info()
		if infoErr != nil {
			continue
		}
		candidates = append(candidates, fileEntry{
			path:         filepath.Join(sessionsDirectory, entry.Name()),
			modifiedUnix: information.ModTime().UnixNano(),
		})
	}
	if len(candidates) <= maximumStoredSessions {
		return
	}
	slices.SortFunc(candidates, func(left, right fileEntry) int {
		return cmp.Compare(left.modifiedUnix, right.modifiedUnix)
	})
	excess := len(candidates) - maximumStoredSessions
	for index := range excess {
		_ = os.Remove(candidates[index].path)
	}
}

func (store *sessionStore) loadCachedModels(now time.Time) ([]discoveredModel, bool) {
	if store.stateDirectory == "" {
		return nil, false
	}
	rawBytes, err := os.ReadFile(filepath.Join(store.stateDirectory, "models.json"))
	if err != nil {
		return nil, false
	}
	var document cachedModelsDocument
	if json.Unmarshal(rawBytes, &document) != nil || len(document.Models) == 0 {
		return nil, false
	}
	observedAt, valid := protocol.ParseTimestamp(document.ObservedAt)
	if !valid || now.Sub(observedAt) > modelCacheTTL {
		return nil, false
	}
	return document.Models, true
}

func (store *sessionStore) saveCachedModels(models []discoveredModel, now time.Time) {
	if store.stateDirectory == "" || len(models) == 0 {
		return
	}
	if err := os.MkdirAll(store.stateDirectory, 0o700); err != nil {
		return
	}
	encodedBytes, err := json.Marshal(cachedModelsDocument{
		ObservedAt: now.UTC().Format(time.RFC3339Nano),
		Models:     models,
	})
	if err != nil {
		return
	}
	_ = writeFileAtomically(filepath.Join(store.stateDirectory, "models.json"), encodedBytes)
}

func writeFileAtomically(targetPath string, content []byte) error {
	parentDirectory := filepath.Dir(targetPath)
	temporaryFile, err := os.CreateTemp(parentDirectory, ".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporaryFile.Name()
	cleanup := func() {
		_ = temporaryFile.Close()
		_ = os.Remove(temporaryPath)
	}
	if err := temporaryFile.Chmod(0o600); err != nil {
		cleanup()
		return err
	}
	if _, err := temporaryFile.Write(content); err != nil {
		cleanup()
		return err
	}
	if err := temporaryFile.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	if err := os.Rename(temporaryPath, targetPath); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	return nil
}
