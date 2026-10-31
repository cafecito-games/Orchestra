package antigravityacp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionStoreCorruptedJSON(t *testing.T) {
	stateDirectory := t.TempDir()
	store := newSessionStore(stateDirectory)

	sessionsDirectory := filepath.Join(stateDirectory, "sessions")
	require.NoError(t, os.MkdirAll(sessionsDirectory, 0o700))

	corruptedSessionPath := filepath.Join(sessionsDirectory, "session-corrupt.json")
	require.NoError(t, os.WriteFile(corruptedSessionPath, []byte("this is not valid json {"), 0o600))

	session, exists, err := store.load("session-corrupt")
	require.Error(t, err)
	assert.False(t, exists)
	assert.Empty(t, session.ConversationID)

	modelsPath := filepath.Join(stateDirectory, "models.json")
	require.NoError(t, os.WriteFile(modelsPath, []byte("[corrupted models"), 0o600))
	models, valid := store.loadCachedModels(time.Now())
	assert.False(t, valid)
	assert.Nil(t, models)
}

func TestTranslateStreamOutputResilientToMalformedLines(t *testing.T) {
	streamInput := strings.Join([]string{
		"",
		"not-json-line-at-all",
		`{"event":"init","conversation_id":"conv-resilient-1"}`,
		`{broken json`,
		`{"event":"step_update","step_update":{"conversation_id":"conv-resilient-1","step_index":1,"state":"ACTIVE","step_type":"thought","text_delta":"Processing"}}`,
		`{"event":"unknown_event_type"}`,
		`{"event":"step_update","step_update":{"conversation_id":"conv-resilient-1","step_index":2,"state":"DONE","step_type":"agent_response","text_delta":"Done"}}`,
		`{"event":"result","result":{"conversation_id":"conv-resilient-1","status":"SUCCESS","response":"Done","usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15}}}`,
		"trailing-garbage-bytes",
	}, "\n")

	var conversationRecorded string
	var emittedUpdates []map[string]any

	outcome, err := translateStreamOutput(
		strings.NewReader(streamInput),
		t.TempDir(),
		1,
		func(conversationID string) { conversationRecorded = conversationID },
		func(update map[string]any) error {
			emittedUpdates = append(emittedUpdates, update)
			return nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "conv-resilient-1", conversationRecorded)
	assert.Equal(t, "conv-resilient-1", outcome.ConversationID)
	assert.NotNil(t, outcome.Usage)
	assert.Equal(t, int64(10), *outcome.Usage.InputTokens)
	assert.Equal(t, int64(5), *outcome.Usage.OutputTokens)
	assert.Len(t, emittedUpdates, 2)
}
