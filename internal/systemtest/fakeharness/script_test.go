package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubHost struct {
	messages []string
	thoughts []string
	cancelCh chan struct{}
}

func (host *stubHost) message(text string) error {
	host.messages = append(host.messages, text)
	return nil
}

func (host *stubHost) thought(text string) error {
	host.thoughts = append(host.thoughts, text)
	return nil
}

func (host *stubHost) update(json.RawMessage) error                            { return nil }
func (host *stubHost) permission(context.Context, map[string]any) (any, error) { return nil, nil }
func (host *stubHost) raw(string) error                                        { return nil }
func (host *stubHost) cancelled() <-chan struct{}                              { return host.cancelCh }

func TestExtractAndExecuteScript(t *testing.T) {
	workingDirectory := t.TempDir()
	promptText := `Task instructions
<fake-script>
{
  "steps": [
    {"writeFile": {"path": "notes/result.txt", "content": "working in {{cwd}}"}},
    {"message": "session={{session}}"}
  ]
}
</fake-script>`
	script, err := extractScript(promptText)
	require.NoError(t, err)
	require.Len(t, script.Steps, 2)

	host := &stubHost{cancelCh: make(chan struct{})}
	engine := newScriptEngine(host, nil, promptText, workingDirectory, "sess-test-1")
	require.NoError(t, engine.run(context.Background(), script))

	written, err := os.ReadFile(filepath.Join(workingDirectory, "notes", "result.txt"))
	require.NoError(t, err)
	assert.Equal(t, "working in "+workingDirectory, string(written))
	assert.Equal(t, []string{"session=sess-test-1"}, host.messages)
}

func TestScriptEngine_ReadFile(t *testing.T) {
	workingDirectory := t.TempDir()
	testFilePath := filepath.Join(workingDirectory, "payload.txt")
	require.NoError(t, os.WriteFile(testFilePath, []byte("orchestra fake harness file content"), 0o644))

	t.Run("read file into variable via nested object", func(t *testing.T) {
		promptText := `<fake-script>
{
  "steps": [
    {"readFile": {"path": "payload.txt", "as": "loadedData"}},
    {"message": "data={{loadedData}}"}
  ]
}
</fake-script>`
		script, err := extractScript(promptText)
		require.NoError(t, err)

		host := &stubHost{cancelCh: make(chan struct{})}
		engine := newScriptEngine(host, nil, promptText, workingDirectory, "session-read-1")
		require.NoError(t, engine.run(context.Background(), script))
		require.Len(t, host.messages, 1)
		assert.Equal(t, "data=orchestra fake harness file content", host.messages[0])
	})

	t.Run("read file into variable via string path and step as", func(t *testing.T) {
		promptText := `<fake-script>
{
  "steps": [
    {"readFile": "payload.txt", "as": "stringData"},
    {"message": "string={{stringData}}"}
  ]
}
</fake-script>`
		script, err := extractScript(promptText)
		require.NoError(t, err)

		host := &stubHost{cancelCh: make(chan struct{})}
		engine := newScriptEngine(host, nil, promptText, workingDirectory, "session-read-2")
		require.NoError(t, engine.run(context.Background(), script))
		require.Len(t, host.messages, 1)
		assert.Equal(t, "string=orchestra fake harness file content", host.messages[0])
	})

	t.Run("missing file returns error", func(t *testing.T) {
		promptText := `<fake-script>
{
  "steps": [
    {"readFile": {"path": "missing.txt", "as": "content"}}
  ]
}
</fake-script>`
		script, err := extractScript(promptText)
		require.NoError(t, err)

		host := &stubHost{cancelCh: make(chan struct{})}
		engine := newScriptEngine(host, nil, promptText, workingDirectory, "session-read-3")
		err = engine.run(context.Background(), script)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing.txt")
	})
}

func TestScriptEngine_FailTurn(t *testing.T) {
	workingDirectory := t.TempDir()

	t.Run("fail turn with specific reason", func(t *testing.T) {
		promptText := `<fake-script>
{
  "steps": [
    {"message": "started"},
    {"failTurn": "context window exhausted"}
  ]
}
</fake-script>`
		script, err := extractScript(promptText)
		require.NoError(t, err)

		host := &stubHost{cancelCh: make(chan struct{})}
		engine := newScriptEngine(host, nil, promptText, workingDirectory, "session-fail-1")
		err = engine.run(context.Background(), script)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "context window exhausted")
		assert.Equal(t, []string{"started"}, host.messages)
	})

	t.Run("fail turn with boolean flag", func(t *testing.T) {
		promptText := `<fake-script>
{
  "steps": [
    {"failTurn": true}
  ]
}
</fake-script>`
		script, err := extractScript(promptText)
		require.NoError(t, err)

		host := &stubHost{cancelCh: make(chan struct{})}
		engine := newScriptEngine(host, nil, promptText, workingDirectory, "session-fail-2")
		err = engine.run(context.Background(), script)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "scripted turn failure")
	})
}

func TestScriptEngine_CrashAndExitCode(t *testing.T) {
	workingDirectory := t.TempDir()
	promptText := `<fake-script>
{
  "steps": [
    {"message": "before crash"},
    {"crash": 42}
  ]
}
</fake-script>`
	script, err := extractScript(promptText)
	require.NoError(t, err)

	host := &stubHost{cancelCh: make(chan struct{})}
	engine := newScriptEngine(host, nil, promptText, workingDirectory, "session-crash-1")
	err = engine.run(context.Background(), script)
	require.Error(t, err)

	crashResult, ok := errors.AsType[scriptedCrash](err)
	require.True(t, ok, "error should unwrap to scriptedCrash")
	assert.Equal(t, 42, crashResult.code)
	assert.Equal(t, []string{"before crash"}, host.messages)
}

func TestScriptEngine_TemplateVariableInterpolation(t *testing.T) {
	workingDirectory := t.TempDir()

	t.Run("default run_id falls back to session", func(t *testing.T) {
		promptText := `<fake-script>
{
  "steps": [
    {"message": "cwd={{cwd}}|session={{session}}|run={{run_id}}"}
  ]
}
</fake-script>`
		script, err := extractScript(promptText)
		require.NoError(t, err)

		host := &stubHost{cancelCh: make(chan struct{})}
		engine := newScriptEngine(host, nil, promptText, workingDirectory, "sess-alpha")
		require.NoError(t, engine.run(context.Background(), script))
		require.Len(t, host.messages, 1)
		assert.Equal(t, "cwd="+workingDirectory+"|session=sess-alpha|run=sess-alpha", host.messages[0])
	})

	t.Run("explicit ORCHESTRA_RUN_ID overrides run_id", func(t *testing.T) {
		t.Setenv("ORCHESTRA_RUN_ID", "run-custom-789")
		promptText := `<fake-script>
{
  "steps": [
    {"message": "cwd={{cwd}}|session={{session}}|run={{run_id}}"}
  ]
}
</fake-script>`
		script, err := extractScript(promptText)
		require.NoError(t, err)

		host := &stubHost{cancelCh: make(chan struct{})}
		engine := newScriptEngine(host, nil, promptText, workingDirectory, "sess-beta")
		require.NoError(t, engine.run(context.Background(), script))
		require.Len(t, host.messages, 1)
		assert.Equal(t, "cwd="+workingDirectory+"|session=sess-beta|run=run-custom-789", host.messages[0])
	})
}
