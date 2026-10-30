package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type stubHost struct {
	messages []string
	cancelCh chan struct{}
}

func (host *stubHost) message(text string) error {
	host.messages = append(host.messages, text)
	return nil
}

func (host *stubHost) thought(string) error                                    { return nil }
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
	require.Equal(t, "working in "+workingDirectory, string(written))
	require.Equal(t, []string{"session=sess-test-1"}, host.messages)
}
