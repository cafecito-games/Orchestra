package acp

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFrameReaderFrameTooLarge(t *testing.T) {
	longPayload := "{" + strings.Repeat("a", 100) + "}\n"
	reader := newFrameReader(strings.NewReader(longPayload), 50)
	_, err := reader.next()
	require.ErrorIs(t, err, ErrFrameTooLarge)
}

func TestFrameReaderUnterminatedFrame(t *testing.T) {
	reader := newFrameReader(strings.NewReader(`{"hello":"world"}`), 100)
	_, err := reader.next()
	require.ErrorIs(t, err, ErrUnterminatedFrame)
}

func TestFrameReaderStdoutContamination(t *testing.T) {
	testCases := []struct {
		name  string
		input string
	}{
		{name: "empty line", input: "\n"},
		{name: "whitespace only", input: "   \n"},
		{name: "plain text", input: "DEBUG: starting adapter\n"},
		{name: "invalid utf8", input: string([]byte{'{', 0xff, 0xfe, '}', '\n'})},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			reader := newFrameReader(strings.NewReader(testCase.input), 100)
			_, err := reader.next()
			require.ErrorIs(t, err, ErrStdoutContamination)
		})
	}
}

func TestFrameReaderValidFrameAndEOF(t *testing.T) {
	reader := newFrameReader(strings.NewReader("{\"jsonrpc\":\"2.0\"}\n"), 100)
	frame, err := reader.next()
	require.NoError(t, err)
	assert.JSONEq(t, `{"jsonrpc":"2.0"}`, string(frame))

	_, err = reader.next()
	require.ErrorIs(t, err, io.EOF)
}
