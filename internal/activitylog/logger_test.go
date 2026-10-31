package activitylog_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/activitylog"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

func TestLoggerTextAndJSONFormattingWithRedaction(t *testing.T) {
	fixedTime := time.Date(2026, 10, 31, 15, 4, 5, 0, time.UTC)
	const masterToken = "super-secret-master-token-9999"

	var textBuffer bytes.Buffer
	colorDisabled := false
	textLogger := activitylog.New(activitylog.Options{
		Writer:   &textBuffer,
		Level:    activitylog.LevelInfo,
		Format:   activitylog.FormatText,
		Color:    &colorDisabled,
		Redactor: protocol.NewRedactor(masterToken),
		Now:      func() time.Time { return fixedTime },
	})

	textLogger.Debug("node", "ignored debug message")
	textLogger.Info(
		"node",
		"connected",
		activitylog.String("node", "worker-linux"),
		activitylog.String("name", "Linux Build Box"),
		activitylog.String("empty", ""),
		activitylog.String("auth", "Bearer "+masterToken),
		activitylog.Int("concurrency", 4),
		activitylog.Duration("duration", 1400*time.Millisecond),
	)

	textLine := textBuffer.String()
	require.NotContains(t, textLine, "ignored debug message")
	require.NotContains(t, textLine, masterToken)
	require.Contains(t, textLine, "INF node     connected")
	require.Contains(t, textLine, "node=worker-linux")
	require.Contains(t, textLine, `name="Linux Build Box"`)
	require.NotContains(t, textLine, "empty=")
	require.Contains(t, textLine, "concurrency=4")
	require.Contains(t, textLine, "duration=1.4s")
	require.Contains(t, textLine, protocol.RedactionMarker)

	var jsonBuffer bytes.Buffer
	jsonLogger := activitylog.New(activitylog.Options{
		Writer:    &jsonBuffer,
		Level:     activitylog.LevelDebug,
		Format:    activitylog.FormatJSON,
		Component: "local-worker",
		Redactor:  protocol.NewRedactor(masterToken),
		Now:       func() time.Time { return fixedTime },
	})
	jsonLogger.Warn(
		"approval",
		"waiting for token "+masterToken,
		activitylog.String("approval", "apr_123"),
		activitylog.Bool("urgent", true),
	)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(jsonBuffer.Bytes(), &decoded))
	require.Equal(t, "warn", decoded["level"])
	require.Equal(t, "approval", decoded["category"])
	require.Equal(t, "local-worker", decoded["component"])
	require.Equal(t, "apr_123", decoded["approval"])
	require.Equal(t, true, decoded["urgent"])
	require.NotContains(t, decoded["message"], masterToken)
}

func TestParseLevelAndFormat(t *testing.T) {
	cases := []struct {
		raw      string
		expected activitylog.Level
		valid    bool
	}{
		{raw: "", expected: activitylog.LevelInfo, valid: true},
		{raw: "debug", expected: activitylog.LevelDebug, valid: true},
		{raw: "verbose", expected: activitylog.LevelDebug, valid: true},
		{raw: "warn", expected: activitylog.LevelWarn, valid: true},
		{raw: "error", expected: activitylog.LevelError, valid: true},
		{raw: "quiet", expected: activitylog.LevelQuiet, valid: true},
		{raw: "bogus", valid: false},
	}
	for _, testCase := range cases {
		parsed, err := activitylog.ParseLevel(testCase.raw)
		if testCase.valid {
			require.NoError(t, err)
			require.Equal(t, testCase.expected, parsed)
		} else {
			require.Error(t, err)
		}
	}

	format, err := activitylog.ParseFormat("json")
	require.NoError(t, err)
	require.Equal(t, activitylog.FormatJSON, format)

	_, err = activitylog.ParseFormat("xml")
	require.Error(t, err)
}
