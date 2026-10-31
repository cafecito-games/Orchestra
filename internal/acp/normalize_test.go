package acp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizePlanEntriesTruncation(t *testing.T) {
	normalizerInstance := newNormalizer()
	entries := make([]map[string]any, 250)
	for index := range entries {
		content := "Plan step"
		entries[index] = map[string]any{
			"content":  content,
			"status":   "in_progress",
			"priority": "medium",
		}
	}
	rawUpdate, err := json.Marshal(map[string]any{
		"sessionUpdate": "plan",
		"entries":       entries,
	})
	require.NoError(t, err)

	events, err := normalizerInstance.sessionUpdate(rawUpdate)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, "warning", events[0].Type)
	assert.Equal(t, warningPlanEntriesTruncated, events[0].Code)
	assert.Equal(t, "plan.updated", events[1].Type)
	assert.Len(t, events[1].Entries, eventPlanEntryLimit)
}

func TestNormalizeDiffTooLargeWarning(t *testing.T) {
	normalizerInstance := newNormalizer()
	oldContent := strings.Repeat("x", eventDiffBytes+1)
	newContent := "updated"

	rawUpdate, err := json.Marshal(map[string]any{
		"sessionUpdate": "tool_call",
		"toolCallId":    "call-diff-1",
		"title":         "Update file",
		"content": []map[string]any{
			{
				"type":    "diff",
				"path":    "large_file.go",
				"oldText": oldContent,
				"newText": newContent,
			},
		},
	})
	require.NoError(t, err)

	events, err := normalizerInstance.sessionUpdate(rawUpdate)
	require.NoError(t, err)
	require.Len(t, events, 2)
	assert.Equal(t, "tool.call", events[0].Type)
	assert.Equal(t, "warning", events[1].Type)
	assert.Equal(t, WarningDiffTooLarge, events[1].Code)
}
