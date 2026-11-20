package protocol

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStructuredBlockValidationAndRedaction(t *testing.T) {
	redactor := NewRedactor("secret-token-xyz")

	validCases := []struct {
		name  string
		block StructuredBlock
		want  string
	}{
		{
			name: "git-commit@1",
			block: StructuredBlock{
				Schema:  "git-commit@1",
				Version: 0,
				Data: map[string]any{
					"sha":          "abcdef1234567890",
					"branch":       "issue-30",
					"summary":      "Contains secret-token-xyz",
					"filesChanged": 4,
				},
			},
			want: "[git-commit@1 sha=abcdef123456 branch=issue-30]",
		},
		{
			name: "workspace-lease@1",
			block: StructuredBlock{
				Schema:  BlockSchemaWorkspaceLease,
				Version: 1,
				Data: map[string]any{
					"leaseId":    "lease_abc",
					"nodeId":     "node-1",
					"branch":     "work/feat",
					"baseBranch": "main",
				},
			},
			want: "[workspace-lease@1 lease=lease_abc branch=work/feat]",
		},
		{
			name: "test-report@1",
			block: StructuredBlock{
				Schema:  BlockSchemaTestReport,
				Version: 1,
				Data: map[string]any{
					"passed":     42,
					"failed":     0,
					"skipped":    1,
					"suite":      "unit",
					"durationMs": 1200,
				},
			},
			want: "[test-report@1 passed=42 failed=0 suite=unit]",
		},
		{
			name: "review-verdict@1",
			block: StructuredBlock{
				Schema:  BlockSchemaReviewVerdict,
				Version: 1,
				Data: map[string]any{
					"verdict":  "approved",
					"sha":      "1234567890abcdef",
					"summary":  "Looks clean",
					"findings": []string{"nit: minor comment"},
				},
			},
			want: "[review-verdict@1 verdict=approved sha=1234567890ab]",
		},
	}

	for _, tc := range validCases {
		t.Run(tc.name, func(t *testing.T) {
			normalized, err := NormalizeStructuredBlocks([]StructuredBlock{tc.block}, redactor)
			require.NoError(t, err)
			require.Len(t, normalized, 1)
			assert.Equal(t, tc.want, FormatStructuredBlockSummary(normalized[0]))
			if summary, ok := normalized[0].Data["summary"].(string); ok {
				assert.NotContains(t, summary, "secret-token-xyz")
			}
		})
	}

	invalidCases := []struct {
		name  string
		block StructuredBlock
	}{
		{name: "unknown schema", block: StructuredBlock{Schema: "custom", Version: 1, Data: map[string]any{}}},
		{name: "unsupported version", block: StructuredBlock{Schema: "git-commit", Version: 2, Data: map[string]any{"sha": "abc"}}},
		{name: "missing required sha", block: StructuredBlock{Schema: "git-commit", Version: 1, Data: map[string]any{"branch": "main"}}},
		{name: "unknown field", block: StructuredBlock{Schema: "git-commit", Version: 1, Data: map[string]any{"sha": "abc", "extra": 1}}},
		{name: "negative passed count", block: StructuredBlock{Schema: "test-report", Version: 1, Data: map[string]any{"passed": -1, "failed": 0}}},
		{name: "invalid review verdict", block: StructuredBlock{Schema: "review-verdict", Version: 1, Data: map[string]any{"verdict": "lgtm"}}},
		{name: "oversized payload", block: StructuredBlock{Schema: "git-commit", Version: 1, Data: map[string]any{"sha": "abc", "summary": strings.Repeat("x", MaximumBlockDataBytes+1)}}},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NormalizeStructuredBlocks([]StructuredBlock{tc.block}, redactor)
			require.Error(t, err)
		})
	}
}
