package protocol

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLabelValidationAndRedaction(t *testing.T) {
	redactor := NewRedactor("super-secret-token")

	normalized, err := NormalizeLabels(map[string]string{
		"stage":    "review",
		"pr.num":   "123",
		"owner_id": "worker-1",
		"note":     "token super-secret-token redacted",
	}, redactor, false)
	require.NoError(t, err)
	assert.Equal(t, "review", normalized["stage"])
	assert.Equal(t, "123", normalized["pr.num"])
	assert.NotContains(t, normalized["note"], "super-secret-token")

	t.Run("rejects reserved orchestra namespace", func(t *testing.T) {
		_, err := NormalizeLabels(map[string]string{"orchestra.internal": "1"}, redactor, false)
		require.ErrorIs(t, err, ErrReservedLabel)
	})

	t.Run("rejects invalid keys and control characters", func(t *testing.T) {
		cases := []struct {
			name   string
			labels map[string]string
		}{
			{name: "uppercase key", labels: map[string]string{"Stage": "review"}},
			{name: "leading dash", labels: map[string]string{"-stage": "review"}},
			{name: "newline in value", labels: map[string]string{"stage": "line1\nline2"}},
			{name: "null byte in value", labels: map[string]string{"stage": "a\x00b"}},
			{name: "value too long", labels: map[string]string{"stage": strings.Repeat("a", LabelValueMaximumBytes+1)}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := NormalizeLabels(tc.labels, redactor, false)
				require.Error(t, err)
			})
		}
	})
}

func TestLabelFilterParsingAndMatching(t *testing.T) {
	labels := map[string]string{
		"stage":    "review",
		"reviewer": "verifier-1",
		"pr":       "42",
	}

	cases := []struct {
		name    string
		filters []string
		want    bool
	}{
		{name: "presence hit", filters: []string{"reviewer"}, want: true},
		{name: "presence miss", filters: []string{"missing"}, want: false},
		{name: "exact hit", filters: []string{"stage=review"}, want: true},
		{name: "exact miss", filters: []string{"stage=draft"}, want: false},
		{name: "alternation hit", filters: []string{"stage=open|review|draft"}, want: true},
		{name: "alternation miss", filters: []string{"stage=open|draft"}, want: false},
		{name: "multi-label conjunction hit", filters: []string{"reviewer", "stage=open|review", "pr=42"}, want: true},
		{name: "multi-label conjunction miss", filters: []string{"reviewer", "stage=open|review", "pr=99"}, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := ParseLabelFilters(tc.filters)
			require.NoError(t, err)
			assert.Equal(t, tc.want, MatchesLabelFilters(labels, parsed))
		})
	}
}
