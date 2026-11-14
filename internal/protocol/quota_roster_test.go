package protocol

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsQuotaOrRateLimitErrorKind(t *testing.T) {
	testCases := []struct {
		name     string
		kind     string
		expected bool
	}{
		{name: "quota exhausted", kind: RunErrorKindQuotaExhausted, expected: true},
		{name: "rate limited", kind: RunErrorKindRateLimited, expected: true},
		{name: "padded quota exhausted", kind: " quota_exhausted ", expected: true},
		{name: "empty", kind: "", expected: false},
		{name: "unknown kind", kind: "execution_failed", expected: false},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expected, IsQuotaOrRateLimitErrorKind(testCase.kind))
		})
	}
}

func TestOfferingCooldownIsActiveAt(t *testing.T) {
	reference := time.Date(2026, time.November, 13, 18, 0, 0, 0, time.UTC)
	cooldown := OfferingCooldown{
		NodeID:     "node-alpha",
		HarnessID:  HarnessClaudeCLI,
		Model:      "sonnet",
		ErrorKind:  RunErrorKindRateLimited,
		OccurredAt: reference.Add(-30 * time.Second).Format(time.RFC3339Nano),
		ExpiresAt:  reference.Add(30 * time.Second).Format(time.RFC3339Nano),
	}
	assert.True(t, cooldown.IsActiveAt(reference))
	assert.False(t, cooldown.IsActiveAt(reference.Add(30*time.Second)))
	assert.False(t, (OfferingCooldown{ExpiresAt: "not-a-timestamp"}).IsActiveAt(reference))
}

func TestDeriveShardedInstanceHandles(t *testing.T) {
	testCases := []struct {
		name            string
		requestedHandle string
		fallbackName    string
		count           int
		expected        []string
		expectError     bool
	}{
		{
			name:            "single without handle",
			requestedHandle: "",
			fallbackName:    "reviewer",
			count:           1,
			expected:        []string{""},
		},
		{
			name:            "single normalized handle",
			requestedHandle: "Verifier-Bot",
			fallbackName:    "",
			count:           1,
			expected:        []string{"verifier-bot"},
		},
		{
			name:            "sharded explicit handle",
			requestedHandle: "shard",
			fallbackName:    "",
			count:           3,
			expected:        []string{"shard-1", "shard-2", "shard-3"},
		},
		{
			name:            "sharded from fallback purpose name",
			requestedHandle: "",
			fallbackName:    "Code Reviewer",
			count:           2,
			expected:        []string{"code-reviewer-1", "code-reviewer-2"},
		},
		{
			name:            "sharded defaults to worker",
			requestedHandle: "",
			fallbackName:    "!!!",
			count:           2,
			expected:        []string{"worker-1", "worker-2"},
		},
		{
			name:            "count zero rejected",
			requestedHandle: "worker",
			count:           0,
			expectError:     true,
		},
		{
			name:            "count above maximum rejected",
			requestedHandle: "worker",
			count:           MaximumSpawnInstanceCount + 1,
			expectError:     true,
		},
		{
			name:            "invalid handle rejected",
			requestedHandle: "-bad-",
			count:           1,
			expectError:     true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			handles, err := DeriveShardedInstanceHandles(testCase.requestedHandle, testCase.fallbackName, testCase.count)
			if testCase.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, testCase.expected, handles)
		})
	}
}
