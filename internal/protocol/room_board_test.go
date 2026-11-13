package protocol

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeRoomChannel(t *testing.T) {
	testCases := []struct {
		name      string
		raw       string
		expected  string
		expectErr bool
	}{
		{name: "empty defaults to main", raw: "", expected: "main"},
		{name: "whitespace defaults to main", raw: "   ", expected: "main"},
		{name: "explicit main", raw: "main", expected: "main"},
		{name: "uppercase MAIN normalizes", raw: "MAIN", expected: "main"},
		{name: "subchannel with hyphens and underscores", raw: "  Review_Stage-2 ", expected: "review_stage-2"},
		{name: "single character channel", raw: "q", expected: "q"},
		{name: "maximum length 64 bytes", raw: "a" + strings.Repeat("b", 63), expected: "a" + strings.Repeat("b", 63)},
		{name: "exceeds 64 bytes", raw: "a" + strings.Repeat("b", 64), expectErr: true},
		{name: "starts with hyphen rejected", raw: "-review", expectErr: true},
		{name: "starts with underscore rejected", raw: "_review", expectErr: true},
		{name: "contains dot rejected", raw: "review.v1", expectErr: true},
		{name: "wildcard rejected for post channel", raw: "*", expectErr: true},
		{name: "secret-like rejected", raw: "sk-live-1234567890abcdef", expectErr: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual, err := NormalizeRoomChannel(testCase.raw)
			if testCase.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestNormalizeRoomReadChannelAndSubscriptions(t *testing.T) {
	readDefault, err := NormalizeRoomReadChannel("")
	require.NoError(t, err)
	assert.Empty(t, readDefault)

	readAll, err := NormalizeRoomReadChannel(" * ")
	require.NoError(t, err)
	assert.Equal(t, "*", readAll)

	readMain, err := NormalizeRoomReadChannel("main")
	require.NoError(t, err)
	assert.Equal(t, "main", readMain)

	readNamed, err := NormalizeRoomReadChannel("Verification-1")
	require.NoError(t, err)
	assert.Equal(t, "verification-1", readNamed)

	_, err = NormalizeRoomReadChannel("bad.channel")
	require.Error(t, err)

	channels, err := NormalizeRoomChannels([]string{"Review", "main", "qa_1", "review", ""})
	require.NoError(t, err)
	assert.Equal(t, []string{"qa_1", "review"}, channels)

	assert.True(t, InstanceSubscribesToRoomChannel(nil, ""))
	assert.True(t, InstanceSubscribesToRoomChannel(nil, "main"))
	assert.True(t, InstanceSubscribesToRoomChannel([]string{"Review"}, "review"))
	assert.False(t, InstanceSubscribesToRoomChannel([]string{"Review"}, "security"))
}

func TestMatchesHandleMention(t *testing.T) {
	testCases := []struct {
		name      string
		candidate string
		mention   string
		expected  bool
	}{
		{name: "exact handle match", candidate: "verifier", mention: "verifier", expected: true},
		{name: "case insensitive exact match", candidate: "Verifier", mention: "VERIFIER", expected: true},
		{name: "numbered prefix first member", candidate: "reviewer-1", mention: "reviewer", expected: true},
		{name: "numbered prefix multi-digit member", candidate: "reviewer-14", mention: "reviewer", expected: true},
		{name: "exact numbered member targeted directly", candidate: "reviewer-2", mention: "reviewer-2", expected: true},
		{name: "different numbered member does not match", candidate: "reviewer-2", mention: "reviewer-1", expected: false},
		{name: "zero suffix rejected", candidate: "reviewer-0", mention: "reviewer", expected: false},
		{name: "leading zero suffix rejected", candidate: "reviewer-01", mention: "reviewer", expected: false},
		{name: "non-numeric hyphenated role rejected", candidate: "board-stalls", mention: "board", expected: false},
		{name: "unhyphenated numeric suffix rejected", candidate: "claude4821", mention: "claude", expected: false},
		{name: "invalid handle rejected", candidate: "-bad", mention: "bad", expected: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expected, MatchesHandleMention(testCase.candidate, testCase.mention))
		})
	}
}

func TestParseRoomMentions(t *testing.T) {
	testCases := []struct {
		name             string
		body             string
		explicitMentions []string
		expected         ParsedRoomMentions
		expectMainErr    bool
		expectErr        bool
	}{
		{
			name: "extracts handles channels and punctuation cleanly while ignoring bare mail host",
			body: "Handing off to @verifier, (@Reviewer-1.) and @#qa_lane! Contact author@local or git@github.com/org/repo for context.",
			expected: ParsedRoomMentions{
				Tokens:   []string{"@#qa_lane", "@reviewer-1", "@verifier"},
				Channels: []string{"qa_lane"},
				Handles:  []string{"reviewer-1", "verifier"},
			},
		},
		{
			name:             "merges explicit mentions and @all",
			body:             "@all please check #3; @author@workstation updated schema.",
			explicitMentions: []string{"verifier", "@#security-audit", "coordinator@master"},
			expected: ParsedRoomMentions{
				Tokens:   []string{"@all", "@#security-audit", "@author", "@coordinator", "@verifier"},
				All:      true,
				Channels: []string{"security-audit"},
				Handles:  []string{"author", "coordinator", "verifier"},
			},
		},
		{
			name:          "rejects @#main in body",
			body:          "Waking @#main is disallowed",
			expectMainErr: true,
			expectErr:     true,
		},
		{
			name:             "rejects #main in explicit mentions",
			body:             "Normal body",
			explicitMentions: []string{"#main"},
			expectMainErr:    true,
			expectErr:        true,
		},
		{
			name:             "rejects empty explicit mention",
			body:             "Normal body",
			explicitMentions: []string{"   "},
			expectErr:        true,
		},
		{
			name:             "rejects malformed explicit mention",
			body:             "Normal body",
			explicitMentions: []string{"@bad/handle"},
			expectErr:        true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual, err := ParseRoomMentions(testCase.body, testCase.explicitMentions)
			if testCase.expectErr {
				require.Error(t, err)
				if testCase.expectMainErr {
					require.ErrorIs(t, err, ErrRoomMainChannelMention)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, testCase.expected, actual)
		})
	}
}
