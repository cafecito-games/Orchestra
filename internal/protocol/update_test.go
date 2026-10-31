package protocol_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

func TestReleaseVersionNormalizationAndComparison(t *testing.T) {
	normalizationCases := []struct {
		name        string
		input       string
		expected    string
		expectError bool
	}{
		{name: "empty defaults to latest", input: "", expected: "latest"},
		{name: "latest case-insensitive", input: " LATEST ", expected: "latest"},
		{name: "v-prefixed semver", input: "v0.2.1", expected: "0.2.1"},
		{name: "bare semver", input: "1.4.0", expected: "1.4.0"},
		{name: "prerelease semver", input: "v1.0.0-rc.1", expected: "1.0.0-rc.1"},
		{name: "rejects dev", input: "dev", expectError: true},
		{name: "rejects traversal", input: "1..0", expectError: true},
		{name: "rejects slashes", input: "0.2.0/evil", expectError: true},
	}

	for _, testCase := range normalizationCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual, err := protocol.NormalizeReleaseTarget(testCase.input)
			if testCase.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, testCase.expected, actual)
		})
	}

	comparisonCases := []struct {
		left     string
		right    string
		expected int
		valid    bool
	}{
		{left: "0.1.0", right: "0.2.0", expected: -1, valid: true},
		{left: "v0.10.0", right: "0.2.0", expected: 1, valid: true},
		{left: "1.2.3", right: "v1.2.3", expected: 0, valid: true},
		{left: "1.2.3-rc.1", right: "1.2.3", expected: -1, valid: true},
		{left: "1.2.3+meta1", right: "1.2.3+meta2", expected: 0, valid: true},
		{left: "dev", right: "0.2.0", expected: 0, valid: false},
		{left: "01.2.3", right: "1.2.3", expected: 0, valid: false},
	}

	for _, testCase := range comparisonCases {
		compared, valid := protocol.CompareSemanticVersions(testCase.left, testCase.right)
		assert.Equal(t, testCase.valid, valid, "%s vs %s validity", testCase.left, testCase.right)
		if testCase.valid {
			assert.Equal(t, testCase.expected, compared, "%s vs %s order", testCase.left, testCase.right)
		}
	}
}

func TestNodeUpdateValidation(t *testing.T) {
	validSHA := strings.Repeat("a", 64)
	directive := protocol.NodeUpdateDirective{
		UpdateID:        "upd-1234",
		TargetVersion:   "0.2.0",
		ProtocolVersion: protocol.ProtocolVersion,
		DownloadPath:    "/api/updates/bundles/0.2.0/linux-amd64",
		ExpectedSHA256:  validSHA,
		SizeBytes:       4096,
	}
	require.NoError(t, directive.Validate())

	badDirective := directive
	badDirective.ExpectedSHA256 = "ABC"
	require.Error(t, badDirective.Validate())

	badDirective = directive
	badDirective.SizeBytes = 0
	require.Error(t, badDirective.Validate())

	report := protocol.NodeUpdateReport{
		UpdateID:       "upd-1234",
		NodeID:         "worker-linux",
		Status:         "downloading",
		CurrentVersion: "0.1.0",
		TargetVersion:  "0.2.0",
		Message:        "fetching verified bundle",
		ObservedAt:     protocol.NowTimestamp(),
	}
	require.NoError(t, report.Validate())
	assert.True(t, protocol.IsActiveNodeUpdateStatus("downloading"))
	assert.True(t, protocol.IsActiveNodeUpdateStatus("draining"))
	assert.True(t, protocol.IsActiveNodeUpdateStatus("restarting"))
	assert.False(t, protocol.IsActiveNodeUpdateStatus("succeeded"))
	assert.False(t, protocol.IsActiveNodeUpdateStatus("failed"))

	badReport := report
	badReport.Status = "unknown"
	require.Error(t, badReport.Validate())
}
