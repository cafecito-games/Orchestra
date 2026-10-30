package protocol_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestVersionParsingAndConstraints(t *testing.T) {
	require.True(t, protocol.IsNormalizedVersion("1.26.2"))
	require.True(t, protocol.IsNormalizedVersion("0.1.0"))
	require.False(t, protocol.IsNormalizedVersion("01.2.3"))
	require.False(t, protocol.IsNormalizedVersion("20261030"))

	require.Equal(t, "1.26.2", protocol.ExtractNormalizedVersion("go version go1.26.2 darwin/arm64"))
	require.Equal(t, -1, protocol.CompareVersions("1.25", "1.26.0"))
	require.Equal(t, 0, protocol.CompareVersions("1.2", "1.2.0.0"))
	require.Equal(t, 1, protocol.CompareVersions("2.0.1", "2.0.0"))

	constraint, err := protocol.ParseVersionConstraint(">=1.26")
	require.NoError(t, err)
	require.True(t, protocol.SatisfiesVersionConstraint("1.26.2", constraint))
	require.False(t, protocol.SatisfiesVersionConstraint("1.25.9", constraint))
}

func TestSecretDetectionAndRedaction(t *testing.T) {
	customToken := "orchestra-super-secret-token-999"
	redactor := protocol.NewRedactor(customToken)

	require.True(t, protocol.LooksSecretLike("sk-ant-api03-1234567890abcdef"))
	require.True(t, protocol.LooksSecretLike("Authorization: Bearer abcdefghijklmnop"))
	require.True(t, protocol.LooksSecretLike("-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----"))
	require.False(t, protocol.LooksSecretLike("normal-label-123"))

	redacted := redactor.Redact("token=" + customToken + " and Authorization: Bearer mysecrettokenvalue123 and ghp_1234567890abcdef")
	require.NotContains(t, redacted, customToken)
	require.NotContains(t, redacted, "mysecrettokenvalue123")
	require.NotContains(t, redacted, "ghp_1234567890abcdef")
	require.Contains(t, redacted, protocol.RedactionMarker)
	require.True(t, redactor.ContainsSecret(customToken))
}

func TestRepositoryIdentityAndWorkspaceLeases(t *testing.T) {
	identity, ok := protocol.NormalizeRepositoryIdentity("https://user:p@ss@github.com/cafecito-games/Orchestra.git/")
	require.True(t, ok)
	require.Equal(t, "https://github.com/cafecito-games/Orchestra", identity)

	scpIdentity, ok := protocol.NormalizeRepositoryIdentity("git@github.com:cafecito-games/Orchestra.git")
	require.True(t, ok)
	require.Equal(t, "github.com:cafecito-games/Orchestra", scpIdentity)

	_, ok = protocol.NormalizeRepositoryIdentity("https://sk-1234567890abcdef@github.com/sk-1234567890abcdef/repo")
	require.False(t, ok)

	grant := protocol.WorkspaceLeaseGrant{
		ID:           "lease-1",
		Status:       "requested",
		Policy:       protocol.WorkspaceIsolationGitWorktree,
		Cleanup:      protocol.WorkspaceCleanupRetain,
		Repository:   identity,
		Root:         "/workspaces",
		SourcePath:   "/workspaces/repo",
		BaseRevision: protocol.WorkspaceLeaseBaseRef("main"),
		Branch:       protocol.WorkspaceLeaseBranch("task-1", "run-1"),
		WorktreePath: protocol.WorkspaceLeaseWorktreePath("/workspaces", "lease-1"),
	}
	require.NoError(t, grant.Validate())
	require.Equal(t, []string{"provisioning", "active"}, protocol.WorkspaceLeasePath("requested", "active"))
	require.Equal(t, []string{"released", "cleaning", "cleaned"}, protocol.WorkspaceLeasePath("active", "cleaned"))
}

func TestEventCursorChecksumsAndOwnership(t *testing.T) {
	cursor, err := protocol.EncodeEventCursor("thread-1", "orchestrator-client:main", 42)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(cursor, "tev1."))

	sequence, err := protocol.DecodeEventCursor(cursor, "thread-1", "orchestrator-client:main")
	require.NoError(t, err)
	require.Equal(t, int64(42), sequence)

	_, err = protocol.DecodeEventCursor(cursor, "thread-2", "orchestrator-client:main")
	require.Error(t, err)

	_, err = protocol.DecodeEventCursor(cursor, "thread-1", "run:run-1")
	require.Error(t, err)

	_, err = protocol.DecodeEventCursor(cursor+"bad", "thread-1", "orchestrator-client:main")
	require.Error(t, err)
}

func TestHarnessEventMarshalAndValidate(t *testing.T) {
	timestamp := protocol.NowTimestamp()
	event := protocol.HarnessEvent{
		Type:     "message.delta",
		RunID:    "run-1",
		Sequence: 1,
		At:       timestamp,
		Text:     "hello world",
		Title:    "unused-variant-field",
	}
	require.NoError(t, event.Validate())
	encodedBytes, err := json.Marshal(event)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(encodedBytes, &decoded))
	require.Equal(t, "hello world", decoded["text"])
	require.NotContains(t, decoded, "title")
}

func TestStateTransitions(t *testing.T) {
	require.True(t, protocol.CanTransitionRun("queued", "running"))
	require.False(t, protocol.CanTransitionRun("completed", "running"))

	require.True(t, protocol.CanTransitionTask("pending", "ready"))
	require.True(t, protocol.CanTransitionTask("running", "ready"))
	require.False(t, protocol.CanTransitionTask("completed", "failed"))

	require.True(t, protocol.CanTransitionInstance("requested", "provisioning"))
	require.True(t, protocol.CanTransitionInstance("ready", "busy"))
	require.True(t, protocol.CanTransitionInstance("busy", "idle"))
	require.False(t, protocol.CanTransitionInstance("released", "ready"))

	require.True(t, protocol.CanTransitionAllocation("reserved", "provisioning"))
	require.True(t, protocol.CanTransitionAllocation("active", "lost"))
	require.False(t, protocol.CanTransitionAllocation("released", "active"))
}
