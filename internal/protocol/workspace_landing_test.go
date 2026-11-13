package protocol_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

func sampleWorktreeGrant() protocol.WorkspaceLeaseGrant {
	return protocol.WorkspaceLeaseGrant{
		ID:                   "lease_author1",
		Status:               "retained",
		Policy:               protocol.WorkspaceIsolationGitWorktree,
		Cleanup:              protocol.WorkspaceCleanupRetain,
		Repository:           "https://github.com/example/orchestra",
		Root:                 "/workspaces",
		SourcePath:           "/workspaces/orchestra",
		BaseRevision:         "refs/heads/main",
		ResolvedBaseRevision: strings.Repeat("a", 40),
		Branch:               "orchestra/task_1/run_1",
		WorktreePath:         "/workspaces/.orchestra/worktrees/lease_author1",
	}
}

func TestWorkspaceLeaseLandRequest_Validate(t *testing.T) {
	validGrant := sampleWorktreeGrant()

	testCases := []struct {
		name               string
		request            protocol.WorkspaceLeaseLandRequest
		expectError        string
		expectedMode       string
		expectedTargetName string
	}{
		{
			name: "defaults mode to fast-forward and target to lease base branch",
			request: protocol.WorkspaceLeaseLandRequest{
				RequestID: "land_1",
				Lease:     validGrant,
			},
			expectedMode:       protocol.WorkspaceLandingModeFastForward,
			expectedTargetName: "main",
		},
		{
			name: "accepts cherry-pick and explicit refs/heads target branch",
			request: protocol.WorkspaceLeaseLandRequest{
				Lease:            validGrant,
				Mode:             protocol.WorkspaceLandingModeCherryPick,
				TargetBranch:     "refs/heads/release/1.0",
				CleanupAfterLand: true,
			},
			expectedMode:       protocol.WorkspaceLandingModeCherryPick,
			expectedTargetName: "release/1.0",
		},
		{
			name: "accepts merge-ff-only",
			request: protocol.WorkspaceLeaseLandRequest{
				Lease:        validGrant,
				Mode:         protocol.WorkspaceLandingModeMergeFFOnly,
				TargetBranch: "main",
			},
			expectedMode:       protocol.WorkspaceLandingModeMergeFFOnly,
			expectedTargetName: "main",
		},
		{
			name: "rejects unsupported mode",
			request: protocol.WorkspaceLeaseLandRequest{
				Lease: validGrant,
				Mode:  "rebase-force",
			},
			expectError: "unsupported",
		},
		{
			name: "rejects malformed targetBranch",
			request: protocol.WorkspaceLeaseLandRequest{
				Lease:        validGrant,
				TargetBranch: "../escape",
			},
			expectError: "targetBranch is invalid",
		},
		{
			name: "rejects exclusive-existing lease",
			request: protocol.WorkspaceLeaseLandRequest{
				Lease: protocol.WorkspaceLeaseGrant{
					ID:           "lease_excl1",
					Status:       "retained",
					Policy:       protocol.WorkspaceIsolationExclusiveExisting,
					Cleanup:      protocol.WorkspaceCleanupRetain,
					Root:         "/workspaces",
					SourcePath:   "/workspaces/orchestra",
					WorktreePath: "/workspaces/orchestra",
				},
			},
			expectError: "only git-worktree",
		},
		{
			name: "rejects read-only shared source lease",
			request: protocol.WorkspaceLeaseLandRequest{
				Lease: func() protocol.WorkspaceLeaseGrant {
					grant := validGrant
					grant.ID = "lease_reader1"
					grant.SourceLeaseID = "lease_author1"
					grant.ReadOnly = true
					return grant
				}(),
			},
			expectError: "read-only shared workspace leases cannot be landed",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.request.Validate()
			if testCase.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), testCase.expectError)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, testCase.expectedMode, testCase.request.EffectiveMode())
			targetBranch, branchErr := testCase.request.NormalizedTargetBranch()
			require.NoError(t, branchErr)
			assert.Equal(t, testCase.expectedTargetName, targetBranch)
		})
	}
}

func TestWorkspaceLeaseLandResult_Validate(t *testing.T) {
	revisionA := strings.Repeat("a", 40)
	revisionB := strings.Repeat("b", 40)

	testCases := []struct {
		name        string
		result      protocol.WorkspaceLeaseLandResult
		expectError string
	}{
		{
			name: "valid landed result with cleanup",
			result: protocol.WorkspaceLeaseLandResult{
				RequestID:              "land_1",
				NodeID:                 "node_1",
				LeaseID:                "lease_author1",
				Status:                 protocol.WorkspaceLandingStatusLanded,
				Mode:                   protocol.WorkspaceLandingModeFastForward,
				Landed:                 true,
				Cleaned:                true,
				WorktreeRetained:       false,
				TargetBranch:           "main",
				BaseRevision:           revisionA,
				SourceRevision:         revisionB,
				PreviousTargetRevision: revisionA,
				LandedRevision:         revisionB,
				CommitsLanded:          1,
			},
		},
		{
			name: "valid conflict result retaining worktree",
			result: protocol.WorkspaceLeaseLandResult{
				LeaseID:          "lease_author1",
				Status:           protocol.WorkspaceLandingStatusConflict,
				Mode:             protocol.WorkspaceLandingModeCherryPick,
				Landed:           false,
				WorktreeRetained: true,
				TargetBranch:     "main",
				Reason:           "merge_conflict",
				Conflicts:        []string{"internal/app/main.go"},
				Detail:           "cherry-pick conflicted on internal/app/main.go",
			},
		},
		{
			name: "rejects cleaned when not landed",
			result: protocol.WorkspaceLeaseLandResult{
				LeaseID:          "lease_author1",
				Status:           protocol.WorkspaceLandingStatusConflict,
				Mode:             protocol.WorkspaceLandingModeFastForward,
				Landed:           false,
				Cleaned:          true,
				WorktreeRetained: false,
			},
			expectError: "cannot mark cleaned when landing did not succeed",
		},
		{
			name: "rejects both cleaned and worktreeRetained",
			result: protocol.WorkspaceLeaseLandResult{
				LeaseID:          "lease_author1",
				Status:           protocol.WorkspaceLandingStatusLanded,
				Mode:             protocol.WorkspaceLandingModeFastForward,
				Landed:           true,
				Cleaned:          true,
				WorktreeRetained: true,
			},
			expectError: "cannot mark both cleaned and worktreeRetained",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.result.Validate()
			if testCase.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), testCase.expectError)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateWorkspaceRequirements_SourceLeaseID(t *testing.T) {
	testCases := []struct {
		name         string
		requirements *protocol.WorkspaceRequirements
		expectError  string
	}{
		{
			name:         "nil requirements allowed",
			requirements: nil,
		},
		{
			name: "valid read-only sourceLeaseId",
			requirements: &protocol.WorkspaceRequirements{
				SourceLeaseID: "lease_author1",
				Writable:      false,
			},
		},
		{
			name: "rejects sourceLeaseId when writable is true",
			requirements: &protocol.WorkspaceRequirements{
				SourceLeaseID: "lease_author1",
				Writable:      true,
			},
			expectError: "requires writable=false",
		},
		{
			name: "rejects sourceLeaseId with isolation",
			requirements: &protocol.WorkspaceRequirements{
				SourceLeaseID: "lease_author1",
				Isolation:     protocol.WorkspaceIsolationGitWorktree,
			},
			expectError: "cannot be combined with isolation",
		},
		{
			name: "rejects malformed sourceLeaseId",
			requirements: &protocol.WorkspaceRequirements{
				SourceLeaseID: "../bad",
			},
			expectError: "malformed",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := protocol.ValidateWorkspaceRequirements(testCase.requirements)
			if testCase.expectError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), testCase.expectError)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestWorkspaceLeaseGrant_ValidateSharedReadOnly(t *testing.T) {
	base := sampleWorktreeGrant()
	base.ID = "lease_verifier1"
	base.SourceLeaseID = "lease_author1"
	base.ReadOnly = true
	require.NoError(t, base.Validate())

	mismatch := base
	mismatch.ReadOnly = false
	require.Error(t, mismatch.Validate())

	selfRef := base
	selfRef.SourceLeaseID = selfRef.ID
	require.Error(t, selfRef.Validate())
}
