package orchestrator

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

const defaultWorkspaceLandTimeout = 2 * time.Minute

type ListWorkspaceLeasesInput struct {
	ThreadID string `json:"threadId,omitempty"`
	TaskID   string `json:"taskId,omitempty"`
	NodeID   string `json:"nodeId,omitempty"`
	Status   string `json:"status,omitempty"`
}

type ListWorkspaceLeasesResponse struct {
	WorkspaceLeases []protocol.WorkspaceLease `json:"workspaceLeases"`
}

type LandWorkspaceLeaseInput struct {
	LeaseID          string `json:"leaseId"`
	Mode             string `json:"mode,omitempty"`
	TargetBranch     string `json:"targetBranch,omitempty"`
	CleanupAfterLand bool   `json:"cleanupAfterLand,omitempty"`
}

type LandWorkspaceLeaseResponse struct {
	WorkspaceLease protocol.WorkspaceLease           `json:"workspaceLease"`
	Result         protocol.WorkspaceLeaseLandResult `json:"result"`
}

func (service *Service) ListWorkspaceLeases(
	caller CallerIdentity,
	input ListWorkspaceLeasesInput,
) (ListWorkspaceLeasesResponse, error) {
	threadID := strings.TrimSpace(input.ThreadID)
	if caller.Kind == "run" {
		if threadID != "" && caller.ThreadID != "" && threadID != caller.ThreadID {
			return ListWorkspaceLeasesResponse{}, forbiddenError("worker runs cannot inspect workspace leases outside their own thread")
		}
		if threadID == "" {
			threadID = caller.ThreadID
		}
	}
	if threadID != "" && !protocol.IsIdentifier(threadID) {
		return ListWorkspaceLeasesResponse{}, invalidRequest(fmt.Sprintf("invalid threadId %q", input.ThreadID))
	}
	taskID := strings.TrimSpace(input.TaskID)
	if taskID != "" && !protocol.IsIdentifier(taskID) {
		return ListWorkspaceLeasesResponse{}, invalidRequest(fmt.Sprintf("invalid taskId %q", input.TaskID))
	}
	nodeID := strings.TrimSpace(input.NodeID)
	if nodeID != "" && !protocol.IsIdentifier(nodeID) {
		return ListWorkspaceLeasesResponse{}, invalidRequest(fmt.Sprintf("invalid nodeId %q", input.NodeID))
	}
	statusFilter := strings.TrimSpace(input.Status)
	if statusFilter != "" && !slices.Contains(protocol.WorkspaceLeaseStatuses, statusFilter) {
		return ListWorkspaceLeasesResponse{}, invalidRequest(fmt.Sprintf("invalid status %q", input.Status))
	}

	var leases []protocol.WorkspaceLease
	readErr := service.store.Read(func(state *store.State) error {
		if threadID != "" {
			if _, exists := state.Threads[threadID]; !exists {
				return notFound(fmt.Sprintf("thread %s does not exist", threadID))
			}
		}
		for _, lease := range state.WorkspaceLeases {
			if threadID != "" && lease.ThreadID != threadID {
				continue
			}
			if taskID != "" && lease.TaskID != taskID {
				continue
			}
			if nodeID != "" && lease.NodeID != nodeID {
				continue
			}
			if statusFilter != "" && lease.Status != statusFilter {
				continue
			}
			leases = append(leases, lease)
		}
		return nil
	})
	if readErr != nil {
		return ListWorkspaceLeasesResponse{}, readErr
	}
	slices.SortFunc(leases, func(left, right protocol.WorkspaceLease) int {
		if compared := cmp.Compare(left.CreatedAt, right.CreatedAt); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})
	if leases == nil {
		leases = []protocol.WorkspaceLease{}
	}
	return ListWorkspaceLeasesResponse{WorkspaceLeases: leases}, nil
}

func (service *Service) LandWorkspaceLease(
	operationContext context.Context,
	caller CallerIdentity,
	input LandWorkspaceLeaseInput,
) (LandWorkspaceLeaseResponse, error) {
	if caller.Kind == "run" && !caller.CanDelegate {
		return LandWorkspaceLeaseResponse{}, forbiddenError("worker runs without delegation permission cannot land workspace leases")
	}

	leaseID := strings.TrimSpace(input.LeaseID)
	if !protocol.IsWorkspaceLeaseIdentity(leaseID) {
		return LandWorkspaceLeaseResponse{}, invalidRequest("leaseId is required")
	}
	mode := strings.TrimSpace(input.Mode)
	if mode != "" && !slices.Contains(protocol.WorkspaceLandingModes, mode) {
		return LandWorkspaceLeaseResponse{}, invalidRequest(fmt.Sprintf("unsupported landing mode %q", input.Mode))
	}
	targetBranch := strings.TrimSpace(input.TargetBranch)
	if targetBranch != "" {
		normalizedBranch := strings.TrimPrefix(targetBranch, "refs/heads/")
		if !protocol.IsWorkspaceLeaseBaseBranch(normalizedBranch) {
			return LandWorkspaceLeaseResponse{}, invalidRequest(fmt.Sprintf("invalid targetBranch %q", input.TargetBranch))
		}
		targetBranch = normalizedBranch
	}

	var lease protocol.WorkspaceLease
	var alreadyCleanedLanded *LandWorkspaceLeaseResponse
	readErr := service.store.Read(func(state *store.State) error {
		existing, exists := state.WorkspaceLeases[leaseID]
		if !exists {
			return notFound(fmt.Sprintf("workspace lease %s does not exist", leaseID))
		}
		if caller.Kind == "run" && caller.ThreadID != "" && existing.ThreadID != caller.ThreadID {
			return forbiddenError("worker runs cannot land workspace leases from another thread")
		}
		if existing.Policy != protocol.WorkspaceIsolationGitWorktree {
			return invalidRequest("only git-worktree workspace leases can be landed")
		}
		if existing.ReadOnly || existing.SourceLeaseID != "" {
			return invalidRequest("read-only shared workspace leases cannot be landed")
		}
		if existing.Status == "cleaned" {
			if existing.LandedRevision != "" && (targetBranch == "" || targetBranch == existing.LandedBranch) {
				effectiveMode := mode
				if effectiveMode == "" {
					effectiveMode = protocol.WorkspaceLandingModeFastForward
				}
				alreadyCleanedLanded = &LandWorkspaceLeaseResponse{
					WorkspaceLease: existing,
					Result: protocol.WorkspaceLeaseLandResult{
						NodeID:           existing.NodeID,
						LeaseID:          existing.ID,
						Status:           protocol.WorkspaceLandingStatusLanded,
						Mode:             effectiveMode,
						Landed:           true,
						AlreadyLanded:    true,
						Cleaned:          true,
						WorktreeRetained: false,
						TargetBranch:     existing.LandedBranch,
						BaseRevision:     existing.ResolvedBaseRevision,
						LandedRevision:   existing.LandedRevision,
						CommitsLanded:    0,
						Detail:           fmt.Sprintf("workspace lease %s was already landed onto %s and cleaned", existing.ID, existing.LandedBranch),
						ObservedAt:       service.nowUTC(),
					},
				}
				return nil
			}
			return conflictError("lease_cleaned", fmt.Sprintf("workspace lease %s has already been cleaned", leaseID))
		}
		if existing.Status == "failed" || existing.Status == "requested" || existing.Status == "provisioning" {
			return conflictError("lease_not_landable", fmt.Sprintf("workspace lease %s is in status %s and cannot be landed", leaseID, existing.Status))
		}
		if existing.Status == "active" {
			return conflictError("lease_active", fmt.Sprintf("workspace lease %s is still attached to an active run", leaseID))
		}
		if runRecord, runExists := state.Runs[existing.RunID]; runExists && protocol.IsActiveRunStatus(runRecord.Status) {
			return conflictError("lease_active", fmt.Sprintf("workspace lease %s belongs to active run %s", leaseID, runRecord.ID))
		}
		if input.CleanupAfterLand && hasActiveSharedInspectorsInState(state, existing.ID) {
			return conflictError("lease_in_use", fmt.Sprintf("workspace lease %s has active read-only verifier runs and cannot be cleaned yet", leaseID))
		}
		lease = existing
		return nil
	})
	if readErr != nil {
		return LandWorkspaceLeaseResponse{}, readErr
	}
	if alreadyCleanedLanded != nil {
		return *alreadyCleanedLanded, nil
	}

	session := service.slaveSessionForNode(lease.NodeID)
	if session == nil {
		return LandWorkspaceLeaseResponse{}, conflictError("node_offline", fmt.Sprintf("node %s is currently offline", lease.NodeID))
	}

	requestID := protocol.NewIdentifier("land")
	landRequest := protocol.WorkspaceLeaseLandRequest{
		RequestID:        requestID,
		Lease:            lease.Grant(),
		Mode:             mode,
		TargetBranch:     targetBranch,
		CleanupAfterLand: input.CleanupAfterLand,
	}
	if validateErr := landRequest.Validate(); validateErr != nil {
		return LandWorkspaceLeaseResponse{}, invalidRequest(validateErr.Error())
	}

	timedContext, cancel := context.WithTimeout(operationContext, defaultWorkspaceLandTimeout)
	defer cancel()

	reply, err := session.awaitRequest(timedContext, requestID, protocol.MasterControlMessage{
		Type:      "workspace.land",
		RequestID: requestID,
		Land:      &landRequest,
		At:        service.nowUTC(),
	})
	if err != nil {
		return LandWorkspaceLeaseResponse{}, newRetryableError(
			"slave_unavailable",
			http.StatusBadGateway,
			protocol.BoundedDiagnostic(service.redactor.Redact(err.Error())),
		)
	}
	if reply.LandResult == nil ||
		reply.LandResult.NodeID != lease.NodeID ||
		reply.LandResult.LeaseID != lease.ID ||
		reply.LandResult.Validate() != nil {
		return LandWorkspaceLeaseResponse{}, newError("invalid_slave_response", http.StatusBadGateway, "slave node returned an invalid workspace landing response")
	}

	sanitizedResult := *reply.LandResult
	sanitizedResult.Conflicts = slices.Clone(reply.LandResult.Conflicts)
	sanitizedResult.DirtyFiles = slices.Clone(reply.LandResult.DirtyFiles)
	sanitizedResult.Detail = protocol.BoundedDiagnostic(service.redactor.Redact(reply.LandResult.Detail))
	sanitizedResult.Error = protocol.BoundedDiagnostic(service.redactor.Redact(reply.LandResult.Error))

	if sanitizedResult.Status == protocol.WorkspaceLandingStatusFailed || sanitizedResult.Error != "" {
		failureMessage := cmp.Or(sanitizedResult.Error, sanitizedResult.Detail, "workspace landing failed on worker node")
		return LandWorkspaceLeaseResponse{}, newError("landing_failed", http.StatusUnprocessableEntity, failureMessage)
	}

	var updatedLease protocol.WorkspaceLease
	_, mutateErr := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		currentLease, exists := state.WorkspaceLeases[lease.ID]
		if !exists {
			currentLease = lease
		}
		nowText := service.nowUTC()
		if sanitizedResult.BaseRevision != "" && currentLease.ResolvedBaseRevision == "" {
			currentLease.ResolvedBaseRevision = sanitizedResult.BaseRevision
		}
		if sanitizedResult.Detail != "" {
			currentLease.Detail = sanitizedResult.Detail
		}

		if sanitizedResult.Landed {
			currentLease.LandedBranch = sanitizedResult.TargetBranch
			currentLease.LandedRevision = sanitizedResult.LandedRevision
			if currentLease.LandedAt == "" {
				currentLease.LandedAt = nowText
			}
			if sanitizedResult.Cleaned {
				currentLease.Status = "cleaned"
				currentLease.RetentionReason = ""
			}
			currentLease.UpdatedAt = nowText
			state.WorkspaceLeases[currentLease.ID] = currentLease
			updatedLease = currentLease

			mutation.EmitEvent(protocol.TaskEventEntry{
				ThreadID: currentLease.ThreadID,
				Type:     "workspace.lease.landed",
				TaskID:   currentLease.TaskID,
				RunID:    currentLease.RunID,
				Status:   currentLease.Status,
				Summary: fmt.Sprintf(
					"Landed workspace lease %s onto %s (%s)",
					currentLease.ID,
					sanitizedResult.TargetBranch,
					sanitizedResult.Mode,
				),
				Detail: map[string]any{
					"leaseId":        currentLease.ID,
					"mode":           sanitizedResult.Mode,
					"targetBranch":   sanitizedResult.TargetBranch,
					"landedRevision": sanitizedResult.LandedRevision,
					"commitsLanded":  sanitizedResult.CommitsLanded,
					"cleaned":        sanitizedResult.Cleaned,
				},
				OccurredAt: nowText,
			})
			return nil
		}

		if sanitizedResult.WorktreeRetained && currentLease.Status != "cleaned" {
			currentLease.Status = "retained"
			if currentLease.RetentionReason == "" {
				if sanitizedResult.Status == protocol.WorkspaceLandingStatusDirty {
					currentLease.RetentionReason = "dirty"
				} else {
					currentLease.RetentionReason = "diverged"
				}
			}
		}
		currentLease.UpdatedAt = nowText
		state.WorkspaceLeases[currentLease.ID] = currentLease
		updatedLease = currentLease

		mutation.EmitEvent(protocol.TaskEventEntry{
			ThreadID: currentLease.ThreadID,
			Type:     "workspace.lease.land_refused",
			TaskID:   currentLease.TaskID,
			RunID:    currentLease.RunID,
			Status:   sanitizedResult.Status,
			Summary:  cmp.Or(sanitizedResult.Detail, fmt.Sprintf("Refused landing workspace lease %s (%s)", currentLease.ID, sanitizedResult.Status)),
			Detail: map[string]any{
				"leaseId":          currentLease.ID,
				"mode":             sanitizedResult.Mode,
				"targetBranch":     sanitizedResult.TargetBranch,
				"reason":           sanitizedResult.Reason,
				"conflicts":        sanitizedResult.Conflicts,
				"dirtyFiles":       sanitizedResult.DirtyFiles,
				"worktreeRetained": sanitizedResult.WorktreeRetained,
			},
			OccurredAt: nowText,
		})
		return nil
	})
	if mutateErr != nil {
		return LandWorkspaceLeaseResponse{}, mutateErr
	}

	return LandWorkspaceLeaseResponse{
		WorkspaceLease: updatedLease,
		Result:         sanitizedResult,
	}, nil
}

func (service *Service) handleWorkspaceLandResult(session *slaveSession, message protocol.SlaveControlMessage) {
	if message.LandResult == nil || message.LandResult.NodeID != session.nodeID || message.RequestID == "" {
		return
	}
	session.deliverResponse(message.RequestID, message)
}

func (service *Service) HandleAPIWorkspaceLeaseLand(writer http.ResponseWriter, request *http.Request, leaseID string) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var input LandWorkspaceLeaseInput
	if request.Body != nil {
		decoder := json.NewDecoder(io.LimitReader(request.Body, 64*1024))
		if err := decoder.Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
	}
	if strings.TrimSpace(input.LeaseID) != "" && strings.TrimSpace(input.LeaseID) != leaseID {
		writeOperationError(writer, invalidRequest("leaseId in body does not match URL path"))
		return
	}
	input.LeaseID = leaseID

	response, err := service.LandWorkspaceLease(request.Context(), CallerIdentity{Kind: "operator", OperatorID: "cli"}, input)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	statusCode := http.StatusOK
	if !response.Result.Landed {
		statusCode = http.StatusConflict
	}
	writeJSONResponse(writer, statusCode, response)
}

func hasActiveSharedInspectorsInState(state *store.State, sourceLeaseID string) bool {
	for _, candidate := range state.WorkspaceLeases {
		if candidate.SourceLeaseID != sourceLeaseID {
			continue
		}
		if candidate.Status == "requested" || candidate.Status == "provisioning" || candidate.Status == "active" {
			return true
		}
		if runRecord, exists := state.Runs[candidate.RunID]; exists && protocol.IsActiveRunStatus(runRecord.Status) {
			return true
		}
	}
	return false
}

func validateSubmittedTaskWorkspaceInState(
	state *store.State,
	threadID string,
	index int,
	taskSpecification SubmittedTaskSpec,
	placementInstanceID string,
) error {
	mergedRequirements := mergeProjectProfileRequirements(state, taskSpecification.Requirements)
	if err := protocol.ValidateWorkspaceRequirements(mergedRequirements.Workspace); err != nil {
		return invalidRequest(fmt.Sprintf("task[%d] workspace requirements are invalid: %v", index, err))
	}
	if mergedRequirements.Workspace == nil {
		return nil
	}
	sourceLeaseID := strings.TrimSpace(mergedRequirements.Workspace.SourceLeaseID)
	if sourceLeaseID == "" {
		return nil
	}

	sourceLease, exists := state.WorkspaceLeases[sourceLeaseID]
	if !exists {
		return notFound(fmt.Sprintf("task[%d] references unknown sourceLeaseId %q", index, sourceLeaseID))
	}
	if sourceLease.ThreadID != threadID {
		return forbiddenError(fmt.Sprintf("task[%d] references sourceLeaseId %q belonging to another thread", index, sourceLeaseID))
	}
	if sourceLease.ReadOnly || sourceLease.SourceLeaseID != "" {
		return invalidRequest(fmt.Sprintf("task[%d] sourceLeaseId %q is a read-only inspector lease", index, sourceLeaseID))
	}
	if protocol.IsTerminalWorkspaceLeaseStatus(sourceLease.Status) {
		return conflictError("source_lease_unavailable", fmt.Sprintf("task[%d] sourceLeaseId %q is %s", index, sourceLeaseID, sourceLease.Status))
	}
	if taskSpecification.PlacementOverride != nil &&
		taskSpecification.PlacementOverride.NodeID != "" &&
		taskSpecification.PlacementOverride.NodeID != sourceLease.NodeID {
		return invalidRequest(fmt.Sprintf(
			"task[%d] pins node %q which does not match sourceLeaseId %q node %q",
			index,
			taskSpecification.PlacementOverride.NodeID,
			sourceLeaseID,
			sourceLease.NodeID,
		))
	}
	if placementInstanceID != "" {
		if allocation, hasAllocation := state.ActiveAllocationForInstance(placementInstanceID); hasAllocation && allocation.NodeID != sourceLease.NodeID {
			return invalidRequest(fmt.Sprintf(
				"task[%d] pins instance %q on node %q which does not match sourceLeaseId %q node %q",
				index,
				placementInstanceID,
				allocation.NodeID,
				sourceLeaseID,
				sourceLease.NodeID,
			))
		}
	}

	node, nodeExists := state.Nodes[sourceLease.NodeID]
	if nodeExists {
		if mismatch := validateSourceLeaseCompatibility(node, sourceLease, *mergedRequirements.Workspace); mismatch != "" {
			return invalidRequest(fmt.Sprintf("task[%d] %s", index, mismatch))
		}
	}
	return nil
}

func validateSourceLeaseCompatibility(
	node protocol.ComputeNode,
	sourceLease protocol.WorkspaceLease,
	workspaceRequirements protocol.WorkspaceRequirements,
) string {
	if !slices.Contains(node.WorkspaceRoots, sourceLease.Root) || matchingWorkspaceRoot(node, sourceLease.WorktreePath) == "" {
		return fmt.Sprintf("sourceLeaseId %s is outside configured workspace roots on node %s", sourceLease.ID, node.ID)
	}
	if requestedRepository := strings.TrimSpace(workspaceRequirements.Repository); requestedRepository != "" {
		normalizedRepository, ok := protocol.NormalizeRepositoryIdentity(requestedRepository)
		if !ok {
			return fmt.Sprintf("requested repository %q is invalid", requestedRepository)
		}
		if sourceLease.Repository == "" || !protocol.RepositoriesMatch(sourceLease.Repository, normalizedRepository) {
			return fmt.Sprintf("requested repository %s does not match sourceLeaseId %s repository %s", normalizedRepository, sourceLease.ID, sourceLease.Repository)
		}
	}
	if requestedPath := strings.TrimSpace(workspaceRequirements.Path); requestedPath != "" {
		var resolvedRequestedPath string
		if strings.HasPrefix(requestedPath, "/") {
			resolvedRequestedPath = path.Clean(requestedPath)
		} else {
			cleanRelative, subpathErr := protocol.ValidateRelativeWorkspaceSubpath(requestedPath)
			if subpathErr != nil {
				return fmt.Sprintf("requested workspace path %q is invalid: %v", requestedPath, subpathErr)
			}
			resolvedRequestedPath = path.Clean(path.Join(sourceLease.Root, cleanRelative))
		}
		if resolvedRequestedPath != path.Clean(sourceLease.SourcePath) && resolvedRequestedPath != path.Clean(sourceLease.WorktreePath) {
			return fmt.Sprintf("requested workspace path %s does not match sourceLeaseId %s", requestedPath, sourceLease.ID)
		}
	}
	if requestedBranch := strings.TrimSpace(workspaceRequirements.BaseBranch); requestedBranch != "" && sourceLease.BaseRevision != "" {
		if sourceLease.BaseRevision != protocol.WorkspaceLeaseBaseRef(requestedBranch) {
			return fmt.Sprintf("requested baseBranch %s does not match sourceLeaseId %s base %s", requestedBranch, sourceLease.ID, sourceLease.BaseRevision)
		}
	}
	return ""
}

func resolveTaskWorkspaceInState(
	state *store.State,
	threadID string,
	node protocol.ComputeNode,
	repositoryReport protocol.NodeRepositoryReport,
	requirements protocol.ExecutionRequirements,
) (string, string, string) {
	if requirements.Workspace == nil || strings.TrimSpace(requirements.Workspace.SourceLeaseID) == "" {
		return resolveWorkspaceOnNode(node, repositoryReport, requirements)
	}
	if err := protocol.ValidateWorkspaceRequirements(requirements.Workspace); err != nil {
		return "", "", err.Error()
	}
	sourceLeaseID := strings.TrimSpace(requirements.Workspace.SourceLeaseID)
	sourceLease, exists := state.WorkspaceLeases[sourceLeaseID]
	if !exists {
		return "", "", fmt.Sprintf("source workspace lease %s does not exist", sourceLeaseID)
	}
	if threadID != "" && sourceLease.ThreadID != threadID {
		return "", "", fmt.Sprintf("source workspace lease %s belongs to another thread", sourceLeaseID)
	}
	if sourceLease.ReadOnly || sourceLease.SourceLeaseID != "" {
		return "", "", fmt.Sprintf("source workspace lease %s is a read-only inspector lease", sourceLeaseID)
	}
	if node.ID != sourceLease.NodeID {
		return "", "", fmt.Sprintf("node %s does not host sourceLeaseId %s (hosted on %s)", node.ID, sourceLeaseID, sourceLease.NodeID)
	}
	if !slices.Contains([]string{"active", "released", "retained"}, sourceLease.Status) {
		return "", "", fmt.Sprintf("source workspace lease %s is in status %s", sourceLeaseID, sourceLease.Status)
	}
	if mismatch := validateSourceLeaseCompatibility(node, sourceLease, *requirements.Workspace); mismatch != "" {
		return "", "", mismatch
	}
	return path.Clean(sourceLease.Root), path.Clean(sourceLease.SourcePath), ""
}

func resolveWorkspaceForNodeInState(
	state *store.State,
	node protocol.ComputeNode,
	repositoryReport protocol.NodeRepositoryReport,
	requirements protocol.ExecutionRequirements,
) (string, string, string) {
	return resolveTaskWorkspaceInState(state, "", node, repositoryReport, requirements)
}

func buildTaskWorkspaceLeaseInState(
	state *store.State,
	task protocol.Task,
	runID string,
	nodeID string,
	workspaceRoot string,
	sourcePath string,
	requirements protocol.ExecutionRequirements,
	nowText string,
) protocol.WorkspaceLease {
	if requirements.Workspace != nil && strings.TrimSpace(requirements.Workspace.SourceLeaseID) != "" {
		sourceLeaseID := strings.TrimSpace(requirements.Workspace.SourceLeaseID)
		sourceLease := state.WorkspaceLeases[sourceLeaseID]
		leaseID := protocol.NewIdentifier("lease")
		return protocol.WorkspaceLease{
			ID:                   leaseID,
			ThreadID:             task.ThreadID,
			TaskID:               task.ID,
			RunID:                runID,
			NodeID:               nodeID,
			ProjectProfileID:     cmp.Or(requirements.ProjectProfileID, sourceLease.ProjectProfileID),
			SourceLeaseID:        sourceLease.ID,
			ReadOnly:             true,
			Policy:               sourceLease.Policy,
			Cleanup:              protocol.WorkspaceCleanupWhenUnchanged,
			Repository:           sourceLease.Repository,
			Root:                 sourceLease.Root,
			SourcePath:           sourceLease.SourcePath,
			BaseRevision:         sourceLease.BaseRevision,
			ResolvedBaseRevision: sourceLease.ResolvedBaseRevision,
			Branch:               sourceLease.Branch,
			WorktreePath:         sourceLease.WorktreePath,
			Status:               "requested",
			CreatedAt:            nowText,
			UpdatedAt:            nowText,
		}
	}
	return buildWorkspaceLease(state, task, runID, nodeID, workspaceRoot, sourcePath, requirements, nowText)
}
