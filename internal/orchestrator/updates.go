package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/selfupdate"
	"github.com/cafecito-games/Orchestra/internal/store"
)

const defaultNodeUpdateTimeout = 5 * time.Minute

type UpdateNodesInput struct {
	Version string   `json:"version,omitempty"`
	NodeIDs []string `json:"nodeIds,omitempty"`
	Force   bool     `json:"force,omitempty"`
}

type UpdateNodesResponse struct {
	TargetVersion string                     `json:"targetVersion"`
	ReleaseTag    string                     `json:"releaseTag"`
	Nodes         []protocol.NodeUpdateState `json:"nodes"`
}

func activeNodeUpdateInState(state *store.State, nodeID string) (protocol.NodeUpdateState, bool) {
	update, exists := state.NodeUpdates[nodeID]
	if !exists || !protocol.IsActiveNodeUpdateStatus(update.Status) {
		return protocol.NodeUpdateState{}, false
	}
	return update, true
}

func isNodeUpdatingInState(state *store.State, nodeID string) bool {
	_, updating := activeNodeUpdateInState(state, nodeID)
	return updating
}

func (service *Service) UpdateNodes(
	operationContext context.Context,
	caller CallerIdentity,
	input UpdateNodesInput,
) (UpdateNodesResponse, error) {
	if caller.Kind == "run" {
		return UpdateNodesResponse{}, forbiddenError("worker runs are not authorized to trigger node self-updates")
	}

	normalizedTarget, err := protocol.NormalizeReleaseTarget(input.Version)
	if err != nil {
		return UpdateNodesResponse{}, invalidRequest(err.Error())
	}

	var requestedNodeIDs []string
	seenNodeIDs := map[string]bool{}
	for _, rawID := range input.NodeIDs {
		trimmedID := strings.TrimSpace(rawID)
		if trimmedID == "" {
			continue
		}
		if !protocol.IsIdentifier(trimmedID) {
			return UpdateNodesResponse{}, invalidRequest(fmt.Sprintf("invalid nodeId %q", rawID))
		}
		if !seenNodeIDs[trimmedID] {
			seenNodeIDs[trimmedID] = true
			requestedNodeIDs = append(requestedNodeIDs, trimmedID)
		}
	}

	if service.releaseClient == nil || service.updateCache == nil {
		return UpdateNodesResponse{}, newError("internal_error", http.StatusInternalServerError, "release update subsystem is not initialized")
	}

	release, err := service.releaseClient.ResolveRelease(operationContext, normalizedTarget)
	if err != nil {
		return UpdateNodesResponse{}, newRetryableError("release_unavailable", http.StatusBadGateway, protocol.BoundedDiagnostic(service.redactor.Redact(err.Error())))
	}

	type targetCandidate struct {
		nodeID    string
		node      protocol.ComputeNode
		connected bool
		existing  protocol.NodeUpdateState
		hasActive bool
	}

	var candidates []targetCandidate
	readErr := service.store.Read(func(state *store.State) error {
		if len(requestedNodeIDs) > 0 {
			for _, nodeID := range requestedNodeIDs {
				node, exists := state.Nodes[nodeID]
				if !exists {
					return notFound(fmt.Sprintf("node %s does not exist", nodeID))
				}
				existing, hasActive := activeNodeUpdateInState(state, nodeID)
				candidates = append(candidates, targetCandidate{
					nodeID:    nodeID,
					node:      node,
					connected: service.isNodeConnected(nodeID),
					existing:  existing,
					hasActive: hasActive,
				})
			}
			return nil
		}

		for _, nodeID := range sortedNodeIDs(state) {
			if !service.isNodeConnected(nodeID) {
				continue
			}
			node := state.Nodes[nodeID]
			existing, hasActive := activeNodeUpdateInState(state, nodeID)
			candidates = append(candidates, targetCandidate{
				nodeID:    nodeID,
				node:      node,
				connected: true,
				existing:  existing,
				hasActive: hasActive,
			})
		}
		return nil
	})
	if readErr != nil {
		return UpdateNodesResponse{}, readErr
	}

	if len(candidates) == 0 {
		return UpdateNodesResponse{
			TargetVersion: release.Version,
			ReleaseTag:    release.TagName,
			Nodes:         []protocol.NodeUpdateState{},
		}, nil
	}

	var requiredPlatforms []string
	for _, candidate := range candidates {
		if !candidate.connected || candidate.node.SelfUpdateDisabled {
			continue
		}
		if candidate.hasActive && candidate.existing.TargetVersion == release.Version {
			continue
		}
		if !input.Force && candidate.node.Version == release.Version {
			continue
		}
		if !slices.Contains(requiredPlatforms, candidate.node.Platform) {
			requiredPlatforms = append(requiredPlatforms, candidate.node.Platform)
		}
	}

	bundlesByPlatform := map[string]selfupdate.CachedBundle{}
	bundleErrorsByPlatform := map[string]string{}
	for _, platform := range requiredPlatforms {
		bundle, bundleErr := service.updateCache.EnsurePlatformBundle(operationContext, service.releaseClient, release, platform)
		if bundleErr != nil {
			bundleErrorsByPlatform[platform] = protocol.BoundedDiagnostic(service.redactor.Redact(bundleErr.Error()))
			continue
		}
		bundlesByPlatform[platform] = bundle
	}

	type outboundDirective struct {
		nodeID    string
		directive protocol.NodeUpdateDirective
	}

	var outbound []outboundDirective
	results := make([]protocol.NodeUpdateState, 0, len(candidates))

	_, mutateErr := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		nowText := service.nowUTC()

		for _, candidate := range candidates {
			node, exists := state.Nodes[candidate.nodeID]
			if !exists {
				continue
			}
			connected := service.isNodeConnected(node.ID)

			if activeUpdate, isUpdating := activeNodeUpdateInState(state, node.ID); isUpdating {
				if activeUpdate.TargetVersion == release.Version {
					results = append(results, activeUpdate)
					continue
				}
				results = append(results, protocol.NodeUpdateState{
					UpdateID:        protocol.NewIdentifier("upd"),
					NodeID:          node.ID,
					PreviousVersion: node.Version,
					CurrentVersion:  node.Version,
					TargetVersion:   release.Version,
					Status:          "skipped",
					Message:         "node is already updating to " + activeUpdate.TargetVersion,
					RequestedAt:     nowText,
					UpdatedAt:       nowText,
					CompletedAt:     nowText,
				})
				continue
			}

			if !connected {
				skippedState := protocol.NodeUpdateState{
					UpdateID:        protocol.NewIdentifier("upd"),
					NodeID:          node.ID,
					PreviousVersion: node.Version,
					CurrentVersion:  node.Version,
					TargetVersion:   release.Version,
					Status:          "skipped",
					Message:         "node is offline",
					RequestedAt:     nowText,
					UpdatedAt:       nowText,
					CompletedAt:     nowText,
				}
				state.NodeUpdates[node.ID] = skippedState
				results = append(results, skippedState)
				continue
			}

			if node.SelfUpdateDisabled {
				skippedState := protocol.NodeUpdateState{
					UpdateID:        protocol.NewIdentifier("upd"),
					NodeID:          node.ID,
					PreviousVersion: node.Version,
					CurrentVersion:  node.Version,
					TargetVersion:   release.Version,
					Status:          "skipped",
					Message:         "self-update is disabled on this node",
					RequestedAt:     nowText,
					UpdatedAt:       nowText,
					CompletedAt:     nowText,
				}
				state.NodeUpdates[node.ID] = skippedState
				results = append(results, skippedState)
				continue
			}

			if !input.Force && node.Version == release.Version {
				upToDateState := protocol.NodeUpdateState{
					UpdateID:        protocol.NewIdentifier("upd"),
					NodeID:          node.ID,
					PreviousVersion: node.Version,
					CurrentVersion:  node.Version,
					TargetVersion:   release.Version,
					Status:          "up-to-date",
					Message:         "node is already running " + release.Version,
					RequestedAt:     nowText,
					UpdatedAt:       nowText,
					CompletedAt:     nowText,
				}
				state.NodeUpdates[node.ID] = upToDateState
				results = append(results, upToDateState)
				continue
			}

			if bundleFailure, failed := bundleErrorsByPlatform[node.Platform]; failed {
				failedState := protocol.NodeUpdateState{
					UpdateID:        protocol.NewIdentifier("upd"),
					NodeID:          node.ID,
					PreviousVersion: node.Version,
					CurrentVersion:  node.Version,
					TargetVersion:   release.Version,
					Status:          "failed",
					Error:           bundleFailure,
					RequestedAt:     nowText,
					UpdatedAt:       nowText,
					CompletedAt:     nowText,
				}
				state.NodeUpdates[node.ID] = failedState
				results = append(results, failedState)
				continue
			}

			bundle, hasBundle := bundlesByPlatform[node.Platform]
			if !hasBundle {
				bundle, err = service.updateCache.EnsurePlatformBundle(operationContext, service.releaseClient, release, node.Platform)
				if err != nil {
					failedState := protocol.NodeUpdateState{
						UpdateID:        protocol.NewIdentifier("upd"),
						NodeID:          node.ID,
						PreviousVersion: node.Version,
						CurrentVersion:  node.Version,
						TargetVersion:   release.Version,
						Status:          "failed",
						Error:           protocol.BoundedDiagnostic(service.redactor.Redact(err.Error())),
						RequestedAt:     nowText,
						UpdatedAt:       nowText,
						CompletedAt:     nowText,
					}
					state.NodeUpdates[node.ID] = failedState
					results = append(results, failedState)
					continue
				}
			}

			updateID := protocol.NewIdentifier("upd")
			queuedState := protocol.NodeUpdateState{
				UpdateID:        updateID,
				NodeID:          node.ID,
				PreviousVersion: node.Version,
				CurrentVersion:  node.Version,
				TargetVersion:   release.Version,
				Status:          "queued",
				Message:         "update directive dispatched to slave",
				RequestedAt:     nowText,
				UpdatedAt:       nowText,
			}
			state.NodeUpdates[node.ID] = queuedState
			results = append(results, queuedState)

			outbound = append(outbound, outboundDirective{
				nodeID: node.ID,
				directive: protocol.NodeUpdateDirective{
					UpdateID:        updateID,
					TargetVersion:   release.Version,
					ProtocolVersion: protocol.ProtocolVersion,
					DownloadPath:    fmt.Sprintf("/api/updates/bundles/%s/%s", bundle.Version, bundle.Platform),
					ExpectedSHA256:  bundle.SHA256,
					SizeBytes:       bundle.SizeBytes,
					Force:           input.Force,
				},
			})
		}

		if len(outbound) > 0 {
			mutation.OnCommit(func() {
				for _, item := range outbound {
					directiveCopy := item.directive
					sendErr := service.sendToSlave(item.nodeID, protocol.MasterControlMessage{
						Type:       "node.update",
						NodeUpdate: &directiveCopy,
						At:         service.nowUTC(),
					})
					if sendErr != nil {
						service.failUndeliveredNodeUpdate(item.nodeID, item.directive.UpdateID, "slave disconnected before update directive could be delivered")
					}
				}
			})
		}
		return nil
	})
	if mutateErr != nil {
		return UpdateNodesResponse{}, mutateErr
	}

	return UpdateNodesResponse{
		TargetVersion: release.Version,
		ReleaseTag:    release.TagName,
		Nodes:         results,
	}, nil
}

func (service *Service) failUndeliveredNodeUpdate(nodeID string, updateID string, reason string) {
	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		update, exists := state.NodeUpdates[nodeID]
		if !exists || update.UpdateID != updateID || !protocol.IsActiveNodeUpdateStatus(update.Status) {
			return nil
		}
		nowText := service.nowUTC()
		update.Status = "failed"
		update.Error = reason
		update.UpdatedAt = nowText
		update.CompletedAt = nowText
		state.NodeUpdates[nodeID] = update
		mutation.OnCommit(func() {
			service.TriggerSchedule()
		})
		return nil
	})
}

func (service *Service) handleNodeUpdateStatus(nodeID string, message protocol.SlaveControlMessage) {
	if message.NodeUpdate == nil || message.NodeUpdate.NodeID != nodeID {
		return
	}
	report := *message.NodeUpdate
	if err := report.Validate(); err != nil {
		return
	}

	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		existing, exists := state.NodeUpdates[nodeID]
		if !exists || existing.UpdateID != report.UpdateID {
			return nil
		}
		if !protocol.IsActiveNodeUpdateStatus(existing.Status) {
			return nil
		}
		nowText := service.nowUTC()
		existing.Status = report.Status
		if report.CurrentVersion != "" {
			existing.CurrentVersion = report.CurrentVersion
		}
		existing.Message = service.redactor.Redact(report.Message)
		existing.Error = service.redactor.Redact(report.Error)
		existing.UpdatedAt = nowText
		if !protocol.IsActiveNodeUpdateStatus(report.Status) {
			existing.CompletedAt = nowText
			mutation.OnCommit(func() {
				service.TriggerSchedule()
			})
		}
		state.NodeUpdates[nodeID] = existing
		return nil
	})
}

func (service *Service) reconcileNodeUpdateOnRegisterInState(
	state *store.State,
	node protocol.ComputeNode,
	nowText string,
) {
	update, exists := state.NodeUpdates[node.ID]
	if !exists || !protocol.IsActiveNodeUpdateStatus(update.Status) {
		return
	}
	if node.Version == update.TargetVersion {
		update.Status = "succeeded"
		update.CurrentVersion = node.Version
		update.Message = fmt.Sprintf("updated from %s to %s", update.PreviousVersion, node.Version)
		update.Error = ""
		update.UpdatedAt = nowText
		update.CompletedAt = nowText
		state.NodeUpdates[node.ID] = update
		return
	}
	if update.Status == "restarting" {
		update.Status = "failed"
		update.CurrentVersion = node.Version
		update.Error = fmt.Sprintf("slave reconnected running %s instead of target %s", node.Version, update.TargetVersion)
		update.UpdatedAt = nowText
		update.CompletedAt = nowText
		state.NodeUpdates[node.ID] = update
	}
}

func (service *Service) expireStaleNodeUpdates() {
	timeout := service.nodeUpdateTimeout
	if timeout <= 0 {
		timeout = defaultNodeUpdateTimeout
	}
	now := service.now()

	_, _ = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		changed := false
		nowText := now.UTC().Format(time.RFC3339Nano)
		for nodeID, update := range state.NodeUpdates {
			if !protocol.IsActiveNodeUpdateStatus(update.Status) {
				continue
			}
			if update.Status == "draining" && service.isNodeConnected(nodeID) && countActiveRunsOnNode(state, nodeID) > 0 {
				continue
			}
			updatedTime, ok := protocol.ParseTimestamp(update.UpdatedAt)
			if !ok {
				updatedTime, ok = protocol.ParseTimestamp(update.RequestedAt)
			}
			if !ok || now.Sub(updatedTime) <= timeout {
				continue
			}
			update.Status = "failed"
			update.Error = "timed out waiting for slave self-update to complete"
			update.UpdatedAt = nowText
			update.CompletedAt = nowText
			state.NodeUpdates[nodeID] = update
			changed = true
		}
		if changed {
			mutation.OnCommit(func() {
				service.TriggerSchedule()
			})
		}
		return nil
	})
}

func (service *Service) handleAPINodesUpdate(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var input UpdateNodesInput
	if request.ContentLength != 0 {
		decoder := json.NewDecoder(io.LimitReader(request.Body, 64*1024))
		if err := decoder.Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
	}
	response, err := service.UpdateNodes(request.Context(), CallerIdentity{Kind: "operator", OperatorID: "cli"}, input)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, response)
}

func (service *Service) handleAPIUpdateBundle(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if service.updateCache == nil {
		writeOperationError(writer, notFound("update bundle cache is unavailable"))
		return
	}
	remainder := strings.TrimPrefix(request.URL.Path, "/api/updates/bundles/")
	remainder = strings.TrimSuffix(remainder, "/")
	version, platform, found := strings.Cut(remainder, "/")
	if !found || strings.Contains(platform, "/") {
		writeOperationError(writer, notFound("update bundle not found"))
		return
	}

	bundleFile, bundle, err := service.updateCache.OpenVerifiedBundle(version, platform)
	if err != nil {
		if errors.Is(err, selfupdate.ErrBundleNotCached) {
			writeOperationError(writer, notFound("update bundle not found"))
			return
		}
		writeOperationError(writer, invalidRequest("invalid update bundle request"))
		return
	}
	defer bundleFile.Close()

	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.Header().Set("Content-Length", strconv.FormatInt(bundle.SizeBytes, 10))
	writer.Header().Set("X-Orchestra-Version", bundle.Version)
	writer.Header().Set("X-Orchestra-SHA256", bundle.SHA256)
	writer.WriteHeader(http.StatusOK)
	if request.Method == http.MethodHead {
		return
	}
	_, _ = io.Copy(writer, bundleFile)
}
