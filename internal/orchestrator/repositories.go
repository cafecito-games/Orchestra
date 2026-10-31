package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

const (
	defaultRepositoryRefreshTimeout = 30 * time.Second
	defaultRepositoryCloneTimeout   = 5 * time.Minute
)

type RefreshRepositoriesInput struct {
	NodeIDs []string `json:"nodeIds,omitempty"`
}

type NodeRepositoryRefreshResult struct {
	NodeID string                         `json:"nodeId"`
	Synced bool                           `json:"synced"`
	Report *protocol.NodeRepositoryReport `json:"report,omitempty"`
	Error  string                         `json:"error,omitempty"`
}

type RefreshRepositoriesResponse struct {
	RefreshedAt string                        `json:"refreshedAt"`
	Nodes       []NodeRepositoryRefreshResult `json:"nodes"`
}

type CloneRepositoryInput struct {
	NodeID        string `json:"nodeId"`
	RepositoryURL string `json:"repositoryUrl"`
	WorkspaceRoot string `json:"workspaceRoot,omitempty"`
	Directory     string `json:"directory,omitempty"`
	Branch        string `json:"branch,omitempty"`
}

type CloneRepositoryResponse struct {
	NodeID     string                        `json:"nodeId"`
	Created    bool                          `json:"created"`
	Repository protocol.DiscoveredRepository `json:"repository"`
	Report     protocol.NodeRepositoryReport `json:"report"`
	ObservedAt string                        `json:"observedAt"`
}

func (service *Service) RefreshRepositories(
	operationContext context.Context,
	caller CallerIdentity,
	input RefreshRepositoriesInput,
) (RefreshRepositoriesResponse, error) {
	if caller.Kind == "run" && !caller.CanDelegate {
		return RefreshRepositoriesResponse{}, forbiddenError("worker runs without delegation permission cannot refresh node repositories")
	}

	var requestedNodeIDs []string
	seenNodeIDs := map[string]bool{}
	for _, rawID := range input.NodeIDs {
		trimmedID := strings.TrimSpace(rawID)
		if trimmedID == "" {
			continue
		}
		if !protocol.IsIdentifier(trimmedID) {
			return RefreshRepositoriesResponse{}, invalidRequest(fmt.Sprintf("invalid nodeId %q", rawID))
		}
		if !seenNodeIDs[trimmedID] {
			seenNodeIDs[trimmedID] = true
			requestedNodeIDs = append(requestedNodeIDs, trimmedID)
		}
	}

	type refreshTarget struct {
		nodeID     string
		session    *slaveSession
		lastReport *protocol.NodeRepositoryReport
	}

	var targets []refreshTarget
	readErr := service.store.Read(func(state *store.State) error {
		if len(requestedNodeIDs) > 0 {
			for _, nodeID := range requestedNodeIDs {
				if _, exists := state.Nodes[nodeID]; !exists {
					return notFound(fmt.Sprintf("node %s does not exist", nodeID))
				}
				var lastReport *protocol.NodeRepositoryReport
				if existing, hasExisting := state.RepositoryReports[nodeID]; hasExisting {
					copyReport := existing
					copyReport.Roots = slices.Clone(existing.Roots)
					copyReport.Repositories = slices.Clone(existing.Repositories)
					lastReport = &copyReport
				}
				targets = append(targets, refreshTarget{
					nodeID:     nodeID,
					session:    service.slaveSessionForNode(nodeID),
					lastReport: lastReport,
				})
			}
			return nil
		}

		for _, nodeID := range sortedNodeIDs(state) {
			session := service.slaveSessionForNode(nodeID)
			if session == nil {
				continue
			}
			var lastReport *protocol.NodeRepositoryReport
			if existing, hasExisting := state.RepositoryReports[nodeID]; hasExisting {
				copyReport := existing
				copyReport.Roots = slices.Clone(existing.Roots)
				copyReport.Repositories = slices.Clone(existing.Repositories)
				lastReport = &copyReport
			}
			targets = append(targets, refreshTarget{
				nodeID:     nodeID,
				session:    session,
				lastReport: lastReport,
			})
		}
		return nil
	})
	if readErr != nil {
		return RefreshRepositoriesResponse{}, readErr
	}

	results := make([]NodeRepositoryRefreshResult, len(targets))
	var waitGroup sync.WaitGroup
	for index, target := range targets {
		if target.session == nil {
			results[index] = NodeRepositoryRefreshResult{
				NodeID: target.nodeID,
				Synced: false,
				Report: target.lastReport,
				Error:  "slave node is offline",
			}
			continue
		}
		waitGroup.Go(func() {
			requestID := protocol.NewIdentifier("req")
			timedContext, cancel := context.WithTimeout(operationContext, defaultRepositoryRefreshTimeout)
			defer cancel()
			reply, err := target.session.awaitRequest(timedContext, requestID, protocol.MasterControlMessage{
				Type:      "workspace.repositories.refresh",
				RequestID: requestID,
				At:        service.nowUTC(),
			})
			if err != nil {
				results[index] = NodeRepositoryRefreshResult{
					NodeID: target.nodeID,
					Synced: false,
					Report: target.lastReport,
					Error:  protocol.BoundedDiagnostic(service.redactor.Redact(err.Error())),
				}
				return
			}
			if reply.RepositoryReport == nil || reply.RepositoryReport.NodeID != target.nodeID || reply.RepositoryReport.Validate() != nil {
				results[index] = NodeRepositoryRefreshResult{
					NodeID: target.nodeID,
					Synced: false,
					Report: target.lastReport,
					Error:  "slave node returned an invalid repository report",
				}
				return
			}
			reportCopy := *reply.RepositoryReport
			reportCopy.Roots = slices.Clone(reportCopy.Roots)
			reportCopy.Repositories = slices.Clone(reportCopy.Repositories)
			results[index] = NodeRepositoryRefreshResult{
				NodeID: target.nodeID,
				Synced: true,
				Report: &reportCopy,
			}
		})
	}
	waitGroup.Wait()

	return RefreshRepositoriesResponse{
		RefreshedAt: service.nowUTC(),
		Nodes:       results,
	}, nil
}

func (service *Service) CloneRepository(
	operationContext context.Context,
	caller CallerIdentity,
	input CloneRepositoryInput,
) (CloneRepositoryResponse, error) {
	if caller.Kind == "run" {
		return CloneRepositoryResponse{}, forbiddenError("worker runs are not authorized to clone repositories on slave nodes")
	}

	nodeID := strings.TrimSpace(input.NodeID)
	if !protocol.IsIdentifier(nodeID) {
		return CloneRepositoryResponse{}, invalidRequest("nodeId is required")
	}

	cleanURL, _, urlErr := protocol.ValidateCloneRepositoryURL(input.RepositoryURL)
	if urlErr != nil {
		return CloneRepositoryResponse{}, invalidRequest(urlErr.Error())
	}

	directory := strings.TrimSpace(input.Directory)
	if directory != "" {
		validatedSubpath, subpathErr := protocol.ValidateRelativeWorkspaceSubpath(directory)
		if subpathErr != nil {
			return CloneRepositoryResponse{}, invalidRequest(subpathErr.Error())
		}
		directory = validatedSubpath
	}

	branch := strings.TrimSpace(input.Branch)
	if branch != "" && !protocol.IsWorkspaceLeaseBaseBranch(branch) {
		return CloneRepositoryResponse{}, invalidRequest("branch is invalid")
	}

	var node protocol.ComputeNode
	readErr := service.store.Read(func(state *store.State) error {
		existing, exists := state.Nodes[nodeID]
		if !exists {
			return notFound(fmt.Sprintf("node %s does not exist", nodeID))
		}
		node = existing
		return nil
	})
	if readErr != nil {
		return CloneRepositoryResponse{}, readErr
	}

	workspaceRoot := strings.TrimSpace(input.WorkspaceRoot)
	if workspaceRoot != "" {
		cleanRoot := path.Clean(workspaceRoot)
		if !strings.HasPrefix(cleanRoot, "/") || !slices.Contains(node.WorkspaceRoots, cleanRoot) {
			return CloneRepositoryResponse{}, invalidRequest(fmt.Sprintf("workspaceRoot %q is not an authorized workspace root on node %s", workspaceRoot, nodeID))
		}
		workspaceRoot = cleanRoot
	} else if len(node.WorkspaceRoots) == 0 {
		return CloneRepositoryResponse{}, conflictError("no_workspace_roots", fmt.Sprintf("node %s has no configured workspace roots", nodeID))
	}

	session := service.slaveSessionForNode(nodeID)
	if session == nil {
		return CloneRepositoryResponse{}, conflictError("node_offline", fmt.Sprintf("node %s is currently offline", nodeID))
	}

	requestID := protocol.NewIdentifier("clone")
	directive := protocol.RepositoryCloneDirective{
		RequestID:     requestID,
		RepositoryURL: cleanURL,
		WorkspaceRoot: workspaceRoot,
		Directory:     directory,
		Branch:        branch,
	}
	if validateErr := directive.Validate(); validateErr != nil {
		return CloneRepositoryResponse{}, invalidRequest(validateErr.Error())
	}

	timedContext, cancel := context.WithTimeout(operationContext, defaultRepositoryCloneTimeout)
	defer cancel()

	reply, err := session.awaitRequest(timedContext, requestID, protocol.MasterControlMessage{
		Type:      "workspace.repository.clone",
		RequestID: requestID,
		Clone:     &directive,
		At:        service.nowUTC(),
	})
	if err != nil {
		return CloneRepositoryResponse{}, newRetryableError(
			"slave_unavailable",
			http.StatusBadGateway,
			protocol.BoundedDiagnostic(service.redactor.Redact(err.Error())),
		)
	}
	if reply.CloneResult == nil || reply.CloneResult.NodeID != nodeID || reply.CloneResult.Validate() != nil {
		return CloneRepositoryResponse{}, newError("invalid_slave_response", http.StatusBadGateway, "slave node returned an invalid clone response")
	}
	if reply.CloneResult.Error != "" {
		return CloneRepositoryResponse{}, newError(
			"clone_failed",
			http.StatusUnprocessableEntity,
			protocol.BoundedDiagnostic(service.redactor.Redact(reply.CloneResult.Error)),
		)
	}
	if reply.CloneResult.Repository == nil {
		return CloneRepositoryResponse{}, newError("invalid_slave_response", http.StatusBadGateway, "slave node omitted cloned repository metadata")
	}

	var report protocol.NodeRepositoryReport
	if reply.CloneResult.Report != nil {
		report = *reply.CloneResult.Report
	} else {
		report = protocol.NodeRepositoryReport{
			NodeID:       nodeID,
			Roots:        slices.Clone(node.WorkspaceRoots),
			Repositories: []protocol.DiscoveredRepository{*reply.CloneResult.Repository},
			ObservedAt:   reply.CloneResult.ObservedAt,
		}
	}

	return CloneRepositoryResponse{
		NodeID:     nodeID,
		Created:    reply.CloneResult.Created,
		Repository: *reply.CloneResult.Repository,
		Report:     report,
		ObservedAt: reply.CloneResult.ObservedAt,
	}, nil
}

func (service *Service) slaveSessionForNode(nodeID string) *slaveSession {
	service.slavesMutex.RLock()
	session := service.slaves[nodeID]
	service.slavesMutex.RUnlock()
	return session
}

func (service *Service) handleAPINodesRefresh(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var input RefreshRepositoriesInput
	if request.Body != nil {
		decoder := json.NewDecoder(io.LimitReader(request.Body, 64*1024))
		if err := decoder.Decode(&input); err != nil && !errors.Is(err, io.EOF) {
			writeOperationError(writer, invalidRequest("malformed JSON body"))
			return
		}
	}
	response, err := service.RefreshRepositories(request.Context(), CallerIdentity{Kind: "operator", OperatorID: "cli"}, input)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSONResponse(writer, http.StatusOK, response)
}

func (service *Service) handleAPINodesClone(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var input CloneRepositoryInput
	if err := json.NewDecoder(io.LimitReader(request.Body, 64*1024)).Decode(&input); err != nil {
		writeOperationError(writer, invalidRequest("malformed JSON body"))
		return
	}
	response, err := service.CloneRepository(request.Context(), CallerIdentity{Kind: "operator", OperatorID: "cli"}, input)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	statusCode := http.StatusOK
	if response.Created {
		statusCode = http.StatusCreated
	}
	writeJSONResponse(writer, statusCode, response)
}
