package slave

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/workspace"
)

const leaseConfirmationTimeout = 30 * time.Second

type pendingLeaseConfirmation struct {
	leaseID   string
	runID     string
	update    protocol.WorkspaceLeaseUpdate
	confirmed chan struct{}
}

type leaseCoordinator struct {
	client  *Client
	manager *workspace.Manager

	mutex   sync.Mutex
	pending map[string]*pendingLeaseConfirmation
}

func newLeaseCoordinator(client *Client, manager *workspace.Manager) *leaseCoordinator {
	return &leaseCoordinator{
		client:  client,
		manager: manager,
		pending: map[string]*pendingLeaseConfirmation{},
	}
}

func (coordinator *leaseCoordinator) confirm(leaseID, status string) {
	if leaseID == "" || status == "" {
		return
	}
	key := leaseID + "\x00" + status
	coordinator.mutex.Lock()
	entry, found := coordinator.pending[key]
	if found {
		delete(coordinator.pending, key)
	}
	coordinator.mutex.Unlock()
	if found {
		close(entry.confirmed)
	}
}

func (coordinator *leaseCoordinator) replayPending() {
	coordinator.mutex.Lock()
	entries := make([]*pendingLeaseConfirmation, 0, len(coordinator.pending))
	for _, entry := range coordinator.pending {
		entries = append(entries, entry)
	}
	coordinator.mutex.Unlock()
	for _, entry := range entries {
		updateCopy := entry.update
		coordinator.client.sendOrQueue(protocol.SlaveControlMessage{
			Type:  "workspace.lease",
			RunID: entry.runID,
			Lease: &updateCopy,
			At:    coordinator.client.nowUTC(),
		})
	}
}

func (coordinator *leaseCoordinator) sendTransition(
	operationContext context.Context,
	runID string,
	currentStatus *string,
	outcome workspace.Outcome,
	leaseID string,
	awaitConfirmation bool,
) error {
	steps := protocol.WorkspaceLeasePath(*currentStatus, outcome.Status)
	if len(steps) == 0 && *currentStatus == outcome.Status {
		return nil
	}
	if len(steps) == 0 {
		steps = []string{outcome.Status}
	}
	for index, step := range steps {
		isFinalStep := index == len(steps)-1
		update := protocol.WorkspaceLeaseUpdate{
			LeaseID: leaseID,
			Status:  step,
		}
		if isFinalStep {
			update = outcome.Update(leaseID)
		}
		if awaitConfirmation && isFinalStep {
			if err := coordinator.sendAndAwaitConfirmation(operationContext, runID, update); err != nil {
				return err
			}
		} else {
			updateCopy := update
			coordinator.client.sendOrQueue(protocol.SlaveControlMessage{
				Type:  "workspace.lease",
				RunID: runID,
				Lease: &updateCopy,
				At:    coordinator.client.nowUTC(),
			})
		}
		*currentStatus = step
	}
	return nil
}

func (coordinator *leaseCoordinator) sendAndAwaitConfirmation(
	operationContext context.Context,
	runID string,
	update protocol.WorkspaceLeaseUpdate,
) error {
	key := update.LeaseID + "\x00" + update.Status
	entry := &pendingLeaseConfirmation{
		leaseID:   update.LeaseID,
		runID:     runID,
		update:    update,
		confirmed: make(chan struct{}),
	}
	coordinator.mutex.Lock()
	if existing, exists := coordinator.pending[key]; exists {
		entry = existing
	} else {
		coordinator.pending[key] = entry
	}
	coordinator.mutex.Unlock()

	defer func() {
		coordinator.mutex.Lock()
		if coordinator.pending[key] == entry {
			delete(coordinator.pending, key)
		}
		coordinator.mutex.Unlock()
	}()

	updateCopy := update
	coordinator.client.sendOrQueue(protocol.SlaveControlMessage{
		Type:  "workspace.lease",
		RunID: runID,
		Lease: &updateCopy,
		At:    coordinator.client.nowUTC(),
	})

	waitContext, cancel := context.WithTimeout(operationContext, leaseConfirmationTimeout)
	defer cancel()
	select {
	case <-entry.confirmed:
		return nil
	case <-waitContext.Done():
		if operationContext.Err() != nil {
			return operationContext.Err()
		}
		return fmt.Errorf("timed out waiting for orchestrator confirmation of workspace lease %s (%s)", update.LeaseID, update.Status)
	}
}

type activeRunLease struct {
	grant         protocol.WorkspaceLeaseGrant
	runID         string
	currentStatus string
	releaseOwner  func()
}

func (coordinator *leaseCoordinator) provisionForRun(
	operationContext context.Context,
	run protocol.Run,
	grant protocol.WorkspaceLeaseGrant,
) (*activeRunLease, string, error) {
	if coordinator.manager == nil {
		return nil, "", errors.New("workspace manager is not configured on this node")
	}
	if err := grant.Validate(); err != nil {
		return nil, "", err
	}
	releaseOwner, err := coordinator.manager.Own(grant)
	if err != nil {
		return nil, "", err
	}

	currentStatus := grant.Status
	if currentStatus == "requested" {
		_ = coordinator.sendTransition(
			operationContext,
			run.ID,
			&currentStatus,
			workspace.Outcome{Status: "provisioning"},
			grant.ID,
			false,
		)
		grant.Status = currentStatus
	}

	taskID := ""
	if run.TaskID != nil {
		taskID = *run.TaskID
	}
	outcome := coordinator.manager.Provision(operationContext, grant, taskID, run.ID)
	if outcome.Status != "active" {
		_ = coordinator.sendTransition(operationContext, run.ID, &currentStatus, outcome, grant.ID, false)
		releaseOwner()
		detail := outcome.Detail
		if detail == "" {
			detail = fmt.Sprintf("workspace lease entered %s status", outcome.Status)
		}
		return nil, "", errors.New(detail)
	}

	if err := coordinator.sendTransition(operationContext, run.ID, &currentStatus, outcome, grant.ID, true); err != nil {
		grant.Status = "active"
		cleanupOutcome := coordinator.manager.Cleanup(context.Background(), grant, run.ID, workspace.ModeReconcile, nil)
		_ = coordinator.sendTransition(context.Background(), run.ID, &currentStatus, cleanupOutcome, grant.ID, false)
		releaseOwner()
		return nil, "", err
	}

	grant.Status = "active"
	if outcome.ResolvedBaseRevision != "" {
		grant.ResolvedBaseRevision = outcome.ResolvedBaseRevision
	}
	return &activeRunLease{
		grant:         grant,
		runID:         run.ID,
		currentStatus: currentStatus,
		releaseOwner:  releaseOwner,
	}, outcome.Path, nil
}

func (coordinator *leaseCoordinator) finishForRun(lease *activeRunLease) {
	if lease == nil {
		return
	}
	defer lease.releaseOwner()

	cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_ = coordinator.sendTransition(
		cleanupContext,
		lease.runID,
		&lease.currentStatus,
		workspace.Outcome{Status: "released"},
		lease.grant.ID,
		false,
	)
	lease.grant.Status = lease.currentStatus

	outcome := coordinator.manager.Cleanup(cleanupContext, lease.grant, lease.runID, workspace.ModeRun, func() {
		_ = coordinator.sendTransition(
			cleanupContext,
			lease.runID,
			&lease.currentStatus,
			workspace.Outcome{Status: "cleaning"},
			lease.grant.ID,
			false,
		)
		lease.grant.Status = lease.currentStatus
	})
	_ = coordinator.sendTransition(cleanupContext, lease.runID, &lease.currentStatus, outcome, lease.grant.ID, false)
}

func (coordinator *leaseCoordinator) handleCleanupCommand(grant protocol.WorkspaceLeaseGrant, mode string) {
	if coordinator.manager == nil {
		return
	}
	if err := grant.Validate(); err != nil {
		return
	}
	if mode != workspace.ModeOperator && mode != workspace.ModeReconcile {
		mode = workspace.ModeReconcile
	}
	releaseOwner, err := coordinator.manager.Own(grant)
	if err != nil {
		if errors.Is(err, workspace.ErrLeaseOwned) {
			return
		}
		currentStatus := grant.Status
		_ = coordinator.sendTransition(
			context.Background(),
			runIDFromBranch(grant.Branch),
			&currentStatus,
			workspace.Outcome{Status: "retained", RetentionReason: "locked", Detail: "workspace lease lock could not be acquired"},
			grant.ID,
			false,
		)
		return
	}
	defer releaseOwner()

	cleanupContext, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	runID := runIDFromBranch(grant.Branch)
	currentStatus := grant.Status
	outcome := coordinator.manager.Cleanup(cleanupContext, grant, runID, mode, func() {
		_ = coordinator.sendTransition(
			cleanupContext,
			runID,
			&currentStatus,
			workspace.Outcome{Status: "cleaning"},
			grant.ID,
			false,
		)
		grant.Status = currentStatus
	})
	_ = coordinator.sendTransition(cleanupContext, runID, &currentStatus, outcome, grant.ID, false)
}

func runIDFromBranch(branch string) string {
	if !strings.HasPrefix(branch, protocol.WorkspaceLeaseBranchPrefix) {
		return ""
	}
	remainder := strings.TrimPrefix(branch, protocol.WorkspaceLeaseBranchPrefix)
	_, runID, found := strings.Cut(remainder, "/")
	if !found {
		return ""
	}
	return runID
}
