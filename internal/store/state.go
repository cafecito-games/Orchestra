// Package store provides pure-Go SQLite WAL persistence, atomic state-diff
// thread event journaling, and SHA-256 verified artifact blob storage for the
// Orchestra master control plane.
package store

import (
	"encoding/json"
	"maps"
	"slices"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

type InstanceDelivery struct {
	ID           string `json:"id"`
	NodeID       string `json:"nodeId"`
	InstanceID   string `json:"instanceId"`
	AllocationID string `json:"allocationId"`
	Kind         string `json:"kind"`
	Mode         string `json:"mode,omitempty"`
	CreatedAt    string `json:"createdAt"`
}

type State struct {
	Nodes              map[string]protocol.ComputeNode
	CapabilityReports  map[string]protocol.NodeCapabilityReport
	ComponentReports   map[string]protocol.ComponentInventoryReport
	ProjectProfiles    map[string]protocol.ProjectProfile
	Threads            map[string]protocol.Thread
	Attachments        map[string]protocol.ThreadAttachment
	Tasks              map[string]protocol.Task
	TaskSubmissions    map[string]protocol.TaskSubmissionReceipt
	TaskUpdates        map[string]protocol.TaskUpdateReceipt
	Instances          map[string]protocol.AgentInstance
	Allocations        map[string]protocol.InstanceAllocation
	InstanceReceipts   map[string]protocol.InstanceLifecycleReceipt
	ReleaseIntents     map[string]protocol.InstanceReleaseIntent
	InstanceDeliveries map[string]InstanceDelivery
	Runs               map[string]protocol.Run
	SessionBindings    map[string]protocol.HarnessSessionBinding
	WorkspaceLeases    map[string]protocol.WorkspaceLease
	Approvals          map[string]protocol.ApprovalRequest
	Artifacts          map[string]protocol.Artifact
	ArtifactGrants     map[string]protocol.ArtifactUploadGrantRecord
	Messages           map[string]protocol.TaskMessage
	MessageAcks        map[string]protocol.TaskMessageAck
	RunActivities      map[string]protocol.RunActivity
	RunTranscripts     map[string]protocol.RunTranscript
	EventStreams       map[string]protocol.ThreadEventStream
	ThreadEvents       map[string][]protocol.TaskEventEntry
}

func NewState() *State {
	return &State{
		Nodes:              map[string]protocol.ComputeNode{},
		CapabilityReports:  map[string]protocol.NodeCapabilityReport{},
		ComponentReports:   map[string]protocol.ComponentInventoryReport{},
		ProjectProfiles:    map[string]protocol.ProjectProfile{},
		Threads:            map[string]protocol.Thread{},
		Attachments:        map[string]protocol.ThreadAttachment{},
		Tasks:              map[string]protocol.Task{},
		TaskSubmissions:    map[string]protocol.TaskSubmissionReceipt{},
		TaskUpdates:        map[string]protocol.TaskUpdateReceipt{},
		Instances:          map[string]protocol.AgentInstance{},
		Allocations:        map[string]protocol.InstanceAllocation{},
		InstanceReceipts:   map[string]protocol.InstanceLifecycleReceipt{},
		ReleaseIntents:     map[string]protocol.InstanceReleaseIntent{},
		InstanceDeliveries: map[string]InstanceDelivery{},
		Runs:               map[string]protocol.Run{},
		SessionBindings:    map[string]protocol.HarnessSessionBinding{},
		WorkspaceLeases:    map[string]protocol.WorkspaceLease{},
		Approvals:          map[string]protocol.ApprovalRequest{},
		Artifacts:          map[string]protocol.Artifact{},
		ArtifactGrants:     map[string]protocol.ArtifactUploadGrantRecord{},
		Messages:           map[string]protocol.TaskMessage{},
		MessageAcks:        map[string]protocol.TaskMessageAck{},
		RunActivities:      map[string]protocol.RunActivity{},
		RunTranscripts:     map[string]protocol.RunTranscript{},
		EventStreams:       map[string]protocol.ThreadEventStream{},
		ThreadEvents:       map[string][]protocol.TaskEventEntry{},
	}
}

func (state *State) Clone() *State {
	encodedBytes, err := json.Marshal(state)
	if err != nil {
		panic("marshal store state for clone: " + err.Error())
	}
	cloned := NewState()
	if err := json.Unmarshal(encodedBytes, cloned); err != nil {
		panic("unmarshal store state for clone: " + err.Error())
	}
	cloned.ensureInitialized()
	return cloned
}

func (state *State) ensureInitialized() {
	if state.Nodes == nil {
		state.Nodes = map[string]protocol.ComputeNode{}
	}
	if state.CapabilityReports == nil {
		state.CapabilityReports = map[string]protocol.NodeCapabilityReport{}
	}
	if state.ComponentReports == nil {
		state.ComponentReports = map[string]protocol.ComponentInventoryReport{}
	}
	if state.ProjectProfiles == nil {
		state.ProjectProfiles = map[string]protocol.ProjectProfile{}
	}
	if state.Threads == nil {
		state.Threads = map[string]protocol.Thread{}
	}
	if state.Attachments == nil {
		state.Attachments = map[string]protocol.ThreadAttachment{}
	}
	if state.Tasks == nil {
		state.Tasks = map[string]protocol.Task{}
	}
	if state.TaskSubmissions == nil {
		state.TaskSubmissions = map[string]protocol.TaskSubmissionReceipt{}
	}
	if state.TaskUpdates == nil {
		state.TaskUpdates = map[string]protocol.TaskUpdateReceipt{}
	}
	if state.Instances == nil {
		state.Instances = map[string]protocol.AgentInstance{}
	}
	if state.Allocations == nil {
		state.Allocations = map[string]protocol.InstanceAllocation{}
	}
	if state.InstanceReceipts == nil {
		state.InstanceReceipts = map[string]protocol.InstanceLifecycleReceipt{}
	}
	if state.ReleaseIntents == nil {
		state.ReleaseIntents = map[string]protocol.InstanceReleaseIntent{}
	}
	if state.InstanceDeliveries == nil {
		state.InstanceDeliveries = map[string]InstanceDelivery{}
	}
	if state.Runs == nil {
		state.Runs = map[string]protocol.Run{}
	}
	if state.SessionBindings == nil {
		state.SessionBindings = map[string]protocol.HarnessSessionBinding{}
	}
	if state.WorkspaceLeases == nil {
		state.WorkspaceLeases = map[string]protocol.WorkspaceLease{}
	}
	if state.Approvals == nil {
		state.Approvals = map[string]protocol.ApprovalRequest{}
	}
	if state.Artifacts == nil {
		state.Artifacts = map[string]protocol.Artifact{}
	}
	if state.ArtifactGrants == nil {
		state.ArtifactGrants = map[string]protocol.ArtifactUploadGrantRecord{}
	}
	if state.Messages == nil {
		state.Messages = map[string]protocol.TaskMessage{}
	}
	if state.MessageAcks == nil {
		state.MessageAcks = map[string]protocol.TaskMessageAck{}
	}
	if state.RunActivities == nil {
		state.RunActivities = map[string]protocol.RunActivity{}
	}
	if state.RunTranscripts == nil {
		state.RunTranscripts = map[string]protocol.RunTranscript{}
	}
	if state.EventStreams == nil {
		state.EventStreams = map[string]protocol.ThreadEventStream{}
	}
	if state.ThreadEvents == nil {
		state.ThreadEvents = map[string][]protocol.TaskEventEntry{}
	}
}

func (state *State) ActiveAttachmentForThread(threadID string) (protocol.ThreadAttachment, bool) {
	for _, attachment := range state.Attachments {
		if attachment.ThreadID == threadID && attachment.Status == "attached" {
			return attachment, true
		}
	}
	return protocol.ThreadAttachment{}, false
}

func (state *State) ActiveAllocationForInstance(instanceID string) (protocol.InstanceAllocation, bool) {
	var best protocol.InstanceAllocation
	found := false
	for _, allocation := range state.Allocations {
		if allocation.InstanceID != instanceID || !protocol.IsOccupyingAllocationStatus(allocation.Status) {
			continue
		}
		if !found || allocation.CreatedAt >= best.CreatedAt {
			best = allocation
			found = true
		}
	}
	return best, found
}

func (state *State) LatestAllocationForInstance(instanceID string) (protocol.InstanceAllocation, bool) {
	var best protocol.InstanceAllocation
	found := false
	for _, allocation := range state.Allocations {
		if allocation.InstanceID != instanceID {
			continue
		}
		if !found || allocation.CreatedAt > best.CreatedAt || (allocation.CreatedAt == best.CreatedAt && allocation.ID > best.ID) {
			best = allocation
			found = true
		}
	}
	return best, found
}

func sortedKeys[Value any](records map[string]Value) []string {
	keys := slices.Collect(maps.Keys(records))
	slices.Sort(keys)
	return keys
}
