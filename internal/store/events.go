package store

import (
	"fmt"
	"reflect"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

func synthesizeThreadEvents(
	previous *State,
	working *State,
	explicitEvents []protocol.TaskEventEntry,
	occurredAt string,
) ([]protocol.TaskEventEntry, map[string]int64) {
	var candidates []protocol.TaskEventEntry

	for _, threadID := range sortedKeys(working.Threads) {
		afterThread := working.Threads[threadID]
		beforeThread, existed := previous.Threads[threadID]
		if !existed {
			candidates = append(candidates, protocol.TaskEventEntry{
				ThreadID:   threadID,
				Type:       "thread.updated",
				Status:     afterThread.Status,
				Summary:    fmt.Sprintf("Thread %q created", afterThread.Title),
				OccurredAt: chooseTimestamp(afterThread.UpdatedAt, occurredAt),
			})
			continue
		}
		if beforeThread.Title != afterThread.Title || beforeThread.Summary != afterThread.Summary || beforeThread.Status != afterThread.Status {
			candidates = append(candidates, protocol.TaskEventEntry{
				ThreadID:   threadID,
				Type:       "thread.updated",
				Status:     afterThread.Status,
				Summary:    fmt.Sprintf("Thread %q is %s", afterThread.Title, afterThread.Status),
				OccurredAt: chooseTimestamp(afterThread.UpdatedAt, occurredAt),
			})
		}
	}

	for _, instanceID := range sortedKeys(working.Instances) {
		afterInstance := working.Instances[instanceID]
		if afterInstance.ThreadID == "" {
			continue
		}
		beforeInstance, existed := previous.Instances[instanceID]
		if !existed {
			candidates = append(candidates, protocol.TaskEventEntry{
				ThreadID:   afterInstance.ThreadID,
				Type:       "instance.created",
				InstanceID: instanceID,
				Status:     afterInstance.Status,
				Summary:    fmt.Sprintf("Instance %s requested (%s)", instanceID, afterInstance.Status),
				OccurredAt: chooseTimestamp(afterInstance.CreatedAt, occurredAt),
			})
			continue
		}
		if beforeInstance.Status != afterInstance.Status {
			candidates = append(candidates, protocol.TaskEventEntry{
				ThreadID:   afterInstance.ThreadID,
				Type:       "instance.updated",
				InstanceID: instanceID,
				Status:     afterInstance.Status,
				Summary:    fmt.Sprintf("Instance %s became %s", instanceID, afterInstance.Status),
				OccurredAt: chooseTimestamp(afterInstance.UpdatedAt, occurredAt),
			})
		}
	}

	for _, taskID := range sortedKeys(working.Tasks) {
		afterTask := working.Tasks[taskID]
		if afterTask.ThreadID == "" {
			continue
		}
		beforeTask, existed := previous.Tasks[taskID]
		if !existed {
			candidates = append(candidates, protocol.TaskEventEntry{
				ThreadID:   afterTask.ThreadID,
				Type:       "task.created",
				TaskID:     taskID,
				Status:     afterTask.Status,
				Summary:    fmt.Sprintf("Task %q submitted (%s)", afterTask.Title, afterTask.Status),
				OccurredAt: chooseTimestamp(afterTask.CreatedAt, occurredAt),
			})
			continue
		}
		if beforeTask.Status != afterTask.Status || beforeTask.Summary != afterTask.Summary || beforeTask.BlockerReason != afterTask.BlockerReason || !reflect.DeepEqual(beforeTask.Assignment, afterTask.Assignment) {
			summary := afterTask.Summary
			if summary == "" {
				if afterTask.BlockerReason != "" {
					summary = afterTask.BlockerReason
				} else {
					summary = fmt.Sprintf("Task %q is %s", afterTask.Title, afterTask.Status)
				}
			}
			runID := ""
			instanceID := ""
			if afterTask.Assignment != nil {
				runID = afterTask.Assignment.RunID
				instanceID = afterTask.Assignment.InstanceID
			}
			candidates = append(candidates, protocol.TaskEventEntry{
				ThreadID:   afterTask.ThreadID,
				Type:       "task.updated",
				TaskID:     taskID,
				RunID:      runID,
				InstanceID: instanceID,
				Status:     afterTask.Status,
				Summary:    summary,
				OccurredAt: chooseTimestamp(afterTask.UpdatedAt, occurredAt),
			})
		}
	}

	for _, runID := range sortedKeys(working.Runs) {
		afterRun := working.Runs[runID]
		if afterRun.ThreadID == "" {
			continue
		}
		taskID := ""
		if afterRun.TaskID != nil {
			taskID = *afterRun.TaskID
		}
		beforeRun, existed := previous.Runs[runID]
		if !existed {
			candidates = append(candidates, protocol.TaskEventEntry{
				ThreadID:   afterRun.ThreadID,
				Type:       "run.created",
				TaskID:     taskID,
				RunID:      runID,
				InstanceID: afterRun.InstanceID,
				Status:     afterRun.Status,
				Summary:    fmt.Sprintf("Run %s queued on %s (%s)", runID, afterRun.NodeID, afterRun.HarnessID),
				OccurredAt: chooseTimestamp(afterRun.CreatedAt, occurredAt),
			})
			continue
		}
		if beforeRun.Status != afterRun.Status {
			summary := fmt.Sprintf("Run %s %s", runID, afterRun.Status)
			if afterRun.Error != nil && *afterRun.Error != "" {
				summary = fmt.Sprintf("Run %s %s: %s", runID, afterRun.Status, *afterRun.Error)
			}
			candidates = append(candidates, protocol.TaskEventEntry{
				ThreadID:   afterRun.ThreadID,
				Type:       "run.updated",
				TaskID:     taskID,
				RunID:      runID,
				InstanceID: afterRun.InstanceID,
				Status:     afterRun.Status,
				Summary:    summary,
				OccurredAt: occurredAt,
			})
		}
	}

	for _, messageID := range sortedKeys(working.Messages) {
		if _, existed := previous.Messages[messageID]; existed {
			continue
		}
		afterMessage := working.Messages[messageID]
		if afterMessage.ThreadID == "" {
			continue
		}
		messageCopy := afterMessage
		taskID := afterMessage.Recipient.TaskID
		if taskID == "" {
			taskID = afterMessage.Sender.TaskID
		}
		candidates = append(candidates, protocol.TaskEventEntry{
			ThreadID:   afterMessage.ThreadID,
			Type:       "message.posted",
			TaskID:     taskID,
			MessageID:  messageID,
			Status:     afterMessage.Kind,
			Summary:    protocol.TruncateUTF8Head(afterMessage.Body, 240),
			Message:    &messageCopy,
			OccurredAt: chooseTimestamp(afterMessage.CreatedAt, occurredAt),
		})
	}

	for _, approvalID := range sortedKeys(working.Approvals) {
		afterApproval := working.Approvals[approvalID]
		if afterApproval.ThreadID == "" {
			continue
		}
		beforeApproval, existed := previous.Approvals[approvalID]
		if !existed {
			candidates = append(candidates, protocol.TaskEventEntry{
				ThreadID:   afterApproval.ThreadID,
				Type:       "approval.requested",
				TaskID:     afterApproval.TaskID,
				RunID:      afterApproval.RunID,
				InstanceID: afterApproval.InstanceID,
				ApprovalID: approvalID,
				Status:     afterApproval.Status,
				Summary:    fmt.Sprintf("Approval requested: %s", afterApproval.Title),
				OccurredAt: chooseTimestamp(afterApproval.RequestedAt, occurredAt),
			})
			continue
		}
		if beforeApproval.Status != afterApproval.Status || beforeApproval.DeliveryState != afterApproval.DeliveryState {
			candidates = append(candidates, protocol.TaskEventEntry{
				ThreadID:   afterApproval.ThreadID,
				Type:       "approval.updated",
				TaskID:     afterApproval.TaskID,
				RunID:      afterApproval.RunID,
				InstanceID: afterApproval.InstanceID,
				ApprovalID: approvalID,
				Status:     afterApproval.Status,
				Summary:    fmt.Sprintf("Approval %s is %s (%s)", approvalID, afterApproval.Status, afterApproval.DeliveryState),
				OccurredAt: chooseTimestamp(afterApproval.UpdatedAt, occurredAt),
			})
		}
	}

	for _, artifactID := range sortedKeys(working.Artifacts) {
		afterArtifact := working.Artifacts[artifactID]
		if afterArtifact.ThreadID == "" || !afterArtifact.Uploaded {
			continue
		}
		beforeArtifact, existed := previous.Artifacts[artifactID]
		if !existed || !beforeArtifact.Uploaded {
			candidates = append(candidates, protocol.TaskEventEntry{
				ThreadID:   afterArtifact.ThreadID,
				Type:       "artifact.posted",
				TaskID:     afterArtifact.TaskID,
				RunID:      afterArtifact.RunID,
				ArtifactID: artifactID,
				Status:     "uploaded",
				Summary:    fmt.Sprintf("Artifact %q uploaded (%d bytes)", afterArtifact.Name, afterArtifact.SizeBytes),
				OccurredAt: chooseTimestamp(afterArtifact.UploadedAt, occurredAt),
			})
		}
	}

	candidates = append(candidates, explicitEvents...)

	var committedEvents []protocol.TaskEventEntry
	prunedBelow := map[string]int64{}

	for _, candidate := range candidates {
		if candidate.ThreadID == "" {
			continue
		}
		stream, exists := working.EventStreams[candidate.ThreadID]
		if !exists {
			stream = protocol.ThreadEventStream{
				ThreadID:  candidate.ThreadID,
				Head:      0,
				Floor:     0,
				UpdatedAt: occurredAt,
			}
		}
		stream.Head++
		candidate.Sequence = stream.Head
		if candidate.OccurredAt == "" {
			candidate.OccurredAt = occurredAt
		}
		stream.UpdatedAt = candidate.OccurredAt

		events := append(working.ThreadEvents[candidate.ThreadID], candidate)
		if len(events) > protocol.ThreadEventRetentionLimit {
			trimCount := len(events) - protocol.ThreadEventRetentionLimit
			events = events[trimCount:]
		}
		if len(events) > 0 {
			stream.Floor = events[0].Sequence - 1
		}
		working.ThreadEvents[candidate.ThreadID] = events
		working.EventStreams[candidate.ThreadID] = stream
		prunedBelow[candidate.ThreadID] = stream.Floor
		committedEvents = append(committedEvents, candidate)
	}

	return committedEvents, prunedBelow
}

func chooseTimestamp(primary, fallback string) string {
	if primary != "" {
		return primary
	}
	return fallback
}
