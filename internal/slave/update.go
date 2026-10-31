package slave

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/cafecito-games/Orchestra/internal/harness"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/selfupdate"
)

const updateHandoffMaximumAge = 5 * time.Minute

type updateHandoffSnapshot struct {
	NodeID    string               `json:"nodeId"`
	SavedAt   string               `json:"savedAt"`
	Instances []updateHandoffEntry `json:"instances"`
}

type updateHandoffEntry struct {
	Instance   protocol.AgentInstance      `json:"instance"`
	Allocation protocol.InstanceAllocation `json:"allocation"`
}

func (client *Client) settleSession() {
	client.mutex.Lock()
	if client.inFlightSessions > 0 {
		client.inFlightSessions--
	}
	notifyChannel := client.sessionSettled
	client.sessionSettled = make(chan struct{})
	client.mutex.Unlock()
	close(notifyChannel)
}

func (client *Client) handleNodeUpdate(directive protocol.NodeUpdateDirective) {
	if err := directive.Validate(); err != nil {
		return
	}
	if client.disableSelfUpdate {
		client.emitNodeUpdateStatus(directive.UpdateID, directive.TargetVersion, "failed", "", "self-update is disabled on this slave node")
		return
	}

	client.updateMutex.Lock()
	if client.activeUpdateID != "" {
		alreadySame := client.activeUpdateID == directive.UpdateID
		client.updateMutex.Unlock()
		if !alreadySame {
			client.emitNodeUpdateStatus(directive.UpdateID, directive.TargetVersion, "failed", "", "another self-update is already in progress on this slave node")
		}
		return
	}
	client.activeUpdateID = directive.UpdateID
	client.updateMutex.Unlock()

	clearActiveUpdate := func() {
		client.updateMutex.Lock()
		if client.activeUpdateID == directive.UpdateID {
			client.activeUpdateID = ""
		}
		client.updateMutex.Unlock()
	}

	executablePath := client.resolveExecutablePath()
	if executablePath == "" {
		clearActiveUpdate()
		client.emitNodeUpdateStatus(directive.UpdateID, directive.TargetVersion, "failed", "", "could not locate running slave executable")
		return
	}

	client.mutex.Lock()
	operationContext := client.rootContext
	currentVersion := client.node.Version
	client.mutex.Unlock()

	if currentVersion == directive.TargetVersion && !directive.Force {
		clearActiveUpdate()
		client.emitNodeUpdateStatus(directive.UpdateID, directive.TargetVersion, "up-to-date", "already running "+directive.TargetVersion, "")
		return
	}

	downloadURL, err := client.resolveHTTPURL(directive.DownloadPath)
	if err != nil {
		clearActiveUpdate()
		client.emitNodeUpdateStatus(directive.UpdateID, directive.TargetVersion, "failed", "", err.Error())
		return
	}

	client.logf("downloading self-update %s -> %s (update=%s)", currentVersion, directive.TargetVersion, directive.UpdateID)
	client.emitNodeUpdateStatus(directive.UpdateID, directive.TargetVersion, "downloading", "downloading verified platform bundle", "")

	stagingParent := os.TempDir()
	if client.dataRoot != "" {
		candidateParent := filepath.Join(client.dataRoot, "updates")
		if mkdirErr := os.MkdirAll(candidateParent, 0o700); mkdirErr == nil {
			stagingParent = candidateParent
		}
	}
	stagingDirectory, err := os.MkdirTemp(stagingParent, "slave-update-*")
	if err != nil {
		clearActiveUpdate()
		client.emitNodeUpdateStatus(directive.UpdateID, directive.TargetVersion, "failed", "", fmt.Sprintf("prepare staging directory: %v", err))
		return
	}
	defer os.RemoveAll(stagingDirectory)

	stagedBinaryPath, err := selfupdate.DownloadVerifiedBinary(
		operationContext,
		client.httpClient,
		downloadURL,
		client.token,
		directive.ExpectedSHA256,
		directive.SizeBytes,
		stagingDirectory,
	)
	if err != nil {
		clearActiveUpdate()
		client.emitNodeUpdateStatus(directive.UpdateID, directive.TargetVersion, "failed", "", err.Error())
		return
	}

	expectedProtocol := directive.ProtocolVersion
	if expectedProtocol == "" {
		expectedProtocol = protocol.ProtocolVersion
	}
	if _, probeErr := selfupdate.ProbeBinary(operationContext, stagedBinaryPath, directive.TargetVersion, expectedProtocol); probeErr != nil {
		clearActiveUpdate()
		client.emitNodeUpdateStatus(directive.UpdateID, directive.TargetVersion, "failed", "", probeErr.Error())
		return
	}

	if err := client.waitForActiveRunsDrain(operationContext, directive.UpdateID, directive.TargetVersion); err != nil {
		clearActiveUpdate()
		return
	}

	installOutcome, err := selfupdate.InstallBinary(stagedBinaryPath, executablePath, client.dataRoot)
	if err != nil {
		clearActiveUpdate()
		client.emitNodeUpdateStatus(directive.UpdateID, directive.TargetVersion, "failed", "", err.Error())
		return
	}

	for {
		client.mutex.Lock()
		if len(client.sessions) == 0 && client.inFlightSessions == 0 {
			client.restartingForUpdate = true
			client.pendingExecPath = installOutcome.InstalledPath
			cancelRunLoop := client.cancelRunLoop
			client.mutex.Unlock()

			client.saveUpdateHandoff()
			restartNote := "restarting into " + directive.TargetVersion
			if installOutcome.UsedDataRootFallback {
				restartNote += " (installed under data-root)"
			}
			client.logf("applying self-update %s -> %s via %s", currentVersion, directive.TargetVersion, installOutcome.InstalledPath)
			client.emitNodeUpdateStatus(directive.UpdateID, directive.TargetVersion, "restarting", restartNote, "")
			client.flushAndCloseForRestart()
			if cancelRunLoop != nil {
				cancelRunLoop()
			}
			return
		}
		client.mutex.Unlock()

		if err := client.waitForActiveRunsDrain(operationContext, directive.UpdateID, directive.TargetVersion); err != nil {
			clearActiveUpdate()
			return
		}
	}
}

func (client *Client) waitForActiveRunsDrain(
	operationContext context.Context,
	updateID string,
	targetVersion string,
) error {
	notifiedDraining := false
	for {
		client.mutex.Lock()
		remaining := max(len(client.sessions), client.inFlightSessions)
		settledChannel := client.sessionSettled
		client.mutex.Unlock()

		if remaining == 0 {
			return nil
		}
		if !notifiedDraining {
			notifiedDraining = true
			client.logf("waiting for %d active run(s) to finish before restarting into %s", remaining, targetVersion)
			client.emitNodeUpdateStatus(
				updateID,
				targetVersion,
				"draining",
				fmt.Sprintf("waiting for %d active run(s) to complete", remaining),
				"",
			)
		}
		select {
		case <-operationContext.Done():
			return operationContext.Err()
		case <-settledChannel:
		}
	}
}

func (client *Client) emitNodeUpdateStatus(
	updateID string,
	targetVersion string,
	status string,
	messageText string,
	errorText string,
) {
	observedAt := client.nowUTC()
	report := protocol.NodeUpdateReport{
		UpdateID:       updateID,
		NodeID:         client.node.ID,
		Status:         status,
		CurrentVersion: client.node.Version,
		TargetVersion:  targetVersion,
		Message:        protocol.TruncateDiagnostic(protocol.RedactText(messageText)),
		Error:          protocol.TruncateDiagnostic(protocol.RedactText(errorText)),
		ObservedAt:     observedAt,
	}
	client.sendOrQueue(protocol.SlaveControlMessage{
		Type:       "node.update.status",
		NodeUpdate: &report,
		At:         observedAt,
	})
}

func (client *Client) flushAndCloseForRestart() {
	client.writeMutex.Lock()
	activeConnection := client.connection
	client.writeMutex.Unlock()
	if activeConnection == nil {
		return
	}
	if client.admitted.Load() {
		flushContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = client.flushOutbox(flushContext, activeConnection)
		cancel()
	}
	_ = activeConnection.Close(websocket.StatusGoingAway, "restarting for self-update")
}

func (client *Client) resolveExecutablePath() string {
	if client.executablePath != "" {
		return client.executablePath
	}
	if registered := harness.OrchestraExecutable(); registered != "" {
		return registered
	}
	detected, err := os.Executable()
	if err != nil {
		return ""
	}
	if evaluated, evalErr := filepath.EvalSymlinks(detected); evalErr == nil {
		detected = evaluated
	}
	return filepath.Clean(detected)
}

func (client *Client) takePendingExecPath() string {
	client.mutex.Lock()
	defer client.mutex.Unlock()
	target := client.pendingExecPath
	client.pendingExecPath = ""
	return target
}

func (client *Client) invokeExecReplacement(targetPath string) error {
	execFunction := client.execReplacement
	if execFunction == nil {
		execFunction = selfupdate.ExecReplacement
	}
	arguments := client.processArguments
	if arguments == nil {
		if len(os.Args) > 1 {
			arguments = os.Args[1:]
		} else {
			arguments = []string{}
		}
	}
	environment := client.processEnvironment
	if environment == nil {
		environment = os.Environ()
	}
	if err := execFunction(targetPath, arguments, environment); err != nil {
		if handoffPath := client.handoffFilePath(); handoffPath != "" {
			_ = os.Remove(handoffPath)
		}
		return fmt.Errorf("exec updated slave binary %s: %w", targetPath, err)
	}
	return nil
}

func (client *Client) handoffFilePath() string {
	if client.dataRoot == "" || !protocol.IsIdentifier(client.node.ID) {
		return ""
	}
	safeNodeID := strings.Map(func(character rune) rune {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '-' || character == '_' {
			return character
		}
		return '_'
	}, client.node.ID)
	return filepath.Join(client.dataRoot, "state", "update-handoff-"+safeNodeID+".json")
}

func (client *Client) saveUpdateHandoff() {
	handoffPath := client.handoffFilePath()
	if handoffPath == "" {
		return
	}

	client.mutex.Lock()
	entries := make([]updateHandoffEntry, 0, len(client.instances))
	for _, resident := range client.instances {
		if resident.status == "active" && resident.releaseMode == "" {
			entries = append(entries, updateHandoffEntry{
				Instance:   resident.instance,
				Allocation: resident.allocation,
			})
		}
	}
	client.mutex.Unlock()

	if len(entries) == 0 {
		_ = os.Remove(handoffPath)
		return
	}

	if err := os.MkdirAll(filepath.Dir(handoffPath), 0o700); err != nil {
		return
	}
	snapshot := updateHandoffSnapshot{
		NodeID:    client.node.ID,
		SavedAt:   client.nowUTC(),
		Instances: entries,
	}
	encodedBytes, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return
	}
	temporaryFile, err := os.CreateTemp(filepath.Dir(handoffPath), ".handoff-*")
	if err != nil {
		return
	}
	temporaryPath := temporaryFile.Name()
	if _, err := temporaryFile.Write(encodedBytes); err != nil {
		_ = temporaryFile.Close()
		_ = os.Remove(temporaryPath)
		return
	}
	_ = temporaryFile.Chmod(0o600)
	_ = temporaryFile.Sync()
	_ = temporaryFile.Close()
	if err := os.Rename(temporaryPath, handoffPath); err != nil {
		_ = os.Remove(temporaryPath)
	}
}

func (client *Client) restoreUpdateHandoff() {
	handoffPath := client.handoffFilePath()
	if handoffPath == "" {
		return
	}
	encodedBytes, err := os.ReadFile(handoffPath)
	if err != nil {
		return
	}
	_ = os.Remove(handoffPath)

	var snapshot updateHandoffSnapshot
	if err := json.Unmarshal(encodedBytes, &snapshot); err != nil {
		return
	}
	if snapshot.NodeID != client.node.ID {
		return
	}
	savedTime, validTime := protocol.ParseTimestamp(snapshot.SavedAt)
	if !validTime || client.now().Sub(savedTime) > updateHandoffMaximumAge {
		return
	}

	client.mutex.Lock()
	defer client.mutex.Unlock()
	capacity := client.instanceCapacity()
	for _, entry := range snapshot.Instances {
		if !protocol.IsIdentifier(entry.Instance.ID) || !protocol.IsIdentifier(entry.Allocation.ID) {
			continue
		}
		if capacity >= 0 && len(client.instances) >= capacity {
			break
		}
		if err := client.validateProvision(entry.Instance, entry.Allocation); err != nil {
			continue
		}
		client.instances[entry.Instance.ID] = &residentInstance{
			instance:   entry.Instance,
			allocation: entry.Allocation,
			status:     "active",
		}
	}
}
