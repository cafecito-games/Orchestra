//go:build system

package systemtest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/slave"
)

func TestSystemAutonomousCoordinatorWakeMultiStageDAG(t *testing.T) {
	binaries := ensureCompiledBinaries(t)
	fakeHarnesses := installFakeHarnesses(t, binaries.fakeHarnessPath)

	workspaceRoot := filepath.Join(t.TempDir(), "workspace")
	require.NoError(t, os.MkdirAll(workspaceRoot, 0o755))
	canonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{workspaceRoot})
	require.NoError(t, err)

	const bearerToken = "system-coord-wake-token"
	listenAddress := allocateFreeLoopbackAddress(t)
	masterBaseURL := "http://" + listenAddress
	masterDataDir := filepath.Join(t.TempDir(), "master-data")
	workerEnv := buildWorkerEnvironment(fakeHarnesses, "ANTHROPIC_MOCK_LOCAL=1")

	masterProcess := startManagedProcess(t, "coord-wake-master", binaries.orchestraPath, []string{
		"master",
		"--config", "none",
		"--listen", listenAddress,
		"--token", bearerToken,
		"--data-dir", masterDataDir,
	}, workerEnv)
	waitForMasterReady(t, masterBaseURL, masterProcess)

	slaveProcess := startManagedProcess(t, "coord-wake-slave", binaries.orchestraPath, []string{
		"slave",
		"--config", "none",
		"--endpoint", masterBaseURL,
		"--token", bearerToken,
		"--id", "coord-wake-node",
		"--name", "Coordinator Wake Node",
		"--concurrency", "4",
		"--instance-capacity", "4",
		"--workspace-root", canonicalRoots[0],
		"--claude-acp-auth-mode", "local-subscription",
		"--approval-policy", "claude-cli=auto",
		"--approval-policy", "codex-cli=auto",
		"--acp-adapter", fakeHarnesses.claudeACPArgument,
		"--acp-adapter", fakeHarnesses.codexACPArgument,
	}, workerEnv)

	cliEnv := append(slices.Clone(workerEnv),
		"ORCHESTRA_ENDPOINT="+masterBaseURL,
		"ORCHESTRA_TOKEN="+bearerToken,
	)

	require.Eventually(t, func() bool {
		output, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnv, "nodes", "--json")
		var response struct {
			Nodes []protocol.NodeDetail `json:"nodes"`
		}
		if json.Unmarshal([]byte(output), &response) != nil {
			return false
		}
		for _, detail := range response.Nodes {
			if detail.Node.ID == "coord-wake-node" && detail.Connected && detail.Synced {
				return true
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond, "master:\n%s\nslave:\n%s", masterProcess.logs(), slaveProcess.logs())

	// Open a short-lived MCP session solely to create the thread and spawn the autonomous
	// coordinator instance, then stop it so the rest of the multi-stage DAG runs unattended.
	mcpSession := startMCPStdioSession(t, binaries.orchestraPath, masterBaseURL, bearerToken, "coord-bootstrap")

	var createdThread struct {
		Thread protocol.Thread `json:"thread"`
	}
	mcpSession.callTool(t, "create_thread", map[string]any{
		"title":  "Unattended Autonomous Coordinator DAG",
		"attach": false,
	}, &createdThread)
	threadID := createdThread.Thread.ID
	require.NotEmpty(t, threadID)

	// Stage 2 script (embedded in Stage 1's completion summary so the coordinator's wake turn
	// executes it automatically when Stage 1 finishes).
	coordinatorResolveScript := formatFakeScript(t,
		map[string]any{
			"call": "update_thread",
			"arguments": map[string]any{
				"status":  "completed",
				"summary": "Resolved autonomously by resident coordinator after stage 1",
			},
		},
		map[string]any{
			"message": "Coordinator finalized thread autonomously.",
		},
	)

	stageOneWorkerScript := formatFakeScript(t,
		map[string]any{
			"call": "update_task",
			"arguments": map[string]any{
				"idempotencyKey": "stage-1-complete",
				"completion": map[string]any{
					"summary": "Stage 1 finished. " + coordinatorResolveScript,
				},
			},
		},
		map[string]any{
			"message": "Stage 1 worker done.",
		},
	)

	coordinatorPlanScript := formatFakeScript(t,
		map[string]any{
			"call": "submit_tasks",
			"arguments": map[string]any{
				"idempotencyKey": "coord-plan-stage-1",
				"tasks": []map[string]any{{
					"key":          "stage-1",
					"title":        "Stage 1 Worker Task",
					"instructions": stageOneWorkerScript,
					"requirements": map[string]any{
						"harnessIds": []string{"claude-cli"},
						"transports": []string{"acp-v1"},
					},
				}},
			},
		},
		map[string]any{
			"message": "Coordinator submitted Stage 1.",
		},
	)

	var spawned protocol.InstanceLifecycleResult
	mcpSession.callTool(t, "spawn_instance", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "spawn-autonomous-coord",
		"handle":         "lead-coord",
		"coordinator":    true,
		"delegation":     map[string]any{"canDelegate": true},
		"requirements": map[string]any{
			"harnessIds": []string{"claude-cli"},
			"transports": []string{"acp-v1"},
		},
		"initialTask": map[string]any{
			"title":        "Plan Stage 1",
			"instructions": coordinatorPlanScript,
		},
	}, &spawned)
	coordInstanceID := spawned.Instance.ID
	require.NotEmpty(t, coordInstanceID)

	// Disconnect external MCP client completely; orchestration proceeds unattended.
	_ = mcpSession.stdin.Close()

	var finalDetail protocol.ThreadDetailResponse
	var lastJSON string
	resolved := assert.Eventually(t, func() bool {
		output, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnv, "thread", threadID, "--json")
		lastJSON = output
		var detail protocol.ThreadDetailResponse
		if json.Unmarshal([]byte(output), &detail) != nil {
			return false
		}
		finalDetail = detail
		return detail.Thread.Status == "resolved"
	}, 10*time.Second, 250*time.Millisecond)
	require.True(t, resolved, "detail:\n%s\nmaster:\n%s\nslave:\n%s", lastJSON, masterProcess.logs(), slaveProcess.logs())

	require.NotNil(t, finalDetail.Thread.Orchestrator)
	assert.Equal(t, "instance", finalDetail.Thread.Orchestrator.Kind)
	assert.Equal(t, coordInstanceID, finalDetail.Thread.Orchestrator.InstanceID)
	assert.Positive(t, finalDetail.Thread.Orchestrator.LastHandledEventSequence)
	assert.Empty(t, finalDetail.Thread.Orchestrator.PendingWakeTaskID)

	foundWakeTask := false
	for _, task := range finalDetail.Tasks {
		if task.Origin.Kind == "coordinator-wake" {
			foundWakeTask = true
			assert.Equal(t, "completed", task.Status)
			assert.Equal(t, coordInstanceID, task.PlacementInstanceID)
		}
	}
	assert.True(t, foundWakeTask, "expected an autonomous coordinator-wake task to execute")

	// Verify human-readable CLI output surfaces Coordinator Instance details.
	textOutput, stderrOutput := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnv, "thread", threadID)
	assert.Empty(t, stderrOutput)
	assert.Contains(t, textOutput, "Coordinator Instance: "+coordInstanceID)
}
