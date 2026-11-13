package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/slave"
	"github.com/cafecito-games/Orchestra/internal/store"
)

type controllableClock struct {
	mutex   sync.Mutex
	current time.Time
}

func newControllableClock(initial time.Time) *controllableClock {
	return &controllableClock{current: initial.UTC()}
}

func (clock *controllableClock) Now() time.Time {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	return clock.current
}

func (clock *controllableClock) Advance(delta time.Duration) time.Time {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	clock.current = clock.current.Add(delta)
	return clock.current
}

func stringSlicePointer(values ...string) *[]string {
	copied := append([]string(nil), values...)
	return &copied
}

func filterMailboxesByThread(mailboxes []protocol.AgentMailbox, threadID string) []protocol.AgentMailbox {
	var matching []protocol.AgentMailbox
	for _, mailbox := range mailboxes {
		if mailbox.ThreadID == threadID {
			matching = append(matching, mailbox)
		}
	}
	return matching
}

func connectMockSlaveNode(
	t *testing.T,
	httpServer *httptest.Server,
	service *Service,
	node protocol.ComputeNode,
) (*websocket.Conn, <-chan protocol.MasterControlMessage) {
	t.Helper()

	websocketURL := strings.Replace(httpServer.URL, "http://", "ws://", 1) + "/control-agent"
	dialContext, cancelDial := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelDial()

	slaveConnection, dialResponse, err := websocket.Dial(dialContext, websocketURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer master-secret-token"}},
	})
	if dialResponse != nil && dialResponse.Body != nil {
		_ = dialResponse.Body.Close()
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = slaveConnection.Close(websocket.StatusNormalClosure, "test finished")
	})

	registerPayload, err := json.Marshal(protocol.SlaveControlMessage{
		Type:            "register",
		ProtocolVersion: protocol.ProtocolVersion,
		Node:            &node,
	})
	require.NoError(t, err)
	require.NoError(t, slaveConnection.Write(t.Context(), websocket.MessageText, registerPayload))

	syncPayload, err := json.Marshal(protocol.SlaveControlMessage{
		Type: "sync.complete",
	})
	require.NoError(t, err)
	require.NoError(t, slaveConnection.Write(t.Context(), websocket.MessageText, syncPayload))

	require.Eventually(t, func() bool {
		return service.isNodeLiveAndSynced(node.ID)
	}, 5*time.Second, 20*time.Millisecond)

	inboundMessages := make(chan protocol.MasterControlMessage, 64)
	go func() {
		for {
			_, rawFrame, readErr := slaveConnection.Read(context.Background())
			if readErr != nil {
				close(inboundMessages)
				return
			}
			var decoded protocol.MasterControlMessage
			if json.Unmarshal(rawFrame, &decoded) == nil {
				inboundMessages <- decoded
			}
		}
	}()

	return slaveConnection, inboundMessages
}

func sendSlaveControlMessage(
	t *testing.T,
	connection *websocket.Conn,
	message protocol.SlaveControlMessage,
) {
	t.Helper()
	encodedBytes, err := json.Marshal(message)
	require.NoError(t, err)
	require.NoError(t, connection.Write(t.Context(), websocket.MessageText, encodedBytes))
}

func awaitMasterMessageOfType(
	t *testing.T,
	messages <-chan protocol.MasterControlMessage,
	expectedType string,
) protocol.MasterControlMessage {
	t.Helper()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case message, open := <-messages:
			require.True(t, open, "slave connection closed while waiting for %s", expectedType)
			if message.Type == expectedType {
				return message
			}
		case <-timeout.C:
			t.Fatalf("timed out waiting for master message %q", expectedType)
		}
	}
}

func TestOfferingCooldowns_ClassificationInventoryAndModelFallback(t *testing.T) {
	clock := newControllableClock(time.Date(2026, 11, 13, 21, 0, 0, 0, time.UTC))

	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
		Now:   clock.Now,
	})
	require.NoError(t, err)
	defer service.Close()

	httpServer := httptest.NewServer(service.Handler())
	defer httpServer.Close()

	instanceCapacity := 12
	activeInstances := 0
	canonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{t.TempDir()})
	require.NoError(t, err)
	slaveConnection, _ := connectMockSlaveNode(t, httpServer, service, protocol.ComputeNode{
		ID:               "node-alpha",
		Name:             "node-alpha",
		Kind:             "local",
		Platform:         "darwin-arm64",
		Status:           "online",
		LastSeen:         clock.Now().Format(time.RFC3339Nano),
		Concurrency:      4,
		InstanceCapacity: &instanceCapacity,
		ActiveInstances:  &activeInstances,
		WorkspaceRoots:   canonicalRoots,
		Version:          "1.0.0",
		Harnesses: []protocol.HarnessProfile{{
			ID:         "claude-cli",
			Label:      "Claude Code",
			Available:  true,
			AuthMode:   "local-subscription",
			Models:     []string{"sonnet", "opus", "haiku"},
			Transports: []string{protocol.TransportNativeCLI},
		}},
	})

	coordinatorCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "coord-quota",
		SessionID: "sess-quota",
	}
	thread, _, err := service.CreateThread(coordinatorCaller, CreateThreadInput{Title: "Quota Fallback Thread"})
	require.NoError(t, err)

	recordFailedOfferingRun := func(runID, model, explicitErrorKind, diagnosticText string) {
		t.Helper()
		nowText := clock.Now().Format(time.RFC3339Nano)
		taskID := protocol.NewIdentifier("task")
		instanceID := protocol.NewIdentifier("inst")
		allocationID := protocol.NewIdentifier("alloc")
		mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
			state.Instances[instanceID] = protocol.AgentInstance{
				ID:          instanceID,
				ThreadID:    thread.ID,
				Status:      "busy",
				AutoManaged: true,
				Lease: protocol.InstanceLease{
					ExpiresAt:          extendLeaseTimestamp(nowText, 300),
					IdleTimeoutSeconds: 300,
				},
				CreatedAt: nowText,
				UpdatedAt: nowText,
			}
			state.Allocations[allocationID] = protocol.InstanceAllocation{
				ID:         allocationID,
				InstanceID: instanceID,
				NodeID:     "node-alpha",
				HarnessID:  "claude-cli",
				Transport:  protocol.TransportNativeCLI,
				Model:      model,
				Status:     "active",
				CreatedAt:  nowText,
				UpdatedAt:  nowText,
			}
			state.Tasks[taskID] = protocol.Task{
				ID:           taskID,
				ThreadID:     thread.ID,
				Title:        "Offering test run",
				Instructions: "Run workload",
				Status:       "running",
				AttemptCount: 1,
				MaxAttempts:  1,
				Assignment: &protocol.TaskAssignment{
					RunID:      runID,
					NodeID:     "node-alpha",
					InstanceID: instanceID,
					Attempt:    1,
					AssignedAt: nowText,
				},
				CreatedAt: nowText,
				UpdatedAt: nowText,
			}
			state.Runs[runID] = protocol.Run{
				ID:           runID,
				ThreadID:     thread.ID,
				TaskID:       &taskID,
				NodeID:       "node-alpha",
				InstanceID:   instanceID,
				AllocationID: allocationID,
				HarnessID:    "claude-cli",
				Transport:    protocol.TransportNativeCLI,
				Model:        model,
				Status:       "running",
				CreatedAt:    nowText,
			}
			return nil
		})

		sendSlaveControlMessage(t, slaveConnection, protocol.SlaveControlMessage{
			Type:      "run.failed",
			RunID:     runID,
			ErrorKind: explicitErrorKind,
			Error:     &diagnosticText,
		})

		require.Eventually(t, func() bool {
			var terminal bool
			_ = openedStore.Read(func(state *store.State) error {
				terminal = state.Runs[runID].Status == "failed"
				return nil
			})
			return terminal
		}, 3*time.Second, 20*time.Millisecond)
	}

	recordFailedOfferingRun(
		"run_quota_sonnet_1",
		"sonnet",
		protocol.RunErrorKindQuotaExhausted,
		"credit balance is too low for model sonnet",
	)

	require.NoError(t, openedStore.Read(func(state *store.State) error {
		failedRun := state.Runs["run_quota_sonnet_1"]
		assert.Equal(t, protocol.RunErrorKindQuotaExhausted, failedRun.ErrorKind)
		assert.Equal(t, "online", state.Nodes["node-alpha"].Status)
		return nil
	}))

	inventory, err := service.GetExecutionInventory()
	require.NoError(t, err)
	require.Len(t, inventory.OfferingCooldowns, 1)
	assert.Equal(t, "node-alpha", inventory.OfferingCooldowns[0].NodeID)
	assert.Equal(t, "claude-cli", inventory.OfferingCooldowns[0].HarnessID)
	assert.Equal(t, "sonnet", inventory.OfferingCooldowns[0].Model)
	assert.Equal(t, protocol.RunErrorKindQuotaExhausted, inventory.OfferingCooldowns[0].ErrorKind)

	nodeDetails, err := service.ListNodeDetails()
	require.NoError(t, err)
	require.Len(t, nodeDetails, 1)
	require.Len(t, nodeDetails[0].OfferingCooldowns, 1)
	assert.Equal(t, "sonnet", nodeDetails[0].OfferingCooldowns[0].Model)

	selectionTable := []struct {
		name           string
		idempotencyKey string
		allowedModels  *[]string
		expectedModel  string
	}{
		{
			name:           "unspecified models automatically skip cooled-down sonnet and pick opus",
			idempotencyKey: "spawn-fallback-unspecified",
			allowedModels:  nil,
			expectedModel:  "opus",
		},
		{
			name:           "multi-model requirement prefers non-cooled-down opus over sonnet",
			idempotencyKey: "spawn-fallback-multi",
			allowedModels:  stringSlicePointer("sonnet", "opus"),
			expectedModel:  "opus",
		},
		{
			name:           "single-model requirement still permits cooled-down sonnet when no alternative allowed",
			idempotencyKey: "spawn-fallback-pinned",
			allowedModels:  stringSlicePointer("sonnet"),
			expectedModel:  "sonnet",
		},
	}

	for _, testCase := range selectionTable {
		t.Run(testCase.name, func(t *testing.T) {
			spawned, spawnErr := service.SpawnInstance(coordinatorCaller, SpawnInstanceInput{
				ThreadID:       thread.ID,
				IdempotencyKey: testCase.idempotencyKey,
				Requirements: &protocol.InstanceExecutionRequirements{
					HarnessIDs: stringSlicePointer("claude-cli"),
					Models:     testCase.allowedModels,
				},
			})
			require.NoError(t, spawnErr)
			require.NotNil(t, spawned.Allocation)
			assert.Equal(t, testCase.expectedModel, spawned.Allocation.Model)
		})
	}

	recordFailedOfferingRun(
		"run_ratelimit_opus_1",
		"opus",
		"",
		"HTTP 429 Too Many Requests: rate_limit_error on model opus",
	)

	require.NoError(t, openedStore.Read(func(state *store.State) error {
		assert.Equal(t, protocol.RunErrorKindRateLimited, state.Runs["run_ratelimit_opus_1"].ErrorKind)
		return nil
	}))

	spawnedHaiku, err := service.SpawnInstance(coordinatorCaller, SpawnInstanceInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "spawn-fallback-haiku",
		Requirements: &protocol.InstanceExecutionRequirements{
			HarnessIDs: stringSlicePointer("claude-cli"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, spawnedHaiku.Allocation)
	assert.Equal(t, "haiku", spawnedHaiku.Allocation.Model)

	clock.Advance(61 * time.Second)
	inventoryAfterMinute, err := service.GetExecutionInventory()
	require.NoError(t, err)
	require.Len(t, inventoryAfterMinute.OfferingCooldowns, 1)
	assert.Equal(t, "sonnet", inventoryAfterMinute.OfferingCooldowns[0].Model)

	spawnedOpusAgain, err := service.SpawnInstance(coordinatorCaller, SpawnInstanceInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "spawn-opus-after-ratelimit-expiry",
		Requirements: &protocol.InstanceExecutionRequirements{
			HarnessIDs: stringSlicePointer("claude-cli"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, spawnedOpusAgain.Allocation)
	assert.Equal(t, "opus", spawnedOpusAgain.Allocation.Model)

	clock.Advance(15 * time.Minute)
	inventoryAfterQuotaWindow, err := service.GetExecutionInventory()
	require.NoError(t, err)
	assert.Empty(t, inventoryAfterQuotaWindow.OfferingCooldowns)

	spawnedDefaultRestored, err := service.SpawnInstance(coordinatorCaller, SpawnInstanceInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "spawn-default-after-quota-expiry",
		Requirements: &protocol.InstanceExecutionRequirements{
			HarnessIDs: stringSlicePointer("claude-cli"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, spawnedDefaultRestored.Allocation)
	assert.Equal(t, "default", spawnedDefaultRestored.Allocation.Model)

	spawnedSonnetRestored, err := service.SpawnInstance(coordinatorCaller, SpawnInstanceInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "spawn-sonnet-after-quota-expiry",
		Requirements: &protocol.InstanceExecutionRequirements{
			HarnessIDs: stringSlicePointer("claude-cli"),
			Models:     stringSlicePointer("sonnet", "opus"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, spawnedSonnetRestored.Allocation)
	assert.Equal(t, "sonnet", spawnedSonnetRestored.Allocation.Model)
}

func TestReconfigureInstance_InPlaceSwitchValidationAndPreservation(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	httpServer := httptest.NewServer(service.Handler())
	defer httpServer.Close()

	instanceCapacity := 8
	activeInstances := 0
	canonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{t.TempDir()})
	require.NoError(t, err)
	workspaceRoot := canonicalRoots[0]
	slaveConnection, masterMessages := connectMockSlaveNode(t, httpServer, service, protocol.ComputeNode{
		ID:               "node-reconfig",
		Name:             "node-reconfig",
		Kind:             "local",
		Platform:         "darwin-arm64",
		Status:           "online",
		LastSeen:         service.nowUTC(),
		Concurrency:      4,
		InstanceCapacity: &instanceCapacity,
		ActiveInstances:  &activeInstances,
		WorkspaceRoots:   canonicalRoots,
		Version:          "1.0.0",
		Harnesses: []protocol.HarnessProfile{{
			ID:         "claude-cli",
			Label:      "Claude Code",
			Available:  true,
			AuthMode:   "local-subscription",
			Models:     []string{"sonnet", "opus", "haiku"},
			Transports: []string{protocol.TransportNativeCLI},
		}},
	})

	coordinatorCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "coord-reconfig",
		SessionID: "sess-reconfig",
	}
	threadMain, _, err := service.CreateThread(coordinatorCaller, CreateThreadInput{Title: "Reconfigure Thread"})
	require.NoError(t, err)
	threadOther, _, err := service.CreateThread(coordinatorCaller, CreateThreadInput{Title: "Other Thread"})
	require.NoError(t, err)

	spawned, err := service.SpawnInstance(coordinatorCaller, SpawnInstanceInput{
		ThreadID:       threadMain.ID,
		IdempotencyKey: "spawn-coder",
		Handle:         "coder",
		Requirements: &protocol.InstanceExecutionRequirements{
			HarnessIDs: stringSlicePointer("claude-cli"),
			Models:     stringSlicePointer("sonnet"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, spawned.Allocation)
	assert.Equal(t, "coder", spawned.Instance.Handle)
	assert.Equal(t, "sonnet", spawned.Allocation.Model)

	provisionMessage := awaitMasterMessageOfType(t, masterMessages, "instance.provision")
	require.NotNil(t, provisionMessage.Instance)
	assert.Equal(t, spawned.Instance.ID, provisionMessage.Instance.ID)

	sendSlaveControlMessage(t, slaveConnection, protocol.SlaveControlMessage{
		Type:         "instance.ready",
		InstanceID:   spawned.Instance.ID,
		AllocationID: spawned.Allocation.ID,
	})
	require.Eventually(t, func() bool {
		snapshot, getErr := service.GetInstance(coordinatorCaller, GetInstanceInput{InstanceID: spawned.Instance.ID})
		return getErr == nil && snapshot.Instance.Status == "ready"
	}, 3*time.Second, 20*time.Millisecond)

	nowText := service.nowUTC()
	bindingID := protocol.NewIdentifier("sbind")
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		state.SessionBindings[bindingID] = protocol.HarnessSessionBinding{
			ID:                bindingID,
			ThreadID:          threadMain.ID,
			InstanceID:        spawned.Instance.ID,
			AllocationID:      spawned.Allocation.ID,
			NodeID:            "node-reconfig",
			HarnessID:         "claude-cli",
			Transport:         protocol.TransportNativeCLI,
			ProviderSessionID: "sess-warm-42",
			Workspace:         workspaceRoot,
			Status:            "idle",
			CreatedByRunID:    "run_seed_1",
			LastRunID:         "run_seed_1",
			CreatedAt:         nowText,
			UpdatedAt:         nowText,
		}
		return nil
	})

	mailboxesBeforeResult, err := service.ListMailboxes(ListMailboxesInput{})
	require.NoError(t, err)
	mailboxesBefore := filterMailboxesByThread(mailboxesBeforeResult.Mailboxes, threadMain.ID)
	require.Len(t, mailboxesBefore, 1)
	originalMailbox := mailboxesBefore[0]
	assert.Equal(t, "coder", originalMailbox.Handle)

	rawReconfigureArguments, err := json.Marshal(ReconfigureInstanceInput{
		InstanceID:     spawned.Instance.ID,
		IdempotencyKey: "reconfig-to-opus",
		Model:          "opus",
	})
	require.NoError(t, err)

	rawReconfigureResponse, err := service.ExecuteOrchestratorTool(
		t.Context(),
		coordinatorCaller,
		"reconfigure_instance",
		rawReconfigureArguments,
	)
	require.NoError(t, err)

	var reconfigured protocol.InstanceLifecycleResult
	require.NoError(t, json.Unmarshal(rawReconfigureResponse, &reconfigured))
	assert.False(t, reconfigured.Replayed)
	require.NotNil(t, reconfigured.Allocation)
	assert.Equal(t, "opus", reconfigured.Allocation.Model)
	assert.Equal(t, "coder", reconfigured.Instance.Handle)
	require.NotNil(t, reconfigured.Instance.Requirements.Models)
	assert.Equal(t, []string{"opus"}, *reconfigured.Instance.Requirements.Models)

	reconfigureControl := awaitMasterMessageOfType(t, masterMessages, protocol.InstanceReconfigureControlType)
	assert.Equal(t, spawned.Instance.ID, reconfigureControl.InstanceID)
	assert.Equal(t, spawned.Allocation.ID, reconfigureControl.AllocationID)
	assert.Equal(t, "opus", reconfigureControl.Model)

	replayed, err := service.ReconfigureInstance(coordinatorCaller, ReconfigureInstanceInput{
		InstanceID:     spawned.Instance.ID,
		IdempotencyKey: "reconfig-to-opus",
		Model:          "opus",
	})
	require.NoError(t, err)
	assert.True(t, replayed.Replayed)
	require.NotNil(t, replayed.Allocation)
	assert.Equal(t, "opus", replayed.Allocation.Model)

	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		idleInstance := state.Instances[spawned.Instance.ID]
		idleInstance.Status = "idle"
		idleInstance.UpdatedAt = service.nowUTC()
		state.Instances[idleInstance.ID] = idleInstance
		return nil
	})

	idleReconfigured, err := service.ReconfigureInstance(coordinatorCaller, ReconfigureInstanceInput{
		InstanceID:     spawned.Instance.ID,
		IdempotencyKey: "reconfig-idle-to-haiku",
		Model:          "haiku",
	})
	require.NoError(t, err)
	assert.False(t, idleReconfigured.Replayed)
	require.NotNil(t, idleReconfigured.Allocation)
	assert.Equal(t, "haiku", idleReconfigured.Allocation.Model)
	assert.Equal(t, "idle", idleReconfigured.Instance.Status)

	idleReconfigureControl := awaitMasterMessageOfType(t, masterMessages, protocol.InstanceReconfigureControlType)
	assert.Equal(t, "haiku", idleReconfigureControl.Model)

	mailboxesAfterResult, err := service.ListMailboxes(ListMailboxesInput{})
	require.NoError(t, err)
	mailboxesAfter := filterMailboxesByThread(mailboxesAfterResult.Mailboxes, threadMain.ID)
	require.Len(t, mailboxesAfter, 1)
	assert.Equal(t, originalMailbox.ID, mailboxesAfter[0].ID)
	assert.Equal(t, originalMailbox.Address, mailboxesAfter[0].Address)

	require.NoError(t, openedStore.Read(func(state *store.State) error {
		binding, exists := state.SessionBindings[bindingID]
		require.True(t, exists)
		assert.Equal(t, "idle", binding.Status)
		assert.Equal(t, "sess-warm-42", binding.ProviderSessionID)
		assert.Equal(t, workspaceRoot, binding.Workspace)
		return nil
	}))

	busyInstanceID := protocol.NewIdentifier("inst")
	releasedInstanceID := protocol.NewIdentifier("inst")
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		state.Instances[busyInstanceID] = protocol.AgentInstance{
			ID:        busyInstanceID,
			ThreadID:  threadMain.ID,
			Status:    "busy",
			CreatedAt: nowText,
			UpdatedAt: nowText,
		}
		state.Instances[releasedInstanceID] = protocol.AgentInstance{
			ID:        releasedInstanceID,
			ThreadID:  threadMain.ID,
			Status:    "released",
			CreatedAt: nowText,
			UpdatedAt: nowText,
		}
		return nil
	})

	negativeCases := []struct {
		name         string
		caller       CallerIdentity
		input        ReconfigureInstanceInput
		expectedCode string
	}{
		{
			name:   "idempotency key reuse with different model rejected",
			caller: coordinatorCaller,
			input: ReconfigureInstanceInput{
				InstanceID:     spawned.Instance.ID,
				IdempotencyKey: "reconfig-to-opus",
				Model:          "haiku",
			},
			expectedCode: "idempotency_conflict",
		},
		{
			name:   "unsupported model on slave offering rejected",
			caller: coordinatorCaller,
			input: ReconfigureInstanceInput{
				InstanceID:     spawned.Instance.ID,
				IdempotencyKey: "reconfig-unsupported",
				Model:          "nonexistent-model-xl",
			},
			expectedCode: "invalid_request",
		},
		{
			name:   "busy instance rejected",
			caller: coordinatorCaller,
			input: ReconfigureInstanceInput{
				InstanceID:     busyInstanceID,
				IdempotencyKey: "reconfig-busy",
				Model:          "haiku",
			},
			expectedCode: "invalid_transition",
		},
		{
			name:   "released instance rejected",
			caller: coordinatorCaller,
			input: ReconfigureInstanceInput{
				InstanceID:     releasedInstanceID,
				IdempotencyKey: "reconfig-released",
				Model:          "haiku",
			},
			expectedCode: "invalid_transition",
		},
		{
			name: "non-delegating worker run forbidden",
			caller: CallerIdentity{
				Kind:        "run",
				RunID:       "run-non-delegating",
				ThreadID:    threadMain.ID,
				CanDelegate: false,
			},
			input: ReconfigureInstanceInput{
				InstanceID:     spawned.Instance.ID,
				IdempotencyKey: "reconfig-non-delegating",
				Model:          "haiku",
			},
			expectedCode: "forbidden",
		},
		{
			name: "cross-thread delegating worker run forbidden",
			caller: CallerIdentity{
				Kind:        "run",
				RunID:       "run-other-thread",
				ThreadID:    threadOther.ID,
				CanDelegate: true,
			},
			input: ReconfigureInstanceInput{
				InstanceID:     spawned.Instance.ID,
				IdempotencyKey: "reconfig-cross-thread",
				Model:          "haiku",
			},
			expectedCode: "forbidden",
		},
	}

	for _, testCase := range negativeCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, reconfigErr := service.ReconfigureInstance(testCase.caller, testCase.input)
			assertOperationErrorCodeValue(t, reconfigErr, testCase.expectedCode)
		})
	}
}

func TestSpawnInstance_HandleBriefShardedCountAndWarmFork(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	httpServer := httptest.NewServer(service.Handler())
	defer httpServer.Close()

	instanceCapacity := 16
	activeInstances := 0
	canonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{t.TempDir()})
	require.NoError(t, err)
	connectMockSlaveNode(t, httpServer, service, protocol.ComputeNode{
		ID:               "node-swarm",
		Name:             "node-swarm",
		Kind:             "local",
		Platform:         "darwin-arm64",
		Status:           "online",
		LastSeen:         service.nowUTC(),
		Concurrency:      8,
		InstanceCapacity: &instanceCapacity,
		ActiveInstances:  &activeInstances,
		WorkspaceRoots:   canonicalRoots,
		Version:          "1.0.0",
		Harnesses: []protocol.HarnessProfile{{
			ID:         "claude-cli",
			Label:      "Claude Code",
			Available:  true,
			AuthMode:   "local-subscription",
			Models:     []string{"sonnet", "opus"},
			Transports: []string{protocol.TransportNativeCLI},
		}},
	})

	coordinatorCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "coord-swarm",
		SessionID: "sess-swarm",
	}
	threadMain, _, err := service.CreateThread(coordinatorCaller, CreateThreadInput{Title: "Swarm Ergonomics Thread"})
	require.NoError(t, err)
	threadOther, _, err := service.CreateThread(coordinatorCaller, CreateThreadInput{Title: "Other Isolated Thread"})
	require.NoError(t, err)

	parserTitle := "Protocol Parser"
	parserSummary := "Owns internal/protocol/ types and wire codecs"
	parserInstructions := "Preserve strict JSON compatibility."
	parserSpawn, err := service.SpawnInstance(coordinatorCaller, SpawnInstanceInput{
		ThreadID:       threadMain.ID,
		IdempotencyKey: "spawn-parser-1",
		Handle:         "parser",
		PublicBrief:    true,
		Brief:          "Owns internal/protocol/; do not modify web/**",
		Purpose: &protocol.InstancePurpose{
			Title:        &parserTitle,
			Summary:      &parserSummary,
			Instructions: &parserInstructions,
		},
		Requirements: &protocol.InstanceExecutionRequirements{
			HarnessIDs: stringSlicePointer("claude-cli"),
			Models:     stringSlicePointer("sonnet"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "parser", parserSpawn.Instance.Handle)
	require.NotNil(t, parserSpawn.BriefRoomSequence)
	assert.Equal(t, int64(1), *parserSpawn.BriefRoomSequence)

	mailboxesAfterParserResult, err := service.ListMailboxes(ListMailboxesInput{})
	require.NoError(t, err)
	mailboxesAfterParser := filterMailboxesByThread(mailboxesAfterParserResult.Mailboxes, threadMain.ID)
	require.Len(t, mailboxesAfterParser, 1)
	assert.Equal(t, "parser", mailboxesAfterParser[0].Handle)
	assert.True(t, strings.HasPrefix(mailboxesAfterParser[0].Address, "parser@"))

	roomAfterParser, err := service.ReadRoom(coordinatorCaller, ReadRoomInput{
		ThreadID:      threadMain.ID,
		AfterSequence: 0,
	})
	require.NoError(t, err)
	require.Len(t, roomAfterParser.Messages, 1)
	assert.Equal(t, "contract", roomAfterParser.Messages[0].Kind)
	assert.Contains(t, roomAfterParser.Messages[0].Body, "@parser")
	assert.Contains(t, roomAfterParser.Messages[0].Body, "Owns internal/protocol/; do not modify web/**")

	rawShardArguments, err := json.Marshal(SpawnInstanceInput{
		ThreadID:       threadMain.ID,
		IdempotencyKey: "spawn-reviewers-sharded",
		Handle:         "reviewer",
		Count:          3,
		PublicBrief:    true,
		InitialTask: &protocol.InstanceInitialTask{
			Title:        "Audit package slice",
			Instructions: "Check race safety and error handling.",
		},
		Requirements: &protocol.InstanceExecutionRequirements{
			HarnessIDs: stringSlicePointer("claude-cli"),
			Models:     stringSlicePointer("sonnet"),
		},
	})
	require.NoError(t, err)

	rawShardResponse, err := service.ExecuteOrchestratorTool(
		t.Context(),
		coordinatorCaller,
		"spawn_instance",
		rawShardArguments,
	)
	require.NoError(t, err)

	var shardResult protocol.InstanceLifecycleResult
	require.NoError(t, json.Unmarshal(rawShardResponse, &shardResult))
	assert.False(t, shardResult.Replayed)
	assert.Equal(t, "reviewer-1", shardResult.Instance.Handle)
	require.Len(t, shardResult.Instances, 3)
	require.Len(t, shardResult.Allocations, 3)
	require.Len(t, shardResult.InitialTaskIDs, 3)
	require.NotNil(t, shardResult.BriefRoomSequence)
	assert.Equal(t, int64(2), *shardResult.BriefRoomSequence)

	assert.Equal(t, "reviewer-1", shardResult.Instances[0].Handle)
	assert.Equal(t, "reviewer-2", shardResult.Instances[1].Handle)
	assert.Equal(t, "reviewer-3", shardResult.Instances[2].Handle)

	replayedShardRaw, err := service.ExecuteOrchestratorTool(
		t.Context(),
		coordinatorCaller,
		"spawn_instance",
		rawShardArguments,
	)
	require.NoError(t, err)
	var replayedShard protocol.InstanceLifecycleResult
	require.NoError(t, json.Unmarshal(replayedShardRaw, &replayedShard))
	assert.True(t, replayedShard.Replayed)
	require.Len(t, replayedShard.Instances, 3)
	require.Len(t, replayedShard.Allocations, 3)
	require.Len(t, replayedShard.InitialTaskIDs, 3)
	require.NotNil(t, replayedShard.BriefRoomSequence)
	assert.Equal(t, int64(2), *replayedShard.BriefRoomSequence)

	mailboxesAfterShardsResult, err := service.ListMailboxes(ListMailboxesInput{})
	require.NoError(t, err)
	mailboxesAfterShards := filterMailboxesByThread(mailboxesAfterShardsResult.Mailboxes, threadMain.ID)
	var handles []string
	for _, mailbox := range mailboxesAfterShards {
		handles = append(handles, mailbox.Handle)
	}
	assert.ElementsMatch(t, []string{"parser", "reviewer-1", "reviewer-2", "reviewer-3"}, handles)

	nowText := service.nowUTC()
	priorRunID := "run_parser_completed_1"
	priorTaskID := protocol.NewIdentifier("task")
	mutateStore(t, openedStore, func(state *store.State, mutation *store.Mutation) error {
		state.Tasks[priorTaskID] = protocol.Task{
			ID:           priorTaskID,
			ThreadID:     threadMain.ID,
			Title:        "Analyze wire schema",
			Instructions: "Inspect domain.go and wire.go",
			Status:       "completed",
			CreatedAt:    nowText,
			UpdatedAt:    nowText,
		}
		state.Runs[priorRunID] = protocol.Run{
			ID:         priorRunID,
			ThreadID:   threadMain.ID,
			TaskID:     &priorTaskID,
			NodeID:     "node-swarm",
			InstanceID: parserSpawn.Instance.ID,
			HarnessID:  "claude-cli",
			Model:      "sonnet",
			Status:     "completed",
			Output:     "Added OfferingCooldown and Run.ErrorKind cleanly; bearer token master-secret-token must never leak.",
			CreatedAt:  nowText,
			FinishedAt: &nowText,
		}
		state.RunTranscripts[priorRunID] = protocol.RunTranscript{
			RunID:     priorRunID,
			UpdatedAt: nowText,
			Entries: []protocol.RunTranscriptEntry{
				{
					ID:        1,
					Kind:      "message",
					Text:      "Verified wire.go backward compatibility across SlaveControlMessage and MasterControlMessage.",
					At:        nowText,
					UpdatedAt: nowText,
				},
				{
					ID:        2,
					Kind:      "diff",
					Path:      "internal/protocol/wire.go",
					At:        nowText,
					UpdatedAt: nowText,
				},
			},
		}
		return nil
	})

	forkInstructions := "Continue with unit test coverage."
	forkedSpawn, err := service.SpawnInstance(coordinatorCaller, SpawnInstanceInput{
		ThreadID:           threadMain.ID,
		IdempotencyKey:     "spawn-parser-fork",
		Handle:             "parser-tests",
		ForkFromInstanceID: parserSpawn.Instance.ID,
		PublicBrief:        true,
		Purpose: &protocol.InstancePurpose{
			Instructions: &forkInstructions,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "parser-tests", forkedSpawn.Instance.Handle)
	assert.Equal(t, parserSpawn.Instance.ID, forkedSpawn.Instance.ForkFromInstanceID)
	require.NotNil(t, forkedSpawn.Instance.Requirements.HarnessIDs)
	assert.Equal(t, []string{"claude-cli"}, *forkedSpawn.Instance.Requirements.HarnessIDs)
	require.NotNil(t, forkedSpawn.Instance.Purpose)
	require.NotNil(t, forkedSpawn.Instance.Purpose.Instructions)
	seededInstructions := *forkedSpawn.Instance.Purpose.Instructions
	assert.Contains(t, seededInstructions, "Continue with unit test coverage.")
	assert.Contains(t, seededInstructions, "Warm Context Forked From @parser")
	assert.Contains(t, seededInstructions, "Verified wire.go backward compatibility")
	assert.Contains(t, seededInstructions, "internal/protocol/wire.go")
	assert.NotContains(t, seededInstructions, "master-secret-token")

	otherThreadSpawn, err := service.SpawnInstance(coordinatorCaller, SpawnInstanceInput{
		ThreadID:       threadOther.ID,
		IdempotencyKey: "spawn-other-thread-inst",
		Handle:         "outsider",
	})
	require.NoError(t, err)

	validationCases := []struct {
		name         string
		input        SpawnInstanceInput
		expectedCode string
	}{
		{
			name: "count above maximum rejected",
			input: SpawnInstanceInput{
				ThreadID:       threadMain.ID,
				IdempotencyKey: "bad-count-high",
				Count:          protocol.MaximumSpawnInstanceCount + 1,
			},
			expectedCode: "invalid_request",
		},
		{
			name: "negative count rejected",
			input: SpawnInstanceInput{
				ThreadID:       threadMain.ID,
				IdempotencyKey: "bad-count-negative",
				Count:          -2,
			},
			expectedCode: "invalid_request",
		},
		{
			name: "missing forkFromInstanceId returns not_found",
			input: SpawnInstanceInput{
				ThreadID:           threadMain.ID,
				IdempotencyKey:     "bad-fork-missing",
				ForkFromInstanceID: "inst_nonexistent_999",
			},
			expectedCode: "not_found",
		},
		{
			name: "cross-thread forkFromInstanceId rejected",
			input: SpawnInstanceInput{
				ThreadID:           threadMain.ID,
				IdempotencyKey:     "bad-fork-cross-thread",
				ForkFromInstanceID: otherThreadSpawn.Instance.ID,
			},
			expectedCode: "invalid_request",
		},
	}

	for _, testCase := range validationCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, validationErr := service.SpawnInstance(coordinatorCaller, testCase.input)
			assertOperationErrorCodeValue(t, validationErr, testCase.expectedCode)
		})
	}
}
