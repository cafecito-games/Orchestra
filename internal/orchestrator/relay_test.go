package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

func TestRelayServiceExposesOnlyCommunicationSurfaceAndGlobalTraffic(t *testing.T) {
	openedStore, err := store.Open(filepath.Join(t.TempDir(), "relay"))
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Mode:  ServiceModeRelay,
		Store: openedStore,
		Token: "relay-secret",
	})
	require.NoError(t, err)
	defer service.Close()

	server := httptest.NewServer(service.Handler())
	defer server.Close()

	healthRequest, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/healthz", nil)
	require.NoError(t, err)
	healthResponse, err := http.DefaultClient.Do(healthRequest)
	require.NoError(t, err)
	defer healthResponse.Body.Close()
	require.Equal(t, http.StatusOK, healthResponse.StatusCode)
	var health map[string]any
	require.NoError(t, json.NewDecoder(healthResponse.Body).Decode(&health))
	assert.Equal(t, "orchestra-relay", health["service"])
	assert.Equal(t, string(ServiceModeRelay), health["mode"])

	for _, path := range []string{"/control-agent", "/api/status", "/api/nodes", "/api/approvals", "/api/workspace-leases"} {
		request, requestErr := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+path, nil)
		require.NoError(t, requestErr)
		request.Header.Set("Authorization", "Bearer relay-secret")
		response, responseErr := http.DefaultClient.Do(request)
		require.NoError(t, responseErr)
		assert.Equal(t, http.StatusNotFound, response.StatusCode, path)
		response.Body.Close()
	}

	for _, path := range []string{"/api/threads", "/api/mail/agents"} {
		request, requestErr := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+path, nil)
		require.NoError(t, requestErr)
		request.Header.Set("Authorization", "Bearer relay-secret")
		response, responseErr := http.DefaultClient.Do(request)
		require.NoError(t, responseErr)
		assert.Equal(t, http.StatusOK, response.StatusCode, path)
		response.Body.Close()
	}

	webRequest, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/", nil)
	require.NoError(t, err)
	webResponse, err := http.DefaultClient.Do(webRequest)
	require.NoError(t, err)
	defer webResponse.Body.Close()
	assert.Equal(t, http.StatusOK, webResponse.StatusCode)

	alpha, err := service.RegisterClientMailbox("client-alpha", "session-alpha", &protocol.ClientMailRegistration{
		RequestedName: "alpha",
		Host:          "laptop",
		Harness:       "codex",
	})
	require.NoError(t, err)
	beta, err := service.RegisterClientMailbox("client-beta", "session-beta", &protocol.ClientMailRegistration{
		RequestedName: "beta",
		Host:          "server",
		Harness:       "claude",
	})
	require.NoError(t, err)

	sent, err := service.SendMail(CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "client-alpha",
		SessionID: "session-alpha",
	}, SendMailInput{
		IdempotencyKey: "relay-message-1",
		To:             beta.Address,
		Subject:        "Contract",
		Body:           "Use API version two.",
	})
	require.NoError(t, err)
	assert.Equal(t, alpha.Address, sent.Message.FromAddress)

	traffic, err := service.ListMailTraffic(ListMailTrafficInput{Limit: 200})
	require.NoError(t, err)
	require.Len(t, traffic.Messages, 1)
	assert.Equal(t, sent.Message.ID, traffic.Messages[0].ID)
	assert.False(t, traffic.Messages[0].Read, "the operator traffic feed must not mark recipient mail as read")

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/api/mail/traffic?limit=200", nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer relay-secret")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusOK, response.StatusCode)
	var apiTraffic ListMailTrafficResult
	require.NoError(t, json.NewDecoder(response.Body).Decode(&apiTraffic))
	require.Len(t, apiTraffic.Messages, 1)
	assert.Equal(t, sent.Message.ID, apiTraffic.Messages[0].ID)

	alphaCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "client-alpha",
		SessionID: "session-alpha",
	}
	betaCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "client-beta",
		SessionID: "session-beta",
	}

	// Remote execution / slave management tools remain strictly forbidden in relay mode.
	for _, forbiddenTool := range []string{"get_execution_inventory", "spawn_instance", "clone_repository", "update_nodes", "land_workspace_lease", "resolve_approval"} {
		_, err = service.ExecuteOrchestratorTool(t.Context(), alphaCaller, forbiddenTool, json.RawMessage(`{}`))
		require.Error(t, err, forbiddenTool)
		var operationError *OperationError
		require.ErrorAs(t, err, &operationError)
		assert.Equal(t, "forbidden", operationError.Code, forbiddenTool)
	}

	// Threads, Blackboard Rooms, and pull-mode claimable tasks work end-to-end in relay mode.
	createdRaw, err := service.ExecuteOrchestratorTool(t.Context(), alphaCaller, "create_thread", json.RawMessage(`{"title":"Relay shared room"}`))
	require.NoError(t, err)
	var createdPayload struct {
		Thread protocol.Thread `json:"thread"`
	}
	require.NoError(t, json.Unmarshal(createdRaw, &createdPayload))
	require.NotEmpty(t, createdPayload.Thread.ID)

	_, err = service.ExecuteOrchestratorTool(t.Context(), betaCaller, "attach_thread", json.RawMessage(`{"threadId":"`+createdPayload.Thread.ID+`"}`))
	require.NoError(t, err)

	_, err = service.ExecuteOrchestratorTool(t.Context(), alphaCaller, "post_room_message", json.RawMessage(`{
		"threadId": "`+createdPayload.Thread.ID+`",
		"idempotencyKey": "relay-room-1",
		"kind": "contract",
		"body": "API v2 schema locked"
	}`))
	require.NoError(t, err)

	roomRaw, err := service.ExecuteOrchestratorTool(t.Context(), betaCaller, "read_room", json.RawMessage(`{"threadId":"`+createdPayload.Thread.ID+`"}`))
	require.NoError(t, err)
	var roomResult ReadRoomResult
	require.NoError(t, json.Unmarshal(roomRaw, &roomResult))
	require.Len(t, roomResult.Messages, 1)
	assert.Equal(t, "API v2 schema locked", roomResult.Messages[0].Body)

	// Relay mode rejects dispatched tasks and workspace mutations.
	_, err = service.ExecuteOrchestratorTool(t.Context(), alphaCaller, "submit_tasks", json.RawMessage(`{
		"threadId": "`+createdPayload.Thread.ID+`",
		"idempotencyKey": "relay-dispatched",
		"tasks": [{"title":"Bad","instructions":"No dispatch","executionMode":"dispatched"}]
	}`))
	require.Error(t, err)

	// Relay mode defaults submitted tasks to claimable and allows atomic claim/renew/complete.
	submittedRaw, err := service.ExecuteOrchestratorTool(t.Context(), alphaCaller, "submit_tasks", json.RawMessage(`{
		"threadId": "`+createdPayload.Thread.ID+`",
		"idempotencyKey": "relay-claimable-1",
		"tasks": [{"title":"Pull task","instructions":"Claim and complete locally"}]
	}`))
	require.NoError(t, err)
	var submittedResult SubmitTasksResult
	require.NoError(t, json.Unmarshal(submittedRaw, &submittedResult))
	require.Len(t, submittedResult.Tasks, 1)
	assert.Equal(t, protocol.TaskExecutionModeClaimable, submittedResult.Tasks[0].ExecutionMode)

	claimedRaw, err := service.ExecuteOrchestratorTool(t.Context(), betaCaller, "claim_task", json.RawMessage(`{"threadId":"`+createdPayload.Thread.ID+`","ttlSeconds":60}`))
	require.NoError(t, err)
	var claimResult ClaimTaskResult
	require.NoError(t, json.Unmarshal(claimedRaw, &claimResult))
	require.True(t, claimResult.Claimed)
	assert.Equal(t, submittedResult.Tasks[0].ID, claimResult.Task.ID)
	assert.Equal(t, int64(1), claimResult.Claim.Epoch)

	updatePayload, err := json.Marshal(map[string]any{
		"taskId":     claimResult.Task.ID,
		"claimEpoch": claimResult.Claim.Epoch,
		"claimToken": claimResult.Claim.Token,
		"status":     "completed",
		"summary":    "finished in relay",
	})
	require.NoError(t, err)
	_, err = service.ExecuteOrchestratorTool(t.Context(), betaCaller, "update_task", updatePayload)
	require.NoError(t, err)

	_, err = service.ExecuteOrchestratorTool(t.Context(), betaCaller, "detach_thread", json.RawMessage(`{"threadId":"`+createdPayload.Thread.ID+`"}`))
	require.NoError(t, err)
}

func TestRelayToolDefinitionsAndConfiguration(t *testing.T) {
	definitions := RelayToolDefinitions()
	names := make([]string, 0, len(definitions))
	for _, definition := range definitions {
		names = append(names, definition.Name)
	}
	assert.Equal(t, protocol.RelayToolNames, names)

	dataDirectory := filepath.Join(t.TempDir(), "state")
	configuration, err := ParseRelayConfiguration([]string{
		"--listen", "127.0.0.1:0",
		"--token", "relay-secret",
		"--data-dir", dataDirectory,
		"--connection-file", "none",
	}, func(string) (string, bool) { return "", false })
	require.NoError(t, err)
	assert.Equal(t, ServiceModeRelay, configuration.Mode)
	assert.Equal(t, dataDirectory, configuration.DataDirectory)
	assert.False(t, configuration.WithLocalWorker)

	_, err = ParseRelayConfiguration([]string{"--with-local-worker"}, func(string) (string, bool) { return "", false })
	require.ErrorContains(t, err, "unavailable in relay mode")
}
