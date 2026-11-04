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

	for _, path := range []string{"/control-agent", "/api/status", "/api/threads", "/api/nodes"} {
		request, requestErr := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+path, nil)
		require.NoError(t, requestErr)
		request.Header.Set("Authorization", "Bearer relay-secret")
		response, responseErr := http.DefaultClient.Do(request)
		require.NoError(t, responseErr)
		assert.Equal(t, http.StatusNotFound, response.StatusCode, path)
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

	_, err = service.ExecuteOrchestratorTool(t.Context(), CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "client-alpha",
		SessionID: "session-alpha",
	}, "create_thread", json.RawMessage(`{"title":"not allowed"}`))
	require.Error(t, err)
	var operationError *OperationError
	require.ErrorAs(t, err, &operationError)
	assert.Equal(t, "forbidden", operationError.Code)
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
