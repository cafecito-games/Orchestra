package orchestrator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

func TestHTTPThreadRoomChannelAndWaitEndpoints(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	bearerToken := "http-room-wait-secret-token"
	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: bearerToken,
	})
	require.NoError(t, err)
	defer service.Close()

	server := httptest.NewServer(service.Handler())
	defer server.Close()

	operatorCaller := CallerIdentity{Kind: "operator", OperatorID: "test"}
	thread, _, err := service.CreateThread(operatorCaller, CreateThreadInput{Title: "HTTP room wait thread"})
	require.NoError(t, err)

	_, err = service.PostRoomMessage(operatorCaller, PostRoomMessageInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "http-room-1",
		Channel:        "main",
		Kind:           "guidance",
		Body:           "Main channel guidance",
	})
	require.NoError(t, err)

	_, err = service.PostRoomMessage(operatorCaller, PostRoomMessageInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "http-room-2",
		Channel:        "verification",
		Kind:           "finding",
		Mentions:       []string{"verifier"},
		Body:           "Verification finding leaking " + bearerToken,
	})
	require.NoError(t, err)

	doGet := func(path string, withAuth bool) *http.Response {
		t.Helper()
		request, reqErr := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+path, nil)
		require.NoError(t, reqErr)
		if withAuth {
			request.Header.Set("Authorization", "Bearer "+bearerToken)
		}
		response, doErr := http.DefaultClient.Do(request)
		require.NoError(t, doErr)
		return response
	}

	// 1. Bearer auth is enforced on /room/wait.
	unauthorizedResp := doGet("/api/threads/"+thread.ID+"/room/wait?timeoutMs=0", false)
	defer unauthorizedResp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, unauthorizedResp.StatusCode)

	// 2. GET /api/threads/<id>/room?channel=verification returns only verification entries.
	channelResp := doGet("/api/threads/"+thread.ID+"/room?channel=verification", true)
	defer channelResp.Body.Close()
	require.Equal(t, http.StatusOK, channelResp.StatusCode)
	var readResult ReadRoomResult
	require.NoError(t, json.NewDecoder(channelResp.Body).Decode(&readResult))
	require.Len(t, readResult.Messages, 1)
	assert.Equal(t, "verification", readResult.Messages[0].Channel)
	assert.Equal(t, int64(2), readResult.ThroughSequence)
	assert.NotContains(t, readResult.Messages[0].Body, bearerToken)

	// 3. Invalid query params return 400.
	invalidCases := []struct {
		name string
		path string
	}{
		{name: "bad channel on read", path: "/api/threads/" + thread.ID + "/room?channel=bad.channel"},
		{name: "bad after on wait", path: "/api/threads/" + thread.ID + "/room/wait?after=abc"},
		{name: "bad timeoutMs on wait", path: "/api/threads/" + thread.ID + "/room/wait?timeoutMs=nope"},
		{name: "bad mention on wait", path: "/api/threads/" + thread.ID + "/room/wait?mentions=@#main"},
		{name: "bad kind on wait", path: "/api/threads/" + thread.ID + "/room/wait?kind=bogus"},
	}
	for _, testCase := range invalidCases {
		t.Run(testCase.name, func(t *testing.T) {
			resp := doGet(testCase.path, true)
			defer resp.Body.Close()
			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}

	// 4. GET /room/wait with after=0 and mentions=verifier-1 matches @verifier prefix and redacts secrets.
	prefixResp := doGet("/api/threads/"+thread.ID+"/room/wait?after=0&channel=verification&mentions=verifier-1&timeoutMs=0", true)
	defer prefixResp.Body.Close()
	require.Equal(t, http.StatusOK, prefixResp.StatusCode)
	var waitResult WaitRoomResult
	require.NoError(t, json.NewDecoder(prefixResp.Body).Decode(&waitResult))
	assert.False(t, waitResult.TimedOut)
	require.Len(t, waitResult.Messages, 1)
	assert.Equal(t, int64(2), waitResult.Messages[0].Sequence)
	assert.Empty(t, waitResult.Events)
	assert.NotContains(t, waitResult.Messages[0].Body, bearerToken)

	// 5. GET /room/wait with timeoutMs=0 at current head returns timedOut: true immediately.
	timeoutResp := doGet("/api/threads/"+thread.ID+"/room/wait?after=2&timeoutMs=0", true)
	defer timeoutResp.Body.Close()
	require.Equal(t, http.StatusOK, timeoutResp.StatusCode)
	var timedOut WaitRoomResult
	require.NoError(t, json.NewDecoder(timeoutResp.Body).Decode(&timedOut))
	assert.True(t, timedOut.TimedOut)
	assert.Equal(t, int64(2), timedOut.HeadSequence)
	assert.Equal(t, int64(2), timedOut.ThroughSequence)
	assert.NotEmpty(t, timedOut.EventCursor)

	// 6. Blocking GET /room/wait wakes on new room post and does NOT duplicate room.message_posted into Events.
	done := make(chan WaitRoomResult, 1)
	go func() {
		resp := doGet("/api/threads/"+thread.ID+"/room/wait?after=2&cursor="+timedOut.EventCursor+"&timeoutMs=5000", true)
		defer resp.Body.Close()
		var decoded WaitRoomResult
		if decodeErr := json.NewDecoder(resp.Body).Decode(&decoded); decodeErr == nil {
			done <- decoded
		}
	}()

	time.Sleep(60 * time.Millisecond)
	_, err = service.PostRoomMessage(operatorCaller, PostRoomMessageInput{
		ThreadID:       thread.ID,
		IdempotencyKey: "http-room-3",
		Channel:        "main",
		Kind:           "answer",
		Body:           "Unblocked answer",
	})
	require.NoError(t, err)

	select {
	case woke := <-done:
		assert.False(t, woke.TimedOut)
		require.Len(t, woke.Messages, 1)
		assert.Equal(t, int64(3), woke.Messages[0].Sequence)
		assert.Equal(t, protocol.DefaultRoomChannel, woke.Messages[0].Channel)
		assert.Empty(t, woke.Events, "room.message_posted must not duplicate into Events")
	case <-time.After(5 * time.Second):
		require.Fail(t, "timed out waiting for HTTP /room/wait to wake")
	}
}
