package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
	"github.com/cafecito-games/Orchestra/internal/tailscaleauth"
)

func newTailscalePrincipalTestService(t *testing.T) (*Service, *httptest.Server) {
	t.Helper()
	openedStore, err := store.Open(filepath.Join(t.TempDir(), "state"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = openedStore.Close() })

	verifier, err := tailscaleauth.NewVerifier(t.Context(), tailscaleauth.Options{
		CommandRunner: func(_ context.Context, _ string, args ...string) ([]byte, error) {
			if len(args) > 0 && args[0] == "status" {
				return []byte(`{
					"Self": {
						"HostName": "orchestra-hub",
						"DNSName": "orchestra-hub.tail.ts.net.",
						"TailscaleIPs": ["100.64.0.1"],
						"UserID": 100
					},
					"MagicDNSSuffix": "tail.ts.net",
					"User": {
						"100": {"ID": 100, "LoginName": "alice@example.com"}
					}
				}`), nil
			}
			return []byte(`{
				"Node": {"Name": "macbook.tail.ts.net.", "Addresses": ["100.64.0.10/32"]},
				"UserProfile": {"ID": 100, "LoginName": "alice@example.com"}
			}`), nil
		},
	})
	require.NoError(t, err)

	service, err := NewService(ServiceOptions{
		Store:             openedStore,
		Token:             "master-admin-secret",
		TailscaleVerifier: verifier,
	})
	require.NoError(t, err)
	t.Cleanup(service.Close)
	server := httptest.NewServer(service.Handler())
	t.Cleanup(server.Close)
	return service, server
}

func TestVerifiedPrincipalAttributionAndSpoofingRejection(t *testing.T) {
	service, _ := newTailscalePrincipalTestService(t)

	// Simulate an HTTP request arriving over Tailscale from alice@example.com with spoofed headers.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/threads", bytes.NewBufferString(`{"title":"Attributed thread"}`))
	req.RemoteAddr = "100.64.0.10:54321"
	req.Header.Set("X-Orchestra-Principal-Id", "bob@evil.example")
	req.Header.Set("X-Orchestra-Principal-Kind", "tailscale-user")
	rec := httptest.NewRecorder()
	service.Handler().ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var createdThreadResp struct {
		Thread protocol.Thread `json:"thread"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createdThreadResp))
	threadID := createdThreadResp.Thread.ID

	// Post a room message as alice@example.com and verify RoomAuthor records verified principal, ignoring spoofed headers.
	postBody := `{"idempotencyKey":"room-attr-1","kind":"finding","body":"Verified finding"}`
	postReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/threads/"+threadID+"/room/messages", bytes.NewBufferString(postBody))
	postReq.RemoteAddr = "100.64.0.10:54321"
	postReq.Header.Set("X-Orchestra-Principal-Id", "mallory@evil.example")
	postRec := httptest.NewRecorder()
	service.Handler().ServeHTTP(postRec, postReq)
	require.Equal(t, http.StatusCreated, postRec.Code, postRec.Body.String())

	var posted PostRoomMessageResult
	require.NoError(t, json.Unmarshal(postRec.Body.Bytes(), &posted))
	assert.Equal(t, protocol.PrincipalKindTailscaleUser, posted.Message.Author.PrincipalKind)
	assert.Equal(t, "alice@example.com", posted.Message.Author.PrincipalID)
	assert.Equal(t, "macbook.tail.ts.net", posted.Message.Author.VerifiedNode)

	// Submit a task via HTTP and verify TaskOrigin carries verified principal.
	submitBody := `{"idempotencyKey":"task-attr-1","tasks":[{"title":"Check","instructions":"Verify","executionMode":"claimable"}]}`
	submitReq := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/threads/"+threadID+"/tasks", bytes.NewBufferString(submitBody))
	submitReq.RemoteAddr = "100.64.0.10:54321"
	submitRec := httptest.NewRecorder()
	service.Handler().ServeHTTP(submitRec, submitReq)
	require.Equal(t, http.StatusCreated, submitRec.Code, submitRec.Body.String())

	var submitted SubmitTasksResult
	require.NoError(t, json.Unmarshal(submitRec.Body.Bytes(), &submitted))
	require.Len(t, submitted.Tasks, 1)
	assert.Equal(t, protocol.PrincipalKindTailscaleUser, submitted.Tasks[0].Origin.PrincipalKind)
	assert.Equal(t, "alice@example.com", submitted.Tasks[0].Origin.PrincipalID)
	assert.Equal(t, "macbook.tail.ts.net", submitted.Tasks[0].Origin.VerifiedNode)

	// Direct service call with verified principal also propagates to InstanceCreator and MailMessage.
	aliceCaller := CallerIdentity{
		Kind:          "orchestrator-client",
		ClientID:      "client-alice",
		SessionID:     "sess-alice",
		PrincipalKind: protocol.PrincipalKindTailscaleUser,
		PrincipalID:   "alice@example.com",
		VerifiedNode:  "macbook.tail.ts.net",
	}
	creator := aliceCaller.InstanceCreator()
	assert.Equal(t, protocol.PrincipalKindTailscaleUser, creator.PrincipalKind)
	assert.Equal(t, "alice@example.com", creator.PrincipalID)
	assert.Equal(t, "macbook.tail.ts.net", creator.VerifiedNode)

	_, err := service.RegisterClientMailbox("client-alice", "sess-alice", &protocol.ClientMailRegistration{RequestedName: "reviewer", Host: "macbook"})
	require.NoError(t, err)
	peerBox, err := service.RegisterClientMailbox("client-bob", "sess-bob", &protocol.ClientMailRegistration{RequestedName: "builder", Host: "linux"})
	require.NoError(t, err)

	sentMail, err := service.SendMail(aliceCaller, SendMailInput{
		IdempotencyKey: "mail-attr-1",
		To:             peerBox.Address,
		Body:           "Hello",
	})
	require.NoError(t, err)
	assert.Equal(t, protocol.PrincipalKindTailscaleUser, sentMail.Message.PrincipalKind)
	assert.Equal(t, "alice@example.com", sentMail.Message.PrincipalID)
	assert.Equal(t, "macbook.tail.ts.net", sentMail.Message.VerifiedNode)
}

func TestScopedAccessGrantsAuthorizationMatrixAndMonotonicNarrowing(t *testing.T) {
	openedStore, err := store.Open(filepath.Join(t.TempDir(), "state"))
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret",
	})
	require.NoError(t, err)
	defer service.Close()

	adminCaller := CallerIdentity{
		Kind:          "operator",
		OperatorID:    "admin",
		PrincipalKind: protocol.PrincipalKindToken,
		PrincipalID:   "alice@example.com",
	}
	threadAllowed, _, err := service.CreateThread(adminCaller, CreateThreadInput{Title: "Allowed Thread"})
	require.NoError(t, err)
	threadOther, _, err := service.CreateThread(adminCaller, CreateThreadInput{Title: "Secret Thread"})
	require.NoError(t, err)

	// Seed messages on main and review-1 channels in threadAllowed.
	_, err = service.PostRoomMessage(adminCaller, PostRoomMessageInput{
		ThreadID:       threadAllowed.ID,
		IdempotencyKey: "seed-main",
		Channel:        "main",
		Kind:           "finding",
		Body:           "Main channel message",
	})
	require.NoError(t, err)
	_, err = service.PostRoomMessage(adminCaller, PostRoomMessageInput{
		ThreadID:       threadAllowed.ID,
		IdempotencyKey: "seed-review",
		Channel:        "review-1",
		Kind:           "finding",
		Body:           "Review channel message",
	})
	require.NoError(t, err)

	// Issue a scoped grant restricted to threadAllowed, channel "review-*", operations ["read", "post"].
	grantRes, err := service.CreateAccessGrant(adminCaller, CreateAccessGrantInput{
		Label: "review-bot",
		Scope: protocol.AccessGrantScope{
			ThreadIDs:    []string{threadAllowed.ID},
			RoomChannels: []string{"review-*"},
			Operations:   []string{protocol.AccessOpRead, protocol.AccessOpPost},
		},
		TTLSeconds: 3600,
	})
	require.NoError(t, err)
	require.NotEmpty(t, grantRes.Token)
	assert.NotEqual(t, grantRes.Token, grantRes.Grant.TokenDigest)

	doReq := func(method, path, body, token string) *httptest.ResponseRecorder {
		var bodyReader *bytes.Buffer
		if body != "" {
			bodyReader = bytes.NewBufferString(body)
		} else {
			bodyReader = &bytes.Buffer{}
		}
		req := httptest.NewRequestWithContext(t.Context(), method, path, bodyReader)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		service.Handler().ServeHTTP(rec, req)
		return rec
	}

	// 1. Allowed: reading threadAllowed room on review-1 returns only review-1 message (filters out main).
	rec := doReq(http.MethodGet, "/api/threads/"+threadAllowed.ID+"/room", "", grantRes.Token)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var roomRes ReadRoomResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &roomRes))
	require.Len(t, roomRes.Messages, 1)
	assert.Equal(t, "review-1", roomRes.Messages[0].Channel)

	// 2. Allowed: posting to review-2 on threadAllowed succeeds and attributes principal.
	rec = doReq(http.MethodPost, "/api/threads/"+threadAllowed.ID+"/room/messages", `{"idempotencyKey":"p1","channel":"review-2","kind":"finding","body":"OK"}`, grantRes.Token)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	// 3. Forbidden: posting to "main" channel (outside review-*).
	rec = doReq(http.MethodPost, "/api/threads/"+threadAllowed.ID+"/room/messages", `{"idempotencyKey":"p2","channel":"main","kind":"finding","body":"Blocked"}`, grantRes.Token)
	assert.Equal(t, http.StatusForbidden, rec.Code)

	// 4. Forbidden: cross-thread read/post on threadOther.
	rec = doReq(http.MethodGet, "/api/threads/"+threadOther.ID+"/room", "", grantRes.Token)
	assert.Equal(t, http.StatusForbidden, rec.Code)
	rec = doReq(http.MethodGet, "/api/threads/"+threadOther.ID, "", grantRes.Token)
	assert.Equal(t, http.StatusForbidden, rec.Code)

	// 5. ListThreads only returns threadAllowed (filters out threadOther).
	rec = doReq(http.MethodGet, "/api/threads", "", grantRes.Token)
	require.Equal(t, http.StatusOK, rec.Code)
	var listRes struct {
		Threads []protocol.Thread `json:"threads"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listRes))
	require.Len(t, listRes.Threads, 1)
	assert.Equal(t, threadAllowed.ID, listRes.Threads[0].ID)

	// 6. Forbidden: cluster admin & unauthorized delegation/claim operations.
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodGet, "/api/nodes", "", grantRes.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodPost, "/api/nodes/update", `{}`, grantRes.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodGet, "/api/status", "", grantRes.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodGet, "/api/approvals", "", grantRes.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodPost, "/api/approvals/appr_1/resolve", `{"status":"approved"}`, grantRes.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodGet, "/api/workspace-leases", "", grantRes.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodPost, "/api/runs/run_1/cancel", `{}`, grantRes.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodPost, "/api/cancel/task_1", `{}`, grantRes.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodPost, "/api/tasks/task_1/cancel", `{}`, grantRes.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodPost, "/api/mail/rename", `{"address":"a@b","name":"c"}`, grantRes.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodGet, "/api/mail/traffic", "", grantRes.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodPost, "/api/threads/"+threadAllowed.ID+"/tasks", `{"idempotencyKey":"t1","tasks":[{"title":"X","instructions":"Y"}]}`, grantRes.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodPost, "/api/threads/"+threadAllowed.ID+"/tasks/claim", `{}`, grantRes.Token).Code)

	// Cross-thread & operation-restricted MCP tool calls are rejected with forbidden.
	scopedCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "scoped-bot",
		SessionID: "scoped-sess",
		Scope: &protocol.AccessGrantScope{
			ThreadIDs:    []string{threadAllowed.ID},
			RoomChannels: []string{"review-*"},
			Operations:   []string{protocol.AccessOpRead},
		},
	}
	var opErr *OperationError
	_, err = service.ExecuteOrchestratorTool(t.Context(), scopedCaller, "send_task_message", json.RawMessage(`{"threadId":"`+threadOther.ID+`","toTaskId":"task_1","idempotencyKey":"m1","kind":"guidance","body":"hi"}`))
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "forbidden", opErr.Code)

	_, err = service.ExecuteOrchestratorTool(t.Context(), scopedCaller, "post_artifact", json.RawMessage(`{"threadId":"`+threadOther.ID+`","name":"x.txt","sizeBytes":1,"sha256":"`+protocol.DigestAccessGrantToken("a")+`"}`))
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "forbidden", opErr.Code)

	_, err = service.ExecuteOrchestratorTool(t.Context(), scopedCaller, "wait_for_activity", json.RawMessage(`{"threadId":"`+threadOther.ID+`","timeoutMilliseconds":50}`))
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "forbidden", opErr.Code)

	_, err = service.ExecuteOrchestratorTool(t.Context(), scopedCaller, "send_mail", json.RawMessage(`{"idempotencyKey":"sm1","to":"a@b","body":"hi"}`))
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "forbidden", opErr.Code)

	_, err = service.ExecuteOrchestratorTool(t.Context(), scopedCaller, "set_mail_identity", json.RawMessage(`{"name":"evil"}`))
	require.ErrorAs(t, err, &opErr)
	assert.Equal(t, "forbidden", opErr.Code)

	// 7. Monotonic narrowing: scoped grant CANNOT mint a broader grant (e.g. adding "delegate" or threadOther).
	rec = doReq(http.MethodPost, "/api/tokens", `{"scope":{"threadIds":["`+threadAllowed.ID+`"],"roomChannels":["review-*"],"operations":["read","delegate"]}}`, grantRes.Token)
	assert.Equal(t, http.StatusForbidden, rec.Code)
	rec = doReq(http.MethodPost, "/api/tokens", `{"scope":{"threadIds":["`+threadOther.ID+`"],"roomChannels":["review-*"],"operations":["read"]}}`, grantRes.Token)
	assert.Equal(t, http.StatusForbidden, rec.Code)

	// 8. Monotonic narrowing: scoped grant CAN mint a narrower sub-grant (read-only on review-1).
	rec = doReq(http.MethodPost, "/api/tokens", `{"scope":{"threadIds":["`+threadAllowed.ID+`"],"roomChannels":["review-1"],"operations":["read"]}}`, grantRes.Token)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var subGrant CreateAccessGrantResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &subGrant))
	assert.Equal(t, http.StatusOK, doReq(http.MethodGet, "/api/threads/"+threadAllowed.ID+"/room", "", subGrant.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodPost, "/api/threads/"+threadAllowed.ID+"/room/messages", `{"idempotencyKey":"p3","channel":"review-1","kind":"finding","body":"No post"}`, subGrant.Token).Code)
	assert.Equal(t, http.StatusForbidden, doReq(http.MethodPost, "/api/mail/messages", `{"idempotencyKey":"m2","to":"a@b","body":"No mail post"}`, subGrant.Token).Code)

	// 9. Revoking a parent grant immediately rejects both the parent AND its derived child sub-grant with 401.
	_, err = service.RevokeAccessGrant(adminCaller, grantRes.Grant.ID)
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, doReq(http.MethodGet, "/api/threads/"+threadAllowed.ID+"/room", "", grantRes.Token).Code)
	assert.Equal(t, http.StatusUnauthorized, doReq(http.MethodGet, "/api/threads/"+threadAllowed.ID+"/room", "", subGrant.Token).Code)
}
