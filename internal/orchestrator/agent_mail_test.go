package orchestrator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
)

func TestAgentMailRegistrationNamingRenamingAliasesAndReclaim(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	claudeMailbox, err := service.RegisterClientMailbox("claude-desktop", "sess-claude-1", &protocol.ClientMailRegistration{
		Host:        "fabrica",
		Harness:     "claude-code",
		Workspace:   "/workspaces/orchestra",
		PushCapable: true,
		ProcessID:   1234,
	})
	require.NoError(t, err)
	require.Equal(t, "claude1234@fabrica", claudeMailbox.Address)
	require.Equal(t, "online", claudeMailbox.Status)
	require.Equal(t, "claude", claudeMailbox.Harness)
	require.True(t, claudeMailbox.PushCapable)

	secondClaudeMailbox, err := service.RegisterClientMailbox("claude-desktop", "sess-claude-2", &protocol.ClientMailRegistration{
		Host:        "fabrica",
		Harness:     "claude-code",
		Workspace:   "/workspaces/orchestra",
		PushCapable: true,
		ProcessID:   1234,
	})
	require.NoError(t, err)
	require.Equal(t, "claude1234-2@fabrica", secondClaudeMailbox.Address)
	require.NotEqual(t, claudeMailbox.ID, secondClaudeMailbox.ID)

	codexMailbox, err := service.RegisterClientMailbox("codex-client", "sess-codex-1", &protocol.ClientMailRegistration{
		Host:        "bunker",
		Harness:     "codex-mcp-client",
		Workspace:   "/workspaces/backend",
		PushCapable: true,
		ProcessID:   345,
	})
	require.NoError(t, err)
	require.Equal(t, "codex345@bunker", codexMailbox.Address)

	claudeCaller := CallerIdentity{Kind: "orchestrator-client", ClientID: "claude-desktop", SessionID: "sess-claude-1"}
	codexCaller := CallerIdentity{Kind: "orchestrator-client", ClientID: "codex-client", SessionID: "sess-codex-1"}
	operatorCaller := CallerIdentity{Kind: "operator", OperatorID: "cli"}

	renamedClaude, err := service.SetMailIdentity(claudeCaller, SetMailIdentityInput{
		Name: "auth-api",
	})
	require.NoError(t, err)
	require.Equal(t, "auth-api@fabrica", renamedClaude.Address)
	require.True(t, renamedClaude.CustomHandle)
	require.Contains(t, renamedClaude.Aliases, "claude1234@fabrica")

	_, err = service.SetMailIdentity(claudeCaller, SetMailIdentityInput{
		Name: "auth-api@otherhost",
	})
	assertOperationErrorCode(t, err, "invalid_request")

	_, err = service.SetMailIdentity(codexCaller, SetMailIdentityInput{
		Name: "codex345",
	})
	require.NoError(t, err)

	_, err = service.SetMailIdentity(claudeCaller, SetMailIdentityInput{
		Name: "claude1234-2",
	})
	assertOperationErrorCode(t, err, "address_in_use")

	operatorRenamedCodex, err := service.SetMailIdentity(operatorCaller, SetMailIdentityInput{
		Address: "codex345@bunker",
		Name:    "reviewer@bunker",
	})
	require.NoError(t, err)
	require.Equal(t, "reviewer@bunker", operatorRenamedCodex.Address)
	require.Contains(t, operatorRenamedCodex.Aliases, "codex345@bunker")

	sentViaAlias, err := service.SendMail(codexCaller, SendMailInput{
		IdempotencyKey: "alias-send-1",
		To:             "claude1234@fabrica",
		Subject:        "Checking old alias routing",
		Body:           "Does mail to claude1234@fabrica still arrive at auth-api@fabrica?",
	})
	require.NoError(t, err)
	require.Equal(t, "reviewer@bunker", sentViaAlias.Message.FromAddress)
	require.Equal(t, "auth-api@fabrica", sentViaAlias.Message.ToAddress)
	require.Equal(t, renamedClaude.ID, sentViaAlias.Recipient.ID)

	service.DisconnectClientMailbox("sess-claude-1")
	listedOnline, err := service.ListMailboxes(ListMailboxesInput{OnlineOnly: true})
	require.NoError(t, err)
	for _, candidate := range listedOnline.Mailboxes {
		require.NotEqual(t, renamedClaude.ID, candidate.ID)
	}

	reclaimedClaude, err := service.RegisterClientMailbox("claude-restarted", "sess-claude-restarted", &protocol.ClientMailRegistration{
		RequestedName: "auth-api",
		Host:          "fabrica",
		Harness:       "claude-code",
		PushCapable:   true,
	})
	require.NoError(t, err)
	require.Equal(t, renamedClaude.ID, reclaimedClaude.ID)
	require.Equal(t, "auth-api@fabrica", reclaimedClaude.Address)
	require.Equal(t, "online", reclaimedClaude.Status)
	require.Equal(t, 1, reclaimedClaude.UnreadCount)
}

func TestAgentMailSendReadWaitIdempotencyRedactionAndThreadlessArtifacts(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	_, err = service.RegisterClientMailbox("claude-client", "sess-claude", &protocol.ClientMailRegistration{
		RequestedName: "claude1234",
		Host:          "fabrica",
		Harness:       "claude-code",
	})
	require.NoError(t, err)

	nowText := service.nowUTC()
	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		state.Nodes["bunker"] = protocol.ComputeNode{
			ID: "bunker", Name: "bunker", Kind: "remote", Platform: "linux-amd64", Status: "online", LastSeen: nowText, Concurrency: 2,
		}
		state.Instances["inst-leaf"] = protocol.AgentInstance{
			ID: "inst-leaf", Status: "busy", CreatedAt: nowText, UpdatedAt: nowText,
		}
		state.Allocations["alloc-leaf"] = protocol.InstanceAllocation{
			ID: "alloc-leaf", InstanceID: "inst-leaf", NodeID: "bunker", HarnessID: "codex-cli", Workspace: "/srv/repo", Status: "active", CreatedAt: nowText, UpdatedAt: nowText,
		}
		state.Runs["run-leaf"] = protocol.Run{
			ID: "run-leaf", InstanceID: "inst-leaf", AllocationID: "alloc-leaf", NodeID: "bunker", Status: "running", CreatedAt: nowText,
		}
		return nil
	})
	require.NoError(t, err)

	leafCaller := CallerIdentity{
		Kind:        "run",
		RunID:       "run-leaf",
		InstanceID:  "inst-leaf",
		CanDelegate: false,
	}
	claudeCaller := CallerIdentity{
		Kind:      "orchestrator-client",
		ClientID:  "claude-client",
		SessionID: "sess-claude",
	}

	leafMailbox, err := service.SetMailIdentity(leafCaller, SetMailIdentityInput{Name: "codex345"})
	require.NoError(t, err)
	require.Equal(t, "codex345@bunker", leafMailbox.Address)

	_, err = service.SetMailIdentity(leafCaller, SetMailIdentityInput{
		Address: "claude1234@fabrica",
		Name:    "hijacked",
	})
	assertOperationErrorCode(t, err, "forbidden")

	artifactPayload := []byte("diff --git a/api.go b/api.go\n+func NewEndpoint() {}\n")
	sum := sha256.Sum256(artifactPayload)
	digest := hex.EncodeToString(sum[:])

	standaloneArtifactResult, err := service.PostArtifact(leafCaller, PostArtifactInput{
		Name:      "api.patch",
		Kind:      "patch",
		MediaType: "text/x-diff",
		SizeBytes: int64(len(artifactPayload)),
		SHA256:    digest,
		Summary:   "Proposed API endpoint patch",
	})
	require.NoError(t, err)
	standaloneArtifact := standaloneArtifactResult.Artifact
	require.Empty(t, standaloneArtifact.ThreadID)

	sendResult, err := service.SendMail(leafCaller, SendMailInput{
		IdempotencyKey: "mail-patch-1",
		To:             "claude1234@fabrica",
		Subject:        "Patch ready (token master-secret-token)",
		Body:           "Please review attached patch; key sk-ant-api03-1234567890abcdef must never appear.",
		ArtifactIDs:    []string{standaloneArtifact.ID},
	})
	require.NoError(t, err)
	require.False(t, sendResult.Replayed)
	require.Equal(t, "codex345@bunker", sendResult.Message.FromAddress)
	require.Equal(t, "claude1234@fabrica", sendResult.Message.ToAddress)
	require.NotContains(t, sendResult.Message.Subject, "master-secret-token")
	require.NotContains(t, sendResult.Message.Body, "sk-ant-api03-1234567890abcdef")
	require.Contains(t, sendResult.Message.Body, protocol.RedactionMarker)
	require.Equal(t, []string{standaloneArtifact.ID}, sendResult.Message.ArtifactIDs)

	replayedResult, err := service.SendMail(leafCaller, SendMailInput{
		IdempotencyKey: "mail-patch-1",
		To:             "claude1234@fabrica",
		Subject:        "Patch ready (token master-secret-token)",
		Body:           "Please review attached patch; key sk-ant-api03-1234567890abcdef must never appear.",
		ArtifactIDs:    []string{standaloneArtifact.ID},
	})
	require.NoError(t, err)
	require.True(t, replayedResult.Replayed)
	require.Equal(t, sendResult.Message.ID, replayedResult.Message.ID)

	_, err = service.SendMail(leafCaller, SendMailInput{
		IdempotencyKey: "mail-patch-1",
		To:             "claude1234@fabrica",
		Subject:        "Different subject",
		Body:           "Different body",
	})
	assertOperationErrorCode(t, err, "idempotency_conflict")

	_, err = service.ReadMail(leafCaller, ReadMailInput{Address: "claude1234@fabrica"})
	assertOperationErrorCode(t, err, "forbidden")

	markReadFalse := false
	peekInbox, err := service.ReadMail(claudeCaller, ReadMailInput{
		UnreadOnly: true,
		MarkRead:   &markReadFalse,
	})
	require.NoError(t, err)
	require.Len(t, peekInbox.Messages, 1)
	require.False(t, peekInbox.Messages[0].Read)
	require.Equal(t, 1, peekInbox.Mailbox.UnreadCount)

	readInbox, err := service.ReadMail(claudeCaller, ReadMailInput{
		UnreadOnly: true,
	})
	require.NoError(t, err)
	require.Len(t, readInbox.Messages, 1)
	require.True(t, readInbox.Messages[0].Read)
	require.Equal(t, 0, readInbox.Mailbox.UnreadCount)

	sentBox, err := service.ReadMail(leafCaller, ReadMailInput{Box: "sent"})
	require.NoError(t, err)
	require.Len(t, sentBox.Messages, 1)
	require.Equal(t, sendResult.Message.ID, sentBox.Messages[0].ID)

	timedOutWait, err := service.WaitForMail(context.Background(), leafCaller, WaitForMailInput{
		TimeoutMilliseconds: 40,
	})
	require.NoError(t, err)
	require.True(t, timedOutWait.TimedOut)
	require.Empty(t, timedOutWait.Messages)

	waitDone := make(chan WaitForMailResult, 1)
	go func() {
		waited, waitErr := service.WaitForMail(context.Background(), leafCaller, WaitForMailInput{
			TimeoutMilliseconds: 3000,
		})
		if waitErr == nil {
			waitDone <- waited
		}
	}()

	time.Sleep(30 * time.Millisecond)
	wakeFalse := false
	replySent, err := service.SendMail(claudeCaller, SendMailInput{
		IdempotencyKey: "reply-patch-1",
		ReplyToID:      sendResult.Message.ID,
		Body:           "LGTM, go ahead and merge.",
		Wake:           &wakeFalse,
	})
	require.NoError(t, err)
	require.Equal(t, "codex345@bunker", replySent.Message.ToAddress)
	require.True(t, strings.HasPrefix(replySent.Message.Subject, "Re: "))

	select {
	case waited := <-waitDone:
		require.False(t, waited.TimedOut)
		require.Len(t, waited.Messages, 1)
		require.Equal(t, replySent.Message.ID, waited.Messages[0].ID)
		require.Equal(t, "LGTM, go ahead and merge.", waited.Messages[0].Body)
	case <-time.After(3 * time.Second):
		require.FailNow(t, "timed out waiting for WaitForMail to wake on incoming reply")
	}

	httpServer := httptest.NewServer(service.Handler())
	defer httpServer.Close()

	unauthorizedRequest, err := http.NewRequestWithContext(t.Context(), http.MethodGet, httpServer.URL+"/api/mail/agents", nil)
	require.NoError(t, err)
	unauthorizedResponse, err := http.DefaultClient.Do(unauthorizedRequest)
	require.NoError(t, err)
	_ = unauthorizedResponse.Body.Close()
	require.Equal(t, http.StatusUnauthorized, unauthorizedResponse.StatusCode)

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, httpServer.URL+"/api/mail/agents", nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer master-secret-token")
	authorizedResponse, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer authorizedResponse.Body.Close()
	require.Equal(t, http.StatusOK, authorizedResponse.StatusCode)

	var listed ListMailboxesResult
	require.NoError(t, json.NewDecoder(authorizedResponse.Body).Decode(&listed))
	require.Len(t, listed.Mailboxes, 2)

	postBody, err := json.Marshal(SendMailInput{
		IdempotencyKey: "http-op-mail-1",
		To:             "claude1234@fabrica",
		Subject:        "Operator ping",
		Body:           "Status check from HTTP API",
	})
	require.NoError(t, err)
	postRequest, err := http.NewRequestWithContext(t.Context(), http.MethodPost, httpServer.URL+"/api/mail/messages", bytes.NewReader(postBody))
	require.NoError(t, err)
	postRequest.Header.Set("Authorization", "Bearer master-secret-token")
	postRequest.Header.Set("Content-Type", "application/json")
	postResponse, err := http.DefaultClient.Do(postRequest)
	require.NoError(t, err)
	defer postResponse.Body.Close()
	require.Equal(t, http.StatusCreated, postResponse.StatusCode)
}

func TestAgentMailInstanceWakeSchedulingSessionResumeAndBusyFlush(t *testing.T) {
	openedStore, err := store.Open(t.TempDir())
	require.NoError(t, err)
	defer openedStore.Close()

	service, err := NewService(ServiceOptions{
		Store: openedStore,
		Token: "master-secret-token",
	})
	require.NoError(t, err)
	defer service.Close()

	workspacePath := t.TempDir()
	nowText := service.nowUTC()
	instanceCapacity := 4
	activeInstances := 1
	purposeName := "backend"

	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		state.Nodes["node-bunker"] = protocol.ComputeNode{
			ID:               "node-bunker",
			Name:             "bunker",
			Kind:             "remote",
			Platform:         "linux-amd64",
			Status:           "online",
			LastSeen:         nowText,
			Concurrency:      4,
			InstanceCapacity: &instanceCapacity,
			ActiveInstances:  &activeInstances,
			WorkspaceRoots:   []string{workspacePath},
			Version:          "1.0.0",
			Harnesses: []protocol.HarnessProfile{{
				ID:         "codex-cli",
				Label:      "Codex",
				Binary:     "/bin/echo",
				Available:  true,
				AuthMode:   "local-subscription",
				Models:     []string{"gpt-5"},
				Transports: []string{"acp-v1"},
			}},
		}
		state.Threads["thread-backend"] = protocol.Thread{
			ID: "thread-backend", Title: "Backend Worker Thread", Status: "open", CreatedAt: nowText, UpdatedAt: nowText,
		}
		harnessIDs := []string{"codex-cli"}
		transports := []string{"acp-v1"}
		instance := protocol.AgentInstance{
			ID:       "inst-backend",
			ThreadID: "thread-backend",
			Status:   "idle",
			Purpose:  &protocol.InstancePurpose{Name: &purposeName},
			Requirements: protocol.InstanceExecutionRequirements{
				HarnessIDs: &harnessIDs,
				Transports: &transports,
				Workspace:  &protocol.InstanceWorkspaceRequirements{Path: &workspacePath, Writable: true},
			},
			Lease:     protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
			CreatedAt: nowText,
			UpdatedAt: nowText,
		}
		allocation := protocol.InstanceAllocation{
			ID:         "alloc-backend",
			InstanceID: "inst-backend",
			NodeID:     "node-bunker",
			HarnessID:  "codex-cli",
			Model:      "gpt-5",
			Transport:  "acp-v1",
			Workspace:  workspacePath,
			Status:     "active",
			Lease:      protocol.InstanceLease{IdleTimeoutSeconds: 600, ExpiresAt: extendLeaseTimestamp(nowText, 600)},
			CreatedAt:  nowText,
			UpdatedAt:  nowText,
		}
		state.Instances[instance.ID] = instance
		state.Allocations[allocation.ID] = allocation
		state.SessionBindings["bind-backend"] = protocol.HarnessSessionBinding{
			ID:                "bind-backend",
			ThreadID:          "thread-backend",
			InstanceID:        "inst-backend",
			AllocationID:      "alloc-backend",
			NodeID:            "node-bunker",
			HarnessID:         "codex-cli",
			Transport:         "acp-v1",
			Workspace:         workspacePath,
			ProviderSessionID: "acp-session-backend-99",
			Status:            "idle",
			CreatedByRunID:    "run-initial",
			LastRunID:         "run-initial",
			CreatedAt:         nowText,
			UpdatedAt:         nowText,
		}
		service.ensureInstanceMailboxInState(state, instance, allocation, nowText)
		return nil
	})
	require.NoError(t, err)

	_, err = service.RegisterClientMailbox("claude-client", "sess-claude", &protocol.ClientMailRegistration{
		RequestedName: "claude1234",
		Host:          "fabrica",
		Harness:       "claude-code",
	})
	require.NoError(t, err)
	claudeCaller := CallerIdentity{Kind: "orchestrator-client", ClientID: "claude-client", SessionID: "sess-claude"}

	wakeSend, err := service.SendMail(claudeCaller, SendMailInput{
		IdempotencyKey: "wake-idle-1",
		To:             "backend@bunker",
		Subject:        "Add healthz metrics",
		Body:           "Expose queue depth in /healthz and reply when done.",
	})
	require.NoError(t, err)
	require.True(t, wakeSend.Message.WakeDispatched)
	require.NotEmpty(t, wakeSend.Message.WakeTaskID)
	require.True(t, wakeSend.Message.Read)

	var dispatchedWake protocol.MasterControlMessage
	_, err = openedStore.Mutate(func(state *store.State, mutation *store.Mutation) error {
		wakeTask := state.Tasks[wakeSend.Message.WakeTaskID]
		require.Equal(t, "ready", wakeTask.Status)
		require.Equal(t, "inst-backend", wakeTask.PlacementInstanceID)
		require.NotNil(t, wakeTask.Requirements.Workspace)
		require.Empty(t, wakeTask.Requirements.Workspace.Isolation)

		effectiveRequirements := mergeProjectProfileRequirements(state, wakeTask.Requirements)
		instance := state.Instances["inst-backend"]
		allocation := state.Allocations["alloc-backend"]
		dispatchedWake = service.assignTaskToResident(state, &wakeTask, &instance, &allocation, effectiveRequirements, service.nowUTC())
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, dispatchedWake.SessionBinding, "wake task must resume existing ACP session binding")
	require.Equal(t, "acp-session-backend-99", dispatchedWake.SessionBinding.ProviderSessionID)
	require.Contains(t, dispatchedWake.Run.Prompt, "claude1234@fabrica")
	require.Contains(t, dispatchedWake.Run.Prompt, "Expose queue depth in /healthz")

	busyMailOne, err := service.SendMail(claudeCaller, SendMailInput{
		IdempotencyKey: "busy-mail-1",
		To:             "backend@bunker",
		Subject:        "Follow-up 1",
		Body:           "Also include active worker count.",
	})
	require.NoError(t, err)
	require.False(t, busyMailOne.Message.WakeDispatched, "busy instance must not be interrupted mid-run")
	require.False(t, busyMailOne.Message.Read)

	busyMailTwo, err := service.SendMail(claudeCaller, SendMailInput{
		IdempotencyKey: "busy-mail-2",
		To:             "backend@bunker",
		Subject:        "Follow-up 2",
		Body:           "Keep JSON field names camelCase.",
	})
	require.NoError(t, err)
	require.False(t, busyMailTwo.Message.WakeDispatched)

	wakeOutput := "completed first wake turn"
	service.handleRunCompleted("node-bunker", protocol.SlaveControlMessage{
		Type:   "run.completed",
		RunID:  dispatchedWake.RunID,
		Output: &wakeOutput,
	})

	require.NoError(t, openedStore.Read(func(state *store.State) error {
		updatedFirst := state.MailMessages[busyMailOne.Message.ID]
		updatedSecond := state.MailMessages[busyMailTwo.Message.ID]
		require.True(t, updatedFirst.WakeDispatched, "completing busy run without reading mail must flush pending wake task")
		require.True(t, updatedSecond.WakeDispatched)
		require.Equal(t, updatedFirst.WakeTaskID, updatedSecond.WakeTaskID, "multiple queued messages should batch into one wake task")

		flushedTask, exists := state.Tasks[updatedFirst.WakeTaskID]
		require.True(t, exists)
		require.Equal(t, "ready", flushedTask.Status)
		require.Equal(t, "inst-backend", flushedTask.PlacementInstanceID)
		require.Contains(t, flushedTask.Instructions, "Also include active worker count.")
		require.Contains(t, flushedTask.Instructions, "Keep JSON field names camelCase.")
		return nil
	}))
}
