package orchestrator

import (
	"cmp"
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/store"
	"github.com/cafecito-games/Orchestra/internal/tailscaleauth"
)

type authContextKeyType struct{}

var authContextKey = authContextKeyType{}

type AuthContext struct {
	PrincipalKind string
	PrincipalID   string
	VerifiedNode  string
	GrantID       string
	Scope         *protocol.AccessGrantScope
	FullAuthority bool
}

func withAuthContext(parent context.Context, auth AuthContext) context.Context {
	return context.WithValue(parent, authContextKey, auth)
}

func authContextFromContext(ctx context.Context) (AuthContext, bool) {
	if ctx == nil {
		return AuthContext{}, false
	}
	auth, ok := ctx.Value(authContextKey).(AuthContext)
	return auth, ok
}

func callerWithAuthContext(base CallerIdentity, auth AuthContext) CallerIdentity {
	base.PrincipalKind = auth.PrincipalKind
	base.PrincipalID = auth.PrincipalID
	base.VerifiedNode = auth.VerifiedNode
	base.GrantID = auth.GrantID
	if auth.Scope != nil {
		scopeCopy := protocol.AccessGrantScope{
			ThreadIDs:    slices.Clone(auth.Scope.ThreadIDs),
			RoomChannels: slices.Clone(auth.Scope.RoomChannels),
			Operations:   slices.Clone(auth.Scope.Operations),
		}
		base.Scope = &scopeCopy
	}
	return base
}

func operatorCallerFromRequest(request *http.Request) CallerIdentity {
	base := CallerIdentity{Kind: "operator", OperatorID: "cli"}
	if auth, ok := authContextFromContext(request.Context()); ok {
		return callerWithAuthContext(base, auth)
	}
	return base
}

func principalFromTailscalePeer(peer tailscaleauth.PeerIdentity) AuthContext {
	nodeName := strings.TrimSpace(peer.NodeName)
	login := strings.TrimSpace(peer.LoginName)
	if login != "" && !strings.EqualFold(login, "tagged-devices") {
		return AuthContext{
			PrincipalKind: protocol.PrincipalKindTailscaleUser,
			PrincipalID:   login,
			VerifiedNode:  nodeName,
			FullAuthority: true,
		}
	}
	if len(peer.Tags) > 0 && strings.TrimSpace(peer.Tags[0]) != "" {
		return AuthContext{
			PrincipalKind: protocol.PrincipalKindTailscaleTag,
			PrincipalID:   strings.TrimSpace(peer.Tags[0]),
			VerifiedNode:  nodeName,
			FullAuthority: true,
		}
	}
	return AuthContext{
		PrincipalKind: protocol.PrincipalKindTailscaleNode,
		PrincipalID:   cmp.Or(nodeName, "tailscale-peer"),
		VerifiedNode:  nodeName,
		FullAuthority: true,
	}
}

func parseNarrowingScopeHeaders(header http.Header) (*protocol.AccessGrantScope, bool) {
	if header == nil {
		return nil, false
	}
	rawThreads := strings.TrimSpace(header.Get("X-Orchestra-Scope-Threads"))
	rawChannels := strings.TrimSpace(header.Get("X-Orchestra-Scope-Channels"))
	rawOps := strings.TrimSpace(header.Get("X-Orchestra-Scope-Ops"))
	if rawThreads == "" && rawChannels == "" && rawOps == "" {
		return nil, false
	}
	splitCSV := func(raw string) []string {
		if raw == "" {
			return nil
		}
		var out []string
		for part := range strings.SplitSeq(raw, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				out = append(out, trimmed)
			}
		}
		return out
	}
	ops := splitCSV(rawOps)
	if len(ops) == 0 {
		ops = slices.Clone(protocol.AccessGrantOperations)
	}
	return &protocol.AccessGrantScope{
		ThreadIDs:    splitCSV(rawThreads),
		RoomChannels: splitCSV(rawChannels),
		Operations:   ops,
	}, true
}

func (service *Service) applyHeaderScopeNarrowing(request *http.Request, auth AuthContext) (AuthContext, error) {
	requested, hasHeaders := parseNarrowingScopeHeaders(request.Header)
	if !hasHeaders {
		return auth, nil
	}
	narrowed, err := protocol.NarrowAccessGrantScope(auth.Scope, *requested)
	if err != nil {
		return AuthContext{}, forbiddenError("scope narrowing rejected: " + err.Error())
	}
	auth.Scope = &narrowed
	auth.FullAuthority = false
	return auth, nil
}

func (service *Service) lookupAccessGrantByToken(rawToken string) (protocol.AccessGrantRecord, bool) {
	if !strings.HasPrefix(rawToken, "orch_grant_") {
		return protocol.AccessGrantRecord{}, false
	}
	providedDigest := protocol.DigestAccessGrantToken(rawToken)
	now := service.now()
	var matched protocol.AccessGrantRecord
	var found bool
	_ = service.store.Read(func(state *store.State) error {
		for _, record := range state.AccessGrants {
			if subtle.ConstantTimeCompare([]byte(record.TokenDigest), []byte(providedDigest)) == 1 {
				if record.RevokedAt != "" {
					return nil
				}
				if record.ExpiresAt != "" {
					if expiresAt, valid := protocol.ParseTimestamp(record.ExpiresAt); valid && !now.Before(expiresAt) {
						return nil
					}
				}
				matched = record
				found = true
				return nil
			}
		}
		return nil
	})
	return matched, found
}

type CreateAccessGrantInput struct {
	Label      string                    `json:"label,omitempty"`
	Scope      protocol.AccessGrantScope `json:"scope"`
	TTLSeconds int                       `json:"ttlSeconds,omitempty"`
}

type CreateAccessGrantResult struct {
	Grant protocol.AccessGrantRecord `json:"grant"`
	Token string                     `json:"token"`
}

func (service *Service) CreateAccessGrant(caller CallerIdentity, input CreateAccessGrantInput) (CreateAccessGrantResult, error) {
	label := strings.TrimSpace(input.Label)
	if label != "" && !protocol.IsBoundedString(label, 128) {
		return CreateAccessGrantResult{}, invalidRequest("grant label must be at most 128 bytes")
	}
	if input.TTLSeconds < 0 {
		return CreateAccessGrantResult{}, invalidRequest("ttlSeconds must be non-negative")
	}
	narrowedScope, err := protocol.NarrowAccessGrantScope(caller.Scope, input.Scope)
	if err != nil {
		return CreateAccessGrantResult{}, forbiddenError(err.Error())
	}

	rawToken, digest, err := protocol.GenerateScopedGrantToken()
	if err != nil {
		return CreateAccessGrantResult{}, err
	}

	now := service.now()
	nowText := now.UTC().Format(time.RFC3339Nano)
	var expiresAtText string
	if input.TTLSeconds > 0 {
		expiresAtText = now.Add(time.Duration(input.TTLSeconds) * time.Second).UTC().Format(time.RFC3339Nano)
	}

	var created protocol.AccessGrantRecord
	_, err = service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		if caller.GrantID != "" {
			if parentGrant, exists := state.AccessGrants[caller.GrantID]; exists && parentGrant.ExpiresAt != "" {
				parentExpires, parentValid := protocol.ParseTimestamp(parentGrant.ExpiresAt)
				if parentValid {
					if expiresAtText == "" {
						expiresAtText = parentGrant.ExpiresAt
					} else if requestedExpires, reqValid := protocol.ParseTimestamp(expiresAtText); reqValid && requestedExpires.After(parentExpires) {
						expiresAtText = parentGrant.ExpiresAt
					}
				}
			}
		}

		principalKind := cmp.Or(strings.TrimSpace(caller.PrincipalKind), protocol.PrincipalKindToken)
		principalID := cmp.Or(strings.TrimSpace(caller.PrincipalID), caller.SourceKey(), "operator:cli")

		created = protocol.AccessGrantRecord{
			ID:            protocol.NewIdentifier("grant"),
			TokenDigest:   digest,
			Label:         service.redactor.Redact(label),
			ParentGrantID: caller.GrantID,
			PrincipalKind: principalKind,
			PrincipalID:   principalID,
			VerifiedNode:  caller.VerifiedNode,
			Scope:         narrowedScope,
			CreatedAt:     nowText,
			ExpiresAt:     expiresAtText,
		}
		state.AccessGrants[created.ID] = created
		return nil
	})
	if err != nil {
		return CreateAccessGrantResult{}, err
	}
	return CreateAccessGrantResult{
		Grant: created,
		Token: rawToken,
	}, nil
}

func (service *Service) ListAccessGrants(caller CallerIdentity) ([]protocol.AccessGrantRecord, error) {
	if caller.Scope != nil {
		return nil, forbiddenError("scoped access grants cannot list cluster access grants")
	}
	var grants []protocol.AccessGrantRecord
	err := service.store.Read(func(state *store.State) error {
		for _, grant := range state.AccessGrants {
			grants = append(grants, grant)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(grants, func(left, right protocol.AccessGrantRecord) int {
		if compared := cmp.Compare(right.CreatedAt, left.CreatedAt); compared != 0 {
			return compared
		}
		return cmp.Compare(left.ID, right.ID)
	})
	if grants == nil {
		grants = []protocol.AccessGrantRecord{}
	}
	return grants, nil
}

func (service *Service) RevokeAccessGrant(caller CallerIdentity, grantID string) (protocol.AccessGrantRecord, error) {
	if caller.Scope != nil {
		return protocol.AccessGrantRecord{}, forbiddenError("scoped access grants cannot revoke access grants")
	}
	trimmedID := strings.TrimSpace(grantID)
	if !protocol.IsIdentifier(trimmedID) {
		return protocol.AccessGrantRecord{}, invalidRequest("grantId is required")
	}
	var revoked protocol.AccessGrantRecord
	_, err := service.store.Mutate(func(state *store.State, mutation *store.Mutation) error {
		record, exists := state.AccessGrants[trimmedID]
		if !exists {
			return notFound(fmt.Sprintf("access grant %s does not exist", trimmedID))
		}
		if record.RevokedAt == "" {
			record.RevokedAt = service.nowUTC()
			state.AccessGrants[record.ID] = record
		}
		revoked = record
		return nil
	})
	return revoked, err
}

func requireScopeOperation(caller CallerIdentity, operation string) error {
	if caller.Scope == nil {
		return nil
	}
	if !protocol.ScopeAllowsOperation(caller.Scope, operation) {
		return forbiddenError(fmt.Sprintf("access grant does not permit operation %q", operation))
	}
	return nil
}

func requireScopeThread(caller CallerIdentity, threadID string) error {
	if caller.Scope == nil {
		return nil
	}
	if !protocol.ScopeAllowsThread(caller.Scope, threadID) {
		return forbiddenError("access grant does not permit access to thread " + threadID)
	}
	return nil
}

func requireScopeRoomChannel(caller CallerIdentity, channel string) error {
	if caller.Scope == nil {
		return nil
	}
	if !protocol.ScopeAllowsRoomChannel(caller.Scope, channel) {
		return forbiddenError(fmt.Sprintf("access grant does not permit access to room channel %q", protocol.EffectiveRoomChannel(channel)))
	}
	return nil
}

func requireClusterAdminAuthority(caller CallerIdentity, action string) error {
	if caller.Scope != nil {
		return forbiddenError(fmt.Sprintf("scoped access grant cannot perform cluster operation %q", action))
	}
	return nil
}
