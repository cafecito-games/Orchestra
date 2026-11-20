package protocol

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
)

const (
	AccessOpRead     = "read"
	AccessOpPost     = "post"
	AccessOpClaim    = "claim"
	AccessOpDelegate = "delegate"

	PrincipalKindTailscaleUser = "tailscale-user"
	PrincipalKindTailscaleTag  = "tailscale-tag"
	PrincipalKindTailscaleNode = "tailscale-node"
	PrincipalKindToken         = "token"
	PrincipalKindLocal         = "local"
)

var AccessGrantOperations = []string{
	AccessOpRead,
	AccessOpPost,
	AccessOpClaim,
	AccessOpDelegate,
}

type AccessGrantScope struct {
	ThreadIDs    []string `json:"threadIds,omitempty"`
	RoomChannels []string `json:"roomChannels,omitempty"`
	Operations   []string `json:"operations,omitempty"`
}

type AccessGrantRecord struct {
	ID            string           `json:"id"`
	TokenDigest   string           `json:"tokenDigest"`
	Label         string           `json:"label,omitempty"`
	ParentGrantID string           `json:"parentGrantId,omitempty"`
	PrincipalKind string           `json:"principalKind"`
	PrincipalID   string           `json:"principalId"`
	VerifiedNode  string           `json:"verifiedNode,omitempty"`
	Scope         AccessGrantScope `json:"scope"`
	CreatedAt     string           `json:"createdAt"`
	ExpiresAt     string           `json:"expiresAt,omitempty"`
	RevokedAt     string           `json:"revokedAt,omitempty"`
}

func GenerateScopedGrantToken() (string, string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	token := "orch_grant_" + hex.EncodeToString(raw)
	return token, DigestAccessGrantToken(token), nil
}

func DigestAccessGrantToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func NormalizeAccessGrantScope(scope AccessGrantScope) (AccessGrantScope, error) {
	var normalized AccessGrantScope

	for _, rawThreadID := range scope.ThreadIDs {
		trimmed := strings.TrimSpace(rawThreadID)
		if trimmed == "" {
			continue
		}
		if !IsIdentifier(trimmed) {
			return AccessGrantScope{}, fmt.Errorf("invalid threadId %q in access grant scope", rawThreadID)
		}
		if !slices.Contains(normalized.ThreadIDs, trimmed) {
			normalized.ThreadIDs = append(normalized.ThreadIDs, trimmed)
		}
	}
	slices.Sort(normalized.ThreadIDs)

	for _, rawPattern := range scope.RoomChannels {
		pattern := strings.ToLower(strings.TrimSpace(rawPattern))
		if pattern == "" {
			continue
		}
		if len(pattern) > RoomChannelMaximumBytes {
			return AccessGrantScope{}, fmt.Errorf("roomChannel pattern %q exceeds %d bytes", rawPattern, RoomChannelMaximumBytes)
		}
		if _, err := path.Match(pattern, DefaultRoomChannel); err != nil {
			return AccessGrantScope{}, fmt.Errorf("invalid roomChannel pattern %q: %w", rawPattern, err)
		}
		if !slices.Contains(normalized.RoomChannels, pattern) {
			normalized.RoomChannels = append(normalized.RoomChannels, pattern)
		}
	}
	slices.Sort(normalized.RoomChannels)

	for _, rawOp := range scope.Operations {
		op := strings.ToLower(strings.TrimSpace(rawOp))
		if op == "" {
			continue
		}
		if !slices.Contains(AccessGrantOperations, op) {
			return AccessGrantScope{}, fmt.Errorf("unknown access grant operation %q (allowed: %s)", rawOp, strings.Join(AccessGrantOperations, ", "))
		}
		if !slices.Contains(normalized.Operations, op) {
			normalized.Operations = append(normalized.Operations, op)
		}
	}
	slices.Sort(normalized.Operations)

	if len(normalized.Operations) == 0 {
		return AccessGrantScope{}, errors.New("access grant scope must specify at least one operation (read, post, claim, delegate)")
	}
	return normalized, nil
}

func ScopeAllowsOperation(scope *AccessGrantScope, operation string) bool {
	if scope == nil {
		return true
	}
	return slices.Contains(scope.Operations, operation)
}

func ScopeAllowsThread(scope *AccessGrantScope, threadID string) bool {
	if scope == nil || len(scope.ThreadIDs) == 0 {
		return true
	}
	return slices.Contains(scope.ThreadIDs, threadID)
}

func ScopeAllowsRoomChannel(scope *AccessGrantScope, channel string) bool {
	if scope == nil || len(scope.RoomChannels) == 0 {
		return true
	}
	normalized := EffectiveRoomChannel(channel)
	for _, pattern := range scope.RoomChannels {
		if pattern == AllRoomChannels {
			return true
		}
		if matched, err := path.Match(pattern, normalized); err == nil && matched {
			return true
		}
	}
	return false
}

func channelPatternCoveredBy(parentPatterns []string, childPattern string) bool {
	if len(parentPatterns) == 0 {
		return true
	}
	for _, parent := range parentPatterns {
		if parent == AllRoomChannels || parent == childPattern {
			return true
		}
		if !strings.ContainsAny(childPattern, "*?[") {
			if matched, err := path.Match(parent, childPattern); err == nil && matched {
				return true
			}
		}
	}
	return false
}

// NarrowAccessGrantScope validates that requested is strictly equal to or narrower than parent.
func NarrowAccessGrantScope(parent *AccessGrantScope, requested AccessGrantScope) (AccessGrantScope, error) {
	normalized, err := NormalizeAccessGrantScope(requested)
	if err != nil {
		return AccessGrantScope{}, err
	}
	if parent == nil {
		return normalized, nil
	}

	for _, op := range normalized.Operations {
		if !slices.Contains(parent.Operations, op) {
			return AccessGrantScope{}, fmt.Errorf("cannot grant operation %q outside parent scope", op)
		}
	}
	if len(parent.ThreadIDs) > 0 {
		if len(normalized.ThreadIDs) == 0 {
			normalized.ThreadIDs = slices.Clone(parent.ThreadIDs)
		} else {
			for _, threadID := range normalized.ThreadIDs {
				if !slices.Contains(parent.ThreadIDs, threadID) {
					return AccessGrantScope{}, fmt.Errorf("cannot grant threadId %q outside parent scope", threadID)
				}
			}
		}
	}
	if len(parent.RoomChannels) > 0 {
		if len(normalized.RoomChannels) == 0 {
			normalized.RoomChannels = slices.Clone(parent.RoomChannels)
		} else {
			for _, pattern := range normalized.RoomChannels {
				if !channelPatternCoveredBy(parent.RoomChannels, pattern) {
					return AccessGrantScope{}, fmt.Errorf("cannot grant roomChannel %q outside parent scope", pattern)
				}
			}
		}
	}
	return normalized, nil
}

func FormatAttributedAuthor(label string, principalID string) string {
	trimmedLabel := strings.TrimSpace(label)
	trimmedPrincipal := strings.TrimSpace(principalID)
	if trimmedPrincipal == "" || trimmedPrincipal == "local" || trimmedPrincipal == "master-token" {
		return trimmedLabel
	}
	if trimmedLabel == "" {
		return trimmedPrincipal
	}
	return fmt.Sprintf("%s (via %s)", trimmedLabel, trimmedPrincipal)
}
