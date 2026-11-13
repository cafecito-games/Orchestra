package protocol

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	RunErrorKindQuotaExhausted     = "quota_exhausted"
	RunErrorKindRateLimited        = "rate_limited"
	MaximumSpawnInstanceCount      = 16
	InstanceReconfigureControlType = "instance.reconfigure"
)

var RunErrorKinds = []string{
	RunErrorKindQuotaExhausted,
	RunErrorKindRateLimited,
}

func IsQuotaOrRateLimitErrorKind(kind string) bool {
	return slices.Contains(RunErrorKinds, strings.TrimSpace(kind))
}

type OfferingCooldown struct {
	NodeID     string `json:"nodeId"`
	HarnessID  string `json:"harnessId"`
	Model      string `json:"model"`
	ErrorKind  string `json:"errorKind"`
	Detail     string `json:"detail,omitempty"`
	RunID      string `json:"runId,omitempty"`
	OccurredAt string `json:"occurredAt"`
	ExpiresAt  string `json:"expiresAt"`
}

func (cooldown OfferingCooldown) IsActiveAt(referenceTime time.Time) bool {
	expiresAt, valid := ParseTimestamp(cooldown.ExpiresAt)
	if !valid {
		return false
	}
	return referenceTime.Before(expiresAt)
}

func DeriveShardedInstanceHandles(requestedHandle string, fallbackName string, count int) ([]string, error) {
	if count < 1 || count > MaximumSpawnInstanceCount {
		return nil, fmt.Errorf("instance count must be between 1 and %d", MaximumSpawnInstanceCount)
	}
	trimmedHandle := strings.TrimSpace(requestedHandle)
	if count == 1 {
		if trimmedHandle == "" {
			return []string{""}, nil
		}
		normalized, valid := NormalizeMailHandle(trimmedHandle)
		if !valid {
			return nil, fmt.Errorf("instance handle %q must be 1..48 lowercase ASCII letters, digits, dots, underscores, or hyphens", requestedHandle)
		}
		return []string{normalized}, nil
	}

	baseHandle := ""
	if trimmedHandle != "" {
		normalized, valid := NormalizeMailHandle(trimmedHandle)
		if !valid {
			return nil, fmt.Errorf("instance handle %q must be 1..48 lowercase ASCII letters, digits, dots, underscores, or hyphens", requestedHandle)
		}
		baseHandle = normalized
	} else {
		baseHandle = sanitizeRosterBaseHandle(fallbackName)
	}
	if baseHandle == "" {
		baseHandle = "worker"
	}

	maximumSuffixLength := len("-" + strconv.Itoa(count))
	maximumBaseLength := 48 - maximumSuffixLength
	if len(baseHandle) > maximumBaseLength {
		baseHandle = strings.TrimRight(baseHandle[:maximumBaseLength], "-._")
		if baseHandle == "" {
			baseHandle = "worker"
		}
	}

	handles := make([]string, 0, count)
	for shardIndex := 1; shardIndex <= count; shardIndex++ {
		candidate := fmt.Sprintf("%s-%d", baseHandle, shardIndex)
		normalized, valid := NormalizeMailHandle(candidate)
		if !valid {
			return nil, fmt.Errorf("derived shard handle %q is invalid", candidate)
		}
		handles = append(handles, normalized)
	}
	return handles, nil
}

func sanitizeRosterBaseHandle(raw string) string {
	lowered := strings.ToLower(strings.TrimSpace(raw))
	var builder strings.Builder
	previousSeparator := false
	for _, character := range lowered {
		switch {
		case (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9'):
			builder.WriteRune(character)
			previousSeparator = false
		case character == '-' || character == '_' || character == '.' || character == ' ' || character == '/':
			if builder.Len() > 0 && !previousSeparator {
				builder.WriteByte('-')
				previousSeparator = true
			}
		}
		if builder.Len() >= 40 {
			break
		}
	}
	return strings.Trim(builder.String(), "-._")
}
