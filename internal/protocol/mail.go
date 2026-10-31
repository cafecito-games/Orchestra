package protocol

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const (
	MailHandleMaximumBytes  = 64
	MailHostMaximumBytes    = 64
	MailAddressMaximumBytes = MailHandleMaximumBytes + 1 + MailHostMaximumBytes
	MailSubjectMaximumBytes = 512
	DefaultMailReadLimit    = 50
	MaximumMailReadLimit    = 200
)

var (
	MailboxKinds    = []string{"mcp-session", "instance", "operator"}
	MailboxStatuses = []string{"online", "idle", "busy", "offline", "released"}

	mailComponentPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,62}[a-z0-9])?$`)
)

type AgentMailbox struct {
	ID           string   `json:"id"`
	Address      string   `json:"address"`
	Handle       string   `json:"handle"`
	Host         string   `json:"host"`
	Aliases      []string `json:"aliases,omitempty"`
	CustomHandle bool     `json:"customHandle,omitempty"`
	Kind         string   `json:"kind"`
	Status       string   `json:"status"`
	Harness      string   `json:"harness,omitempty"`
	Workspace    string   `json:"workspace,omitempty"`
	PushCapable  bool     `json:"pushCapable,omitempty"`
	ClientID     string   `json:"clientId,omitempty"`
	SessionID    string   `json:"sessionId,omitempty"`
	InstanceID   string   `json:"instanceId,omitempty"`
	NodeID       string   `json:"nodeId,omitempty"`
	ThreadID     string   `json:"threadId,omitempty"`
	UnreadCount  int      `json:"unreadCount"`
	CreatedAt    string   `json:"createdAt"`
	UpdatedAt    string   `json:"updatedAt"`
	LastSeenAt   string   `json:"lastSeenAt"`
}

type MailMessage struct {
	ID              string   `json:"id"`
	Sequence        int64    `json:"sequence"`
	SourceKey       string   `json:"sourceKey"`
	IdempotencyKey  string   `json:"idempotencyKey"`
	Digest          string   `json:"digest"`
	FromMailboxID   string   `json:"fromMailboxId,omitempty"`
	FromAddress     string   `json:"from"`
	ToMailboxID     string   `json:"toMailboxId"`
	ToAddress       string   `json:"to"`
	ReplyToID       string   `json:"replyToId,omitempty"`
	Subject         string   `json:"subject,omitempty"`
	Body            string   `json:"body"`
	ArtifactIDs     []string `json:"artifactIds,omitempty"`
	WakeRequested   bool     `json:"wakeRequested"`
	WakeDispatched  bool     `json:"wakeDispatched,omitempty"`
	WakeTaskID      string   `json:"wakeTaskId,omitempty"`
	Read            bool     `json:"read"`
	ReadAt          string   `json:"readAt,omitempty"`
	DeliveredPushAt string   `json:"deliveredPushAt,omitempty"`
	CreatedAt       string   `json:"createdAt"`
}

type MailReceipt struct {
	ID             string `json:"id"`
	SourceKey      string `json:"sourceKey"`
	IdempotencyKey string `json:"idempotencyKey"`
	Digest         string `json:"digest"`
	MessageID      string `json:"messageId"`
	CreatedAt      string `json:"createdAt"`
}

type ClientMailRegistration struct {
	RequestedName string `json:"requestedName,omitempty"`
	Host          string `json:"host,omitempty"`
	Harness       string `json:"harness,omitempty"`
	Workspace     string `json:"workspace,omitempty"`
	PushCapable   bool   `json:"pushCapable,omitempty"`
	ProcessID     int    `json:"processId,omitempty"`
}

type MailNotification struct {
	MailboxAddress string      `json:"mailboxAddress"`
	Message        MailMessage `json:"message"`
	OccurredAt     string      `json:"occurredAt"`
}

func NormalizeMailHandle(raw string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" || len(normalized) > MailHandleMaximumBytes || LooksSecretLike(normalized) {
		return "", false
	}
	if !mailComponentPattern.MatchString(normalized) {
		return "", false
	}
	return normalized, true
}

func NormalizeMailHost(raw string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" || len(normalized) > MailHostMaximumBytes || LooksSecretLike(normalized) {
		return "", false
	}
	if !mailComponentPattern.MatchString(normalized) {
		return "", false
	}
	return normalized, true
}

// NormalizeHostnameSlug derives a clean mail host component from an OS hostname or node name.
func NormalizeHostnameSlug(raw string) string {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if firstSegment, _, hasDot := strings.Cut(trimmed, "."); hasDot && firstSegment != "" {
		trimmed = firstSegment
	}
	slug := sanitizeMailSlug(trimmed, MailHostMaximumBytes)
	if normalized, ok := NormalizeMailHost(slug); ok {
		return normalized
	}
	return "local"
}

// NormalizeHarnessMailSlug maps known harness and MCP client names to concise handle prefixes.
func NormalizeHarnessMailSlug(raw string) string {
	lower := strings.ToLower(strings.TrimSpace(raw))
	switch {
	case strings.Contains(lower, "claude"):
		return "claude"
	case strings.Contains(lower, "codex"):
		return "codex"
	case strings.Contains(lower, "opencode"):
		return "opencode"
	case strings.Contains(lower, "antigravity") || lower == "agy":
		return "antigravity"
	case strings.Contains(lower, "cursor"):
		return "cursor"
	case strings.Contains(lower, "gemini"):
		return "gemini"
	}
	slug := sanitizeMailSlug(lower, 24)
	if slug == "" || slug == "orchestra" || slug == "mcp" ||
		strings.HasPrefix(slug, "mcp-client") || strings.HasPrefix(slug, "mcp-session") ||
		strings.HasPrefix(slug, "client-") || strings.HasPrefix(slug, "osess-") {
		return "agent"
	}
	return slug
}

// SanitizeMailHandle converts free-form purpose or node labels into a valid mail handle when possible.
func SanitizeMailHandle(raw string) (string, bool) {
	slug := sanitizeMailSlug(strings.ToLower(strings.TrimSpace(raw)), MailHandleMaximumBytes)
	return NormalizeMailHandle(slug)
}

func FormatMailAddress(handle, host string) (string, error) {
	normalizedHandle, handleOK := NormalizeMailHandle(handle)
	if !handleOK {
		return "", fmt.Errorf("invalid mail handle %q", handle)
	}
	normalizedHost, hostOK := NormalizeMailHost(host)
	if !hostOK {
		return "", fmt.Errorf("invalid mail host %q", host)
	}
	return normalizedHandle + "@" + normalizedHost, nil
}

func ParseMailAddress(raw string) (handle string, host string, address string, ok bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || len(trimmed) > MailAddressMaximumBytes {
		return "", "", "", false
	}
	left, right, hasAt := strings.Cut(trimmed, "@")
	if !hasAt || strings.Contains(right, "@") {
		return "", "", "", false
	}
	normalizedHandle, handleOK := NormalizeMailHandle(left)
	normalizedHost, hostOK := NormalizeMailHost(right)
	if !handleOK || !hostOK {
		return "", "", "", false
	}
	return normalizedHandle, normalizedHost, normalizedHandle + "@" + normalizedHost, true
}

// NormalizeRenameTarget accepts either a bare handle ("auth-api") or a full address ("auth-api@fabrica")
// and returns the validated handle, host, and canonical address.
func NormalizeRenameTarget(raw string, defaultHost string) (string, string, string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", "", errors.New("mail name must not be empty")
	}
	if strings.Contains(trimmed, "@") {
		parsedHandle, parsedHost, parsedAddress, ok := ParseMailAddress(trimmed)
		if !ok {
			return "", "", "", fmt.Errorf("invalid mail address %q", raw)
		}
		return parsedHandle, parsedHost, parsedAddress, nil
	}
	normalizedHandle, handleOK := NormalizeMailHandle(trimmed)
	if !handleOK {
		return "", "", "", fmt.Errorf("invalid mail name %q: use letters, numbers, '.', '_', or '-'", raw)
	}
	normalizedHost, hostOK := NormalizeMailHost(defaultHost)
	if !hostOK {
		normalizedHost = NormalizeHostnameSlug(defaultHost)
	}
	return normalizedHandle, normalizedHost, normalizedHandle + "@" + normalizedHost, nil
}

func sanitizeMailSlug(raw string, maximumBytes int) string {
	var builder strings.Builder
	previousHyphen := false
	for _, character := range raw {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') {
			if builder.Len() >= maximumBytes {
				break
			}
			builder.WriteRune(character)
			previousHyphen = false
			continue
		}
		if character == '-' || character == '_' || character == '.' || character == ' ' {
			if builder.Len() == 0 || previousHyphen || builder.Len()+1 >= maximumBytes {
				continue
			}
			if character == ' ' {
				character = '-'
			}
			builder.WriteRune(character)
			previousHyphen = true
		}
	}
	return strings.Trim(builder.String(), "-_.")
}
