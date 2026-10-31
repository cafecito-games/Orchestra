// Package protocol defines the shared Orchestra v1 domain and wire contracts
// exchanged between the master orchestrator, slave worker nodes, MCP bridges,
// and operator CLI commands.
package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	ProtocolVersion = "1"

	IdentifierMaximumBytes = 256
	DiagnosticMaximumBytes = 2048
	TextMaximumBytes       = 64 * 1024
	DiffMaximumBytes       = 256 * 1024
	PromptMaximumBytes     = 256 * 1024
)

var BuildVersion = "0.1.0"

func NewIdentifier(prefix string) string {
	var randomBytes [8]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return prefix + "-" + time.Now().UTC().Format("20060102150405.000000000")
	}
	return prefix + "-" + hex.EncodeToString(randomBytes[:])
}

func NewRoomMessageIdentifier() string {
	var randomBytes [8]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return "rmsg_" + time.Now().UTC().Format("20060102150405000000000")
	}
	return "rmsg_" + hex.EncodeToString(randomBytes[:])
}

func NowTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func IsIdentifier(value string) bool {
	return value != "" && len(value) <= IdentifierMaximumBytes && utf8.ValidString(value)
}

func IsTimestamp(value string) bool {
	if !IsIdentifier(value) {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, value)
	return err == nil
}

func ParseTimestamp(value string) (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func IsBoundedString(value string, limit int) bool {
	return len(value) <= limit && utf8.ValidString(value)
}

// TruncateUTF8Head returns at most limit leading bytes without splitting a multi-byte UTF-8 sequence.
func TruncateUTF8Head(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(value) <= limit {
		return value
	}
	truncated := value[:limit]
	for len(truncated) > 0 {
		character, size := utf8.DecodeLastRuneInString(truncated)
		if character != utf8.RuneError || size > 1 {
			break
		}
		truncated = truncated[:len(truncated)-1]
	}
	return truncated
}

// TruncateUTF8Tail returns at most limit trailing bytes without splitting a multi-byte UTF-8 sequence.
func TruncateUTF8Tail(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(value) <= limit {
		return value
	}
	start := len(value) - limit
	for start < len(value) && !utf8.RuneStart(value[start]) {
		start++
	}
	return value[start:]
}

const TruncationMarker = " [truncated]"

func TruncateField(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	if limit <= len(TruncationMarker) {
		return TruncateUTF8Head(value, limit)
	}
	return TruncateUTF8Head(value, limit-len(TruncationMarker)) + TruncationMarker
}

func TruncateDiagnostic(value string) string {
	return TruncateUTF8Head(value, DiagnosticMaximumBytes)
}

func IsCanonicalWorkspacePath(value string) bool {
	if !IsBoundedString(value, 4096) || value == "" || LooksSecretLike(value) {
		return false
	}
	for _, character := range value {
		if character < 32 {
			return false
		}
	}
	if !strings.HasPrefix(value, "/") {
		return false
	}
	if value != "/" && strings.HasSuffix(value, "/") {
		return false
	}
	for segment := range strings.SplitSeq(strings.TrimPrefix(value, "/"), "/") {
		if (segment == "" && value != "/") || segment == "." || segment == ".." {
			return false
		}
	}
	return true
}

func IsWorkspaceWithinRoot(workspacePath string, rootPath string) bool {
	if !IsCanonicalWorkspacePath(workspacePath) || !IsCanonicalWorkspacePath(rootPath) {
		return false
	}
	if rootPath == "/" {
		return true
	}
	return workspacePath == rootPath || strings.HasPrefix(workspacePath, rootPath+"/")
}

func SortedUniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	cloned := slices.Clone(values)
	slices.Sort(cloned)
	return slices.Compact(cloned)
}
