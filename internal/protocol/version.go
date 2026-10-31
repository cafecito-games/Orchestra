// Package protocol defines the shared Orchestra v1 domain and wire contracts
// exchanged between the master orchestrator, slave worker nodes, MCP bridges,
// and operator CLI commands.
package protocol

import (
	"cmp"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
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

// Linker-injected build metadata set via -ldflags -X during release and Task builds.
var (
	BuildVersion   = "dev"
	BuildCommit    = ""
	BuildDate      = ""
	BuildTreeState = ""
)

// BuildInfo describes the application version, protocol version, VCS revision,
// and Go toolchain embedded in an Orchestra binary.
type BuildInfo struct {
	Version         string `json:"version"`
	ProtocolVersion string `json:"protocolVersion"`
	Commit          string `json:"commit,omitempty"`
	Date            string `json:"date,omitempty"`
	TreeState       string `json:"treeState,omitempty"`
	GoVersion       string `json:"goVersion"`
}

// CurrentBuildInfo returns linker-injected build metadata supplemented by Go's
// embedded VCS and module build settings.
func CurrentBuildInfo() BuildInfo {
	embeddedBuildInfo, _ := debug.ReadBuildInfo()
	return ResolveBuildInfo(BuildVersion, BuildCommit, BuildDate, BuildTreeState, runtime.Version(), embeddedBuildInfo)
}

// EffectiveBuildVersion returns the normalized Orchestra build version.
func EffectiveBuildVersion() string {
	return CurrentBuildInfo().Version
}

// ResolveBuildInfo merges linker-injected metadata with embedded Go build settings.
func ResolveBuildInfo(
	rawVersion string,
	rawCommit string,
	rawDate string,
	rawTreeState string,
	goVersion string,
	embeddedBuildInfo *debug.BuildInfo,
) BuildInfo {
	versionValue := normalizeBuildVersion(rawVersion)
	commitHash := strings.TrimSpace(rawCommit)
	buildDate := strings.TrimSpace(rawDate)
	treeState := normalizeTreeState(rawTreeState)

	if embeddedBuildInfo != nil {
		if versionValue == "dev" && embeddedBuildInfo.Main.Version != "" && embeddedBuildInfo.Main.Version != "(devel)" {
			versionValue = normalizeBuildVersion(embeddedBuildInfo.Main.Version)
		}
		for _, setting := range embeddedBuildInfo.Settings {
			switch setting.Key {
			case "vcs.revision":
				if commitHash == "" {
					commitHash = strings.TrimSpace(setting.Value)
				}
			case "vcs.time":
				if buildDate == "" {
					buildDate = strings.TrimSpace(setting.Value)
				}
			case "vcs.modified":
				if treeState == "" {
					if modified, err := strconv.ParseBool(setting.Value); err == nil {
						if modified {
							treeState = "dirty"
						} else {
							treeState = "clean"
						}
					}
				}
			}
		}
	}

	return BuildInfo{
		Version:         versionValue,
		ProtocolVersion: ProtocolVersion,
		Commit:          commitHash,
		Date:            buildDate,
		TreeState:       treeState,
		GoVersion:       strings.TrimSpace(goVersion),
	}
}

// String formats BuildInfo as a single human-readable line for CLI output.
func (info BuildInfo) String() string {
	details := []string{"protocol v" + cmp.Or(info.ProtocolVersion, ProtocolVersion)}
	if info.Commit != "" {
		commitLabel := "commit " + shortCommitHash(info.Commit)
		if info.TreeState == "dirty" {
			commitLabel += ", dirty"
		}
		details = append(details, commitLabel)
	} else if info.TreeState == "dirty" {
		details = append(details, "dirty")
	}
	if info.Date != "" {
		details = append(details, "built "+info.Date)
	}
	return "orchestra " + cmp.Or(info.Version, "dev") + " (" + strings.Join(details, ", ") + ")"
}

func normalizeBuildVersion(rawValue string) string {
	trimmed := strings.TrimSpace(rawValue)
	if trimmed == "" || trimmed == "(devel)" {
		return "dev"
	}
	if len(trimmed) > 1 && trimmed[0] == 'v' && trimmed[1] >= '0' && trimmed[1] <= '9' {
		return trimmed[1:]
	}
	return trimmed
}

// NormalizeReleaseTarget normalizes a requested update version ("", "latest",
// "v1.2.3", or "1.2.3") into either "latest" or a tagless release version.
func NormalizeReleaseTarget(rawValue string) (string, error) {
	trimmed := strings.TrimSpace(rawValue)
	if trimmed == "" || strings.EqualFold(trimmed, "latest") {
		return "latest", nil
	}
	normalized := normalizeBuildVersion(trimmed)
	if !IsSafeReleaseVersion(normalized) {
		return "", errors.New("release version must be 'latest' or a semantic version such as 0.2.0")
	}
	return normalized, nil
}

// IsSafeReleaseVersion reports whether value is a safe, normalized release version
// suitable for URL segments and filesystem cache directories.
func IsSafeReleaseVersion(value string) bool {
	if value == "" || value == "." || value == ".." || len(value) > 64 || strings.EqualFold(value, "latest") || value == "dev" {
		return false
	}
	if value[0] < '0' || value[0] > '9' {
		return false
	}
	for _, character := range value {
		isDigit := character >= '0' && character <= '9'
		isLower := character >= 'a' && character <= 'z'
		isUpper := character >= 'A' && character <= 'Z'
		if !isDigit && !isLower && !isUpper && character != '.' && character != '-' && character != '+' {
			return false
		}
	}
	return !strings.Contains(value, "..")
}

// CompareSemanticVersions compares two normalized or v-prefixed semantic versions.
// It returns (-1, true) if left < right, (0, true) if equal, (1, true) if left > right,
// or (0, false) if either version is not a valid MAJOR.MINOR.PATCH semantic version.
func CompareSemanticVersions(leftRaw string, rightRaw string) (int, bool) {
	leftMajor, leftMinor, leftPatch, leftPrerelease, leftValid := parseSemanticVersion(leftRaw)
	rightMajor, rightMinor, rightPatch, rightPrerelease, rightValid := parseSemanticVersion(rightRaw)
	if !leftValid || !rightValid {
		return 0, false
	}
	if compared := cmp.Compare(leftMajor, rightMajor); compared != 0 {
		return compared, true
	}
	if compared := cmp.Compare(leftMinor, rightMinor); compared != 0 {
		return compared, true
	}
	if compared := cmp.Compare(leftPatch, rightPatch); compared != 0 {
		return compared, true
	}
	switch {
	case leftPrerelease == "" && rightPrerelease == "":
		return 0, true
	case leftPrerelease == "":
		return 1, true
	case rightPrerelease == "":
		return -1, true
	default:
		return cmp.Compare(leftPrerelease, rightPrerelease), true
	}
}

func parseSemanticVersion(rawValue string) (int, int, int, string, bool) {
	normalized := normalizeBuildVersion(rawValue)
	withoutBuild, _, _ := strings.Cut(normalized, "+")
	corePart, prerelease, hasPrerelease := strings.Cut(withoutBuild, "-")
	if hasPrerelease && prerelease == "" {
		return 0, 0, 0, "", false
	}
	segments := strings.Split(corePart, ".")
	if len(segments) != 3 {
		return 0, 0, 0, "", false
	}
	var numbers [3]int
	for index, segment := range segments {
		if segment == "" || (len(segment) > 1 && segment[0] == '0') {
			return 0, 0, 0, "", false
		}
		parsed, err := strconv.Atoi(segment)
		if err != nil || parsed < 0 {
			return 0, 0, 0, "", false
		}
		numbers[index] = parsed
	}
	return numbers[0], numbers[1], numbers[2], prerelease, true
}

func normalizeTreeState(rawValue string) string {
	switch strings.ToLower(strings.TrimSpace(rawValue)) {
	case "clean":
		return "clean"
	case "dirty":
		return "dirty"
	default:
		return ""
	}
}

func shortCommitHash(commitHash string) string {
	if len(commitHash) > 12 {
		return commitHash[:12]
	}
	return commitHash
}

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
