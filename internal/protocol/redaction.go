package protocol

import (
	"regexp"
	"slices"
	"strings"
)

const RedactionMarker = "[redacted]"

var (
	secretLikeTokenPattern = regexp.MustCompile(`(^|[^A-Za-z0-9_])((sk-[a-z0-9_-]{10,})|(rk-[a-z0-9_-]{10,})|(pk_(test|live)_[a-z0-9_-]{10,})|((ghp|gho|ghu|ghs|ghr)_[a-z0-9_-]{10,})|(github_pat_[a-z0-9_]{20,})|(xox[abeprs]-[a-z0-9_-]{10,})|((akia|asia)[a-z0-9_-]{10,})|(glpat-[a-z0-9_-]{10,})|(aiza[a-z0-9_-]{20,}))($|[^A-Za-z0-9_])`)
	bearerHeaderPattern    = regexp.MustCompile(`(^|[^A-Za-z0-9_])bearer[ \t\n\f\r]+[^ \t\n\f\r]{10,}`)

	redactSecretPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?(?:-----END [A-Z0-9 ]*PRIVATE KEY-----|$)`),
		regexp.MustCompile(`\b(?:sk|rk)-(?:ant-|proj-|live-|test-)?[A-Za-z0-9_-]{10,}`),
		regexp.MustCompile(`\bpk_(?:test|live)_[A-Za-z0-9_-]{10,}`),
		regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9_]{10,}\b`),
		regexp.MustCompile(`\bgithub_pat_[A-Za-z0-9_]{20,}\b`),
		regexp.MustCompile(`\bxox[abeprs]-[A-Za-z0-9-]{10,}\b`),
		regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`),
		regexp.MustCompile(`\bglpat-[A-Za-z0-9_-]{10,}\b`),
		regexp.MustCompile(`\bAIza[0-9A-Za-z_-]{20,}\b`),
		regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b`),
	}
	redactCredentialHeaderPattern = regexp.MustCompile(`(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/-]{10,}=*`)
)

func foldASCII(text string) string {
	return strings.Map(func(character rune) rune {
		if character >= 'A' && character <= 'Z' {
			return character + ('a' - 'A')
		}
		return character
	}, text)
}

// LooksSecretLike reports whether text contains a credential-shaped token, bearer header,
// or PEM private key block.
func LooksSecretLike(text string) bool {
	if text == "" {
		return false
	}
	folded := foldASCII(text)
	return secretLikeTokenPattern.MatchString(folded) ||
		bearerHeaderPattern.MatchString(folded) ||
		(strings.Contains(text, "-----BEGIN") && strings.Contains(text, "PRIVATE KEY"))
}

// Redactor strips configured exact secrets and common credential patterns from free-form text.
type Redactor struct {
	exactSecrets []string
}

func NewRedactor(secrets ...string) *Redactor {
	seen := make(map[string]struct{}, len(secrets))
	exactSecrets := make([]string, 0, len(secrets))
	for _, secret := range secrets {
		if len(secret) < 8 {
			continue
		}
		if _, exists := seen[secret]; exists {
			continue
		}
		seen[secret] = struct{}{}
		exactSecrets = append(exactSecrets, secret)
	}
	slices.SortFunc(exactSecrets, func(left, right string) int {
		if len(left) != len(right) {
			return len(right) - len(left)
		}
		return strings.Compare(left, right)
	})
	return &Redactor{exactSecrets: exactSecrets}
}

func (redactor *Redactor) Redact(value string) string {
	if value == "" {
		return ""
	}
	result := value
	if redactor != nil {
		for _, secret := range redactor.exactSecrets {
			result = strings.ReplaceAll(result, secret, RedactionMarker)
		}
	}
	for _, pattern := range redactSecretPatterns {
		result = pattern.ReplaceAllString(result, RedactionMarker)
	}
	result = redactCredentialHeaderPattern.ReplaceAllStringFunc(result, func(match string) string {
		scheme, _, _ := strings.Cut(match, " ")
		scheme = strings.TrimSpace(scheme)
		return scheme + " " + RedactionMarker
	})
	return result
}

func (redactor *Redactor) ContainsSecret(value string) bool {
	if value == "" {
		return false
	}
	if LooksSecretLike(value) {
		return true
	}
	return redactor.Redact(value) != value
}

func RedactText(value string) string {
	return (*Redactor)(nil).Redact(value)
}

func BoundedTextValue(value string, limit int) string {
	return TruncateUTF8Head(value, limit)
}

func BoundedDiagnostic(value string) string {
	return TruncateUTF8Head(value, DiagnosticMaximumBytes)
}

func ScrubChildEnvironment(environment []string) []string {
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, _ := strings.Cut(entry, "=")
		switch strings.ToUpper(key) {
		case "ORCHESTRA_TOKEN", "ORCHESTRA_MCP_TOKEN":
			continue
		default:
			filtered = append(filtered, entry)
		}
	}
	return filtered
}
