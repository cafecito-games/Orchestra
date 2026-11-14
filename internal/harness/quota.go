package harness

import (
	"context"
	"errors"
	"regexp"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

type ClassifiedError struct {
	Kind string
	Err  error
}

func (classified *ClassifiedError) Error() string {
	if classified == nil || classified.Err == nil {
		return ""
	}
	return classified.Err.Error()
}

func (classified *ClassifiedError) Unwrap() error {
	if classified == nil {
		return nil
	}
	return classified.Err
}

var quotaExhaustedPatterns = []string{
	"insufficient_quota",
	"insufficient quota",
	"quota_exceeded",
	"quota exceeded",
	"quota_exhausted",
	"quota exhausted",
	"out of quota",
	"exceeded your current quota",
	"exceeded current quota",
	"credit balance is too low",
	"insufficient credits",
	"out of credits",
	"no credits remaining",
	"billing_hard_limit_reached",
	"billing hard limit",
	"hard billing limit",
	"usage limit",
	"plan limit",
	"spend limit",
	"monthly limit",
	"daily limit",
	"weekly limit",
	"you've hit your limit",
	"you have hit your limit",
	"hit your usage limit",
	"reached your usage limit",
}

var rateLimitedPatterns = []string{
	"rate_limit_error",
	"rate_limit_exceeded",
	"rate_limited",
	"rate limit",
	"ratelimit",
	"rate-limit",
	"too many requests",
	"request was throttled",
	"throttlingexception",
	"resource_exhausted",
	"resource exhausted",
}

var status429Pattern = regexp.MustCompile(`(?i)(?:\b(?:http|status|code|error)\b[^\n]{0,32}\b429\b|\b429\s+too\s+many\s+requests\b|\b429\s*\{)`)

func isNonProviderError(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrDriverUnavailable) ||
		errors.Is(err, ErrMCPUnavailable) ||
		errors.Is(err, acp.ErrCancelled)
}

func WrapClassifiedError(err error) error {
	if err == nil || isNonProviderError(err) {
		return err
	}
	var existing *ClassifiedError
	if errors.As(err, &existing) && existing != nil && protocol.IsQuotaOrRateLimitErrorKind(existing.Kind) {
		return err
	}
	kind := ClassifyDiagnostic(err.Error())
	if kind == "" {
		return err
	}
	return &ClassifiedError{Kind: kind, Err: err}
}

func ClassifyError(err error) string {
	if err == nil || isNonProviderError(err) {
		return ""
	}
	var classified *ClassifiedError
	if errors.As(err, &classified) && classified != nil && protocol.IsQuotaOrRateLimitErrorKind(classified.Kind) {
		return classified.Kind
	}
	return ClassifyDiagnostic(err.Error())
}

func ClassifyDiagnostic(diagnostic string) string {
	normalized := strings.ToLower(strings.TrimSpace(diagnostic))
	if normalized == "" {
		return ""
	}
	for _, indicator := range quotaExhaustedPatterns {
		if strings.Contains(normalized, indicator) {
			return protocol.RunErrorKindQuotaExhausted
		}
	}
	for _, indicator := range rateLimitedPatterns {
		if strings.Contains(normalized, indicator) {
			return protocol.RunErrorKindRateLimited
		}
	}
	if status429Pattern.MatchString(diagnostic) {
		return protocol.RunErrorKindRateLimited
	}
	return ""
}
