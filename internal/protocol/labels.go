package protocol

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	LabelKeyMaximumBytes     = 64
	LabelValueMaximumBytes   = 256
	MaximumLabelsPerResource = 64
	ReservedLabelPrefix      = "orchestra."
)

var (
	ErrReservedLabel = errors.New("label key uses reserved orchestra.* namespace")
	labelKeyPattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
)

type LabelFilter struct {
	Key    string
	HasVal bool
	Values []string
}

func IsReservedLabelKey(key string) bool {
	return strings.HasPrefix(key, ReservedLabelPrefix)
}

func hasControlCharacters(value string) bool {
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

func ValidateLabelKey(raw string, allowReserved bool) (string, error) {
	if raw == "" || hasControlCharacters(raw) {
		return "", fmt.Errorf("invalid label key %q: must match [a-z0-9][a-z0-9._-]{0,63}", raw)
	}
	key := strings.TrimSpace(raw)
	if len(key) > LabelKeyMaximumBytes || !labelKeyPattern.MatchString(key) || LooksSecretLike(key) {
		return "", fmt.Errorf("invalid label key %q: must match [a-z0-9][a-z0-9._-]{0,63}", raw)
	}
	if !allowReserved && IsReservedLabelKey(key) {
		return "", fmt.Errorf("%w: %q", ErrReservedLabel, key)
	}
	return key, nil
}

func NormalizeLabelValue(raw string, redactor *Redactor) (string, error) {
	if !utf8.ValidString(raw) || hasControlCharacters(raw) {
		return "", errors.New("label value must be single-line valid UTF-8 without control characters")
	}
	trimmed := strings.TrimSpace(raw)
	if redactor != nil {
		trimmed = redactor.Redact(trimmed)
	} else {
		trimmed = RedactText(trimmed)
	}
	if len(trimmed) > LabelValueMaximumBytes {
		return "", fmt.Errorf("label value exceeds %d bytes", LabelValueMaximumBytes)
	}
	return trimmed, nil
}

func NormalizeLabels(raw map[string]string, redactor *Redactor, allowReserved bool) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if len(raw) > MaximumLabelsPerResource {
		return nil, fmt.Errorf("labels exceed maximum of %d entries", MaximumLabelsPerResource)
	}
	normalized := make(map[string]string, len(raw))
	for rawKey, rawValue := range raw {
		key, err := ValidateLabelKey(rawKey, allowReserved)
		if err != nil {
			return nil, err
		}
		value, err := NormalizeLabelValue(rawValue, redactor)
		if err != nil {
			return nil, fmt.Errorf("label %q: %w", key, err)
		}
		normalized[key] = value
	}
	return normalized, nil
}

func ParseLabelFilter(raw string) (LabelFilter, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || hasControlCharacters(trimmed) || !utf8.ValidString(trimmed) {
		return LabelFilter{}, fmt.Errorf("invalid label filter %q", raw)
	}
	rawKey, rawValues, hasEquals := strings.Cut(trimmed, "=")
	key, err := ValidateLabelKey(strings.TrimSpace(rawKey), true)
	if err != nil {
		return LabelFilter{}, fmt.Errorf("invalid label filter %q: %w", raw, err)
	}
	if !hasEquals {
		return LabelFilter{Key: key, HasVal: false}, nil
	}
	parts := strings.Split(rawValues, "|")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		candidate := strings.TrimSpace(part)
		if len(candidate) > LabelValueMaximumBytes {
			return LabelFilter{}, fmt.Errorf("label filter %q value exceeds %d bytes", raw, LabelValueMaximumBytes)
		}
		values = append(values, candidate)
	}
	return LabelFilter{
		Key:    key,
		HasVal: true,
		Values: values,
	}, nil
}

func ParseLabelFilters(rawFilters []string) ([]LabelFilter, error) {
	if len(rawFilters) == 0 {
		return nil, nil
	}
	filters := make([]LabelFilter, 0, len(rawFilters))
	for _, raw := range rawFilters {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		parsed, err := ParseLabelFilter(trimmed)
		if err != nil {
			return nil, err
		}
		filters = append(filters, parsed)
	}
	return filters, nil
}

func MatchesLabelFilters(labels map[string]string, filters []LabelFilter) bool {
	if len(filters) == 0 {
		return true
	}
	for _, filter := range filters {
		actual, exists := labels[filter.Key]
		if !exists {
			return false
		}
		if filter.HasVal && !slices.Contains(filter.Values, actual) {
			return false
		}
	}
	return true
}

func CloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	return maps.Clone(labels)
}
