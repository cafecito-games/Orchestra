package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

const (
	MaximumBlocksPerMessage = 16
	MaximumBlockDataBytes   = 16 * 1024
	MaximumBlockDepth       = 8

	BlockSchemaGitCommit      = "git-commit"
	BlockSchemaWorkspaceLease = "workspace-lease"
	BlockSchemaTestReport     = "test-report"
	BlockSchemaReviewVerdict  = "review-verdict"
)

var (
	ReviewVerdictValues = []string{"approved", "changes_requested", "comment", "blocked"}
)

type StructuredBlock struct {
	Schema  string         `json:"schema"`
	Version int            `json:"version"`
	Data    map[string]any `json:"data"`
}

func NormalizeStructuredBlocks(blocks []StructuredBlock, redactor *Redactor) ([]StructuredBlock, error) {
	if len(blocks) == 0 {
		return nil, nil
	}
	if len(blocks) > MaximumBlocksPerMessage {
		return nil, fmt.Errorf("blocks exceed maximum of %d per message", MaximumBlocksPerMessage)
	}
	normalized := make([]StructuredBlock, 0, len(blocks))
	for index, block := range blocks {
		clean, err := normalizeStructuredBlock(block, redactor)
		if err != nil {
			return nil, fmt.Errorf("block[%d]: %w", index, err)
		}
		normalized = append(normalized, clean)
	}
	return normalized, nil
}

func parseBlockSchemaAndVersion(rawSchema string, rawVersion int) (string, int, error) {
	schema := strings.ToLower(strings.TrimSpace(rawSchema))
	version := rawVersion
	if base, suffix, hasAt := strings.Cut(schema, "@"); hasAt {
		parsedVersion, err := strconv.Atoi(suffix)
		if err != nil || parsedVersion <= 0 {
			return "", 0, fmt.Errorf("invalid schema version suffix in %q", rawSchema)
		}
		if version != 0 && version != parsedVersion {
			return "", 0, fmt.Errorf("schema %q version conflicts with version=%d", rawSchema, version)
		}
		schema = base
		version = parsedVersion
	}
	if schema == "" || version <= 0 {
		return "", 0, fmt.Errorf("schema and positive version are required (got schema=%q version=%d)", rawSchema, rawVersion)
	}
	return schema, version, nil
}

func normalizeStructuredBlock(block StructuredBlock, redactor *Redactor) (StructuredBlock, error) {
	schema, version, err := parseBlockSchemaAndVersion(block.Schema, block.Version)
	if err != nil {
		return StructuredBlock{}, err
	}
	if block.Data == nil {
		return StructuredBlock{}, errors.New("block data object is required")
	}

	encoded, err := json.Marshal(block.Data)
	if err != nil {
		return StructuredBlock{}, fmt.Errorf("encode block data: %w", err)
	}
	if len(encoded) > MaximumBlockDataBytes {
		return StructuredBlock{}, fmt.Errorf("block data exceeds %d bytes", MaximumBlockDataBytes)
	}

	var decoded any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return StructuredBlock{}, fmt.Errorf("decode block data: %w", err)
	}
	if err := checkBlockDepth(decoded, 0); err != nil {
		return StructuredBlock{}, err
	}
	redactedAny := redactBlockValue(decoded, redactor)
	data, ok := redactedAny.(map[string]any)
	if !ok {
		return StructuredBlock{}, errors.New("block data must be a JSON object")
	}

	if err := validateBuiltInBlockSchema(schema, version, data); err != nil {
		return StructuredBlock{}, err
	}

	return StructuredBlock{
		Schema:  schema,
		Version: version,
		Data:    data,
	}, nil
}

func checkBlockDepth(value any, depth int) error {
	if depth > MaximumBlockDepth {
		return fmt.Errorf("block data exceeds maximum nesting depth of %d", MaximumBlockDepth)
	}
	switch typed := value.(type) {
	case map[string]any:
		for _, child := range typed {
			if err := checkBlockDepth(child, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := checkBlockDepth(child, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func redactBlockValue(value any, redactor *Redactor) any {
	switch typed := value.(type) {
	case string:
		if redactor != nil {
			return redactor.Redact(typed)
		}
		return RedactText(typed)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			out[key] = redactBlockValue(child, redactor)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, child := range typed {
			out[i] = redactBlockValue(child, redactor)
		}
		return out
	default:
		return value
	}
}

func validateBuiltInBlockSchema(schema string, version int, data map[string]any) error {
	if version != 1 {
		return fmt.Errorf("unsupported version %d for schema %q (expected 1)", version, schema)
	}
	switch schema {
	case BlockSchemaGitCommit:
		allowed := []string{"sha", "branch", "repository", "summary", "filesChanged"}
		if err := rejectUnknownBlockKeys(schema, data, allowed); err != nil {
			return err
		}
		sha, err := requireBlockString(data, "sha", 1, 64)
		if err != nil {
			return err
		}
		if strings.ContainsAny(sha, " \t\r\n") {
			return errors.New("git-commit sha must not contain whitespace")
		}
		if err := optionalBlockString(data, "branch", 256); err != nil {
			return err
		}
		if err := optionalBlockString(data, "repository", 512); err != nil {
			return err
		}
		if err := optionalBlockString(data, "summary", 2048); err != nil {
			return err
		}
		if err := optionalBlockNonNegativeInt(data, "filesChanged"); err != nil {
			return err
		}
		return nil

	case BlockSchemaWorkspaceLease:
		allowed := []string{"leaseId", "nodeId", "branch", "baseBranch", "path", "status"}
		if err := rejectUnknownBlockKeys(schema, data, allowed); err != nil {
			return err
		}
		leaseID, err := requireBlockString(data, "leaseId", 1, 128)
		if err != nil {
			return err
		}
		if !IsIdentifier(leaseID) {
			return fmt.Errorf("workspace-lease leaseId %q is malformed", leaseID)
		}
		if err := optionalBlockString(data, "nodeId", 128); err != nil {
			return err
		}
		if err := optionalBlockString(data, "branch", 256); err != nil {
			return err
		}
		if err := optionalBlockString(data, "baseBranch", 256); err != nil {
			return err
		}
		if err := optionalBlockString(data, "path", 1024); err != nil {
			return err
		}
		if err := optionalBlockString(data, "status", 64); err != nil {
			return err
		}
		return nil

	case BlockSchemaTestReport:
		allowed := []string{"passed", "failed", "skipped", "suite", "command", "durationMs", "summary"}
		if err := rejectUnknownBlockKeys(schema, data, allowed); err != nil {
			return err
		}
		if err := requireBlockNonNegativeInt(data, "passed"); err != nil {
			return err
		}
		if err := requireBlockNonNegativeInt(data, "failed"); err != nil {
			return err
		}
		if err := optionalBlockNonNegativeInt(data, "skipped"); err != nil {
			return err
		}
		if err := optionalBlockNonNegativeInt(data, "durationMs"); err != nil {
			return err
		}
		if err := optionalBlockString(data, "suite", 256); err != nil {
			return err
		}
		if err := optionalBlockString(data, "command", 1024); err != nil {
			return err
		}
		if err := optionalBlockString(data, "summary", 2048); err != nil {
			return err
		}
		return nil

	case BlockSchemaReviewVerdict:
		allowed := []string{"verdict", "sha", "summary", "findings"}
		if err := rejectUnknownBlockKeys(schema, data, allowed); err != nil {
			return err
		}
		verdict, err := requireBlockString(data, "verdict", 1, 64)
		if err != nil {
			return err
		}
		if !slices.Contains(ReviewVerdictValues, verdict) {
			return fmt.Errorf("review-verdict verdict %q must be one of %s", verdict, strings.Join(ReviewVerdictValues, ", "))
		}
		if err := optionalBlockString(data, "sha", 64); err != nil {
			return err
		}
		if err := optionalBlockString(data, "summary", 2048); err != nil {
			return err
		}
		if rawFindings, present := data["findings"]; present {
			items, ok := rawFindings.([]any)
			if !ok || len(items) > 64 {
				return errors.New("review-verdict findings must be an array of at most 64 strings")
			}
			for _, item := range items {
				text, isStr := item.(string)
				if !isStr || !IsBoundedString(text, 1024) {
					return errors.New("review-verdict finding must be a string up to 1024 bytes")
				}
			}
		}
		return nil

	default:
		return fmt.Errorf("unknown structured block schema %q@%d", schema, version)
	}
}

func rejectUnknownBlockKeys(schema string, data map[string]any, allowed []string) error {
	for key := range data {
		if !slices.Contains(allowed, key) {
			return fmt.Errorf("schema %q does not allow field %q", schema, key)
		}
	}
	return nil
}

func requireBlockString(data map[string]any, key string, minLen, maxLen int) (string, error) {
	raw, exists := data[key]
	if !exists {
		return "", fmt.Errorf("missing required field %q", key)
	}
	text, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("field %q must be a string", key)
	}
	trimmed := strings.TrimSpace(text)
	if len(trimmed) < minLen || !IsBoundedString(trimmed, maxLen) {
		return "", fmt.Errorf("field %q must be between %d and %d bytes", key, minLen, maxLen)
	}
	return trimmed, nil
}

func optionalBlockString(data map[string]any, key string, maxLen int) error {
	raw, exists := data[key]
	if !exists {
		return nil
	}
	text, ok := raw.(string)
	if !ok {
		return fmt.Errorf("field %q must be a string", key)
	}
	if !IsBoundedString(text, maxLen) {
		return fmt.Errorf("field %q exceeds %d bytes", key, maxLen)
	}
	return nil
}

func requireBlockNonNegativeInt(data map[string]any, key string) error {
	raw, exists := data[key]
	if !exists {
		return fmt.Errorf("missing required field %q", key)
	}
	return validateNonNegativeJSONInt(key, raw)
}

func optionalBlockNonNegativeInt(data map[string]any, key string) error {
	raw, exists := data[key]
	if !exists {
		return nil
	}
	return validateNonNegativeJSONInt(key, raw)
}

func validateNonNegativeJSONInt(key string, raw any) error {
	num, ok := raw.(float64)
	if !ok || num < 0 || num != float64(int64(num)) {
		return fmt.Errorf("field %q must be a non-negative integer", key)
	}
	return nil
}

func FormatStructuredBlockSummary(block StructuredBlock) string {
	tag := fmt.Sprintf("%s@%d", block.Schema, block.Version)
	var parts []string
	switch block.Schema {
	case BlockSchemaGitCommit:
		if sha, ok := block.Data["sha"].(string); ok && sha != "" {
			if len(sha) > 12 {
				sha = sha[:12]
			}
			parts = append(parts, "sha="+sha)
		}
		if branch, ok := block.Data["branch"].(string); ok && branch != "" {
			parts = append(parts, "branch="+branch)
		}
	case BlockSchemaWorkspaceLease:
		if id, ok := block.Data["leaseId"].(string); ok && id != "" {
			parts = append(parts, "lease="+id)
		}
		if branch, ok := block.Data["branch"].(string); ok && branch != "" {
			parts = append(parts, "branch="+branch)
		}
	case BlockSchemaTestReport:
		if passed, ok := block.Data["passed"].(float64); ok {
			parts = append(parts, fmt.Sprintf("passed=%d", int64(passed)))
		}
		if failed, ok := block.Data["failed"].(float64); ok {
			parts = append(parts, fmt.Sprintf("failed=%d", int64(failed)))
		}
		if suite, ok := block.Data["suite"].(string); ok && suite != "" {
			parts = append(parts, "suite="+suite)
		}
	case BlockSchemaReviewVerdict:
		if verdict, ok := block.Data["verdict"].(string); ok && verdict != "" {
			parts = append(parts, "verdict="+verdict)
		}
		if sha, ok := block.Data["sha"].(string); ok && sha != "" {
			if len(sha) > 12 {
				sha = sha[:12]
			}
			parts = append(parts, "sha="+sha)
		}
	}
	if len(parts) == 0 {
		return "[" + tag + "]"
	}
	return "[" + tag + " " + strings.Join(parts, " ") + "]"
}
