package setup

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const ownershipLedgerFilename = "ownership.json"

const (
	OwnershipLedgerVersion          = "2"
	OwnershipLedgerGenerationAbsent = ""
)

type OwnershipRecord struct {
	Path          string             `json:"path"`
	Component     ComponentReference `json:"component"`
	HarnessID     string             `json:"harnessId"`
	ContentSHA256 string             `json:"contentSha256"`
	SizeBytes     int64              `json:"sizeBytes"`
	InstalledAt   string             `json:"installedAt"`
}

type OwnershipLedger struct {
	LedgerVersion string            `json:"ledgerVersion"`
	Records       []OwnershipRecord `json:"records"`
}

type LedgerRecordRejection struct {
	RecordIndex int
	Reason      string
}

type LedgerRejectionError struct {
	SourceGeneration string
	Rejections       []LedgerRecordRejection
}

func (err *LedgerRejectionError) Error() string {
	reasons := make([]string, 0, len(err.Rejections))
	for _, rejection := range err.Rejections {
		reasons = append(reasons, fmt.Sprintf("record %d: %s", rejection.RecordIndex, rejection.Reason))
	}
	return fmt.Sprintf("ownership ledger generation %s cannot be accepted; it was left unchanged and no record is treated as owned: %s", err.SourceGeneration, strings.Join(reasons, "; "))
}

type ownershipDocument struct {
	LedgerVersion string                    `json:"ledgerVersion,omitempty"`
	Records       []ownershipRecordDocument `json:"records"`
}

type ownershipRecordDocument struct {
	Path          string              `json:"path"`
	Component     *ComponentReference `json:"component,omitempty"`
	HarnessID     string              `json:"harnessId,omitempty"`
	ContentSHA256 string              `json:"contentSha256"`
	SizeBytes     int64               `json:"sizeBytes"`
	InstalledAt   string              `json:"installedAt"`
}

func LoadOwnershipLedger(dataRoot string) (OwnershipLedger, error) {
	ledger, _, err := LoadOwnershipLedgerGeneration(dataRoot)
	return ledger, err
}

func LoadOwnershipLedgerGeneration(dataRoot string) (OwnershipLedger, string, error) {
	data, err := os.ReadFile(filepath.Join(dataRoot, ownershipLedgerFilename))
	if errors.Is(err, fs.ErrNotExist) {
		return OwnershipLedger{LedgerVersion: OwnershipLedgerVersion}, OwnershipLedgerGenerationAbsent, nil
	}
	if err != nil {
		return OwnershipLedger{}, "", fmt.Errorf("read ownership ledger: %w", err)
	}
	return ParseOwnershipLedger(data)
}

func ParseOwnershipLedger(data []byte) (OwnershipLedger, string, error) {
	var document ownershipDocument
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return OwnershipLedger{}, "", fmt.Errorf("decode ownership ledger: %w", err)
	}
	if decoder.More() {
		return OwnershipLedger{}, "", errors.New("decode ownership ledger: trailing data after the ledger object")
	}
	if document.LedgerVersion != OwnershipLedgerVersion {
		return OwnershipLedger{}, "", fmt.Errorf("ownership ledger schema generation is unknown; only generation %q is supported", OwnershipLedgerVersion)
	}
	rejections := []LedgerRecordRejection{}
	records := make([]OwnershipRecord, 0, len(document.Records))
	seenPaths := make(map[string]bool, len(document.Records))
	for index, record := range document.Records {
		reason := ""
		switch {
		case record.Component == nil:
			reason = "has no component identity"
		case record.HarnessID == "" || !protocol.LabelOrAcceleratorPattern.MatchString(record.HarnessID):
			reason = "harnessId is not kebab-case"
		case seenPaths[record.Path]:
			reason = "duplicates an earlier record's path"
		}
		if reason == "" {
			reason = validateRecordPathGrammar(record.Path)
		}
		if reason == "" {
			if err := record.Component.Validate(); err != nil {
				reason = err.Error()
			}
		}
		if reason == "" {
			reason = validateRecordIntegrityFields(record)
		}
		if reason != "" {
			rejections = append(rejections, LedgerRecordRejection{RecordIndex: index, Reason: reason})
			continue
		}
		seenPaths[record.Path] = true
		records = append(records, OwnershipRecord{
			Path:          record.Path,
			Component:     *record.Component,
			HarnessID:     record.HarnessID,
			ContentSHA256: strings.ToLower(record.ContentSHA256),
			SizeBytes:     record.SizeBytes,
			InstalledAt:   record.InstalledAt,
		})
	}
	if len(rejections) > 0 {
		return OwnershipLedger{}, "", &LedgerRejectionError{SourceGeneration: OwnershipLedgerVersion, Rejections: rejections}
	}
	return OwnershipLedger{LedgerVersion: OwnershipLedgerVersion, Records: records}, OwnershipLedgerVersion, nil
}

func validateRecordPathGrammar(path string) string {
	if path == "" {
		return "path is empty"
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "path is not an absolute, already-clean path"
	}
	return ""
}

func validateRecordIntegrityFields(record ownershipRecordDocument) string {
	if !ChecksumPattern.MatchString(strings.ToLower(record.ContentSHA256)) {
		return "contentSha256 is not a sha256 digest"
	}
	if record.SizeBytes < 0 {
		return "sizeBytes is negative"
	}
	if record.InstalledAt == "" {
		return "installedAt is empty"
	}
	if _, err := time.Parse(time.RFC3339, record.InstalledAt); err != nil {
		return "installedAt is not an RFC3339 timestamp"
	}
	return ""
}

func (ledger OwnershipLedger) Save(dataRoot string) error {
	ledger.LedgerVersion = OwnershipLedgerVersion
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		return fmt.Errorf("create data root: %w", err)
	}
	encoded, err := json.MarshalIndent(ledger, "", "  ")
	if err != nil {
		return fmt.Errorf("encode ownership ledger: %w", err)
	}
	destination := filepath.Join(dataRoot, ownershipLedgerFilename)
	temporaryFile, err := os.CreateTemp(dataRoot, ".ownership-*")
	if err != nil {
		return fmt.Errorf("create ownership ledger temporary file: %w", err)
	}
	temporaryPath := temporaryFile.Name()
	if _, err := temporaryFile.Write(encoded); err != nil {
		temporaryFile.Close()
		os.Remove(temporaryPath)
		return fmt.Errorf("write ownership ledger temporary file: %w", err)
	}
	if err := temporaryFile.Sync(); err != nil {
		temporaryFile.Close()
		os.Remove(temporaryPath)
		return fmt.Errorf("sync ownership ledger temporary file: %w", err)
	}
	if err := temporaryFile.Close(); err != nil {
		os.Remove(temporaryPath)
		return fmt.Errorf("close ownership ledger temporary file: %w", err)
	}
	if err := os.Rename(temporaryPath, destination); err != nil {
		os.Remove(temporaryPath)
		return fmt.Errorf("replace ownership ledger: %w", err)
	}
	if directory, err := os.Open(dataRoot); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

func (ledger OwnershipLedger) RecordFor(path string) (OwnershipRecord, bool) {
	for _, record := range ledger.Records {
		if record.Path == path {
			return record, true
		}
	}
	return OwnershipRecord{}, false
}

func (ledger OwnershipLedger) WithRecord(record OwnershipRecord) OwnershipLedger {
	updated := OwnershipLedger{
		LedgerVersion: OwnershipLedgerVersion,
		Records:       make([]OwnershipRecord, 0, len(ledger.Records)+1),
	}
	replaced := false
	for _, existing := range ledger.Records {
		switch {
		case existing.Path == record.Path && !replaced:
			updated.Records = append(updated.Records, record)
			replaced = true
		case existing.Path == record.Path:
		default:
			updated.Records = append(updated.Records, existing)
		}
	}
	if !replaced {
		updated.Records = append(updated.Records, record)
	}
	return updated
}
