package setup

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

type RollbackResult struct {
	Removed  []OwnershipRecord
	Retained []OwnershipRecord
}

type ComponentSelector struct {
	Kind    ComponentKind
	ID      string
	Version string
}

func (selector ComponentSelector) Validate() error {
	if !selector.Kind.Valid() {
		return errors.New("component kind is unknown")
	}
	if selector.ID == "" || !protocol.LabelOrAcceleratorPattern.MatchString(selector.ID) {
		return errors.New("component id is not kebab-case")
	}
	if selector.Version != "" && !protocol.IsNormalizedVersion(selector.Version) {
		return errors.New("component version is not a normalized dotted version")
	}
	return nil
}

func (selector ComponentSelector) Matches(reference ComponentReference) bool {
	if reference.Kind != selector.Kind || reference.ID != selector.ID {
		return false
	}
	return selector.Version == "" || reference.Version == selector.Version
}

func Uninstall(dataRoot string, ledger OwnershipLedger, selector ComponentSelector) (RollbackResult, OwnershipLedger, error) {
	if err := selector.Validate(); err != nil {
		return RollbackResult{}, OwnershipLedger{}, fmt.Errorf("uninstall selector: %w", err)
	}
	result := RollbackResult{}
	updated := OwnershipLedger{LedgerVersion: OwnershipLedgerVersion, Records: make([]OwnershipRecord, 0, len(ledger.Records))}
	for _, record := range ledger.Records {
		if !selector.Matches(record.Component) {
			updated.Records = append(updated.Records, record)
			continue
		}
		if !recordIsSafelyRemovable(record, dataRoot) {
			result.Retained = append(result.Retained, record)
			updated.Records = append(updated.Records, record)
			continue
		}
		if err := os.Remove(record.Path); err != nil {
			result.Retained = append(result.Retained, record)
			updated.Records = append(updated.Records, record)
			continue
		}
		result.Removed = append(result.Removed, record)
	}
	if err := updated.Save(dataRoot); err != nil {
		return RollbackResult{}, OwnershipLedger{}, fmt.Errorf("persist ownership ledger after uninstall: %w", err)
	}
	return result, updated, nil
}

func recordIsSafelyRemovable(record OwnershipRecord, dataRoot string) bool {
	if !filepath.IsAbs(record.Path) {
		return false
	}
	if _, err := verifyDirectoryWithinRoot(dataRoot, filepath.Dir(record.Path)); err != nil {
		return false
	}
	information, err := os.Lstat(record.Path)
	if err != nil || !information.Mode().IsRegular() {
		return false
	}
	digest, err := fileChecksum(record.Path)
	if err != nil || !strings.EqualFold(digest, record.ContentSHA256) {
		return false
	}
	return true
}
