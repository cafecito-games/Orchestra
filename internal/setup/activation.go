package setup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const activationLedgerFilename = "activation.json"

const (
	ActivationLedgerVersion          = "1"
	ActivationLedgerGenerationAbsent = ""
)

var ErrComponentNotActivated = errors.New("component has no active version")

type ComponentIdentity struct {
	Kind ComponentKind
	ID   string
}

func (reference ComponentReference) Identity() ComponentIdentity {
	return ComponentIdentity{Kind: reference.Kind, ID: reference.ID}
}

func (identity ComponentIdentity) Validate() error {
	reference := ComponentReference{Kind: identity.Kind, ID: identity.ID, Version: "0"}
	return reference.Validate()
}

func (identity ComponentIdentity) String() string {
	return string(identity.Kind) + "/" + identity.ID
}

func (selector ComponentSelector) Identity() ComponentIdentity {
	return ComponentIdentity{Kind: selector.Kind, ID: selector.ID}
}

type ActivationTarget struct {
	Component     ComponentReference `json:"component"`
	Path          string             `json:"path"`
	ContentSHA256 string             `json:"contentSha256"`
}

type ActivationRecord struct {
	Active   ActivationTarget  `json:"active"`
	Previous *ActivationTarget `json:"previous,omitempty"`
}

func (record ActivationRecord) Identity() ComponentIdentity {
	return record.Active.Component.Identity()
}

type ActivationLedger struct {
	LedgerVersion string             `json:"ledgerVersion"`
	Records       []ActivationRecord `json:"records"`
}

type ActivationRejectionError struct {
	SourceGeneration string
	Rejections       []LedgerRecordRejection
}

func (err *ActivationRejectionError) Error() string {
	reasons := make([]string, 0, len(err.Rejections))
	for _, rejection := range err.Rejections {
		reasons = append(reasons, fmt.Sprintf("record %d: %s", rejection.RecordIndex, rejection.Reason))
	}
	return fmt.Sprintf("activation ledger generation %s cannot be accepted; it was left unchanged and no component is treated as activated: %s", err.SourceGeneration, strings.Join(reasons, "; "))
}

type activationDocument struct {
	LedgerVersion string                     `json:"ledgerVersion"`
	Records       []activationRecordDocument `json:"records"`
}

type activationRecordDocument struct {
	Active   *activationTargetDocument `json:"active"`
	Previous *activationTargetDocument `json:"previous,omitempty"`
}

type activationTargetDocument struct {
	Component     *ComponentReference `json:"component"`
	Path          string              `json:"path"`
	ContentSHA256 string              `json:"contentSha256"`
}

func ParseActivationLedger(data []byte) (ActivationLedger, string, error) {
	var document activationDocument
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		return ActivationLedger{}, "", fmt.Errorf("decode activation ledger: %w", err)
	}
	if decoder.More() {
		return ActivationLedger{}, "", errors.New("decode activation ledger: trailing data after the ledger object")
	}
	if document.LedgerVersion != ActivationLedgerVersion {
		return ActivationLedger{}, "", fmt.Errorf("activation ledger schema generation is unknown; only generation %q is supported", ActivationLedgerVersion)
	}
	rejections := []LedgerRecordRejection{}
	records := make([]ActivationRecord, 0, len(document.Records))
	seenIdentities := make(map[ComponentIdentity]bool, len(document.Records))
	for index, record := range document.Records {
		active, reason := record.Active.target()
		if reason != "" {
			rejections = append(rejections, LedgerRecordRejection{RecordIndex: index, Reason: "active " + reason})
			continue
		}
		var previous *ActivationTarget
		if record.Previous != nil {
			retained, previousReason := record.Previous.target()
			if previousReason != "" {
				rejections = append(rejections, LedgerRecordRejection{RecordIndex: index, Reason: "previous " + previousReason})
				continue
			}
			if retained.Component.Identity() != active.Component.Identity() {
				rejections = append(rejections, LedgerRecordRejection{RecordIndex: index, Reason: "previous names a different component identity than active"})
				continue
			}
			if retained.Component.Version == active.Component.Version {
				rejections = append(rejections, LedgerRecordRejection{RecordIndex: index, Reason: "previous names the same version as active"})
				continue
			}
			previous = &retained
		}
		if seenIdentities[active.Component.Identity()] {
			rejections = append(rejections, LedgerRecordRejection{RecordIndex: index, Reason: "duplicates an earlier record's component identity"})
			continue
		}
		seenIdentities[active.Component.Identity()] = true
		records = append(records, ActivationRecord{Active: active, Previous: previous})
	}
	if len(rejections) > 0 {
		return ActivationLedger{}, "", &ActivationRejectionError{SourceGeneration: ActivationLedgerVersion, Rejections: rejections}
	}
	return ActivationLedger{LedgerVersion: ActivationLedgerVersion, Records: records}, ActivationLedgerVersion, nil
}

func (document *activationTargetDocument) target() (ActivationTarget, string) {
	if document == nil {
		return ActivationTarget{}, "selection is missing"
	}
	if document.Component == nil {
		return ActivationTarget{}, "selection has no component identity"
	}
	if err := document.Component.Validate(); err != nil {
		return ActivationTarget{}, "selection " + err.Error()
	}
	if reason := validateRecordPathGrammar(document.Path); reason != "" {
		return ActivationTarget{}, "selection " + reason
	}
	if !ChecksumPattern.MatchString(strings.ToLower(document.ContentSHA256)) {
		return ActivationTarget{}, "selection contentSha256 is not a sha256 digest"
	}
	return ActivationTarget{
		Component:     *document.Component,
		Path:          document.Path,
		ContentSHA256: strings.ToLower(document.ContentSHA256),
	}, ""
}

func LoadActivationLedgerGeneration(dataRoot string) (ActivationLedger, string, error) {
	data, err := os.ReadFile(filepath.Join(dataRoot, activationLedgerFilename))
	if errors.Is(err, fs.ErrNotExist) {
		return ActivationLedger{LedgerVersion: ActivationLedgerVersion}, ActivationLedgerGenerationAbsent, nil
	}
	if err != nil {
		return ActivationLedger{}, "", fmt.Errorf("read activation ledger: %w", err)
	}
	return ParseActivationLedger(data)
}

type ActivationState struct {
	Ledger     ActivationLedger
	Generation string
	Rejection  error
}

func LoadActivationState(dataRoot string) ActivationState {
	ledger, generation, err := LoadActivationLedgerGeneration(dataRoot)
	if err != nil {
		return ActivationState{Generation: generation, Rejection: err}
	}
	return ActivationState{Ledger: ledger, Generation: generation}
}

func (state ActivationState) Fingerprint() string {
	rejection := ""
	if state.Rejection != nil {
		rejection = state.Rejection.Error()
	}
	encoded, err := json.Marshal(struct {
		Generation string           `json:"generation"`
		Rejection  string           `json:"rejection"`
		Ledger     ActivationLedger `json:"ledger"`
	}{Generation: state.Generation, Rejection: rejection, Ledger: state.Ledger})
	if err != nil {
		return ""
	}
	summed := sha256.Sum256(encoded)
	return hex.EncodeToString(summed[:])
}

func (ledger ActivationLedger) RecordFor(identity ComponentIdentity) (ActivationRecord, bool) {
	for _, record := range ledger.Records {
		if record.Identity() == identity {
			return record, true
		}
	}
	return ActivationRecord{}, false
}

func (ledger ActivationLedger) WithRecord(record ActivationRecord) ActivationLedger {
	updated := ActivationLedger{
		LedgerVersion: ActivationLedgerVersion,
		Records:       make([]ActivationRecord, 0, len(ledger.Records)+1),
	}
	replaced := false
	for _, existing := range ledger.Records {
		switch {
		case existing.Identity() == record.Identity() && !replaced:
			updated.Records = append(updated.Records, record)
			replaced = true
		case existing.Identity() == record.Identity():
		default:
			updated.Records = append(updated.Records, existing)
		}
	}
	if !replaced {
		updated.Records = append(updated.Records, record)
	}
	return updated
}

func (ledger ActivationLedger) Save(dataRoot string) error {
	ledger.LedgerVersion = ActivationLedgerVersion
	if ledger.Records == nil {
		ledger.Records = []ActivationRecord{}
	}
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		return fmt.Errorf("create data root: %w", err)
	}
	encoded, err := json.MarshalIndent(ledger, "", "  ")
	if err != nil {
		return fmt.Errorf("encode activation ledger: %w", err)
	}
	destination := filepath.Join(dataRoot, activationLedgerFilename)
	temporaryFile, err := os.CreateTemp(dataRoot, ".activation-*")
	if err != nil {
		return fmt.Errorf("create activation ledger temporary file: %w", err)
	}
	temporaryPath := temporaryFile.Name()
	if _, err := temporaryFile.Write(encoded); err != nil {
		temporaryFile.Close()
		os.Remove(temporaryPath)
		return fmt.Errorf("write activation ledger temporary file: %w", err)
	}
	if err := temporaryFile.Sync(); err != nil {
		temporaryFile.Close()
		os.Remove(temporaryPath)
		return fmt.Errorf("sync activation ledger temporary file: %w", err)
	}
	if err := temporaryFile.Close(); err != nil {
		os.Remove(temporaryPath)
		return fmt.Errorf("close activation ledger temporary file: %w", err)
	}
	if err := os.Rename(temporaryPath, destination); err != nil {
		os.Remove(temporaryPath)
		return fmt.Errorf("replace activation ledger: %w", err)
	}
	if directory, err := os.Open(dataRoot); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

type ComponentProbe func(operationContext context.Context, installed InstalledComponent) error

var HarnessVersionProbeAllowlist = map[string]AuthProbeSpecification{
	protocol.HarnessClaudeCLI:      {Arguments: []string{"--version"}, SuccessExitCode: 0},
	protocol.HarnessCodexCLI:       {Arguments: []string{"--version"}, SuccessExitCode: 0},
	protocol.HarnessOpenCodeCLI:    {Arguments: []string{"--version"}, SuccessExitCode: 0},
	protocol.HarnessAntigravityCLI: {Arguments: []string{"--version"}, SuccessExitCode: 0},
}

const (
	harnessVersionProbeUnansweredReason = "did not answer its version contract"
	harnessVersionProbeUnknownReason    = "could not be probed for its version contract"
	harnessVersionProbeMalformedReason  = "reported no parsable version for its version contract"
	harnessVersionProbeMismatchReason   = "reported a version other than the one pinned for it"
)

func ProbeHarnessVersion(operationContext context.Context, installed InstalledComponent) error {
	specification, allowed := HarnessVersionProbeAllowlist[installed.Entry.HarnessID]
	if !allowed {
		return fmt.Errorf("%s has no compiled-in version contract, so it cannot be activated", installed.Reference())
	}
	readiness, output := runProbeCapturingOutput(operationContext, installed.Path, specification.Arguments, specification.SuccessExitCode)
	switch readiness {
	case AuthReadinessReady:
	case AuthReadinessNotReady:
		return fmt.Errorf("%s %s", installed.Reference(), harnessVersionProbeUnansweredReason)
	default:
		return fmt.Errorf("%s %s", installed.Reference(), harnessVersionProbeUnknownReason)
	}
	reported := protocol.ExtractNormalizedVersion(output)
	if reported == "" {
		return fmt.Errorf("%s %s", installed.Reference(), harnessVersionProbeMalformedReason)
	}
	if reported != installed.Reference().Version {
		return fmt.Errorf("%s %s", installed.Reference(), harnessVersionProbeMismatchReason)
	}
	return nil
}

type ActivationContext struct {
	DataRoot   string
	Manifest   Manifest
	Platform   string
	Ownership  OwnershipLedger
	Activation ActivationState
	Probe      ComponentProbe
	InUse      func(path string) bool
}

func (activationContext ActivationContext) validate() error {
	if !filepath.IsAbs(activationContext.DataRoot) {
		return errors.New("data root must be an absolute path")
	}
	if !platformKeyPattern.MatchString(activationContext.Platform) {
		return errors.New("platform must be GOOS-GOARCH shaped")
	}
	if activationContext.InUse == nil {
		return errors.New("an active-run usage check is required")
	}
	if activationContext.Activation.Rejection != nil {
		return fmt.Errorf("the activation ledger cannot be accepted: %w", activationContext.Activation.Rejection)
	}
	return nil
}

type ActivationOutcome struct {
	Identity ComponentIdentity
	Active   ComponentReference
	Previous *ComponentReference
	Changed  bool
}

func Activate(operationContext context.Context, activationContext ActivationContext, selector ComponentSelector) (ActivationOutcome, error) {
	if err := activationContext.validate(); err != nil {
		return ActivationOutcome{}, fmt.Errorf("activate: %w", err)
	}
	if err := selector.Validate(); err != nil {
		return ActivationOutcome{}, fmt.Errorf("activate: %w", err)
	}
	if selector.Version == "" {
		return ActivationOutcome{}, errors.New("activate: an exact component version is required")
	}
	if activationContext.Probe == nil {
		return ActivationOutcome{}, errors.New("activate: a candidate probe is required")
	}
	identity := selector.Identity()
	reference := ComponentReference(selector)
	if _, declared := manifestEntryFor(activationContext.Manifest, reference); !declared {
		return ActivationOutcome{}, fmt.Errorf("activate %s: the component manifest does not declare this version", reference)
	}
	installed, err := verifiedComponentVersion(activationContext, reference)
	if err != nil {
		return ActivationOutcome{}, fmt.Errorf("activate %s: %w", reference, err)
	}
	current, activated := activationContext.Activation.Ledger.RecordFor(identity)
	if activated && current.Active.Component == reference && current.Active.Path == installed.Path &&
		current.Active.ContentSHA256 == installed.ContentSHA256 {
		return outcome(current, false), nil
	}
	if err := refuseBusySelection(activationContext, installed.Path, current, activated); err != nil {
		return ActivationOutcome{}, fmt.Errorf("activate %s: %w", reference, err)
	}
	if err := activationContext.Probe(operationContext, installed); err != nil {
		return ActivationOutcome{}, fmt.Errorf("activate %s: the candidate failed its probe: %w", reference, err)
	}
	record := ActivationRecord{Active: targetFor(installed)}
	if activated {
		switch {
		case current.Active.Component.Version != record.Active.Component.Version:
			retained := current.Active
			record.Previous = &retained
		case current.Previous != nil:
			retained := *current.Previous
			record.Previous = &retained
		}
	}
	if err := activationContext.Activation.Ledger.WithRecord(record).Save(activationContext.DataRoot); err != nil {
		return ActivationOutcome{}, fmt.Errorf("activate %s: %w", reference, err)
	}
	return outcome(record, true), nil
}

func Rollback(operationContext context.Context, activationContext ActivationContext, selector ComponentSelector) (ActivationOutcome, error) {
	if err := activationContext.validate(); err != nil {
		return ActivationOutcome{}, fmt.Errorf("rollback: %w", err)
	}
	if err := selector.Validate(); err != nil {
		return ActivationOutcome{}, fmt.Errorf("rollback: %w", err)
	}
	if selector.Version != "" {
		return ActivationOutcome{}, errors.New("rollback: the target comes from the retained rollback version, so no version may be given")
	}
	if activationContext.Probe == nil {
		return ActivationOutcome{}, errors.New("rollback: a candidate probe is required")
	}
	identity := selector.Identity()
	current, activated := activationContext.Activation.Ledger.RecordFor(identity)
	if !activated {
		return ActivationOutcome{}, fmt.Errorf("rollback %s: %w", identity, ErrComponentNotActivated)
	}
	if current.Previous == nil {
		return ActivationOutcome{}, fmt.Errorf("rollback %s: no previously verified version is retained", identity)
	}
	reference := current.Previous.Component
	installed, err := verifiedComponentVersion(activationContext, reference)
	if err != nil {
		return ActivationOutcome{}, fmt.Errorf("rollback %s: %w", reference, err)
	}
	if current.Previous.Path != installed.Path || current.Previous.ContentSHA256 != installed.ContentSHA256 {
		return ActivationOutcome{}, fmt.Errorf("rollback %s: the retained rollback target no longer matches the ownership ledger", reference)
	}
	if err := refuseBusySelection(activationContext, installed.Path, current, true); err != nil {
		return ActivationOutcome{}, fmt.Errorf("rollback %s: %w", reference, err)
	}
	if err := activationContext.Probe(operationContext, installed); err != nil {
		return ActivationOutcome{}, fmt.Errorf("rollback %s: the retained version failed its probe: %w", reference, err)
	}
	record := ActivationRecord{Active: targetFor(installed)}
	if err := activationContext.Activation.Ledger.WithRecord(record).Save(activationContext.DataRoot); err != nil {
		return ActivationOutcome{}, fmt.Errorf("rollback %s: %w", reference, err)
	}
	return outcome(record, true), nil
}

func verifiedComponentVersion(activationContext ActivationContext, reference ComponentReference) (InstalledComponent, error) {
	return resolveInstalledVersion(activationContext.DataRoot, activationContext.Manifest, activationContext.Platform, activationContext.Ownership, reference)
}

func resolveInstalledVersion(dataRoot string, manifest Manifest, platform string, ownership OwnershipLedger, reference ComponentReference) (InstalledComponent, error) {
	if entry, declared := manifestEntryFor(manifest, reference); declared {
		return VerifyInstalledComponent(dataRoot, entry, platform, ownership)
	}
	sibling, known := manifestEntryForIdentity(manifest, reference.Identity())
	if !known {
		return InstalledComponent{}, errors.New("the component manifest does not declare this component")
	}
	record, owned, err := ownershipRecordForComponent(ownership, reference)
	if err != nil {
		return InstalledComponent{}, err
	}
	if !owned {
		return InstalledComponent{}, fmt.Errorf("%w: the ownership ledger records no file for this version", ErrComponentNotInstalled)
	}
	if record.HarnessID != sibling.HarnessID {
		return InstalledComponent{}, errors.New("the ownership ledger binds this component version to a different harness")
	}
	versionDirectory, err := componentVersionDirectory(dataRoot, sibling, reference.Version)
	if err != nil {
		return InstalledComponent{}, err
	}
	if !strings.HasPrefix(record.Path, versionDirectory+string(filepath.Separator)) {
		return InstalledComponent{}, errors.New("the ownership ledger records this component version outside its own install directory")
	}
	entry := sibling
	entry.Version = reference.Version
	if _, statErr := os.Lstat(record.Path); errors.Is(statErr, fs.ErrNotExist) {
		return InstalledComponent{}, ErrComponentNotInstalled
	}
	installed := InstalledComponent{
		Entry:         entry,
		Path:          record.Path,
		ContentSHA256: strings.ToLower(record.ContentSHA256),
		dataRoot:      dataRoot,
	}
	if err := installed.Verify(); err != nil {
		return InstalledComponent{}, err
	}
	return installed, nil
}

func manifestEntryFor(manifest Manifest, reference ComponentReference) (ComponentManifestEntry, bool) {
	for index := range manifest.Components {
		if manifest.Components[index].Reference() == reference {
			return manifest.Components[index], true
		}
	}
	return ComponentManifestEntry{}, false
}

func manifestEntryForIdentity(manifest Manifest, identity ComponentIdentity) (ComponentManifestEntry, bool) {
	for index := range manifest.Components {
		if manifest.Components[index].Reference().Identity() == identity {
			return manifest.Components[index], true
		}
	}
	return ComponentManifestEntry{}, false
}

func ownershipRecordForComponent(ownership OwnershipLedger, reference ComponentReference) (OwnershipRecord, bool, error) {
	found := OwnershipRecord{}
	matches := 0
	for _, record := range ownership.Records {
		if record.Component == reference {
			found = record
			matches++
		}
	}
	switch matches {
	case 0:
		return OwnershipRecord{}, false, nil
	case 1:
		return found, true, nil
	default:
		return OwnershipRecord{}, false, errors.New("the ownership ledger records more than one file for this component version")
	}
}

func refuseBusySelection(activationContext ActivationContext, candidatePath string, current ActivationRecord, activated bool) error {
	if activationContext.InUse(candidatePath) {
		return errors.New("a run is currently using this component, so its selection cannot change")
	}
	if activated && activationContext.InUse(current.Active.Path) {
		return errors.New("a run is currently using the active version of this component, so its selection cannot change")
	}
	return nil
}

func targetFor(installed InstalledComponent) ActivationTarget {
	return ActivationTarget{
		Component:     installed.Reference(),
		Path:          installed.Path,
		ContentSHA256: installed.ContentSHA256,
	}
}

func outcome(record ActivationRecord, changed bool) ActivationOutcome {
	result := ActivationOutcome{Identity: record.Identity(), Active: record.Active.Component, Changed: changed}
	if record.Previous != nil {
		previous := record.Previous.Component
		result.Previous = &previous
	}
	return result
}

func ActiveInstalledComponent(
	dataRoot string,
	manifest Manifest,
	platform string,
	ownership OwnershipLedger,
	activation ActivationState,
	identity ComponentIdentity,
) (InstalledComponent, error) {
	if err := identity.Validate(); err != nil {
		return InstalledComponent{}, err
	}
	if activation.Rejection != nil {
		return InstalledComponent{}, fmt.Errorf("the activation ledger cannot be accepted: %w", activation.Rejection)
	}
	record, activated := activation.Ledger.RecordFor(identity)
	if !activated {
		return InstalledComponent{}, ErrComponentNotActivated
	}
	installed, err := resolveInstalledVersion(dataRoot, manifest, platform, ownership, record.Active.Component)
	if err != nil {
		return InstalledComponent{}, err
	}
	if installed.Path != record.Active.Path {
		return InstalledComponent{}, errors.New("the active version's recorded path is not this component version's install location")
	}
	if installed.ContentSHA256 != record.Active.ContentSHA256 {
		return InstalledComponent{}, errors.New("the active version's recorded digest no longer matches the ownership ledger")
	}
	return installed, nil
}

type PruneRetention struct {
	Record OwnershipRecord
	Reason string
}

const (
	PruneRetainedActive   = "the version is active"
	PruneRetainedRollback = "the version is the retained rollback target"
	PruneRetainedDrifted  = "the file no longer matches its ownership record"
)

type PruneResult struct {
	Removed  []OwnershipRecord
	Retained []PruneRetention
}

func Prune(activationContext ActivationContext, selector ComponentSelector) (PruneResult, OwnershipLedger, error) {
	if err := activationContext.validate(); err != nil {
		return PruneResult{}, OwnershipLedger{}, fmt.Errorf("prune: %w", err)
	}
	if err := selector.Validate(); err != nil {
		return PruneResult{}, OwnershipLedger{}, fmt.Errorf("prune: %w", err)
	}
	if selector.Version != "" {
		return PruneResult{}, OwnershipLedger{}, errors.New("prune: the versions to remove are derived from the activation record, so no version may be given")
	}
	identity := selector.Identity()
	record, activated := activationContext.Activation.Ledger.RecordFor(identity)
	protected := map[string]string{}
	if activated {
		protected[record.Active.Component.Version] = PruneRetainedActive
		if record.Previous != nil {
			protected[record.Previous.Component.Version] = PruneRetainedRollback
		}
	}
	result := PruneResult{}
	ledger := activationContext.Ownership
	versions := []string{}
	seen := map[string]bool{}
	for _, owned := range ledger.Records {
		if selector.Matches(owned.Component) && activationContext.InUse(owned.Path) {
			return PruneResult{}, OwnershipLedger{}, fmt.Errorf("prune %s: a run is currently using this component, so nothing was removed", identity)
		}
	}
	for _, owned := range ledger.Records {
		if !selector.Matches(owned.Component) {
			continue
		}
		if reason, isProtected := protected[owned.Component.Version]; isProtected {
			result.Retained = append(result.Retained, PruneRetention{Record: owned, Reason: reason})
			continue
		}
		if !seen[owned.Component.Version] {
			seen[owned.Component.Version] = true
			versions = append(versions, owned.Component.Version)
		}
	}
	for _, version := range versions {
		removed, updated, err := Uninstall(activationContext.DataRoot, ledger, ComponentSelector{Kind: selector.Kind, ID: selector.ID, Version: version})
		if err != nil {
			return PruneResult{}, OwnershipLedger{}, fmt.Errorf("prune %s: %w", identity, err)
		}
		ledger = updated
		result.Removed = append(result.Removed, removed.Removed...)
		for _, retained := range removed.Retained {
			result.Retained = append(result.Retained, PruneRetention{Record: retained, Reason: PruneRetainedDrifted})
		}
	}
	return result, ledger, nil
}
