package protocol

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	CapabilityEvidenceSourceRuntime    = "runtime"
	CapabilityEvidenceSourceConfigured = "configured"
	CapabilityEvidenceSourceProbe      = "probe"
)

var CapabilityEvidenceSources = []string{
	CapabilityEvidenceSourceRuntime,
	CapabilityEvidenceSourceConfigured,
	CapabilityEvidenceSourceProbe,
}

const (
	capabilityIDMaximumBytes           = 128
	rawValueMaximumBytes               = 256
	normalizedValueMaximumBytes        = 64
	probeDefinitionVersionMaximumBytes = 64
	capabilityDiagnosticMaximumBytes   = 512
	maximumCapabilityEvidenceEntries   = 256

	LabelOrAcceleratorMaximumBytes = normalizedValueMaximumBytes
)

var (
	capabilityIDPattern       = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*(:[a-z0-9]+(-[a-z0-9]+)*)?$`)
	projectIDPattern          = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	LabelOrAcceleratorPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	normalizedVersionPattern  = regexp.MustCompile(`^\d{1,4}(\.\d{1,4}){0,3}$`)
	reportedVersionPattern    = regexp.MustCompile(`\d+(\.\d+)+`)
)

type NodeCapabilityEvidence struct {
	CapabilityID           string `json:"capabilityId"`
	Source                 string `json:"source"`
	Success                bool   `json:"success"`
	RawValue               string `json:"rawValue,omitempty"`
	NormalizedValue        string `json:"normalizedValue,omitempty"`
	ProbeDefinitionVersion string `json:"probeDefinitionVersion,omitempty"`
	ObservedAt             string `json:"observedAt"`
	Diagnostic             string `json:"diagnostic,omitempty"`
}

type NodeCapabilityReport struct {
	NodeID           string                   `json:"nodeId"`
	ProjectAllowlist []string                 `json:"projectAllowlist,omitempty"`
	Evidence         []NodeCapabilityEvidence `json:"evidence"`
	At               string                   `json:"at"`
}

func (evidence NodeCapabilityEvidence) Validate() error {
	if evidence.CapabilityID == "" || len(evidence.CapabilityID) > capabilityIDMaximumBytes || !capabilityIDPattern.MatchString(evidence.CapabilityID) {
		return fmt.Errorf("node capability evidence capabilityId is malformed: %q", evidence.CapabilityID)
	}
	if !slices.Contains(CapabilityEvidenceSources, evidence.Source) {
		return fmt.Errorf("node capability evidence source is missing or unknown: %q", evidence.Source)
	}
	if !IsBoundedString(evidence.RawValue, rawValueMaximumBytes) {
		return errors.New("node capability evidence rawValue exceeds its bound")
	}
	if evidence.NormalizedValue != "" && len(evidence.NormalizedValue) > normalizedValueMaximumBytes {
		return errors.New("node capability evidence normalizedValue is malformed")
	}
	isProbe := evidence.Source == CapabilityEvidenceSourceProbe
	if isProbe && (evidence.ProbeDefinitionVersion == "" || len(evidence.ProbeDefinitionVersion) > probeDefinitionVersionMaximumBytes) {
		return errors.New("probe definition version does not match its source")
	}
	if !isProbe && evidence.ProbeDefinitionVersion != "" {
		return errors.New("probe definition version does not match its source")
	}
	if !IsBoundedString(evidence.Diagnostic, capabilityDiagnosticMaximumBytes) {
		return errors.New("node capability evidence diagnostic exceeds its bound")
	}
	if !IsTimestamp(evidence.ObservedAt) {
		return errors.New("node capability evidence observedAt is malformed")
	}
	return nil
}

func (report NodeCapabilityReport) Validate() error {
	if !IsIdentifier(report.NodeID) {
		return errors.New("node capability report is missing node identity")
	}
	seenProjects := make(map[string]bool, len(report.ProjectAllowlist))
	for _, projectID := range report.ProjectAllowlist {
		if !projectIDPattern.MatchString(projectID) {
			return fmt.Errorf("node capability report projectAllowlist is malformed: %q", projectID)
		}
		if seenProjects[projectID] {
			return errors.New("node capability report projectAllowlist contains a duplicate")
		}
		seenProjects[projectID] = true
	}
	if len(report.Evidence) > maximumCapabilityEvidenceEntries {
		return errors.New("node capability report evidence exceeds its bound")
	}
	seenEvidence := make(map[string]bool, len(report.Evidence))
	for _, entry := range report.Evidence {
		if err := entry.Validate(); err != nil {
			return err
		}
		pair := entry.CapabilityID + "\x00" + entry.Source
		if seenEvidence[pair] {
			return fmt.Errorf("node capability report contains duplicate evidence for %s via %s", entry.CapabilityID, entry.Source)
		}
		seenEvidence[pair] = true
	}
	if !IsTimestamp(report.At) {
		return errors.New("node capability report timestamp is malformed")
	}
	return nil
}

const (
	VersionComparatorEqual          = "="
	VersionComparatorGreaterOrEqual = ">="
	VersionComparatorGreater        = ">"
	VersionComparatorLessOrEqual    = "<="
	VersionComparatorLess           = "<"
)

var VersionComparators = []string{
	VersionComparatorGreaterOrEqual,
	VersionComparatorLessOrEqual,
	VersionComparatorGreater,
	VersionComparatorLess,
	VersionComparatorEqual,
}

type VersionConstraint struct {
	Comparator string
	Version    string
}

func IsNormalizedVersion(value string) bool {
	if len(value) > 32 || !normalizedVersionPattern.MatchString(value) {
		return false
	}
	for segment := range strings.SplitSeq(value, ".") {
		if segment != "0" && strings.HasPrefix(segment, "0") {
			return false
		}
	}
	return true
}

func ExtractNormalizedVersion(output string) string {
	version := reportedVersionPattern.FindString(output)
	if !IsNormalizedVersion(version) {
		return ""
	}
	return version
}

func ParseVersionConstraint(raw string) (VersionConstraint, error) {
	if len(raw) > 40 {
		return VersionConstraint{}, fmt.Errorf("version constraint exceeds its bound: %q", raw)
	}
	for _, comparator := range VersionComparators {
		if strings.HasPrefix(raw, comparator) {
			version := strings.TrimPrefix(raw, comparator)
			if !IsNormalizedVersion(version) {
				return VersionConstraint{}, fmt.Errorf("version constraint has an invalid version: %q", raw)
			}
			return VersionConstraint{Comparator: comparator, Version: version}, nil
		}
	}
	if !IsNormalizedVersion(raw) {
		return VersionConstraint{}, fmt.Errorf("version constraint has an invalid version: %q", raw)
	}
	return VersionConstraint{Comparator: VersionComparatorEqual, Version: raw}, nil
}

func CompareVersions(leftVersion, rightVersion string) int {
	leftSegments := strings.Split(leftVersion, ".")
	rightSegments := strings.Split(rightVersion, ".")
	for index := range max(len(leftSegments), len(rightSegments)) {
		var leftValue, rightValue int
		if index < len(leftSegments) {
			leftValue, _ = strconv.Atoi(leftSegments[index])
		}
		if index < len(rightSegments) {
			rightValue, _ = strconv.Atoi(rightSegments[index])
		}
		if leftValue != rightValue {
			if leftValue < rightValue {
				return -1
			}
			return 1
		}
	}
	return 0
}

func SatisfiesVersionConstraint(normalizedVersion string, constraint VersionConstraint) bool {
	if !IsNormalizedVersion(normalizedVersion) {
		return false
	}
	compared := CompareVersions(normalizedVersion, constraint.Version)
	switch constraint.Comparator {
	case VersionComparatorEqual:
		return compared == 0
	case VersionComparatorGreaterOrEqual:
		return compared >= 0
	case VersionComparatorGreater:
		return compared > 0
	case VersionComparatorLessOrEqual:
		return compared <= 0
	case VersionComparatorLess:
		return compared < 0
	default:
		return false
	}
}

const (
	ComponentInventoryLimit  = 64
	ComponentVersionLimit    = 32
	ComponentDiagnosticLimit = 16
)

var (
	ComponentKinds           = []string{"harness", "acp-adapter"}
	ComponentProvenances     = []string{"managed", "external", "none", "rejected"}
	ComponentReadinesses     = []string{"ready", "inactive", "unavailable", "rejected", "unhealthy", "not-applicable"}
	ComponentDiagnosticCodes = []string{
		"activation-rejected",
		"not-activated",
		"active-unverified",
		"harness-unavailable",
		"auth-unavailable",
		"auth-unhealthy",
		"platform-unsupported",
		"update-available",
		"rollback-available",
	}
)

type ComponentInventoryEntry struct {
	Kind              string   `json:"kind"`
	ID                string   `json:"id"`
	HarnessID         string   `json:"harnessId,omitempty"`
	DeclaredVersion   string   `json:"declaredVersion"`
	InstalledVersions []string `json:"installedVersions"`
	ActiveVersion     string   `json:"activeVersion,omitempty"`
	RollbackVersion   string   `json:"rollbackVersion,omitempty"`
	Provenance        string   `json:"provenance"`
	Readiness         string   `json:"readiness"`
	UpdateVersion     string   `json:"updateVersion,omitempty"`
	RollbackAvailable bool     `json:"rollbackAvailable"`
	DiagnosticCodes   []string `json:"diagnosticCodes"`
}

type ComponentInventoryReport struct {
	NodeID     string                    `json:"nodeId"`
	ObservedAt string                    `json:"observedAt"`
	Components []ComponentInventoryEntry `json:"components"`
}

func isSortedUnique(values []string) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] >= values[index] {
			return false
		}
	}
	return true
}

func (entry ComponentInventoryEntry) Validate() error {
	if !slices.Contains(ComponentKinds, entry.Kind) || len(entry.ID) > 128 || !LabelOrAcceleratorPattern.MatchString(entry.ID) || !IsNormalizedVersion(entry.DeclaredVersion) {
		return errors.New("component inventory entry identity is malformed")
	}
	if len(entry.HarnessID) > 128 || !LabelOrAcceleratorPattern.MatchString(entry.HarnessID) {
		return errors.New("component inventory harness identity is malformed")
	}
	if entry.InstalledVersions == nil || len(entry.InstalledVersions) > ComponentVersionLimit || !isSortedUnique(entry.InstalledVersions) {
		return errors.New("component inventory installed versions are malformed")
	}
	for _, version := range entry.InstalledVersions {
		if !IsNormalizedVersion(version) {
			return errors.New("component inventory installed version is malformed")
		}
	}
	for _, version := range []string{entry.ActiveVersion, entry.RollbackVersion, entry.UpdateVersion} {
		if version != "" && !IsNormalizedVersion(version) {
			return errors.New("component inventory version is malformed")
		}
	}
	if !slices.Contains(ComponentProvenances, entry.Provenance) || !slices.Contains(ComponentReadinesses, entry.Readiness) ||
		entry.DiagnosticCodes == nil || len(entry.DiagnosticCodes) > ComponentDiagnosticLimit || !isSortedUnique(entry.DiagnosticCodes) {
		return errors.New("component inventory status is malformed")
	}
	for _, code := range entry.DiagnosticCodes {
		if !slices.Contains(ComponentDiagnosticCodes, code) {
			return errors.New("component inventory diagnostic code is unknown")
		}
	}
	rollbackVerified := entry.RollbackVersion != "" && slices.Contains(entry.InstalledVersions, entry.RollbackVersion)
	if entry.RollbackAvailable != rollbackVerified {
		return errors.New("component inventory rollback facts conflict")
	}
	return nil
}

func (report ComponentInventoryReport) Validate() error {
	if !IsIdentifier(report.NodeID) || !IsTimestamp(report.ObservedAt) || report.Components == nil || len(report.Components) > ComponentInventoryLimit {
		return errors.New("component inventory report is malformed")
	}
	previousKey := ""
	for index, entry := range report.Components {
		if err := entry.Validate(); err != nil {
			return fmt.Errorf("component inventory entry %d: %w", index, err)
		}
		key := entry.Kind + "\x00" + entry.ID
		if previousKey != "" && previousKey >= key {
			return errors.New("component inventory keys must be sorted and unique")
		}
		previousKey = key
	}
	return nil
}
