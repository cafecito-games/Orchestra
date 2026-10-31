package setup

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const hubDetailMaximumBytes = 256
const hubConnectivityGenericDetail = "connection attempt failed"
const activationRejectionMaximumBytes = 512
const activationRejectionGenericDetail = "the activation ledger could not be parsed"
const ActivationRepairGuidance = "Orchestra left the file unchanged and will launch no managed component: inspect activation.json under the data root, remove it, and re-run `orchestra setup activate` for each managed component."
const hubEndpointInvalidDisplay = "invalid control endpoint"

type ComponentProvenance string

const (
	ComponentProvenanceManaged  ComponentProvenance = "managed"
	ComponentProvenanceExternal ComponentProvenance = "external"
	ComponentProvenanceNone     ComponentProvenance = "none"
	ComponentProvenanceRejected ComponentProvenance = "rejected"
)

var ComponentProvenances = []ComponentProvenance{
	ComponentProvenanceManaged,
	ComponentProvenanceExternal,
	ComponentProvenanceNone,
	ComponentProvenanceRejected,
}

func (provenance ComponentProvenance) Valid() bool {
	return slices.Contains(ComponentProvenances, provenance)
}

type ActivationLedgerStatus struct {
	Generation string `json:"generation"`
	Accepted   bool   `json:"accepted"`
	Rejection  string `json:"rejection,omitempty"`
	Repair     string `json:"repair,omitempty"`
}

type ComponentDoctorEntry struct {
	Component          ComponentReference  `json:"component"`
	HarnessID          string              `json:"harnessId"`
	HarnessInstalled   bool                `json:"harnessInstalled"`
	HarnessApplicable  bool                `json:"harnessApplicable"`
	ComponentInstalled bool                `json:"componentInstalled"`
	ComponentPath      string              `json:"componentPath,omitempty"`
	AuthReadiness      AuthReadiness       `json:"authReadiness"`
	ACPLaunchReady     bool                `json:"acpLaunchReady"`
	ActiveVersion      string              `json:"activeVersion,omitempty"`
	RollbackVersion    string              `json:"rollbackVersion,omitempty"`
	Provenance         ComponentProvenance `json:"provenance"`
	Notes              []string            `json:"notes,omitempty"`
}

type HubConnectivity struct {
	Endpoint  string `json:"endpoint"`
	Reachable bool   `json:"reachable"`
	Detail    string `json:"detail,omitempty"`
}

type Report struct {
	Platform         string                 `json:"platform"`
	DataRoot         string                 `json:"dataRoot"`
	Components       []ComponentDoctorEntry `json:"components"`
	HubConnectivity  HubConnectivity        `json:"hubConnectivity"`
	Activation       ActivationLedgerStatus `json:"activation"`
	ApprovalPolicies map[string]string      `json:"approvalPolicies,omitempty"`
}

func RunDoctor(
	operationContext context.Context,
	manifest Manifest,
	ledger OwnershipLedger,
	dataRoot string,
	platform string,
	activation ActivationState,
	harnesses []protocol.HarnessProfile,
	controlEndpoint string,
	dial func(operationContext context.Context, endpoint string) error,
	assessmentOptions ...ComponentAssessmentOptions,
) Report {
	report := Report{
		Platform:   platform,
		DataRoot:   dataRoot,
		Components: make([]ComponentDoctorEntry, 0, len(manifest.Components)),
		Activation: activationStatus(activation),
	}
	for _, assessment := range AssessComponents(operationContext, manifest, ledger, dataRoot, platform, activation, harnesses, assessmentOptions...) {
		report.Components = append(report.Components, ComponentDoctorEntry{
			Component:          assessment.Component,
			HarnessID:          assessment.HarnessID,
			HarnessInstalled:   assessment.HarnessInstalled,
			HarnessApplicable:  assessment.HarnessApplicable,
			ComponentInstalled: assessment.ComponentInstalled,
			ComponentPath:      assessment.ComponentPath,
			AuthReadiness:      assessment.AuthReadiness,
			ACPLaunchReady:     assessment.ACPLaunchReady,
			ActiveVersion:      assessment.ActiveVersion,
			RollbackVersion:    assessment.RollbackVersion,
			Provenance:         assessment.Provenance,
			Notes:              assessment.Notes,
		})
	}
	report.HubConnectivity = HubConnectivity{Endpoint: sanitizeEndpointForDisplay(controlEndpoint)}
	if dial == nil {
		report.HubConnectivity.Detail = hubConnectivityGenericDetail
	} else if err := dial(operationContext, controlEndpoint); err != nil {
		report.HubConnectivity.Detail = screenedHubDetail(err.Error())
	} else {
		report.HubConnectivity.Reachable = true
	}
	return report
}

func activationStatus(activation ActivationState) ActivationLedgerStatus {
	status := ActivationLedgerStatus{Generation: activation.Generation, Accepted: activation.Rejection == nil}
	if activation.Rejection == nil {
		return status
	}
	status.Rejection = screenedActivationDetail(activation.Rejection.Error())
	status.Repair = ActivationRepairGuidance
	if rejection, ok := errors.AsType[*ActivationRejectionError](activation.Rejection); ok {
		status.Generation = rejection.SourceGeneration
	}
	return status
}

func screenedActivationDetail(detail string) string {
	bounded := truncateAtRuneBoundary(detail, activationRejectionMaximumBytes)
	if protocol.LooksSecretLike(bounded) {
		return activationRejectionGenericDetail
	}
	return bounded
}

func describeActivation(entry *ComponentDoctorEntry, activation ActivationState, activeErr error, harnessAvailable bool, externalAdapterAvailable bool) {
	if activation.Rejection != nil {
		entry.Provenance = ComponentProvenanceRejected
		return
	}
	record, activated := activation.Ledger.RecordFor(entry.Component.Identity())
	if activated {
		entry.ActiveVersion = record.Active.Component.Version
		if record.Previous != nil {
			entry.RollbackVersion = record.Previous.Component.Version
		}
		if activeErr == nil {
			entry.Provenance = ComponentProvenanceManaged
			return
		}
		entry.Notes = append(entry.Notes, "the activated version could not be verified: "+screenedActivationDetail(activeErr.Error()))
		if entry.Component.Kind == ComponentKindHarness && harnessAvailable {
			entry.Provenance = ComponentProvenanceExternal
			return
		}
		entry.Provenance = ComponentProvenanceNone
		return
	}
	if entry.Component.Kind == ComponentKindHarness && harnessAvailable {
		entry.Provenance = ComponentProvenanceExternal
		return
	}
	if entry.Component.Kind == ComponentKindACPAdapter && externalAdapterAvailable {
		entry.Provenance = ComponentProvenanceExternal
		return
	}
	entry.Provenance = ComponentProvenanceNone
}

func findHarnessProfile(harnesses []protocol.HarnessProfile, harnessID string) (protocol.HarnessProfile, bool) {
	for _, profile := range harnesses {
		if profile.ID == harnessID {
			return profile, true
		}
	}
	return protocol.HarnessProfile{}, false
}

func screenedHubDetail(detail string) string {
	bounded := truncateAtRuneBoundary(detail, hubDetailMaximumBytes)
	if protocol.LooksSecretLike(bounded) {
		return hubConnectivityGenericDetail
	}
	return bounded
}

func sanitizeEndpointForDisplay(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return value
	}
	addedScheme := !strings.Contains(value, "://")
	parseable := value
	if addedScheme {
		parseable = "https://" + value
	}
	parsed, err := url.Parse(parseable)
	if err != nil || parsed.Host == "" {
		return hubEndpointInvalidDisplay
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	parsed.RawFragment = ""
	sanitized := parsed.String()
	if addedScheme {
		sanitized = strings.TrimPrefix(sanitized, "https://")
	}
	return sanitized
}
