package setup

import (
	"context"
	"errors"
	"slices"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

type ComponentAssessment struct {
	Component          ComponentReference
	HarnessID          string
	HarnessInstalled   bool
	HarnessApplicable  bool
	ComponentInstalled bool
	ComponentPath      string
	AuthReadiness      AuthReadiness
	ACPLaunchReady     bool
	InstalledVersions  []string
	ActiveVersion      string
	RollbackVersion    string
	Provenance         ComponentProvenance
	Readiness          string
	UpdateVersion      string
	RollbackAvailable  bool
	DiagnosticCodes    []string
	Notes              []string
}

type ComponentAssessmentOptions struct {
	ACPStartupEvidenceKnown bool
}

func appendCode(assessment *ComponentAssessment, code string) {
	if !slices.Contains(assessment.DiagnosticCodes, code) {
		assessment.DiagnosticCodes = append(assessment.DiagnosticCodes, code)
	}
}

func AssessComponents(operationContext context.Context, manifest Manifest, ledger OwnershipLedger, dataRoot, platform string, activation ActivationState, harnesses []protocol.HarnessProfile, supplied ...ComponentAssessmentOptions) []ComponentAssessment {
	options := ComponentAssessmentOptions{}
	if len(supplied) > 0 {
		options = supplied[0]
	}
	assessments := make([]ComponentAssessment, 0, len(manifest.Components))
	for _, entry := range manifest.Components {
		profile, discovered := findHarnessProfile(harnesses, entry.HarnessID)
		assessment := ComponentAssessment{
			Component:         entry.Reference(),
			HarnessID:         entry.HarnessID,
			HarnessApplicable: entry.Kind.HasHarnessOfItsOwn(),
			AuthReadiness:     AuthReadinessUnknown,
			Provenance:        ComponentProvenanceNone,
			Readiness:         "unavailable",
		}
		assessment.HarnessInstalled = assessment.HarnessApplicable && discovered && profile.Available
		externalAdapterPath := ""
		if entry.Kind == ComponentKindACPAdapter && activation.Rejection == nil {
			if discoveredPath, found := DiscoverExternalACPAdapter(entry, platform); found {
				externalAdapterPath = discoveredPath
			}
		}
		activeInstalled, activeErr := ActiveInstalledComponent(dataRoot, manifest, platform, ledger, activation, entry.Reference().Identity())
		if entry.Kind == ComponentKindACPAdapter && options.ACPStartupEvidenceKnown && activeErr == nil &&
			!slices.Contains(profile.Transports, protocol.TransportACP) {
			activeErr = errors.New("the selected ACP adapter did not pass the daemon startup probe")
		}
		doctorEntry := ComponentDoctorEntry{Component: entry.Reference(), Provenance: ComponentProvenanceNone}
		describeActivation(&doctorEntry, activation, activeErr, assessment.HarnessInstalled, externalAdapterPath != "")
		assessment.ActiveVersion, assessment.RollbackVersion, assessment.Provenance = doctorEntry.ActiveVersion, doctorEntry.RollbackVersion, doctorEntry.Provenance
		assessment.Notes = append(assessment.Notes, doctorEntry.Notes...)

		references := []ComponentReference{entry.Reference()}
		for _, owned := range ledger.Records {
			if owned.Component.Identity() == entry.Reference().Identity() {
				references = append(references, owned.Component)
			}
		}
		if record, ok := activation.Ledger.RecordFor(entry.Reference().Identity()); ok {
			references = append(references, record.Active.Component)
			if record.Previous != nil {
				references = append(references, record.Previous.Component)
			}
		}
		for _, reference := range references {
			if slices.Contains(assessment.InstalledVersions, reference.Version) {
				continue
			}
			if _, err := resolveInstalledVersion(dataRoot, manifest, platform, ledger, reference); err == nil {
				assessment.InstalledVersions = append(assessment.InstalledVersions, reference.Version)
			}
		}
		slices.Sort(assessment.InstalledVersions)
		managedDeclaredInstalled := slices.Contains(assessment.InstalledVersions, entry.Version)
		assessment.ComponentInstalled = managedDeclaredInstalled || (entry.Kind == ComponentKindACPAdapter && assessment.Provenance == ComponentProvenanceExternal)
		assessment.RollbackAvailable = assessment.RollbackVersion != "" && slices.Contains(assessment.InstalledVersions, assessment.RollbackVersion)
		if assessment.ActiveVersion != "" && assessment.ActiveVersion != entry.Version && managedDeclaredInstalled {
			assessment.UpdateVersion = entry.Version
			appendCode(&assessment, "update-available")
		}
		if assessment.RollbackAvailable {
			appendCode(&assessment, "rollback-available")
		}

		distribution, supported := entry.Platforms[platform]
		if !supported {
			assessment.Notes = append(assessment.Notes, "no platform distribution for "+platform)
			appendCode(&assessment, "platform-unsupported")
		} else if target, err := ComponentTargetPath(dataRoot, entry, distribution); err == nil {
			assessment.ComponentPath = target
		} else {
			assessment.Notes = append(assessment.Notes, "no install location for component kind "+string(entry.Kind))
		}
		if entry.Kind == ComponentKindACPAdapter && assessment.Provenance == ComponentProvenanceExternal && externalAdapterPath != "" {
			assessment.ComponentPath = externalAdapterPath
			assessment.Notes = append(assessment.Notes, "discovered external ACP adapter at "+externalAdapterPath)
		}
		if managedDeclaredInstalled && assessment.ActiveVersion == "" && assessment.Provenance != ComponentProvenanceExternal && activation.Rejection == nil {
			assessment.Notes = append(assessment.Notes, "installed but not activated; run `orchestra setup activate`")
		}
		if specification, allowed := AuthProbeAllowlist[entry.HarnessID]; allowed && assessment.HarnessInstalled {
			assessment.AuthReadiness = RunAuthProbe(operationContext, profile.Binary, specification.Arguments, specification.SuccessExitCode)
		}
		externalACPVerified := !options.ACPStartupEvidenceKnown || slices.Contains(profile.Transports, protocol.TransportACP)
		if entry.Kind == ComponentKindACPAdapter && assessment.Provenance == ComponentProvenanceExternal && options.ACPStartupEvidenceKnown && !externalACPVerified {
			assessment.Notes = append(assessment.Notes, "the external ACP adapter was not enabled or did not pass the daemon startup probe")
		}
		acpAdapterReady := entry.Kind == ComponentKindACPAdapter &&
			assessment.HarnessInstalled &&
			assessment.AuthReadiness == AuthReadinessReady &&
			((assessment.ActiveVersion != "" && activeErr == nil) ||
				(assessment.Provenance == ComponentProvenanceExternal && externalACPVerified))
		antigravityBuiltInReady := entry.Kind == ComponentKindHarness &&
			entry.HarnessID == protocol.HarnessAntigravityCLI &&
			assessment.HarnessInstalled &&
			assessment.AuthReadiness == AuthReadinessReady &&
			(!options.ACPStartupEvidenceKnown || slices.Contains(profile.Transports, protocol.TransportACP))
		assessment.ACPLaunchReady = acpAdapterReady || antigravityBuiltInReady
		if entry.Kind == ComponentKindHarness && entry.HarnessID == protocol.HarnessAntigravityCLI && assessment.HarnessInstalled {
			assessment.Notes = append(assessment.Notes, "built-in ACP bridge available via `orchestra acp-bridge antigravity`")
		}
		if activeErr == nil && activeInstalled.Reference().Version != entry.Version {
			assessment.Notes = append(assessment.Notes, "the active version is retained from an earlier manifest and is no longer declared by this one")
		}

		switch {
		case activation.Rejection != nil:
			assessment.Readiness = "rejected"
			appendCode(&assessment, "activation-rejected")
		case assessment.ActiveVersion == "":
			switch {
			case assessment.Provenance == ComponentProvenanceExternal && assessment.HarnessInstalled && (entry.Kind != ComponentKindACPAdapter || externalACPVerified):
				assessment.Readiness = authComponentReadiness(assessment.AuthReadiness)
			case managedDeclaredInstalled:
				assessment.Readiness = "inactive"
				appendCode(&assessment, "not-activated")
			default:
				assessment.Readiness = "unavailable"
			}
		case activeErr != nil:
			assessment.Readiness = "unhealthy"
			appendCode(&assessment, "active-unverified")
		case !assessment.HarnessInstalled:
			assessment.Readiness = "unavailable"
			appendCode(&assessment, "harness-unavailable")
		default:
			assessment.Readiness = authComponentReadiness(assessment.AuthReadiness)
		}
		if assessment.HarnessApplicable && assessment.HarnessInstalled {
			switch assessment.AuthReadiness {
			case AuthReadinessNotReady:
				appendCode(&assessment, "auth-unhealthy")
			case AuthReadinessUnknown:
				appendCode(&assessment, "auth-unavailable")
			}
		}
		slices.Sort(assessment.DiagnosticCodes)
		assessments = append(assessments, assessment)
	}
	slices.SortFunc(assessments, func(first, second ComponentAssessment) int {
		if first.Component.Kind != second.Component.Kind {
			return stringsCompare(string(first.Component.Kind), string(second.Component.Kind))
		}
		return stringsCompare(first.Component.ID, second.Component.ID)
	})
	return assessments
}

func stringsCompare(first, second string) int {
	if first < second {
		return -1
	}
	if first > second {
		return 1
	}
	return 0
}

func authComponentReadiness(readiness AuthReadiness) string {
	switch readiness {
	case AuthReadinessReady:
		return "ready"
	case AuthReadinessNotReady:
		return "unhealthy"
	default:
		return "unavailable"
	}
}

func ComponentInventoryReport(nodeID, observedAt string, assessments []ComponentAssessment) protocol.ComponentInventoryReport {
	components := make([]protocol.ComponentInventoryEntry, 0, len(assessments))
	for _, assessment := range assessments {
		entry := protocol.ComponentInventoryEntry{
			Kind:              string(assessment.Component.Kind),
			ID:                assessment.Component.ID,
			HarnessID:         assessment.HarnessID,
			DeclaredVersion:   assessment.Component.Version,
			InstalledVersions: append([]string{}, assessment.InstalledVersions...),
			ActiveVersion:     assessment.ActiveVersion,
			RollbackVersion:   assessment.RollbackVersion,
			Provenance:        string(assessment.Provenance),
			Readiness:         assessment.Readiness,
			UpdateVersion:     assessment.UpdateVersion,
			RollbackAvailable: assessment.RollbackAvailable,
			DiagnosticCodes:   append([]string{}, assessment.DiagnosticCodes...),
		}
		components = append(components, entry)
	}
	return protocol.ComponentInventoryReport{NodeID: nodeID, ObservedAt: observedAt, Components: components}
}
