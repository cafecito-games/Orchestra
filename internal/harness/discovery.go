package harness

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

type provider struct {
	id       string
	label    string
	binary   string
	authMode string
	models   []string
}

var providers = []provider{
	{id: protocol.HarnessClaudeCLI, label: "Claude Code", binary: "claude", authMode: "local-subscription", models: []string{"sonnet", "opus", "haiku"}},
	{id: protocol.HarnessCodexCLI, label: "Codex", binary: "codex", authMode: "local-account", models: []string{}},
	{id: protocol.HarnessOpenCodeCLI, label: "OpenCode", binary: "opencode", authMode: "local-account", models: []string{}},
}

type HarnessProvenance string

const (
	HarnessProvenanceManaged  HarnessProvenance = "managed"
	HarnessProvenanceExternal HarnessProvenance = "external"
	HarnessProvenanceAbsent   HarnessProvenance = "absent"
)

var HarnessProvenances = []HarnessProvenance{HarnessProvenanceManaged, HarnessProvenanceExternal, HarnessProvenanceAbsent}

func (provenance HarnessProvenance) Valid() bool {
	return slices.Contains(HarnessProvenances, provenance)
}

type ManagedHarness struct {
	Binary  string
	Version string
	Verify  func() error
}

type HarnessCandidate struct {
	Provenance  HarnessProvenance
	Binary      string
	Version     string
	Description string
}

type HarnessResolution struct {
	HarnessID       string
	Selected        HarnessProvenance
	Managed         *HarnessCandidate
	External        *HarnessCandidate
	Profile         protocol.HarnessProfile
	ManagedRejected string
	verified        *ManagedHarness
}

func Discover(operationContext context.Context) []protocol.HarnessProfile {
	return Profiles(Resolve(operationContext, nil))
}

func Resolve(operationContext context.Context, managed map[string]ManagedHarness) []HarnessResolution {
	resolutions := make([]HarnessResolution, 0, len(providers))
	for _, item := range providers {
		resolution := HarnessResolution{
			HarnessID: item.id,
			Selected:  HarnessProvenanceAbsent,
			Profile: protocol.HarnessProfile{
				ID:          item.id,
				Label:       item.label,
				Description: "Not installed",
				Binary:      item.binary,
				Available:   false,
				AuthMode:    item.authMode,
				Models:      item.models,
				Transports:  []string{TransportNative},
			},
		}
		candidate, entry, rejection := managedCandidate(managed, item.id)
		switch {
		case rejection != "":
			resolution.ManagedRejected = rejection
		case entry.Binary != "":
			resolution.Managed = &candidate
			resolution.verified = &entry
		}
		if candidate, present := externalCandidate(operationContext, item.binary); present {
			resolution.External = &candidate
		}
		switch {
		case resolution.Managed != nil:
			resolution.Selected = HarnessProvenanceManaged
		case resolution.External != nil:
			resolution.Selected = HarnessProvenanceExternal
		}
		if selected := resolution.selectedCandidate(); selected != nil {
			resolution.Profile.Available = true
			resolution.Profile.Binary = selected.Binary
			resolution.Profile.Description = selected.Description
		}
		resolutions = append(resolutions, resolution)
	}
	return resolutions
}

func (resolution HarnessResolution) selectedCandidate() *HarnessCandidate {
	switch resolution.Selected {
	case HarnessProvenanceManaged:
		return resolution.Managed
	case HarnessProvenanceExternal:
		return resolution.External
	case HarnessProvenanceAbsent:
		return nil
	default:
		return nil
	}
}

func Profiles(resolutions []HarnessResolution) []protocol.HarnessProfile {
	profiles := make([]protocol.HarnessProfile, 0, len(resolutions))
	for _, resolution := range resolutions {
		profiles = append(profiles, resolution.Profile)
	}
	return profiles
}

func ManagedHarnesses(resolutions []HarnessResolution) map[string]ManagedHarness {
	harnesses := map[string]ManagedHarness{}
	for _, resolution := range resolutions {
		if resolution.Selected == HarnessProvenanceManaged && resolution.verified != nil {
			harnesses[resolution.HarnessID] = *resolution.verified
		}
	}
	return harnesses
}

const (
	ManagedRejectedNotAbsolute  = "the activated version's path is not absolute"
	ManagedRejectedUnverifiable = "the activated version has no verification function"
	ManagedRejectedDrifted      = "the activated version no longer verifies against the ownership ledger"
)

func managedCandidate(managed map[string]ManagedHarness, harnessID string) (HarnessCandidate, ManagedHarness, string) {
	entry, present := managed[harnessID]
	if !present {
		return HarnessCandidate{}, ManagedHarness{}, ""
	}
	if !filepath.IsAbs(entry.Binary) {
		return HarnessCandidate{}, ManagedHarness{}, ManagedRejectedNotAbsolute
	}
	if entry.Verify == nil {
		return HarnessCandidate{}, ManagedHarness{}, ManagedRejectedUnverifiable
	}
	if err := entry.Verify(); err != nil {
		return HarnessCandidate{}, ManagedHarness{}, ManagedRejectedDrifted
	}
	description := "Managed harness"
	if entry.Version != "" {
		description += " " + entry.Version
	}
	return HarnessCandidate{
		Provenance:  HarnessProvenanceManaged,
		Binary:      entry.Binary,
		Version:     entry.Version,
		Description: description,
	}, entry, ""
}

func externalCandidate(operationContext context.Context, binary string) (HarnessCandidate, bool) {
	path, err := exec.LookPath(binary)
	if err != nil {
		return HarnessCandidate{}, false
	}
	versionContext, cancel := context.WithTimeout(operationContext, 5*time.Second)
	command := exec.CommandContext(versionContext, path, "--version")
	command.Env = protocol.ScrubChildEnvironment(os.Environ())
	output, commandErr := command.CombinedOutput()
	cancel()
	if commandErr != nil {
		return HarnessCandidate{}, false
	}
	description := strings.TrimSpace(string(output))
	if description == "" {
		description = "Installed"
	}
	return HarnessCandidate{
		Provenance:  HarnessProvenanceExternal,
		Binary:      path,
		Version:     protocol.ExtractNormalizedVersion(description),
		Description: description,
	}, true
}

func Available(profiles []protocol.HarnessProfile, id string) bool {
	for _, profile := range profiles {
		if profile.ID == id {
			return profile.Available
		}
	}
	return false
}

const acpProbeTimeout = 15 * time.Second

func AdvertiseACP(operationContext context.Context, profiles []protocol.HarnessProfile, driver *ACPDriver) ([]protocol.HarnessProfile, map[string]error) {
	advertised := make([]protocol.HarnessProfile, len(profiles))
	copy(advertised, profiles)
	failures := map[string]error{}
	if driver == nil {
		return advertised, failures
	}
	for _, harnessID := range driver.HarnessIDs() {
		index := slices.IndexFunc(advertised, func(profile protocol.HarnessProfile) bool { return profile.ID == harnessID })
		if index < 0 {
			continue
		}
		probeContext, cancel := context.WithTimeout(operationContext, acpProbeTimeout)
		probe, err := driver.Probe(probeContext, harnessID)
		cancel()
		capabilities := probe.Capabilities
		if err != nil {
			driver.Disable(harnessID, err)
			failures[harnessID] = err
			continue
		}
		profile := advertised[index]
		transports := []string{}
		if profile.Available {
			transports = append(transports, TransportNative)
		}
		profile.Transports = append(transports, TransportACP)
		if !profile.Available {
			profile.Description = "ACP adapter"
			if capabilities.AdapterVersion != "" {
				profile.Description += " " + capabilities.AdapterVersion
			}
		}
		profile.Models = advertisedModels(profile.Models, profile.Available, probe.Models)
		profile.Available = true
		profile.ACP = &capabilities
		advertised[index] = profile
	}
	return advertised, failures
}

const (
	MaximumAdvertisedModels     = 32
	MaximumAdvertisedModelBytes = 128
)

var modelIdentifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$`)

func screenedModels(offered []string) []string {
	models := []string{}
	seen := map[string]bool{}
	for _, model := range offered {
		if len(models) >= MaximumAdvertisedModels {
			break
		}
		if seen[model] || len(model) > MaximumAdvertisedModelBytes || !modelIdentifierPattern.MatchString(model) || protocol.LooksSecretLike(model) {
			continue
		}
		seen[model] = true
		models = append(models, model)
	}
	return models
}

func advertisedModels(existing []string, nativeAvailable bool, adapterModels []string) []string {
	if len(adapterModels) == 0 {
		return existing
	}
	base := []string{}
	if nativeAvailable {
		base = append(base, existing...)
	}
	if len(base) == 0 {
		base = append(base, "default")
	}
	merged := make([]string, 0, len(base)+len(adapterModels))
	seen := map[string]bool{}
	for _, model := range append(base, adapterModels...) {
		if !seen[model] && len(merged) < MaximumAdvertisedModels {
			seen[model] = true
			merged = append(merged, model)
		}
	}
	return merged
}
