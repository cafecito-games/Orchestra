package setup

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

// ManifestVersion identifies the schema generation of the managed component manifest.
const ManifestVersion = "2"

//go:embed manifest/components.json
var embeddedManifest []byte

// ComponentKind is the closed vocabulary of things Orchestra setup can own.
type ComponentKind string

const (
	ComponentKindHarness    ComponentKind = "harness"
	ComponentKindACPAdapter ComponentKind = "acp-adapter"
)

var ComponentKinds = []ComponentKind{ComponentKindHarness, ComponentKindACPAdapter}

func (kind ComponentKind) Valid() bool {
	return slices.Contains(ComponentKinds, kind)
}

type componentKindBehavior struct {
	harnessIDIsOwnIdentity bool
	consumesLaunchTemplate bool
	hasHarnessOfItsOwn     bool
}

var componentKindBehaviors = map[ComponentKind]componentKindBehavior{
	ComponentKindHarness:    {harnessIDIsOwnIdentity: true, consumesLaunchTemplate: false, hasHarnessOfItsOwn: true},
	ComponentKindACPAdapter: {harnessIDIsOwnIdentity: false, consumesLaunchTemplate: true, hasHarnessOfItsOwn: true},
}

func (kind ComponentKind) HasHarnessOfItsOwn() bool {
	return componentKindBehaviors[kind].hasHarnessOfItsOwn
}

// ComponentReference is the identity triple every managed component is addressed by.
type ComponentReference struct {
	Kind    ComponentKind `json:"kind"`
	ID      string        `json:"id"`
	Version string        `json:"version"`
}

func (reference ComponentReference) Validate() error {
	if !reference.Kind.Valid() {
		return errors.New("component kind is unknown")
	}
	if reference.ID == "" || !protocol.LabelOrAcceleratorPattern.MatchString(reference.ID) {
		return errors.New("component id is not kebab-case")
	}
	if !protocol.IsNormalizedVersion(reference.Version) {
		return errors.New("component version is not a normalized dotted version")
	}
	return nil
}

func (reference ComponentReference) String() string {
	return string(reference.Kind) + "/" + reference.ID + "@" + reference.Version
}

type DistributionKind string

const (
	DistributionKindArchive DistributionKind = "archive"
	DistributionKindManual  DistributionKind = "manual"
)

var DistributionKinds = []DistributionKind{DistributionKindArchive, DistributionKindManual}

func (kind DistributionKind) Valid() bool {
	return slices.Contains(DistributionKinds, kind)
}

type PlatformDistribution struct {
	Kind           DistributionKind `json:"kind"`
	URL            string           `json:"url,omitempty"`
	SHA256         string           `json:"sha256,omitempty"`
	SizeBytes      int64            `json:"sizeBytes,omitempty"`
	ExecutablePath string           `json:"executablePath"`
}

type LaunchTemplate struct {
	Arguments   []string `json:"arguments,omitempty"`
	Environment []string `json:"environment,omitempty"`
}

type ComponentManifestEntry struct {
	ID          string                          `json:"id"`
	Kind        ComponentKind                   `json:"kind"`
	HarnessID   string                          `json:"harnessId"`
	Provider    string                          `json:"provider"`
	Label       string                          `json:"label"`
	Version     string                          `json:"version"`
	Platforms   map[string]PlatformDistribution `json:"platforms"`
	Launch      LaunchTemplate                  `json:"launch"`
	AuthDocsURL string                          `json:"authDocsUrl,omitempty"`
}

func (entry ComponentManifestEntry) Reference() ComponentReference {
	return ComponentReference{Kind: entry.Kind, ID: entry.ID, Version: entry.Version}
}

type Manifest struct {
	ManifestVersion string                   `json:"manifestVersion"`
	Components      []ComponentManifestEntry `json:"components"`
}

func (manifest Manifest) ComponentsOfKind(kind ComponentKind) []ComponentManifestEntry {
	matching := make([]ComponentManifestEntry, 0, len(manifest.Components))
	for _, entry := range manifest.Components {
		if entry.Kind == kind {
			matching = append(matching, entry)
		}
	}
	return matching
}

var ChecksumPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

var platformKeyPattern = protocol.LabelOrAcceleratorPattern

func ParseManifest(data []byte) (Manifest, error) {
	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode component manifest: %w", err)
	}
	if decoder.More() {
		return Manifest{}, errors.New("decode component manifest: trailing data after the manifest object")
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func LoadDefaultManifest() (Manifest, error) {
	manifest, err := ParseManifest(embeddedManifest)
	if err != nil {
		return Manifest{}, fmt.Errorf("parse embedded component manifest: %w", err)
	}
	return manifest, nil
}

func DefaultManifestBytes() []byte {
	return embeddedManifest
}

func (manifest Manifest) Validate() error {
	for index := range manifest.Components {
		entry := &manifest.Components[index]
		if protocol.LooksSecretLike(entry.Label) {
			return fmt.Errorf("component at index %d: label looks secret-like", index)
		}
		if protocol.LooksSecretLike(entry.AuthDocsURL) {
			return fmt.Errorf("component at index %d: authDocsUrl looks secret-like", index)
		}
		if slices.ContainsFunc(entry.Launch.Environment, protocol.LooksSecretLike) {
			return fmt.Errorf("component at index %d: launch environment entry looks secret-like", index)
		}
		for _, distribution := range entry.Platforms {
			if protocol.LooksSecretLike(distribution.URL) {
				return fmt.Errorf("component at index %d: platform distribution url looks secret-like", index)
			}
		}
	}
	if manifest.ManifestVersion != ManifestVersion {
		return fmt.Errorf("component manifest schema generation is unknown; only generation %q is supported", ManifestVersion)
	}
	if len(manifest.Components) == 0 {
		return errors.New("component manifest declares no components")
	}
	seenIDs := make(map[string]bool, len(manifest.Components))
	providerByHarness := make(map[string]string, len(manifest.Components))
	seenTargets := make(map[string]bool, len(manifest.Components))
	for index := range manifest.Components {
		entry := manifest.Components[index]
		if !entry.Kind.Valid() {
			return fmt.Errorf("component at index %d: kind is unknown", index)
		}
		if entry.ID == "" || !protocol.LabelOrAcceleratorPattern.MatchString(entry.ID) {
			return fmt.Errorf("component at index %d: id is not kebab-case", index)
		}
		if entry.HarnessID == "" || !protocol.LabelOrAcceleratorPattern.MatchString(entry.HarnessID) {
			return fmt.Errorf("component at index %d: harnessId is not kebab-case", index)
		}
		behavior, described := componentKindBehaviors[entry.Kind]
		if !described {
			return fmt.Errorf("component at index %d: kind has no schema rules", index)
		}
		if behavior.harnessIDIsOwnIdentity && entry.HarnessID != entry.ID {
			return fmt.Errorf("component at index %d: a %s component's harnessId must equal its id", index, entry.Kind)
		}
		if !behavior.consumesLaunchTemplate && (len(entry.Launch.Arguments) > 0 || len(entry.Launch.Environment) > 0) {
			return fmt.Errorf("component at index %d: a %s component's launch template must be empty", index, entry.Kind)
		}
		if entry.Provider == "" || !protocol.LabelOrAcceleratorPattern.MatchString(entry.Provider) {
			return fmt.Errorf("component at index %d: provider is not kebab-case", index)
		}
		if seenIDs[entry.ID] {
			return fmt.Errorf("component at index %d: duplicate id", index)
		}
		seenIDs[entry.ID] = true
		if existing, seen := providerByHarness[entry.HarnessID]; seen && existing != entry.Provider {
			return fmt.Errorf("component at index %d: harnessId is already associated with a different provider", index)
		}
		providerByHarness[entry.HarnessID] = entry.Provider
		if entry.Label == "" || len(entry.Label) > 128 {
			return fmt.Errorf("component at index %d: label is empty or exceeds 128 bytes", index)
		}
		if !protocol.IsNormalizedVersion(entry.Version) {
			return fmt.Errorf("component at index %d: version is not a normalized dotted version", index)
		}
		if err := entry.Reference().Validate(); err != nil {
			return fmt.Errorf("component at index %d: %w", index, err)
		}
		if len(entry.Platforms) == 0 {
			return fmt.Errorf("component at index %d: platforms is empty", index)
		}
		for platformKey, distribution := range entry.Platforms {
			if !platformKeyPattern.MatchString(platformKey) {
				return fmt.Errorf("component at index %d: platform key is not GOOS-GOARCH shaped", index)
			}
			if err := validatePlatformDistribution(distribution); err != nil {
				return fmt.Errorf("component at index %d: %w", index, err)
			}
			relative, err := componentRelativeTargetPath(entry, distribution)
			if err != nil {
				return fmt.Errorf("component at index %d: %w", index, err)
			}
			targetKey := platformKey + "\x00" + relative
			if seenTargets[targetKey] {
				return fmt.Errorf("component at index %d: duplicate install target path for a platform", index)
			}
			seenTargets[targetKey] = true
		}
	}
	return nil
}

func validatePlatformDistribution(distribution PlatformDistribution) error {
	if !distribution.Kind.Valid() {
		return fmt.Errorf("platform distribution kind must be %q or %q", DistributionKindArchive, DistributionKindManual)
	}
	if err := validateExecutablePath(distribution.ExecutablePath); err != nil {
		return err
	}
	switch distribution.Kind {
	case DistributionKindArchive:
		if !strings.HasPrefix(distribution.URL, "https://") {
			return errors.New("archive platform distribution url must use https")
		}
		if !ChecksumPattern.MatchString(distribution.SHA256) {
			return errors.New("archive platform distribution sha256 must be 64 lowercase hex characters")
		}
		if distribution.SizeBytes <= 0 {
			return errors.New("archive platform distribution sizeBytes must be positive")
		}
		return nil
	case DistributionKindManual:
		if distribution.URL != "" || distribution.SHA256 != "" {
			return errors.New("manual platform distribution must not carry a url or sha256")
		}
		return nil
	default:
		return fmt.Errorf("platform distribution kind %q has no validation rule", distribution.Kind)
	}
}

func validateExecutablePath(executablePath string) error {
	if executablePath == "" {
		return errors.New("platform distribution executablePath is empty")
	}
	if strings.ContainsRune(executablePath, '\\') {
		return errors.New("platform distribution executablePath must use forward slashes")
	}
	if strings.HasPrefix(executablePath, "/") || filepath.IsAbs(executablePath) {
		return errors.New("platform distribution executablePath must be relative")
	}
	if slices.Contains(strings.Split(executablePath, "/"), "..") {
		return errors.New("platform distribution executablePath must not traverse upward")
	}
	return nil
}
