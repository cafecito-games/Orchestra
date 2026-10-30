package setup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"unicode/utf8"
)

func CurrentPlatform() string {
	return runtime.GOOS + "-" + runtime.GOARCH
}

type OperationKind string

const (
	OperationInstallArchive       OperationKind = "install-archive"
	OperationManualPlacementCheck OperationKind = "manual-placement-check"
)

var OperationKinds = []OperationKind{OperationInstallArchive, OperationManualPlacementCheck}

func (kind OperationKind) Valid() bool {
	return slices.Contains(OperationKinds, kind)
}

func operationKindForDistribution(kind DistributionKind) (OperationKind, error) {
	switch kind {
	case DistributionKindArchive:
		return OperationInstallArchive, nil
	case DistributionKindManual:
		return OperationManualPlacementCheck, nil
	default:
		return "", fmt.Errorf("distribution kind %q has no install operation", kind)
	}
}

type ExpectedCurrentState string

const (
	ExpectedAbsent        ExpectedCurrentState = "absent"
	ExpectedOwnedMatch    ExpectedCurrentState = "owned-match"
	ExpectedUnownedExists ExpectedCurrentState = "unowned-exists"
)

var ExpectedCurrentStates = []ExpectedCurrentState{ExpectedAbsent, ExpectedOwnedMatch, ExpectedUnownedExists}

func (state ExpectedCurrentState) Valid() bool {
	return slices.Contains(ExpectedCurrentStates, state)
}

const postconditionMaximumBytes = 256

type Operation struct {
	Kind                 OperationKind        `json:"kind"`
	Component            ComponentReference   `json:"component"`
	HarnessID            string               `json:"harnessId"`
	TargetPath           string               `json:"targetPath"`
	ExpectedChecksum     string               `json:"expectedChecksum"`
	ExpectedCurrentState ExpectedCurrentState `json:"expectedCurrentState"`
	Source               PlatformDistribution `json:"source"`
}

func (operation Operation) IdempotencyKey() string {
	return string(operation.Kind) + "|" + operation.Component.String() + "|" + operation.TargetPath
}

type Plan struct {
	ManifestVersion string      `json:"manifestVersion"`
	ManifestDigest  string      `json:"manifestDigest"`
	Platform        string      `json:"platform"`
	DataRoot        string      `json:"dataRoot"`
	Operations      []Operation `json:"operations"`
	Postconditions  []string    `json:"postconditions"`
	Digest          string      `json:"digest"`
}

func BuildPlan(manifestBytes []byte, manifest Manifest, platform string, dataRoot string, existingLedger OwnershipLedger) (plan Plan, skipped []ComponentReference, err error) {
	if err := manifest.Validate(); err != nil {
		return Plan{}, nil, err
	}
	if !filepath.IsAbs(dataRoot) {
		return Plan{}, nil, errors.New("data root must be an absolute path")
	}
	if !platformKeyPattern.MatchString(platform) {
		return Plan{}, nil, errors.New("platform must be GOOS-GOARCH shaped")
	}
	manifestSum := sha256.Sum256(manifestBytes)
	plan = Plan{
		ManifestVersion: manifest.ManifestVersion,
		ManifestDigest:  hex.EncodeToString(manifestSum[:]),
		Platform:        platform,
		DataRoot:        dataRoot,
	}
	seenOperations := make(map[string]bool, len(manifest.Components))
	for index := range manifest.Components {
		entry := manifest.Components[index]
		distribution, supported := entry.Platforms[platform]
		if !supported {
			skipped = append(skipped, entry.Reference())
			continue
		}
		targetPath, err := ComponentTargetPath(dataRoot, entry, distribution)
		if err != nil {
			return Plan{}, nil, fmt.Errorf("component at index %d: %w", index, err)
		}
		operationKind, err := operationKindForDistribution(distribution.Kind)
		if err != nil {
			return Plan{}, nil, fmt.Errorf("component at index %d: %w", index, err)
		}
		operation := Operation{
			Kind:                 operationKind,
			Component:            entry.Reference(),
			HarnessID:            entry.HarnessID,
			TargetPath:           targetPath,
			ExpectedCurrentState: observeCurrentState(targetPath, existingLedger),
			Source:               distribution,
		}
		if distribution.Kind == DistributionKindArchive {
			operation.ExpectedChecksum = distribution.SHA256
		}
		if seenOperations[operation.IdempotencyKey()] {
			return Plan{}, nil, fmt.Errorf("component at index %d: duplicate install operation", index)
		}
		seenOperations[operation.IdempotencyKey()] = true
		plan.Operations = append(plan.Operations, operation)
		postcondition := fmt.Sprintf("%s (%s for harness %s) verified at %s", entry.Label, entry.Kind, entry.HarnessID, targetPath)
		plan.Postconditions = append(plan.Postconditions, truncateAtRuneBoundary(postcondition, postconditionMaximumBytes))
	}
	plan.Digest = ComputePlanDigest(plan)
	return plan, skipped, nil
}

func ComputePlanDigest(plan Plan) string {
	digestInput := plan
	digestInput.Digest = ""
	encoded, err := json.Marshal(digestInput)
	if err != nil {
		return ""
	}
	summed := sha256.Sum256(encoded)
	return hex.EncodeToString(summed[:])
}

var componentKindDirectories = map[ComponentKind]string{
	ComponentKindACPAdapter: "adapters",
	ComponentKindHarness:    "harnesses",
}

func componentRelativeTargetPath(entry ComponentManifestEntry, distribution PlatformDistribution) (string, error) {
	directory, known := componentKindDirectories[entry.Kind]
	if !known {
		return "", fmt.Errorf("component kind %q has no install location", entry.Kind)
	}
	return filepath.Join(directory, entry.HarnessID, entry.ID, entry.Version, filepath.FromSlash(distribution.ExecutablePath)), nil
}

func componentVersionDirectory(dataRoot string, entry ComponentManifestEntry, version string) (string, error) {
	directory, known := componentKindDirectories[entry.Kind]
	if !known {
		return "", fmt.Errorf("component kind %q has no install location", entry.Kind)
	}
	return filepath.Join(dataRoot, directory, entry.HarnessID, entry.ID, version), nil
}

func ComponentTargetPath(dataRoot string, entry ComponentManifestEntry, distribution PlatformDistribution) (string, error) {
	relative, err := componentRelativeTargetPath(entry, distribution)
	if err != nil {
		return "", err
	}
	return filepath.Join(dataRoot, relative), nil
}

func observeCurrentState(targetPath string, ledger OwnershipLedger) ExpectedCurrentState {
	information, err := os.Lstat(targetPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return ExpectedAbsent
		}
		return ExpectedUnownedExists
	}
	if !information.Mode().IsRegular() {
		return ExpectedUnownedExists
	}
	record, owned := ledger.RecordFor(targetPath)
	if !owned {
		return ExpectedUnownedExists
	}
	digest, err := fileChecksum(targetPath)
	if err != nil || !strings.EqualFold(digest, record.ContentSHA256) {
		return ExpectedUnownedExists
	}
	return ExpectedOwnedMatch
}

func fileChecksum(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func truncateAtRuneBoundary(value string, maximumBytes int) string {
	if len(value) <= maximumBytes {
		return value
	}
	truncated := value[:maximumBytes]
	for len(truncated) > 0 {
		character, size := utf8.DecodeLastRuneInString(truncated)
		if character != utf8.RuneError || size > 1 {
			break
		}
		truncated = truncated[:len(truncated)-1]
	}
	return truncated
}
