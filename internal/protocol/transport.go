package protocol

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"time"
)

const (
	TransportNativeCLI = "native-cli"
	TransportACP       = "acp-v1"
)

const (
	HarnessClaudeCLI      = "claude-cli"
	HarnessCodexCLI       = "codex-cli"
	HarnessOpenCodeCLI    = "opencode-cli"
	HarnessAntigravityCLI = "antigravity-cli"
)

const (
	FallbackACPAdapterUnavailable   = "acp-adapter-unavailable"
	FallbackACPProtocolIncompatible = "acp-protocol-incompatible"
	FallbackACPCapabilityMissing    = "acp-capability-missing"
	FallbackACPMCPUnavailable       = "acp-mcp-unavailable"
)

const (
	ACPAdapterSourceSetupLedger   = "setup-ledger"
	ACPAdapterSourceAdministrator = "administrator-override"
	ACPAdapterSourceBuiltIn       = "built-in"
	ACPAdapterSourceExternal      = "external"
)

const (
	ApprovalPolicyManual = "manual"
	ApprovalPolicyAuto   = "auto"
	ApprovalPolicyBypass = "bypass"
)

const (
	WarningTransportNativeFallback = "transport-native-fallback"
	ACPAdapterNameMaximumBytes     = 128
)

var (
	HarnessTransports        = []string{TransportNativeCLI, TransportACP}
	HarnessIDs               = []string{HarnessClaudeCLI, HarnessCodexCLI, HarnessOpenCodeCLI, HarnessAntigravityCLI}
	TransportFallbackReasons = []string{FallbackACPAdapterUnavailable, FallbackACPProtocolIncompatible, FallbackACPCapabilityMissing, FallbackACPMCPUnavailable}
	ACPAdapterSources        = []string{ACPAdapterSourceSetupLedger, ACPAdapterSourceAdministrator, ACPAdapterSourceBuiltIn, ACPAdapterSourceExternal}
	ApprovalPolicies         = []string{ApprovalPolicyManual, ApprovalPolicyAuto, ApprovalPolicyBypass}
	NodeKinds                = []string{"local", "home-server", "cloud"}
	NodeStatuses             = []string{"online", "offline", "busy", "updating"}
	NodeUpdateStatuses       = []string{"queued", "downloading", "draining", "restarting", "succeeded", "up-to-date", "skipped", "failed"}
	AuthModes                = []string{"local-subscription", "local-account", "api", "none"}
)

type AcpPromptCapabilities struct {
	Image           bool `json:"image"`
	Audio           bool `json:"audio"`
	EmbeddedContext bool `json:"embeddedContext"`
}

type AcpMcpCapabilities struct {
	HTTP bool `json:"http"`
	SSE  bool `json:"sse"`
}

type AcpAgentCapabilities struct {
	ProtocolVersion int                   `json:"protocolVersion"`
	LoadSession     bool                  `json:"loadSession"`
	ResumeSession   bool                  `json:"resumeSession"`
	Prompt          AcpPromptCapabilities `json:"prompt"`
	Mcp             AcpMcpCapabilities    `json:"mcp"`
	AdapterName     string                `json:"adapterName,omitempty"`
	AdapterVersion  string                `json:"adapterVersion,omitempty"`
}

func (capabilities AcpAgentCapabilities) Validate() error {
	if capabilities.ProtocolVersion != 1 {
		return errors.New("ACP capabilities name an unsupported protocol version")
	}
	if len(capabilities.AdapterName) > ACPAdapterNameMaximumBytes || LooksSecretLike(capabilities.AdapterName) {
		return errors.New("ACP adapter name is too long or looks secret-like")
	}
	if capabilities.AdapterVersion != "" && !IsNormalizedVersion(capabilities.AdapterVersion) {
		return errors.New("ACP adapter version is not a normalized version")
	}
	return nil
}

func (capabilities *AcpAgentCapabilities) SupportsResume() bool {
	return capabilities != nil && (capabilities.ResumeSession || capabilities.LoadSession)
}

type ACPAdapterProvenance struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	Source  string `json:"source"`
}

type RunTransportSelection struct {
	RequestedTransport string                `json:"requestedTransport"`
	SelectedTransport  string                `json:"selectedTransport"`
	FallbackReason     string                `json:"fallbackReason,omitempty"`
	HarnessVersion     string                `json:"harnessVersion,omitempty"`
	Adapter            *ACPAdapterProvenance `json:"adapter,omitempty"`
	ACP                *AcpAgentCapabilities `json:"acp,omitempty"`
	ApprovalPolicy     string                `json:"approvalPolicy,omitempty"`
}

func (selection RunTransportSelection) Validate() error {
	if !slices.Contains(HarnessTransports, selection.RequestedTransport) || !slices.Contains(HarnessTransports, selection.SelectedTransport) {
		return errors.New("transport selection names an unknown transport")
	}
	if selection.SelectedTransport == selection.RequestedTransport {
		if selection.FallbackReason != "" {
			return errors.New("transport selection has a fallback reason without a fallback")
		}
	} else if selection.RequestedTransport != TransportACP || selection.SelectedTransport != TransportNativeCLI || !slices.Contains(TransportFallbackReasons, selection.FallbackReason) {
		return errors.New("transport selection falls back other than from acp-v1 to native-cli for a known reason")
	}
	if selection.HarnessVersion != "" && !IsNormalizedVersion(selection.HarnessVersion) {
		return errors.New("transport selection harness version is not a normalized version")
	}
	if adapter := selection.Adapter; adapter != nil {
		if len(adapter.ID) > LabelOrAcceleratorMaximumBytes || !LabelOrAcceleratorPattern.MatchString(adapter.ID) || !IsNormalizedVersion(adapter.Version) || !slices.Contains(ACPAdapterSources, adapter.Source) {
			return errors.New("transport selection adapter provenance is malformed")
		}
	}
	if selection.ApprovalPolicy != "" && !slices.Contains(ApprovalPolicies, selection.ApprovalPolicy) {
		return errors.New("transport selection names an unknown approval policy")
	}
	if selection.ACP != nil {
		if selection.SelectedTransport != TransportACP {
			return errors.New("transport selection carries ACP capabilities for a native run")
		}
		if err := selection.ACP.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type HarnessProfile struct {
	ID             string                `json:"id"`
	Label          string                `json:"label"`
	Description    string                `json:"description"`
	Available      bool                  `json:"available"`
	Binary         string                `json:"binary,omitempty"`
	AuthMode       string                `json:"authMode"`
	Models         []string              `json:"models"`
	Transports     []string              `json:"transports,omitempty"`
	ACP            *AcpAgentCapabilities `json:"acp,omitempty"`
	ApprovalPolicy string                `json:"approvalPolicy,omitempty"`
}

func (profile HarnessProfile) Validate() error {
	if !slices.Contains(HarnessIDs, profile.ID) || !IsIdentifier(profile.Label) || !IsBoundedString(profile.Description, DiagnosticMaximumBytes) {
		return errors.New("harness profile identity or description is malformed")
	}
	if !slices.Contains(AuthModes, profile.AuthMode) {
		return errors.New("harness profile authMode is unknown")
	}
	if len(profile.Models) > 64 {
		return errors.New("harness profile models exceed bound")
	}
	for _, model := range profile.Models {
		if !IsIdentifier(model) {
			return errors.New("harness profile model is malformed")
		}
	}
	for _, transport := range profile.Transports {
		if !slices.Contains(HarnessTransports, transport) {
			return errors.New("harness profile transport is unknown")
		}
	}
	if profile.ACP != nil {
		if err := profile.ACP.Validate(); err != nil {
			return err
		}
	}
	if profile.ApprovalPolicy != "" && !slices.Contains(ApprovalPolicies, profile.ApprovalPolicy) {
		return errors.New("harness profile approvalPolicy is unknown")
	}
	return nil
}

type ComputeNode struct {
	ID                 string           `json:"id"`
	Name               string           `json:"name"`
	Kind               string           `json:"kind"`
	Platform           string           `json:"platform"`
	Status             string           `json:"status"`
	LastSeen           string           `json:"lastSeen"`
	ActiveRuns         int              `json:"activeRuns"`
	Concurrency        int              `json:"concurrency"`
	InstanceCapacity   *int             `json:"instanceCapacity,omitempty"`
	ActiveInstances    *int             `json:"activeInstances,omitempty"`
	WorkspaceRoots     []string         `json:"workspaceRoots"`
	Version            string           `json:"version"`
	SelfUpdateDisabled bool             `json:"selfUpdateDisabled,omitempty"`
	Harnesses          []HarnessProfile `json:"harnesses"`
}

func IsActiveNodeUpdateStatus(status string) bool {
	switch status {
	case "queued", "downloading", "draining", "restarting":
		return true
	default:
		return false
	}
}

type NodeUpdateDirective struct {
	UpdateID        string `json:"updateId"`
	TargetVersion   string `json:"targetVersion"`
	ProtocolVersion string `json:"protocolVersion"`
	DownloadPath    string `json:"downloadPath"`
	ExpectedSHA256  string `json:"expectedSha256"`
	SizeBytes       int64  `json:"sizeBytes"`
	Force           bool   `json:"force,omitempty"`
}

func (directive NodeUpdateDirective) Validate() error {
	if !IsIdentifier(directive.UpdateID) || !IsSafeReleaseVersion(directive.TargetVersion) || !IsIdentifier(directive.ProtocolVersion) {
		return errors.New("node update directive identity or version is malformed")
	}
	if directive.DownloadPath == "" || !IsBoundedString(directive.DownloadPath, 1024) || LooksSecretLike(directive.DownloadPath) {
		return errors.New("node update directive downloadPath is malformed")
	}
	if len(directive.ExpectedSHA256) != 64 || !isLowerHexDigest(directive.ExpectedSHA256) {
		return errors.New("node update directive expectedSha256 must be 64 lowercase hex characters")
	}
	if directive.SizeBytes <= 0 {
		return errors.New("node update directive sizeBytes must be positive")
	}
	return nil
}

type NodeUpdateReport struct {
	UpdateID       string `json:"updateId"`
	NodeID         string `json:"nodeId"`
	Status         string `json:"status"`
	CurrentVersion string `json:"currentVersion,omitempty"`
	TargetVersion  string `json:"targetVersion,omitempty"`
	Message        string `json:"message,omitempty"`
	Error          string `json:"error,omitempty"`
	ObservedAt     string `json:"observedAt"`
}

func (report NodeUpdateReport) Validate() error {
	if !IsIdentifier(report.UpdateID) || !IsIdentifier(report.NodeID) {
		return errors.New("node update report identity is malformed")
	}
	if !slices.Contains(NodeUpdateStatuses, report.Status) {
		return errors.New("node update report status is unknown")
	}
	if report.CurrentVersion != "" && !IsIdentifier(report.CurrentVersion) {
		return errors.New("node update report currentVersion is malformed")
	}
	if report.TargetVersion != "" && !IsSafeReleaseVersion(report.TargetVersion) {
		return errors.New("node update report targetVersion is malformed")
	}
	if !IsBoundedString(report.Message, DiagnosticMaximumBytes) || !IsBoundedString(report.Error, DiagnosticMaximumBytes) {
		return errors.New("node update report diagnostic exceeds bound")
	}
	if !IsTimestamp(report.ObservedAt) {
		return errors.New("node update report observedAt is malformed")
	}
	return nil
}

type NodeUpdateState struct {
	UpdateID        string `json:"updateId"`
	NodeID          string `json:"nodeId"`
	PreviousVersion string `json:"previousVersion"`
	CurrentVersion  string `json:"currentVersion"`
	TargetVersion   string `json:"targetVersion"`
	Status          string `json:"status"`
	Message         string `json:"message,omitempty"`
	Error           string `json:"error,omitempty"`
	RequestedAt     string `json:"requestedAt"`
	UpdatedAt       string `json:"updatedAt"`
	CompletedAt     string `json:"completedAt,omitempty"`
}

func isLowerHexDigest(value string) bool {
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return value != ""
}

func (node ComputeNode) Validate() error {
	if !IsIdentifier(node.ID) || !IsIdentifier(node.Name) || !IsIdentifier(node.Platform) || !IsIdentifier(node.Version) {
		return errors.New("compute node identity is malformed")
	}
	if !slices.Contains(NodeKinds, node.Kind) || !slices.Contains(NodeStatuses, node.Status) {
		return errors.New("compute node kind or status is unknown")
	}
	if !IsTimestamp(node.LastSeen) || node.ActiveRuns < 0 || node.Concurrency < 0 || node.Concurrency > 65535 {
		return errors.New("compute node timing or capacity is malformed")
	}
	if node.InstanceCapacity != nil && (*node.InstanceCapacity < 0 || *node.InstanceCapacity > 65535) {
		return errors.New("compute node instanceCapacity is out of bounds")
	}
	if node.ActiveInstances != nil && (*node.ActiveInstances < 0 || *node.ActiveInstances > 65535) {
		return errors.New("compute node activeInstances is out of bounds")
	}
	if node.InstanceCapacity != nil && node.ActiveInstances != nil && *node.ActiveInstances > *node.InstanceCapacity {
		return errors.New("compute node activeInstances exceeds instanceCapacity")
	}
	if len(node.WorkspaceRoots) > 64 {
		return errors.New("compute node workspaceRoots exceed bound")
	}
	for _, root := range node.WorkspaceRoots {
		if !IsCanonicalWorkspacePath(root) {
			return errors.New("compute node workspaceRoot is not a canonical path")
		}
	}
	seenHarnesses := make(map[string]bool, len(node.Harnesses))
	for _, profile := range node.Harnesses {
		if err := profile.Validate(); err != nil {
			return err
		}
		if seenHarnesses[profile.ID] {
			return errors.New("compute node harnesses contain duplicate harness ID")
		}
		seenHarnesses[profile.ID] = true
	}
	return nil
}

func BuildHTTPClient(caCertPath string, insecureSkipVerify bool, timeout time.Duration) (*http.Client, error) {
	client := &http.Client{Timeout: timeout}
	if caCertPath == "" && !insecureSkipVerify {
		return client, nil
	}
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: insecureSkipVerify,
	}
	if caCertPath != "" {
		certBytes, err := os.ReadFile(caCertPath)
		if err != nil {
			return nil, fmt.Errorf("read TLS CA certificate %s: %w", caCertPath, err)
		}
		pool, poolErr := x509.SystemCertPool()
		if poolErr != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(certBytes) {
			return nil, fmt.Errorf("parse TLS CA certificate %s: no valid PEM certificates found", caCertPath)
		}
		tlsConfig.RootCAs = pool
	}
	client.Transport = &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	return client, nil
}
