package harness

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/mcpserver"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const (
	TransportNative = protocol.TransportNativeCLI
	TransportACP    = protocol.TransportACP
)

var ErrDriverUnavailable = errors.New("harness driver unavailable")

type PermissionHandler = acp.PermissionHandler

type Invocation struct {
	Run               protocol.Run
	SystemPrompt      string
	Workspace         string
	MCP               mcpserver.Configuration
	Output            func(string)
	Events            func(protocol.HarnessEvent)
	Permission        PermissionHandler
	FallbackTransport string
	Started           func(protocol.RunTransportSelection)
	Resume            *SessionResume
	Session           func(EstablishedSession)
	ProviderSession   func(string)

	begin          func(transportDetails)
	approvalPolicy string
}

type SessionResume struct {
	ProviderSessionID string
	Prompt            string
}

type EstablishedSession struct {
	ProviderSessionID string
	Resumed           bool
}

type transportDetails struct {
	capabilities *protocol.AcpAgentCapabilities
}

func (invocation Invocation) announce(details transportDetails) {
	if invocation.begin != nil {
		invocation.begin(details)
	}
}

type Driver interface {
	Execute(operationContext context.Context, invocation Invocation) (string, error)
}

type Runner struct {
	nativeProfiles     []protocol.HarnessProfile
	advertisedProfiles []protocol.HarnessProfile
	native             Driver
	acp                *ACPDriver
	nativeFallback     map[string]bool
	approvalPolicies   ApprovalPolicies
	now                func() time.Time
	managedHarnesses   map[string]ManagedHarness
	usageMutex         sync.Mutex
	executableUsage    map[string]int
}

func NewRunner(profiles []protocol.HarnessProfile) *Runner {
	runner := &Runner{
		nativeProfiles:   profiles,
		nativeFallback:   map[string]bool{},
		now:              time.Now,
		managedHarnesses: map[string]ManagedHarness{},
		executableUsage:  map[string]int{},
	}
	runner.native = nativeDriver{runner: runner}
	return runner
}

func (runner *Runner) WithManagedHarnesses(managed map[string]ManagedHarness) *Runner {
	runner.managedHarnesses = map[string]ManagedHarness{}
	maps.Copy(runner.managedHarnesses, managed)
	return runner
}

func (runner *Runner) verifyManagedHarness(harnessID string) error {
	entry, managed := runner.managedHarnesses[harnessID]
	if !managed {
		return nil
	}
	if entry.Verify == nil {
		return fmt.Errorf("managed harness %s has no verification function", harnessID)
	}
	if err := entry.Verify(); err != nil {
		return fmt.Errorf("managed harness %s failed verification: %w", harnessID, err)
	}
	return nil
}

func (runner *Runner) holdExecutable(path string) func() {
	if path == "" {
		return func() {}
	}
	runner.usageMutex.Lock()
	runner.executableUsage[path]++
	runner.usageMutex.Unlock()
	released := false
	return func() {
		runner.usageMutex.Lock()
		defer runner.usageMutex.Unlock()
		if released {
			return
		}
		released = true
		if runner.executableUsage[path] <= 1 {
			delete(runner.executableUsage, path)
			return
		}
		runner.executableUsage[path]--
	}
}

func (runner *Runner) ExecutableInUse(path string) bool {
	if path == "" {
		return false
	}
	runner.usageMutex.Lock()
	defer runner.usageMutex.Unlock()
	return runner.executableUsage[path] > 0
}

func (runner *Runner) WithAdvertisedProfiles(profiles []protocol.HarnessProfile) *Runner {
	runner.advertisedProfiles = profiles
	return runner
}

func (runner *Runner) WithACP(driver *ACPDriver) *Runner {
	runner.acp = driver
	return runner
}

func (runner *Runner) WithNativeFallback(harnessIDs ...string) *Runner {
	for _, harnessID := range harnessIDs {
		runner.nativeFallback[harnessID] = true
	}
	return runner
}

func (runner *Runner) WithApprovalPolicies(policies ApprovalPolicies) *Runner {
	runner.approvalPolicies = ApprovalPolicies{}
	maps.Copy(runner.approvalPolicies, policies)
	return runner
}

func (runner *Runner) ApprovalPolicy(harnessID string) string {
	return runner.approvalPolicies.For(harnessID)
}

func (runner *Runner) Admit(harnessID, transport, fallbackTransport string) error {
	switch transport {
	case "":
		return nil
	case TransportNative:
		return runner.nativeTransportAdmitted(harnessID)
	case TransportACP:
		err := runner.acpAvailable(harnessID)
		if err == nil || runner.fallbackPermitted(harnessID, fallbackTransport) {
			return nil
		}
		return err
	default:
		return fmt.Errorf("%w: unknown harness transport", ErrDriverUnavailable)
	}
}

func (runner *Runner) nativeTransportAdmitted(harnessID string) error {
	profile, available := runner.advertisedProfile(harnessID)
	if !available {
		return fmt.Errorf("%w: harness %s is not available on this slave node", ErrDriverUnavailable, harnessID)
	}
	if len(profile.Transports) > 0 && !slices.Contains(profile.Transports, TransportNative) {
		return fmt.Errorf("%w: harness %s does not advertise the %s transport", ErrDriverUnavailable, harnessID, TransportNative)
	}
	return nil
}

func (runner *Runner) AdmitResume(harnessID, transport string) error {
	switch transport {
	case "", TransportNative:
		if err := runner.nativeTransportAdmitted(harnessID); err != nil {
			return err
		}
		if _, available := runner.profile(harnessID); !available {
			return fmt.Errorf("%w: native harness %s is unavailable", ErrDriverUnavailable, harnessID)
		}
		if !slices.Contains(protocol.HarnessIDs, harnessID) {
			return fmt.Errorf("%w: harness %s does not support native session resume", ErrDriverUnavailable, harnessID)
		}
		return nil
	case TransportACP:
		if err := runner.acpAvailable(harnessID); err != nil {
			return err
		}
		return runner.acp.SupportsResume(harnessID)
	default:
		return fmt.Errorf("%w: unknown harness transport", ErrDriverUnavailable)
	}
}

func (runner *Runner) AdmitModel(harnessID, model string) error {
	profile, available := runner.advertisedProfile(harnessID)
	if !available {
		return fmt.Errorf("harness %s is not installed or did not pass its version check", harnessID)
	}
	if len(profile.Models) > 0 && model != "" && model != "default" && !slices.Contains(profile.Models, model) {
		return fmt.Errorf("model %s is not advertised by harness %s on this slave node", model, harnessID)
	}
	return nil
}

func (runner *Runner) Execute(operationContext context.Context, invocation Invocation) (string, error) {
	if invocation.Output == nil {
		invocation.Output = func(string) {}
	}
	invocation.approvalPolicy = runner.approvalPolicies.For(invocation.Run.HarnessID)
	switch invocation.Run.Transport {
	case "", TransportNative:
		if err := runner.nativeTransportAdmitted(invocation.Run.HarnessID); err != nil {
			return "", err
		}
		return runner.native.Execute(operationContext, runner.beginning(invocation, protocol.RunTransportSelection{RequestedTransport: TransportNative, SelectedTransport: TransportNative}, nil))
	case TransportACP:
		return runner.executeACP(operationContext, invocation)
	default:
		return "", fmt.Errorf("%w: unknown harness transport", ErrDriverUnavailable)
	}
}

func (runner *Runner) executeACP(operationContext context.Context, invocation Invocation) (string, error) {
	harnessID := invocation.Run.HarnessID
	permitted := invocation.Resume == nil && runner.fallbackPermitted(harnessID, invocation.FallbackTransport)
	if err := runner.acpAvailable(harnessID); err != nil {
		if permitted {
			return runner.fallBack(operationContext, invocation, protocol.FallbackACPAdapterUnavailable)
		}
		return "", err
	}
	var started atomic.Bool
	selection := protocol.RunTransportSelection{RequestedTransport: TransportACP, SelectedTransport: TransportACP, Adapter: runner.acp.provenance(harnessID)}
	releaseAdapter := runner.holdExecutable(runner.acp.AdapterBinary(harnessID))
	result, err := runner.acp.Execute(operationContext, runner.beginning(invocation, selection, &started))
	releaseAdapter()
	if err == nil || started.Load() || !permitted || operationContext.Err() != nil {
		return result, err
	}
	reason := FallbackReason(err)
	if reason == "" {
		return "", err
	}
	return runner.fallBack(operationContext, invocation, reason)
}

func (runner *Runner) fallBack(operationContext context.Context, invocation Invocation, reason string) (string, error) {
	if invocation.Events != nil {
		invocation.Events(protocol.HarnessEvent{
			Type:    "warning",
			RunID:   invocation.Run.ID,
			At:      runner.now().UTC().Format(time.RFC3339Nano),
			Code:    protocol.WarningTransportNativeFallback,
			Message: "ACP transport was not usable before the prompt was sent (" + reason + "); running through the native CLI as the dispatch and operator permit",
		})
	}
	selection := protocol.RunTransportSelection{RequestedTransport: TransportACP, SelectedTransport: TransportNative, FallbackReason: reason}
	if runner.acp != nil {
		selection.Adapter = runner.acp.provenance(invocation.Run.HarnessID)
	}
	return runner.native.Execute(operationContext, runner.beginning(invocation, selection, nil))
}

func (runner *Runner) beginning(invocation Invocation, selection protocol.RunTransportSelection, started *atomic.Bool) Invocation {
	selection.ApprovalPolicy = reportedApprovalPolicy(invocation.approvalPolicy)
	if selection.SelectedTransport == TransportNative {
		if profile, available := runner.profile(invocation.Run.HarnessID); available {
			selection.HarnessVersion = protocol.ExtractNormalizedVersion(profile.Description)
		}
	}
	var once sync.Once
	report := invocation.Started
	invocation.begin = func(details transportDetails) {
		once.Do(func() {
			if started != nil {
				started.Store(true)
			}
			if details.capabilities != nil {
				capabilities := *details.capabilities
				selection.ACP = &capabilities
			}
			if report != nil {
				report(selection)
			}
		})
	}
	return invocation
}

func (runner *Runner) acpAvailable(harnessID string) error {
	if runner.acp == nil {
		return fmt.Errorf("%w: no ACP adapter is configured on this slave node", ErrDriverUnavailable)
	}
	return runner.acp.Available(harnessID)
}

func (runner *Runner) fallbackPermitted(harnessID, fallbackTransport string) bool {
	if fallbackTransport != TransportNative || !runner.nativeFallback[harnessID] {
		return false
	}
	_, available := runner.profile(harnessID)
	return available
}
