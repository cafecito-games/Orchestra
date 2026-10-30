package harness

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const (
	mcpServerName              = "orchestra_hub"
	defaultShutdownGracePeriod = 5 * time.Second
	defaultMCPConnectTimeout   = 30 * time.Second
	pipeReleaseDelay           = 3 * time.Second
)

var errShutdownAbandoned = errors.New("ACP adapter stdio did not close after its process tree was terminated")

var ErrMCPUnavailable = errors.New("ACP adapter did not connect to the Orchestra MCP server before the prompt")

const (
	warningAdapterExit     = "acp-adapter-exit"
	warningForcedShutdown  = "acp-adapter-forced-shutdown"
	adapterStderrSeparator = "; adapter stderr: "
)

var environmentSecrets = []string{"ORCHESTRA_TOKEN", "ORCHESTRA_MCP_TOKEN"}

type ACPAdapter struct {
	Binary      string
	Arguments   []string
	Environment []string
	ID          string
	Version     string
	Source      string
	Verify      func() error
}

type ACPDriverOptions struct {
	Adapters            map[string]ACPAdapter
	Providers           map[string]ACPProvider
	NativeBinaries      map[string]string
	ClientVersion       string
	RequestTimeout      time.Duration
	PermissionTimeout   time.Duration
	CancelGracePeriod   time.Duration
	ShutdownGracePeriod time.Duration
	MCPConnectTimeout   time.Duration
}

type ACPDriver struct {
	options  ACPDriverOptions
	mutex    sync.Mutex
	disabled map[string]error
	probed   map[string]protocol.AcpAgentCapabilities
}

func NewACPDriver(options ACPDriverOptions) *ACPDriver {
	if options.ShutdownGracePeriod <= 0 {
		options.ShutdownGracePeriod = defaultShutdownGracePeriod
	}
	if options.MCPConnectTimeout <= 0 {
		options.MCPConnectTimeout = defaultMCPConnectTimeout
	}
	if options.Providers == nil {
		options.Providers = DefaultACPProviders()
	}
	return &ACPDriver{options: options, disabled: map[string]error{}, probed: map[string]protocol.AcpAgentCapabilities{}}
}

func (driver *ACPDriver) HarnessIDs() []string {
	identifiers := make([]string, 0, len(driver.options.Adapters))
	for harnessID := range driver.options.Adapters {
		identifiers = append(identifiers, harnessID)
	}
	sort.Strings(identifiers)
	return identifiers
}

func (driver *ACPDriver) Available(harnessID string) error {
	_, _, err := driver.adapter(harnessID)
	return err
}

var ErrResumeUnsupported = errors.New("ACP adapter did not negotiate session resume or load")

func (driver *ACPDriver) SupportsResume(harnessID string) error {
	driver.mutex.Lock()
	capabilities, probed := driver.probed[harnessID]
	driver.mutex.Unlock()
	if !probed || !(capabilities.ResumeSession || capabilities.LoadSession) {
		return fmt.Errorf("%w for harness %s", ErrResumeUnsupported, harnessID)
	}
	return nil
}

func (driver *ACPDriver) AdapterBinary(harnessID string) string {
	adapter, configured := driver.options.Adapters[harnessID]
	if !configured {
		return ""
	}
	return adapter.Binary
}

func (driver *ACPDriver) Disable(harnessID string, reason error) {
	driver.mutex.Lock()
	defer driver.mutex.Unlock()
	driver.disabled[harnessID] = reason
}

func (driver *ACPDriver) provenance(harnessID string) *protocol.ACPAdapterProvenance {
	adapter, configured := driver.options.Adapters[harnessID]
	if !configured || adapter.ID == "" || adapter.Version == "" || adapter.Source == "" {
		return nil
	}
	return &protocol.ACPAdapterProvenance{ID: adapter.ID, Version: adapter.Version, Source: adapter.Source}
}

func (driver *ACPDriver) adapter(harnessID string) (ACPAdapter, ACPProvider, error) {
	adapter, configured := driver.options.Adapters[harnessID]
	if !configured {
		return ACPAdapter{}, ACPProvider{}, fmt.Errorf("%w: no ACP adapter is configured for harness %s", ErrDriverUnavailable, harnessID)
	}
	provider, supported := driver.options.Providers[harnessID]
	if !supported {
		return ACPAdapter{}, ACPProvider{}, fmt.Errorf("%w: Orchestra has no ACP provider policy for harness %s", ErrDriverUnavailable, harnessID)
	}
	driver.mutex.Lock()
	disabled := driver.disabled[harnessID]
	driver.mutex.Unlock()
	if disabled != nil {
		return ACPAdapter{}, ACPProvider{}, fmt.Errorf("%w: ACP adapter for %s failed its startup probe", ErrDriverUnavailable, harnessID)
	}
	if !filepath.IsAbs(adapter.Binary) {
		return ACPAdapter{}, ACPProvider{}, fmt.Errorf("%w: ACP adapter path for %s must be absolute", ErrDriverUnavailable, harnessID)
	}
	information, err := os.Stat(adapter.Binary)
	if err != nil || !information.Mode().IsRegular() || (runtime.GOOS != "windows" && information.Mode().Perm()&0o111 == 0) {
		return ACPAdapter{}, ACPProvider{}, fmt.Errorf("%w: ACP adapter for %s is missing or not executable", ErrDriverUnavailable, harnessID)
	}
	if adapter.Verify != nil {
		if err := adapter.Verify(); err != nil {
			return ACPAdapter{}, ACPProvider{}, fmt.Errorf("%w: ACP adapter for %s failed verification: %w", ErrDriverUnavailable, harnessID, err)
		}
	}
	return adapter, provider, nil
}

type adapterProcess struct {
	command         *exec.Cmd
	stdin           io.WriteCloser
	stdout          io.ReadCloser
	stderr          *acp.StderrTail
	killProcessTree context.CancelFunc
}

func (driver *ACPDriver) start(harnessID string, adapter ACPAdapter, provider ACPProvider, approvalPolicy string, directory string, secrets []string) (*adapterProcess, error) {
	killContext, killProcessTree := context.WithCancel(context.Background())
	command := exec.CommandContext(killContext, adapter.Binary, adapter.Arguments...)
	configureProcessCancellation(command)
	command.Dir = directory
	environment := append([]string{}, adapter.Environment...)
	if provider.Environment != nil {
		environment = append(environment, provider.Environment(approvalPolicy)...)
	}
	if native := driver.options.NativeBinaries[harnessID]; provider.NativeBinaryVariable != "" && filepath.IsAbs(native) {
		environment = append(environment, provider.NativeBinaryVariable+"="+native)
	}
	command.Env = adapterEnvironment(os.Environ(), environment)
	stdin, err := command.StdinPipe()
	if err != nil {
		killProcessTree()
		return nil, err
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		killProcessTree()
		return nil, err
	}
	stderr := acp.NewStderrTail(acp.MaximumDiagnosticBytes, secrets)
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		killProcessTree()
		return nil, fmt.Errorf("%w: start ACP adapter: %w", ErrDriverUnavailable, err)
	}
	return &adapterProcess{command: command, stdin: stdin, stdout: stdout, stderr: stderr, killProcessTree: killProcessTree}, nil
}

func (driver *ACPDriver) stop(process *adapterProcess, client *acp.Client, graceful bool) (error, bool) {
	defer process.killProcessTree()
	exited := make(chan error, 1)
	go func() {
		client.Wait()
		exited <- process.command.Wait()
	}()
	client.Close()
	return driver.shutdown(graceful, exited, process.killProcessTree, func() { _ = process.stdin.Close() }, func() { _ = process.stdout.Close() })
}

type ACPProbe struct {
	Capabilities protocol.AcpAgentCapabilities
	Models       []string
}

func (driver *ACPDriver) Probe(operationContext context.Context, harnessID string) (ACPProbe, error) {
	adapter, provider, err := driver.adapter(harnessID)
	if err != nil {
		return ACPProbe{}, err
	}
	directory := ""
	if provider.ModelOption != "" {
		temporary, err := os.MkdirTemp("", "orchestra-acp-probe-")
		if err == nil {
			defer os.RemoveAll(temporary)
			directory, _ = canonicalDirectory(temporary)
		}
	}
	process, err := driver.start(harnessID, adapter, provider, protocol.ApprovalPolicyManual, directory, nil)
	if err != nil {
		return ACPProbe{}, err
	}
	client := acp.NewClient(process.stdout, process.stdin, acp.Options{
		ClientVersion:        driver.options.ClientVersion,
		ExpectedAgentVersion: adapter.Version,
		RequestTimeout:       driver.options.RequestTimeout,
	})
	capabilities, probeErr := client.Probe(operationContext)
	if probeErr == nil && !capabilities.Mcp.HTTP {
		probeErr = fmt.Errorf("%w: HTTP MCP servers are required for the Orchestra MCP server", acp.ErrMissingCapability)
	}
	var offered []string
	if probeErr == nil && directory != "" {
		offered = client.ProbeOption(operationContext, directory, provider.ModelOption)
	}
	driver.stop(process, client, probeErr == nil)
	if probeErr != nil {
		if diagnostic := process.stderr.String(); diagnostic != "" {
			return ACPProbe{}, fmt.Errorf("%w%s%s", probeErr, adapterStderrSeparator, diagnostic)
		}
		return ACPProbe{}, probeErr
	}
	driver.mutex.Lock()
	driver.probed[harnessID] = capabilities
	driver.mutex.Unlock()
	return ACPProbe{Capabilities: capabilities, Models: screenedModels(offered)}, nil
}

func (driver *ACPDriver) Execute(operationContext context.Context, invocation Invocation) (string, error) {
	adapter, provider, err := driver.adapter(invocation.Run.HarnessID)
	if err != nil {
		return "", err
	}
	workingDirectory, err := canonicalDirectory(invocation.Workspace)
	if err != nil {
		return "", err
	}
	secrets := []string{invocation.MCP.Token}
	process, err := driver.start(invocation.Run.HarnessID, adapter, provider, invocation.approvalPolicy, workingDirectory, secrets)
	if err != nil {
		return "", err
	}

	output := invocation.Output
	permission := invocation.Permission
	if provider.Permission != nil {
		permission = provider.Permission(invocation.approvalPolicy, permission)
	}
	client := acp.NewClient(process.stdout, process.stdin, acp.Options{
		RunID:                invocation.Run.ID,
		ClientVersion:        driver.options.ClientVersion,
		ExpectedAgentVersion: adapter.Version,
		Secrets:              secrets,
		Permission:           permission,
		Events: func(event protocol.HarnessEvent) {
			if event.Type == "message.delta" {
				output(event.Text)
			}
			if invocation.Events != nil {
				invocation.Events(event)
			}
		},
		RequestTimeout:    driver.options.RequestTimeout,
		PermissionTimeout: driver.options.PermissionTimeout,
		CancelGracePeriod: driver.options.CancelGracePeriod,
	})
	request := acp.SessionRequest{WorkingDirectory: workingDirectory, Prompt: composePrompt(invocation.Run, invocation.SystemPrompt)}
	if resume := invocation.Resume; resume != nil {
		request.Resume = &acp.ResumeRequest{SessionID: resume.ProviderSessionID}
		if resume.Prompt != "" {
			resumedRun := invocation.Run
			resumedRun.Prompt = resume.Prompt
			request.Resume.Prompt = composePrompt(resumedRun, invocation.SystemPrompt)
		}
	}
	if provider.Configuration != nil {
		request.Configuration = provider.Configuration(invocation.Run, invocation.approvalPolicy)
	}
	if invocation.MCP.URL != "" {
		request.MCPServer = &acp.MCPServer{Name: mcpServerName, URL: invocation.MCP.URL, BearerToken: invocation.MCP.Token}
	}
	request.BeforePrompt = func(beforeContext context.Context) error {
		if provider.RequireMCPConnection && request.MCPServer != nil {
			if err := driver.awaitMCPConnection(beforeContext, invocation.MCP.Connected); err != nil {
				return err
			}
		}
		if sessionID, resumed := client.Session(); invocation.Session != nil && sessionID != "" {
			invocation.Session(EstablishedSession{ProviderSessionID: sessionID, Resumed: resumed})
		}
		details := transportDetails{}
		if capabilities, negotiated := client.Negotiated(); negotiated {
			details.capabilities = &capabilities
		}
		invocation.announce(details)
		return nil
	}
	result, runErr := client.Run(operationContext, request)
	waitErr, forced := driver.stop(process, client, runErr == nil)

	if runErr != nil {
		if diagnostic := process.stderr.String(); diagnostic != "" {
			return "", fmt.Errorf("%w%s%s", runErr, adapterStderrSeparator, diagnostic)
		}
		return "", runErr
	}
	if forced {
		client.Warn(warningForcedShutdown, "ACP adapter did not exit after its session ended and was terminated")
	} else if waitErr != nil {
		client.Warn(warningAdapterExit, "ACP adapter exited abnormally after completing the prompt turn: "+client.Redact(waitErr.Error()))
	}
	if strings.TrimSpace(result.Text) == "" {
		return emptyResponse, nil
	}
	return strings.TrimSpace(result.Text), nil
}

func (driver *ACPDriver) awaitMCPConnection(operationContext context.Context, connected <-chan struct{}) error {
	if connected == nil {
		return fmt.Errorf("%w: the run's MCP grant cannot report a connection", ErrMCPUnavailable)
	}
	timer := time.NewTimer(driver.options.MCPConnectTimeout)
	defer timer.Stop()
	select {
	case <-connected:
		return nil
	case <-operationContext.Done():
		return fmt.Errorf("%w before the Orchestra MCP server connected", acp.ErrCancelled)
	case <-timer.C:
		return fmt.Errorf("%w within %s", ErrMCPUnavailable, driver.options.MCPConnectTimeout)
	}
}

func (driver *ACPDriver) shutdown(graceful bool, exited <-chan error, killProcessTree, releaseInput, releaseOutput func()) (error, bool) {
	if graceful {
		timer := time.NewTimer(driver.options.ShutdownGracePeriod)
		defer timer.Stop()
		select {
		case err := <-exited:
			return err, false
		case <-timer.C:
		}
	}
	killProcessTree()
	releaseInput()
	for _, release := range []func(){releaseOutput, func() {}} {
		timer := time.NewTimer(pipeReleaseDelay)
		select {
		case err := <-exited:
			timer.Stop()
			return err, graceful
		case <-timer.C:
		}
		release()
	}
	return errShutdownAbandoned, graceful
}

func canonicalDirectory(workspace string) (string, error) {
	if !filepath.IsAbs(workspace) {
		return "", errors.New("ACP session workspace must be an absolute path")
	}
	resolved, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return "", fmt.Errorf("resolve ACP session workspace: %w", err)
	}
	information, err := os.Stat(resolved)
	if err != nil || !information.IsDir() {
		return "", errors.New("ACP session workspace is not a directory")
	}
	return filepath.Clean(resolved), nil
}

func adapterEnvironment(inherited []string, additional []string) []string {
	environment := make([]string, 0, len(inherited)+len(additional))
	for _, entry := range inherited {
		name, _, _ := strings.Cut(entry, "=")
		secret := false
		for _, blocked := range environmentSecrets {
			if strings.EqualFold(name, blocked) {
				secret = true
			}
		}
		if !secret {
			environment = append(environment, entry)
		}
	}
	return append(environment, additional...)
}
