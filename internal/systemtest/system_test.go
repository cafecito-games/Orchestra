//go:build system

package systemtest

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/setup"
	"github.com/cafecito-games/Orchestra/internal/slave"
)

type builtBinaries struct {
	orchestraPath   string
	fakeHarnessPath string
}

var (
	compileOnce   sync.Once
	compiledPaths builtBinaries
	compileError  error
	compileRoot   string
)

func TestMain(m *testing.M) {
	temporaryRoot, err := os.MkdirTemp("", "orchestra-systemtest-bins-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create systemtest binary directory: %v\n", err)
		os.Exit(1)
	}
	compileRoot = temporaryRoot
	exitCode := m.Run()
	_ = os.RemoveAll(compileRoot)
	os.Exit(exitCode)
}

func ensureCompiledBinaries(t *testing.T) builtBinaries {
	t.Helper()
	compileOnce.Do(func() {
		_, currentFile, _, ok := runtime.Caller(0)
		if !ok {
			compileError = fmt.Errorf("locate current source file")
			return
		}
		repositoryRoot := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", ".."))
		extension := ""
		if runtime.GOOS == "windows" {
			extension = ".exe"
		}
		orchestraBinary := filepath.Join(compileRoot, "orchestra"+extension)
		fakeHarnessBinary := filepath.Join(compileRoot, "fakeharness"+extension)

		buildOrchestra := exec.Command("go", "build", "-trimpath", "-o", orchestraBinary, "./cmd/orchestra")
		buildOrchestra.Dir = repositoryRoot
		buildOrchestra.Env = append(os.Environ(), "CGO_ENABLED=0")
		if output, err := buildOrchestra.CombinedOutput(); err != nil {
			compileError = fmt.Errorf("build orchestra binary: %w\n%s", err, string(output))
			return
		}

		buildFakeHarness := exec.Command("go", "build", "-trimpath", "-o", fakeHarnessBinary, "./internal/systemtest/fakeharness")
		buildFakeHarness.Dir = repositoryRoot
		buildFakeHarness.Env = append(os.Environ(), "CGO_ENABLED=0")
		if output, err := buildFakeHarness.CombinedOutput(); err != nil {
			compileError = fmt.Errorf("build fakeharness binary: %w\n%s", err, string(output))
			return
		}

		compiledPaths = builtBinaries{
			orchestraPath:   orchestraBinary,
			fakeHarnessPath: fakeHarnessBinary,
		}
	})
	require.NoError(t, compileError)
	return compiledPaths
}

type fakeHarnessInstallation struct {
	binDirectory      string
	recordDirectory   string
	sessionDirectory  string
	gateDirectory     string
	claudeACPArgument string
	codexACPArgument  string
	claudeAdapter     string
	claudeSHA256      string
	codexAdapter      string
	codexSHA256       string
}

func installFakeHarnesses(t *testing.T, sourceBinary string) fakeHarnessInstallation {
	t.Helper()
	baseDirectory := t.TempDir()
	binDirectory := filepath.Join(baseDirectory, "bin")
	recordDirectory := filepath.Join(baseDirectory, "records")
	sessionDirectory := filepath.Join(baseDirectory, "sessions")
	gateDirectory := filepath.Join(baseDirectory, "gates")
	for _, directory := range []string{binDirectory, recordDirectory, sessionDirectory, gateDirectory} {
		require.NoError(t, os.MkdirAll(directory, 0o755))
	}

	binaryBytes, err := os.ReadFile(sourceBinary)
	require.NoError(t, err)
	digest := sha256.Sum256(binaryBytes)
	digestHex := hex.EncodeToString(digest[:])

	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	copyRole := func(name string) string {
		destination := filepath.Join(binDirectory, name+extension)
		require.NoError(t, os.WriteFile(destination, binaryBytes, 0o755))
		return destination
	}

	_ = copyRole("claude")
	_ = copyRole("codex")
	claudeAdapterPath := copyRole("claude-agent-acp")
	codexAdapterPath := copyRole("codex-acp")

	return fakeHarnessInstallation{
		binDirectory:      binDirectory,
		recordDirectory:   recordDirectory,
		sessionDirectory:  sessionDirectory,
		gateDirectory:     gateDirectory,
		claudeACPArgument: "claude-cli=sha256:" + digestHex + ":" + claudeAdapterPath,
		codexACPArgument:  "codex-cli=sha256:" + digestHex + ":" + codexAdapterPath,
		claudeAdapter:     claudeAdapterPath,
		claudeSHA256:      digestHex,
		codexAdapter:      codexAdapterPath,
		codexSHA256:       digestHex,
	}
}

func (installation fakeHarnessInstallation) openGate(t *testing.T, gateName string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(installation.gateDirectory, gateName), []byte("open\n"), 0o600))
}

func (installation fakeHarnessInstallation) installAntigravity(t *testing.T, sourceBinary string) {
	t.Helper()
	binaryBytes, err := os.ReadFile(sourceBinary)
	require.NoError(t, err)
	extension := ""
	if runtime.GOOS == "windows" {
		extension = ".exe"
	}
	require.NoError(t, os.WriteFile(filepath.Join(installation.binDirectory, "agy"+extension), binaryBytes, 0o755))
}

func (installation fakeHarnessInstallation) readRecords(t *testing.T) []map[string]any {
	t.Helper()
	entries, err := os.ReadDir(installation.recordDirectory)
	require.NoError(t, err)
	var records []map[string]any
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		fileBytes, readErr := os.ReadFile(filepath.Join(installation.recordDirectory, entry.Name()))
		require.NoError(t, readErr)
		for line := range strings.SplitSeq(string(fileBytes), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" {
				continue
			}
			var decoded map[string]any
			if json.Unmarshal([]byte(trimmed), &decoded) == nil {
				records = append(records, decoded)
			}
		}
	}
	return records
}

type managedProcess struct {
	name      string
	command   *exec.Cmd
	logMutex  sync.Mutex
	logBuffer bytes.Buffer
	exited    chan error
}

type lockedLogWriter struct {
	process *managedProcess
}

func (writer lockedLogWriter) Write(chunk []byte) (int, error) {
	writer.process.logMutex.Lock()
	defer writer.process.logMutex.Unlock()
	return writer.process.logBuffer.Write(chunk)
}

func startManagedProcess(t *testing.T, name string, binaryPath string, arguments []string, environment []string) *managedProcess {
	t.Helper()
	command := exec.Command(binaryPath, arguments...)
	command.Env = environment
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.WaitDelay = 2 * time.Second
	process := &managedProcess{
		name:    name,
		command: command,
		exited:  make(chan error, 1),
	}
	writer := lockedLogWriter{process: process}
	command.Stdout = writer
	command.Stderr = writer

	require.NoError(t, command.Start(), "start %s", name)
	go func() {
		process.exited <- command.Wait()
	}()

	t.Cleanup(func() {
		process.stop()
	})
	return process
}

func (process *managedProcess) logs() string {
	process.logMutex.Lock()
	defer process.logMutex.Unlock()
	return process.logBuffer.String()
}

func (process *managedProcess) stop() {
	if process == nil || process.command == nil || process.command.Process == nil {
		return
	}
	select {
	case <-process.exited:
		return
	default:
	}
	processID := process.command.Process.Pid
	_ = syscall.Kill(-processID, syscall.SIGTERM)
	select {
	case <-process.exited:
		return
	case <-time.After(3 * time.Second):
		_ = syscall.Kill(-processID, syscall.SIGKILL)
		select {
		case <-process.exited:
		case <-time.After(3 * time.Second):
		}
	}
}

func allocateFreeLoopbackAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}

func waitForMasterReady(t *testing.T, baseURL string, masterProcess *managedProcess) {
	t.Helper()
	httpClient := &http.Client{Timeout: 1 * time.Second}
	if !assert.Eventually(t, func() bool {
		response, err := httpClient.Get(baseURL + "/healthz")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 10*time.Second, 50*time.Millisecond) {
		t.Fatalf("master failed to become healthy; logs:\n%s", masterProcess.logs())
	}
}

func initializeGitRepository(t *testing.T, repositoryPath string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(repositoryPath, 0o755))
	runGit := func(arguments ...string) {
		command := exec.Command("git", arguments...)
		command.Dir = repositoryPath
		output, err := command.CombinedOutput()
		require.NoError(t, err, "git %s failed: %s", strings.Join(arguments, " "), string(output))
	}
	runGit("init", "-b", "main")
	runGit("config", "user.email", "orchestra-test@example.com")
	runGit("config", "user.name", "Orchestra System Test")
	require.NoError(t, os.WriteFile(filepath.Join(repositoryPath, "README.md"), []byte("# Orchestra Test Repo\n"), 0o644))
	runGit("add", "README.md")
	runGit("commit", "-m", "Initial commit")
}

type mcpStdioSession struct {
	command           *exec.Cmd
	stdin             io.WriteCloser
	stderrMutex       sync.Mutex
	stderrBuffer      bytes.Buffer
	writeMutex        sync.Mutex
	sequence          atomic.Int64
	pendingMutex      sync.Mutex
	pending           map[int64]chan map[string]any
	notificationMutex sync.Mutex
	notifications     []map[string]any
	closed            chan error
}

func startMCPStdioSession(t *testing.T, orchestraBinary string, endpoint string, token string, clientID string, extraArguments ...string) *mcpStdioSession {
	t.Helper()
	commandArguments := append([]string{"mcp", "--endpoint", endpoint, "--token", token, "--client-id", clientID}, extraArguments...)
	command := exec.Command(orchestraBinary, commandArguments...)
	stdinPipe, err := command.StdinPipe()
	require.NoError(t, err)
	stdoutPipe, err := command.StdoutPipe()
	require.NoError(t, err)

	session := &mcpStdioSession{
		command: command,
		stdin:   stdinPipe,
		pending: map[int64]chan map[string]any{},
		closed:  make(chan error, 1),
	}
	command.Stderr = writerFunc(func(chunk []byte) (int, error) {
		session.stderrMutex.Lock()
		defer session.stderrMutex.Unlock()
		return session.stderrBuffer.Write(chunk)
	})

	require.NoError(t, command.Start())

	go func() {
		scanner := bufio.NewScanner(stdoutPipe)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			var frame map[string]any
			if json.Unmarshal(scanner.Bytes(), &frame) != nil {
				continue
			}
			if methodName, isNotification := frame["method"].(string); isNotification && methodName != "" {
				session.notificationMutex.Lock()
				session.notifications = append(session.notifications, frame)
				session.notificationMutex.Unlock()
				continue
			}
			idFloat, hasID := frame["id"].(float64)
			if !hasID {
				continue
			}
			requestID := int64(idFloat)
			session.pendingMutex.Lock()
			waiter := session.pending[requestID]
			delete(session.pending, requestID)
			session.pendingMutex.Unlock()
			if waiter != nil {
				waiter <- frame
			}
		}
	}()

	go func() {
		session.closed <- command.Wait()
	}()

	t.Cleanup(func() {
		_ = session.stdin.Close()
		select {
		case <-session.closed:
		case <-time.After(4 * time.Second):
			if session.command.Process != nil {
				_ = session.command.Process.Kill()
			}
			<-session.closed
		}
	})

	initResponse := session.callRPC(t, "initialize", map[string]any{
		"protocolVersion": "2025-06-18",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]string{"name": "system-test-orchestrator", "version": "1.0.0"},
	})
	require.Nil(t, initResponse["error"])

	return session
}

type writerFunc func([]byte) (int, error)

func (function writerFunc) Write(chunk []byte) (int, error) {
	return function(chunk)
}

func (session *mcpStdioSession) callRPC(t *testing.T, method string, parameters any) map[string]any {
	t.Helper()
	requestID := session.sequence.Add(1)
	waiter := make(chan map[string]any, 1)
	session.pendingMutex.Lock()
	session.pending[requestID] = waiter
	session.pendingMutex.Unlock()

	encodedBytes, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      requestID,
		"method":  method,
		"params":  parameters,
	})
	require.NoError(t, err)

	session.writeMutex.Lock()
	_, writeErr := session.stdin.Write(append(encodedBytes, '\n'))
	session.writeMutex.Unlock()
	require.NoError(t, writeErr)

	select {
	case response := <-waiter:
		return response
	case <-time.After(25 * time.Second):
		session.stderrMutex.Lock()
		stderrText := session.stderrBuffer.String()
		session.stderrMutex.Unlock()
		t.Fatalf("timed out waiting for MCP stdio %s response; stderr:\n%s", method, stderrText)
		return nil
	}
}

func (session *mcpStdioSession) callTool(t *testing.T, name string, arguments any, destination any) {
	t.Helper()
	response := session.callRPC(t, "tools/call", map[string]any{
		"name":      name,
		"arguments": arguments,
	})
	require.Nil(t, response["error"], "MCP RPC error calling %s: %v", name, response["error"])
	resultObject, _ := response["result"].(map[string]any)
	isError, _ := resultObject["isError"].(bool)
	require.False(t, isError, "MCP tool %s returned tool error: %v", name, resultObject["content"])

	if destination != nil {
		encodedStructured, err := json.Marshal(resultObject["structuredContent"])
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(encodedStructured, destination))
	}
}

func (session *mcpStdioSession) notificationsSnapshot() []map[string]any {
	session.notificationMutex.Lock()
	defer session.notificationMutex.Unlock()
	cloned := make([]map[string]any, len(session.notifications))
	copy(cloned, session.notifications)
	return cloned
}

func invokeOrchestraCLI(t *testing.T, orchestraBinary string, environment []string, arguments ...string) (string, string) {
	t.Helper()
	command := exec.Command(orchestraBinary, arguments...)
	command.Env = environment
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	err := command.Run()
	require.NoError(t, err, "orchestra %s failed:\nstdout: %s\nstderr: %s", strings.Join(arguments, " "), stdout.String(), stderr.String())
	return stdout.String(), stderr.String()
}

func formatFakeScript(t *testing.T, steps ...map[string]any) string {
	t.Helper()
	encodedBytes, err := json.Marshal(map[string]any{"steps": steps})
	require.NoError(t, err)
	return "<fake-script>" + string(encodedBytes) + "</fake-script>"
}

func buildWorkerEnvironment(fakeHarnesses fakeHarnessInstallation, extra ...string) []string {
	var filtered []string
	blockedPrefixes := []string{
		"ANTHROPIC_",
		"CLAUDE_CODE_",
		"AWS_REGION=",
		"CLOUD_ML_REGION=",
		"ORCHESTRA_",
		"WORKSPACE_ROOTS=",
	}
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "PATH=") {
			continue
		}
		skip := false
		for _, prefix := range blockedPrefixes {
			if strings.HasPrefix(entry, prefix) {
				skip = true
				break
			}
		}
		if !skip {
			filtered = append(filtered, entry)
		}
	}
	filtered = append(filtered,
		"PATH="+fakeHarnesses.binDirectory+string(os.PathListSeparator)+os.Getenv("PATH"),
		"ORCHESTRA_FAKE_RECORD_DIRECTORY="+fakeHarnesses.recordDirectory,
		"ORCHESTRA_FAKE_SESSION_DIRECTORY="+fakeHarnesses.sessionDirectory,
		"ORCHESTRA_FAKE_GATE_DIRECTORY="+fakeHarnesses.gateDirectory,
	)
	return append(filtered, extra...)
}

func TestMultiNodeEndToEndSystem(t *testing.T) {
	binaries := ensureCompiledBinaries(t)
	fakeHarnesses := installFakeHarnesses(t, binaries.fakeHarnessPath)

	const bearerToken = "system-test-orchestra-bearer-token-998877"
	listenAddress := allocateFreeLoopbackAddress(t)
	masterBaseURL := "http://" + listenAddress
	masterDataDirectory := filepath.Join(t.TempDir(), "master-sqlite")

	macRawRoot := filepath.Join(t.TempDir(), "mac-workspaces")
	linuxRawRoot := filepath.Join(t.TempDir(), "linux-workspaces")
	require.NoError(t, os.MkdirAll(macRawRoot, 0o755))
	require.NoError(t, os.MkdirAll(linuxRawRoot, 0o755))

	macCanonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{macRawRoot})
	require.NoError(t, err)
	linuxCanonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{linuxRawRoot})
	require.NoError(t, err)
	macWorkspaceRoot := macCanonicalRoots[0]
	linuxWorkspaceRoot := linuxCanonicalRoots[0]

	macRepositoryPath := filepath.Join(macWorkspaceRoot, "game-client")
	linuxRepositoryPath := filepath.Join(linuxWorkspaceRoot, "game-server")
	initializeGitRepository(t, macRepositoryPath)
	initializeGitRepository(t, linuxRepositoryPath)

	workerMacDataRoot := filepath.Join(t.TempDir(), "worker-mac-data")
	workerLinuxDataRoot := filepath.Join(t.TempDir(), "worker-linux-data")

	defaultManifest, err := setup.LoadDefaultManifest()
	require.NoError(t, err)
	adapterOnlyManifest := setup.Manifest{
		ManifestVersion: defaultManifest.ManifestVersion,
		Components:      defaultManifest.ComponentsOfKind(setup.ComponentKindACPAdapter),
	}
	adapterManifestBytes, err := json.MarshalIndent(adapterOnlyManifest, "", "  ")
	require.NoError(t, err)
	adapterManifestPath := filepath.Join(t.TempDir(), "adapters-manifest.json")
	require.NoError(t, os.WriteFile(adapterManifestPath, adapterManifestBytes, 0o600))

	cliEnvironment := buildWorkerEnvironment(
		fakeHarnesses,
		"ORCHESTRA_ENDPOINT="+masterBaseURL,
		"ORCHESTRA_TOKEN="+bearerToken,
		"ORCHESTRA_ADAPTER_MANIFEST="+adapterManifestPath,
	)

	planPath := filepath.Join(t.TempDir(), "setup-plan.json")
	invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment,
		"setup", "plan", "--data-root", workerLinuxDataRoot, "--manifest", adapterManifestPath, "--out", planPath,
	)
	invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment,
		"setup", "apply",
		"--data-root", workerLinuxDataRoot,
		"--manifest", adapterManifestPath,
		"--plan", planPath,
		"--manual-artifact", "claude-acp="+fakeHarnesses.claudeAdapter,
		"--manual-checksum", "claude-acp="+fakeHarnesses.claudeSHA256,
		"--manual-artifact", "codex-acp="+fakeHarnesses.codexAdapter,
		"--manual-checksum", "codex-acp="+fakeHarnesses.codexSHA256,
	)
	invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment,
		"setup", "activate",
		"--data-root", workerLinuxDataRoot,
		"--manifest", adapterManifestPath,
		"--kind", "acp-adapter",
		"--id", "claude-acp",
		"--version", "0.79.0",
	)
	invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment,
		"setup", "activate",
		"--data-root", workerLinuxDataRoot,
		"--manifest", adapterManifestPath,
		"--kind", "acp-adapter",
		"--id", "codex-acp",
		"--version", "1.12.0",
	)

	masterEnvironment := buildWorkerEnvironment(fakeHarnesses)
	startMasterProcess := func() *managedProcess {
		process := startManagedProcess(t, "orchestra-master", binaries.orchestraPath, []string{
			"master",
			"--listen", listenAddress,
			"--token", bearerToken,
			"--data-dir", masterDataDirectory,
		}, masterEnvironment)
		waitForMasterReady(t, masterBaseURL, process)
		return process
	}

	masterProcess := startMasterProcess()

	doctorOutput, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment,
		"doctor",
		"--data-root", workerLinuxDataRoot,
		"--endpoint", masterBaseURL,
		"--claude-acp-auth-mode", "local-subscription",
	)
	require.Contains(t, doctorOutput, "active=0.79.0")
	require.Contains(t, doctorOutput, "active=1.12.0")
	require.Contains(t, doctorOutput, "reachable")

	startWorkerMac := func() *managedProcess {
		return startManagedProcess(t, "slave-mac", binaries.orchestraPath, []string{
			"slave",
			"--endpoint", masterBaseURL,
			"--token", bearerToken,
			"--id", "worker-mac",
			"--name", "Studio Mac M4",
			"--kind", "local",
			"--workspace-root", macWorkspaceRoot,
			"--label", "macos",
			"--label", "apple-silicon",
			"--concurrency", "3",
			"--instance-capacity", "4",
			"--data-root", workerMacDataRoot,
			"--claude-acp-auth-mode", "local-subscription",
			"--acp-adapter", fakeHarnesses.claudeACPArgument,
			"--acp-adapter", fakeHarnesses.codexACPArgument,
		}, buildWorkerEnvironment(fakeHarnesses, "ANTHROPIC_MOCK_LOCAL=1"))
	}

	startWorkerLinux := func() *managedProcess {
		return startManagedProcess(t, "slave-linux", binaries.orchestraPath, []string{
			"slave",
			"--endpoint", masterBaseURL,
			"--token", bearerToken,
			"--id", "worker-linux",
			"--name", "Build Box Linux",
			"--kind", "home-server",
			"--workspace-root", linuxWorkspaceRoot,
			"--label", "linux",
			"--label", "gpu",
			"--concurrency", "3",
			"--instance-capacity", "4",
			"--data-root", workerLinuxDataRoot,
			"--claude-acp-auth-mode", "local-subscription",
		}, buildWorkerEnvironment(fakeHarnesses, "OPENAI_API_KEY=sk-local-worker-only-secret", "ORCHESTRA_ADAPTER_MANIFEST="+adapterManifestPath))
	}

	workerMac := startWorkerMac()
	workerLinux := startWorkerLinux()

	mcpSession := startMCPStdioSession(t, binaries.orchestraPath, masterBaseURL, bearerToken, "main-orchestrator")

	var toolsListResponse map[string]any
	toolsListResponse = mcpSession.callRPC(t, "tools/list", map[string]any{})
	toolsResult, _ := toolsListResponse["result"].(map[string]any)
	listedTools, _ := toolsResult["tools"].([]any)
	require.Len(t, listedTools, len(protocol.OrchestratorToolNames))

	if !assert.Eventually(t, func() bool {
		var inventory struct {
			Nodes []protocol.NodeDetail `json:"nodes"`
		}
		mcpSession.callTool(t, "get_execution_inventory", map[string]any{}, &inventory)
		if len(inventory.Nodes) < 2 {
			return false
		}
		readyCount := 0
		for _, detail := range inventory.Nodes {
			if detail.Connected && detail.Synced {
				readyCount++
			}
		}
		return readyCount == 2
	}, 12*time.Second, 100*time.Millisecond) {
		t.Fatalf("workers failed to register and sync;\nmac:\n%s\nlinux:\n%s", workerMac.logs(), workerLinux.logs())
	}

	statusOutput, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment, "status")
	require.Contains(t, statusOutput, "2 connected / 2 total")
	nodesOutput, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment, "nodes")
	require.Contains(t, nodesOutput, "worker-mac (Studio Mac M4)")
	require.Contains(t, nodesOutput, "worker-linux (Build Box Linux)")
	require.Contains(t, nodesOutput, "acp-v1")

	fencingDuplicate := startManagedProcess(t, "slave-mac-duplicate", binaries.orchestraPath, []string{
		"slave",
		"--endpoint", masterBaseURL,
		"--token", bearerToken,
		"--id", "worker-mac",
		"--name", "Duplicate Mac",
		"--workspace-root", macWorkspaceRoot,
		"--data-root", filepath.Join(t.TempDir(), "dup-data"),
	}, buildWorkerEnvironment(fakeHarnesses, "ORCHESTRA_ADAPTER_MANIFEST="+adapterManifestPath))
	if !assert.Eventually(t, func() bool {
		duplicateLogs := fencingDuplicate.logs()
		return strings.Contains(duplicateLogs, "StatusPolicyViolation") && strings.Contains(duplicateLogs, "node already has a live connection")
	}, 8*time.Second, 100*time.Millisecond) {
		t.Fatalf("expected single-live-connection fencing for duplicate node ID; logs:\n%s", fencingDuplicate.logs())
	}
	fencingDuplicate.stop()

	var createdThread struct {
		Thread     protocol.Thread            `json:"thread"`
		Attachment *protocol.ThreadAttachment `json:"attachment"`
	}
	mcpSession.callTool(t, "create_thread", map[string]any{
		"title":   "Cross-Machine Feature Build",
		"summary": "End-to-end verification across Studio Mac and Linux Build Box",
		"attach":  true,
	}, &createdThread)
	threadID := createdThread.Thread.ID
	require.NotEmpty(t, threadID)
	require.NotNil(t, createdThread.Attachment)

	var spawnedInstance protocol.InstanceLifecycleResult
	mcpSession.callTool(t, "spawn_instance", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "spawn-mac-resident-1",
		"purpose": map[string]any{
			"name":    "mac-claude-specialist",
			"summary": "Resident ACP session on Studio Mac",
		},
		"requirements": map[string]any{
			"harnessIds": []string{"claude-cli"},
			"transports": []string{"acp-v1"},
			"labels":     []string{"macos"},
			"workspace": map[string]any{
				"path":     macRepositoryPath,
				"writable": true,
			},
		},
		"idleTimeoutSeconds": 300,
	}, &spawnedInstance)
	residentInstanceID := spawnedInstance.Instance.ID
	require.NotEmpty(t, residentInstanceID)

	require.Eventually(t, func() bool {
		var instanceState struct {
			Instance protocol.AgentInstance `json:"instance"`
		}
		mcpSession.callTool(t, "get_instance", map[string]any{"instanceId": residentInstanceID}, &instanceState)
		return instanceState.Instance.Status == "ready" || instanceState.Instance.Status == "idle"
	}, 10*time.Second, 100*time.Millisecond)

	turnOneScript := formatFakeScript(t,
		map[string]any{
			"call": "get_task_context",
			"as":   "context",
		},
		map[string]any{
			"message": "turn1 session={{session}} task={{context.task.id}}",
		},
	)
	var turnOneSubmit struct {
		Tasks []protocol.Task `json:"tasks"`
	}
	mcpSession.callTool(t, "submit_tasks", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "resident-turn-1",
		"tasks": []map[string]any{{
			"clientTaskId": "turn-1",
			"title":        "Resident ACP Turn 1",
			"instructions": "Run first ACP turn\n" + turnOneScript,
			"instanceId":   residentInstanceID,
			"requirements": map[string]any{
				"harnessIds": []string{"claude-cli"},
				"transports": []string{"acp-v1"},
				"workspace":  map[string]any{"path": macRepositoryPath, "writable": true},
			},
		}},
	}, &turnOneSubmit)
	turnOneTaskID := turnOneSubmit.Tasks[0].ID

	waitForTaskTerminalStatus := func(targetTaskID string, expectedStatus string) protocol.Task {
		t.Helper()
		var matched protocol.Task
		var lastSnapshot any
		if !assert.Eventually(t, func() bool {
			var threadContext map[string]any
			mcpSession.callTool(t, "get_thread_context", map[string]any{"threadId": threadID}, &threadContext)
			lastSnapshot = threadContext
			encodedTasks, _ := json.Marshal(threadContext["tasks"])
			var tasks []protocol.Task
			_ = json.Unmarshal(encodedTasks, &tasks)
			for _, candidate := range tasks {
				if candidate.ID == targetTaskID && candidate.Status == expectedStatus {
					matched = candidate
					return true
				}
			}
			return false
		}, 15*time.Second, 100*time.Millisecond) {
			snapshotBytes, _ := json.MarshalIndent(lastSnapshot, "", "  ")
			t.Fatalf(
				"task %s did not reach status %s;\nthreadContext:\n%s\nmaster logs:\n%s\nmac logs:\n%s\nlinux logs:\n%s",
				targetTaskID,
				expectedStatus,
				string(snapshotBytes),
				masterProcess.logs(),
				workerMac.logs(),
				workerLinux.logs(),
			)
		}
		return matched
	}

	waitForTaskTerminalStatus(turnOneTaskID, "completed")

	require.Eventually(t, func() bool {
		return len(mcpSession.notificationsSnapshot()) > 0
	}, 5*time.Second, 50*time.Millisecond, "expected doorbell notification over orchestra mcp stdio")

	turnTwoScript := formatFakeScript(t,
		map[string]any{
			"message": "turn2 session={{session}}",
		},
	)
	var turnTwoSubmit struct {
		Tasks []protocol.Task `json:"tasks"`
	}
	mcpSession.callTool(t, "submit_tasks", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "resident-turn-2",
		"tasks": []map[string]any{{
			"clientTaskId": "turn-2",
			"title":        "Resident ACP Turn 2 (Session Resume)",
			"instructions": "Resume ACP session\n" + turnTwoScript,
			"instanceId":   residentInstanceID,
			"requirements": map[string]any{
				"harnessIds": []string{"claude-cli"},
				"transports": []string{"acp-v1"},
				"workspace":  map[string]any{"path": macRepositoryPath, "writable": true},
			},
		}},
	}, &turnTwoSubmit)
	waitForTaskTerminalStatus(turnTwoSubmit.Tasks[0].ID, "completed")

	var detailAfterTurns protocol.ThreadDetailResponse
	detailJSON, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment, "thread", threadID, "--json")
	require.NoError(t, json.Unmarshal([]byte(detailJSON), &detailAfterTurns))
	require.Len(t, detailAfterTurns.Runs, 2)
	require.NotNil(t, detailAfterTurns.Runs[0].ProviderSessionID)
	require.NotNil(t, detailAfterTurns.Runs[1].ProviderSessionID)
	require.Equal(t, *detailAfterTurns.Runs[0].ProviderSessionID, *detailAfterTurns.Runs[1].ProviderSessionID, "resident instance should resume exact ACP provider session")

	mcpSession.callTool(t, "renew_instance", map[string]any{
		"instanceId":         residentInstanceID,
		"idempotencyKey":     "renew-resident-1",
		"idleTimeoutSeconds": 600,
	}, nil)

	dagStepAScript := formatFakeScript(t,
		map[string]any{
			"writeFile": map[string]any{
				"path":    "worktree-change.txt",
				"content": "isolated git worktree write on branch",
			},
		},
		map[string]any{
			"writeFile": map[string]any{
				"path":    "build-report.txt",
				"content": "Codex ACP Linux build artifact payload\n",
			},
		},
		map[string]any{
			"call": "post_artifact",
			"arguments": map[string]any{
				"relativePath": "build-report.txt",
				"name":         "linux-build-report",
				"mediaType":    "text/plain",
				"summary":      "Generated on worker-linux",
			},
			"as": "uploadedArtifact",
		},
		map[string]any{
			"call": "update_task",
			"arguments": map[string]any{
				"idempotencyKey": "update-dag-a",
				"summary":        "Built in isolated git worktree",
				"artifactIds":    []any{"{{uploadedArtifact.artifact.id}}"},
			},
		},
		map[string]any{
			"message": "dag-a completed in {{cwd}}",
		},
	)

	dagStepBScript := formatFakeScript(t,
		map[string]any{
			"call": "get_task_context",
			"as":   "taskContext",
		},
		map[string]any{
			"message": "native codex verified dependency count={{taskContext.dependencies.0.status}}",
		},
	)

	var dagSubmit struct {
		Tasks         []protocol.Task   `json:"tasks"`
		ClientTaskMap map[string]string `json:"clientTaskMap"`
	}
	mcpSession.callTool(t, "submit_tasks", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "cross-node-dag-1",
		"tasks": []map[string]any{
			{
				"clientTaskId": "linux-worktree-build",
				"title":        "Linux Git Worktree Build (Codex ACP)",
				"instructions": "Build in isolated git worktree\n" + dagStepAScript,
				"requirements": map[string]any{
					"harnessIds": []string{"codex-cli"},
					"transports": []string{"acp-v1"},
					"labels":     []string{"linux"},
					"workspace": map[string]any{
						"path":      linuxRepositoryPath,
						"writable":  true,
						"isolation": "git-worktree",
						"cleanup":   "when-unchanged",
					},
				},
			},
			{
				"clientTaskId": "mac-native-verify",
				"title":        "Mac Native CLI Verification",
				"instructions": "Verify after Linux build completes\n" + dagStepBScript,
				"dependencies": []map[string]any{
					{"clientTaskId": "linux-worktree-build"},
				},
				"requirements": map[string]any{
					"harnessIds": []string{"codex-cli"},
					"transports": []string{"native-cli"},
					"labels":     []string{"macos"},
					"workspace": map[string]any{
						"path":     macRepositoryPath,
						"writable": false,
					},
				},
			},
		},
	}, &dagSubmit)

	linuxTaskID := dagSubmit.ClientTaskMap["linux-worktree-build"]
	macVerifyTaskID := dagSubmit.ClientTaskMap["mac-native-verify"]

	waitForTaskTerminalStatus(linuxTaskID, "completed")
	waitForTaskTerminalStatus(macVerifyTaskID, "completed")

	_, baseDirtyErr := os.Stat(filepath.Join(linuxRepositoryPath, "worktree-change.txt"))
	require.True(t, os.IsNotExist(baseDirtyErr), "main git checkout must remain untouched by git-worktree task")

	detailJSON, _ = invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment, "thread", threadID, "--json")
	require.NoError(t, json.Unmarshal([]byte(detailJSON), &detailAfterTurns))
	require.NotEmpty(t, detailAfterTurns.WorkspaceLeases)
	leaseRecord := detailAfterTurns.WorkspaceLeases[0]
	require.Equal(t, "git-worktree", leaseRecord.Policy)
	require.Equal(t, "retained", leaseRecord.Status)
	require.Equal(t, "untracked", leaseRecord.RetentionReason)
	worktreeWritten, err := os.ReadFile(filepath.Join(leaseRecord.WorktreePath, "worktree-change.txt"))
	require.NoError(t, err)
	require.Equal(t, "isolated git worktree write on branch", string(worktreeWritten))

	require.Len(t, detailAfterTurns.Artifacts, 1)
	workerArtifact := detailAfterTurns.Artifacts[0]
	require.True(t, workerArtifact.Uploaded)

	orchestratorLocalFile := filepath.Join(t.TempDir(), "orchestrator-spec.md")
	orchestratorContent := "# Architecture Spec\nShared from main machine via orchestra mcp.\n"
	require.NoError(t, os.WriteFile(orchestratorLocalFile, []byte(orchestratorContent), 0o600))

	var orchestratorArtifactResult struct {
		Artifact protocol.Artifact `json:"artifact"`
	}
	mcpSession.callTool(t, "post_artifact", map[string]any{
		"threadId":  threadID,
		"filePath":  orchestratorLocalFile,
		"name":      "architecture-spec",
		"mediaType": "text/markdown",
		"summary":   "Uploaded by orchestrator via stdio MCP bridge",
	}, &orchestratorArtifactResult)
	require.True(t, orchestratorArtifactResult.Artifact.Uploaded)

	downloadArtifact := func(artifactID string) string {
		request, requestErr := http.NewRequest(http.MethodGet, masterBaseURL+"/api/artifacts/"+artifactID+"/content", nil)
		require.NoError(t, requestErr)
		request.Header.Set("Authorization", "Bearer "+bearerToken)
		response, executeErr := http.DefaultClient.Do(request)
		require.NoError(t, executeErr)
		defer response.Body.Close()
		require.Equal(t, http.StatusOK, response.StatusCode)
		bodyBytes, readErr := io.ReadAll(response.Body)
		require.NoError(t, readErr)
		return string(bodyBytes)
	}
	require.Equal(t, "Codex ACP Linux build artifact payload\n", downloadArtifact(workerArtifact.ID))
	require.Equal(t, orchestratorContent, downloadArtifact(orchestratorArtifactResult.Artifact.ID))

	mailboxScript := formatFakeScript(t,
		map[string]any{
			"call": "send_task_message",
			"arguments": map[string]any{
				"idempotencyKey": "worker-question-1",
				"recipient":      map[string]any{"kind": "orchestrator"},
				"kind":           "question",
				"body":           "Should we enable strict validation?",
			},
		},
		map[string]any{
			"waitFor": map[string]any{
				"type": "message.posted",
				"message": map[string]any{
					"kind": "guidance",
				},
			},
			"as": "replyEvent",
		},
		map[string]any{
			"message": "mailbox answered: {{replyEvent.message.body}}",
		},
	)

	var eventPageBeforeMailbox struct {
		Cursor string `json:"cursor"`
	}
	mcpSession.callTool(t, "get_thread_events", map[string]any{
		"threadId":            threadID,
		"timeoutMilliseconds": 0,
	}, &eventPageBeforeMailbox)

	var mailboxSubmit struct {
		Tasks []protocol.Task `json:"tasks"`
	}
	mcpSession.callTool(t, "submit_tasks", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "mailbox-roundtrip-1",
		"tasks": []map[string]any{{
			"title":        "Bidirectional Mailbox Task (Claude Native)",
			"instructions": "Ask orchestrator and wait for reply\n" + mailboxScript,
			"requirements": map[string]any{
				"harnessIds": []string{"claude-cli"},
				"transports": []string{"native-cli"},
				"labels":     []string{"linux"},
			},
		}},
	}, &mailboxSubmit)
	mailboxTaskID := mailboxSubmit.Tasks[0].ID

	cursor := eventPageBeforeMailbox.Cursor
	require.Eventually(t, func() bool {
		var eventPage struct {
			Events []protocol.TaskEventEntry `json:"events"`
			Cursor string                    `json:"cursor"`
		}
		mcpSession.callTool(t, "get_thread_events", map[string]any{
			"threadId":            threadID,
			"cursor":              cursor,
			"timeoutMilliseconds": 250,
		}, &eventPage)
		if eventPage.Cursor != "" {
			cursor = eventPage.Cursor
		}
		for _, event := range eventPage.Events {
			if event.Type == "message.posted" && event.Message != nil && event.Message.Kind == "question" && event.TaskID == mailboxTaskID {
				return true
			}
		}
		return false
	}, 12*time.Second, 100*time.Millisecond, "did not observe worker question in get_thread_events")

	mcpSession.callTool(t, "send_task_message", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "orchestrator-guidance-1",
		"recipient":      map[string]any{"kind": "task", "taskId": mailboxTaskID},
		"kind":           "guidance",
		"body":           "Yes, enable strict validation.",
	}, nil)

	waitForTaskTerminalStatus(mailboxTaskID, "completed")

	approvalScript := func(title string) string {
		return formatFakeScript(t,
			map[string]any{
				"permission": map[string]any{
					"toolCallId": "perm-call-1",
					"title":      title,
					"kind":       "execute",
					"options": []map[string]any{
						{"optionId": "allow_once", "name": "Allow once", "kind": "allow_once"},
						{"optionId": "reject_once", "name": "Reject", "kind": "reject_once"},
					},
					"as": "decision",
				},
			},
			map[string]any{
				"message": "permission outcome={{decision.outcome.outcome}} option={{decision.outcome.optionId}}",
			},
		)
	}

	var mcpApprovalSubmit struct {
		Tasks []protocol.Task `json:"tasks"`
	}
	mcpSession.callTool(t, "submit_tasks", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "approval-mcp-1",
		"tasks": []map[string]any{{
			"title":        "Approval via MCP",
			"instructions": "Request permission resolved via MCP\n" + approvalScript("Run database migration"),
			"requirements": map[string]any{
				"harnessIds": []string{"codex-cli"},
				"transports": []string{"acp-v1"},
				"labels":     []string{"macos"},
			},
		}},
	}, &mcpApprovalSubmit)
	mcpApprovalTaskID := mcpApprovalSubmit.Tasks[0].ID

	var pendingMCPApprovalID string
	require.Eventually(t, func() bool {
		var listResult struct {
			Approvals []protocol.ApprovalRequest `json:"approvals"`
		}
		mcpSession.callTool(t, "list_approvals", map[string]any{
			"threadId": threadID,
			"status":   "pending",
		}, &listResult)
		for _, item := range listResult.Approvals {
			if item.TaskID == mcpApprovalTaskID {
				pendingMCPApprovalID = item.ID
				return true
			}
		}
		return false
	}, 12*time.Second, 100*time.Millisecond)

	mcpSession.callTool(t, "resolve_approval", map[string]any{
		"approvalId": pendingMCPApprovalID,
		"status":     "approved",
		"optionId":   "allow_once",
	}, nil)
	waitForTaskTerminalStatus(mcpApprovalTaskID, "completed")

	var cliApprovalSubmit struct {
		Tasks []protocol.Task `json:"tasks"`
	}
	mcpSession.callTool(t, "submit_tasks", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "approval-cli-1",
		"tasks": []map[string]any{{
			"title":        "Approval via Operator CLI",
			"instructions": "Request permission resolved via CLI\n" + approvalScript("Deploy staging config"),
			"requirements": map[string]any{
				"harnessIds": []string{"claude-cli"},
				"transports": []string{"acp-v1"},
				"labels":     []string{"linux"},
			},
		}},
	}, &cliApprovalSubmit)
	cliApprovalTaskID := cliApprovalSubmit.Tasks[0].ID

	var pendingCLIApprovalID string
	require.Eventually(t, func() bool {
		approvalsJSON, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment,
			"approvals", "--thread", threadID, "--status", "pending", "--json",
		)
		var decoded struct {
			Approvals []protocol.ApprovalRequest `json:"approvals"`
		}
		if json.Unmarshal([]byte(approvalsJSON), &decoded) != nil {
			return false
		}
		for _, item := range decoded.Approvals {
			if item.TaskID == cliApprovalTaskID {
				pendingCLIApprovalID = item.ID
				return true
			}
		}
		return false
	}, 12*time.Second, 100*time.Millisecond)

	approveOutput, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment,
		"approve", pendingCLIApprovalID, "--option", "allow_once",
	)
	require.Contains(t, approveOutput, "approved")
	waitForTaskTerminalStatus(cliApprovalTaskID, "completed")

	cancelScript := formatFakeScript(t,
		map[string]any{
			"message": "starting long-running task before cancellation",
		},
		map[string]any{
			"hang": true,
		},
	)
	var cancelSubmit struct {
		Tasks []protocol.Task `json:"tasks"`
	}
	mcpSession.callTool(t, "submit_tasks", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "cancel-task-1",
		"tasks": []map[string]any{{
			"title":        "Long Running Cancellable Task",
			"instructions": "Hang until cancelled\n" + cancelScript,
			"requirements": map[string]any{
				"harnessIds": []string{"codex-cli"},
				"transports": []string{"acp-v1"},
				"labels":     []string{"macos"},
			},
		}},
	}, &cancelSubmit)
	cancellableTaskID := cancelSubmit.Tasks[0].ID

	require.Eventually(t, func() bool {
		detailText, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment, "thread", threadID, "--json")
		var parsed protocol.ThreadDetailResponse
		if json.Unmarshal([]byte(detailText), &parsed) != nil {
			return false
		}
		for _, activity := range parsed.Activities {
			if strings.Contains(activity.Message.Text, "starting long-running task") {
				return true
			}
		}
		return false
	}, 12*time.Second, 100*time.Millisecond)

	cancelOutput, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment, "cancel", cancellableTaskID)
	require.Contains(t, cancelOutput, "Cancelled task "+cancellableTaskID)
	waitForTaskTerminalStatus(cancellableTaskID, "cancelled")

	mcpSession.callTool(t, "release_instance", map[string]any{
		"instanceId":     residentInstanceID,
		"idempotencyKey": "release-mac-resident-1",
		"mode":           "drain",
	}, nil)
	require.Eventually(t, func() bool {
		var instanceState struct {
			Instance protocol.AgentInstance `json:"instance"`
		}
		mcpSession.callTool(t, "get_instance", map[string]any{"instanceId": residentInstanceID}, &instanceState)
		return instanceState.Instance.Status == "released"
	}, 10*time.Second, 100*time.Millisecond)

	workerLinux.stop()
	require.Eventually(t, func() bool {
		var inventory struct {
			Nodes []protocol.NodeDetail `json:"nodes"`
		}
		mcpSession.callTool(t, "get_execution_inventory", map[string]any{}, &inventory)
		for _, node := range inventory.Nodes {
			if node.Node.ID == "worker-linux" && !node.Connected {
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond)

	workerLinux = startWorkerLinux()
	require.Eventually(t, func() bool {
		var inventory struct {
			Nodes []protocol.NodeDetail `json:"nodes"`
		}
		mcpSession.callTool(t, "get_execution_inventory", map[string]any{}, &inventory)
		for _, node := range inventory.Nodes {
			if node.Node.ID == "worker-linux" && node.Connected && node.Synced {
				return true
			}
		}
		return false
	}, 12*time.Second, 100*time.Millisecond)

	masterProcess.stop()
	masterProcess = startMasterProcess()

	require.Eventually(t, func() bool {
		statusJSON, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment, "status", "--json")
		var status protocol.StatusResponse
		if json.Unmarshal([]byte(statusJSON), &status) != nil {
			return false
		}
		return status.ConnectedNodes == 2 && status.OpenThreads == 1
	}, 15*time.Second, 150*time.Millisecond, "workers failed to auto-reconnect after master restart")

	postRestartScript := formatFakeScript(t,
		map[string]any{
			"message": "completed after master restart and slave reconnect",
		},
	)
	var postRestartSubmit struct {
		Tasks []protocol.Task `json:"tasks"`
	}
	mcpSession.callTool(t, "submit_tasks", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "post-restart-task-1",
		"tasks": []map[string]any{{
			"title":        "Post-Restart Reconciliation Task",
			"instructions": "Run on reconnected Linux worker\n" + postRestartScript,
			"requirements": map[string]any{
				"harnessIds": []string{"codex-cli"},
				"transports": []string{"acp-v1"},
				"labels":     []string{"linux"},
			},
		}},
	}, &postRestartSubmit)
	waitForTaskTerminalStatus(postRestartSubmit.Tasks[0].ID, "completed")

	mcpSession.callTool(t, "update_thread", map[string]any{
		"threadId": threadID,
		"status":   "resolved",
		"summary":  "All multi-node E2E scenarios verified.",
	}, nil)

	mcpSession.callTool(t, "detach_thread", map[string]any{
		"threadId": threadID,
	}, nil)

	threadSummaryOutput, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment, "thread", threadID)
	require.Contains(t, threadSummaryOutput, "[resolved]")
	require.Contains(t, threadSummaryOutput, "Post-Restart Reconciliation Task")

	records := fakeHarnesses.readRecords(t)
	require.NotEmpty(t, records)
	sawLinuxVendorEnvironment := false
	for _, record := range records {
		if record["event"] != "start" {
			continue
		}
		environmentPresence, _ := record["environment"].(map[string]any)
		require.Equal(t, false, environmentPresence["ORCHESTRA_TOKEN"], "master ORCHESTRA_TOKEN must never leak into harness child environment")
		if environmentPresence["OPENAI_API_KEY"] == true {
			sawLinuxVendorEnvironment = true
		}
	}
	require.True(t, sawLinuxVendorEnvironment, "slave-local vendor credential stayed on worker-linux for local harness execution")
}

func generateLoopbackTLSCertificate(t *testing.T) (string, string) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "127.0.0.1",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	keyBytes, err := x509.MarshalECPrivateKey(privateKey)
	require.NoError(t, err)

	certificateDirectory := t.TempDir()
	certificatePath := filepath.Join(certificateDirectory, "master.crt")
	privateKeyPath := filepath.Join(certificateDirectory, "master.key")

	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	privateKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	require.NoError(t, os.WriteFile(certificatePath, certificatePEM, 0o600))
	require.NoError(t, os.WriteFile(privateKeyPath, privateKeyPEM, 0o600))
	return certificatePath, privateKeyPath
}

func TestTLSEndToEndWithEmbeddedLocalWorker(t *testing.T) {
	binaries := ensureCompiledBinaries(t)
	fakeHarnesses := installFakeHarnesses(t, binaries.fakeHarnessPath)
	certificatePath, privateKeyPath := generateLoopbackTLSCertificate(t)

	defaultManifest, err := setup.LoadDefaultManifest()
	require.NoError(t, err)
	adapterOnlyManifest := setup.Manifest{
		ManifestVersion: defaultManifest.ManifestVersion,
		Components:      defaultManifest.ComponentsOfKind(setup.ComponentKindACPAdapter),
	}
	adapterManifestBytes, err := json.MarshalIndent(adapterOnlyManifest, "", "  ")
	require.NoError(t, err)
	adapterManifestPath := filepath.Join(t.TempDir(), "adapters-manifest.json")
	require.NoError(t, os.WriteFile(adapterManifestPath, adapterManifestBytes, 0o600))

	const bearerToken = "tls-system-test-token-445566"
	listenAddress := allocateFreeLoopbackAddress(t)
	masterHTTPSURL := "https://" + listenAddress
	masterDataDirectory := filepath.Join(t.TempDir(), "tls-master-sqlite")

	localWorkerRawRoot := filepath.Join(t.TempDir(), "local-worker-root")
	remoteWorkerRawRoot := filepath.Join(t.TempDir(), "remote-worker-root")
	require.NoError(t, os.MkdirAll(localWorkerRawRoot, 0o755))
	require.NoError(t, os.MkdirAll(remoteWorkerRawRoot, 0o755))

	localCanonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{localWorkerRawRoot})
	require.NoError(t, err)
	remoteCanonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{remoteWorkerRawRoot})
	require.NoError(t, err)

	baseEnvironment := buildWorkerEnvironment(
		fakeHarnesses,
		"ORCHESTRA_ADAPTER_MANIFEST="+adapterManifestPath,
		"ORCHESTRA_DATA_ROOT="+filepath.Join(t.TempDir(), "embedded-worker-data"),
	)

	masterProcess := startManagedProcess(t, "tls-orchestra-master", binaries.orchestraPath, []string{
		"master",
		"--listen", listenAddress,
		"--token", bearerToken,
		"--data-dir", masterDataDirectory,
		"--tls-cert", certificatePath,
		"--tls-key", privateKeyPath,
		"--with-local-worker",
		"--worker-id", "embedded-main",
		"--worker-name", "Embedded Main Worker",
		"--workspace-root", localCanonicalRoots[0],
		"--worker-label", "embedded",
		"--claude-acp-auth-mode", "local-subscription",
		"--acp-adapter", fakeHarnesses.codexACPArgument,
	}, baseEnvironment)

	tlsHTTPClient, err := protocol.BuildHTTPClient(certificatePath, false, 2*time.Second)
	require.NoError(t, err)
	if !assert.Eventually(t, func() bool {
		response, requestErr := tlsHTTPClient.Get(masterHTTPSURL + "/healthz")
		if requestErr != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 10*time.Second, 50*time.Millisecond) {
		t.Fatalf("TLS master failed to become healthy; logs:\n%s", masterProcess.logs())
	}

	remoteSlave := startManagedProcess(t, "tls-remote-slave", binaries.orchestraPath, []string{
		"slave",
		"--endpoint", masterHTTPSURL,
		"--tls-ca", certificatePath,
		"--token", bearerToken,
		"--id", "remote-tls-worker",
		"--name", "Remote TLS Worker",
		"--workspace-root", remoteCanonicalRoots[0],
		"--label", "remote-tls",
		"--data-root", filepath.Join(t.TempDir(), "remote-tls-data"),
		"--claude-acp-auth-mode", "local-subscription",
		"--acp-adapter", fakeHarnesses.codexACPArgument,
	}, buildWorkerEnvironment(fakeHarnesses, "ORCHESTRA_ADAPTER_MANIFEST="+adapterManifestPath))

	mcpSession := startMCPStdioSession(t, binaries.orchestraPath, masterHTTPSURL, bearerToken, "tls-orchestrator", "--tls-ca", certificatePath)

	if !assert.Eventually(t, func() bool {
		var inventory struct {
			Nodes []protocol.NodeDetail `json:"nodes"`
		}
		mcpSession.callTool(t, "get_execution_inventory", map[string]any{}, &inventory)
		readyNodes := 0
		for _, detail := range inventory.Nodes {
			if detail.Connected && detail.Synced {
				readyNodes++
			}
		}
		return readyNodes == 2
	}, 12*time.Second, 100*time.Millisecond) {
		t.Fatalf("TLS workers failed to sync;\nmaster:\n%s\nremote:\n%s", masterProcess.logs(), remoteSlave.logs())
	}

	cliEnvironment := buildWorkerEnvironment(
		fakeHarnesses,
		"ORCHESTRA_ENDPOINT="+masterHTTPSURL,
		"ORCHESTRA_TOKEN="+bearerToken,
		"ORCHESTRA_TLS_CA="+certificatePath,
	)
	statusOutput, _ := invokeOrchestraCLI(t, binaries.orchestraPath, cliEnvironment, "status")
	require.Contains(t, statusOutput, "2 connected / 2 total")

	var createdThread struct {
		Thread protocol.Thread `json:"thread"`
	}
	mcpSession.callTool(t, "create_thread", map[string]any{
		"title": "TLS + Embedded Worker Verification",
	}, &createdThread)

	script := formatFakeScript(t, map[string]any{"message": "tls task executed cleanly"})
	var submitted struct {
		Tasks []protocol.Task `json:"tasks"`
	}
	mcpSession.callTool(t, "submit_tasks", map[string]any{
		"threadId":       createdThread.Thread.ID,
		"idempotencyKey": "tls-batch-1",
		"tasks": []map[string]any{
			{
				"title":        "Embedded Worker Task",
				"instructions": "Run on embedded worker\n" + script,
				"requirements": map[string]any{
					"harnessIds": []string{"codex-cli"},
					"transports": []string{"acp-v1"},
					"labels":     []string{"embedded"},
				},
			},
			{
				"title":        "Remote WSS Worker Task",
				"instructions": "Run on remote WSS worker\n" + script,
				"requirements": map[string]any{
					"harnessIds": []string{"codex-cli"},
					"transports": []string{"acp-v1"},
					"labels":     []string{"remote-tls"},
				},
			},
		},
	}, &submitted)
	require.Len(t, submitted.Tasks, 2)

	require.Eventually(t, func() bool {
		var threadContext struct {
			Tasks []protocol.Task `json:"tasks"`
		}
		mcpSession.callTool(t, "get_thread_context", map[string]any{"threadId": createdThread.Thread.ID}, &threadContext)
		completedCount := 0
		for _, candidate := range threadContext.Tasks {
			if candidate.Status == "completed" {
				completedCount++
			}
		}
		return completedCount == 2
	}, 15*time.Second, 100*time.Millisecond)
}

func TestAntigravityBuiltInACPAndNativeEndToEnd(t *testing.T) {
	binaries := ensureCompiledBinaries(t)
	fakeHarnesses := installFakeHarnesses(t, binaries.fakeHarnessPath)
	fakeHarnesses.installAntigravity(t, binaries.fakeHarnessPath)

	const bearerToken = "agy-system-test-token-778899"
	listenAddress := allocateFreeLoopbackAddress(t)
	masterBaseURL := "http://" + listenAddress
	masterDataDirectory := filepath.Join(t.TempDir(), "agy-master-sqlite")

	rawWorkspaceRoot := filepath.Join(t.TempDir(), "agy-workspace-root")
	require.NoError(t, os.MkdirAll(rawWorkspaceRoot, 0o755))
	canonicalRoots, err := slave.CanonicalizeWorkspaceRoots([]string{rawWorkspaceRoot})
	require.NoError(t, err)
	canonicalRepositoryPath := filepath.Join(canonicalRoots[0], "repo")
	initializeGitRepository(t, canonicalRepositoryPath)

	workerDataRoot := filepath.Join(t.TempDir(), "agy-worker-data")
	workerEnvironment := buildWorkerEnvironment(fakeHarnesses, "ORCHESTRA_DATA_ROOT="+workerDataRoot)

	masterProcess := startManagedProcess(t, "agy-master", binaries.orchestraPath, []string{
		"master",
		"--listen", listenAddress,
		"--token", bearerToken,
		"--data-dir", masterDataDirectory,
	}, workerEnvironment)
	waitForMasterReady(t, masterBaseURL, masterProcess)

	doctorJSONOutput, _ := invokeOrchestraCLI(
		t,
		binaries.orchestraPath,
		append(slices.Clone(workerEnvironment), "ORCHESTRA_ENDPOINT="+masterBaseURL, "ORCHESTRA_TOKEN="+bearerToken),
		"doctor",
		"--data-root", workerDataRoot,
		"--endpoint", masterBaseURL,
		"--json",
	)
	require.Contains(t, doctorJSONOutput, `"antigravity-cli"`)
	require.Contains(t, doctorJSONOutput, `"acpLaunchReady": true`)

	workerProcess := startManagedProcess(t, "agy-slave", binaries.orchestraPath, []string{
		"slave",
		"--endpoint", masterBaseURL,
		"--token", bearerToken,
		"--id", "worker-agy",
		"--name", "Antigravity Worker",
		"--workspace-root", canonicalRoots[0],
		"--data-root", workerDataRoot,
	}, workerEnvironment)

	mcpSession := startMCPStdioSession(t, binaries.orchestraPath, masterBaseURL, bearerToken, "agy-orchestrator")

	var antigravityProfile protocol.HarnessProfile
	if !assert.Eventually(t, func() bool {
		var inventory struct {
			Nodes []protocol.NodeDetail `json:"nodes"`
		}
		mcpSession.callTool(t, "get_execution_inventory", map[string]any{}, &inventory)
		for _, detail := range inventory.Nodes {
			if detail.Node.ID != "worker-agy" || !detail.Connected || !detail.Synced {
				continue
			}
			for _, profile := range detail.Node.Harnesses {
				if profile.ID == protocol.HarnessAntigravityCLI && profile.Available {
					antigravityProfile = profile
					return true
				}
			}
		}
		return false
	}, 12*time.Second, 100*time.Millisecond) {
		t.Fatalf("antigravity worker failed to register;\nmaster:\n%s\nworker:\n%s", masterProcess.logs(), workerProcess.logs())
	}

	require.Contains(t, antigravityProfile.Transports, protocol.TransportACP)
	require.Contains(t, antigravityProfile.Transports, protocol.TransportNativeCLI)
	require.NotNil(t, antigravityProfile.ACP)
	require.True(t, antigravityProfile.ACP.ResumeSession)
	require.True(t, antigravityProfile.ACP.Mcp.HTTP)
	require.Contains(t, antigravityProfile.Models, "gemini-4-argon-medium")
	require.Contains(t, antigravityProfile.Models, "gemini-3.8-flash-medium")

	var createdThread struct {
		Thread protocol.Thread `json:"thread"`
	}
	mcpSession.callTool(t, "create_thread", map[string]any{
		"title": "Antigravity Built-In ACP Continuity & Native Verification",
	}, &createdThread)
	threadID := createdThread.Thread.ID

	var spawnedInstance protocol.InstanceLifecycleResult
	mcpSession.callTool(t, "spawn_instance", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "spawn-agy-resident-1",
		"purpose": map[string]any{
			"name":    "agy-specialist",
			"summary": "Resident Antigravity session over built-in ACP bridge",
		},
		"requirements": map[string]any{
			"harnessIds": []string{protocol.HarnessAntigravityCLI},
			"models":     []string{"gemini-3.8-flash-medium"},
			"transports": []string{protocol.TransportACP},
			"workspace": map[string]any{
				"path":     canonicalRepositoryPath,
				"writable": true,
			},
		},
		"idleTimeoutSeconds": 300,
	}, &spawnedInstance)
	residentInstanceID := spawnedInstance.Instance.ID

	require.Eventually(t, func() bool {
		var instanceState struct {
			Instance protocol.AgentInstance `json:"instance"`
		}
		mcpSession.callTool(t, "get_instance", map[string]any{"instanceId": residentInstanceID}, &instanceState)
		return instanceState.Instance.Status == "ready" || instanceState.Instance.Status == "idle"
	}, 10*time.Second, 100*time.Millisecond)

	turnOneScript := formatFakeScript(t,
		map[string]any{"call": "get_task_context", "as": "context"},
		map[string]any{"thought": "inspecting thread context in turn 1"},
		map[string]any{"message": "agy-turn1 conv={{session}} task={{context.task.id}}"},
	)
	var firstSubmitted struct {
		Tasks []protocol.Task `json:"tasks"`
	}
	mcpSession.callTool(t, "submit_tasks", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "agy-turn-1",
		"tasks": []map[string]any{{
			"clientTaskId": "agy-t1",
			"title":        "Antigravity ACP Turn 1",
			"instructions": "Run first Antigravity turn\n" + turnOneScript,
			"instanceId":   residentInstanceID,
			"requirements": map[string]any{
				"harnessIds": []string{protocol.HarnessAntigravityCLI},
				"models":     []string{"gemini-3.8-flash-medium"},
				"transports": []string{protocol.TransportACP},
				"workspace":  map[string]any{"path": canonicalRepositoryPath, "writable": true},
			},
		}},
	}, &firstSubmitted)
	firstTaskID := firstSubmitted.Tasks[0].ID

	var firstRun protocol.Run
	require.Eventually(t, func() bool {
		var contextSnapshot struct {
			Tasks []protocol.Task `json:"tasks"`
			Runs  []protocol.Run  `json:"runs"`
		}
		mcpSession.callTool(t, "get_thread_context", map[string]any{"threadId": threadID}, &contextSnapshot)
		for _, candidate := range contextSnapshot.Tasks {
			if candidate.ID == firstTaskID && candidate.Status == "completed" && candidate.Assignment != nil {
				for _, runCandidate := range contextSnapshot.Runs {
					if runCandidate.ID == candidate.Assignment.RunID {
						firstRun = runCandidate
						return true
					}
				}
			}
		}
		return false
	}, 15*time.Second, 100*time.Millisecond)

	require.NotNil(t, firstRun.TransportSelection)
	require.Equal(t, protocol.TransportACP, firstRun.TransportSelection.SelectedTransport)
	require.NotNil(t, firstRun.TransportSelection.Adapter)
	require.Equal(t, "antigravity-acp", firstRun.TransportSelection.Adapter.ID)
	require.Equal(t, protocol.ACPAdapterSourceBuiltIn, firstRun.TransportSelection.Adapter.Source)
	require.Contains(t, firstRun.Output, "agy-turn1 conv=agy-conv-")

	turnTwoScript := formatFakeScript(t,
		map[string]any{"call": "get_task_context", "as": "context"},
		map[string]any{"message": "agy-turn2 conv={{session}} task={{context.task.id}}"},
	)
	var secondSubmitted struct {
		Tasks []protocol.Task `json:"tasks"`
	}
	mcpSession.callTool(t, "submit_tasks", map[string]any{
		"threadId":       threadID,
		"idempotencyKey": "agy-turn-2",
		"tasks": []map[string]any{
			{
				"clientTaskId": "agy-t2",
				"title":        "Antigravity ACP Turn 2 (Resumed)",
				"instructions": "Run resumed Antigravity turn\n" + turnTwoScript,
				"instanceId":   residentInstanceID,
				"requirements": map[string]any{
					"harnessIds": []string{protocol.HarnessAntigravityCLI},
					"models":     []string{"gemini-3.8-flash-medium"},
					"transports": []string{protocol.TransportACP},
					"workspace":  map[string]any{"path": canonicalRepositoryPath, "writable": true},
				},
			},
			{
				"clientTaskId": "agy-native",
				"title":        "Antigravity Native CLI Turn",
				"instructions": "Run native Antigravity CLI turn\n" + formatFakeScript(t,
					map[string]any{"call": "get_task_context", "as": "context"},
					map[string]any{"message": "agy-native task={{context.task.id}}"},
				),
				"requirements": map[string]any{
					"harnessIds": []string{protocol.HarnessAntigravityCLI},
					"transports": []string{protocol.TransportNativeCLI},
					"workspace":  map[string]any{"path": canonicalRepositoryPath, "writable": true},
				},
			},
		},
	}, &secondSubmitted)
	require.Len(t, secondSubmitted.Tasks, 2)
	secondTaskID := secondSubmitted.Tasks[0].ID
	nativeTaskID := secondSubmitted.Tasks[1].ID

	var secondRun protocol.Run
	var nativeRun protocol.Run
	require.Eventually(t, func() bool {
		var contextSnapshot struct {
			Tasks []protocol.Task `json:"tasks"`
			Runs  []protocol.Run  `json:"runs"`
		}
		mcpSession.callTool(t, "get_thread_context", map[string]any{"threadId": threadID}, &contextSnapshot)
		foundSecond := false
		foundNative := false
		for _, candidate := range contextSnapshot.Tasks {
			if candidate.Status != "completed" || candidate.Assignment == nil {
				continue
			}
			for _, runCandidate := range contextSnapshot.Runs {
				if runCandidate.ID != candidate.Assignment.RunID {
					continue
				}
				if candidate.ID == secondTaskID {
					secondRun = runCandidate
					foundSecond = true
				}
				if candidate.ID == nativeTaskID {
					nativeRun = runCandidate
					foundNative = true
				}
			}
		}
		return foundSecond && foundNative
	}, 15*time.Second, 100*time.Millisecond)

	require.NotNil(t, firstRun.ProviderSessionID)
	require.NotNil(t, secondRun.ProviderSessionID)
	require.Equal(t, *firstRun.ProviderSessionID, *secondRun.ProviderSessionID)
	require.NotNil(t, secondRun.SessionBindingID)
	require.Contains(t, secondRun.Output, "agy-turn2 conv=")
	firstConversationToken, _, _ := strings.Cut(strings.TrimPrefix(firstRun.Output, "agy-turn1 conv="), " ")
	secondConversationToken, _, _ := strings.Cut(strings.TrimPrefix(secondRun.Output, "agy-turn2 conv="), " ")
	require.NotEmpty(t, firstConversationToken)
	require.Equal(t, firstConversationToken, secondConversationToken)

	require.NotNil(t, nativeRun.TransportSelection)
	require.Equal(t, protocol.TransportNativeCLI, nativeRun.TransportSelection.SelectedTransport)
	require.Contains(t, nativeRun.Output, "agy-native task="+nativeTaskID)

	records := fakeHarnesses.readRecords(t)
	sawResumedAgyRun := false
	for _, record := range records {
		if record["role"] == "agy" && record["event"] == "native-run" && record["resumed"] == true {
			sawResumedAgyRun = true
			require.Equal(t, firstConversationToken, record["conversationId"])
			require.Equal(t, true, record["mcp"])
			require.Equal(t, "gemini-3.8-flash-medium", record["model"])
		}
	}
	require.True(t, sawResumedAgyRun)
}
