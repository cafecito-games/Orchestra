package mcpserver

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/safepath"
)

const maximumArtifactSize = protocol.MaximumArtifactSizeBytes

type Caller func(operationContext context.Context, runID string, method string, arguments json.RawMessage) (json.RawMessage, error)

type ToolError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (err *ToolError) Error() string { return err.Code + ": " + err.Message }

type UploadError struct {
	StatusCode int
	Uncertain  bool
}

func (err *UploadError) Error() string {
	if err.Uncertain {
		return "artifact upload did not complete cleanly"
	}
	if err.StatusCode > 0 {
		return fmt.Sprintf("artifact upload failed with status %d", err.StatusCode)
	}
	return "artifact upload failed"
}

type Uploader func(operationContext context.Context, uploadPath string, content io.Reader, size int64) error

type Configuration struct {
	URL         string
	Token       string
	CanDelegate bool
	Connected   <-chan struct{}
}

type grant struct {
	token       string
	runID       string
	workspace   string
	canDelegate bool
	connection  *connectionSignal
}

type connectionSignal struct {
	once      sync.Once
	connected chan struct{}
}

func (signal *connectionSignal) mark() {
	signal.once.Do(func() { close(signal.connected) })
}

type Server struct {
	caller   Caller
	uploader Uploader

	mutex   sync.RWMutex
	grants  []grant
	url     string
	server  *http.Server
	handler http.Handler
}

func New(caller Caller, uploader Uploader) *Server {
	server := &Server{caller: caller, uploader: uploader}
	server.handler = mcp.NewStreamableHTTPHandler(
		func(request *http.Request) *mcp.Server {
			activeGrant, ok := server.requestGrant(request)
			if !ok {
				return nil
			}
			return server.newProtocolServer(activeGrant)
		},
		&mcp.StreamableHTTPOptions{
			Stateless:           true,
			JSONResponse:        true,
			MaxRequestBodyBytes: 1 << 20,
		},
	)
	return server
}

func (server *Server) Start(operationContext context.Context) error {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("start MCP listener: %w", err)
	}
	server.mutex.Lock()
	server.url = "http://" + listener.Addr().String() + "/mcp"
	server.server = &http.Server{Handler: http.HandlerFunc(server.serveHTTP), ReadHeaderTimeout: 5 * time.Second}
	server.mutex.Unlock()
	go func() {
		<-operationContext.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.server.Shutdown(shutdownContext)
	}()
	go func() { _ = server.server.Serve(listener) }()
	return nil
}

func (server *Server) Grant(runID, workspace string, canDelegate bool) (Configuration, error) {
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return Configuration{}, fmt.Errorf("create MCP capability: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes)
	server.mutex.Lock()
	defer server.mutex.Unlock()
	connection := &connectionSignal{connected: make(chan struct{})}
	server.grants = append(server.grants, grant{
		token:       token,
		runID:       runID,
		workspace:   workspace,
		canDelegate: canDelegate,
		connection:  connection,
	})
	return Configuration{URL: server.url, Token: token, CanDelegate: canDelegate, Connected: connection.connected}, nil
}

func (server *Server) Revoke(token string) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	remaining := server.grants[:0]
	for _, entry := range server.grants {
		if subtle.ConstantTimeCompare([]byte(entry.token), []byte(token)) != 1 {
			remaining = append(remaining, entry)
		}
	}
	server.grants = remaining
}

func (server *Server) lookupGrant(token string) (grant, bool) {
	if token == "" {
		return grant{}, false
	}
	server.mutex.RLock()
	defer server.mutex.RUnlock()
	var matched grant
	found := false
	for _, entry := range server.grants {
		if subtle.ConstantTimeCompare([]byte(entry.token), []byte(token)) == 1 {
			matched = entry
			found = true
		}
	}
	return matched, found
}

func (server *Server) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/mcp" {
		http.NotFound(writer, request)
		return
	}
	if !validOrigin(request.Header.Get("Origin")) {
		http.Error(writer, "invalid origin", http.StatusForbidden)
		return
	}
	if _, ok := server.requestGrant(request); !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	server.handler.ServeHTTP(writer, request)
}

func (server *Server) requestGrant(request *http.Request) (grant, bool) {
	header := request.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return grant{}, false
	}
	return server.lookupGrant(strings.TrimPrefix(header, "Bearer "))
}

func validOrigin(raw string) bool {
	if raw == "" {
		return true
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := parsed.Hostname()
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func (server *Server) newProtocolServer(activeGrant grant) *mcp.Server {
	protocolServer := mcp.NewServer(
		&mcp.Implementation{Name: "orchestra_hub", Version: protocol.ProtocolVersion},
		&mcp.ServerOptions{
			Instructions: instructions,
			Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}},
		},
	)
	protocolServer.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(operationContext context.Context, method string, request mcp.Request) (mcp.Result, error) {
			if method == "tools/call" && !activeGrant.canDelegate {
				callRequest, ok := request.(*mcp.CallToolRequest)
				if ok && callRequest.Params != nil && protocol.IsDelegationHubToolName(callRequest.Params.Name) {
					return toolErrorResult(&ToolError{Code: "forbidden", Message: "This run is not allowed to delegate tasks"}), nil
				}
			}
			result, err := next(operationContext, method, request)
			if method == "tools/list" && err == nil {
				activeGrant.connection.mark()
			}
			return result, err
		}
	})
	for _, name := range ToolNames(activeGrant.canDelegate) {
		definition := definitions[name]
		destructive := false
		protocolServer.AddTool(&mcp.Tool{
			Name:         name,
			Title:        definition.title,
			Description:  definition.description,
			InputSchema:  definition.input,
			OutputSchema: definition.output,
			Annotations: &mcp.ToolAnnotations{
				ReadOnlyHint:    definition.readOnly,
				DestructiveHint: &destructive,
				IdempotentHint:  true,
			},
		}, func(operationContext context.Context, request *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return server.callTool(operationContext, activeGrant, name, request.Params.Arguments), nil
		})
	}
	return protocolServer
}

func (server *Server) callTool(operationContext context.Context, activeGrant grant, name string, arguments json.RawMessage) *mcp.CallToolResult {
	if len(arguments) == 0 || string(arguments) == "null" {
		arguments = json.RawMessage(`{}`)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(arguments, &object); err != nil || object == nil {
		return toolErrorResult(&ToolError{Code: "invalid_arguments", Message: "Tool arguments must be a JSON object"})
	}
	var (
		result json.RawMessage
		err    error
	)
	if name == "post_artifact" {
		result, err = server.postArtifact(operationContext, activeGrant, arguments)
	} else {
		result, err = server.caller(operationContext, activeGrant.runID, name, arguments)
	}
	if err != nil {
		return toolErrorResult(asToolError(err))
	}
	var structured map[string]any
	if err := json.Unmarshal(result, &structured); err != nil || structured == nil {
		return toolErrorResult(&ToolError{Code: "invalid_result", Message: "Master returned an invalid tool result", Retryable: true})
	}
	return &mcp.CallToolResult{
		Content:           []mcp.Content{&mcp.TextContent{Text: string(result)}},
		StructuredContent: structured,
	}
}

type postArtifactArguments struct {
	RelativePath   string `json:"relativePath"`
	Name           string `json:"name"`
	Title          string `json:"title"`
	Kind           string `json:"kind"`
	MediaType      string `json:"mediaType"`
	Summary        string `json:"summary,omitempty"`
	IdempotencyKey string `json:"idempotencyKey"`
}

func (server *Server) postArtifact(operationContext context.Context, activeGrant grant, raw json.RawMessage) (json.RawMessage, error) {
	var arguments postArtifactArguments
	if err := json.Unmarshal(raw, &arguments); err != nil {
		return nil, errors.New("invalid post_artifact arguments")
	}
	file, relativePath, size, digest, err := openArtifact(activeGrant.workspace, arguments.RelativePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	artifactName := strings.TrimSpace(arguments.Name)
	if artifactName == "" {
		artifactName = strings.TrimSpace(arguments.Title)
	}
	requestArguments, err := json.Marshal(map[string]any{
		"relativePath":   relativePath,
		"name":           artifactName,
		"title":          arguments.Title,
		"kind":           arguments.Kind,
		"mediaType":      arguments.MediaType,
		"summary":        arguments.Summary,
		"idempotencyKey": arguments.IdempotencyKey,
		"sizeBytes":      size,
		"size":           size,
		"sha256":         digest,
	})
	if err != nil {
		return nil, err
	}
	result, err := server.caller(operationContext, activeGrant.runID, "post_artifact", requestArguments)
	if err != nil {
		return nil, err
	}
	var registration struct {
		Artifact   map[string]any `json:"artifact"`
		UploadPath string         `json:"uploadPath"`
		Upload     struct {
			Path string `json:"path"`
		} `json:"upload"`
	}
	if err := json.Unmarshal(result, &registration); err != nil {
		return nil, errors.New("master returned an invalid artifact registration")
	}
	uploadPath := registration.Upload.Path
	if uploadPath == "" {
		uploadPath = registration.UploadPath
	}
	if uploadPath == "" {
		return nil, errors.New("master returned an invalid artifact registration")
	}
	if uploaded, _ := registration.Artifact["uploaded"].(bool); uploaded {
		return json.Marshal(map[string]any{"artifact": registration.Artifact})
	}
	if server.uploader == nil {
		return nil, errors.New("artifact upload is unavailable")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	if err := server.uploader(operationContext, uploadPath, file, size); err != nil {
		var uploadFailure *UploadError
		if errors.As(err, &uploadFailure) && uploadFailure.Uncertain {
			return nil, &ToolError{Code: "upload_uncertain", Message: uploadFailure.Error(), Retryable: true}
		}
		return nil, fmt.Errorf("upload artifact: %w", err)
	}
	registration.Artifact["uploaded"] = true
	return json.Marshal(map[string]any{"artifact": registration.Artifact})
}

func openArtifact(workspace, requested string) (*os.File, string, int64, string, error) {
	if requested == "" || filepath.IsAbs(requested) {
		return nil, "", 0, "", errors.New("relativePath must be relative to the run workspace")
	}
	workspacePath, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return nil, "", 0, "", fmt.Errorf("resolve workspace: %w", err)
	}
	candidate := filepath.Join(workspacePath, filepath.FromSlash(requested))
	verifiedPath, _, err := safepath.VerifyRegularFileWithinRoot(workspacePath, candidate)
	if err != nil {
		return nil, "", 0, "", fmt.Errorf("verify artifact path: %w", err)
	}
	relative, err := filepath.Rel(workspacePath, verifiedPath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return nil, "", 0, "", errors.New("artifact is outside the run workspace")
	}
	file, err := os.Open(verifiedPath)
	if err != nil {
		return nil, "", 0, "", err
	}
	information, err := file.Stat()
	if err != nil || !information.Mode().IsRegular() {
		file.Close()
		return nil, "", 0, "", errors.New("artifact must be a regular file")
	}
	if information.Size() > maximumArtifactSize {
		file.Close()
		return nil, "", 0, "", errors.New("artifact must be at most 10 MiB")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		file.Close()
		return nil, "", 0, "", err
	}
	return file, filepath.ToSlash(relative), information.Size(), hex.EncodeToString(hash.Sum(nil)), nil
}

func asToolError(err error) *ToolError {
	var typed *ToolError
	if errors.As(err, &typed) {
		return typed
	}
	return &ToolError{Code: "tool_failed", Message: err.Error()}
}

func toolErrorResult(failure *ToolError) *mcp.CallToolResult {
	text, _ := json.Marshal(map[string]*ToolError{"error": failure})
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(text)}},
		IsError: true,
	}
}
