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

	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/cafecito-games/Orchestra/internal/safepath"
)

const (
	protocolVersion     = "2025-06-18"
	maximumArtifactSize = protocol.MaximumArtifactSizeBytes
)

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

	mutex  sync.RWMutex
	grants []grant
	url    string
	server *http.Server
}

func New(caller Caller, uploader Uploader) *Server {
	return &Server{caller: caller, uploader: uploader}
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

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type callParameters struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (server *Server) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/mcp" {
		http.NotFound(writer, request)
		return
	}
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !validOrigin(request.Header.Get("Origin")) {
		http.Error(writer, "invalid origin", http.StatusForbidden)
		return
	}
	header := request.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	token := strings.TrimPrefix(header, "Bearer ")
	activeGrant, ok := server.lookupGrant(token)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, 1<<20)
	var message rpcRequest
	if err := json.NewDecoder(request.Body).Decode(&message); err != nil || message.JSONRPC != "2.0" || message.Method == "" {
		writeRPC(writer, rpcResponse{JSONRPC: "2.0", ID: message.ID, Error: &rpcError{Code: -32600, Message: "Invalid request"}})
		return
	}
	if len(message.ID) == 0 {
		writer.WriteHeader(http.StatusAccepted)
		return
	}

	switch message.Method {
	case "initialize":
		writeRPC(writer, rpcResponse{JSONRPC: "2.0", ID: message.ID, Result: map[string]any{
			"protocolVersion": protocolVersion,
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]string{"name": "orchestra_hub", "version": protocol.ProtocolVersion},
			"instructions":    instructions,
		}})
	case "tools/list":
		writeRPC(writer, rpcResponse{JSONRPC: "2.0", ID: message.ID, Result: map[string]any{"tools": tools(activeGrant.canDelegate)}})
		activeGrant.connection.mark()
	case "tools/call":
		server.callTool(writer, request, message, activeGrant)
	default:
		writeRPC(writer, rpcResponse{JSONRPC: "2.0", ID: message.ID, Error: &rpcError{Code: -32601, Message: "Method not found"}})
	}
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

func (server *Server) callTool(writer http.ResponseWriter, request *http.Request, message rpcRequest, activeGrant grant) {
	var parameters callParameters
	if err := json.Unmarshal(message.Params, &parameters); err != nil || parameters.Name == "" {
		writeRPC(writer, rpcResponse{JSONRPC: "2.0", ID: message.ID, Error: &rpcError{Code: -32602, Message: "Invalid tool arguments"}})
		return
	}
	if !protocol.IsHubToolName(parameters.Name) {
		writeRPC(writer, rpcResponse{JSONRPC: "2.0", ID: message.ID, Error: &rpcError{Code: -32602, Message: "Unknown tool"}})
		return
	}
	if protocol.IsDelegationHubToolName(parameters.Name) && !activeGrant.canDelegate {
		writeToolError(writer, message.ID, &ToolError{Code: "forbidden", Message: "This run is not allowed to delegate tasks"})
		return
	}
	arguments := parameters.Arguments
	if len(arguments) == 0 || string(arguments) == "null" {
		arguments = json.RawMessage(`{}`)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(arguments, &object); err != nil || object == nil {
		writeToolError(writer, message.ID, &ToolError{Code: "invalid_arguments", Message: "Tool arguments must be a JSON object"})
		return
	}
	var (
		result json.RawMessage
		err    error
	)
	if parameters.Name == "post_artifact" {
		result, err = server.postArtifact(request.Context(), activeGrant, arguments)
	} else {
		result, err = server.caller(request.Context(), activeGrant.runID, parameters.Name, arguments)
	}
	if err != nil {
		writeToolError(writer, message.ID, asToolError(err))
		return
	}
	var structured map[string]any
	if err := json.Unmarshal(result, &structured); err != nil || structured == nil {
		writeToolError(writer, message.ID, &ToolError{Code: "invalid_result", Message: "Master returned an invalid tool result", Retryable: true})
		return
	}
	writeRPC(writer, rpcResponse{JSONRPC: "2.0", ID: message.ID, Result: map[string]any{
		"content":           []map[string]string{{"type": "text", "text": string(result)}},
		"structuredContent": structured,
		"isError":           false,
	}})
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

func writeRPC(writer http.ResponseWriter, response rpcResponse) {
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(response)
}

func asToolError(err error) *ToolError {
	var typed *ToolError
	if errors.As(err, &typed) {
		return typed
	}
	return &ToolError{Code: "tool_failed", Message: err.Error()}
}

func writeToolError(writer http.ResponseWriter, id json.RawMessage, failure *ToolError) {
	text, _ := json.Marshal(map[string]*ToolError{"error": failure})
	writeRPC(writer, rpcResponse{JSONRPC: "2.0", ID: id, Result: map[string]any{
		"content": []map[string]string{{"type": "text", "text": string(text)}},
		"isError": true,
	}})
}
