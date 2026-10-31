// Package localstate manages local machine discovery state (connection.json),
// persisted master bearer tokens, and zero-secret MCP configuration across
// local coding agent harnesses.
package localstate

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ConnectionState struct {
	Endpoint      string `json:"endpoint"`
	AuthMode      string `json:"authMode,omitempty"`
	Token         string `json:"token,omitempty"`
	TLSCACert     string `json:"tlsCaCert,omitempty"`
	TLSInsecure   bool   `json:"tlsInsecure,omitempty"`
	DataDirectory string `json:"dataDir,omitempty"`
	ProcessID     int    `json:"pid,omitempty"`
	UpdatedAt     string `json:"updatedAt"`
}

func IsDisabledConnectionFilePath(value string) bool {
	trimmed := strings.TrimSpace(value)
	return trimmed == "" ||
		strings.EqualFold(trimmed, "none") ||
		strings.EqualFold(trimmed, "off") ||
		strings.EqualFold(trimmed, "false")
}

// ResolveConnectionFilePath returns the local connection discovery file path.
// When a custom lookupEnvironment omits HOME, USERPROFILE, ORCHESTRA_DATA_ROOT,
// and ORCHESTRA_CONNECTION_FILE (such as isolated unit tests), it returns ""
// so tests never read or modify the operator's real home directory.
func ResolveConnectionFilePath(lookupEnvironment func(string) (string, bool)) string {
	useOSFallback := lookupEnvironment == nil
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}

	homeDirectory := resolveHomeDirectory(lookupEnvironment, useOSFallback)

	if explicitPath, found := lookupEnvironment("ORCHESTRA_CONNECTION_FILE"); found {
		if IsDisabledConnectionFilePath(explicitPath) {
			return ""
		}
		return expandHomePrefix(strings.TrimSpace(explicitPath), homeDirectory)
	}

	if dataRoot, found := lookupEnvironment("ORCHESTRA_DATA_ROOT"); found && strings.TrimSpace(dataRoot) != "" {
		return filepath.Join(expandHomePrefix(strings.TrimSpace(dataRoot), homeDirectory), "connection.json")
	}

	if homeDirectory == "" {
		return ""
	}
	return filepath.Join(homeDirectory, ".local", "share", "orchestra", "connection.json")
}

func WriteConnectionState(filePath string, state ConnectionState) error {
	if IsDisabledConnectionFilePath(filePath) {
		return nil
	}
	trimmedPath := strings.TrimSpace(filePath)
	if state.UpdatedAt == "" {
		state.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	encodedBytes, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode connection state: %w", err)
	}
	encodedBytes = append(encodedBytes, '\n')
	return writePrivateFileAtomically(trimmedPath, encodedBytes)
}

func ReadConnectionState(filePath string) (ConnectionState, bool, error) {
	if IsDisabledConnectionFilePath(filePath) {
		return ConnectionState{}, false, nil
	}
	trimmedPath := strings.TrimSpace(filePath)
	rawBytes, err := os.ReadFile(trimmedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ConnectionState{}, false, nil
		}
		return ConnectionState{}, false, fmt.Errorf("read connection state %s: %w", trimmedPath, err)
	}
	var state ConnectionState
	if unmarshalErr := json.Unmarshal(rawBytes, &state); unmarshalErr != nil {
		return ConnectionState{}, false, fmt.Errorf("parse connection state %s: %w", trimmedPath, unmarshalErr)
	}
	state.Endpoint = strings.TrimSpace(state.Endpoint)
	state.AuthMode = strings.ToLower(strings.TrimSpace(state.AuthMode))
	state.Token = strings.TrimSpace(state.Token)
	state.TLSCACert = strings.TrimSpace(state.TLSCACert)
	if state.Endpoint == "" && state.Token == "" {
		return ConnectionState{}, false, nil
	}
	return state, true, nil
}

// EnsureMasterToken returns an explicit token when provided (seeding <dataDir>/token
// if not yet present), or loads/generates a persistent 256-bit hex bearer token
// stored with 0600 permissions at <dataDir>/token.
func EnsureMasterToken(dataDirectory string, explicitToken string) (string, string, bool, error) {
	trimmedExplicit := strings.TrimSpace(explicitToken)
	trimmedDirectory := strings.TrimSpace(dataDirectory)

	if trimmedDirectory == "" {
		if trimmedExplicit != "" {
			return trimmedExplicit, "", false, nil
		}
		generatedToken, err := generateRandomHexToken()
		if err != nil {
			return "", "", false, err
		}
		return generatedToken, "", true, nil
	}

	tokenPath := filepath.Join(trimmedDirectory, "token")
	if trimmedExplicit != "" {
		if _, statErr := os.Stat(tokenPath); errors.Is(statErr, os.ErrNotExist) {
			_ = writePrivateFileAtomically(tokenPath, []byte(trimmedExplicit+"\n"))
		}
		return trimmedExplicit, tokenPath, false, nil
	}

	existingBytes, readErr := os.ReadFile(tokenPath)
	if readErr == nil {
		persisted := strings.TrimSpace(string(existingBytes))
		if persisted != "" {
			_ = os.Chmod(tokenPath, 0o600)
			return persisted, tokenPath, false, nil
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return "", tokenPath, false, fmt.Errorf("read master token %s: %w", tokenPath, readErr)
	}

	generatedToken, generateErr := generateRandomHexToken()
	if generateErr != nil {
		return "", tokenPath, false, generateErr
	}
	if writeErr := writePrivateFileAtomically(tokenPath, []byte(generatedToken+"\n")); writeErr != nil {
		return "", tokenPath, false, fmt.Errorf("persist master token %s: %w", tokenPath, writeErr)
	}
	return generatedToken, tokenPath, true, nil
}

// LocalDialEndpoint computes a dialable URL on the local machine for a bound
// net.Listener address, mapping wildcard binds (0.0.0.0 / ::) to 127.0.0.1 while
// preserving interface-specific binds such as a Tailscale IPv4 address.
func LocalDialEndpoint(scheme string, boundAddress string) string {
	cleanScheme := strings.TrimSpace(scheme)
	if cleanScheme == "" {
		cleanScheme = "http"
	}
	host, port, splitErr := net.SplitHostPort(strings.TrimSpace(boundAddress))
	if splitErr != nil {
		return cleanScheme + "://" + strings.TrimSpace(boundAddress)
	}
	parsedIP := net.ParseIP(host)
	if host == "" || (parsedIP != nil && parsedIP.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return cleanScheme + "://" + net.JoinHostPort(host, port)
}

func generateRandomHexToken() (string, error) {
	var randomBytes [32]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return "", fmt.Errorf("generate random bearer token: %w", err)
	}
	return hex.EncodeToString(randomBytes[:]), nil
}

func writePrivateFileAtomically(filePath string, content []byte) error {
	parentDirectory := filepath.Dir(filePath)
	if err := os.MkdirAll(parentDirectory, 0o700); err != nil {
		return err
	}
	temporaryFile, err := os.CreateTemp(parentDirectory, ".orchestra-tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporaryFile.Name()
	cleanupTemporary := true
	defer func() {
		if cleanupTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()

	if chmodErr := temporaryFile.Chmod(0o600); chmodErr != nil {
		_ = temporaryFile.Close()
		return chmodErr
	}
	if _, writeErr := temporaryFile.Write(content); writeErr != nil {
		_ = temporaryFile.Close()
		return writeErr
	}
	if syncErr := temporaryFile.Sync(); syncErr != nil {
		_ = temporaryFile.Close()
		return syncErr
	}
	if closeErr := temporaryFile.Close(); closeErr != nil {
		return closeErr
	}
	if renameErr := os.Rename(temporaryPath, filePath); renameErr != nil {
		return renameErr
	}
	cleanupTemporary = false
	return nil
}

func resolveHomeDirectory(lookupEnvironment func(string) (string, bool), allowOSFallback bool) string {
	for _, variableName := range []string{"HOME", "USERPROFILE"} {
		if value, found := lookupEnvironment(variableName); found && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	if allowOSFallback {
		if homeDirectory, err := os.UserHomeDir(); err == nil {
			return strings.TrimSpace(homeDirectory)
		}
	}
	return ""
}

func expandHomePrefix(rawPath string, homeDirectory string) string {
	if homeDirectory == "" {
		return rawPath
	}
	if rawPath == "~" {
		return homeDirectory
	}
	if strings.HasPrefix(rawPath, "~/") || strings.HasPrefix(rawPath, `~\`) {
		return filepath.Join(homeDirectory, rawPath[2:])
	}
	return rawPath
}
