// Package orchestrator implements the Orchestra master control plane,
// capability-matching scheduler, durable mailbox, MCP gateway, and operator API.
package orchestrator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

type OperationError struct {
	Code       string
	Message    string
	HTTPStatus int
	Retryable  bool
	Detail     map[string]any
}

func (err *OperationError) Error() string {
	return err.Code + ": " + err.Message
}

func (err *OperationError) HubError() *protocol.HubRPCError {
	return &protocol.HubRPCError{
		Code:      err.Code,
		Message:   err.Message,
		Retryable: err.Retryable,
		Detail:    err.Detail,
	}
}

func newError(code string, status int, message string) *OperationError {
	return &OperationError{
		Code:       code,
		Message:    message,
		HTTPStatus: status,
	}
}

func newRetryableError(code string, status int, message string) *OperationError {
	return &OperationError{
		Code:       code,
		Message:    message,
		HTTPStatus: status,
		Retryable:  true,
	}
}

func invalidRequest(message string) *OperationError {
	return newError("invalid_request", http.StatusBadRequest, message)
}

func notFound(message string) *OperationError {
	return newError("not_found", http.StatusNotFound, message)
}

func forbiddenError(message string) *OperationError {
	return newError("forbidden", http.StatusForbidden, message)
}

func conflictError(code, message string) *OperationError {
	return newError(code, http.StatusConflict, message)
}

func conflictErrorWithDetail(code, message string, detail map[string]any) *OperationError {
	return &OperationError{
		Code:       code,
		Message:    message,
		HTTPStatus: http.StatusConflict,
		Detail:     detail,
	}
}

func labelValidationError(err error) *OperationError {
	if errors.Is(err, protocol.ErrReservedLabel) {
		return newError("reserved_label", http.StatusBadRequest, err.Error())
	}
	return invalidRequest(err.Error())
}

func toHubRPCError(err error) *protocol.HubRPCError {
	if err == nil {
		return nil
	}
	if operationError, ok := errors.AsType[*OperationError](err); ok {
		return operationError.HubError()
	}
	return &protocol.HubRPCError{
		Code:    "internal_error",
		Message: protocol.BoundedDiagnostic(protocol.RedactText(err.Error())),
	}
}

func canonicalDigest(value any) string {
	encodedBytes, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(encodedBytes)
	return hex.EncodeToString(sum[:])
}

type CallerIdentity struct {
	Kind          string
	OperatorID    string
	ClientID      string
	SessionID     string
	RunID         string
	TaskID        string
	ThreadID      string
	InstanceID    string
	CanDelegate   bool
	PrincipalKind string
	PrincipalID   string
	VerifiedNode  string
	GrantID       string
	Scope         *protocol.AccessGrantScope
}

func (caller CallerIdentity) SourceKey() string {
	switch caller.Kind {
	case "operator":
		if caller.OperatorID != "" {
			return "operator:" + caller.OperatorID
		}
		return "operator:cli"
	case "orchestrator-client":
		return "orchestrator-client:" + caller.ClientID
	case "run":
		return "run:" + caller.RunID
	default:
		return caller.Kind
	}
}

func (caller CallerIdentity) InstanceCreator() protocol.InstanceCreator {
	switch caller.Kind {
	case "operator":
		operatorID := caller.OperatorID
		if operatorID == "" {
			operatorID = "cli"
		}
		return protocol.InstanceCreator{
			Kind:          "operator",
			OperatorID:    operatorID,
			PrincipalKind: caller.PrincipalKind,
			PrincipalID:   caller.PrincipalID,
			VerifiedNode:  caller.VerifiedNode,
		}
	case "run":
		return protocol.InstanceCreator{
			Kind:          "run",
			RunID:         caller.RunID,
			InstanceID:    caller.InstanceID,
			PrincipalKind: caller.PrincipalKind,
			PrincipalID:   caller.PrincipalID,
			VerifiedNode:  caller.VerifiedNode,
		}
	default:
		clientID := caller.ClientID
		if clientID == "" {
			clientID = "mcp-http"
		}
		return protocol.InstanceCreator{
			Kind:          "orchestrator-client",
			ClientID:      clientID,
			PrincipalKind: caller.PrincipalKind,
			PrincipalID:   caller.PrincipalID,
			VerifiedNode:  caller.VerifiedNode,
		}
	}
}
