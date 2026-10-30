package harness

import (
	"errors"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

func FallbackReason(err error) string {
	switch {
	case err == nil,
		errors.Is(err, acp.ErrCancelled),
		errors.Is(err, acp.ErrCancelGraceExpired),
		errors.Is(err, acp.ErrAuthenticationRequired),
		errors.Is(err, acp.ErrConfigurationRejected):
		return ""
	case errors.Is(err, ErrMCPUnavailable):
		return protocol.FallbackACPMCPUnavailable
	case errors.Is(err, acp.ErrMissingCapability):
		return protocol.FallbackACPCapabilityMissing
	case errors.Is(err, acp.ErrUnsupportedVersion),
		errors.Is(err, acp.ErrAdapterVersionMismatch),
		errors.Is(err, acp.ErrProtocolViolation),
		errors.Is(err, acp.ErrAdapterClosed):
		return protocol.FallbackACPProtocolIncompatible
	case errors.Is(err, ErrDriverUnavailable):
		return protocol.FallbackACPAdapterUnavailable
	default:
		return ""
	}
}
