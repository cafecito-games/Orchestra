// Package acp implements the Agent Client Protocol v1 client used by slave worker nodes
// to supervise locally installed ACP adapters over newline-delimited JSON-RPC 2.0 on stdio.
package acp

import "time"

const ProtocolVersion = 1

const (
	MaximumFrameBytes       = 4 * 1024 * 1024
	MaximumPendingRequests  = 32
	MaximumInboundRequests  = 16
	OutboundQueueDepth      = 64
	MaximumToolContentItems = 64
	MaximumTrackedToolCalls = 4096
	MaximumResultBytes      = 256 * 1024
	MaximumDiagnosticBytes  = 2 * 1024
)

const (
	eventTextBytes       = 64 * 1024
	eventDiffBytes       = 256 * 1024
	eventTitleBytes      = 2 * 1024
	eventIdentifierBytes = 256
	eventPlanEntryLimit  = 200
	eventOptionLimit     = 8
)

const (
	DefaultRequestTimeout    = 30 * time.Second
	DefaultPermissionTimeout = 10 * time.Minute
	DefaultCancelGracePeriod = 5 * time.Second
)
