package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cafecito-games/Orchestra/internal/acp"
	"github.com/cafecito-games/Orchestra/internal/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyDiagnostic(t *testing.T) {
	testCases := []struct {
		name       string
		diagnostic string
		expected   string
	}{
		{
			name:       "claude credit balance low",
			diagnostic: "API Error: 400 {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Credit balance is too low\"}}",
			expected:   protocol.RunErrorKindQuotaExhausted,
		},
		{
			name:       "claude usage limit hit",
			diagnostic: "You've hit your limit · resets 7pm (America/New_York)",
			expected:   protocol.RunErrorKindQuotaExhausted,
		},
		{
			name:       "claude rate limit error",
			diagnostic: "API Error: 429 {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"Number of request tokens has exceeded your per-minute rate limit\"}}",
			expected:   protocol.RunErrorKindRateLimited,
		},
		{
			name:       "codex insufficient quota over 429",
			diagnostic: "HTTP 429 Too Many Requests: {\"error\":{\"code\":\"insufficient_quota\",\"message\":\"You exceeded your current quota, please check your plan and billing details.\"}}",
			expected:   protocol.RunErrorKindQuotaExhausted,
		},
		{
			name:       "codex rate limit exceeded",
			diagnostic: "error code: rate_limit_exceeded - Rate limit reached for gpt-5-codex on tokens per min",
			expected:   protocol.RunErrorKindRateLimited,
		},
		{
			name:       "opencode provider usage limit",
			diagnostic: "Provider returned error: monthly usage limit reached for this account",
			expected:   protocol.RunErrorKindQuotaExhausted,
		},
		{
			name:       "opencode http 429 status",
			diagnostic: "upstream provider failed with HTTP status 429",
			expected:   protocol.RunErrorKindRateLimited,
		},
		{
			name:       "antigravity out of quota over resource exhausted",
			diagnostic: "rpc error: code = ResourceExhausted desc = model gemini-4-argon-medium is out of quota for this billing period",
			expected:   protocol.RunErrorKindQuotaExhausted,
		},
		{
			name:       "antigravity resource exhausted throttling",
			diagnostic: "rpc error: code = RESOURCE_EXHAUSTED desc = Too Many Requests",
			expected:   protocol.RunErrorKindRateLimited,
		},
		{
			name:       "unrelated port 429 or task id does not false positive",
			diagnostic: "connection refused on 127.0.0.1:4290 while running task-429",
			expected:   "",
		},
		{
			name:       "generic syntax failure",
			diagnostic: "exit status 1: undefined identifier in main.go",
			expected:   "",
		},
		{
			name:       "empty diagnostic",
			diagnostic: "   ",
			expected:   "",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expected, ClassifyDiagnostic(testCase.diagnostic))
		})
	}
}

func TestWrapAndClassifyErrorPreservesUnwrapAndIgnoresCancellation(t *testing.T) {
	underlying := errors.New("API Error: 429 {\"error\":{\"type\":\"rate_limit_error\"}}")
	wrapped := WrapClassifiedError(underlying)
	require.Error(t, wrapped)
	assert.Equal(t, protocol.RunErrorKindRateLimited, ClassifyError(wrapped))
	require.ErrorIs(t, wrapped, underlying)

	assert.Empty(t, ClassifyError(context.Canceled))
	assert.Empty(t, ClassifyError(fmt.Errorf("%w: rate limit text during shutdown", acp.ErrCancelled)))
	assert.Empty(t, ClassifyError(fmt.Errorf("%w: HTTP 429", ErrDriverUnavailable)))
}

func TestNativeRunnerClassifiesStructuredStreamErrorsAcrossHarnesses(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell stub uses POSIX sh")
	}

	testCases := []struct {
		name         string
		harnessID    string
		scriptBody   string
		expectedKind string
		expectedText string
	}{
		{
			name:      "claude result is_error quota",
			harnessID: protocol.HarnessClaudeCLI,
			scriptBody: `#!/bin/sh
printf '%s\n' '{"type":"result","subtype":"success","is_error":true,"result":"Credit balance is too low"}'
exit 1
`,
			expectedKind: protocol.RunErrorKindQuotaExhausted,
			expectedText: "Credit balance is too low",
		},
		{
			name:      "codex turn failed rate limit",
			harnessID: protocol.HarnessCodexCLI,
			scriptBody: `#!/bin/sh
printf '%s\n' '{"type":"turn.failed","error":{"message":"HTTP 429 rate_limit_exceeded"}}'
exit 1
`,
			expectedKind: protocol.RunErrorKindRateLimited,
			expectedText: "HTTP 429 rate_limit_exceeded",
		},
		{
			name:      "opencode json error quota",
			harnessID: protocol.HarnessOpenCodeCLI,
			scriptBody: `#!/bin/sh
printf '%s\n' '{"type":"error","error":{"data":{"message":"insufficient_quota: billing hard limit reached"}}}'
exit 1
`,
			expectedKind: protocol.RunErrorKindQuotaExhausted,
			expectedText: "insufficient_quota: billing hard limit reached",
		},
		{
			name:      "antigravity result failure rate limit",
			harnessID: protocol.HarnessAntigravityCLI,
			scriptBody: `#!/bin/sh
printf '%s\n' '{"event":"result","result":{"status":"ERROR","error":"RESOURCE_EXHAUSTED: Too Many Requests"}}'
exit 1
`,
			expectedKind: protocol.RunErrorKindRateLimited,
			expectedText: "RESOURCE_EXHAUSTED: Too Many Requests",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			temporaryDirectory := t.TempDir()
			binaryPath := filepath.Join(temporaryDirectory, "fake-harness")
			require.NoError(t, os.WriteFile(binaryPath, []byte(testCase.scriptBody), 0o755))

			workspaceDirectory := t.TempDir()
			authorizedWorkspace, err := AuthorizeWorkspace(workspaceDirectory, []string{workspaceDirectory})
			require.NoError(t, err)

			runner := NewRunner([]protocol.HarnessProfile{{
				ID:          testCase.harnessID,
				Label:       testCase.harnessID,
				Description: "1.0.0",
				Binary:      binaryPath,
				Available:   true,
				Transports:  []string{TransportNative},
			}})

			_, executeErr := runner.Execute(t.Context(), Invocation{
				Run: protocol.Run{
					ID:        "run-quota-check",
					HarnessID: testCase.harnessID,
					Model:     "default",
					Workspace: authorizedWorkspace,
					Prompt:    "Test quota classification",
					Transport: TransportNative,
				},
				Workspace: authorizedWorkspace,
			})
			require.Error(t, executeErr)
			assert.Contains(t, executeErr.Error(), testCase.expectedText)
			assert.Equal(t, testCase.expectedKind, ClassifyError(executeErr))
		})
	}
}
