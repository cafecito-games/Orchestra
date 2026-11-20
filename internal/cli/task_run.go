package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

type repeatableStringFlag []string

func (values *repeatableStringFlag) String() string {
	return strings.Join(*values, ",")
}

func (values *repeatableStringFlag) Set(raw string) error {
	for part := range strings.SplitSeq(raw, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			*values = append(*values, trimmed)
		}
	}
	return nil
}

func splitTaskRunArguments(arguments []string) ([]string, []string) {
	separatorIndex := slices.Index(arguments, "--")
	if separatorIndex == -1 {
		return arguments, nil
	}
	return arguments[:separatorIndex], arguments[separatorIndex+1:]
}

func buildTaskRunChildEnvironment(baseEnvironment []string, task protocol.Task, claim protocol.TaskClaim) []string {
	overrideKeys := map[string]bool{
		"ORCHESTRA_THREAD_ID":   true,
		"ORCHESTRA_TASK_ID":     true,
		"ORCHESTRA_CLAIM_EPOCH": true,
		"ORCHESTRA_CLAIM_TOKEN": true,
	}
	environment := make([]string, 0, len(baseEnvironment)+4)
	for _, entry := range baseEnvironment {
		key, _, hasEquals := strings.Cut(entry, "=")
		if hasEquals && overrideKeys[key] {
			continue
		}
		environment = append(environment, entry)
	}
	environment = append(
		environment,
		"ORCHESTRA_THREAD_ID="+task.ThreadID,
		"ORCHESTRA_TASK_ID="+task.ID,
		"ORCHESTRA_CLAIM_EPOCH="+strconv.FormatInt(claim.Epoch, 10),
		"ORCHESTRA_CLAIM_TOKEN="+claim.Token,
	)
	return environment
}

func runTaskRun(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagArgs, commandArgs := splitTaskRunArguments(arguments)
	flagSet := flag.NewFlagSet("orchestra task run", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)

	threadID := flagSet.String("thread", "", "Thread ID to claim a ready task from")
	taskID := flagSet.String("task", "", "Specific claimable task ID to claim")
	var labels repeatableStringFlag
	flagSet.Var(&labels, "label", "Capability label filter (repeatable or comma-separated)")
	ttlFlag := flagSet.Duration("ttl", time.Duration(protocol.DefaultTaskClaimTTLSeconds)*time.Second, "Claim lease TTL (1s to 5m)")
	waitFlag := flagSet.Duration("wait", 0, "Optional maximum time to wait for a ready claimable task")
	claimerID := flagSet.String("claimer-id", "", "Optional claimer identifier")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, flagArgs)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		if len(commandArgs) == 0 {
			commandArgs = flagSet.Args()
		} else {
			fmt.Fprintf(standardError, "orchestra task run: unexpected arguments before --: %s\n", strings.Join(flagSet.Args(), " "))
			return 2
		}
	}
	if len(commandArgs) == 0 {
		fmt.Fprintln(standardError, "orchestra task run: expected a command after -- (for example: orchestra task run --thread <id> -- <command> [args...])")
		return 2
	}

	ttlSeconds := int(ttlFlag.Round(time.Second) / time.Second)
	if *ttlFlag > 0 && ttlSeconds == 0 {
		ttlSeconds = protocol.MinimumTaskClaimTTLSeconds
	}
	if ttlSeconds < protocol.MinimumTaskClaimTTLSeconds || ttlSeconds > protocol.MaximumTaskClaimTTLSeconds {
		fmt.Fprintf(
			standardError,
			"orchestra task run: --ttl must be between %ds and %ds\n",
			protocol.MinimumTaskClaimTTLSeconds,
			protocol.MaximumTaskClaimTTLSeconds,
		)
		return 2
	}
	if *waitFlag < 0 {
		fmt.Fprintln(standardError, "orchestra task run: --wait must be non-negative")
		return 2
	}

	_, resolvedToken, _, _, targetErr := connection.resolvedTarget()
	if targetErr != nil {
		fmt.Fprintf(standardError, "orchestra task run: %v\n", targetErr)
		return 1
	}
	redactor := protocol.NewRedactor(resolvedToken)

	signalContext, stopSignals := signal.NotifyContext(operationContext, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	effectiveClaimerID := strings.TrimSpace(*claimerID)
	if effectiveClaimerID == "" {
		if hostName, err := os.Hostname(); err == nil && strings.TrimSpace(hostName) != "" {
			effectiveClaimerID = fmt.Sprintf("cli:%s:%d", strings.TrimSpace(hostName), os.Getpid())
		} else {
			effectiveClaimerID = fmt.Sprintf("cli:%d", os.Getpid())
		}
	}

	claimResult, claimErr := claimTaskWithWait(signalContext, connection, orchestrator.ClaimTaskInput{
		ThreadID:   strings.TrimSpace(*threadID),
		TaskID:     strings.TrimSpace(*taskID),
		Labels:     labels,
		TTLSeconds: ttlSeconds,
		ClaimerID:  effectiveClaimerID,
	}, *waitFlag)
	if claimErr != nil {
		fmt.Fprintf(standardError, "orchestra task run: %s\n", redactor.Redact(claimErr.Error()))
		return 1
	}
	if !claimResult.Claimed || claimResult.Task == nil || claimResult.Claim == nil {
		if *connection.asJSON {
			return writePrettyJSON(standardOutput, standardError, claimResult)
		}
		fmt.Fprintln(standardError, "orchestra task run: no matching claimable task available")
		return 1
	}

	claimedTask := *claimResult.Task
	activeClaim := *claimResult.Claim

	childContext, cancelChild := context.WithCancel(signalContext)
	defer cancelChild()

	renewInterval := max(time.Duration(ttlSeconds)*time.Second/3, 200*time.Millisecond)
	var claimLost atomic.Bool
	renewDone := make(chan struct{})
	renewStop := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-renewStop:
				return
			case <-childContext.Done():
				return
			case <-ticker.C:
				var renewed orchestrator.RenewTaskClaimResult
				err := connection.requestJSON(
					childContext,
					http.MethodPost,
					"/api/tasks/"+url.PathEscape(claimedTask.ID)+"/claim/renew",
					nil,
					orchestrator.RenewTaskClaimInput{
						TaskID:     claimedTask.ID,
						ClaimEpoch: activeClaim.Epoch,
						ClaimToken: activeClaim.Token,
						TTLSeconds: ttlSeconds,
					},
					&renewed,
				)
				if err != nil {
					if strings.Contains(err.Error(), "(claim_lost)") {
						claimLost.Store(true)
						cancelChild()
						return
					}
				}
			}
		}
	}()

	command := exec.CommandContext(childContext, commandArgs[0], commandArgs[1:]...)
	command.Env = buildTaskRunChildEnvironment(os.Environ(), claimedTask, activeClaim)
	command.Stdout = standardOutput
	command.Stderr = standardError

	runErr := command.Run()
	close(renewStop)
	<-renewDone

	if claimLost.Load() {
		fmt.Fprintf(standardError, "orchestra task run: claim lost for task %s (epoch %d)\n", claimedTask.ID, activeClaim.Epoch)
		return 1
	}

	if signalContext.Err() != nil {
		releaseContext, cancelRelease := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelRelease()
		var released orchestrator.ReleaseTaskClaimResult
		releaseErr := connection.requestJSON(
			releaseContext,
			http.MethodPost,
			"/api/tasks/"+url.PathEscape(claimedTask.ID)+"/claim/release",
			nil,
			orchestrator.ReleaseTaskClaimInput{
				TaskID:     claimedTask.ID,
				ClaimEpoch: activeClaim.Epoch,
				ClaimToken: activeClaim.Token,
				Reason:     "interrupted by signal",
			},
			&released,
		)
		if releaseErr != nil {
			fmt.Fprintf(standardError, "orchestra task run: failed to release claim for task %s: %s\n", claimedTask.ID, redactor.Redact(releaseErr.Error()))
		}
		return 130
	}

	exitCode := 0
	finalStatus := "completed"
	summary := "Command exited with code 0"
	if runErr != nil {
		finalStatus = "failed"
		var exitError *exec.ExitError
		if errors.As(runErr, &exitError) {
			exitCode = exitError.ExitCode()
			summary = fmt.Sprintf("Command exited with code %d", exitCode)
		} else {
			exitCode = 1
			summary = redactor.Redact("Command execution failed: " + runErr.Error())
		}
	}

	settleContext, cancelSettle := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelSettle()
	epoch := activeClaim.Epoch
	var settled struct {
		Task protocol.Task `json:"task"`
	}
	settleErr := connection.requestJSON(
		settleContext,
		http.MethodPost,
		"/api/tasks/"+url.PathEscape(claimedTask.ID)+"/update",
		nil,
		orchestrator.UpdateTaskInput{
			TaskID:     claimedTask.ID,
			ClaimEpoch: &epoch,
			ClaimToken: activeClaim.Token,
			Status:     finalStatus,
			Summary:    &summary,
		},
		&settled,
	)
	if settleErr != nil {
		fmt.Fprintf(standardError, "orchestra task run: failed to settle task %s: %s\n", claimedTask.ID, redactor.Redact(settleErr.Error()))
		return 1
	}

	if *connection.asJSON {
		if writeCode := writePrettyJSON(standardOutput, standardError, map[string]any{
			"task":     settled.Task,
			"claim":    activeClaim,
			"exitCode": exitCode,
		}); writeCode != 0 {
			return writeCode
		}
	}
	return exitCode
}

func claimTaskWithWait(
	operationContext context.Context,
	connection operatorConnectionFlags,
	input orchestrator.ClaimTaskInput,
	totalWait time.Duration,
) (orchestrator.ClaimTaskResult, error) {
	deadline := time.Now().Add(totalWait)
	for {
		remaining := time.Until(deadline)
		if totalWait <= 0 {
			remaining = 0
		} else if remaining < 0 {
			remaining = 0
		}
		stepMilliseconds := min(int(remaining/time.Millisecond), protocol.MaximumWaitMilliseconds)
		requestInput := input
		requestInput.TimeoutMilliseconds = stepMilliseconds

		requestTimeout := operatorRequestTimeout + time.Duration(stepMilliseconds)*time.Millisecond
		var result orchestrator.ClaimTaskResult
		if err := connection.requestJSONWithTimeout(
			operationContext,
			http.MethodPost,
			"/api/tasks/claim",
			nil,
			requestInput,
			&result,
			requestTimeout,
		); err != nil {
			return orchestrator.ClaimTaskResult{}, err
		}
		if result.Claimed || totalWait <= 0 || time.Now().After(deadline) || operationContext.Err() != nil {
			return result, nil
		}
	}
}
