package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

func runToken(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if len(arguments) == 0 || hasHelpFlag(arguments[:1]) {
		printTokenUsage(standardOutput)
		return 0
	}
	switch arguments[0] {
	case "create":
		return runTokenCreate(operationContext, arguments[1:], standardOutput, standardError, lookupEnvironment)
	case "list":
		return runTokenList(operationContext, arguments[1:], standardOutput, standardError, lookupEnvironment)
	case "revoke":
		return runTokenRevoke(operationContext, arguments[1:], standardOutput, standardError, lookupEnvironment)
	default:
		fmt.Fprintf(standardError, "orchestra token: unknown subcommand %q\n\n", arguments[0])
		printTokenUsage(standardError)
		return 2
	}
}

func printTokenUsage(output io.Writer) {
	fmt.Fprint(output, `Usage:
  orchestra token create [--label "..."] [--thread <id>...] [--channel <pattern>...] [--op read|post|claim|delegate...] [--ttl <duration>] [flags]
  orchestra token list [flags]
  orchestra token revoke <grant-id> [flags]

Subcommands:
  create      Issue a narrowed scoped access grant token
  list        List issued access grants
  revoke      Revoke an issued access grant by ID
`)
}

func runTokenCreate(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra token create", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	label := flagSet.String("label", "", "Optional human-readable label for this grant")
	var threadIDs cliStringSliceFlag
	flagSet.Var(&threadIDs, "thread", "Restrict grant to specific thread ID (repeatable or comma-separated)")
	var roomChannels cliStringSliceFlag
	flagSet.Var(&roomChannels, "channel", "Restrict grant to room channel glob pattern (repeatable or comma-separated)")
	var operations cliStringSliceFlag
	flagSet.Var(&operations, "op", "Allowed operation: read, post, claim, delegate (repeatable or comma-separated)")
	ttl := flagSet.Duration("ttl", 0, "Optional grant TTL (for example 1h, 24h)")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra token create: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}
	if *ttl < 0 {
		fmt.Fprintln(standardError, "orchestra token create: --ttl must be non-negative")
		return 2
	}
	ops := operations.values
	if len(ops) == 0 {
		ops = []string{protocol.AccessOpRead}
	}

	requestBody := orchestrator.CreateAccessGrantInput{
		Label: strings.TrimSpace(*label),
		Scope: protocol.AccessGrantScope{
			ThreadIDs:    threadIDs.values,
			RoomChannels: roomChannels.values,
			Operations:   ops,
		},
		TTLSeconds: int(ttl.Round(time.Second) / time.Second),
	}

	var result orchestrator.CreateAccessGrantResult
	if err := connection.requestJSON(operationContext, http.MethodPost, "/api/tokens", nil, requestBody, &result); err != nil {
		fmt.Fprintf(standardError, "orchestra token create: %v\n", err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, result)
	}
	fmt.Fprintf(standardOutput, "Grant ID:   %s\n", result.Grant.ID)
	fmt.Fprintf(standardOutput, "Principal:  %s (%s)\n", result.Grant.PrincipalID, result.Grant.PrincipalKind)
	fmt.Fprintf(standardOutput, "Operations: %s\n", strings.Join(result.Grant.Scope.Operations, ","))
	if len(result.Grant.Scope.ThreadIDs) > 0 {
		fmt.Fprintf(standardOutput, "Threads:    %s\n", strings.Join(result.Grant.Scope.ThreadIDs, ","))
	}
	if len(result.Grant.Scope.RoomChannels) > 0 {
		fmt.Fprintf(standardOutput, "Channels:   %s\n", strings.Join(result.Grant.Scope.RoomChannels, ","))
	}
	if result.Grant.ExpiresAt != "" {
		fmt.Fprintf(standardOutput, "Expires:    %s\n", result.Grant.ExpiresAt)
	}
	fmt.Fprintf(standardOutput, "Token:      %s\n", result.Token)
	return 0
}

func runTokenList(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra token list", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	var payload struct {
		Grants []protocol.AccessGrantRecord `json:"grants"`
	}
	if err := connection.requestJSON(operationContext, http.MethodGet, "/api/tokens", nil, nil, &payload); err != nil {
		fmt.Fprintf(standardError, "orchestra token list: %v\n", err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, payload)
	}
	if len(payload.Grants) == 0 {
		fmt.Fprintln(standardOutput, "No access grants found.")
		return 0
	}
	writer := tabwriter.NewWriter(standardOutput, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "GRANT ID\tPRINCIPAL\tOPS\tTHREADS\tCHANNELS\tREVOKED")
	for _, grant := range payload.Grants {
		fmt.Fprintf(
			writer,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			grant.ID,
			grant.PrincipalID,
			strings.Join(grant.Scope.Operations, ","),
			defaultIfEmpty(strings.Join(grant.Scope.ThreadIDs, ","), "*"),
			defaultIfEmpty(strings.Join(grant.Scope.RoomChannels, ","), "*"),
			defaultIfEmpty(grant.RevokedAt, "-"),
		)
	}
	_ = writer.Flush()
	return 0
}

func runTokenRevoke(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra token revoke", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 1 {
		fmt.Fprintln(standardError, "orchestra token revoke: expected exactly one <grant-id> argument")
		return 2
	}
	grantID := flagSet.Arg(0)
	var payload struct {
		Grant protocol.AccessGrantRecord `json:"grant"`
	}
	if err := connection.requestJSON(operationContext, http.MethodDelete, "/api/tokens/"+url.PathEscape(grantID), nil, nil, &payload); err != nil {
		fmt.Fprintf(standardError, "orchestra token revoke: %v\n", err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, payload)
	}
	fmt.Fprintf(standardOutput, "Revoked grant %s\n", payload.Grant.ID)
	return 0
}
