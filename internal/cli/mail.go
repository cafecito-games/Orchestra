package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

func runMail(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if len(arguments) == 0 {
		printMailUsage(standardError)
		return 2
	}
	action := arguments[0]
	remaining := arguments[1:]

	switch action {
	case "-h", "--help", "help":
		printMailUsage(standardOutput)
		return 0
	case "agents", "who", "list":
		return runMailAgents(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "send":
		return runMailSend(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "inbox", "read":
		return runMailInbox(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "rename":
		return runMailRename(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	default:
		fmt.Fprintf(standardError, "orchestra mail: unknown subcommand %q\n\n", action)
		printMailUsage(standardError)
		return 2
	}
}

func printMailUsage(output io.Writer) {
	fmt.Fprintf(output, `Usage: orchestra mail <subcommand> [flags]

Subcommands:
  agents                         List registered <handle>@<host> agent mailboxes (aliases: who, list)
  send   <to> <body>             Send mail to an agent mailbox (waking it immediately when supported)
  inbox  [address]               Read messages in an agent or operator mailbox (alias: read)
  rename <address> <new-name>    Rename an agent mailbox handle while keeping its previous address as an alias
`)
}

func runMailAgents(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra mail agents", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	host := flagSet.String("host", "", "Filter mailboxes by host name")
	onlineOnly := flagSet.Bool("online", false, "List only active (online, idle, or busy) mailboxes")
	includeAll := flagSet.Bool("all", false, "Include released instance mailboxes")
	workspace := flagSet.String("workspace", "", "Filter mailboxes by workspace substring")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 0 {
		fmt.Fprintln(standardError, "orchestra mail agents: unexpected positional arguments")
		return 2
	}

	query := url.Values{}
	if strings.TrimSpace(*host) != "" {
		query.Set("host", strings.TrimSpace(*host))
	}
	if *onlineOnly {
		query.Set("onlineOnly", "true")
	}
	if *includeAll {
		query.Set("includeReleased", "true")
	}
	if strings.TrimSpace(*workspace) != "" {
		query.Set("workspace", strings.TrimSpace(*workspace))
	}

	var result orchestrator.ListMailboxesResult
	if err := connection.requestJSON(operationContext, http.MethodGet, "/api/mail/agents", query, nil, &result); err != nil {
		fmt.Fprintf(standardError, "orchestra mail agents: %v\n", err)
		return 1
	}

	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, result)
	}
	if len(result.Mailboxes) == 0 {
		fmt.Fprintln(standardOutput, "No agent mailboxes registered.")
		return 0
	}

	table := tabwriter.NewWriter(standardOutput, 0, 2, 2, ' ', 0)
	fmt.Fprintln(table, "ADDRESS\tSTATUS\tKIND\tHARNESS\tUNREAD\tWORKSPACE\tALIASES")
	for _, mailbox := range result.Mailboxes {
		workspaceLabel := mailbox.Workspace
		if workspaceLabel == "" {
			workspaceLabel = "-"
		}
		aliasesLabel := "-"
		if len(mailbox.Aliases) > 0 {
			aliasesLabel = strings.Join(mailbox.Aliases, ",")
		}
		harnessLabel := mailbox.Harness
		if harnessLabel == "" {
			harnessLabel = "-"
		}
		fmt.Fprintf(
			table,
			"%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
			mailbox.Address,
			mailbox.Status,
			mailbox.Kind,
			harnessLabel,
			mailbox.UnreadCount,
			workspaceLabel,
			aliasesLabel,
		)
	}
	_ = table.Flush()
	return 0
}

func runMailSend(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra mail send", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	subject := flagSet.String("subject", "", "Optional mail subject")
	fromAddress := flagSet.String("from", "", "Optional operator sender handle or <handle>@<host>")
	replyToID := flagSet.String("reply-to", "", "Optional message ID being replied to")
	noWake := flagSet.Bool("no-wake", false, "Deliver to inbox without waking/pushing the target agent")
	idempotencyKey := flagSet.String("idempotency-key", "", "Optional idempotency key")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 2 {
		fmt.Fprintln(standardError, "orchestra mail send: expected <to> <body>")
		return 2
	}

	effectiveKey := strings.TrimSpace(*idempotencyKey)
	if effectiveKey == "" {
		effectiveKey = protocol.NewIdentifier("cli")
	}
	wake := !*noWake

	requestBody := orchestrator.SendMailInput{
		IdempotencyKey: effectiveKey,
		From:           strings.TrimSpace(*fromAddress),
		To:             flagSet.Arg(0),
		ReplyToID:      strings.TrimSpace(*replyToID),
		Subject:        *subject,
		Body:           flagSet.Arg(1),
		Wake:           &wake,
	}

	var result orchestrator.SendMailResult
	if err := connection.requestJSON(operationContext, http.MethodPost, "/api/mail/messages", nil, requestBody, &result); err != nil {
		fmt.Fprintf(standardError, "orchestra mail send: %v\n", err)
		return 1
	}

	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, result)
	}
	fmt.Fprintf(
		standardOutput,
		"Sent mail %s (%s -> %s, wakeDispatched=%t, pushed=%t, replayed=%t)\n",
		result.Message.ID,
		result.Message.FromAddress,
		result.Message.ToAddress,
		result.Message.WakeDispatched,
		result.Message.DeliveredPushAt != "",
		result.Replayed,
	)
	return 0
}

func runMailInbox(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra mail inbox", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	box := flagSet.String("box", "inbox", "Mail folder: inbox or sent")
	unreadOnly := flagSet.Bool("unread", false, "Show only unread messages")
	afterSequence := flagSet.Int64("after", 0, "Show messages after sequence number")
	limit := flagSet.Int("limit", protocol.DefaultMailReadLimit, "Maximum messages to return")
	markRead := flagSet.Bool("mark-read", false, "Mark returned inbox messages as read")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 1 {
		fmt.Fprintln(standardError, "orchestra mail inbox: expected at most one [address] argument")
		return 2
	}

	query := url.Values{}
	if flagSet.NArg() == 1 {
		query.Set("address", flagSet.Arg(0))
	}
	if strings.TrimSpace(*box) != "" {
		query.Set("box", strings.TrimSpace(*box))
	}
	if *unreadOnly {
		query.Set("unreadOnly", "true")
	}
	if *afterSequence > 0 {
		query.Set("afterSequence", strconv.FormatInt(*afterSequence, 10))
	}
	if *limit > 0 {
		query.Set("limit", strconv.Itoa(*limit))
	}
	if *markRead {
		query.Set("markRead", "true")
	} else {
		query.Set("markRead", "false")
	}

	var result orchestrator.ReadMailResult
	if err := connection.requestJSON(operationContext, http.MethodGet, "/api/mail/messages", query, nil, &result); err != nil {
		fmt.Fprintf(standardError, "orchestra mail inbox: %v\n", err)
		return 1
	}

	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, result)
	}
	fmt.Fprintf(standardOutput, "Mailbox: %s (status=%s, unread=%d)\n", result.Mailbox.Address, result.Mailbox.Status, result.Mailbox.UnreadCount)
	if len(result.Messages) == 0 {
		fmt.Fprintln(standardOutput, "No messages.")
		return 0
	}
	for _, message := range result.Messages {
		stateLabel := "read"
		if !message.Read {
			stateLabel = "unread"
		}
		fmt.Fprintf(standardOutput, "\n[#%d %s | %s | %s -> %s | %s]\n", message.Sequence, message.ID, stateLabel, message.FromAddress, message.ToAddress, message.CreatedAt)
		if message.Subject != "" {
			fmt.Fprintf(standardOutput, "Subject: %s\n", message.Subject)
		}
		fmt.Fprintln(standardOutput, message.Body)
	}
	return 0
}

func runMailRename(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra mail rename", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 2 {
		fmt.Fprintln(standardError, "orchestra mail rename: expected <current-address> <new-name>")
		return 2
	}

	requestBody := orchestrator.SetMailIdentityInput{
		Address: flagSet.Arg(0),
		Name:    flagSet.Arg(1),
	}
	var response struct {
		Mailbox protocol.AgentMailbox `json:"mailbox"`
	}
	if err := connection.requestJSON(operationContext, http.MethodPost, "/api/mail/rename", nil, requestBody, &response); err != nil {
		fmt.Fprintf(standardError, "orchestra mail rename: %v\n", err)
		return 1
	}

	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, response)
	}
	fmt.Fprintf(standardOutput, "Renamed mailbox to %s (aliases=%s)\n", response.Mailbox.Address, strings.Join(response.Mailbox.Aliases, ","))
	return 0
}
