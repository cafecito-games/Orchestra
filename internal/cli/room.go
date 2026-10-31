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
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

type cliStringSliceFlag struct {
	values []string
}

func (flagValue *cliStringSliceFlag) String() string {
	return strings.Join(flagValue.values, ",")
}

func (flagValue *cliStringSliceFlag) Set(raw string) error {
	for part := range strings.SplitSeq(raw, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			flagValue.values = append(flagValue.values, trimmed)
		}
	}
	return nil
}

func runRoom(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if len(arguments) == 0 {
		printRoomUsage(standardError)
		return 2
	}
	action := arguments[0]
	remaining := arguments[1:]

	switch action {
	case "-h", "--help", "help":
		printRoomUsage(standardOutput)
		return 0
	case "post":
		return runRoomPost(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "nudge":
		return runRoomNudge(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "read":
		return runRoomRead(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "path":
		return runRoomPath(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	default:
		fmt.Fprintf(standardError, "orchestra room: unknown subcommand %q\n\n", action)
		printRoomUsage(standardError)
		return 2
	}
}

func printRoomUsage(output io.Writer) {
	fmt.Fprintf(output, `Usage: orchestra room <subcommand> [flags]

Subcommands:
  post   <thread-id> <body> [--kind guidance] [--topic ...] [--nudge] [--task <id>...] [--idempotency-key ...]
  nudge  <thread-id> [--task <id>...] [--idempotency-key ...]
  read   <thread-id> [--after <seq>] [--limit <n>] [--kind <k>...]
  path   <thread-id>
`)
}

func runRoomPost(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra room post", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	kind := flagSet.String("kind", "guidance", "Room message kind ("+strings.Join(protocol.RoomMessageKinds, ", ")+")")
	topic := flagSet.String("topic", "", "Optional short topic summary")
	nudge := flagSet.Bool("nudge", false, "Nudge non-terminal tasks in the thread (or --task targets) to read the room before completing")
	idempotencyKey := flagSet.String("idempotency-key", "", "Optional idempotency key")
	var taskIDs cliStringSliceFlag
	flagSet.Var(&taskIDs, "task", "Optional target task ID to nudge (repeatable)")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 2 {
		fmt.Fprintln(standardError, "orchestra room post: expected <thread-id> <body>")
		return 2
	}
	threadID := flagSet.Arg(0)
	body := flagSet.Arg(1)
	effectiveKey := strings.TrimSpace(*idempotencyKey)
	if effectiveKey == "" {
		effectiveKey = protocol.NewIdentifier("cli")
	}

	requestBody := orchestrator.PostRoomMessageInput{
		IdempotencyKey: effectiveKey,
		Kind:           *kind,
		Topic:          *topic,
		Body:           body,
		Nudge:          *nudge && len(taskIDs.values) == 0,
		NudgeTaskIDs:   taskIDs.values,
	}

	var result orchestrator.PostRoomMessageResult
	if err := connection.requestJSON(
		operationContext,
		http.MethodPost,
		"/api/threads/"+url.PathEscape(threadID)+"/room/messages",
		nil,
		requestBody,
		&result,
	); err != nil {
		fmt.Fprintf(standardError, "orchestra room post: %v\n", err)
		return 1
	}

	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, result)
	}
	fmt.Fprintf(
		standardOutput,
		"Posted room message #%d (%s, kind=%s, nudged=%d, replayed=%t)\n",
		result.Message.Sequence,
		result.Message.ID,
		result.Message.Kind,
		len(result.NudgedTaskIDs),
		result.Replayed,
	)
	return 0
}

func runRoomNudge(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra room nudge", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	idempotencyKey := flagSet.String("idempotency-key", "", "Optional idempotency key")
	var taskIDs cliStringSliceFlag
	flagSet.Var(&taskIDs, "task", "Optional target task ID to nudge (repeatable)")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 1 {
		fmt.Fprintln(standardError, "orchestra room nudge: expected exactly one <thread-id> argument")
		return 2
	}
	threadID := flagSet.Arg(0)
	effectiveKey := strings.TrimSpace(*idempotencyKey)
	if effectiveKey == "" {
		effectiveKey = protocol.NewIdentifier("cli")
	}

	requestBody := orchestrator.NudgeRoomInput{
		IdempotencyKey: effectiveKey,
		TaskIDs:        taskIDs.values,
	}

	var result orchestrator.NudgeRoomResult
	if err := connection.requestJSON(
		operationContext,
		http.MethodPost,
		"/api/threads/"+url.PathEscape(threadID)+"/room/nudge",
		nil,
		requestBody,
		&result,
	); err != nil {
		fmt.Fprintf(standardError, "orchestra room nudge: %v\n", err)
		return 1
	}

	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, result)
	}
	fmt.Fprintf(
		standardOutput,
		"Nudged %d task(s) in thread %s through room sequence #%d (replayed=%t)\n",
		len(result.NudgedTaskIDs),
		result.ThreadID,
		result.RequiredSequence,
		result.Replayed,
	)
	return 0
}

func runRoomRead(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra room read", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	afterSequence := flagSet.Int64("after", 0, "Read room entries strictly after this sequence number")
	limit := flagSet.Int("limit", 0, "Maximum number of room entries to return")
	var kinds cliStringSliceFlag
	flagSet.Var(&kinds, "kind", "Filter by room message kind (repeatable)")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 1 {
		fmt.Fprintln(standardError, "orchestra room read: expected exactly one <thread-id> argument")
		return 2
	}
	threadID := flagSet.Arg(0)

	query := url.Values{}
	if *afterSequence > 0 {
		query.Set("after", strconv.FormatInt(*afterSequence, 10))
	}
	if *limit > 0 {
		query.Set("limit", strconv.Itoa(*limit))
	}
	for _, kind := range kinds.values {
		query.Add("kind", kind)
	}

	var result orchestrator.ReadRoomResult
	if err := connection.requestJSON(
		operationContext,
		http.MethodGet,
		"/api/threads/"+url.PathEscape(threadID)+"/room",
		query,
		nil,
		&result,
	); err != nil {
		fmt.Fprintf(standardError, "orchestra room read: %v\n", err)
		return 1
	}

	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, result)
	}
	fmt.Fprintf(
		standardOutput,
		"Room %s  head=#%d  returned=%d  hasMore=%t\n",
		result.Room.DirectoryName,
		result.HeadSequence,
		len(result.Messages),
		result.HasMore,
	)
	if len(result.Messages) == 0 {
		fmt.Fprintln(standardOutput, "(no matching room entries)")
		return 0
	}
	writer := tabwriter.NewWriter(standardOutput, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "SEQ\tKIND\tAUTHOR\tTOPIC\tBODY\tCREATED")
	for _, entry := range result.Messages {
		fmt.Fprintf(
			writer,
			"%d\t%s\t%s\t%s\t%s\t%s\n",
			entry.Sequence,
			entry.Kind,
			formatRoomAuthor(entry.Author),
			defaultIfEmpty(compactLine(entry.Topic, 28), "-"),
			compactLine(entry.Body, 64),
			entry.CreatedAt,
		)
	}
	_ = writer.Flush()
	return 0
}

func runRoomPath(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra room path", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 1 {
		fmt.Fprintln(standardError, "orchestra room path: expected exactly one <thread-id> argument")
		return 2
	}
	threadID := flagSet.Arg(0)

	query := url.Values{}
	query.Set("limit", "1")
	var result orchestrator.ReadRoomResult
	if err := connection.requestJSON(
		operationContext,
		http.MethodGet,
		"/api/threads/"+url.PathEscape(threadID)+"/room",
		query,
		nil,
		&result,
	); err != nil {
		fmt.Fprintf(standardError, "orchestra room path: %v\n", err)
		return 1
	}

	localAccessible := false
	if result.Path != "" {
		if information, statErr := os.Lstat(result.Path); statErr == nil && information.Mode().IsRegular() {
			localAccessible = true
		}
	}
	if !localAccessible {
		fmt.Fprintf(standardError, "orchestra room path: room JSONL mirror for %s is not accessible on the local filesystem; use 'orchestra room read %s' instead\n", threadID, threadID)
		return 1
	}

	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, map[string]any{
			"threadId": result.ThreadID,
			"path":     result.Path,
		})
	}
	fmt.Fprintln(standardOutput, result.Path)
	return 0
}

func formatRoomAuthor(author protocol.RoomAuthor) string {
	switch author.Kind {
	case "task":
		if author.TaskID != "" {
			return "task:" + author.TaskID
		}
		return "task"
	case "orchestrator":
		if author.ClientID != "" {
			return "orchestrator:" + author.ClientID
		}
		return "orchestrator"
	case "operator":
		if author.OperatorID != "" {
			return "operator:" + author.OperatorID
		}
		return "operator"
	default:
		return author.Kind
	}
}
