package cli

import (
	"context"
	"encoding/json"
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
	"time"

	"github.com/cafecito-games/Orchestra/internal/orchestrator"
	"github.com/cafecito-games/Orchestra/internal/protocol"
)

const (
	roomWaitTimeoutExitCode  = 124
	roomWaitCanceledExitCode = 130
	defaultRoomWaitTimeout   = 30 * time.Second
	roomWaitRequestBuffer    = 10 * time.Second
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
	case "wait":
		return runRoomWait(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "tail":
		return runRoomTail(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "path":
		return runRoomPath(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	case "schedule":
		return runRoomSchedule(operationContext, remaining, standardOutput, standardError, lookupEnvironment)
	default:
		fmt.Fprintf(standardError, "orchestra room: unknown subcommand %q\n\n", action)
		printRoomUsage(standardError)
		return 2
	}
}

func printRoomUsage(output io.Writer) {
	fmt.Fprintf(output, `Usage: orchestra room <subcommand> [flags]

Subcommands:
  post   <thread-id> <body> [--channel <name>] [--kind guidance] [--topic ...] [--mention <target>...] [--reply-to <seq>] [--supersedes <seq>] [--no-wake] [--nudge] [--task <id>...] [--idempotency-key ...]
  nudge  <thread-id> [--task <id>...] [--idempotency-key ...]
  read   <thread-id> [--channel <name|*>] [--after <seq>] [--limit <n>] [--kind <k>...]
  wait   <thread-id> [--after <seq>] [--cursor <cursor>] [--channel <name|*>] [--kind <k>...] [--mentions <target>] [--timeout <duration>] [--limit <n>]
  tail   <thread-id> [--after <seq>] [--cursor <cursor>] [--channel <name|*>] [--kind <k>...] [--mentions <target>]
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
	channel := flagSet.String("channel", "", "Optional room sub-channel name (default \"main\")")
	kind := flagSet.String("kind", "guidance", "Room message kind ("+strings.Join(protocol.RoomMessageKinds, ", ")+")")
	topic := flagSet.String("topic", "", "Optional short topic summary")
	var mentions cliStringSliceFlag
	flagSet.Var(&mentions, "mention", "Optional @handle, @#channel, or @all mention target (repeatable)")
	var rawLabels cliStringSliceFlag
	flagSet.Var(&rawLabels, "label", "Optional key=value label to attach to the message (repeatable)")
	expectedHead := flagSet.Int64("expected-head", -1, "Optional expected room head sequence guard")
	expectedChannelSeq := flagSet.Int64("expected-channel-seq", -1, "Optional expected channel head sequence guard")
	replyToSequence := flagSet.Int64("reply-to", 0, "Optional room message sequence number being replied to")
	supersedesSequence := flagSet.Int64("supersedes", 0, "Optional room message sequence number being superseded")
	noWake := flagSet.Bool("no-wake", false, "Post without waking mentioned resident instances")
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
	if *replyToSequence < 0 {
		fmt.Fprintln(standardError, "orchestra room post: --reply-to must be greater than or equal to 1")
		return 2
	}
	if *supersedesSequence < 0 {
		fmt.Fprintln(standardError, "orchestra room post: --supersedes must be greater than or equal to 1")
		return 2
	}

	threadID := flagSet.Arg(0)
	body := flagSet.Arg(1)
	effectiveKey := strings.TrimSpace(*idempotencyKey)
	if effectiveKey == "" {
		effectiveKey = protocol.NewIdentifier("cli")
	}

	var parsedLabels map[string]string
	if len(rawLabels.values) > 0 {
		parsedLabels = make(map[string]string, len(rawLabels.values))
		for _, entry := range rawLabels.values {
			key, value, hasEquals := strings.Cut(entry, "=")
			if !hasEquals {
				fmt.Fprintf(standardError, "orchestra room post: --label %q must be formatted as key=value\n", entry)
				return 2
			}
			parsedLabels[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}

	requestBody := orchestrator.PostRoomMessageInput{
		IdempotencyKey: effectiveKey,
		Channel:        strings.TrimSpace(*channel),
		Kind:           *kind,
		Topic:          *topic,
		Body:           body,
		Labels:         parsedLabels,
		Mentions:       mentions.values,
		Nudge:          *nudge && len(taskIDs.values) == 0,
		NudgeTaskIDs:   taskIDs.values,
	}
	if *expectedHead >= 0 {
		seq := *expectedHead
		requestBody.ExpectedHeadSequence = &seq
	}
	if *expectedChannelSeq >= 0 {
		seq := *expectedChannelSeq
		requestBody.ExpectedChannelSequence = &seq
	}
	if *noWake {
		disabled := false
		requestBody.Wake = &disabled
	}
	if *replyToSequence > 0 {
		sequence := *replyToSequence
		requestBody.ReplyToSequence = &sequence
	}
	if *supersedesSequence > 0 {
		sequence := *supersedesSequence
		requestBody.SupersedesSequence = &sequence
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
		"Posted room message #%d (%s, channel=%s, kind=%s, nudged=%d, replayed=%t)\n",
		result.Message.Sequence,
		result.Message.ID,
		protocol.EffectiveRoomChannel(result.Message.Channel),
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
	channel := flagSet.String("channel", "", "Filter by room sub-channel name or '*' for all channels")
	afterSequence := flagSet.Int64("after", 0, "Read room entries strictly after this sequence number")
	limit := flagSet.Int("limit", 0, "Maximum number of room entries to return")
	var kinds cliStringSliceFlag
	flagSet.Var(&kinds, "kind", "Filter by room message kind (repeatable)")
	var labelFilters cliStringSliceFlag
	flagSet.Var(&labelFilters, "label", "Filter by label expression (key, key=value, key=a|b; repeatable)")

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
	if *afterSequence < 0 {
		fmt.Fprintln(standardError, "orchestra room read: --after must be greater than or equal to 0")
		return 2
	}
	if *limit < 0 {
		fmt.Fprintln(standardError, "orchestra room read: --limit must be greater than or equal to 0")
		return 2
	}
	threadID := flagSet.Arg(0)
	trimmedChannel := strings.TrimSpace(*channel)

	query := url.Values{}
	if trimmedChannel != "" {
		query.Set("channel", trimmedChannel)
	}
	if *afterSequence > 0 {
		query.Set("after", strconv.FormatInt(*afterSequence, 10))
	}
	if *limit > 0 {
		query.Set("limit", strconv.Itoa(*limit))
	}
	for _, kind := range kinds.values {
		query.Add("kind", kind)
	}
	for _, labelExpr := range labelFilters.values {
		query.Add("label", labelExpr)
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
	displayChannel := defaultIfEmpty(result.Channel, protocol.AllRoomChannels)
	fmt.Fprintf(
		standardOutput,
		"Room %s  channel=%s  head=#%d  through=#%d  returned=%d  hasMore=%t\n",
		result.Room.DirectoryName,
		displayChannel,
		result.HeadSequence,
		result.ThroughSequence,
		len(result.Messages),
		result.HasMore,
	)
	if len(result.Messages) == 0 {
		fmt.Fprintln(standardOutput, "(no matching room entries)")
	} else {
		writer := tabwriter.NewWriter(standardOutput, 0, 0, 2, ' ', 0)
		fmt.Fprintln(writer, "SEQ\tCHANNEL\tKIND\tAUTHOR\tMENTIONS\tTOPIC\tBODY\tCREATED")
		for _, entry := range result.Messages {
			bodyDisplay := compactLine(entry.Body, 64)
			if len(entry.Blocks) > 0 {
				var summaries []string
				for _, block := range entry.Blocks {
					summaries = append(summaries, protocol.FormatStructuredBlockSummary(block))
				}
				bodyDisplay = strings.TrimSpace(bodyDisplay + " " + strings.Join(summaries, " "))
			}
			fmt.Fprintf(
				writer,
				"%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				entry.Sequence,
				protocol.EffectiveRoomChannel(entry.Channel),
				entry.Kind,
				formatRoomAuthor(entry.Author),
				formatRoomMentions(entry.Mentions),
				defaultIfEmpty(compactLine(entry.Topic, 28), "-"),
				bodyDisplay,
				entry.CreatedAt,
			)
		}
		_ = writer.Flush()
	}
	fmt.Fprintf(standardOutput, "Next: %s\n", formatRoomReadNextCommand(threadID, trimmedChannel, result.ThroughSequence))
	return 0
}

func runRoomWait(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra room wait", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	afterSequence := flagSet.Int64("after", 0, "Wait for room entries strictly after this sequence number (defaults to current room head)")
	eventCursor := flagSet.String("cursor", "", "Optional event cursor from a previous wait")
	channel := flagSet.String("channel", "", "Filter room entries by sub-channel name or '*' for all channels")
	var kinds cliStringSliceFlag
	flagSet.Var(&kinds, "kind", "Filter room entries by kind (repeatable)")
	var labelFilters cliStringSliceFlag
	flagSet.Var(&labelFilters, "label", "Filter room entries by label expression (key, key=value, key=a|b; repeatable)")
	mentions := flagSet.String("mentions", "", "Filter room entries mentioning a handle, @#channel, or @all")
	timeout := flagSet.Duration("timeout", defaultRoomWaitTimeout, "Maximum duration to wait before timing out")
	limit := flagSet.Int("limit", 0, "Maximum number of matching entries to return")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 1 {
		fmt.Fprintln(standardError, "orchestra room wait: expected exactly one <thread-id> argument")
		return 2
	}
	if *afterSequence < 0 {
		fmt.Fprintln(standardError, "orchestra room wait: --after must be greater than or equal to 0")
		return 2
	}
	if *timeout < 0 {
		fmt.Fprintln(standardError, "orchestra room wait: --timeout must be greater than or equal to 0")
		return 2
	}
	if *limit < 0 {
		fmt.Fprintln(standardError, "orchestra room wait: --limit must be greater than or equal to 0")
		return 2
	}

	afterExplicit := false
	flagSet.Visit(func(visited *flag.Flag) {
		if visited.Name == "after" {
			afterExplicit = true
		}
	})

	threadID := flagSet.Arg(0)
	trimmedChannel := strings.TrimSpace(*channel)
	trimmedMentions := strings.TrimSpace(*mentions)
	hasAfter := afterExplicit
	currentAfter := *afterSequence
	currentCursor := strings.TrimSpace(*eventCursor)
	deadline := time.Now().Add(*timeout)

	var lastResult orchestrator.WaitRoomResult
	for {
		remaining := max(time.Until(deadline), 0)
		sliceMilliseconds := min(int(remaining.Milliseconds()), protocol.MaximumWaitMilliseconds)
		if remaining > 0 && sliceMilliseconds == 0 {
			sliceMilliseconds = 1
		}

		result, err := fetchRoomWaitSlice(
			operationContext,
			connection,
			threadID,
			hasAfter,
			currentAfter,
			currentCursor,
			trimmedChannel,
			kinds.values,
			labelFilters.values,
			trimmedMentions,
			*limit,
			sliceMilliseconds,
		)
		if err != nil {
			if operationContext.Err() != nil {
				return roomWaitCanceledExitCode
			}
			fmt.Fprintf(standardError, "orchestra room wait: %v\n", err)
			return 1
		}
		lastResult = result
		hasAfter = true
		currentAfter = result.ThroughSequence
		if result.EventCursor != "" {
			currentCursor = result.EventCursor
		}

		if len(result.Messages) > 0 || len(result.Events) > 0 || result.CursorExpired {
			if *connection.asJSON {
				return writePrettyJSON(standardOutput, standardError, result)
			}
			writeRoomWaitText(standardOutput, threadID, trimmedChannel, result)
			return 0
		}

		if operationContext.Err() != nil {
			return roomWaitCanceledExitCode
		}
		if *timeout == 0 || !time.Now().Before(deadline) {
			lastResult.TimedOut = true
			if *connection.asJSON {
				if writeCode := writePrettyJSON(standardOutput, standardError, lastResult); writeCode != 0 {
					return writeCode
				}
				return roomWaitTimeoutExitCode
			}
			fmt.Fprintf(
				standardError,
				"Timed out waiting for room activity in %s (head=#%d, through=#%d, --after %d)\n",
				threadID,
				lastResult.HeadSequence,
				lastResult.ThroughSequence,
				lastResult.ThroughSequence,
			)
			return roomWaitTimeoutExitCode
		}
	}
}

type roomTailStreamItem struct {
	Type            string                   `json:"type"`
	ThreadID        string                   `json:"threadId"`
	ThroughSequence int64                    `json:"throughSequence"`
	EventCursor     string                   `json:"eventCursor,omitempty"`
	Message         *protocol.RoomMessage    `json:"message,omitempty"`
	Event           *protocol.TaskEventEntry `json:"event,omitempty"`
}

func runRoomTail(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra room tail", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	afterSequence := flagSet.Int64("after", 0, "Stream room entries strictly after this sequence number (defaults to current room head)")
	eventCursor := flagSet.String("cursor", "", "Optional event cursor to resume from")
	channel := flagSet.String("channel", "", "Filter room entries by sub-channel name or '*' for all channels")
	var kinds cliStringSliceFlag
	flagSet.Var(&kinds, "kind", "Filter room entries by kind (repeatable)")
	var labelFilters cliStringSliceFlag
	flagSet.Var(&labelFilters, "label", "Filter room entries by label expression (key, key=value, key=a|b; repeatable)")
	mentions := flagSet.String("mentions", "", "Filter room entries mentioning a handle, @#channel, or @all")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 1 {
		fmt.Fprintln(standardError, "orchestra room tail: expected exactly one <thread-id> argument")
		return 2
	}
	if *afterSequence < 0 {
		fmt.Fprintln(standardError, "orchestra room tail: --after must be greater than or equal to 0")
		return 2
	}

	afterExplicit := false
	flagSet.Visit(func(visited *flag.Flag) {
		if visited.Name == "after" {
			afterExplicit = true
		}
	})

	threadID := flagSet.Arg(0)
	trimmedChannel := strings.TrimSpace(*channel)
	trimmedMentions := strings.TrimSpace(*mentions)
	hasAfter := afterExplicit
	currentAfter := *afterSequence
	currentCursor := strings.TrimSpace(*eventCursor)
	printedHeader := false

	for {
		if operationContext.Err() != nil {
			return 0
		}
		result, err := fetchRoomWaitSlice(
			operationContext,
			connection,
			threadID,
			hasAfter,
			currentAfter,
			currentCursor,
			trimmedChannel,
			kinds.values,
			labelFilters.values,
			trimmedMentions,
			0,
			protocol.MaximumWaitMilliseconds,
		)
		if err != nil {
			if operationContext.Err() != nil {
				return 0
			}
			fmt.Fprintf(standardError, "orchestra room tail: %v\n", err)
			return 1
		}
		hasAfter = true
		currentAfter = result.ThroughSequence
		if result.EventCursor != "" {
			currentCursor = result.EventCursor
		}

		for _, message := range result.Messages {
			if *connection.asJSON {
				messageCopy := message
				if encodeErr := writeCompactJSONLine(standardOutput, roomTailStreamItem{
					Type:            "room_message",
					ThreadID:        result.ThreadID,
					ThroughSequence: result.ThroughSequence,
					EventCursor:     result.EventCursor,
					Message:         &messageCopy,
				}); encodeErr != nil {
					fmt.Fprintf(standardError, "orchestra room tail: %v\n", encodeErr)
					return 1
				}
				continue
			}
			if !printedHeader {
				fmt.Fprintln(standardOutput, "TYPE\tSEQ\tCHANNEL\tKIND\tAUTHOR\tMENTIONS\tTOPIC\tBODY\tCREATED")
				printedHeader = true
			}
			fmt.Fprintf(
				standardOutput,
				"room\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				message.Sequence,
				protocol.EffectiveRoomChannel(message.Channel),
				message.Kind,
				formatRoomAuthor(message.Author),
				formatRoomMentions(message.Mentions),
				defaultIfEmpty(compactLine(message.Topic, 28), "-"),
				compactLine(message.Body, 64),
				message.CreatedAt,
			)
		}

		for _, event := range result.Events {
			if *connection.asJSON {
				eventCopy := event
				if encodeErr := writeCompactJSONLine(standardOutput, roomTailStreamItem{
					Type:            "thread_event",
					ThreadID:        result.ThreadID,
					ThroughSequence: result.ThroughSequence,
					EventCursor:     result.EventCursor,
					Event:           &eventCopy,
				}); encodeErr != nil {
					fmt.Fprintf(standardError, "orchestra room tail: %v\n", encodeErr)
					return 1
				}
				continue
			}
			if !printedHeader {
				fmt.Fprintln(standardOutput, "TYPE\tSEQ\tCHANNEL\tKIND\tAUTHOR\tMENTIONS\tTOPIC\tBODY\tCREATED")
				printedHeader = true
			}
			fmt.Fprintf(
				standardOutput,
				"event\t%d\t-\t%s\t-\t-\t-\t%s\t%s\n",
				event.Sequence,
				event.Type,
				defaultIfEmpty(compactLine(event.Summary, 64), "-"),
				event.OccurredAt,
			)
		}
	}
}

func fetchRoomWaitSlice(
	operationContext context.Context,
	connection operatorConnectionFlags,
	threadID string,
	hasAfter bool,
	afterSequence int64,
	eventCursor string,
	channel string,
	kinds []string,
	labels []string,
	mentions string,
	limit int,
	timeoutMilliseconds int,
) (orchestrator.WaitRoomResult, error) {
	query := url.Values{}
	if hasAfter {
		query.Set("after", strconv.FormatInt(afterSequence, 10))
	}
	if eventCursor != "" {
		query.Set("cursor", eventCursor)
	}
	if channel != "" {
		query.Set("channel", channel)
	}
	for _, kind := range kinds {
		query.Add("kind", kind)
	}
	for _, labelExpr := range labels {
		query.Add("label", labelExpr)
	}
	if mentions != "" {
		query.Set("mentions", mentions)
	}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	query.Set("timeoutMs", strconv.Itoa(max(timeoutMilliseconds, 0)))

	requestTimeout := max(
		time.Duration(max(timeoutMilliseconds, 0))*time.Millisecond+roomWaitRequestBuffer,
		operatorRequestTimeout,
	)

	var result orchestrator.WaitRoomResult
	err := connection.requestJSONWithTimeout(
		operationContext,
		http.MethodGet,
		"/api/threads/"+url.PathEscape(threadID)+"/room/wait",
		query,
		nil,
		&result,
		requestTimeout,
	)
	return result, err
}

func writeRoomWaitText(output io.Writer, threadID string, requestedChannel string, result orchestrator.WaitRoomResult) {
	if len(result.Messages) > 0 {
		writer := tabwriter.NewWriter(output, 0, 0, 2, ' ', 0)
		fmt.Fprintln(writer, "SEQ\tCHANNEL\tKIND\tAUTHOR\tMENTIONS\tTOPIC\tBODY\tCREATED")
		for _, entry := range result.Messages {
			bodyDisplay := compactLine(entry.Body, 64)
			if len(entry.Blocks) > 0 {
				var summaries []string
				for _, block := range entry.Blocks {
					summaries = append(summaries, protocol.FormatStructuredBlockSummary(block))
				}
				bodyDisplay = strings.TrimSpace(bodyDisplay + " " + strings.Join(summaries, " "))
			}
			fmt.Fprintf(
				writer,
				"%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				entry.Sequence,
				protocol.EffectiveRoomChannel(entry.Channel),
				entry.Kind,
				formatRoomAuthor(entry.Author),
				formatRoomMentions(entry.Mentions),
				defaultIfEmpty(compactLine(entry.Topic, 28), "-"),
				bodyDisplay,
				entry.CreatedAt,
			)
		}
		_ = writer.Flush()
	}
	if len(result.Events) > 0 {
		writer := tabwriter.NewWriter(output, 0, 0, 2, ' ', 0)
		fmt.Fprintln(writer, "EVENT_SEQ\tTYPE\tSUMMARY\tCREATED")
		for _, event := range result.Events {
			fmt.Fprintf(
				writer,
				"%d\t%s\t%s\t%s\n",
				event.Sequence,
				event.Type,
				defaultIfEmpty(compactLine(event.Summary, 64), "-"),
				event.OccurredAt,
			)
		}
		_ = writer.Flush()
	}
	if result.CursorExpired && len(result.Messages) == 0 && len(result.Events) == 0 {
		fmt.Fprintln(output, "Event cursor expired; advanced to current stream floor.")
	}
	fmt.Fprintf(output, "Next: %s\n", formatRoomWaitNextCommand(threadID, requestedChannel, result.ThroughSequence, result.EventCursor))
}

func formatRoomReadNextCommand(threadID string, channel string, throughSequence int64) string {
	parts := []string{"orchestra room read", threadID, "--after", strconv.FormatInt(throughSequence, 10)}
	if channel != "" {
		parts = append(parts, "--channel", channel)
	}
	return strings.Join(parts, " ")
}

func formatRoomWaitNextCommand(threadID string, channel string, throughSequence int64, eventCursor string) string {
	parts := []string{"orchestra room wait", threadID, "--after", strconv.FormatInt(throughSequence, 10)}
	if channel != "" {
		parts = append(parts, "--channel", channel)
	}
	if eventCursor != "" {
		parts = append(parts, "--cursor", eventCursor)
	}
	return strings.Join(parts, " ")
}

func formatRoomMentions(mentions []string) string {
	if len(mentions) == 0 {
		return "-"
	}
	return strings.Join(mentions, ",")
}

func writeCompactJSONLine(output io.Writer, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, string(encoded))
	return err
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
	var base string
	switch author.Kind {
	case "task":
		switch {
		case author.Handle != "":
			base = author.Handle
		case author.TaskID != "":
			base = "task:" + author.TaskID
		default:
			base = "task"
		}
	case "orchestrator":
		switch {
		case author.Handle != "":
			base = author.Handle
		case author.ClientID != "":
			base = "orchestrator:" + author.ClientID
		default:
			base = "orchestrator"
		}
	case "operator":
		if author.OperatorID != "" {
			base = "operator:" + author.OperatorID
		} else {
			base = "operator"
		}
	default:
		base = author.Kind
	}
	return protocol.FormatAttributedAuthor(base, author.PrincipalID)
}
