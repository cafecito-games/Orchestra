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

func runSchedules(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	if len(arguments) > 0 {
		switch arguments[0] {
		case "-h", "--help", "help":
			printSchedulesUsage(standardOutput)
			return 0
		case "cancel":
			return runSchedulesCancel(operationContext, arguments[1:], standardOutput, standardError, lookupEnvironment)
		}
	}

	flagSet := flag.NewFlagSet("orchestra schedules", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	threadID := flagSet.String("thread", "", "Filter schedules by thread ID")
	targetKind := flagSet.String("target", "", "Filter schedules by target kind (room or mail)")
	status := flagSet.String("status", "", "Filter schedules by status (active, completed, or cancelled)")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() > 0 {
		fmt.Fprintf(standardError, "orchestra schedules: unexpected arguments: %s\n", strings.Join(flagSet.Args(), " "))
		return 2
	}

	query := url.Values{}
	if strings.TrimSpace(*threadID) != "" {
		query.Set("threadId", strings.TrimSpace(*threadID))
	}
	if strings.TrimSpace(*targetKind) != "" {
		query.Set("targetKind", strings.TrimSpace(*targetKind))
	}
	if strings.TrimSpace(*status) != "" {
		query.Set("status", strings.TrimSpace(*status))
	}

	var payload struct {
		Schedules []protocol.ScheduledMessage `json:"schedules"`
	}
	if err := connection.requestJSON(operationContext, http.MethodGet, "/api/schedules", query, nil, &payload); err != nil {
		fmt.Fprintf(standardError, "orchestra schedules: %v\n", err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, payload)
	}
	if len(payload.Schedules) == 0 {
		fmt.Fprintln(standardOutput, "No schedules found.")
		return 0
	}
	writer := tabwriter.NewWriter(standardOutput, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "SCHEDULE ID\tSTATUS\tTARGET\tNEXT DELIVER\tFIRED\tBODY")
	for _, schedule := range payload.Schedules {
		target := schedule.ThreadID
		if schedule.TargetKind == protocol.ScheduleTargetKindMail {
			target = schedule.To
		}
		fmt.Fprintf(
			writer,
			"%s\t%s\t%s:%s\t%s\t%d/%d\t%s\n",
			schedule.ID,
			schedule.Status,
			schedule.TargetKind,
			target,
			schedule.NextDeliverAt,
			schedule.OccurrencesFired,
			schedule.MaxOccurrences,
			compactLine(schedule.Body, 48),
		)
	}
	_ = writer.Flush()
	return 0
}

func printSchedulesUsage(output io.Writer) {
	fmt.Fprint(output, `Usage:
  orchestra schedules [--thread <id>] [--target room|mail] [--status active|completed|cancelled] [flags]
  orchestra schedules cancel <schedule-id> [flags]
`)
}

func runSchedulesCancel(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra schedules cancel", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 1 {
		fmt.Fprintln(standardError, "orchestra schedules cancel: expected exactly one <schedule-id> argument")
		return 2
	}
	scheduleID := flagSet.Arg(0)
	var payload struct {
		Schedule protocol.ScheduledMessage `json:"schedule"`
	}
	if err := connection.requestJSON(operationContext, http.MethodDelete, "/api/schedules/"+url.PathEscape(scheduleID), nil, nil, &payload); err != nil {
		fmt.Fprintf(standardError, "orchestra schedules cancel: %v\n", err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, payload)
	}
	fmt.Fprintf(standardOutput, "Cancelled schedule %s (status=%s)\n", payload.Schedule.ID, payload.Schedule.Status)
	return 0
}

func runRoomSchedule(
	operationContext context.Context,
	arguments []string,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	flagSet := flag.NewFlagSet("orchestra room schedule", flag.ContinueOnError)
	flagSet.SetOutput(standardError)
	connection := registerOperatorFlags(flagSet, lookupEnvironment)
	channel := flagSet.String("channel", "", "Optional room sub-channel name (default \"main\")")
	kind := flagSet.String("kind", "guidance", "Room message kind ("+strings.Join(protocol.RoomMessageKinds, ", ")+")")
	topic := flagSet.String("topic", "", "Optional short topic summary")
	inDuration := flagSet.Duration("in", 0, "Relative delay before first delivery (for example 5m)")
	deliverAt := flagSet.String("at", "", "Absolute RFC 3339 delivery timestamp")
	everyDuration := flagSet.Duration("every", 0, "Optional recurring interval (minimum 60s)")
	maxOccurrences := flagSet.Int("max", 0, "Optional maximum number of recurring deliveries")
	untilAt := flagSet.String("until", "", "Optional RFC 3339 end timestamp for recurring deliveries")
	idempotencyKey := flagSet.String("idempotency-key", "", "Optional idempotency key")

	if err := flagSet.Parse(normalizeFlagOrder(flagSet, arguments)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flagSet.NArg() != 2 {
		fmt.Fprintln(standardError, "orchestra room schedule: expected <thread-id> <body>")
		return 2
	}
	threadID := flagSet.Arg(0)
	body := flagSet.Arg(1)
	effectiveKey := strings.TrimSpace(*idempotencyKey)
	if effectiveKey == "" {
		effectiveKey = protocol.NewIdentifier("cli")
	}

	requestBody := orchestrator.ScheduleMessageInput{
		IdempotencyKey: effectiveKey,
		TargetKind:     protocol.ScheduleTargetKindRoom,
		ThreadID:       threadID,
		Channel:        strings.TrimSpace(*channel),
		Kind:           strings.TrimSpace(*kind),
		Topic:          strings.TrimSpace(*topic),
		Body:           body,
		InSeconds:      int(inDuration.Round(time.Second) / time.Second),
		DeliverAt:      strings.TrimSpace(*deliverAt),
		EverySeconds:   int(everyDuration.Round(time.Second) / time.Second),
		MaxOccurrences: *maxOccurrences,
		UntilAt:        strings.TrimSpace(*untilAt),
	}

	var result orchestrator.ScheduleMessageResult
	if err := connection.requestJSON(operationContext, http.MethodPost, "/api/schedules", nil, requestBody, &result); err != nil {
		fmt.Fprintf(standardError, "orchestra room schedule: %v\n", err)
		return 1
	}
	if *connection.asJSON {
		return writePrettyJSON(standardOutput, standardError, result)
	}
	fmt.Fprintf(
		standardOutput,
		"Scheduled %s for thread %s (next=%s, max=%d, replayed=%t)\n",
		result.Schedule.ID,
		result.Schedule.ThreadID,
		result.Schedule.NextDeliverAt,
		result.Schedule.MaxOccurrences,
		result.Replayed,
	)
	return 0
}
