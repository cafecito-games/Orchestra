// Package activitylog provides a concurrency-safe, secret-redacting operational
// logger for Orchestra master and slave daemons with human-readable terminal
// output and structured JSON output.
package activitylog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/cafecito-games/Orchestra/internal/protocol"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelQuiet
)

func ParseLevel(raw string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "info":
		return LevelInfo, nil
	case "debug", "verbose":
		return LevelDebug, nil
	case "warn", "warning":
		return LevelWarn, nil
	case "error":
		return LevelError, nil
	case "quiet", "silent", "none", "off":
		return LevelQuiet, nil
	default:
		return LevelInfo, fmt.Errorf("unknown log level %q (expected debug, info, warn, error, or quiet)", raw)
	}
}

func (level Level) String() string {
	switch level {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	case LevelQuiet:
		return "quiet"
	default:
		return "info"
	}
}

func (level Level) shortLabel() string {
	switch level {
	case LevelDebug:
		return "DBG"
	case LevelInfo:
		return "INF"
	case LevelWarn:
		return "WRN"
	case LevelError:
		return "ERR"
	default:
		return "INF"
	}
}

type Format string

const (
	FormatText Format = "text"
	FormatJSON Format = "json"
)

func ParseFormat(raw string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "text", "pretty", "human":
		return FormatText, nil
	case "json":
		return FormatJSON, nil
	default:
		return FormatText, fmt.Errorf("unknown log format %q (expected text or json)", raw)
	}
}

type Field struct {
	Key          string
	stringValue  string
	integerValue int64
	booleanValue bool
	kind         fieldKind
	omitIfZero   bool
}

type fieldKind int

const (
	fieldString fieldKind = iota
	fieldInteger
	fieldBoolean
	fieldDuration
)

// String creates a string log attribute that is automatically omitted when empty.
func String(key, value string) Field {
	return Field{
		Key:         key,
		stringValue: value,
		kind:        fieldString,
		omitIfZero:  true,
	}
}

func Int(key string, value int) Field {
	return Field{
		Key:          key,
		integerValue: int64(value),
		kind:         fieldInteger,
	}
}

func Int64(key string, value int64) Field {
	return Field{
		Key:          key,
		integerValue: value,
		kind:         fieldInteger,
	}
}

func Bool(key string, value bool) Field {
	return Field{
		Key:          key,
		booleanValue: value,
		kind:         fieldBoolean,
	}
}

func Duration(key string, duration time.Duration) Field {
	return Field{
		Key:          key,
		integerValue: int64(duration),
		kind:         fieldDuration,
	}
}

type Options struct {
	Writer            io.Writer
	Level             Level
	Format            Format
	Color             *bool
	Redactor          *protocol.Redactor
	Component         string
	Now               func() time.Time
	LookupEnvironment func(string) (string, bool)
	MaximumTextBytes  int
}

type writerLock struct {
	mutex  sync.Mutex
	writer io.Writer
}

type Logger struct {
	shared           *writerLock
	level            Level
	format           Format
	useColor         bool
	redactor         *protocol.Redactor
	component        string
	now              func() time.Time
	maximumTextBytes int
}

const (
	ansiReset  = "\x1b[0m"
	ansiDim    = "\x1b[2m"
	ansiBold   = "\x1b[1m"
	ansiCyan   = "\x1b[36m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiRed    = "\x1b[31m"
)

func New(options Options) *Logger {
	writer := options.Writer
	if writer == nil {
		writer = io.Discard
	}
	format := options.Format
	if format == "" {
		format = FormatText
	}
	nowFunction := options.Now
	if nowFunction == nil {
		nowFunction = time.Now
	}
	redactor := options.Redactor
	if redactor == nil {
		redactor = protocol.NewRedactor()
	}
	maximumTextBytes := options.MaximumTextBytes
	if maximumTextBytes <= 0 {
		maximumTextBytes = 160
	}
	useColor := false
	if format == FormatText {
		if options.Color != nil {
			useColor = *options.Color
		} else {
			useColor = detectTerminalColor(writer, options.LookupEnvironment)
		}
	}
	return &Logger{
		shared:           &writerLock{writer: writer},
		level:            options.Level,
		format:           format,
		useColor:         useColor,
		redactor:         redactor,
		component:        strings.TrimSpace(options.Component),
		now:              nowFunction,
		maximumTextBytes: maximumTextBytes,
	}
}

func (logger *Logger) WithComponent(component string) *Logger {
	if logger == nil {
		return nil
	}
	child := *logger
	child.component = strings.TrimSpace(component)
	return &child
}

func (logger *Logger) WithSecrets(secrets ...string) *Logger {
	if logger == nil {
		return nil
	}
	child := *logger
	child.redactor = logger.redactor.WithSecrets(secrets...)
	return &child
}

func (logger *Logger) Enabled(level Level) bool {
	if logger == nil {
		return false
	}
	return level >= logger.level && logger.level < LevelQuiet
}

func (logger *Logger) Debug(category, message string, fields ...Field) {
	logger.log(LevelDebug, category, message, fields...)
}

func (logger *Logger) Info(category, message string, fields ...Field) {
	logger.log(LevelInfo, category, message, fields...)
}

func (logger *Logger) Warn(category, message string, fields ...Field) {
	logger.log(LevelWarn, category, message, fields...)
}

func (logger *Logger) Error(category, message string, fields ...Field) {
	logger.log(LevelError, category, message, fields...)
}

func (logger *Logger) log(level Level, category, message string, fields ...Field) {
	if !logger.Enabled(level) {
		return
	}
	timestamp := logger.now()
	redactedMessage := logger.redact(message)
	cleanCategory := strings.TrimSpace(category)
	if cleanCategory == "" {
		cleanCategory = "event"
	}

	var encoded []byte
	if logger.format == FormatJSON {
		encoded = logger.encodeJSON(timestamp, level, cleanCategory, redactedMessage, fields)
	} else {
		encoded = logger.encodeText(timestamp, level, cleanCategory, redactedMessage, fields)
	}

	logger.shared.mutex.Lock()
	_, _ = logger.shared.writer.Write(encoded)
	logger.shared.mutex.Unlock()
}

func (logger *Logger) encodeText(
	timestamp time.Time,
	level Level,
	category string,
	message string,
	fields []Field,
) []byte {
	var buffer bytes.Buffer
	timeString := timestamp.Local().Format("15:04:05")
	label := level.shortLabel()

	displayCategory := category
	if logger.component != "" {
		displayCategory = logger.component + ":" + category
	}
	paddedCategory := fmt.Sprintf("%-8s", displayCategory)

	if logger.useColor {
		buffer.WriteString(ansiDim)
		buffer.WriteString(timeString)
		buffer.WriteString(ansiReset)
		buffer.WriteByte(' ')
		buffer.WriteString(levelColor(level))
		buffer.WriteString(label)
		buffer.WriteString(ansiReset)
		buffer.WriteByte(' ')
		buffer.WriteString(ansiBold)
		buffer.WriteString(paddedCategory)
		buffer.WriteString(ansiReset)
	} else {
		buffer.WriteString(timeString)
		buffer.WriteByte(' ')
		buffer.WriteString(label)
		buffer.WriteByte(' ')
		buffer.WriteString(paddedCategory)
	}

	singleLineMessage := collapseSingleLine(message, logger.maximumTextBytes)
	if singleLineMessage != "" {
		buffer.WriteByte(' ')
		buffer.WriteString(singleLineMessage)
	}

	for _, field := range fields {
		if field.Key == "" {
			continue
		}
		formattedValue, include := logger.formatTextField(field)
		if !include {
			continue
		}
		buffer.WriteByte(' ')
		if logger.useColor {
			buffer.WriteString(ansiDim)
			buffer.WriteString(field.Key)
			buffer.WriteByte('=')
			buffer.WriteString(ansiReset)
		} else {
			buffer.WriteString(field.Key)
			buffer.WriteByte('=')
		}
		buffer.WriteString(formattedValue)
	}

	buffer.WriteByte('\n')
	return buffer.Bytes()
}

func (logger *Logger) formatTextField(field Field) (string, bool) {
	switch field.kind {
	case fieldString:
		cleaned := collapseSingleLine(logger.redact(field.stringValue), logger.maximumTextBytes)
		if field.omitIfZero && cleaned == "" {
			return "", false
		}
		return quoteIfNeeded(cleaned), true
	case fieldInteger:
		return strconv.FormatInt(field.integerValue, 10), true
	case fieldBoolean:
		return strconv.FormatBool(field.booleanValue), true
	case fieldDuration:
		return FormatCompactDuration(time.Duration(field.integerValue)), true
	default:
		return "", false
	}
}

func (logger *Logger) encodeJSON(
	timestamp time.Time,
	level Level,
	category string,
	message string,
	fields []Field,
) []byte {
	record := make(map[string]any, len(fields)+5)
	record["time"] = timestamp.UTC().Format(time.RFC3339Nano)
	record["level"] = level.String()
	record["category"] = category
	record["message"] = message
	if logger.component != "" {
		record["component"] = logger.component
	}
	for _, field := range fields {
		if field.Key == "" {
			continue
		}
		switch field.kind {
		case fieldString:
			redacted := logger.redact(field.stringValue)
			if field.omitIfZero && redacted == "" {
				continue
			}
			record[field.Key] = redacted
		case fieldInteger:
			record[field.Key] = field.integerValue
		case fieldBoolean:
			record[field.Key] = field.booleanValue
		case fieldDuration:
			record[field.Key] = time.Duration(field.integerValue).Milliseconds()
		}
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return []byte("{\"level\":\"error\",\"message\":\"log marshal failure\"}\n")
	}
	return append(encoded, '\n')
}

func (logger *Logger) redact(value string) string {
	if value == "" {
		return ""
	}
	if logger != nil && logger.redactor != nil {
		return logger.redactor.Redact(value)
	}
	return protocol.RedactText(value)
}

func FormatCompactDuration(duration time.Duration) string {
	if duration < 0 {
		duration = 0
	}
	switch {
	case duration < time.Millisecond:
		return "<1ms"
	case duration < time.Second:
		return fmt.Sprintf("%dms", duration.Milliseconds())
	case duration < time.Minute:
		return fmt.Sprintf("%.1fs", duration.Seconds())
	default:
		minutes := int(duration / time.Minute)
		seconds := int((duration % time.Minute) / time.Second)
		return fmt.Sprintf("%dm%02ds", minutes, seconds)
	}
}

func collapseSingleLine(value string, maxBytes int) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	var builder strings.Builder
	builder.Grow(min(len(trimmed), maxBytes+8))
	previousSpace := false
	for _, character := range trimmed {
		if unicode.IsSpace(character) || character < 0x20 {
			if !previousSpace {
				builder.WriteByte(' ')
				previousSpace = true
			}
			continue
		}
		previousSpace = false
		builder.WriteRune(character)
	}
	collapsed := strings.TrimSpace(builder.String())
	if maxBytes > 0 && len(collapsed) > maxBytes {
		return protocol.TruncateUTF8Head(collapsed, maxBytes) + "..."
	}
	return collapsed
}

func quoteIfNeeded(value string) string {
	if value == "" {
		return `""`
	}
	needsQuote := false
	for _, character := range value {
		if unicode.IsSpace(character) || character == '"' || character == '=' || character == '\\' {
			needsQuote = true
			break
		}
	}
	if !needsQuote {
		return value
	}
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return `"` + escaped + `"`
}

func levelColor(level Level) string {
	switch level {
	case LevelDebug:
		return ansiDim
	case LevelInfo:
		return ansiCyan
	case LevelWarn:
		return ansiYellow
	case LevelError:
		return ansiRed
	default:
		return ansiGreen
	}
}

func detectTerminalColor(writer io.Writer, lookupEnvironment func(string) (string, bool)) bool {
	if lookupEnvironment == nil {
		lookupEnvironment = os.LookupEnv
	}
	if value, present := lookupEnvironment("NO_COLOR"); present && value != "" {
		return false
	}
	if terminalName, present := lookupEnvironment("TERM"); present && strings.EqualFold(strings.TrimSpace(terminalName), "dumb") {
		return false
	}
	file, isFile := writer.(*os.File)
	if !isFile {
		return false
	}
	information, err := file.Stat()
	if err != nil {
		return false
	}
	return (information.Mode() & os.ModeCharDevice) != 0
}
