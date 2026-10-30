package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type eventRecorder struct {
	mutex      sync.Mutex
	file       *os.File
	role       string
	executable string
}

var activeRecorder *eventRecorder

func newEventRecorder(role string) *eventRecorder {
	executable, _ := os.Executable()
	instance := &eventRecorder{role: role, executable: executable}
	activeRecorder = instance

	recordDirectory := os.Getenv(recordDirectoryVariable)
	if recordDirectory == "" {
		return instance
	}
	logPath := filepath.Join(recordDirectory, role+"-"+strconv.Itoa(os.Getpid())+".jsonl")
	file, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return instance
	}
	instance.file = file

	presentVariables := map[string]bool{}
	for _, variableName := range observedEnvironmentVariables {
		_, isSet := os.LookupEnv(variableName)
		presentVariables[variableName] = isSet
	}
	workingDirectory, _ := os.Getwd()
	instance.write(map[string]any{
		"event":            "start",
		"role":             role,
		"pid":              os.Getpid(),
		"environment":      presentVariables,
		"workingDirectory": workingDirectory,
		"executable":       executable,
	})
	return instance
}

func (recorder *eventRecorder) write(entry map[string]any) {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	if recorder.file == nil {
		return
	}
	if _, present := entry["role"]; !present {
		entry["role"] = recorder.role
	}
	eventName, _ := entry["event"].(string)
	isToolEvent := strings.HasPrefix(eventName, "mcp-tool")
	if _, present := entry["executable"]; !present && !isToolEvent {
		entry["executable"] = recorder.executable
	}
	entry["at"] = time.Now().UTC().Format(time.RFC3339Nano)
	encodedBytes, err := json.Marshal(entry)
	if err != nil {
		return
	}
	_, _ = recorder.file.Write(append(encodedBytes, '\n'))
}

func (recorder *eventRecorder) recordExit(code int) {
	recorder.write(map[string]any{"event": "exit", "code": code})
	recorder.mutex.Lock()
	if recorder.file != nil {
		_ = recorder.file.Close()
		recorder.file = nil
	}
	recorder.mutex.Unlock()
}

func terminateProcess(code int) {
	if activeRecorder != nil {
		activeRecorder.recordExit(code)
	}
	os.Exit(code)
}
