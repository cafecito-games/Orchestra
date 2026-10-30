package acp

import (
	"strings"
	"sync"
	"unicode/utf8"
)

const redactionMarker = "[redacted]"

type secretRedactor struct {
	secrets []string
}

func newSecretRedactor(secrets []string) secretRedactor {
	kept := make([]string, 0, len(secrets))
	for _, secret := range secrets {
		if secret != "" {
			kept = append(kept, secret)
		}
	}
	return secretRedactor{secrets: kept}
}

func (redaction secretRedactor) apply(value string) string {
	for _, secret := range redaction.secrets {
		value = strings.ReplaceAll(value, secret, redactionMarker)
	}
	return value
}

func (redaction secretRedactor) longestSecret() int {
	longest := 0
	for _, secret := range redaction.secrets {
		longest = max(longest, len(secret))
	}
	return longest
}

func truncateBytes(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	cut := limit
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut]
}

func splitBytes(value string, limit int) []string {
	chunks := []string{}
	for len(value) > limit {
		chunk := truncateBytes(value, limit)
		if chunk == "" {
			chunk = value[:limit]
		}
		chunks = append(chunks, chunk)
		value = value[len(chunk):]
	}
	return append(chunks, value)
}

type tailBuffer struct {
	mutex     sync.Mutex
	limit     int
	data      []byte
	truncated bool
}

func newTailBuffer(limit int) *tailBuffer {
	return &tailBuffer{limit: limit}
}

func (buffer *tailBuffer) Write(data []byte) (int, error) {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	written := len(data)
	if len(data) >= buffer.limit {
		buffer.data = append(buffer.data[:0], data[len(data)-buffer.limit:]...)
		buffer.truncated = true
		return written, nil
	}
	if overflow := len(buffer.data) + len(data) - buffer.limit; overflow > 0 {
		buffer.data = append(buffer.data[:0], buffer.data[overflow:]...)
		buffer.truncated = true
	}
	buffer.data = append(buffer.data, data...)
	return written, nil
}

type StderrTail struct {
	buffer    *tailBuffer
	redaction secretRedactor
	limit     int
}

func NewStderrTail(limit int, secrets []string) *StderrTail {
	redaction := newSecretRedactor(secrets)
	return &StderrTail{buffer: newTailBuffer(limit + redaction.longestSecret()), redaction: redaction, limit: limit}
}

func (tail *StderrTail) Write(data []byte) (int, error) {
	return tail.buffer.Write(data)
}

func (tail *StderrTail) String() string {
	tail.buffer.mutex.Lock()
	raw := string(tail.buffer.data)
	truncated := tail.buffer.truncated
	tail.buffer.mutex.Unlock()
	if !utf8.ValidString(raw) {
		raw = strings.ToValidUTF8(raw, "\uFFFD")
	}
	value := tail.redaction.apply(raw)
	if truncated {
		value = value[min(len(value), tail.redaction.longestSecret()):]
		for len(value) > 0 && !utf8.RuneStart(value[0]) {
			value = value[1:]
		}
	}
	if len(value) > tail.limit {
		value = value[len(value)-tail.limit:]
		for len(value) > 0 && !utf8.RuneStart(value[0]) {
			value = value[1:]
		}
	}
	return strings.TrimSpace(value)
}
