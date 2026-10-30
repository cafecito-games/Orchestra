package acp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

var (
	ErrFrameTooLarge       = errors.New("acp frame exceeds its size limit")
	ErrUnterminatedFrame   = errors.New("acp stdout ended inside an unterminated frame")
	ErrStdoutContamination = errors.New("acp stdout contains a non-protocol line")
)

const frameReadBufferBytes = 64 * 1024

type frameReader struct {
	reader *bufio.Reader
	limit  int
	frame  []byte
}

func newFrameReader(reader io.Reader, limit int) *frameReader {
	return &frameReader{reader: bufio.NewReaderSize(reader, frameReadBufferBytes), limit: limit}
}

func (reader *frameReader) next() ([]byte, error) {
	reader.frame = reader.frame[:0]
	for {
		segment, err := reader.reader.ReadSlice('\n')
		complete := err == nil
		if complete {
			segment = segment[:len(segment)-1]
		}
		if len(reader.frame)+len(segment) > reader.limit {
			return nil, fmt.Errorf("%w (%d bytes)", ErrFrameTooLarge, reader.limit)
		}
		reader.frame = append(reader.frame, segment...)
		switch {
		case complete:
			return reader.validate(reader.frame)
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			if len(reader.frame) == 0 {
				return nil, io.EOF
			}
			return nil, ErrUnterminatedFrame
		default:
			return nil, err
		}
	}
}

func (reader *frameReader) validate(frame []byte) ([]byte, error) {
	trimmed := bytes.TrimSpace(frame)
	if len(trimmed) == 0 || trimmed[0] != '{' || !utf8.Valid(frame) {
		return nil, ErrStdoutContamination
	}
	return frame, nil
}
