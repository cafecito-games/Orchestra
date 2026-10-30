package acp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
)

const (
	CodeParseError             = -32700
	CodeInvalidRequest         = -32600
	CodeMethodNotFound         = -32601
	CodeInvalidParams          = -32602
	CodeInternalError          = -32603
	CodeRequestCancelled       = -32800
	CodeAuthenticationRequired = -32000
)

const cancelRequestMethod = "$/cancel_request"

var (
	ErrProtocolViolation      = errors.New("acp protocol violation")
	ErrAdapterClosed          = errors.New("acp adapter closed its output")
	ErrTooManyPendingRequests = errors.New("acp pending request limit reached")
	errConnectionClosing      = errors.New("acp connection is closing")
)

type ResponseError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (err *ResponseError) Error() string {
	return fmt.Sprintf("acp error %d: %s", err.Code, err.Message)
}

type envelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  *string         `json:"method"`
	Params  json.RawMessage `json:"params"`
	Result  json.RawMessage `json:"result"`
	Error   json.RawMessage `json:"error"`
}

type outboundRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type outboundNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type outboundResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *ResponseError  `json:"error,omitempty"`
}

type callResponse struct {
	result json.RawMessage
	err    *ResponseError
}

type pendingCall struct {
	method  string
	reply   chan callResponse
	settled func()
}

type inboundHandler interface {
	handleNotification(method string, params json.RawMessage) error
	handleRequest(requestContext context.Context, method string, params json.RawMessage) (any, *ResponseError)
}

type connection struct {
	frames  *frameReader
	writer  io.WriteCloser
	handler inboundHandler

	outbound   chan []byte
	closing    chan struct{}
	closeOnce  sync.Once
	failed     chan struct{}
	failOnce   sync.Once
	readerDone chan struct{}
	writerDone chan struct{}
	requests   sync.WaitGroup

	baseContext context.Context
	cancelBase  context.CancelFunc

	mutex          sync.Mutex
	failure        error
	nextID         int64
	pending        map[int64]*pendingCall
	inboundCancels map[string]context.CancelFunc
}

func newConnection(reader io.Reader, writer io.WriteCloser, handler inboundHandler) *connection {
	baseContext, cancelBase := context.WithCancel(context.Background())
	return &connection{
		frames:         newFrameReader(reader, MaximumFrameBytes),
		writer:         writer,
		handler:        handler,
		outbound:       make(chan []byte, OutboundQueueDepth),
		closing:        make(chan struct{}),
		failed:         make(chan struct{}),
		readerDone:     make(chan struct{}),
		writerDone:     make(chan struct{}),
		baseContext:    baseContext,
		cancelBase:     cancelBase,
		nextID:         1,
		pending:        map[int64]*pendingCall{},
		inboundCancels: map[string]context.CancelFunc{},
	}
}

func (peer *connection) start() {
	go peer.readLoop()
	go peer.writeLoop()
}

func (peer *connection) err() error {
	peer.mutex.Lock()
	defer peer.mutex.Unlock()
	return peer.failure
}

func (peer *connection) fail(err error) {
	peer.failOnce.Do(func() {
		peer.mutex.Lock()
		peer.failure = err
		peer.mutex.Unlock()
		close(peer.failed)
		peer.cancelBase()
	})
}

func (peer *connection) closeInput() {
	peer.closeOnce.Do(func() { close(peer.closing) })
}

func (peer *connection) wait() {
	<-peer.readerDone
	<-peer.writerDone
	peer.requests.Wait()
}

func (peer *connection) readLoop() {
	defer close(peer.readerDone)
	for {
		frame, err := peer.frames.next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				peer.fail(ErrAdapterClosed)
			} else {
				peer.fail(fmt.Errorf("%w: %w", ErrProtocolViolation, err))
			}
			return
		}
		if err := peer.dispatch(frame); err != nil {
			peer.fail(fmt.Errorf("%w: %w", ErrProtocolViolation, err))
			return
		}
	}
}

func (peer *connection) writeLoop() {
	defer close(peer.writerDone)
	defer peer.writer.Close()
	for {
		select {
		case frame := <-peer.outbound:
			if !peer.write(frame) {
				return
			}
		case <-peer.closing:
			for {
				select {
				case frame := <-peer.outbound:
					if !peer.write(frame) {
						return
					}
				default:
					return
				}
			}
		case <-peer.failed:
			return
		}
	}
}

func (peer *connection) write(frame []byte) bool {
	if _, err := peer.writer.Write(frame); err != nil {
		peer.fail(fmt.Errorf("write acp frame: %w", err))
		return false
	}
	return true
}

func (peer *connection) enqueue(operationContext context.Context, message any) error {
	frame, err := json.Marshal(message)
	if err != nil {
		return err
	}
	frame = append(frame, '\n')
	select {
	case <-peer.closing:
		return errConnectionClosing
	default:
	}
	select {
	case peer.outbound <- frame:
		return nil
	case <-peer.failed:
		return peer.err()
	case <-peer.closing:
		return errConnectionClosing
	case <-operationContext.Done():
		return operationContext.Err()
	}
}

func (peer *connection) call(operationContext context.Context, method string, params any, result any) error {
	return peer.callSettled(operationContext, method, params, result, nil)
}

func (peer *connection) callSettled(operationContext context.Context, method string, params any, result any, settled func()) error {
	peer.mutex.Lock()
	if peer.failure != nil {
		failure := peer.failure
		peer.mutex.Unlock()
		return failure
	}
	if len(peer.pending) >= MaximumPendingRequests {
		peer.mutex.Unlock()
		return ErrTooManyPendingRequests
	}
	id := peer.nextID
	peer.nextID++
	call := &pendingCall{method: method, reply: make(chan callResponse, 1), settled: settled}
	peer.pending[id] = call
	peer.mutex.Unlock()

	if err := peer.enqueue(operationContext, outboundRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params}); err != nil {
		return err
	}
	select {
	case reply := <-call.reply:
		if reply.err != nil {
			return reply.err
		}
		if result == nil {
			return nil
		}
		if err := decodeStrict(reply.result, result); err != nil {
			failure := fmt.Errorf("%w: malformed %s response: %w", ErrProtocolViolation, method, err)
			peer.fail(failure)
			return failure
		}
		return nil
	case <-peer.failed:
		return peer.err()
	case <-operationContext.Done():
		return operationContext.Err()
	}
}

func (peer *connection) notify(operationContext context.Context, method string, params any) error {
	return peer.enqueue(operationContext, outboundNotification{JSONRPC: "2.0", Method: method, Params: params})
}

func (peer *connection) dispatch(frame []byte) error {
	var message envelope
	if err := json.Unmarshal(frame, &message); err != nil {
		return fmt.Errorf("%w: %w", ErrStdoutContamination, err)
	}
	if message.JSONRPC != "2.0" {
		return errors.New("frame is not a JSON-RPC 2.0 message")
	}
	if message.Method != nil {
		if message.Result != nil || message.Error != nil {
			return errors.New("request or notification carries a result or error")
		}
		if *message.Method == "" {
			return errors.New("request or notification has an empty method")
		}
		if message.ID == nil {
			return peer.dispatchNotification(*message.Method, message.Params)
		}
		return peer.dispatchRequest(message.ID, *message.Method, message.Params)
	}
	return peer.dispatchResponse(message)
}

func (peer *connection) dispatchResponse(message envelope) error {
	if message.ID == nil {
		return errors.New("frame is neither a request, a notification, nor a response")
	}
	if (message.Result == nil) == (message.Error == nil) {
		return errors.New("response must carry exactly one of result or error")
	}
	id, err := strconv.ParseInt(string(bytes.TrimSpace(message.ID)), 10, 64)
	if err != nil {
		return fmt.Errorf("response id %s was never issued", truncateBytes(string(message.ID), eventIdentifierBytes))
	}
	peer.mutex.Lock()
	call, known := peer.pending[id]
	if known {
		delete(peer.pending, id)
	}
	peer.mutex.Unlock()
	if !known {
		return fmt.Errorf("response id %d is unknown or duplicated", id)
	}
	reply := callResponse{result: message.Result}
	if message.Error != nil {
		var responseError ResponseError
		if err := decodeStrict(message.Error, &responseError); err != nil || bytes.Equal(bytes.TrimSpace(message.Error), []byte("null")) {
			return fmt.Errorf("malformed error response to %s", call.method)
		}
		reply.err = &responseError
	}
	if call.settled != nil {
		call.settled()
	}
	call.reply <- reply
	return nil
}

func (peer *connection) dispatchNotification(method string, params json.RawMessage) error {
	if method == cancelRequestMethod {
		var cancellation struct {
			RequestID json.RawMessage `json:"requestId"`
		}
		if json.Unmarshal(params, &cancellation) == nil && cancellation.RequestID != nil {
			peer.mutex.Lock()
			cancel := peer.inboundCancels[string(bytes.TrimSpace(cancellation.RequestID))]
			peer.mutex.Unlock()
			if cancel != nil {
				cancel()
			}
		}
		return nil
	}
	return peer.handler.handleNotification(method, params)
}

func (peer *connection) dispatchRequest(rawID json.RawMessage, method string, params json.RawMessage) error {
	id := bytes.TrimSpace(rawID)
	if !isRequestIdentifier(id) {
		return errors.New("request id must be a string or an integer")
	}
	key := string(id)
	peer.mutex.Lock()
	if _, active := peer.inboundCancels[key]; active {
		peer.mutex.Unlock()
		return fmt.Errorf("request id %s reused while pending", truncateBytes(key, eventIdentifierBytes))
	}
	if len(peer.inboundCancels) >= MaximumInboundRequests {
		peer.mutex.Unlock()
		return peer.respond(id, nil, &ResponseError{Code: CodeInternalError, Message: "too many concurrent requests"})
	}
	requestContext, cancel := context.WithCancel(peer.baseContext)
	peer.inboundCancels[key] = cancel
	peer.requests.Add(1)
	peer.mutex.Unlock()

	go func() {
		defer peer.requests.Done()
		defer func() {
			peer.mutex.Lock()
			delete(peer.inboundCancels, key)
			peer.mutex.Unlock()
			cancel()
		}()
		result, responseError := peer.handler.handleRequest(requestContext, method, params)
		_ = peer.respond(id, result, responseError)
	}()
	return nil
}

func (peer *connection) respond(id json.RawMessage, result any, responseError *ResponseError) error {
	message := outboundResponse{JSONRPC: "2.0", ID: id, Error: responseError}
	if responseError == nil {
		if result == nil {
			result = struct{}{}
		}
		message.Result = result
	}
	err := peer.enqueue(peer.baseContext, message)
	if errors.Is(err, errConnectionClosing) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func isRequestIdentifier(id []byte) bool {
	if len(id) == 0 || len(id) > eventIdentifierBytes {
		return false
	}
	if id[0] == '"' {
		var value string
		return json.Unmarshal(id, &value) == nil
	}
	_, err := strconv.ParseInt(string(id), 10, 64)
	return err == nil
}

func decodeStrict(data json.RawMessage, target any) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("expected a JSON object")
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if decoder.More() {
		return errors.New("unexpected trailing data")
	}
	return nil
}
