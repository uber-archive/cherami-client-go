// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package websocket

import (
	"bytes"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	gorilla "github.com/gorilla/websocket"
	"github.com/uber/cherami-client-go/common"
)

type (
	// Stream defines the interface for the "stream", that implements the shared
	// functionality between the server and the client ends of the stream
	Stream interface {
		// Start starts the stream
		Start() error
		// Write writes a message into the buffer
		Write(thrift.TStruct) error
		// Flush flushes out buffered messages
		Flush() error
		// Read reads and returns a message
		Read() (thrift.TStruct, error)
		// CloseWrite closes the write-path
		CloseWrite() error
		// Close shuts down read/write path and closes the underlying connection
		Close() error
	}

	// stream provides a buffered websocket connection for use with streaming
	stream struct {
		conn Conn // the underlying websocket connection

		readMsgs     []thrift.TStruct // buffered read messages
		readMsgIndex int              // index to the next message that will be returned
		readMsgType  reflect.Type     // the actual type of read-message (for reflection)

		writeMsgs     []thrift.TStruct // buffered write messages
		writeMsgLen   int              // max number of messages that will be buffered
		writeMsgIndex int              // index to where the next message would be buffered

		server       bool          // indicates that this is the 'server' end of the stream
		pingInterval time.Duration // interval at which 'ping' messages should be sent

		started     int32         // indicates that the stream has been started
		writeClosed int32         // indicates that the write-path has been closed
		connClosed  int32         // indicates that the underlying connection has been closed
		closed      int32         // indicates that close has been called on the stream
		closeC      chan struct{} // channel used to signal the ping-pump to stop
	}

	// StreamOpts are the options passed when creating the stream
	StreamOpts struct {
		// Server indicates whether the stream should be configured to behave
		// like the "server" end of the connection.
		Server bool

		// FlushThreshold is the number of messages to buffer before flushing automatically
		FlushThreshold int

		// PingInterval is the interval at which websocket ping-messages need to be sent; if
		// this is zero, pings will not be sent
		PingInterval time.Duration

		// readMsgType is the type of read-message
		ReadMsgType reflect.Type
	}
)

const closeMessageTimeout = 10 * time.Second

var errNotStarted = fmt.Errorf("Stream not started")

// NewStream initializes a new stream object
func NewStream(conn Conn, opts *StreamOpts) Stream {

	// ensure we buffer at least one message
	if opts.FlushThreshold < 1 {
		opts.FlushThreshold = 1
	}

	// create and return stream object
	return &stream{
		conn: conn,

		readMsgType:  opts.ReadMsgType,
		readMsgIndex: 0,

		writeMsgs:     make([]thrift.TStruct, opts.FlushThreshold),
		writeMsgLen:   opts.FlushThreshold,
		writeMsgIndex: 0,

		server:       opts.Server,
		pingInterval: opts.PingInterval,
		closeC:       make(chan struct{}),
	}
}

// Start starts the stream, starting the ping-pump, if needed.
func (s *stream) Start() error {

	// make Start idempotent
	if !atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		return nil
	}

	// customize response to a close-message, based on if this is the
	// server or the client end of the stream
	if s.server {

		// on the server-end of the stream, simply return an error to
		// the ReadMessage call, giving an opportunity to flush out
		// any pending messages before closing out the connection
		s.conn.SetCloseHandler(func(code int, text string) error {
			// when the close-handler returns 'nil' here, the gorilla
			// implementation returns a 'CloseError' to the Read
			return nil
		})

	} else {

		// on the client-end of the stream, if we have received a
		// close-message from the server, there's no point sending
		// any more messages to the server, so simply respond back
		// back with a close-message
		s.conn.SetCloseHandler(func(code int, text string) error {
			// send a close-message; NB: after a close-message is sent, the gorilla-websocket
			// implementation fails any future writes on this connection
			return s.conn.WriteControl(gorilla.CloseMessage, gorilla.FormatCloseMessage(code, "close ack"), time.Now().Add(closeMessageTimeout))
		})
	}

	// start ping pump, if needed
	if s.pingInterval.Nanoseconds() != 0 {
		go s.pingPump()
	}

	return nil
}

// CloseWrite closes the write path after flushing out any buffered writes.
// NB: Write(), Flush(), Close() and CloseWrite() are not thread-safe between
// each other; in other words, the caller needs to ensure that only one of the
// them is invoked at any given time.
func (s *stream) CloseWrite() error {

	// ensure Start had been called
	if atomic.LoadInt32(&s.started) != 1 {
		return errNotStarted
	}

	// make CloseWrite idempotent
	if !atomic.CompareAndSwapInt32(&s.writeClosed, 0, 1) {
		return nil
	}

	err0 := s.Flush() // flush out any buffered messages before sending close message

	// send a close-message; NB: after a close-message is sent, the gorilla-websocket
	// implementation fails any future writes on this connection
	err1 := s.conn.WriteControl(gorilla.CloseMessage, gorilla.FormatCloseMessage(gorilla.CloseGoingAway, "write closed"), time.Now().Add(closeMessageTimeout))

	switch {
	case err0 != nil:
		return fmt.Errorf("CloseWrite error: flush failed: %v", err0)

	case err1 != nil:
		return fmt.Errorf("CloseWrite error: write close msg failed: %v", err1)

	default:
		return nil
	}
}

func (s *stream) closeConn() error {

	// ensure we close the conn only once
	if !atomic.CompareAndSwapInt32(&s.connClosed, 0, 1) {
		return nil
	}

	// stop ping-pump
	close(s.closeC)

	// send a close-message; NB: after a close-message is sent, the gorilla-websocket
	// implementation fails any future writes on this connection
	err0 := s.conn.WriteControl(gorilla.CloseMessage, gorilla.FormatCloseMessage(gorilla.CloseGoingAway, "stream closed"), time.Now().Add(closeMessageTimeout))

	// cose underlying connection
	err1 := s.conn.Close() // close underlying connection

	switch {
	case err0 != nil:
		return fmt.Errorf("write close msg failed: %v", err0)

	case err1 != nil:
		return fmt.Errorf("conn close failed: %v", err1)

	default:
		return nil
	}
}

// Close flushes any buffered messages before closing the underlying websocket connection.
func (s *stream) Close() error {

	// ensure Start had been called
	if atomic.LoadInt32(&s.started) != 1 {
		return errNotStarted
	}

	// ensure we close the conn only once
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}

	err0 := s.Flush() // flush out any buffered messages before sending close message

	err1 := s.closeConn() // close underlying connection

	switch {
	case err0 != nil:
		return fmt.Errorf("Close error: flush: %v", err0)

	case err1 != nil:
		return fmt.Errorf("Close error: closeConn: %v", err1)

	default:
		return nil
	}
}

// setFlushThreshold sets the write-buffer size (in number of messages); this
// is primarily intended for testing.
func (s *stream) setFlushThreshold(flushThreshold int) {

	if flushThreshold < 1 {
		flushThreshold = 1 // ensure we buffer at least one message
	}

	// flush out any buffered messages
	if s.writeMsgIndex > 0 {
		if err := s.Flush(); err != nil {
			return
		}
	}

	s.writeMsgs = make([]thrift.TStruct, flushThreshold)
	s.writeMsgLen = flushThreshold
}

// Read returns one message from the read-buffer; if there aren't any buffered
// messages available, it reads a 'payload' from the wire, deserializes them
// into messages and returns one.
// NB: Read is *not* thread-safe; in other words, the caller needs to ensure
// that there there will *not* be more than one concurrent calls into Read().
func (s *stream) Read() (thrift.TStruct, error) {

	// if we don't have any buffered messages, read from the wire
	if s.readMsgIndex >= len(s.readMsgs) {

		msgType, payload, err := s.conn.ReadMessage()
		if err != nil {
			// if we got an error from read, then tear down the connection
			// if this is the client-end of the stream (since the server has
			// gone away). if this is the server-end of the stream, then
			// simply return an error back. when the Close call comes in
			// we would flush out any buffered messages before tearing down
			// the connection.
			if !s.server {
				s.closeConn()
			}

			return nil, fmt.Errorf("Read error: %v", err)
		}

		switch msgType {
		case gorilla.BinaryMessage:
			// deserialize into 'readMsgType' messages
			s.readMsgs, err = common.TListDeserialize(s.readMsgType, payload)
			if err != nil {
				if !s.server {
					s.closeConn() // on any read error, close connection
				}

				return nil, fmt.Errorf("Deserialize error: %v", err)
			}

			s.readMsgIndex = 0

		case gorilla.TextMessage:
			// earlier versions of gorilla-websocket used to automatically
			// respond to a close-message preventing our code from being able
			// to flush out any buffered messages. to workaround that, we used
			// to send out a special 'TextMessage' to convey close (from the
			// client end of the stream). we still respond to that and handle
			// it appropriately, in case we have an older client connect in
			// to a newer server
			if bytes.Equal(payload, []byte("close")) {
				return nil, fmt.Errorf("Closed")
			}

			fallthrough

		default:
			return nil, fmt.Errorf("Invalid message type [%d]", msgType)
		}
	}

	msg := s.readMsgs[s.readMsgIndex]
	s.readMsgIndex++

	return msg, nil
}

// Write buffers a message; if the write-message buffer is full, it triggers a flush
// of all the buffered messages.
// NB: Write(), Flush(), Close() and CloseWrite() are not thread-safe between
// each other; in other words, the caller needs to ensure that only one of the
// them is invoked at any given time.
func (s *stream) Write(msg thrift.TStruct) error {

	s.writeMsgs[s.writeMsgIndex] = msg
	s.writeMsgIndex++

	// trigger a flush if the write buffer is full
	if s.writeMsgIndex >= s.writeMsgLen {
		return s.Flush()
	}

	return nil
}

// Flush flushes any buffered messages.
// NB: Write(), Flush(), Close() and CloseWrite() are not thread-safe between
// each other; in other words, the caller needs to ensure that only one of the
// them is invoked at any given time.
func (s *stream) Flush() error {

	// no-op, if there's nothing to flush
	if s.writeMsgIndex <= 0 {
		return nil
	}

	// serialize all the buffered messages together into one payload
	payload, err := common.TListSerialize(s.writeMsgs[:s.writeMsgIndex])
	if err != nil {
		return fmt.Errorf("Serialize error: %v", err)
	}

	for i := 0; i < s.writeMsgIndex; i++ {
		s.writeMsgs[i] = nil // free-up for GC
	}

	s.writeMsgIndex = 0 // reset index into write-buffer

	return s.conn.WriteMessage(gorilla.BinaryMessage, payload) // flush!
}

// pingPump sends ping-messages at regular intervals
func (s *stream) pingPump() {

	pingTicker := time.NewTicker(s.pingInterval)
	defer pingTicker.Stop()

	for {
		select {
		case <-pingTicker.C:
			// keeping sending 'pings', until the connection is closed (or the ping-write fails)
			if err := s.conn.WriteControl(gorilla.PingMessage, []byte{}, time.Now().Add(s.pingInterval)); err != nil {
				return
			}

		case <-s.closeC:
			return
		}
	}
}
