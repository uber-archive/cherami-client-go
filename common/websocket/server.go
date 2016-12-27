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
	"fmt"
	"net/http"
	"reflect"
	"sync/atomic"

	"github.com/apache/thrift/lib/go/thrift"
)

const (
	defaultServerFlushThreshold = 64 // 640 kiB
)

type (
	// Upgrader interface
	Upgrader interface {
		// Upgrade handshakes and upgrades websocket connection from server side
		Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (Conn, error)
	}

	// StreamServer defines the interface for the 'server' side of the stream that batches responses/requests
	StreamServer interface {
		// Start starts the stream-server
		Start() error
		// Write writes a message into the write buffer
		Write(msg thrift.TStruct) (err error)
		// Flush flushes out buffered messages
		Flush() (err error)
		// Read reads and returns a message
		Read() (msg thrift.TStruct, err error)
		// Done is used to indicate that the server is done
		Done() (err error)
	}

	// streamServer is a wrapper for websocket-stream server that batches responses/requests
	streamServer struct {
		Stream

		httpRespWriter http.ResponseWriter
		httpRequest    *http.Request
		upgrader       Upgrader
		readMsgType    reflect.Type

		started int32
	}
)

// NewStreamServer initializes a new websocket-streaming server
func NewStreamServer(httpRespWriter http.ResponseWriter, httpRequest *http.Request, upgrader Upgrader, readMsgType reflect.Type) StreamServer {

	return &streamServer{
		httpRespWriter: httpRespWriter,
		httpRequest:    httpRequest,
		upgrader:       upgrader,
		readMsgType:    readMsgType,
	}
}

func (s *streamServer) Start() error {

	// make Start idempotent
	if !atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		return nil
	}

	wsConn, err := s.upgrader.Upgrade(s.httpRespWriter, s.httpRequest, nil)
	if err != nil {
		return fmt.Errorf("Upgrade failed: %v", err)
	}

	s.Stream = NewStream(wsConn, &StreamOpts{ReadMsgType: s.readMsgType, FlushThreshold: defaultServerFlushThreshold, Server: true})

	return s.Stream.Start()
}

// Done closes both the read and write-paths and the underlying connection on the server
func (s *streamServer) Done() error {

	// tear-down the underlying connection
	return s.Stream.Close()
}
