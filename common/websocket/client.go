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
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

const (
	defaultClientFlushThreshold = 64               // 640 kiB
	defaultPingInterval         = 60 * time.Second // defaultPingInterval is the interval at which websocket PingMessages are sent
)

type (
	// Dialer interface
	Dialer interface {
		// Dial opens a websocket connection from the client
		Dial(urlStr string, requestHeader http.Header) (Conn, *http.Response, error)
	}

	// StreamClient defines the interface for the 'client' side of the stream
	StreamClient interface {
		// Start starts the stream-client
		Start() error
		// Write writes a message into the write buffer
		Write(thrift.TStruct) error
		// Flush flushes out buffered messages
		Flush() error
		// Read reads and returns a message
		Read() (thrift.TStruct, error)
		// Done is used to indicate that the client is done
		Done() error
	}

	// streamClient is a wrapper for websocket-stream client that batches responses/requests
	streamClient struct {
		Stream

		url           string
		requestHeader http.Header
		dialer        Dialer
		readMsgType   reflect.Type

		started int32
	}
)

// NewStreamClient initializes a websocket-streaming client to the given url
func NewStreamClient(url string, requestHeader http.Header, dialer Dialer, readMsgType reflect.Type) StreamClient {

	return &streamClient{
		url:           url,
		requestHeader: requestHeader,
		dialer:        dialer,
		readMsgType:   readMsgType,
	}
}

func (s *streamClient) Start() error {

	// make Start idempotent
	if !atomic.CompareAndSwapInt32(&s.started, 0, 1) {
		return nil
	}

	wsConn, _, err := s.dialer.Dial(s.url, s.requestHeader)
	if err != nil {
		return fmt.Errorf("Dial failed: %v", err)
	}

	s.Stream = NewStream(wsConn, &StreamOpts{ReadMsgType: s.readMsgType, FlushThreshold: defaultClientFlushThreshold, PingInterval: defaultPingInterval})

	return s.Stream.Start()
}

// Done closes the write-path on the client, leaving the read-path open so it can be drained
func (s *streamClient) Done() error {

	// shutdown only the write-path, leaving the read-path open so it can be drained by the application
	return s.Stream.CloseWrite()
}
