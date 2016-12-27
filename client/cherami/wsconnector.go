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

package cherami

import (
	"fmt"
	"net/http"
	"reflect"

	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/websocket"
	"github.com/uber/cherami-client-go/stream"
)

type (
	// WSConnector takes care of establishing connection via websocket stream
	WSConnector interface {
		OpenPublisherStream(hostPort string, requestHeader http.Header) (stream.BInOpenPublisherStreamOutCall, error)
		OpenConsumerStream(hostPort string, requestHeader http.Header) (stream.BOutOpenConsumerStreamOutCall, error)
	}

	wsConnectorImpl struct {
		wsHub websocket.Hub

		openPublisherOutStreamReadType reflect.Type
		openConsumerOutStreamReadType  reflect.Type
	}

	// OpenPublisherOutWebsocketStream is a wrapper for websocket to work with OpenPublisherStream
	OpenPublisherOutWebsocketStream struct {
		stream websocket.StreamClient
	}

	// OpenConsumerOutWebsocketStream is a wrapper for websocket to work with OpenPublisherStream
	OpenConsumerOutWebsocketStream struct {
		stream websocket.StreamClient
	}
)

// interface implementation check
var _ WSConnector = (*wsConnectorImpl)(nil)
var _ stream.BInOpenPublisherStreamOutCall = &OpenPublisherOutWebsocketStream{}
var _ stream.BOutOpenConsumerStreamOutCall = &OpenConsumerOutWebsocketStream{}

// NewWSConnector creates a WSConnector
func NewWSConnector() WSConnector {
	return &wsConnectorImpl{
		wsHub: websocket.NewWebsocketHub(),
		openPublisherOutStreamReadType: reflect.TypeOf((*cherami.InputHostCommand)(nil)).Elem(),
		openConsumerOutStreamReadType:  reflect.TypeOf((*cherami.OutputHostCommand)(nil)).Elem(),
	}
}

func (c *wsConnectorImpl) OpenPublisherStream(hostPort string, requestHeader http.Header) (stream.BInOpenPublisherStreamOutCall, error) {
	url := fmt.Sprintf(common.WSUrlFormat, hostPort, common.EndpointOpenPublisherStream)

	stream := websocket.NewStreamClient(url, requestHeader, c.wsHub, c.openPublisherOutStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return &OpenPublisherOutWebsocketStream{stream: stream}, nil
}

// Write writes a result to the response stream
func (s *OpenPublisherOutWebsocketStream) Write(arg *cherami.PutMessage) error {
	return s.stream.Write(arg)
}

// Flush flushes all written arguments.
func (s *OpenPublisherOutWebsocketStream) Flush() error {
	return s.stream.Flush()
}

// Done closes the request stream and should be called after all arguments have been written.
func (s *OpenPublisherOutWebsocketStream) Done() error {
	return s.stream.Done()
}

// Read returns the next argument, if any is available.
func (s *OpenPublisherOutWebsocketStream) Read() (*cherami.InputHostCommand, error) {

	msg, err := s.stream.Read()
	if err != nil {
		return nil, err
	}
	return msg.(*cherami.InputHostCommand), err
}

// ResponseHeaders is defined to conform to the tchannel-stream .*OutCall interface
func (s *OpenPublisherOutWebsocketStream) ResponseHeaders() (map[string]string, error) {
	return map[string]string{}, nil
}

func (c *wsConnectorImpl) OpenConsumerStream(hostPort string, requestHeader http.Header) (stream.BOutOpenConsumerStreamOutCall, error) {

	url := fmt.Sprintf(common.WSUrlFormat, hostPort, common.EndpointOpenConsumerStream)

	stream := websocket.NewStreamClient(url, requestHeader, c.wsHub, c.openConsumerOutStreamReadType)

	if err := stream.Start(); err != nil {
		return nil, err
	}

	return &OpenConsumerOutWebsocketStream{stream: stream}, nil
}

// Write writes a result to the response stream
func (s *OpenConsumerOutWebsocketStream) Write(arg *cherami.ControlFlow) error {
	return s.stream.Write(arg)
}

// Flush flushes all written arguments.
func (s *OpenConsumerOutWebsocketStream) Flush() error {
	return s.stream.Flush()
}

// Done closes the request stream and should be called after all arguments have been written.
func (s *OpenConsumerOutWebsocketStream) Done() error {
	return s.stream.Done()
}

// Read returns the next argument, if any is available.
func (s *OpenConsumerOutWebsocketStream) Read() (*cherami.OutputHostCommand, error) {

	msg, err := s.stream.Read()
	if err != nil {
		return nil, err
	}
	return msg.(*cherami.OutputHostCommand), err
}

// ResponseHeaders is defined to conform to the tchannel-stream .*OutCall interface
func (s *OpenConsumerOutWebsocketStream) ResponseHeaders() (map[string]string, error) {
	return map[string]string{}, nil
}
