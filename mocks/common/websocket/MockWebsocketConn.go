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

import "github.com/stretchr/testify/mock"

import "time"

// MockWebsocketConn is a mock for WebsocketConn used for unit testing
type MockWebsocketConn struct {
	mock.Mock
}

// ReadMessage reads a message
func (_m *MockWebsocketConn) ReadMessage() (int, []byte, error) {
	ret := _m.Called()

	var r0 int
	if rf, ok := ret.Get(0).(func() int); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int)
	}

	var r1 []byte
	if rf, ok := ret.Get(1).(func() []byte); ok {
		r1 = rf()
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([]byte)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func() error); ok {
		r2 = rf()
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// WriteMessage writes a message
func (_m *MockWebsocketConn) WriteMessage(messageType int, data []byte) error {
	ret := _m.Called(messageType, data)

	var r0 error
	if rf, ok := ret.Get(0).(func(int, []byte) error); ok {
		r0 = rf(messageType, data)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// WriteControl writes a control message
func (_m *MockWebsocketConn) WriteControl(messageType int, data []byte, deadline time.Time) error {
	ret := _m.Called(messageType, data, deadline)

	var r0 error
	if rf, ok := ret.Get(0).(func(int, []byte, time.Time) error); ok {
		r0 = rf(messageType, data, deadline)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SetCloseHandler sets a close handler
func (_m *MockWebsocketConn) SetCloseHandler(h func(code int, text string) error) {

	ret := _m.Called(h)

	if rf, ok := ret.Get(0).(func(h func(code int, text string) error) error); ok {
		rf(h)
	}

	return
}

// Close closes the connection
func (_m *MockWebsocketConn) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
