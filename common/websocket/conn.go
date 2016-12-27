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
	"time"

	gorilla "github.com/gorilla/websocket"
)

type (
	// Conn interfaces out the underlying (gorilla) websocket implementation, so we
	// can mock it out easily when testing, etc.
	Conn interface {
		ReadMessage() (messageType int, p []byte, err error)
		WriteMessage(messageType int, data []byte) error
		WriteControl(messageType int, data []byte, deadline time.Time) error
		SetCloseHandler(h func(code int, text string) error)
		Close() error
	}

	connImpl struct {
		*gorilla.Conn
	}
)

// interface implementation check
var _ Conn = (*connImpl)(nil)

// NewWebsocketConn creates a websocket Conn
func NewWebsocketConn(c *gorilla.Conn) Conn {
	return &connImpl{Conn: c}
}
