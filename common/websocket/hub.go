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
	"net/http"

	gorilla "github.com/gorilla/websocket"
)

type (
	// Hub is the interface for websocket Hub that can be used to establish websocket connection
	Hub interface {
		// Dial handshakes websocket connection from client side
		Dial(urlStr string, requestHeader http.Header) (Conn, *http.Response, error)
		// Upgrade handshakes and upgrades websocket connection from server side
		Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (Conn, error)
	}

	hubImpl struct {
		// dialer to handshake websocket connection from client side
		dialer *gorilla.Dialer
		// upgrader to handshake and upgrade websocket connection from server side
		upgrader *gorilla.Upgrader
	}
)

// interface implementation check
var _ Hub = (*hubImpl)(nil)

// NewWebsocketHub creates a websocket Hub
func NewWebsocketHub() Hub {
	return &hubImpl{
		upgrader: &gorilla.Upgrader{
			ReadBufferSize:  102400,
			WriteBufferSize: 102400,
		},
		dialer: &gorilla.Dialer{
			ReadBufferSize:  102400,
			WriteBufferSize: 102400,
		},
	}
}

// Dial handshakes websocket connection from client side
func (hub *hubImpl) Dial(urlStr string, requestHeader http.Header) (Conn, *http.Response, error) {
	wsConn, httpResp, err := hub.dialer.Dial(urlStr, requestHeader)
	return NewWebsocketConn(wsConn), httpResp, err
}

// Upgrade handshakes and upgrades websocket connection from server side
func (hub *hubImpl) Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (Conn, error) {
	wsConn, err := hub.upgrader.Upgrade(w, r, responseHeader)
	return NewWebsocketConn(wsConn), err
}
