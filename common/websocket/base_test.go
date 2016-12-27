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
	"sync"

	"github.com/uber/cherami-thrift/.generated/go/cherami"
	log "github.com/Sirupsen/logrus"
)

// -- Websocket Streaming Server -- //
type streamHandler func(recvC <-chan int, sendC chan<- int, flushC chan<- interface{})

func startTestServer(port int, handler streamHandler) error {

	mux := http.NewServeMux()
	wsHub := NewWebsocketHub()
	readMsgType := reflect.TypeOf((*cherami.PutMessage)(nil)).Elem()

	mux.HandleFunc("/test", func(w http.ResponseWriter, r *http.Request) {

		stream := NewStreamServer(w, r, wsHub, readMsgType)

		if err := stream.Start(); err != nil {
			log.Errorf("testServer.startTestServer (start): %v", err)
			return
		}

		serve(stream, handler)
	})

	// listen-and-serve on a separate go-routine
	go http.ListenAndServe(fmt.Sprintf(":%d", port), mux)

	return nil

}

func serve(stream StreamServer, handler streamHandler) {

	recvC, sendC, flushC := make(chan int), make(chan int), make(chan interface{})

	var wg sync.WaitGroup

	// start read/write pumps
	wg.Add(3)
	go readPump(stream, recvC, &wg)
	go writePump(sendC, flushC, stream, &wg)
	go func() {
		defer wg.Done()
		handler(recvC, sendC, flushC)
	}()

	// wait until the read/write/handler pumps are done
	wg.Wait()
}

func readPump(stream StreamServer, recvC chan<- int, wg *sync.WaitGroup) {

	defer wg.Done()
	defer close(recvC) // close outgoing pipe

	for {
		tMsg, err := stream.Read()

		if err != nil {
			// log.Debugf("testServer.readPump (read): %v", err)
			return
		}

		msg, ok := tMsg.(*cherami.PutMessage)

		if !ok {
			log.Errorf("testServer.readPump: invalid msg type")
			return
		}

		// log.Debugf("testServer.readPump: recv %d", getMsgID(msg))

		recvC <- getMsgID(msg)
	}
}

func writePump(sendC chan int, flushC <-chan interface{}, stream StreamServer, wg *sync.WaitGroup) {

	defer wg.Done()
	defer func() {
		// log.Debugf("testServer.writePump: Done")
		if err := stream.Done(); err != nil { // close outgoing pipe
			log.Errorf("testServer.writePump (done): %v", err)
		}
	}()

	for {
		select {
		case i, ok := <-sendC:
			if !ok {
				// log.Debugf("testServer.writePump: closed")
				return
			}

			// log.Debugf("testServer.writePump: Write [%d]", i)
			err := stream.Write(newTestMsg(i))

			if err != nil {
				log.Errorf("testServer.writePump (write): %v", err)
				return
			}

			// log.Debugf("testServer.writePump: sent %d", i)

		case <-flushC:
			// log.Debugf("testServer.writePump: Flush")
			err := stream.Flush()

			if err != nil {
				log.Errorf("testServer.writePump (flush): %v", err)
				return
			}
		}
	}
}

// -- Websocket Streaming Client -- //

type testClient struct {
	recvC  chan int
	sendC  chan int
	flushC chan interface{}
	stream StreamClient

	wg    sync.WaitGroup
	doneC chan struct{}
}

func startTestClient(port int) (*testClient, error) {

	readMsgType := reflect.TypeOf((*cherami.PutMessage)(nil)).Elem()

	stream := NewStreamClient(fmt.Sprintf("ws://localhost:%d/test", port), http.Header{}, NewWebsocketHub(), readMsgType)

	if err := stream.Start(); err != nil {
		log.Errorf("error starting client: %v", err)
		return nil, err
	}

	recvC, sendC, flushC := make(chan int), make(chan int), make(chan interface{})

	t := &testClient{
		recvC:  recvC,
		sendC:  sendC,
		flushC: flushC,
		stream: stream,
		doneC:  make(chan struct{}),
	}

	// start read/write pumps
	t.wg.Add(2)
	go t.readPump(stream, recvC)
	go t.writePump(sendC, flushC, stream)

	go func() {
		t.wg.Wait()    // wait until the read/write/handler pumps are done
		close(t.doneC) // signal doneC
	}()

	return t, nil
}

func (t *testClient) done() <-chan struct{} {
	return t.doneC
}

func (t *testClient) readPump(stream StreamClient, recvC chan<- int) {

	defer t.wg.Done()
	defer close(recvC) // close outgoing pipe

	for {
		tMsg, err := stream.Read()

		if err != nil {
			// log.Debugf("testClient.readPump: %v", err)
			return
		}

		msg, ok := tMsg.(*cherami.PutMessage)

		if !ok {
			log.Errorf("testClient.readPump: invalid msg type")
			return
		}

		// log.Debugf("testClient.readPump: recv %d", getMsgID(msg))

		recvC <- getMsgID(msg)
	}
}

func (t *testClient) writePump(sendC chan int, flushC <-chan interface{}, stream StreamClient) {

	defer t.wg.Done()
	defer func() {
		// log.Debugf("testClient.writePump: Done")
		if err := stream.Done(); err != nil { // close outgoing pipe
			log.Errorf("testClient.writePump (done): %v", err)
		}
	}()

	for {
		select {
		case i, ok := <-sendC:
			if !ok {
				// log.Debugf("testClient.writePump: closed")
				return
			}

			// log.Debugf("testClient.writePump: Write [%d]", i)
			err := stream.Write(newTestMsg(i))

			if err != nil {
				log.Errorf("testClient.writePump: write error=%v", err)
				return
			}

			// log.Debugf("testClient.writePump: sent %d", i)

		case <-flushC:
			// log.Debugf("testClient.writePump: Flush")
			err := stream.Flush()

			if err != nil {
				log.Errorf("testClient.writePump: flush error=%v", err)
				return
			}
		}
	}
}
