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
	"os"
	"testing"
	"time"

	// "github.com/stretchr/testify/mock"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type (
	// ServerStreamSuite tests websocket stream implementation
	// This test suite uses thrift PutMessage as test message struct (or it can be anything thrift struct)
	ServerStreamSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func TestServerStreamSuite(t *testing.T) {
	suite.Run(t, new(ServerStreamSuite))
}

func (s *ServerStreamSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
		log.SetLevel(log.DebugLevel) // test logs at debug level
	}
}

func (s *ServerStreamSuite) TearDownTest() {
}

func (s *ServerStreamSuite) TestStreaming() {

	port := 6192 // use ephemeral port

	stopC := make(chan struct{})

	serverPump := func(recvC <-chan int, sendC chan<- int, flushC chan<- interface{}) {

		defer close(sendC) // close outgoing pipe (to stop stream)

		flushTicker := time.NewTicker(100 * time.Millisecond)
		defer flushTicker.Stop()

		for {
			select {
			case i, ok := <-recvC:

				if !ok {
					return
				}

				sendC <- -i // negate and send back
				// log.Debugf("serverPump: recv=%d send=%d", i, -i)

			case <-flushTicker.C:
				flushC <- true

			case <-stopC:
				return
			}
		}
	}

	// log.Debugf("starting test server:")
	err := startTestServer(port, serverPump)

	if err != nil {
		log.Errorf("error starting testServer: %v", err)
		return
	}

	// -- TEST 1: client going away gracefully -- //
	numSend := 4099

	// log.Debugf("starting test client:")
	client, err := startTestClient(port)

	if err != nil {
		log.Errorf("error starting testClient: %v", err)
		return
	}

	// do the writes
	for i := 1; i <= numSend; i++ {
		client.sendC <- i
	}

	// client.flushC <- true
	// log.Debugf("client done sending")
	close(client.sendC) // stop writePump; effectively calls stream.Done()

	// now drain all the reads
	var numRecv int
	for i := range client.recvC {
		// log.Debugf("client recv: %d", i)
		numRecv++
		s.Equal(numRecv, -i)
	}

	// log.Debugf("sent=%d msgs; recv=%d msgs", numSend, numRecv)

	s.Equal(numSend, numRecv, fmt.Sprintf("numSend=%d != numRecv=%d", numSend, numRecv))
	s.True(waitFor(5000, func() bool {
		select {
		case <-client.done():
			return true
		default:
			return false
		}
	}), "timed out waiting for client to be done")

	/*
		// -- TEST 2: server going away abruptly -- //
		numSend := 4099

		// log.Debugf("starting test client:")
		client, err := startTestClient(port)

		if err != nil {
			log.Errorf("error starting testClient: %v", err)
			return
		}rl

		// do the writes
		for i := 1; i <= numSend; i++ {
			client.sendC <- i
		}

		// client.flushC <- true
		close(client.sendC) // stop writePump; effectively calls stream.Done()

		// now drain all the reads
		var numRecv int
		for i := range client.recvC {
			// log.Debugf("client recv: %d", i)
			numRecv++
			s.Equal(numRecv, -i)
		}

		// log.Debugf("client stream closed (sent=%d msgs; recv=%d msgs)", numSend, numRecv)

		s.Equal(numSend, numRecv, fmt.Sprintf("numSend=%d != numRecv=%d", numSend, numRecv))
		s.True(waitFor(5000, func() bool {
			select {
			case <-client.done():
				return true
			default:
				return false
			}
		}), "timed out waiting for client to be done")
	*/
}
