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
	"errors"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	gorilla "github.com/gorilla/websocket"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cherami-client-go/common"
	mockWS "github.com/uber/cherami-client-go/mocks/common/websocket"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
)

type (
	// StreamSuite tests websocket stream implementation
	// This test suite uses thrift PutMessage as test message struct (or it can be anything thrift struct)
	StreamSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
	}
)

func TestStreamSuite(t *testing.T) {
	suite.Run(t, new(StreamSuite))
}

func (s *StreamSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}
}

func (s *StreamSuite) TearDownTest() {
}

var testMsgType = reflect.TypeOf((*cherami.PutMessage)(nil)).Elem()

func newTestMsg(id int) *cherami.PutMessage {
	stringID := strconv.Itoa(id)
	return &cherami.PutMessage{
		ID:   common.StringPtr(stringID),
		Data: []byte(stringID),
	}
}

func getMsgID(msg *cherami.PutMessage) int {
	id, _ := strconv.Atoi(msg.GetID())
	return id
}

func newTestPayload(n int) []byte {
	messages := make([]thrift.TStruct, 0, n)
	for i := 0; i < n; i++ {
		messages = append(messages, newTestMsg(i))
	}
	payload, _ := common.TListSerialize(messages)
	return payload
}

// TestOpen tests normal open/cloase operations
func (s *StreamSuite) TestOpenClose() {

	mockConn := &mockWS.MockWebsocketConn{}
	mockConn.On("SetCloseHandler", mock.Anything).Return(nil).Once()
	mockConn.On("WriteControl", gorilla.CloseMessage, mock.Anything, mock.Anything).Return(nil).Once()
	mockConn.On("Close").Return(nil)

	stream := NewStream(mockConn, &StreamOpts{})
	stream.Start()

	err := stream.Close()
	s.NoError(err)

	mockConn.AssertExpectations(s.T())
}

// TestOpenCloseError tests error out open/done operations
func (s *StreamSuite) TestOpenCloseError() {

	mockConn := &mockWS.MockWebsocketConn{}
	mockConn.On("SetCloseHandler", mock.Anything).Return(nil).Once()
	mockConn.On("WriteControl", gorilla.CloseMessage, mock.Anything, mock.Anything).Return(nil).Once()
	mockConn.On("Close").Return(errors.New("Close error"))

	stream := NewStream(mockConn, &StreamOpts{})
	stream.Start()

	err := stream.Close()
	s.Error(err)

	mockConn.AssertExpectations(s.T())
}

// TestPing tests open with ping pump enabled
func (s *StreamSuite) TestPing() {

	mockConn := &mockWS.MockWebsocketConn{}
	mockConn.On("SetCloseHandler", mock.Anything).Return(nil).Once()
	mockConn.On("WriteControl", gorilla.PingMessage, mock.Anything, mock.Anything).Return(nil)
	mockConn.On("WriteControl", gorilla.CloseMessage, mock.Anything, mock.Anything).Return(nil).Once()
	mockConn.On("Close").Return(nil)

	pingInternal := 10 * time.Millisecond

	stream := NewStream(mockConn, &StreamOpts{PingInterval: pingInternal})
	stream.Start()

	time.Sleep(2 * pingInternal) // wait for at least one ping to go through

	err := stream.Close()
	s.NoError(err)

	mockConn.AssertExpectations(s.T())
}

// TestPingError tests ping pump error out
func (s *StreamSuite) TestPingError() {

	mockConn := &mockWS.MockWebsocketConn{}
	mockConn.On("SetCloseHandler", mock.Anything).Return(nil).Once()
	mockConn.On("WriteControl", gorilla.PingMessage, mock.Anything, mock.Anything).Return(errors.New("WriteControl error"))
	mockConn.On("WriteControl", gorilla.CloseMessage, mock.Anything, mock.Anything).Return(nil).Once()
	mockConn.On("Close").Return(nil)

	pingInternal := 10 * time.Millisecond

	stream := NewStream(mockConn, &StreamOpts{PingInterval: pingInternal})
	stream.Start()

	time.Sleep(2 * pingInternal) // wait for at least one ping to go through

	err := stream.Close()
	s.NoError(err) // ping pump error should not impact stream's normal operations

	mockConn.AssertExpectations(s.T())
}

// TestRead tests multiple normal read operations
func (s *StreamSuite) TestRead() {

	msgCount := 3
	readLoop := 2

	mockConn := &mockWS.MockWebsocketConn{}
	mockConn.On("SetCloseHandler", mock.Anything).Return(nil).Once()
	mockConn.On("ReadMessage").Return(gorilla.BinaryMessage, newTestPayload(msgCount), nil)
	mockConn.On("WriteControl", gorilla.CloseMessage, mock.Anything, mock.Anything).Return(nil).Once()
	mockConn.On("Close").Return(nil)

	stream := NewStream(mockConn, &StreamOpts{ReadMsgType: testMsgType})
	stream.Start()

	// each read loop will read all message in one payload
	for i := 0; i < readLoop; i++ {
		for j := 0; j < msgCount; j++ {

			msg, err := stream.Read()
			s.NoError(err)

			typedMsg := msg.(*cherami.PutMessage)
			id, _ := strconv.Atoi(typedMsg.GetID())
			s.Equal(j, id, "id mismatch")
		}
	}

	err := stream.Close()
	s.NoError(err)

	mockConn.AssertExpectations(s.T())
}

// TestReadError tests normal read error out
func (s *StreamSuite) TestReadError() {

	mockConn := &mockWS.MockWebsocketConn{}
	mockConn.On("SetCloseHandler", mock.Anything).Return(nil).Once()
	mockConn.On("ReadMessage").Return(gorilla.BinaryMessage, newTestPayload(1), nil).Once()
	mockConn.On("ReadMessage").Return(gorilla.BinaryMessage, nil, errors.New("ReadMessage error")).Once()
	mockConn.On("WriteControl", gorilla.CloseMessage, mock.Anything, mock.Anything).Return(nil).Once()
	mockConn.On("Close").Return(nil)

	stream := NewStream(mockConn, &StreamOpts{ReadMsgType: testMsgType})
	stream.Start()

	_, err := stream.Read()
	s.NoError(err)

	_, err = stream.Read()
	s.Error(err)

	err = stream.Close()
	s.NoError(err)

	mockConn.AssertExpectations(s.T())
}

// TestWrite tests multiple normal write operations
func (s *StreamSuite) TestWrite() {

	flushThreshold := 1
	writeLoop := 3
	totalMsgCount := flushThreshold * writeLoop

	mockConn := &mockWS.MockWebsocketConn{}
	mockConn.On("SetCloseHandler", mock.Anything).Return(nil).Once()
	mockConn.On("WriteMessage", gorilla.BinaryMessage, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		payload := args.Get(1).([]byte)
		msgs, err := common.TListDeserialize(testMsgType, payload)
		s.NoError(err)
		s.Equal(flushThreshold, len(msgs), "unexpected number of messages")
		typedMsg := msgs[0].(*cherami.PutMessage)
		id, _ := strconv.Atoi(typedMsg.GetID())
		s.True(id >= 1 && id <= totalMsgCount, "id out of range")
	})
	mockConn.On("WriteControl", gorilla.CloseMessage, mock.Anything, mock.Anything).Return(nil).Once()
	mockConn.On("Close").Return(nil)

	stream := NewStream(mockConn, &StreamOpts{ReadMsgType: testMsgType, FlushThreshold: flushThreshold})
	stream.Start()

	// write multiple messages
	for i := 1; i <= totalMsgCount; i++ {
		err := stream.Write(newTestMsg(i))
		s.NoError(err)
	}

	err := stream.Close()
	s.NoError(err)
	mockConn.AssertExpectations(s.T())
}

// TestWriteFlush tests the flush mechanisms:
// 1. flush automatically, when we have buffered messages equal to the flushThreshold
// 2. flush, when Flush is called
// 3. flush, when Close is called and there were buffered messages
func (s *StreamSuite) TestWriteFlushThreshold() {

	flushThreshold := 3
	writeLoop := 2
	flushMsgs := 1
	tailMsgs := 2

	mockConn := &mockWS.MockWebsocketConn{}
	mockConn.On("SetCloseHandler", mock.Anything).Return(nil).Once()
	mockConn.On("WriteMessage", gorilla.BinaryMessage, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		payload := args.Get(1).([]byte)
		msgs, err := common.TListDeserialize(testMsgType, payload)
		s.NoError(err)
		s.Equal(flushThreshold, len(msgs), "unexpected number of messages")
		for i := 0; i < flushThreshold; i++ {
			typedMsg := msgs[i].(*cherami.PutMessage)
			id, _ := strconv.Atoi(typedMsg.GetID())
			s.Equal(i, id, "id mismatch")
		}
	}).Times(writeLoop)
	mockConn.On("WriteMessage", gorilla.BinaryMessage, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		payload := args.Get(1).([]byte)
		msgs, err := common.TListDeserialize(testMsgType, payload)
		s.NoError(err)
		s.Equal(flushMsgs, len(msgs), "unexpected number of messages")
		for i := 0; i < flushMsgs; i++ {
			typedMsg := msgs[i].(*cherami.PutMessage)
			id, _ := strconv.Atoi(typedMsg.GetID())
			s.Equal(i, id, "id mismatch")
		}
	}).Once()
	mockConn.On("WriteMessage", gorilla.BinaryMessage, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		payload := args.Get(1).([]byte)
		msgs, err := common.TListDeserialize(testMsgType, payload)
		s.NoError(err)
		s.Equal(tailMsgs, len(msgs), "unexpected number of messages")
		for i := 0; i < tailMsgs; i++ {
			typedMsg := msgs[i].(*cherami.PutMessage)
			id, _ := strconv.Atoi(typedMsg.GetID())
			s.Equal(i, id, "id mismatch")
		}
	}).Once()
	mockConn.On("WriteControl", gorilla.CloseMessage, mock.Anything, mock.Anything).Return(nil).Once()
	mockConn.On("Close").Return(nil)

	stream := NewStream(mockConn, &StreamOpts{ReadMsgType: testMsgType, FlushThreshold: flushThreshold})
	stream.Start()

	// each write loop will write batch of messages up to flush threshold
	for i := 0; i < writeLoop; i++ {
		for j := 0; j < flushThreshold; j++ {

			err := stream.Write(newTestMsg(j))
			s.NoError(err)
		}
	}

	// write some messages and call Flush
	for j := 0; j < flushMsgs; j++ {

		err := stream.Write(newTestMsg(j))
		s.NoError(err)
	}
	err := stream.Flush()
	s.NoError(err)

	// write some extra "tail" messages, that should get flushed with the close
	for j := 0; j < tailMsgs; j++ {

		err = stream.Write(newTestMsg(j))
		s.NoError(err)
	}

	err = stream.Close()
	s.NoError(err)

	mockConn.AssertExpectations(s.T())
}

// TestWriteError tests write error out
func (s *StreamSuite) TestWriteError() {

	flushThreshold := 2

	mockConn := &mockWS.MockWebsocketConn{}
	mockConn.On("SetCloseHandler", mock.Anything).Return(nil).Once()
	mockConn.On("WriteMessage", gorilla.BinaryMessage, mock.Anything).Return(errors.New("WriteMessage error"))
	mockConn.On("WriteControl", gorilla.CloseMessage, mock.Anything, mock.Anything).Return(nil).Once()
	mockConn.On("Close").Return(nil)

	stream := NewStream(mockConn, &StreamOpts{ReadMsgType: testMsgType, FlushThreshold: flushThreshold})
	stream.Start()

	err := stream.Write(newTestMsg(0))
	s.NoError(err) // the first msg just gets buffered and will not see an error

	err = stream.Write(newTestMsg(1))
	s.Error(err) // the second msg will fill the buffer, causing a flush that will see the error

	err = stream.Close()
	s.NoError(err)

	mockConn.AssertExpectations(s.T())
}

// mocks the low-level 'Conn' and exposes it as a pair of IO and error channels
type mockConn struct {
	writeMsgC chan []byte   // WriteMessage writes out messages onto this channel
	writeErrC chan error    // WriteMessage returns errors sent to this channel
	readMsgC  chan []byte   // ReadMessage reads and returns messages sent to this channel
	readErrC  chan error    // ReadMessage returns errors sent to this channel
	closeC    chan struct{} // Done closes this channel
}

type msgType int

const (
	msgBinary = iota
	msgClose
	msgPing
	msgPong
	msgError
)

type msg struct {
	msgType msgType
	payload []byte
}

func newBinaryMsg(payload []byte) *msg {
	return &msg{msgType: msgBinary, payload: payload}
}

func newMockConn() *mockConn {
	return &mockConn{
		writeMsgC: make(chan []byte),
		readMsgC:  make(chan []byte),
		closeC:    make(chan struct{}),
	}
}

func (t *mockConn) ReadMessage() (int, []byte, error) {
	select {
	case msg, ok := <-t.readMsgC:
		if !ok {
			// if readMsgC was closed, return an error
			return gorilla.BinaryMessage, nil, errors.New(`EOF`)
		}
		return gorilla.BinaryMessage, msg, nil
	case err := <-t.readErrC:
		return gorilla.BinaryMessage, nil, err

	}
}

func (t *mockConn) WriteMessage(messageType int, msg []byte) (err error) {
	select {
	case t.writeMsgC <- msg:
		return nil
	case err = <-t.writeErrC:
		return err
	case <-t.closeC:
		return errors.New(`closed`)
	}
}

func (t *mockConn) WriteControl(messageType int, data []byte, deadline time.Time) error {
	return nil
}

func (t *mockConn) SetCloseHandler(h func(code int, text string) error) {
	return
}

func (t *mockConn) Close() error {
	close(t.readMsgC) // on 'conn' close, the websocket layer returns messages on read, until the end
	close(t.closeC)
	return nil
}

// wait for given 'timeout' (in milliseconds) for condition 'cond' to satisfy
func waitFor(timeout int, cond func() bool) bool {
	for i := 0; i < timeout/10; i++ {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestWriteWithClose tests to verify that all writes are received despite a close
func (s *StreamSuite) TestWritesWithClose() {

	numMsgs := 1500 // greater than defaultChannelSize (= 1024)
	flushThreshold := 32
	writeMessagesDelay := 10 * time.Millisecond

	var lastMsgID int64

	conn := newMockConn()

	// the following responds to conn.WriteMessage by validating that the messages
	// are in order each time also storing the id of the message in 'lastMsgID'.
	// in addition, this sleeps to simulate a "slow" write .. this would have the
	// effect of keeping the 'writeChan' in websocket-stream to fill up.
	// in the meantime, the call to conn.ReadMessages remains blocked.
	go func() {
	pump:
		for {
			select {
			case payload := <-conn.writeMsgC: // mockConn.WriteMessage puts messages into writeMsgC
				msgs, err := common.TListDeserialize(testMsgType, payload)
				s.NoError(err)

				for _, m := range msgs {
					typedMsg := m.(*cherami.PutMessage)
					id, _ := strconv.Atoi(typedMsg.GetID())
					s.Equal(atomic.LoadInt64(&lastMsgID)+1, int64(id), "msg out of order")
					atomic.StoreInt64(&lastMsgID, int64(id))
				}

				time.Sleep(writeMessagesDelay) // slow writes, to cause 'writeChan' to fill up

			case <-conn.closeC: // mockConn.Done() closes closeC
				break pump
			}
		}
	}()

	stream := NewStream(conn, &StreamOpts{ReadMsgType: testMsgType, FlushThreshold: flushThreshold})
	stream.Start()

	// write multiple messages
	for i := 1; i <= numMsgs; i++ {

		err := stream.Write(newTestMsg(i))
		s.NoError(err)
	}

	err := stream.Close()
	s.NoError(err)

	// wait 2 seconds until all the messages have been flushed out
	s.True(waitFor(2000, func() bool {
		return int64(numMsgs) == atomic.LoadInt64(&lastMsgID)
	}), `missing messages`)
}

// TestReadWithClose tests to verify that all reads are received despite a close
func (s *StreamSuite) TestReadsWithClose() {

	numMsgs := 3*1024 + 5 // ensure there's more than 'defaultChannelSize' payloads
	payloadMsgs := 3      // every 3 messages, make a payload and "send"

	var lastMsgID int64

	conn := newMockConn()

	stream := NewStream(conn, &StreamOpts{ReadMsgType: testMsgType})
	stream.Start()

	// the following responds to conn.ReadMessage by validating that the messages
	// are in order each time also storing the id of the message in 'lastMsgID'.
	// in addition, this sleeps to simulate a "slow" read .. this would have the
	// effect of keeping the 'readChan' in websocket-stream to fill up.
	// in the meantime, the call to conn.ReadMessages remains blocked.
	go func() {

		payload := make([]thrift.TStruct, 0, payloadMsgs)

		for i := 1; i <= numMsgs; i++ {

			payload = append(payload, newTestMsg(i))

			if len(payload) >= payloadMsgs || i == numMsgs {

				pl, err := common.TListSerialize(payload)
				if err != nil {
					panic("TListSerialize failed")
				}

				conn.readMsgC <- pl
				payload = payload[:0] // truncate, keeping capacity
			}
		}

		// once all the payloads are on the connection-stream, call 'Close' on the stream
		stream.Close()
	}()

	time.Sleep(time.Second) // wait for messages to be written and the stream closed

	// we should still be able to "drain" out all messages on the stream
	for {
		tMsg, err := stream.Read()

		if err != nil {
			break
		}

		msg := tMsg.(*cherami.PutMessage)

		id, _ := strconv.Atoi(msg.GetID())
		s.Equal(lastMsgID+1, int64(id), "msg out of order")
		lastMsgID = int64(id)
	}

	s.EqualValues(numMsgs, lastMsgID) // we should have received all the messages on the stream
}
