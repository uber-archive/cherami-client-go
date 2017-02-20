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
	"errors"
	"io"
	"testing"
	"time"

	_ "fmt"
	_ "strconv"

	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/metrics"
	mc "github.com/uber/cherami-client-go/mocks/clients/cherami"
	"github.com/uber/cherami-thrift/.generated/go/cherami"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
)

type ConnectionSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestConnectionSuite(t *testing.T) {
	suite.Run(t, new(ConnectionSuite))
}

func (s *ConnectionSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *ConnectionSuite) TestSuccess() {
	conn, inputHostClient, messagesCh := createConnection()

	// Setup inputHostClient mock for successful message
	inputHostClient.On("Write", mock.Anything).Return(nil)
	inputHostClient.On("Flush").Return(nil)
	inputHostClient.On("Read").Return(wrapAckInCommand(&cherami.PutMessageAck{
		ID:     common.StringPtr("1"),
		Status: common.CheramiStatusPtr(cherami.Status_OK),
	}), nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	message := &cherami.PutMessage{
		ID:   common.StringPtr("1"),
		Data: []byte("test"),
	}
	requestDone := make(chan *PublisherReceipt, 1)

	messagesCh <- putMessageRequest{message, requestDone}
	receipt := <-requestDone

	s.Equal(*message.ID, receipt.ID)
	s.Nil(receipt.Error, "Expected the message to be written successfully.")
	// TODO: mock assert expectations is not working for some reason.
	//inputHostClient.AssertExpectations(s.T())
}

func (s *ConnectionSuite) TestFailedResponse() {
	conn, inputHostClient, messagesCh := createConnection()

	// Setup inputHostClient mock for successful message
	inputHostClient.On("Write", mock.Anything).Return(nil)
	inputHostClient.On("Flush").Return(nil)
	inputHostClient.On("Read").Return(wrapAckInCommand(&cherami.PutMessageAck{
		ID:      common.StringPtr("1"),
		Status:  common.CheramiStatusPtr(cherami.Status_FAILED),
		Message: common.StringPtr("Failed"),
	}), nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	message := &cherami.PutMessage{
		ID:   common.StringPtr("1"),
		Data: []byte("test"),
	}
	requestDone := make(chan *PublisherReceipt, 1)

	messagesCh <- putMessageRequest{message, requestDone}
	receipt := <-requestDone

	s.Equal(*message.ID, receipt.ID)
	s.NotNil(receipt.Error)
	s.Equal("Failed", receipt.Error.Error())
	// TODO: mock assert expectations is not working for some reason.
	//inputHostClient.AssertExpectations(s.T())
}

func (s *ConnectionSuite) TestWriteFailed() {
	conn, inputHostClient, messagesCh := createConnection()

	// Setup inputHostClient mock for successful message
	inputHostClient.On("Write", mock.Anything).Return(errors.New("Failed"))
	inputHostClient.On("Done").Return(nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	message := &cherami.PutMessage{
		ID:   common.StringPtr("1"),
		Data: []byte("test"),
	}
	requestDone := make(chan *PublisherReceipt, 1)

	messagesCh <- putMessageRequest{message, requestDone}
	receipt := <-requestDone

	s.Equal(*message.ID, receipt.ID)
	s.NotNil(receipt.Error)
	s.Equal("Failed", receipt.Error.Error())
	// TODO: mock assert expectations is not working for some reason.
	//inputHostClient.AssertExpectations(s.T())
}

func (s *ConnectionSuite) TestFlushFailed() {
	conn, inputHostClient, messagesCh := createConnection()

	// Setup inputHostClient mock for successful message
	inputHostClient.On("Write", mock.Anything).Return(nil)
	inputHostClient.On("Flush").Return(errors.New("Failed"))
	inputHostClient.On("Done", mock.Anything).Return(nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	message := &cherami.PutMessage{
		ID:   common.StringPtr("1"),
		Data: []byte("test"),
	}
	requestDone := make(chan *PublisherReceipt, 1)

	messagesCh <- putMessageRequest{message, requestDone}
	receipt := <-requestDone

	s.Equal(*message.ID, receipt.ID)
	s.NotNil(receipt.Error)
	s.Equal("Failed", receipt.Error.Error())
	// TODO: mock assert expectations is not working for some reason.
	//inputHostClient.AssertExpectations(s.T())
}

func (s *ConnectionSuite) TestAckClosedByInputHost() {
	conn, inputHostClient, messagesCh := createConnection()

	// Setup inputHostClient mock for successful message
	inputHostClient.On("Write", mock.Anything).Return(nil)
	inputHostClient.On("Flush").Return(nil)
	inputHostClient.On("Read").Return(nil, io.EOF)
	inputHostClient.On("Done").Return(nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	message := &cherami.PutMessage{
		ID:   common.StringPtr("1"),
		Data: []byte("test"),
	}
	requestDone := make(chan *PublisherReceipt, 1)

	messagesCh <- putMessageRequest{message, requestDone}
	receipt := <-requestDone

	s.Equal(*message.ID, receipt.ID)
	s.NotNil(receipt.Error)
	s.IsType(&ackChannelClosedError{}, receipt.Error)
	// TODO: mock assert expectations is not working for some reason.
	//inputHostClient.AssertExpectations(s.T())
}

func (s *ConnectionSuite) TestClientClosed() {
	conn, inputHostClient, messagesCh := createConnection()

	// Setup inputHostClient mock for successful message
	inputHostClient.On("Write", mock.Anything).Return(nil)
	inputHostClient.On("Flush").Return(nil)
	inputHostClient.On("Read").Return(wrapAckInCommand(&cherami.PutMessageAck{
		ID:     common.StringPtr("1"),
		Status: common.CheramiStatusPtr(cherami.Status_OK),
	}), nil).After(10 * time.Millisecond).WaitUntil(time.After(100 * time.Millisecond))
	inputHostClient.On("Done").Return(nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	message := &cherami.PutMessage{
		ID:   common.StringPtr("1"),
		Data: []byte("test"),
	}
	requestDone := make(chan *PublisherReceipt, 1)

	messagesCh <- putMessageRequest{message, requestDone}
	<-time.After(10 * time.Millisecond)
	conn.close()
	receipt := <-requestDone

	s.Equal(*message.ID, receipt.ID)
	s.Nil(receipt.Error, "Expected the message to be written successfully.")
	s.True(conn.isClosed())
	// TODO: mock assert expectations is not working for some reason.
	//inputHostClient.AssertExpectations(s.T())
}

func (s *ConnectionSuite) TestOutOfOrderAcks() {
	conn, inputHostClient, messagesCh := createConnection()

	// Setup inputHostClient mock for successful message
	inputHostClient.On("Write", mock.Anything).Return(nil)
	inputHostClient.On("Flush").Return(nil)
	inputHostClient.On("Read").Return(wrapAckInCommand(&cherami.PutMessageAck{
		ID:     common.StringPtr("2"),
		Status: common.CheramiStatusPtr(cherami.Status_OK),
	}), nil).Once()
	inputHostClient.On("Read").Return(wrapAckInCommand(&cherami.PutMessageAck{
		ID:     common.StringPtr("1"),
		Status: common.CheramiStatusPtr(cherami.Status_OK),
	}), nil).Once()
	inputHostClient.On("Done").Return(nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	message1 := &cherami.PutMessage{
		ID:   common.StringPtr("1"),
		Data: []byte("test"),
	}
	requestDone1 := make(chan *PublisherReceipt, 1)
	messagesCh <- putMessageRequest{message1, requestDone1}

	message2 := &cherami.PutMessage{
		ID:   common.StringPtr("2"),
		Data: []byte("test"),
	}
	requestDone2 := make(chan *PublisherReceipt, 1)
	messagesCh <- putMessageRequest{message2, requestDone2}

	receipt2 := <-requestDone2
	s.Equal(*message2.ID, receipt2.ID)

	receipt1 := <-requestDone1
	s.Equal(*message1.ID, receipt1.ID)

	conn.close()
	s.True(conn.isClosed())
	// TODO: mock assert expectations is not working for some reason.
	//inputHostClient.AssertExpectations(s.T())
}

// TODO: Figure out a way to test multiple messages with mocks
/*func (s *ConnectionSuite) TestManySuccess() {
	numberOfMessages := 10
	conn, inputHostClient, messagesCh := createConnection()

	w := make(chan time.Time)
	// Setup inputHostClient mock for successful message
	inputHostClient.On("Write", mock.Anything).Return(nil).Times(numberOfMessages)
	inputHostClient.On("Flush").Return(nil).Times(numberOfMessages)
	for i := 0; i < numberOfMessages; i++ {
		inputHostClient.On("Read").Return(&cherami.PutMessageAck{
			ID:     common.StringPtr(strconv.Itoa(i)),
			Status: common.CheramiStatusPtr(cherami.Status_OK),
		}, nil).WaitUntil(w).Once()
//.After(3 * time.Second).Once()
	}

	conn.Open()
	s.True(conn.opened, "Connection not opened.")

	done := make(chan bool, numberOfMessages)
	for i := 0; i < numberOfMessages; i++ {
		go func(id int) {
			fmt.Printf("Writing message with id: %d\n", id)
			message := &cherami.PutMessage{
				ID: strconv.Itoa(id),
				Data: []byte("test"),
			}

			requestDone := make(chan error, 1)

			messagesCh <- PutMessageRequest{message, requestDone}
			w <- time.Now()

			err := <-requestDone
			fmt.Printf("Got response for message id: %d\n", id)
			s.Nil(err, "Expected the message to be written successfully.")

			fmt.Printf("Go Routine for id '%d' setting done.\n", id)
			done<- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		fmt.Printf("Waiting for message: %d\n", i)
		<-done
		fmt.Printf("Received message: %d\n", i)
	}

	// TODO: mock assert expectations is not working for some reason.
	//inputHostClient.AssertExpectations(s.T())
}*/

func createConnection() (*connection, *mc.MockBInOpenPublisherStreamOutCall, chan putMessageRequest) {
	host := "testHost"
	messagesCh := make(chan putMessageRequest)
	reconfigureCh := make(chan reconfigureInfo, 10)
	inputHostClient := new(mc.MockTChanBInClient)
	wsConnector := new(mc.MockWSConnector)
	stream := new(mc.MockBInOpenPublisherStreamOutCall)

	inputHostClient.On("OpenPublisherStream", mock.Anything).Return(stream, nil)
	wsConnector.On("OpenPublisherStream", mock.Anything, mock.Anything).Return(stream, nil)

	return newConnection(
			inputHostClient,
			wsConnector,
			"/test/inputhostconnection",
			messagesCh,
			reconfigureCh,
			host,
			cherami.Protocol_WS,
			0,
			bark.NewLoggerFromLogrus(log.StandardLogger()),
			metrics.NewTestReporter(nil)),
		stream,
		messagesCh
}

func wrapAckInCommand(ack *cherami.PutMessageAck) *cherami.InputHostCommand {
	cmd := cherami.NewInputHostCommand()
	cmd.Type = common.CheramiInputHostCommandTypePtr(cherami.InputHostCommandType_ACK)
	cmd.Ack = ack

	return cmd
}
