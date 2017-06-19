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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"errors"
	"io"

	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/metrics"
	mc "github.com/uber/cherami-client-go/mocks/clients/cherami"
	"github.com/uber/cherami-thrift/.generated/go/cherami"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/uber-common/bark"
)

const (
	testPrefetchSize = int32(100)
)

type OutputHostConnectionSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestOutputHostConnectionSuite(t *testing.T) {
	suite.Run(t, new(OutputHostConnectionSuite))
}

func (s *OutputHostConnectionSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *OutputHostConnectionSuite) TestOutputHostBasic() {
	conn, _, stream, messagesCh := createOutputHostConnection()

	stream.On("Write", mock.Anything).Return(nil)
	stream.On("Flush").Return(nil)
	stream.On("Read").Return(wrapMessageInCommand(&cherami.ConsumerMessage{
		AckId: common.StringPtr("test"),
	}), nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	delivery := <-messagesCh
	s.NotNil(delivery, "Delivery cannot be nil.")

	msg := delivery.GetMessage()
	s.NotNil(msg, "Message cannot be nil.")
	s.Equal("test", msg.GetAckId())
}

func (s *OutputHostConnectionSuite) TestReadFailed() {
	conn, _, stream, _ := createOutputHostConnection()

	stream.On("Write", mock.Anything).Return(nil)
	stream.On("Flush").Return(nil)
	stream.On("Read").Return(nil, errors.New("some error"))
	stream.On("Done").Return(nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	time.Sleep(10 * time.Millisecond)

	s.True(conn.isClosed(), "Connection not opened.")
}

func (s *OutputHostConnectionSuite) TestReadEOF() {
	conn, _, stream, _ := createOutputHostConnection()

	stream.On("Write", mock.Anything).Return(nil)
	stream.On("Flush").Return(nil)
	stream.On("Read").Return(nil, io.EOF)
	stream.On("Done").Return(nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	time.Sleep(10 * time.Millisecond)

	s.True(conn.isClosed(), "Connection not opened.")
}

func (s *OutputHostConnectionSuite) TestCreditsRenewSuccess() {
	conn, _, stream, messagesCh := createOutputHostConnection()

	initialFlows := cherami.NewControlFlow()
	initialFlows.Credits = common.Int32Ptr(conn.prefetchSize)

	renewFlows := cherami.NewControlFlow()
	renewFlows.Credits = common.Int32Ptr(conn.creditBatchSize)

	stream.On("Write", initialFlows).Return(nil).Once()
	stream.On("Write", renewFlows).Return(nil).Once()
	stream.On("Flush").Return(nil)
	stream.On("Read").Return(wrapMessageInCommand(&cherami.ConsumerMessage{
		AckId: common.StringPtr("test"),
	}), nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	for i := 0; i < int(conn.creditBatchSize); i++ {
		delivery := <-messagesCh
		s.NotNil(delivery, "Delivery cannot be nil.")

		msg := delivery.GetMessage()
		s.NotNil(msg, "Message cannot be nil.")
		s.Equal("test", msg.GetAckId())
	}

	time.Sleep(10 * time.Millisecond)

	stream.AssertExpectations(s.T())
}

func (s *OutputHostConnectionSuite) TestInitialCreditsWriteFailed() {
	conn, _, stream, _ := createOutputHostConnection()

	initialFlows := cherami.NewControlFlow()
	initialFlows.Credits = common.Int32Ptr(conn.prefetchSize)

	stream.On("Write", initialFlows).Return(errors.New("some error")).After(10 * time.Millisecond).Once()
	stream.On("Read").Return(wrapMessageInCommand(&cherami.ConsumerMessage{
		AckId: common.StringPtr("test"),
	}), nil)
	stream.On("Done").Return(nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	time.Sleep(20 * time.Millisecond)
	s.True(conn.isClosed(), "Connection not closed.")

	stream.AssertExpectations(s.T())
}

func (s *OutputHostConnectionSuite) TestInitialCreditsFlushFailed() {
	conn, _, stream, _ := createOutputHostConnection()

	initialFlows := cherami.NewControlFlow()
	initialFlows.Credits = common.Int32Ptr(conn.prefetchSize)

	stream.On("Write", initialFlows).Return(nil).After(10 * time.Millisecond).Once()
	stream.On("Read").Return(wrapMessageInCommand(&cherami.ConsumerMessage{
		AckId: common.StringPtr("test"),
	}), nil)
	stream.On("Flush").Return(errors.New("some error"))
	stream.On("Done").Return(nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	time.Sleep(20 * time.Millisecond)
	s.True(conn.isClosed(), "Connection not closed.")

	stream.AssertExpectations(s.T())
}

func (s *OutputHostConnectionSuite) TestRenewCreditsFailed() {
	var rate, latency, localCreditsLastVal int64
	defer metrics.RegisterHandler(metrics.ConsumeCreditRate, ``, ``, nil)
	defer metrics.RegisterHandler(metrics.ConsumeCreditLatency, ``, ``, nil)
	defer metrics.RegisterHandler(metrics.ConsumeLocalCredits, ``, ``, nil)
	metrics.RegisterHandler(metrics.ConsumeCreditRate, ``, ``, metrics.SummingHandler(&rate, nil))
	metrics.RegisterHandler(metrics.ConsumeCreditLatency, ``, ``, metrics.SummingHandler(&latency, nil))
	metrics.RegisterHandler(metrics.ConsumeLocalCredits, ``, ``, metrics.KeepLastValueHandler(&localCreditsLastVal, nil))

	conn, _, stream, messagesCh := createOutputHostConnection()

	initialFlows := cherami.NewControlFlow()
	initialFlows.Credits = common.Int32Ptr(conn.prefetchSize)

	renewFlows := cherami.NewControlFlow()
	renewFlows.Credits = common.Int32Ptr(conn.creditBatchSize)

	stream.On("Write", initialFlows).Return(nil).Once()
	stream.On("Write", renewFlows).Return(errors.New("some error")).Once()
	stream.On("Flush").Return(nil)
	stream.On("Read").Return(wrapMessageInCommand(&cherami.ConsumerMessage{
		AckId: common.StringPtr("test"),
	}), nil)
	stream.On("Done").Return(nil)

	conn.open()
	s.True(conn.isOpened(), "Connection not opened.")

	halfBatch := int(conn.creditBatchSize) / 2
	for i := 0; i < int(conn.creditBatchSize); i++ {
		delivery := <-messagesCh
		s.NotNil(delivery, "Delivery cannot be nil.")

		msg := delivery.GetMessage()
		s.NotNil(msg, "Message cannot be nil.")
		s.Equal("test", msg.GetAckId())

		if i == halfBatch { // Halfway through, wait to see the local credits stabilize to the right value
			for {
				localCredits := atomic.LoadInt64(&localCreditsLastVal)
				if localCredits == int64(halfBatch+1) { // +1 because we started counting i from 0
					break
				}
				time.Sleep(time.Second)
			}
		}
	}
	s.True(atomic.LoadInt64(&localCreditsLastVal) < int64(conn.creditBatchSize))

	time.Sleep(10 * time.Millisecond)
	s.True(conn.isClosed(), "Connection not closed.")

	stream.AssertExpectations(s.T())

	s.InDelta(int64(time.Second+time.Microsecond), atomic.LoadInt64(&latency), float64(time.Second))
	s.True(atomic.LoadInt64(&rate) > 2, fmt.Sprintf("rate was %v", atomic.LoadInt64(&rate)))
	s.True(atomic.LoadInt64(&rate) <= int64(conn.creditBatchSize)+int64(testPrefetchSize), fmt.Sprintf("rate was %v", atomic.LoadInt64(&rate)))
}

func createOutputHostConnection() (*outputHostConnection, *mc.MockTChanBOutClient, *mc.MockBOutOpenConsumerStreamOutCall, chan Delivery) {
	host := "testHost"
	ackClient := new(mc.MockTChanBOutClient)
	wsConnector := new(mc.MockWSConnector)
	stream := new(mc.MockBOutOpenConsumerStreamOutCall)
	deliveryCh := make(chan Delivery)
	reconfigureCh := make(chan reconfigureInfo, 10)
	options := &ClientOptions{Timeout: time.Minute}

	wsConnector.On("OpenConsumerStream", mock.Anything, mock.Anything).Return(stream, nil)
	conn := newOutputHostConnection(
		ackClient,
		wsConnector,
		"/test/outputhostconnection",
		"/consumer",
		options,
		deliveryCh,
		reconfigureCh,
		host,
		cherami.Protocol_WS,
		testPrefetchSize,
		bark.NewLoggerFromLogrus(log.StandardLogger()),
		metrics.NewTestReporter(nil))

	return conn, ackClient, stream, deliveryCh
}

func wrapMessageInCommand(msg *cherami.ConsumerMessage) *cherami.OutputHostCommand {
	cmd := cherami.NewOutputHostCommand()
	cmd.Type = common.CheramiOutputHostCommandTypePtr(cherami.OutputHostCommandType_MESSAGE)
	cmd.Message = msg

	return cmd
}
