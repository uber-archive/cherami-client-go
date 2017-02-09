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
	"math/rand"
	"sync"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"

	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/metrics"
	mc "github.com/uber/cherami-client-go/mocks/clients/cherami"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
)

type (
	TChanBatchPublisherSuite struct {
		*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
		suite.Suite
		client    *mockClient
		logger    bark.Logger
		publisher *tchannelBatchPublisher
	}
)

func TestTChanBatchPublisherSuite(t *testing.T) {
	suite.Run(t, new(TChanBatchPublisherSuite))
}

func (s *TChanBatchPublisherSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	s.logger = bark.NewLoggerFromLogrus(log.StandardLogger())
	s.client = new(mockClient)
	s.publisher = newTChannelBatchPublisher(s.client, nil, "/test/tchanBatchPublisher", s.logger, metrics.NewNullReporter(), time.Second).(*tchannelBatchPublisher)
	s.publisher.opened = 1
}

func (s *TChanBatchPublisherSuite) TestBatchSzWithinRange() {
	s.True(maxBatchSize > 0, "invalid maxBatchSz")
	s.True(maxBatchSize <= 16, "invalid maxBatchSz, cannot be greater than 16")
}

func (s *TChanBatchPublisherSuite) TestPublishBatchSuccess() {
	for _, sz := range []int{1, 16, 16, 5} {
		ackIDs := make([]string, common.MinInt(16, sz))
		for i := range ackIDs {
			ackIDs[i] = s.publisher.msgIDToHexStr(i)
		}
		result := newBatchResult(ackIDs, []string{})
		mockInput := new(mc.MockTChanBInClient)
		mockInput.On("PutMessageBatch", mock.Anything, mock.Anything).Return(result, nil).Times((sz + 15) / 16)

		endpoints := newEndpoints()
		key := "127.0.0.1:4240"
		endpoints.addrs = append(endpoints.addrs, key)
		endpoints.addrToThriftClient[key] = mockInput
		s.publisher.endpoints.Store(endpoints)

		msg := newPublisherMessage()
		ids := make([]string, sz)
		ackChs := make([]chan *PublisherReceipt, sz)
		for i := range ackIDs {
			ackChs[i] = make(chan *PublisherReceipt, 1)
			id, err := s.publisher.PublishAsync(msg, ackChs[i])
			s.Nil(err, "publishAsync returned unexpected error")
			ids[i] = id
		}

		s.publisher.closeCh = make(chan struct{})

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			s.publisher.processor()
			wg.Done()
		}()

		for i, ch := range ackChs {
			receipt := <-ch
			s.Nil(receipt.Error, "publishBatch receipt contains unexpected error")
			s.Equal(ids[i], receipt.ID, "publishBatch receipt contains invalid message id")
		}

		close(s.publisher.closeCh)
		wg.Wait()
	}
}

func (s *TChanBatchPublisherSuite) TestPublishBatchFailure() {
	for _, sz := range []int{1, 16, 16, 5} {
		ackIDs := make([]string, common.MinInt(16, sz))
		for i := range ackIDs {
			ackIDs[i] = s.publisher.msgIDToHexStr(i)
		}
		result := newBatchResult([]string{}, ackIDs)
		mockInput := new(mc.MockTChanBInClient)
		mockInput.On("PutMessageBatch", mock.Anything, mock.Anything).Return(result, nil).Times((sz + 15) / 16)

		endpoints := newEndpoints()
		key := "127.0.0.1:4240"
		endpoints.addrs = append(endpoints.addrs, key)
		endpoints.addrToThriftClient[key] = mockInput
		s.publisher.endpoints.Store(endpoints)

		msg := newPublisherMessage()
		ids := make([]string, sz)
		ackChs := make([]chan *PublisherReceipt, sz)
		for i := range ackIDs {
			ackChs[i] = make(chan *PublisherReceipt, 1)
			id, err := s.publisher.PublishAsync(msg, ackChs[i])
			s.Nil(err, "publishAsync returned unexpected error")
			ids[i] = id
		}

		s.publisher.closeCh = make(chan struct{})

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			s.publisher.processor()
			wg.Done()
		}()

		for i, ch := range ackChs {
			receipt := <-ch
			s.NotNil(receipt.Error, "publishBatch receipt contains unexpected error")
			s.Equal(ids[i], receipt.ID, "publishBatch receipt contains invalid message id")
		}

		close(s.publisher.closeCh)
		wg.Wait()
	}
}

func (s *TChanBatchPublisherSuite) TestPublishBatchPartialFailure() {
	sz := 16
	succIDs := make([]string, 0, common.MinInt(8, sz))
	failIDs := make([]string, 0, common.MinInt(8, sz))
	for i := 0; i < sz; i++ {
		if i%2 == 0 {
			succIDs = append(succIDs, s.publisher.msgIDToHexStr(i))
			continue
		}
		failIDs = append(failIDs, s.publisher.msgIDToHexStr(i))
	}

	result := newBatchResult(succIDs, failIDs)
	mockInput := new(mc.MockTChanBInClient)
	mockInput.On("PutMessageBatch", mock.Anything, mock.Anything).Return(result, nil).Times((sz + 15) / 16)

	endpoints := newEndpoints()
	key := "127.0.0.1:4240"
	endpoints.addrs = append(endpoints.addrs, key)
	endpoints.addrToThriftClient[key] = mockInput
	s.publisher.endpoints.Store(endpoints)

	msg := newPublisherMessage()
	ids := make([]string, sz)
	ackChs := make([]chan *PublisherReceipt, sz)
	for i := 0; i < sz; i++ {
		ackChs[i] = make(chan *PublisherReceipt, 1)
		id, err := s.publisher.PublishAsync(msg, ackChs[i])
		s.Nil(err, "publishAsync returned unexpected error")
		ids[i] = id
	}

	s.publisher.closeCh = make(chan struct{})

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		s.publisher.processor()
		wg.Done()
	}()

	for i, ch := range ackChs {
		receipt := <-ch
		s.Equal(ids[i], receipt.ID, "publishBatch receipt contains invalid message id")
		if i%2 == 0 {
			s.Nil(receipt.Error, "publishBatch receipt contains unexpected error")
		} else {
			s.NotNil(receipt.Error, "publishBatch receipt contains unexpected error")
		}
	}

	close(s.publisher.closeCh)
	wg.Wait()
}

func newBatchResult(succIDs []string, failIDs []string) *cherami.PutMessageBatchResult_ {
	result := cherami.NewPutMessageBatchResult_()
	for _, id := range shuffle(succIDs) {
		ack := cherami.NewPutMessageAck()
		ack.Status = cherami.StatusPtr(cherami.Status_OK)
		ack.ID = common.StringPtr(id)
		ack.Receipt = common.StringPtr(id)
		result.SuccessMessages = append(result.SuccessMessages, ack)
	}
	for _, id := range shuffle(failIDs) {
		ack := cherami.NewPutMessageAck()
		ack.Status = cherami.StatusPtr(cherami.Status_FAILED)
		ack.Message = common.StringPtr("Internal service error")
		ack.ID = common.StringPtr(id)
		ack.Receipt = common.StringPtr(id)
		result.FailedMessages = append(result.FailedMessages, ack)
	}
	return result
}

func shuffle(input []string) []string {
	perms := rand.Perm(len(input))
	output := make([]string, len(input))
	for i, srcIdx := range perms {
		output[i] = input[srcIdx]
	}
	return output
}

func newPublisherMessage() *PublisherMessage {
	return &PublisherMessage{
		Data:        []byte("test"),
		UserContext: map[string]string{"test-name": "tchanelBatchPublisher"},
	}
}
