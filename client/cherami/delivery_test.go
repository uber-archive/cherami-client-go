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
	"crypto/md5"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-thrift/.generated/go/cherami"

	"github.com/stretchr/testify/require"
)

type DeliverySuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestDeliverySuite(t *testing.T) {
	suite.Run(t, new(DeliverySuite))
}

func (s *DeliverySuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

func (s *DeliverySuite) TestVerifyChecksumCrc32IEEE() {
	delivery := newTestDelivery()
	// 3957769958
	delivery.GetMessage().GetPayload().Crc32IEEEDataChecksum = common.Int64Ptr(int64(crc32.ChecksumIEEE(delivery.GetMessage().GetPayload().GetData())))
	s.True(delivery.VerifyChecksum(), "Crc32IEEE checksum verification failed")
}

func (s *DeliverySuite) TestVerifyChecksumCrc32IEEEFail() {
	delivery := newTestDelivery()
	delivery.GetMessage().GetPayload().Crc32IEEEDataChecksum = common.Int64Ptr(int64(123))
	s.False(delivery.VerifyChecksum(), "Crc32IEEE checksum verification failed")
}

func (s *DeliverySuite) TestVerifyChecksumMd5() {
	delivery := newTestDelivery()
	// 6CD3556DEB0DA54BCA060B4C39479839
	md5Checksum := md5.Sum(delivery.GetMessage().GetPayload().GetData())
	delivery.GetMessage().GetPayload().Md5DataChecksum = md5Checksum[:]
	s.True(delivery.VerifyChecksum(), "Md5 checksum verification failed")
}

func (s *DeliverySuite) TestVerifyChecksumMd5Fail() {
	delivery := newTestDelivery()
	delivery.GetMessage().GetPayload().Md5DataChecksum = []byte("0123456789ABCDEF")
	s.False(delivery.VerifyChecksum(), "Md5 checksum verification failed")
}

func (s *DeliverySuite) TestVerifyChecksumNone() {
	delivery := newTestDelivery()
	s.True(delivery.VerifyChecksum(), "None checksum verification should just pass")
}

func (s *DeliverySuite) TestVerifyChecksumMultiple() {
	delivery := newTestDelivery()
	// checksum verification is done with ordering
	// since crc32 IEEE checksum verification is done firstly, other invalid checksum wont matter
	delivery.GetMessage().GetPayload().Crc32IEEEDataChecksum = common.Int64Ptr(int64(crc32.ChecksumIEEE(delivery.GetMessage().GetPayload().GetData())))
	delivery.GetMessage().GetPayload().Md5DataChecksum = []byte("123")
	s.True(delivery.VerifyChecksum(), "Crc32IEEE checksum verification shoud result in pass")
}

func newTestDelivery() Delivery {
	putMessage := cherami.NewPutMessage()
	putMessage.Data = []byte("Hello, world!")
	consumerMessage := cherami.NewConsumerMessage()
	consumerMessage.Payload = putMessage
	return newDelivery(consumerMessage, nil)
}
