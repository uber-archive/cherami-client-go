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
	"errors"
	"hash/crc32"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/backoff"
	"github.com/uber/cherami-client-go/common/metrics"
)

type (
	// basePublisher contains the data/code
	// common to all types of Publisher
	// implementations
	basePublisher struct {
		idCounter      			 int64
		path           			 string
		client         			 Client
		logger         			 bark.Logger
		reporter       			 metrics.Reporter
		retryPolicy    			 backoff.RetryPolicy
		checksumOption 			 cherami.ChecksumOption
		reconfigurationPollingInterval   time.Duration
	}

	// publishError represents a message publishing error
	publishError struct {
		msg string
	}
)

// ErrMessageTimedout is returned by Publish when no ack is received within timeout interval
var ErrMessageTimedout = errors.New("Timed out.")

// Publisher default retry policy
const (
	publisherRetryInterval           = 50 * time.Millisecond
	publisherRetryMaxInterval        = 10 * time.Second
	publisherRetryExpirationInterval = 2 * time.Minute
)

// choosePublishEndpoints selects the list of publish endpoints
// from the set of all possible (protocol -> endpoints) returned
// from the server
func (bp *basePublisher) choosePublishEndpoints(publisherOptions *cherami.ReadPublisherOptionsResult_) (cherami.Protocol, []*cherami.HostAddress) {
	// pick best protocol from what server suggested
	hostProtocols := publisherOptions.GetHostProtocols()
	chosenIdx, err := bp.chooseProcotol(hostProtocols)
	chosenProtocol := cherami.Protocol_WS
	chosenHostAddresses := publisherOptions.GetHostAddresses()
	if err == nil {
		chosenProtocol = hostProtocols[chosenIdx].GetProtocol()
		chosenHostAddresses = hostProtocols[chosenIdx].GetHostAddresses()
	}
	return chosenProtocol, chosenHostAddresses
}

// chooseProtocol selects a preferred protocol from the list of
// available protocols returned from the server
func (bp *basePublisher) chooseProcotol(hostProtocols []*cherami.HostProtocol) (int, error) {
	clientSupportedProtocol := map[cherami.Protocol]bool{cherami.Protocol_WS: true}
	clientSupportButDeprecated := -1
	serverSupportedProtocol := make([]cherami.Protocol, 0, len(hostProtocols))

	for idx, hostProtocol := range hostProtocols {
		serverSupportedProtocol = append(serverSupportedProtocol, hostProtocol.GetProtocol())
		if _, found := clientSupportedProtocol[hostProtocol.GetProtocol()]; found {
			if !hostProtocol.GetDeprecated() {
				// found first supported and non-deprecated one, done
				return idx, nil
			} else if clientSupportButDeprecated == -1 {
				// found first supported but deprecated one, keep looking
				clientSupportButDeprecated = idx
			}
		}
	}

	if clientSupportButDeprecated == -1 {
		bp.logger.WithField(`protocols`, serverSupportedProtocol).Error("No protocol is supported by client")
		return clientSupportButDeprecated, &cherami.BadRequestError{Message: `No protocol is supported by client`}
	}

	bp.logger.WithField(`protocol`, hostProtocols[clientSupportButDeprecated].GetProtocol()).Warn("Client using deprecated protocol")
	return clientSupportButDeprecated, nil
}

func (bp *basePublisher) readPublisherOptions() (*cherami.ReadPublisherOptionsResult_, error) {
	return bp.client.ReadPublisherOptions(bp.path)
}

func (bp *basePublisher) addChecksum(msg *cherami.PutMessage) {
	switch bp.checksumOption {
	case cherami.ChecksumOption_CRC32IEEE:
		msg.Crc32IEEEDataChecksum = common.Int64Ptr(int64(crc32.ChecksumIEEE(msg.GetData())))
	case cherami.ChecksumOption_MD5:
		md5Checksum := md5.Sum(msg.GetData())
		msg.Md5DataChecksum = md5Checksum[:]
	}
}

// toPutMessage converts a PublisherMessage to cherami.PutMessage
func (bp *basePublisher) toPutMessage(pubMessage *PublisherMessage) *cherami.PutMessage {

	msgID := atomic.AddInt64(&bp.idCounter, 1)
	idStr := strconv.FormatInt(msgID, 10)
	delay := int32(pubMessage.Delay.Seconds())

	msg := &cherami.PutMessage{
		ID:   common.StringPtr(idStr),
		Data: pubMessage.Data,
		DelayMessageInSeconds: &delay,
		UserContext:           pubMessage.UserContext,
	}

	bp.addChecksum(msg)
	return msg
}

func createDefaultPublisherRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(publisherRetryInterval)
	policy.SetMaximumInterval(publisherRetryMaxInterval)
	policy.SetExpirationInterval(publisherRetryExpirationInterval)
	return policy
}

func newPublishError(status cherami.Status) error {
	return &publishError{
		msg: "Publish failed with error:" + status.String(),
	}
}

func (e *publishError) Error() string {
	return e.msg
}
