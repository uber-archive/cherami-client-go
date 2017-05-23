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
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"

	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/backoff"
	"github.com/uber/cherami-client-go/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
)

type (
	// endpoints represents the current
	// set of host:port addresses that
	// should be used for publishing
	endpoints struct {
		addrs              []string                    // list of ip:ports
		addrToThriftClient map[string]cherami.TChanBIn // ip:port to thriftClient
	}

	tchannelBatchPublisher struct {
		basePublisher
		sync.RWMutex
		opened         int32
		closed         int32
		tchan          *tchannel.Channel
		endpoints      atomic.Value // atomically refreshed by reconfigHandler
		reconfigureCh  chan reconfigureInfo
		reconfigurable *reconfigurable
		messagesCh     chan *putMessageRequest
		closeCh        chan struct{}
	}
)

var errInvalidAckID = errors.New("invalid msg id found in ack")
var errPublisherClosed = errors.New("publisher is closed")
var errPublisherUnopened = errors.New("publish is not open")
var errNoPublishEndpoint = errors.New("no publish endpoints")

const (
	maxBatchSize              = 16 // max number of messages in a single batch to server
	endpointsInitialSz        = 8  // initial size of the endpoints map
	messageBatchThriftTimeout = 30 * time.Second
	enqueueTimeout            = time.Minute
	inputServiceTChannelPort  = "4240"
)

var _ Publisher = (*tchannelBatchPublisher)(nil)

func newTChannelBatchPublisher(client Client, tchan *tchannel.Channel, path string, logger bark.Logger, metricsReporter metrics.Reporter, reconfigurationPollingInterval time.Duration) Publisher {
	base := basePublisher{
		client:                         client,
		retryPolicy:                    createDefaultPublisherRetryPolicy(),
		path:                           path,
		logger:                         logger.WithField(common.TagDstPth, common.FmtDstPth(path)),
		reporter:                       metricsReporter,
		reconfigurationPollingInterval: reconfigurationPollingInterval,
	}
	return &tchannelBatchPublisher{
		basePublisher: base,
		tchan:         tchan,
		reconfigureCh: make(chan reconfigureInfo, 1),
		messagesCh:    make(chan *putMessageRequest, maxBatchSize),
		closeCh:       make(chan struct{}),
	}
}

// Open prepares the publisher for message publishing
func (p *tchannelBatchPublisher) Open() error {

	p.Lock()
	defer p.Unlock()

	if atomic.LoadInt32(&p.opened) == 1 {
		return nil
	}

	publisherOptions, err := p.readPublisherOptions()
	if err != nil {
		p.logger.Errorf("Error resolving input hosts: %v", err)
		return err
	}

	p.checksumOption = publisherOptions.GetChecksumOption()

	endpoints := newEndpoints()

	_, hostAddrs := p.choosePublishEndpoints(publisherOptions)
	for _, addr := range hostAddrs {
		key := net.JoinHostPort(addr.GetHost(), inputServiceTChannelPort)
		if _, ok := endpoints.addrToThriftClient[key]; !ok {
			endpoints.addrs = append(endpoints.addrs, key)
			endpoints.addrToThriftClient[key] = p.newThriftClient(key)
		}
	}

	p.endpoints.Store(endpoints)
	p.reporter.UpdateGauge(metrics.PublishNumConnections, nil, int64(len(hostAddrs)))

	p.reconfigurable = newReconfigurable(p.reconfigureCh, p.closeCh, p.reconfigureHandler, p.logger, p.reconfigurationPollingInterval)
	go p.reconfigurable.reconfigurePump()
	go p.processor()
	atomic.StoreInt32(&p.opened, 1)
	p.logger.WithField(`endpoints`, endpoints.addrs).Info("Publisher Opened.")

	return nil
}

// Close closes the publisher, no more messages
// can be published and in-flight messages will
// be failed.
func (p *tchannelBatchPublisher) Close() {
	p.Lock()
	defer p.Unlock()

	if p.isClosed() {
		return
	}

	close(p.closeCh)

	p.drain()
	p.reporter.UpdateGauge(metrics.PublishNumConnections, nil, int64(0))
	atomic.StoreInt32(&p.closed, 1)
	p.logger.Info("Publisher Closed.")
}

func (p *tchannelBatchPublisher) Pause() {
	return
}

func (p *tchannelBatchPublisher) Resume() {
	return
}

// Publish publishes a message to cherami
func (p *tchannelBatchPublisher) Publish(message *PublisherMessage) *PublisherReceipt {

	if !p.isOpened() {
		return &PublisherReceipt{Error: errPublisherUnopened}
	}
	if p.isClosed() {
		return &PublisherReceipt{Error: errPublisherClosed}
	}

	var receipt *PublisherReceipt

	publishOp := func() error {
		ackCh := make(chan *PublisherReceipt, 1)
		_, err := p.PublishAsync(message, ackCh)
		if err != nil {
			return err
		}
		receipt = <-ackCh
		if receipt.Error != nil {
			return receipt.Error
		}
		return nil
	}

	err := backoff.Retry(publishOp, p.retryPolicy, nil)
	if err != nil {
		return &PublisherReceipt{Error: err}
	}

	return receipt
}

// PublishAsync publishes a message asynchronously.
// On completion, the receipt will be enqueued into
// the done channel.
func (p *tchannelBatchPublisher) PublishAsync(message *PublisherMessage, done chan<- *PublisherReceipt) (string, error) {

	if !p.isOpened() {
		return "", errPublisherUnopened
	}
	if p.isClosed() {
		return "", errPublisherClosed
	}

	putMsg := &putMessageRequest{
		message:    p.toPutMessage(message),
		messageAck: done,
	}

	msgID := putMsg.message.GetID()
	timer := time.NewTimer(enqueueTimeout)
	defer timer.Stop()

	select {
	case p.messagesCh <- putMsg:
	case <-timer.C:
		return "", ErrMessageTimedout
	}

	return msgID, nil
}

// publishBatch publishes the given batch of messages
// to cherami. On success, returns a slice of receipts
// where the order of the receipts is the same as
// the order of the given messages. If an error is
// encountered before publishing the whole batch, the
// returned receipts will be for a subset of messages.
//
// This func will return err != nil if and only if no
// messages can be published.
func (p *tchannelBatchPublisher) publishBatch(putMessages []*cherami.PutMessage) ([]*PublisherReceipt, error) {

	if p.isClosed() {
		return nil, errPublisherClosed
	}

	batchRequest := &cherami.PutMessageBatchRequest{
		DestinationPath: common.StringPtr(p.path),
		Messages:        putMessages,
	}

	p.reporter.IncCounter(metrics.PublishMessageRate, nil, 1)
	sw := p.reporter.StartTimer(metrics.PublishMessageLatency, nil)

	result, err := p.putMessageBatch(batchRequest)
	sw.Stop()
	if err != nil {
		p.reporter.IncCounter(metrics.PublishMessageFailedRate, nil, 1)
		return nil, err
	}

	receipts := make([]*PublisherReceipt, len(putMessages))

	if e := p.processAcks(result.GetSuccessMessages(), receipts); e != nil {
		return nil, e
	}
	if e := p.processAcks(result.GetFailedMessages(), receipts); e != nil {
		return nil, e
	}
	return receipts, nil
}

func (p *tchannelBatchPublisher) putMessageBatch(request *cherami.PutMessageBatchRequest) (*cherami.PutMessageBatchResult_, error) {
	ctx, cancel := thrift.NewContext(messageBatchThriftTimeout)
	defer cancel()
	endpoints := p.endpoints.Load().(*endpoints)
	if len(endpoints.addrs) == 0 {
		return nil, errNoPublishEndpoint
	}
	addr := endpoints.addrs[rand.Intn(len(endpoints.addrs))]
	thriftClient, _ := endpoints.addrToThriftClient[addr]
	return thriftClient.PutMessageBatch(ctx, request)
}

// processAcks takes a set of acks received in response
// to putMessageBatch, converts them into receipts and
// stores them into the receipts slice. Stores receipt
// for ack with ID:id into receipts[id].
func (p *tchannelBatchPublisher) processAcks(acks []*cherami.PutMessageAck, receipts []*PublisherReceipt) error {
	for _, ack := range acks {
		id, err := p.hexStrToMsgID(ack.GetID())
		if err != nil {
			return err
		}
		if id < 0 || id >= len(receipts) {
			p.logger.WithField(`id`, ack.GetID()).Error("putMessageBatch ack result contains invalid message id")
			return errInvalidAckID
		}
		receipts[id] = &PublisherReceipt{
			ID:          ack.GetID(),
			Receipt:     ack.GetReceipt(),
			UserContext: ack.GetUserContext(),
		}
		if ack.GetStatus() != cherami.Status_OK {
			receipts[id].Error = newPublishError(ack.GetStatus())
		}
	}
	return nil
}

// processor is the main loop that dequeues
// messages and publishes them in batch
func (p *tchannelBatchPublisher) processor() {

	msgIDs := make([]string, maxBatchSize) // original message ids
	putMessages := make([]*cherami.PutMessage, maxBatchSize)
	ackChannels := make([]chan<- *PublisherReceipt, maxBatchSize)

	for {

		batchSz := 0

		select {
		case <-p.closeCh:
			return
		case m := <-p.messagesCh:

		msgLoop:
			for m != nil {

				msgIDs[batchSz] = m.message.GetID()
				putMessages[batchSz] = m.message
				putMessages[batchSz].ID = common.StringPtr(p.msgIDToHexStr(batchSz))
				ackChannels[batchSz] = m.messageAck
				batchSz++

				if batchSz == maxBatchSize {
					break msgLoop
				}

				select {
				case m = <-p.messagesCh:
				default:
					m = nil
				}
			}

			receipts, err := p.publishBatch(putMessages[:batchSz])

			for i := 0; i < batchSz; i++ {
				if err != nil {
					ackChannels[i] <- &PublisherReceipt{Error: err}
				} else {
					receipts[i].ID = msgIDs[i]
					ackChannels[i] <- receipts[i]
				}
				putMessages[i] = nil
				ackChannels[i] = nil
				msgIDs[i] = ""
			}
		}
	}
}

// reconfigureHandler re-disovers the publish endpoints
// and updates the tchannel peers list.
func (p *tchannelBatchPublisher) reconfigureHandler() {

	publisherOptions, err := p.readPublisherOptions()
	if err != nil {
		return
	}

	if err != nil {
		p.logger.Infof("Error resolving input hosts: %v", err)
		if _, ok := err.(*cherami.EntityNotExistsError); ok {
			// Destination is deleted. Continue with reconfigure
			// remove all addrs from the peers list
			publisherOptions = &cherami.ReadPublisherOptionsResult_{}
		} else {
			// This is a potentially a transient error.
			// Retry on next reconfigure
			return
		}
	}

	currEndpoints := newEndpoints()
	oldEndpoints := p.endpoints.Load().(*endpoints)

	_, addrs := p.choosePublishEndpoints(publisherOptions)
	for _, addr := range addrs {

		key := net.JoinHostPort(addr.GetHost(), inputServiceTChannelPort)
		_, ok := oldEndpoints.addrToThriftClient[key]
		if ok {
			currEndpoints.addrs = append(currEndpoints.addrs, key)
			currEndpoints.addrToThriftClient[key] = oldEndpoints.addrToThriftClient[key]
			continue
		}

		currEndpoints.addrs = append(currEndpoints.addrs, key)
		currEndpoints.addrToThriftClient[key] = p.newThriftClient(key)
	}

	p.endpoints.Store(currEndpoints)
	p.reporter.UpdateGauge(metrics.PublishNumConnections, nil, int64(len(currEndpoints.addrs)))
}

func (p *tchannelBatchPublisher) isClosed() bool {
	return atomic.LoadInt32(&p.closed) == 1
}

func (p *tchannelBatchPublisher) isOpened() bool {
	return atomic.LoadInt32(&p.opened) == 1
}

func (p *tchannelBatchPublisher) drain() {
	for {
		select {
		case m := <-p.messagesCh:
			m.messageAck <- &PublisherReceipt{Error: errPublisherClosed}
		default:
			return
		}
	}
}

func (p *tchannelBatchPublisher) newThriftClient(addr string) cherami.TChanBIn {
	options := &thrift.ClientOptions{HostPort: addr}
	return cherami.NewTChanBInClient(thrift.NewClient(p.tchan, common.InputServiceName, options))
}

func newEndpoints() *endpoints {
	return &endpoints{
		addrs:              make([]string, 0, endpointsInitialSz),
		addrToThriftClient: make(map[string]cherami.TChanBIn, endpointsInitialSz),
	}
}

func (p *tchannelBatchPublisher) msgIDToHexStr(id int) string {
	switch {
	case id >= 0 && id <= 9:
		return string(byte('0') + byte(id))
	case id > 9 && id < 16:
		return string(byte('A') + byte(id-10))
	default:
		p.logger.WithField(`id`, id).Fatal("msgIDToHexStr() encountered invalid msgID")
	}
	return ""
}

func (p *tchannelBatchPublisher) hexStrToMsgID(id string) (int, error) {
	val := byte(id[0])
	switch {
	case val >= '0' && val <= '9':
		return int(val - byte('0')), nil
	case val >= 'A' && val <= 'F':
		return 10 + int(val-byte('A')), nil
	default:
		return 0, errInvalidAckID
	}
}
