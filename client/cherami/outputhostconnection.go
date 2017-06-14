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
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/metrics"
	"github.com/uber/cherami-client-go/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"

	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go/thrift"
	"golang.org/x/net/context"
)

type (
	outputHostConnection struct {
		ackClient         cherami.TChanBOut
		wsConnector       WSConnector
		path              string
		consumerGroupName string
		options           *ClientOptions
		deliveryCh        chan<- Delivery
		prefetchSize      int32
		creditBatchSize   int32
		outputHostStream  stream.BOutOpenConsumerStreamOutCall
		cancel            context.CancelFunc
		closeChannel      chan struct{}
		connKey           string
		protocol          cherami.Protocol
		reconfigureCh     chan<- reconfigureInfo
		creditsCh         chan int32
		logger            bark.Logger
		reporter          metrics.Reporter

		lk     sync.Mutex
		opened int32
		closed int32

		// We need this lock to protect writes to acksBatchCh after it getting closed
		acksBatchLk     sync.RWMutex
		acksBatchCh     chan []string
		acksBatchClosed chan struct{}
	}
)

const (
	creditsChBuffer = 10
	ackBatchSize    = 9 // ackIds are about 133 bytes each and MTU is about 1500, so try to keep it to one packet full
	ackBatchDelay   = time.Second / 10
)

func newOutputHostConnection(ackClient cherami.TChanBOut, wsConnector WSConnector,
	path, consumerGroupName string, options *ClientOptions, deliveryCh chan<- Delivery,
	reconfigureCh chan<- reconfigureInfo, connKey string, protocol cherami.Protocol,
	prefetchSize int32, logger bark.Logger, reporter metrics.Reporter) *outputHostConnection {

	creditBatchSize := prefetchSize / 10
	if creditBatchSize < 1 {
		creditBatchSize = 1
	}

	// Don't check for nil options; better to panic here than panic later
	common.ValidateTimeout(options.Timeout)

	return &outputHostConnection{
		connKey:           connKey,
		protocol:          protocol,
		ackClient:         ackClient,
		wsConnector:       wsConnector,
		path:              path,
		consumerGroupName: consumerGroupName,
		options:           options,
		deliveryCh:        deliveryCh,
		prefetchSize:      prefetchSize,
		creditBatchSize:   creditBatchSize,
		closeChannel:      make(chan struct{}),
		reconfigureCh:     reconfigureCh,
		creditsCh:         make(chan int32, creditsChBuffer),
		acksBatchCh:       make(chan []string, ackBatchSize*2),
		acksBatchClosed:   make(chan struct{}),
		logger:            logger,
		reporter:          reporter,
	}
}

func (conn *outputHostConnection) open() error {
	conn.lk.Lock()
	defer conn.lk.Unlock()

	if atomic.LoadInt32(&conn.opened) == 0 {
		switch conn.protocol {
		case cherami.Protocol_WS:
			conn.logger.Infof("Using websocket to connect to output host %s", conn.connKey)
			wsStream, err := conn.wsConnector.OpenConsumerStream(conn.connKey, http.Header{
				"path":              {conn.path},
				"consumerGroupName": {conn.consumerGroupName},
			})
			if err != nil {
				conn.logger.Infof("Error opening websocket connection to output host %s: %v", conn.connKey, err)
				return err
			}

			conn.outputHostStream = wsStream
			conn.cancel = nil

		default:
			return &cherami.BadRequestError{Message: `Protocol not supported`}
		}

		// Now start the message pump
		go conn.readMessagesPump()
		go conn.writeCreditsPump()
		// We only bail out of this pump when acksBatchCh is closed by consumerImpl after this connections is removed.
		// This is the only guarantee we will not receive more acks on the channel and it is safe to shutdown the pump.
		// Closing the pump earlier has the potential to cause deadlock between consumer writing acks and connection writing
		// messages to deliveryCh.
		go conn.writeAcksPump()

		atomic.StoreInt32(&conn.opened, 1)
		conn.logger.Info("Output host connection opened.")
	}

	return nil
}

func (conn *outputHostConnection) close() {
	conn.lk.Lock()
	defer conn.lk.Unlock()

	if atomic.LoadInt32(&conn.closed) == 0 {
		select {
		case conn.reconfigureCh <- reconfigureInfo{eventType: connClosedReconfigureType, reconfigureID: conn.connKey}:
		default:
			conn.logger.Info("Reconfigure channel is full.  Drop reconfigure command due to connection close.")
		}

		close(conn.closeChannel)
		conn.closeAcksBatchCh() // necessary to shutdown writeAcksPump within the connection
		atomic.StoreInt32(&conn.closed, 1)
		conn.logger.Info("Output host connection closed.")
	}
}

func (conn *outputHostConnection) isOpened() bool {
	return atomic.LoadInt32(&conn.opened) != 0
}

func (conn *outputHostConnection) isClosed() bool {
	return atomic.LoadInt32(&conn.closed) != 0
}

// drainReadPipe reads and discards all messages on
// the outputHostStream until it encounters
// a read stream error
func (conn *outputHostConnection) drainReadPipe() {
	for {
		if _, err := conn.outputHostStream.Read(); err != nil {
			return
		}
	}
}

func (conn *outputHostConnection) readMessagesPump() {

	defer func() {
		conn.logger.Info("readMessagesPump done")
	}()

	var localCredits int32
	for {
		if localCredits >= conn.creditBatchSize {
			// Issue more credits
			select {
			case conn.creditsCh <- localCredits:
				localCredits = 0
			default:
				conn.logger.Debugf("Credits channel is full. Unable to write to creditsCh.")
			}
		}
		conn.reporter.UpdateGauge(metrics.ConsumeLocalCredits, nil, int64(localCredits))

		cmd, err := conn.outputHostStream.Read()
		if err != nil {
			conn.reporter.IncCounter(metrics.ConsumeReadFailed, nil, 1)
			// Error reading from stream.  Time to close and bail out.
			conn.logger.Infof("Error reading OutputHost Message Stream: %v", err)
			// Stream is closed.  Close the connection and bail out
			conn.close()
			return
		}

		if cmd.GetType() == cherami.OutputHostCommandType_MESSAGE {
			conn.reporter.IncCounter(metrics.ConsumeMessageRate, nil, 1)
			msg := cmd.Message
			delivery := newDelivery(msg, conn)

			if conn.isClosed() {
				delivery.Nack()
				return
			}

			select {
			case conn.deliveryCh <- delivery:
			case <-conn.closeChannel:
				conn.logger.Info("close signal received, initiating readPump drain")
				conn.drainReadPipe()
				return
			}

			localCredits++
		} else if cmd.GetType() == cherami.OutputHostCommandType_RECONFIGURE {
			conn.reporter.IncCounter(metrics.ConsumeReconfigureRate, nil, 1)
			reconfigInfo := cmd.Reconfigure
			conn.logger.WithField(common.TagReconfigureID, common.FmtReconfigureID(reconfigInfo.GetUpdateUUID())).Info("Reconfigure command received from OutputHost.")
			select {
			case conn.reconfigureCh <- reconfigureInfo{eventType: reconfigureCmdReconfigureType, reconfigureID: reconfigInfo.GetUpdateUUID()}:
			default:
				conn.logger.WithField(common.TagReconfigureID, common.FmtReconfigureID(reconfigInfo.GetUpdateUUID())).Info("Reconfigure channel is full.  Drop reconfigure command.")
			}
		}
	}
}

func (conn *outputHostConnection) writeCreditsPump() {
	// This will unblock any pending read operations on the stream.
	defer func() {
		if conn.cancel != nil {
			conn.cancel()
		}
		conn.outputHostStream.Done()
	}()

	// Send initial credits to OutputHost
	if err := conn.sendCredits(int32(conn.prefetchSize)); err != nil {
		conn.logger.Infof("Error sending initialCredits to OutputHost: %v", err)

		conn.close()
		return
	}

	// Start the write pump
	for {
		select {
		case credits := <-conn.creditsCh:
			// TODO: this needs to be converted into a metric
			//conn.logger.Infof("Sending credits to output host: %v", credits)
			if err := conn.sendCredits(credits); err != nil {
				conn.logger.Infof("Error sending creditBatchSize to OutputHost: %v", err)

				conn.close()
			}
		case <-conn.closeChannel:
			conn.logger.Info("WriteCreditsPump closing due to connection closed.")
			return
		}
	}
}

func (conn *outputHostConnection) writeAcksPump() {
	var buffer []string
	var bufferTicker <-chan time.Time
	var err error

	bufferTicker = time.Tick(time.Second / 10)

	push := func(buf *[]string) {
		ackRequest := cherami.NewAckMessagesRequest()
		ackRequest.AckIds = *buf
		*buf = nil

		ctx, cancel := thrift.NewContext(conn.options.Timeout)
		conn.reporter.IncCounter(metrics.ConsumeAckRate, nil, int64(len(ackRequest.AckIds)))
		err = conn.ackClient.AckMessages(ctx, ackRequest)
		if err != nil {
			conn.logger.Infof("error in ack batch: %v", err)
			conn.reporter.IncCounter(metrics.ConsumeAckFailedRate, nil, int64(len(ackRequest.AckIds)))
		}
		cancel()
	}

ackPump:
	for {
		select {

		case ackIds, ok := <-conn.acksBatchCh:
			if !ok {
				conn.logger.Info("writeAcksPump closing.")
				break ackPump
			}
			buffer = append(buffer, ackIds...)

			if len(buffer) >= ackBatchSize {
				push(&buffer)
			}

		case <-bufferTicker:
			// Check for excessive idleness and disable the ticker if this happens
			if len(buffer) > 0 {
				push(&buffer)
			}
		}
	}

	// Make sure we push all acks before bailing out of the pump
	if len(buffer) > 0 {
		push(&buffer)
	}
}

func (conn *outputHostConnection) sendCredits(credits int32) error {
	flows := cherami.NewControlFlow()
	flows.Credits = common.Int32Ptr(credits)

	sw := conn.reporter.StartTimer(metrics.ConsumeCreditLatency, nil)
	defer sw.Stop()

	err := conn.outputHostStream.Write(flows)
	if err == nil {
		err = conn.outputHostStream.Flush()
	} else {
		conn.reporter.IncCounter(metrics.ConsumeCreditRate, nil, int64(credits))
	}

	return err
}

func (conn *outputHostConnection) GetAcknowledgerID() string {
	return conn.connKey
}

func (conn *outputHostConnection) Ack(ids []string) error {
	conn.acksBatchLk.RLock()
	defer conn.acksBatchLk.RUnlock()

	select {
	case <-conn.acksBatchClosed:
		return nil
	default:
		conn.acksBatchCh <- ids
	}

	return nil
}

func (conn *outputHostConnection) Nack(ids []string) error {
	ackRequest := cherami.NewAckMessagesRequest()
	ackRequest.NackIds = ids

	ctx, cancel := thrift.NewContext(conn.options.Timeout)
	defer cancel()

	conn.reporter.IncCounter(metrics.ConsumeNackRate, nil, int64(len(ids)))
	return conn.ackClient.AckMessages(ctx, ackRequest)
}

func (conn *outputHostConnection) ReportProcessingTime(t time.Duration, ack bool) {
	conn.reporter.RecordTimer(metrics.ProcessLatency, nil, t)
	if ack {
		conn.reporter.RecordTimer(metrics.ProcessAckLatency, nil, t)
	} else {
		conn.reporter.RecordTimer(metrics.ProcessNackLatency, nil, t)
	}
}

func (conn *outputHostConnection) closeAcksBatchCh() {
	conn.acksBatchLk.Lock()
	defer conn.acksBatchLk.Unlock()

	close(conn.acksBatchCh)
	close(conn.acksBatchClosed)
}
