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
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/uber-common/bark"
	"golang.org/x/net/context"

	"time"

	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/metrics"
	"github.com/uber/cherami-client-go/stream"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
)

type (
	connection struct {
		inputHostStream stream.BInOpenPublisherStreamOutCall
		path            string
		inputHostClient cherami.TChanBIn
		wsConnector     WSConnector
		connKey         string
		protocol        cherami.Protocol
		messagesCh      <-chan putMessageRequest
		reconfigureCh   chan<- reconfigureInfo
		replyCh         chan *messageResponse
		shuttingDownCh  chan struct{}
		closeCh         chan struct{}
		writeMsgPumpWG  sync.WaitGroup
		readAckPumpWG   sync.WaitGroup
		cancel          context.CancelFunc
		logger          bark.Logger
		reporter        metrics.Reporter

		lk      sync.Mutex
		opened  int32
		closed  int32
		drained int32

		sentMsgs        int64 // no: of messages sent to the inputhost
		sentAcks        int64 // no: of messages successfully acked
		sentNacks       int64 // no: of messages nacked (STATUS_FAILED)
		sentThrottled   int64 // no: of messages throttled
		failedMsgs      int64 // no: of failed messages *after* we successfully wrote to the stream
		writeFailedMsgs int64 // no: of messages failed to even write to the stream
	}

	// This struct is created by writePump after writing message to stream.
	// readAcksPump reads it from replyCh to populate the inflightMessages
	messageResponse struct {
		// This is unique identifier of message
		ackID string
		// This is the channel created by Publisher for each Publish request
		// It is written to by readAcksPump when the ack is received from InputHost on the AckStream
		completion chan<- *PublisherReceipt
		// This is user specified context to pass through
		userContext map[string]string
	}

	ackChannelClosedError struct{}
)

const (
	defaultMaxInflightMessages = 1000
	defaultWGTimeout           = time.Minute
	defaultDrainTimeout        = time.Minute
	defaultCheckDrainTimeout   = 5 * time.Second
)

func newConnection(client cherami.TChanBIn, wsConnector WSConnector, path string, messages <-chan putMessageRequest,
	reconfigureCh chan<- reconfigureInfo, connKey string, protocol cherami.Protocol,
	maxInflightMessages int, logger bark.Logger, reporter metrics.Reporter) *connection {
	if maxInflightMessages <= 0 {
		maxInflightMessages = defaultMaxInflightMessages
	}

	conn := &connection{
		inputHostClient: client,
		wsConnector:     wsConnector,
		path:            path,
		connKey:         connKey,
		protocol:        protocol,
		messagesCh:      messages,
		reconfigureCh:   reconfigureCh,
		replyCh:         make(chan *messageResponse, maxInflightMessages),
		shuttingDownCh:  make(chan struct{}),
		closeCh:         make(chan struct{}),
		logger:          logger,
		reporter:        reporter,
	}

	return conn
}

func (conn *connection) open() error {
	conn.lk.Lock()
	defer conn.lk.Unlock()

	if atomic.LoadInt32(&conn.opened) == 0 {
		switch conn.protocol {
		case cherami.Protocol_WS:
			conn.logger.Infof("Using websocket to connect to input host %s", conn.connKey)
			wsStream, err := conn.wsConnector.OpenPublisherStream(conn.connKey, http.Header{
				"path": {conn.path},
			})
			if err != nil {
				conn.logger.Infof("Error opening websocket connection to input host %s: %v", conn.connKey, err)
				return err
			}

			conn.inputHostStream = wsStream
			conn.cancel = nil
		default:
			return &cherami.BadRequestError{Message: `Protocol not supported`}
		}

		conn.readAckPumpWG.Add(1)
		go conn.readAcksPump()
		conn.writeMsgPumpWG.Add(1)
		go conn.writeMessagesPump()

		atomic.StoreInt32(&conn.opened, 1)
		conn.logger.Info("Input host connection opened.")
	}

	return nil
}

func (conn *connection) isShuttingDown() bool {
	select {
	case <-conn.shuttingDownCh:
		// already shutdown
		return true
	default:
	}

	return false
}

// stopWritePump should drain the pump after getting the lock
func (conn *connection) stopWritePump() {
	conn.lk.Lock()
	conn.stopWritePumpWithLock()
	conn.lk.Unlock()
}

// Note: this needs to be called with the conn lock held!
func (conn *connection) stopWritePumpWithLock() {
	if !conn.isShuttingDown() {
		close(conn.shuttingDownCh)
		if ok := common.AwaitWaitGroup(&conn.writeMsgPumpWG, defaultWGTimeout); !ok {
			conn.logger.Warn("writeMsgPumpWG timed out")
		}
		conn.logger.Info("stopped write pump")
		atomic.StoreInt32(&conn.drained, 1)
		// now make sure we spawn a routine to wait for drain and
		// close the connection if needed
		go conn.waitForDrain()
	}
}
func (conn *connection) close() {
	conn.lk.Lock()
	defer conn.lk.Unlock()

	// no need to close if we are already closing and/or draining
	if atomic.LoadInt32(&conn.closed) == 0 {
		// First shutdown the write pump to make sure we don't leave any message without ack
		conn.stopWritePumpWithLock()
		// Now shutdown the read pump and drain all inflight messages
		close(conn.closeCh)
		if ok := common.AwaitWaitGroup(&conn.readAckPumpWG, defaultWGTimeout); !ok {
			conn.logger.Warn("readAckPumpWG timed out")
		}

		// Both pumps are shutdown.  Close the underlying stream.
		if conn.cancel != nil {
			conn.cancel()
		}

		if conn.inputHostStream != nil {
			conn.inputHostStream.Done()
		}

		atomic.StoreInt32(&conn.closed, 1)
		conn.logger.WithFields(bark.Fields{
			`sentMsgs`:        conn.sentMsgs,
			`sentAcks`:        conn.sentAcks,
			`sentNacks`:       conn.sentNacks,
			`sentThrottled`:   conn.sentThrottled,
			`failedMsgs`:      conn.failedMsgs,
			`writeFailedMsgs`: conn.writeFailedMsgs,
		}).Info("Input host connection closed.")

		// trigger a reconfiguration due to connection closed
		select {
		case conn.reconfigureCh <- reconfigureInfo{eventType: connClosedReconfigureType, reconfigureID: conn.connKey}:
		default:
			conn.logger.Info("Reconfigure channel is full.  Drop reconfigure command due to connection close.")
		}
	}
}

func (conn *connection) writeMessagesPump() {
	defer conn.writeMsgPumpWG.Done()

	for {
		select {
		case pr := <-conn.messagesCh:
			sw := conn.reporter.StartTimer(metrics.PublishMessageLatency, nil)
			conn.reporter.IncCounter(metrics.PublishMessageRate, nil, 1)

			err := conn.inputHostStream.Write(pr.message)
			if err == nil {
				// TODO: We don't need to flush on every message. Rewrite the method to flush once messagesCh is empty
				err = conn.inputHostStream.Flush()
			}
			sw.Stop()

			if err == nil {
				conn.replyCh <- &messageResponse{pr.message.GetID(), pr.messageAck, pr.message.GetUserContext()}
				atomic.AddInt64(&conn.sentMsgs, 1)
			} else {
				conn.reporter.IncCounter(metrics.PublishMessageFailedRate, nil, 1)
				conn.logger.WithField(common.TagMsgID, common.FmtMsgID(pr.message.GetID())).Infof("Error writing message to stream: %v", err)

				pr.messageAck <- &PublisherReceipt{
					ID:          pr.message.GetID(),
					Error:       err,
					UserContext: pr.message.GetUserContext(),
				}
				conn.writeFailedMsgs++

				// Write failed, rebuild connection
				go conn.close()
			}
		case <-conn.shuttingDownCh:
			// Connection is closed.  Bail out of the pump and close stream
			return
		}
	}
}

func (conn *connection) handleReconfigCmd(reconfigInfo *cherami.ReconfigureInfo) {
	select {
	case conn.reconfigureCh <- reconfigureInfo{eventType: reconfigureCmdReconfigureType, reconfigureID: reconfigInfo.GetUpdateUUID()}:
	default:
		conn.logger.WithField(common.TagReconfigureID, common.FmtReconfigureID(reconfigInfo.GetUpdateUUID())).Warn("Reconfigure channel is full.  Drop reconfigure command.")
	}
}

func (conn *connection) readAcksPump() {
	defer conn.readAckPumpWG.Done()

	inflightMessages := make(map[string]*messageResponse)
	// This map is needed when we receive a reply out of order before the inflightMessages is populated
	earlyReplyAcks := make(map[string]*PublisherReceipt)
	defer conn.failInflightMessages(inflightMessages)

	// Flag which is set when AckStream is closed by InputHost
	isEOF := false
	for {
		conn.reporter.UpdateGauge(metrics.PublishNumInflightMessagess, nil, int64(len(inflightMessages)))

		if isEOF || len(inflightMessages) == 0 {
			select {
			// We want to make sure that ackId is in the inflightMessages before we read a response for it
			case resCh := <-conn.replyCh:
				populateInflightMapUtil(inflightMessages, earlyReplyAcks, resCh)
			// Connection is closed just fail all inflightMessages
			case <-conn.closeCh:
				// First drain the replyCh to get all inflight messages which are in the
				// buffer. This is essential because we could have some messages in the channel buffer
				// which we need to fail as well so that the clients can retry. If not,
				// we will get unnecessary timeouts.
			DrainLoop:
				for {
					select {
					case resCh, ok := <-conn.replyCh:
						if !ok {
							break DrainLoop
						}
						populateInflightMapUtil(inflightMessages, earlyReplyAcks, resCh)
					default:
						break DrainLoop
					}
				}

				return
			}
		} else {
			cmd, err := conn.inputHostStream.Read()
			if err != nil {
				// Error reading from stream.  Time to close and bail out.
				conn.logger.Infof("Error reading Ack Stream: %v", err)
				// Ack stream is closed.  Also close the Connection incase this is triggered by InputHost
				// Any inflight messages at this point needs to be failed
				go conn.close()
				// AckStream is closed by InputHost.  There is no point in calling Read again.
				// This flag is used to prevent readAckPump from calling read when we know ack stream is closed.
				// We still need the pump going to drain the conn.replyCh to populate any inflight messages
				isEOF = true
			} else {
				if cmd.GetType() == cherami.InputHostCommandType_ACK {
					conn.reporter.IncCounter(metrics.PublishAckRate, nil, 1)
					ack := cmd.Ack
					messageResp, exists := inflightMessages[ack.GetID()]
					if !exists {
						// Probably we received the ack even before the inflightMessages map is populated.
						// Let's put it in the earlyReplyAcks map so we can immediately complete the response when seen by this pump.
						//conn.logger.WithField(common.TagAckID, common.FmtAckID(ack.GetID())).Debug("Received Ack before populating inflight messages.")
						earlyReplyAcks[ack.GetID()] = conn.processMessageAck(ack)
					} else {
						delete(inflightMessages, ack.GetID())
						messageResp.completion <- conn.processMessageAck(ack)
					}
				} else if cmd.GetType() == cherami.InputHostCommandType_RECONFIGURE {
					conn.reporter.IncCounter(metrics.PublishReconfigureRate, nil, 1)
					rInfo := cmd.Reconfigure
					conn.logger.WithField(common.TagReconfigureID, common.FmtReconfigureID(rInfo.GetUpdateUUID())).Info("Reconfigure command received from InputHost.")
					conn.handleReconfigCmd(rInfo)
				} else if cmd.GetType() == cherami.InputHostCommandType_DRAIN {
					// drain all inflight messages
					// reconfigure to pick up new extents if any
					conn.reporter.IncCounter(metrics.PublishDrainRate, nil, 1)
					// start draining by just stopping the write pump.
					// this makes sure, we don't send any new messages.
					// the read pump will exit when the server completes the drain
					go conn.stopWritePump()
					rInfo := cmd.Reconfigure
					conn.logger.WithField(common.TagReconfigureID, common.FmtReconfigureID(rInfo.GetUpdateUUID())).Info("Drain command received from InputHost.")
					conn.handleReconfigCmd(rInfo)
				}

			}
		}
	}
}

func (conn *connection) isOpened() bool {
	return atomic.LoadInt32(&conn.opened) != 0
}

// isClosed should return true if either closed it set
// or if "drain" is set, which means the connection has already
// stopped the write pump.
// This is needed to make sure if reconfigure gives the same
// inputhost, we should open up a new connection object (most
// likely for a new extent). In the worst case, if for some reason
// the controller returns the same old extent which is draining,
// the server will anyway reject the connection
func (conn *connection) isClosed() bool {
	return (atomic.LoadInt32(&conn.closed) != 0 || atomic.LoadInt32(&conn.drained) != 0)
}

func (conn *connection) isDraining() bool {
	return atomic.LoadInt32(&conn.drained) != 0
}

func (e *ackChannelClosedError) Error() string {
	return "Ack channel closed."
}

func (conn *connection) processMessageAck(messageAck *cherami.PutMessageAck) *PublisherReceipt {
	ret := &PublisherReceipt{
		ID:          messageAck.GetID(),
		UserContext: messageAck.GetUserContext(),
	}

	stat := messageAck.GetStatus()
	if stat != cherami.Status_OK {
		if stat == cherami.Status_THROTTLED {
			atomic.AddInt64(&conn.sentThrottled, 1)
		} else {
			atomic.AddInt64(&conn.sentNacks, 1)
		}
		ret.Error = errors.New(messageAck.GetMessage())
	} else {
		atomic.AddInt64(&conn.sentAcks, 1)
		ret.Receipt = messageAck.GetReceipt()
	}

	return ret
}

// isAlreadyDrained returns true when we have sent response for all the sent messages
// response should include acks, nacks and throttled messages
// Note: failed messages and writeFailed messages should not be counted
func (conn *connection) isAlreadyDrained() bool {
	respSent := atomic.LoadInt64(&conn.sentAcks) + atomic.LoadInt64(&conn.sentNacks) + atomic.LoadInt64(&conn.sentThrottled)
	if respSent == atomic.LoadInt64(&conn.sentMsgs) {
		return true
	}

	return false
}

func (conn *connection) waitForDrain() {
	// wait for some timeout period and close the connection
	// this is needed because even though server closes the
	// connection, we never call "stream.Read" until we have some
	// messages in-flight, which will not be the case when we have already drained
	drainTimer := time.NewTimer(defaultDrainTimeout)
	defer drainTimer.Stop()

	checkDrainTimer := time.NewTimer(defaultCheckDrainTimeout)
	defer checkDrainTimer.Stop()
	select {
	case <-conn.closeCh:
		// already closed
		return
	case <-checkDrainTimer.C:
		// check if we got the acks for all sent messages
		if conn.isAlreadyDrained() {
			conn.logger.Infof("Inputhost connection drained completely")
			go conn.close()
			return
		}
	case <-drainTimer.C:
		go conn.close()
		return
	}
}

// populateInflightMapUtil is used to populate the inflightMessages Map,
// based on the acks we received as well.
func populateInflightMapUtil(inflightMessages map[string]*messageResponse, earlyReplyAcks map[string]*PublisherReceipt, resCh *messageResponse) {
	// First check if we have already seen the ack for this ID
	if ack, ok := earlyReplyAcks[resCh.ackID]; ok {
		// We already received the ack for this msgID.  Complete the request immediately.
		//conn.logger.WithField(common.TagAckID, common.FmtAckID(resCh.ackID)).Debug("Found ack for this response in earlyReplyAcks map.  Completing immediately.")
		delete(earlyReplyAcks, resCh.ackID)
		resCh.completion <- ack
	} else {
		// Populate the inflightMessages map so we can complete the response after reading the ack from Cherami
		inflightMessages[resCh.ackID] = resCh
	}
}

func (conn *connection) failInflightMessages(inflightMessages map[string]*messageResponse) {
	for id, messageResp := range inflightMessages {
		messageResp.completion <- &PublisherReceipt{
			ID:          id,
			Error:       &ackChannelClosedError{},
			UserContext: messageResp.userContext,
		}
		conn.failedMsgs++
	}
}
