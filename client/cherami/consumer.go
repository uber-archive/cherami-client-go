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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
)

type (
	consumerImpl struct {
		path              string
		consumerGroupName string
		consumerName      string
		ackConnection     *tchannel.Channel
		prefetchSize      int
		options           *ClientOptions
		client            *clientImpl
		deliveryCh        chan Delivery
		reconfigureCh     chan reconfigureInfo
		closingCh         chan struct{}
		isClosing         int32
		paused            uint32
		logger            bark.Logger
		reporter          metrics.Reporter

		lk             sync.Mutex
		opened         bool
		connections    map[string]*outputHostConnection
		wsConnector    WSConnector
		reconfigurable *reconfigurable
	}

	deliveryID struct {
		// AckowledgerID is the identifier for underlying acknowledger which can be used to Ack/Nack this delivery
		AcknowledgerID string
		// MessageAckID is the Ack identifier for this delivery
		MessageAckID string
	}
)

func newConsumer(client *clientImpl, path, consumerGroupName, consumerName string, prefetchSize int, options *ClientOptions, reporter metrics.Reporter) Consumer {
	consumer := &consumerImpl{
		client:            client,
		options:           options,
		path:              path,
		consumerGroupName: consumerGroupName,
		consumerName:      consumerName,
		prefetchSize:      prefetchSize,
		reconfigureCh:     make(chan reconfigureInfo, reconfigureChBufferSize),
		closingCh:         make(chan struct{}),
		isClosing:         0,
		logger:            client.options.Logger.WithFields(bark.Fields{common.TagDstPth: common.FmtDstPth(path), common.TagCnsPth: common.FmtCnsPth(consumerGroupName)}),
		reporter:          reporter,
		wsConnector:       NewWSConnector(),
	}

	return consumer
}

// This token is generated in delivery.GetDeliveryToken.  Make sure to keep both implementations in sync to
// serialize/deserialize these tokens
func newDeliveryID(token string) (deliveryID, error) {
	parts := strings.Split(token, deliveryTokenSplitter)
	if len(parts) != 2 {
		return deliveryID{}, fmt.Errorf("Invalid delivery token: %v", token)
	}

	return deliveryID{
		AcknowledgerID: parts[0],
		MessageAckID:   parts[1],
	}, nil
}

func (c *consumerImpl) Open(deliveryCh chan Delivery) (chan Delivery, error) {
	c.lk.Lock()
	defer c.lk.Unlock()

	if !c.opened {
		c.deliveryCh = deliveryCh
		consumerOptions, err := c.client.ReadConsumerGroupHosts(c.path, c.consumerGroupName)
		if err != nil {
			c.logger.Errorf("Error resolving output hosts: %v", err)
			c.deliveryCh = nil
			return nil, err
		}

		// Create a separate connection for ack messages call
		ackCh, err := tchannel.NewChannel(uuid.New(), nil)
		if err != nil {
			return nil, err
		}
		c.ackConnection = ackCh

		// pick best protocol from what server suggested
		hostProtocols := consumerOptions.GetHostProtocols()
		chosenIdx, tchanProtocolIdx, err := c.chooseProcotol(hostProtocols)
		chosenProtocol := cherami.Protocol_WS
		chosenHostAddresses := consumerOptions.GetHostAddresses()
		tchanHostAddresses := consumerOptions.GetHostAddresses()
		if err == nil {
			chosenProtocol = hostProtocols[chosenIdx].GetProtocol()
			chosenHostAddresses = hostProtocols[chosenIdx].GetHostAddresses()
			tchanHostAddresses = hostProtocols[tchanProtocolIdx].GetHostAddresses()
		}

		c.connections = make(map[string]*outputHostConnection)
		for idx, host := range chosenHostAddresses {
			connKey := common.GetConnectionKey(host)
			// ReadConsumerGroupHosts can return duplicates, so we need to dedupe to make sure we create a single connection for each host
			if _, ok := c.connections[connKey]; !ok {
				conn, err := c.createOutputHostConnection(common.GetConnectionKey(tchanHostAddresses[idx]), connKey, chosenProtocol)
				if err != nil {
					if conn != nil {
						conn.close()
					}

					c.logger.Errorf("Error opening outputhost connection on %v:%v: %v", host.GetHost(), host.GetPort(), err)
					// TODO: We should be returning failure only when no connections could be opened to OutputHost
					c.deliveryCh = nil
					return nil, err
				}

				// Add the connection to map used by Ack/Nack API to find the connection using DeliveryId
				c.connections[conn.GetAcknowledgerID()] = conn
			}
		}
		c.reporter.UpdateGauge(metrics.ConsumeNumConnections, nil, int64(len(c.connections)))

		c.reconfigurable = newReconfigurable(c.reconfigureCh, c.closingCh, c.reconfigureConsumer, c.logger, c.options.ReconfigurationPollingInterval)
		go c.reconfigurable.reconfigurePump()

		c.opened = true
	}

	return c.deliveryCh, nil
}

func (c *consumerImpl) Close() {
	// TODO: ideally this should be synchronized, i.e. wait until all
	// connections are properly shutdown, so that we make sure that
	// nothing gets written to c.deliveyCh afterwards, because owner of that
	// channel would likely close it after Close() returns.
	if atomic.CompareAndSwapInt32(&c.isClosing, 0, 1) {
		close(c.closingCh)
	} else {
		return
	}

	c.lk.Lock()
	defer c.lk.Unlock()
	if c.connections != nil {
		for _, outputHostConn := range c.connections {
			outputHostConn.close()
		}
		c.reporter.UpdateGauge(metrics.ConsumeNumConnections, nil, 0)
	}

	if c.ackConnection != nil {
		c.ackConnection.Close()
	}

	c.opened = false
}

func (c *consumerImpl) Pause() {
	atomic.StoreUint32(&c.paused, 1)
}

func (c *consumerImpl) Resume() {
	atomic.StoreUint32(&c.paused, 0)
}

func (c *consumerImpl) AckDelivery(token string) error {
	acknowledger, id, err := c.getAcknowledger(token)
	if err != nil {
		return err
	}

	return acknowledger.Ack([]string{id.MessageAckID})
}

func (c *consumerImpl) NackDelivery(token string) error {
	acknowledger, id, err := c.getAcknowledger(token)
	if err != nil {
		return err
	}

	return acknowledger.Nack([]string{id.MessageAckID})
}

func (c *consumerImpl) reconfigureConsumer() {
	c.lk.Lock()
	defer c.lk.Unlock()

	var err error

	select {
	case <-c.closingCh:
		c.logger.Info("Consumer is closing.  Ignore reconfiguration.")
	default:
		var conn *outputHostConnection

		consumerOptions := &cherami.ReadConsumerGroupHostsResult_{}
		if atomic.LoadUint32(&c.paused) == 0 {
			consumerOptions, err = c.client.ReadConsumerGroupHosts(c.path, c.consumerGroupName)
			if err != nil {
				c.logger.Warnf("Error resolving output hosts: %v", err)
				if _, ok := err.(*cherami.EntityNotExistsError); ok {
					// ConsumerGroup is deleted. Continue with reconfigure and close all connections
					consumerOptions = &cherami.ReadConsumerGroupHostsResult_{}
				} else {
					// This is a potentially a transient error.
					// Retry on next reconfigure
					return
				}
			}
		}

		// pick best protocol from what server suggested
		// note: tchannel is must have here since AckMessage is still using tchannel (non-streaming)
		// tchanProtocol stores tchannel hosts, same order as other protocols
		hostProtocols := consumerOptions.GetHostProtocols()
		chosenIdx, tchanProtocolIdx, err := c.chooseProcotol(hostProtocols)
		chosenProtocol := cherami.Protocol_WS
		chosenHostAddresses := consumerOptions.GetHostAddresses()
		tchanHostAddresses := consumerOptions.GetHostAddresses()
		if err == nil {
			chosenProtocol = hostProtocols[chosenIdx].GetProtocol()
			chosenHostAddresses = hostProtocols[chosenIdx].GetHostAddresses()
			tchanHostAddresses = hostProtocols[tchanProtocolIdx].GetHostAddresses()
		}

		// First remove any closed connections from the connections map
		for existingConnKey, existingConn := range c.connections {
			if existingConn.isClosed() {
				c.logger.WithField(common.TagHostIP, common.FmtHostIP(existingConnKey)).Info("Removing closed connection from cache.")
				existingConn.close()
				delete(c.connections, existingConnKey)
				c.logger.WithField(common.TagHostIP, common.FmtHostIP(existingConn.connKey)).Info("Removed connection from cache.")
			}
		}

		currentHosts := make(map[string]*outputHostConnection)
		for idx, host := range chosenHostAddresses {
			connKey := common.GetConnectionKey(host)
			conn = c.connections[connKey]
			if conn == nil || conn.isClosed() {
				// Newly assigned host, create a connection
				connLogger := c.logger.WithField(common.TagHostIP, common.FmtHostIP(connKey))
				connLogger.Info("Discovered new OutputHost during reconfiguration.")
				conn, err = c.createOutputHostConnection(common.GetConnectionKey(tchanHostAddresses[idx]), connKey, chosenProtocol)
				if err != nil {
					connLogger.Info("Error creating connection to OutputHost after reconfiguration.")
					if conn != nil {
						conn.close()
					}
				} else {
					connLogger.Info("Successfully created connection to OutputHost after reconfiguration.")
					// Successfully created a connection to new host.  Add it to current list of output hosts
					currentHosts[conn.GetAcknowledgerID()] = conn
				}
			} else {
				// Existing output host connection, copy it over to current collection of output hosts
				currentHosts[conn.GetAcknowledgerID()] = conn
			}
		}

		// Now close all remaining list of output host connections
		for host, outputHostConn := range c.connections {
			if _, ok := currentHosts[host]; !ok {
				connLogger := c.logger.WithField(common.TagHostIP, common.FmtHostIP(outputHostConn.connKey))
				connLogger.Info("Closing connection to OutputHost after reconfiguration.")
				outputHostConn.close()
			}
		}

		connectedHosts := make([]string, len(currentHosts))
		for k := range currentHosts {
			connectedHosts = append(connectedHosts, k)
		}
		c.logger.WithField(common.TagHosts, connectedHosts).Debug("List of connected output hosts.")
		// Now assign the list of current output hosts to list of connections
		c.connections = currentHosts
		c.reporter.UpdateGauge(metrics.ConsumeNumConnections, nil, int64(len(c.connections)))
	}
}

func (c *consumerImpl) createOutputHostConnection(tchanHostPort string, connKey string, protocol cherami.Protocol) (*outputHostConnection, error) {
	connLogger := c.logger.WithField(common.TagHostIP, common.FmtHostIP(connKey))

	// We use a separate connection for acks to make sure response for acks won't get blocked behind streaming messages
	ackClient, err := createOutputHostClient(c.ackConnection, tchanHostPort)
	if err != nil {
		connLogger.Infof("Error creating AckClient: %v", err)
		return nil, err
	}

	conn := newOutputHostConnection(ackClient, c.wsConnector, c.path, c.consumerGroupName, c.options, c.deliveryCh,
		c.reconfigureCh, connKey, protocol, int32(c.prefetchSize), connLogger, c.reporter)

	// Now open the connection
	err = conn.open()
	if err != nil {
		connLogger.Infof("Error opening OutputHost connection: %v", err)
		return nil, err
	}

	return conn, nil
}

func (c *consumerImpl) getAcknowledger(token string) (*outputHostConnection, deliveryID, error) {
	id, err := newDeliveryID(token)
	if err != nil {
		return nil, deliveryID{}, err
	}

	c.lk.Lock()
	acknowledger, ok := c.connections[id.AcknowledgerID]
	c.lk.Unlock()
	if !ok {
		return nil, id, fmt.Errorf("Cannot Ack/Nack message '%s'.  Acknowledger Id '%s' not found.",
			id.MessageAckID, id.AcknowledgerID)
	}

	return acknowledger, id, nil
}

func (c *consumerImpl) chooseProcotol(hostProtocols []*cherami.HostProtocol) (int, int, error) {
	clientSupportedProtocol := map[cherami.Protocol]bool{cherami.Protocol_WS: true}
	clientSupportButDeprecated := -1
	serverSupportedProtocol := make([]cherami.Protocol, 0, len(hostProtocols))

	// tchannel is needed for ack message
	tchanProtocolIdx := -1
	for idx, hostProtocol := range hostProtocols {
		if hostProtocol.GetProtocol() == cherami.Protocol_TCHANNEL {
			tchanProtocolIdx = idx
			break
		}
	}
	if tchanProtocolIdx == -1 {
		return -1, -1, &cherami.BadRequestError{Message: `TChannel is needed for client to ack message`}
	}

	for idx, hostProtocol := range hostProtocols {
		serverSupportedProtocol = append(serverSupportedProtocol, hostProtocol.GetProtocol())
		if _, found := clientSupportedProtocol[hostProtocol.GetProtocol()]; found {
			if !hostProtocol.GetDeprecated() {
				// found first supported and non-deprecated one, done
				return idx, tchanProtocolIdx, nil
			} else if clientSupportButDeprecated == -1 {
				// found first supported but deprecated one, keep looking
				clientSupportButDeprecated = idx
			}
		}
	}

	if clientSupportButDeprecated == -1 {
		c.logger.WithField(`protocols`, serverSupportedProtocol).Error("No protocol is supported by client")
		return -1, -1, &cherami.BadRequestError{Message: `No protocol is supported by client`}
	}

	c.logger.WithField(`protocol`, hostProtocols[clientSupportButDeprecated].GetProtocol()).Warn("Client using deprecated protocol")
	return clientSupportButDeprecated, tchanProtocolIdx, nil
}

func createOutputHostClient(ch *tchannel.Channel, hostPort string) (cherami.TChanBOut, error) {
	tClient := thrift.NewClient(ch, common.OutputServiceName, &thrift.ClientOptions{
		HostPort: hostPort,
	})
	client := cherami.NewTChanBOutClient(tClient)

	return client, nil
}
