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
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/backoff"
	"github.com/uber/cherami-client-go/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/cherami"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"

	"github.com/pborman/uuid"
)

type (
	publisherImpl struct {
		basePublisher
		connection                       *tchannel.Channel
		messagesCh                       chan putMessageRequest
		reconfigureCh                    chan reconfigureInfo
		closingCh                        chan struct{}
		closeChannel                     chan struct{}
		isClosing                        int32
		maxInflightMessagesPerConnection int
		lk                               sync.Mutex
		opened                           bool
		closed                           bool
		paused                           uint32
		connections                      map[string]*connection
		wsConnector                      WSConnector
		reconfigurable                   *reconfigurable
	}

	putMessageRequest struct {
		message    *cherami.PutMessage
		messageAck chan<- *PublisherReceipt
	}
)

const (
	defaultMessageTimeout = time.Minute
	pauseError            = `Cherami publisher is paused`
)

var _ Publisher = (*publisherImpl)(nil)

// NewPublisher constructs a new Publisher object
// Deprecated: NewPublisher is deprecated, please use NewPublisherWithReporter
func NewPublisher(client *clientImpl, path string, maxInflightMessagesPerConnection int) Publisher {
	client.options.Logger.Warn("NewPublisher is a depredcated method, please use the new method NewPublisherWithReporter")
	return NewPublisherWithReporter(client, path, maxInflightMessagesPerConnection, client.options.MetricsReporter)
}

// NewPublisherWithReporter constructs a new Publisher object
func NewPublisherWithReporter(client *clientImpl, path string, maxInflightMessagesPerConnection int, reporter metrics.Reporter) Publisher {
	base := basePublisher{
		client:                         client,
		retryPolicy:                    createDefaultPublisherRetryPolicy(),
		path:                           path,
		logger:                         client.options.Logger.WithField(common.TagDstPth, common.FmtDstPth(path)),
		reporter:                       reporter,
		reconfigurationPollingInterval: client.options.ReconfigurationPollingInterval,
	}
	publisher := &publisherImpl{
		basePublisher:                    base,
		maxInflightMessagesPerConnection: maxInflightMessagesPerConnection,
		messagesCh:                       make(chan putMessageRequest),
		reconfigureCh:                    make(chan reconfigureInfo, reconfigureChBufferSize),
		closingCh:                        make(chan struct{}),
		closeChannel:                     make(chan struct{}),
		isClosing:                        0,
		wsConnector:                      NewWSConnector(),
	}

	return publisher
}

func (s *publisherImpl) Open() error {
	s.lk.Lock()
	defer s.lk.Unlock()

	if !s.opened {
		publisherOptions, err := s.readPublisherOptions()
		if err != nil {
			s.logger.Errorf("Error resolving input hosts: %v", err)
			return err
		}
		s.checksumOption = publisherOptions.GetChecksumOption()

		ch, err := tchannel.NewChannel(uuid.New(), nil)
		if err != nil {
			return err
		}
		s.connection = ch

		chosenProtocol, chosenHostAddresses := s.choosePublishEndpoints(publisherOptions)
		s.connections = make(map[string]*connection)
		for _, host := range chosenHostAddresses {
			connKey := common.GetConnectionKey(host)
			// ReadDestinationHosts can return duplicates, so we need to dedupe to make sure we create a single connection for each host
			if _, ok := s.connections[connKey]; !ok {
				conn, err := s.createInputHostConnection(connKey, chosenProtocol)
				if err != nil {
					if conn != nil {
						conn.close()
					}

					// TODO: We should be returning failure only when no connections could be opened to InputHost
					return err
				}

				s.connections[connKey] = conn
			}
		}
		s.reporter.UpdateGauge(metrics.PublishNumConnections, nil, int64(len(s.connections)))

		s.reconfigurable = newReconfigurable(s.reconfigureCh, s.closingCh, s.reconfigurePublisher, s.logger, s.reconfigurationPollingInterval)
		go s.reconfigurable.reconfigurePump()

		s.opened = true
		s.logger.Info("Publisher Opened.")
	}

	return nil
}

func (s *publisherImpl) Pause() {
	atomic.StoreUint32(&s.paused, 1)
}

func (s *publisherImpl) Resume() {
	atomic.StoreUint32(&s.paused, 0)
}

func (s *publisherImpl) Close() {
	if atomic.CompareAndSwapInt32(&s.isClosing, 0, 1) {
		close(s.closingCh)
	} else {
		return
	}

	s.lk.Lock()
	defer s.lk.Unlock()
	if s.connections != nil {
		for _, inputHostConn := range s.connections {
			inputHostConn.close()
		}
		s.reporter.UpdateGauge(metrics.PublishNumConnections, nil, 0)
	}

	if s.connection != nil {
		s.connection.Close()
	}

	// closing channel should make all outstanding publish to fail
	close(s.closeChannel)
	s.closed = true
	s.opened = false
	s.logger.Info("Publisher Closed.")
}

// Publish can be used to synchronously publish a message to Cherami
func (s *publisherImpl) Publish(message *PublisherMessage) *PublisherReceipt {
	if atomic.LoadUint32(&s.paused) > 0 {
		return &PublisherReceipt{Error: fmt.Errorf(pauseError)}
	}
	timeoutTimer := time.NewTimer(defaultMessageTimeout)
	defer timeoutTimer.Stop()

	var receipt *PublisherReceipt
	publishOp := func() error {
		srCh := make(chan *PublisherReceipt, 1)
		_, err := s.PublishAsync(message, srCh)
		if err != nil {
			s.reporter.IncCounter(metrics.PublisherMessageFailed, nil, 1)
			return err
		}

		select {
		case receipt = <-srCh:
			if receipt.Error != nil {
				s.reporter.IncCounter(metrics.PublisherMessageFailed, nil, 1)
			}
			return receipt.Error
		case <-timeoutTimer.C:
			s.reporter.IncCounter(metrics.PublisherMessageTimedout, nil, 1)
			return ErrMessageTimedout
		}
	}

	err := backoff.Retry(publishOp, s.retryPolicy, nil)
	if err != nil {
		return &PublisherReceipt{Error: err}
	}

	return receipt
}

// PublishAsync accepts a message, but returns immediately with the local
// reference ID
func (s *publisherImpl) PublishAsync(message *PublisherMessage, done chan<- *PublisherReceipt) (string, error) {
	if atomic.LoadUint32(&s.paused) > 0 {
		return "", fmt.Errorf(pauseError)
	}

	if !s.opened {
		return "", fmt.Errorf("Cannot publish message to path '%s'. Publisher is not opened.", s.path)
	}

	request := putMessageRequest{
		message:    s.toPutMessage(message),
		messageAck: done,
	}
	id := request.message.GetID()
	s.messagesCh <- request
	return id, nil
}

func (s *publisherImpl) reconfigurePublisher() {
	s.lk.Lock()
	defer s.lk.Unlock()

	var err error

	select {
	case <-s.closingCh:
		s.logger.Info("Publisher is closing.  Ignore reconfiguration.")
	default:
		var conn *connection

		publisherOptions := &cherami.ReadPublisherOptionsResult_{}
		if atomic.LoadUint32(&s.paused) == 0 {
			publisherOptions, err = s.readPublisherOptions()
			if err != nil {
				s.logger.Infof("Error resolving input hosts: %v", err)
				if _, ok := err.(*cherami.EntityNotExistsError); ok {
					// Destination is deleted. Continue with reconfigure and close all connections
					publisherOptions = &cherami.ReadPublisherOptionsResult_{}
				} else {
					// This is a potentially a transient error.
					// Retry on next reconfigure
					return
				}
			}
		}

		chosenProtocol, chosenHostAddresses := s.choosePublishEndpoints(publisherOptions)

		// First remove any closed connections from the connections map
		for existingConnKey, existingConn := range s.connections {
			if existingConn.isClosed() {
				delete(s.connections, existingConnKey)
			}
		}

		currentHosts := make(map[string]*connection)
		for _, host := range chosenHostAddresses {
			connKey := common.GetConnectionKey(host)
			conn = s.connections[connKey]
			if conn == nil || conn.isClosed() {
				// Newly assigned host, create a connection
				connLogger := s.logger.WithField(common.TagHostIP, common.FmtHostIP(connKey))
				connLogger.Info("Discovered new InputHost during reconfiguration.")
				conn, err = s.createInputHostConnection(connKey, chosenProtocol)
				if err != nil {
					connLogger.Info("Error creating connection to InputHost after reconfiguration.")
					if conn != nil {
						conn.close()
					}
				} else {
					connLogger.Info("Successfully created connection to InputHost after reconfiguration.")
					// Successfully created a connection to new host.  Add it to current list of input hosts
					currentHosts[connKey] = conn
				}
			} else {
				// Existing input host connection, copy it over to current collection of input hosts
				currentHosts[connKey] = conn
			}
		}

		// Now close all remaining list of input host connections
		for host, inputHostConn := range s.connections {
			if _, ok := currentHosts[host]; !ok {
				connLogger := s.logger.WithField(common.TagHostIP, common.FmtHostIP(inputHostConn.connKey))
				connLogger.Info("Closing connection to InputHost after reconfiguration.")
				inputHostConn.close()
			}
		}

		connectedHosts := make([]string, len(currentHosts))
		for k := range currentHosts {
			connectedHosts = append(connectedHosts, k)
		}
		s.logger.WithField(common.TagHosts, connectedHosts).Debug("List of connected input hosts.")

		// Now assign the list of current input hosts to list of connections
		s.connections = currentHosts
		s.reporter.UpdateGauge(metrics.PublishNumConnections, nil, int64(len(s.connections)))
	}
}

func (s *publisherImpl) createInputHostConnection(connKey string, protocol cherami.Protocol) (*connection, error) {
	connLogger := s.logger.WithField(common.TagHostIP, common.FmtHostIP(connKey))

	// TODO [ljj] to be removed once moved to websocket
	c, err := s.createInputHostClient(connKey)
	if err != nil {
		connLogger.Infof("Error creating InputHost client: %v", err)
		return nil, err
	}

	conn := newConnection(c, s.wsConnector, s.path, s.messagesCh, s.reconfigureCh, connKey, protocol, s.maxInflightMessagesPerConnection, connLogger, s.reporter)
	err = conn.open()
	if err != nil {
		connLogger.Infof("Error opening InputHost connection: %v", err)
		return conn, err
	}

	return conn, nil
}

func (s *publisherImpl) createInputHostClient(hostPort string) (cherami.TChanBIn, error) {
	tClient := thrift.NewClient(s.connection, common.InputServiceName, &thrift.ClientOptions{
		HostPort: hostPort,
	})
	client := cherami.NewTChanBInClient(tClient)

	return client, nil
}

func (s *publisherImpl) chooseProcotol(hostProtocols []*cherami.HostProtocol) (int, error) {
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
		s.logger.WithField(`protocols`, serverSupportedProtocol).Error("No protocol is supported by client")
		return clientSupportButDeprecated, &cherami.BadRequestError{Message: `No protocol is supported by client`}
	}

	s.logger.WithField(`protocol`, hostProtocols[clientSupportButDeprecated].GetProtocol()).Warn("Client using deprecated protocol")
	return clientSupportButDeprecated, nil
}
