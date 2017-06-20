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
	"sync"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber/cherami-client-go/common"
)

type (
	reconfigureType int

	reconfigureInfo struct {
		eventType     reconfigureType
		reconfigureID string
	}

	reconfigurable struct {
		reconfigureCh      <-chan reconfigureInfo
		reconfigureHandler func()
		connClosedHandler  func(host string)
		closingCh          chan struct{}
		logger             bark.Logger
		pollingInterval    time.Duration
	}
)

const (
	reconfigureChBufferSize = 1
	limiterDuration         = time.Millisecond * 500
)

const (
	reconfigureCmdReconfigureType = iota
	connClosedReconfigureType
)

func newReconfigurable(reconfigureCh <-chan reconfigureInfo, closingCh chan struct{}, reconfigureHandler func(),
	logger bark.Logger, pollingInterval time.Duration) *reconfigurable {
	r := &reconfigurable{
		reconfigureCh:      reconfigureCh,
		closingCh:          closingCh,
		reconfigureHandler: reconfigureHandler,
		logger:             logger,
		pollingInterval:    pollingInterval,
	}

	return r
}

func (s *reconfigurable) reconfigurePump(wg *sync.WaitGroup) {

	defer func() {
		if wg != nil {
			wg.Done()
		}
	}()

	s.logger.Info("Reconfiguration pump started.")
	heartbeat := time.NewTicker(s.pollingInterval)
	limiter := time.NewTicker(limiterDuration)
	lastReconfigureID := ""
	for {
		select {
		case <-s.closingCh:
			s.logger.Info("Reconfigure pump closing.")
			// Publisher/Consumer is going away, stop heartbeating and bail out of the pump
			heartbeat.Stop()
			limiter.Stop()
			return
		default:
			select {
			case reconfigure := <-s.reconfigureCh:
				<-limiter.C
				switch reconfigure.eventType {
				case reconfigureCmdReconfigureType:
					s.logger.WithField(common.TagReconfigureID, common.FmtReconfigureID(reconfigure.reconfigureID)).Infof("Reconfiguration command received from host connection.")
					if lastReconfigureID != reconfigure.reconfigureID {
						lastReconfigureID = reconfigure.reconfigureID
						s.reconfigureHandler()
					}
				case connClosedReconfigureType:
					s.logger.WithField(common.TagHostIP, common.FmtHostIP(reconfigure.reconfigureID)).Infof("Reconfiguration due to host connection closed.")
					s.reconfigureHandler()
				}
			case <-heartbeat.C:
				s.reconfigureHandler()
			}
		}
	}
}
