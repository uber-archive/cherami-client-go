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
	"testing"
	"time"

	"github.com/uber/cherami-client-go/common/metrics"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go"
)

type ClientSuite struct {
	*require.Assertions // override suite.Suite.Assertions with require.Assertions; this means that s.NotNil(nil) will stop the test, not merely log an error
	suite.Suite
}

func TestClientSuite(t *testing.T) {
	suite.Run(t, new(ClientSuite))
}

func (s *ClientSuite) SetupTest() {
	s.Assertions = require.New(s.T()) // Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
}

// TestClientNoOptions tests to make sure we setup a default logger
// and a null reporter even if the options are nil
func (s *ClientSuite) TestClientNoOptions() {
	ip, err := tchannel.ListenIP()
	s.Nil(err)
	listenIP := ip.String()

	// pass nil options
	cheramiClient, err := NewClient("cherami-client-test-nil", listenIP, 0, nil)
	s.Nil(err)

	cheramiImpl := cheramiClient.(*clientImpl)

	// Make sure the reporter is null reporter
	assert.IsType(s.T(), &metrics.NullReporter{}, cheramiImpl.options.MetricsReporter)

	// Make sure logger is not nil as well
	s.NotNil(cheramiImpl.options.Logger)
	cheramiClient.Close()
}

// TestClientOptionsOnlyTimeout tests to make sure we setup the logger
// and reporter even if the options has only the timeout set.
func (s *ClientSuite) TestClientOptionsOnlyTimeout() {
	ip, err := tchannel.ListenIP()
	s.Nil(err)
	listenIP := ip.String()

	// setup options with just the timeout set
	options := &ClientOptions{
		Timeout: 1 * time.Minute,
	}

	cheramiClient, err := NewClient("cherami-client-test-both", listenIP, 0, options)
	s.Nil(err)

	cheramiImpl := cheramiClient.(*clientImpl)

	// Make sure the reporter is null reporter
	assert.IsType(s.T(), &metrics.NullReporter{}, cheramiImpl.options.MetricsReporter)

	// Make sure logger is not nil as well
	s.NotNil(cheramiImpl.options.Logger)
	cheramiClient.Close()
}

// TestClientOptionsNoReporter tests to make sure we setup the null reporter
// even if the options has a logger set
func (s *ClientSuite) TestClientOptionsNoReporter() {
	ip, err := tchannel.ListenIP()
	s.Nil(err)
	listenIP := ip.String()

	// setup options with just the logger
	options := &ClientOptions{
		Timeout: 1 * time.Minute,
		Logger:  bark.NewLoggerFromLogrus(log.StandardLogger()),
	}

	cheramiClient, err := NewClient("cherami-client-test-reporter", listenIP, 0, options)
	s.Nil(err)

	cheramiImpl := cheramiClient.(*clientImpl)

	// Make sure the reporter is null reporter
	assert.IsType(s.T(), &metrics.NullReporter{}, cheramiImpl.options.MetricsReporter)
	cheramiClient.Close()
}

// TestClientOptionsNoLogger tests to make sure we setup the default logger even
// if the options is valid and has just the reporter set
func (s *ClientSuite) TestClientOptionsNoLogger() {
	ip, err := tchannel.ListenIP()
	s.Nil(err)
	listenIP := ip.String()

	// setup options with just the reporter
	options := &ClientOptions{
		Timeout:         1 * time.Minute,
		MetricsReporter: metrics.NewNullReporter(),
	}

	cheramiClient, err := NewClient("cherami-client-test-logger", listenIP, 0, options)
	s.Nil(err)

	cheramiImpl := cheramiClient.(*clientImpl)

	// Make sure logger is not nil as well
	s.NotNil(cheramiImpl.options.Logger)
	cheramiClient.Close()
}

// TestClientOptionsBypassAuthProvider tests the client with BypassAuthProvider
func (s *ClientSuite) TestClientOptionsBypassAuthProvider() {
	ip, err := tchannel.ListenIP()
	s.Nil(err)
	listenIP := ip.String()

	// setup options with just the AuthProvider
	options := &ClientOptions{
		AuthProvider: NewBypassAuthProvider(),
	}

	cheramiClient, err := NewClient("cherami-client-test-logger", listenIP, 0, options)
	s.Nil(err)

	cheramiImpl := cheramiClient.(*clientImpl)

	s.NotNil(cheramiImpl.options.AuthProvider)
	cheramiClient.Close()
}
