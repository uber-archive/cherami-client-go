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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/uber/cherami-client-go/common"
	"github.com/uber/cherami-client-go/common/backoff"
	"github.com/uber/cherami-client-go/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/cherami"

	log "github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"

	"golang.org/x/net/context"
)

type (
	clientImpl struct {
		connection  *tchannel.Channel
		client      cherami.TChanBFrontend
		options     *ClientOptions
		retryPolicy backoff.RetryPolicy

		sync.Mutex
		hostPort string
	}
)

const (
	clientRetryInterval                   = 50 * time.Millisecond
	clientRetryMaxInterval                = 10 * time.Second
	clientRetryExpirationInterval         = 1 * time.Minute
	defaultReconfigurationPollingInterval = 10 * time.Second
)

var envUserName = os.Getenv("USER")
var envHostName, _ = os.Hostname()

// NewClient returns the singleton Cherami client used for communicating with the service at given port
func NewClient(serviceName string, host string, port int, options *ClientOptions) (Client, error) {
	ch, err := tchannel.NewChannel(serviceName, nil)
	if err != nil {
		return nil, err
	}

	ch.Peers().Add(fmt.Sprintf("%s:%d", host, port))
	return newClientWithTChannel(ch, options)
}

// NewHyperbahnClient returns the singleton Cherami client used for communicating with the service via Hyperbahn or
// Muttley. Streaming methods (for LOG/Consistent destinations) will not work.
func NewHyperbahnClient(serviceName string, bootstrapFile string, options *ClientOptions) (Client, error) {
	ch, err := tchannel.NewChannel(serviceName, nil)
	if err != nil {
		return nil, err
	}

	common.CreateHyperbahnClient(ch, bootstrapFile)
	return newClientWithTChannel(ch, options)
}

// NewClientWithFEClient is used by Frontend to create a Cherami client for itself.
// It is used by non-streaming publish/consume APIs.
// ** Internal Cherami Use Only **
func NewClientWithFEClient(feClient cherami.TChanBFrontend, options *ClientOptions) (Client, error) {
	options, err := verifyOptions(options)
	if err != nil {
		return nil, err
	}

	return &clientImpl{
		client:      feClient,
		options:     options,
		retryPolicy: createDefaultRetryPolicy(),
	}, nil
}

func newClientWithTChannel(ch *tchannel.Channel, options *ClientOptions) (Client, error) {
	options, err := verifyOptions(options)
	if err != nil {
		return nil, err
	}

	tClient := thrift.NewClient(ch, getFrontEndServiceName(options.DeploymentStr), nil)

	client := &clientImpl{
		connection:  ch,
		client:      cherami.NewTChanBFrontendClient(tClient),
		options:     options,
		retryPolicy: createDefaultRetryPolicy(),
	}
	return client, nil
}

func (c *clientImpl) createStreamingClient() (cherami.TChanBFrontend, error) {
	// create a streaming client directly connecting to the frontend
	c.Lock()
	defer c.Unlock()
	// if hostPort is not known (e.g. hyperbahn client), need to query the
	// frontened to find its IP
	if c.hostPort == "" {
		ctx, cancel := c.createContext()
		defer cancel()
		hostport, err := c.client.HostPort(ctx)
		if err != nil {
			return nil, err
		}

		c.hostPort = hostport
	}

	ch, err := tchannel.NewChannel(uuid.New(), nil)
	if err != nil {
		return nil, err
	}

	tClient := thrift.NewClient(ch, getFrontEndServiceName(c.options.DeploymentStr), &thrift.ClientOptions{
		HostPort: c.hostPort,
	})

	streamingClient := cherami.NewTChanBFrontendClient(tClient)

	return streamingClient, nil
}

// Close shuts down the connection to Cherami frontend
func (c *clientImpl) Close() {
	c.Lock()
	defer c.Unlock()

	if c.connection != nil {
		c.connection.Close()
	}
}

func (c *clientImpl) CreateDestination(request *cherami.CreateDestinationRequest) (*cherami.DestinationDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.CreateDestination(ctx, request)
}

func (c *clientImpl) ReadDestination(request *cherami.ReadDestinationRequest) (*cherami.DestinationDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ReadDestination(ctx, request)
}

func (c *clientImpl) UpdateDestination(request *cherami.UpdateDestinationRequest) (*cherami.DestinationDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.UpdateDestination(ctx, request)
}

func (c *clientImpl) DeleteDestination(request *cherami.DeleteDestinationRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.DeleteDestination(ctx, request)
}

func (c *clientImpl) ListDestinations(request *cherami.ListDestinationsRequest) (*cherami.ListDestinationsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ListDestinations(ctx, request)
}

func (c *clientImpl) CreateConsumerGroup(request *cherami.CreateConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.CreateConsumerGroup(ctx, request)
}

func (c *clientImpl) ReadConsumerGroup(request *cherami.ReadConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ReadConsumerGroup(ctx, request)
}

func (c *clientImpl) UpdateConsumerGroup(request *cherami.UpdateConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.UpdateConsumerGroup(ctx, request)
}

func (c *clientImpl) MergeDLQForConsumerGroup(request *cherami.MergeDLQForConsumerGroupRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.MergeDLQForConsumerGroup(ctx, request)
}

func (c *clientImpl) PurgeDLQForConsumerGroup(request *cherami.PurgeDLQForConsumerGroupRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.PurgeDLQForConsumerGroup(ctx, request)
}

func (c *clientImpl) DeleteConsumerGroup(request *cherami.DeleteConsumerGroupRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.DeleteConsumerGroup(ctx, request)
}

func (c *clientImpl) ListConsumerGroups(request *cherami.ListConsumerGroupRequest) (*cherami.ListConsumerGroupResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.ListConsumerGroups(ctx, request)
}

func (c *clientImpl) GetQueueDepthInfo(request *cherami.GetQueueDepthInfoRequest) (*cherami.GetQueueDepthInfoResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	return c.client.GetQueueDepthInfo(ctx, request)
}

func (c *clientImpl) CreatePublisher(request *CreatePublisherRequest) Publisher {
	reporter := c.options.MetricsReporter
	if reporter != nil {
		childReporter := reporter.GetChildReporter(map[string]string{
			metrics.DestinationTag: getMetricTagValueForPath(request.Path),
			metrics.PublisherTypeTag: fmt.Sprintf("%v", request.PublisherType),
		})
		if childReporter != nil {
			reporter = childReporter
		}
	}

	switch request.PublisherType {
	case PublisherTypeStreaming:
		return NewPublisher2(c, request.Path, request.MaxInflightMessagesPerConnection, reporter)
	case PublisherTypeNonStreaming:
		return newTChannelBatchPublisher(c, c.connection, request.Path, c.options.Logger, reporter, c.options.ReconfigurationPollingInterval)
	}
	return nil
}

func (c *clientImpl) CreateConsumer(request *CreateConsumerRequest) Consumer {
	if request.Options != nil {
		c.options.Logger.Warn(`CreateConsumerRequest.Options is deprecated. Client options should be set when creating the client`)

		var err error
		if request.Options, err = verifyOptions(request.Options); err != nil {
			panic(fmt.Sprintf(`Client option is invalid (and CreateConsumerRequest.Options is deprecated). Error: %v`, err))
		}
	} else {
		request.Options = c.options
	}

	reporter := c.options.MetricsReporter
	if reporter != nil {
		childReporter := reporter.GetChildReporter(map[string]string{
			metrics.DestinationTag: getMetricTagValueForPath(request.Path),
			metrics.ConsumerGroupTag: getMetricTagValueForPath(request.ConsumerGroupName),
		})
		if childReporter != nil {
			reporter = childReporter
		}
	}

	return newConsumer(c, request.Path, request.ConsumerGroupName, request.ConsumerName, request.PrefetchCount, request.Options, reporter)
}

func (c *clientImpl) createContext() (thrift.Context, context.CancelFunc) {
	ctx, cancel := thrift.NewContext(c.options.Timeout)
	ctx = thrift.WithHeaders(ctx, map[string]string{
		common.HeaderClientVersion: common.ClientVersion,
		common.HeaderUserName:      envUserName,
		common.HeaderHostName:      envHostName,
	})

	if c.options.AuthProvider != nil {
		var err error
		ctx, err = c.options.AuthProvider.CreateSecurityContext(ctx)
		if err != nil {
			c.options.Logger.WithField(common.TagErr, err).Warn("Failed to create security context in client")
		}
	}

	return ctx, cancel
}

func (c *clientImpl) ReadPublisherOptions(path string) (*cherami.ReadPublisherOptionsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	var result *cherami.ReadPublisherOptionsResult_
	readOp := func() error {
		var e error
		request := &cherami.ReadPublisherOptionsRequest{
			Path: common.StringPtr(path),
		}

		result, e = c.client.ReadPublisherOptions(ctx, request)

		return e
	}

	err := backoff.Retry(readOp, c.retryPolicy, isTransientError)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (c *clientImpl) ReadConsumerGroupHosts(path string, consumerGroupName string) (*cherami.ReadConsumerGroupHostsResult_, error) {
	ctx, cancel := c.createContext()
	defer cancel()

	var result *cherami.ReadConsumerGroupHostsResult_
	readOp := func() error {
		var e error
		request := &cherami.ReadConsumerGroupHostsRequest{
			DestinationPath:   common.StringPtr(path),
			ConsumerGroupName: common.StringPtr(consumerGroupName),
		}

		result, e = c.client.ReadConsumerGroupHosts(ctx, request)

		return e
	}

	err := backoff.Retry(readOp, c.retryPolicy, isTransientError)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func getFrontEndServiceName(deploymentStr string) string {
	if len(deploymentStr) == 0 || strings.HasPrefix(strings.ToLower(deploymentStr), `prod`) || strings.HasPrefix(strings.ToLower(deploymentStr), `dev`) {
		return common.FrontendServiceName
	}
	return fmt.Sprintf("%v_%v", common.FrontendServiceName, deploymentStr)
}

func getDefaultLogger() bark.Logger {
	return bark.NewLoggerFromLogrus(log.StandardLogger())
}

func getDefaultOptions() *ClientOptions {
	return &ClientOptions{
		Timeout:         		time.Minute,
		Logger:          		getDefaultLogger(),
		MetricsReporter: 		metrics.NewNullReporter(),
		ReconfigurationPollingInterval: defaultReconfigurationPollingInterval,
	}
}

// verifyOptions is used to verify if we have a metrics reporter and
// a logger. If not, just setup a default logger and a null reporter
// it also validate the timeout is sane
func verifyOptions(opts *ClientOptions) (*ClientOptions, error){
	if opts == nil {
		opts = getDefaultOptions()
	}
	if opts.Timeout.Nanoseconds() == 0 {
		opts.Timeout = getDefaultOptions().Timeout
	}
	if err := common.ValidateTimeout(opts.Timeout); err != nil {
		return nil, err
	}

	if opts.Logger == nil {
		opts.Logger = getDefaultLogger()
	}

	if opts.MetricsReporter == nil {
		opts.MetricsReporter = metrics.NewNullReporter()
	}
	// Now make sure we init the default metrics as well
	opts.MetricsReporter.InitMetrics(metrics.MetricDefs)

	if int64(opts.ReconfigurationPollingInterval/time.Second) == 0 {
		opts.ReconfigurationPollingInterval = defaultReconfigurationPollingInterval
	}

	return opts, nil
}

func createDefaultRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(clientRetryInterval)
	policy.SetMaximumInterval(clientRetryMaxInterval)
	policy.SetExpirationInterval(clientRetryExpirationInterval)

	return policy
}

func isTransientError(err error) bool {
	// Only EntityNotExistsError/EntityDisabledError from Cherami is treated as non-transient error today
	switch err.(type) {
	case *cherami.EntityNotExistsError:
		return false
	case *cherami.EntityDisabledError:
		return false
	default:
		return true
	}
}

func getMetricTagValueForPath(path string) string {
	return strings.Replace(path, "/", "_", -1)
}