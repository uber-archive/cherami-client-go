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
	"time"

	"github.com/uber/cherami-client-go/common/metrics"
	"github.com/uber/cherami-thrift/.generated/go/cherami"

	"github.com/uber-common/bark"
)

type (
	// Client exposes API for destination and consumer group CRUD and capability to publish and consume messages
	Client interface {
		Close()
		CreateConsumerGroup(request *cherami.CreateConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error)
		CreateDestination(request *cherami.CreateDestinationRequest) (*cherami.DestinationDescription, error)
		CreateConsumer(request *CreateConsumerRequest) Consumer
		CreatePublisher(request *CreatePublisherRequest) Publisher
		DeleteConsumerGroup(request *cherami.DeleteConsumerGroupRequest) error
		DeleteDestination(request *cherami.DeleteDestinationRequest) error
		ListConsumerGroups(request *cherami.ListConsumerGroupRequest) (*cherami.ListConsumerGroupResult_, error)
		ListDestinations(request *cherami.ListDestinationsRequest) (*cherami.ListDestinationsResult_, error)
		ReadConsumerGroup(request *cherami.ReadConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error)
		ReadDestination(request *cherami.ReadDestinationRequest) (*cherami.DestinationDescription, error)
		UpdateConsumerGroup(request *cherami.UpdateConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error)
		UpdateDestination(request *cherami.UpdateDestinationRequest) (*cherami.DestinationDescription, error)
		GetQueueDepthInfo(request *cherami.GetQueueDepthInfoRequest) (*cherami.GetQueueDepthInfoResult_, error)
		MergeDLQForConsumerGroup(request *cherami.MergeDLQForConsumerGroupRequest) error
		PurgeDLQForConsumerGroup(request *cherami.PurgeDLQForConsumerGroupRequest) error
		ReadPublisherOptions(path string) (*cherami.ReadPublisherOptionsResult_, error)
		ReadConsumerGroupHosts(path string, consumerGroupName string) (*cherami.ReadConsumerGroupHostsResult_, error)
	}

	// Publisher is used by an application to publish messages to Cherami service
	Publisher interface {
		Open() error
		Close()
		Publish(message *PublisherMessage) *PublisherReceipt
		PublishAsync(message *PublisherMessage, done chan<- *PublisherReceipt) (string, error)
	}

	// PublisherMessage is a struct that wraps the message payload and a delay time duration.
	PublisherMessage struct {
		Data  []byte
		Delay time.Duration
		// UserContext is user specified context to pass through
		UserContext map[string]string
	}

	// PublisherReceipt is an token for publisher as the prove of message being
	// durably stored.
	PublisherReceipt struct {
		// ID is the message id passed with message when published
		ID string
		// Receipt is a token that contains info where the message is stored
		Receipt string
		// Error is the error if any that associates with the publishing of this message
		Error error
		// UserContext is user specified context to pass through
		UserContext map[string]string
	}

	// Consumer is used by an application to receive messages from Cherami service
	Consumer interface {
		// Open will connect to Cherami nodes and start delivering messages to
		// a provided Delivery channel for registered consumer group.
		//
		// It is ADVISED that deliveryCh's buffer size should be bigger than the
		// total PrefetchCount in CreateConsumerRequest of the consumers writing
		// to this channel.
		Open(deliveryCh chan Delivery) (chan Delivery, error)
		// Closed all the connections to Cherami nodes for this consumer
		Close()
		// AckDelivery can be used by application to Ack a message so it is not delivered to any other consumer
		AckDelivery(deliveryToken string) error
		// NackDelivery can be used by application to Nack a message so it can be delivered to another consumer immediately
		// without waiting for the timeout to expire
		NackDelivery(deliveryToken string) error
	}

	// Delivery is the container which has the actual message returned by Cherami
	Delivery interface {
		// Returns the message returned by Cherami
		GetMessage() *cherami.ConsumerMessage
		// Returns a delivery token which can be used to Ack/Nack delivery using the Consumer API
		// Consumer has 2 options to Ack/Nack a delivery:
		// 1) Simply call the Ack/Nack API on the delivery after processing the message
		// 2) If the consumer wants to forward the message to downstream component for processing then they can get the
		// DeliveryToken by calling this function and pass it along.  Later the downstream component can call the
		// API on the Consumer with this token to Ack/Nack the message.
		GetDeliveryToken() string
		// Acks this delivery
		Ack() error
		// Nacks this delivery
		Nack() error
		// VerifyChecksum verifies checksum of the message if exist
		// Consumer needs to perform this verification and decide what to do based on returned result
		VerifyChecksum() bool
	}

	// CreatePublisherRequest struct used to call Client.CreatePublisher to create an object used by application to publish messages
	CreatePublisherRequest struct {
		Path                             string
		MaxInflightMessagesPerConnection int
		// PublisherType represents the mode in which
		// publishing should be done i.e. either through
		// websocket streaming or through tchannel batch API
		// Defaults to websocket streaming. Choose non-streaming
		// batch API for low throughput publishing.
		PublisherType PublisherType
	}

	// CreateConsumerRequest struct is used to call Client.CreateConsumer to create an object used by application to
	// consume messages
	CreateConsumerRequest struct {
		// Path to destination consumer wants to consume messages from
		Path string
		// ConsumerGroupName registered with Cherami for a particular destination
		ConsumerGroupName string
		// Name of consumer (worker) connecting to Cherami
		ConsumerName string
		// Number of messages to buffer locally.  Clients which process messages very fast may want to specify larger value
		// for PrefetchCount for faster throughput.  On the flip side larger values for PrefetchCount will result in
		// more messages being buffered locally causing high memory foot print
		PrefetchCount int
		// Options used for making API calls to Cherami services
		// This option is now deprecated. If you need to specify any option, you can specify it when you call NewClient()
		Options *ClientOptions
	}

	// PublisherType represents the type of publisher viz. streaming/non-streaming
	PublisherType int

	// ClientOptions used by Cherami client
	ClientOptions struct {
		Timeout time.Duration
		// DeploymentStr specifies which deployment(staging,prod,dev,etc) the client should connect to
		// If the string is empty, client will connect to prod
		// If the string is 'prod', client will connect to prod
		// If the string is 'staging' or 'staging2', client will connect to staging or staging2
		// If the string is 'dev', client will connect to dev server
		DeploymentStr string
		// MetricsReporter is the reporter object
		MetricsReporter metrics.Reporter
		// Logger is the logger object
		Logger bark.Logger
		// Interval for polling input/output host updates. Normally client doesn't need to explicitly set this option
		// because the default setting should work fine. This is only useful in testing or other edge scenarios
		ReconfigurationPollingInterval time.Duration
		// AuthProvider provides the authentication information in client side
		AuthProvider AuthProvider
	}

	// Task represents the task queued in Cherami
	Task interface {
		// GetType returns the unique type name that can be used to identify cooresponding task handler
		GetType() string
		// GetID returns the unique identifier of this specific task
		GetID() string
		// GetValue deserializes task value into given struct that matches the type used to publish the task
		GetValue(instance interface{}) error
		// GetContext returns key value pairs context accosicated with the task when published
		GetContext() map[string]string
	}

	// TaskFunc is function signature of task handler
	TaskFunc func(task Task) error

	// TaskScheduler is used to put tasks into Cherami
	TaskScheduler interface {
		// Open gets TaskScheduler for scheduling tasks
		Open() error
		// Close make sure resources are released
		Close()
		// ScheduleTask enqueues a task
		ScheduleTask(request *ScheduleTaskRequest) error
	}

	// TaskExecutor is used to pull tasks from Cherami and execute their task handlers accordingly
	TaskExecutor interface {
		// Register registers task handler with its *unique* task type
		Register(taskType string, taskFunc TaskFunc)
		// Start starts dequeuing tasks and execute them
		Start() error
		// Stop stops dequeuing/exeuction of tasks
		// There's no guarantee to drain scheduled tasks when Stop is invoked
		Stop()
	}

	// CreateTaskSchedulerRequest is used to call Client.CreateTaskScheduler to create a task scheduler
	CreateTaskSchedulerRequest struct {
		// Path to destination which tasks enqueue into
		Path string
		// MaxInflightMessagesPerConnection is number of messages pending confirmation per connection
		MaxInflightMessagesPerConnection int
	}

	// CreateTaskExecutorRequest is used to call Client.CreateTaskExecutor to create a task executor
	CreateTaskExecutorRequest struct {
		// Concurrency is the number of concurrent workers to execute tasks
		Concurrency int
		// Path to destination which tasks dequeued from
		Path string
		// ConsumerGroupName registered with Cherami for a particular destination
		ConsumerGroupName string
		// ConsumerName is name of consumer (worker) connecting to Cherami
		ConsumerName string
		// PrefetchCount is number of messages to buffer locally
		PrefetchCount int
		// Timeout is timeout setting used when ack/nack back to Cherami
		Timeout time.Duration
	}

	// ScheduleTaskRequest is used to call TaskScheduler.ScheduleTask to schedule a new task
	ScheduleTaskRequest struct {
		// TaskType is the unique type name which is used to register task handler with task executor
		TaskType string
		// TaskID is the unique identifier of this specific task
		TaskID string
		// TaskValue can be anything represent the task
		TaskValue interface{}
		// Context is key value pairs context accosicated with the task
		Context map[string]string
		// Delay is the time duration before task can be executed
		Delay time.Duration
	}
)

const (
	// PublisherTypeStreaming indicates a publisher that uses websocket streaming
	PublisherTypeStreaming PublisherType = iota
	// PublisherTypeNonStreaming indicates a publisher that uses tchannel batch api
	PublisherTypeNonStreaming
)
