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

package common

import "math"

const (
	// ClientVersion is current client library's version
	// It uses Semantic Versions MAJOR.MINOR.PATCH (https://blog.gopheracademy.com/advent-2015/semver/)
	// ClientVersion needs to be updated to reflect client library changes:
	// 1. MAJOR version when you make incompatible API changes,
	// 2. MINOR version when you add functionality in a backwards-compatible manner, and
	// 3. PATCH version when you make backwards-compatible bug fixes.
	ClientVersion = "0.1.0"
	// HeaderClientVersion is the name of thrift context header contains client version
	HeaderClientVersion = "client-version"
	// HeaderUserName is the name of thrift context header contains current user name
	HeaderUserName = "user-name"
	// HeaderHostName is the name of thrift context header contains current host name
	HeaderHostName = "host-name"

	// SequenceBegin refers to the beginning of an extent
	SequenceBegin = 0
	// SequenceEnd refers to the end of an extent
	SequenceEnd = math.MaxInt64

	// UUIDStringLength is the length of an UUID represented as a hex string
	UUIDStringLength = 36 // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

	// InputServiceName refers to the name of the cherami in service
	InputServiceName = "cherami-inputhost"
	// OutputServiceName refers to the name of the cherami out service
	OutputServiceName = "cherami-outputhost"
	// FrontendServiceName refers to the name of the cherami frontend service
	FrontendServiceName = "cherami-frontendhost"
	// FrontendStagingServiceName refers to the name of the cherami staging frontend service
	FrontendStagingServiceName = "cherami-frontendhost-staging"
	// ControllerServiceName refers to the name of the cherami controller service
	ControllerServiceName = "cherami-controllerhost"
	// StoreServiceName refers to the name of the cherami store service
	StoreServiceName = "cherami-storehost"

	// EndpointOpenPublisherStream is websocket endpoint name for OpenPublisherStream
	EndpointOpenPublisherStream = "open_publisher_stream"
	// EndpointOpenConsumerStream is websocket endpoint name for OpenConsumerStream
	EndpointOpenConsumerStream = "open_consumer_stream"
	// EndpointOpenAppendStream is websocket endpoint name for OpenAppendStream
	EndpointOpenAppendStream = "open_append_stream"
	// EndpointOpenReadStream is websocket endpoint name for OpenReadStream
	EndpointOpenReadStream = "open_read_stream"
	// EndpointOpenReplicationRemoteReadStream is websocket endpoint name for OpenReplicationRemoteReadStream
	EndpointOpenReplicationRemoteReadStream = "open_replication_remote_read_stream"
	// EndpointOpenReplicationReadStream is websocket endpoint name for OpenReplicationReadStream
	EndpointOpenReplicationReadStream = "open_replication_read_stream"
	// HTTPHandlerPattern is pattern format for http handler, eg "/endpoint"
	HTTPHandlerPattern = "/%s"
	// WSUrlFormat is url format for websocket, eg "ws://host:port/endpoint"
	WSUrlFormat = "ws://%s/%s"
)
