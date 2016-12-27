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

import "github.com/stretchr/testify/mock"
import "github.com/uber/cherami-thrift/.generated/go/cherami"

type mockClient struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *mockClient) Close() {
	_m.Called()
}

// CreateConsumerGroup provides a mock function with given fields: request
func (_m *mockClient) CreateConsumerGroup(request *cherami.CreateConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error) {
	ret := _m.Called(request)

	var r0 *cherami.ConsumerGroupDescription
	if rf, ok := ret.Get(0).(func(*cherami.CreateConsumerGroupRequest) *cherami.ConsumerGroupDescription); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*cherami.ConsumerGroupDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*cherami.CreateConsumerGroupRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CreateDestination provides a mock function with given fields: request
func (_m *mockClient) CreateDestination(request *cherami.CreateDestinationRequest) (*cherami.DestinationDescription, error) {
	ret := _m.Called(request)

	var r0 *cherami.DestinationDescription
	if rf, ok := ret.Get(0).(func(*cherami.CreateDestinationRequest) *cherami.DestinationDescription); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*cherami.DestinationDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*cherami.CreateDestinationRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CreateConsumer provides a mock function with given fields: request
func (_m *mockClient) CreateConsumer(request *CreateConsumerRequest) Consumer {
	ret := _m.Called(request)

	var r0 Consumer
	if rf, ok := ret.Get(0).(func(*CreateConsumerRequest) Consumer); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Get(0).(Consumer)
	}

	return r0
}

// CreatePublisher provides a mock function with given fields: request
func (_m *mockClient) CreatePublisher(request *CreatePublisherRequest) Publisher {
	ret := _m.Called(request)

	var r0 Publisher
	if rf, ok := ret.Get(0).(func(*CreatePublisherRequest) Publisher); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Get(0).(Publisher)
	}

	return r0
}

// DeleteConsumerGroup provides a mock function with given fields: request
func (_m *mockClient) DeleteConsumerGroup(request *cherami.DeleteConsumerGroupRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*cherami.DeleteConsumerGroupRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// DeleteDestination provides a mock function with given fields: request
func (_m *mockClient) DeleteDestination(request *cherami.DeleteDestinationRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*cherami.DeleteDestinationRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ListConsumerGroups provides a mock function with given fields: request
func (_m *mockClient) ListConsumerGroups(request *cherami.ListConsumerGroupRequest) (*cherami.ListConsumerGroupResult_, error) {
	ret := _m.Called(request)

	var r0 *cherami.ListConsumerGroupResult_
	if rf, ok := ret.Get(0).(func(*cherami.ListConsumerGroupRequest) *cherami.ListConsumerGroupResult_); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*cherami.ListConsumerGroupResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*cherami.ListConsumerGroupRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListDestinations provides a mock function with given fields: request
func (_m *mockClient) ListDestinations(request *cherami.ListDestinationsRequest) (*cherami.ListDestinationsResult_, error) {
	ret := _m.Called(request)

	var r0 *cherami.ListDestinationsResult_
	if rf, ok := ret.Get(0).(func(*cherami.ListDestinationsRequest) *cherami.ListDestinationsResult_); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*cherami.ListDestinationsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*cherami.ListDestinationsRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadConsumerGroup provides a mock function with given fields: request
func (_m *mockClient) ReadConsumerGroup(request *cherami.ReadConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error) {
	ret := _m.Called(request)

	var r0 *cherami.ConsumerGroupDescription
	if rf, ok := ret.Get(0).(func(*cherami.ReadConsumerGroupRequest) *cherami.ConsumerGroupDescription); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*cherami.ConsumerGroupDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*cherami.ReadConsumerGroupRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadDestination provides a mock function with given fields: request
func (_m *mockClient) ReadDestination(request *cherami.ReadDestinationRequest) (*cherami.DestinationDescription, error) {
	ret := _m.Called(request)

	var r0 *cherami.DestinationDescription
	if rf, ok := ret.Get(0).(func(*cherami.ReadDestinationRequest) *cherami.DestinationDescription); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*cherami.DestinationDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*cherami.ReadDestinationRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateConsumerGroup provides a mock function with given fields: request
func (_m *mockClient) UpdateConsumerGroup(request *cherami.UpdateConsumerGroupRequest) (*cherami.ConsumerGroupDescription, error) {
	ret := _m.Called(request)

	var r0 *cherami.ConsumerGroupDescription
	if rf, ok := ret.Get(0).(func(*cherami.UpdateConsumerGroupRequest) *cherami.ConsumerGroupDescription); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*cherami.ConsumerGroupDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*cherami.UpdateConsumerGroupRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// UpdateDestination provides a mock function with given fields: request
func (_m *mockClient) UpdateDestination(request *cherami.UpdateDestinationRequest) (*cherami.DestinationDescription, error) {
	ret := _m.Called(request)

	var r0 *cherami.DestinationDescription
	if rf, ok := ret.Get(0).(func(*cherami.UpdateDestinationRequest) *cherami.DestinationDescription); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*cherami.DestinationDescription)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*cherami.UpdateDestinationRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetQueueDepthInfo provides a mock function with given fields: request
func (_m *mockClient) GetQueueDepthInfo(request *cherami.GetQueueDepthInfoRequest) (*cherami.GetQueueDepthInfoResult_, error) {
	ret := _m.Called(request)

	var r0 *cherami.GetQueueDepthInfoResult_
	if rf, ok := ret.Get(0).(func(*cherami.GetQueueDepthInfoRequest) *cherami.GetQueueDepthInfoResult_); ok {
		r0 = rf(request)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*cherami.GetQueueDepthInfoResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*cherami.GetQueueDepthInfoRequest) error); ok {
		r1 = rf(request)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MergeDLQForConsumerGroup provides a mock function with given fields: request
func (_m *mockClient) MergeDLQForConsumerGroup(request *cherami.MergeDLQForConsumerGroupRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*cherami.MergeDLQForConsumerGroupRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// PurgeDLQForConsumerGroup provides a mock function with given fields: request
func (_m *mockClient) PurgeDLQForConsumerGroup(request *cherami.PurgeDLQForConsumerGroupRequest) error {
	ret := _m.Called(request)

	var r0 error
	if rf, ok := ret.Get(0).(func(*cherami.PurgeDLQForConsumerGroupRequest) error); ok {
		r0 = rf(request)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// ReadPublisherOptions provides a mock function with given fields: path
func (_m *mockClient) ReadPublisherOptions(path string) (*cherami.ReadPublisherOptionsResult_, error) {
	ret := _m.Called(path)

	var r0 *cherami.ReadPublisherOptionsResult_
	if rf, ok := ret.Get(0).(func(string) *cherami.ReadPublisherOptionsResult_); ok {
		r0 = rf(path)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*cherami.ReadPublisherOptionsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(path)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ReadConsumerGroupHosts provides a mock function with given fields: path, consumerGroupName
func (_m *mockClient) ReadConsumerGroupHosts(path string, consumerGroupName string) (*cherami.ReadConsumerGroupHostsResult_, error) {
	ret := _m.Called(path, consumerGroupName)

	var r0 *cherami.ReadConsumerGroupHostsResult_
	if rf, ok := ret.Get(0).(func(string, string) *cherami.ReadConsumerGroupHostsResult_); ok {
		r0 = rf(path, consumerGroupName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*cherami.ReadConsumerGroupHostsResult_)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, string) error); ok {
		r1 = rf(path, consumerGroupName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
