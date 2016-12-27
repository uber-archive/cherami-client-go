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

import "net/http"
import "github.com/stretchr/testify/mock"

import "github.com/uber/cherami-client-go/stream"

// MockWSConnector is a mock for WSConnector used for unit testing
type MockWSConnector struct {
	mock.Mock
}

func (_m *MockWSConnector) OpenPublisherStream(hostPort string, requestHeader http.Header) (stream.BInOpenPublisherStreamOutCall, error) {
	ret := _m.Called(hostPort, requestHeader)

	var r0 stream.BInOpenPublisherStreamOutCall
	if rf, ok := ret.Get(0).(func(string, http.Header) stream.BInOpenPublisherStreamOutCall); ok {
		r0 = rf(hostPort, requestHeader)
	} else {
		r0 = ret.Get(0).(stream.BInOpenPublisherStreamOutCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, http.Header) error); ok {
		r1 = rf(hostPort, requestHeader)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
func (_m *MockWSConnector) OpenConsumerStream(hostPort string, requestHeader http.Header) (stream.BOutOpenConsumerStreamOutCall, error) {
	ret := _m.Called(hostPort, requestHeader)

	var r0 stream.BOutOpenConsumerStreamOutCall
	if rf, ok := ret.Get(0).(func(string, http.Header) stream.BOutOpenConsumerStreamOutCall); ok {
		r0 = rf(hostPort, requestHeader)
	} else {
		r0 = ret.Get(0).(stream.BOutOpenConsumerStreamOutCall)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(string, http.Header) error); ok {
		r1 = rf(hostPort, requestHeader)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
