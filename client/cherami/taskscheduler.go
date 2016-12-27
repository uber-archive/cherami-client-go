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
	"encoding/json"
	"sync"
	"sync/atomic"
)

type (
	taskSchedulerImpl struct {
		client    Client
		publisher Publisher

		path                             string
		maxInflightMessagesPerConnection int

		initialized uint32
		mu          sync.Mutex
	}
)

// NewTaskScheduler creates a task scheduler
func NewTaskScheduler(client Client, request *CreateTaskSchedulerRequest) TaskScheduler {
	if client == nil || request == nil {
		return nil
	}
	return &taskSchedulerImpl{
		client:    client,
		publisher: nil,
		path:      request.Path,
		maxInflightMessagesPerConnection: request.MaxInflightMessagesPerConnection,
	}
}

// Open gets TaskScheduler for scheduling tasks
func (t *taskSchedulerImpl) Open() error {
	return t.getPublisher().Open()
}

// Close make sure resources are released
func (t *taskSchedulerImpl) Close() {
	if atomic.LoadUint32(&t.initialized) == 1 {
		t.publisher.Close()
	}
}

// ScheduleTask enqueues a task
func (t *taskSchedulerImpl) ScheduleTask(request *ScheduleTaskRequest) error {
	jsonValue, err := json.Marshal(request.TaskValue)
	if err != nil {
		return err
	}

	messageData, err := json.Marshal(&taskImpl{
		Type:      request.TaskType,
		ID:        request.TaskID,
		JSONValue: string(jsonValue),
		Context:   request.Context,
	})
	if err != nil {
		return err
	}

	receipt := t.getPublisher().Publish(&PublisherMessage{
		Data:  messageData,
		Delay: request.Delay,
	})
	return receipt.Error
}

func (t *taskSchedulerImpl) getPublisher() Publisher {
	if atomic.LoadUint32(&t.initialized) == 1 {
		return t.publisher
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.initialized == 0 {
		t.publisher = t.client.CreatePublisher(&CreatePublisherRequest{
			Path: t.path,
			MaxInflightMessagesPerConnection: t.maxInflightMessagesPerConnection,
		})
		atomic.StoreUint32(&t.initialized, 1)
	}

	return t.publisher
}
