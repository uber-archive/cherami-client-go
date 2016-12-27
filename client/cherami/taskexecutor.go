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
	"time"
)

type (
	taskExecutorImpl struct {
		client        Client
		consumer      Consumer
		concurrency   int
		taskFuncs     map[string]TaskFunc
		taskFuncsLock sync.RWMutex

		path         string
		cgName       string
		consumerName string
		prefetchSize int
		timeout      time.Duration

		waitGroup   *sync.WaitGroup
		killSignal  chan struct{}
		initialized uint32
		mu          sync.Mutex
	}
)

// NewTaskExecutor creates a task executor
func NewTaskExecutor(client Client, request *CreateTaskExecutorRequest) TaskExecutor {
	if client == nil || request == nil {
		return nil
	}
	return &taskExecutorImpl{
		client:      client,
		consumer:    nil,
		concurrency: request.Concurrency,
		taskFuncs:   make(map[string]TaskFunc),

		path:         request.Path,
		cgName:       request.ConsumerGroupName,
		consumerName: request.ConsumerName,
		prefetchSize: request.PrefetchCount,
		timeout:      request.Timeout,

		waitGroup:  &sync.WaitGroup{},
		killSignal: make(chan struct{}),
	}
}

// Register registers task handler with its *unique* task type
func (t *taskExecutorImpl) Register(taskType string, taskFunc TaskFunc) {
	t.taskFuncsLock.Lock()
	defer t.taskFuncsLock.Unlock()
	t.taskFuncs[taskType] = taskFunc
}

// Start starts dequeuing tasks and execute them
func (t *taskExecutorImpl) Start() error {
	taskCh := make(chan Delivery, t.prefetchSize)
	if _, err := t.getConsumer().Open(taskCh); err != nil {
		return err
	}

	// spin up workers to handle tasks
	for i := 0; i < t.concurrency; i++ {

		go func(workerID int) {
			defer t.waitGroup.Done()

		LOOP:
			for {
				select {

				// if asked to stop
				case <-t.killSignal:
					// fmt.Printf("[Worker %d] Killed\n", workerID)
					break LOOP

				// or get new task
				case taskDelivery := <-taskCh:
					// deserialize task data
					taskData := taskDelivery.GetMessage().GetPayload().GetData()
					task := &taskImpl{}
					if err := json.Unmarshal(taskData, task); err != nil {
						// fmt.Printf("[Worker %d] Failed to json unmarshal task: %v\n", workerID, err)
						taskDelivery.Nack()
						continue
					}

					// process
					if taskFunc, found := t.getTaskFunc(task.GetType()); found {
						if err := taskFunc(task); err != nil {
							// fmt.Printf("[Worker %d] Failed execute task %s: %v\n", workerID, task.GetID(), err)
							taskDelivery.Nack()
							continue
						}
					} else {
						// fmt.Printf("[Worker %d] Task %s type %s not registered\n", workerID, task.GetID(), task.GetType())
						taskDelivery.Nack()
						continue
					}

					// ack back to Cherami
					taskDelivery.Ack()
				}
			}
		}(i)

		t.waitGroup.Add(1)
	}

	return nil
}

// Stop stops dequeuing/exeuction of tasks
// There's no guarantee to drain scheduled tasks when Stop is invoked
func (t *taskExecutorImpl) Stop() {
	// signal task kill
	close(t.killSignal)

	// wait for worker to be done
	t.waitGroup.Wait()

	// close consumer
	if atomic.LoadUint32(&t.initialized) == 1 {
		t.consumer.Close()
	}
}

func (t *taskExecutorImpl) getConsumer() Consumer {
	if atomic.LoadUint32(&t.initialized) == 1 {
		return t.consumer
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.initialized == 0 {
		t.consumer = t.client.CreateConsumer(&CreateConsumerRequest{
			Path:              t.path,
			ConsumerGroupName: t.cgName,
			ConsumerName:      t.consumerName,
			PrefetchCount:     t.prefetchSize,
			Options: &ClientOptions{
				Timeout: t.timeout,
			},
		})
		atomic.StoreUint32(&t.initialized, 1)
	}

	return t.consumer
}

func (t *taskExecutorImpl) getTaskFunc(taskType string) (taskFunc TaskFunc, found bool) {
	t.taskFuncsLock.RLock()
	defer t.taskFuncsLock.RUnlock()
	taskFunc, found = t.taskFuncs[taskType]
	return
}
