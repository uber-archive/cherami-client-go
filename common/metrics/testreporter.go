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

package metrics

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type (
	// TestReporter is the reporter used to analyze metrics for unit tests
	TestReporter struct {
		tags map[string]string
	}

	testStopWatch struct {
		metricName string
		reporter   *TestReporter
		startTime  time.Time
		elapsed    time.Duration
	}
)

type HandlerFn func(metricName string, baseTags, tags map[string]string, value int64)

var handlers = make(map[string]map[string]HandlerFn) // Key1 - metricName; Key2 - "filterTag:filterVal"
var handlerMutex sync.RWMutex

// NewTestReporter create an instance of Reporter which can be used for driver to emit metric to console
func NewTestReporter(tags map[string]string) Reporter {
	reporter := &TestReporter{
		tags: mapCopy(tags),
	}

	return reporter
}

// InitMetrics is used to initialize the metrics map with the respective type
func (r *TestReporter) InitMetrics(metricMap map[MetricName]MetricType) {
	// This is a no-op for test reporter as it is already have a static list of metric to work with
}

// GetChildReporter creates the child reporter for this parent reporter
func (r *TestReporter) GetChildReporter(tags map[string]string) Reporter {
	if len(r.tags) != 0 {
		panic(r.tags)
	}

	sr := &TestReporter{
		tags: mapCopy(tags),
	}

	return sr
}

// GetTags returns the tags for this reporter object
func (r *TestReporter) GetTags() map[string]string {
	return r.tags
}

// IncCounter reports Counter metric to M3
func (r *TestReporter) IncCounter(name string, tags map[string]string, delta int64) {
	r.executeHandler(name, tags, delta)
}

// UpdateGauge reports Gauge type metric
func (r *TestReporter) UpdateGauge(name string, tags map[string]string, value int64) {
	r.executeHandler(name, tags, value)
}

func (r *TestReporter) executeHandler(name string, tags map[string]string, value int64) {
	handlerMutex.RLock()
	_, ok0 := handlers[``]
	_, ok1 := handlers[name]
	if ok0 || ok1 {
		if allHandler2, ok2 := handlers[``][``]; ok2 { // Global handler
			allHandler2(name, r.tags, tags, value)
		}
		if allHandler3, ok3 := handlers[name][``]; ok3 { // Handler for all metrics named 'name'
			allHandler3(name, r.tags, tags, value)
		}

		// TODO: technically, this is wrong, as we don't have the local tags overriding the struct tags, but this
		// has no practical effect in our current use of metrics, since we never override
		for _, q := range []map[string]string{r.tags, tags} {
			for filterTag, filterTagVal := range q {
				key2 := filterTag + `:` + filterTagVal
				if handler4, ok4 := handlers[``][key2]; ok4 { // Handler for this tag, any name
					handler4(name, r.tags, tags, value)
				}
				if handler5, ok5 := handlers[name][key2]; ok5 { // Handler for specifically this name and tag
					handler5(name, r.tags, tags, value)
				}
			}
		}
	}
	handlerMutex.RUnlock()
}

// Register a handler (closure) that receives updates for a particular guage or counter based on the metric name and
// the name/value of one of the metric's tags. If the filterTag/Val are both empty, all updates to that metric will
// trigger the handler. If metricName is empty, all metrics matching the tag filter will pass through your function.
// A nil handler unregisters the handler for the given filter parameters
//
// Dev notes:
// * It is advisible to defer a call to unregister your handler when your test ends
// * Your handler can be called concurrently. Capture your own sync.Mutex if you must serialize
// * Counters report the delta; you must maintain the cumulative value of your counter if it is important
// * Your handler executes synchronously with the metrics code; DO NOT BLOCK
func RegisterHandler(metricName, filterTag, filterTagVal string, handler HandlerFn) {
	defer handlerMutex.Unlock()
	handlerMutex.Lock()
	if _, ok := handlers[metricName]; !ok {
		handlers[metricName] = make(map[string]HandlerFn)
	}

	key2 := filterTag + `:` + filterTagVal
	if key2 == `:` {
		key2 = ``
	}

	if handler == nil {
		delete(handlers[metricName], key2)
		if len(handlers[metricName]) == 0 {
			delete(handlers, metricName)
		}
		return
	}

	if hf, ok2 := handlers[metricName][key2]; ok2 {
		panic(fmt.Sprintf("Metrics handler %v (for '%s'/'%s') should have been unregistered", hf, metricName, key2))
	}

	handlers[metricName][key2] = handler
}

func newTestStopWatch(metricName string, reporter *TestReporter) *testStopWatch {
	watch := &testStopWatch{
		metricName: metricName,
		reporter:   reporter,
	}

	return watch
}

func (w *testStopWatch) Start() {
	w.startTime = time.Now()
}

func (w *testStopWatch) Stop() time.Duration {
	w.elapsed = time.Since(w.startTime)
	w.reporter.executeHandler(w.metricName, w.reporter.GetTags(), int64(w.elapsed))
	return w.elapsed
}

// StartTimer returns a Stopwatch which when stopped will report the metric to M3
func (r *TestReporter) StartTimer(name string, tags map[string]string) Stopwatch {
	w := newTestStopWatch(name, r)
	w.Start()
	return w
}

// RecordTimer should be used for measuring latency when you cannot start the stop watch.
func (r *TestReporter) RecordTimer(name string, tags map[string]string, d time.Duration) {
	r.executeHandler(name, tags, int64(d))
}

func mapCopy(s map[string]string) (d map[string]string) {
	d = make(map[string]string, len(s))
	for a, b := range s {
		d[a] = b
	}
	return
}

// SummingHandler returns a HandlerFn that atomically stores the sum and count of successive calls into the provided
// atomic variables
// NOTE: there are two race-free way to reset the counts together:
// 1) unregister the handler, then zero the counts, and re-register; may lose events
// 2) register a new handler with a second set of counting variables
func SummingHandler(atomicSum, atomicCount *int64) HandlerFn {
	return func(metricName string, baseTags, tags map[string]string, value int64) {
		atomic.AddInt64(atomicSum, value)
		if atomicCount != nil {
			atomic.AddInt64(atomicCount, 1)
		}
	}
}

// KeepLastValueHandler returns a HandlerFn that atomically stores the last value and count of successive calls into
// the provided atomic variables
func KeepLastValueHandler(atomicLastValue, atomicCount *int64) HandlerFn {
	return func(metricName string, baseTags, tags map[string]string, value int64) {
		atomic.StoreInt64(atomicLastValue, value)
		if atomicCount != nil {
			atomic.AddInt64(atomicCount, 1)
		}
	}
}
