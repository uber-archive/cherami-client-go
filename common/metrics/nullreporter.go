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

import "time"

type (
	// NullReporter is a dummy reporter which implements the Reporter interface
	NullReporter struct {
		tags map[string]string
	}

	nullStopWatch struct {
		startTime time.Time
		elasped   time.Duration
	}
)

// NewNullReporter create an instance of Reporter which can be used emit metric to console
func NewNullReporter() Reporter {
	reporter := &NullReporter{
		tags: make(map[string]string),
	}

	return reporter
}

// InitMetrics is used to initialize the metrics map with the respective type
func (r *NullReporter) InitMetrics(metricMap map[MetricName]MetricType) {
	// This is a no-op for simple reporter as it is already have a static list of metric to work with
}

// GetChildReporter creates the child reporter for this parent reporter
func (r *NullReporter) GetChildReporter(tags map[string]string) Reporter {
	return r
}

// GetTags returns the tags for this reporter object
func (r *NullReporter) GetTags() map[string]string {
	return r.tags
}

// IncCounter reports Counter metric to M3
func (r *NullReporter) IncCounter(name string, tags map[string]string, delta int64) {
	// not implemented
}

// UpdateGauge reports Gauge type metric
func (r *NullReporter) UpdateGauge(name string, tags map[string]string, value int64) {
	// Not implemented
}

// Start is the implementation of the stop watch routine
func (w *nullStopWatch) Start() {
	w.startTime = time.Now()
}

// Stop is the implementation of the corresponding stop watch routine
func (w *nullStopWatch) Stop() time.Duration {
	w.elasped = time.Since(w.startTime)

	return w.elasped
}

// StartTimer returns a Stopwatch which when stopped will report the metric
func (r *NullReporter) StartTimer(name string, tags map[string]string) Stopwatch {
	w := &nullStopWatch{}
	w.Start()
	return w
}

// RecordTimer should be used for measuring latency when you cannot start the stop watch.
func (r *NullReporter) RecordTimer(name string, tags map[string]string, d time.Duration) {
	// Record the time as counter of time in milliseconds
	// not implemented
}
