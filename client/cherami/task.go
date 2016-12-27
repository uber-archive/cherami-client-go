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

import "encoding/json"

type (
	taskImpl struct {
		Type      string            `json:"type"`
		ID        string            `json:"id"`
		JSONValue string            `json:"value"`
		Context   map[string]string `json:"context"`
	}
)

// GetType returns the unique type name that can be used to identify cooresponding task handler
func (t *taskImpl) GetType() string {
	return t.Type
}

// GetID returns the unique identifier of this specific task
func (t *taskImpl) GetID() string {
	return t.ID
}

// GetValue deserializes task value into given struct that matches the type used to publish the task
func (t *taskImpl) GetValue(instance interface{}) error {
	return json.Unmarshal([]byte(t.JSONValue), instance)
}

// GetContext returns key value pairs context accosicated with the task when published
func (t *taskImpl) GetContext() map[string]string {
	return t.Context
}
