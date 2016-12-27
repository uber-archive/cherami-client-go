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

import "github.com/uber/cherami-thrift/.generated/go/cherami"

// IntPtr makes a copy and returns the pointer to an int.
func IntPtr(v int) *int {
	return &v
}

// Int32Ptr makes a copy and returns the pointer to an int32.
func Int32Ptr(v int32) *int32 {
	return &v
}

// Int64Ptr makes a copy and returns the pointer to an int64.
func Int64Ptr(v int64) *int64 {
	return &v
}

// Uint32Ptr makes a copy and returns the pointer to a uint32.
func Uint32Ptr(v uint32) *uint32 {
	return &v
}

// Uint64Ptr makes a copy and returns the pointer to a uint64.
func Uint64Ptr(v uint64) *uint64 {
	return &v
}

// Float64Ptr makes a copy and returns the pointer to an int64.
func Float64Ptr(v float64) *float64 {
	return &v
}

// BoolPtr makes a copy and returns the pointer to a bool.
func BoolPtr(v bool) *bool {
	return &v
}

// StringPtr makes a copy and returns the pointer to a string.
func StringPtr(v string) *string {
	return &v
}

// CheramiStatusPtr makes a copy and returns the pointer to a CheramiStatus.
func CheramiStatusPtr(status cherami.Status) *cherami.Status {
	return &status
}

// CheramiInputHostCommandTypePtr makes a copy and returns the pointer to a
// CheramiInputHostCommandType.
func CheramiInputHostCommandTypePtr(cmdType cherami.InputHostCommandType) *cherami.InputHostCommandType {
	return &cmdType
}

// CheramiOutputHostCommandTypePtr makes a copy and returns the pointer to a
// CheramiOutputHostCommandType.
func CheramiOutputHostCommandTypePtr(cmdType cherami.OutputHostCommandType) *cherami.OutputHostCommandType {
	return &cmdType
}

// CheramiDestinationTypePtr makes a copy and returns the pointer to a
// CheramiDestinationType.
func CheramiDestinationTypePtr(destType cherami.DestinationType) *cherami.DestinationType {
	return &destType
}

// CheramiDestinationStatusPtr makes a copy and returns the pointer to a
// CheramiDestinationStatus.
func CheramiDestinationStatusPtr(status cherami.DestinationStatus) *cherami.DestinationStatus {
	return &status
}

// CheramiConsumerGroupStatusPtr makes a copy and returns the pointer to a
// CheramiConsumerGroupStatus.
func CheramiConsumerGroupStatusPtr(status cherami.ConsumerGroupStatus) *cherami.ConsumerGroupStatus {
	return &status
}
