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
	"bytes"
	"crypto/md5"
	"hash/crc32"
	"strings"

	"github.com/uber/cherami-thrift/.generated/go/cherami"
)

type (
	deliveryImpl struct {
		message      *cherami.ConsumerMessage
		acknowledger acknowledger
	}

	// acknowledger can be used to Ack/Nack messages from a specific OutputHost
	acknowledger interface {
		GetAcknowledgerID() string
		Ack(ids []string) error
		Nack(ids []string) error
	}
)

const (
	deliveryTokenSplitter = "|"
)

func newDelivery(msg *cherami.ConsumerMessage, acknowledger acknowledger) Delivery {
	return &deliveryImpl{
		message:      msg,
		acknowledger: acknowledger,
	}
}

func (d *deliveryImpl) GetMessage() *cherami.ConsumerMessage {
	return d.message
}

// This token is parsed in consumer.newDeliveryID
// Make sure to keep both implementations in sync to serialize/deserialize these tokens
func (d *deliveryImpl) GetDeliveryToken() string {
	return strings.Join([]string{d.acknowledger.GetAcknowledgerID(), d.message.GetAckId()}, deliveryTokenSplitter)
}

func (d *deliveryImpl) Ack() error {
	return d.acknowledger.Ack([]string{d.message.GetAckId()})
}

func (d *deliveryImpl) Nack() error {
	return d.acknowledger.Nack([]string{d.message.GetAckId()})
}

func (d *deliveryImpl) VerifyChecksum() bool {
	if d.message.IsSetPayload() == false {
		return false
	}
	payload := d.message.GetPayload()

	if payload.IsSetCrc32IEEEDataChecksum() {
		gotChecksum := int64(crc32.ChecksumIEEE(payload.GetData()))
		return gotChecksum == payload.GetCrc32IEEEDataChecksum()
	} else if payload.IsSetMd5DataChecksum() {
		gotChecksum := md5.Sum(payload.GetData())
		return bytes.Equal(gotChecksum[:], payload.GetMd5DataChecksum())
	}

	// no known checksum provided, just pass
	return true
}
