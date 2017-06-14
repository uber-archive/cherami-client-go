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

package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/uber/cherami-client-go/client/cherami"
	cthrift "github.com/uber/cherami-thrift/.generated/go/cherami"
)

var host = flag.String("host", "127.0.0.1", "cherami-frontend host IP")
var port = flag.Int("port", 4922, "cherami-frontend port")

// helper function to print out a thrift object in json
func jsonify(obj thrift.TStruct) string {
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTSimpleJSONProtocol(transport)
	obj.Write(protocol)
	protocol.Flush()
	transport.Flush()
	return transport.String()
}

func exitIfError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		debug.PrintStack()
		os.Exit(1)
	}
}

func main() {
	flag.Parse()

	// First, create the client to interact with Cherami
	// Here we directly connect to cherami running on host:port
	cClient, err := cherami.NewClient("cherami-example", *host, *port, &cherami.ClientOptions{
		Timeout:      time.Minute,
		AuthProvider: cherami.NewBypassAuthProvider(),
	})
	exitIfError(err)

	// Now, create a destination with timestamp to avoid collision.
	path := fmt.Sprintf("/test/test_%d", time.Now().UnixNano())
	dType := cthrift.DestinationType_PLAIN
	consumedMessagesRetention := int32(3600)
	unconsumedMessagesRetention := int32(7200)
	ownerEmail := "cherami-client-example@cherami"

	desc, err := cClient.CreateDestination(&cthrift.CreateDestinationRequest{
		Path: &path,
		Type: &dType,
		ConsumedMessagesRetention:   &consumedMessagesRetention,
		UnconsumedMessagesRetention: &unconsumedMessagesRetention,
		OwnerEmail:                  &ownerEmail,
	})

	exitIfError(err)
	fmt.Printf("%v\n", jsonify(desc))

	// Create a consumer group for that destination
	name := fmt.Sprintf("%s_reader", path)
	startTime := int64(0)
	lockTimeout := int32(60)
	maxDelivery := int32(3)
	skipOlder := int32(3600)

	cdesc, err := cClient.CreateConsumerGroup(&cthrift.CreateConsumerGroupRequest{
		DestinationPath:            &path,
		ConsumerGroupName:          &name,
		StartFrom:                  &startTime,
		LockTimeoutInSeconds:       &lockTimeout,
		MaxDeliveryCount:           &maxDelivery,
		SkipOlderMessagesInSeconds: &skipOlder,
		OwnerEmail:                 &ownerEmail,
	})

	exitIfError(err)
	fmt.Printf("%v\n", jsonify(cdesc))

	// To publish, we need to create a Publisher for the specific destination
	publisher := cClient.CreatePublisher(&cherami.CreatePublisherRequest{
		Path: path,
	})

	err = publisher.Open()
	exitIfError(err)

	// We will do async publishing, so we need to have a channel to receive
	// publish receipts. Spin up a goroutine to print the receipt or error.
	receiptCh := make(chan *cherami.PublisherReceipt)
	go func() {
		for receipt := range receiptCh {
			if receipt.Error != nil {
				fmt.Fprintf(os.Stdout, "Error for publish ID %s is %s. With context userMsgID: %s\n", receipt.ID, receipt.Error.Error(), receipt.UserContext["userMsgID"])
			} else {
				fmt.Fprintf(os.Stdout, "Receipt for publish ID %s is %s. With context userMsgID: %s\n", receipt.ID, receipt.Receipt, receipt.UserContext["userMsgID"])
			}
		}
	}()

	// To consume, we need to create a Consumer object to handle the consumption
	// from the destination. The Consumer is part of the Consumer Group.
	consumer := cClient.CreateConsumer(&cherami.CreateConsumerRequest{
		Path:              path,
		ConsumerGroupName: name,
		ConsumerName:      "",
		PrefetchCount:     1,
		Options: &cherami.ClientOptions{
			Timeout: 15 * time.Second,
		},
	})

	// The messages will be delivered via a channel. Spin up a goroutine to print out the message content.
	ch := make(chan cherami.Delivery, 1)
	_, err = consumer.Open(ch)

	doneCh := make(chan struct{})
	go func() {
		i := 0
		for delivery := range ch {
			msg := delivery.GetMessage()
			fmt.Fprintf(os.Stdout, "msg: '%s', ack_token: %s\n", string(msg.GetPayload().GetData()), delivery.GetDeliveryToken())
			delivery.Ack()
			i++
			if i == 10 {
				doneCh <- struct{}{}
				return
			}
		}
	}()

	// Start publishing
	for i := 0; i < 10; i++ {
		var id string
		data := fmt.Sprintf("message %d", i)
		userMsgID := fmt.Sprintf("user-msg-%d", i)
		id, err = publisher.PublishAsync(&cherami.PublisherMessage{
			Data:        []byte(data),
			UserContext: map[string]string{"userMsgID": userMsgID},
		}, receiptCh)

		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			break
		}

		fmt.Fprintf(os.Stdout, "Local publish ID for message '%s': %s. With context userMsgID: %s\n", data, id, userMsgID)
	}

	publisher.Close()
	close(receiptCh)

	// Wait for all messages are consumed.
	<-doneCh

	// Clean up consumer group and destination. System will take care of actual deleting of messages.
	err = cClient.DeleteConsumerGroup(&cthrift.DeleteConsumerGroupRequest{
		DestinationPath:   &path,
		ConsumerGroupName: &name,
	})

	exitIfError(err)

	err = cClient.DeleteDestination(&cthrift.DeleteDestinationRequest{
		Path: &path,
	})

	exitIfError(err)

	println("end")
}
