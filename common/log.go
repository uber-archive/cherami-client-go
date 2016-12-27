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

import (
	"fmt"
	"regexp"
	"strings"
)

// TagErr is the tag for error object message
const TagErr = `err`

// TagDst is the tag for Destination UUID
const TagDst = `destID`

// TagCnsm is the logging tag for Consumer Group UUID
const TagCnsm = `cnsmID`

// TagExt is the logging tag for Extent UUID
const TagExt = `extnID`

// TagIn is the logging tag for Inputhost UUID
const TagIn = `inhoID`

// TagOut is the logging tag for Outputhost UUID
const TagOut = `outhID`

// TagCtrl is the logging tag for Extent Controller UUID
const TagCtrl = `ctrlID`

// TagFrnt is the logging tag for Frontend UUID
const TagFrnt = `frntID`

// TagStor is the logging tag for StoreHost UUID
const TagStor = `storID`

// TagDstPth is the logging tag for Destination Path
const TagDstPth = `dstPth`

// TagCnsPth is the logging tag for Consumer group Path
const TagCnsPth = `cnsPth`

// TagMsgID is the logging tag for MsgId
const TagMsgID = `msgID`

// TagAckID is the logging tag for AckId
const TagAckID = `ackID`

// TagHostIP is the logging tag for host IP
const TagHostIP = `hostIP`

// TagReconfigureID is the logging tag for reconfiguration identifiers
const TagReconfigureID = `reconfigID`

// TagDLQID is the logging tag for a Dead Letter Queue destination UUID
const TagDLQID = `dlqID`

// TagReconfigureType is the logging tag for reconfiguration type
const TagReconfigureType = `reconfigType`

// TagInPutAckID is the logging tag for PutMessageAck ID
const TagInPutAckID = `inPutAckID`

// TagInPubConnID is the logging tag for input pubconnection ID
const TagInPubConnID = `inPubConnID`

// TagInReplicaHost is the logging tag for replica host on input
const TagInReplicaHost = `inReplicaHost`

// TagUpdateUUID is the logging tag for reconfiguration update UUIDs
const TagUpdateUUID = `updateUUID`

// TagService is the log tag for the service
const TagService = "service"

// TagHostPort is the log tag for hostport
const TagHostPort = "hostport"

// TagHosts is the log tag for list of hosts
const TagHosts = "hosts"

const checkFormatAndPanic = true // TODO : Disable in production

var longLowercaseGUIDRegex = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var shortLowercaseGUIDRegex = regexp.MustCompile(`^[0-9a-f]{8}$`)

// Create a shortened, lowercased GUID suitable for low-volume object like host IDs. If there are
// more than 9,200 active objects with random GUIDs, then the birthday problem indicates that there
// will be a >1% chance of a collision
// Ex: `354754BD-B73E-4D20-8021-AB93A3D145C0` => `354754bd`
func fmtShortGUID(s string) string {
	s = ShortenGUIDString(strings.ToLower(s))

	if checkFormatAndPanic {
		if !shortLowercaseGUIDRegex.MatchString(s) {
			panic(fmt.Errorf("Format error on string %#q", s))
		}
	}

	return s
}

// Create a lowercased GUID. Panics if checkFormatAndPanic is enabled and the final GUID doesn't
// match the regular expression
// ex: `354754BD-B73E-4D20-8021-AB93A3D145C0` => `354754bd-b73e-4d20-8021-ab93a3d145c0`
func fmtGUID(s string) string {
	s = strings.ToLower(s)

	if checkFormatAndPanic {
		if !longLowercaseGUIDRegex.MatchString(s) {
			panic(fmt.Errorf("Format error on string %#q", s))
		}
	}

	return s
}

// FmtDst formats a string to be used with TagDst
func FmtDst(s string) string {
	return fmtGUID(s)
}

// FmtCnsm formats a string to be used with TagCnsm
func FmtCnsm(s string) string {
	return fmtGUID(s)
}

// FmtExt formats a string to be used with TagExt
func FmtExt(s string) string {
	return fmtGUID(s)
}

// FmtIn formats a string to be used with TagIn
func FmtIn(s string) string {
	return fmtShortGUID(s)
}

// FmtOut formats a string to be used with TagOut
func FmtOut(s string) string {
	return fmtShortGUID(s)
}

// FmtCtrl formats a string to be used with TagCtrl
func FmtCtrl(s string) string {
	return fmtShortGUID(s)
}

// FmtFrnt formats a string to be used with TagFrnt
func FmtFrnt(s string) string {
	return fmtShortGUID(s)
}

// FmtStor formats a string to be used with TagStor
func FmtStor(s string) string {
	return fmtShortGUID(s)
}

// FmtDstPth formats a string to be used with TagDstPth
func FmtDstPth(s string) string {
	return s
}

// FmtCnsPth formats a string to be used with TagCnsPth
func FmtCnsPth(s string) string {
	return s
}

// FmtMsgID formats a string to be used with TagMsgID
func FmtMsgID(s string) string {
	return s
}

// FmtAckID formats a string to be used with TagAckID
func FmtAckID(s string) string {
	return s
}

// FmtHostIP formats a string to be used with TagHostIP
func FmtHostIP(s string) string {
	return s
}

// FmtReconfigureID formats a string to be used with TagReconfigureID
func FmtReconfigureID(s string) string {
	return s
}

// FmtInPutAckID formats a string to be used with TagInPutAckID
func FmtInPutAckID(s string) string {
	return s
}

// FmtInPubConnID formats an int to be used with TagInPubConnID
func FmtInPubConnID(s int) string {
	return fmt.Sprintf("%v", s)
}

// FmtInReplicaHost formats a string to be used with TagInReplicaHost
func FmtInReplicaHost(s string) string {
	return s
}

// FmtDLQID formats a string to be used with TagDLQID
func FmtDLQID(s string) string {
	return fmtGUID(s)
}

// FmtService formats a string to be used with TagService
func FmtService(s string) string {
	return s
}

// FmtHostPort formats a string to be used with TagHostPort
func FmtHostPort(s string) string {
	return s
}
