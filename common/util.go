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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/uber/cherami-thrift/.generated/go/cherami"

	log "github.com/sirupsen/logrus"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/hyperbahn"
)

const hyperbahnPort int16 = 21300
const rpAppNamePrefix string = "cherami"
const maxRpJoinTimeout = 2 * 60 * time.Second
const defaultNumReplicas = 3

const (
	inputHostAdminChannelName  = "inputhost-admin-client"
	outputHostAdminChannelName = "outputhost-admin-client"
	storeHostClientChannelName = "storehost-client"
)

// CreateTChannel creates the top level TChannel object for the
// specified servicer
func CreateTChannel(service string) *tchannel.Channel {
	ch, err := tchannel.NewChannel(service, &tchannel.ChannelOptions{
		DefaultConnectionOptions: tchannel.ConnectionOptions{
			FramePool: tchannel.NewSyncFramePool(),
		},
	})
	if err != nil {
		log.Fatalf("Failed to create tchannel: %v", err)
	}
	return ch
}

func getHyperbahnInitialNodes(bootstrapFile string) []string {
	ip, _ := tchannel.ListenIP()
	ret := []string{fmt.Sprintf("%s:%d", ip.String(), hyperbahnPort)}

	if len(bootstrapFile) < 1 {
		return ret
	}

	blob, err := ioutil.ReadFile(bootstrapFile)
	if err != nil {
		return ret
	}

	err = json.Unmarshal(blob, &ret)
	if err != nil {
		return ret
	}

	return ret
}

func isPersistentService(name string) bool {
	return (strings.Compare(name, StoreServiceName) == 0)
}

// CreateHyperbahnClient returns a hyperbahn client
func CreateHyperbahnClient(ch *tchannel.Channel, bootstrapFile string) *hyperbahn.Client {
	initialNodes := getHyperbahnInitialNodes(bootstrapFile)
	config := hyperbahn.Configuration{InitialNodes: initialNodes}
	if len(config.InitialNodes) == 0 {
		log.Fatalf("No Hyperbahn nodes to connect to.")
	}
	hClient, _ := hyperbahn.NewClient(ch, config, nil)
	return hClient
}

// AdvertiseInHyperbahn advertises this node in Hyperbahn
func AdvertiseInHyperbahn(ch *tchannel.Channel, bootstrapFile string) *hyperbahn.Client {
	hbClient := CreateHyperbahnClient(ch, bootstrapFile)
	if err := hbClient.Advertise(); err != nil {
		log.Errorf("Failed to advertise in Hyperbahn: %v", err)
		return nil
	}
	return hbClient
}

// This is just a utility to satisfy the RPM service so that listen hosts is
// map[string][]string
func convertListenHosts(cfgListenHosts map[string]string) map[string][]string {
	listenHosts := make(map[string][]string)

	for service, hosts := range cfgListenHosts {
		listenHosts[service] = strings.Split(hosts, ",")
	}

	return listenHosts
}

// SplitHostPort takes a x.x.x.x:yyyy string and split it into host and ports
func SplitHostPort(hostPort string) (string, int, error) {
	parts := strings.Split(hostPort, ":")
	port, err := strconv.Atoi(parts[1])
	return parts[0], port, err
}

var guidRegex = regexp.MustCompile(`([[:xdigit:]]{8})-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12}`)

// ShortenGUIDString takes a string with one or more GUIDs and elides them to make it more human readable. It turns
// "354754bd-b73e-4d20-8021-ab93a3d145c0:67af70c5-f45e-4b3d-9d20-6758195e2ff4:3:2" into "354754bd:67af70c5:3:2"
func ShortenGUIDString(s string) string {
	return guidRegex.ReplaceAllString(s, "$1")
}

// ConditionFunc represents an expression that evaluates to
// true on when some condition is satisfied and false otherwise
type ConditionFunc func() bool

// SpinWaitOnCondition busy waits for a given condition to be true until the timeout
// Returns true if the condition was satisfied, false on timeout
func SpinWaitOnCondition(condition ConditionFunc, timeout time.Duration) bool {

	timeoutCh := time.After(timeout)

	for !condition() {
		select {
		case <-timeoutCh:
			return false
		default:
			time.Sleep(time.Millisecond * 5)
		}
	}

	return true
}

// AwaitWaitGroup calls Wait on the given wait
// Returns true if the Wait() call succeeded before the timeout
// Returns false if the Wait() did not return before the timeout
func AwaitWaitGroup(wg *sync.WaitGroup, timeout time.Duration) bool {

	doneC := make(chan struct{})

	go func() {
		wg.Wait()
		close(doneC)
	}()

	select {
	case <-doneC:
		return true
	case <-time.After(timeout):
		return false
	}
}

// IsRetryableTChanErr returns true if the given tchannel
// error is a retryable error.
func IsRetryableTChanErr(err error) bool {
	return (err == tchannel.ErrTimeout ||
		err == tchannel.ErrServerBusy ||
		err == tchannel.ErrRequestCancelled)
}

// GetDirectoryName function gives the directory name given a path used for destination or consumer groups
func GetDirectoryName(path string) (string, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 3 {
		return "", fmt.Errorf("Invalid path: %v", path)
	}

	return parts[1], nil
}

// GetDateTag returns the current date used for tagging daily metric
func GetDateTag() string {
	return time.Now().Format("2006-01-02")
}

// GetConnectionKey is used to create a key used by connections for looking up connections
func GetConnectionKey(host *cherami.HostAddress) string {
	return net.JoinHostPort(host.GetHost(), strconv.Itoa(int(host.GetPort())))
}

// GetRandInt64 is used to get a 64 bit random number between min and max
func GetRandInt64(min int64, max int64) int64 {
	// we need to get a random number between min and max
	return min + rand.Int63n(max-min)
}

// UUIDHashCode is a hash function for hashing string uuid
// if the uuid is malformed, then the hash function always
// returns 0 as the hash value
func UUIDHashCode(key string) uint32 {
	if len(key) != UUIDStringLength {
		return 0
	}
	// Use the first 4 bytes of the uuid as the hash
	b, err := hex.DecodeString(key[:8])
	if err != nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

// SequenceNumber is an int64 number represents the sequence of messages in Extent
type SequenceNumber int64

// UnixNanoTime is Unix time as nanoseconds since Jan 1st, 1970, 00:00 GMT
type UnixNanoTime int64

// Seconds is time as seconds, either relative or absolute since the epoch
type Seconds float64

// ToSeconds turns a relative or absolute UnixNanoTime to float Seconds
func (u UnixNanoTime) ToSeconds() Seconds {
	return Seconds(float64(u) / float64(1e9))
}

// DurationToSeconds converts a time.Duration to Seconds
func DurationToSeconds(t time.Duration) Seconds {
	return Seconds(float64(int64(t)) / float64(int64(time.Second)))
}

// Now is the version to return UnixNanoTime
func Now() UnixNanoTime {
	return UnixNanoTime(time.Now().UnixNano())
}

// CalculateRate does a simple rate calculation
func CalculateRate(last, curr SequenceNumber, lastTime, currTime UnixNanoTime) float64 {
	deltaV := float64(curr - last)
	deltaT := float64(currTime-lastTime) / float64(1e9) // 10^9 nanoseconds per second
	return float64(deltaV / deltaT)
}

// GeometricRollingAverage is the value of a geometrically diminishing rolling average
type GeometricRollingAverage float64

// SetGeometricRollingAverage adds a value to the geometric rolling average
func (avg *GeometricRollingAverage) SetGeometricRollingAverage(val float64) {
	const rollingAverageFalloff = 100
	*avg -= *avg / GeometricRollingAverage(rollingAverageFalloff)
	*avg += GeometricRollingAverage(val) / GeometricRollingAverage(rollingAverageFalloff)
}

// GetGeometricRollingAverage returns the result of the geometric rolling average
func (avg *GeometricRollingAverage) GetGeometricRollingAverage() float64 {
	return float64(*avg)
}

// ValidateTimeout returns an error if the passed timeout is unreasonable
func ValidateTimeout(t time.Duration) error {
	if t >= time.Millisecond*100 && t <= time.Minute*5 {
		return nil
	}

	return errors.New(fmt.Sprintf(`Configured timeout [%v] must be in range [100ms-5min]`, t))
}

// MinInt returns the minimum of values (a, b)
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
