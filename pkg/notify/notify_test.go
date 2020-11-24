// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package notify

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type notifySuite struct{}

var _ = check.Suite(&notifySuite{})

func (s *notifySuite) TestNotifyHub(c *check.C) {
	defer testleak.AfterTest(c)()
	notifier := new(Notifier)
	r1 := notifier.NewReceiver(-1)
	r2 := notifier.NewReceiver(-1)
	r3 := notifier.NewReceiver(-1)
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(time.Second)
			notifier.Notify()
		}
	}()
	<-r1.C
	r1.Stop()
	<-r2.C
	<-r3.C

	r2.Stop()
	r3.Stop()
	c.Assert(len(notifier.receivers), check.Equals, 0)
	time.Sleep(time.Second)
	r4 := notifier.NewReceiver(-1)
	<-r4.C
	r4.Stop()

	notifier2 := new(Notifier)
	r5 := notifier2.NewReceiver(10 * time.Millisecond)
	<-r5.C
	r5.Stop()
}

func (s *notifySuite) TestContinusStop(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	notifier := new(Notifier)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			notifier.Notify()
		}
	}()
	n := 50
	receivers := make([]*Receiver, n)
	for i := 0; i < n; i++ {
		receivers[i] = notifier.NewReceiver(10 * time.Millisecond)
	}
	for i := 0; i < n; i++ {
		i := i
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-receivers[i].C:
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		receivers[i].Stop()
	}
	<-ctx.Done()
}
