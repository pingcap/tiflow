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
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

func Test(t *testing.T) {
	check.TestingT(t)
}

type notifySuite struct{}

var _ = check.Suite(&notifySuite{})

func (s *notifySuite) TestNotifyHub(c *check.C) {
	defer testleak.AfterTest(c)()
	notifier := new(Notifier)
	r1, err := notifier.NewReceiver(-1)
	c.Assert(err, check.IsNil)
	r2, err := notifier.NewReceiver(-1)
	c.Assert(err, check.IsNil)
	r3, err := notifier.NewReceiver(-1)
	c.Assert(err, check.IsNil)
	finishedCh := make(chan struct{})
	go func() {
		for i := 0; i < 5; i++ {
			time.Sleep(time.Second)
			notifier.Notify()
		}
		close(finishedCh)
	}()
	<-r1.C
	r1.Stop()
	<-r2.C
	<-r3.C

	r2.Stop()
	r3.Stop()
	c.Assert(len(notifier.receivers), check.Equals, 0)
	r4, err := notifier.NewReceiver(-1)
	c.Assert(err, check.IsNil)
	<-r4.C
	r4.Stop()

	notifier2 := new(Notifier)
	r5, err := notifier2.NewReceiver(10 * time.Millisecond)
	c.Assert(err, check.IsNil)
	<-r5.C
	r5.Stop()
	<-finishedCh // To make the leak checker happy
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
	var err error
	for i := 0; i < n; i++ {
		receivers[i], err = notifier.NewReceiver(10 * time.Millisecond)
		c.Assert(err, check.IsNil)
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

func (s *notifySuite) TestNewReceiverWithClosedNotifier(c *check.C) {
	defer testleak.AfterTest(c)()
	notifier := new(Notifier)
	notifier.Close()
	_, err := notifier.NewReceiver(50 * time.Millisecond)
	c.Assert(errors.ErrOperateOnClosedNotifier.Equal(err), check.IsTrue)
}
