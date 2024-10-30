// Copyright 2019 PingCAP, Inc.
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

package relay

import (
	"context"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

var _ = check.Suite(&testStreamerSuite{})

type testStreamerSuite struct{}

func (t *testStreamerSuite) TestStreamer(c *check.C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval", "return(10000)"), check.IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval"), check.IsNil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// generate an event
	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  11,
	}
	ev, err := event.GenFormatDescriptionEvent(header, 4)
	c.Assert(err, check.IsNil)

	// 1. get event and error
	s := newLocalStreamer() // with buffer
	s.ch <- ev
	ev2, err := s.GetEvent(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(ev2, check.DeepEquals, ev)

	// read error
	errIn := errors.New("error use for streamer test 1")
	s.ech <- errIn
	ev2, err = s.GetEvent(ctx)
	c.Assert(err, check.Equals, errIn)
	c.Assert(ev2, check.IsNil)

	// can not get event anymore because got error
	ev2, err = s.GetEvent(ctx)
	c.Assert(terror.ErrNeedSyncAgain.Equal(err), check.IsTrue)
	c.Assert(ev2, check.IsNil)

	// 2. close with error
	s = newLocalStreamer()
	errClose := errors.New("error use for streamer test 2")
	s.closeWithError(errClose)
	ev2, err = s.GetEvent(ctx)
	c.Assert(err, check.Equals, errClose)
	c.Assert(ev2, check.IsNil)

	// can not get event anymore
	ev2, err = s.GetEvent(ctx)
	c.Assert(terror.ErrNeedSyncAgain.Equal(err), check.IsTrue)
	c.Assert(ev2, check.IsNil)

	// 3. close without error
	s = newLocalStreamer()
	s.close()
	ev2, err = s.GetEvent(ctx)
	c.Assert(terror.ErrSyncClosed.Equal(err), check.IsTrue)
	c.Assert(ev2, check.IsNil)

	// can not get event anymore
	ev2, err = s.GetEvent(ctx)
	c.Assert(terror.ErrNeedSyncAgain.Equal(err), check.IsTrue)
	c.Assert(ev2, check.IsNil)

	// 4. close with nil error
	s = newLocalStreamer()
	s.closeWithError(nil)
	ev2, err = s.GetEvent(ctx)
	c.Assert(terror.ErrSyncClosed.Equal(err), check.IsTrue)
	c.Assert(ev2, check.IsNil)

	// can not get event anymore
	ev2, err = s.GetEvent(ctx)
	c.Assert(terror.ErrNeedSyncAgain.Equal(err), check.IsTrue)
	c.Assert(ev2, check.IsNil)
}

func (t *testStreamerSuite) TestHeartbeat(c *check.C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval", "return(1)"), check.IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval"), check.IsNil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s := newLocalStreamer()
	ev, err := s.GetEvent(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(ev.Header.EventType, check.Equals, replication.HEARTBEAT_EVENT)
}
