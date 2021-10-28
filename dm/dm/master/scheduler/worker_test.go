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

package scheduler

import (
	"context"
	"time"

	. "github.com/pingcap/check"

	"github.com/pingcap/ticdc/dm/dm/config"
	"github.com/pingcap/ticdc/dm/dm/master/workerrpc"
	"github.com/pingcap/ticdc/dm/dm/pb"
	"github.com/pingcap/ticdc/dm/pkg/ha"
	"github.com/pingcap/ticdc/dm/pkg/terror"
)

type testWorker struct{}

var _ = Suite(&testWorker{})

func (t *testWorker) TestWorker(c *C) {
	var (
		name    = "dm-worker-1"
		info    = ha.NewWorkerInfo(name, "127.0.0.1:51803") // must ensure no worker listening one this address.
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		bound   = ha.NewSourceBound(source1, name)
	)

	// create a worker with Offline stage and not bound.
	w, err := NewWorker(info, config.Security{})
	c.Assert(err, IsNil)
	defer w.Close()
	c.Assert(w.BaseInfo(), DeepEquals, info)
	c.Assert(w.Stage(), Equals, WorkerOffline)
	c.Assert(w.Bound(), DeepEquals, nullBound)

	// Offline to Free.
	w.ToFree()
	c.Assert(w.Stage(), Equals, WorkerFree)
	c.Assert(w.Bound(), DeepEquals, nullBound)

	// Free to Bound.
	c.Assert(w.ToBound(bound), IsNil)
	c.Assert(w.Stage(), Equals, WorkerBound)
	c.Assert(w.Bound(), DeepEquals, bound)

	// Bound to Free.
	w.ToFree()
	c.Assert(w.Stage(), Equals, WorkerFree)
	c.Assert(w.Bound(), DeepEquals, nullBound)

	// Free to Offline.
	w.ToOffline()
	c.Assert(w.Stage(), Equals, WorkerOffline)
	c.Assert(w.Bound(), DeepEquals, nullBound)

	// Offline to Bound, invalid.
	c.Assert(terror.ErrSchedulerWorkerInvalidTrans.Equal(w.ToBound(bound)), IsTrue)
	c.Assert(w.Stage(), Equals, WorkerOffline)
	c.Assert(w.Bound(), DeepEquals, nullBound)

	// Offline to Free to Bound again.
	w.ToFree()
	c.Assert(w.ToBound(bound), IsNil)
	c.Assert(w.Stage(), Equals, WorkerBound)
	c.Assert(w.Bound(), DeepEquals, bound)

	// Bound to Offline.
	w.ToOffline()
	c.Assert(w.Stage(), Equals, WorkerOffline)
	c.Assert(w.Bound(), DeepEquals, nullBound)

	// Offline to Free to Relay
	w.ToFree()
	c.Assert(w.StartRelay(source1), IsNil)
	c.Assert(w.Stage(), Equals, WorkerRelay)
	c.Assert(w.RelaySourceID(), Equals, source1)

	// Relay to Free
	w.StopRelay()
	c.Assert(w.Stage(), Equals, WorkerFree)
	c.Assert(w.RelaySourceID(), HasLen, 0)

	// Relay to Bound (bound with relay)
	c.Assert(w.StartRelay(source1), IsNil)
	c.Assert(w.ToBound(bound), IsNil)
	c.Assert(w.Stage(), Equals, WorkerBound)
	c.Assert(w.Bound(), DeepEquals, bound)
	c.Assert(w.relaySource, Equals, source1)

	// Bound turn off relay
	w.StopRelay()
	c.Assert(w.Stage(), Equals, WorkerBound)
	c.Assert(w.relaySource, HasLen, 0)

	// Bound try to turn on relay, but with wrong source ID
	err = w.StartRelay(source2)
	c.Assert(terror.ErrSchedulerRelayWorkersWrongBound.Equal(err), IsTrue)
	c.Assert(w.relaySource, HasLen, 0)

	// Bound turn on relay
	c.Assert(w.StartRelay(source1), IsNil)
	c.Assert(w.Stage(), Equals, WorkerBound)
	c.Assert(w.relaySource, Equals, source1)

	// Bound to Relay
	c.Assert(w.Unbound(), IsNil)
	c.Assert(w.Stage(), Equals, WorkerRelay)
	c.Assert(w.bound, DeepEquals, nullBound)
	c.Assert(w.relaySource, Equals, source1)

	// Relay to Offline
	w.ToOffline()
	c.Assert(w.Stage(), Equals, WorkerOffline)
	c.Assert(w.RelaySourceID(), Equals, source1)

	// Offline turn off relay (when DM worker is offline, stop-relay)
	w.StopRelay()
	c.Assert(w.stage, Equals, WorkerOffline)
	c.Assert(w.RelaySourceID(), HasLen, 0)

	// SendRequest.
	req := &workerrpc.Request{
		Type: workerrpc.CmdQueryStatus,
		QueryStatus: &pb.QueryStatusRequest{
			Name: "task1",
		},
	}
	resp, err := w.SendRequest(context.Background(), req, time.Second)
	c.Assert(err, ErrorMatches, ".*connection refused.*")
	c.Assert(resp, IsNil)
}
