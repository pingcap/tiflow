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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

func TestWorker(t *testing.T) {
	t.Parallel()

	var (
		name    = "dm-worker-1"
		info    = ha.NewWorkerInfo(name, "127.0.0.1:51803") // must ensure no worker listening one this address.
		source1 = "mysql-replica-1"
		source2 = "mysql-replica-2"
		bound   = ha.NewSourceBound(source1, name)
	)

	// create a worker with Offline stage and not bound.
	w, err := NewWorker(info, config.Security{})
	require.NoError(t, err)
	defer w.Close()
	require.Equal(t, info, w.BaseInfo())
	require.Equal(t, WorkerOffline, w.Stage())
	require.Equal(t, nullBound, w.Bound())

	// Offline to Free.
	w.ToFree()
	require.Equal(t, WorkerFree, w.Stage())
	require.Equal(t, nullBound, w.Bound())

	// Free to Bound.
	require.NoError(t, w.ToBound(bound))
	require.Equal(t, WorkerBound, w.Stage())
	require.Equal(t, bound, w.Bound())

	// Bound to Free.
	w.ToFree()
	require.Equal(t, WorkerFree, w.Stage())
	require.Equal(t, nullBound, w.Bound())

	// Free to Offline.
	w.ToOffline()
	require.Equal(t, WorkerOffline, w.Stage())
	require.Equal(t, nullBound, w.Bound())

	// Offline to Bound, invalid.
	require.True(t, terror.ErrSchedulerWorkerInvalidTrans.Equal(w.ToBound(bound)))
	require.Equal(t, WorkerOffline, w.Stage())
	require.Equal(t, nullBound, w.Bound())

	// Offline to Free to Bound again.
	w.ToFree()
	require.NoError(t, w.ToBound(bound))
	require.Equal(t, WorkerBound, w.Stage())
	require.Equal(t, bound, w.Bound())

	// Bound to Offline.
	w.ToOffline()
	require.Equal(t, WorkerOffline, w.Stage())
	require.Equal(t, nullBound, w.Bound())

	// Offline to Free to Relay
	w.ToFree()
	require.NoError(t, w.StartRelay(source1))
	require.Equal(t, WorkerRelay, w.Stage())
	require.Equal(t, source1, w.RelaySourceID())

	// Relay to Free
	w.StopRelay()
	require.Equal(t, WorkerFree, w.Stage())
	require.Len(t, w.RelaySourceID(), 0)

	// Relay to Bound (bound with relay)
	require.NoError(t, w.StartRelay(source1))
	require.NoError(t, w.ToBound(bound))
	require.Equal(t, WorkerBound, w.Stage())
	require.Equal(t, bound, w.Bound())
	require.Equal(t, source1, w.relaySource)

	// Bound turn off relay
	w.StopRelay()
	require.Equal(t, WorkerBound, w.Stage())
	require.Len(t, w.relaySource, 0)

	// Bound try to turn on relay, but with wrong source ID
	err = w.StartRelay(source2)
	require.True(t, terror.ErrSchedulerRelayWorkersWrongBound.Equal(err))
	require.Len(t, w.relaySource, 0)

	// Bound turn on relay
	require.NoError(t, w.StartRelay(source1))
	require.Equal(t, WorkerBound, w.Stage())
	require.Equal(t, source1, w.relaySource)

	// Bound to Relay
	require.NoError(t, w.Unbound())
	require.Equal(t, WorkerRelay, w.Stage())
	require.Equal(t, nullBound, w.bound)
	require.Equal(t, source1, w.relaySource)

	// Relay to Offline
	w.ToOffline()
	require.Equal(t, WorkerOffline, w.Stage())
	require.Equal(t, source1, w.RelaySourceID())

	// Offline turn off relay (when DM worker is offline, stop-relay)
	w.StopRelay()
	require.Equal(t, WorkerOffline, w.stage)
	require.Len(t, w.RelaySourceID(), 0)

	// SendRequest.
	req := &workerrpc.Request{
		Type: workerrpc.CmdQueryStatus,
		QueryStatus: &pb.QueryStatusRequest{
			Name: "task1",
		},
	}
	resp, err := w.SendRequest(context.Background(), req, time.Second)
	require.Contains(t, err.Error(), "connection refused")
	require.Nil(t, resp)
}
