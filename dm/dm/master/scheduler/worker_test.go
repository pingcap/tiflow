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

func checkBounds(t *testing.T, workerBounds map[string]ha.SourceBound, bounds ...ha.SourceBound) {
	t.Helper()
	require.Len(t, workerBounds, len(bounds))
	for _, b := range bounds {
		require.Equal(t, workerBounds[b.Source], b)
	}
}

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
	require.Len(t, w.Bounds(), 0)

	// Offline to Free.
	w.ToFree()
	require.Equal(t, WorkerFree, w.Stage())
	require.Len(t, w.Bounds(), 0)

	// Free to Bound.
	require.NoError(t, w.ToBound(bound))
	require.Equal(t, WorkerBound, w.Stage())
	checkBounds(t, w.Bounds(), bound)

	// Bound to Free.
	w.ToFree()
	require.Equal(t, WorkerFree, w.Stage())
	require.Len(t, w.Bounds(), 0)

	// Free to Offline.
	w.ToOffline()
	require.Equal(t, WorkerOffline, w.Stage())
	require.Len(t, w.Bounds(), 0)

	// Offline to Bound, invalid.
	require.True(t, terror.ErrSchedulerWorkerInvalidTrans.Equal(w.ToBound(bound)))
	require.Equal(t, WorkerOffline, w.Stage())
	require.Len(t, w.Bounds(), 0)

	// Offline to Free to Bound again.
	w.ToFree()
	require.NoError(t, w.ToBound(bound))
	require.Equal(t, WorkerBound, w.Stage())
	checkBounds(t, w.Bounds(), bound)

	// Bound to Offline.
	w.ToOffline()
	require.Equal(t, WorkerOffline, w.Stage())
	require.Len(t, w.Bounds(), 0)

	// Offline to Free to Relay
	w.ToFree()
	require.NoError(t, w.StartRelay(source1))
	require.Equal(t, WorkerBound, w.Stage())
	checkRelaySource(t, w.RelaySources(), source1)

	// Relay to Free
	w.StopRelay(source1)
	require.Equal(t, WorkerFree, w.Stage())
	require.Len(t, w.RelaySources(), 0)

	// Relay to Bound (bound with relay)
	require.NoError(t, w.StartRelay(source1))
	require.NoError(t, w.ToBound(bound))
	require.Equal(t, WorkerBound, w.Stage())
	checkBounds(t, w.Bounds(), bound)
	checkRelaySource(t, w.RelaySources(), source1)

	// Bound turn off relay
	w.StopRelay(source1)
	require.Equal(t, WorkerBound, w.Stage())
	require.Len(t, w.RelaySources(), 0)

	// Bound try to turn on relay, but with different source ID
	// Should start relay successfully
	err = w.StartRelay(source2)
	require.NoError(t, err)
	checkRelaySource(t, w.RelaySources(), source2)

	// Bound turn on relay for another source
	require.NoError(t, w.StartRelay(source1))
	require.Equal(t, WorkerBound, w.Stage())
	checkRelaySource(t, w.RelaySources(), source1, source2)

	// Bound to Relay
	require.NoError(t, w.Unbound(source1))
	require.Equal(t, WorkerBound, w.Stage())
	require.Len(t, w.Bounds(), 0)
	checkRelaySource(t, w.RelaySources(), source1, source2)

	// Relay to Offline
	w.ToOffline()
	require.Equal(t, WorkerOffline, w.Stage())
	checkRelaySource(t, w.RelaySources(), source1, source2)

	// Offline turn off relay (when DM worker is offline, stop-relay)
	w.StopRelay(source2)
	require.Equal(t, WorkerOffline, w.stage)
	checkRelaySource(t, w.RelaySources(), source1)

	// Offline turn off relay (when DM worker is offline, stop-relay)
	w.StopRelay(source1)
	require.Equal(t, WorkerOffline, w.stage)
	require.Len(t, w.RelaySources(), 0)

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
