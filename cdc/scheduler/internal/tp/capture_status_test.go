// Copyright 2022 PingCAP, Inc.
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

package tp

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/stretchr/testify/require"
)

func TestCaptureStatusHandleHeartbeatResponse(t *testing.T) {
	t.Parallel()

	rev := schedulepb.OwnerRevision{Revision: 1}
	epoch := schedulepb.ProcessorEpoch{Epoch: "test"}
	c := newCaptureStatus(rev)
	require.Equal(t, CaptureStateUninitialize, c.State)

	// Owner revision mismatch
	c.handleHeartbeatResponse(&schedulepb.HeartbeatResponse{
		OwnerRevision:  schedulepb.OwnerRevision{Revision: 2},
		ProcessorEpoch: epoch,
	})
	require.Equal(t, CaptureStateUninitialize, c.State)
	require.Equal(t, schedulepb.ProcessorEpoch{}, c.Epoch)

	// Uninitialize -> Initialized
	c.handleHeartbeatResponse(&schedulepb.HeartbeatResponse{
		OwnerRevision:  rev,
		ProcessorEpoch: epoch,
	})
	require.Equal(t, CaptureStateInitialized, c.State)
	require.Equal(t, epoch, c.Epoch)

	// Processor epoch mismatch
	c.handleHeartbeatResponse(&schedulepb.HeartbeatResponse{
		OwnerRevision:  rev,
		ProcessorEpoch: schedulepb.ProcessorEpoch{Epoch: "unknown"},
		IsStopping:     true,
	})
	require.Equal(t, CaptureStateInitialized, c.State)

	// Initialized -> Stopping
	c.handleHeartbeatResponse(&schedulepb.HeartbeatResponse{
		OwnerRevision:  rev,
		ProcessorEpoch: epoch,
		IsStopping:     true,
	})
	require.Equal(t, CaptureStateStopping, c.State)
	require.Equal(t, epoch, c.Epoch)
}
