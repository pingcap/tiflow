// Copyright 2023 PingCAP, Inc.
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

package owner

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
)

func TestUpdateGCSafePoint(t *testing.T) {
	mockPDClient := &gc.MockPDClient{}
	m := upstream.NewManager4Test(mockPDClient)
	o := NewServerManager(m, config.NewDefaultSchedulerConfig()).(*serverManager)
	ctx := cdcContext.NewBackendContext4Test(true)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()
	state := orchestrator.NewGlobalState(etcd.DefaultCDCClusterID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)

	// no changefeed, the gc safe point should be max uint64
	mockPDClient.UpdateServiceGCSafePointFunc = func(
		ctx context.Context, serviceID string, ttl int64, safePoint uint64,
	) (uint64, error) {
		// Owner will do a snapshot read at (checkpointTs - 1) from TiKV,
		// set GC safepoint to (checkpointTs - 1)
		require.Equal(t, safePoint, uint64(math.MaxUint64-1))
		return 0, nil
	}

	// add a failed changefeed, it must not trigger update GC safepoint.
	mockPDClient.UpdateServiceGCSafePointFunc = func(
		ctx context.Context, serviceID string, ttl int64, safePoint uint64,
	) (uint64, error) {
		t.Fatal("must not update")
		return 0, nil
	}
	changefeedID1 := model.DefaultChangeFeedID("test-changefeed1")
	tester.MustUpdate(
		fmt.Sprintf("%s/changefeed/info/%s",
			etcd.DefaultClusterAndNamespacePrefix,
			changefeedID1.ID),
		[]byte(`{"config":{},"state":"failed"}`))
	tester.MustApplyPatches()
	state.Changefeeds[changefeedID1].PatchStatus(
		func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			return &model.ChangeFeedStatus{CheckpointTs: 2}, true, nil
		})
	tester.MustApplyPatches()
	err := o.updateGCSafepoint(ctx, state)
	require.Nil(t, err)

	// switch the state of changefeed to normal, it must update GC safepoint to
	// 1 (checkpoint Ts of changefeed-test1).
	ch := make(chan struct{}, 1)
	mockPDClient.UpdateServiceGCSafePointFunc = func(
		ctx context.Context, serviceID string, ttl int64, safePoint uint64,
	) (uint64, error) {
		// Owner will do a snapshot read at (checkpointTs - 1) from TiKV,
		// set GC safepoint to (checkpointTs - 1)
		require.Equal(t, safePoint, uint64(1))
		require.Equal(t, serviceID, etcd.GcServiceIDForTest())
		ch <- struct{}{}
		return 0, nil
	}
	state.Changefeeds[changefeedID1].PatchInfo(
		func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
			info.State = model.StateNormal
			return info, true, nil
		})
	tester.MustApplyPatches()
	err = o.updateGCSafepoint(ctx, state)
	require.Nil(t, err)
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	case <-ch:
	}

	// add another changefeed, it must update GC safepoint.
	changefeedID2 := model.DefaultChangeFeedID("test-changefeed2")
	tester.MustUpdate(
		fmt.Sprintf("%s/changefeed/info/%s",
			etcd.DefaultClusterAndNamespacePrefix,
			changefeedID2.ID),
		[]byte(`{"config":{},"state":"normal"}`))
	tester.MustApplyPatches()
	state.Changefeeds[changefeedID1].PatchStatus(
		func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			return &model.ChangeFeedStatus{CheckpointTs: 20}, true, nil
		})
	state.Changefeeds[changefeedID2].PatchStatus(
		func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			return &model.ChangeFeedStatus{CheckpointTs: 30}, true, nil
		})
	tester.MustApplyPatches()
	mockPDClient.UpdateServiceGCSafePointFunc = func(
		ctx context.Context, serviceID string, ttl int64, safePoint uint64,
	) (uint64, error) {
		// Owner will do a snapshot read at (checkpointTs - 1) from TiKV,
		// set GC safepoint to (checkpointTs - 1)
		require.Equal(t, safePoint, uint64(19))
		require.Equal(t, serviceID, etcd.GcServiceIDForTest())
		ch <- struct{}{}
		return 0, nil
	}
	err = o.updateGCSafepoint(ctx, state)
	require.Nil(t, err)
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	case <-ch:
	}
}

func TestCalculateGCSafepointTs(t *testing.T) {
	state := orchestrator.NewGlobalState(etcd.DefaultCDCClusterID)
	expectMinTsMap := make(map[uint64]uint64)
	expectForceUpdateMap := make(map[uint64]interface{})
	o := &serverManager{changefeeds: make(map[model.ChangeFeedID]interface{})}

	for i := 0; i < 100; i++ {
		cfID := model.DefaultChangeFeedID(fmt.Sprintf("testChangefeed-%d", i))
		upstreamID := uint64(i / 10)
		cfInfo := &model.ChangeFeedInfo{UpstreamID: upstreamID, State: model.StateNormal}
		cfStatus := &model.ChangeFeedStatus{CheckpointTs: uint64(i)}
		changefeed := &orchestrator.ChangefeedReactorState{
			ID:     cfID,
			Info:   cfInfo,
			Status: cfStatus,
		}
		state.Changefeeds[cfID] = changefeed

		// expectMinTsMap will be like map[upstreamID]{0, 10, 20, ..., 90}
		if i%10 == 0 {
			expectMinTsMap[upstreamID] = uint64(i)
		}

		// If a changefeed does not exist in ownerImpl.changefeeds,
		// forceUpdate should be true.
		if upstreamID%2 == 0 {
			expectForceUpdateMap[upstreamID] = nil
		} else {
			o.changefeeds[cfID] = nil
		}
	}

	minCheckpoinTsMap, forceUpdateMap := o.calculateGCSafepoint(state)

	require.Equal(t, expectMinTsMap, minCheckpoinTsMap)
	require.Equal(t, expectForceUpdateMap, forceUpdateMap)
}
