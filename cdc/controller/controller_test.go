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

package controller

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func createController4Test(globalVars *vars.GlobalVars,
	t *testing.T) (*controllerImpl, *orchestrator.GlobalReactorState,
	*orchestrator.ReactorStateTester,
) {
	pdClient := &gc.MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return safePoint, nil
		},
	}

	m := upstream.NewManager4Test(pdClient)
	o := NewController(m, &model.CaptureInfo{}, nil).(*controllerImpl)

	state := orchestrator.NewGlobalStateForTest(etcd.DefaultCDCClusterID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)

	// set captures
	cdcKey := etcd.CDCKey{
		ClusterID: state.ClusterID,
		Tp:        etcd.CDCKeyTypeCapture,
		CaptureID: globalVars.CaptureInfo.ID,
	}
	captureBytes, err := globalVars.CaptureInfo.Marshal()
	require.Nil(t, err)
	tester.MustUpdate(cdcKey.String(), captureBytes)
	return o, state, tester
}

func TestUpdateGCSafePoint(t *testing.T) {
	mockPDClient := &gc.MockPDClient{}
	m := upstream.NewManager4Test(mockPDClient)
	o := NewController(m, &model.CaptureInfo{}, nil).(*controllerImpl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := orchestrator.NewGlobalStateForTest(etcd.DefaultCDCClusterID)
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
		return 0, nil
	}
	changefeedID1 := model.DefaultChangeFeedID("test-changefeed1")
	tester.MustUpdate(
		fmt.Sprintf("%s/changefeed/info/%s",
			etcd.DefaultClusterAndNamespacePrefix,
			changefeedID1.ID),
		[]byte(`{"config":{},"state":"failed"}`))
	tester.MustApplyPatches()
	gcErr := errors.ChangeFeedGCFastFailError[rand.Intn(len(errors.ChangeFeedGCFastFailError))]
	errCode, ok := errors.RFCCode(gcErr)
	require.True(t, ok)
	state.Changefeeds[changefeedID1].PatchInfo(
		func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
			if info == nil {
				return nil, false, nil
			}
			info.Error = &model.RunningError{Code: string(errCode), Message: gcErr.Error()}
			return info, true, nil
		})
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
	state := orchestrator.NewGlobalStateForTest(etcd.DefaultCDCClusterID)
	expectMinTsMap := make(map[uint64]uint64)
	expectForceUpdateMap := make(map[uint64]interface{})
	o := &controllerImpl{changefeeds: make(map[model.ChangeFeedID]*orchestrator.ChangefeedReactorState)}
	o.upstreamManager = upstream.NewManager4Test(nil)

	stateMap := []model.FeedState{
		model.StateNormal, model.StateStopped,
		model.StateWarning, model.StatePending,
		model.StateFailed, /* failed changefeed with normal error should not be ignored */
	}
	for i := 0; i < 100; i++ {
		cfID := model.DefaultChangeFeedID(fmt.Sprintf("testChangefeed-%d", i))
		upstreamID := uint64(i / 10)
		cfStatus := &model.ChangeFeedStatus{CheckpointTs: uint64(i) + 100}
		cfInfo := &model.ChangeFeedInfo{UpstreamID: upstreamID, State: stateMap[rand.Intn(4)]}
		if cfInfo.State == model.StateFailed {
			cfInfo.Error = &model.RunningError{
				Addr:    "test",
				Code:    "test",
				Message: "test",
			}
		}
		changefeed := &orchestrator.ChangefeedReactorState{
			ID:     cfID,
			Info:   cfInfo,
			Status: cfStatus,
		}
		state.Changefeeds[cfID] = changefeed

		// expectMinTsMap will be like map[upstreamID]{0, 10, 20, ..., 90}
		if i%10 == 0 {
			expectMinTsMap[upstreamID] = uint64(i) + 100
		}

		// If a changefeed does not exist in ownerImpl.changefeeds,
		// forceUpdate should be true.
		if upstreamID%2 == 0 {
			expectForceUpdateMap[upstreamID] = nil
		} else {
			o.changefeeds[cfID] = nil
		}
	}

	for i := 0; i < 10; i++ {
		cfID := model.DefaultChangeFeedID(fmt.Sprintf("testChangefeed-ignored-%d", i))
		upstreamID := uint64(i)
		cfStatus := &model.ChangeFeedStatus{CheckpointTs: uint64(i)}
		err := errors.ChangeFeedGCFastFailError[rand.Intn(len(errors.ChangeFeedGCFastFailError))]
		errCode, ok := errors.RFCCode(err)
		require.True(t, ok)
		cfInfo := &model.ChangeFeedInfo{
			UpstreamID: upstreamID,
			State:      model.StateFailed,
			Error:      &model.RunningError{Code: string(errCode), Message: err.Error()},
		}
		changefeed := &orchestrator.ChangefeedReactorState{
			ID:     cfID,
			Info:   cfInfo,
			Status: cfStatus,
		}
		state.Changefeeds[cfID] = changefeed
	}

	minCheckpoinTsMap, forceUpdateMap := o.calculateGCSafepoint(state)

	require.Equal(t, expectMinTsMap, minCheckpoinTsMap)
	require.Equal(t, expectForceUpdateMap, forceUpdateMap)
}

func TestCalculateGCSafepointTsNoChangefeed(t *testing.T) {
	state := orchestrator.NewGlobalStateForTest(etcd.DefaultCDCClusterID)
	expectForceUpdateMap := make(map[uint64]interface{})
	o := &controllerImpl{changefeeds: make(map[model.ChangeFeedID]*orchestrator.ChangefeedReactorState)}
	o.upstreamManager = upstream.NewManager4Test(nil)
	up, err := o.upstreamManager.GetDefaultUpstream()
	require.Nil(t, err)
	up.PDClock = pdutil.NewClock4Test()

	minCheckpoinTsMap, forceUpdateMap := o.calculateGCSafepoint(state)
	require.Equal(t, 1, len(minCheckpoinTsMap))
	require.Equal(t, expectForceUpdateMap, forceUpdateMap)
}

func TestFixChangefeedState(t *testing.T) {
	globalVars := vars.NewGlobalVars4Test()
	ctx := context.Background()
	controller4Test, state, tester := createController4Test(globalVars, t)
	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	// Mismatched state and admin job.
	changefeedInfo := &model.ChangeFeedInfo{
		State:        model.StateNormal,
		AdminJobType: model.AdminStop,
		StartTs:      oracle.GoTimeToTS(time.Now()),
		Config:       config.GetDefaultReplicaConfig(),
	}
	changefeedStr, err := changefeedInfo.Marshal()
	require.Nil(t, err)
	cdcKey := etcd.CDCKey{
		ClusterID:    state.ClusterID,
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	// For the first tick, we do a bootstrap, and it tries to fix the meta information.
	_, err = controller4Test.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.NotContains(t, controller4Test.changefeeds, changefeedID)
	// Start tick normally.
	_, err = controller4Test.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.Contains(t, controller4Test.changefeeds, changefeedID)
	// The meta information is fixed correctly.
	require.Equal(t, controller4Test.changefeeds[changefeedID].Info.State, model.StateStopped)
}

func TestCheckClusterVersion(t *testing.T) {
	globalVars := vars.NewGlobalVars4Test()
	controller4Test, state, tester := createController4Test(globalVars, t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tester.MustUpdate(fmt.Sprintf("%s/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		etcd.DefaultClusterAndMetaPrefix),
		[]byte(`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225",
"address":"127.0.0.1:8300","version":"v6.0.0"}`))

	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	changefeedInfo := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
	}
	changefeedStr, err := changefeedInfo.Marshal()
	require.Nil(t, err)
	cdcKey := etcd.CDCKey{
		ClusterID:    state.ClusterID,
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))

	// check the tick is skipped and the changefeed will not be handled
	_, err = controller4Test.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.NotContains(t, controller4Test.changefeeds, changefeedID)

	tester.MustUpdate(fmt.Sprintf("%s/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		etcd.DefaultClusterAndMetaPrefix,
	),
		[]byte(`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300","version":"`+
			globalVars.CaptureInfo.Version+`"}`))

	// check the tick is not skipped and the changefeed will be handled normally
	_, err = controller4Test.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.Contains(t, controller4Test.changefeeds, changefeedID)
}

func TestFixChangefeedSinkProtocol(t *testing.T) {
	globalVars := vars.NewGlobalVars4Test()
	controller4Test, state, tester := createController4Test(globalVars, t)
	ctx := context.Background()
	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	// Unknown protocol.
	changefeedInfo := &model.ChangeFeedInfo{
		State:          model.StateNormal,
		AdminJobType:   model.AdminStop,
		StartTs:        oracle.GoTimeToTS(time.Now()),
		CreatorVersion: "5.3.0",
		SinkURI:        "kafka://127.0.0.1:9092/ticdc-test2?protocol=random",
		Config: &config.ReplicaConfig{
			Sink: &config.SinkConfig{Protocol: util.AddressOf(config.ProtocolDefault.String())},
		},
	}
	changefeedStr, err := changefeedInfo.Marshal()
	require.Nil(t, err)
	cdcKey := etcd.CDCKey{
		ClusterID:    state.ClusterID,
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	// For the first tick, we do a bootstrap, and it tries to fix the meta information.
	_, err = controller4Test.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.NotContains(t, controller4Test.changefeeds, changefeedID)

	// Start tick normally.
	_, err = controller4Test.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.Contains(t, controller4Test.changefeeds, changefeedID)
	// The meta information is fixed correctly.
	require.Equal(t, controller4Test.changefeeds[changefeedID].Info.SinkURI,
		"kafka://127.0.0.1:9092/ticdc-test2?protocol=open-protocol")
}
