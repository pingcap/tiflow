// Copyright 2021 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
	"math"
	"net/url"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type mockManager struct {
	gc.Manager
}

func (m *mockManager) CheckStaleCheckpointTs(
	ctx context.Context, changefeedID model.ChangeFeedID, checkpointTs model.Ts,
) error {
	return cerror.ErrGCTTLExceeded.GenWithStackByArgs()
}

var _ gc.Manager = (*mockManager)(nil)

func createOwner4Test(ctx cdcContext.Context, t *testing.T) (*ownerImpl, *orchestrator.GlobalReactorState, *orchestrator.ReactorStateTester) {
	pdClient := &gc.MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return safePoint, nil
		},
	}

	owner := NewOwner4Test(func(ctx cdcContext.Context, upStream *upstream.Upstream, startTs uint64) (DDLPuller, error) {
		return &mockDDLPuller{resolvedTs: startTs - 1}, nil
	}, func() DDLSink {
		return &mockDDLSink{}
	},
		pdClient,
	)
	o := owner.(*ownerImpl)
	o.upstreamManager = upstream.NewManager4Test(pdClient)

	state := orchestrator.NewGlobalState()
	tester := orchestrator.NewReactorStateTester(t, state, nil)

	// set captures
	cdcKey := etcd.CDCKey{
		Tp:        etcd.CDCKeyTypeCapture,
		CaptureID: ctx.GlobalVars().CaptureInfo.ID,
	}
	captureBytes, err := ctx.GlobalVars().CaptureInfo.Marshal()
	require.Nil(t, err)
	tester.MustUpdate(cdcKey.String(), captureBytes)
	return o, state, tester
}

func TestCreateRemoveChangefeed(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()

	owner, state, tester := createOwner4Test(ctx, t)

	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	changefeedInfo := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
	}
	changefeedStr, err := changefeedInfo.Marshal()
	require.Nil(t, err)
	cdcKey := etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.Contains(t, owner.changefeeds, changefeedID)

	// delete changefeed info key to remove changefeed
	tester.MustUpdate(cdcKey.String(), nil)
	// this tick to clean the leak info of the removed changefeed
	_, err = owner.Tick(ctx, state)
	require.Nil(t, err)
	// this tick to remove the changefeed state in memory
	tester.MustApplyPatches()
	_, err = owner.Tick(ctx, state)
	require.Nil(t, err)
	tester.MustApplyPatches()

	require.NotContains(t, owner.changefeeds, changefeedID)
	require.NotContains(t, state.Changefeeds, changefeedID)

	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.Contains(t, owner.changefeeds, changefeedID)
	removeJob := model.AdminJob{
		CfID:  changefeedID,
		Type:  model.AdminRemove,
		Opts:  &model.AdminJobOption{ForceRemove: true},
		Error: nil,
	}

	// this will make changefeed always meet ErrGCTTLExceeded
	mockedManager := &mockManager{Manager: owner.upstreamManager.Get(changefeedInfo.UpstreamID).GCManager}
	owner.upstreamManager.Get(changefeedInfo.UpstreamID).GCManager = mockedManager
	err = owner.upstreamManager.Get(changefeedInfo.UpstreamID).GCManager.CheckStaleCheckpointTs(ctx, changefeedID, 0)
	require.NotNil(t, err)

	// this tick create remove changefeed patches
	done := make(chan error, 1)
	owner.EnqueueJob(removeJob, done)
	_, err = owner.Tick(ctx, state)
	require.Nil(t, err)
	require.Nil(t, <-done)

	// apply patches and update owner's in memory changefeed states
	tester.MustApplyPatches()
	_, err = owner.Tick(ctx, state)
	require.Nil(t, err)
	require.NotContains(t, owner.changefeeds, changefeedID)
}

func TestStopChangefeed(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	owner, state, tester := createOwner4Test(ctx, t)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()

	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	changefeedInfo := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
	}
	changefeedStr, err := changefeedInfo.Marshal()
	require.Nil(t, err)
	cdcKey := etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.Contains(t, owner.changefeeds, changefeedID)
	// remove changefeed forcibly
	done := make(chan error, 1)
	owner.EnqueueJob(model.AdminJob{
		CfID: changefeedID,
		Type: model.AdminRemove,
		Opts: &model.AdminJobOption{
			ForceRemove: true,
		},
	}, done)

	// this tick to clean the leak info fo the removed changefeed
	_, err = owner.Tick(ctx, state)
	require.Nil(t, err)
	require.Nil(t, <-done)

	// this tick to remove the changefeed state in memory
	tester.MustApplyPatches()
	_, err = owner.Tick(ctx, state)
	require.Nil(t, err)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.NotContains(t, owner.changefeeds, changefeedID)
	require.NotContains(t, state.Changefeeds, changefeedID)
}

func TestFixChangefeedState(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	owner, state, tester := createOwner4Test(ctx, t)
	// We need to do bootstrap.
	owner.bootstrapped = false
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
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	// For the first tick, we do a bootstrap, and it tries to fix the meta information.
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.True(t, owner.bootstrapped)
	require.NotContains(t, owner.changefeeds, changefeedID)
	// Start tick normally.
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.Contains(t, owner.changefeeds, changefeedID)
	// The meta information is fixed correctly.
	require.Equal(t, owner.changefeeds[changefeedID].state.Info.State, model.StateStopped)
}

func TestFixChangefeedSinkProtocol(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	owner, state, tester := createOwner4Test(ctx, t)
	// We need to do bootstrap.
	owner.bootstrapped = false
	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	// Unknown protocol.
	changefeedInfo := &model.ChangeFeedInfo{
		State:          model.StateNormal,
		AdminJobType:   model.AdminStop,
		StartTs:        oracle.GoTimeToTS(time.Now()),
		CreatorVersion: "5.3.0",
		SinkURI:        "kafka://127.0.0.1:9092/ticdc-test2?protocol=random",
		Config: &config.ReplicaConfig{
			Sink: &config.SinkConfig{Protocol: config.ProtocolDefault.String()},
		},
	}
	changefeedStr, err := changefeedInfo.Marshal()
	require.Nil(t, err)
	cdcKey := etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	// For the first tick, we do a bootstrap, and it tries to fix the meta information.
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.True(t, owner.bootstrapped)
	require.NotContains(t, owner.changefeeds, changefeedID)

	// Start tick normally.
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.Contains(t, owner.changefeeds, changefeedID)
	// The meta information is fixed correctly.
	require.Equal(t, owner.changefeeds[changefeedID].state.Info.SinkURI,
		"kafka://127.0.0.1:9092/ticdc-test2?protocol=open-protocol")
}

func TestCheckClusterVersion(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	owner, state, tester := createOwner4Test(ctx, t)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()

	tester.MustUpdate("/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225", []byte(`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300","version":"v6.0.0"}`))

	changefeedID := model.DefaultChangeFeedID("test-changefeed")
	changefeedInfo := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
	}
	changefeedStr, err := changefeedInfo.Marshal()
	require.Nil(t, err)
	cdcKey := etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))

	// check the tick is skipped and the changefeed will not be handled
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.NotContains(t, owner.changefeeds, changefeedID)

	tester.MustUpdate("/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		[]byte(`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300","version":"`+ctx.GlobalVars().CaptureInfo.Version+`"}`))

	// check the tick is not skipped and the changefeed will be handled normally
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	require.Contains(t, owner.changefeeds, changefeedID)
}

func TestAdminJob(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()

	done1 := make(chan error, 1)
	owner, _, _ := createOwner4Test(ctx, t)
	owner.EnqueueJob(model.AdminJob{
		CfID: model.DefaultChangeFeedID("test-changefeed1"),
		Type: model.AdminResume,
	}, done1)
	done2 := make(chan error, 1)
	owner.RebalanceTables(model.DefaultChangeFeedID("test-changefeed2"), done2)
	done3 := make(chan error, 1)
	owner.ScheduleTable(model.DefaultChangeFeedID("test-changefeed3"),
		"test-caputre1", 10, done3)
	done4 := make(chan error, 1)
	var buf bytes.Buffer
	owner.WriteDebugInfo(&buf, done4)

	// remove job.done, it's hard to check deep equals
	jobs := owner.takeOwnerJobs()
	for _, job := range jobs {
		require.NotNil(t, job.done)
		close(job.done)
		job.done = nil
	}
	require.Equal(t, jobs, []*ownerJob{
		{
			Tp: ownerJobTypeAdminJob,
			AdminJob: &model.AdminJob{
				CfID: model.DefaultChangeFeedID("test-changefeed1"),
				Type: model.AdminResume,
			},
			ChangefeedID: model.DefaultChangeFeedID("test-changefeed1"),
		}, {
			Tp:           ownerJobTypeRebalance,
			ChangefeedID: model.DefaultChangeFeedID("test-changefeed2"),
		}, {
			Tp:              ownerJobTypeScheduleTable,
			ChangefeedID:    model.DefaultChangeFeedID("test-changefeed3"),
			TargetCaptureID: "test-caputre1",
			TableID:         10,
		}, {
			Tp:              ownerJobTypeDebugInfo,
			debugInfoWriter: &buf,
		},
	})
	require.Len(t, owner.takeOwnerJobs(), 0)
}

func TestUpdateGCSafePoint(t *testing.T) {
	mockPDClient := &gc.MockPDClient{}
	m := upstream.NewManager4Test(mockPDClient)
	o := NewOwner(m).(*ownerImpl)
	ctx := cdcContext.NewBackendContext4Test(true)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()
	state := orchestrator.NewGlobalState()
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
	err := o.updateGCSafepoint(ctx, state)
	require.Nil(t, err)

	// add a failed changefeed, it must not trigger update GC safepoint.
	mockPDClient.UpdateServiceGCSafePointFunc = func(
		ctx context.Context, serviceID string, ttl int64, safePoint uint64,
	) (uint64, error) {
		t.Fatal("must not update")
		return 0, nil
	}
	changefeedID1 := model.DefaultChangeFeedID("test-changefeed1")
	tester.MustUpdate(
		fmt.Sprintf("/tidb/cdc/changefeed/info/%s", changefeedID1.ID),
		[]byte(`{"config":{"cyclic-replication":{}},"state":"failed"}`))
	tester.MustApplyPatches()
	state.Changefeeds[changefeedID1].PatchStatus(
		func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			return &model.ChangeFeedStatus{CheckpointTs: 2}, true, nil
		})
	tester.MustApplyPatches()
	err = o.updateGCSafepoint(ctx, state)
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
		require.Equal(t, serviceID, gc.CDCServiceSafePointID)
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
		fmt.Sprintf("/tidb/cdc/changefeed/info/%s", changefeedID2.ID),
		[]byte(`{"config":{"cyclic-replication":{}},"state":"normal"}`))
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
		require.Equal(t, serviceID, gc.CDCServiceSafePointID)
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

// make sure handleJobs works well even if there is two different
// version of captures in the cluster
func TestHandleJobsDontBlock(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()
	owner, state, tester := createOwner4Test(ctx, t)

	statusProvider := owner.StatusProvider()
	// work well
	cf1 := model.DefaultChangeFeedID("test-changefeed")
	cfInfo1 := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateNormal,
	}
	changefeedStr, err := cfInfo1.Marshal()
	require.Nil(t, err)
	cdcKey := etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: cf1,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)

	require.Contains(t, owner.changefeeds, cf1)

	// add an non-consistent version capture
	captureInfo := &model.CaptureInfo{
		ID:            "capture-id-owner-test",
		AdvertiseAddr: "127.0.0.1:0000",
		Version:       " v0.0.1-test-only",
	}
	cdcKey = etcd.CDCKey{
		Tp:        etcd.CDCKeyTypeCapture,
		CaptureID: captureInfo.ID,
	}
	v, err := captureInfo.Marshal()
	require.Nil(t, err)
	tester.MustUpdate(cdcKey.String(), v)

	// try to add another changefeed
	cf2 := model.DefaultChangeFeedID("test-changefeed1")
	cfInfo2 := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateNormal,
	}
	changefeedStr1, err := cfInfo2.Marshal()
	require.Nil(t, err)
	cdcKey = etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: cf2,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr1))
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	// make sure this changefeed add failed, which means that owner are return
	// in clusterVersionConsistent check
	require.Nil(t, owner.changefeeds[cf2])

	// make sure statusProvider works well
	ctx1, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var errIn error
	var infos map[model.ChangeFeedID]*model.ChangeFeedInfo
	done := make(chan struct{})
	go func() {
		infos, errIn = statusProvider.GetAllChangeFeedInfo(ctx1)
		done <- struct{}{}
	}()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
WorkLoop:
	for {
		select {
		case <-done:
			break WorkLoop
		case <-ctx1.Done():
			t.Fatal(ctx1.Err())
		case <-ticker.C:
			_, err = owner.Tick(ctx, state)
			require.Nil(t, err)
		}
	}
	require.Nil(t, errIn)
	require.NotNil(t, infos[cf1])
	require.Nil(t, infos[cf2])
}

func TestCalculateGCSafepointTs(t *testing.T) {
	state := orchestrator.NewGlobalState()
	expectMinTsMap := make(map[uint64]uint64)
	expectForceUpdateMap := make(map[uint64]interface{})
	o := ownerImpl{changefeeds: make(map[model.ChangeFeedID]*changefeed)}

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

	minCheckpoinTsMap, forceUpdateMap := o.calculateGCSagepoint(state)

	require.Equal(t, expectMinTsMap, minCheckpoinTsMap)
	require.Equal(t, expectForceUpdateMap, forceUpdateMap)
}

func TestValidateChangefeed(t *testing.T) {
	t.Parallel()

	// Test `ValidateChangefeed` by setting `hasCIEnv` to false.
	//
	// FIXME: We need a better way to enable following tests
	//        Changing global variable in a unit test is BAD practice.
	hasCIEnv = false

	o := &ownerImpl{
		changefeeds: make(map[model.ChangeFeedID]*changefeed),
		// logLimiter:  rate.NewLimiter(1, 1),
		removedChangefeed: make(map[model.ChangeFeedID]time.Time),
		removedSinkURI:    make(map[url.URL]time.Time),
	}

	id := model.ChangeFeedID{Namespace: "a", ID: "b"}
	sinkURI := "mysql://host:1234/"
	o.changefeeds[id] = &changefeed{
		state: &orchestrator.ChangefeedReactorState{
			Info: &model.ChangeFeedInfo{SinkURI: sinkURI},
		},
		feedStateManager: &feedStateManager{},
	}

	o.pushOwnerJob(&ownerJob{
		Tp:           ownerJobTypeAdminJob,
		ChangefeedID: id,
		AdminJob: &model.AdminJob{
			CfID: id,
			Type: model.AdminRemove,
		},
		done: make(chan<- error, 1),
	})
	o.handleJobs()

	require.Error(t, o.ValidateChangefeed(&model.ChangefeedConfig{
		ID:        id.ID,
		Namespace: id.Namespace,
	}))
	require.Error(t, o.ValidateChangefeed(&model.ChangefeedConfig{
		ID:        "unknown",
		Namespace: "unknown",
		SinkURI:   sinkURI,
	}))

	// Test invalid sink URI
	require.Error(t, o.ValidateChangefeed(&model.ChangefeedConfig{
		SinkURI: "wrong uri\n\t",
	}))

	// Test limit passed.
	o.removedChangefeed[id] = time.Now().Add(-2 * recreateChangefeedDelayLimit)
	o.removedSinkURI[url.URL{
		Scheme: "mysql",
		Host:   "host:1234",
	}] = time.Now().Add(-2 * recreateChangefeedDelayLimit)

	require.Nil(t, o.ValidateChangefeed(&model.ChangefeedConfig{
		ID:        id.ID,
		Namespace: id.Namespace,
	}))
	require.Nil(t, o.ValidateChangefeed(&model.ChangefeedConfig{
		ID:        "unknown",
		Namespace: "unknown",
		SinkURI:   sinkURI,
	}))

	// Test GC.
	o.handleJobs()
	require.Equal(t, 0, len(o.removedChangefeed))
	require.Equal(t, 0, len(o.removedSinkURI))
}
