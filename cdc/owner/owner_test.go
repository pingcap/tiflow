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
<<<<<<< HEAD
=======
	"net/url"
	"testing"
>>>>>>> 1f6ae1fd8c (cdc: add delay for recreating changefeed (#7730))
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

var _ = check.Suite(&ownerSuite{})

type ownerSuite struct{}

type mockManager struct {
	gc.Manager
}

func (m *mockManager) CheckStaleCheckpointTs(
	ctx context.Context, changefeedID model.ChangeFeedID, checkpointTs model.Ts,
) error {
	return cerror.ErrGCTTLExceeded.GenWithStackByArgs()
}

var _ gc.Manager = (*mockManager)(nil)

func createOwner4Test(ctx cdcContext.Context, c *check.C) (*Owner, *model.GlobalReactorState, *orchestrator.ReactorStateTester) {
	ctx.GlobalVars().PDClient = &gc.MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return safePoint, nil
		},
	}
	cf := NewOwner4Test(func(ctx cdcContext.Context, startTs uint64) (DDLPuller, error) {
		return &mockDDLPuller{resolvedTs: startTs - 1}, nil
	}, func(ctx cdcContext.Context) (AsyncSink, error) {
		return &mockAsyncSink{}, nil
	},
		ctx.GlobalVars().PDClient,
	)
	state := model.NewGlobalState().(*model.GlobalReactorState)
	tester := orchestrator.NewReactorStateTester(c, state, nil)

	// set captures
	cdcKey := etcd.CDCKey{
		Tp:        etcd.CDCKeyTypeCapture,
		CaptureID: ctx.GlobalVars().CaptureInfo.ID,
	}
	captureBytes, err := ctx.GlobalVars().CaptureInfo.Marshal()
	c.Assert(err, check.IsNil)
	tester.MustUpdate(cdcKey.String(), captureBytes)
	return cf, state, tester
}

func (s *ownerSuite) TestCreateRemoveChangefeed(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	owner, state, tester := createOwner4Test(ctx, c)
	changefeedID := "test-changefeed"
	changefeedInfo := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
	}
	changefeedStr, err := changefeedInfo.Marshal()
	c.Assert(err, check.IsNil)
	cdcKey := etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(owner.changefeeds, check.HasKey, changefeedID)

	// delete changefeed info key to remove changefeed
	tester.MustUpdate(cdcKey.String(), nil)
	// this tick to clean the leak info fo the removed changefeed
	_, err = owner.Tick(ctx, state)
	c.Assert(err, check.IsNil)
	// this tick to remove the changefeed state in memory
	tester.MustApplyPatches()
	_, err = owner.Tick(ctx, state)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(owner.changefeeds, check.Not(check.HasKey), changefeedID)
	c.Assert(state.Changefeeds, check.Not(check.HasKey), changefeedID)

	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(owner.changefeeds, check.HasKey, changefeedID)

	removeJob := model.AdminJob{
		CfID:  changefeedID,
		Type:  model.AdminRemove,
		Opts:  &model.AdminJobOption{ForceRemove: true},
		Error: nil,
	}

	// this will make changefeed always meet ErrGCTTLExceeded
	mockedManager := &mockManager{Manager: owner.gcManager}
	owner.gcManager = mockedManager
	err = owner.gcManager.CheckStaleCheckpointTs(ctx, changefeedID, 0)
	c.Assert(err, check.NotNil)

	// this tick create remove changefeed patches
	owner.EnqueueJob(removeJob)
	_, err = owner.Tick(ctx, state)
	c.Assert(err, check.IsNil)

	// apply patches and update owner's in memory changefeed states
	tester.MustApplyPatches()
	_, err = owner.Tick(ctx, state)
	c.Assert(err, check.IsNil)
	c.Assert(owner.changefeeds, check.Not(check.HasKey), changefeedID)
}

func (s *ownerSuite) TestStopChangefeed(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	owner, state, tester := createOwner4Test(ctx, c)
	changefeedID := "test-changefeed"
	changefeedInfo := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
	}
	changefeedStr, err := changefeedInfo.Marshal()
	c.Assert(err, check.IsNil)
	cdcKey := etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(owner.changefeeds, check.HasKey, changefeedID)

	// remove changefeed forcibly
	owner.EnqueueJob(model.AdminJob{
		CfID: changefeedID,
		Type: model.AdminRemove,
		Opts: &model.AdminJobOption{
			ForceRemove: true,
		},
	})

	// this tick to clean the leak info fo the removed changefeed
	_, err = owner.Tick(ctx, state)
	c.Assert(err, check.IsNil)
	c.Assert(err, check.IsNil)
	// this tick to remove the changefeed state in memory
	tester.MustApplyPatches()
	_, err = owner.Tick(ctx, state)
	c.Assert(err, check.IsNil)
	c.Assert(err, check.IsNil)
	tester.MustApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(owner.changefeeds, check.Not(check.HasKey), changefeedID)
	c.Assert(state.Changefeeds, check.Not(check.HasKey), changefeedID)
}

func (s *ownerSuite) TestFixChangefeedInfos(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	owner, state, tester := createOwner4Test(ctx, c)
	// We need to do bootstrap.
	owner.bootstrapped = false
	changefeedID := "test-changefeed"
	// Mismatched state and admin job.
	changefeedInfo := &model.ChangeFeedInfo{
		State:          model.StateNormal,
		AdminJobType:   model.AdminStop,
		StartTs:        oracle.ComposeTS(oracle.GetPhysical(time.Now()), 0),
		Config:         config.GetDefaultReplicaConfig(),
		CreatorVersion: "4.0.14",
	}
	changefeedStr, err := changefeedInfo.Marshal()
	c.Assert(err, check.IsNil)
	cdcKey := etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	// For the first tick, we do a bootstrap, and it tries to fix the meta information.
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(owner.bootstrapped, check.IsTrue)
	c.Assert(owner.changefeeds, check.Not(check.HasKey), changefeedID)

	// Start tick normally.
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(owner.changefeeds, check.HasKey, changefeedID)
	// The meta information is fixed correctly.
	c.Assert(owner.changefeeds[changefeedID].state.Info.State, check.Equals, model.StateStopped)
}

func (s *ownerSuite) TestCheckClusterVersion(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	owner, state, tester := createOwner4Test(ctx, c)
	tester.MustUpdate("/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225", []byte(`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300","version":"v6.0.0"}`))

	changefeedID := "test-changefeed"
	changefeedInfo := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
	}
	changefeedStr, err := changefeedInfo.Marshal()
	c.Assert(err, check.IsNil)
	cdcKey := etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: changefeedID,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))

	// check the tick is skipped and the changefeed will not be handled
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(owner.changefeeds, check.Not(check.HasKey), changefeedID)

	tester.MustUpdate("/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225",
		[]byte(`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300","version":"`+ctx.GlobalVars().CaptureInfo.Version+`"}`))

	// check the tick is not skipped and the changefeed will be handled normally
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(owner.changefeeds, check.HasKey, changefeedID)
}

func (s *ownerSuite) TestAdminJob(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	owner, _, _ := createOwner4Test(ctx, c)
	owner.EnqueueJob(model.AdminJob{
		CfID: "test-changefeed1",
		Type: model.AdminResume,
	})
	owner.TriggerRebalance("test-changefeed2")
	owner.ManualSchedule("test-changefeed3", "test-caputre1", 10)
	var buf bytes.Buffer
	owner.WriteDebugInfo(&buf)

	// remove job.done, it's hard to check deep equals
	jobs := owner.takeOwnerJobs()
	for _, job := range jobs {
		c.Assert(job.done, check.NotNil)
		close(job.done)
		job.done = nil
	}
	c.Assert(jobs, check.DeepEquals, []*ownerJob{
		{
			tp: ownerJobTypeAdminJob,
			adminJob: &model.AdminJob{
				CfID: "test-changefeed1",
				Type: model.AdminResume,
			},
			changefeedID: "test-changefeed1",
		}, {
			tp:           ownerJobTypeRebalance,
			changefeedID: "test-changefeed2",
		}, {
			tp:              ownerJobTypeManualSchedule,
			changefeedID:    "test-changefeed3",
			targetCaptureID: "test-caputre1",
			tableID:         10,
		}, {
			tp:              ownerJobTypeDebugInfo,
			debugInfoWriter: &buf,
		},
	})
	c.Assert(owner.takeOwnerJobs(), check.HasLen, 0)
}

func (s *ownerSuite) TestUpdateGCSafePoint(c *check.C) {
	defer testleak.AfterTest(c)()
	mockPDClient := &gc.MockPDClient{}
	o := NewOwner(mockPDClient)
	o.gcManager = gc.NewManager(mockPDClient)
	ctx := cdcContext.NewBackendContext4Test(true)
	state := model.NewGlobalState().(*model.GlobalReactorState)
	tester := orchestrator.NewReactorStateTester(c, state, nil)

	// no changefeed, the gc safe point should be max uint64
	mockPDClient.UpdateServiceGCSafePointFunc =
		func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			// Owner will do a snapshot read at (checkpointTs - 1) from TiKV,
			// set GC safepoint to (checkpointTs - 1)
			c.Assert(safePoint, check.Equals, uint64(math.MaxUint64-1))
			return 0, nil
		}
	err := o.updateGCSafepoint(ctx, state)
	c.Assert(err, check.IsNil)

	// add a failed changefeed, it must not trigger update GC safepoint.
	mockPDClient.UpdateServiceGCSafePointFunc =
		func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			c.Fatal("must not update")
			return 0, nil
		}
	changefeedID1 := "changefeed-test1"
	tester.MustUpdate(
		fmt.Sprintf("/tidb/cdc/changefeed/info/%s", changefeedID1),
		[]byte(`{"config":{"cyclic-replication":{}},"state":"failed"}`))
	tester.MustApplyPatches()
	state.Changefeeds[changefeedID1].PatchStatus(
		func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			return &model.ChangeFeedStatus{CheckpointTs: 2}, true, nil
		})
	tester.MustApplyPatches()
	err = o.updateGCSafepoint(ctx, state)
	c.Assert(err, check.IsNil)

	// switch the state of changefeed to normal, it must update GC safepoint to
	// 1 (checkpoint Ts of changefeed-test1).
	ch := make(chan struct{}, 1)
	mockPDClient.UpdateServiceGCSafePointFunc =
		func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			// Owner will do a snapshot read at (checkpointTs - 1) from TiKV,
			// set GC safepoint to (checkpointTs - 1)
			c.Assert(safePoint, check.Equals, uint64(1))
			c.Assert(serviceID, check.Equals, gc.CDCServiceSafePointID)
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
	c.Assert(err, check.IsNil)
	select {
	case <-time.After(5 * time.Second):
		c.Fatal("timeout")
	case <-ch:
	}

	// add another changefeed, it must update GC safepoint.
	changefeedID2 := "changefeed-test2"
	tester.MustUpdate(
		fmt.Sprintf("/tidb/cdc/changefeed/info/%s", changefeedID2),
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
	mockPDClient.UpdateServiceGCSafePointFunc =
		func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			// Owner will do a snapshot read at (checkpointTs - 1) from TiKV,
			// set GC safepoint to (checkpointTs - 1)
			c.Assert(safePoint, check.Equals, uint64(19))
			c.Assert(serviceID, check.Equals, gc.CDCServiceSafePointID)
			ch <- struct{}{}
			return 0, nil
		}
	err = o.updateGCSafepoint(ctx, state)
	c.Assert(err, check.IsNil)
	select {
	case <-time.After(5 * time.Second):
		c.Fatal("timeout")
	case <-ch:
	}
}
<<<<<<< HEAD
=======

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
		ClusterID:    state.ClusterID,
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
		ID:            "capture-higher-version",
		AdvertiseAddr: "127.0.0.1:0000",
		// owner version is `v6.3.0`, use `v6.4.0` to make version inconsistent
		Version: "v6.4.0",
	}
	cdcKey = etcd.CDCKey{
		ClusterID: state.ClusterID,
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
		ClusterID:    state.ClusterID,
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: cf2,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr1))
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.Nil(t, err)
	// add changefeed success when the cluster have mixed version.
	require.NotNil(t, owner.changefeeds[cf2])

	// add third non-consistent version capture
	captureInfo = &model.CaptureInfo{
		ID:            "capture-higher-version-2",
		AdvertiseAddr: "127.0.0.1:8302",
		// only allow at most 2 different version instances in the cdc cluster.
		Version: "v6.5.0",
	}
	cdcKey = etcd.CDCKey{
		ClusterID: state.ClusterID,
		Tp:        etcd.CDCKeyTypeCapture,
		CaptureID: captureInfo.ID,
	}
	v, err = captureInfo.Marshal()
	require.NoError(t, err)
	tester.MustUpdate(cdcKey.String(), v)

	// try to add another changefeed, this should not be handled
	cf3 := model.DefaultChangeFeedID("test-changefeed2")
	cfInfo3 := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateNormal,
	}
	changefeedStr2, err := cfInfo3.Marshal()
	require.NoError(t, err)
	cdcKey = etcd.CDCKey{
		ClusterID:    state.ClusterID,
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: cf3,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr2))
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	require.NoError(t, err)
	// add changefeed failed, since 3 different version instances in the cluster.
	require.Nil(t, owner.changefeeds[cf3])

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
	require.NotNil(t, infos[cf2])
	require.Nil(t, infos[cf3])
}

func TestCalculateGCSafepointTs(t *testing.T) {
	state := orchestrator.NewGlobalState(etcd.DefaultCDCClusterID)
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

	minCheckpoinTsMap, forceUpdateMap := o.calculateGCSafepoint(state)

	require.Equal(t, expectMinTsMap, minCheckpoinTsMap)
	require.Equal(t, expectForceUpdateMap, forceUpdateMap)
}

// AsyncStop should cleanup jobs and reject.
func TestAsyncStop(t *testing.T) {
	t.Parallel()

	owner := ownerImpl{}
	done := make(chan error, 1)
	owner.EnqueueJob(model.AdminJob{
		CfID: model.DefaultChangeFeedID("test-changefeed1"),
		Type: model.AdminResume,
	}, done)
	owner.AsyncStop()
	select {
	case err := <-done:
		require.Error(t, err)
	default:
		require.Fail(t, "unexpected")
	}

	done = make(chan error, 1)
	owner.EnqueueJob(model.AdminJob{
		CfID: model.DefaultChangeFeedID("test-changefeed1"),
		Type: model.AdminResume,
	}, done)
	select {
	case err := <-done:
		require.Error(t, err)
	default:
		require.Fail(t, "unexpected")
	}
}

func TestHandleDrainCapturesSchedulerNotReady(t *testing.T) {
	t.Parallel()

	cf := &changefeed{
		scheduler: nil, // scheduler is not set.
		state: &orchestrator.ChangefeedReactorState{
			Info: &model.ChangeFeedInfo{State: model.StateNormal},
		},
	}

	pdClient := &gc.MockPDClient{}
	o := &ownerImpl{
		changefeeds:     make(map[model.ChangeFeedID]*changefeed),
		upstreamManager: upstream.NewManager4Test(pdClient),
	}
	o.changefeeds[model.ChangeFeedID{}] = cf

	ctx := context.Background()
	query := &scheduler.Query{CaptureID: "test"}
	done := make(chan error, 1)

	// check store version failed.
	pdClient.GetAllStoresFunc = func(
		ctx context.Context, opts ...pd.GetStoreOption,
	) ([]*metapb.Store, error) {
		return nil, errors.New("store version check failed")
	}
	o.handleDrainCaptures(ctx, query, done)
	require.Equal(t, 0, query.Resp.(*model.DrainCaptureResp).CurrentTableCount)
	require.Error(t, <-done)

	pdClient.GetAllStoresFunc = func(
		ctx context.Context, opts ...pd.GetStoreOption,
	) ([]*metapb.Store, error) {
		return nil, nil
	}
	done = make(chan error, 1)
	o.handleDrainCaptures(ctx, query, done)
	require.NotEqualValues(t, 0, query.Resp.(*model.DrainCaptureResp).CurrentTableCount)
	require.Nil(t, <-done)

	// Only count changefeed that is normal.
	cf.state.Info.State = model.StateStopped
	query = &scheduler.Query{CaptureID: "test"}
	done = make(chan error, 1)
	o.handleDrainCaptures(ctx, query, done)
	require.EqualValues(t, 0, query.Resp.(*model.DrainCaptureResp).CurrentTableCount)
	require.Nil(t, <-done)
}

type healthScheduler struct {
	scheduler.Scheduler
	scheduler.InfoProvider
	init bool
}

func (h *healthScheduler) IsInitialized() bool {
	return h.init
}

func TestIsHealthyWithAbnormalChangefeeds(t *testing.T) {
	t.Parallel()

	// There is at least one changefeed not in the normal state, the whole cluster should
	// still be healthy, since abnormal changefeeds does not affect other normal changefeeds.
	o := &ownerImpl{
		changefeeds:      make(map[model.ChangeFeedID]*changefeed),
		changefeedTicked: true,
	}

	query := &Query{Tp: QueryHealth}

	// no changefeed at the first, should be healthy
	err := o.handleQueries(query)
	require.NoError(t, err)
	require.True(t, query.Data.(bool))

	// 1 changefeed, state is nil
	cf := &changefeed{}
	o.changefeeds[model.ChangeFeedID{ID: "1"}] = cf
	err = o.handleQueries(query)
	require.NoError(t, err)
	require.True(t, query.Data.(bool))

	// state is not normal
	cf.state = &orchestrator.ChangefeedReactorState{
		Info: &model.ChangeFeedInfo{State: model.StateStopped},
	}
	err = o.handleQueries(query)
	require.NoError(t, err)
	require.True(t, query.Data.(bool))

	// 2 changefeeds, another is normal, and scheduler initialized.
	o.changefeeds[model.ChangeFeedID{ID: "2"}] = &changefeed{
		state: &orchestrator.ChangefeedReactorState{
			Info: &model.ChangeFeedInfo{State: model.StateNormal},
		},
		scheduler: &healthScheduler{init: true},
	}
	err = o.handleQueries(query)
	require.NoError(t, err)
	require.True(t, query.Data.(bool))
}

func TestIsHealthy(t *testing.T) {
	t.Parallel()

	o := &ownerImpl{
		changefeeds: make(map[model.ChangeFeedID]*changefeed),
		logLimiter:  rate.NewLimiter(1, 1),
	}
	query := &Query{Tp: QueryHealth}

	// Unhealthy, changefeeds are not ticked.
	o.changefeedTicked = false
	err := o.handleQueries(query)
	require.NoError(t, err)
	require.False(t, query.Data.(bool))

	o.changefeedTicked = true
	// Unhealthy, cdc cluster version is inconsistent
	o.captures = map[model.CaptureID]*model.CaptureInfo{
		"1": {
			Version: version.MinTiCDCVersion.String(),
		},
		"2": {
			Version: version.MaxTiCDCVersion.String(),
		},
	}
	err = o.handleQueries(query)
	require.NoError(t, err)
	require.False(t, query.Data.(bool))

	// make all captures version consistent.
	o.captures["2"].Version = version.MinTiCDCVersion.String()
	// Healthy, no changefeed.
	err = o.handleQueries(query)
	require.NoError(t, err)
	require.True(t, query.Data.(bool))

	// changefeed in normal, but the scheduler is not set, Unhealthy.
	cf := &changefeed{
		state: &orchestrator.ChangefeedReactorState{
			Info: &model.ChangeFeedInfo{State: model.StateNormal},
		},
		scheduler: nil, // scheduler is not set.
	}
	o.changefeeds[model.ChangeFeedID{ID: "1"}] = cf
	o.changefeedTicked = true
	err = o.handleQueries(query)
	require.NoError(t, err)
	require.False(t, query.Data.(bool))

	// Healthy, scheduler is set and return true.
	cf.scheduler = &healthScheduler{init: true}
	o.changefeedTicked = true
	err = o.handleQueries(query)
	require.NoError(t, err)
	require.True(t, query.Data.(bool))

	// Unhealthy, changefeeds are not ticked.
	o.changefeedTicked = false
	err = o.handleQueries(query)
	require.NoError(t, err)
	require.False(t, query.Data.(bool))

	// Unhealthy, there is another changefeed is not initialized.
	o.changefeeds[model.ChangeFeedID{ID: "1"}] = &changefeed{
		state: &orchestrator.ChangefeedReactorState{
			Info: &model.ChangeFeedInfo{State: model.StateNormal},
		},
		scheduler: &healthScheduler{init: false},
	}
	o.changefeedTicked = true
	err = o.handleQueries(query)
	require.NoError(t, err)
	require.False(t, query.Data.(bool))
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
	o.handleJobs(context.Background())

	require.Error(t, o.ValidateChangefeed(&model.ChangeFeedInfo{
		ID:        id.ID,
		Namespace: id.Namespace,
	}))
	require.Error(t, o.ValidateChangefeed(&model.ChangeFeedInfo{
		ID:        "unknown",
		Namespace: "unknown",
		SinkURI:   sinkURI,
	}))

	// Test invalid sink URI
	require.Error(t, o.ValidateChangefeed(&model.ChangeFeedInfo{
		SinkURI: "wrong uri\n\t",
	}))

	// Test limit passed.
	o.removedChangefeed[id] = time.Now().Add(-2 * recreateChangefeedDelayLimit)
	o.removedSinkURI[url.URL{
		Scheme: "mysql",
		Host:   "host:1234",
	}] = time.Now().Add(-2 * recreateChangefeedDelayLimit)

	require.Nil(t, o.ValidateChangefeed(&model.ChangeFeedInfo{
		ID:        id.ID,
		Namespace: id.Namespace,
	}))
	require.Nil(t, o.ValidateChangefeed(&model.ChangeFeedInfo{
		ID:        "unknown",
		Namespace: "unknown",
		SinkURI:   sinkURI,
	}))

	// Test GC.
	o.handleJobs(context.Background())
	require.Equal(t, 0, len(o.removedChangefeed))
	require.Equal(t, 0, len(o.removedSinkURI))
}
>>>>>>> 1f6ae1fd8c (cdc: add delay for recreating changefeed (#7730))
