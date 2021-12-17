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
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/tikv/client-go/v2/oracle"
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

func createOwner4Test(ctx cdcContext.Context, c *check.C) (*Owner, *orchestrator.GlobalReactorState, *orchestrator.ReactorStateTester) {
	ctx.GlobalVars().PDClient = &gc.MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return safePoint, nil
		},
	}
	owner := NewOwner4Test(func(ctx cdcContext.Context, startTs uint64) (DDLPuller, error) {
		return &mockDDLPuller{resolvedTs: startTs - 1}, nil
	}, func() DDLSink {
		return &mockDDLSink{}
	},
		ctx.GlobalVars().PDClient,
	)
	state := orchestrator.NewGlobalState()
	tester := orchestrator.NewReactorStateTester(c, state, nil)

	// set captures
	cdcKey := etcd.CDCKey{
		Tp:        etcd.CDCKeyTypeCapture,
		CaptureID: ctx.GlobalVars().CaptureInfo.ID,
	}
	captureBytes, err := ctx.GlobalVars().CaptureInfo.Marshal()
	c.Assert(err, check.IsNil)
	tester.MustUpdate(cdcKey.String(), captureBytes)
	return owner, state, tester
}

func (s *ownerSuite) TestCreateRemoveChangefeed(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()

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
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()

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
		StartTs:        oracle.GoTimeToTS(time.Now()),
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
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()

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
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()

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
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()
	state := orchestrator.NewGlobalState()
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

// make sure handleJobs works well even if there is two different
// version of captures in the cluster
func (s *ownerSuite) TestHandleJobsDontBlock(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()
	owner, state, tester := createOwner4Test(ctx, c)

	statusProvider := owner.StatusProvider()
	// work well
	cf1 := "test-changefeed"
	cfInfo1 := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateNormal,
	}
	changefeedStr, err := cfInfo1.Marshal()
	c.Assert(err, check.IsNil)
	cdcKey := etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: cf1,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr))
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	c.Assert(err, check.IsNil)

	c.Assert(owner.changefeeds, check.HasKey, cf1)

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
	c.Assert(err, check.IsNil)
	tester.MustUpdate(cdcKey.String(), v)

	// try to add another changefeed
	cf2 := "test-changefeed1"
	cfInfo2 := &model.ChangeFeedInfo{
		StartTs: oracle.GoTimeToTS(time.Now()),
		Config:  config.GetDefaultReplicaConfig(),
		State:   model.StateNormal,
	}
	changefeedStr1, err := cfInfo2.Marshal()
	c.Assert(err, check.IsNil)
	cdcKey = etcd.CDCKey{
		Tp:           etcd.CDCKeyTypeChangefeedInfo,
		ChangefeedID: cf2,
	}
	tester.MustUpdate(cdcKey.String(), []byte(changefeedStr1))
	_, err = owner.Tick(ctx, state)
	tester.MustApplyPatches()
	c.Assert(err, check.IsNil)
	// make sure this changefeed add failed, which means that owner are return
	// in clusterVersionConsistent check
	c.Assert(owner.changefeeds[cf2], check.IsNil)

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
			c.Fatal(ctx1.Err())
		case <-ticker.C:
			_, err = owner.Tick(ctx, state)
			c.Assert(err, check.IsNil)
		}
	}
	c.Assert(errIn, check.IsNil)
	c.Assert(infos[cf1], check.NotNil)
	c.Assert(infos[cf2], check.IsNil)
}
