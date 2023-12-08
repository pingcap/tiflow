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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/sink/observer"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"golang.org/x/time/rate"
)

type mockManager struct {
	gc.Manager
}

func (m *mockManager) CheckStaleCheckpointTs(
	ctx context.Context, changefeedID model.ChangeFeedID, checkpointTs model.Ts,
) error {
	return cerror.ErrStartTsBeforeGC.GenWithStackByArgs()
}

var _ gc.Manager = (*mockManager)(nil)

// newOwner4Test creates a new Owner for test
func newOwner4Test(
	newDDLPuller func(ctx context.Context,
		up *upstream.Upstream,
		startTs uint64,
		changefeed model.ChangeFeedID,
		schemaStorage entry.SchemaStorage,
		filter filter.Filter,
	) puller.DDLPuller,
	newSink func(model.ChangeFeedID, *model.ChangeFeedInfo, func(error), func(error)) DDLSink,
	newScheduler func(
		ctx cdcContext.Context, up *upstream.Upstream, changefeedEpoch uint64,
		cfg *config.SchedulerConfig, redoMetaManager redo.MetaManager,
	) (scheduler.Scheduler, error),
	newDownstreamObserver func(
		ctx context.Context, changefeedID model.ChangeFeedID, sinkURIStr string, replCfg *config.ReplicaConfig,
		opts ...observer.NewObserverOption,
	) (observer.Observer, error),
	pdClient pd.Client,
	etcdClient etcd.CDCEtcdClient,
) Owner {
	m := upstream.NewManager4Test(pdClient)
	o := NewOwner(m, config.NewDefaultSchedulerConfig(), etcdClient).(*ownerImpl)
	o.newChangefeed = func(
		id model.ChangeFeedID,
		cfInfo *model.ChangeFeedInfo,
		cfStatus *model.ChangeFeedStatus,
		cfstateManager FeedStateManager,
		up *upstream.Upstream,
		cfg *config.SchedulerConfig,
	) *changefeed {
		return newChangefeed4Test(id, cfInfo, cfStatus, cfstateManager, up, newDDLPuller, newSink,
			newScheduler, newDownstreamObserver)
	}
	return o
}

func createOwner4Test(ctx cdcContext.Context, t *testing.T) (*ownerImpl, *orchestrator.GlobalReactorState, *orchestrator.ReactorStateTester) {
	pdClient := &gc.MockPDClient{
		UpdateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
			return safePoint, nil
		},
	}

	owner := newOwner4Test(
		// new ddl puller
		func(ctx context.Context,
			up *upstream.Upstream,
			startTs uint64,
			changefeed model.ChangeFeedID,
			schemaStorage entry.SchemaStorage,
			filter filter.Filter,
		) puller.DDLPuller {
			return &mockDDLPuller{resolvedTs: startTs - 1}
		},
		// new ddl sink
		func(model.ChangeFeedID, *model.ChangeFeedInfo, func(error), func(error)) DDLSink {
			return &mockDDLSink{}
		},
		// new scheduler
		func(
			ctx cdcContext.Context, up *upstream.Upstream, changefeedEpoch uint64,
			cfg *config.SchedulerConfig, redoMetaAManager redo.MetaManager,
		) (scheduler.Scheduler, error) {
			return &mockScheduler{}, nil
		},
		// new downstream observer
		func(
			ctx context.Context, chnagefeedID model.ChangeFeedID,
			sinkURIStr string, replCfg *config.ReplicaConfig,
			opts ...observer.NewObserverOption,
		) (observer.Observer, error) {
			return observer.NewDummyObserver(), nil
		},
		pdClient,
		nil,
	)
	o := owner.(*ownerImpl)
	o.upstreamManager = upstream.NewManager4Test(pdClient)

	state := orchestrator.NewGlobalStateForTest(etcd.DefaultCDCClusterID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)

	// set captures
	cdcKey := etcd.CDCKey{
		ClusterID: state.ClusterID,
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
		ClusterID:    state.ClusterID,
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
		Error: nil,
	}

	// this will make changefeed always meet ErrStartTsBeforeGC
	up, _ := owner.upstreamManager.Get(changefeedInfo.UpstreamID)
	mockedManager := &mockManager{Manager: up.GCManager}
	up.GCManager = mockedManager
	err = up.GCManager.CheckStaleCheckpointTs(ctx, changefeedID, 0)
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
		ClusterID:    state.ClusterID,
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
	require.NotNil(t, owner.changefeeds[cf3])

	// make sure statusProvider works well
	ctx1, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var infos map[model.ChangeFeedID]*model.ChangeFeedInfo
	done := make(chan struct{})
	go func() {
		info1, err := statusProvider.GetChangeFeedInfo(ctx1, cf1)
		require.Nil(t, err)
		info2, err := statusProvider.GetChangeFeedInfo(ctx1, cf2)
		require.Nil(t, err)
		info3, err := statusProvider.GetChangeFeedInfo(ctx1, cf3)
		require.Nil(t, err)
		infos = map[model.ChangeFeedID]*model.ChangeFeedInfo{
			cf1: info1,
			cf2: info2,
			cf3: info3,
		}
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
	require.NotNil(t, infos[cf1])
	require.NotNil(t, infos[cf2])
	require.NotNil(t, infos[cf3])
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

	state := &orchestrator.ChangefeedReactorState{
		Info: &model.ChangeFeedInfo{State: model.StateNormal},
	}
	cf := &changefeed{
		scheduler:    nil, // scheduler is not set.
		latestStatus: state.Status,
		latestInfo:   state.Info,
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
	state.Info.State = model.StateStopped
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
	state := &orchestrator.ChangefeedReactorState{
		Info: &model.ChangeFeedInfo{State: model.StateStopped},
	}
	cf.latestInfo = state.Info
	cf.latestStatus = state.Status
	err = o.handleQueries(query)
	require.NoError(t, err)
	require.True(t, query.Data.(bool))

	// 2 changefeeds, another is normal, and scheduler initialized.
	o.changefeeds[model.ChangeFeedID{ID: "2"}] = &changefeed{
		latestInfo: &model.ChangeFeedInfo{State: model.StateNormal},
		scheduler:  &healthScheduler{init: true},
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
		latestInfo: &model.ChangeFeedInfo{State: model.StateNormal},
		scheduler:  nil, // scheduler is not set.
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
		latestInfo: &model.ChangeFeedInfo{State: model.StateNormal},
		scheduler:  &healthScheduler{init: false},
	}
	o.changefeedTicked = true
	err = o.handleQueries(query)
	require.NoError(t, err)
	require.False(t, query.Data.(bool))
}
