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
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

var _ = check.Suite(&ownerSuite{})

type ownerSuite struct{}

type mockGcManager struct {
	GcManager
}

func (m *mockGcManager) checkStaleCheckpointTs(ctx cdcContext.Context, changefeedID model.ChangeFeedID, checkpointTs model.Ts) error {
	return cerror.ErrGCTTLExceeded.GenWithStackByArgs()
}

var _ GcManager = &mockGcManager{}

func createOwner4Test(ctx cdcContext.Context, c *check.C) (*Owner, *model.GlobalReactorState, *orchestrator.ReactorStateTester) {
	ctx.GlobalVars().PDClient = &mockPDClient{updateServiceGCSafePointFunc: func(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
		return safePoint, nil
	}}
	cf := NewOwner4Test(func(ctx cdcContext.Context, startTs uint64) (DDLPuller, error) {
		return &mockDDLPuller{resolvedTs: startTs - 1}, nil
	}, func(ctx cdcContext.Context) (AsyncSink, error) {
		return &mockAsyncSink{}, nil
	})
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
		StartTs: oracle.ComposeTS(oracle.GetPhysical(time.Now()), 0),
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
	mockedGcManager := &mockGcManager{GcManager: owner.gcManager}
	owner.gcManager = mockedGcManager

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
		StartTs: oracle.ComposeTS(oracle.GetPhysical(time.Now()), 0),
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

func (s *ownerSuite) TestCheckClusterVersion(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	owner, state, tester := createOwner4Test(ctx, c)
	tester.MustUpdate("/tidb/cdc/capture/6bbc01c8-0605-4f86-a0f9-b3119109b225", []byte(`{"id":"6bbc01c8-0605-4f86-a0f9-b3119109b225","address":"127.0.0.1:8300","version":"v6.0.0"}`))

	changefeedID := "test-changefeed"
	changefeedInfo := &model.ChangeFeedInfo{
		StartTs: oracle.ComposeTS(oracle.GetPhysical(time.Now()), 0),
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
