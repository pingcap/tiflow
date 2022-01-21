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
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

var _ = check.Suite(&feedStateManagerSuite{})

type feedStateManagerSuite struct {
}

func (s *feedStateManagerSuite) TestHandleJob(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test()
	state := model.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(c, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		c.Assert(info, check.IsNil)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		c.Assert(status, check.IsNil)
		return &model.ChangeFeedStatus{}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(state)
	tester.MustApplyPatches()
	c.Assert(manager.ShouldRunning(), check.IsTrue)

	// an admin job which of changefeed is not match
	manager.PushAdminJob(&model.AdminJob{
		CfID: "fake-changefeed-id",
		Type: model.AdminStop,
	})
	manager.Tick(state)
	tester.MustApplyPatches()
	c.Assert(manager.ShouldRunning(), check.IsTrue)

	// a running can not be resume
	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminResume,
	})
	manager.Tick(state)
	tester.MustApplyPatches()
	c.Assert(manager.ShouldRunning(), check.IsTrue)

	// stop a changefeed
	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminStop,
	})
	manager.Tick(state)
	tester.MustApplyPatches()
	c.Assert(manager.ShouldRunning(), check.IsFalse)
	c.Assert(state.Info.State, check.Equals, model.StateStopped)
	c.Assert(state.Info.AdminJobType, check.Equals, model.AdminStop)
	c.Assert(state.Status.AdminJobType, check.Equals, model.AdminStop)

	// resume a changefeed
	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminResume,
	})
	manager.Tick(state)
	tester.MustApplyPatches()
	c.Assert(manager.ShouldRunning(), check.IsTrue)
	c.Assert(state.Info.State, check.Equals, model.StateNormal)
	c.Assert(state.Info.AdminJobType, check.Equals, model.AdminNone)
	c.Assert(state.Status.AdminJobType, check.Equals, model.AdminNone)

	// remove a changefeed
	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminRemove,
	})
	manager.Tick(state)
	tester.MustApplyPatches()
	c.Assert(manager.ShouldRunning(), check.IsFalse)
	c.Assert(state.Exist(), check.IsFalse)
}

func (s *feedStateManagerSuite) TestMarkFinished(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test()
	state := model.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(c, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		c.Assert(info, check.IsNil)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		c.Assert(status, check.IsNil)
		return &model.ChangeFeedStatus{}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(state)
	tester.MustApplyPatches()
	c.Assert(manager.ShouldRunning(), check.IsTrue)

	manager.MarkFinished()
	manager.Tick(state)
	tester.MustApplyPatches()
	c.Assert(manager.ShouldRunning(), check.IsFalse)
	c.Assert(state.Info.State, check.Equals, model.StateFinished)
	c.Assert(state.Info.AdminJobType, check.Equals, model.AdminFinish)
	c.Assert(state.Status.AdminJobType, check.Equals, model.AdminFinish)
}

func (s *feedStateManagerSuite) TestCleanUpInfos(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test()
	state := model.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(c, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		c.Assert(info, check.IsNil)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		c.Assert(status, check.IsNil)
		return &model.ChangeFeedStatus{}, true, nil
	})
	state.PatchTaskStatus(ctx.GlobalVars().CaptureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return &model.TaskStatus{}, true, nil
	})
	state.PatchTaskPosition(ctx.GlobalVars().CaptureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		return &model.TaskPosition{}, true, nil
	})
	state.PatchTaskWorkload(ctx.GlobalVars().CaptureInfo.ID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		return model.TaskWorkload{}, true, nil
	})
	tester.MustApplyPatches()
	c.Assert(state.TaskStatuses, check.HasKey, ctx.GlobalVars().CaptureInfo.ID)
	c.Assert(state.TaskPositions, check.HasKey, ctx.GlobalVars().CaptureInfo.ID)
	c.Assert(state.Workloads, check.HasKey, ctx.GlobalVars().CaptureInfo.ID)
	manager.Tick(state)
	tester.MustApplyPatches()
	c.Assert(manager.ShouldRunning(), check.IsTrue)

	manager.MarkFinished()
	manager.Tick(state)
	tester.MustApplyPatches()
	c.Assert(manager.ShouldRunning(), check.IsFalse)
	c.Assert(state.Info.State, check.Equals, model.StateFinished)
	c.Assert(state.Info.AdminJobType, check.Equals, model.AdminFinish)
	c.Assert(state.Status.AdminJobType, check.Equals, model.AdminFinish)
	c.Assert(state.TaskStatuses, check.Not(check.HasKey), ctx.GlobalVars().CaptureInfo.ID)
	c.Assert(state.TaskPositions, check.Not(check.HasKey), ctx.GlobalVars().CaptureInfo.ID)
	c.Assert(state.Workloads, check.Not(check.HasKey), ctx.GlobalVars().CaptureInfo.ID)
}

func (s *feedStateManagerSuite) TestHandleError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test()
	state := model.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(c, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		c.Assert(info, check.IsNil)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		c.Assert(status, check.IsNil)
		return &model.ChangeFeedStatus{}, true, nil
	})
	state.PatchTaskStatus(ctx.GlobalVars().CaptureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return &model.TaskStatus{}, true, nil
	})

	state.PatchTaskWorkload(ctx.GlobalVars().CaptureInfo.ID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
		return model.TaskWorkload{}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(state)
	tester.MustApplyPatches()

	// the backoff will be stopped after 4600ms because 4600ms + 1600ms > 6000ms.
	intervals := []time.Duration{200, 400, 800, 1600, 1600}
	for i, d := range intervals {
		intervals[i] = d * time.Millisecond
	}

	for _, d := range intervals {
		c.Assert(manager.ShouldRunning(), check.IsTrue)
		state.PatchTaskPosition(ctx.GlobalVars().CaptureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Error: &model.RunningError{
				Addr:    ctx.GlobalVars().CaptureInfo.AdvertiseAddr,
				Code:    "[CDC:ErrEtcdSessionDone]",
				Message: "fake error for test",
			}}, true, nil
		})
		tester.MustApplyPatches()
		manager.Tick(state)
		tester.MustApplyPatches()
		c.Assert(manager.ShouldRunning(), check.IsFalse)
		time.Sleep(d)
		manager.Tick(state)
		tester.MustApplyPatches()
	}

	c.Assert(manager.ShouldRunning(), check.IsFalse)
	c.Assert(state.Info.State, check.Equals, model.StateFailed)
	c.Assert(state.Info.AdminJobType, check.Equals, model.AdminStop)
	c.Assert(state.Status.AdminJobType, check.Equals, model.AdminStop)

	// admin resume must retry changefeed immediately.
	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminResume,
		Opts: &model.AdminJobOption{ForceRemove: false},
	})
	manager.Tick(state)
	tester.MustApplyPatches()
	c.Assert(manager.ShouldRunning(), check.IsTrue)
	c.Assert(state.Info.State, check.Equals, model.StateNormal)
	c.Assert(state.Info.AdminJobType, check.Equals, model.AdminNone)
	c.Assert(state.Status.AdminJobType, check.Equals, model.AdminNone)
}

func (s *feedStateManagerSuite) TestChangefeedStatusNotExist(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test()
	state := model.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(c, state, map[string]string{
		"/tidb/cdc/capture/d563bfc0-f406-4f34-bc7d-6dc2e35a44e5": `{"id":"d563bfc0-f406-4f34-bc7d-6dc2e35a44e5","address":"172.16.6.147:8300","version":"v5.0.0-master-dirty"}`,
		"/tidb/cdc/changefeed/info/" + ctx.ChangefeedVars().ID:   `{"sink-uri":"blackhole:///","opts":{},"create-time":"2021-06-05T00:44:15.065939487+08:00","start-ts":425381670108266496,"target-ts":0,"admin-job-type":1,"sort-engine":"unified","config":{"case-sensitive":true,"enable-old-value":true,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}},"state":"failed","history":[],"error":{"addr":"172.16.6.147:8300","code":"CDC:ErrSnapshotLostByGC","message":"[CDC:ErrSnapshotLostByGC]fail to create or maintain changefeed due to snapshot loss caused by GC. checkpoint-ts 425381670108266496 is earlier than GC safepoint at 0"},"sync-point-enabled":false,"sync-point-interval":600000000000,"creator-version":"v5.0.0-master-dirty"}`,
		"/tidb/cdc/owner/156579d017f84a68":                       "d563bfc0-f406-4f34-bc7d-6dc2e35a44e5",
	})
	manager.Tick(state)
	c.Assert(manager.ShouldRunning(), check.IsFalse)
	tester.MustApplyPatches()

	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminRemove,
		Opts: &model.AdminJobOption{ForceRemove: true},
	})
	manager.Tick(state)
	c.Assert(manager.ShouldRunning(), check.IsFalse)
	tester.MustApplyPatches()
	c.Assert(state.Info, check.IsNil)
	c.Assert(state.Exist(), check.IsFalse)
}
