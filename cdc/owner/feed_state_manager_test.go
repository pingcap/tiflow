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
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/stretchr/testify/require"
)

func TestHandleJob(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test()
	state := orchestrator.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.Nil(t, status)
		return &model.ChangeFeedStatus{}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(state)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())

	// an admin job which of changefeed is not match
	manager.PushAdminJob(&model.AdminJob{
		CfID: "fake-changefeed-id",
		Type: model.AdminStop,
	})
	manager.Tick(state)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())

	// a running can not be resume
	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminResume,
	})
	manager.Tick(state)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())

	// stop a changefeed
	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminStop,
	})
	manager.Tick(state)
	tester.MustApplyPatches()

	require.False(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	require.Equal(t, state.Info.State, model.StateStopped)
	require.Equal(t, state.Info.AdminJobType, model.AdminStop)
	require.Equal(t, state.Status.AdminJobType, model.AdminStop)

	// resume a changefeed
	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminResume,
	})
	manager.Tick(state)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	require.Equal(t, state.Info.State, model.StateNormal)
	require.Equal(t, state.Info.AdminJobType, model.AdminNone)
	require.Equal(t, state.Status.AdminJobType, model.AdminNone)

	// remove a changefeed
	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminRemove,
	})
	manager.Tick(state)
	tester.MustApplyPatches()

	require.False(t, manager.ShouldRunning())
	require.True(t, manager.ShouldRemoved())
	require.False(t, state.Exist())
}

func TestMarkFinished(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test()
	state := orchestrator.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.Nil(t, status)
		return &model.ChangeFeedStatus{}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(state)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())

	manager.MarkFinished()
	manager.Tick(state)
	tester.MustApplyPatches()

	require.False(t, manager.ShouldRunning())
	require.Equal(t, state.Info.State, model.StateFinished)
	require.Equal(t, state.Info.AdminJobType, model.AdminFinish)
	require.Equal(t, state.Status.AdminJobType, model.AdminFinish)
}

func TestCleanUpInfos(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test()
	state := orchestrator.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.Nil(t, status)
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
	require.Contains(t, state.TaskStatuses, ctx.GlobalVars().CaptureInfo.ID)
	require.Contains(t, state.TaskPositions, ctx.GlobalVars().CaptureInfo.ID)
	require.Contains(t, state.Workloads, ctx.GlobalVars().CaptureInfo.ID)
	manager.Tick(state)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())

	manager.MarkFinished()
	manager.Tick(state)
	tester.MustApplyPatches()
	require.False(t, manager.ShouldRunning())
	require.Equal(t, state.Info.State, model.StateFinished)
	require.Equal(t, state.Info.AdminJobType, model.AdminFinish)
	require.Equal(t, state.Status.AdminJobType, model.AdminFinish)
	require.NotContains(t, state.TaskStatuses, ctx.GlobalVars().CaptureInfo.ID)
	require.NotContains(t, state.TaskPositions, ctx.GlobalVars().CaptureInfo.ID)
	require.NotContains(t, state.Workloads, ctx.GlobalVars().CaptureInfo.ID)
}

func TestHandleError(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test()
	state := orchestrator.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.Nil(t, status)
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
		require.True(t, manager.ShouldRunning())
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
		require.False(t, manager.ShouldRunning())
		time.Sleep(d)
		manager.Tick(state)
		tester.MustApplyPatches()
	}

	require.False(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	require.Equal(t, state.Info.State, model.StateFailed)
	require.Equal(t, state.Info.AdminJobType, model.AdminStop)
	require.Equal(t, state.Status.AdminJobType, model.AdminStop)

	// admin resume must retry changefeed immediately.
	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminResume,
		Opts: &model.AdminJobOption{ForceRemove: false},
	})
	manager.Tick(state)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	require.Equal(t, state.Info.State, model.StateNormal)
	require.Equal(t, state.Info.AdminJobType, model.AdminNone)
	require.Equal(t, state.Status.AdminJobType, model.AdminNone)
}

func TestHandleFastFailError(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := new(feedStateManager)
	state := orchestrator.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchTaskPosition(ctx.GlobalVars().CaptureInfo.ID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
		return &model.TaskPosition{Error: &model.RunningError{
			Addr:    ctx.GlobalVars().CaptureInfo.AdvertiseAddr,
			Code:    "CDC:ErrGCTTLExceeded",
			Message: "fake error for test",
		}}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(state)
	// test handling fast failed error with non-nil ChangeFeedInfo
	tester.MustApplyPatches()
	// test handling fast failed error with nil ChangeFeedInfo
	// set info to nil when this patch is applied
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return nil, true, nil
	})
	manager.Tick(state)
	// When the patches are applied, the callback function of PatchInfo in feedStateManager.HandleError will be called.
	// At that time, the nil pointer will be checked instead of throwing a panic. See issue #3128 for more detail.
	tester.MustApplyPatches()
}

func TestChangefeedStatusNotExist(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test()
	state := orchestrator.NewChangefeedReactorState(ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(t, state, map[string]string{
		"/tidb/cdc/capture/d563bfc0-f406-4f34-bc7d-6dc2e35a44e5": `{"id":"d563bfc0-f406-4f34-bc7d-6dc2e35a44e5","address":"172.16.6.147:8300","version":"v5.0.0-master-dirty"}`,
		"/tidb/cdc/changefeed/info/" + ctx.ChangefeedVars().ID:   `{"sink-uri":"blackhole:///","opts":{},"create-time":"2021-06-05T00:44:15.065939487+08:00","start-ts":425381670108266496,"target-ts":0,"admin-job-type":1,"sort-engine":"unified","config":{"case-sensitive":true,"enable-old-value":true,"force-replicate":false,"check-gc-safe-point":true,"filter":{"rules":["*.*"],"ignore-txn-start-ts":null},"mounter":{"worker-num":16},"sink":{"dispatchers":null,"protocol":"open-protocol"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1}},"state":"failed","history":[],"error":{"addr":"172.16.6.147:8300","code":"CDC:ErrSnapshotLostByGC","message":"[CDC:ErrSnapshotLostByGC]fail to create or maintain changefeed due to snapshot loss caused by GC. checkpoint-ts 425381670108266496 is earlier than GC safepoint at 0"},"sync-point-enabled":false,"sync-point-interval":600000000000,"creator-version":"v5.0.0-master-dirty"}`,
		"/tidb/cdc/owner/156579d017f84a68":                       "d563bfc0-f406-4f34-bc7d-6dc2e35a44e5",
	})
	manager.Tick(state)
	require.False(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	tester.MustApplyPatches()

	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminRemove,
		Opts: &model.AdminJobOption{ForceRemove: true},
	})
	manager.Tick(state)
	require.False(t, manager.ShouldRunning())
	require.True(t, manager.ShouldRemoved())
	tester.MustApplyPatches()
	require.Nil(t, state.Info)
	require.False(t, state.Exist())
}
