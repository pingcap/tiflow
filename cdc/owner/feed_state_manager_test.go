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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/stretchr/testify/require"
)

func TestHandleJob(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		ctx.ChangefeedVars().ID)
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
		CfID: model.DefaultChangeFeedID("fake-changefeed-id"),
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

func TestResumeChangefeedWithCheckpointTs(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		ctx.ChangefeedVars().ID)
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

	// resume the changefeed in stopped state
	manager.PushAdminJob(&model.AdminJob{
		CfID:                  ctx.ChangefeedVars().ID,
		Type:                  model.AdminResume,
		OverwriteCheckpointTs: 100,
	})
	manager.Tick(state)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	require.Equal(t, state.Info.State, model.StateNormal)
	require.Equal(t, state.Info.AdminJobType, model.AdminNone)
	require.Equal(t, state.Status.AdminJobType, model.AdminNone)

	// mock a non-retryable error occurs for this changefeed
	state.PatchTaskPosition(ctx.GlobalVars().CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Error: &model.RunningError{
				Addr:    ctx.GlobalVars().CaptureInfo.AdvertiseAddr,
				Code:    "CDC:ErrGCTTLExceeded",
				Message: "fake error for test",
			}}, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(state)
	tester.MustApplyPatches()
	require.Equal(t, state.Info.State, model.StateFailed)
	require.Equal(t, state.Info.AdminJobType, model.AdminStop)
	require.Equal(t, state.Status.AdminJobType, model.AdminStop)

	// resume the changefeed in failed state
	manager.PushAdminJob(&model.AdminJob{
		CfID:                  ctx.ChangefeedVars().ID,
		Type:                  model.AdminResume,
		OverwriteCheckpointTs: 200,
	})
	manager.Tick(state)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	require.Equal(t, state.Info.State, model.StateNormal)
	require.Equal(t, state.Info.AdminJobType, model.AdminNone)
	require.Equal(t, state.Status.AdminJobType, model.AdminNone)
}

func TestMarkFinished(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		ctx.ChangefeedVars().ID)
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
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.Nil(t, status)
		return &model.ChangeFeedStatus{}, true, nil
	})
	state.PatchTaskPosition(ctx.GlobalVars().CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{}, true, nil
		})
	tester.MustApplyPatches()
	require.Contains(t, state.TaskPositions, ctx.GlobalVars().CaptureInfo.ID)
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
	require.NotContains(t, state.TaskPositions, ctx.GlobalVars().CaptureInfo.ID)
}

func TestHandleError(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		ctx.ChangefeedVars().ID)
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

	intervals := []time.Duration{200, 400, 800, 1600, 1600}
	for i, d := range intervals {
		intervals[i] = d * time.Millisecond
	}

	for _, d := range intervals {
		require.True(t, manager.ShouldRunning())
		state.PatchTaskPosition(ctx.GlobalVars().CaptureInfo.ID,
			func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
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
		require.Equal(t, state.Info.State, model.StateError)
		require.Equal(t, state.Info.AdminJobType, model.AdminStop)
		require.Equal(t, state.Status.AdminJobType, model.AdminStop)
		time.Sleep(d)
		manager.Tick(state)
		tester.MustApplyPatches()
	}
}

func TestHandleFastFailError(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := new(feedStateManager)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchTaskPosition(ctx.GlobalVars().CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
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

func TestHandleErrorWhenChangefeedIsPaused(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test(0, 0, 0, 0)
	manager.state = orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		ctx.ChangefeedVars().ID)
	err := &model.RunningError{
		Addr:    ctx.GlobalVars().CaptureInfo.AdvertiseAddr,
		Code:    "CDC:ErrReachMaxTry",
		Message: "fake error for test",
	}
	manager.state.Info = &model.ChangeFeedInfo{
		State: model.StateStopped,
	}
	manager.handleError(err)
	require.Equal(t, model.StateStopped, manager.state.Info.State)
}

func TestChangefeedStatusNotExist(t *testing.T) {
	changefeedInfo := `
{
    "sink-uri": "blackhole:///",
    "create-time": "2021-06-05T00:44:15.065939487+08:00",
    "start-ts": 425381670108266496,
    "target-ts": 0,
    "admin-job-type": 1,
    "sort-engine": "unified",
    "config": {
        "case-sensitive": true,
        "enable-old-value": true,
        "force-replicate": false,
        "check-gc-safe-point": true,
        "filter": {
            "rules": [
                "*.*"
            ],
            "ignore-txn-start-ts": null
        },
        "mounter": {
            "worker-num": 16
        },
        "sink": {
            "dispatchers": null,
            "protocol": "open-protocol"
        }
    },
    "state": "failed",
    "history": [],
    "error": {
        "addr": "172.16.6.147:8300",
        "code": "CDC:ErrSnapshotLostByGC",
        "message": ` + "\"[CDC:ErrSnapshotLostByGC]fail to create or maintain changefeed " +
		"due to snapshot loss caused by GC. checkpoint-ts 425381670108266496 " +
		"is earlier than GC safepoint at 0\"" + `
    },
    "sync-point-enabled": false,
    "sync-point-interval": 600000000000,
    "creator-version": "v5.0.0-master-dirty"
}
`
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(t, state, map[string]string{
		fmt.Sprintf("%s/capture/d563bfc0-f406-4f34-bc7d-6dc2e35a44e5",
			etcd.DefaultClusterAndMetaPrefix): `
{"id":"d563bfc0-f406-4f34-bc7d-6dc2e35a44e5",
"address":"172.16.6.147:8300","version":"v5.0.0-master-dirty"}`,
		fmt.Sprintf("%s/changefeed/info/",
			etcd.DefaultClusterAndNamespacePrefix) +
			ctx.ChangefeedVars().ID.ID: changefeedInfo,
		fmt.Sprintf("%s/owner/156579d017f84a68",
			etcd.DefaultClusterAndMetaPrefix,
		): "d563bfc0-f406-4f34-bc7d-6dc2e35a44e5",
	})
	manager.Tick(state)
	require.False(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	tester.MustApplyPatches()

	manager.PushAdminJob(&model.AdminJob{
		CfID: ctx.ChangefeedVars().ID,
		Type: model.AdminRemove,
	})
	manager.Tick(state)
	require.False(t, manager.ShouldRunning())
	require.True(t, manager.ShouldRemoved())
	tester.MustApplyPatches()
	require.Nil(t, state.Info)
	require.False(t, state.Exist())
}

func TestChangefeedNotRetry(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		ctx.ChangefeedVars().ID)
	tester := orchestrator.NewReactorStateTester(t, state, nil)

	// changefeed state normal
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}, State: model.StateNormal}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(state)
	require.True(t, manager.ShouldRunning())

	// changefeed in error state but error can be retried
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return &model.ChangeFeedInfo{
			SinkURI: "123",
			Config:  &config.ReplicaConfig{},
			State:   model.StateError,
			Error: &model.RunningError{
				Addr: "127.0.0.1",
				Code: "CDC:ErrPipelineTryAgain",
				Message: "pipeline is full, please try again. Internal use only, " +
					"report a bug if seen externally",
			},
		}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(state)
	require.True(t, manager.ShouldRunning())

	// changefeed in error state and error can't be retried
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return &model.ChangeFeedInfo{
			SinkURI: "123",
			Config:  &config.ReplicaConfig{},
			State:   model.StateError,
			Error: &model.RunningError{
				Addr:    "127.0.0.1",
				Code:    "CDC:ErrExpressionColumnNotFound",
				Message: "what ever",
			},
		}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(state)
	require.False(t, manager.ShouldRunning())

	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return &model.ChangeFeedInfo{
			SinkURI: "123",
			Config:  &config.ReplicaConfig{},
			State:   model.StateError,
			Error: &model.RunningError{
				Addr:    "127.0.0.1",
				Code:    string(cerror.ErrExpressionColumnNotFound.RFCCode()),
				Message: cerror.ErrExpressionColumnNotFound.Error(),
			},
		}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(state)
	// should be false
	require.False(t, manager.ShouldRunning())

	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return &model.ChangeFeedInfo{
			SinkURI: "123",
			Config:  &config.ReplicaConfig{},
			State:   model.StateError,
			Error: &model.RunningError{
				Addr:    "127.0.0.1",
				Code:    string(cerror.ErrExpressionParseFailed.RFCCode()),
				Message: cerror.ErrExpressionParseFailed.Error(),
			},
		}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(state)
	// should be false
	require.False(t, manager.ShouldRunning())
}

func TestBackoffStopsUnexpectedly(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	// after 4000ms, the backoff will stop
	manager := newFeedStateManager4Test(500, 500, 4000, 1.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		ctx.ChangefeedVars().ID)
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

	for i := 1; i <= 10; i++ {
		require.Equal(t, state.Info.State, model.StateNormal)
		require.True(t, manager.ShouldRunning())
		state.PatchTaskPosition(ctx.GlobalVars().CaptureInfo.ID,
			func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
				return &model.TaskPosition{Error: &model.RunningError{
					Addr:    ctx.GlobalVars().CaptureInfo.AdvertiseAddr,
					Code:    "[CDC:ErrEtcdSessionDone]",
					Message: "fake error for test",
				}}, true, nil
			})
		tester.MustApplyPatches()
		manager.Tick(state)
		tester.MustApplyPatches()
		// after round 8, the maxElapsedTime of backoff will exceed 4000ms,
		// and NextBackOff() will return -1, so the changefeed state will
		// never turn into error state.
		if i >= 8 {
			require.True(t, manager.ShouldRunning())
			require.Equal(t, state.Info.State, model.StateNormal)
		} else {
			require.False(t, manager.ShouldRunning())
			require.Equal(t, state.Info.State, model.StateError)
			require.Equal(t, state.Info.AdminJobType, model.AdminStop)
			require.Equal(t, state.Status.AdminJobType, model.AdminStop)
		}
		// 500ms is the backoff interval, so sleep 500ms and after a manager tick,
		// the changefeed will turn into normal state
		time.Sleep(500 * time.Millisecond)
		manager.Tick(state)
		tester.MustApplyPatches()
	}
}

func TestBackoffNeverStops(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(true)
	// the backoff will never stop
	manager := newFeedStateManager4Test(100, 100, 0, 1.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		ctx.ChangefeedVars().ID)
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

	for i := 1; i <= 30; i++ {
		require.Equal(t, state.Info.State, model.StateNormal)
		require.True(t, manager.ShouldRunning())
		state.PatchTaskPosition(ctx.GlobalVars().CaptureInfo.ID,
			func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
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
		require.Equal(t, state.Info.State, model.StateError)
		require.Equal(t, state.Info.AdminJobType, model.AdminStop)
		require.Equal(t, state.Status.AdminJobType, model.AdminStop)
		// 100ms is the backoff interval, so sleep 100ms and after a manager tick,
		// the changefeed will turn into normal state
		time.Sleep(100 * time.Millisecond)
		manager.Tick(state)
		tester.MustApplyPatches()
	}
}
