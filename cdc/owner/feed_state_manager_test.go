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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type mockPD struct {
	pd.Client

	getTs func() (int64, int64, error)
}

func (p *mockPD) GetTS(_ context.Context) (int64, int64, error) {
	if p.getTs != nil {
		return p.getTs()
	}
	return 1, 2, nil
}

// newFeedStateManager4Test creates feedStateManager for test
func newFeedStateManager4Test(
	initialIntervalInMs, maxIntervalInMs, maxElapsedTimeInMs int,
	multiplier float64,
) *feedStateManager {
	f := new(feedStateManager)
	f.upstream = new(upstream.Upstream)
	f.upstream.PDClient = &mockPD{}
	f.upstream.PDClock = pdutil.NewClock4Test()

	f.errBackoff = backoff.NewExponentialBackOff()
	f.errBackoff.InitialInterval = time.Duration(initialIntervalInMs) * time.Millisecond
	f.errBackoff.MaxInterval = time.Duration(maxIntervalInMs) * time.Millisecond
	f.errBackoff.MaxElapsedTime = time.Duration(maxElapsedTimeInMs) * time.Millisecond
	f.errBackoff.Multiplier = multiplier
	f.errBackoff.RandomizationFactor = 0

	f.resetErrRetry()

	f.changefeedErrorStuckDuration = time.Second * 3

	return f
}

func TestHandleJob(t *testing.T) {
	_, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
	manager.state = state
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
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())

	// an admin job which of changefeed is not match
	manager.PushAdminJob(&model.AdminJob{
		CfID: model.DefaultChangeFeedID("fake-changefeed-id"),
		Type: model.AdminStop,
	})
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())

	// a running can not be resume
	manager.PushAdminJob(&model.AdminJob{
		CfID: model.DefaultChangeFeedID(changefeedInfo.ID),
		Type: model.AdminResume,
	})
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())

	// stop a changefeed
	manager.PushAdminJob(&model.AdminJob{
		CfID: model.DefaultChangeFeedID(changefeedInfo.ID),
		Type: model.AdminStop,
	})
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()

	require.False(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	require.Equal(t, state.Info.State, model.StateStopped)
	require.Equal(t, state.Info.AdminJobType, model.AdminStop)
	require.Equal(t, state.Status.AdminJobType, model.AdminStop)

	// resume a changefeed
	manager.PushAdminJob(&model.AdminJob{
		CfID: model.DefaultChangeFeedID(changefeedInfo.ID),
		Type: model.AdminResume,
	})
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	require.Equal(t, state.Info.State, model.StateNormal)
	require.Equal(t, state.Info.AdminJobType, model.AdminNone)
	require.Equal(t, state.Status.AdminJobType, model.AdminNone)

	// remove a changefeed
	manager.PushAdminJob(&model.AdminJob{
		CfID: model.DefaultChangeFeedID(changefeedInfo.ID),
		Type: model.AdminRemove,
	})
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()

	require.False(t, manager.ShouldRunning())
	require.True(t, manager.ShouldRemoved())
	require.False(t, state.Exist())
}

func TestResumeChangefeedWithCheckpointTs(t *testing.T) {
	globalVars, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
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
	manager.state = state
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())

	// stop a changefeed
	manager.PushAdminJob(&model.AdminJob{
		CfID: model.DefaultChangeFeedID(changefeedInfo.ID),
		Type: model.AdminStop,
	})
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()

	require.False(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	require.Equal(t, state.Info.State, model.StateStopped)
	require.Equal(t, state.Info.AdminJobType, model.AdminStop)
	require.Equal(t, state.Status.AdminJobType, model.AdminStop)

	// resume the changefeed in stopped state
	manager.PushAdminJob(&model.AdminJob{
		CfID:                  model.DefaultChangeFeedID(changefeedInfo.ID),
		Type:                  model.AdminResume,
		OverwriteCheckpointTs: 100,
	})
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	require.Equal(t, state.Info.State, model.StateNormal)
	require.Equal(t, state.Info.AdminJobType, model.AdminNone)
	require.Equal(t, state.Status.AdminJobType, model.AdminNone)

	// mock a non-retryable error occurs for this changefeed
	state.PatchTaskPosition(globalVars.CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Error: &model.RunningError{
				Addr:    globalVars.CaptureInfo.AdvertiseAddr,
				Code:    "CDC:ErrStartTsBeforeGC",
				Message: "fake error for test",
			}}, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.Equal(t, state.Info.State, model.StateFailed)
	require.Equal(t, state.Info.AdminJobType, model.AdminStop)
	require.Equal(t, state.Status.AdminJobType, model.AdminStop)

	// resume the changefeed in failed state
	manager.isRetrying = true
	manager.PushAdminJob(&model.AdminJob{
		CfID:                  model.DefaultChangeFeedID(changefeedInfo.ID),
		Type:                  model.AdminResume,
		OverwriteCheckpointTs: 200,
	})
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	require.Equal(t, state.Info.State, model.StateNormal)
	require.Equal(t, state.Info.AdminJobType, model.AdminNone)
	require.Equal(t, state.Status.AdminJobType, model.AdminNone)
	require.False(t, manager.isRetrying)
}

func TestMarkFinished(t *testing.T) {
	_, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
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
	manager.state = state
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())

	manager.MarkFinished()
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()

	require.False(t, manager.ShouldRunning())
	require.Equal(t, state.Info.State, model.StateFinished)
	require.Equal(t, state.Info.AdminJobType, model.AdminFinish)
	require.Equal(t, state.Status.AdminJobType, model.AdminFinish)
}

func TestCleanUpInfos(t *testing.T) {
	globalVars, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.Nil(t, status)
		return &model.ChangeFeedStatus{}, true, nil
	})
	state.PatchTaskPosition(globalVars.CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{}, true, nil
		})
	tester.MustApplyPatches()
	require.Contains(t, state.TaskPositions, globalVars.CaptureInfo.ID)
	manager.state = state
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())

	manager.MarkFinished()
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.False(t, manager.ShouldRunning())
	require.Equal(t, state.Info.State, model.StateFinished)
	require.Equal(t, state.Info.AdminJobType, model.AdminFinish)
	require.Equal(t, state.Status.AdminJobType, model.AdminFinish)
	require.NotContains(t, state.TaskPositions, globalVars.CaptureInfo.ID)
}

func TestHandleError(t *testing.T) {
	globalVars, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.Nil(t, status)
		return &model.ChangeFeedStatus{
			CheckpointTs: 200,
		}, true, nil
	})

	tester.MustApplyPatches()
	manager.state = state
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()

	intervals := []time.Duration{200, 400, 800, 1600, 1600}
	for i, d := range intervals {
		intervals[i] = d * time.Millisecond
	}

	for _, d := range intervals {
		require.True(t, manager.ShouldRunning())
		state.PatchTaskPosition(globalVars.CaptureInfo.ID,
			func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
				return &model.TaskPosition{Error: &model.RunningError{
					Addr:    globalVars.CaptureInfo.AdvertiseAddr,
					Code:    "[CDC:ErrEtcdSessionDone]",
					Message: "fake error for test",
				}}, true, nil
			})
		tester.MustApplyPatches()
		manager.Tick(0, state.Status, state.Info)
		tester.MustApplyPatches()
		require.False(t, manager.ShouldRunning())
		require.Equal(t, state.Info.State, model.StatePending)
		require.Equal(t, state.Info.AdminJobType, model.AdminStop)
		require.Equal(t, state.Status.AdminJobType, model.AdminStop)
		time.Sleep(d)
		manager.Tick(0, state.Status, state.Info)
		tester.MustApplyPatches()
	}

	// no error tick, state should be transferred from pending to warning
	manager.Tick(0, state.Status, state.Info)
	require.True(t, manager.ShouldRunning())
	require.Equal(t, model.StateWarning, state.Info.State)
	require.Equal(t, model.AdminNone, state.Info.AdminJobType)
	require.Equal(t, model.AdminNone, state.Status.AdminJobType)

	// no error tick and checkpointTs is progressing,
	// state should be transferred from warning to normal
	state.PatchStatus(
		func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			status.CheckpointTs += 1
			return status, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.True(t, manager.ShouldRunning())
	state.PatchStatus(
		func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
			status.CheckpointTs += 1
			return status, true, nil
		})
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.Equal(t, model.StateNormal, state.Info.State)
	require.Equal(t, model.AdminNone, state.Info.AdminJobType)
	require.Equal(t, model.AdminNone, state.Status.AdminJobType)
}

func TestHandleFastFailError(t *testing.T) {
	globalVars, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	manager := newFeedStateManager4Test(0, 0, 0, 0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchTaskPosition(globalVars.CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Error: &model.RunningError{
				Addr:    globalVars.CaptureInfo.AdvertiseAddr,
				Code:    "CDC:ErrStartTsBeforeGC",
				Message: "fake error for test",
			}}, true, nil
		})
	tester.MustApplyPatches()
	manager.state = state
	manager.Tick(0, state.Status, state.Info)
	// test handling fast failed error with non-nil ChangeFeedInfo
	tester.MustApplyPatches()
	// test handling fast failed error with nil ChangeFeedInfo
	// set info to nil when this patch is applied
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return nil, true, nil
	})
	manager.Tick(0, state.Status, state.Info)
	// When the patches are applied, the callback function of PatchInfo in feedStateManager.HandleError will be called.
	// At that time, the nil pointer will be checked instead of throwing a panic. See issue #3128 for more detail.
	tester.MustApplyPatches()
}

func TestHandleErrorWhenChangefeedIsPaused(t *testing.T) {
	globalVars, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	manager := newFeedStateManager4Test(0, 0, 0, 0)
	manager.state = orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
	err := &model.RunningError{
		Addr:    globalVars.CaptureInfo.AdvertiseAddr,
		Code:    "CDC:ErrReachMaxTry",
		Message: "fake error for test",
	}
	manager.state.(*orchestrator.ChangefeedReactorState).Info = &model.ChangeFeedInfo{
		State: model.StateStopped,
	}
	manager.HandleError(err)
	require.Equal(t, model.StateStopped, manager.state.(*orchestrator.ChangefeedReactorState).Info.State)
}

func TestChangefeedStatusNotExist(t *testing.T) {
	changefeedInfo := `
{
    "ddlSink-uri": "blackhole:///",
    "create-time": "2021-06-05T00:44:15.065939487+08:00",
    "start-ts": 425381670108266496,
    "target-ts": 0,
    "admin-job-type": 1,
    "sort-engine": "unified",
    "config": {
        "case-sensitive": true,
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
        "ddlSink": {
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
		"due to snapshot loss caused by GC. tableCheckpoint-ts 425381670108266496 " +
		"is earlier than GC safepoint at 0\"" + `
    },
    "sync-point-enabled": false,
    "sync-point-interval": 600000000000,
    "creator-version": "v5.0.0-master-dirty"
}
`
	_, changefeedConfig := vars.NewGlobalVarsAndChangefeedInfo4Test()
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedConfig.ID))
	tester := orchestrator.NewReactorStateTester(t, state, map[string]string{
		fmt.Sprintf("%s/capture/d563bfc0-f406-4f34-bc7d-6dc2e35a44e5",
			etcd.DefaultClusterAndMetaPrefix): `
{"id":"d563bfc0-f406-4f34-bc7d-6dc2e35a44e5",
"address":"172.16.6.147:8300","version":"v5.0.0-master-dirty"}`,
		fmt.Sprintf("%s/changefeed/info/",
			etcd.DefaultClusterAndNamespacePrefix) +
			changefeedConfig.ID: changefeedInfo,
		fmt.Sprintf("%s/owner/156579d017f84a68",
			etcd.DefaultClusterAndMetaPrefix,
		): "d563bfc0-f406-4f34-bc7d-6dc2e35a44e5",
	})
	manager.state = state
	manager.Tick(0, state.Status, state.Info)
	require.False(t, manager.ShouldRunning())
	require.False(t, manager.ShouldRemoved())
	tester.MustApplyPatches()

	manager.PushAdminJob(&model.AdminJob{
		CfID: model.DefaultChangeFeedID(changefeedConfig.ID),
		Type: model.AdminRemove,
	})
	manager.Tick(0, state.Status, state.Info)
	require.False(t, manager.ShouldRunning())
	require.True(t, manager.ShouldRemoved())
	tester.MustApplyPatches()
	require.Nil(t, state.Info)
	require.False(t, state.Exist())
}

func TestChangefeedNotRetry(t *testing.T) {
	_, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
	tester := orchestrator.NewReactorStateTester(t, state, nil)

	// changefeed state normal
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}, State: model.StateNormal}, true, nil
	})
	tester.MustApplyPatches()
	manager.state = state
	manager.Tick(0, state.Status, state.Info)
	require.True(t, manager.ShouldRunning())

	// changefeed in error state but error can be retried
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return &model.ChangeFeedInfo{
			SinkURI: "123",
			Config:  &config.ReplicaConfig{},
			State:   model.StateWarning,
			Error: &model.RunningError{
				Addr: "127.0.0.1",
				Code: "CDC:ErrPipelineTryAgain",
				Message: "pipeline is full, please try again. Internal use only, " +
					"report a bug if seen externally",
			},
		}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(0, state.Status, state.Info)
	require.True(t, manager.ShouldRunning())

	state.PatchTaskPosition("test",
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			if position == nil {
				position = &model.TaskPosition{}
			}
			position.Error = &model.RunningError{
				Time:    time.Now(),
				Addr:    "test",
				Code:    "CDC:ErrExpressionColumnNotFound",
				Message: "what ever",
			}
			return position, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(0, state.Status, state.Info)
	require.False(t, manager.ShouldRunning())

	state.PatchTaskPosition("test",
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			if position == nil {
				position = &model.TaskPosition{}
			}
			position.Error = &model.RunningError{
				Addr:    "127.0.0.1",
				Code:    string(cerror.ErrExpressionColumnNotFound.RFCCode()),
				Message: cerror.ErrExpressionColumnNotFound.Error(),
			}
			return position, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(0, state.Status, state.Info)
	// should be false
	require.False(t, manager.ShouldRunning())

	state.PatchTaskPosition("test",
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			if position == nil {
				position = &model.TaskPosition{}
			}
			position.Error = &model.RunningError{
				Addr:    "127.0.0.1",
				Code:    string(cerror.ErrExpressionParseFailed.RFCCode()),
				Message: cerror.ErrExpressionParseFailed.Error(),
			}
			return position, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(0, state.Status, state.Info)
	// should be false
	require.False(t, manager.ShouldRunning())
}

func TestBackoffStopsUnexpectedly(t *testing.T) {
	globalVars, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	// after 4000ms, the backoff will stop
	manager := newFeedStateManager4Test(500, 500, 4000, 1.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
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
	manager.state = state
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()

	for i := 1; i <= 10; i++ {
		if i >= 8 {
			// after round 8, the maxElapsedTime of backoff will exceed 4000ms,
			// and NextBackOff() will return -1, so the changefeed state will
			// never turn into error state.
			require.Equal(t, state.Info.State, model.StateFailed)
			require.False(t, manager.ShouldRunning())
		} else {
			if i == 1 {
				require.Equal(t, model.StateNormal, state.Info.State)
			} else {
				require.Equal(t, model.StateWarning, state.Info.State)
			}
			require.True(t, manager.ShouldRunning())
			state.PatchTaskPosition(globalVars.CaptureInfo.ID,
				func(position *model.TaskPosition) (
					*model.TaskPosition, bool, error,
				) {
					return &model.TaskPosition{Error: &model.RunningError{
						Addr:    globalVars.CaptureInfo.AdvertiseAddr,
						Code:    "[CDC:ErrEtcdSessionDone]",
						Message: "fake error for test",
					}}, true, nil
				})
			tester.MustApplyPatches()
			manager.Tick(0, state.Status, state.Info)
			tester.MustApplyPatches()
			// If an error occurs, backing off from running the task.
			require.False(t, manager.ShouldRunning())
			require.Equal(t, model.StatePending, state.Info.State)
			require.Equal(t, state.Info.AdminJobType, model.AdminStop)
			require.Equal(t, state.Status.AdminJobType, model.AdminStop)
		}

		// 500ms is the backoff interval, so sleep 500ms and after a manager
		// tick, the changefeed will turn into normal state
		time.Sleep(500 * time.Millisecond)
		manager.Tick(0, state.Status, state.Info)
		tester.MustApplyPatches()
	}
}

func TestBackoffNeverStops(t *testing.T) {
	globalVars, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	// the backoff will never stop
	manager := newFeedStateManager4Test(100, 100, 0, 1.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
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
	manager.state = state
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()

	for i := 1; i <= 30; i++ {
		if i == 1 {
			require.Equal(t, model.StateNormal, state.Info.State)
		} else {
			require.Equal(t, model.StateWarning, state.Info.State)
		}
		require.True(t, manager.ShouldRunning())
		state.PatchTaskPosition(globalVars.CaptureInfo.ID,
			func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
				return &model.TaskPosition{Error: &model.RunningError{
					Addr:    globalVars.CaptureInfo.AdvertiseAddr,
					Code:    "[CDC:ErrEtcdSessionDone]",
					Message: "fake error for test",
				}}, true, nil
			})
		tester.MustApplyPatches()
		manager.Tick(0, state.Status, state.Info)
		tester.MustApplyPatches()
		require.False(t, manager.ShouldRunning())
		require.Equal(t, model.StatePending, state.Info.State)
		require.Equal(t, state.Info.AdminJobType, model.AdminStop)
		require.Equal(t, state.Status.AdminJobType, model.AdminStop)
		// 100ms is the backoff interval, so sleep 100ms and after a manager tick,
		// the changefeed will turn into normal state
		time.Sleep(100 * time.Millisecond)
		manager.Tick(0, state.Status, state.Info)
		tester.MustApplyPatches()
	}
}

func TestUpdateChangefeedEpoch(t *testing.T) {
	globalVars, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	// Set a long backoff time
	manager := newFeedStateManager4Test(int(time.Hour), int(time.Hour), 0, 1.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
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
	manager.state = state
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.Equal(t, state.Info.State, model.StateNormal)
	require.True(t, manager.ShouldRunning())

	for i := 1; i <= 30; i++ {
		manager.upstream.PDClient.(*mockPD).getTs = func() (int64, int64, error) {
			return int64(i), 0, nil
		}
		previousEpoch := state.Info.Epoch
		previousState := state.Info.State
		state.PatchTaskPosition(globalVars.CaptureInfo.ID,
			func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
				return &model.TaskPosition{Error: &model.RunningError{
					Addr:    globalVars.CaptureInfo.AdvertiseAddr,
					Code:    "[CDC:ErrEtcdSessionDone]",
					Message: "fake error for test",
				}}, true, nil
			})
		tester.MustApplyPatches()
		manager.Tick(0, state.Status, state.Info)
		tester.MustApplyPatches()
		require.False(t, manager.ShouldRunning())
		require.Equal(t, model.StatePending, state.Info.State, i)

		require.Equal(t, state.Info.AdminJobType, model.AdminStop)
		require.Equal(t, state.Status.AdminJobType, model.AdminStop)

		// Epoch only changes when State changes.
		if previousState == state.Info.State {
			require.Equal(t, previousEpoch, state.Info.Epoch)
		} else {
			require.NotEqual(t, previousEpoch, state.Info.Epoch)
		}
	}
}

func TestHandleWarning(t *testing.T) {
	globalVars, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	manager := newFeedStateManager4Test(200, 1600, 0, 2.0)
	manager.changefeedErrorStuckDuration = 100 * time.Millisecond
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.Nil(t, status)
		return &model.ChangeFeedStatus{
			CheckpointTs: 200,
		}, true, nil
	})

	tester.MustApplyPatches()
	manager.state = state
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.Equal(t, model.StateNormal, state.Info.State)
	require.True(t, manager.ShouldRunning())

	// 1. test when an warning occurs, the changefeed state will be changed to warning
	// and it will still keep running
	state.PatchTaskPosition(globalVars.CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Warning: &model.RunningError{
				Addr:    globalVars.CaptureInfo.AdvertiseAddr,
				Code:    "[CDC:ErrSinkManagerRunError]", // it is fake error
				Message: "fake error for test",
			}}, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(0, state.Status, state.Info)
	// some patches will be generated when the manager.Tick is called
	// so we need to apply the patches before we check the state
	tester.MustApplyPatches()
	require.Equal(t, model.StateWarning, state.Info.State)
	require.True(t, manager.ShouldRunning())

	// 2. test when the changefeed is in warning state, and the checkpointTs is not progressing,
	// the changefeed state will remain warning
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.NotNil(t, status)
		return &model.ChangeFeedStatus{
			CheckpointTs: 200,
		}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.Equal(t, model.StateWarning, state.Info.State)
	require.True(t, manager.ShouldRunning())

	// 3. test when the changefeed is in warning state, and the checkpointTs is progressing,
	// the changefeed state will be changed to normal
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.NotNil(t, status)
		return &model.ChangeFeedStatus{
			CheckpointTs: 201,
		}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.Equal(t, model.StateNormal, state.Info.State)
	require.True(t, manager.ShouldRunning())

	// 4. test when the changefeed is in warning state, and the checkpointTs is not progressing
	// for defaultBackoffMaxElapsedTime time, the changefeed state will be changed to failed
	// and it will stop running
	state.PatchTaskPosition(globalVars.CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Warning: &model.RunningError{
				Addr:    globalVars.CaptureInfo.AdvertiseAddr,
				Code:    "[CDC:ErrSinkManagerRunError]", // it is fake error
				Message: "fake error for test",
			}}, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(0, state.Status, state.Info)
	// some patches will be generated when the manager.Tick is called
	// so we need to apply the patches before we check the state
	tester.MustApplyPatches()
	require.Equal(t, model.StateWarning, state.Info.State)
	require.True(t, manager.ShouldRunning())

	state.PatchTaskPosition(globalVars.CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Warning: &model.RunningError{
				Addr:    globalVars.CaptureInfo.AdvertiseAddr,
				Code:    "[CDC:ErrSinkManagerRunError]", // it is fake error
				Message: "fake error for test",
			}}, true, nil
		})
	tester.MustApplyPatches()
	// mock the checkpointTs is not progressing for defaultBackoffMaxElapsedTime time
	manager.checkpointTsAdvanced = manager.
		checkpointTsAdvanced.Add(-(manager.changefeedErrorStuckDuration + 1))
	// resolveTs = 202 > checkpointTs = 201
	manager.Tick(202, state.Status, state.Info)
	// some patches will be generated when the manager.Tick is called
	// so we need to apply the patches before we check the state
	tester.MustApplyPatches()
	require.Equal(t, model.StateFailed, state.Info.State)
	require.False(t, manager.ShouldRunning())
}

func TestErrorAfterWarning(t *testing.T) {
	t.Parallel()

	maxElapsedTimeInMs := 2000
	globalVars, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	manager := newFeedStateManager4Test(200, 1600, maxElapsedTimeInMs, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.Nil(t, status)
		return &model.ChangeFeedStatus{
			CheckpointTs: 200,
		}, true, nil
	})

	tester.MustApplyPatches()
	manager.state = state
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.Equal(t, model.StateNormal, state.Info.State)
	require.True(t, manager.ShouldRunning())

	// 1. test when an warning occurs, the changefeed state will be changed to warning
	// and it will still keep running
	state.PatchTaskPosition(globalVars.CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Warning: &model.RunningError{
				Addr:    globalVars.CaptureInfo.AdvertiseAddr,
				Code:    "[CDC:ErrSinkManagerRunError]", // it is fake error
				Message: "fake error for test",
			}}, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(0, state.Status, state.Info)
	// some patches will be generated when the manager.Tick is called
	// so we need to apply the patches before we check the state
	tester.MustApplyPatches()
	require.Equal(t, model.StateWarning, state.Info.State)
	require.True(t, manager.ShouldRunning())

	// 2. test when the changefeed is in warning state, and the checkpointTs is not progressing,
	// the changefeed state will remain warning
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.NotNil(t, status)
		return &model.ChangeFeedStatus{
			CheckpointTs: 200,
		}, true, nil
	})
	tester.MustApplyPatches()
	manager.Tick(0, state.Status, state.Info)
	tester.MustApplyPatches()
	require.Equal(t, model.StateWarning, state.Info.State)
	require.True(t, manager.ShouldRunning())

	// 3. Sleep maxElapsedTimeInMs to wait backoff timeout. And when an error occurs after an warning,
	// the backoff will be reseted, and changefeed state will be changed to warning and it will still
	// keep running.
	time.Sleep(time.Millisecond * time.Duration(maxElapsedTimeInMs))
	state.PatchTaskPosition(globalVars.CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Error: &model.RunningError{
				Addr:    globalVars.CaptureInfo.AdvertiseAddr,
				Code:    "[CDC:ErrSinkManagerRunError]", // it is fake error
				Message: "fake error for test",
			}}, true, nil
		})
	tester.MustApplyPatches()

	manager.Tick(0, state.Status, state.Info)
	// some patches will be generated when the manager.Tick is called
	// so we need to apply the patches before we check the state
	tester.MustApplyPatches()
	require.Equal(t, model.StatePending, state.Info.State)
	require.False(t, manager.ShouldRunning())
	manager.Tick(0, state.Status, state.Info)

	// some patches will be generated when the manager.Tick is called
	// so we need to apply the patches before we check the state
	tester.MustApplyPatches()
	require.Equal(t, model.StateWarning, state.Info.State)
	require.True(t, manager.ShouldRunning())
}

func TestHandleWarningWhileAdvanceResolvedTs(t *testing.T) {
	t.Parallel()

	maxElapsedTimeInMs := 2000
	globalVars, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	manager := newFeedStateManager4Test(200, 1600, maxElapsedTimeInMs, 2.0)
	state := orchestrator.NewChangefeedReactorState(etcd.DefaultCDCClusterID,
		model.DefaultChangeFeedID(changefeedInfo.ID))
	manager.state = state
	tester := orchestrator.NewReactorStateTester(t, state, nil)
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		require.Nil(t, info)
		return &model.ChangeFeedInfo{SinkURI: "123", Config: &config.ReplicaConfig{}}, true, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.Nil(t, status)
		return &model.ChangeFeedStatus{
			CheckpointTs: 200,
		}, true, nil
	})

	tester.MustApplyPatches()
	manager.Tick(200, state.Status, state.Info)
	tester.MustApplyPatches()
	require.Equal(t, model.StateNormal, state.Info.State)
	require.True(t, manager.ShouldRunning())

	// 1. test when an warning occurs, the changefeed state will be changed to warning
	// and it will still keep running
	state.PatchTaskPosition(globalVars.CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Warning: &model.RunningError{
				Addr:    globalVars.CaptureInfo.AdvertiseAddr,
				Code:    "[CDC:ErrSinkManagerRunError]", // it is fake error
				Message: "fake error for test",
			}}, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(200, state.Status, state.Info)
	// some patches will be generated when the manager.Tick is called
	// so we need to apply the patches before we check the state
	tester.MustApplyPatches()
	require.Equal(t, model.StateWarning, state.Info.State)
	require.True(t, manager.ShouldRunning())

	// 2. test when the changefeed is in warning state, and the resolvedTs and checkpointTs is not progressing,
	// the changefeed state will remain warning whena new warning is encountered.
	time.Sleep(manager.changefeedErrorStuckDuration + 10)
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.NotNil(t, status)
		return &model.ChangeFeedStatus{
			CheckpointTs: 200,
		}, true, nil
	})
	state.PatchTaskPosition(globalVars.CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Warning: &model.RunningError{
				Addr:    globalVars.CaptureInfo.AdvertiseAddr,
				Code:    "[CDC:ErrSinkManagerRunError]", // it is fake error
				Message: "fake error for test",
			}}, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(200, state.Status, state.Info)
	tester.MustApplyPatches()
	require.Equal(t, model.StateWarning, state.Info.State)
	require.True(t, manager.ShouldRunning())

	// 3. Test changefeed remain warning when resolvedTs is progressing after stuck beyond the detection time.
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.NotNil(t, status)
		return &model.ChangeFeedStatus{
			CheckpointTs: 200,
		}, true, nil
	})
	state.PatchTaskPosition(globalVars.CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Warning: &model.RunningError{
				Addr:    globalVars.CaptureInfo.AdvertiseAddr,
				Code:    "[CDC:ErrSinkManagerRunError]", // it is fake error
				Message: "fake error for test",
			}}, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(400, state.Status, state.Info)
	tester.MustApplyPatches()
	require.Equal(t, model.StateWarning, state.Info.State)
	require.True(t, manager.ShouldRunning())

	// 4. Test changefeed failed when checkpointTs is not progressing for changefeedErrorStuckDuration time.
	time.Sleep(manager.changefeedErrorStuckDuration + 10)
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		require.NotNil(t, status)
		return &model.ChangeFeedStatus{
			CheckpointTs: 200,
		}, true, nil
	})
	state.PatchTaskPosition(globalVars.CaptureInfo.ID,
		func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return &model.TaskPosition{Warning: &model.RunningError{
				Addr:    globalVars.CaptureInfo.AdvertiseAddr,
				Code:    "[CDC:ErrSinkManagerRunError]", // it is fake error
				Message: "fake error for test",
			}}, true, nil
		})
	tester.MustApplyPatches()
	manager.Tick(400, state.Status, state.Info)
	tester.MustApplyPatches()
	require.Equal(t, model.StateFailed, state.Info.State)
	require.False(t, manager.ShouldRunning())
}
