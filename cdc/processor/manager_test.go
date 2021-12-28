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

package processor

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	tablepipeline "github.com/pingcap/tiflow/cdc/processor/pipeline"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/orchestrator"
	"github.com/stretchr/testify/require"
)

type managerTester struct {
	manager *Manager
	state   *orchestrator.GlobalReactorState
	tester  *orchestrator.ReactorStateTester
}

// NewManager4Test creates a new processor manager for test
func NewManager4Test(
	t *testing.T,
	createTablePipeline func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error),
) *Manager {
	m := NewManager()
	m.newProcessor = func(ctx cdcContext.Context) *processor {
		return newProcessor4Test(ctx, t, createTablePipeline)
	}
	return m
}

func (s *managerTester) resetSuit(ctx cdcContext.Context, t *testing.T) {
	s.manager = NewManager4Test(t, func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error) {
		return &mockTablePipeline{
			tableID:      tableID,
			name:         fmt.Sprintf("`test`.`table%d`", tableID),
			status:       tablepipeline.TableStatusRunning,
			resolvedTs:   replicaInfo.StartTs,
			checkpointTs: replicaInfo.StartTs,
		}, nil
	})
	s.state = orchestrator.NewGlobalState()
	captureInfoBytes, err := ctx.GlobalVars().CaptureInfo.Marshal()
	require.Nil(t, err)
	s.tester = orchestrator.NewReactorStateTester(t, s.state, map[string]string{
		fmt.Sprintf("/tidb/cdc/capture/%s", ctx.GlobalVars().CaptureInfo.ID): string(captureInfoBytes),
	})
}

func TestChangefeed(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	s := &managerTester{}
	s.resetSuit(ctx, t)
	var err error

	// no changefeed
	_, err = s.manager.Tick(ctx, s.state)
	require.Nil(t, err)

	// an inactive changefeed
	s.state.Changefeeds["test-changefeed"] = orchestrator.NewChangefeedReactorState("test-changefeed")
	_, err = s.manager.Tick(ctx, s.state)
	s.tester.MustApplyPatches()
	require.Nil(t, err)
	require.Len(t, s.manager.processors, 0)

	// an active changefeed
	s.state.Changefeeds["test-changefeed"].PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return &model.ChangeFeedInfo{
			SinkURI:    "blackhole://",
			CreateTime: time.Now(),
			StartTs:    0,
			TargetTs:   math.MaxUint64,
			Config:     config.GetDefaultReplicaConfig(),
		}, true, nil
	})
	s.state.Changefeeds["test-changefeed"].PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{}, true, nil
	})
	s.state.Changefeeds["test-changefeed"].PatchTaskStatus(ctx.GlobalVars().CaptureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return &model.TaskStatus{
			Tables: map[int64]*model.TableReplicaInfo{1: {}},
		}, true, nil
	})
	s.tester.MustApplyPatches()
	_, err = s.manager.Tick(ctx, s.state)
	s.tester.MustApplyPatches()
	require.Nil(t, err)
	require.Len(t, s.manager.processors, 1)

	// processor return errors
	s.state.Changefeeds["test-changefeed"].PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		status.AdminJobType = model.AdminStop
		return status, true, nil
	})
	s.state.Changefeeds["test-changefeed"].PatchTaskStatus(ctx.GlobalVars().CaptureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		status.AdminJobType = model.AdminStop
		return status, true, nil
	})
	s.tester.MustApplyPatches()
	_, err = s.manager.Tick(ctx, s.state)
	s.tester.MustApplyPatches()
	require.Nil(t, err)
	require.Len(t, s.manager.processors, 0)
}

func TestDebugInfo(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	s := &managerTester{}
	s.resetSuit(ctx, t)
	var err error

	// no changefeed
	_, err = s.manager.Tick(ctx, s.state)
	require.Nil(t, err)

	// an active changefeed
	s.state.Changefeeds["test-changefeed"] = orchestrator.NewChangefeedReactorState("test-changefeed")
	s.state.Changefeeds["test-changefeed"].PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return &model.ChangeFeedInfo{
			SinkURI:    "blackhole://",
			CreateTime: time.Now(),
			StartTs:    0,
			TargetTs:   math.MaxUint64,
			Config:     config.GetDefaultReplicaConfig(),
		}, true, nil
	})
	s.state.Changefeeds["test-changefeed"].PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{}, true, nil
	})
	s.state.Changefeeds["test-changefeed"].PatchTaskStatus(ctx.GlobalVars().CaptureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return &model.TaskStatus{
			Tables: map[int64]*model.TableReplicaInfo{1: {}},
		}, true, nil
	})
	s.tester.MustApplyPatches()
	_, err = s.manager.Tick(ctx, s.state)
	require.Nil(t, err)
	s.tester.MustApplyPatches()
	require.Len(t, s.manager.processors, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, err = s.manager.Tick(ctx, s.state)
			if err != nil {
				require.True(t, cerrors.ErrReactorFinished.Equal(errors.Cause(err)))
				return
			}
			require.Nil(t, err)
			s.tester.MustApplyPatches()
		}
	}()
	buf := bytes.NewBufferString("")
	s.manager.WriteDebugInfo(buf)
	require.Greater(t, len(buf.String()), 0)
	s.manager.AsyncClose()
	<-done
}

func TestClose(t *testing.T) {
	ctx := cdcContext.NewBackendContext4Test(false)
	s := &managerTester{}
	s.resetSuit(ctx, t)
	var err error

	// no changefeed
	_, err = s.manager.Tick(ctx, s.state)
	require.Nil(t, err)

	// an active changefeed
	s.state.Changefeeds["test-changefeed"] = orchestrator.NewChangefeedReactorState("test-changefeed")
	s.state.Changefeeds["test-changefeed"].PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return &model.ChangeFeedInfo{
			SinkURI:    "blackhole://",
			CreateTime: time.Now(),
			StartTs:    0,
			TargetTs:   math.MaxUint64,
			Config:     config.GetDefaultReplicaConfig(),
		}, true, nil
	})
	s.state.Changefeeds["test-changefeed"].PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return &model.ChangeFeedStatus{}, true, nil
	})
	s.state.Changefeeds["test-changefeed"].PatchTaskStatus(ctx.GlobalVars().CaptureInfo.ID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
		return &model.TaskStatus{
			Tables: map[int64]*model.TableReplicaInfo{1: {}},
		}, true, nil
	})
	s.tester.MustApplyPatches()
	_, err = s.manager.Tick(ctx, s.state)
	require.Nil(t, err)
	s.tester.MustApplyPatches()
	require.Len(t, s.manager.processors, 1)

	s.manager.AsyncClose()
	_, err = s.manager.Tick(ctx, s.state)
	require.True(t, cerrors.ErrReactorFinished.Equal(errors.Cause(err)))
	s.tester.MustApplyPatches()
	require.Len(t, s.manager.processors, 0)
}
