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
<<<<<<< HEAD
	"context"
=======
	"fmt"
>>>>>>> e76856f6 (new_owner: refine processor (#1821))
	"math"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	tablepipeline "github.com/pingcap/ticdc/cdc/processor/pipeline"
	"github.com/pingcap/ticdc/pkg/config"
<<<<<<< HEAD
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
=======
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator"
>>>>>>> e76856f6 (new_owner: refine processor (#1821))
	"github.com/pingcap/ticdc/pkg/util/testleak"
	pd "github.com/tikv/pd/client"
)

type managerSuite struct {
	manager *Manager
	state   *model.GlobalReactorState
	tester  *orchestrator.ReactorStateTester
}

var _ = check.Suite(&managerSuite{})

<<<<<<< HEAD
func newManager4Test() *Manager {
	m := NewManager(nil, nil, &model.CaptureInfo{
		ID:            "test-captureID",
		AdvertiseAddr: "127.0.0.1:0000",
	})
	m.newProcessor = func(
		pdCli pd.Client,
		changefeedID model.ChangeFeedID,
		credential *security.Credential,
		captureInfo *model.CaptureInfo,
	) *processor {
		return newProcessor4Test()
	}
	return m
=======
func (s *managerSuite) resetSuit(ctx cdcContext.Context, c *check.C) {
	s.manager = NewManager4Test(func(ctx cdcContext.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) (tablepipeline.TablePipeline, error) {
		return &mockTablePipeline{
			tableID:      tableID,
			name:         fmt.Sprintf("`test`.`table%d`", tableID),
			status:       tablepipeline.TableStatusRunning,
			resolvedTs:   replicaInfo.StartTs,
			checkpointTs: replicaInfo.StartTs,
		}, nil
	})
	s.state = model.NewGlobalState().(*model.GlobalReactorState)
	captureInfoBytes, err := ctx.GlobalVars().CaptureInfo.Marshal()
	c.Assert(err, check.IsNil)
	s.tester = orchestrator.NewReactorStateTester(c, s.state, map[string]string{
		fmt.Sprintf("/tidb/cdc/capture/%s", ctx.GlobalVars().CaptureInfo.ID): string(captureInfoBytes),
	})
>>>>>>> e76856f6 (new_owner: refine processor (#1821))
}

func (s *managerSuite) TestChangefeed(c *check.C) {
	defer testleak.AfterTest(c)()
<<<<<<< HEAD
	ctx := context.Background()
	m := newManager4Test()
	state := &globalState{
		CaptureID:   "test-captureID",
		Changefeeds: make(map[model.ChangeFeedID]*changefeedState),
	}
=======
	ctx := cdcContext.NewBackendContext4Test(false)
	s.resetSuit(ctx, c)
>>>>>>> e76856f6 (new_owner: refine processor (#1821))
	var err error

	// no changefeed
	_, err = s.manager.Tick(ctx, s.state)
	c.Assert(err, check.IsNil)

	// an inactive changefeed
	s.state.Changefeeds["test-changefeed"] = model.NewChangefeedReactorState("test-changefeed")
	_, err = s.manager.Tick(ctx, s.state)
	s.tester.MustApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(s.manager.processors, check.HasLen, 0)

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
			Tables: map[int64]*model.TableReplicaInfo{},
		}, true, nil
	})
	s.tester.MustApplyPatches()
	_, err = s.manager.Tick(ctx, s.state)
	s.tester.MustApplyPatches()
	c.Assert(err, check.IsNil)
	c.Assert(s.manager.processors, check.HasLen, 1)

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
	c.Assert(err, check.IsNil)
	c.Assert(s.manager.processors, check.HasLen, 0)
}

func (s *managerSuite) TestDebugInfo(c *check.C) {
	defer testleak.AfterTest(c)()
<<<<<<< HEAD
	ctx := context.Background()
	m := newManager4Test()
	state := &globalState{
		CaptureID:   "test-captureID",
		Changefeeds: make(map[model.ChangeFeedID]*changefeedState),
	}
=======
	ctx := cdcContext.NewBackendContext4Test(false)
	s.resetSuit(ctx, c)
>>>>>>> e76856f6 (new_owner: refine processor (#1821))
	var err error

	// no changefeed
	_, err = s.manager.Tick(ctx, s.state)
	c.Assert(err, check.IsNil)

	// an active changefeed
	s.state.Changefeeds["test-changefeed"] = model.NewChangefeedReactorState("test-changefeed")
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
			Tables: map[int64]*model.TableReplicaInfo{},
		}, true, nil
	})
	s.tester.MustApplyPatches()
	_, err = s.manager.Tick(ctx, s.state)
	c.Assert(err, check.IsNil)
	s.tester.MustApplyPatches()
	c.Assert(s.manager.processors, check.HasLen, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, err = s.manager.Tick(ctx, s.state)
			if err != nil {
				c.Assert(cerrors.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
				return
			}
			c.Assert(err, check.IsNil)
			s.tester.MustApplyPatches()
		}
	}()
	buf := bytes.NewBufferString("")
	s.manager.WriteDebugInfo(buf)
	c.Assert(len(buf.String()), check.Greater, 0)
	s.manager.AsyncClose()
	<-done
}

func (s *managerSuite) TestClose(c *check.C) {
	defer testleak.AfterTest(c)()
<<<<<<< HEAD
	ctx := context.Background()
	m := newManager4Test()
	state := &globalState{
		CaptureID:   "test-captureID",
		Changefeeds: make(map[model.ChangeFeedID]*changefeedState),
	}
=======
	ctx := cdcContext.NewBackendContext4Test(false)
	s.resetSuit(ctx, c)
>>>>>>> e76856f6 (new_owner: refine processor (#1821))
	var err error

	// no changefeed
	_, err = s.manager.Tick(ctx, s.state)
	c.Assert(err, check.IsNil)

	// an active changefeed
	s.state.Changefeeds["test-changefeed"] = model.NewChangefeedReactorState("test-changefeed")
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
			Tables: map[int64]*model.TableReplicaInfo{},
		}, true, nil
	})
	s.tester.MustApplyPatches()
	_, err = s.manager.Tick(ctx, s.state)
	c.Assert(err, check.IsNil)
	s.tester.MustApplyPatches()
	c.Assert(s.manager.processors, check.HasLen, 1)

	s.manager.AsyncClose()
	_, err = s.manager.Tick(ctx, s.state)
	c.Assert(cerrors.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	s.tester.MustApplyPatches()
	c.Assert(s.manager.processors, check.HasLen, 0)
}
