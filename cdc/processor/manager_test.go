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
	"context"
	"math"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	pd "github.com/tikv/pd/client"
)

type managerSuite struct{}

var _ = check.Suite(&managerSuite{})

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
}

func (s *managerSuite) TestChangefeed(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	m := newManager4Test()
	state := &globalState{
		CaptureID:   "test-captureID",
		Changefeeds: make(map[model.ChangeFeedID]*changefeedState),
	}
	var err error

	// no changefeed
	_, err = m.Tick(ctx, state)
	c.Assert(err, check.IsNil)

	// an inactive changefeed
	state.Changefeeds["test-changefeed"] = newChangeFeedState("test-changefeed", state.CaptureID)
	_, err = m.Tick(ctx, state)
	c.Assert(err, check.IsNil)
	c.Assert(m.processors, check.HasLen, 0)

	// an active changefeed
	state.Changefeeds["test-changefeed"].Info = &model.ChangeFeedInfo{
		SinkURI:    "blackhole://",
		CreateTime: time.Now(),
		StartTs:    0,
		TargetTs:   math.MaxUint64,
		Config:     config.GetDefaultReplicaConfig(),
	}
	state.Changefeeds["test-changefeed"].Status = &model.ChangeFeedStatus{}
	state.Changefeeds["test-changefeed"].TaskStatus = &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
	}
	_, err = m.Tick(ctx, state)
	c.Assert(err, check.IsNil)
	c.Assert(m.processors, check.HasLen, 1)

	// processor return errors
	state.Changefeeds["test-changefeed"].TaskStatus.AdminJobType = model.AdminStop
	_, err = m.Tick(ctx, state)
	c.Assert(err, check.IsNil)
	c.Assert(m.processors, check.HasLen, 0)
}

func (s *managerSuite) TestDebugInfo(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	m := newManager4Test()
	state := &globalState{
		CaptureID:   "test-captureID",
		Changefeeds: make(map[model.ChangeFeedID]*changefeedState),
	}
	var err error

	// no changefeed
	_, err = m.Tick(ctx, state)
	c.Assert(err, check.IsNil)

	// an active changefeed
	state.Changefeeds["test-changefeed"] = newChangeFeedState("test-changefeed", state.CaptureID)
	state.Changefeeds["test-changefeed"].Info = &model.ChangeFeedInfo{
		SinkURI:    "blackhole://",
		CreateTime: time.Now(),
		StartTs:    0,
		TargetTs:   math.MaxUint64,
		Config:     config.GetDefaultReplicaConfig(),
	}
	state.Changefeeds["test-changefeed"].Status = &model.ChangeFeedStatus{}
	state.Changefeeds["test-changefeed"].TaskStatus = &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
	}
	_, err = m.Tick(ctx, state)
	c.Assert(err, check.IsNil)
	c.Assert(m.processors, check.HasLen, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, err := m.Tick(ctx, state)
			if err != nil {
				c.Assert(cerrors.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
				return
			}
			c.Assert(err, check.IsNil)
		}
	}()
	buf := bytes.NewBufferString("")
	m.WriteDebugInfo(buf)
	c.Assert(len(buf.String()), check.Greater, 0)
	m.AsyncClose()
	<-done
}

func (s *managerSuite) TestClose(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	m := newManager4Test()
	state := &globalState{
		CaptureID:   "test-captureID",
		Changefeeds: make(map[model.ChangeFeedID]*changefeedState),
	}
	var err error

	// no changefeed
	_, err = m.Tick(ctx, state)
	c.Assert(err, check.IsNil)

	// an active changefeed
	state.Changefeeds["test-changefeed"] = newChangeFeedState("test-changefeed", state.CaptureID)
	state.Changefeeds["test-changefeed"].Info = &model.ChangeFeedInfo{
		SinkURI:    "blackhole://",
		CreateTime: time.Now(),
		StartTs:    0,
		TargetTs:   math.MaxUint64,
		Config:     config.GetDefaultReplicaConfig(),
	}
	state.Changefeeds["test-changefeed"].Status = &model.ChangeFeedStatus{}
	state.Changefeeds["test-changefeed"].TaskStatus = &model.TaskStatus{
		Tables: map[int64]*model.TableReplicaInfo{},
	}
	_, err = m.Tick(ctx, state)
	c.Assert(err, check.IsNil)
	c.Assert(m.processors, check.HasLen, 1)

	m.AsyncClose()
	_, err = m.Tick(ctx, state)
	c.Assert(cerrors.ErrReactorFinished.Equal(errors.Cause(err)), check.IsTrue)
	c.Assert(m.processors, check.HasLen, 0)
}
