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

package cdc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/config"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

var _ = check.Suite(&asyncSinkSuite{})

type asyncSinkSuite struct{}

type mockAsyncSink struct {
	sink.Sink
	initTableInfo []*model.SimpleTableInfo
	checkpointTs  model.Ts
	ddl           *model.DDLEvent
	ddlMu         sync.Mutex
	ddlError      error
}

func (m *mockAsyncSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	m.initTableInfo = tableInfo
	return nil
}

func (m *mockAsyncSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	atomic.StoreUint64(&m.checkpointTs, ts)
	return nil
}

func (m *mockAsyncSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	m.ddlMu.Lock()
	defer m.ddlMu.Unlock()
	time.Sleep(1 * time.Second)
	m.ddl = ddl
	return m.ddlError
}

func (m *mockAsyncSink) Close(ctx context.Context) error {
	return nil
}

func (m *mockAsyncSink) Barrier(ctx context.Context) error {
	return nil
}

func (m *mockAsyncSink) GetDDL() *model.DDLEvent {
	m.ddlMu.Lock()
	defer m.ddlMu.Unlock()
	return m.ddl
}

func newAsyncSink4Test(ctx cdcContext.Context, c *check.C) (cdcContext.Context, AsyncSink, *mockAsyncSink) {
	ctx = cdcContext.WithChangefeedVars(ctx, &cdcContext.ChangefeedVars{
		ID:   "test-changefeed",
		Info: &model.ChangeFeedInfo{SinkURI: "blackhole://", Config: config.GetDefaultReplicaConfig()},
	})
	sink, err := newAsyncSink(ctx)
	c.Assert(err, check.IsNil)
	mockAsyncSink := &mockAsyncSink{}
	sink.(*asyncSinkImpl).sink = mockAsyncSink
	return ctx, sink, mockAsyncSink
}

func (s *asyncSinkSuite) TestInitialize(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	ctx, sink, mockAsyncSink := newAsyncSink4Test(ctx, c)
	defer sink.Close(ctx)
	tableInfos := []*model.SimpleTableInfo{{Schema: "test"}}
	err := sink.Initialize(ctx, tableInfos)
	c.Assert(err, check.IsNil)
	c.Assert(tableInfos, check.DeepEquals, mockAsyncSink.initTableInfo)
}

func (s *asyncSinkSuite) TestCheckpoint(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	ctx, sink, mSink := newAsyncSink4Test(ctx, c)
	defer sink.Close(ctx)

	waitCheckpointGrowingUp := func(m *mockAsyncSink, targetTs model.Ts) error {
		return retry.Run(100*time.Millisecond, 30, func() error {
			if targetTs != atomic.LoadUint64(&m.checkpointTs) {
				return errors.New("targetTs!=checkpointTs")
			}
			return nil
		})
	}
	err := sink.EmitCheckpointTs(ctx, 1)
	c.Assert(err, check.IsNil)
	c.Assert(waitCheckpointGrowingUp(mSink, 1), check.IsNil)
	err = sink.EmitCheckpointTs(ctx, 10)
	c.Assert(err, check.IsNil)
	c.Assert(waitCheckpointGrowingUp(mSink, 10), check.IsNil)
}

func (s *asyncSinkSuite) TestExecDDL(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	ctx, sink, mSink := newAsyncSink4Test(ctx, c)
	defer sink.Close(ctx)
	ddl1 := &model.DDLEvent{CommitTs: 1}
	for {
		done, err := sink.EmitDDLEvent(ctx, ddl1)
		c.Assert(err, check.IsNil)
		if done {
			c.Assert(mSink.GetDDL(), check.DeepEquals, ddl1)
			break
		}
	}
	ddl2 := &model.DDLEvent{CommitTs: 2}
	ddl3 := &model.DDLEvent{CommitTs: 3}
	_, err := sink.EmitDDLEvent(ctx, ddl2)
	c.Assert(err, check.IsNil)
	for {
		done, err := sink.EmitDDLEvent(ctx, ddl2)
		c.Assert(err, check.IsNil)
		if done {
			c.Assert(mSink.GetDDL(), check.DeepEquals, ddl2)
			break
		}
	}
	_, err = sink.EmitDDLEvent(ctx, ddl3)
	c.Assert(err, check.IsNil)
	for {
		done, err := sink.EmitDDLEvent(ctx, ddl3)
		c.Assert(err, check.IsNil)
		if done {
			c.Assert(mSink.GetDDL(), check.DeepEquals, ddl3)
			break
		}
	}
}

func (s *asyncSinkSuite) TestExecDDLError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	var resultErr error
	var resultErrMu sync.Mutex
	getResultErr := func() error {
		resultErrMu.Lock()
		defer resultErrMu.Unlock()
		return resultErr
	}
	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		resultErrMu.Lock()
		defer resultErrMu.Unlock()
		resultErr = err
		return nil
	})
	ctx, sink, mSink := newAsyncSink4Test(ctx, c)
	defer sink.Close(ctx)
	mSink.ddlError = cerror.ErrDDLEventIgnored.GenWithStackByArgs()
	ddl1 := &model.DDLEvent{CommitTs: 1}
	for {
		done, err := sink.EmitDDLEvent(ctx, ddl1)
		c.Assert(err, check.IsNil)
		if done {
			c.Assert(mSink.GetDDL(), check.DeepEquals, ddl1)
			break
		}
	}
	c.Assert(getResultErr(), check.IsNil)
	mSink.ddlError = cerror.ErrExecDDLFailed.GenWithStackByArgs()
	ddl2 := &model.DDLEvent{CommitTs: 2}
	for {
		done, err := sink.EmitDDLEvent(ctx, ddl2)
		c.Assert(err, check.IsNil)
		if done || getResultErr() != nil {
			c.Assert(mSink.GetDDL(), check.DeepEquals, ddl2)
			break
		}
	}
	c.Assert(cerror.ErrExecDDLFailed.Equal(errors.Cause(getResultErr())), check.IsTrue)
}
