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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

var _ = check.Suite(&ddlSinkSuite{})

type ddlSinkSuite struct{}

type mockSink struct {
	sink.Sink
	checkpointTs model.Ts
	ddl          *model.DDLEvent
	ddlMu        sync.Mutex
	ddlError     error
}

func (m *mockSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	atomic.StoreUint64(&m.checkpointTs, ts)
	return nil
}

func (m *mockSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	m.ddlMu.Lock()
	defer m.ddlMu.Unlock()
	time.Sleep(1 * time.Second)
	m.ddl = ddl
	return m.ddlError
}

func (m *mockSink) Close(ctx context.Context) error {
	return nil
}

func (m *mockSink) Barrier(ctx context.Context, tableID model.TableID) error {
	return nil
}

func (m *mockSink) GetDDL() *model.DDLEvent {
	m.ddlMu.Lock()
	defer m.ddlMu.Unlock()
	return m.ddl
}

func newDDLSink4Test() (DDLSink, *mockSink) {
	mockSink := &mockSink{}
	ddlSink := newDDLSink()
	ddlSink.(*ddlSinkImpl).sinkInitHandler = func(ctx cdcContext.Context, a *ddlSinkImpl, _ model.ChangeFeedID, _ *model.ChangeFeedInfo) error {
		a.sink = mockSink
		return nil
	}
	return ddlSink, mockSink
}

func (s *ddlSinkSuite) TestCheckpoint(c *check.C) {
	defer testleak.AfterTest(c)()
	ddlSink, mSink := newDDLSink4Test()
	ctx := cdcContext.NewBackendContext4Test(true)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer func() {
		cancel()
		ddlSink.close(ctx)
	}()
	ddlSink.run(ctx, ctx.ChangefeedVars().ID, ctx.ChangefeedVars().Info)

	waitCheckpointGrowingUp := func(m *mockSink, targetTs model.Ts) error {
		return retry.Do(context.Background(), func() error {
			if targetTs != atomic.LoadUint64(&m.checkpointTs) {
				return errors.New("targetTs!=checkpointTs")
			}
			return nil
		}, retry.WithBackoffBaseDelay(100), retry.WithMaxTries(30))
	}
	ddlSink.emitCheckpointTs(ctx, 1)
	c.Assert(waitCheckpointGrowingUp(mSink, 1), check.IsNil)
	ddlSink.emitCheckpointTs(ctx, 10)
	c.Assert(waitCheckpointGrowingUp(mSink, 10), check.IsNil)
}

func (s *ddlSinkSuite) TestExecDDL(c *check.C) {
	defer testleak.AfterTest(c)()
	ddlSink, mSink := newDDLSink4Test()
	ctx := cdcContext.NewBackendContext4Test(true)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer func() {
		cancel()
		ddlSink.close(ctx)
	}()
	ddlSink.run(ctx, ctx.ChangefeedVars().ID, ctx.ChangefeedVars().Info)

	ddlEvents := []*model.DDLEvent{
		{CommitTs: 1},
		{CommitTs: 2},
		{CommitTs: 3},
	}

	for _, event := range ddlEvents {
		for {
			done, err := ddlSink.emitDDLEvent(ctx, event)
			c.Assert(err, check.IsNil)
			if done {
				c.Assert(mSink.GetDDL(), check.DeepEquals, event)
				break
			}
		}
	}
}

func (s *ddlSinkSuite) TestExecDDLError(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(true)

	var (
		resultErr   error
		resultErrMu sync.Mutex
	)
	readResultErr := func() error {
		resultErrMu.Lock()
		defer resultErrMu.Unlock()
		return resultErr
	}

	ddlSink, mSink := newDDLSink4Test()
	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		resultErrMu.Lock()
		defer resultErrMu.Unlock()
		resultErr = err
		return nil
	})
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer func() {
		cancel()
		ddlSink.close(ctx)
	}()

	ddlSink.run(ctx, ctx.ChangefeedVars().ID, ctx.ChangefeedVars().Info)

	mSink.ddlError = cerror.ErrDDLEventIgnored.GenWithStackByArgs()
	ddl1 := &model.DDLEvent{CommitTs: 1}
	for {
		done, err := ddlSink.emitDDLEvent(ctx, ddl1)
		c.Assert(err, check.IsNil)
		if done {
			c.Assert(mSink.GetDDL(), check.DeepEquals, ddl1)
			break
		}
	}
	c.Assert(resultErr, check.IsNil)

	mSink.ddlError = cerror.ErrExecDDLFailed.GenWithStackByArgs()
	ddl2 := &model.DDLEvent{CommitTs: 2}
	for {
		done, err := ddlSink.emitDDLEvent(ctx, ddl2)
		c.Assert(err, check.IsNil)

		if done || readResultErr() != nil {
			c.Assert(mSink.GetDDL(), check.DeepEquals, ddl2)
			break
		}
	}
	c.Assert(cerror.ErrExecDDLFailed.Equal(readResultErr()), check.IsTrue)
}
