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
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

var _ = check.Suite(&asyncSinkSuite{})

type asyncSinkSuite struct{}

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

func newAsyncSink4Test() (asyncSink, *mockSink) {
	mockSink := &mockSink{}
	asyncSink := newAsyncSink()
	asyncSink.(*asyncSinkImpl).sinkInitHandler = func(ctx cdcContext.Context, a *asyncSinkImpl, _ model.ChangeFeedID, _ *model.ChangeFeedInfo) error {
		a.sink = mockSink
		return nil
	}
	return asyncSink, mockSink
}

func (s *asyncSinkSuite) TestCheckpoint(c *check.C) {
	defer testleak.AfterTest(c)()
	asyncSink, mSink := newAsyncSink4Test()

	ctx := cdcContext.NewBackendContext4Test(true)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- asyncSink.run(ctx, ctx.ChangefeedVars().ID, ctx.ChangefeedVars().Info)
	}()

	go func() {
		wg.Wait()
		asyncSink.close(ctx)
		c.Assert(<-errCh, check.IsNil)
	}()

	waitCheckpointGrowingUp := func(m *mockSink, targetTs model.Ts) error {
		return retry.Do(context.Background(), func() error {
			if targetTs != atomic.LoadUint64(&m.checkpointTs) {
				return errors.New("targetTs!=checkpointTs")
			}
			return nil
		}, retry.WithBackoffBaseDelay(100), retry.WithMaxTries(30))
	}
	asyncSink.emitCheckpointTs(ctx, 1)
	c.Assert(waitCheckpointGrowingUp(mSink, 1), check.IsNil)
	asyncSink.emitCheckpointTs(ctx, 10)
	c.Assert(waitCheckpointGrowingUp(mSink, 10), check.IsNil)
}

func (s *asyncSinkSuite) TestExecDDL(c *check.C) {
	defer testleak.AfterTest(c)()
	asyncSink, mSink := newAsyncSink4Test()

	ctx := cdcContext.NewBackendContext4Test(true)
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- asyncSink.run(ctx, ctx.ChangefeedVars().ID, ctx.ChangefeedVars().Info)
	}()

	ddlEvents := []*model.DDLEvent{
		{CommitTs: 1},
		{CommitTs: 2},
		{CommitTs: 3},
	}

	for _, event := range ddlEvents {
		for {
			done, err := asyncSink.emitDDLEvent(ctx, event)
			c.Assert(err, check.IsNil)
			if done {
				c.Assert(mSink.GetDDL(), check.DeepEquals, event)
				break
			}
		}
	}

	asyncSink.close(ctx)
	wg.Wait()
	c.Assert(<-errCh, check.IsNil)
}

func (s *asyncSinkSuite) TestExecDDLError(c *check.C) {
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
	writeResultErr := func(err error) {
		resultErrMu.Lock()
		defer resultErrMu.Unlock()
		resultErr = err
	}

	asyncSink, mSink := newAsyncSink4Test()
	ctx, cancel := cdcContext.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := asyncSink.run(ctx, ctx.ChangefeedVars().ID, ctx.ChangefeedVars().Info)
		if err != nil {
			writeResultErr(err)
		}
	}()

	mSink.ddlError = cerror.ErrDDLEventIgnored.GenWithStackByArgs()
	ddl1 := &model.DDLEvent{CommitTs: 1}
	for {
		done, err := asyncSink.emitDDLEvent(ctx, ddl1)
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
		done, err := asyncSink.emitDDLEvent(ctx, ddl2)
		c.Assert(err, check.IsNil)

		if done || readResultErr() != nil {
			c.Assert(mSink.GetDDL(), check.DeepEquals, ddl2)
			break
		}
	}
	c.Assert(cerror.ErrExecDDLFailed.Equal(readResultErr()), check.IsTrue)

	wg.Wait()
	asyncSink.close(ctx)
}
