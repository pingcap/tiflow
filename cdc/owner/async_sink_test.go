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

func newAsyncSink4Test() (AsyncSink, *mockSink) {
	mockSink := &mockSink{}
	asyncSink := newAsyncSinkImpl()
	asyncSink.(*asyncSinkImpl).sinkInitHandler = func(ctx cdcContext.Context, a *asyncSinkImpl) error {
		a.sink = mockSink
		return nil
	}
	return asyncSink, mockSink
}

func (s *asyncSinkSuite) TestCheckpoint(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	asyncSink, mSink := newAsyncSink4Test()

	var (
		wg    sync.WaitGroup
		errCh chan error
	)
	defer func() {
		asyncSink.Close(ctx)
		c.Assert(<-errCh, check.IsNil)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- asyncSink.Run(ctx)
	}()

	waitCheckpointGrowingUp := func(m *mockSink, targetTs model.Ts) error {
		return retry.Do(context.Background(), func() error {
			if targetTs != atomic.LoadUint64(&m.checkpointTs) {
				return errors.New("targetTs!=checkpointTs")
			}
			return nil
		}, retry.WithBackoffBaseDelay(100), retry.WithMaxTries(30))
	}
	asyncSink.EmitCheckpointTs(ctx, 1)
	c.Assert(waitCheckpointGrowingUp(mSink, 1), check.IsNil)
	asyncSink.EmitCheckpointTs(ctx, 10)
	c.Assert(waitCheckpointGrowingUp(mSink, 10), check.IsNil)
}

func (s *asyncSinkSuite) TestExecDDL(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := cdcContext.NewBackendContext4Test(false)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	asyncSink, mSink := newAsyncSink4Test()

	var (
		wg    sync.WaitGroup
		errCh chan error
	)
	defer func() {
		asyncSink.Close(ctx)
		c.Assert(<-errCh, check.IsNil)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- asyncSink.Run(ctx)
	}()

	ddlEvents := []*model.DDLEvent{
		{CommitTs: 1},
		{CommitTs: 2},
		{CommitTs: 3},
	}

	for _, event := range ddlEvents {
		for {
			done, err := asyncSink.EmitDDLEvent(ctx, event)
			c.Assert(err, check.IsNil)
			if done {
				c.Assert(mSink.GetDDL(), check.DeepEquals, event)
				break
			}
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

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	asyncSink, mSink := newAsyncSink4Test()

	var (
		wg    sync.WaitGroup
		errCh chan error
	)
	defer func() {
		asyncSink.Close(ctx)
		c.Assert(<-errCh, check.IsNil)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		errCh <- asyncSink.Run(ctx)
	}()

	mSink.ddlError = cerror.ErrDDLEventIgnored.GenWithStackByArgs()
	ddl1 := &model.DDLEvent{CommitTs: 1}
	for {
		done, err := asyncSink.EmitDDLEvent(ctx, ddl1)
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
		done, err := asyncSink.EmitDDLEvent(ctx, ddl2)
		c.Assert(err, check.IsNil)
		if done || getResultErr() != nil {
			c.Assert(mSink.GetDDL(), check.DeepEquals, ddl2)
			break
		}
	}
	c.Assert(cerror.ErrExecDDLFailed.Equal(errors.Cause(getResultErr())), check.IsTrue)
}
