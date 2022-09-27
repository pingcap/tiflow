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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/stretchr/testify/require"
)

type mockSink struct {
	sink.Sink
	checkpointTs model.Ts
	ddl          *model.DDLEvent
	ddlMu        sync.Mutex
	ddlError     error
}

func (m *mockSink) EmitCheckpointTs(_ context.Context, ts uint64, _ []model.TableName) error {
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

func newDDLSink4Test(reportErr func(err error)) (DDLSink, *mockSink) {
	mockSink := &mockSink{}
	ddlSink := newDDLSink(model.DefaultChangeFeedID("changefeed-test"), &model.ChangeFeedInfo{}, reportErr)
	ddlSink.(*ddlSinkImpl).sinkInitHandler = func(ctx context.Context, s *ddlSinkImpl) error {
		s.sinkV1 = mockSink
		return nil
	}
	return ddlSink, mockSink
}

func TestCheckpoint(t *testing.T) {
	ddlSink, mSink := newDDLSink4Test(func(err error) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		ddlSink.close(ctx)
	}()
	ddlSink.run(ctx)

	waitCheckpointGrowingUp := func(m *mockSink, targetTs model.Ts) error {
		return retry.Do(ctx, func() error {
			if targetTs != atomic.LoadUint64(&m.checkpointTs) {
				return errors.New("targetTs!=checkpointTs")
			}
			return nil
		}, retry.WithBackoffBaseDelay(100), retry.WithMaxTries(30))
	}
	ddlSink.emitCheckpointTs(1, nil)
	require.Nil(t, waitCheckpointGrowingUp(mSink, 1))
	ddlSink.emitCheckpointTs(10, nil)
	require.Nil(t, waitCheckpointGrowingUp(mSink, 10))
}

func TestExecDDLEvents(t *testing.T) {
	ddlSink, mSink := newDDLSink4Test(func(err error) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		ddlSink.close(ctx)
	}()
	ddlSink.run(ctx)

	ddlEvents := []*model.DDLEvent{
		{CommitTs: 1},
		{CommitTs: 2},
		{CommitTs: 3},
	}

	for _, event := range ddlEvents {
		for {
			done, err := ddlSink.emitDDLEvent(ctx, event)
			require.Nil(t, err)
			if done {
				require.Equal(t, mSink.GetDDL(), event)
				break
			}
		}
	}
}

func TestExecDDLError(t *testing.T) {
	var (
		resultErr   error
		resultErrMu sync.Mutex
	)
	readResultErr := func() error {
		resultErrMu.Lock()
		defer resultErrMu.Unlock()
		return resultErr
	}

	ddlSink, mSink := newDDLSink4Test(func(err error) {
		resultErrMu.Lock()
		defer resultErrMu.Unlock()
		resultErr = err
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		ddlSink.close(ctx)
	}()

	ddlSink.run(ctx)

	mSink.ddlError = cerror.ErrExecDDLFailed.GenWithStackByArgs()
	ddl2 := &model.DDLEvent{CommitTs: 2}
	for {
		done, err := ddlSink.emitDDLEvent(ctx, ddl2)
		require.Nil(t, err)

		if done || readResultErr() != nil {
			require.Equal(t, mSink.GetDDL(), ddl2)
			break
		}
	}
	require.True(t, cerror.ErrExecDDLFailed.Equal(readResultErr()))
}
