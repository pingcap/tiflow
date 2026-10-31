// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package syncer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/config"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func rowExecutorTestContext(n uint32) eventContext {
	ec := eventContext{header: &replication.EventHeader{EventSize: n, EventType: replication.XID_EVENT}}
	ec.startLocation.Position.Name = "mysql-bin.000001"
	ec.endLocation.Position.Name = "mysql-bin.000001"
	ec.startLocation.Position.Pos = n
	ec.endLocation.Position.Pos = n + 1
	return ec
}

func TestRowEventExecutorOrderAndDrain(t *testing.T) {
	var got []uint32
	e := newRowEventExecutor(context.Background(), 3, 16, func(op rowEventOperation) error {
		got = append(got, op.context.startLocation.Position.Pos)
		return nil
	}, func() {})
	for i := uint32(1); i <= 200; i++ {
		var rows *replication.RowsEvent
		if i%3 != 0 {
			rows = &replication.RowsEvent{}
		}
		require.NoError(t, e.Submit(rows, rowExecutorTestContext(i)))
		if i%13 == 0 {
			require.NoError(t, e.Drain())
			require.Len(t, got, int(i))
		}
	}
	require.NoError(t, e.Close())
	require.Len(t, got, 200)
	for i, v := range got {
		require.Equal(t, uint32(i+1), v)
	}
	require.True(t, e.budget.TryAcquire(16))
}

func TestRowEventExecutorBackpressureAndCancellation(t *testing.T) {
	for _, tt := range []struct {
		name     string
		capacity int
		limit    int64
		size     uint32
	}{
		{"bytes", 4, 8, 8}, {"oversized", 4, 8, 100}, {"events", 1, 1000, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			releaseHandler := sync.OnceFunc(func() { close(release) })
			t.Cleanup(releaseHandler)
			calls := 0
			e := newRowEventExecutor(ctx, tt.capacity, tt.limit, func(op rowEventOperation) error {
				calls++
				if calls == 1 {
					close(entered)
					<-release
				}
				return nil
			}, cancel)
			require.NoError(t, e.Submit(nil, rowExecutorTestContext(tt.size)))
			<-entered
			if tt.name == "events" {
				require.NoError(t, e.Submit(nil, rowExecutorTestContext(tt.size)))
			}
			submitted := make(chan error, 1)
			go func() { submitted <- e.Submit(nil, rowExecutorTestContext(tt.size)) }()
			select {
			case err := <-submitted:
				t.Fatalf("admission must block: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			cancel()
			select {
			case err := <-submitted:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("blocked admission did not cancel")
			}
			drained := make(chan error, 1)
			go func() { drained <- e.Drain() }()
			select {
			case <-drained:
				t.Fatal("Drain passed executing work")
			case <-time.After(20 * time.Millisecond):
			}
			releaseHandler()
			require.NoError(t, <-drained)
			require.NoError(t, e.Close())
			require.True(t, e.budget.TryAcquire(tt.limit))
		})
	}
}

func TestRowEventExecutorFirstError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	releaseHandler := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseHandler)
	injected := errors.New("row failure")
	calls := 0
	e := newRowEventExecutor(ctx, 8, 1024, func(op rowEventOperation) error {
		calls++
		close(entered)
		<-release
		return injected
	}, cancel)
	require.NoError(t, e.Submit(&replication.RowsEvent{}, rowExecutorTestContext(1)))
	<-entered
	for i := uint32(2); i <= 5; i++ {
		require.NoError(t, e.Submit(nil, rowExecutorTestContext(i)))
	}
	releaseHandler()
	require.ErrorIs(t, e.Drain(), injected)
	require.ErrorIs(t, e.Submit(nil, rowExecutorTestContext(6)), injected)
	require.ErrorIs(t, e.Close(), injected)
	require.Equal(t, 1, calls)
	require.True(t, e.budget.TryAcquire(1024))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestOrderedRowHandlerErrorLocationAndPanic(t *testing.T) {
	for _, panicCase := range []bool{false, true} {
		s := &Syncer{cfg: &config.SubTaskConfig{}, tctx: tcontext.Background()}
		s.runCtx = s.tctx
		s.runCancel = func() {}
		injected := errors.New("commit failed")
		s.handleJobFunc = func(*job) (bool, error) {
			if panicCase {
				panic("injected")
			}
			return false, injected
		}
		h := &orderedRowHandler{syncer: s, affectedSourceTables: make(map[string]map[string]struct{})}
		ec := rowExecutorTestContext(42)
		err := h.handle(rowEventOperation{context: ec})
		require.Error(t, err)
		require.ErrorContains(t, err, "42")
		require.Equal(t, ec.startLocation, s.errLocation.startLocation.Clone())
		require.Equal(t, ec.endLocation, s.errLocation.endLocation.Clone())
		if panicCase {
			require.True(t, terror.ErrSyncerUnitPanic.Equal(err))
		} else {
			require.ErrorIs(t, err, injected)
		}
	}
}

func TestOrderedRowHandlerStopsAtTransactionBoundary(t *testing.T) {
	s := &Syncer{cfg: &config.SubTaskConfig{}, tctx: tcontext.Background()}
	s.runCtx = s.tctx
	canceled := 0
	s.runCancel = func() { canceled++ }
	calls := 0
	s.handleJobFunc = func(*job) (bool, error) { calls++; s.waitXIDJob.Store(int64(waitComplete)); return false, nil }
	h := &orderedRowHandler{syncer: s, affectedSourceTables: make(map[string]map[string]struct{})}
	require.NoError(t, h.handle(rowEventOperation{context: rowExecutorTestContext(1)}))
	// No later row/commit is processed after the accepted transaction boundary.
	require.NoError(t, h.handle(rowEventOperation{rows: &replication.RowsEvent{}, context: rowExecutorTestContext(2)}))
	require.NoError(t, h.handle(rowEventOperation{context: rowExecutorTestContext(3)}))
	require.Equal(t, 1, calls)
	require.Equal(t, 3, canceled)
}

func TestOrderedRowHandlerCanceledWorkDoesNotAdvance(t *testing.T) {
	s := &Syncer{cfg: &config.SubTaskConfig{}, tctx: tcontext.Background()}
	ctx, cancel := context.WithCancel(context.Background())
	s.runCtx = s.tctx.WithContext(ctx)
	s.runCancel = cancel
	s.handleJobFunc = func(*job) (bool, error) { t.Error("canceled work must not publish jobs"); return false, nil }
	h := &orderedRowHandler{syncer: s, affectedSourceTables: make(map[string]map[string]struct{})}
	cancel()
	require.NoError(t, h.handle(rowEventOperation{rows: &replication.RowsEvent{}, context: rowExecutorTestContext(1)}))
	require.NoError(t, h.handle(rowEventOperation{context: rowExecutorTestContext(2)}))
}

// Exercise the existing mixed DML/DDL, pause/resume and route-update fixture
// with transaction-level GTID checkpoints as well as its position-mode run.
func (s *testSyncerSuite) TestRowEventRunGTID(c *check.C) {
	oldCfg, oldGenerator := s.cfg, s.eventsGenerator
	defer func() { s.cfg, s.eventsGenerator = oldCfg, oldGenerator }()
	cfg, err := s.cfg.Clone()
	c.Assert(err, check.IsNil)
	s.cfg = cfg
	s.cfg.EnableGTID = true
	s.resetEventsGenerator(c)
	s.TestRun(c)
}
