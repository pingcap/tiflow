// Copyright 2022 PingCAP, Inc.
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

package txn

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

type blackhole struct {
	blockOnEvents int32
	panicOnFlush  int32
}

func (b *blackhole) OnTxnEvent(e *dmlsink.TxnCallbackableEvent) bool {
	for {
		if atomic.LoadInt32(&b.blockOnEvents) > 0 {
			time.Sleep(time.Millisecond * time.Duration(100))
		} else {
			break
		}
	}
	e.Callback()
	return true
}

func (b *blackhole) Flush(ctx context.Context) error {
	if atomic.LoadInt32(&b.panicOnFlush) > 0 {
		panic("blackhole panics")
	}
	return nil
}

func (b *blackhole) MaxFlushInterval() time.Duration {
	return 100 * time.Millisecond
}

func (b *blackhole) Close() error {
	return nil
}

// TestTxnSinkNolocking checks TxnSink must be nonblocking even if the associated
// backends can be blocked in OnTxnEvent.
func TestTxnSinkNolocking(t *testing.T) {
	t.Parallel()

	bes := make([]backend, 0, 4)
	for i := 0; i < 4; i++ {
		bes = append(bes, &blackhole{blockOnEvents: 1})
	}
	errCh := make(chan error, 1)
	sink := newSink(context.Background(),
		model.DefaultChangeFeedID("test"), bes, errCh, DefaultConflictDetectorSlots)

	// Test `WriteEvents` shouldn't be blocked by slow workers.
	var handled uint32 = 0
	tableInfo := model.BuildTableInfo("test", "t1", []*model.Column{
		{Name: "a", Type: mysql.TypeLong},
		{Name: "b", Type: mysql.TypeLong},
	}, nil)
	for i := 0; i < 100; i++ {
		sinkState := new(state.TableSinkState)
		*sinkState = state.TableSinkSinking
		e := &dmlsink.CallbackableEvent[*model.SingleTableTxn]{
			Event: &model.SingleTableTxn{
				Rows: []*model.RowChangedEvent{
					{
						TableInfo: tableInfo,
						Columns: model.Columns2ColumnDatas([]*model.Column{
							{Name: "a", Value: 1},
							{Name: "b", Value: 2},
						}, tableInfo),
					},
				},
			},
			Callback:  func() { atomic.AddUint32(&handled, 1) },
			SinkState: sinkState,
		}
		sink.WriteEvents(e)
	}

	time.Sleep(time.Second)
	require.Equal(t, uint32(0), atomic.LoadUint32(&handled))

	for _, be := range bes {
		atomic.StoreInt32(&be.(*blackhole).blockOnEvents, 0)
	}
	time.Sleep(time.Second)
	require.Equal(t, uint32(100), atomic.LoadUint32(&handled))
	sink.Close()
}

// commitRecorder keeps transaction boundaries visible while simulating a blocked sink.
type commitRecorder struct {
	events  []*dmlsink.TxnCallbackableEvent
	commits chan []uint64
	release <-chan struct{}
	started chan struct{}
	once    *sync.Once
}

func (b *commitRecorder) OnTxnEvent(e *dmlsink.TxnCallbackableEvent) bool {
	b.events = append(b.events, e)
	return true // Model a backend configured with max-txn-row=1.
}

func (b *commitRecorder) Flush(ctx context.Context) error {
	if len(b.events) == 0 {
		return nil
	}
	if b.started != nil {
		b.once.Do(func() { close(b.started) })
	}
	if b.release != nil {
		select {
		case <-b.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ts := make([]uint64, 0, len(b.events))
	for _, event := range b.events {
		ts = append(ts, event.Event.CommitTs)
		event.Callback()
	}
	b.events = nil
	b.commits <- ts
	return nil
}

func (b *commitRecorder) MaxFlushInterval() time.Duration { return time.Hour }
func (b *commitRecorder) Close() error                    { return nil }

func orderedTestEvent(commitTs uint64, tableID int64, tableName string, rowID int) *dmlsink.TxnCallbackableEvent {
	ti := model.BuildTableInfo("app", tableName, []*model.Column{
		{Name: "id", Type: mysql.TypeLong, Flag: model.PrimaryKeyFlag | model.HandleKeyFlag},
	}, [][]int{{0}})
	sinkState := state.TableSinkSinking
	return &dmlsink.TxnCallbackableEvent{
		Event: &model.SingleTableTxn{
			PhysicalTableID: tableID, TableInfo: ti, StartTs: commitTs - 1, CommitTs: commitTs,
			Rows: []*model.RowChangedEvent{{
				TableInfo: ti, PhysicalTableID: tableID,
				Columns: []*model.ColumnData{{ColumnID: ti.Columns[0].ID, Value: rowID}},
			}},
		},
		Callback: func() {}, SinkState: &sinkState,
	}
}

func TestTxnOrderTablesBacklog(t *testing.T) {
	for _, workerCount := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers=%d", workerCount), func(t *testing.T) {
			const count = 2101 // Exceed the 1024-entry worker cache, as in issue #12936.
			commits := make(chan []uint64, count)
			release, started := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			backends := make([]backend, workerCount)
			for i := range backends {
				backends[i] = &commitRecorder{commits: commits, release: release, started: started, once: &once}
			}
			errCh := make(chan error, 1)
			s := newSink(context.Background(), model.DefaultChangeFeedID("ordered"), backends, errCh, DefaultConflictDetectorSlots)
			defer s.Close()
			defer unblock() // Release a blocked Flush before closing the sink on test failure.
			cfg := config.GetDefaultReplicaConfig()
			cfg.Sink.MySQLConfig = &config.MySQLConfig{TxnOrderTables: []string{"app.orders"}}
			var err error
			s.txnOrderFilter, err = cfg.Sink.GetTxnOrderFilter(false)
			require.NoError(t, err)
			first := orderedTestEvent(1, 1, "orders", 1)
			require.NoError(t, s.WriteEvents(first))
			select {
			case <-started:
			case <-time.After(10 * time.Second):
				t.Fatal("sink did not start flushing")
			}
			for i := 2; i <= count; i++ {
				rowID := 1
				if i == count {
					rowID = 2 // An independent row committed after all hot-row transactions.
				}
				require.NoError(t, s.WriteEvents(orderedTestEvent(uint64(i), 1, "orders", rowID)))
			}
			unblock()
			for i := 1; i <= count; i++ {
				select {
				case commit := <-commits:
					require.Equal(t, []uint64{uint64(i)}, commit)
				case err := <-errCh:
					t.Fatal(err)
				case <-time.After(10 * time.Second):
					t.Fatalf("missing transaction %d", i)
				}
			}
		})
	}
}
