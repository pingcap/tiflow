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

// OnTxnEvent waits until released and acknowledges the received transaction.
func (b *blackhole) OnTxnEvent(e *dmlsink.TxnCallbackableEvent, _ bool) bool {
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

// commitRecorder records transaction batches and table-order flags received by the backend.
type commitRecorder struct {
	events  []*dmlsink.TxnCallbackableEvent
	commits chan []uint64

	tableOrders chan bool
}

// OnTxnEvent records the event and requests an immediate flush for ordered tables.
func (b *commitRecorder) OnTxnEvent(e *dmlsink.TxnCallbackableEvent, tableOrder bool) bool {
	if b.tableOrders != nil {
		b.tableOrders <- tableOrder
	}
	b.events = append(b.events, e)
	return tableOrder
}

// Flush records commit timestamps and acknowledges buffered events.
func (b *commitRecorder) Flush(_ context.Context) error {
	if len(b.events) == 0 {
		return nil
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

// MaxFlushInterval bounds the recorder's batching delay.
func (b *commitRecorder) MaxFlushInterval() time.Duration { return 10 * time.Millisecond }

// Close releases no resources because the recorder owns no background workers.
func (b *commitRecorder) Close() error { return nil }

// orderedTestEvent builds a single-row transaction for ordering tests.
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

// TestTxnOrderTableFlag checks forwarding of matched ordering rules to the backend.
func TestTxnOrderTableFlag(t *testing.T) {
	tableOrders := make(chan bool, 3)
	errCh := make(chan error, 1)
	recorder := &commitRecorder{commits: make(chan []uint64, 3), tableOrders: tableOrders}
	s := newSink(context.Background(), model.DefaultChangeFeedID(t.Name()), []backend{recorder}, errCh, DefaultConflictDetectorSlots)
	defer s.Close()
	cfg := config.GetDefaultReplicaConfig()
	cfg.Sink.MySQLConfig = &config.MySQLConfig{TxnOrderTables: []string{"app.orders"}}
	var err error
	s.txnOrderFilter, err = cfg.Sink.GetTxnOrderFilter(false)
	require.NoError(t, err)
	withoutTableInfo := orderedTestEvent(3, 1, "orders", 3)
	withoutTableInfo.Event.TableInfo = nil
	require.NoError(t, s.WriteEvents(
		orderedTestEvent(1, 2, "other", 1),
		orderedTestEvent(2, 1, "orders", 2),
		withoutTableInfo,
	))
	for _, want := range []bool{false, true, false} {
		select {
		case got := <-tableOrders:
			require.Equal(t, want, got)
		case err := <-errCh:
			t.Fatal(err)
		case <-time.After(10 * time.Second):
			t.Fatal("backend did not receive the ordering flag")
		}
	}
}
