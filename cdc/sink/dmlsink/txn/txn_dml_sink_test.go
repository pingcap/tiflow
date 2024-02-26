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
