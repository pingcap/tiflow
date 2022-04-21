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

package sink

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/stretchr/testify/require"
)

func TestTableIsNotFlushed(t *testing.T) {
	t.Parallel()

	b := bufferSink{changeFeedCheckpointTs: 1}
	require.Equal(t, uint64(1), b.getTableCheckpointTs(2))
	b.UpdateChangeFeedCheckpointTs(3)
	require.Equal(t, uint64(3), b.getTableCheckpointTs(2))
}

func TestFlushTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	b := newBufferSink(newBlackHoleSink(ctx), 5)
	go b.run(ctx, "", make(chan error))

	require.Equal(t, uint64(5), b.getTableCheckpointTs(2))
	require.Nil(t, b.EmitRowChangedEvents(ctx))
	tbl1 := &model.TableName{TableID: 1}
	tbl2 := &model.TableName{TableID: 2}
	tbl3 := &model.TableName{TableID: 3}
	tbl4 := &model.TableName{TableID: 4}
	require.Nil(t, b.EmitRowChangedEvents(ctx, []*model.RowChangedEvent{
		{CommitTs: 6, Table: tbl1},
		{CommitTs: 6, Table: tbl2},
		{CommitTs: 6, Table: tbl3},
		{CommitTs: 6, Table: tbl4},
		{CommitTs: 10, Table: tbl1},
		{CommitTs: 10, Table: tbl2},
		{CommitTs: 10, Table: tbl3},
		{CommitTs: 10, Table: tbl4},
	}...))
	checkpoint, err := b.FlushRowChangedEvents(ctx, 1, 7)
	require.True(t, checkpoint <= 7)
	require.Nil(t, err)
	checkpoint, err = b.FlushRowChangedEvents(ctx, 2, 6)
	require.True(t, checkpoint <= 6)
	require.Nil(t, err)
	checkpoint, err = b.FlushRowChangedEvents(ctx, 3, 8)
	require.True(t, checkpoint <= 8)
	require.Nil(t, err)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, uint64(7), b.getTableCheckpointTs(1))
	require.Equal(t, uint64(6), b.getTableCheckpointTs(2))
	require.Equal(t, uint64(8), b.getTableCheckpointTs(3))
	require.Equal(t, uint64(5), b.getTableCheckpointTs(4))
	b.UpdateChangeFeedCheckpointTs(6)
	require.Equal(t, uint64(7), b.getTableCheckpointTs(1))
	require.Equal(t, uint64(6), b.getTableCheckpointTs(2))
	require.Equal(t, uint64(8), b.getTableCheckpointTs(3))
	require.Equal(t, uint64(6), b.getTableCheckpointTs(4))
}

func TestFlushFailed(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.TODO())
	b := newBufferSink(newBlackHoleSink(ctx), 5)
	go b.run(ctx, "", make(chan error))

	checkpoint, err := b.FlushRowChangedEvents(ctx, 3, 8)
	require.True(t, checkpoint <= 8)
	require.Nil(t, err)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, uint64(8), b.getTableCheckpointTs(3))
	cancel()
	checkpoint, _ = b.FlushRowChangedEvents(ctx, 3, 18)
	require.Equal(t, uint64(8), checkpoint)
	checkpoint, _ = b.FlushRowChangedEvents(ctx, 1, 18)
	require.Equal(t, uint64(5), checkpoint)
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, uint64(8), b.getTableCheckpointTs(3))
	require.Equal(t, uint64(5), b.getTableCheckpointTs(1))
}

func TestCleanBufferedData(t *testing.T) {
	t.Parallel()

	tblID := model.TableID(1)
	b := newBufferSink(newBlackHoleSink(context.TODO()), 5)
	b.buffer[tblID] = []*model.RowChangedEvent{}
	_, ok := b.buffer[tblID]
	require.True(t, ok)
	require.Nil(t, b.Init(tblID))
	_, ok = b.buffer[tblID]
	require.False(t, ok)
}

type benchSink struct {
	Sink
}

func (b *benchSink) EmitRowChangedEvents(
	ctx context.Context, rows ...*model.RowChangedEvent,
) error {
	return nil
}

func (b *benchSink) FlushRowChangedEvents(
	ctx context.Context, tableID model.TableID, resolvedTs uint64,
) (uint64, error) {
	return 0, nil
}

func BenchmarkRun(b *testing.B) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	state := runState{
		metricTotalRows: metrics.BufferSinkTotalRowsCountCounter.WithLabelValues(b.Name()),
	}

	for exp := 0; exp < 9; exp++ {
		count := int(math.Pow(4, float64(exp)))
		s := newBufferSink(&benchSink{}, 5)
		s.flushTsChan = make(chan flushMsg, count)
		for i := 0; i < count; i++ {
			s.buffer[int64(i)] = []*model.RowChangedEvent{{CommitTs: 5}}
		}
		b.ResetTimer()

		b.Run(fmt.Sprintf("%d table(s)", count), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for j := 0; j < count; j++ {
					s.flushTsChan <- flushMsg{tableID: int64(0)}
				}
				for len(s.flushTsChan) != 0 {
					keepRun, err := s.runOnce(ctx, &state)
					if err != nil || !keepRun {
						b.Fatal(keepRun, err)
					}
				}
			}
		})
	}
}
