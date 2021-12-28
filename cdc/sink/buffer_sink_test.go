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
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestTableIsNotFlushed(t *testing.T) {
	b := bufferSink{changeFeedCheckpointTs: 1}
	require.Equal(t, uint64(1), b.getTableCheckpointTs(2))
	b.UpdateChangeFeedCheckpointTs(3)
	require.Equal(t, uint64(3), b.getTableCheckpointTs(2))
}

func TestFlushTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer func() {
		cancel()
	}()
	b := newBufferSink(ctx, newBlackHoleSink(ctx, make(map[string]string)), make(chan error), 5, make(chan drawbackMsg))
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
	ctx, cancel := context.WithCancel(context.TODO())
	b := newBufferSink(ctx, newBlackHoleSink(ctx, make(map[string]string)), make(chan error), 5, make(chan drawbackMsg))
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
