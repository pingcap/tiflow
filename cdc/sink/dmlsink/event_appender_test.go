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
// limitations under the License

package dmlsink

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestRowChangeEventAppender(t *testing.T) {
	t.Parallel()

	tableInfo := &model.TableInfo{
		TableName: model.TableName{
			Schema:      "test",
			Table:       "t1",
			TableID:     1,
			IsPartition: false,
		},
		Version: 1,
	}

	appender := &RowChangeEventAppender{}
	var buffer []*model.RowChangedEvent
	rows := []*model.RowChangedEvent{
		{
			TableInfo: tableInfo,
			CommitTs:  1,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  2,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  2,
		},
	}
	buffer = appender.Append(buffer, rows...)
	require.Len(t, buffer, 3)
	// Should be ordered by commitTs.
	require.Equal(t, uint64(1), buffer[0].GetCommitTs())
	require.Equal(t, uint64(2), buffer[1].GetCommitTs())
	require.Equal(t, uint64(2), buffer[2].GetCommitTs())
}

func TestTxnEventAppenderWithoutIgnoreStartTs(t *testing.T) {
	t.Parallel()

	tableInfo := &model.TableInfo{
		Version: 1,
		TableName: model.TableName{
			Schema:      "test",
			Table:       "t1",
			TableID:     1,
			IsPartition: false,
		},
	}

	appender := &TxnEventAppender{}
	var buffer []*model.SingleTableTxn
	rows := []*model.RowChangedEvent{
		{
			TableInfo: tableInfo,
			CommitTs:  101,
			StartTs:   98,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  102,
			StartTs:   99,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  102,
			StartTs:   100,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  102,
			StartTs:   100,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  103,
			StartTs:   101,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  103,
			StartTs:   101,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  104,
			StartTs:   102,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  105,
			StartTs:   103,
			// Batch1
			SplitTxn: true,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  105,
			StartTs:   103,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  105,
			StartTs:   103,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  105,
			StartTs:   103,
			// Batch2
			SplitTxn: true,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  105,
			StartTs:   103,
		},
	}
	buffer = appender.Append(buffer, rows...)
	require.Len(t, buffer, 7)
	// Make sure the order is correct.
	require.Equal(t, uint64(101), buffer[0].GetCommitTs())
	// Make sure grouped by startTs and batch.
	require.Len(t, buffer[0].Rows, 1)

	require.Equal(t, uint64(102), buffer[1].GetCommitTs())
	require.Len(t, buffer[1].Rows, 1)

	require.Equal(t, uint64(102), buffer[2].GetCommitTs())
	require.Len(t, buffer[2].Rows, 2)

	require.Equal(t, uint64(103), buffer[3].GetCommitTs())
	require.Len(t, buffer[3].Rows, 2)

	require.Equal(t, uint64(104), buffer[4].GetCommitTs())
	require.Len(t, buffer[4].Rows, 1)

	require.Equal(t, uint64(105), buffer[5].GetCommitTs())
	require.Len(t, buffer[5].Rows, 3)

	require.Equal(t, uint64(105), buffer[6].GetCommitTs())
	require.Len(t, buffer[6].Rows, 2)

	// Test the case which the commitTs is not strictly increasing.
	rows = []*model.RowChangedEvent{
		{
			TableInfo: tableInfo,
			CommitTs:  101,
			StartTs:   98,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  100,
			StartTs:   99,
		},
	}
	buffer = buffer[:0]
	require.Panics(t, func() {
		buffer = appender.Append(buffer, rows...)
	})

	// Test the case which the startTs is not strictly increasing.
	rows = []*model.RowChangedEvent{
		{
			TableInfo: tableInfo,
			CommitTs:  101,
			StartTs:   98,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  101,
			StartTs:   80,
		},
	}
	buffer = buffer[:0]
	require.Panics(t, func() {
		buffer = appender.Append(buffer, rows...)
	})
}

func TestTxnEventAppenderWithIgnoreStartTs(t *testing.T) {
	t.Parallel()

	tableInfo := &model.TableInfo{
		Version: 1,
		TableName: model.TableName{
			Schema:      "test",
			Table:       "t1",
			TableID:     1,
			IsPartition: false,
		},
	}

	appender := &TxnEventAppender{IgnoreStartTs: true}
	var buffer []*model.SingleTableTxn
	rows := []*model.RowChangedEvent{
		{
			TableInfo: tableInfo,
			CommitTs:  101,
			StartTs:   0,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  101,
			StartTs:   0,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  102,
			StartTs:   90,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  102,
			StartTs:   91,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  103,
			StartTs:   0,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  103,
			StartTs:   0,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  104,
			StartTs:   0,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  105,
			StartTs:   0,
			// Batch1
			SplitTxn: true,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  105,
			StartTs:   0,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  105,
			StartTs:   0,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  105,
			StartTs:   0,
			// Batch2
			SplitTxn: true,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  105,
			StartTs:   0,
		},
	}
	buffer = appender.Append(buffer, rows...)
	require.Len(t, buffer, 7)
	// Make sure the order is correct.
	require.Equal(t, uint64(101), buffer[0].GetCommitTs())
	// Make we can ignore the startTs.
	require.Len(t, buffer[0].Rows, 2)

	// Make sure if the startTs is not 0, we can't deal with it.
	require.Equal(t, uint64(102), buffer[1].GetCommitTs())
	require.Len(t, buffer[1].Rows, 1)
	require.Equal(t, uint64(102), buffer[2].GetCommitTs())
	require.Len(t, buffer[2].Rows, 1)

	require.Equal(t, uint64(103), buffer[3].GetCommitTs())
	require.Len(t, buffer[3].Rows, 2)

	require.Equal(t, uint64(104), buffer[4].GetCommitTs())
	require.Len(t, buffer[4].Rows, 1)

	// First batch.
	require.Equal(t, uint64(105), buffer[5].GetCommitTs())
	require.Len(t, buffer[5].Rows, 3)

	// Second batch.
	require.Equal(t, uint64(105), buffer[6].GetCommitTs())
	require.Len(t, buffer[6].Rows, 2)

	// Test the case which the commitTs is not strictly increasing.
	rows = []*model.RowChangedEvent{
		{
			TableInfo: tableInfo,
			CommitTs:  101,
			StartTs:   98,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  100,
			StartTs:   99,
		},
	}
	buffer = buffer[:0]
	require.Panics(t, func() {
		buffer = appender.Append(buffer, rows...)
	})

	// Test the case which the startTs is not strictly increasing.
	rows = []*model.RowChangedEvent{
		{
			TableInfo: tableInfo,
			CommitTs:  101,
			StartTs:   98,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  101,
			StartTs:   80,
		},
	}
	buffer = buffer[:0]
	require.Panics(t, func() {
		buffer = appender.Append(buffer, rows...)
	})

	// Test the case which the startTs all is 0.
	rows = []*model.RowChangedEvent{
		{
			TableInfo: tableInfo,
			CommitTs:  101,
			StartTs:   0,
		},
		{
			TableInfo: tableInfo,
			CommitTs:  101,
			StartTs:   0,
		},
	}
	buffer = buffer[:0]
	require.NotPanics(t, func() {
		buffer = appender.Append(buffer, rows...)
	})
}
