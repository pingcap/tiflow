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

package sinkmanager

import (
	"context"
	"sync"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sorter"
	"github.com/pingcap/tiflow/pkg/sorter/memory"
	"github.com/stretchr/testify/require"
)

func createWorker(changefeedID model.ChangeFeedID, memQuota uint64, splitTxn bool) worker {
	sorterEngine := memory.New(context.Background())
	quota := newMemQuota(changefeedID, memQuota)
	return newWorker(changefeedID, sorterEngine, nil, quota, splitTxn, false)
}

func addEventsToSorterEngine(t *testing.T, events []*model.PolymorphicEvent, sorterEngine sorter.EventSortEngine, tableID model.TableID) {
	sorterEngine.AddTable(tableID)
	for _, event := range events {
		err := sorterEngine.Add(tableID, event)
		require.NoError(t, err)
	}
}

//nolint:unparam
func genRowChangedEvent(startTs, commitTs uint64, tableID model.TableID) *model.RowChangedEvent {
	return &model.RowChangedEvent{
		StartTs:  startTs,
		CommitTs: commitTs,
		Table: &model.TableName{
			Schema:      "table",
			Table:       "table",
			TableID:     tableID,
			IsPartition: false,
		},
		Columns: []*model.Column{
			{Name: "a", Value: 2},
		},
		PreColumns: []*model.Column{
			{Name: "a", Value: 1},
		},
	}
}

// Test the case that the worker will stop when no memory quota and meet the txn boundary.
func TestReceiveTableSinkTaskWithSplitTxnAndAbortWhenNoMemAndOneTxnFinished(t *testing.T) {
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Only for three events.
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	eventSize := uint64(218 * 2)
	defaultRequestMemSize = 218

	events := []*model.PolymorphicEvent{
		{
			StartTs: 1,
			CRTs:    1,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    1,
			},
			Row: genRowChangedEvent(1, 1, tableID),
		},
		{
			StartTs: 1,
			CRTs:    2,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    2,
			},
			Row: genRowChangedEvent(1, 2, tableID),
		},
		{
			StartTs: 1,
			CRTs:    3,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    3,
			},
			Row: genRowChangedEvent(1, 3, tableID),
		},
		{
			StartTs: 2,
			CRTs:    4,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 2,
				CRTs:    4,
			},
			Row: genRowChangedEvent(2, 4, tableID),
		},
		{
			CRTs: 4,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
				CRTs:   4,
			},
		},
	}

	w := createWorker(changefeedID, eventSize, true)
	addEventsToSorterEngine(t, events, w.(*workerImpl).sortEngine, tableID)

	taskChan := make(chan *tableSinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.receiveTableSinkTask(ctx, taskChan)
		require.Equal(t, context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(changefeedID, tableID)
	lowerBoundPos := sorter.Position{
		StartTs:  0,
		CommitTs: 1,
	}
	upperBoundGetter := func() sorter.Position {
		return sorter.Position{
			StartTs:  3,
			CommitTs: 4,
		}
	}
	callback := func(lastWritePos sorter.Position) {
		require.Equal(t, sorter.Position{
			StartTs:  1,
			CommitTs: 3,
		}, lastWritePos)
		require.Equal(t, sorter.Position{
			StartTs:  2,
			CommitTs: 3,
		}, lastWritePos.Next())
		cancel()
	}
	taskChan <- &tableSinkTask{
		tableID:              tableID,
		lowerBound:           lowerBoundPos,
		upperBarrierTsGetter: upperBoundGetter,
		tableSink:            wrapper,
		callback:             callback,
	}
	wg.Wait()
	require.Len(t, sink.events, 3)
}

// Test the case that worker will block when no memory quota until the mem quota is aborted.
func TestReceiveTableSinkTaskWithSplitTxnAndAbortWhenNoMemAndBlocked(t *testing.T) {
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Only for three events.
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	eventSize := uint64(218 * 2)
	defaultRequestMemSize = 218

	events := []*model.PolymorphicEvent{
		{
			StartTs: 1,
			CRTs:    1,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    1,
			},
			Row: genRowChangedEvent(1, 1, tableID),
		},
		{
			StartTs: 1,
			CRTs:    2,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    2,
			},
			Row: genRowChangedEvent(1, 2, tableID),
		},
		{
			StartTs: 1,
			CRTs:    2,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    2,
			},
			Row: genRowChangedEvent(1, 2, tableID),
		},
		{
			StartTs: 1,
			CRTs:    2,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    2,
			},
			Row: genRowChangedEvent(1, 2, tableID),
		},
		{
			StartTs: 1,
			CRTs:    3,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    3,
			},
			Row: genRowChangedEvent(1, 3, tableID),
		},
		{
			CRTs: 4,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
				CRTs:   4,
			},
		},
	}
	w := createWorker(changefeedID, eventSize, true)
	addEventsToSorterEngine(t, events, w.(*workerImpl).sortEngine, tableID)

	taskChan := make(chan *tableSinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.receiveTableSinkTask(ctx, taskChan)
		require.ErrorIs(t, err, cerrors.ErrFlowControllerAborted)
	}()

	wrapper, sink := createTableSinkWrapper(changefeedID, tableID)
	lowerBoundPos := sorter.Position{
		StartTs:  0,
		CommitTs: 1,
	}
	upperBoundGetter := func() sorter.Position {
		return sorter.Position{
			StartTs:  3,
			CommitTs: 4,
		}
	}
	callback := func(lastWritePos sorter.Position) {
		require.Equal(t, sorter.Position{
			StartTs:  1,
			CommitTs: 2,
		}, lastWritePos)
		require.Equal(t, sorter.Position{
			StartTs:  2,
			CommitTs: 2,
		}, lastWritePos.Next())
		cancel()
	}
	taskChan <- &tableSinkTask{
		tableID:              tableID,
		lowerBound:           lowerBoundPos,
		upperBarrierTsGetter: upperBoundGetter,
		tableSink:            wrapper,
		callback:             callback,
	}
	// Abort the task when no memory quota and blocked.
	w.(*workerImpl).memQuota.close()
	wg.Wait()
	require.Len(t, sink.events, 1, "Only one txn should be sent to sink before abort")
}

// Test the case that the worker will force consume only one Txn when the memory quota is not enough.
func TestReceiveTableSinkTaskWithoutSplitTxnAndAbortWhenNoMemAndForceConsume(t *testing.T) {
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Only for three events.
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	eventSize := uint64(218 * 2)
	defaultRequestMemSize = 218

	events := []*model.PolymorphicEvent{
		{
			StartTs: 1,
			CRTs:    1,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    1,
			},
			Row: genRowChangedEvent(1, 1, tableID),
		},
		{
			StartTs: 1,
			CRTs:    2,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    2,
			},
			Row: genRowChangedEvent(1, 2, tableID),
		},
		{
			StartTs: 1,
			CRTs:    3,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    3,
			},
			Row: genRowChangedEvent(1, 3, tableID),
		},
		{
			StartTs: 1,
			CRTs:    3,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    3,
			},
			Row: genRowChangedEvent(1, 3, tableID),
		},
		{
			StartTs: 1,
			CRTs:    3,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    3,
			},
			Row: genRowChangedEvent(1, 3, tableID),
		},
		{
			StartTs: 1,
			CRTs:    4,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    4,
			},
			Row: genRowChangedEvent(1, 4, tableID),
		},
		{
			CRTs: 5,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
				CRTs:   5,
			},
		},
	}
	w := createWorker(changefeedID, eventSize, false)
	addEventsToSorterEngine(t, events, w.(*workerImpl).sortEngine, tableID)

	taskChan := make(chan *tableSinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.receiveTableSinkTask(ctx, taskChan)
		require.Equal(t, context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(changefeedID, tableID)
	lowerBoundPos := sorter.Position{
		StartTs:  0,
		CommitTs: 1,
	}
	upperBoundGetter := func() sorter.Position {
		return sorter.Position{
			StartTs:  3,
			CommitTs: 4,
		}
	}
	callback := func(lastWritePos sorter.Position) {
		require.Equal(t, sorter.Position{
			StartTs:  1,
			CommitTs: 3,
		}, lastWritePos)
		require.Equal(t, sorter.Position{
			StartTs:  2,
			CommitTs: 3,
		}, lastWritePos.Next())
		cancel()
	}
	taskChan <- &tableSinkTask{
		tableID:              tableID,
		lowerBound:           lowerBoundPos,
		upperBarrierTsGetter: upperBoundGetter,
		tableSink:            wrapper,
		callback:             callback,
	}
	wg.Wait()
	require.Len(t, sink.events, 5, "All events should be sent to sink")
}
