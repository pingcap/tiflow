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

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/memory"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func createWorker(changefeedID model.ChangeFeedID, memQuota uint64, splitTxn bool) *sinkWorker {
	sortEngine := memory.New(context.Background())
	quota := newMemQuota(changefeedID, memQuota)
	return newSinkWorker(changefeedID, &entry.MockMountGroup{}, sortEngine, quota, nil, splitTxn, false)
}

// nolint:unparam
// It is ok to use the same tableID in test.
func addEventsToSortEngine(t *testing.T, events []*model.PolymorphicEvent, sortEngine engine.SortEngine, tableID model.TableID) {
	sortEngine.AddTable(tableID)
	for _, event := range events {
		err := sortEngine.Add(tableID, event)
		require.NoError(t, err)
	}
}

// It is ok to use the same tableID in test.
//
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

type workerSuite struct {
	suite.Suite
}

func (suite *workerSuite) SetupSuite() {
	requestMemSize = 218
	// For one batch size.
	maxBigTxnBatchSize = 218 * 2
	// Advance table sink per 2 events.
	maxUpdateIntervalSize = 218 * 2
}

func (suite *workerSuite) TearDownSuite() {
	requestMemSize = defaultRequestMemSize
	maxUpdateIntervalSize = defaultMaxUpdateIntervalSize
	maxBigTxnBatchSize = defaultMaxBigTxnBatchSize
}

// Test the case that the worker will stop when no memory quota and meet the txn boundary.
func (suite *workerSuite) TestReceiveTableSinkTaskWithSplitTxnAndAbortWhenNoMemAndOneTxnFinished() {
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Only for three events.
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	eventSize := uint64(218 * 2)

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
	addEventsToSortEngine(suite.T(), events, w.sortEngine, tableID)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(changefeedID, tableID)
	lowerBoundPos := engine.Position{
		StartTs:  0,
		CommitTs: 1,
	}
	upperBoundGetter := func() engine.Position {
		return engine.Position{
			StartTs:  3,
			CommitTs: 4,
		}
	}
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  1,
			CommitTs: 3,
		}, lastWritePos)
		require.Equal(suite.T(), engine.Position{
			StartTs:  2,
			CommitTs: 3,
		}, lastWritePos.Next())
		cancel()
	}
	taskChan <- &sinkTask{
		tableID:       tableID,
		lowerBound:    lowerBoundPos,
		getUpperBound: upperBoundGetter,
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	wg.Wait()
	require.Len(suite.T(), sink.events, 3)
}

// Test the case that worker will block when no memory quota until the mem quota is aborted.
func (suite *workerSuite) TestReceiveTableSinkTaskWithSplitTxnAndAbortWhenNoMemAndBlocked() {
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Only for three events.
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	eventSize := uint64(218 * 2)

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
	addEventsToSortEngine(suite.T(), events, w.sortEngine, tableID)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.ErrorIs(suite.T(), err, cerrors.ErrFlowControllerAborted)
	}()

	wrapper, sink := createTableSinkWrapper(changefeedID, tableID)
	lowerBoundPos := engine.Position{
		StartTs:  0,
		CommitTs: 1,
	}
	upperBoundGetter := func() engine.Position {
		return engine.Position{
			StartTs:  3,
			CommitTs: 4,
		}
	}
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  1,
			CommitTs: 2,
		}, lastWritePos)
		require.Equal(suite.T(), engine.Position{
			StartTs:  2,
			CommitTs: 2,
		}, lastWritePos.Next())
		cancel()
	}
	taskChan <- &sinkTask{
		tableID:       tableID,
		lowerBound:    lowerBoundPos,
		getUpperBound: upperBoundGetter,
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	// Abort the task when no memory quota and blocked.
	w.memQuota.close()
	wg.Wait()
	require.Len(suite.T(), sink.events, 1, "Only one txn should be sent to sink before abort")
}

// Test the case that worker will advance the table sink only when it reaches the batch size.
func (suite *workerSuite) TestReceiveTableSinkTaskWithSplitTxnAndOnlyAdvanceTableSinkWhenReachOneBatchSize() {
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	ctx, cancel := context.WithCancel(context.Background())

	// For five events.
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	eventSize := uint64(218 * 4)

	events := []*model.PolymorphicEvent{
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
	addEventsToSortEngine(suite.T(), events, w.sortEngine, tableID)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.ErrorIs(suite.T(), err, context.Canceled)
	}()

	wrapper, sink := createTableSinkWrapper(changefeedID, tableID)
	lowerBoundPos := engine.Position{
		StartTs:  0,
		CommitTs: 1,
	}
	upperBoundGetter := func() engine.Position {
		return engine.Position{
			StartTs:  1,
			CommitTs: 2,
		}
	}
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  1,
			CommitTs: 2,
		}, lastWritePos)
		require.Equal(suite.T(), engine.Position{
			StartTs:  2,
			CommitTs: 2,
		}, lastWritePos.Next())
		cancel()
	}
	taskChan <- &sinkTask{
		tableID:       tableID,
		lowerBound:    lowerBoundPos,
		getUpperBound: upperBoundGetter,
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	wg.Wait()
	require.Len(suite.T(), sink.events, 5, "All events should be sent to sink")
	require.Equal(suite.T(), 3, sink.writeTimes, "Three txn batch should be sent to sink")
}

// Test the case that the worker will force consume only one Txn when the memory quota is not enough.
func (suite *workerSuite) TestReceiveTableSinkTaskWithoutSplitTxnAndAbortWhenNoMemAndForceConsume() {
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Only for three events.
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	eventSize := uint64(218 * 2)

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
	addEventsToSortEngine(suite.T(), events, w.sortEngine, tableID)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(changefeedID, tableID)
	lowerBoundPos := engine.Position{
		StartTs:  0,
		CommitTs: 1,
	}
	upperBoundGetter := func() engine.Position {
		return engine.Position{
			StartTs:  3,
			CommitTs: 4,
		}
	}
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  1,
			CommitTs: 3,
		}, lastWritePos)
		require.Equal(suite.T(), engine.Position{
			StartTs:  2,
			CommitTs: 3,
		}, lastWritePos.Next())
		cancel()
	}
	taskChan <- &sinkTask{
		tableID:       tableID,
		lowerBound:    lowerBoundPos,
		getUpperBound: upperBoundGetter,
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	wg.Wait()
	require.Len(suite.T(), sink.events, 5, "All events should be sent to sink")
}

// Test the case that the worker will advance the table sink only when it reaches the max update interval size.
func (suite *workerSuite) TestReceiveTableSinkTaskWithoutSplitTxnOnlyAdvanceTableSinkWhenReachMaxUpdateIntervalSize() {
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Only for three events.
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	eventSize := uint64(218 * 2)

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
	addEventsToSortEngine(suite.T(), events, w.sortEngine, tableID)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(changefeedID, tableID)
	lowerBoundPos := engine.Position{
		StartTs:  0,
		CommitTs: 1,
	}
	upperBoundGetter := func() engine.Position {
		return engine.Position{
			StartTs:  3,
			CommitTs: 4,
		}
	}
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  1,
			CommitTs: 3,
		}, lastWritePos)
		require.Equal(suite.T(), engine.Position{
			StartTs:  2,
			CommitTs: 3,
		}, lastWritePos.Next())
		cancel()
	}
	taskChan <- &sinkTask{
		tableID:       tableID,
		lowerBound:    lowerBoundPos,
		getUpperBound: upperBoundGetter,
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	wg.Wait()
	require.Len(suite.T(), sink.events, 5, "All events should be sent to sink")
	require.Equal(suite.T(), 2, sink.writeTimes, "Only two times write to sink")
}

func TestWorkerSuite(t *testing.T) {
	suite.Run(t, new(workerSuite))
}
