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
	"time"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/memory"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func createWorker(
	changefeedID model.ChangeFeedID,
	memQuota uint64,
	splitTxn bool,
	tableIDs ...model.TableID,
) (*sinkWorker, engine.SortEngine) {
	sortEngine := memory.New(context.Background())
	sm := sourcemanager.New(changefeedID, upstream.NewUpstream4Test(&mockPD{}),
		&entry.MockMountGroup{}, sortEngine, make(chan error, 1), false)

	// To avoid refund or release panics.
	quota := memquota.NewMemQuota(changefeedID, memQuota+1024*1024*1024, "")
	quota.ForceAcquire(1024 * 1024 * 1024)
	for _, tableID := range tableIDs {
		quota.AddTable(tableID)
	}

	return newSinkWorker(changefeedID, sm, quota, nil, nil, splitTxn, false), sortEngine
}

// nolint:unparam
// It is ok to use the same tableID in test.
func addEventsToSortEngine(t *testing.T, events []*model.PolymorphicEvent, sortEngine engine.SortEngine, tableID model.TableID) {
	sortEngine.AddTable(tableID)
	for _, event := range events {
<<<<<<< HEAD
		err := sortEngine.Add(tableID, event)
		require.NoError(t, err)
=======
		sortEngine.Add(span, event)
>>>>>>> 02b9286700 (cdc: fix sourcemanager.Close deadlock (#8370))
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
	// Advance table sink per 2 events.
	maxUpdateIntervalSize = 218 * 2
}

func (suite *workerSuite) TearDownSuite() {
	requestMemSize = defaultRequestMemSize
	maxUpdateIntervalSize = defaultMaxUpdateIntervalSize
}

// Test the case that the worker will ignore filtered events.
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndGotSomeFilteredEvents() {
	suite.T().Skip("need to be fixed")

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
		// This event will be filtered, so its Row will be nil.
		{
			StartTs: 1,
			CRTs:    1,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    1,
			},
			Row: nil,
		},
		{
			StartTs: 1,
			CRTs:    1,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    1,
			},
			Row: nil,
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

	w, e := createWorker(changefeedID, eventSize, true)
	defer w.sinkMemQuota.Close()
	addEventsToSortEngine(suite.T(), events, e, tableID)

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
	upperBoundGetter := func(_ *tableSinkWrapper) engine.Position {
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
	require.Len(suite.T(), sink.GetEvents(), 3)
}

// Test the case that the worker will stop when no memory quota and meet the txn boundary.
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndAbortWhenNoMemAndOneTxnFinished() {
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

	w, e := createWorker(changefeedID, eventSize, true, tableID)
	defer w.sinkMemQuota.Close()
	addEventsToSortEngine(suite.T(), events, e, tableID)

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
	upperBoundGetter := func(_ *tableSinkWrapper) engine.Position {
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
	require.Len(suite.T(), sink.GetEvents(), 3)
}

// Test the case that worker will block when no memory quota until the mem quota is aborted.
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndAbortWhenNoMemAndBlocked() {
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Only for three events.
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	eventSize := uint64(218 * 2)

	events := []*model.PolymorphicEvent{
		{
			StartTs: 1,
			CRTs:    10,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    10,
			},
			Row: genRowChangedEvent(1, 10, tableID),
		},
		{
			StartTs: 1,
			CRTs:    10,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    10,
			},
			Row: genRowChangedEvent(1, 10, tableID),
		},
		{
			StartTs: 1,
			CRTs:    10,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    10,
			},
			Row: genRowChangedEvent(1, 10, tableID),
		},
		{
			StartTs: 1,
			CRTs:    10,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    10,
			},
			Row: genRowChangedEvent(1, 10, tableID),
		},
		{
			CRTs: 14,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
				CRTs:   14,
			},
		},
	}
	w, e := createWorker(changefeedID, eventSize, true, tableID)
	defer w.sinkMemQuota.Close()
	addEventsToSortEngine(suite.T(), events, e, tableID)

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
	upperBoundGetter := func(_ *tableSinkWrapper) engine.Position {
		return engine.Position{
			StartTs:  13,
			CommitTs: 14,
		}
	}
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  0,
			CommitTs: 0,
		}, lastWritePos)
	}
	taskChan <- &sinkTask{
		tableID:       tableID,
		lowerBound:    lowerBoundPos,
		getUpperBound: upperBoundGetter,
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	require.Eventually(suite.T(), func() bool {
		return len(sink.GetEvents()) == 2
	}, 5*time.Second, 10*time.Millisecond)
	// Abort the task when no memory quota and blocked.
	w.sinkMemQuota.Close()
	cancel()
	wg.Wait()
	require.Len(suite.T(), sink.GetEvents(), 2, "Only two events should be sent to sink")
}

// Test the case that worker will advance the table sink only when it reaches the batch size.
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndOnlyAdvanceTableSinkWhenReachOneBatchSize() {
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
	w, e := createWorker(changefeedID, eventSize, true, tableID)
	defer w.sinkMemQuota.Close()
	addEventsToSortEngine(suite.T(), events, e, tableID)

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
	upperBoundGetter := func(_ *tableSinkWrapper) engine.Position {
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
	require.Len(suite.T(), sink.GetEvents(), 5, "All events should be sent to sink")
	require.Equal(suite.T(), 3, sink.GetWriteTimes(), "Three txn batch should be sent to sink")
}

// Test the case that the worker will force consume only one Txn when the memory quota is not enough.
func (suite *workerSuite) TestHandleTaskWithoutSplitTxnAndAbortWhenNoMemAndForceConsume() {
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Only for three events.
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	eventSize := uint64(218 * 2)

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
	w, e := createWorker(changefeedID, eventSize, false, tableID)
	defer w.sinkMemQuota.Close()
	w.splitTxn = false
	addEventsToSortEngine(suite.T(), events, e, tableID)

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
	upperBoundGetter := func(_ *tableSinkWrapper) engine.Position {
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
	wg.Wait()
	require.Len(suite.T(), sink.GetEvents(), 5, "All events should be sent to sink")
}

// Test the case that the worker will advance the table sink only when it reaches the max update interval size.
func (suite *workerSuite) TestHandleTaskWithoutSplitTxnOnlyAdvanceTableSinkWhenReachMaxUpdateIntervalSize() {
	suite.T().Skip("need to be fixed")

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
	w, e := createWorker(changefeedID, eventSize, false)
	defer w.sinkMemQuota.Close()
	addEventsToSortEngine(suite.T(), events, e, tableID)

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
	upperBoundGetter := func(_ *tableSinkWrapper) engine.Position {
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
	require.Len(suite.T(), sink.GetEvents(), 5, "All events should be sent to sink")
	require.Equal(suite.T(), 2, sink.GetWriteTimes(), "Only two times write to sink")
}

// Test the case that the worker will advance the table sink only when meet the new commit ts.
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndDoNotAdvanceTableUntilMeetNewCommitTs() {
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
		// Although the commit ts is 2, the event is not sent to sink because the commit ts is not changed.
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
		// We will block at this event.
		{
			StartTs: 1,
			CRTs:    3,
			RawKV: &model.RawKVEntry{
				OpType:  model.OpTypePut,
				StartTs: 1,
				CRTs:    3,
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
	w, e := createWorker(changefeedID, eventSize, true, tableID)
	defer w.sinkMemQuota.Close()
	addEventsToSortEngine(suite.T(), events, e, tableID)

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
	upperBoundGetter := func(_ *tableSinkWrapper) engine.Position {
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
	}
	taskChan <- &sinkTask{
		tableID:       tableID,
		lowerBound:    lowerBoundPos,
		getUpperBound: upperBoundGetter,
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	require.Eventually(suite.T(), func() bool {
		return len(sink.GetEvents()) == 3
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	wg.Wait()
	receivedEvents := sink.GetEvents()
	receivedEvents[0].Callback()
	receivedEvents[1].Callback()
	receivedEvents[2].Callback()
	require.Len(suite.T(), sink.GetEvents(), 3, "No more events should be sent to sink")
	require.Equal(suite.T(), uint64(2), wrapper.getCheckpointTs().ResolvedMark(),
		"Only can advance resolved mark to 2")
}

// Test the case that the worker will advance the table sink only when task is finished.
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndAdvanceTableUntilTaskIsFinished() {
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
			CRTs: 4,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
				CRTs:   4,
			},
		},
	}
	w, e := createWorker(changefeedID, eventSize, true, tableID)
	defer w.sinkMemQuota.Close()
	addEventsToSortEngine(suite.T(), events, e, tableID)

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
	upperBoundGetter := func(_ *tableSinkWrapper) engine.Position {
		return engine.Position{
			StartTs:  3,
			CommitTs: 4,
		}
	}
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  3,
			CommitTs: 4,
		}, lastWritePos)
		require.Equal(suite.T(), engine.Position{
			StartTs:  4,
			CommitTs: 4,
		}, lastWritePos.Next())
	}
	taskChan <- &sinkTask{
		tableID:       tableID,
		lowerBound:    lowerBoundPos,
		getUpperBound: upperBoundGetter,
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	require.Eventually(suite.T(), func() bool {
		return len(sink.GetEvents()) == 1
	}, 5*time.Second, 10*time.Millisecond)
	cancel()
	wg.Wait()
	receivedEvents := sink.GetEvents()
	receivedEvents[0].Callback()
	require.Len(suite.T(), sink.GetEvents(), 1, "No more events should be sent to sink")
	require.Equal(suite.T(), uint64(4), wrapper.getCheckpointTs().ResolvedMark())
}

// Test the case that the worker will advance the table sink directly when there are no events.
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndAdvanceTableIfNoWorkload() {
	changefeedID := model.DefaultChangeFeedID("1")
	tableID := model.TableID(1)
	ctx, cancel := context.WithCancel(context.Background())

	// Only for three events.
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	eventSize := uint64(218 * 2)

	events := []*model.PolymorphicEvent{
		{
			CRTs: 4,
			RawKV: &model.RawKVEntry{
				OpType: model.OpTypeResolved,
				CRTs:   4,
			},
		},
	}
	w, e := createWorker(changefeedID, eventSize, true, tableID)
	defer w.sinkMemQuota.Close()
	addEventsToSortEngine(suite.T(), events, e, tableID)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.ErrorIs(suite.T(), err, context.Canceled)
	}()

	wrapper, _ := createTableSinkWrapper(changefeedID, tableID)
	lowerBoundPos := engine.Position{
		StartTs:  0,
		CommitTs: 1,
	}
	upperBoundGetter := func(_ *tableSinkWrapper) engine.Position {
		return engine.Position{
			StartTs:  3,
			CommitTs: 4,
		}
	}
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  3,
			CommitTs: 4,
		}, lastWritePos)
		require.Equal(suite.T(), engine.Position{
			StartTs:  4,
			CommitTs: 4,
		}, lastWritePos.Next())
	}
	taskChan <- &sinkTask{
		tableID:       tableID,
		lowerBound:    lowerBoundPos,
		getUpperBound: upperBoundGetter,
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	require.Eventually(suite.T(), func() bool {
		return wrapper.getCheckpointTs().ResolvedMark() == 4
	}, 5*time.Second, 10*time.Millisecond, "Directly advance resolved mark to 4")
	cancel()
	wg.Wait()
}

func TestWorkerSuite(t *testing.T) {
	suite.Run(t, new(workerSuite))
}
