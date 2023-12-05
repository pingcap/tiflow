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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/memory"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// testEventSize is the size of a test event.
// It is used to calculate the memory quota.
const testEventSize = 226

//nolint:unparam
func genPolymorphicEventWithNilRow(startTs,
	commitTs uint64,
) *model.PolymorphicEvent {
	return &model.PolymorphicEvent{
		StartTs: startTs,
		CRTs:    commitTs,
		RawKV: &model.RawKVEntry{
			OpType:  model.OpTypePut,
			StartTs: startTs,
			CRTs:    commitTs,
		},
		Row: nil,
	}
}

func genPolymorphicResolvedEvent(resolvedTs uint64) *model.PolymorphicEvent {
	return &model.PolymorphicEvent{
		CRTs: resolvedTs,
		RawKV: &model.RawKVEntry{
			OpType: model.OpTypeResolved,
			CRTs:   resolvedTs,
		},
	}
}

func genPolymorphicEvent(startTs, commitTs uint64, span tablepb.Span) *model.PolymorphicEvent {
	return &model.PolymorphicEvent{
		StartTs: startTs,
		CRTs:    commitTs,
		RawKV: &model.RawKVEntry{
			OpType:  model.OpTypePut,
			StartTs: startTs,
			CRTs:    commitTs,
		},
		Row: genRowChangedEvent(startTs, commitTs, span),
	}
}

func genRowChangedEvent(startTs, commitTs uint64, span tablepb.Span) *model.RowChangedEvent {
	return &model.RowChangedEvent{
		StartTs:  startTs,
		CommitTs: commitTs,
		Table: &model.TableName{
			Schema:      "table",
			Table:       "table",
			TableID:     span.TableID,
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

type tableSinkWorkerSuite struct {
	suite.Suite
	testChangefeedID model.ChangeFeedID
	testSpan         tablepb.Span
}

func TestTableSinkWorkerSuite(t *testing.T) {
	suite.Run(t, new(tableSinkWorkerSuite))
}

func (suite *tableSinkWorkerSuite) SetupSuite() {
	requestMemSize = testEventSize
	// For one batch size.
	// Advance table sink per 2 events.
	maxUpdateIntervalSize = testEventSize * 2
	suite.testChangefeedID = model.DefaultChangeFeedID("1")
	suite.testSpan = spanz.TableIDToComparableSpan(1)
}

func (suite *tableSinkWorkerSuite) SetupTest() {
	// reset batchID
	batchID.Store(0)
}

func (suite *tableSinkWorkerSuite) TearDownSuite() {
	requestMemSize = defaultRequestMemSize
	maxUpdateIntervalSize = defaultMaxUpdateIntervalSize
}

func (suite *tableSinkWorkerSuite) createWorker(
	ctx context.Context, memQuota uint64, splitTxn bool,
) (*sinkWorker, engine.SortEngine) {
	sortEngine := memory.New(context.Background())
	sm := sourcemanager.New(suite.testChangefeedID, upstream.NewUpstream4Test(&MockPD{}),
		&entry.MockMountGroup{}, sortEngine, false)
	go func() { sm.Run(ctx) }()

	// To avoid refund or release panics.
	quota := memquota.NewMemQuota(suite.testChangefeedID, memQuota, "sink")
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	quota.ForceAcquire(testEventSize)
	quota.AddTable(suite.testSpan)

	return newSinkWorker(suite.testChangefeedID, sm, quota, nil, nil, splitTxn), sortEngine
}

func (suite *tableSinkWorkerSuite) addEventsToSortEngine(
	events []*model.PolymorphicEvent,
	sortEngine engine.SortEngine,
) {
	sortEngine.AddTable(suite.testSpan, 0)
	for _, event := range events {
		sortEngine.Add(suite.testSpan, event)
	}
}

func genUpperBoundGetter(commitTs model.Ts) func(_ model.Ts) engine.Position {
	return func(_ model.Ts) engine.Position {
		return engine.Position{
			StartTs:  commitTs - 1,
			CommitTs: commitTs,
		}
	}
}

func genLowerBound() engine.Position {
	return engine.Position{
		StartTs:  0,
		CommitTs: 1,
	}
}

// Test Scenario:
// Worker should ignore the filtered events(row is nil).
func (suite *tableSinkWorkerSuite) TestHandleTaskWithSplitTxnAndGotSomeFilteredEvents() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		genPolymorphicEvent(1, 2, suite.testSpan),
		// This event will be filtered, so its Row will be nil.
		genPolymorphicEventWithNilRow(1, 2),
		genPolymorphicEventWithNilRow(1, 2),
		genPolymorphicEvent(1, 3, suite.testSpan),
		genPolymorphicEvent(1, 4, suite.testSpan),
		genPolymorphicResolvedEvent(4),
	}

	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(ctx, eventSize, true)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  1,
			CommitTs: 4,
		}, lastWritePos)
		require.Equal(suite.T(), engine.Position{
			StartTs:  2,
			CommitTs: 4,
		}, lastWritePos.Next())
		cancel()
	}
	taskChan <- &sinkTask{
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(4),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	wg.Wait()
	require.Len(suite.T(), sink.GetEvents(), 3)
}

// Test Scenario:
// worker will stop when no memory quota and meet the txn boundary.
func (suite *tableSinkWorkerSuite) TestHandleTaskWithSplitTxnAndAbortWhenNoMemAndOneTxnFinished() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 3, suite.testSpan),
		genPolymorphicEvent(2, 4, suite.testSpan),
		genPolymorphicResolvedEvent(4),
	}

	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(ctx, eventSize, true)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  1,
			CommitTs: 3,
		}, lastWritePos, "we only write 3 events because of the memory quota")
		require.Equal(suite.T(), engine.Position{
			StartTs:  2,
			CommitTs: 3,
		}, lastWritePos.Next())
		cancel()
	}
	taskChan <- &sinkTask{
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(4),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	wg.Wait()
	require.Len(suite.T(), sink.GetEvents(), 3)
}

// Test Scenario:
// worker will block when no memory quota until the mem quota is aborted.
func (suite *tableSinkWorkerSuite) TestHandleTaskWithSplitTxnAndAbortWhenNoMemAndBlocked() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		genPolymorphicEvent(1, 10, suite.testSpan),
		genPolymorphicEvent(1, 10, suite.testSpan),
		genPolymorphicEvent(1, 10, suite.testSpan),
		genPolymorphicEvent(1, 10, suite.testSpan),
		genPolymorphicResolvedEvent(14),
	}
	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(ctx, eventSize, true)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.ErrorIs(suite.T(), err, context.Canceled)
	}()

	wrapper, sink := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  0,
			CommitTs: 0,
		}, lastWritePos)
	}
	taskChan <- &sinkTask{
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(14),
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

// Test Scenario:
// worker will advance the table sink only when it reaches the batch size.
func (suite *tableSinkWorkerSuite) TestHandleTaskWithSplitTxnAndOnlyAdvanceWhenReachOneBatchSize() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 3, suite.testSpan),
		genPolymorphicResolvedEvent(4),
	}
	// For five events.
	eventSize := uint64(testEventSize * 5)
	w, e := suite.createWorker(ctx, eventSize, true)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.ErrorIs(suite.T(), err, context.Canceled)
	}()

	wrapper, sink := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
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
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(2),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	wg.Wait()
	require.Len(suite.T(), sink.GetEvents(), 5, "All events should be sent to sink")
	require.Equal(suite.T(), 3, sink.GetWriteTimes(), "Three txn batch should be sent to sink")
}

// Test Scenario:
// worker will force consume only one Txn when the memory quota is not enough.
func (suite *tableSinkWorkerSuite) TestHandleTaskWithoutSplitTxnAndAbortWhenNoMemAndForceConsume() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 4, suite.testSpan),
		genPolymorphicResolvedEvent(5),
	}

	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	// Disable split txn.
	w, e := suite.createWorker(ctx, eventSize, false)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
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
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(4),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	wg.Wait()
	require.Len(suite.T(), sink.GetEvents(), 5,
		"All events from the first txn should be sent to sink")
}

// Test Scenario:
// worker will advance the table sink only when it reaches the max update interval size.
func (suite *tableSinkWorkerSuite) TestTaskWithoutSplitTxnOnlyAdvanceWhenReachMaxUpdateIntSize() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		genPolymorphicEvent(1, 2, suite.testSpan),
		genPolymorphicEvent(1, 3, suite.testSpan),
		genPolymorphicEvent(1, 4, suite.testSpan),
		genPolymorphicEvent(1, 4, suite.testSpan),
		genPolymorphicEvent(1, 5, suite.testSpan),
		genPolymorphicEvent(1, 6, suite.testSpan),
		genPolymorphicResolvedEvent(6),
	}
	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(ctx, eventSize, false)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  1,
			CommitTs: 4,
		}, lastWritePos)
		require.Equal(suite.T(), engine.Position{
			StartTs:  2,
			CommitTs: 4,
		}, lastWritePos.Next())
		cancel()
	}
	taskChan <- &sinkTask{
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(6),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	wg.Wait()
	require.Len(suite.T(), sink.GetEvents(), 4, "All events should be sent to sink")
	require.Equal(suite.T(), 2, sink.GetWriteTimes(), "Only two times write to sink, "+
		"because the max update interval size is 2 * event size")
}

// Test Scenario:
// worker will advance the table sink only when task is finished.
func (suite *tableSinkWorkerSuite) TestHandleTaskWithSplitTxnAndAdvanceTableWhenTaskIsFinished() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		genPolymorphicEvent(0, 1, suite.testSpan),
		genPolymorphicResolvedEvent(4),
	}
	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(ctx, eventSize, true)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.ErrorIs(suite.T(), err, context.Canceled)
	}()

	wrapper, sink := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
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
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(4),
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
	checkpointTs := wrapper.getCheckpointTs()
	require.Equal(suite.T(), uint64(4), checkpointTs.ResolvedMark())
}

// Test Scenario:
// worker will advance the table sink directly when there are no events.
func (suite *tableSinkWorkerSuite) TestHandleTaskWithSplitTxnAndAdvanceTableIfNoWorkload() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		genPolymorphicResolvedEvent(4),
	}
	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(ctx, eventSize, true)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.ErrorIs(suite.T(), err, context.Canceled)
	}()

	wrapper, _ := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
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
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(4),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	require.Eventually(suite.T(), func() bool {
		checkpointTs := wrapper.getCheckpointTs()
		return checkpointTs.ResolvedMark() == 4
	}, 5*time.Second, 10*time.Millisecond, "Directly advance resolved mark to 4")
	cancel()
	wg.Wait()
}

func (suite *tableSinkWorkerSuite) TestHandleTaskUseDifferentBatchIDEveryTime() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		genPolymorphicEvent(1, 3, suite.testSpan),
		genPolymorphicEvent(1, 3, suite.testSpan),
		genPolymorphicEvent(1, 3, suite.testSpan),
		genPolymorphicResolvedEvent(4),
	}
	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(ctx, eventSize, true)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  1,
			CommitTs: 3,
		}, lastWritePos)
		require.Equal(suite.T(), engine.Position{
			StartTs:  2,
			CommitTs: 3,
		}, lastWritePos.Next())
	}
	taskChan <- &sinkTask{
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(4),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	require.Eventually(suite.T(), func() bool {
		// Only three events should be sent to sink
		return len(sink.GetEvents()) == 3
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(suite.T(), 2, sink.GetWriteTimes(), "Only two times write to sink, "+
		"because the max update interval size is 2 * event size")
	require.Equal(suite.T(), uint64(3), batchID.Load())
	sink.AckAllEvents()
	require.Eventually(suite.T(), func() bool {
		checkpointTs := wrapper.getCheckpointTs()
		return checkpointTs.ResolvedMark() == 2
	}, 5*time.Second, 10*time.Millisecond)

	events = []*model.PolymorphicEvent{
		genPolymorphicEvent(2, 5, suite.testSpan),
		genPolymorphicResolvedEvent(6),
	}
	e.Add(suite.testSpan, events...)
	// Send another task to make sure the batchID is started from 2.
	callback = func(_ engine.Position) {
		cancel()
	}
	taskChan <- &sinkTask{
		span: suite.testSpan,
		lowerBound: engine.Position{
			StartTs:  2,
			CommitTs: 3,
		},
		getUpperBound: genUpperBoundGetter(6),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	wg.Wait()
	require.Equal(suite.T(), uint64(5), batchID.Load(), "The batchID should be 5, "+
		"because the first task has 3 events, the second task has 1 event")
}

func (suite *tableSinkWorkerSuite) TestFetchFromCacheWithFailure() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		genPolymorphicEvent(1, 3, suite.testSpan),
		genPolymorphicEvent(1, 3, suite.testSpan),
		genPolymorphicEvent(1, 3, suite.testSpan),
		genPolymorphicResolvedEvent(4),
	}
	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(ctx, eventSize, true)
	w.eventCache = newRedoEventCache(suite.testChangefeedID, 1024*1024)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	_ = failpoint.Enable("github.com/pingcap/tiflow/cdc/processor/sinkmanager/TableSinkWorkerFetchFromCache", "return")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/processor/sinkmanager/TableSinkWorkerFetchFromCache")
	}()

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
	defer sink.Close()

	chShouldBeClosed := make(chan struct{}, 1)
	callback := func(lastWritePos engine.Position) {
		close(chShouldBeClosed)
	}
	taskChan <- &sinkTask{
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(4),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}

	<-chShouldBeClosed
	cancel()
	wg.Wait()
}

// When starts to handle a task, advancer.lastPos should be set to a correct position.
// Otherwise if advancer.lastPos isn't updated during scanning, callback will get an
// invalid `advancer.lastPos`.
func (suite *tableSinkWorkerSuite) TestHandleTaskWithoutMemory() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		genPolymorphicEvent(1, 3, suite.testSpan),
		genPolymorphicResolvedEvent(4),
	}
	w, e := suite.createWorker(ctx, 0, true)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
	defer sink.Close()

	chShouldBeClosed := make(chan struct{}, 1)
	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), genLowerBound().Prev(), lastWritePos)
		close(chShouldBeClosed)
	}
	taskChan <- &sinkTask{
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(4),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return true },
	}

	<-chShouldBeClosed
	cancel()
	wg.Wait()
}

func (suite *tableSinkWorkerSuite) TestHandleTaskWithCache() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		genPolymorphicEvent(2, 4, suite.testSpan),
		genPolymorphicEvent(2, 4, suite.testSpan),
		genPolymorphicResolvedEvent(4),
	}
	w, e := suite.createWorker(ctx, 0, true)
	w.eventCache = newRedoEventCache(suite.testChangefeedID, 1024*1024)
	appender := w.eventCache.maybeCreateAppender(suite.testSpan, engine.Position{StartTs: 1, CommitTs: 3})
	appender.pushBatch(
		[]*model.RowChangedEvent{events[0].Row, events[1].Row},
		uint64(0), engine.Position{StartTs: 2, CommitTs: 4},
	)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

	wrapper, sink := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
	defer sink.Close()

	chShouldBeClosed := make(chan struct{}, 1)
	callback := func(lastWrittenPos engine.Position) {
		require.Equal(suite.T(), engine.Position{StartTs: 2, CommitTs: 4}, lastWrittenPos)
		close(chShouldBeClosed)
	}
	taskChan <- &sinkTask{
		span:          suite.testSpan,
		lowerBound:    engine.Position{StartTs: 1, CommitTs: 3},
		getUpperBound: genUpperBoundGetter(4),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return true },
	}

	<-chShouldBeClosed
	cancel()
	wg.Wait()
}
