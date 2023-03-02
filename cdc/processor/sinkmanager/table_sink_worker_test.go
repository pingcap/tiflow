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
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/oracle"
)

// testEventSize is the size of a test event.
// It is used to calculate the memory quota.
const testEventSize = 218

type workerSuite struct {
	suite.Suite
	testChangefeedID model.ChangeFeedID
	testSpan         tablepb.Span
}

func TestWorkerSuite(t *testing.T) {
	suite.Run(t, new(workerSuite))
}

func (suite *workerSuite) SetupSuite() {
	requestMemSize = testEventSize
	// For one batch size.
	// Advance table sink per 2 events.
	maxUpdateIntervalSize = testEventSize * 2
	suite.testChangefeedID = model.DefaultChangeFeedID("1")
	suite.testSpan = spanz.TableIDToComparableSpan(1)
}

func (suite *workerSuite) SetupTest() {
	// reset batchID
	batchID.Store(0)
}

func (suite *workerSuite) TearDownSuite() {
	requestMemSize = defaultRequestMemSize
	maxUpdateIntervalSize = defaultMaxUpdateIntervalSize
}

func (suite *workerSuite) createWorker(
	memQuota uint64,
	splitTxn bool,
) (*sinkWorker, engine.SortEngine) {
	sortEngine := memory.New(context.Background())
	sm := sourcemanager.New(suite.testChangefeedID, upstream.NewUpstream4Test(&mockPD{}),
		&entry.MockMountGroup{}, sortEngine, make(chan error, 1), false)

	// To avoid refund or release panics.
	quota := memquota.NewMemQuota(suite.testChangefeedID, memQuota, "sink")
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	quota.ForceAcquire(testEventSize)
	quota.AddTable(suite.testSpan)

	return newSinkWorker(suite.testChangefeedID, sm, quota, nil, nil, splitTxn, false), sortEngine
}

func (suite *workerSuite) addEventsToSortEngine(
	events []*model.PolymorphicEvent,
	sortEngine engine.SortEngine,
) {
	sortEngine.AddTable(suite.testSpan)
	for _, event := range events {
		sortEngine.Add(suite.testSpan, event)
	}
}

func (suite *workerSuite) genPolymorphicEventWithNilRow(startTs,
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

func (suite *workerSuite) genPolymorphicResolvedEvent(resolvedTs uint64) *model.PolymorphicEvent {
	return &model.PolymorphicEvent{
		CRTs: resolvedTs,
		RawKV: &model.RawKVEntry{
			OpType: model.OpTypeResolved,
			CRTs:   resolvedTs,
		},
	}
}

func (suite *workerSuite) genPolymorphicEvent(startTs, commitTs uint64) *model.PolymorphicEvent {
	return &model.PolymorphicEvent{
		StartTs: startTs,
		CRTs:    commitTs,
		RawKV: &model.RawKVEntry{
			OpType:  model.OpTypePut,
			StartTs: startTs,
			CRTs:    commitTs,
		},
		Row: suite.genRowChangedEvent(startTs, commitTs),
	}
}

func (suite *workerSuite) genRowChangedEvent(startTs, commitTs uint64) *model.RowChangedEvent {
	return &model.RowChangedEvent{
		StartTs:  startTs,
		CommitTs: commitTs,
		Table: &model.TableName{
			Schema:      "table",
			Table:       "table",
			TableID:     suite.testSpan.TableID,
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
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndGotSomeFilteredEvents() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		suite.genPolymorphicEvent(1, 2),
		// This event will be filtered, so its Row will be nil.
		suite.genPolymorphicEventWithNilRow(1, 2),
		suite.genPolymorphicEventWithNilRow(1, 2),
		suite.genPolymorphicEvent(1, 3),
		suite.genPolymorphicEvent(1, 4),
		suite.genPolymorphicResolvedEvent(4),
	}

	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(eventSize, true)
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
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndAbortWhenNoMemAndOneTxnFinished() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 3),
		suite.genPolymorphicEvent(2, 4),
		suite.genPolymorphicResolvedEvent(4),
	}

	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(eventSize, true)
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
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndAbortWhenNoMemAndBlocked() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		suite.genPolymorphicEvent(1, 10),
		suite.genPolymorphicEvent(1, 10),
		suite.genPolymorphicEvent(1, 10),
		suite.genPolymorphicEvent(1, 10),
		suite.genPolymorphicResolvedEvent(14),
	}
	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(eventSize, true)
	defer w.sinkMemQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *sinkTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.ErrorIs(suite.T(), err, cerrors.ErrFlowControllerAborted)
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
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndOnlyAdvanceTableSinkWhenReachOneBatchSize() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 3),
		suite.genPolymorphicResolvedEvent(4),
	}
	// For five events.
	eventSize := uint64(testEventSize * 5)
	w, e := suite.createWorker(eventSize, true)
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
func (suite *workerSuite) TestHandleTaskWithoutSplitTxnAndAbortWhenNoMemAndForceConsume() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 4),
		suite.genPolymorphicResolvedEvent(5),
	}

	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	// Disable split txn.
	w, e := suite.createWorker(eventSize, false)
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
func (suite *workerSuite) TestHandleTaskWithoutSplitTxnOnlyAdvanceTableSinkWhenReachMaxUpdateIntervalSize() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		suite.genPolymorphicEvent(1, 2),
		suite.genPolymorphicEvent(1, 3),
		suite.genPolymorphicEvent(1, 4),
		suite.genPolymorphicEvent(1, 4),
		suite.genPolymorphicEvent(1, 5),
		suite.genPolymorphicEvent(1, 6),
		suite.genPolymorphicResolvedEvent(6),
	}
	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(eventSize, false)
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
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndAdvanceTableWhenTaskIsFinished() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		suite.genPolymorphicEvent(0, 1),
		suite.genPolymorphicResolvedEvent(4),
	}
	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(eventSize, true)
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
	require.Equal(suite.T(), uint64(4), wrapper.getCheckpointTs().ResolvedMark())
}

// Test Scenario:
// worker will advance the table sink directly when there are no events.
func (suite *workerSuite) TestHandleTaskWithSplitTxnAndAdvanceTableIfNoWorkload() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		suite.genPolymorphicResolvedEvent(4),
	}
	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(eventSize, true)
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
		return wrapper.getCheckpointTs().ResolvedMark() == 4
	}, 5*time.Second, 10*time.Millisecond, "Directly advance resolved mark to 4")
	cancel()
	wg.Wait()
}

func (suite *workerSuite) TestHandleTaskUseDifferentBatchIDEveryTime() {
	ctx, cancel := context.WithCancel(context.Background())
	events := []*model.PolymorphicEvent{
		suite.genPolymorphicEvent(1, 3),
		suite.genPolymorphicEvent(1, 3),
		suite.genPolymorphicEvent(1, 3),
		suite.genPolymorphicResolvedEvent(4),
	}
	// Only for three events.
	eventSize := uint64(testEventSize * 3)
	w, e := suite.createWorker(eventSize, true)
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
	require.Equal(suite.T(), uint64(1), batchID.Load())
	sink.AckAllEvents()
	require.Eventually(suite.T(), func() bool {
		return wrapper.getCheckpointTs().ResolvedMark() == 2
	}, 5*time.Second, 10*time.Millisecond)

	events = []*model.PolymorphicEvent{
		suite.genPolymorphicEvent(2, 5),
		suite.genPolymorphicResolvedEvent(6),
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
	require.Equal(suite.T(), uint64(2), batchID.Load(), "The batchID should be 2")
}

func (suite *workerSuite) TestValidateAndAdjustBound() {
	for _, tc := range []struct {
		name          string
		lowerBound    engine.Position
		taskTimeRange time.Duration
		expectAdjust  bool
	}{
		{
			name: "bigger than maxTaskTimeRange",
			lowerBound: engine.Position{
				StartTs:  439333515018895365,
				CommitTs: 439333515018895366,
			},
			taskTimeRange: 10 * time.Second,
			expectAdjust:  true,
		},
		{
			name: "smaller than maxTaskTimeRange",
			lowerBound: engine.Position{
				StartTs:  439333515018895365,
				CommitTs: 439333515018895366,
			},
			taskTimeRange: 1 * time.Second,
			expectAdjust:  false,
		},
	} {
		suite.Run(tc.name, func() {
			changefeedID := model.DefaultChangeFeedID("1")
			span := spanz.TableIDToComparableSpan(1)
			wrapper, _ := createTableSinkWrapper(changefeedID, span)
			task := &sinkTask{
				span:       span,
				lowerBound: tc.lowerBound,
				getUpperBound: func(_ model.Ts) engine.Position {
					lowerPhs := oracle.GetTimeFromTS(tc.lowerBound.CommitTs)
					newUpperCommitTs := oracle.GoTimeToTS(lowerPhs.Add(tc.taskTimeRange))
					upperBound := engine.GenCommitFence(newUpperCommitTs)
					return upperBound
				},
				tableSink: wrapper,
			}
			lowerBound, upperBound := validateAndAdjustBound(changefeedID, task)
			if tc.expectAdjust {
				lowerPhs := oracle.GetTimeFromTS(lowerBound.CommitTs)
				upperPhs := oracle.GetTimeFromTS(upperBound.CommitTs)
				require.Equal(suite.T(), maxTaskTimeRange, upperPhs.Sub(lowerPhs))
			} else {
				require.Equal(suite.T(), tc.lowerBound, lowerBound)
				require.Equal(suite.T(), task.getUpperBound(0), upperBound)
			}
		})
	}
}
