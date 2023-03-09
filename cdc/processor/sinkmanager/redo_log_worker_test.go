// Copyright 2023 PingCAP, Inc.
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
)

type redoLogWorkerSuite struct {
	suite.Suite
	testChangefeedID model.ChangeFeedID
	testSpan         tablepb.Span
}

func (suite *redoLogWorkerSuite) SetupSuite() {
	requestMemSize = testEventSize
	// For one batch size.
	// Advance table sink per 2 events.
	maxUpdateIntervalSize = testEventSize * 2
	suite.testChangefeedID = model.DefaultChangeFeedID("1")
	suite.testSpan = spanz.TableIDToComparableSpan(1)
}

func (suite *redoLogWorkerSuite) TearDownSuite() {
	requestMemSize = defaultRequestMemSize
	maxUpdateIntervalSize = defaultMaxUpdateIntervalSize
}

func TestRedoLogWorkerSuite(t *testing.T) {
	suite.Run(t, new(redoLogWorkerSuite))
}

func (suite *redoLogWorkerSuite) createWorker(
	memQuota uint64,
) (*redoWorker, engine.SortEngine, *mockRedoDMLManager) {
	sortEngine := memory.New(context.Background())
	sm := sourcemanager.New(suite.testChangefeedID, upstream.NewUpstream4Test(&mockPD{}),
		&entry.MockMountGroup{}, sortEngine, make(chan error, 1), false)

	// To avoid refund or release panics.
	quota := memquota.NewMemQuota(suite.testChangefeedID, memQuota, "sink")
	// NOTICE: Do not forget the initial memory quota in the worker first time running.
	quota.ForceAcquire(testEventSize)
	quota.AddTable(suite.testSpan)
	redoDMLManager := newMockRedoDMLManager()
	eventCache := newRedoEventCache(suite.testChangefeedID, 1024)

	return newRedoWorker(suite.testChangefeedID, sm, quota,
		redoDMLManager, eventCache, false), sortEngine, redoDMLManager
}

func (suite *redoLogWorkerSuite) addEventsToSortEngine(
	events []*model.PolymorphicEvent,
	sortEngine engine.SortEngine,
) {
	sortEngine.AddTable(suite.testSpan)
	for _, event := range events {
		sortEngine.Add(suite.testSpan, event)
	}
}

func (suite *redoLogWorkerSuite) TestHandleTaskGotSomeFilteredEvents() {
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
	w, e, m := suite.createWorker(eventSize)
	defer w.memQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *redoTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

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
	wrapper, _ := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
	taskChan <- &redoTask{
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(4),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	wg.Wait()
	require.Len(suite.T(), m.getEvents(suite.testSpan), 3)
	require.Len(suite.T(), w.eventCache.getAppender(suite.testSpan).events, 3)
}

func (suite *redoLogWorkerSuite) TestHandleTaskAbortWhenNoMemAndOneTxnFinished() {
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
	w, e, m := suite.createWorker(eventSize)
	defer w.memQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *redoTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.Equal(suite.T(), context.Canceled, err)
	}()

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
	wrapper, _ := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
	taskChan <- &redoTask{
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(4),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	wg.Wait()
	require.Len(suite.T(), m.getEvents(suite.testSpan), 3)
	require.Len(suite.T(), w.eventCache.getAppender(suite.testSpan).events, 3)
}

func (suite *redoLogWorkerSuite) TestHandleTaskAbortWhenNoMemAndBlocked() {
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
	w, e, m := suite.createWorker(eventSize)
	defer w.memQuota.Close()
	suite.addEventsToSortEngine(events, e)

	taskChan := make(chan *redoTask)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := w.handleTasks(ctx, taskChan)
		require.ErrorIs(suite.T(), err, cerrors.ErrFlowControllerAborted)
	}()

	callback := func(lastWritePos engine.Position) {
		require.Equal(suite.T(), engine.Position{
			StartTs:  0,
			CommitTs: 0,
		}, lastWritePos)
	}
	wrapper, _ := createTableSinkWrapper(suite.testChangefeedID, suite.testSpan)
	taskChan <- &redoTask{
		span:          suite.testSpan,
		lowerBound:    genLowerBound(),
		getUpperBound: genUpperBoundGetter(14),
		tableSink:     wrapper,
		callback:      callback,
		isCanceled:    func() bool { return false },
	}
	require.Eventually(suite.T(), func() bool {
		return len(m.getEvents(suite.testSpan)) == 2
	}, 5*time.Second, 10*time.Millisecond)
	// Abort the task when no memory quota and blocked.
	w.memQuota.Close()
	cancel()
	wg.Wait()
	require.Len(suite.T(), w.eventCache.getAppender(suite.testSpan).events, 2)
	require.Len(suite.T(), m.getEvents(suite.testSpan), 2, "Only two events should be sent to sink")
}
