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

package tablesink

import (
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
)

// Only for test.
func (r *progressTracker) pendingResolvedTsEventsCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.resolvedTsCache)
}

func TestNewProgressTracker(t *testing.T) {
	t.Parallel()

	tracker := newProgressTracker(spanz.TableIDToComparableSpan(1), defaultBufferSize)
	require.Equal(
		t,
		uint64(0),
		tracker.advance().Ts,
		"init lastMinResolvedTs should be 0",
	)
}

func TestAddEvent(t *testing.T) {
	t.Parallel()

	tracker := newProgressTracker(spanz.TableIDToComparableSpan(1), defaultBufferSize)
	tracker.addEvent()
	tracker.addEvent()
	tracker.addEvent()
	require.Equal(t, 3, tracker.trackingCount(), "event should be added")
}

func TestAddResolvedTs(t *testing.T) {
	t.Parallel()

	// There is no event in the tracker.
	tracker := newProgressTracker(spanz.TableIDToComparableSpan(1), defaultBufferSize)
	tracker.addResolvedTs(model.NewResolvedTs(1))
	tracker.addResolvedTs(model.NewResolvedTs(2))
	tracker.addResolvedTs(model.NewResolvedTs(3))
	require.Equal(t, 0, tracker.trackingCount(), "resolved ts should not be added")
	require.Equal(t, uint64(3), tracker.advance().Ts, "lastMinResolvedTs should be 3")

	// There is an event in the tracker.
	tracker = newProgressTracker(spanz.TableIDToComparableSpan(1), defaultBufferSize)
	tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(2))
	tracker.addResolvedTs(model.NewResolvedTs(3))
	require.Equal(t, 1, tracker.trackingCount(), "resolved ts should be added")
	require.Equal(t, uint64(0), tracker.advance().Ts, "lastMinResolvedTs should not be updated")
}

func TestRemove(t *testing.T) {
	t.Parallel()
	var cb1, cb2, cb4, cb5 func()

	// Only event.
	tracker := newProgressTracker(spanz.TableIDToComparableSpan(1), defaultBufferSize)
	tracker.addEvent()
	cb2 = tracker.addEvent()
	tracker.addEvent()
	cb2()
	tracker.advance()
	require.Equal(t, 3, tracker.trackingCount(), "not advanced")

	// Both event and resolved ts.
	tracker = newProgressTracker(spanz.TableIDToComparableSpan(1), defaultBufferSize)
	cb1 = tracker.addEvent()
	cb2 = tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(3))
	cb4 = tracker.addEvent()
	cb5 = tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(6))
	tracker.addResolvedTs(model.NewResolvedTs(7))
	tracker.addResolvedTs(model.NewResolvedTs(8))
	// Remove one event.
	cb2()
	tracker.advance()
	require.Equal(t, 4, tracker.trackingCount())
	require.Equal(t, uint64(0), tracker.advance().Ts, "lastMinResolvedTs should not be updated")
	// Remove one more event.
	cb4()
	tracker.advance()
	require.Equal(t, 4, tracker.trackingCount())
	require.Equal(t, uint64(0), tracker.advance().Ts, "lastMinResolvedTs should not be updated")
	// Remove one more event.
	cb1()
	tracker.advance()
	require.Equal(t, 1, tracker.trackingCount())
	require.Equal(t, uint64(3), tracker.advance().Ts, "lastMinResolvedTs should be advanced")
	// Remove the last event.
	cb5()
	tracker.advance()
	require.Equal(t, 0, tracker.trackingCount())
	require.Equal(t, uint64(8), tracker.advance().Ts, "lastMinResolvedTs should be 8")
}

func TestCloseTracker(t *testing.T) {
	t.Parallel()

	tracker := newProgressTracker(spanz.TableIDToComparableSpan(1), defaultBufferSize)
	cb1 := tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(1))
	cb2 := tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(2))
	cb3 := tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(3))
	require.Equal(t, 3, tracker.trackingCount(), "event should be added")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tracker.freezeProcess()
		tracker.waitClosed(make(chan struct{}))
		wg.Done()
	}()

	cb1()
	cb2()
	cb3()
	wg.Wait()
	require.Eventually(t, func() bool {
		return tracker.trackingCount() == 0
	}, 3*time.Second, 100*time.Millisecond, "all events should be removed")
}

func TestCloseTrackerCancellable(t *testing.T) {
	t.Parallel()

	tracker := newProgressTracker(spanz.TableIDToComparableSpan(1), defaultBufferSize)
	tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(1))
	tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(2))
	tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(3))
	require.Equal(t, 3, tracker.trackingCount(), "event should be added")

	dead := make(chan struct{})
	go func() {
		time.Sleep(time.Millisecond * 10)
		close(dead)
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		tracker.freezeProcess()
		tracker.waitClosed(dead)
		wg.Done()
	}()
	wg.Wait()
}

func TestTrackerBufferBoundary(t *testing.T) {
	t.Parallel()

	tracker := newProgressTracker(spanz.TableIDToComparableSpan(1), 8)

	cbs := make([]func(), 0)
	for i := 0; i < 65; i++ {
		cbs = append(cbs, tracker.addEvent())
	}
	require.Equal(t, 2, len(tracker.pendingEvents))
	require.Equal(t, 1, len(tracker.pendingEvents[0]))
	require.Equal(t, 1, len(tracker.pendingEvents[1]))
	for i, cb := range cbs {
		cb()
		tracker.advance()
		require.Equal(t, 65-i-1, tracker.trackingCount())
	}
	require.Equal(t, 1, len(tracker.pendingEvents))

	cbs = nil
	for i := 65; i < 128; i++ {
		cbs = append(cbs, tracker.addEvent())
		require.Equal(t, 1, len(tracker.pendingEvents))
		require.Equal(t, 1, len(tracker.pendingEvents[0]))
	}
	for i, cb := range cbs {
		cb()
		tracker.advance()
		require.Equal(t, 63-i-1, tracker.trackingCount())
	}
	require.Equal(t, 0, len(tracker.pendingEvents))
}

func TestClosedTrackerDoNotAdvanceCheckpointTs(t *testing.T) {
	t.Parallel()

	tracker := newProgressTracker(spanz.TableIDToComparableSpan(1), defaultBufferSize)
	cb1 := tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(1))
	cb2 := tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(2))
	cb3 := tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(3))
	require.Equal(t, 3, tracker.trackingCount(), "event should be added")

	var wg sync.WaitGroup
	wg.Add(1)
	freezed := make(chan struct{})
	go func() {
		tracker.freezeProcess()
		close(freezed)
		tracker.waitClosed(make(chan struct{}))
		wg.Done()
	}()
	<-freezed
	currentTs := tracker.advance()
	cb1()
	cb2()
	cb3()
	wg.Wait()
	require.Eventually(t, func() bool {
		return tracker.trackingCount() == 0
	}, 3*time.Second, 100*time.Millisecond, "all events should be removed")
	require.Equal(t, currentTs, tracker.advance(), "checkpointTs should not be advanced")
}

func TestOnlyResolvedTsShouldDirectlyAdvanceCheckpointTs(t *testing.T) {
	t.Parallel()

	tracker := newProgressTracker(spanz.TableIDToComparableSpan(1), defaultBufferSize)
	cb1 := tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(1))
	cb2 := tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(2))
	tracker.addResolvedTs(model.NewResolvedTs(3))
	cb3 := tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(4))
	tracker.addResolvedTs(model.NewResolvedTs(5))
	require.Equal(t, 3, tracker.trackingCount(), "Events should be added")
	cb1()
	cb2()
	tracker.addResolvedTs(model.NewResolvedTs(6))
	require.Equal(t, uint64(3), tracker.advance().Ts, "CheckpointTs should be advanced")
	require.Equal(t, 1, tracker.trackingCount(), "Only one event should be left")
	require.Equal(t, uint64(3), tracker.advance().Ts, "CheckpointTs still should be 3")
	cb3()
	require.Equal(t, uint64(6), tracker.advance().Ts, "CheckpointTs should be advanced")
	tracker.addResolvedTs(model.NewResolvedTs(7))
	tracker.addResolvedTs(model.NewResolvedTs(8))
	tracker.addResolvedTs(model.NewResolvedTs(9))
	require.Equal(t, 0, tracker.pendingResolvedTsEventsCount(), "ResolvedTsCache should be empty")
	require.Equal(t, uint64(9), tracker.advance().Ts, "CheckpointTs should be advanced")
}

func TestShouldDirectlyUpdateResolvedTsIfNoMoreEvents(t *testing.T) {
	t.Parallel()

	tracker := newProgressTracker(spanz.TableIDToComparableSpan(1), defaultBufferSize)
	cb1 := tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(1))
	cb2 := tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(2))
	tracker.addResolvedTs(model.NewResolvedTs(3))
	require.Equal(t, 2, tracker.pendingResolvedTsEventsCount(), "ResolvedTsCache should only have 2 events")
	cb3 := tracker.addEvent()
	tracker.addResolvedTs(model.NewResolvedTs(4))
	tracker.addResolvedTs(model.NewResolvedTs(5))
	tracker.addResolvedTs(model.NewResolvedTs(6))
	cb1()
	cb2()
	require.Equal(t, uint64(3), tracker.advance().Ts, "CheckpointTs should be advanced")
	require.Equal(t, 1, tracker.pendingResolvedTsEventsCount(), "ResolvedTsCache should only have one event")
	cb3()
	require.Equal(t, uint64(6), tracker.advance().Ts, "CheckpointTs should be advanced")
	require.Equal(t, 0, tracker.pendingResolvedTsEventsCount(), "ResolvedTsCache should be empty")
}
