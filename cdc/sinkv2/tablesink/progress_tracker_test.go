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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"

	"github.com/stretchr/testify/require"
)

func TestNewProgressTracker(t *testing.T) {
	tracker := newProgressTracker()
	require.NotNil(
		t,
		tracker.pendingEventAndResolvedTs,
		"pendingEventAndResolvedTs should not be nil",
	)
	require.Equal(
		t,
		uint64(0),
		tracker.minTs().Ts,
		"init lastMinResolvedTs should be 0",
	)
}

func TestAddEvent(t *testing.T) {
	tracker := newProgressTracker()
	tracker.addEvent(1)
	tracker.addEvent(2)
	tracker.addEvent(3)
	require.Equal(t, 3, tracker.pendingEventAndResolvedTs.Size(), "event should be added")
}

func TestAddResolvedTs(t *testing.T) {
	// There is no event in the tracker.
	tracker := newProgressTracker()
	tracker.addResolvedTs(1, model.NewResolvedTs(1))
	tracker.addResolvedTs(2, model.NewResolvedTs(2))
	tracker.addResolvedTs(3, model.NewResolvedTs(3))
	require.Equal(t, 0, tracker.pendingEventAndResolvedTs.Size(), "resolved ts should not be added")
	require.Equal(t, uint64(3), tracker.minTs().Ts, "lastMinResolvedTs should be 3")

	// There is an event in the tracker.
	tracker = newProgressTracker()
	tracker.addEvent(1)
	tracker.addResolvedTs(2, model.NewResolvedTs(2))
	tracker.addResolvedTs(3, model.NewResolvedTs(3))
	require.Equal(t, 3, tracker.pendingEventAndResolvedTs.Size(), "resolved ts should be added")
	require.Equal(t, uint64(0), tracker.minTs().Ts, "lastMinResolvedTs should not be updated")
}

func TestRemove(t *testing.T) {
	// Only event.
	tracker := newProgressTracker()
	tracker.addEvent(1)
	tracker.addEvent(2)
	tracker.addEvent(3)
	tracker.remove(2)
	require.Equal(t, 2, tracker.pendingEventAndResolvedTs.Size(), "event2 should be removed")
	// Only resolved ts.
	tracker = newProgressTracker()
	tracker.addResolvedTs(1, model.NewResolvedTs(1))
	tracker.addResolvedTs(2, model.NewResolvedTs(2))
	tracker.addResolvedTs(3, model.NewResolvedTs(3))
	// Hack here to trigger the remove. Actually, we don't have this key in the tracker.
	tracker.remove(4)
	require.Equal(
		t,
		0,
		tracker.pendingEventAndResolvedTs.Size(),
		"all resolved ts should be removed",
	)
	require.Equal(t, uint64(3), tracker.minTs().Ts, "lastMinResolvedTs should be 3")
	// Both event and resolved ts.
	tracker = newProgressTracker()
	tracker.addEvent(1)
	tracker.addEvent(2)
	tracker.addResolvedTs(3, model.NewResolvedTs(3))
	tracker.addEvent(4)
	tracker.addEvent(5)
	tracker.addResolvedTs(6, model.NewResolvedTs(6))
	tracker.addResolvedTs(7, model.NewResolvedTs(7))
	tracker.addResolvedTs(8, model.NewResolvedTs(8))
	// Remove one event.
	tracker.remove(2)
	require.Equal(t, 7, tracker.pendingEventAndResolvedTs.Size(), "event2 should be removed")
	require.Equal(t, uint64(0), tracker.minTs().Ts, "lastMinResolvedTs should not be updated")
	// Remove one more event.
	tracker.remove(4)
	require.Equal(t, 6, tracker.pendingEventAndResolvedTs.Size(), "event4 should be removed")
	require.Equal(t, uint64(0), tracker.minTs().Ts, "lastMinResolvedTs should not be updated")
	// Remove one more event.
	tracker.remove(1)
	require.Equal(t, 4, tracker.pendingEventAndResolvedTs.Size(), "event1 should be removed")
	require.Equal(t, uint64(3), tracker.minTs().Ts, "lastMinResolvedTs should be advanced")
	// Remove the last event.
	tracker.remove(5)
	require.Equal(
		t,
		0,
		tracker.pendingEventAndResolvedTs.Size(),
		"all events and resolved ts should be removed",
	)
	require.Equal(t, uint64(8), tracker.minTs().Ts, "lastMinResolvedTs should be 8")
}
