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

package sorter

import (
	"github.com/pingcap/tiflow/cdc/model"
)

// EventSortEngine is a storage engine to store and sort CDC events.
// Every changefeed will have one EventSortEngine instance.
// NOTE: All interfaces are thread-safe.
type EventSortEngine interface {
	// IsTableBased tells whether the sort engine is based on table or not.
	// If it's based on table, fetching events by table is prefered.
	IsTableBased() bool

	// AddTable adds the table into the engine.
	AddTable(tableID model.TableID)

	// Remove removes the table from the engine.
	RemoveTable(tableID model.TableID)

	// Add adds the given event into the sort engine.
	Add(event *model.PolymorphicEvent) error

	// SetOnResolve pushes action into EventSortEngine's hook list, which
	// will be called after any events are resolved.
	SetOnResolve(action func(model.TableID, model.ResolvedTs))

	// Fetch creates an iterator to fetch events with CRTs in [lowerBound, unlimited).
	// If tableID is -1 it means fetching events from all tables.
	Fetch(tableID model.TableID, lowerBound model.Ts) EventIterator

	// Clean tells the engine events in (unlimited, upperBound] are committed and not
	// necessary any more. The EventSortEngine instance can GC them later.
	Clean(tableID model.TableID, upperBound model.Ts)
}

// EventIterator is an iterator to fetch events from EventSortEngine.
// It's unnecessary to be thread-safe.
type EventIterator interface {
	// Next is used to fetch one event. nil indicates it reaches the stop point.
	Next() (*model.PolymorphicEvent, error)

	// Close closes the iterator.
	Close() error
}
