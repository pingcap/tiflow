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
type EventSortEngine[Pos Position] interface {
	// IsTableBased tells whether the sort engine is based on table or not.
	// If it's based on table, fetching events by table is prefered.
	IsTableBased() bool

	// AddTable adds the table into the engine.
	AddTable(tableID model.TableID)

	// Remove removes the table from the engine.
	RemoveTable(tableID model.TableID)

	// Add adds the given events into the sort engine.
	// NOTE: it's an asynchronous interface. To get the notification of when
	// events are available for `Fetch`, SetOnResolve is what you want.
	Add(tableID model.TableID, events ...*model.PolymorphicEvent) error

	// SetOnResolve pushes action into EventSortEngine's hook list, which
	// will be called after any events are resolved.
	SetOnResolve(action func(model.TableID, Pos))

	// FetchByTable creates an iterator to fetch events from the given table.
	// lowerBound is inclusive and only resolved events can be retrieved.
	//
	// NOTE: FetchByTable is always available even if IsTableBased returns false.
	FetchByTable(tableID model.TableID, lowerBound Pos) EventIterator[Pos]

	// FetchAllTables creates an iterator to fetch events from all tables.
	// lowerBound is inclusive and only resolved events can be retrieved.
	//
	// NOTE: It's only available if IsTableBased returns true.
	FetchAllTables(lowerBound Pos) EventIterator[Pos]

	// CleanByTable tells the engine events of the given table in the given range
	// (unlimited, upperBound] are committed and not necessary any more.
	// The EventSortEngine instance can GC them later.
	//
	// NOTE: CleanByTable is always available even if IsTableBased returns false.
	CleanByTable(tableID model.TableID, upperBound Pos)

	// CleanAllTables tells the engine events of all tables in the given range
	// (unlimited, upperBound] are committed and not necessary any more.
	// The EventSortEngine instance can GC them later.
	//
	// NOTE: It's only available if IsTableBased returns true.
	CleanAllTables(upperBound Pos)

	// Close closes the engine. All data written by this instance can be deleted.
	Close()

	// Create a zero position to fetch events for the first time.
	ZeroPosition() Pos
}

// EventIterator is an iterator to fetch events from EventSortEngine.
// It's unnecessary to be thread-safe.
type EventIterator[Pos Position] interface {
	// Next is used to fetch one event. nil indicates it reaches the stop point.
	Next() (*model.PolymorphicEvent, Pos, error)

	// Close closes the iterator.
	Close() error
}

// Position is used to
//  1. notify downstream components events are available, see EventSortEngine.SetOnResolve.
//  2. fetch or clear events from an engine, for example, see EventSortEngine.FetchByTable.
//  3. calculate the next position with method Next.
type Position interface {
	Next() Position
}
