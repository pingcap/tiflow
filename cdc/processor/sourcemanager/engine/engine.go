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

package engine

import (
	"github.com/pingcap/tiflow/cdc/model"
)

// SortEngine is a storage engine to store and sort CDC events.
// Every changefeed will have one SortEngine instance.
// NOTE: All interfaces are thread-safe.
type SortEngine interface {
	// IsTableBased tells whether the sort engine is based on table or not.
	// If it's based on table, fetching events by table is preferred.
	IsTableBased() bool

	// AddTable adds the table into the engine.
	AddTable(tableID model.TableID)

	// RemoveTable removes the table from the engine.
	RemoveTable(tableID model.TableID)

	// Add adds the given events into the sort engine.
	//
	// NOTE: it's an asynchronous interface. To get the notification of when
	// events are available for fetching, OnResolve is what you want.
	Add(tableID model.TableID, events ...*model.PolymorphicEvent)

	// GetResolvedTs gets resolved timestamp of the given table.
	GetResolvedTs(tableID model.TableID) model.Ts

	// OnResolve pushes action into SortEngine's hook list, which
	// will be called after any events are resolved.
	OnResolve(action func(model.TableID, model.Ts))

	// FetchByTable creates an iterator to fetch events from the given table.
	// lowerBound is inclusive and only resolved events can be retrieved.
	//
	// NOTE: FetchByTable is always available even if IsTableBased returns false.
	FetchByTable(tableID model.TableID, lowerBound, upperBound Position) EventIterator

	// FetchAllTables creates an iterator to fetch events from all tables.
	// lowerBound is inclusive and only resolved events can be retrieved.
	//
	// NOTE: It's only available if IsTableBased returns false.
	FetchAllTables(lowerBound Position) EventIterator

	// CleanByTable tells the engine events of the given table in the given range
	// (unlimited, upperBound] are committed and not necessary any more.
	// The SortEngine instance can GC them later.
	//
	// NOTE: CleanByTable is always available even if IsTableBased returns false.
	CleanByTable(tableID model.TableID, upperBound Position) error

	// CleanAllTables tells the engine events of all tables in the given range
	// (unlimited, upperBound] are committed and not necessary any more.
	// The SortEngine instance can GC them later.
	//
	// NOTE: It's only available if IsTableBased returns false.
	CleanAllTables(upperBound Position) error

	// GetStatsByTable gets the statistics of the given table.
	GetStatsByTable(tableID model.TableID) TableStats

	// ReceivedEvents returns the number of events received by the sort engine.
	ReceivedEvents() int64

	// Close closes the engine. All data written by this instance can be deleted.
	//
	// NOTE: it leads an undefined behavior to close an engine with active iterators.
	Close() error
}

// EventIterator is an iterator to fetch events from SortEngine.
// It's unnecessary to be thread-safe.
type EventIterator interface {
	// Next is used to fetch one event. nil indicates it reaches the stop point.
	//
	// txnFinished indicates whether all events in the current transaction are
	// fetched or not. Users should keep fetching events until txnFinished.Valid()
	// returns true.
	//
	// NOTE: event.IsResolved() will always be false.
	Next() (event *model.PolymorphicEvent, txnFinished Position, err error)

	// Close closes the iterator.
	Close() error
}

// Position is used to
//  1. fetch or clear events from an engine, for example, see SortEngine.FetchByTable.
//  2. calculate the next position with method Next.
type Position struct {
	StartTs  model.Ts
	CommitTs model.Ts
}

// Valid indicates whether the position is valid or not.
func (p Position) Valid() bool {
	return p.CommitTs != 0
}

// Next can only be called on a valid Position.
func (p Position) Next() Position {
	return Position{
		StartTs:  p.StartTs + 1, // it will never overflow.
		CommitTs: p.CommitTs,
	}
}

// Prev can only be called on a valid Position.
func (p Position) Prev() Position {
	if p.StartTs == 0 {
		return Position{
			StartTs:  p.CommitTs - 2,
			CommitTs: p.CommitTs - 1,
		}
	}
	return Position{
		StartTs:  p.StartTs - 1,
		CommitTs: p.CommitTs,
	}
}

// Compare compares 2 Position, just like strcmp in C.
func (p Position) Compare(q Position) int {
	if p.CommitTs < q.CommitTs {
		return -1
	} else if p.CommitTs == q.CommitTs {
		if p.StartTs < q.StartTs {
			return -1
		} else if p.StartTs == q.StartTs {
			return 0
		} else {
			return 1
		}
	} else {
		return 1
	}
}

// IsCommitFence indicates all transactions with same CommitTs are less than the position.
func (p Position) IsCommitFence() bool {
	// NOTE: currently p.StartTs will always less than p.CommitTs.
	// But maybe we will allow p.StartTs == p.CommitTs later.
	return p.CommitTs > 0 && p.StartTs+1 >= p.CommitTs
}

// TableStats of a sort engine.
type TableStats struct {
	ReceivedMaxCommitTs   model.Ts
	ReceivedMaxResolvedTs model.Ts
}
