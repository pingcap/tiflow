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
    "context"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
)

// EventSortEngine is a storage engine to store and sort CDC events.
// Every changefeed will have one EventSortEngine instance.
// NOTE: All interfaces are thread-safe.
type EventSortEngine interface {
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
	Add(tableID model.TableID, events ...*model.PolymorphicEvent) error

	// GetResolvedTs gets resolved timestamp of the given table.
	GetResolvedTs(tableID model.TableID) model.Ts

	// OnResolve pushes action into EventSortEngine's hook list, which
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
	// The EventSortEngine instance can GC them later.
	//
	// NOTE: CleanByTable is always available even if IsTableBased returns false.
	CleanByTable(tableID model.TableID, upperBound Position) error

	// CleanAllTables tells the engine events of all tables in the given range
	// (unlimited, upperBound] are committed and not necessary any more.
	// The EventSortEngine instance can GC them later.
	//
	// NOTE: It's only available if IsTableBased returns false.
	CleanAllTables(upperBound Position) error

	// Close closes the engine. All data written by this instance can be deleted.
	//
	// NOTE: it leads an undefined behavior to close an engine with active iterators.
	Close() error
}

// EventIterator is an iterator to fetch events from EventSortEngine.
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
//  1. fetch or clear events from an engine, for example, see EventSortEngine.FetchByTable.
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

// MountedEventIter is just like EventIterator, but returns mounted events.
type MountedEventIter struct {
    iter EventIterator
    mg entry.MounterGroup
    maxMemUsage uint64
    maxBatchSize int

    rawEvents []rawEvent
    totalMemUsage uint64
    nextToMount int
    nextToEmit int
    savedIterError error
}

type rawEvent struct {
    event *model.PolymorphicEvent
    txnFinished Position
    size uint64
}

// NewMountedEventIter creates a MountedEventIter instance.
func NewMountedEventIter(
    iter EventIterator,
    mg entry.MounterGroup,
    maxMemUsage uint64,
    maxBatchSize int,
) *MountedEventIter {
    return &MountedEventIter {
        iter: iter,
        mg: mg,
        maxMemUsage: maxMemUsage,
        maxBatchSize: maxBatchSize,
    }
}

func (i *MountedEventIter) Next(ctx context.Context) (event *model.PolymorphicEvent, txnFinished Position, err error) {
    for idx := i.nextToEmit; idx < i.nextToMount; idx++ {
        if err = i.rawEvents[idx].event.WaitFinished(ctx); err == nil {
            event = i.rawEvents[idx].event
            txnFinished = i.rawEvents[idx].txnFinished
            i.totalMemUsage -= i.rawEvents[idx].size
            i.nextToEmit += 1
        }
        return
    }

    if i.mg != nil && i.iter != nil {
        i.nextToMount = 0
        i.nextToEmit = 0
        if cap(i.rawEvents) == 0 {
            i.rawEvents= make([]rawEvent, 0, i.maxBatchSize)
        } else {
            i.rawEvents= i.rawEvents[:0]
        }

        for i.totalMemUsage < i.maxMemUsage && len(i.rawEvents) < cap(i.rawEvents) {
            event, txnFinished, err = i.iter.Next()
            if err != nil {
                return
            }
            if event == nil {
                i.savedIterError = i.iter.Close()
                i.iter = nil
                break
            } else {
                size := uint64(event.Row.ApproximateBytes())
                i.totalMemUsage += size
                i.rawEvents = append(i.rawEvents, rawEvent{ event, txnFinished, size })
            }
        }
        for idx := i.nextToMount; idx < len(i.rawEvents); idx++ {
            i.rawEvents[idx].event.SetUpFinishedCh()
            if err = i.mg.AddEvent(ctx, i.rawEvents[idx].event); err != nil {
                i.mg = nil
                return
            }
            i.nextToMount += 1
        }
    }
    return
}

func (i *MountedEventIter) Close() error {
    if i.savedIterError != nil {
        return i.savedIterError
    }
    return i.iter.Close()
}
