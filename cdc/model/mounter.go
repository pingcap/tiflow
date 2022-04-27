// Copyright 2020 PingCAP, Inc.
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

package model

// PolymorphicEvent describes an event can be in multiple states
type PolymorphicEvent struct {
	StartTs uint64
	// Commit or resolved TS
	CRTs uint64

	RawKV *RawKVEntry
	Row   *RowChangedEvent
}

// NewPolymorphicEvent creates a new PolymorphicEvent with a raw KV
func NewPolymorphicEvent(rawKV *RawKVEntry) *PolymorphicEvent {
	if rawKV.OpType == OpTypeResolved {
		return NewResolvedPolymorphicEvent(rawKV.RegionID, rawKV.CRTs)
	}
	return &PolymorphicEvent{
		StartTs: rawKV.StartTs,
		CRTs:    rawKV.CRTs,
		RawKV:   rawKV,
	}
}

// NewResolvedPolymorphicEvent creates a new PolymorphicEvent with the resolved ts
func NewResolvedPolymorphicEvent(regionID uint64, resolvedTs uint64) *PolymorphicEvent {
	return &PolymorphicEvent{
		CRTs:  resolvedTs,
		RawKV: &RawKVEntry{CRTs: resolvedTs, OpType: OpTypeResolved, RegionID: regionID},
		Row:   nil,
	}
}

// RegionID returns the region ID where the event comes from.
func (e *PolymorphicEvent) RegionID() uint64 {
	return e.RawKV.RegionID
}

// ComparePolymorphicEvents compares two events by CRTs, Resolved, StartTs, Delete/Put order.
// It returns true if and only if i should precede j.
func ComparePolymorphicEvents(i, j *PolymorphicEvent) bool {
	if i.CRTs == j.CRTs {
		if i.RawKV.OpType == OpTypeResolved {
			return false
		} else if j.RawKV.OpType == OpTypeResolved {
			return true
		}

		if i.StartTs > j.StartTs {
			return false
		} else if i.StartTs < j.StartTs {
			return true
		}

		if i.RawKV.OpType == OpTypeDelete && j.RawKV.OpType != OpTypeDelete {
			return true
		}
	}
	return i.CRTs < j.CRTs
}
