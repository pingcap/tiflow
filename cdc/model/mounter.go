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

import (
	"math"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// PolymorphicEvent describes an event can be in multiple states.
type PolymorphicEvent struct {
	StartTs  uint64
	CRTs     uint64
	Resolved *ResolvedTs

	RawKV *RawKVEntry
	Row   *RowChangedEvent
}

// NewEmptyPolymorphicEvent creates a new empty PolymorphicEvent.
func NewEmptyPolymorphicEvent(ts uint64) *PolymorphicEvent {
	return &PolymorphicEvent{
		CRTs:  ts,
		RawKV: &RawKVEntry{},
		Row:   &RowChangedEvent{},
	}
}

// NewPolymorphicEvent creates a new PolymorphicEvent with a raw KV.
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

// NewResolvedPolymorphicEvent creates a new PolymorphicEvent with the resolved ts.
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

// IsResolved returns true if the event is resolved. Note that this function can
// only be called when `RawKV != nil`.
func (e *PolymorphicEvent) IsResolved() bool {
	return e.RawKV.OpType == OpTypeResolved
}

// ComparePolymorphicEvents compares two events by CRTs, Resolved, StartTs, Delete/Put order.
// It returns true if and only if i should precede j.
func ComparePolymorphicEvents(i, j *PolymorphicEvent) bool {
	if i.CRTs == j.CRTs {
		if i.IsResolved() {
			return false
		} else if j.IsResolved() {
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

// ResolvedMode describes the batch type of a resolved event.
type ResolvedMode int

const (
	// NormalResolvedMode means that all events whose commitTs is less than or equal to
	// `resolved.Ts` are sent to Sink.
	NormalResolvedMode ResolvedMode = iota
	// BatchResolvedMode means that all events whose commitTs is less than
	// 'resolved.Ts' are sent to Sink.
	BatchResolvedMode
)

// ResolvedTs is the resolved timestamp of sink module.
type ResolvedTs struct {
	Mode    ResolvedMode
	Ts      uint64
	BatchID uint64
}

// NewResolvedTs creates a normal ResolvedTs.
func NewResolvedTs(t uint64) ResolvedTs {
	return ResolvedTs{Ts: t, Mode: NormalResolvedMode, BatchID: math.MaxUint64}
}

// IsBatchMode returns true if the resolved ts is BatchResolvedMode.
func (r ResolvedTs) IsBatchMode() bool {
	return r.Mode == BatchResolvedMode
}

// ResolvedMark returns a timestamp `ts` based on the r.mode, which marks that all events
// whose commitTs is less than or equal to `ts` are sent to Sink.
func (r ResolvedTs) ResolvedMark() uint64 {
	switch r.Mode {
	case NormalResolvedMode:
		// with NormalResolvedMode, cdc guarantees all events whose commitTs is
		// less than or equal to `resolved.Ts` are sent to Sink.
		return r.Ts
	case BatchResolvedMode:
		// with BatchResolvedMode, cdc guarantees all events whose commitTs is
		// less than `resolved.Ts` are sent to Sink.
		return r.Ts - 1
	default:
		log.Error("unknown resolved mode", zap.Any("resolved", r))
		return 0
	}
}

// EqualOrGreater judge whether the resolved ts is equal or greater than the given ts.
func (r ResolvedTs) EqualOrGreater(r1 ResolvedTs) bool {
	if r.Ts == r1.Ts {
		return r.BatchID >= r1.BatchID
	}
	return r.Ts > r1.Ts
}

// Less judge whether the resolved ts is less than the given ts.
func (r ResolvedTs) Less(r1 ResolvedTs) bool {
	return !r.EqualOrGreater(r1)
}
