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
	"context"
	"math"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

// RowChangedDatums is used to store the changed datums of a row.
type RowChangedDatums struct {
	RowDatums    []types.Datum
	PreRowDatums []types.Datum
}

// IsEmpty returns true if the RowChangeDatums is empty.
func (r RowChangedDatums) IsEmpty() bool {
	return len(r.RowDatums) == 0 && len(r.PreRowDatums) == 0
}

// PolymorphicEvent describes an event can be in multiple states.
type PolymorphicEvent struct {
	StartTs  uint64
	CRTs     uint64
	Resolved *ResolvedTs

	RawKV *RawKVEntry
	Row   *RowChangedEvent

	finished chan struct{}
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

// SetUpFinishedCh set up the finished chan, should be called before mounting the event.
func (e *PolymorphicEvent) SetUpFinishedCh() {
	if e.finished == nil {
		e.finished = make(chan struct{})
	}
}

// MarkFinished is called to indicate that mount is finished.
func (e *PolymorphicEvent) MarkFinished() {
	if e.finished != nil {
		close(e.finished)
	}
}

// WaitFinished is called by caller to wait for the mount finished.
func (e *PolymorphicEvent) WaitFinished(ctx context.Context) error {
	if e.finished != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.finished:
		}
	}
	return nil
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
		// update DML
		if i.RawKV.OldValue != nil && j.RawKV.OldValue == nil {
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

// AdvanceBatch advances the batch id of the resolved ts.
func (r ResolvedTs) AdvanceBatch() ResolvedTs {
	if !r.IsBatchMode() {
		log.Panic("can't advance batch since resolved ts is not in batch mode",
			zap.Any("resolved", r))
	}
	return ResolvedTs{
		Mode:    BatchResolvedMode,
		Ts:      r.Ts,
		BatchID: r.BatchID + 1,
	}
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

// Greater judge whether the resolved ts is greater than the given ts.
func (r ResolvedTs) Greater(r1 ResolvedTs) bool {
	if r.Ts == r1.Ts {
		return r.BatchID > r1.BatchID
	}
	return r.Ts > r1.Ts
}

// Equal judge whether the resolved ts is equal to the given ts.
func (r ResolvedTs) Equal(r1 ResolvedTs) bool {
	return r == r1
}
