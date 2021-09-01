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
)

// PolymorphicEvent describes an event can be in multiple states
type PolymorphicEvent struct {
	StartTs uint64
	// Commit or resolved TS
	CRTs uint64

	RawKV    *RawKVEntry
	Row      *RowChangedEvent
	finished chan struct{}
}

// NewPolymorphicEvent creates a new PolymorphicEvent with a raw KV
func NewPolymorphicEvent(rawKV *RawKVEntry) *PolymorphicEvent {
	if rawKV.OpType == OpTypeResolved {
		return NewResolvedPolymorphicEvent(rawKV.RegionID, rawKV.CRTs)
	}
	return &PolymorphicEvent{
		StartTs:  rawKV.StartTs,
		CRTs:     rawKV.CRTs,
		RawKV:    rawKV,
		finished: nil,
	}
}

// NewResolvedPolymorphicEvent creates a new PolymorphicEvent with the resolved ts
func NewResolvedPolymorphicEvent(regionID uint64, resolvedTs uint64) *PolymorphicEvent {
	return &PolymorphicEvent{
		CRTs:     resolvedTs,
		RawKV:    &RawKVEntry{CRTs: resolvedTs, OpType: OpTypeResolved, RegionID: regionID},
		Row:      nil,
		finished: nil,
	}
}

// RegionID returns the region ID where the event comes from.
func (e *PolymorphicEvent) RegionID() uint64 {
	return e.RawKV.RegionID
}

// SetUpFinishedChan creates an internal channel to support PrepareFinished and WaitPrepare
func (e *PolymorphicEvent) SetUpFinishedChan() {
	if e.finished == nil {
		e.finished = make(chan struct{})
	}
}

// PrepareFinished marks the prepare process is finished
// In prepare process, Mounter will translate raw KV to row data
func (e *PolymorphicEvent) PrepareFinished() {
	if e.finished != nil {
		close(e.finished)
	}
}

// WaitPrepare waits for prepare process finished
func (e *PolymorphicEvent) WaitPrepare(ctx context.Context) error {
	if e.finished != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.finished:
		}
	}
	return nil
}
