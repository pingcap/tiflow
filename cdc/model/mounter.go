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

// PolymorphicEvent describes a event can be in multiple states
type PolymorphicEvent struct {
	StartTs  uint64
	CommitTs uint64

	RawKV    *RawKVEntry
	Row      *RowChangedEvent
	finished chan struct{}
}

// NewPolymorphicEvent creates a new PolymorphicEvent with a raw KV
func NewPolymorphicEvent(rawKV *RawKVEntry) *PolymorphicEvent {
	if rawKV.OpType == OpTypeResolved {
		return NewResolvedPolymorphicEvent(rawKV.CommitTs)
	}
	return &PolymorphicEvent{
		StartTs:  rawKV.StartTs,
		CommitTs: rawKV.CommitTs,
		RawKV:    rawKV,
		finished: make(chan struct{}),
	}
}

// NewResolvedPolymorphicEvent creates a new PolymorphicEvent with the resolved ts
func NewResolvedPolymorphicEvent(resolvedTs uint64) *PolymorphicEvent {
	return &PolymorphicEvent{
		CommitTs: resolvedTs,
		RawKV:    &RawKVEntry{CommitTs: resolvedTs, OpType: OpTypeResolved},
		Row:      nil,
		finished: nil,
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
