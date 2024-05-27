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

//go:generate msgp

package model

import (
	"fmt"

	"github.com/pingcap/tiflow/cdc/processor/tablepb"
)

// OpType for the kv, delete or put
type OpType int

// OpType for kv
const (
	OpTypeUnknown OpType = iota
	OpTypePut
	OpTypeDelete
	OpTypeResolved
)

// RegionFeedEvent from the kv layer.
// Only one of the event will be set.
//
//msgp:ignore RegionFeedEvent
type RegionFeedEvent struct {
	Val      *RawKVEntry
	Resolved *ResolvedSpans

	// Additional debug info, not used
	RegionID uint64
}

// GetValue returns the underlying value
func (e *RegionFeedEvent) GetValue() interface{} {
	if e.Val != nil {
		return e.Val
	} else if e.Resolved != nil {
		return e.Resolved
	} else {
		return nil
	}
}

// ResolvedSpans guarantees all the KV value event
// with commit ts less than ResolvedTs has been emitted.
//
//msgp:ignore ResolvedSpans
type ResolvedSpans struct {
	Spans      []RegionComparableSpan
	ResolvedTs uint64
}

// String implements fmt.Stringer interface.
func (rs *ResolvedSpans) String() string {
	return fmt.Sprintf("span: %v, resolved-ts: %d", rs.Spans, rs.ResolvedTs)
}

// RegionComparableSpan contains a comparable span and a region id of that span
//
//msgp:ignore RegionComparableSpan
type RegionComparableSpan struct {
	Span   tablepb.Span
	Region uint64
}

// RawKVEntry notify the KV operator
type RawKVEntry struct {
	OpType OpType `msg:"op_type"`
	Key    []byte `msg:"key"`
	// nil for delete type
	Value []byte `msg:"value"`
	// nil for insert type
	OldValue []byte `msg:"old_value"`
	StartTs  uint64 `msg:"start_ts"`
	// Commit or resolved TS
	CRTs uint64 `msg:"crts"`

	// Additional debug info
	RegionID uint64 `msg:"region_id"`
}

// IsUpdate checks if the event is an update event.
func (v *RawKVEntry) IsUpdate() bool {
	return v.OpType == OpTypePut && v.OldValue != nil && v.Value != nil
}

func (v *RawKVEntry) String() string {
	// TODO: redact values.
	return fmt.Sprintf(
		"OpType: %v, Key: %s, Value: %s, OldValue: %s, StartTs: %d, CRTs: %d, RegionID: %d",
		v.OpType, string(v.Key), string(v.Value), string(v.OldValue), v.StartTs, v.CRTs, v.RegionID)
}

// ApproximateDataSize calculate the approximate size of protobuf binary
// representation of this event.
func (v *RawKVEntry) ApproximateDataSize() int64 {
	return int64(len(v.Key) + len(v.Value) + len(v.OldValue))
}
