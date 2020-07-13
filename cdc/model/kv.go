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
	"fmt"

	"github.com/pingcap/ticdc/pkg/regionspan"
)

// OpType for the kv, delete or put
type OpType int

// OpType for kv
const (
	OpTypeUnknow OpType = iota
	OpTypePut
	OpTypeDelete
	OpTypeResolved
)

// RegionFeedEvent from the kv layer.
// Only one of the event will be setted.
type RegionFeedEvent struct {
	Val      *RawKVEntry
	Resolved *ResolvedSpan
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

// ResolvedSpan guarantees all the KV value event
// with commit ts less than ResolvedTs has been emitted.
type ResolvedSpan struct {
	Span       regionspan.ComparableSpan
	ResolvedTs uint64
}

// RawKVEntry notify the KV operator
type RawKVEntry struct {
	OpType OpType
	Key    []byte
	// Nil fro delete type
	Value   []byte
	StartTs uint64
	// Commit or resolved TS
	CRTs uint64
}

func (v *RawKVEntry) String() string {
	return fmt.Sprintf("OpType: %v, Key: %s, Value: %s, StartTs: %d, CRTs: %d",
		v.OpType, string(v.Key), string(v.Value), v.StartTs, v.CRTs)
}
