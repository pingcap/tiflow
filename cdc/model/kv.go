package model

import (
	"fmt"

	"github.com/pingcap/ticdc/pkg/util"
)

// OpType for the kv, delete or put
type OpType int

// OpType for kv
const (
	OpTypeUnknow OpType = 0
	OpTypePut    OpType = 1
	OpTypeDelete OpType = 2
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
	Span       util.Span
	ResolvedTs uint64
}

// RawKVEntry notify the KV operator
type RawKVEntry struct {
	OpType OpType
	Key    []byte
	// Nil fro delete type
	Value []byte
	Ts    uint64
}

func (v *RawKVEntry) String() string {
	return fmt.Sprintf("OpType: %v, Key: %s, Value: %s, ts: %d", v.OpType, string(v.Key), string(v.Value), v.Ts)
}
