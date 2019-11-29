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

// RawKVEntry is entries received from TiKV
type RawKVEntry = RegionFeedValue

// KvOrResolved can be used as a union type of RawKVEntry and ResolvedSpan
type KvOrResolved struct {
	KV       *RawKVEntry
	Resolved *ResolvedSpan
}

// ResolvedSpan represents that a span is resolved at a certain timestamp
type ResolvedSpan struct {
	Span      util.Span
	Timestamp uint64
}

// GetValue returns the underlying value
func (e *KvOrResolved) GetValue() interface{} {
	if e.KV != nil {
		return e.KV
	} else if e.Resolved != nil {
		return e.Resolved
	} else {
		return nil
	}
}

// RegionFeedEvent from the kv layer.
// Only one of the event will be setted.
type RegionFeedEvent struct {
	Val        *RegionFeedValue
	Checkpoint *RegionFeedCheckpoint
}

// RegionFeedCheckpoint guarantees all the KV value event
// with commit ts less than ResolvedTs has been emitted.
type RegionFeedCheckpoint struct {
	Span       util.Span
	ResolvedTs uint64
}

// RegionFeedValue notify the KV operator
type RegionFeedValue struct {
	OpType OpType
	Key    []byte
	// Nil fro delete type
	Value []byte
	Ts    uint64
}

func (v *RegionFeedValue) String() string {
	return fmt.Sprintf("OpType: %v, Key: %s, Value: %s, ts: %d", v.OpType, string(v.Key), string(v.Value), v.Ts)
}
