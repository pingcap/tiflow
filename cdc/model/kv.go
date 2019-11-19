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

type RawKVEntry = RegionFeedValue

type KvOrResolved struct {
	KV       *RawKVEntry
	Resolved *ResolvedSpan
}

type ResolvedSpan struct {
	Span      util.Span
	Timestamp uint64
}

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
