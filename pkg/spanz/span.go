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

package spanz

import (
	"bytes"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

const (
	// JobTableID is the id of `tidb_ddl_job`.
	JobTableID = ddl.JobTableID
)

// UpperBoundKey represents the maximum value.
var UpperBoundKey = []byte{255, 255, 255, 255, 255}

// HackSpan will set End as UpperBoundKey if End is Nil.
func HackSpan(span tablepb.Span) tablepb.Span {
	if span.StartKey == nil {
		span.StartKey = []byte{}
	}

	if span.EndKey == nil {
		span.EndKey = UpperBoundKey
	}
	return span
}

// GetTableRange returns the span to watch for the specified table
// Note that returned keys are not in memcomparable format.
func GetTableRange(tableID int64) (startKey, endKey []byte) {
	tablePrefix := tablecodec.GenTablePrefix(tableID)
	sep := byte('_')
	recordMarker := byte('r')

	var start, end kv.Key
	// ignore index keys.
	start = append(tablePrefix, sep, recordMarker)
	end = append(tablePrefix, sep, recordMarker+1)
	return start, end
}

// getDDLRange returns the span to watch for DDL related events
// Note that returned keys are not in memcomparable format.
func getDDLRange() (startKey, endKey []byte) {
	return getMetaListKey("DDLJobList")
}

// getAddIndexDDLRange returns the span to watch for Add Index DDL related events
// Note that returned keys are not in memcomparable format.
func getAddIndexDDLRange() (startKey, endKey []byte) {
	return getMetaListKey("DDLJobAddIdxList")
}

// GetAllDDLSpan return all cdc interested spans for DDL.
func GetAllDDLSpan() []tablepb.Span {
	spans := make([]tablepb.Span, 0, 3)
	start, end := getDDLRange()
	spans = append(spans, tablepb.Span{
		StartKey: ToComparableKey(start),
		EndKey:   ToComparableKey(end),
	})
	start, end = getAddIndexDDLRange()
	spans = append(spans, tablepb.Span{
		StartKey: ToComparableKey(start),
		EndKey:   ToComparableKey(end),
	})
	start, end = GetTableRange(JobTableID)
	spans = append(spans, tablepb.Span{
		StartKey: ToComparableKey(start),
		EndKey:   ToComparableKey(end),
	})
	return spans
}

func getMetaListKey(key string) (startKey, endKey []byte) {
	metaPrefix := []byte("m")
	metaKey := []byte(key)
	listData := 'l'
	start := make([]byte, 0, len(metaPrefix)+len(metaKey)+8)
	start = append(start, metaPrefix...)
	start = codec.EncodeBytes(start, metaKey)
	start = codec.EncodeUint(start, uint64(listData))
	end := make([]byte, len(start))
	copy(end, start)
	end[len(end)-1]++
	return start, end
}

// KeyInSpan check if k in the span range.
func KeyInSpan(k tablepb.Key, span tablepb.Span) bool {
	if StartCompare(k, span.StartKey) >= 0 &&
		EndCompare(k, span.EndKey) < 0 {
		return true
	}

	return false
}

// StartCompare compares two start keys.
// The result will be 0 if lhs==rhs, -1 if lhs < rhs, and +1 if lhs > rhs
func StartCompare(lhs []byte, rhs []byte) int {
	if len(lhs) == 0 && len(rhs) == 0 {
		return 0
	}

	// Nil means Negative infinity.
	// It's difference with EndCompare.
	if len(lhs) == 0 {
		return -1
	}

	if len(rhs) == 0 {
		return 1
	}

	return bytes.Compare(lhs, rhs)
}

// EndCompare compares two end keys.
// The result will be 0 if lhs==rhs, -1 if lhs < rhs, and +1 if lhs > rhs
func EndCompare(lhs []byte, rhs []byte) int {
	if len(lhs) == 0 && len(rhs) == 0 {
		return 0
	}

	// Nil means Positive infinity.
	// It's difference with StartCompare.
	if len(lhs) == 0 {
		return 1
	}

	if len(rhs) == 0 {
		return -1
	}

	return bytes.Compare(lhs, rhs)
}

// Intersect return to intersect part of lhs and rhs span.
// Return error if there's no intersect part
func Intersect(lhs tablepb.Span, rhs tablepb.Span) (span tablepb.Span, err error) {
	if len(lhs.StartKey) != 0 && EndCompare(lhs.StartKey, rhs.EndKey) >= 0 ||
		len(rhs.StartKey) != 0 && EndCompare(rhs.StartKey, lhs.EndKey) >= 0 {
		return tablepb.Span{}, errors.ErrIntersectNoOverlap.GenWithStackByArgs(lhs, rhs)
	}

	start := lhs.StartKey

	if StartCompare(rhs.StartKey, start) > 0 {
		start = rhs.StartKey
	}

	end := lhs.EndKey

	if EndCompare(rhs.EndKey, end) < 0 {
		end = rhs.EndKey
	}

	return tablepb.Span{StartKey: start, EndKey: end}, nil
}

// IsSubSpan returns true if the sub span is parents spans
func IsSubSpan(sub tablepb.Span, parents ...tablepb.Span) bool {
	if bytes.Compare(sub.StartKey, sub.EndKey) >= 0 {
		log.Panic("the sub span is invalid", zap.Reflect("subSpan", sub))
	}
	for _, parent := range parents {
		if StartCompare(parent.StartKey, sub.StartKey) <= 0 &&
			EndCompare(sub.EndKey, parent.EndKey) <= 0 {
			return true
		}
	}
	return false
}

// ToSpan returns a span, keys are encoded in memcomparable format.
// See: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format
func ToSpan(startKey, endKey []byte) tablepb.Span {
	return tablepb.Span{
		StartKey: ToComparableKey(startKey),
		EndKey:   ToComparableKey(endKey),
	}
}

// ToComparableKey returns a memcomparable key.
// See: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format
func ToComparableKey(key []byte) tablepb.Key {
	return codec.EncodeBytes(nil, key)
}
