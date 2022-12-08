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

package regionspan

import (
	"bytes"
	"encoding/hex"
	"fmt"

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

// LegacySpan represents an arbitrary kv range
type LegacySpan struct {
	StartKey []byte
	EndKey   []byte
}

// String returns a string that encodes Span in hex format.
func (s LegacySpan) String() string {
	return fmt.Sprintf("[%s, %s)", hex.EncodeToString(s.StartKey), hex.EncodeToString(s.EndKey))
}

func hackSpan(originStart []byte, originEnd []byte) (start []byte, end []byte) {
	start = originStart
	end = originEnd

	if start == nil {
		start = []byte{}
	}

	if end == nil {
		end = UpperBoundKey
	}
	return
}

// ComparableSpan represents an arbitrary kv range which is comparable
type ComparableSpan tablepb.Span

// String returns a string that encodes ComparableSpan in hex format.
func (s ComparableSpan) String() string {
	return fmt.Sprintf("[%s, %s)", hex.EncodeToString(s.StartKey), hex.EncodeToString(s.EndKey))
}

// Hack will set End as UpperBoundKey if End is Nil.
func (s ComparableSpan) Hack() ComparableSpan {
	s.StartKey, s.EndKey = hackSpan(s.StartKey, s.EndKey)
	return s
}

// GetTableSpan returns the span to watch for the specified table
func GetTableSpan(tableID int64) LegacySpan {
	tablePrefix := tablecodec.GenTablePrefix(tableID)
	sep := byte('_')
	recordMarker := byte('r')

	var start, end kv.Key
	// ignore index keys.
	start = append(tablePrefix, sep, recordMarker)
	end = append(tablePrefix, sep, recordMarker+1)
	return LegacySpan{
		StartKey: start,
		EndKey:   end,
	}
}

// getDDLSpan returns the span to watch for DDL related events
func getDDLSpan() LegacySpan {
	return getMetaListKey("DDLJobList")
}

// getAddIndexDDLSpan returns the span to watch for Add Index DDL related events
func getAddIndexDDLSpan() LegacySpan {
	return getMetaListKey("DDLJobAddIdxList")
}

// GetAllDDLSpan return all cdc interested spans for DDL.
func GetAllDDLSpan() []LegacySpan {
	return []LegacySpan{getDDLSpan(), getAddIndexDDLSpan(), GetTableSpan(JobTableID)}
}

func getMetaListKey(key string) LegacySpan {
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
	return LegacySpan{
		StartKey: start,
		EndKey:   end,
	}
}

// KeyInSpan check if k in the span range.
func KeyInSpan(k []byte, span ComparableSpan) bool {
	if StartCompare(k, span.StartKey) >= 0 &&
		EndCompare(k, span.EndKey) < 0 {
		return true
	}

	return false
}

// StartCompare compares two start keys.
// The result will be 0 if lhs==rhs, -1 if lhs < rhs, and +1 if lhs > rhs
func StartCompare(lhs []byte, rhs []byte) int {
	if lhs == nil && rhs == nil {
		return 0
	}

	// Nil means Negative infinity.
	// It's difference with EndCompare.
	if lhs == nil {
		return -1
	}

	if rhs == nil {
		return 1
	}

	return bytes.Compare(lhs, rhs)
}

// EndCompare compares two end keys.
// The result will be 0 if lhs==rhs, -1 if lhs < rhs, and +1 if lhs > rhs
func EndCompare(lhs []byte, rhs []byte) int {
	if lhs == nil && rhs == nil {
		return 0
	}

	// Nil means Positive infinity.
	// It's difference with StartCompare.
	if lhs == nil {
		return 1
	}

	if rhs == nil {
		return -1
	}

	return bytes.Compare(lhs, rhs)
}

// Intersect return to intersect part of lhs and rhs span.
// Return error if there's no intersect part
func Intersect(lhs ComparableSpan, rhs ComparableSpan) (span ComparableSpan, err error) {
	if lhs.StartKey != nil && EndCompare(lhs.StartKey, rhs.EndKey) >= 0 ||
		rhs.StartKey != nil && EndCompare(rhs.StartKey, lhs.EndKey) >= 0 {
		return ComparableSpan{}, errors.ErrIntersectNoOverlap.GenWithStackByArgs(lhs, rhs)
	}

	start := lhs.StartKey

	if StartCompare(rhs.StartKey, start) > 0 {
		start = rhs.StartKey
	}

	end := lhs.EndKey

	if EndCompare(rhs.EndKey, end) < 0 {
		end = rhs.EndKey
	}

	return ComparableSpan{StartKey: start, EndKey: end}, nil
}

// IsSubSpan returns true if the sub span is parents spans
func IsSubSpan(sub ComparableSpan, parents ...ComparableSpan) bool {
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

// ToComparableSpan returns a memcomparable span.
// See: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format
func ToComparableSpan(span LegacySpan) ComparableSpan {
	return ComparableSpan{
		StartKey: codec.EncodeBytes(nil, span.StartKey),
		EndKey:   codec.EncodeBytes(nil, span.EndKey),
	}
}

// ToComparableKey returns a memcomparable key.
func ToComparableKey(key []byte) []byte {
	return codec.EncodeBytes(nil, key)
}
