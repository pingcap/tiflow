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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
)

// Span represents a arbitrary kv range
type Span struct {
	Start []byte
	End   []byte
}

// String returns a string that encodes Span in hex format.
func (s Span) String() string {
	return fmt.Sprintf("[%s, %s)", hex.EncodeToString(s.Start), hex.EncodeToString(s.End))
}

// UpperBoundKey represents the maximum value.
var UpperBoundKey = []byte{255, 255, 255, 255, 255}

// ComparableSpan represents a arbitrary kv range which is comparable
type ComparableSpan Span

// String returns a string that encodes ComparableSpan in hex format.
func (s ComparableSpan) String() string {
	return Span(s).String()
}

// Hack will set End as UpperBoundKey if End is Nil.
func (s ComparableSpan) Hack() ComparableSpan {
	s.Start, s.End = hackSpan(s.Start, s.End)
	return s
}

// Hack will set End as UpperBoundKey if End is Nil.
func (s Span) Hack() Span {
	s.Start, s.End = hackSpan(s.Start, s.End)
	return s
}

func hackSpan(originStart []byte, originEnd []byte) (start []byte, end []byte) {
	start = originStart
	end = originEnd

	if originStart == nil {
		start = []byte{}
	}

	if originEnd == nil {
		end = UpperBoundKey
	}
	return
}

// GetTableSpan returns the span to watch for the specified table
func GetTableSpan(tableID int64, exceptIndexSpan bool) Span {
	sep := byte('_')
	recordMarker := byte('r')
	tablePrefix := tablecodec.GenTablePrefix(tableID)
	var start, end kv.Key
	// ignore index keys if we don't need them
	if exceptIndexSpan {
		start = append(tablePrefix, sep, recordMarker)
		end = append(tablePrefix, sep, recordMarker+1)
	} else {
		start = append(tablePrefix, sep)
		end = append(tablePrefix, sep+1)
	}
	return Span{
		Start: start,
		End:   end,
	}
}

// GetDDLSpan returns the span to watch for DDL related events
func GetDDLSpan() Span {
	return getMetaListKey("DDLJobList")
}

// GetAddIndexDDLSpan returns the span to watch for Add Index DDL related events
func GetAddIndexDDLSpan() Span {
	return getMetaListKey("DDLJobAddIdxList")
}

func getMetaListKey(key string) Span {
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
	return Span{
		Start: start,
		End:   end,
	}
}

// KeyInSpans check if k in the range of spans.
func KeyInSpans(k []byte, spans []ComparableSpan) bool {
	for _, span := range spans {
		if KeyInSpan(k, span) {
			return true
		}
	}

	return false
}

// KeyInSpan check if k in the span range.
func KeyInSpan(k []byte, span ComparableSpan) bool {
	if StartCompare(k, span.Start) >= 0 &&
		EndCompare(k, span.End) < 0 {
		return true
	}

	return false
}

// StartCompare compares two start keys.
// Nil means Negative infinity
// The result will be 0 if lhs==rhs, -1 if lhs < rhs, and +1 if lhs > rhs
func StartCompare(lhs []byte, rhs []byte) int {
	if lhs == nil && rhs == nil {
		return 0
	}

	if lhs == nil {
		return -1
	}

	if rhs == nil {
		return 1
	}

	return bytes.Compare(lhs, rhs)
}

// EndCompare compares two end keys.
// Nil means Positive infinity
// The result will be 0 if lhs==rhs, -1 if lhs < rhs, and +1 if lhs > rhs
func EndCompare(lhs []byte, rhs []byte) int {
	if lhs == nil && rhs == nil {
		return 0
	}

	if lhs == nil {
		return 1
	}

	if rhs == nil {
		return -1
	}

	return bytes.Compare(lhs, rhs)
}

// Intersect return the intersect part of lhs and rhs span.
// Return error if there's no intersect part
func Intersect(lhs ComparableSpan, rhs ComparableSpan) (span ComparableSpan, err error) {
	if lhs.Start != nil && EndCompare(lhs.Start, rhs.End) >= 0 ||
		rhs.Start != nil && EndCompare(rhs.Start, lhs.End) >= 0 {
		return ComparableSpan{}, errors.Errorf("span do not overlap: %+v vs %+v", lhs, rhs)
	}

	start := lhs.Start

	if StartCompare(rhs.Start, start) > 0 {
		start = rhs.Start
	}

	end := lhs.End

	if EndCompare(rhs.End, end) < 0 {
		end = rhs.End
	}

	return ComparableSpan{Start: start, End: end}, nil
}

// IsSubSpan returns true if the sub span is parents spans
func IsSubSpan(sub ComparableSpan, parents ...ComparableSpan) bool {
	if bytes.Compare(sub.Start, sub.End) >= 0 {
		log.Fatal("the sub span is invalid", zap.Reflect("sub span", sub))
	}
	for _, parent := range parents {
		if StartCompare(parent.Start, sub.Start) <= 0 &&
			EndCompare(sub.End, parent.End) <= 0 {
			return true
		}
	}
	return false
}

// ToComparableSpan returns a memcomparable span
func ToComparableSpan(span Span) ComparableSpan {
	return ComparableSpan{
		Start: codec.EncodeBytes(nil, span.Start),
		End:   codec.EncodeBytes(nil, span.End),
	}
}

// ToComparableKey returns a memcomparable key.
func ToComparableKey(key []byte) []byte {
	return codec.EncodeBytes(nil, key)
}
