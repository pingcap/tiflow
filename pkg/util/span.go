package util

import (
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
)

// Span represents a arbitrary kv range
type Span struct {
	Start []byte
	End   []byte
}

// UpperBoundKey represents the maximum value.
var UpperBoundKey = []byte{255, 255, 255, 255, 255}

// Hack will set End as UpperBoundKey if End is Nil.
func (s Span) Hack() Span {
	if s.End != nil && s.Start != nil {
		return s
	}

	r := Span{
		Start: s.Start,
		End:   s.End,
	}

	if r.Start == nil {
		r.Start = []byte{}
	}

	if r.End == nil {
		r.End = UpperBoundKey
	}

	return r
}

// GetTableSpan returns the span to watch for the specified table
func GetTableSpan(tableID int64, needEncode bool) Span {
	sep := byte('_')
	tablePrefix := tablecodec.GenTablePrefix(tableID)
	start := append(tablePrefix, sep)
	end := append(tablePrefix, sep+1)
	if needEncode {
		start = codec.EncodeBytes(nil, start)
		end = codec.EncodeBytes(nil, end)
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
func KeyInSpans(k []byte, spans []Span, needEncode bool) bool {
	encodedK := k
	if needEncode {
		encodedK = codec.EncodeBytes(nil, k)
	}
	for _, span := range spans {
		if KeyInSpan(encodedK, span) {
			return true
		}
	}

	return false
}

// KeyInSpan check if k in the span range.
func KeyInSpan(k []byte, span Span) bool {
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
func Intersect(lhs Span, rhs Span) (span Span, err error) {
	if lhs.Start != nil && EndCompare(lhs.Start, rhs.End) >= 0 ||
		rhs.Start != nil && EndCompare(rhs.Start, lhs.End) >= 0 {
		return Span{}, errors.Errorf("span do not overlap: %+v vs %+v", lhs, rhs)
	}

	start := lhs.Start

	if StartCompare(rhs.Start, start) > 0 {
		start = rhs.Start
	}

	end := lhs.End

	if EndCompare(rhs.End, end) < 0 {
		end = rhs.End
	}

	return Span{Start: start, End: end}, nil
}
