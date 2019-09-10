package util

import (
	"bytes"

	"github.com/pingcap/errors"
)

// Span represents a arbitrary kv range
type Span struct {
	Start []byte
	End   []byte
}

// BiggestEnd represents the maximum value.
var BiggestEnd []byte = []byte{255, 255, 255, 255, 255}

// Hack will set End as BiggestEnd if End is Nil.
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
		r.End = BiggestEnd
	}

	return r
}

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

	return Span{start, end}, nil
}
