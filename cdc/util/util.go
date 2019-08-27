package util

import (
	"bytes"

	"github.com/pingcap/errors"
)

type Span struct {
	Start []byte
	End   []byte
}

// nil means Negative infinity
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

// nil means Positive infinity
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
	if lhs.Start != nil && rhs.End != nil && bytes.Compare(lhs.Start, rhs.End) >= 0 ||
		rhs.Start != nil && lhs.End != nil && bytes.Compare(rhs.Start, lhs.End) >= 0 {
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
