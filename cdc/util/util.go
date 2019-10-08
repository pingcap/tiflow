package util

import (
	"bytes"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
)

// Span represents a arbitrary kv range
type Span struct {
	Start []byte
	End   []byte
}

// UpperBoundKey represents the maximum value.
var UpperBoundKey []byte = []byte{255, 255, 255, 255, 255}

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

// LoadHistoryDDLJobs loads all history DDL jobs from TiDB
func LoadHistoryDDLJobs(tiStore kv.Storage) ([]*model.Job, error) {
	snapMeta, err := getSnapshotMeta(tiStore)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// jobs from GetAllHistoryDDLJobs are sorted by job id, need sorted by schema version
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].BinlogInfo.FinishedTS < jobs[j].BinlogInfo.FinishedTS
	})

	return jobs, nil
}

func getSnapshotMeta(tiStore kv.Storage) (*meta.Meta, error) {
	version, err := tiStore.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot, err := tiStore.GetSnapshot(version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta.NewSnapshotMeta(snapshot), nil
}
