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
	"testing"

	"github.com/pingcap/tidb/tablecodec"
	"github.com/stretchr/testify/require"
)

func TestStartCompare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lhs []byte
		rhs []byte
		res int
	}{
		{nil, nil, 0},
		{nil, []byte{}, -1},
		{[]byte{}, nil, 1},
		{[]byte{}, []byte{}, 0},
		{[]byte{1}, []byte{2}, -1},
		{[]byte{2}, []byte{1}, 1},
		{[]byte{3}, []byte{3}, 0},
	}

	for _, test := range tests {
		require.Equal(t, test.res, StartCompare(test.lhs, test.rhs))
	}
}

func TestEndCompare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lhs []byte
		rhs []byte
		res int
	}{
		{nil, nil, 0},
		{nil, []byte{}, 1},
		{[]byte{}, nil, -1},
		{[]byte{}, []byte{}, 0},
		{[]byte{1}, []byte{2}, -1},
		{[]byte{2}, []byte{1}, 1},
		{[]byte{3}, []byte{3}, 0},
	}

	for _, test := range tests {
		require.Equal(t, test.res, EndCompare(test.lhs, test.rhs))
	}
}

func TestIntersect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lhs ComparableSpan
		rhs ComparableSpan
		// Set nil for non-intersect
		res *ComparableSpan
	}{
		{ComparableSpan{nil, []byte{1}}, ComparableSpan{[]byte{1}, nil}, nil},
		{ComparableSpan{nil, nil}, ComparableSpan{nil, nil}, &ComparableSpan{nil, nil}},
		{ComparableSpan{nil, nil}, ComparableSpan{[]byte{1}, []byte{2}}, &ComparableSpan{[]byte{1}, []byte{2}}},
		{ComparableSpan{[]byte{0}, []byte{3}}, ComparableSpan{[]byte{1}, []byte{2}}, &ComparableSpan{[]byte{1}, []byte{2}}},
		{ComparableSpan{[]byte{0}, []byte{2}}, ComparableSpan{[]byte{1}, []byte{2}}, &ComparableSpan{[]byte{1}, []byte{2}}},
	}

	for _, test := range tests {
		t.Logf("running.., %v", test)
		res, err := Intersect(test.lhs, test.rhs)
		if test.res == nil {
			require.NotNil(t, err)
		} else {
			require.Equal(t, *test.res, res)
		}

		// Swap lhs and rhs, should get the same result
		res2, err2 := Intersect(test.rhs, test.lhs)
		if test.res == nil {
			require.NotNil(t, err2)
		} else {
			require.Equal(t, *test.res, res2)
		}
	}
}

func TestGetTableSpan(t *testing.T) {
	t.Parallel()

	span := GetTableSpan(123)
	require.Equal(t, -1, bytes.Compare(span.Start, span.End))
	prefix := []byte(tablecodec.GenTableRecordPrefix(123))
	require.GreaterOrEqual(t, 0, bytes.Compare(span.Start, prefix))
	prefix[len(prefix)-1]++
	require.LessOrEqual(t, 0, bytes.Compare(span.End, prefix))
}
