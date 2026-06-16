// Copyright 2022 PingCAP, Inc.
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

package sqlmodel

import (
	"testing"

	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestIdentity(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	sourceTI1 := mockTableInfo(t, "CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT)")

	change := NewRowChange(source, nil, []any{1, 2}, nil, sourceTI1, nil, nil)
	pre, post := change.IdentityValues()
	require.Equal(t, []any{1}, pre)
	require.Len(t, post, 0)

	change = NewRowChange(source, nil, []any{1, 2}, []any{1, 4}, sourceTI1, nil, nil)
	pre, post = change.IdentityValues()
	require.Equal(t, []any{1}, pre)
	require.Equal(t, []any{1}, post)
	require.False(t, change.IsIdentityUpdated())

	sourceTI2 := mockTableInfo(t, "CREATE TABLE tb1 (c INT, c2 INT)")
	change = NewRowChange(source, nil, nil, []any{5, 6}, sourceTI2, nil, nil)
	pre, post = change.IdentityValues()
	require.Len(t, pre, 0)
	require.Equal(t, []any{5, 6}, post)
}

func TestIdentityUpdatedWithUniqueKeys(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	sourceTI := mockTableInfo(t, "CREATE TABLE tb1 (id INT PRIMARY KEY, uk1 INT UNIQUE NOT NULL, uk2 INT UNIQUE, val INT)")

	change := NewRowChange(source, nil, []any{1, 10, 100, 7}, []any{1, 10, 100, 9}, sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.False(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil, []any{1, 10, 100, 7}, []any{2, 10, 100, 7}, sourceTI, nil, nil)
	require.True(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil, []any{2, 10, 100, 7}, []any{2, 20, 100, 7}, sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil, []any{2, 20, 100, 7}, []any{2, 20, 200, 7}, sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil, []any{2, 20, nil, 7}, []any{2, 20, 200, 7}, sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())
}

func TestSplit(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	sourceTI1 := mockTableInfo(t, "CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT)")

	change := NewRowChange(source, nil, []any{1, 2}, []any{3, 4}, sourceTI1, nil, nil)
	require.True(t, change.IsIdentityUpdated())
	del, ins := change.SplitUpdate()
	delIDKey := del.IdentityKey()
	require.NotZero(t, delIDKey)
	insIDKey := ins.IdentityKey()
	require.NotZero(t, insIDKey)
	require.NotEqual(t, delIDKey, insIDKey)
}

func (s *dpanicSuite) TestReduce() {
	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	sourceTI := mockTableInfo(s.T(), "CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT)")

	cases := []struct {
		pre1      []any
		post1     []any
		pre2      []any
		post2     []any
		preAfter  []any
		postAfter []any
	}{
		// INSERT + UPDATE
		{
			nil,
			[]any{1, 2},
			[]any{1, 2},
			[]any{3, 4},
			nil,
			[]any{3, 4},
		},
		// INSERT + DELETE
		{
			nil,
			[]any{1, 2},
			[]any{1, 2},
			nil,
			[]any{1, 2},
			nil,
		},
		// UPDATE + UPDATE
		{
			[]any{1, 2},
			[]any{1, 3},
			[]any{1, 3},
			[]any{1, 4},
			[]any{1, 2},
			[]any{1, 4},
		},
		// UPDATE + DELETE
		{
			[]any{1, 2},
			[]any{1, 3},
			[]any{1, 3},
			nil,
			[]any{1, 2},
			nil,
		},
	}

	for _, c := range cases {
		change1 := NewRowChange(source, nil, c.pre1, c.post1, sourceTI, nil, nil)
		change2 := NewRowChange(source, nil, c.pre2, c.post2, sourceTI, nil, nil)
		changeAfter := NewRowChange(source, nil, c.preAfter, c.postAfter, sourceTI, nil, nil)
		changeAfter.lazyInitWhereHandle()

		change2.Reduce(change1)
		s.Equal(changeAfter, change2)
	}

	// test reduce on IdentityUpdated will DPanic
	change1 := NewRowChange(source, nil, []any{1, 2}, []any{3, 4}, sourceTI, nil, nil)
	change2 := NewRowChange(source, nil, []any{3, 4}, []any{5, 6}, sourceTI, nil, nil)
	s.Panics(func() {
		change2.Reduce(change1)
	})
}
