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

	"github.com/stretchr/testify/require"

	cdcmodel "github.com/pingcap/tiflow/cdc/model"
)

func TestIdentity(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	sourceTI1 := mockTableInfo(t, "CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT)")

	change := NewRowChange(source, nil, []interface{}{1, 2}, nil, sourceTI1, nil, nil)
	pre, post := change.IdentityValues()
	require.Equal(t, []interface{}{1}, pre)
	require.Len(t, post, 0)

	change = NewRowChange(source, nil, []interface{}{1, 2}, []interface{}{1, 4}, sourceTI1, nil, nil)
	pre, post = change.IdentityValues()
	require.Equal(t, []interface{}{1}, pre)
	require.Equal(t, []interface{}{1}, post)
	require.False(t, change.IsIdentityUpdated())

	sourceTI2 := mockTableInfo(t, "CREATE TABLE tb1 (c INT, c2 INT)")
	change = NewRowChange(source, nil, nil, []interface{}{5, 6}, sourceTI2, nil, nil)
	pre, post = change.IdentityValues()
	require.Len(t, pre, 0)
	require.Equal(t, []interface{}{5, 6}, post)
}

func TestSplit(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	sourceTI1 := mockTableInfo(t, "CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT)")

	change := NewRowChange(source, nil, []interface{}{1, 2}, []interface{}{3, 4}, sourceTI1, nil, nil)
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
		pre1      []interface{}
		post1     []interface{}
		pre2      []interface{}
		post2     []interface{}
		preAfter  []interface{}
		postAfter []interface{}
	}{
		// INSERT + UPDATE
		{
			nil,
			[]interface{}{1, 2},
			[]interface{}{1, 2},
			[]interface{}{3, 4},
			nil,
			[]interface{}{3, 4},
		},
		// INSERT + DELETE
		{
			nil,
			[]interface{}{1, 2},
			[]interface{}{1, 2},
			nil,
			[]interface{}{1, 2},
			nil,
		},
		// UPDATE + UPDATE
		{
			[]interface{}{1, 2},
			[]interface{}{1, 3},
			[]interface{}{1, 3},
			[]interface{}{1, 4},
			[]interface{}{1, 2},
			[]interface{}{1, 4},
		},
		// UPDATE + DELETE
		{
			[]interface{}{1, 2},
			[]interface{}{1, 3},
			[]interface{}{1, 3},
			nil,
			[]interface{}{1, 2},
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
	change1 := NewRowChange(source, nil, []interface{}{1, 2}, []interface{}{3, 4}, sourceTI, nil, nil)
	change2 := NewRowChange(source, nil, []interface{}{3, 4}, []interface{}{5, 6}, sourceTI, nil, nil)
	s.Panics(func() {
		change2.Reduce(change1)
	})
}
