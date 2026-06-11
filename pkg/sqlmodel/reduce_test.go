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

func TestIdentityUpdatedWithUniqueKeys(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	sourceTI := mockTableInfo(t, "CREATE TABLE tb1 (id INT PRIMARY KEY, uk1 INT UNIQUE NOT NULL, uk2 INT UNIQUE, val INT)")

	change := NewRowChange(source, nil, []interface{}{1, 10, 100, 7}, []interface{}{1, 10, 100, 9}, sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.False(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil, []interface{}{1, 10, 100, 7}, []interface{}{2, 10, 100, 7}, sourceTI, nil, nil)
	require.True(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil, []interface{}{2, 10, 100, 7}, []interface{}{2, 20, 100, 7}, sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil, []interface{}{2, 20, 100, 7}, []interface{}{2, 20, 200, 7}, sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil, []interface{}{2, 20, nil, 7}, []interface{}{2, 20, 200, 7}, sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())
}

func TestPrimaryOrUniqueKeyUpdatedWithExpressionIndex(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	cases := []struct {
		name       string
		createSQL  string
		preValues  []any
		postValues []any
		updated    bool
	}{
		{
			name: "expression unchanged",
			createSQL: "CREATE TABLE tb1 (id INT PRIMARY KEY, name VARCHAR(255), " +
				"UNIQUE KEY only_one_alice ((CASE name WHEN 'Alice' THEN 1 ELSE NULL END)))",
			preValues:  []any{1, "Bob"},
			postValues: []any{1, "Charlie"},
		},
		{
			name: "expression changed",
			createSQL: "CREATE TABLE tb1 (id INT PRIMARY KEY, name VARCHAR(255), " +
				"UNIQUE KEY only_one_alice ((CASE name WHEN 'Alice' THEN 1 ELSE NULL END)))",
			preValues:  []any{1, "Bob"},
			postValues: []any{1, "Alice"},
			updated:    true,
		},
		{
			name: "ordinary unique changed with lower expression index",
			createSQL: "CREATE TABLE tb1 (id INT PRIMARY KEY, email VARCHAR(255) UNIQUE, name VARCHAR(255), " +
				"UNIQUE KEY lower_name ((lower(name))))",
			preValues:  []any{1, "a@example.com", "Bob"},
			postValues: []any{1, "b@example.com", "Bob"},
			updated:    true,
		},
		{
			name: "arithmetic expression index unchanged",
			createSQL: "CREATE TABLE tb1 (id INT PRIMARY KEY, code INT, name VARCHAR(255), " +
				"UNIQUE KEY next_code ((code + 1)))",
			preValues:  []any{1, 10, "Bob"},
			postValues: []any{1, 10, "Alice"},
		},
		{
			name: "composite expression index changed by visible column",
			createSQL: "CREATE TABLE tb1 (id INT PRIMARY KEY, score INT, code INT, " +
				"UNIQUE KEY next_score_code ((score + 1), code))",
			preValues:  []any{1, -7, 10},
			postValues: []any{1, -7, 20},
			updated:    true,
		},
		{
			name: "binary expression index changed",
			createSQL: "CREATE TABLE tb1 (id INT PRIMARY KEY, payload VARBINARY(16), " +
				"UNIQUE KEY uk_payload_expr ((CAST(payload AS BINARY(16)))))",
			preValues:  []any{1, "alice"},
			postValues: []any{1, "bob"},
			updated:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sourceTI := mockTableInfo(t, tc.createSQL)
			change := NewRowChange(source, nil, tc.preValues, tc.postValues, sourceTI, nil, nil)
			require.False(t, change.IsIdentityUpdated())
			require.Equal(t, tc.updated, change.IsPrimaryOrUniqueKeyUpdated())
		})
	}
}

func TestPrimaryOrUniqueKeyUpdatedExpressionIndexMaterializeFailure(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	sourceTI := mockTableInfo(t, "CREATE TABLE tb1 (id INT PRIMARY KEY, email VARCHAR(255) UNIQUE, name VARCHAR(255), "+
		"UNIQUE KEY lower_name ((lower(name))))")
	corruptHiddenGeneratedExpr(t, sourceTI)

	change := NewRowChange(source, nil,
		[]any{1, "a@example.com", "Bob"},
		[]any{1, "b@example.com", "Alice"},
		sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil,
		[]any{1, "a@example.com", "Bob"},
		[]any{1, "a@example.com", "Alice"},
		sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.False(t, change.IsPrimaryOrUniqueKeyUpdated())
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
