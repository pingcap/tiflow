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
	"github.com/pingcap/tiflow/pkg/util/testutil"
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

// TestIdentityUpdatedWithBinaryKeys covers key columns that reach sqlmodel as
// []byte. Applying != to an interface that holds a slice panics with
// "comparing uncomparable type []uint8", so equality has to go through
// bytes.Equal. Values with equal contents but distinct backing arrays must
// compare equal, otherwise a safe-mode UPDATE reports a spurious key change and
// writes a needless DELETE.
//
// Two column kinds reach here as []byte:
//   - BINARY and binary CHAR, which adjustValues converts from string because
//     the column is TypeString with the binary flag set;
//   - BLOB and TEXT, which the binlog decoder already returns as []byte.
//
// A GBK column takes the same conversion, so it behaves like the first kind.
func TestIdentityUpdatedWithBinaryKeys(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}

	// A single BINARY primary key. The first comparison hits the binary column.
	singleColTI := mockTableInfo(t, "CREATE TABLE tb1 ("+
		"id BINARY(16) NOT NULL, val INT, PRIMARY KEY (id))")

	change := NewRowChange(source, nil,
		[]interface{}{[]byte("0123456789abcdef"), 7},
		[]interface{}{[]byte("0123456789abcdef"), 9},
		singleColTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.False(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil,
		[]interface{}{[]byte("0123456789abcdef"), 7},
		[]interface{}{[]byte("fedcba9876543210"), 7},
		singleColTI, nil, nil)
	require.True(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())

	// A multi-column primary key whose trailing columns are BINARY. All columns
	// are NOT NULL, so the primary key becomes UniqueNotNullIdx and the fault
	// would land on a later comparison rather than the first.
	multiColTI := mockTableInfo(t, "CREATE TABLE tb1 ("+
		"entity_id BIGINT NOT NULL, entity_type_id INT NOT NULL, "+
		"principal BINARY(16) NOT NULL, reference BINARY(16) NOT NULL, "+
		"val INT, "+
		"PRIMARY KEY (entity_id, entity_type_id, principal, reference))")

	change = NewRowChange(source, nil,
		[]interface{}{1, 2, []byte("principal00000001"), []byte("reference00000001"), 7},
		[]interface{}{1, 2, []byte("principal00000001"), []byte("reference00000001"), 9},
		multiColTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.False(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil,
		[]interface{}{1, 2, []byte("principal00000001"), []byte("reference00000001"), 7},
		[]interface{}{1, 2, []byte("principal00000001"), []byte("reference00000002"), 7},
		multiColTI, nil, nil)
	require.True(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())

	// A secondary UNIQUE key over a BLOB prefix. The column is nullable, so the
	// index lands in UniqueIdxs rather than UniqueNotNullIdx and only
	// IsPrimaryOrUniqueKeyUpdated reads it.
	blobUKTI := mockTableInfo(t, "CREATE TABLE tb1 ("+
		"id INT PRIMARY KEY, acl BLOB, val INT, UNIQUE KEY uk_acl (acl(255)))")

	change = NewRowChange(source, nil,
		[]interface{}{1, []byte("acl"), 7},
		[]interface{}{1, []byte("acl"), 9},
		blobUKTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.False(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil,
		[]interface{}{1, []byte("acl"), 7},
		[]interface{}{1, []byte("acl2"), 7},
		blobUKTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())

	// A NULL binary key value on one side only.
	change = NewRowChange(source, nil,
		[]interface{}{1, nil, 7},
		[]interface{}{1, []byte("acl"), 7},
		blobUKTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())

	// VARBINARY reaches sqlmodel as string today, because adjustValues only
	// converts TypeString columns and VARBINARY parses to TypeVarchar. Cover
	// both representations so the comparison does not depend on that detail.
	varbinaryTI := mockTableInfo(t, "CREATE TABLE tb1 ("+
		"id INT PRIMARY KEY, acl VARBINARY(255) UNIQUE, val INT)")

	for _, v := range []struct {
		name       string
		base, same interface{}
		diff       interface{}
	}{
		{"string", "acl", "acl", "acl2"},
		{"bytes", []byte("acl"), []byte("acl"), []byte("acl2")},
	} {
		t.Run(v.name, func(t *testing.T) {
			change := NewRowChange(source, nil,
				[]interface{}{1, v.base, 7},
				[]interface{}{1, v.same, 9},
				varbinaryTI, nil, nil)
			require.False(t, change.IsIdentityUpdated())
			require.False(t, change.IsPrimaryOrUniqueKeyUpdated())

			change = NewRowChange(source, nil,
				[]interface{}{1, v.base, 7},
				[]interface{}{1, v.diff, 7},
				varbinaryTI, nil, nil)
			require.False(t, change.IsIdentityUpdated())
			require.True(t, change.IsPrimaryOrUniqueKeyUpdated())
		})
	}
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
		{
			name: "binary expression index unchanged",
			createSQL: "CREATE TABLE tb1 (id INT PRIMARY KEY, payload VARBINARY(16), note VARCHAR(16), " +
				"UNIQUE KEY uk_payload_expr ((CAST(payload AS BINARY(16)))))",
			preValues:  []any{1, "alice", "old"},
			postValues: []any{1, "alice", "new"},
		},
		{
			name: "decimal expression index unchanged",
			createSQL: "CREATE TABLE tb1 (id INT PRIMARY KEY, price DECIMAL(10, 2), note VARCHAR(16), " +
				"UNIQUE KEY uk_price_expr ((price + 0)))",
			preValues:  []any{1, "12.30", "old"},
			postValues: []any{1, "12.30", "new"},
		},
		{
			name: "bit expression index unchanged",
			createSQL: "CREATE TABLE tb1 (id INT PRIMARY KEY, flag BIT(8), note VARCHAR(16), " +
				"UNIQUE KEY uk_flag_expr ((flag | b'00000000')))",
			preValues:  []any{1, uint64(1), "old"},
			postValues: []any{1, uint64(1), "new"},
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

func TestPrimaryOrUniqueKeyUpdatedInterleavedHiddenColumn(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	sourceTI := mockTableInfo(t, "CREATE TABLE tb1 ("+
		"id INT PRIMARY KEY, "+
		"a VARCHAR(32), "+
		"b VARCHAR(32), "+
		"UNIQUE KEY uk_a ((lower(a))), "+
		"UNIQUE KEY uk_b ((lower(b))))")

	hiddenA := testutil.ExpressionIndexColumnName(t, sourceTI, "uk_a")
	hiddenB := testutil.ExpressionIndexColumnName(t, sourceTI, "uk_b")
	testutil.ReorderColumnsByName(t, sourceTI, "id", "a", hiddenA, "b", hiddenB)

	change := NewRowChange(source, nil,
		[]any{1, "Alice", "Bob"},
		[]any{1, "Alice", "Carol"},
		sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())
}

func TestPrimaryOrUniqueKeyUpdatedUniqueAfterHiddenColumn(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	sourceTI := mockTableInfo(t, "CREATE TABLE tb1 ("+
		"a VARCHAR(32), "+
		"b INT NOT NULL UNIQUE, "+
		"c VARCHAR(32), "+
		"UNIQUE KEY uk_a ((lower(a))))")

	hiddenA := testutil.ExpressionIndexColumnName(t, sourceTI, "uk_a")
	testutil.ReorderColumnsByName(t, sourceTI, "a", hiddenA, "b", "c")

	change := NewRowChange(source, nil,
		[]any{"Alice", 1, "old"},
		[]any{"Alice", 2, "old"},
		sourceTI, nil, nil)
	require.NotPanics(t, func() {
		require.True(t, change.IsIdentityUpdated())
	})
	require.NotPanics(t, func() {
		require.True(t, change.IsPrimaryOrUniqueKeyUpdated())
	})

	change = NewRowChange(source, nil,
		[]any{"Alice", 1, "old"},
		[]any{"Alice", 1, "new"},
		sourceTI, nil, nil)
	require.NotPanics(t, func() {
		require.False(t, change.IsIdentityUpdated())
	})
	require.NotPanics(t, func() {
		require.Equal(t, []any{1}, change.RowIdentity())
	})
	require.NotPanics(t, func() {
		require.False(t, change.IsPrimaryOrUniqueKeyUpdated())
	})
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

func TestPrimaryOrUniqueKeyUpdatedWithStoredGeneratedUniqueIndex(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	sourceTI := mockTableInfo(t, "CREATE TABLE tb1 ("+
		"id BIGINT PRIMARY KEY, name VARCHAR(255), "+
		"lower_name VARCHAR(255) GENERATED ALWAYS AS (lower(name)) STORED, "+
		"UNIQUE KEY uk_lower_name (lower_name))")

	change := NewRowChange(source, nil,
		[]any{1, "Alice", "alice"},
		[]any{1, "ALICE", "alice"},
		sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.False(t, change.IsPrimaryOrUniqueKeyUpdated())

	change = NewRowChange(source, nil,
		[]any{1, "Alice", "alice"},
		[]any{1, "Bob", "bob"},
		sourceTI, nil, nil)
	require.False(t, change.IsIdentityUpdated())
	require.True(t, change.IsPrimaryOrUniqueKeyUpdated())
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
