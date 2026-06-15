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
	"sync"
	"testing"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestCausalityKeys(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}

	cases := []struct {
		createSQL string
		preValue  []interface{}
		postValue []interface{}

		causalityKeys []string
	}{
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT, c3 VARCHAR(10) UNIQUE)",
			[]interface{}{1, 2, "abc"},
			[]interface{}{3, 4, "abc"},
			[]string{"abc.c3.db.tb1", "1.c.db.tb1", "abc.c3.db.tb1", "3.c.db.tb1"},
		},
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT, c3 VARCHAR(10), UNIQUE INDEX(c3(1)))",
			[]interface{}{1, 2, "abc"},
			[]interface{}{3, 4, "adef"},
			[]string{"a.c3.db.tb1", "1.c.db.tb1", "a.c3.db.tb1", "3.c.db.tb1"},
		},

		// test not string key
		{
			"CREATE TABLE tb1 (a INT, b INT, UNIQUE KEY a(a))",
			[]interface{}{100, 200},
			nil,
			[]string{"100.a.db.tb1"},
		},

		// test text
		{
			"CREATE TABLE tb1 (a INT, b TEXT, UNIQUE KEY b(b(3)))",
			[]interface{}{1, "1234"},
			nil,
			[]string{"123.b.db.tb1"},
		},

		// test composite keys
		{
			"CREATE TABLE tb1 (a INT, b TEXT, UNIQUE KEY c2(a, b(3)))",
			[]interface{}{1, "1234"},
			nil,
			[]string{"1.a.123.b.db.tb1"},
		},

		// test value is null
		{
			"CREATE TABLE tb1 (a INT, b TEXT, UNIQUE KEY c2(a, b(3)))",
			[]interface{}{1, nil},
			nil,
			[]string{"1.a.db.tb1"},
		},
	}

	for _, ca := range cases {
		ti := mockTableInfo(t, ca.createSQL)
		change := NewRowChange(source, nil, ca.preValue, ca.postValue, ti, nil, nil)
		require.Equal(t, ca.causalityKeys, change.CausalityKeys())
	}
}

func TestCausalityKeysWithCausalityKeySourceTable(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "DB", Table: "Parent"}
	causalityKeySource := &cdcmodel.TableName{Schema: "db", Table: "parent"}
	ti := mockTableInfo(t, "CREATE TABLE parent (id INT PRIMARY KEY)")
	change := NewRowChange(source, nil, nil, []interface{}{10}, ti, nil, nil)
	change.SetCausalityKeySourceTable(causalityKeySource)

	require.Equal(t, []string{"10.id.db.parent"}, change.CausalityKeys())
	require.Equal(t, source, change.GetSourceTable())
}

func TestCausalityKeysNoRace(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	ti := mockTableInfo(t, "CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT, c3 VARCHAR(10) UNIQUE)")
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			change := NewRowChange(source, nil, []interface{}{1, 2, "abc"}, []interface{}{3, 4, "abc"}, ti, nil, nil)
			change.CausalityKeys()
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestGetCausalityString(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tbl"}

	testCases := []struct {
		schema string
		values []interface{}
		keys   []string
	}{
		{
			// test no keys will use full row data instead of table name
			schema: `create table t1(a int)`,
			values: []interface{}{10},
			keys:   []string{"10.a.db.tbl"},
		},
		{
			// one primary key
			schema: `create table t2(a int primary key, b double)`,
			values: []interface{}{60, 70.5},
			keys:   []string{"60.a.db.tbl"},
		},
		{
			// one unique key
			schema: `create table t3(a int unique, b double)`,
			values: []interface{}{60, 70.5},
			keys:   []string{"60.a.db.tbl"},
		},
		{
			// one ordinary key
			schema: `create table t4(a int, b double, key(b))`,
			values: []interface{}{60, 70.5},
			keys:   []string{"60.a.70.5.b.db.tbl"},
		},
		{
			// multiple keys
			schema: `create table t5(a int, b text, c int, key(a), key(b(3)))`,
			values: []interface{}{13, "abcdef", 15},
			keys:   []string{"13.a.abcdef.b.15.c.db.tbl"},
		},
		{
			// multiple keys with primary key
			schema: `create table t6(a int primary key, b varchar(16) unique)`,
			values: []interface{}{16, "xyz"},
			keys:   []string{"xyz.b.db.tbl", "16.a.db.tbl"},
		},
		{
			// non-integer primary key
			schema: `create table t65(a int unique, b varchar(16) primary key)`,
			values: []interface{}{16, "xyz"},
			keys:   []string{"16.a.db.tbl", "xyz.b.db.tbl"},
		},
		{
			// case insensitive
			schema: `create table t_ci(a int unique, b varchar(16) primary key)default charset=utf8 collate=utf8_unicode_ci`,
			values: []interface{}{16, "XyZ"},
			keys:   []string{"16.a.db.tbl", "xyz.b.db.tbl"},
		},
		{
			// case sensitive
			schema: `create table t_bin(a int unique, b varchar(16) primary key)default charset=utf8 collate=utf8_bin`,
			values: []interface{}{16, "XyZ"},
			keys:   []string{"16.a.db.tbl", "XyZ.b.db.tbl"},
		},
		{
			// primary key of multiple columns
			schema: `create table t7(a int, b int, primary key(a, b))`,
			values: []interface{}{59, 69},
			keys:   []string{"59.a.69.b.db.tbl"},
		},
		{
			// ordinary key of multiple columns
			schema: `create table t75(a int, b int, c int, key(a, b), key(c, b))`,
			values: []interface{}{48, 58, 68},
			keys:   []string{"48.a.58.b.68.c.db.tbl"},
		},
		{
			// so many keys
			schema: `
				create table t8(
					a int, b int, c int,
					primary key(a, b),
					unique key(b, c),
					key(a, b, c),
					unique key(c, a)
				)
			`,
			values: []interface{}{27, 37, 47},
			keys:   []string{"27.a.37.b.db.tbl", "37.b.47.c.db.tbl", "47.c.27.a.db.tbl"},
		},
		{
			// `null` for unique key
			schema: `
				create table t8(
					a int, b int default null,
					primary key(a),
					unique key(b)
				)
			`,
			values: []interface{}{17, nil},
			keys:   []string{"17.a.db.tbl"},
		},
	}

	for _, ca := range testCases {
		ti := mockTableInfo(t, ca.schema)
		change := NewRowChange(source, nil, nil, ca.values, ti, nil, nil)
		change.lazyInitWhereHandle()
		require.Equal(t, ca.keys, change.getCausalityString(ca.values))
	}
}

func TestCausalityKeysExpressionIndex(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	// A functional UNIQUE index is backed by a hidden generated column (#12696).
	// The binlog row image carries only the two stored columns (id, name). The
	// hidden column's value must be materialized from its expression so the
	// expression index still participates in conflict detection, instead of
	// panicking with "index out of range".
	ti := mockTableInfo(t, "CREATE TABLE tb1 (id BIGINT PRIMARY KEY, name VARCHAR(255), "+
		"UNIQUE KEY only_one_alice ((CASE name WHEN 'Alice' THEN 1 ELSE NULL END)))")

	// name = 'Alice' -> the expression evaluates to 1, so the expression index
	// produces a key; the PK produces another. Two different rows that collide on
	// the expression value will share the expression key and be serialized.
	change := NewRowChange(source, nil, nil, []any{1, "Alice"}, ti, nil, nil)
	change.lazyInitWhereHandle()
	var keys []string
	require.NotPanics(t, func() {
		keys = change.getCausalityString([]any{1, "Alice"})
	})
	require.Contains(t, keys, "1.id.db.tb1") // PK key
	require.Len(t, keys, 2)                  // PK key + expression-index key

	// A second, different row that also evaluates to 'Alice' collides on the
	// expression value. The two rows must share the expression-index key so that
	// they are serialized (skipping the index would lose this and could replicate
	// them out of order, hitting a transient unique-key violation downstream).
	other := NewRowChange(source, nil, nil, []any{9, "Alice"}, ti, nil, nil)
	other.lazyInitWhereHandle()
	otherKeys := other.getCausalityString([]any{9, "Alice"})
	exprKey := func(ks []string) string {
		for _, k := range ks {
			if k != "1.id.db.tb1" && k != "9.id.db.tb1" {
				return k
			}
		}
		return ""
	}
	require.NotEmpty(t, exprKey(keys))
	require.Equal(t, exprKey(keys), exprKey(otherKeys),
		"rows colliding on the expression value must share the expression-index key")

	// name = 'Bob' -> the expression evaluates to NULL, so the expression index
	// contributes no key and only the PK key remains.
	change = NewRowChange(source, nil, nil, []any{2, "Bob"}, ti, nil, nil)
	change.lazyInitWhereHandle()
	require.NotPanics(t, func() {
		keys = change.getCausalityString([]any{2, "Bob"})
	})
	require.Equal(t, []string{"2.id.db.tb1"}, keys)
}

func TestCausalityKeysExpressionIndexMaterializeFailure(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	ti := mockTableInfo(t, "CREATE TABLE tb1 (id BIGINT PRIMARY KEY, name VARCHAR(255), "+
		"UNIQUE KEY only_one_alice ((CASE name WHEN 'Alice' THEN 1 ELSE NULL END)))")
	corruptHiddenGeneratedExpr(t, ti)

	change := NewRowChange(source, nil, nil, []any{1, "Alice"}, ti, nil, nil)

	var keys []string
	require.NotPanics(t, func() {
		keys = change.CausalityKeys()
	})
	require.Equal(t, []string{"1.id.db.tb1"}, keys)
}

func TestCausalityKeysExpressionIndexNoRace(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	ti := mockTableInfo(t, "CREATE TABLE tb1 (id BIGINT PRIMARY KEY, name VARCHAR(255), "+
		"UNIQUE KEY only_one_alice ((CASE name WHEN 'Alice' THEN 1 ELSE NULL END)))")
	// One shared WhereHandle so the cached generated-column expression is built
	// once and read concurrently (run with -race).
	handle := GetWhereHandle(ti, ti)
	var g errgroup.Group
	for i := range 50 {
		g.Go(func() error {
			change := NewRowChange(source, nil, nil, []any{i, "Alice"}, ti, nil, nil)
			change.SetWhereHandle(handle)
			change.CausalityKeys()
			return nil
		})
	}
	require.NoError(t, g.Wait())
}

func TestGeneratedColumnExprContextCached(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	ti := mockTableInfo(t, "CREATE TABLE tb1 (id BIGINT PRIMARY KEY, name VARCHAR(255), "+
		"UNIQUE KEY uk_lower_name ((lower(name))))")
	handle := GetWhereHandle(ti, ti)
	require.NotNil(t, handle.hiddenGeneratedColumnExprCache)

	tiCtx := utils.NewSessionCtx(map[string]string{
		"sql_mode": "ANSI_QUOTES",
	})
	change := NewRowChange(source, nil, nil, []any{1, "Alice"}, ti, nil, tiCtx)
	change.SetWhereHandle(handle)
	change.CausalityKeys()

	cache := handle.hiddenGeneratedColumnExprCache
	require.NotNil(t, cache.exprCtx)
	require.True(t, cache.exprCtx.GetStaticEvalCtx().SQLMode().HasANSIQuotesMode())
	require.NotEqual(t, mysql.ModeNone, cache.exprCtx.GetStaticEvalCtx().SQLMode())
	cachedCtx := cache.exprCtx

	other := NewRowChange(source, nil, nil, []any{2, "Bob"}, ti, nil, tiCtx)
	other.SetWhereHandle(handle)
	other.CausalityKeys()
	require.Same(t, cachedCtx, cache.exprCtx)
}

func corruptHiddenGeneratedExpr(t *testing.T, ti *timodel.TableInfo) {
	t.Helper()

	found := false
	for _, col := range ti.Columns {
		if col.Hidden && col.IsGenerated() {
			col.GeneratedExprString = "not a valid expression +"
			found = true
		}
	}
	require.True(t, found, "expression index should create a hidden generated column")
}
