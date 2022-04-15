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

	"github.com/stretchr/testify/require"

	cdcmodel "github.com/pingcap/tiflow/cdc/model"
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
