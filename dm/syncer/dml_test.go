// Copyright 2019 PingCAP, Inc.
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

package syncer

import (
	"math"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
)

func (s *testSyncerSuite) TestCastUnsigned(c *C) {
	// ref: https://dev.mysql.com/doc/refman/5.7/en/integer-types.html
	cases := []struct {
		data     interface{}
		unsigned bool
		Type     byte
		expected interface{}
	}{
		{int8(-math.Exp2(7)), false, mysql.TypeTiny, int8(-math.Exp2(7))}, // TINYINT
		{int8(-math.Exp2(7)), true, mysql.TypeTiny, uint8(math.Exp2(7))},
		{int16(-math.Exp2(15)), false, mysql.TypeShort, int16(-math.Exp2(15))}, // SMALLINT
		{int16(-math.Exp2(15)), true, mysql.TypeShort, uint16(math.Exp2(15))},
		{int32(-math.Exp2(23)), false, mysql.TypeInt24, int32(-math.Exp2(23))}, // MEDIUMINT
		{int32(-math.Exp2(23)), true, mysql.TypeInt24, uint32(math.Exp2(23))},
		{int32(-math.Exp2(31)), false, mysql.TypeLong, int32(-math.Exp2(31))}, // INT
		{int32(-math.Exp2(31)), true, mysql.TypeLong, uint32(math.Exp2(31))},
		{int64(-math.Exp2(63)), false, mysql.TypeLonglong, int64(-math.Exp2(63))}, // BIGINT
		{int64(-math.Exp2(63)), true, mysql.TypeLonglong, uint64(math.Exp2(63))},
	}
	for _, cs := range cases {
		ft := types.NewFieldType(cs.Type)
		if cs.unsigned {
			ft.Flag |= mysql.UnsignedFlag
		}
		obtained := castUnsigned(cs.data, ft)
		c.Assert(obtained, Equals, cs.expected)
	}
}

func createTableInfo(p *parser.Parser, se sessionctx.Context, tableID int64, sql string) (*model.TableInfo, error) {
	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	if err != nil {
		return nil, err
	}
	return tiddl.MockTableInfo(se, node.(*ast.CreateTableStmt), tableID)
}

func (s *testSyncerSuite) TestFindFitIndex(c *C) {
	p := parser.New()
	se := mock.NewContext()

	ti, err := createTableInfo(p, se, 1, `
		create table t1(
			a int,
			b int,
			c int,
			d int not null,
			primary key(a, b),
			unique key(c),
			unique key(d)
		);
	`)
	c.Assert(err, IsNil)

	columns := findFitIndex(ti)
	c.Assert(columns, NotNil)
	c.Assert(columns.Columns, HasLen, 2)
	c.Assert(columns.Columns[0].Name.L, Equals, "a")
	c.Assert(columns.Columns[1].Name.L, Equals, "b")

	ti, err = createTableInfo(p, se, 2, `create table t2(c int unique);`)
	c.Assert(err, IsNil)
	columns = findFitIndex(ti)
	c.Assert(columns, IsNil)

	ti, err = createTableInfo(p, se, 3, `create table t3(d int not null unique);`)
	c.Assert(err, IsNil)
	columns = findFitIndex(ti)
	c.Assert(columns, NotNil)
	c.Assert(columns.Columns, HasLen, 1)
	c.Assert(columns.Columns[0].Name.L, Equals, "d")

	ti, err = createTableInfo(p, se, 4, `create table t4(e int not null, key(e));`)
	c.Assert(err, IsNil)
	columns = findFitIndex(ti)
	c.Assert(columns, IsNil)

	ti, err = createTableInfo(p, se, 5, `create table t5(f datetime primary key);`)
	c.Assert(err, IsNil)
	columns = findFitIndex(ti)
	c.Assert(columns, NotNil)
	c.Assert(columns.Columns, HasLen, 1)
	c.Assert(columns.Columns[0].Name.L, Equals, "f")

	ti, err = createTableInfo(p, se, 6, `create table t6(g int primary key);`)
	c.Assert(err, IsNil)
	columns = findFitIndex(ti)
	c.Assert(columns, NotNil)
	c.Assert(columns.Columns, HasLen, 1)
	c.Assert(columns.Columns[0].Name.L, Equals, "g")
}

func (s *testSyncerSuite) TestGenMultipleKeys(c *C) {
	p := parser.New()
	se := mock.NewContext()

	testCases := []struct {
		schema string
		values []interface{}
		keys   []string
	}{
		{
			// test no keys
			schema: `create table t1(a int)`,
			values: []interface{}{10},
			keys:   []string{"table"},
		},
		{
			// one primary key
			schema: `create table t2(a int primary key, b double)`,
			values: []interface{}{60, 70.5},
			keys:   []string{"60.a.table"},
		},
		{
			// one unique key
			schema: `create table t3(a int unique, b double)`,
			values: []interface{}{60, 70.5},
			keys:   []string{"60.a.table"},
		},
		{
			// one ordinary key
			schema: `create table t4(a int, b double, key(b))`,
			values: []interface{}{60, 70.5},
			keys:   []string{"table"},
		},
		{
			// multiple keys
			schema: `create table t5(a int, b text, c int, key(a), key(b(3)))`,
			values: []interface{}{13, "abcdef", 15},
			keys:   []string{"table"},
		},
		{
			// multiple keys with primary key
			schema: `create table t6(a int primary key, b varchar(16) unique)`,
			values: []interface{}{16, "xyz"},
			keys:   []string{"16.a.table", "xyz.b.table"},
		},
		{
			// non-integer primary key
			schema: `create table t65(a int unique, b varchar(16) primary key)`,
			values: []interface{}{16, "xyz"},
			keys:   []string{"16.a.table", "xyz.b.table"},
		},
		{
			// primary key of multiple columns
			schema: `create table t7(a int, b int, primary key(a, b))`,
			values: []interface{}{59, 69},
			keys:   []string{"59.a.69.b.table"},
		},
		{
			// ordinary key of multiple columns
			schema: `create table t75(a int, b int, c int, key(a, b), key(c, b))`,
			values: []interface{}{48, 58, 68},
			keys:   []string{"table"},
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
			keys:   []string{"27.a.37.b.table", "37.b.47.c.table", "47.c.27.a.table"},
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
			keys:   []string{"17.a.table"},
		},
	}

	for i, tc := range testCases {
		schema := tc.schema
		assert := func(obtained interface{}, checker Checker, args ...interface{}) {
			c.Assert(obtained, checker, append(args, Commentf("test case schema: %s", schema))...)
		}

		ti, err := createTableInfo(p, se, int64(i+1), tc.schema)
		assert(err, IsNil)
		keys := genMultipleKeys(ti, tc.values, "table")
		assert(keys, DeepEquals, tc.keys)
	}
}
