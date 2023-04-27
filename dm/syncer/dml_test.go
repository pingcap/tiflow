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
	"testing"

	tiddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/stretchr/testify/require"
)

var (
	location = binlog.Location{
		Position: binlog.MinPosition,
	}
	ec             = &eventContext{startLocation: location, endLocation: location, lastLocation: location}
	ecWithSafeMode = &eventContext{startLocation: location, endLocation: location, lastLocation: location, safeMode: true}
)

func TestCastUnsigned(t *testing.T) {
	t.Parallel()

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
			ft.AddFlag(mysql.UnsignedFlag)
		}
		obtained := castUnsigned(cs.data, ft)
		require.Equal(t, cs.expected, obtained)
	}
}

func createTableInfo(p *parser.Parser, se sessionctx.Context, tableID int64, sql string) (*model.TableInfo, error) {
	node, err := p.ParseOneStmt(sql, "utf8mb4", "utf8mb4_bin")
	if err != nil {
		return nil, err
	}
	return tiddl.MockTableInfo(se, node.(*ast.CreateTableStmt), tableID)
}

func TestGenDMLWithSameOp(t *testing.T) {
	t.Parallel()

	targetTable1 := &cdcmodel.TableName{Schema: "db1", Table: "tb1"}
	targetTable2 := &cdcmodel.TableName{Schema: "db2", Table: "tb2"}
	sourceTable11 := &cdcmodel.TableName{Schema: "dba", Table: "tba"}
	sourceTable12 := &cdcmodel.TableName{Schema: "dba", Table: "tbb"}
	sourceTable21 := &cdcmodel.TableName{Schema: "dbb", Table: "tba"}
	sourceTable22 := &cdcmodel.TableName{Schema: "dbb", Table: "tbb"}

	tableInfo11 := mockTableInfo(t, "create table db.tb(id int primary key, col1 int unique not null, name varchar(24))")
	tableInfo12 := mockTableInfo(t, "create table db.tb(id int primary key, col1 int unique not null, name varchar(24))")
	tableInfo21 := mockTableInfo(t, "create table db.tb(id int primary key, col2 int unique not null, name varchar(24))")
	tableInfo22 := mockTableInfo(t, "create table db.tb(id int primary key, col3 int unique not null, name varchar(24))")

	dmls := []*job{
		// insert
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable11, targetTable1, nil, []interface{}{1, 1, "a"}, tableInfo11, nil, nil),
			ecWithSafeMode,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable11, targetTable1, nil, []interface{}{2, 2, "b"}, tableInfo11, nil, nil),
			ecWithSafeMode,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable12, targetTable1, nil, []interface{}{3, 3, "c"}, tableInfo12, nil, nil),
			ecWithSafeMode,
		),

		// update no index but safemode
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable11, targetTable1, []interface{}{1, 1, "a"}, []interface{}{1, 1, "aa"}, tableInfo11, nil, nil),
			ecWithSafeMode,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable11, targetTable1, []interface{}{2, 2, "b"}, []interface{}{2, 2, "bb"}, tableInfo11, nil, nil),
			ecWithSafeMode,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable12, targetTable1, []interface{}{3, 3, "c"}, []interface{}{3, 3, "cc"}, tableInfo12, nil, nil),
			ecWithSafeMode,
		),

		// update uk
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable11, targetTable1, []interface{}{1, 1, "aa"}, []interface{}{1, 4, "aa"}, tableInfo11, nil, nil),
			ecWithSafeMode,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable11, targetTable1, []interface{}{2, 2, "bb"}, []interface{}{2, 5, "bb"}, tableInfo11, nil, nil),
			ecWithSafeMode,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable12, targetTable1, []interface{}{3, 3, "cc"}, []interface{}{3, 6, "cc"}, tableInfo12, nil, nil),
			ecWithSafeMode,
		),

		// update pk
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable11, targetTable1, []interface{}{1, 4, "aa"}, []interface{}{4, 4, "aa"}, tableInfo11, nil, nil),
			ecWithSafeMode,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable11, targetTable1, []interface{}{2, 5, "bb"}, []interface{}{5, 5, "bb"}, tableInfo11, nil, nil),
			ecWithSafeMode,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable12, targetTable1, []interface{}{3, 6, "cc"}, []interface{}{6, 6, "cc"}, tableInfo12, nil, nil),
			ecWithSafeMode,
		),
		// delete
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable11, targetTable1, []interface{}{4, 4, "aa"}, nil, tableInfo11, nil, nil),
			ecWithSafeMode,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable11, targetTable1, []interface{}{5, 5, "bb"}, nil, tableInfo11, nil, nil),
			ecWithSafeMode,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable12, targetTable1, []interface{}{6, 6, "cc"}, nil, tableInfo12, nil, nil),
			ecWithSafeMode,
		),

		// target table 2
		// insert
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable21, targetTable2, nil, []interface{}{1, 1, "a"}, tableInfo21, nil, nil),
			ecWithSafeMode,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable21, targetTable2, nil, []interface{}{2, 2, "b"}, tableInfo21, nil, nil),
			ec,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable22, targetTable2, nil, []interface{}{3, 3, "c"}, tableInfo22, nil, nil),
			ec,
		),

		// update no index
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable21, targetTable2, []interface{}{1, 1, "a"}, []interface{}{1, 1, "aa"}, tableInfo21, nil, nil),
			ec,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable21, targetTable2, []interface{}{2, 2, "b"}, []interface{}{2, 2, "bb"}, tableInfo21, nil, nil),
			ec,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable22, targetTable2, []interface{}{3, 3, "c"}, []interface{}{3, 3, "cc"}, tableInfo22, nil, nil),
			ec,
		),

		// update uk
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable21, targetTable2, []interface{}{1, 1, "aa"}, []interface{}{1, 4, "aa"}, tableInfo21, nil, nil),
			ec,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable21, targetTable2, []interface{}{2, 2, "bb"}, []interface{}{2, 5, "bb"}, tableInfo21, nil, nil),
			ec,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable22, targetTable2, []interface{}{3, 3, "cc"}, []interface{}{3, 6, "cc"}, tableInfo22, nil, nil),
			ec,
		),

		// update pk
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable21, targetTable2, []interface{}{1, 4, "aa"}, []interface{}{4, 4, "aa"}, tableInfo21, nil, nil),
			ec,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable21, targetTable2, []interface{}{2, 5, "bb"}, []interface{}{5, 5, "bb"}, tableInfo21, nil, nil),
			ec,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable22, targetTable2, []interface{}{3, 6, "cc"}, []interface{}{6, 6, "cc"}, tableInfo22, nil, nil),
			ec,
		),

		// delete
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable21, targetTable2, []interface{}{4, 4, "aa"}, nil, tableInfo21, nil, nil),
			ec,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable21, targetTable2, []interface{}{5, 5, "bb"}, nil, tableInfo21, nil, nil),
			ec,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable22, targetTable2, []interface{}{6, 6, "cc"}, nil, tableInfo22, nil, nil),
			ec,
		),

		// table1
		// detele
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable11, targetTable1, []interface{}{44, 44, "aaa"}, nil, tableInfo11, nil, nil),
			ec,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable11, targetTable1, []interface{}{55, 55, "bbb"}, nil, tableInfo11, nil, nil),
			ec,
		),
		newDMLJob(
			sqlmodel.NewRowChange(sourceTable12, targetTable1, []interface{}{66, 66, "ccc"}, nil, tableInfo12, nil, nil),
			ec,
		),
	}

	expectQueries := []string{
		// table1
		"REPLACE INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?),(?,?,?),(?,?,?)",
		"DELETE FROM `db1`.`tb1` WHERE `id` = ? LIMIT 1",
		"REPLACE INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?)",
		"DELETE FROM `db1`.`tb1` WHERE `id` = ? LIMIT 1",
		"REPLACE INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?)",
		"DELETE FROM `db1`.`tb1` WHERE `id` = ? LIMIT 1",
		"REPLACE INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?)",
		"DELETE FROM `db1`.`tb1` WHERE `id` = ? LIMIT 1",
		"REPLACE INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?)",
		"DELETE FROM `db1`.`tb1` WHERE `id` = ? LIMIT 1",
		"REPLACE INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?)",
		"DELETE FROM `db1`.`tb1` WHERE `id` = ? LIMIT 1",
		"REPLACE INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?)",
		"DELETE FROM `db1`.`tb1` WHERE `id` = ? LIMIT 1",
		"REPLACE INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?)",
		"DELETE FROM `db1`.`tb1` WHERE `id` = ? LIMIT 1",
		"REPLACE INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?)",
		"DELETE FROM `db1`.`tb1` WHERE `id` = ? LIMIT 1",
		"REPLACE INTO `db1`.`tb1` (`id`,`col1`,`name`) VALUES (?,?,?)",
		"DELETE FROM `db1`.`tb1` WHERE (`id` = ?) OR (`id` = ?) OR (`id` = ?)",

		// table2
		"REPLACE INTO `db2`.`tb2` (`id`,`col2`,`name`) VALUES (?,?,?)",
		"INSERT INTO `db2`.`tb2` (`id`,`col2`,`name`) VALUES (?,?,?)",
		"INSERT INTO `db2`.`tb2` (`id`,`col3`,`name`) VALUES (?,?,?)",
		"INSERT INTO `db2`.`tb2` (`id`,`col2`,`name`) VALUES (?,?,?),(?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col2`=VALUES(`col2`),`name`=VALUES(`name`)",
		"INSERT INTO `db2`.`tb2` (`id`,`col3`,`name`) VALUES (?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col3`=VALUES(`col3`),`name`=VALUES(`name`)",
		"INSERT INTO `db2`.`tb2` (`id`,`col2`,`name`) VALUES (?,?,?),(?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col2`=VALUES(`col2`),`name`=VALUES(`name`)",
		"INSERT INTO `db2`.`tb2` (`id`,`col3`,`name`) VALUES (?,?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`col3`=VALUES(`col3`),`name`=VALUES(`name`)",
		"UPDATE `db2`.`tb2` SET `id`=CASE WHEN `id` = ? THEN ? WHEN `id` = ? THEN ? END, `col2`=CASE WHEN `id` = ? THEN ? WHEN `id` = ? THEN ? END, `name`=CASE WHEN `id` = ? THEN ? WHEN `id` = ? THEN ? END WHERE (`id` = ?) OR (`id` = ?)",
		"UPDATE `db2`.`tb2` SET `id`=CASE WHEN `id` = ? THEN ? END, `col3`=CASE WHEN `id` = ? THEN ? END, `name`=CASE WHEN `id` = ? THEN ? END WHERE (`id` = ?)",
		"DELETE FROM `db2`.`tb2` WHERE (`id` = ?) OR (`id` = ?) OR (`id` = ?)",

		// table1
		"DELETE FROM `db1`.`tb1` WHERE (`id` = ?) OR (`id` = ?) OR (`id` = ?)",
	}

	expectArgs := [][]interface{}{
		// table1
		{1, 1, "a", 2, 2, "b", 3, 3, "c"},
		{1},
		{1, 1, "aa"},
		{2},
		{2, 2, "bb"},
		{3},
		{3, 3, "cc"},
		{1},
		{1, 4, "aa"},
		{2},
		{2, 5, "bb"},
		{3},
		{3, 6, "cc"},
		{1},
		{4, 4, "aa"},
		{2},
		{5, 5, "bb"},
		{3},
		{6, 6, "cc"},
		{4, 5, 6},

		// table2
		{1, 1, "a"},
		{2, 2, "b"},
		{3, 3, "c"},
		{1, 1, "aa", 2, 2, "bb"},
		{3, 3, "cc"},
		{1, 4, "aa", 2, 5, "bb"},
		{3, 6, "cc"},
		{1, 4, 2, 5, 1, 4, 2, 5, 1, "aa", 2, "bb", 1, 2},
		{3, 6, 3, 6, 3, "cc", 3},
		{4, 5, 6},

		// table1
		{44, 55, 66},
	}

	queries, args := genDMLsWithSameOp(dmls)
	require.Equal(t, expectQueries, queries)
	require.Equal(t, expectArgs, args)
}

func TestGBKExtractValueFromData(t *testing.T) {
	t.Parallel()

	table := `CREATE TABLE t (c INT PRIMARY KEY, d VARCHAR(20) CHARSET GBK);`
	se := mock.NewContext()
	p := parser.New()
	ti, err := createTableInfo(p, se, 0, table)
	require.NoError(t, err)

	row := []interface{}{1, "\xc4\xe3\xba\xc3"}
	expect := []interface{}{1, []byte("\xc4\xe3\xba\xc3")}
	got, err := adjustValueFromBinlogData(row, ti)
	require.NoError(t, err)
	require.Equal(t, expect, got)
}
