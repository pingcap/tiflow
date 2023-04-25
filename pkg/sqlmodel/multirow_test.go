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

type genSQLFunc func(changes ...*RowChange) (string, []interface{})

func TestGenDeleteMultiRows(t *testing.T) {
	t.Parallel()

	source1 := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	source2 := &cdcmodel.TableName{Schema: "db", Table: "tb2"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb"}

	sourceTI1 := mockTableInfo(t, "CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT)")
	sourceTI2 := mockTableInfo(t, "CREATE TABLE tb2 (c INT PRIMARY KEY, c2 INT)")
	targetTI := mockTableInfo(t, "CREATE TABLE tb (c INT PRIMARY KEY, c2 INT)")

	change1 := NewRowChange(source1, target, []interface{}{1, 2}, nil, sourceTI1, targetTI, nil)
	change2 := NewRowChange(source2, target, []interface{}{3, 4}, nil, sourceTI2, targetTI, nil)
	sql, args := GenDeleteSQL(change1, change2)

	require.Equal(t, "DELETE FROM `db`.`tb` WHERE (`c` = ?) OR (`c` = ?)", sql)
	require.Equal(t, []interface{}{1, 3}, args)
}

func TestGenUpdateMultiRows(t *testing.T) {
	t.Parallel()
	testGenUpdateMultiRows(t, GenUpdateSQL)
}

func TestGenUpdateMultiRowsOneColPK(t *testing.T) {
	t.Parallel()
	testGenUpdateMultiRowsOneColPK(t, GenUpdateSQL)
}

func TestGenUpdateMultiRowsWithVirtualGeneratedColumn(t *testing.T) {
	t.Parallel()
	testGenUpdateMultiRowsWithVirtualGeneratedColumn(t, GenUpdateSQL)
	testGenUpdateMultiRowsWithVirtualGeneratedColumns(t, GenUpdateSQL)
}

func TestGenUpdateMultiRowsWithStoredGeneratedColumn(t *testing.T) {
	t.Parallel()
	testGenUpdateMultiRowsWithStoredGeneratedColumn(t, GenUpdateSQL)
}

func testGenUpdateMultiRows(t *testing.T, genUpdate genSQLFunc) {
	source1 := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	source2 := &cdcmodel.TableName{Schema: "db", Table: "tb2"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb"}

	sourceTI1 := mockTableInfo(t, "CREATE TABLE tb1 (c INT, c2 INT, c3 INT, UNIQUE KEY (c, c2))")
	sourceTI2 := mockTableInfo(t, "CREATE TABLE tb2 (c INT, c2 INT, c3 INT, UNIQUE KEY (c, c2))")
	targetTI := mockTableInfo(t, "CREATE TABLE tb (c INT, c2 INT, c3 INT, UNIQUE KEY (c, c2))")

	change1 := NewRowChange(source1, target, []interface{}{1, 2, 3}, []interface{}{10, 20, 30}, sourceTI1, targetTI, nil)
	change2 := NewRowChange(source2, target, []interface{}{4, 5, 6}, []interface{}{40, 50, 60}, sourceTI2, targetTI, nil)
	sql, args := genUpdate(change1, change2)

	expectedSQL := "UPDATE `db`.`tb` SET " +
		"`c`=CASE WHEN `c` = ? AND `c2` = ? THEN ? WHEN `c` = ? AND `c2` = ? THEN ? END, " +
		"`c2`=CASE WHEN `c` = ? AND `c2` = ? THEN ? WHEN `c` = ? AND `c2` = ? THEN ? END, " +
		"`c3`=CASE WHEN `c` = ? AND `c2` = ? THEN ? WHEN `c` = ? AND `c2` = ? THEN ? END " +
		"WHERE (`c` = ? AND `c2` = ?) OR (`c` = ? AND `c2` = ?)"
	expectedArgs := []interface{}{
		1, 2, 10, 4, 5, 40,
		1, 2, 20, 4, 5, 50,
		1, 2, 30, 4, 5, 60,
		1, 2, 4, 5,
	}

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedArgs, args)
}

func testGenUpdateMultiRowsOneColPK(t *testing.T, genUpdate genSQLFunc) {
	source1 := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	source2 := &cdcmodel.TableName{Schema: "db", Table: "tb2"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb"}

	sourceTI1 := mockTableInfo(t, "CREATE TABLE tb1 (c INT, c2 INT, c3 INT, PRIMARY KEY (c))")
	sourceTI2 := mockTableInfo(t, "CREATE TABLE tb2 (c INT, c2 INT, c3 INT, PRIMARY KEY (c))")
	targetTI := mockTableInfo(t, "CREATE TABLE tb (c INT, c2 INT, c3 INT, PRIMARY KEY (c))")

	change1 := NewRowChange(source1, target, []interface{}{1, 2, 3}, []interface{}{10, 20, 30}, sourceTI1, targetTI, nil)
	change2 := NewRowChange(source2, target, []interface{}{4, 5, 6}, []interface{}{40, 50, 60}, sourceTI2, targetTI, nil)
	sql, args := genUpdate(change1, change2)

	expectedSQL := "UPDATE `db`.`tb` SET " +
		"`c`=CASE WHEN `c` = ? THEN ? WHEN `c` = ? THEN ? END, " +
		"`c2`=CASE WHEN `c` = ? THEN ? WHEN `c` = ? THEN ? END, " +
		"`c3`=CASE WHEN `c` = ? THEN ? WHEN `c` = ? THEN ? END " +
		"WHERE (`c` = ?) OR (`c` = ?)"
	expectedArgs := []interface{}{
		1, 10, 4, 40,
		1, 20, 4, 50,
		1, 30, 4, 60,
		1, 4,
	}

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedArgs, args)
}

func testGenUpdateMultiRowsWithVirtualGeneratedColumn(t *testing.T, genUpdate genSQLFunc) {
	source := &cdcmodel.TableName{Schema: "db", Table: "tb"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb"}

	sourceTI := mockTableInfo(t, "CREATE TABLE tb1 (c INT, c1 int as (c+100) virtual not null, c2 INT, c3 INT, PRIMARY KEY (c))")
	targetTI := mockTableInfo(t, "CREATE TABLE tb (c INT, c1 int as (c+100) virtual not null, c2 INT, c3 INT, PRIMARY KEY (c))")

	change1 := NewRowChange(source, target, []interface{}{1, 101, 2, 3}, []interface{}{10, 110, 20, 30}, sourceTI, targetTI, nil)
	change2 := NewRowChange(source, target, []interface{}{4, 104, 5, 6}, []interface{}{40, 140, 50, 60}, sourceTI, targetTI, nil)
	change3 := NewRowChange(source, target, []interface{}{7, 107, 8, 9}, []interface{}{70, 170, 80, 90}, sourceTI, targetTI, nil)
	sql, args := genUpdate(change1, change2, change3)

	expectedSQL := "UPDATE `db`.`tb` SET " +
		"`c`=CASE WHEN `c` = ? THEN ? WHEN `c` = ? THEN ? WHEN `c` = ? THEN ? END, " +
		"`c2`=CASE WHEN `c` = ? THEN ? WHEN `c` = ? THEN ? WHEN `c` = ? THEN ? END, " +
		"`c3`=CASE WHEN `c` = ? THEN ? WHEN `c` = ? THEN ? WHEN `c` = ? THEN ? END " +
		"WHERE (`c` = ?) OR (`c` = ?) OR (`c` = ?)"
	expectedArgs := []interface{}{
		1, 10, 4, 40, 7, 70,
		1, 20, 4, 50, 7, 80,
		1, 30, 4, 60, 7, 90,
		1, 4, 7,
	}

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedArgs, args)
}

// multiple generated columns test case
func testGenUpdateMultiRowsWithVirtualGeneratedColumns(t *testing.T, genUpdate genSQLFunc) {
	source := &cdcmodel.TableName{Schema: "db", Table: "tb"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb"}

	sourceTI := mockTableInfo(t, `CREATE TABLE tb1 (c0 int as (c4*c4) virtual not null,
	c1 int as (c+100) virtual not null, c2 INT, c3 INT, c4 INT, PRIMARY KEY (c4))`)
	targetTI := mockTableInfo(t, `CREATE TABLE tb (c0 int as (c4*c4) virtual not null,
	c1 int as (c+100) virtual not null, c2 INT, c3 INT, c4 INT, PRIMARY KEY (c4))`)

	change1 := NewRowChange(source, target, []interface{}{1, 101, 2, 3, 1}, []interface{}{100, 110, 20, 30, 10}, sourceTI, targetTI, nil)
	change2 := NewRowChange(source, target, []interface{}{16, 104, 5, 6, 4}, []interface{}{1600, 140, 50, 60, 40}, sourceTI, targetTI, nil)
	change3 := NewRowChange(source, target, []interface{}{49, 107, 8, 9, 7}, []interface{}{4900, 170, 80, 90, 70}, sourceTI, targetTI, nil)
	sql, args := genUpdate(change1, change2, change3)

	expectedSQL := "UPDATE `db`.`tb` SET " +
		"`c2`=CASE WHEN `c4` = ? THEN ? WHEN `c4` = ? THEN ? WHEN `c4` = ? THEN ? END, " +
		"`c3`=CASE WHEN `c4` = ? THEN ? WHEN `c4` = ? THEN ? WHEN `c4` = ? THEN ? END, " +
		"`c4`=CASE WHEN `c4` = ? THEN ? WHEN `c4` = ? THEN ? WHEN `c4` = ? THEN ? END " +
		"WHERE (`c4` = ?) OR (`c4` = ?) OR (`c4` = ?)"
	expectedArgs := []interface{}{
		1, 20, 4, 50, 7, 80,
		1, 30, 4, 60, 7, 90,
		1, 10, 4, 40, 7, 70,
		1, 4, 7,
	}

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedArgs, args)
}

func testGenUpdateMultiRowsWithStoredGeneratedColumn(t *testing.T, genUpdate genSQLFunc) {
	source := &cdcmodel.TableName{Schema: "db", Table: "tb"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb"}

	sourceTI := mockTableInfo(t, "CREATE TABLE tb1 (c INT, c1 int as (c+100) stored, c2 INT, c3 INT, PRIMARY KEY (c1))")
	targetTI := mockTableInfo(t, "CREATE TABLE tb (c INT, c1 int as (c+100) stored, c2 INT, c3 INT, PRIMARY KEY (c1))")

	change1 := NewRowChange(source, target, []interface{}{1, 101, 2, 3}, []interface{}{10, 110, 20, 30}, sourceTI, targetTI, nil)
	change2 := NewRowChange(source, target, []interface{}{4, 104, 5, 6}, []interface{}{40, 140, 50, 60}, sourceTI, targetTI, nil)
	change3 := NewRowChange(source, target, []interface{}{7, 107, 8, 9}, []interface{}{70, 170, 80, 90}, sourceTI, targetTI, nil)
	sql, args := genUpdate(change1, change2, change3)

	expectedSQL := "UPDATE `db`.`tb` SET " +
		"`c`=CASE WHEN `c1` = ? THEN ? WHEN `c1` = ? THEN ? WHEN `c1` = ? THEN ? END, " +
		"`c2`=CASE WHEN `c1` = ? THEN ? WHEN `c1` = ? THEN ? WHEN `c1` = ? THEN ? END, " +
		"`c3`=CASE WHEN `c1` = ? THEN ? WHEN `c1` = ? THEN ? WHEN `c1` = ? THEN ? END " +
		"WHERE (`c1` = ?) OR (`c1` = ?) OR (`c1` = ?)"
	expectedArgs := []interface{}{
		101, 10, 104, 40, 107, 70,
		101, 20, 104, 50, 107, 80,
		101, 30, 104, 60, 107, 90,
		101, 104, 107,
	}

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedArgs, args)
}

func TestGenInsertMultiRows(t *testing.T) {
	t.Parallel()

	source1 := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	source2 := &cdcmodel.TableName{Schema: "db", Table: "tb2"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb"}

	sourceTI1 := mockTableInfo(t, "CREATE TABLE tb1 (gen INT AS (c+1), c INT PRIMARY KEY, c2 INT)")
	sourceTI2 := mockTableInfo(t, "CREATE TABLE tb2 (gen INT AS (c+1), c INT PRIMARY KEY, c2 INT)")
	targetTI := mockTableInfo(t, "CREATE TABLE tb (gen INT AS (c+1), c INT PRIMARY KEY, c2 INT)")

	change1 := NewRowChange(source1, target, nil, []interface{}{2, 1, 2}, sourceTI1, targetTI, nil)
	change2 := NewRowChange(source2, target, nil, []interface{}{4, 3, 4}, sourceTI2, targetTI, nil)

	sql, args := GenInsertSQL(DMLInsert, change1, change2)
	require.Equal(t, "INSERT INTO `db`.`tb` (`c`,`c2`) VALUES (?,?),(?,?)", sql)
	require.Equal(t, []interface{}{1, 2, 3, 4}, args)

	sql, args = GenInsertSQL(DMLReplace, change1, change2)
	require.Equal(t, "REPLACE INTO `db`.`tb` (`c`,`c2`) VALUES (?,?),(?,?)", sql)
	require.Equal(t, []interface{}{1, 2, 3, 4}, args)

	sql, args = GenInsertSQL(DMLInsertOnDuplicateUpdate, change1, change2)
	require.Equal(t, "INSERT INTO `db`.`tb` (`c`,`c2`) VALUES (?,?),(?,?) ON DUPLICATE KEY UPDATE `c`=VALUES(`c`),`c2`=VALUES(`c2`)", sql)
	require.Equal(t, []interface{}{1, 2, 3, 4}, args)
}
