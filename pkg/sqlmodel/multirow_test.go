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

	require.Equal(t, "DELETE FROM `db`.`tb` WHERE (`c`) IN ((?),(?))", sql)
	require.Equal(t, []interface{}{1, 3}, args)
}

func TestGenUpdateMultiRows(t *testing.T) {
	t.Parallel()

	source1 := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	source2 := &cdcmodel.TableName{Schema: "db", Table: "tb2"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb"}

	sourceTI1 := mockTableInfo(t, "CREATE TABLE tb1 (c INT, c2 INT, c3 INT, PRIMARY KEY (c, c2))")
	sourceTI2 := mockTableInfo(t, "CREATE TABLE tb2 (c INT, c2 INT, c3 INT, PRIMARY KEY (c, c2))")
	targetTI := mockTableInfo(t, "CREATE TABLE tb (c INT, c2 INT, c3 INT, PRIMARY KEY (c, c2))")

	change1 := NewRowChange(source1, target, []interface{}{1, 2, 3}, []interface{}{10, 20, 30}, sourceTI1, targetTI, nil)
	change2 := NewRowChange(source2, target, []interface{}{4, 5, 6}, []interface{}{40, 50, 60}, sourceTI2, targetTI, nil)
	sql, args := GenUpdateSQL(change1, change2)

	expectedSQL := "UPDATE `db`.`tb` SET " +
		"`c`=CASE WHEN ROW(`c`,`c2`)=ROW(?,?) THEN ? WHEN ROW(`c`,`c2`)=ROW(?,?) THEN ? END, " +
		"`c2`=CASE WHEN ROW(`c`,`c2`)=ROW(?,?) THEN ? WHEN ROW(`c`,`c2`)=ROW(?,?) THEN ? END, " +
		"`c3`=CASE WHEN ROW(`c`,`c2`)=ROW(?,?) THEN ? WHEN ROW(`c`,`c2`)=ROW(?,?) THEN ? END " +
		"WHERE ROW(`c`,`c2`) IN (ROW(?,?),ROW(?,?))"
	expectedArgs := []interface{}{
		1, 2, 10, 4, 5, 40,
		1, 2, 20, 4, 5, 50,
		1, 2, 30, 4, 5, 60,
		1, 2, 4, 5,
	}

	require.Equal(t, expectedSQL, sql)
	require.Equal(t, expectedArgs, args)
}

func TestGenUpdateMultiRowsOneColPK(t *testing.T) {
	t.Parallel()

	source1 := &cdcmodel.TableName{Schema: "db", Table: "tb1"}
	source2 := &cdcmodel.TableName{Schema: "db", Table: "tb2"}
	target := &cdcmodel.TableName{Schema: "db", Table: "tb"}

	sourceTI1 := mockTableInfo(t, "CREATE TABLE tb1 (c INT, c2 INT, c3 INT, PRIMARY KEY (c))")
	sourceTI2 := mockTableInfo(t, "CREATE TABLE tb2 (c INT, c2 INT, c3 INT, PRIMARY KEY (c))")
	targetTI := mockTableInfo(t, "CREATE TABLE tb (c INT, c2 INT, c3 INT, PRIMARY KEY (c))")

	change1 := NewRowChange(source1, target, []interface{}{1, 2, 3}, []interface{}{10, 20, 30}, sourceTI1, targetTI, nil)
	change2 := NewRowChange(source2, target, []interface{}{4, 5, 6}, []interface{}{40, 50, 60}, sourceTI2, targetTI, nil)
	sql, args := GenUpdateSQL(change1, change2)

	expectedSQL := "UPDATE `db`.`tb` SET " +
		"`c`=CASE WHEN `c`=? THEN ? WHEN `c`=? THEN ? END, " +
		"`c2`=CASE WHEN `c`=? THEN ? WHEN `c`=? THEN ? END, " +
		"`c3`=CASE WHEN `c`=? THEN ? WHEN `c`=? THEN ? END " +
		"WHERE `c` IN (?,?)"
	expectedArgs := []interface{}{
		1, 10, 4, 40,
		1, 20, 4, 50,
		1, 30, 4, 60,
		1, 4,
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
