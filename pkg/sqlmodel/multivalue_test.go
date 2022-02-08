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

func TestGenDeleteMultiValue(t *testing.T) {
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

func TestGenInsertMultiValue(t *testing.T) {
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
