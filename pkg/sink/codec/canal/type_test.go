// Copyright 2023 PingCAP, Inc.
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

package canal

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestGetMySQLType4IntTypes(t *testing.T) {
	t.Parallel()
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1 (
    	a int primary key,
    	b tinyint,
    	c smallint,
    	d mediumint,
    	e bigint)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()
	mysqlType := getMySQLType(colInfos[0].Ft, tableInfo.ColumnsFlag[colInfos[0].ID], false)
	require.Equal(t, "int", mysqlType)

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "tinyint", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "smallint", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], false)
	require.Equal(t, "mediumint", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], false)
	require.Equal(t, "bigint", mysqlType)

	// mysql type with the default type length
	mysqlType = getMySQLType(colInfos[0].Ft, tableInfo.ColumnsFlag[colInfos[0].ID], true)
	require.Equal(t, "int(11)", mysqlType)

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "tinyint(4)", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "smallint(6)", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], true)
	require.Equal(t, "mediumint(9)", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], true)
	require.Equal(t, "bigint(20)", mysqlType)

	sql = `create table test.t2 (
    	a int unsigned primary key,
    	b tinyint unsigned,
    	c smallint unsigned,
    	d mediumint unsigned,
    	e bigint unsigned)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()
	mysqlType = getMySQLType(colInfos[0].Ft, tableInfo.ColumnsFlag[colInfos[0].ID], false)
	require.Equal(t, "int unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "tinyint unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "smallint unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], false)
	require.Equal(t, "mediumint unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], false)
	require.Equal(t, "bigint unsigned", mysqlType)

	// mysql type with the default type length
	mysqlType = getMySQLType(colInfos[0].Ft, tableInfo.ColumnsFlag[colInfos[0].ID], true)
	require.Equal(t, "int(10) unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "tinyint(3) unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "smallint(5) unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], true)
	require.Equal(t, "mediumint(8) unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], true)
	require.Equal(t, "bigint(20) unsigned", mysqlType)

	sql = `create table test.t3 (
    	a int(10) primary key,
    	b tinyint(3) ,
    	c smallint(5),
    	d mediumint(8),
    	e bigint(19))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()
	mysqlType = getMySQLType(colInfos[0].Ft, tableInfo.ColumnsFlag[colInfos[0].ID], false)
	require.Equal(t, "int", mysqlType)

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "tinyint", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "smallint", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], false)
	require.Equal(t, "mediumint", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], false)
	require.Equal(t, "bigint", mysqlType)

	mysqlType = getMySQLType(colInfos[0].Ft, tableInfo.ColumnsFlag[colInfos[0].ID], true)
	require.Equal(t, "int(10)", mysqlType)

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "tinyint(3)", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "smallint(5)", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], true)
	require.Equal(t, "mediumint(8)", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], true)
	require.Equal(t, "bigint(19)", mysqlType)

	sql = `create table test.t4 (
    	a int(10) unsigned primary key,
    	b tinyint(3) unsigned,
    	c smallint(5) unsigned,
    	d mediumint(8) unsigned,
    	e bigint(19) unsigned)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()
	mysqlType = getMySQLType(colInfos[0].Ft, tableInfo.ColumnsFlag[colInfos[0].ID], false)
	require.Equal(t, "int unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "tinyint unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "smallint unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], false)
	require.Equal(t, "mediumint unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], false)
	require.Equal(t, "bigint unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[0].Ft, tableInfo.ColumnsFlag[colInfos[0].ID], true)
	require.Equal(t, "int(10) unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "tinyint(3) unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "smallint(5) unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], true)
	require.Equal(t, "mediumint(8) unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], true)
	require.Equal(t, "bigint(19) unsigned", mysqlType)

	sql = `create table test.t5 (
    	a int zerofill primary key,
    	b tinyint zerofill,
    	c smallint unsigned zerofill,
    	d mediumint zerofill,
    	e bigint zerofill)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()
	mysqlType = getMySQLType(colInfos[0].Ft, tableInfo.ColumnsFlag[colInfos[0].ID], false)
	require.Equal(t, "int unsigned zerofill", mysqlType)

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "tinyint unsigned zerofill", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "smallint unsigned zerofill", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], false)
	require.Equal(t, "mediumint unsigned zerofill", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], false)
	require.Equal(t, "bigint unsigned zerofill", mysqlType)

	// todo: verify this on the canal before merge this PR
	mysqlType = getMySQLType(colInfos[0].Ft, tableInfo.ColumnsFlag[colInfos[0].ID], true)
	require.Equal(t, "int(10) unsigned zerofill", mysqlType)

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "tinyint(3) unsigned zerofill", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "smallint(5) unsigned zerofill", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], true)
	require.Equal(t, "mediumint(8) unsigned zerofill", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], true)
	require.Equal(t, "bigint(20) unsigned zerofill", mysqlType)

	sql = `create table test.t6(
		a int primary key,
		b bit,
		c bit(3),
		d bool)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()
	mysqlType = getMySQLType(colInfos[0].Ft, tableInfo.ColumnsFlag[colInfos[0].ID], false)
	require.Equal(t, "int", mysqlType)

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "bit", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "bit", mysqlType)

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "bit(1)", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "bit(3)", mysqlType)
}

func TestGetMySQLType4FloatType(t *testing.T) {
	t.Parallel()

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(
		a int primary key,
		b float,
		c double)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	mysqlType := getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "float", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "float", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "double", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "double", mysqlType)

	sql = `create table test.t2(
    	a int primary key,
    	b float(20),
    	c double(26, 3))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "float", mysqlType)
	// todo: this is diffrent to the canal-json, revise this.
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "float", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "double", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "double(26,3)", mysqlType)

	sql = `create table test.t2(
    	a int primary key,
    	b float unsigned,
    	c double unsigned,
    	d float zerofill,
    	e double zerofill)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "float unsigned", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "float unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "double unsigned", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "double unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], false)
	require.Equal(t, "float unsigned zerofill", mysqlType)
	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], true)
	require.Equal(t, "float unsigned zerofill", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], false)
	require.Equal(t, "double unsigned zerofill", mysqlType)
	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], true)
	require.Equal(t, "double unsigned zerofill", mysqlType)
}

func TestGetMySQLType4Decimal(t *testing.T) {
	t.Parallel()

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b decimal, c numeric)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	mysqlType := getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "decimal", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "decimal(10,0)", mysqlType)

	// todo: this is different to the canal-json, it shows `numeric` instead of `decimal`
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "decimal", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "decimal(10,0)", mysqlType)

	sql = `create table test.t2(a int primary key, b decimal(5), c decimal(5, 2))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "decimal", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "decimal(5,0)", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "decimal", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "decimal(5,2)", mysqlType)

	sql = `create table test.t3(a int primary key, b decimal unsigned, c decimal zerofill)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "decimal unsigned", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "decimal(10,0) unsigned", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "decimal unsigned zerofill", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "decimal(10,0) unsigned zerofill", mysqlType)
}

func TestGetMySQLType4TimeTypes(t *testing.T) {
	t.Parallel()

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b time, c time(3))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	mysqlType := getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "time", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "time", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "time", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "time(3)", mysqlType)

	sql = `create table test.t2(a int primary key, b datetime, c datetime(3))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "datetime", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "datetime", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "datetime", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "datetime(3)", mysqlType)

	sql = `create table test.t3(a int primary key, b timestamp, c timestamp(3))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "timestamp", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "timestamp", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "timestamp", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "timestamp(3)", mysqlType)

	sql = `create table test.t4(a int primary key, b year, c year(4))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "year", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "year(4)", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "year", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "year(4)", mysqlType)
}

func TestGetMySQLType4Char(t *testing.T) {
	t.Parallel()

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a int primary key, b char, c char(123))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	mysqlType := getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "char", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "char(1)", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "char", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "char(123)", mysqlType)

	sql = `create table test.t1(a int primary key, b varchar(123))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "varchar", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "varchar(123)", mysqlType)
}

func TestGetMySQLType4TextTypes(t *testing.T) {
	t.Parallel()

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b text, c tinytext, d mediumtext, e longtext)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	mysqlType := getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "text", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "text", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "tinytext", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "tinytext", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], false)
	require.Equal(t, "mediumtext", mysqlType)
	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], true)
	require.Equal(t, "mediumtext", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], false)
	require.Equal(t, "longtext", mysqlType)
	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], true)
	require.Equal(t, "longtext", mysqlType)
}

func TestGetMySQLType4BinaryType(t *testing.T) {
	t.Parallel()

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b binary, c binary(10))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	mysqlType := getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "binary", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "binary(1)", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "binary", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "binary(10)", mysqlType)

	sql = `create table test.t2(a int primary key, b varbinary(23))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "varbinary", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "varbinary(23)", mysqlType)
}

func TestGetMySQLType4BlobType(t *testing.T) {
	t.Parallel()

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b blob, c tinyblob, d mediumblob, e longblob)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	mysqlType := getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "blob", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "blob", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "tinyblob", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "tinyblob", mysqlType)

	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], false)
	require.Equal(t, "mediumblob", mysqlType)
	mysqlType = getMySQLType(colInfos[3].Ft, tableInfo.ColumnsFlag[colInfos[3].ID], true)
	require.Equal(t, "mediumblob", mysqlType)

	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], false)
	require.Equal(t, "longblob", mysqlType)
	mysqlType = getMySQLType(colInfos[4].Ft, tableInfo.ColumnsFlag[colInfos[4].ID], true)
	require.Equal(t, "longblob", mysqlType)
}

func TestGetMySQLType4EnumAndSet(t *testing.T) {
	t.Parallel()

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a int primary key, b enum('a', 'b', 'c'), c set('a', 'b', 'c'))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	mysqlType := getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "enum", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "enum('a','b','c')", mysqlType)

	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], false)
	require.Equal(t, "set", mysqlType)
	mysqlType = getMySQLType(colInfos[2].Ft, tableInfo.ColumnsFlag[colInfos[2].ID], true)
	require.Equal(t, "set('a','b','c')", mysqlType)
}

func TestGetMySQLType4JSON(t *testing.T) {
	t.Parallel()

	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a int primary key, b json)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	mysqlType := getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], false)
	require.Equal(t, "json", mysqlType)
	mysqlType = getMySQLType(colInfos[1].Ft, tableInfo.ColumnsFlag[colInfos[1].ID], true)
	require.Equal(t, "json", mysqlType)
}

//func TestGetMySQLTypeAndJavaSQLType(t *testing.T) {
//	t.Parallel()
//	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
//	canalEntryBuilder := newCanalEntryBuilder(codecConfig)
//	for _, item := range testColumnsTable {
//		obtainedMySQLType := getMySQLType(item.column)
//		require.Equal(t, item.expectedMySQLType, obtainedMySQLType)
//
//		obtainedJavaSQLType, err := getJavaSQLType(item.column, obtainedMySQLType)
//		require.Nil(t, err)
//		require.Equal(t, item.expectedJavaSQLType, obtainedJavaSQLType)
//
//		if !item.column.Flag.IsBinary() {
//			obtainedFinalValue, err := canalEntryBuilder.formatValue(item.column.Value, obtainedJavaSQLType)
//			require.Nil(t, err)
//			require.Equal(t, item.expectedEncodedValue, obtainedFinalValue)
//		}
//	}
//}
