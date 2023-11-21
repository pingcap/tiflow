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
	"github.com/pingcap/tiflow/pkg/sink/codec/internal"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"github.com/stretchr/testify/require"
)

func TestGetMySQLType4IntTypes(t *testing.T) {
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

	columnInfo, ok := tableInfo.GetColumnInfo(colInfos[0].ID)
	require.True(t, ok)

	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "int", mysqlType)
	// mysql type with the default type length
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "int(11)", mysqlType)

	flag := tableInfo.ColumnsFlag[colInfos[0].ID]
	javaType, err := getJavaSQLType(int64(2147483647), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyint(4)", mysqlType)

	flag = tableInfo.ColumnsFlag[colInfos[1].ID]
	javaType, err = getJavaSQLType(int64(127), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeTINYINT, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "smallint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "smallint(6)", mysqlType)

	flag = tableInfo.ColumnsFlag[colInfos[2].ID]
	javaType, err = getJavaSQLType(int64(32767), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeSMALLINT, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[3].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[3].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumint(9)", mysqlType)
	javaType, err = getJavaSQLType(int64(8388607), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[4].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[4].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bigint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "bigint(20)", mysqlType)
	javaType, err = getJavaSQLType(int64(9223372036854775807), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIGINT, javaType)

	sql = `create table test.t2 (
    	a int unsigned primary key,
    	b tinyint unsigned,
    	c smallint unsigned,
    	d mediumint unsigned,
    	e bigint unsigned)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[0].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[0].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "int unsigned", mysqlType)
	// mysql type with the default type length
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "int(10) unsigned", mysqlType)

	javaType, err = getJavaSQLType(uint64(2147483647), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)
	javaType, err = getJavaSQLType(uint64(2147483648), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIGINT, javaType)
	javaType, err = getJavaSQLType("0", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)
	javaType, err = getJavaSQLType(nil, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[1].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyint(3) unsigned", mysqlType)

	javaType, err = getJavaSQLType(uint64(127), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeTINYINT, javaType)
	javaType, err = getJavaSQLType(uint64(128), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeSMALLINT, javaType)
	javaType, err = getJavaSQLType("0", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeTINYINT, javaType)
	javaType, err = getJavaSQLType(nil, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeTINYINT, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "smallint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "smallint(5) unsigned", mysqlType)
	javaType, err = getJavaSQLType(uint64(32767), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeSMALLINT, javaType)
	javaType, err = getJavaSQLType(uint64(32768), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)
	javaType, err = getJavaSQLType("0", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeSMALLINT, javaType)
	javaType, err = getJavaSQLType(nil, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeSMALLINT, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[3].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[3].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumint(8) unsigned", mysqlType)
	javaType, err = getJavaSQLType(uint64(8388607), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)
	javaType, err = getJavaSQLType(uint64(8388608), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)
	javaType, err = getJavaSQLType("0", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)
	javaType, err = getJavaSQLType(nil, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[4].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[4].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bigint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "bigint(20) unsigned", mysqlType)
	javaType, err = getJavaSQLType(uint64(9223372036854775807), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIGINT, javaType)
	javaType, err = getJavaSQLType(uint64(9223372036854775808), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeDECIMAL, javaType)
	javaType, err = getJavaSQLType("0", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIGINT, javaType)
	javaType, err = getJavaSQLType(nil, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIGINT, javaType)

	sql = `create table test.t3 (
    	a int(10) primary key,
    	b tinyint(3) ,
    	c smallint(5),
    	d mediumint(8),
    	e bigint(19))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[0].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "int", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "int(10)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyint(3)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "smallint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "smallint(5)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[3].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumint(8)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[4].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bigint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
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

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[0].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "int unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "int(10) unsigned", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyint(3) unsigned", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "smallint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "smallint(5) unsigned", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[3].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumint(8) unsigned", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[4].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bigint unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
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

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[0].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "int unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "int(10) unsigned zerofill", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyint unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyint(3) unsigned zerofill", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "smallint unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "smallint(5) unsigned zerofill", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[3].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumint unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumint(8) unsigned zerofill", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[4].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bigint unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "bigint(20) unsigned zerofill", mysqlType)

	sql = `create table test.t6(
		a int primary key,
		b bit,
		c bit(3),
		d bool)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bit", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "bit(1)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "bit", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "bit(3)", mysqlType)
	javaType, err = getJavaSQLType(uint64(65), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIT, javaType)

	// bool is identical to tinyint in the TiDB.
	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[3].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyint", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyint(1)", mysqlType)
}

func TestGetMySQLType4FloatType(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(
		a int primary key,
		b float,
		c double)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	columnInfo, ok := tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	flag := tableInfo.ColumnsFlag[colInfos[1].ID]
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "float", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "float", mysqlType)
	javaType, err := getJavaSQLType(3.14, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeREAL, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "double", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "double", mysqlType)
	javaType, err = getJavaSQLType(2.71, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeDOUBLE, javaType)

	sql = `create table test.t2(a int primary key, b float(10, 3), c float(10))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "float", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "float(10,3)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "float", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "float", mysqlType)

	sql = `create table test.t3(a int primary key, b double(20, 3))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "double", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "double(20,3)", mysqlType)

	sql = `create table test.t4(
    	a int primary key,
    	b float unsigned,
    	c double unsigned,
    	d float zerofill,
    	e double zerofill)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[1].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "float unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "float unsigned", mysqlType)
	javaType, err = getJavaSQLType(3.14, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeREAL, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "double unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "double unsigned", mysqlType)
	javaType, err = getJavaSQLType(2.71, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeDOUBLE, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[3].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "float unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "float unsigned zerofill", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[4].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "double unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "double unsigned zerofill", mysqlType)
}

func TestGetMySQLType4Decimal(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b decimal, c numeric)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	columnInfo, ok := tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "decimal", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "decimal(10,0)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "decimal", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "decimal(10,0)", mysqlType)

	sql = `create table test.t2(a int primary key, b decimal(5), c decimal(5, 2))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "decimal", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "decimal(5,0)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag := tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "decimal", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "decimal(5,2)", mysqlType)
	javaType, err := getJavaSQLType("2333", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeDECIMAL, javaType)

	sql = `create table test.t3(a int primary key, b decimal unsigned, c decimal zerofill)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "decimal unsigned", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "decimal(10,0) unsigned", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "decimal unsigned zerofill", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "decimal(10,0) unsigned zerofill", mysqlType)
	javaType, err = getJavaSQLType("2333", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeDECIMAL, javaType)
}

func TestGetMySQLType4TimeTypes(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b time, c time(3))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	columnInfo, ok := tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "time", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "time", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag := tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "time", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "time(3)", mysqlType)
	javaType, err := getJavaSQLType("02:20:20", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeTIME)

	sql = `create table test.t2(a int primary key, b datetime, c datetime(3))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "datetime", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "datetime", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "datetime", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "datetime(3)", mysqlType)
	javaType, err = getJavaSQLType("2020-02-20 02:20:20", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeTIMESTAMP)

	sql = `create table test.t3(a int primary key, b timestamp, c timestamp(3))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "timestamp", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "timestamp", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "timestamp", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "timestamp(3)", mysqlType)
	javaType, err = getJavaSQLType("2020-02-20 02:20:20", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeTIMESTAMP)

	sql = `create table test.t4(a int primary key, b date)`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)
	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[1].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "date", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "date", mysqlType)
	javaType, err = getJavaSQLType("2020-02-20", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeDATE)

	sql = `create table test.t5(a int primary key, b year, c year(4))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "year", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "year(4)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "year", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "year(4)", mysqlType)
	javaType, err = getJavaSQLType("2020", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeVARCHAR)
}

func TestGetMySQLType4Char(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a int primary key, b char, c char(123))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	columnInfo, ok := tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "char", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "char(1)", mysqlType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag := tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "char", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "char(123)", mysqlType)
	javaType, err := getJavaSQLType([]uint8("测试char"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeCHAR)

	sql = `create table test.t1(a int primary key, b varchar(123))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[1].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "varchar", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "varchar(123)", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试varchar"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeVARCHAR)
}

func TestGetMySQLType4TextTypes(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b text, c tinytext, d mediumtext, e longtext)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	columnInfo, ok := tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	flag := tableInfo.ColumnsFlag[colInfos[1].ID]
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "text", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "text", mysqlType)
	javaType, err := getJavaSQLType([]uint8("测试text"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeCLOB)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinytext", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinytext", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试tinytext"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeCLOB)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[3].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[3].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumtext", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumtext", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试mediumtext"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeCLOB)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[4].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[4].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "longtext", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "longtext", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试longtext"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeCLOB)
}

func TestGetMySQLType4BinaryType(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b binary, c binary(10))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	columnInfo, ok := tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	flag := tableInfo.ColumnsFlag[colInfos[1].ID]
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "binary", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "binary(1)", mysqlType)
	javaType, err := getJavaSQLType([]uint8("测试binary"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeBLOB)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "binary", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "binary(10)", mysqlType)

	sql = `create table test.t2(a int primary key, b varbinary(23))`
	job = helper.DDL2Job(sql)
	tableInfo = model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos = tableInfo.GetRowColInfos()

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[1].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "varbinary", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "varbinary(23)", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试varbinary"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBLOB, javaType)
}

func TestGetMySQLType4BlobType(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t1(a int primary key, b blob, c tinyblob, d mediumblob, e longblob)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	columnInfo, ok := tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	flag := tableInfo.ColumnsFlag[colInfos[1].ID]
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "blob", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "blob", mysqlType)
	javaType, err := getJavaSQLType([]uint8("测试blob"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeBLOB)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[2].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "tinyblob", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "tinyblob", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试tinyblob"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeBLOB)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[3].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[3].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "mediumblob", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "mediumblob", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试mediumblob"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeBLOB)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[4].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[4].ID]
	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "longblob", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "longblob", mysqlType)
	javaType, err = getJavaSQLType([]uint8("测试longblob"), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, javaType, internal.JavaSQLTypeBLOB)
}

func TestGetMySQLType4EnumAndSet(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a int primary key, b enum('a', 'b', 'c'), c set('a', 'b', 'c'))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	columnInfo, ok := tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	flag := tableInfo.ColumnsFlag[colInfos[1].ID]

	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "enum", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "enum('a','b','c')", mysqlType)

	javaType, err := getJavaSQLType(uint64(1), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeINTEGER, javaType)

	columnInfo, ok = tableInfo.GetColumnInfo(colInfos[2].ID)
	require.True(t, ok)
	flag = tableInfo.ColumnsFlag[colInfos[2].ID]

	mysqlType = utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "set", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "set('a','b','c')", mysqlType)

	javaType, err = getJavaSQLType(uint64(2), columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeBIT, javaType)
}

func TestGetMySQLType4JSON(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a int primary key, b json)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	columnInfo, ok := tableInfo.GetColumnInfo(colInfos[1].ID)
	require.True(t, ok)
	flag := tableInfo.ColumnsFlag[colInfos[1].ID]
	mysqlType := utils.GetMySQLType(columnInfo, false)
	require.Equal(t, "json", mysqlType)
	mysqlType = utils.GetMySQLType(columnInfo, true)
	require.Equal(t, "json", mysqlType)

	javaType, err := getJavaSQLType("{\"key1\": \"value1\"}", columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeVARCHAR, javaType)

	javaType, err = getJavaSQLType(nil, columnInfo.FieldType.GetType(), flag)
	require.NoError(t, err)
	require.Equal(t, internal.JavaSQLTypeVARCHAR, javaType)
}
