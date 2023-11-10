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

package canal

import (
	"testing"

	"github.com/golang/protobuf/proto"
	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec/internal"
	canal "github.com/pingcap/tiflow/proto/canal"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/charmap"
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

func TestConvertEntry(t *testing.T) {
	t.Parallel()
	testInsert(t)
	testUpdate(t)
	testDelete(t)
	testDdl(t)
}

func testInsert(t *testing.T) {
	testCaseInsert := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.PrimaryKeyFlag, Value: 1},
			{Name: "name", Type: mysql.TypeVarchar, Value: "Bob"},
			{Name: "tiny", Type: mysql.TypeTiny, Value: 255},
			{Name: "comment", Type: mysql.TypeBlob, Value: []byte("测试")},
			{Name: "blob", Type: mysql.TypeBlob, Value: []byte("测试blob"), Flag: model.BinaryFlag},
		},
	}

	builder := newCanalEntryBuilder(nil)
	entry, err := builder.fromRowEvent(testCaseInsert, false)
	require.Nil(t, err)
	require.Equal(t, canal.EntryType_ROWDATA, entry.GetEntryType())
	header := entry.GetHeader()
	require.Equal(t, int64(1591943372224), header.GetExecuteTime())
	require.Equal(t, canal.Type_MYSQL, header.GetSourceType())
	require.Equal(t, testCaseInsert.Table.Schema, header.GetSchemaName())
	require.Equal(t, testCaseInsert.Table.Table, header.GetTableName())
	require.Equal(t, canal.EventType_INSERT, header.GetEventType())
	store := entry.GetStoreValue()
	require.NotNil(t, store)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	require.Nil(t, err)
	require.False(t, rc.GetIsDdl())
	rowDatas := rc.GetRowDatas()
	require.Equal(t, 1, len(rowDatas))

	columns := rowDatas[0].AfterColumns
	require.Equal(t, len(testCaseInsert.Columns), len(columns))
	for _, col := range columns {
		require.True(t, col.GetUpdated())
		switch col.GetName() {
		case "id":
			require.Equal(t, int32(internal.JavaSQLTypeINTEGER), col.GetSqlType())
			require.True(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "1", col.GetValue())
			require.Equal(t, "int", col.GetMysqlType())
		case "name":
			require.Equal(t, int32(internal.JavaSQLTypeVARCHAR), col.GetSqlType())
			require.False(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "Bob", col.GetValue())
			require.Equal(t, "varchar", col.GetMysqlType())
		case "tiny":
			require.Equal(t, int32(internal.JavaSQLTypeTINYINT), col.GetSqlType())
			require.False(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "255", col.GetValue())
		case "comment":
			require.Equal(t, int32(internal.JavaSQLTypeCLOB), col.GetSqlType())
			require.False(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Nil(t, err)
			require.Equal(t, "测试", col.GetValue())
			require.Equal(t, "text", col.GetMysqlType())
		case "blob":
			require.Equal(t, int32(internal.JavaSQLTypeBLOB), col.GetSqlType())
			require.False(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			s, err := charmap.ISO8859_1.NewEncoder().String(col.GetValue())
			require.Nil(t, err)
			require.Equal(t, "测试blob", s)
			require.Equal(t, "blob", col.GetMysqlType())
		}
	}
}

func testUpdate(t *testing.T) {
	testCaseUpdate := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.PrimaryKeyFlag, Value: 1},
			{Name: "name", Type: mysql.TypeVarchar, Value: "Bob"},
		},
		PreColumns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.PrimaryKeyFlag, Value: 2},
			{Name: "name", Type: mysql.TypeVarchar, Value: "Nancy"},
		},
	}
	builder := newCanalEntryBuilder(nil)
	entry, err := builder.fromRowEvent(testCaseUpdate, false)
	require.Nil(t, err)
	require.Equal(t, canal.EntryType_ROWDATA, entry.GetEntryType())

	header := entry.GetHeader()
	require.Equal(t, int64(1591943372224), header.GetExecuteTime())
	require.Equal(t, canal.Type_MYSQL, header.GetSourceType())
	require.Equal(t, testCaseUpdate.Table.Schema, header.GetSchemaName())
	require.Equal(t, testCaseUpdate.Table.Table, header.GetTableName())
	require.Equal(t, canal.EventType_UPDATE, header.GetEventType())
	store := entry.GetStoreValue()
	require.NotNil(t, store)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	require.Nil(t, err)
	require.False(t, rc.GetIsDdl())
	rowDatas := rc.GetRowDatas()
	require.Equal(t, 1, len(rowDatas))

	beforeColumns := rowDatas[0].BeforeColumns
	require.Equal(t, len(testCaseUpdate.PreColumns), len(beforeColumns))
	for _, col := range beforeColumns {
		require.True(t, col.GetUpdated())
		switch col.GetName() {
		case "id":
			require.Equal(t, int32(internal.JavaSQLTypeINTEGER), col.GetSqlType())
			require.True(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "2", col.GetValue())
			require.Equal(t, "int", col.GetMysqlType())
		case "name":
			require.Equal(t, int32(internal.JavaSQLTypeVARCHAR), col.GetSqlType())
			require.False(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "Nancy", col.GetValue())
			require.Equal(t, "varchar", col.GetMysqlType())
		}
	}

	afterColumns := rowDatas[0].AfterColumns
	require.Equal(t, len(testCaseUpdate.Columns), len(afterColumns))
	for _, col := range afterColumns {
		require.True(t, col.GetUpdated())
		switch col.GetName() {
		case "id":
			require.Equal(t, int32(internal.JavaSQLTypeINTEGER), col.GetSqlType())
			require.True(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "1", col.GetValue())
			require.Equal(t, "int", col.GetMysqlType())
		case "name":
			require.Equal(t, int32(internal.JavaSQLTypeVARCHAR), col.GetSqlType())
			require.False(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "Bob", col.GetValue())
			require.Equal(t, "varchar", col.GetMysqlType())
		}
	}
}

func testDelete(t *testing.T) {
	testCaseDelete := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		PreColumns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.PrimaryKeyFlag, Value: 1},
		},
	}

	builder := newCanalEntryBuilder(nil)
	entry, err := builder.fromRowEvent(testCaseDelete, false)
	require.Nil(t, err)
	require.Equal(t, canal.EntryType_ROWDATA, entry.GetEntryType())
	header := entry.GetHeader()
	require.Equal(t, testCaseDelete.Table.Schema, header.GetSchemaName())
	require.Equal(t, testCaseDelete.Table.Table, header.GetTableName())
	require.Equal(t, canal.EventType_DELETE, header.GetEventType())
	store := entry.GetStoreValue()
	require.NotNil(t, store)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	require.Nil(t, err)
	require.False(t, rc.GetIsDdl())
	rowDatas := rc.GetRowDatas()
	require.Equal(t, 1, len(rowDatas))

	columns := rowDatas[0].BeforeColumns
	require.Equal(t, len(testCaseDelete.PreColumns), len(columns))
	for _, col := range columns {
		require.False(t, col.GetUpdated())
		switch col.GetName() {
		case "id":
			require.Equal(t, int32(internal.JavaSQLTypeINTEGER), col.GetSqlType())
			require.True(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "1", col.GetValue())
			require.Equal(t, "int", col.GetMysqlType())
		}
	}
}

func testDdl(t *testing.T) {
	testCaseDdl := &model.DDLEvent{
		CommitTs: 417318403368288260,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "cdc", Table: "person",
			},
		},
		Query: "create table person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))",
		Type:  mm.ActionCreateTable,
	}
	builder := newCanalEntryBuilder(nil)
	entry, err := builder.fromDDLEvent(testCaseDdl)
	require.Nil(t, err)
	require.Equal(t, canal.EntryType_ROWDATA, entry.GetEntryType())
	header := entry.GetHeader()
	require.Equal(t, testCaseDdl.TableInfo.TableName.Schema, header.GetSchemaName())
	require.Equal(t, testCaseDdl.TableInfo.TableName.Table, header.GetTableName())
	require.Equal(t, canal.EventType_CREATE, header.GetEventType())
	store := entry.GetStoreValue()
	require.NotNil(t, store)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	require.Nil(t, err)
	require.True(t, rc.GetIsDdl())
	require.Equal(t, testCaseDdl.TableInfo.TableName.Schema, rc.GetDdlSchemaName())
}
