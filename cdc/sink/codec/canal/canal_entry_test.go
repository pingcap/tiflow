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
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sink/codec/internal"
	"github.com/pingcap/tiflow/pkg/config"
	canal "github.com/pingcap/tiflow/proto/canal"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/charmap"
)

func TestInsert(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(
		id int primary key,
		name varchar(32),
		tiny tinyint unsigned,
		comment text,
		bb blob)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	event := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		TableInfo: tableInfo,
		Columns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.PrimaryKeyFlag, Value: 1},
			{Name: "name", Type: mysql.TypeVarchar, Value: "Bob"},
			{Name: "tiny", Type: mysql.TypeTiny, Value: 255},
			{Name: "comment", Type: mysql.TypeBlob, Value: []byte("测试")},
			{Name: "bb", Type: mysql.TypeBlob, Value: []byte("测试blob"), Flag: model.BinaryFlag},
		},
		ColInfos: colInfos,
	}

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	builder := newCanalEntryBuilder(codecConfig)
	entry, err := builder.fromRowEvent(event, false)
	require.NoError(t, err)
	require.Equal(t, canal.EntryType_ROWDATA, entry.GetEntryType())
	header := entry.GetHeader()
	require.Equal(t, int64(1591943372224), header.GetExecuteTime())
	require.Equal(t, canal.Type_MYSQL, header.GetSourceType())
	require.Equal(t, event.Table.Schema, header.GetSchemaName())
	require.Equal(t, event.Table.Table, header.GetTableName())
	require.Equal(t, canal.EventType_INSERT, header.GetEventType())
	store := entry.GetStoreValue()
	require.NotNil(t, store)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	require.NoError(t, err)
	require.False(t, rc.GetIsDdl())
	rowDatas := rc.GetRowDatas()
	require.Equal(t, 1, len(rowDatas))

	columns := rowDatas[0].AfterColumns
	require.Equal(t, len(event.Columns), len(columns))
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
			require.NoError(t, err)
			require.Equal(t, "测试", col.GetValue())
			require.Equal(t, "text", col.GetMysqlType())
		case "bb":
			require.Equal(t, int32(internal.JavaSQLTypeBLOB), col.GetSqlType())
			require.False(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			s, err := charmap.ISO8859_1.NewEncoder().String(col.GetValue())
			require.NoError(t, err)
			require.Equal(t, "测试blob", s)
			require.Equal(t, "blob", col.GetMysqlType())
		}
	}
}

func TestUpdate(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(id int primary key, name varchar(32))`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	event := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "test",
			Table:  "t",
		},
		TableInfo: tableInfo,
		Columns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.PrimaryKeyFlag, Value: 1},
			{Name: "name", Type: mysql.TypeVarchar, Value: "Bob"},
		},
		PreColumns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.PrimaryKeyFlag, Value: 2},
			{Name: "name", Type: mysql.TypeVarchar, Value: "Nancy"},
		},
		ColInfos: colInfos,
	}
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	builder := newCanalEntryBuilder(codecConfig)
	entry, err := builder.fromRowEvent(event, false)
	require.NoError(t, err)
	require.Equal(t, canal.EntryType_ROWDATA, entry.GetEntryType())

	header := entry.GetHeader()
	require.Equal(t, int64(1591943372224), header.GetExecuteTime())
	require.Equal(t, canal.Type_MYSQL, header.GetSourceType())
	require.Equal(t, event.Table.Schema, header.GetSchemaName())
	require.Equal(t, event.Table.Table, header.GetTableName())
	require.Equal(t, canal.EventType_UPDATE, header.GetEventType())
	store := entry.GetStoreValue()
	require.NotNil(t, store)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	require.NoError(t, err)
	require.False(t, rc.GetIsDdl())
	rowDatas := rc.GetRowDatas()
	require.Equal(t, 1, len(rowDatas))

	beforeColumns := rowDatas[0].BeforeColumns
	require.Equal(t, len(event.PreColumns), len(beforeColumns))
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
	require.Equal(t, len(event.Columns), len(afterColumns))
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

func TestDelete(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(id int primary key)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	event := &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "test",
			Table:  "t",
		},
		TableInfo: tableInfo,
		PreColumns: []*model.Column{
			{Name: "id", Type: mysql.TypeLong, Flag: model.PrimaryKeyFlag, Value: 1},
		},
		ColInfos: colInfos,
	}
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	builder := newCanalEntryBuilder(codecConfig)
	entry, err := builder.fromRowEvent(event, false)
	require.NoError(t, err)
	require.Equal(t, canal.EntryType_ROWDATA, entry.GetEntryType())
	header := entry.GetHeader()
	require.Equal(t, event.Table.Schema, header.GetSchemaName())
	require.Equal(t, event.Table.Table, header.GetTableName())
	require.Equal(t, canal.EventType_DELETE, header.GetEventType())
	store := entry.GetStoreValue()
	require.NotNil(t, store)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	require.NoError(t, err)
	require.False(t, rc.GetIsDdl())
	rowDatas := rc.GetRowDatas()
	require.Equal(t, 1, len(rowDatas))

	columns := rowDatas[0].BeforeColumns
	require.Equal(t, len(event.PreColumns), len(columns))
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

func TestDDL(t *testing.T) {
	event := &model.DDLEvent{
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
	entry, err := builder.fromDDLEvent(event)
	require.NoError(t, err)
	require.Equal(t, canal.EntryType_ROWDATA, entry.GetEntryType())
	header := entry.GetHeader()
	require.Equal(t, event.TableInfo.TableName.Schema, header.GetSchemaName())
	require.Equal(t, event.TableInfo.TableName.Table, header.GetTableName())
	require.Equal(t, canal.EventType_CREATE, header.GetEventType())
	store := entry.GetStoreValue()
	require.NotNil(t, store)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	require.NoError(t, err)
	require.True(t, rc.GetIsDdl())
	require.Equal(t, event.TableInfo.TableName.Schema, rc.GetDdlSchemaName())
}
