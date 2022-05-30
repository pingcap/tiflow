// Copyright 2020 PingCAP, Inc.
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

package codec

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto" // nolint:staticcheck
	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/charmap"

	"github.com/pingcap/tiflow/cdc/model"
	canal "github.com/pingcap/tiflow/proto/canal"
)

var defaultCanalBatchTester = &struct {
	rowCases [][]*model.RowChangedEvent
	ddlCases [][]*model.DDLEvent
}{
	rowCases: [][]*model.RowChangedEvent{
		{{
			CommitTs: 1,
			Table:    &model.TableName{Schema: "a", Table: "b"},
			Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
		}},
		{
			{
				CommitTs: 1,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
			},
			{
				CommitTs: 2,
				Table:    &model.TableName{Schema: "a", Table: "b"},
				Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
			},
		},
	},
	ddlCases: [][]*model.DDLEvent{
		{{
			CommitTs: 1,
			TableInfo: &model.SimpleTableInfo{
				Schema: "a", Table: "b",
			},
			Query: "create table a",
			Type:  1,
		}},
		{
			{
				CommitTs: 2,
				TableInfo: &model.SimpleTableInfo{
					Schema: "a", Table: "b",
				},
				Query: "create table b",
				Type:  3,
			},
			{
				CommitTs: 3,
				TableInfo: &model.SimpleTableInfo{
					Schema: "a", Table: "b",
				},
				Query: "create table c",
				Type:  3,
			},
		},
	},
}

func TestCanalEventBatchEncoder(t *testing.T) {
	t.Parallel()
	s := defaultCanalBatchTester
	for _, cs := range s.rowCases {
		encoder := NewCanalEventBatchEncoder()
		for _, row := range cs {
			err := encoder.AppendRowChangedEvent(context.Background(), "", row)
			require.Nil(t, err)
		}
		res := encoder.Build()

		if len(cs) == 0 {
			require.Nil(t, res)
			continue
		}

		require.Len(t, res, 1)
		require.Nil(t, res[0].Key)
		require.Equal(t, len(cs), res[0].GetRowsCount())

		packet := &canal.Packet{}
		err := proto.Unmarshal(res[0].Value, packet)
		require.Nil(t, err)
		require.Equal(t, canal.PacketType_MESSAGES, packet.GetType())
		messages := &canal.Messages{}
		err = proto.Unmarshal(packet.GetBody(), messages)
		require.Nil(t, err)
		require.Equal(t, len(cs), len(messages.GetMessages()))
	}

	for _, cs := range s.ddlCases {
		encoder := NewCanalEventBatchEncoder()
		for _, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			require.Nil(t, err)
			require.NotNil(t, msg)
			require.Nil(t, msg.Key)

			packet := &canal.Packet{}
			err = proto.Unmarshal(msg.Value, packet)
			require.Nil(t, err)
			require.Equal(t, canal.PacketType_MESSAGES, packet.GetType())
			messages := &canal.Messages{}
			err = proto.Unmarshal(packet.GetBody(), messages)
			require.Nil(t, err)
			require.Equal(t, 1, len(messages.GetMessages()))
			require.Nil(t, err)
		}
	}
}

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

	builder := NewCanalEntryBuilder()
	entry, err := builder.FromRowEvent(testCaseInsert)
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
			require.Equal(t, int32(JavaSQLTypeINTEGER), col.GetSqlType())
			require.True(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "1", col.GetValue())
			require.Equal(t, "int", col.GetMysqlType())
		case "name":
			require.Equal(t, int32(JavaSQLTypeVARCHAR), col.GetSqlType())
			require.False(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "Bob", col.GetValue())
			require.Equal(t, "varchar", col.GetMysqlType())
		case "tiny":
			require.Equal(t, int32(JavaSQLTypeTINYINT), col.GetSqlType())
			require.False(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "255", col.GetValue())
		case "comment":
			require.Equal(t, int32(JavaSQLTypeCLOB), col.GetSqlType())
			require.False(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Nil(t, err)
			require.Equal(t, "测试", col.GetValue())
			require.Equal(t, "text", col.GetMysqlType())
		case "blob":
			require.Equal(t, int32(JavaSQLTypeBLOB), col.GetSqlType())
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
	builder := NewCanalEntryBuilder()
	entry, err := builder.FromRowEvent(testCaseUpdate)
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
			require.Equal(t, int32(JavaSQLTypeINTEGER), col.GetSqlType())
			require.True(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "2", col.GetValue())
			require.Equal(t, "int", col.GetMysqlType())
		case "name":
			require.Equal(t, int32(JavaSQLTypeVARCHAR), col.GetSqlType())
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
			require.Equal(t, int32(JavaSQLTypeINTEGER), col.GetSqlType())
			require.True(t, col.GetIsKey())
			require.False(t, col.GetIsNull())
			require.Equal(t, "1", col.GetValue())
			require.Equal(t, "int", col.GetMysqlType())
		case "name":
			require.Equal(t, int32(JavaSQLTypeVARCHAR), col.GetSqlType())
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

	builder := NewCanalEntryBuilder()
	entry, err := builder.FromRowEvent(testCaseDelete)
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
			require.Equal(t, int32(JavaSQLTypeINTEGER), col.GetSqlType())
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
		TableInfo: &model.SimpleTableInfo{
			Schema: "cdc", Table: "person",
		},
		Query: "create table person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))",
		Type:  mm.ActionCreateTable,
	}
	builder := NewCanalEntryBuilder()
	entry, err := builder.FromDdlEvent(testCaseDdl)
	require.Nil(t, err)
	require.Equal(t, canal.EntryType_ROWDATA, entry.GetEntryType())
	header := entry.GetHeader()
	require.Equal(t, testCaseDdl.TableInfo.Schema, header.GetSchemaName())
	require.Equal(t, testCaseDdl.TableInfo.Table, header.GetTableName())
	require.Equal(t, canal.EventType_CREATE, header.GetEventType())
	store := entry.GetStoreValue()
	require.NotNil(t, store)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	require.Nil(t, err)
	require.True(t, rc.GetIsDdl())
	require.Equal(t, testCaseDdl.TableInfo.Schema, rc.GetDdlSchemaName())
}

type testColumnTuple struct {
	column              *model.Column
	expectedMySQLType   string
	expectedJavaSQLType JavaSQLType

	// expectedEncodedValue is expected by encoding
	expectedEncodedValue string

	// expectedDecodedValue is expected by decoding
	expectedDecodedValue interface{}
}

func collectAllColumns(groups []*testColumnTuple) []*model.Column {
	result := make([]*model.Column, 0, len(groups))
	for _, item := range groups {
		result = append(result, item.column)
	}
	return result
}

func collectExpectedDecodedValue(columns []*testColumnTuple) map[string]interface{} {
	result := make(map[string]interface{}, len(columns))
	for _, item := range columns {
		result[item.column.Name] = item.expectedDecodedValue
	}
	return result
}

var testColumnsTable = []*testColumnTuple{
	{
		&model.Column{Name: "tinyint", Type: mysql.TypeTiny, Value: int64(127)},
		"tinyint", JavaSQLTypeTINYINT, "127", "127",
	},

	{
		&model.Column{
			Name: "tinyint unsigned", Type: mysql.TypeTiny, Value: uint64(127),
			Flag: model.UnsignedFlag,
		},
		"tinyint unsigned", JavaSQLTypeTINYINT, "127", "127",
	},

	{
		&model.Column{
			Name: "tinyint unsigned 2", Type: mysql.TypeTiny, Value: uint64(128),
			Flag: model.UnsignedFlag,
		},
		"tinyint unsigned", JavaSQLTypeSMALLINT, "128", "128",
	},

	{
		&model.Column{
			Name: "tinyint unsigned 3", Type: mysql.TypeTiny, Value: "0",
			Flag: model.UnsignedFlag,
		},
		"tinyint unsigned", JavaSQLTypeTINYINT, "0", "0",
	},

	{
		&model.Column{
			Name: "tinyint unsigned 4", Type: mysql.TypeTiny, Value: nil,
			Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
		},
		"tinyint unsigned", JavaSQLTypeTINYINT, "", nil,
	},

	{
		&model.Column{Name: "smallint", Type: mysql.TypeShort, Value: int64(32767)},
		"smallint", JavaSQLTypeSMALLINT, "32767", "32767",
	},
	{
		&model.Column{
			Name: "smallint unsigned", Type: mysql.TypeShort, Value: uint64(32767),
			Flag: model.UnsignedFlag,
		},
		"smallint unsigned", JavaSQLTypeSMALLINT, "32767", "32767",
	},
	{
		&model.Column{
			Name: "smallint unsigned 2", Type: mysql.TypeShort, Value: uint64(32768),
			Flag: model.UnsignedFlag,
		},
		"smallint unsigned", JavaSQLTypeINTEGER, "32768", "32768",
	},
	{
		&model.Column{
			Name: "smallint unsigned 3", Type: mysql.TypeShort, Value: "0",
			Flag: model.UnsignedFlag,
		},
		"smallint unsigned", JavaSQLTypeSMALLINT, "0", "0",
	},
	{
		&model.Column{
			Name: "smallint unsigned 4", Type: mysql.TypeShort, Value: nil,
			Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
		},
		"smallint unsigned", JavaSQLTypeSMALLINT, "", nil,
	},

	{
		&model.Column{Name: "mediumint", Type: mysql.TypeInt24, Value: int64(8388607)},
		"mediumint", JavaSQLTypeINTEGER, "8388607", "8388607",
	},
	{
		&model.Column{
			Name: "mediumint unsigned", Type: mysql.TypeInt24, Value: uint64(8388607),
			Flag: model.UnsignedFlag,
		},
		"mediumint unsigned", JavaSQLTypeINTEGER, "8388607", "8388607",
	},
	{
		&model.Column{
			Name: "mediumint unsigned 2", Type: mysql.TypeInt24, Value: uint64(8388608),
			Flag: model.UnsignedFlag,
		},
		"mediumint unsigned", JavaSQLTypeINTEGER, "8388608", "8388608",
	},
	{
		&model.Column{
			Name: "mediumint unsigned 3", Type: mysql.TypeInt24, Value: "0",
			Flag: model.UnsignedFlag,
		},
		"mediumint unsigned", JavaSQLTypeINTEGER, "0", "0",
	},
	{
		&model.Column{
			Name: "mediumint unsigned 4", Type: mysql.TypeInt24, Value: nil,
			Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
		},
		"mediumint unsigned", JavaSQLTypeINTEGER, "", nil,
	},

	{
		&model.Column{Name: "int", Type: mysql.TypeLong, Value: int64(2147483647)},
		"int", JavaSQLTypeINTEGER, "2147483647", "2147483647",
	},
	{
		&model.Column{
			Name: "int unsigned", Type: mysql.TypeLong, Value: uint64(2147483647),
			Flag: model.UnsignedFlag,
		},
		"int unsigned", JavaSQLTypeINTEGER, "2147483647", "2147483647",
	},
	{
		&model.Column{
			Name: "int unsigned 2", Type: mysql.TypeLong, Value: uint64(2147483648),
			Flag: model.UnsignedFlag,
		},
		"int unsigned", JavaSQLTypeBIGINT, "2147483648", "2147483648",
	},
	{
		&model.Column{
			Name: "int unsigned 3", Type: mysql.TypeLong, Value: "0",
			Flag: model.UnsignedFlag,
		},
		"int unsigned", JavaSQLTypeINTEGER, "0", "0",
	},
	{
		&model.Column{
			Name: "int unsigned 4", Type: mysql.TypeLong, Value: nil,
			Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
		},
		"int unsigned", JavaSQLTypeINTEGER, "", nil,
	},

	{
		&model.Column{Name: "bigint", Type: mysql.TypeLonglong, Value: int64(9223372036854775807)},
		"bigint", JavaSQLTypeBIGINT, "9223372036854775807", "9223372036854775807",
	},
	{
		&model.Column{
			Name: "bigint unsigned", Type: mysql.TypeLonglong, Value: uint64(9223372036854775807),
			Flag: model.UnsignedFlag,
		},
		"bigint unsigned", JavaSQLTypeBIGINT, "9223372036854775807", "9223372036854775807",
	},
	{
		&model.Column{
			Name: "bigint unsigned 2", Type: mysql.TypeLonglong, Value: uint64(9223372036854775808),
			Flag: model.UnsignedFlag,
		},
		"bigint unsigned", JavaSQLTypeDECIMAL, "9223372036854775808", "9223372036854775808",
	},
	{
		&model.Column{
			Name: "bigint unsigned 3", Type: mysql.TypeLonglong, Value: "0",
			Flag: model.UnsignedFlag,
		},
		"bigint unsigned", JavaSQLTypeBIGINT, "0", "0",
	},
	{
		&model.Column{
			Name: "bigint unsigned 4", Type: mysql.TypeLonglong, Value: nil,
			Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag,
		},
		"bigint unsigned", JavaSQLTypeBIGINT, "", nil,
	},

	{
		&model.Column{Name: "float", Type: mysql.TypeFloat, Value: 3.14},
		"float", JavaSQLTypeREAL, "3.14", "3.14",
	},
	{
		&model.Column{Name: "double", Type: mysql.TypeDouble, Value: 2.71},
		"double", JavaSQLTypeDOUBLE, "2.71", "2.71",
	},
	{
		&model.Column{Name: "decimal", Type: mysql.TypeNewDecimal, Value: "2333"},
		"decimal", JavaSQLTypeDECIMAL, "2333", "2333",
	},

	{
		&model.Column{
			Name: "float unsigned", Type: mysql.TypeFloat, Value: 3.14,
			Flag: model.UnsignedFlag,
		},
		"float unsigned", JavaSQLTypeREAL, "3.14", "3.14",
	},
	{
		&model.Column{
			Name: "double unsigned", Type: mysql.TypeDouble, Value: 2.71,
			Flag: model.UnsignedFlag,
		},
		"double unsigned", JavaSQLTypeDOUBLE, "2.71", "2.71",
	},
	{
		&model.Column{
			Name: "decimal unsigned", Type: mysql.TypeNewDecimal, Value: "2333",
			Flag: model.UnsignedFlag,
		},
		"decimal unsigned", JavaSQLTypeDECIMAL, "2333", "2333",
	},

	// for column value type in `[]uint8` and have `BinaryFlag`, expectedEncodedValue is dummy.
	{
		&model.Column{Name: "varchar", Type: mysql.TypeVarchar, Value: []uint8("测试Varchar")},
		"varchar", JavaSQLTypeVARCHAR, "测试Varchar", "测试Varchar",
	},
	{
		&model.Column{Name: "char", Type: mysql.TypeString, Value: []uint8("测试String")},
		"char", JavaSQLTypeCHAR, "测试String", "测试String",
	},
	{
		&model.Column{
			Name: "binary", Type: mysql.TypeString, Value: []uint8("测试Binary"),
			Flag: model.BinaryFlag,
		},
		"binary", JavaSQLTypeBLOB, "测试Binary", "测试Binary",
	},
	{
		&model.Column{
			Name: "varbinary", Type: mysql.TypeVarchar, Value: []uint8("测试varbinary"),
			Flag: model.BinaryFlag,
		},
		"varbinary", JavaSQLTypeBLOB, "测试varbinary", "测试varbinary",
	},

	{
		&model.Column{Name: "tinytext", Type: mysql.TypeTinyBlob, Value: []uint8("测试Tinytext")},
		"tinytext", JavaSQLTypeCLOB, "测试Tinytext", "测试Tinytext",
	},
	{
		&model.Column{Name: "text", Type: mysql.TypeBlob, Value: []uint8("测试text")},
		"text", JavaSQLTypeCLOB, "测试text", "测试text",
	},
	{
		&model.Column{
			Name: "mediumtext", Type: mysql.TypeMediumBlob,
			Value: []uint8("测试mediumtext"),
		},
		"mediumtext", JavaSQLTypeCLOB, "测试mediumtext", "测试mediumtext",
	},
	{
		&model.Column{Name: "longtext", Type: mysql.TypeLongBlob, Value: []uint8("测试longtext")},
		"longtext", JavaSQLTypeCLOB, "测试longtext", "测试longtext",
	},

	{
		&model.Column{
			Name: "tinyblob", Type: mysql.TypeTinyBlob, Value: []uint8("测试tinyblob"),
			Flag: model.BinaryFlag,
		},
		"tinyblob", JavaSQLTypeBLOB, "测试tinyblob", "测试tinyblob",
	},
	{
		&model.Column{
			Name: "blob", Type: mysql.TypeBlob, Value: []uint8("测试blob"),
			Flag: model.BinaryFlag,
		},
		"blob", JavaSQLTypeBLOB, "测试blob", "测试blob",
	},
	{
		&model.Column{
			Name: "mediumblob", Type: mysql.TypeMediumBlob, Value: []uint8("测试mediumblob"),
			Flag: model.BinaryFlag,
		},
		"mediumblob", JavaSQLTypeBLOB, "测试mediumblob", "测试mediumblob",
	},
	{
		&model.Column{
			Name: "longblob", Type: mysql.TypeLongBlob, Value: []uint8("测试longblob"),
			Flag: model.BinaryFlag,
		},
		"longblob", JavaSQLTypeBLOB, "测试longblob", "测试longblob",
	},

	{
		&model.Column{Name: "date", Type: mysql.TypeDate, Value: "2020-02-20"},
		"date", JavaSQLTypeDATE, "2020-02-20", "2020-02-20",
	},
	{
		&model.Column{Name: "datetime", Type: mysql.TypeDatetime, Value: "2020-02-20 02:20:20"},
		"datetime", JavaSQLTypeTIMESTAMP, "2020-02-20 02:20:20", "2020-02-20 02:20:20",
	},
	{
		&model.Column{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2020-02-20 10:20:20"},
		"timestamp", JavaSQLTypeTIMESTAMP, "2020-02-20 10:20:20", "2020-02-20 10:20:20",
	},
	{
		&model.Column{Name: "time", Type: mysql.TypeDuration, Value: "02:20:20"},
		"time", JavaSQLTypeTIME, "02:20:20", "02:20:20",
	},
	{
		&model.Column{Name: "year", Type: mysql.TypeYear, Value: "2020", Flag: model.UnsignedFlag},
		"year", JavaSQLTypeVARCHAR, "2020", "2020",
	},

	{
		&model.Column{Name: "enum", Type: mysql.TypeEnum, Value: uint64(1)},
		"enum", JavaSQLTypeINTEGER, "1", "1",
	},
	{
		&model.Column{Name: "set", Type: mysql.TypeSet, Value: uint64(3)},
		"set", JavaSQLTypeBIT, "3", uint64(3),
	},
	{
		&model.Column{
			Name: "bit", Type: mysql.TypeBit, Value: uint64(65),
			Flag: model.UnsignedFlag | model.BinaryFlag,
		},
		"bit", JavaSQLTypeBIT, "65", uint64(65),
	},
	{
		&model.Column{
			Name: "json", Type: mysql.TypeJSON, Value: "{\"key1\": \"value1\"}",
			Flag: model.BinaryFlag,
		},
		"json", JavaSQLTypeVARCHAR, "{\"key1\": \"value1\"}", "{\"key1\": \"value1\"}",
	},
}

func TestGetMySQLTypeAndJavaSQLType(t *testing.T) {
	t.Parallel()
	canalEntryBuilder := NewCanalEntryBuilder()
	for _, item := range testColumnsTable {
		obtainedMySQLType := getMySQLType(item.column)
		require.Equal(t, item.expectedMySQLType, obtainedMySQLType)

		obtainedJavaSQLType, err := getJavaSQLType(item.column, obtainedMySQLType)
		require.Nil(t, err)
		require.Equal(t, item.expectedJavaSQLType, obtainedJavaSQLType)

		if !item.column.Flag.IsBinary() {
			obtainedFinalValue, err := canalEntryBuilder.formatValue(item.column.Value, obtainedJavaSQLType)
			require.Nil(t, err)
			require.Equal(t, item.expectedEncodedValue, obtainedFinalValue)
		}
	}
}
