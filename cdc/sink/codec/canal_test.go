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
	"github.com/golang/protobuf/proto" // nolint:staticcheck
	"github.com/pingcap/check"
	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"golang.org/x/text/encoding/charmap"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	canal "github.com/pingcap/tiflow/proto/canal"
)

type canalBatchSuite struct {
	rowCases [][]*model.RowChangedEvent
	ddlCases [][]*model.DDLEvent
}

var _ = check.Suite(&canalBatchSuite{
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
})

func (s *canalBatchSuite) TestCanalEventBatchEncoder(c *check.C) {
	defer testleak.AfterTest(c)()
	for _, cs := range s.rowCases {
		encoder := NewCanalEventBatchEncoder()
		for _, row := range cs {
			_, err := encoder.AppendRowChangedEvent(row)
			c.Assert(err, check.IsNil)
		}
		size := encoder.Size()
		res := encoder.Build()

		if len(cs) == 0 {
			c.Assert(res, check.IsNil)
			continue
		}

		c.Assert(res, check.HasLen, 1)
		c.Assert(res[0].Key, check.IsNil)
		c.Assert(len(res[0].Value), check.Equals, size)
		c.Assert(res[0].GetRowsCount(), check.Equals, len(cs))

		packet := &canal.Packet{}
		err := proto.Unmarshal(res[0].Value, packet)
		c.Assert(err, check.IsNil)
		c.Assert(packet.GetType(), check.Equals, canal.PacketType_MESSAGES)
		messages := &canal.Messages{}
		err = proto.Unmarshal(packet.GetBody(), messages)
		c.Assert(err, check.IsNil)
		c.Assert(len(messages.GetMessages()), check.Equals, len(cs))
	}

	for _, cs := range s.ddlCases {
		encoder := NewCanalEventBatchEncoder()
		for _, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			c.Assert(err, check.IsNil)
			c.Assert(msg, check.NotNil)
			c.Assert(msg.Key, check.IsNil)

			packet := &canal.Packet{}
			err = proto.Unmarshal(msg.Value, packet)
			c.Assert(err, check.IsNil)
			c.Assert(packet.GetType(), check.Equals, canal.PacketType_MESSAGES)
			messages := &canal.Messages{}
			err = proto.Unmarshal(packet.GetBody(), messages)
			c.Assert(err, check.IsNil)
			c.Assert(len(messages.GetMessages()), check.Equals, 1)
			c.Assert(err, check.IsNil)
		}
	}
}

type canalEntrySuite struct{}

var _ = check.Suite(&canalEntrySuite{})

func (s *canalEntrySuite) TestConvertEntry(c *check.C) {
	defer testleak.AfterTest(c)()
	testInsert(c)
	testUpdate(c)
	testDelete(c)
	testDdl(c)
}

func testInsert(c *check.C) {
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
	c.Assert(err, check.IsNil)
	c.Assert(entry.GetEntryType(), check.Equals, canal.EntryType_ROWDATA)
	header := entry.GetHeader()
	c.Assert(header.GetExecuteTime(), check.Equals, int64(1591943372224))
	c.Assert(header.GetSourceType(), check.Equals, canal.Type_MYSQL)
	c.Assert(header.GetSchemaName(), check.Equals, testCaseInsert.Table.Schema)
	c.Assert(header.GetTableName(), check.Equals, testCaseInsert.Table.Table)
	c.Assert(header.GetEventType(), check.Equals, canal.EventType_INSERT)
	store := entry.GetStoreValue()
	c.Assert(store, check.NotNil)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	c.Assert(err, check.IsNil)
	c.Assert(rc.GetIsDdl(), check.IsFalse)
	rowDatas := rc.GetRowDatas()
	c.Assert(len(rowDatas), check.Equals, 1)

	columns := rowDatas[0].AfterColumns
	c.Assert(len(columns), check.Equals, len(testCaseInsert.Columns))
	for _, col := range columns {
		c.Assert(col.GetUpdated(), check.IsTrue)
		switch col.GetName() {
		case "id":
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeINTEGER))
			c.Assert(col.GetIsKey(), check.IsTrue)
			c.Assert(col.GetIsNull(), check.IsFalse)
			c.Assert(col.GetValue(), check.Equals, "1")
			c.Assert(col.GetMysqlType(), check.Equals, "int")
		case "name":
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeVARCHAR))
			c.Assert(col.GetIsKey(), check.IsFalse)
			c.Assert(col.GetIsNull(), check.IsFalse)
			c.Assert(col.GetValue(), check.Equals, "Bob")
			c.Assert(col.GetMysqlType(), check.Equals, "varchar")
		case "tiny":
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeTINYINT))
			c.Assert(col.GetIsKey(), check.IsFalse)
			c.Assert(col.GetIsNull(), check.IsFalse)
			c.Assert(col.GetValue(), check.Equals, "255")
		case "comment":
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeCLOB))
			c.Assert(col.GetIsKey(), check.IsFalse)
			c.Assert(col.GetIsNull(), check.IsFalse)
			c.Assert(err, check.IsNil)
			c.Assert(col.GetValue(), check.Equals, "测试")
			c.Assert(col.GetMysqlType(), check.Equals, "text")
		case "blob":
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeBLOB))
			c.Assert(col.GetIsKey(), check.IsFalse)
			c.Assert(col.GetIsNull(), check.IsFalse)
			s, err := charmap.ISO8859_1.NewEncoder().String(col.GetValue())
			c.Assert(err, check.IsNil)
			c.Assert(s, check.Equals, "测试blob")
			c.Assert(col.GetMysqlType(), check.Equals, "blob")
		}
	}
}

func testUpdate(c *check.C) {
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
	c.Assert(err, check.IsNil)
	c.Assert(entry.GetEntryType(), check.Equals, canal.EntryType_ROWDATA)

	header := entry.GetHeader()
	c.Assert(header.GetExecuteTime(), check.Equals, int64(1591943372224))
	c.Assert(header.GetSourceType(), check.Equals, canal.Type_MYSQL)
	c.Assert(header.GetSchemaName(), check.Equals, testCaseUpdate.Table.Schema)
	c.Assert(header.GetTableName(), check.Equals, testCaseUpdate.Table.Table)
	c.Assert(header.GetEventType(), check.Equals, canal.EventType_UPDATE)
	store := entry.GetStoreValue()
	c.Assert(store, check.NotNil)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	c.Assert(err, check.IsNil)
	c.Assert(rc.GetIsDdl(), check.IsFalse)
	rowDatas := rc.GetRowDatas()
	c.Assert(len(rowDatas), check.Equals, 1)

	beforeColumns := rowDatas[0].BeforeColumns
	c.Assert(len(beforeColumns), check.Equals, len(testCaseUpdate.PreColumns))
	for _, col := range beforeColumns {
		c.Assert(col.GetUpdated(), check.IsTrue)
		switch col.GetName() {
		case "id":
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeINTEGER))
			c.Assert(col.GetIsKey(), check.IsTrue)
			c.Assert(col.GetIsNull(), check.IsFalse)
			c.Assert(col.GetValue(), check.Equals, "2")
			c.Assert(col.GetMysqlType(), check.Equals, "int")
		case "name":
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeVARCHAR))
			c.Assert(col.GetIsKey(), check.IsFalse)
			c.Assert(col.GetIsNull(), check.IsFalse)
			c.Assert(col.GetValue(), check.Equals, "Nancy")
			c.Assert(col.GetMysqlType(), check.Equals, "varchar")
		}
	}

	afterColumns := rowDatas[0].AfterColumns
	c.Assert(len(afterColumns), check.Equals, len(testCaseUpdate.Columns))
	for _, col := range afterColumns {
		c.Assert(col.GetUpdated(), check.IsTrue)
		switch col.GetName() {
		case "id":
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeINTEGER))
			c.Assert(col.GetIsKey(), check.IsTrue)
			c.Assert(col.GetIsNull(), check.IsFalse)
			c.Assert(col.GetValue(), check.Equals, "1")
			c.Assert(col.GetMysqlType(), check.Equals, "int")
		case "name":
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeVARCHAR))
			c.Assert(col.GetIsKey(), check.IsFalse)
			c.Assert(col.GetIsNull(), check.IsFalse)
			c.Assert(col.GetValue(), check.Equals, "Bob")
			c.Assert(col.GetMysqlType(), check.Equals, "varchar")
		}
	}
}

func testDelete(c *check.C) {
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
	c.Assert(err, check.IsNil)
	c.Assert(entry.GetEntryType(), check.Equals, canal.EntryType_ROWDATA)
	header := entry.GetHeader()
	c.Assert(header.GetSchemaName(), check.Equals, testCaseDelete.Table.Schema)
	c.Assert(header.GetTableName(), check.Equals, testCaseDelete.Table.Table)
	c.Assert(header.GetEventType(), check.Equals, canal.EventType_DELETE)
	store := entry.GetStoreValue()
	c.Assert(store, check.NotNil)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	c.Assert(err, check.IsNil)
	c.Assert(rc.GetIsDdl(), check.IsFalse)
	rowDatas := rc.GetRowDatas()
	c.Assert(len(rowDatas), check.Equals, 1)

	columns := rowDatas[0].BeforeColumns
	c.Assert(len(columns), check.Equals, len(testCaseDelete.PreColumns))
	for _, col := range columns {
		c.Assert(col.GetUpdated(), check.IsFalse)
		switch col.GetName() {
		case "id":
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeINTEGER))
			c.Assert(col.GetIsKey(), check.IsTrue)
			c.Assert(col.GetIsNull(), check.IsFalse)
			c.Assert(col.GetValue(), check.Equals, "1")
			c.Assert(col.GetMysqlType(), check.Equals, "int")
		}
	}
}

func testDdl(c *check.C) {
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
	c.Assert(err, check.IsNil)
	c.Assert(entry.GetEntryType(), check.Equals, canal.EntryType_ROWDATA)
	header := entry.GetHeader()
	c.Assert(header.GetSchemaName(), check.Equals, testCaseDdl.TableInfo.Schema)
	c.Assert(header.GetTableName(), check.Equals, testCaseDdl.TableInfo.Table)
	c.Assert(header.GetEventType(), check.Equals, canal.EventType_CREATE)
	store := entry.GetStoreValue()
	c.Assert(store, check.NotNil)
	rc := &canal.RowChange{}
	err = proto.Unmarshal(store, rc)
	c.Assert(err, check.IsNil)
	c.Assert(rc.GetIsDdl(), check.IsTrue)
	c.Assert(rc.GetDdlSchemaName(), check.Equals, testCaseDdl.TableInfo.Schema)
}

type testColumnTuple struct {
	column              *model.Column
	expectedMySQLType   string
	expectedJavaSQLType JavaSQLType
	// expectedValue is expected by both encoding and decoding
	expectedValue string
}

func collectAllColumns(groups []*testColumnTuple) []*model.Column {
	result := make([]*model.Column, 0, len(groups))
	for _, item := range groups {
		result = append(result, item.column)
	}
	return result
}

func collectDecodeValueByColumns(columns []*testColumnTuple) map[string]string {
	result := make(map[string]string, len(columns))
	for _, item := range columns {
		result[item.column.Name] = item.expectedValue
	}
	return result
}

var testColumnsTable = []*testColumnTuple{
	{&model.Column{Name: "tinyint", Type: mysql.TypeTiny, Value: int64(127)}, "tinyint", JavaSQLTypeTINYINT, "127"}, // TinyInt
	{&model.Column{Name: "tinyint unsigned", Type: mysql.TypeTiny, Value: uint64(127), Flag: model.UnsignedFlag}, "tinyint unsigned", JavaSQLTypeTINYINT, "127"},
	{&model.Column{Name: "tinyint unsigned 2", Type: mysql.TypeTiny, Value: uint64(128), Flag: model.UnsignedFlag}, "tinyint unsigned", JavaSQLTypeSMALLINT, "128"},
	{&model.Column{Name: "tinyint unsigned 3", Type: mysql.TypeTiny, Value: "0", Flag: model.UnsignedFlag}, "tinyint unsigned", JavaSQLTypeTINYINT, "0"},
	{&model.Column{Name: "tinyint unsigned 4", Type: mysql.TypeTiny, Value: nil, Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag}, "tinyint unsigned", JavaSQLTypeTINYINT, ""},

	{&model.Column{Name: "smallint", Type: mysql.TypeShort, Value: int64(32767)}, "smallint", JavaSQLTypeSMALLINT, "32767"},
	{&model.Column{Name: "smallint unsigned", Type: mysql.TypeShort, Value: uint64(32767), Flag: model.UnsignedFlag}, "smallint unsigned", JavaSQLTypeSMALLINT, "32767"},
	{&model.Column{Name: "smallint unsigned 2", Type: mysql.TypeShort, Value: uint64(32768), Flag: model.UnsignedFlag}, "smallint unsigned", JavaSQLTypeINTEGER, "32768"},
	{&model.Column{Name: "smallint unsigned 3", Type: mysql.TypeShort, Value: "0", Flag: model.UnsignedFlag}, "smallint unsigned", JavaSQLTypeSMALLINT, "0"},
	{&model.Column{Name: "smallint unsigned 4", Type: mysql.TypeShort, Value: nil, Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag}, "smallint unsigned", JavaSQLTypeSMALLINT, ""},

	{&model.Column{Name: "mediumint", Type: mysql.TypeInt24, Value: int64(8388607)}, "mediumint", JavaSQLTypeINTEGER, "8388607"},
	{&model.Column{Name: "mediumint unsigned", Type: mysql.TypeInt24, Value: uint64(8388607), Flag: model.UnsignedFlag}, "mediumint unsigned", JavaSQLTypeINTEGER, "8388607"},
	{&model.Column{Name: "mediumint unsigned 2", Type: mysql.TypeInt24, Value: uint64(8388608), Flag: model.UnsignedFlag}, "mediumint unsigned", JavaSQLTypeINTEGER, "8388608"},
	{&model.Column{Name: "mediumint unsigned 3", Type: mysql.TypeInt24, Value: "0", Flag: model.UnsignedFlag}, "mediumint unsigned", JavaSQLTypeINTEGER, "0"},
	{&model.Column{Name: "mediumint unsigned 4", Type: mysql.TypeInt24, Value: nil, Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag}, "mediumint unsigned", JavaSQLTypeINTEGER, ""},

	{&model.Column{Name: "int", Type: mysql.TypeLong, Value: int64(2147483647)}, "int", JavaSQLTypeINTEGER, "2147483647"},
	{&model.Column{Name: "int unsigned", Type: mysql.TypeLong, Value: uint64(2147483647), Flag: model.UnsignedFlag}, "int unsigned", JavaSQLTypeINTEGER, "2147483647"},
	{&model.Column{Name: "int unsigned 2", Type: mysql.TypeLong, Value: uint64(2147483648), Flag: model.UnsignedFlag}, "int unsigned", JavaSQLTypeBIGINT, "2147483648"},
	{&model.Column{Name: "int unsigned 3", Type: mysql.TypeLong, Value: "0", Flag: model.UnsignedFlag}, "int unsigned", JavaSQLTypeINTEGER, "0"},
	{&model.Column{Name: "int unsigned 4", Type: mysql.TypeLong, Value: nil, Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag}, "int unsigned", JavaSQLTypeINTEGER, ""},

	{&model.Column{Name: "bigint", Type: mysql.TypeLonglong, Value: int64(9223372036854775807)}, "bigint", JavaSQLTypeBIGINT, "9223372036854775807"},
	{&model.Column{Name: "bigint unsigned", Type: mysql.TypeLonglong, Value: uint64(9223372036854775807), Flag: model.UnsignedFlag}, "bigint unsigned", JavaSQLTypeBIGINT, "9223372036854775807"},
	{&model.Column{Name: "bigint unsigned 2", Type: mysql.TypeLonglong, Value: uint64(9223372036854775808), Flag: model.UnsignedFlag}, "bigint unsigned", JavaSQLTypeDECIMAL, "9223372036854775808"},
	{&model.Column{Name: "bigint unsigned 3", Type: mysql.TypeLonglong, Value: "0", Flag: model.UnsignedFlag}, "bigint unsigned", JavaSQLTypeBIGINT, "0"},
	{&model.Column{Name: "bigint unsigned 4", Type: mysql.TypeLonglong, Value: nil, Flag: model.BinaryFlag | model.UnsignedFlag | model.NullableFlag}, "bigint unsigned", JavaSQLTypeBIGINT, ""},

	{&model.Column{Name: "float", Type: mysql.TypeFloat, Value: 3.14}, "float", JavaSQLTypeREAL, "3.14"},
	{&model.Column{Name: "double", Type: mysql.TypeDouble, Value: 2.71}, "double", JavaSQLTypeDOUBLE, "2.71"},
	{&model.Column{Name: "decimal", Type: mysql.TypeNewDecimal, Value: "2333"}, "decimal", JavaSQLTypeDECIMAL, "2333"},

	{&model.Column{Name: "float unsigned", Type: mysql.TypeFloat, Value: 3.14, Flag: model.UnsignedFlag}, "float unsigned", JavaSQLTypeREAL, "3.14"},
	{&model.Column{Name: "double unsigned", Type: mysql.TypeDouble, Value: 2.71, Flag: model.UnsignedFlag}, "double unsigned", JavaSQLTypeDOUBLE, "2.71"},
	{&model.Column{Name: "decimal unsigned", Type: mysql.TypeNewDecimal, Value: "2333", Flag: model.UnsignedFlag}, "decimal unsigned", JavaSQLTypeDECIMAL, "2333"},

	// for column value type in `[]uint8` and have `BinaryFlag`, expectedValue is dummy.
	{&model.Column{Name: "varchar", Type: mysql.TypeVarchar, Value: []uint8("测试Varchar")}, "varchar", JavaSQLTypeVARCHAR, "测试Varchar"},
	{&model.Column{Name: "char", Type: mysql.TypeString, Value: []uint8("测试String")}, "char", JavaSQLTypeCHAR, "测试String"},
	{&model.Column{Name: "binary", Type: mysql.TypeString, Value: []uint8("测试Binary"), Flag: model.BinaryFlag}, "binary", JavaSQLTypeBLOB, "测试Binary"},
	{&model.Column{Name: "varbinary", Type: mysql.TypeVarchar, Value: []uint8("测试varbinary"), Flag: model.BinaryFlag}, "varbinary", JavaSQLTypeBLOB, "测试varbinary"},

	{&model.Column{Name: "tinytext", Type: mysql.TypeTinyBlob, Value: []uint8("测试Tinytext")}, "tinytext", JavaSQLTypeCLOB, "测试Tinytext"},
	{&model.Column{Name: "text", Type: mysql.TypeBlob, Value: []uint8("测试text")}, "text", JavaSQLTypeCLOB, "测试text"},
	{&model.Column{Name: "mediumtext", Type: mysql.TypeMediumBlob, Value: []uint8("测试mediumtext")}, "mediumtext", JavaSQLTypeCLOB, "测试mediumtext"},
	{&model.Column{Name: "longtext", Type: mysql.TypeLongBlob, Value: []uint8("测试longtext")}, "longtext", JavaSQLTypeCLOB, "测试longtext"},

	{&model.Column{Name: "tinyblob", Type: mysql.TypeTinyBlob, Value: []uint8("测试tinyblob"), Flag: model.BinaryFlag}, "tinyblob", JavaSQLTypeBLOB, "测试tinyblob"},
	{&model.Column{Name: "blob", Type: mysql.TypeBlob, Value: []uint8("测试blob"), Flag: model.BinaryFlag}, "blob", JavaSQLTypeBLOB, "测试blob"},
	{&model.Column{Name: "mediumblob", Type: mysql.TypeMediumBlob, Value: []uint8("测试mediumblob"), Flag: model.BinaryFlag}, "mediumblob", JavaSQLTypeBLOB, "测试mediumblob"},
	{&model.Column{Name: "longblob", Type: mysql.TypeLongBlob, Value: []uint8("测试longblob"), Flag: model.BinaryFlag}, "longblob", JavaSQLTypeBLOB, "测试longblob"},

	{&model.Column{Name: "date", Type: mysql.TypeDate, Value: "2020-02-20"}, "date", JavaSQLTypeDATE, "2020-02-20"},
	{&model.Column{Name: "datetime", Type: mysql.TypeDatetime, Value: "2020-02-20 02:20:20"}, "datetime", JavaSQLTypeTIMESTAMP, "2020-02-20 02:20:20"},
	{&model.Column{Name: "timestamp", Type: mysql.TypeTimestamp, Value: "2020-02-20 10:20:20"}, "timestamp", JavaSQLTypeTIMESTAMP, "2020-02-20 10:20:20"},
	{&model.Column{Name: "time", Type: mysql.TypeDuration, Value: "02:20:20"}, "time", JavaSQLTypeTIME, "02:20:20"},
	{&model.Column{Name: "year", Type: mysql.TypeYear, Value: "2020", Flag: model.UnsignedFlag}, "year", JavaSQLTypeVARCHAR, "2020"},

	{&model.Column{Name: "enum", Type: mysql.TypeEnum, Value: uint64(1)}, "enum", JavaSQLTypeINTEGER, "1"},
	{&model.Column{Name: "set", Type: mysql.TypeSet, Value: uint64(3)}, "set", JavaSQLTypeBIT, "3"},
	{&model.Column{Name: "bit", Type: mysql.TypeBit, Value: uint64(65), Flag: model.UnsignedFlag | model.BinaryFlag}, "bit", JavaSQLTypeBIT, "65"},
	{&model.Column{Name: "json", Type: mysql.TypeJSON, Value: "{\"key1\": \"value1\"}", Flag: model.BinaryFlag}, "json", JavaSQLTypeVARCHAR, "{\"key1\": \"value1\"}"},
}

func (s *canalEntrySuite) TestGetMySQLTypeAndJavaSQLType(c *check.C) {
	defer testleak.AfterTest(c)()
	for _, item := range testColumnsTable {
		obtainedMySQLType := getMySQLType(item.column)
		c.Assert(obtainedMySQLType, check.Equals, item.expectedMySQLType)

		obtainedJavaSQLType, err := getJavaSQLType(item.column, obtainedMySQLType)
		c.Assert(err, check.IsNil)
		c.Assert(obtainedJavaSQLType, check.Equals, item.expectedJavaSQLType)
	}
}
