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
	"github.com/golang/protobuf/proto"
	"github.com/pingcap/check"
	mm "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"golang.org/x/text/encoding/charmap"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	canal "github.com/pingcap/ticdc/proto/canal"
)

type canalBatchSuite struct {
	rowCases [][]*model.RowChangedEvent
	ddlCases [][]*model.DDLEvent
}

var _ = check.Suite(&canalBatchSuite{
	rowCases: [][]*model.RowChangedEvent{{{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}}, {{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}, {
		CommitTs: 2,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
	}, {
		CommitTs: 3,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "bb"}},
	}, {
		CommitTs: 4,
		Table:    &model.TableName{Schema: "a", Table: "c", TableID: 6, IsPartition: true},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "cc"}},
	}}, {}},
	ddlCases: [][]*model.DDLEvent{{{
		CommitTs: 1,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table a",
		Type:  1,
	}}, {{
		CommitTs: 1,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table a",
		Type:  1,
	}, {
		CommitTs: 2,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table b",
		Type:  2,
	}, {
		CommitTs: 3,
		TableInfo: &model.SimpleTableInfo{
			Schema: "a", Table: "b",
		},
		Query: "create table c",
		Type:  3,
	}}, {}},
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
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeBIGINT))
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
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeSMALLINT))
			c.Assert(col.GetIsKey(), check.IsFalse)
			c.Assert(col.GetIsNull(), check.IsFalse)
			c.Assert(col.GetValue(), check.Equals, "255")
		case "comment":
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeVARCHAR))
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
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeBIGINT))
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
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeBIGINT))
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
			c.Assert(col.GetSqlType(), check.Equals, int32(JavaSQLTypeBIGINT))
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
