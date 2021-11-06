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
	"encoding/json"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"golang.org/x/text/encoding/charmap"
)

type canalFlatSuite struct{}

var _ = check.Suite(&canalFlatSuite{})

func (s *canalFlatSuite) TestNewCanalFlatMessageFromDML(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder()}
	c.Assert(encoder, check.NotNil)
	message, err := encoder.newFlatMessageForDML(testCaseUpdate)
	c.Assert(err, check.IsNil)

	msg, ok := message.(*canalFlatMessage)
	c.Assert(ok, check.IsTrue)
	c.Assert(msg.EventType, check.Equals, "UPDATE")
	c.Assert(msg.ExecutionTime, check.Equals, convertToCanalTs(testCaseUpdate.CommitTs))
	c.Assert(msg.tikvTs, check.Equals, testCaseUpdate.CommitTs)
	c.Assert(msg.Schema, check.Equals, "cdc")
	c.Assert(msg.Table, check.Equals, "person")
	c.Assert(msg.IsDDL, check.IsFalse)
	c.Assert(msg.SQLType, check.DeepEquals, map[string]int32{
		"id":      int32(JavaSQLTypeBIGINT),
		"name":    int32(JavaSQLTypeVARCHAR),
		"tiny":    int32(JavaSQLTypeSMALLINT),
		"comment": int32(JavaSQLTypeVARCHAR),
		"blob":    int32(JavaSQLTypeBLOB),
	})
	c.Assert(msg.MySQLType, check.DeepEquals, map[string]string{
		"id":      "int",
		"name":    "varchar",
		"tiny":    "tinyint",
		"comment": "text",
		"blob":    "blob",
	})
	encodedBytes, err := charmap.ISO8859_1.NewDecoder().Bytes([]byte("测试blob"))
	c.Assert(err, check.IsNil)
	c.Assert(msg.Data, check.DeepEquals, []map[string]interface{}{
		{
			"id":      "1",
			"name":    "Bob",
			"tiny":    "255",
			"comment": "测试",
			"blob":    string(encodedBytes),
		},
	})
	c.Assert(msg.Old, check.DeepEquals, []map[string]interface{}{
		{
			"id":      "1",
			"name":    "Alice",
			"tiny":    "255",
			"comment": "测试",
			"blob":    string(encodedBytes),
		},
	})

	encoder = &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: true}
	c.Assert(encoder, check.NotNil)
	message, err = encoder.newFlatMessageForDML(testCaseUpdate)
	c.Assert(err, check.IsNil)

	withExtension, ok := message.(*canalFlatMessageWithTiDBExtension)
	c.Assert(ok, check.IsTrue)

	c.Assert(withExtension.Extensions, check.NotNil)
	c.Assert(withExtension.Extensions.CommitTs, check.Equals, testCaseUpdate.CommitTs)
}

func (s *canalFlatSuite) TestNewCanalFlatMessageFromDDL(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder()}
	c.Assert(encoder, check.NotNil)

	message := encoder.newFlatMessageForDDL(testCaseDdl)
	c.Assert(message, check.NotNil)

	msg, ok := message.(*canalFlatMessage)
	c.Assert(ok, check.IsTrue)
	c.Assert(msg.tikvTs, check.Equals, testCaseDdl.CommitTs)
	c.Assert(msg.ExecutionTime, check.Equals, convertToCanalTs(testCaseDdl.CommitTs))
	c.Assert(msg.IsDDL, check.IsTrue)
	c.Assert(msg.Schema, check.Equals, "cdc")
	c.Assert(msg.Table, check.Equals, "person")
	c.Assert(msg.Query, check.Equals, testCaseDdl.Query)
	c.Assert(msg.EventType, check.Equals, "CREATE")

	encoder = &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: true}
	c.Assert(encoder, check.NotNil)

	message = encoder.newFlatMessageForDDL(testCaseDdl)
	c.Assert(message, check.NotNil)

	withExtension, ok := message.(*canalFlatMessageWithTiDBExtension)
	c.Assert(ok, check.IsTrue)

	c.Assert(withExtension.Extensions, check.NotNil)
	c.Assert(withExtension.Extensions.CommitTs, check.Equals, testCaseDdl.CommitTs)
}

func (s *canalFlatSuite) TestBatching(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder()}
	c.Assert(encoder, check.NotNil)

	updateCase := *testCaseUpdate
	lastResolved := uint64(0)
	for i := 1; i < 1000; i++ {
		ts := uint64(i)
		updateCase.CommitTs = ts
		result, err := encoder.AppendRowChangedEvent(&updateCase)
		c.Assert(err, check.IsNil)
		c.Assert(result, check.Equals, EncoderNoOperation)

		if i >= 100 && (i%100 == 0 || i == 999) {
			resolvedTs := uint64(i - 50)
			if i == 999 {
				resolvedTs = 999
			}
			result, err := encoder.AppendResolvedEvent(resolvedTs)

			c.Assert(err, check.IsNil)
			c.Assert(result, check.Equals, EncoderNeedAsyncWrite)

			msgs := encoder.Build()
			c.Assert(msgs, check.NotNil)
			c.Assert(msgs, check.HasLen, int(resolvedTs-lastResolved))

			for j := range msgs {
				var msg canalFlatMessage
				err := json.Unmarshal(msgs[j].Value, &msg)
				c.Assert(err, check.IsNil)
				c.Assert(msg.EventType, check.Equals, "UPDATE")
				c.Assert(msg.ExecutionTime, check.Equals, convertToCanalTs(lastResolved+uint64(i)))
			}

			lastResolved = resolvedTs
		}
	}

	c.Assert(encoder.unresolvedBuf, check.HasLen, 0)
	c.Assert(encoder.resolvedBuf, check.HasLen, 0)
}

func (s *canalFlatSuite) TestEncodeCheckpointEvent(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: false}
	c.Assert(encoder, check.NotNil)

	msg, err := encoder.EncodeCheckpointEvent(2333)
	c.Assert(err, check.IsNil)
	c.Assert(msg, check.IsNil)

	encoder.enableTiDBExtension = true
	msg, err = encoder.EncodeCheckpointEvent(2333)
	c.Assert(err, check.IsNil)
	c.Assert(msg, check.NotNil)
}

var testCaseUpdate = &model.RowChangedEvent{
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
	PreColumns: []*model.Column{
		{Name: "id", Type: mysql.TypeLong, Flag: model.HandleKeyFlag, Value: 1},
		{Name: "name", Type: mysql.TypeVarchar, Value: "Alice"},
		{Name: "tiny", Type: mysql.TypeTiny, Value: 255},
		{Name: "comment", Type: mysql.TypeBlob, Value: []byte("测试")},
		{Name: "blob", Type: mysql.TypeBlob, Value: []byte("测试blob"), Flag: model.BinaryFlag},
	},
}

var testCaseDdl = &model.DDLEvent{
	CommitTs: 417318403368288260,
	TableInfo: &model.SimpleTableInfo{
		Schema: "cdc", Table: "person",
	},
	Query: "create table person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))",
	Type:  mm.ActionCreateTable,
}
