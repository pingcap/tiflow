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
	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"golang.org/x/text/encoding/charmap"
)

type canalFlatSuite struct{}

var _ = check.Suite(&canalFlatSuite{})

var (
	testColumns = collectAllColumns(testColumnsTable)

	testCaseInsert = &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns:    testColumns,
		PreColumns: nil,
	}

	testCaseUpdate = &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns:    testColumns,
		PreColumns: testColumns,
	}

	testCaseDelete = &model.RowChangedEvent{
		CommitTs: 417318403368288260,
		Table: &model.TableName{
			Schema: "cdc",
			Table:  "person",
		},
		Columns:    nil,
		PreColumns: testColumns,
	}
)

var testCaseDDL = &model.DDLEvent{
	CommitTs: 417318403368288260,
	TableInfo: &model.SimpleTableInfo{
		Schema: "cdc", Table: "person",
	},
	Query: "create table person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))",
	Type:  mm.ActionCreateTable,
}

func (s *canalFlatSuite) TestSetParams(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder()}
	c.Assert(encoder, check.NotNil)
	c.Assert(encoder.enableTiDBExtension, check.IsFalse)

	params := make(map[string]string)
	params["enable-tidb-extension"] = "true"
	err := encoder.SetParams(params)
	c.Assert(err, check.IsNil)
	c.Assert(encoder.enableTiDBExtension, check.IsTrue)
}

func (s *canalFlatSuite) TestNewCanalFlatMessage4DML(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder()}
	c.Assert(encoder, check.NotNil)

	message, err := encoder.newFlatMessageForDML(testCaseInsert)
	c.Assert(err, check.IsNil)
	flatMessage, ok := message.(*canalFlatMessage)
	c.Assert(ok, check.IsTrue)
	c.Assert(flatMessage.Data, check.NotNil)
	c.Assert(flatMessage.Old, check.IsNil)
	c.Assert(flatMessage.EventType, check.Equals, "INSERT")
	c.Assert(flatMessage.ExecutionTime, check.Equals, convertToCanalTs(testCaseInsert.CommitTs))
	c.Assert(flatMessage.tikvTs, check.Equals, testCaseInsert.CommitTs)
	c.Assert(flatMessage.Schema, check.Equals, "cdc")
	c.Assert(flatMessage.Table, check.Equals, "person")
	c.Assert(flatMessage.IsDDL, check.IsFalse)

	// check data is enough
	obtainedDataMap := flatMessage.getData()
	c.Assert(obtainedDataMap, check.NotNil)

	for _, item := range testColumnsTable {
		obtainedValue, ok := obtainedDataMap[item.column.Name]
		c.Assert(ok, check.IsTrue)
		if !item.column.Flag.IsBinary() {
			c.Assert(obtainedValue, check.Equals, item.expectedValue)
			continue
		}

		// for `Column.Value` is nil, which mean's it is nullable, set the value to `""`
		if obtainedValue == nil {
			c.Assert(item.expectedValue, check.Equals, "")
			continue
		}

		if bytes, ok := item.column.Value.([]byte); ok {
			expectedValue, err := charmap.ISO8859_1.NewDecoder().Bytes(bytes)
			c.Assert(err, check.IsNil)
			c.Assert(obtainedValue, check.Equals, string(expectedValue))
			continue
		}

		c.Assert(obtainedValue, check.Equals, item.expectedValue)
	}

	message, err = encoder.newFlatMessageForDML(testCaseUpdate)
	c.Assert(err, check.IsNil)
	flatMessage, ok = message.(*canalFlatMessage)
	c.Assert(ok, check.IsTrue)
	c.Assert(flatMessage.Data, check.NotNil)
	c.Assert(flatMessage.Old, check.NotNil)
	c.Assert(flatMessage.EventType, check.Equals, "UPDATE")

	message, err = encoder.newFlatMessageForDML(testCaseDelete)
	c.Assert(err, check.IsNil)
	flatMessage, ok = message.(*canalFlatMessage)
	c.Assert(ok, check.IsTrue)
	c.Assert(flatMessage.Data, check.NotNil)
	c.Assert(flatMessage.Old, check.IsNil)
	c.Assert(flatMessage.EventType, check.Equals, "DELETE")

	encoder = &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: true}
	c.Assert(encoder, check.NotNil)
	message, err = encoder.newFlatMessageForDML(testCaseUpdate)
	c.Assert(err, check.IsNil)

	withExtension, ok := message.(*canalFlatMessageWithTiDBExtension)
	c.Assert(ok, check.IsTrue)

	c.Assert(withExtension.Extensions, check.NotNil)
	c.Assert(withExtension.Extensions.CommitTs, check.Equals, testCaseUpdate.CommitTs)
}

func (s *canalFlatSuite) TestNewCanalFlatEventBatchDecoder4RowMessage(c *check.C) {
	defer testleak.AfterTest(c)()

	expectedDecodedValues := collectDecodeValueByColumns(testColumnsTable)
	for _, encodeEnable := range []bool{false, true} {
		encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: encodeEnable}
		c.Assert(encoder, check.NotNil)

		result, err := encoder.AppendRowChangedEvent(testCaseInsert)
		c.Assert(err, check.IsNil)
		c.Assert(result, check.Equals, EncoderNoOperation)

		result, err = encoder.AppendResolvedEvent(417318403368288260)
		c.Assert(err, check.IsNil)
		c.Assert(result, check.Equals, EncoderNeedAsyncWrite)

		mqMessages := encoder.Build()
		c.Assert(len(mqMessages), check.Equals, 1)

		rawBytes, err := json.Marshal(mqMessages[0])
		c.Assert(err, check.IsNil)

		for _, decodeEnable := range []bool{false, true} {
			decoder := NewCanalFlatEventBatchDecoder(rawBytes, decodeEnable)

			ty, hasNext, err := decoder.HasNext()
			c.Assert(err, check.IsNil)
			c.Assert(hasNext, check.IsTrue)
			c.Assert(ty, check.Equals, model.MqMessageTypeRow)

			consumed, err := decoder.NextRowChangedEvent()
			c.Assert(err, check.IsNil)

			c.Assert(consumed.Table, check.DeepEquals, testCaseInsert.Table)
			if encodeEnable && decodeEnable {
				c.Assert(consumed.CommitTs, check.Equals, testCaseInsert.CommitTs)
			} else {
				c.Assert(consumed.CommitTs, check.Equals, uint64(0))
			}

			for _, col := range consumed.Columns {
				expected, ok := expectedDecodedValues[col.Name]
				c.Assert(ok, check.IsTrue)
				if col.Value == nil {
					c.Assert(expected, check.Equals, "")
				} else {
					c.Assert(col.Value, check.Equals, expected)
				}

				for _, item := range testCaseInsert.Columns {
					if item.Name == col.Name {
						c.Assert(col.Type, check.Equals, item.Type)
					}
				}
			}

			_, hasNext, _ = decoder.HasNext()
			c.Assert(hasNext, check.IsFalse)

			consumed, err = decoder.NextRowChangedEvent()
			c.Assert(err, check.NotNil)
			c.Assert(consumed, check.IsNil)
		}
	}
}

func (s *canalFlatSuite) TestNewCanalFlatMessageFromDDL(c *check.C) {
	defer testleak.AfterTest(c)()
	encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder()}
	c.Assert(encoder, check.NotNil)

	message := encoder.newFlatMessageForDDL(testCaseDDL)
	c.Assert(message, check.NotNil)

	msg, ok := message.(*canalFlatMessage)
	c.Assert(ok, check.IsTrue)
	c.Assert(msg.tikvTs, check.Equals, testCaseDDL.CommitTs)
	c.Assert(msg.ExecutionTime, check.Equals, convertToCanalTs(testCaseDDL.CommitTs))
	c.Assert(msg.IsDDL, check.IsTrue)
	c.Assert(msg.Schema, check.Equals, "cdc")
	c.Assert(msg.Table, check.Equals, "person")
	c.Assert(msg.Query, check.Equals, testCaseDDL.Query)
	c.Assert(msg.EventType, check.Equals, "CREATE")

	encoder = &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: true}
	c.Assert(encoder, check.NotNil)

	message = encoder.newFlatMessageForDDL(testCaseDDL)
	c.Assert(message, check.NotNil)

	withExtension, ok := message.(*canalFlatMessageWithTiDBExtension)
	c.Assert(ok, check.IsTrue)

	c.Assert(withExtension.Extensions, check.NotNil)
	c.Assert(withExtension.Extensions.CommitTs, check.Equals, testCaseDDL.CommitTs)
}

func (s *canalFlatSuite) TestNewCanalFlatEventBatchDecoder4DDLMessage(c *check.C) {
	defer testleak.AfterTest(c)()
	for _, encodeEnable := range []bool{false, true} {
		encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: encodeEnable}
		c.Assert(encoder, check.NotNil)

		result, err := encoder.EncodeDDLEvent(testCaseDDL)
		c.Assert(err, check.IsNil)
		c.Assert(result, check.NotNil)

		rawBytes, err := json.Marshal(result)
		c.Assert(err, check.IsNil)

		for _, decodeEnable := range []bool{false, true} {
			decoder := NewCanalFlatEventBatchDecoder(rawBytes, decodeEnable)

			ty, hasNext, err := decoder.HasNext()
			c.Assert(err, check.IsNil)
			c.Assert(hasNext, check.IsTrue)
			c.Assert(ty, check.Equals, model.MqMessageTypeDDL)

			consumed, err := decoder.NextDDLEvent()
			c.Assert(err, check.IsNil)

			if encodeEnable && decodeEnable {
				c.Assert(consumed.CommitTs, check.Equals, testCaseDDL.CommitTs)
			} else {
				c.Assert(consumed.CommitTs, check.Equals, uint64(0))
			}

			c.Assert(consumed.TableInfo, check.DeepEquals, testCaseDDL.TableInfo)
			c.Assert(consumed.Query, check.Equals, testCaseDDL.Query)

			ty, hasNext, err = decoder.HasNext()
			c.Assert(err, check.IsNil)
			c.Assert(hasNext, check.IsFalse)
			c.Assert(ty, check.Equals, model.MqMessageTypeUnknown)

			consumed, err = decoder.NextDDLEvent()
			c.Assert(err, check.NotNil)
			c.Assert(consumed, check.IsNil)
		}
	}
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
				c.Assert(msgs[j].GetRowsCount(), check.Equals, 1)

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
	var watermark uint64 = 2333
	for _, enable := range []bool{false, true} {
		encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: enable}
		c.Assert(encoder, check.NotNil)

		msg, err := encoder.EncodeCheckpointEvent(watermark)
		c.Assert(err, check.IsNil)
		if enable {
			c.Assert(msg, check.NotNil)
		} else {
			c.Assert(msg, check.IsNil)
		}

		rawBytes, err := json.Marshal(msg)
		c.Assert(err, check.IsNil)
		c.Assert(rawBytes, check.NotNil)

		decoder := NewCanalFlatEventBatchDecoder(rawBytes, enable)

		ty, hasNext, err := decoder.HasNext()
		c.Assert(err, check.IsNil)
		if enable {
			c.Assert(hasNext, check.IsTrue)
			c.Assert(ty, check.Equals, model.MqMessageTypeResolved)
			consumed, err := decoder.NextResolvedEvent()
			c.Assert(err, check.IsNil)
			c.Assert(consumed, check.Equals, watermark)
		} else {
			c.Assert(hasNext, check.IsFalse)
			c.Assert(ty, check.Equals, model.MqMessageTypeUnknown)
		}

		ty, hasNext, err = decoder.HasNext()
		c.Assert(err, check.IsNil)
		c.Assert(hasNext, check.IsFalse)
		c.Assert(ty, check.Equals, model.MqMessageTypeUnknown)
	}
}

func (s *canalFlatSuite) TestCheckpointEventValueMarshal(c *check.C) {
	defer testleak.AfterTest(c)()

	var watermark uint64 = 1024
	encoder := &CanalFlatEventBatchEncoder{
		builder:             NewCanalEntryBuilder(),
		enableTiDBExtension: true,
	}
	c.Assert(encoder, check.NotNil)
	msg, err := encoder.EncodeCheckpointEvent(watermark)
	c.Assert(err, check.IsNil)
	c.Assert(msg, check.NotNil)

	// Unmarshal from the data we have encoded.
	flatMsg := canalFlatMessageWithTiDBExtension{
		&canalFlatMessage{},
		&tidbExtension{},
	}
	err = json.Unmarshal(msg.Value, &flatMsg)
	c.Assert(err, check.IsNil)
	c.Assert(flatMsg.Extensions.WatermarkTs, check.Equals, watermark)
	// Hack the build time.
	// Otherwise, the timing will be inconsistent.
	flatMsg.BuildTime = 1469579899
	rawBytes, err := json.MarshalIndent(flatMsg, "", "  ")
	c.Assert(err, check.IsNil)

	// No commit ts will be output.
	expectedJSON := `{
  "id": 0,
  "database": "",
  "table": "",
  "pkNames": null,
  "isDdl": false,
  "type": "TIDB_WATERMARK",
  "es": 0,
  "ts": 1469579899,
  "sql": "",
  "sqlType": null,
  "mysqlType": null,
  "data": null,
  "old": null,
  "_tidb": {
    "watermarkTs": 1024
  }
}`
	c.Assert(string(rawBytes), check.Equals, expectedJSON)
}

func (s *canalFlatSuite) TestDDLEventWithExtensionValueMarshal(c *check.C) {
	defer testleak.AfterTest(c)()

	encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: true}
	c.Assert(encoder, check.NotNil)

	message := encoder.newFlatMessageForDDL(testCaseDDL)
	c.Assert(message, check.NotNil)

	msg, ok := message.(*canalFlatMessageWithTiDBExtension)
	c.Assert(ok, check.IsTrue)
	// Hack the build time.
	// Otherwise, the timing will be inconsistent.
	msg.BuildTime = 1469579899
	rawBytes, err := json.MarshalIndent(msg, "", "  ")
	c.Assert(err, check.IsNil)

	// No watermark ts will be output.
	expectedJSON := `{
  "id": 0,
  "database": "cdc",
  "table": "person",
  "pkNames": null,
  "isDdl": true,
  "type": "CREATE",
  "es": 1591943372224,
  "ts": 1469579899,
  "sql": "create table person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))",
  "sqlType": null,
  "mysqlType": null,
  "data": null,
  "old": null,
  "_tidb": {
    "commitTs": 417318403368288260
  }
}`
	c.Assert(string(rawBytes), check.Equals, expectedJSON)
}
