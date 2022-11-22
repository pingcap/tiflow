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
	"encoding/json"
	"testing"

	mm "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/charmap"
)

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

func TestBuildCanalFlatEventBatchEncoder(t *testing.T) {
	t.Parallel()
	config := NewConfig(config.ProtocolCanalJSON)

	builder := &canalFlatEventBatchEncoderBuilder{config: config}
	encoder, ok := builder.Build().(*CanalFlatEventBatchEncoder)
	require.True(t, ok)
	require.False(t, encoder.enableTiDBExtension)

	config.enableTiDBExtension = true
	builder = &canalFlatEventBatchEncoderBuilder{config: config}
	encoder, ok = builder.Build().(*CanalFlatEventBatchEncoder)
	require.True(t, ok)
	require.True(t, encoder.enableTiDBExtension)
}

func TestNewCanalFlatMessage4DML(t *testing.T) {
	t.Parallel()
	encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder()}
	require.NotNil(t, encoder)

	message, err := encoder.newFlatMessageForDML(testCaseInsert)
	require.Nil(t, err)
	flatMessage := &JSONMessage{}
	err = json.Unmarshal(message.Value, flatMessage)
	require.Nil(t, err)
	require.NotNil(t, flatMessage.Data)
	require.Nil(t, flatMessage.Old)
	require.Equal(t, "INSERT", flatMessage.EventType)
	require.Equal(t, convertToCanalTs(testCaseInsert.CommitTs), flatMessage.ExecutionTime)
	require.Equal(t, uint64(0), flatMessage.getCommitTs())
	require.Equal(t, "cdc", flatMessage.Schema)
	require.Equal(t, "person", flatMessage.Table)
	require.False(t, flatMessage.IsDDL)

	// check data is enough
	obtainedDataMap := flatMessage.getData()
	require.NotNil(t, obtainedDataMap)

	for _, item := range testColumnsTable {
		obtainedValue, ok := obtainedDataMap[item.column.Name]
		require.True(t, ok)
		if !item.column.Flag.IsBinary() {
			require.Equal(t, item.expectedEncodedValue, obtainedValue)
			continue
		}

		// for `Column.Value` is nil, which mean's it is nullable, set the value to `""`
		if obtainedValue == nil {
			require.Equal(t, "", item.expectedEncodedValue)
			continue
		}

		if bytes, ok := item.column.Value.([]byte); ok {
			expectedValue, err := charmap.ISO8859_1.NewDecoder().Bytes(bytes)
			require.Nil(t, err)
			require.Equal(t, string(expectedValue), obtainedValue)
			continue
		}

		require.Equal(t, item.expectedEncodedValue, obtainedValue)
	}

	message, err = encoder.newFlatMessageForDML(testCaseUpdate)
	require.Nil(t, err)
	flatMessage = &JSONMessage{}
	err = json.Unmarshal(message.Value, flatMessage)
	require.Nil(t, err)
	require.NotNil(t, flatMessage.Data)
	require.NotNil(t, flatMessage.Old)
	require.Equal(t, "UPDATE", flatMessage.EventType)

	message, err = encoder.newFlatMessageForDML(testCaseDelete)
	require.Nil(t, err)
	flatMessage = &JSONMessage{}
	err = json.Unmarshal(message.Value, flatMessage)
	require.Nil(t, err)
	require.NotNil(t, flatMessage.Data)
	require.Nil(t, flatMessage.Old)
	require.Equal(t, "DELETE", flatMessage.EventType)

	encoder = &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: true}
	require.NotNil(t, encoder)
	message, err = encoder.newFlatMessageForDML(testCaseUpdate)
	require.Nil(t, err)

	withExtension := &canalFlatMessageWithTiDBExtension{}
	err = json.Unmarshal(message.Value, withExtension)
	require.Nil(t, err)

	require.NotNil(t, withExtension.Extensions)
	require.Equal(t, testCaseUpdate.CommitTs, withExtension.Extensions.CommitTs)
}

func TestNewCanalFlatEventBatchDecoder4RowMessage(t *testing.T) {
	t.Parallel()
	expectedDecodedValue := collectExpectedDecodedValue(testColumnsTable)
	for _, encodeEnable := range []bool{false, true} {
		encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: encodeEnable}
		require.NotNil(t, encoder)

		err := encoder.AppendRowChangedEvent(context.Background(), "", testCaseInsert)
		require.Nil(t, err)

		mqMessages := encoder.Build()
		require.Equal(t, 1, len(mqMessages))
		msg := mqMessages[0]

		for _, decodeEnable := range []bool{false, true} {
			decoder := NewCanalFlatEventBatchDecoder(msg.Value, decodeEnable)

			ty, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeRow, ty)

			consumed, err := decoder.NextRowChangedEvent()
			require.Nil(t, err)

			require.Equal(t, testCaseInsert.Table, consumed.Table)
			if encodeEnable && decodeEnable {
				require.Equal(t, testCaseInsert.CommitTs, consumed.CommitTs)
			} else {
				require.Equal(t, uint64(0), consumed.CommitTs)
			}

			for _, col := range consumed.Columns {
				expected, ok := expectedDecodedValue[col.Name]
				require.True(t, ok)
				require.Equal(t, expected, col.Value)

				for _, item := range testCaseInsert.Columns {
					if item.Name == col.Name {
						require.Equal(t, item.Type, col.Type)
					}
				}
			}

			_, hasNext, _ = decoder.HasNext()
			require.False(t, hasNext)

			consumed, err = decoder.NextRowChangedEvent()
			require.NotNil(t, err)
			require.Nil(t, consumed)
		}
	}
}

func TestNewCanalFlatMessageFromDDL(t *testing.T) {
	t.Parallel()
	encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder()}
	require.NotNil(t, encoder)

	message := encoder.newFlatMessageForDDL(testCaseDDL)
	require.NotNil(t, message)

	msg, ok := message.(*JSONMessage)
	require.True(t, ok)
	require.Equal(t, testCaseDDL.CommitTs, msg.tikvTs)
	require.Equal(t, convertToCanalTs(testCaseDDL.CommitTs), msg.ExecutionTime)
	require.True(t, msg.IsDDL)
	require.Equal(t, "cdc", msg.Schema)
	require.Equal(t, "person", msg.Table)
	require.Equal(t, testCaseDDL.Query, msg.Query)
	require.Equal(t, "CREATE", msg.EventType)

	encoder = &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: true}
	require.NotNil(t, encoder)

	message = encoder.newFlatMessageForDDL(testCaseDDL)
	require.NotNil(t, message)

	withExtension, ok := message.(*canalFlatMessageWithTiDBExtension)
	require.True(t, ok)

	require.NotNil(t, withExtension.Extensions)
	require.Equal(t, testCaseDDL.CommitTs, withExtension.Extensions.CommitTs)
}

func TestNewCanalFlatEventBatchDecoder4DDLMessage(t *testing.T) {
	t.Parallel()
	for _, encodeEnable := range []bool{false, true} {
		encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: encodeEnable}
		require.NotNil(t, encoder)

		result, err := encoder.EncodeDDLEvent(testCaseDDL)
		require.Nil(t, err)
		require.NotNil(t, result)

		for _, decodeEnable := range []bool{false, true} {
			decoder := NewCanalFlatEventBatchDecoder(result.Value, decodeEnable)

			ty, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeDDL, ty)

			consumed, err := decoder.NextDDLEvent()
			require.Nil(t, err)

			if encodeEnable && decodeEnable {
				require.Equal(t, testCaseDDL.CommitTs, consumed.CommitTs)
			} else {
				require.Equal(t, uint64(0), consumed.CommitTs)
			}

			require.Equal(t, testCaseDDL.TableInfo, consumed.TableInfo)
			require.Equal(t, testCaseDDL.Query, consumed.Query)

			ty, hasNext, err = decoder.HasNext()
			require.Nil(t, err)
			require.False(t, hasNext)
			require.Equal(t, model.MessageTypeUnknown, ty)

			consumed, err = decoder.NextDDLEvent()
			require.NotNil(t, err)
			require.Nil(t, consumed)
		}
	}
}

func TestBatching(t *testing.T) {
	t.Parallel()
	encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder()}
	require.NotNil(t, encoder)

	updateCase := *testCaseUpdate
	for i := 1; i <= 1000; i++ {
		ts := uint64(i)
		updateCase.CommitTs = ts
		err := encoder.AppendRowChangedEvent(context.Background(), "", &updateCase)
		require.Nil(t, err)

		if i%100 == 0 {
			msgs := encoder.Build()
			require.NotNil(t, msgs)
			require.Len(t, msgs, 100)

			for j := range msgs {
				require.Equal(t, 1, msgs[j].GetRowsCount())

				var msg JSONMessage
				err := json.Unmarshal(msgs[j].Value, &msg)
				require.Nil(t, err)
				require.Equal(t, "UPDATE", msg.EventType)
			}
		}
	}

	require.Len(t, encoder.messageBuf, 0)
}

func TestEncodeCheckpointEvent(t *testing.T) {
	t.Parallel()
	var watermark uint64 = 2333
	for _, enable := range []bool{false, true} {
		encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: enable}
		require.NotNil(t, encoder)

		msg, err := encoder.EncodeCheckpointEvent(watermark)
		require.Nil(t, err)

		if !enable {
			require.Nil(t, msg)
			continue
		}

		require.NotNil(t, msg)
		decoder := NewCanalFlatEventBatchDecoder(msg.Value, enable)

		ty, hasNext, err := decoder.HasNext()
		require.Nil(t, err)
		if enable {
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeResolved, ty)
			consumed, err := decoder.NextResolvedEvent()
			require.Nil(t, err)
			require.Equal(t, watermark, consumed)
		} else {
			require.False(t, hasNext)
			require.Equal(t, model.MessageTypeUnknown, ty)
		}

		ty, hasNext, err = decoder.HasNext()
		require.Nil(t, err)
		require.False(t, hasNext)
		require.Equal(t, model.MessageTypeUnknown, ty)
	}
}

func TestCheckpointEventValueMarshal(t *testing.T) {
	t.Parallel()
	var watermark uint64 = 1024
	encoder := &CanalFlatEventBatchEncoder{
		builder:             NewCanalEntryBuilder(),
		enableTiDBExtension: true,
	}
	require.NotNil(t, encoder)
	msg, err := encoder.EncodeCheckpointEvent(watermark)
	require.Nil(t, err)
	require.NotNil(t, msg)

	// Unmarshal from the data we have encoded.
	flatMsg := canalFlatMessageWithTiDBExtension{
		&JSONMessage{},
		&tidbExtension{},
	}
	err = json.Unmarshal(msg.Value, &flatMsg)
	require.Nil(t, err)
	require.Equal(t, watermark, flatMsg.Extensions.WatermarkTs)
	// Hack the build time.
	// Otherwise, the timing will be inconsistent.
	flatMsg.BuildTime = 1469579899
	rawBytes, err := json.MarshalIndent(flatMsg, "", "  ")
	require.Nil(t, err)

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
	require.Equal(t, expectedJSON, string(rawBytes))
}

func TestDDLEventWithExtensionValueMarshal(t *testing.T) {
	t.Parallel()
	encoder := &CanalFlatEventBatchEncoder{builder: NewCanalEntryBuilder(), enableTiDBExtension: true}
	require.NotNil(t, encoder)

	message := encoder.newFlatMessageForDDL(testCaseDDL)
	require.NotNil(t, message)

	msg, ok := message.(*canalFlatMessageWithTiDBExtension)
	require.True(t, ok)
	// Hack the build time.
	// Otherwise, the timing will be inconsistent.
	msg.BuildTime = 1469579899
	rawBytes, err := json.MarshalIndent(msg, "", "  ")
	require.Nil(t, err)

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
	require.Equal(t, expectedJSON, string(rawBytes))
}
