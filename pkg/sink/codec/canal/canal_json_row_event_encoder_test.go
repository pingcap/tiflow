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

//go:build intest
// +build intest

package canal

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/charmap"
)

func TestBuildCanalJSONRowEventEncoder(t *testing.T) {
	t.Parallel()
	cfg := common.NewConfig(config.ProtocolCanalJSON)

	builder := NewJSONRowEventEncoderBuilder(cfg)
	encoder, ok := builder.Build().(*JSONRowEventEncoder)
	require.True(t, ok)
	require.NotNil(t, encoder.config)
}

func TestNewCanalJSONMessage4DML(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	builder := NewJSONRowEventEncoderBuilder(codecConfig)

	encoder, ok := builder.Build().(*JSONRowEventEncoder)
	require.True(t, ok)

	insertEvent, updateEvent, deleteEvent := newLargeEvent4Test(t)
	data, err := newJSONMessageForDML(insertEvent, codecConfig, encoder.builder, false)
	require.NoError(t, err)

	var msg canalJSONMessageInterface = &JSONMessage{}
	err = json.Unmarshal(data, msg)
	require.NoError(t, err)

	jsonMsg, ok := msg.(*JSONMessage)
	require.True(t, ok)
	require.NotNil(t, jsonMsg.Data)
	require.Nil(t, jsonMsg.Old)
	require.Equal(t, "INSERT", jsonMsg.EventType)
	require.Equal(t, convertToCanalTs(insertEvent.CommitTs), jsonMsg.ExecutionTime)
	require.Equal(t, "test", jsonMsg.Schema)
	require.Equal(t, "t", jsonMsg.Table)
	require.False(t, jsonMsg.IsDDL)

	for _, col := range insertEvent.Columns {
		require.Contains(t, jsonMsg.Data[0], col.Name)
		require.Contains(t, jsonMsg.SQLType, col.Name)
		require.Contains(t, jsonMsg.MySQLType, col.Name)
	}

	// check data is enough
	obtainedDataMap := jsonMsg.getData()
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
			require.NoError(t, err)
			require.Equal(t, string(expectedValue), obtainedValue)
			continue
		}

		require.Equal(t, item.expectedEncodedValue, obtainedValue)
	}

	data, err = newJSONMessageForDML(updateEvent, codecConfig, encoder.builder, false)
	require.NoError(t, err)

	jsonMsg = &JSONMessage{}
	err = json.Unmarshal(data, jsonMsg)
	require.NoError(t, err)

	require.NotNil(t, jsonMsg.Data)
	require.NotNil(t, jsonMsg.Old)
	require.Equal(t, "UPDATE", jsonMsg.EventType)

	for _, col := range updateEvent.Columns {
		require.Contains(t, jsonMsg.Data[0], col.Name)
		require.Contains(t, jsonMsg.SQLType, col.Name)
		require.Contains(t, jsonMsg.MySQLType, col.Name)
	}
	for _, col := range updateEvent.PreColumns {
		require.Contains(t, jsonMsg.Old[0], col.Name)
	}

	data, err = newJSONMessageForDML(deleteEvent, codecConfig, encoder.builder, false)
	require.NoError(t, err)

	jsonMsg = &JSONMessage{}
	err = json.Unmarshal(data, jsonMsg)
	require.NoError(t, err)
	require.NotNil(t, jsonMsg.Data)
	require.Nil(t, jsonMsg.Old)
	require.Equal(t, "DELETE", jsonMsg.EventType)

	for _, col := range deleteEvent.PreColumns {
		require.Contains(t, jsonMsg.Data[0], col.Name)
	}

	codecConfig = &common.Config{DeleteOnlyHandleKeyColumns: true}
	data, err = newJSONMessageForDML(deleteEvent, codecConfig, encoder.builder, false)
	require.NoError(t, err)

	jsonMsg = &JSONMessage{}
	err = json.Unmarshal(data, jsonMsg)
	require.NoError(t, err)
	require.NotNil(t, jsonMsg.Data)
	require.Nil(t, jsonMsg.Old)

	for _, col := range deleteEvent.PreColumns {
		if col.Flag.IsHandleKey() {
			require.Contains(t, jsonMsg.Data[0], col.Name)
			require.Contains(t, jsonMsg.SQLType, col.Name)
			require.Contains(t, jsonMsg.MySQLType, col.Name)
		} else {
			require.NotContains(t, jsonMsg.Data[0], col.Name)
			require.NotContains(t, jsonMsg.SQLType, col.Name)
			require.NotContains(t, jsonMsg.MySQLType, col.Name)
		}
	}

	codecConfig = common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.OnlyOutputUpdatedColumns = true

	builder = NewJSONRowEventEncoderBuilder(codecConfig)
	encoder, ok = builder.Build().(*JSONRowEventEncoder)
	require.True(t, ok)
	data, err = newJSONMessageForDML(updateEvent, codecConfig, encoder.builder, false)
	require.NoError(t, err)

	withExtension := &canalJSONMessageWithTiDBExtension{}
	err = json.Unmarshal(data, withExtension)
	require.NoError(t, err)

	require.NotNil(t, withExtension.Extensions)
	require.Equal(t, updateEvent.CommitTs, withExtension.Extensions.CommitTs)

	encoder, ok = builder.Build().(*JSONRowEventEncoder)
	require.True(t, ok)
	data, err = newJSONMessageForDML(updateEvent, codecConfig, encoder.builder, false)
	require.NoError(t, err)

	withExtension = &canalJSONMessageWithTiDBExtension{}
	err = json.Unmarshal(data, withExtension)
	require.NoError(t, err)
	require.Equal(t, 0, len(withExtension.JSONMessage.Old[0]))

	require.NotNil(t, withExtension.Extensions)
	require.Equal(t, updateEvent.CommitTs, withExtension.Extensions.CommitTs)
}

func TestNewCanalJSONMessageHandleKeyOnly4LargeMessage(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
	codecConfig.MaxMessageBytes = 500

	builder := NewJSONRowEventEncoderBuilder(codecConfig)
	encoder := builder.Build()

	insertEvent, _, _ := newLargeEvent4Test(t)
	err := encoder.AppendRowChangedEvent(context.Background(), "", insertEvent, func() {})
	require.NoError(t, err)

	message := encoder.Build()[0]

	decoder, err := NewBatchDecoder(context.Background(), codecConfig, &sql.DB{})
	require.NoError(t, err)

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, ok, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, messageType, model.MessageTypeRow)

	handleKeyOnlyMessage := decoder.(*batchDecoder).msg.(*canalJSONMessageWithTiDBExtension)
	require.True(t, handleKeyOnlyMessage.Extensions.OnlyHandleKey)

	for _, col := range insertEvent.Columns {
		if col.Flag.IsHandleKey() {
			require.Contains(t, handleKeyOnlyMessage.Data[0], col.Name)
			require.Contains(t, handleKeyOnlyMessage.SQLType, col.Name)
			require.Contains(t, handleKeyOnlyMessage.MySQLType, col.Name)
		} else {
			require.NotContains(t, handleKeyOnlyMessage.Data[0], col.Name)
			require.NotContains(t, handleKeyOnlyMessage.SQLType, col.Name)
			require.NotContains(t, handleKeyOnlyMessage.MySQLType, col.Name)
		}
	}
}

func TestNewCanalJSONMessageFromDDL(t *testing.T) {
	t.Parallel()

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	builder := NewJSONRowEventEncoderBuilder(codecConfig)
	encoder := builder.Build().(*JSONRowEventEncoder)

	message := encoder.newJSONMessageForDDL(testCaseDDL)
	require.NotNil(t, message)

	msg, ok := message.(*JSONMessage)
	require.True(t, ok)
	require.Equal(t, convertToCanalTs(testCaseDDL.CommitTs), msg.ExecutionTime)
	require.True(t, msg.IsDDL)
	require.Equal(t, "cdc", msg.Schema)
	require.Equal(t, "person", msg.Table)
	require.Equal(t, testCaseDDL.Query, msg.Query)
	require.Equal(t, "CREATE", msg.EventType)

	codecConfig.EnableTiDBExtension = true
	builder = NewJSONRowEventEncoderBuilder(codecConfig)

	encoder = builder.Build().(*JSONRowEventEncoder)
	message = encoder.newJSONMessageForDDL(testCaseDDL)
	require.NotNil(t, message)

	withExtension, ok := message.(*canalJSONMessageWithTiDBExtension)
	require.True(t, ok)

	require.NotNil(t, withExtension.Extensions)
	require.Equal(t, testCaseDDL.CommitTs, withExtension.Extensions.CommitTs)
}

func TestBatching(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	builder := NewJSONRowEventEncoderBuilder(codecConfig)
	encoder := builder.Build()
	require.NotNil(t, encoder)

	_, updateEvent, _ := newLargeEvent4Test(t)
	updateCase := *updateEvent
	for i := 1; i <= 1000; i++ {
		ts := uint64(i)
		updateCase.CommitTs = ts
		err := encoder.AppendRowChangedEvent(context.Background(), "", &updateCase, nil)
		require.NoError(t, err)

		if i%100 == 0 {
			msgs := encoder.Build()
			require.NotNil(t, msgs)
			require.Len(t, msgs, 100)

			for j := range msgs {
				require.Equal(t, 1, msgs[j].GetRowsCount())

				var msg JSONMessage
				err := json.Unmarshal(msgs[j].Value, &msg)
				require.NoError(t, err)
				require.Equal(t, "UPDATE", msg.EventType)
			}
		}
	}

	require.Len(t, encoder.(*JSONRowEventEncoder).messages, 0)
}

func TestEncodeCheckpointEvent(t *testing.T) {
	t.Parallel()

	var watermark uint64 = 2333
	for _, enable := range []bool{false, true} {
		codecConfig := common.NewConfig(config.ProtocolCanalJSON)
		codecConfig.EnableTiDBExtension = enable

		builder := NewJSONRowEventEncoderBuilder(codecConfig)
		encoder := builder.Build()

		msg, err := encoder.EncodeCheckpointEvent(watermark)
		require.NoError(t, err)

		if !enable {
			require.Nil(t, msg)
			continue
		}

		require.NotNil(t, msg)

		ctx := context.Background()
		decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)

		err = decoder.AddKeyValue(msg.Key, msg.Value)
		require.NoError(t, err)

		ty, hasNext, err := decoder.HasNext()
		require.NoError(t, err)
		if enable {
			require.True(t, hasNext)
			require.Equal(t, model.MessageTypeResolved, ty)
			consumed, err := decoder.NextResolvedEvent()
			require.NoError(t, err)
			require.Equal(t, watermark, consumed)
		} else {
			require.False(t, hasNext)
			require.Equal(t, model.MessageTypeUnknown, ty)
		}

		ty, hasNext, err = decoder.HasNext()
		require.NoError(t, err)
		require.False(t, hasNext)
		require.Equal(t, model.MessageTypeUnknown, ty)
	}
}

func TestCheckpointEventValueMarshal(t *testing.T) {
	t.Parallel()

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true

	builder := NewJSONRowEventEncoderBuilder(codecConfig)

	encoder := builder.Build()
	var watermark uint64 = 1024
	msg, err := encoder.EncodeCheckpointEvent(watermark)
	require.NoError(t, err)
	require.NotNil(t, msg)

	// Unmarshal from the data we have encoded.
	jsonMsg := canalJSONMessageWithTiDBExtension{
		&JSONMessage{},
		&tidbExtension{},
	}
	err = json.Unmarshal(msg.Value, &jsonMsg)
	require.NoError(t, err)
	require.Equal(t, watermark, jsonMsg.Extensions.WatermarkTs)
	// Hack the build time.
	// Otherwise, the timing will be inconsistent.
	jsonMsg.BuildTime = 1469579899
	rawBytes, err := json.MarshalIndent(jsonMsg, "", "  ")
	require.NoError(t, err)

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
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	encoder := &JSONRowEventEncoder{
		builder: newCanalEntryBuilder(codecConfig),
		config:  &common.Config{EnableTiDBExtension: true},
	}
	require.NotNil(t, encoder)

	message := encoder.newJSONMessageForDDL(testCaseDDL)
	require.NotNil(t, message)

	msg, ok := message.(*canalJSONMessageWithTiDBExtension)
	require.True(t, ok)
	// Hack the build time.
	// Otherwise, the timing will be inconsistent.
	msg.BuildTime = 1469579899
	rawBytes, err := json.MarshalIndent(msg, "", "  ")
	require.NoError(t, err)

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

func TestCanalJSONAppendRowChangedEventWithCallback(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(255) primary key)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	builder := NewJSONRowEventEncoderBuilder(codecConfig)
	encoder := builder.Build()

	count := 0
	row := &model.RowChangedEvent{
		CommitTs:  1,
		Table:     &model.TableName{Schema: "test", Table: "t"},
		TableInfo: tableInfo,
		Columns: []*model.Column{{
			Name:  "a",
			Type:  mysql.TypeVarchar,
			Value: []byte("aa"),
		}},
		ColInfos: colInfos,
	}

	tests := []struct {
		row      *model.RowChangedEvent
		callback func()
	}{
		{
			row: row,
			callback: func() {
				count += 1
			},
		},
		{
			row: row,
			callback: func() {
				count += 2
			},
		},
		{
			row: row,
			callback: func() {
				count += 3
			},
		},
		{
			row: row,
			callback: func() {
				count += 4
			},
		},
		{
			row: row,
			callback: func() {
				count += 5
			},
		},
	}

	// Empty build makes sure that the callback build logic not broken.
	msgs := encoder.Build()
	require.Len(t, msgs, 0, "no message should be built and no panic")

	// Append the events.
	for _, test := range tests {
		err := encoder.AppendRowChangedEvent(context.Background(), "", test.row, test.callback)
		require.NoError(t, err)
	}
	require.Equal(t, 0, count, "nothing should be called")

	msgs = encoder.Build()
	require.Len(t, msgs, 5, "expected 5 messages")
	msgs[0].Callback()
	require.Equal(t, 1, count, "expected one callback be called")
	msgs[1].Callback()
	require.Equal(t, 3, count, "expected one callback be called")
	msgs[2].Callback()
	require.Equal(t, 6, count, "expected one callback be called")
	msgs[3].Callback()
	require.Equal(t, 10, count, "expected one callback be called")
	msgs[4].Callback()
	require.Equal(t, 15, count, "expected one callback be called")
}

func TestMaxMessageBytes(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(255) primary key)`
	job := helper.DDL2Job(sql)
	tableInfo := model.WrapTableInfo(0, "test", 1, job.BinlogInfo.TableInfo)

	_, _, colInfos := tableInfo.GetRowColInfos()

	// the size of `testEvent` after being encoded by canal-json is 200
	testEvent := &model.RowChangedEvent{
		CommitTs:  1,
		Table:     &model.TableName{Schema: "test", Table: "t"},
		TableInfo: tableInfo,
		Columns: []*model.Column{{
			Name:  "a",
			Type:  mysql.TypeVarchar,
			Value: []byte("aa"),
		}},
		ColInfos: colInfos,
	}

	ctx := context.Background()
	topic := ""

	// the test message length is smaller than max-message-bytes
	maxMessageBytes := 300
	codecConfig := common.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(maxMessageBytes)

	builder := NewJSONRowEventEncoderBuilder(codecConfig)
	encoder := builder.Build()

	err := encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
	require.NoError(t, err)

	// the test message length is larger than max-message-bytes
	codecConfig = codecConfig.WithMaxMessageBytes(100)
	builder = NewJSONRowEventEncoderBuilder(codecConfig)

	encoder = builder.Build()
	err = encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
	require.Error(t, err, cerror.ErrMessageTooLarge)
}

func TestCanalJSONContentCompatibleE2E(t *testing.T) {
	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.ContentCompatible = true

	builder := NewJSONRowEventEncoderBuilder(codecConfig)
	encoder := builder.Build()

	insertEvent, _, _ := newLargeEvent4Test(t)
	err := encoder.AppendRowChangedEvent(ctx, "", insertEvent, func() {})
	require.NoError(t, err)

	message := encoder.Build()[0]

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, messageType, model.MessageTypeRow)

	decodedEvent, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)
	require.Equal(t, decodedEvent.CommitTs, insertEvent.CommitTs)
	require.Equal(t, decodedEvent.Table.Schema, insertEvent.Table.Schema)
	require.Equal(t, decodedEvent.Table.Table, insertEvent.Table.Table)

	obtainedColumns := make(map[string]*model.Column, len(decodedEvent.Columns))
	for _, column := range decodedEvent.Columns {
		obtainedColumns[column.Name] = column
	}

	expectedValue := collectExpectedDecodedValue(testColumnsTable)
	for _, actual := range insertEvent.Columns {
		obtained, ok := obtainedColumns[actual.Name]
		require.True(t, ok)
		require.Equal(t, actual.Type, obtained.Type)
		require.Equal(t, expectedValue[actual.Name], obtained.Value)
	}
}
