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
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"golang.org/x/text/encoding/charmap"
)

func TestBuildCanalJSONRowEventEncoder(t *testing.T) {
	t.Parallel()
	cfg := common.NewConfig(config.ProtocolCanalJSON)

	builder := &jsonRowEventEncoderBuilder{config: cfg}
	encoder, ok := builder.Build().(*JSONRowEventEncoder)
	require.True(t, ok)
	require.NotNil(t, encoder.config)
}

func TestNewCanalJSONMessage4DML(t *testing.T) {
	t.Parallel()
	e := newJSONRowEventEncoder(&common.Config{
		EnableTiDBExtension: false,
		Terminator:          "",
	})
	require.NotNil(t, e)

	encoder, ok := e.(*JSONRowEventEncoder)
	require.True(t, ok)

	data, err := newJSONMessageForDML(testCaseInsert, encoder.config, encoder.builder, false)
	require.Nil(t, err)
	var msg canalJSONMessageInterface = &JSONMessage{}
	err = json.Unmarshal(data, msg)
	require.Nil(t, err)
	jsonMsg, ok := msg.(*JSONMessage)
	require.True(t, ok)
	require.NotNil(t, jsonMsg.Data)
	require.Nil(t, jsonMsg.Old)
	require.Equal(t, "INSERT", jsonMsg.EventType)
	require.Equal(t, convertToCanalTs(testCaseInsert.CommitTs), jsonMsg.ExecutionTime)
	require.Equal(t, "cdc", jsonMsg.Schema)
	require.Equal(t, "person", jsonMsg.Table)
	require.False(t, jsonMsg.IsDDL)

	for _, col := range testCaseInsert.Columns {
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
			require.Nil(t, err)
			require.Equal(t, string(expectedValue), obtainedValue)
			continue
		}

		require.Equal(t, item.expectedEncodedValue, obtainedValue)
	}

	data, err = newJSONMessageForDML(testCaseUpdate, encoder.config, encoder.builder, false)
	require.Nil(t, err)
	jsonMsg = &JSONMessage{}
	err = json.Unmarshal(data, jsonMsg)
	require.Nil(t, err)
	require.NotNil(t, jsonMsg.Data)
	require.NotNil(t, jsonMsg.Old)
	require.Equal(t, "UPDATE", jsonMsg.EventType)

	for _, col := range testCaseUpdate.Columns {
		require.Contains(t, jsonMsg.Data[0], col.Name)
		require.Contains(t, jsonMsg.SQLType, col.Name)
		require.Contains(t, jsonMsg.MySQLType, col.Name)
	}
	for _, col := range testCaseUpdate.PreColumns {
		require.Contains(t, jsonMsg.Old[0], col.Name)
	}

	data, err = newJSONMessageForDML(testCaseDelete, encoder.config, encoder.builder, false)
	require.Nil(t, err)
	jsonMsg = &JSONMessage{}
	err = json.Unmarshal(data, jsonMsg)
	require.Nil(t, err)
	require.NotNil(t, jsonMsg.Data)
	require.Nil(t, jsonMsg.Old)
	require.Equal(t, "DELETE", jsonMsg.EventType)

	for _, col := range testCaseDelete.PreColumns {
		require.Contains(t, jsonMsg.Data[0], col.Name)
	}

	data, err = newJSONMessageForDML(testCaseDelete, &common.Config{DeleteOnlyHandleKeyColumns: true}, encoder.builder, false)
	require.NoError(t, err)
	jsonMsg = &JSONMessage{}
	err = json.Unmarshal(data, jsonMsg)
	require.NoError(t, err)
	require.NotNil(t, jsonMsg.Data)
	require.Nil(t, jsonMsg.Old)

	for _, col := range testCaseDelete.PreColumns {
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

	e = newJSONRowEventEncoder(&common.Config{
		EnableTiDBExtension:      true,
		Terminator:               "",
		OnlyOutputUpdatedColumns: true,
	})
	require.NotNil(t, e)

	encoder, ok = e.(*JSONRowEventEncoder)
	require.True(t, ok)
	data, err = newJSONMessageForDML(testCaseUpdate, encoder.config, encoder.builder, false)
	require.Nil(t, err)

	withExtension := &canalJSONMessageWithTiDBExtension{}
	err = json.Unmarshal(data, withExtension)
	require.Nil(t, err)

	require.NotNil(t, withExtension.Extensions)
	require.Equal(t, testCaseUpdate.CommitTs, withExtension.Extensions.CommitTs)

	encoder, ok = e.(*JSONRowEventEncoder)
	require.True(t, ok)
	data, err = newJSONMessageForDML(testCaseUpdate, encoder.config, encoder.builder, false)
	require.Nil(t, err)

	withExtension = &canalJSONMessageWithTiDBExtension{}
	err = json.Unmarshal(data, withExtension)
	require.Nil(t, err)
	require.Equal(t, 0, len(withExtension.JSONMessage.Old[0]))

	require.NotNil(t, withExtension.Extensions)
	require.Equal(t, testCaseUpdate.CommitTs, withExtension.Extensions.CommitTs)
}

func TestNewCanalJSONMessageHandleKeyOnly4LargeMessage(t *testing.T) {
	t.Parallel()

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
	codecConfig.MaxMessageBytes = 500
	encoder := newJSONRowEventEncoder(codecConfig)

	err := encoder.AppendRowChangedEvent(context.Background(), "", testCaseInsert, func() {})
	require.NoError(t, err)

	message := encoder.Build()[0]

	var decoded canalJSONMessageWithTiDBExtension
	err = json.Unmarshal(message.Value, &decoded)
	require.NoError(t, err)
	require.True(t, decoded.Extensions.OnlyHandleKey)

	for _, col := range testCaseInsert.Columns {
		if col.Flag.IsHandleKey() {
			require.Contains(t, decoded.Data[0], col.Name)
			require.Contains(t, decoded.SQLType, col.Name)
			require.Contains(t, decoded.MySQLType, col.Name)
		} else {
			require.NotContains(t, decoded.Data[0], col.Name)
			require.NotContains(t, decoded.SQLType, col.Name)
			require.NotContains(t, decoded.MySQLType, col.Name)
		}
	}
}

func TestNewCanalJSONMessageFromDDL(t *testing.T) {
	t.Parallel()

	encoder, ok := newJSONRowEventEncoder(&common.Config{}).(*JSONRowEventEncoder)
	require.True(t, ok)

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

	encoder, ok = newJSONRowEventEncoder(&common.Config{
		EnableTiDBExtension: true,
	}).(*JSONRowEventEncoder)
	require.True(t, ok)

	message = encoder.newJSONMessageForDDL(testCaseDDL)
	require.NotNil(t, message)

	withExtension, ok := message.(*canalJSONMessageWithTiDBExtension)
	require.True(t, ok)

	require.NotNil(t, withExtension.Extensions)
	require.Equal(t, testCaseDDL.CommitTs, withExtension.Extensions.CommitTs)
}

func TestBatching(t *testing.T) {
	t.Parallel()
	encoder := newJSONRowEventEncoder(&common.Config{
		EnableTiDBExtension: false,
		Terminator:          "",
		MaxMessageBytes:     config.DefaultMaxMessageBytes,
	})
	require.NotNil(t, encoder)

	updateCase := *testCaseUpdate
	for i := 1; i <= 1000; i++ {
		ts := uint64(i)
		updateCase.CommitTs = ts
		err := encoder.AppendRowChangedEvent(context.Background(), "", &updateCase, nil)
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

	require.Len(t, encoder.(*JSONRowEventEncoder).messages, 0)
}

func TestEncodeCheckpointEvent(t *testing.T) {
	t.Parallel()
	var watermark uint64 = 2333
	for _, enable := range []bool{false, true} {
		codecConfig := common.NewConfig(config.ProtocolCanalJSON)
		codecConfig.EnableTiDBExtension = enable
		encoder := newJSONRowEventEncoder(codecConfig).(*JSONRowEventEncoder)
		require.NotNil(t, encoder)

		msg, err := encoder.EncodeCheckpointEvent(watermark)
		require.Nil(t, err)

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
	encoder := &JSONRowEventEncoder{
		builder: newCanalEntryBuilder(),
		config:  &common.Config{EnableTiDBExtension: true},
	}
	require.NotNil(t, encoder)
	msg, err := encoder.EncodeCheckpointEvent(watermark)
	require.Nil(t, err)
	require.NotNil(t, msg)

	// Unmarshal from the data we have encoded.
	jsonMsg := canalJSONMessageWithTiDBExtension{
		&JSONMessage{},
		&tidbExtension{},
	}
	err = json.Unmarshal(msg.Value, &jsonMsg)
	require.Nil(t, err)
	require.Equal(t, watermark, jsonMsg.Extensions.WatermarkTs)
	// Hack the build time.
	// Otherwise, the timing will be inconsistent.
	jsonMsg.BuildTime = 1469579899
	rawBytes, err := json.MarshalIndent(jsonMsg, "", "  ")
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
	encoder := &JSONRowEventEncoder{
		builder: newCanalEntryBuilder(),
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

func TestCanalJSONAppendRowChangedEventWithCallback(t *testing.T) {
	encoder := newJSONRowEventEncoder(&common.Config{
		EnableTiDBExtension: true,
		Terminator:          "",
		MaxMessageBytes:     config.DefaultMaxMessageBytes,
	})
	require.NotNil(t, encoder)

	count := 0

	row := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{{
			Name:  "col1",
			Type:  mysql.TypeVarchar,
			Value: []byte("aa"),
		}},
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
		require.Nil(t, err)
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
	// the size of `testEvent` after being encoded by canal-json is 200
	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{{
			Name:  "col1",
			Type:  mysql.TypeVarchar,
			Value: []byte("aa"),
		}},
	}

	ctx := context.Background()
	topic := ""

	// the test message length is smaller than max-message-bytes
	maxMessageBytes := 300
	cfg := common.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(maxMessageBytes)
	encoder := NewJSONRowEventEncoderBuilder(cfg).Build()
	err := encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
	require.Nil(t, err)

	// the test message length is larger than max-message-bytes
	cfg = cfg.WithMaxMessageBytes(100)
	cfg.LargeMessageHandle = config.NewDefaultLargeMessageHandleConfig()
	encoder = NewJSONRowEventEncoderBuilder(cfg).Build()
	err = encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
	require.NotNil(t, err)
}
