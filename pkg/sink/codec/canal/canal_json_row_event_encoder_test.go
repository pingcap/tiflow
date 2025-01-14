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
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/compression"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"github.com/stretchr/testify/require"
)

func TestBuildCanalJSONRowEventEncoder(t *testing.T) {
	t.Parallel()
	cfg := common.NewConfig(config.ProtocolCanalJSON)

	builder, err := NewJSONRowEventEncoderBuilder(context.Background(), cfg)
	require.NoError(t, err)
	encoder, ok := builder.Build().(*JSONRowEventEncoder)
	require.True(t, ok)
	require.NotNil(t, encoder.config)
}

func TestDMLE2E(t *testing.T) {
	createTableDDLEvent, insertEvent, updateEvent, deleteEvent := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	for _, enableTiDBExtension := range []bool{false, true} {
		codecConfig.EnableTiDBExtension = enableTiDBExtension
		builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
		require.NoError(t, err)
		encoder := builder.Build()

		decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)

		message, err := encoder.EncodeDDLEvent(createTableDDLEvent)
		require.NoError(t, err)

		err = decoder.AddKeyValue(message.Key, message.Value)
		require.NoError(t, err)

		messageType, hasNext, err := decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, messageType, model.MessageTypeDDL)

		decodedDDL, err := decoder.NextDDLEvent()
		require.NoError(t, err)
		if enableTiDBExtension {
			require.Equal(t, createTableDDLEvent.CommitTs, decodedDDL.CommitTs)
		}
		require.Equal(t, createTableDDLEvent.Query, decodedDDL.Query)

		err = encoder.AppendRowChangedEvent(ctx, "", insertEvent, func() {})
		require.NoError(t, err)

		message = encoder.Build()[0]
		err = decoder.AddKeyValue(message.Key, message.Value)
		require.NoError(t, err)

		messageType, hasNext, err = decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, messageType, model.MessageTypeRow)

		decodedEvent, err := decoder.NextRowChangedEvent()
		require.NoError(t, err)

		require.True(t, decodedEvent.IsInsert())
		if enableTiDBExtension {
			require.Equal(t, insertEvent.GetCommitTs(), decodedEvent.GetCommitTs())
		}
		require.NotZero(t, decodedEvent.GetTableID())
		require.Equal(t, insertEvent.TableInfo.GetSchemaName(), decodedEvent.TableInfo.GetSchemaName())
		require.Equal(t, insertEvent.TableInfo.GetTableName(), decodedEvent.TableInfo.GetTableName())

		decodedColumns := make(map[string]*model.ColumnData, len(decodedEvent.Columns))
		for _, column := range decodedEvent.Columns {
			colName := decodedEvent.TableInfo.ForceGetColumnName(column.ColumnID)
			decodedColumns[colName] = column
		}
		for _, col := range insertEvent.Columns {
			colName := insertEvent.TableInfo.ForceGetColumnName(col.ColumnID)
			decoded, ok := decodedColumns[colName]
			require.True(t, ok)
			switch v := col.Value.(type) {
			case types.VectorFloat32:
				require.EqualValues(t, v.String(), decoded.Value)
			default:
				require.EqualValues(t, v, decoded.Value)
			}
		}

		err = encoder.AppendRowChangedEvent(ctx, "", updateEvent, func() {})
		require.NoError(t, err)

		message = encoder.Build()[0]

		err = decoder.AddKeyValue(message.Key, message.Value)
		require.NoError(t, err)

		messageType, hasNext, err = decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.EqualValues(t, messageType, model.MessageTypeRow)

		decodedEvent, err = decoder.NextRowChangedEvent()
		require.NoError(t, err)
		require.True(t, decodedEvent.IsUpdate())

		err = encoder.AppendRowChangedEvent(ctx, "", deleteEvent, func() {})
		require.NoError(t, err)

		message = encoder.Build()[0]
		err = decoder.AddKeyValue(message.Key, message.Value)
		require.NoError(t, err)

		messageType, hasNext, err = decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.EqualValues(t, messageType, model.MessageTypeRow)

		decodedEvent, err = decoder.NextRowChangedEvent()
		require.NoError(t, err)
		require.True(t, decodedEvent.IsDelete())
	}
}

func TestCanalJSONCompressionE2E(t *testing.T) {
	_, insertEvent, _, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compression.LZ4

	ctx := context.Background()
	builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	// encode normal row changed event
	err = encoder.AppendRowChangedEvent(ctx, "", insertEvent, func() {})
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
	require.Equal(t, decodedEvent.TableInfo.GetSchemaName(), insertEvent.TableInfo.GetSchemaName())
	require.Equal(t, decodedEvent.TableInfo.GetTableName(), insertEvent.TableInfo.GetTableName())

	// encode DDL event
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))`
	ddlEvent := helper.DDL2Event(sql)

	message, err = encoder.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, hasNext, err = decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, messageType, model.MessageTypeDDL)

	decodedDDL, err := decoder.NextDDLEvent()
	require.NoError(t, err)

	require.Equal(t, decodedDDL.Query, ddlEvent.Query)
	require.Equal(t, decodedDDL.CommitTs, ddlEvent.CommitTs)
	require.Equal(t, decodedDDL.TableInfo.TableName.Schema, ddlEvent.TableInfo.TableName.Schema)
	require.Equal(t, decodedDDL.TableInfo.TableName.Table, ddlEvent.TableInfo.TableName.Table)

	// encode checkpoint event
	waterMark := uint64(2333)
	message, err = encoder.EncodeCheckpointEvent(waterMark)
	require.NoError(t, err)

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, hasNext, err = decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, messageType, model.MessageTypeResolved)

	decodedWatermark, err := decoder.NextResolvedEvent()
	require.NoError(t, err)
	require.Equal(t, decodedWatermark, waterMark)
}

func TestCanalJSONClaimCheckE2E(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionClaimCheck
	codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compression.Snappy
	codecConfig.LargeMessageHandle.ClaimCheckStorageURI = "file:///tmp/canal-json-claim-check"
	codecConfig.MaxMessageBytes = 500
	ctx := context.Background()

	for _, rawValue := range []bool{false, true} {
		codecConfig.LargeMessageHandle.ClaimCheckRawValue = rawValue

		builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
		require.NoError(t, err)
		encoder := builder.Build()

		_, insertEvent, _, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())
		err = encoder.AppendRowChangedEvent(ctx, "", insertEvent, func() {})
		require.NoError(t, err)

		// this is a large message, should be delivered to the external storage.
		claimCheckLocationMessage := encoder.Build()[0]

		decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)

		err = decoder.AddKeyValue(claimCheckLocationMessage.Key, claimCheckLocationMessage.Value)
		require.NoError(t, err)

		messageType, ok, err := decoder.HasNext()
		require.NoError(t, err)
		require.Equal(t, messageType, model.MessageTypeRow)
		require.True(t, ok)

		decodedLargeEvent, err := decoder.NextRowChangedEvent()
		require.NoError(t, err, rawValue)

		require.Equal(t, insertEvent.CommitTs, decodedLargeEvent.CommitTs)
		require.Equal(t, insertEvent.TableInfo.GetSchemaName(), decodedLargeEvent.TableInfo.GetSchemaName())
		require.Equal(t, insertEvent.TableInfo.GetTableName(), decodedLargeEvent.TableInfo.GetTableName())
		require.Nil(t, nil, decodedLargeEvent.PreColumns)

		decodedColumns := make(map[string]*model.ColumnData, len(decodedLargeEvent.Columns))
		for _, column := range decodedLargeEvent.Columns {
			colName := decodedLargeEvent.TableInfo.ForceGetColumnName(column.ColumnID)
			decodedColumns[colName] = column
		}
		for _, col := range insertEvent.Columns {
			colName := insertEvent.TableInfo.ForceGetColumnName(col.ColumnID)
			decoded, ok := decodedColumns[colName]
			require.True(t, ok)
			switch v := col.Value.(type) {
			case types.VectorFloat32:
				require.EqualValues(t, v.String(), decoded.Value)
			default:
				require.EqualValues(t, v, decoded.Value)
			}
		}
	}
}

func TestNewCanalJSONMessageHandleKeyOnly4LargeMessage(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
	codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compression.LZ4
	codecConfig.MaxMessageBytes = 500

	ctx := context.Background()

	builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	_, insertEvent, _, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())
	err = encoder.AppendRowChangedEvent(context.Background(), "", insertEvent, func() {})
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
		colName := insertEvent.TableInfo.ForceGetColumnName(col.ColumnID)
		if insertEvent.TableInfo.ForceGetColumnFlagType(col.ColumnID).IsHandleKey() {
			require.Contains(t, handleKeyOnlyMessage.Data[0], colName)
			require.Contains(t, handleKeyOnlyMessage.SQLType, colName)
			require.Contains(t, handleKeyOnlyMessage.MySQLType, colName)
		} else {
			require.NotContains(t, handleKeyOnlyMessage.Data[0], colName)
			require.NotContains(t, handleKeyOnlyMessage.SQLType, colName)
			require.NotContains(t, handleKeyOnlyMessage.MySQLType, colName)
		}
	}
}

func TestNewCanalJSONMessageFromDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	ctx := context.Background()

	builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build().(*JSONRowEventEncoder)

	sql := `create table test.person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))`
	ddlEvent := helper.DDL2Event(sql)

	message := encoder.newJSONMessageForDDL(ddlEvent)
	require.NotNil(t, message)

	msg, ok := message.(*JSONMessage)
	require.True(t, ok)
	require.Equal(t, convertToCanalTs(ddlEvent.CommitTs), msg.ExecutionTime)
	require.True(t, msg.IsDDL)
	require.Equal(t, "test", msg.Schema)
	require.Equal(t, "person", msg.Table)
	require.Equal(t, ddlEvent.Query, msg.Query)
	require.Equal(t, "CREATE", msg.EventType)

	codecConfig.EnableTiDBExtension = true
	builder, err = NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)

	encoder = builder.Build().(*JSONRowEventEncoder)
	message = encoder.newJSONMessageForDDL(ddlEvent)
	require.NotNil(t, message)

	withExtension, ok := message.(*canalJSONMessageWithTiDBExtension)
	require.True(t, ok)

	require.NotNil(t, withExtension.Extensions)
	require.Equal(t, ddlEvent.CommitTs, withExtension.Extensions.CommitTs)
}

func TestBatching(t *testing.T) {
	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()
	require.NotNil(t, encoder)

	_, _, updateEvent, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())
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

	ctx := context.Background()
	var watermark uint64 = 2333
	for _, enable := range []bool{false, true} {
		codecConfig := common.NewConfig(config.ProtocolCanalJSON)
		codecConfig.EnableTiDBExtension = enable

		builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
		require.NoError(t, err)

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

	ctx := context.Background()

	builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)

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
	require.Equal(t, tidbWaterMarkType, jsonMsg.EventType)
	require.Equal(t, "", jsonMsg.Schema)
	require.Equal(t, "", jsonMsg.Table)
	require.Equal(t, "", jsonMsg.Query)
	require.False(t, jsonMsg.IsDDL)
	require.EqualValues(t, 0, jsonMsg.ExecutionTime)
	require.Nil(t, jsonMsg.Data)
	require.Nil(t, jsonMsg.Old)
	require.Nil(t, jsonMsg.SQLType)
	require.Nil(t, jsonMsg.MySQLType)
}

func TestDDLEventWithExtension(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()
	require.NotNil(t, encoder)

	sql := `create table test.person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))`
	ddlEvent := helper.DDL2Event(sql)

	message, err := encoder.EncodeDDLEvent(ddlEvent)
	require.NoError(t, err)

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	messageType, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, messageType, model.MessageTypeDDL)

	decodedDDL, err := decoder.NextDDLEvent()
	require.NoError(t, err)
	require.Equal(t, ddlEvent.Query, decodedDDL.Query)
	require.Equal(t, ddlEvent.CommitTs, decodedDDL.CommitTs)
	require.Equal(t, ddlEvent.TableInfo.TableName.Schema, decodedDDL.TableInfo.TableName.Schema)
	require.Equal(t, ddlEvent.TableInfo.TableName.Table, decodedDDL.TableInfo.TableName.Table)
}

func TestCanalJSONAppendRowChangedEventWithCallback(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	sql := `create table test.t(a varchar(255) primary key)`
	_ = helper.DDL2Event(sql)

	sql = `insert into test.t values ("aa")`
	row := helper.DML2Event(sql, "test", "t")

	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	ctx := context.Background()

	builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	count := 0
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
	_ = helper.DDL2Event(sql)

	sql = `insert into test.t values ("aa")`
	row := helper.DML2Event(sql, "test", "t")

	ctx := context.Background()
	topic := ""

	// the test message length is smaller than max-message-bytes
	maxMessageBytes := 300
	codecConfig := common.NewConfig(config.ProtocolCanalJSON).WithMaxMessageBytes(maxMessageBytes)

	builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	err = encoder.AppendRowChangedEvent(ctx, topic, row, nil)
	require.NoError(t, err)

	// the test message length is larger than max-message-bytes
	codecConfig = codecConfig.WithMaxMessageBytes(100)

	builder, err = NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)

	encoder = builder.Build()
	err = encoder.AppendRowChangedEvent(ctx, topic, row, nil)
	require.Error(t, err, cerror.ErrMessageTooLarge)
}

func TestCanalJSONContentCompatibleE2E(t *testing.T) {
	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	codecConfig.EnableTiDBExtension = true
	codecConfig.ContentCompatible = true
	codecConfig.OnlyOutputUpdatedColumns = true

	builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)

	encoder := builder.Build()

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	_, insertEvent, updateEvent, deleteEvent := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())
	events := []*model.RowChangedEvent{
		insertEvent,
		updateEvent,
		deleteEvent,
	}

	for _, event := range events {
		err = encoder.AppendRowChangedEvent(ctx, "", event, func() {})
		require.NoError(t, err)

		message := encoder.Build()[0]

		err = decoder.AddKeyValue(message.Key, message.Value)
		require.NoError(t, err)

		messageType, hasNext, err := decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, messageType, model.MessageTypeRow)

		decodedEvent, err := decoder.NextRowChangedEvent()
		require.NoError(t, err)
		require.Equal(t, decodedEvent.CommitTs, event.CommitTs)
		require.Equal(t, decodedEvent.TableInfo.GetSchemaName(), event.TableInfo.GetSchemaName())
		require.Equal(t, decodedEvent.TableInfo.GetTableName(), event.TableInfo.GetTableName())

		obtainedColumns := make(map[string]*model.ColumnData, len(decodedEvent.Columns))
		for _, column := range decodedEvent.Columns {
			colName := decodedEvent.TableInfo.ForceGetColumnName(column.ColumnID)
			obtainedColumns[colName] = column
		}
		for _, col := range event.Columns {
			colName := event.TableInfo.ForceGetColumnName(col.ColumnID)
			decoded, ok := obtainedColumns[colName]
			require.True(t, ok)
			switch v := col.Value.(type) {
			case types.VectorFloat32:
				require.EqualValues(t, v.String(), decoded.Value)
			default:
				require.EqualValues(t, v, decoded.Value)
			}
		}

		obtainedPreColumns := make(map[string]*model.ColumnData, len(decodedEvent.PreColumns))
		for _, column := range decodedEvent.PreColumns {
			colName := decodedEvent.TableInfo.ForceGetColumnName(column.ColumnID)
			obtainedPreColumns[colName] = column
		}
		for _, col := range event.PreColumns {
			colName := event.TableInfo.ForceGetColumnName(col.ColumnID)
			decoded, ok := obtainedPreColumns[colName]
			require.True(t, ok)
			switch v := col.Value.(type) {
			case types.VectorFloat32:
				require.EqualValues(t, v.String(), decoded.Value)
			default:
				require.EqualValues(t, v, decoded.Value)
			}
		}
	}
}

func TestE2EPartitionTableByHash(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	createTableDDLEvent := helper.DDL2Event(`CREATE TABLE t (a INT,PRIMARY KEY(a)) PARTITION BY HASH (a) PARTITIONS 5`)
	require.NotNil(t, createTableDDLEvent)
	insertEvent := helper.DML2Event(`insert into t values (5)`, "test", "t", "p0")
	require.NotNil(t, insertEvent)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	message, err := encoder.EncodeDDLEvent(createTableDDLEvent)
	require.NoError(t, err)

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	tp, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeDDL, tp)

	decodedDDL, err := decoder.NextDDLEvent()
	require.NoError(t, err)
	require.NotNil(t, decodedDDL)

	err = encoder.AppendRowChangedEvent(ctx, "", insertEvent, nil)
	require.NoError(t, err)
	message = encoder.Build()[0]

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)
	tp, hasNext, err = decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeRow, tp)

	decodedEvent, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)
	require.NotZero(t, decodedEvent.GetTableID())
	require.Equal(t, decodedEvent.GetTableID(), decodedEvent.TableInfo.GetPartitionInfo().Definitions[0].ID)
}

func TestE2EPartitionTableByRange(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	createTableDDLEvent := helper.DDL2Event(`create table t (id int primary key, a int) PARTITION BY RANGE ( id ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (21))`)
	require.NotNil(t, createTableDDLEvent)

	insertEvent := helper.DML2Event(`insert into t (id) values (6)`, "test", "t", "p1")
	require.NotNil(t, insertEvent)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)

	builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)

	message, err := encoder.EncodeDDLEvent(createTableDDLEvent)
	require.NoError(t, err)

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	tp, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeDDL, tp)

	decodedDDL, err := decoder.NextDDLEvent()
	require.NoError(t, err)
	require.NotNil(t, decodedDDL)

	err = encoder.AppendRowChangedEvent(ctx, "", insertEvent, nil)
	require.NoError(t, err)
	message = encoder.Build()[0]

	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)
	tp, hasNext, err = decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeRow, tp)

	decodedEvent, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)
	require.NotZero(t, decodedEvent.GetTableID())
	require.Equal(t, decodedEvent.GetTableID(), decodedEvent.TableInfo.GetPartitionInfo().Definitions[1].ID)
}

func TestE2EPartitionTable(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")

	createPartitionTableDDL := helper.DDL2Event(`create table test.t(a int primary key, b int) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than MAXVALUE)`)
	require.NotNil(t, createPartitionTableDDL)

	insertEvent := helper.DML2Event(`insert into test.t values (1, 1)`, "test", "t", "p0")
	require.NotNil(t, insertEvent)

	insertEvent1 := helper.DML2Event(`insert into test.t values (11, 11)`, "test", "t", "p1")
	require.NotNil(t, insertEvent1)

	insertEvent2 := helper.DML2Event(`insert into test.t values (21, 21)`, "test", "t", "p2")
	require.NotNil(t, insertEvent2)

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolCanalJSON)
	for _, enableTiDBExtension := range []bool{false, true} {
		codecConfig.EnableTiDBExtension = enableTiDBExtension

		builder, err := NewJSONRowEventEncoderBuilder(ctx, codecConfig)
		require.NoError(t, err)
		encoder := builder.Build()

		decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
		require.NoError(t, err)

		message, err := encoder.EncodeDDLEvent(createPartitionTableDDL)
		require.NoError(t, err)

		err = decoder.AddKeyValue(message.Key, message.Value)
		require.NoError(t, err)

		tp, hasNext, err := decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, model.MessageTypeDDL, tp)

		decodedDDL, err := decoder.NextDDLEvent()
		require.NoError(t, err)
		require.NotNil(t, decodedDDL)

		err = encoder.AppendRowChangedEvent(ctx, "", insertEvent, nil)
		require.NoError(t, err)
		message = encoder.Build()[0]

		err = decoder.AddKeyValue(message.Key, message.Value)
		require.NoError(t, err)
		tp, hasNext, err = decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, model.MessageTypeRow, tp)

		decodedEvent, err := decoder.NextRowChangedEvent()
		require.NoError(t, err)
		require.NotZero(t, decodedEvent.GetTableID())
		require.Equal(t, decodedEvent.GetTableID(), decodedEvent.TableInfo.GetPartitionInfo().Definitions[0].ID)

		err = encoder.AppendRowChangedEvent(ctx, "", insertEvent1, nil)
		require.NoError(t, err)
		message = encoder.Build()[0]

		err = decoder.AddKeyValue(message.Key, message.Value)
		require.NoError(t, err)
		tp, hasNext, err = decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, model.MessageTypeRow, tp)

		decodedEvent, err = decoder.NextRowChangedEvent()
		require.NoError(t, err)

		require.NotZero(t, decodedEvent.GetTableID())
		require.Equal(t, decodedEvent.GetTableID(), decodedEvent.TableInfo.GetPartitionInfo().Definitions[1].ID)

		err = encoder.AppendRowChangedEvent(ctx, "", insertEvent2, nil)
		require.NoError(t, err)
		message = encoder.Build()[0]

		err = decoder.AddKeyValue(message.Key, message.Value)
		require.NoError(t, err)
		tp, hasNext, err = decoder.HasNext()
		require.NoError(t, err)
		require.True(t, hasNext)
		require.Equal(t, model.MessageTypeRow, tp)

		decodedEvent, err = decoder.NextRowChangedEvent()
		require.NoError(t, err)

		require.NotZero(t, decodedEvent.GetTableID())
		require.Equal(t, decodedEvent.GetTableID(), decodedEvent.TableInfo.GetPartitionInfo().Definitions[2].ID)
	}
}
