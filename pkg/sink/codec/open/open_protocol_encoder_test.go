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

package open

import (
	"context"
	"database/sql"
	"testing"

	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/compression"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/internal"
	"github.com/stretchr/testify/require"
)

func TestBuildOpenProtocolBatchEncoder(t *testing.T) {
	t.Parallel()
	codecConfig := common.NewConfig(config.ProtocolOpen)
	builder := &batchEncoderBuilder{config: codecConfig}
	encoder, ok := builder.Build().(*BatchEncoder)
	require.True(t, ok)
	require.NotNil(t, encoder.config)
}

var (
	testEvent = &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{
			{
				Name:  "col1",
				Type:  mysql.TypeVarchar,
				Value: []byte("aa"),
				Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
			},
			{
				Name:  "col2",
				Type:  mysql.TypeVarchar,
				Value: []byte("bb"),
			},
		},
	}
	largeTestEvent = &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{
			{
				Name:  "col1",
				Type:  mysql.TypeVarchar,
				Value: []byte("12345678910"),
				Flag:  model.HandleKeyFlag,
			},
			{
				Name:  "col2",
				Type:  mysql.TypeVarchar,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col3",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col4",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col5",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col6",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col7",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col8",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col9",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col10",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col11",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col12",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col13",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col14",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
			{
				Name:  "col15",
				Type:  mysql.TypeBlob,
				Value: []byte("12345678910"),
			},
		},
	}

	testCaseDDL = &model.DDLEvent{
		CommitTs: 417318403368288260,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: "cdc", Table: "person",
			},
		},
		Query: "create table person(id int, name varchar(32), tiny tinyint unsigned, comment text, primary key(id))",
		Type:  timodel.ActionCreateTable,
	}
	updateEvent = &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{
			{
				Name:  "col1",
				Type:  mysql.TypeVarchar,
				Value: []byte("aa"),
				Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
			},
			{
				Name:  "col2",
				Type:  mysql.TypeVarchar,
				Value: []byte("bb"),
			},
		},
	}
)

func TestMaxMessageBytes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	topic := ""
	// just can hold it.
	a := 173
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(a)
	builder, err := NewBatchEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()
	err = encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
	require.NoError(t, err)

	// cannot hold a single message
	codecConfig = codecConfig.WithMaxMessageBytes(a - 1)
	builder, err = NewBatchEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder = builder.Build()
	err = encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
	require.ErrorIs(t, err, cerror.ErrMessageTooLarge)

	// make sure each batch's `Length` not greater than `max-message-bytes`
	codecConfig = codecConfig.WithMaxMessageBytes(256)
	builder, err = NewBatchEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder = builder.Build()
	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
		require.NoError(t, err)
	}

	messages := encoder.Build()
	for _, msg := range messages {
		require.LessOrEqual(t, msg.Length(), 256)
	}
}

func TestMaxBatchSize(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(1048576)
	codecConfig.MaxBatchSize = 64
	builder, err := NewBatchEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(ctx, "", testEvent, nil)
		require.NoError(t, err)
	}

	messages := encoder.Build()

	decoder, err := NewBatchDecoder(context.Background(), codecConfig, nil)
	require.NoError(t, err)
	sum := 0
	for _, msg := range messages {
		err := decoder.AddKeyValue(msg.Key, msg.Value)
		require.NoError(t, err)
		count := 0
		for {
			v, hasNext, err := decoder.HasNext()
			require.NoError(t, err)
			if !hasNext {
				break
			}

			require.Equal(t, model.MessageTypeRow, v)
			_, err = decoder.NextRowChangedEvent()
			require.NoError(t, err)
			count++
		}
		require.LessOrEqual(t, count, 64)
		sum += count
	}
	require.Equal(t, 10000, sum)
}

func TestOpenProtocolAppendRowChangedEventWithCallback(t *testing.T) {
	t.Parallel()

	cfg := common.NewConfig(config.ProtocolOpen)
	// Set the max batch size to 2, so that we can test the callback.
	cfg.MaxBatchSize = 2
	builder := &batchEncoderBuilder{config: cfg}
	encoder, ok := builder.Build().(*BatchEncoder)
	require.True(t, ok)

	count := 0

	tests := []struct {
		row      *model.RowChangedEvent
		callback func()
	}{
		{
			row: testEvent,
			callback: func() {
				count += 1
			},
		},
		{
			row: testEvent,
			callback: func() {
				count += 2
			},
		},
		{
			row: testEvent,
			callback: func() {
				count += 3
			},
		},
		{
			row: testEvent,
			callback: func() {
				count += 4
			},
		},
		{
			row: testEvent,
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
	require.Len(t, msgs, 3, "expected 3 messages")
	msgs[0].Callback()
	require.Equal(t, 3, count, "expected 2 callbacks be called")
	msgs[1].Callback()
	require.Equal(t, 10, count, "expected 2 callbacks be called")
	msgs[2].Callback()
	require.Equal(t, 15, count, "expected 1 callback be called")
}

func TestOpenProtocolBatchCodec(t *testing.T) {
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(8192)
	codecConfig.MaxBatchSize = 64
	tester := internal.NewDefaultBatchTester()
	builder, err := NewBatchEncoderBuilder(context.Background(), codecConfig)
	require.NoError(t, err)
	tester.TestBatchCodec(t, builder,
		func(key []byte, value []byte) (codec.RowEventDecoder, error) {
			decoder, err := NewBatchDecoder(context.Background(), codecConfig, nil)
			require.NoError(t, err)
			err = decoder.AddKeyValue(key, value)
			return decoder, err
		})
}

func TestEncodeDecodeE2E(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	topic := "test"

	codecConfig := common.NewConfig(config.ProtocolOpen)
	codecConfig.OpenOutputOldValue = false
	builder, err := NewBatchEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	err = encoder.AppendRowChangedEvent(ctx, topic, testEvent, func() {})
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

	decoded, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)

	obtainedColumns := make(map[string]*model.Column)
	for _, col := range decoded.Columns {
		obtainedColumns[col.Name] = col
	}

	for _, col := range testEvent.Columns {
		require.Contains(t, obtainedColumns, col.Name)
	}
}

func TestE2EDDLCompression(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	codecConfig := common.NewConfig(config.ProtocolOpen)
	codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compression.Snappy

	builder, err := NewBatchEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	// encode DDL event
	message, err := encoder.EncodeDDLEvent(testCaseDDL)
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

	require.Equal(t, decodedDDL.Query, testCaseDDL.Query)
	require.Equal(t, decodedDDL.CommitTs, testCaseDDL.CommitTs)
	require.Equal(t, decodedDDL.TableInfo.TableName.Schema, testCaseDDL.TableInfo.TableName.Schema)
	require.Equal(t, decodedDDL.TableInfo.TableName.Table, testCaseDDL.TableInfo.TableName.Table)

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

func TestE2EHandleKeyOnlyEvent(t *testing.T) {
	t.Parallel()

	codecConfig := common.NewConfig(config.ProtocolOpen)
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
	codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compression.Snappy

	codecConfig.MaxMessageBytes = 251

	ctx := context.Background()
	builder, err := NewBatchEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	topic := "test"
	err = encoder.AppendRowChangedEvent(ctx, topic, largeTestEvent, nil)
	require.NoError(t, err)

	message := encoder.Build()[0]

	decoder, err := NewBatchDecoder(ctx, codecConfig, &sql.DB{})
	require.NoError(t, err)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)
	tp, hasNext, err := decoder.HasNext()
	require.NoError(t, err)
	require.True(t, hasNext)
	require.Equal(t, model.MessageTypeRow, tp)

	require.True(t, decoder.(*BatchDecoder).nextKey.OnlyHandleKey)

	nextEvent := decoder.(*BatchDecoder).nextEvent
	require.NotNil(t, nextEvent)

	obtainedColumns := make(map[string]*model.Column)
	for _, col := range nextEvent.Columns {
		obtainedColumns[col.Name] = col
		require.True(t, col.Flag.IsHandleKey())
	}

	for _, col := range largeTestEvent.Columns {
		if col.Flag.IsHandleKey() {
			require.Contains(t, obtainedColumns, col.Name)
		}
	}
}

func TestE2EClaimCheckMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	topic := ""

	a := 244
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(a)
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionClaimCheck
	codecConfig.LargeMessageHandle.LargeMessageHandleCompression = compression.LZ4
	codecConfig.LargeMessageHandle.ClaimCheckStorageURI = "file:///tmp/claim-check"

	builder, err := NewBatchEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	err = encoder.AppendRowChangedEvent(ctx, topic, testEvent, func() {})
	require.NoError(t, err)

	// cannot hold this message, it's encoded as the claim check location message.
	err = encoder.AppendRowChangedEvent(ctx, topic, largeTestEvent, func() {})
	require.NoError(t, err)

	messages := encoder.Build()
	require.Len(t, messages, 2)

	claimCheckLocationMessage := messages[1]
	decoder, err := NewBatchDecoder(ctx, codecConfig, nil)
	require.NoError(t, err)
	err = decoder.AddKeyValue(claimCheckLocationMessage.Key, claimCheckLocationMessage.Value)
	require.NoError(t, err)

	messageType, ok, err := decoder.HasNext()
	require.NoError(t, err)
	require.Equal(t, messageType, model.MessageTypeRow)
	require.True(t, ok)

	decodedLargeEvent, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)

	require.Equal(t, largeTestEvent.CommitTs, decodedLargeEvent.CommitTs)
	require.Equal(t, largeTestEvent.Table, decodedLargeEvent.Table)

	decodedColumns := make(map[string]*model.Column, len(decodedLargeEvent.Columns))
	for _, column := range decodedLargeEvent.Columns {
		decodedColumns[column.Name] = column
	}

	for _, column := range largeTestEvent.Columns {
		decodedColumn, ok := decodedColumns[column.Name]
		require.True(t, ok)
		require.Equal(t, column.Type, decodedColumn.Type)
		require.Equal(t, column.Value, decodedColumn.Value)
	}
}

func TestOutputOldValueFalse(t *testing.T) {
	ctx := context.Background()
	topic := "test"

	codecConfig := common.NewConfig(config.ProtocolOpen)
	codecConfig.OpenOutputOldValue = false
	builder, err := NewBatchEncoderBuilder(ctx, codecConfig)
	require.NoError(t, err)
	encoder := builder.Build()

	err = encoder.AppendRowChangedEvent(ctx, topic, updateEvent, func() {})
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

	decoded, err := decoder.NextRowChangedEvent()
	require.NoError(t, err)
	require.Nil(t, decoded.PreColumns)
}
