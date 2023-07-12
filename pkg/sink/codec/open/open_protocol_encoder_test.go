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
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/internal"
	"github.com/stretchr/testify/require"
)

func TestBuildOpenProtocolBatchEncoder(t *testing.T) {
	t.Parallel()
	config := common.NewConfig(config.ProtocolOpen)
	builder := &batchEncoderBuilder{config: config}
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
				Flag:  model.HandleKeyFlag,
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
				Value: []byte("aabb"),
				Flag:  model.HandleKeyFlag,
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
	a := 172
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(a)
	encoder := NewBatchEncoderBuilder(codecConfig).Build()
	err := encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
	require.NoError(t, err)

	// cannot hold a single message
	codecConfig = codecConfig.WithMaxMessageBytes(a - 1)
	encoder = NewBatchEncoderBuilder(codecConfig).Build()
	err = encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
	require.ErrorIs(t, err, cerror.ErrMessageTooLarge)

	// make sure each batch's `Length` not greater than `max-message-bytes`
	codecConfig = codecConfig.WithMaxMessageBytes(256)
	encoder = NewBatchEncoderBuilder(codecConfig).Build()
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
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(1048576)
	codecConfig.MaxBatchSize = 64
	encoder := NewBatchEncoderBuilder(codecConfig).Build()

	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(context.Background(), "", testEvent, nil)
		require.NoError(t, err)
	}

	messages := encoder.Build()
	decoder, err := NewBatchDecoder(context.Background(), config.GetDefaultReplicaConfig())
	require.NoError(t, err)
	sum := 0
	for _, msg := range messages {
		err := decoder.AddKeyValue(msg.Key, msg.Value)
		require.NoError(t, err)
		count := 0
		for {
			v, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
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
	tester.TestBatchCodec(t, NewBatchEncoderBuilder(codecConfig),
		func(key []byte, value []byte) (codec.RowEventDecoder, error) {
			decoder, err := NewBatchDecoder(context.Background(), config.GetDefaultReplicaConfig())
			require.NoError(t, err)
			err = decoder.AddKeyValue(key, value)
			return decoder, err
		})
}

func TestAppendClaimCheckMessage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	topic := ""
	// just can hold it.
	a := 172
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(a)
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionClaimCheck
	codecConfig.LargeMessageHandle.ClaimCheckStorageURI = "file:///tmp/claim-check"
	encoder := NewBatchEncoderBuilder(codecConfig).Build()

	err := encoder.AppendRowChangedEvent(ctx, topic, testEvent, func() {})
	require.NoError(t, err)

	// cannot hold this message, encode it as claim check message by force.
	err = encoder.AppendRowChangedEvent(ctx, topic, largeTestEvent, func() {})
	require.NoError(t, err)

	err = encoder.AppendRowChangedEvent(ctx, topic, testEvent, func() {})
	require.NoError(t, err)

	messages := encoder.Build()
	require.Len(t, messages, 3)

	require.Empty(t, messages[0].ClaimCheckFileName)
	require.NotEmptyf(t, messages[1].ClaimCheckFileName, "claim check file name should not be empty")
	require.Empty(t, messages[2].ClaimCheckFileName)
}

func TestAppendMessageOnlyHandleKeyColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	topic := ""

	// cannot hold one message
	a := 171
	codecConfig := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(a)
	codecConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
	encoder := NewBatchEncoderBuilder(codecConfig).Build()

	// only handle key is encoded into the message
	err := encoder.AppendRowChangedEvent(ctx, topic, testEvent, func() {})
	require.NoError(t, err)

	message := encoder.Build()[0]

	decoder, err := NewBatchDecoder(context.Background(), config.GetDefaultReplicaConfig())
	require.NoError(t, err)
	err = decoder.AddKeyValue(message.Key, message.Value)
	require.NoError(t, err)

	batchDecoder := decoder.(*BatchDecoder)
	err = batchDecoder.decodeNextKey()
	require.NoError(t, err)
	require.True(t, batchDecoder.nextKey.OnlyHandleKey)
}
