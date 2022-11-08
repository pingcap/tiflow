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
	"github.com/pingcap/tiflow/cdc/sinkv2/codec/common"
	"github.com/pingcap/tiflow/cdc/sinkv2/codec/internal"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestBuildOpenProtocolBatchEncoder(t *testing.T) {
	t.Parallel()
	config := common.NewConfig(config.ProtocolOpen)
	builder := &batchEncoderBuilder{config: config}
	encoder, ok := builder.Build().(*BatchEncoder)
	require.True(t, ok)
	require.Equal(t, config.MaxBatchSize, encoder.MaxBatchSize)
	require.Equal(t, config.MaxMessageBytes, encoder.MaxMessageBytes)
}

func TestMaxMessageBytes(t *testing.T) {
	t.Parallel()
	// the size of `testEvent` is 87
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
	// for a single message, the overhead is 36(maxRecordOverhead) + 8(versionHea) = 44, just can hold it.
	a := 88 + 44
	config := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(a)
	encoder := NewBatchEncoderBuilder(config).Build()
	err := encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
	require.Nil(t, err)

	// cannot hold a single message
	config = config.WithMaxMessageBytes(a - 1)
	encoder = NewBatchEncoderBuilder(config).Build()
	err = encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
	require.NotNil(t, err)

	// make sure each batch's `Length` not greater than `max-message-bytes`
	config = config.WithMaxMessageBytes(256)
	encoder = NewBatchEncoderBuilder(config).Build()
	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
		require.Nil(t, err)
	}

	messages := encoder.Build()
	for _, msg := range messages {
		require.LessOrEqual(t, msg.Length(), 256)
	}
}

func TestMaxBatchSize(t *testing.T) {
	t.Parallel()
	config := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(1048576)
	config.MaxBatchSize = 64
	encoder := NewBatchEncoderBuilder(config).Build()

	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns: []*model.Column{{
			Name:  "col1",
			Type:  mysql.TypeVarchar,
			Value: []byte("aa"),
		}},
	}

	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(context.Background(), "", testEvent, nil)
		require.Nil(t, err)
	}

	messages := encoder.Build()
	sum := 0
	for _, msg := range messages {
		decoder, err := NewBatchDecoder(msg.Key, msg.Value)
		require.Nil(t, err)
		count := 0
		for {
			v, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			if !hasNext {
				break
			}

			require.Equal(t, model.MessageTypeRow, v)
			_, err = decoder.NextRowChangedEvent()
			require.Nil(t, err)
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
	require.Equal(t, cfg.MaxBatchSize, encoder.MaxBatchSize)

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
	require.Len(t, msgs, 3, "expected 3 messages")
	msgs[0].Callback()
	require.Equal(t, 3, count, "expected 2 callbacks be called")
	msgs[1].Callback()
	require.Equal(t, 10, count, "expected 2 callbacks be called")
	msgs[2].Callback()
	require.Equal(t, 15, count, "expected 1 callback be called")
}

func TestOpenProtocolBatchCodec(t *testing.T) {
	config := common.NewConfig(config.ProtocolOpen).WithMaxMessageBytes(8192)
	config.MaxBatchSize = 64
	tester := internal.NewDefaultBatchTester()
	tester.TestBatchCodec(t, NewBatchEncoderBuilder(config), NewBatchDecoder)
}
