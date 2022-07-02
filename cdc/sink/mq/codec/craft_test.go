// Copyright 2021 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func testBatchCodec(
	t *testing.T,
	encoderBuilder EncoderBuilder,
	newDecoder func(value []byte) (EventBatchDecoder, error),
) {
	checkRowDecoder := func(decoder EventBatchDecoder, cs []*model.RowChangedEvent) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			if !hasNext {
				break
			}
			require.Equal(t, model.MessageTypeRow, tp)
			row, err := decoder.NextRowChangedEvent()
			require.Nil(t, err)
			require.Equal(t, cs[index], row)
			index++
		}
	}
	checkDDLDecoder := func(decoder EventBatchDecoder, cs []*model.DDLEvent) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			if !hasNext {
				break
			}
			require.Equal(t, model.MessageTypeDDL, tp)
			ddl, err := decoder.NextDDLEvent()
			require.Nil(t, err)
			require.Equal(t, cs[index], ddl)
			index++
		}
	}
	checkTSDecoder := func(decoder EventBatchDecoder, cs []uint64) {
		index := 0
		for {
			tp, hasNext, err := decoder.HasNext()
			require.Nil(t, err)
			if !hasNext {
				break
			}
			require.Equal(t, model.MessageTypeResolved, tp)
			ts, err := decoder.NextResolvedEvent()
			require.Nil(t, err)
			require.Equal(t, cs[index], ts)
			index++
		}
	}

	encoder := encoderBuilder.Build()
	s := NewDefaultBatchTester()

	for _, cs := range s.rowCases {
		events := 0
		for _, row := range cs {
			err := encoder.AppendRowChangedEvent(context.Background(), "", row)
			events++
			require.Nil(t, err)
		}
		// test normal decode
		if len(cs) > 0 {
			res := encoder.Build()
			require.Len(t, res, 1)
			decoder, err := newDecoder(res[0].Value)
			require.Nil(t, err)
			checkRowDecoder(decoder, cs)
		}
	}

	encoder = encoderBuilder.Build()
	for _, cs := range s.ddlCases {
		for i, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			require.Nil(t, err)
			require.NotNil(t, msg)
			decoder, err := newDecoder(msg.Value)
			require.Nil(t, err)
			checkDDLDecoder(decoder, cs[i:i+1])
		}
	}

	encoder = encoderBuilder.Build()
	for _, cs := range s.resolvedTsCases {
		for i, ts := range cs {
			msg, err := encoder.EncodeCheckpointEvent(ts)
			require.Nil(t, err)
			require.NotNil(t, msg)
			decoder, err := newDecoder(msg.Value)
			require.Nil(t, err)
			checkTSDecoder(decoder, cs[i:i+1])
		}
	}
}

func TestCraftMaxMessageBytes(t *testing.T) {
	t.Parallel()
	config := NewConfig(config.ProtocolCraft).WithMaxMessageBytes(256)
	encoder := newCraftEventBatchEncoderBuilder(config).Build()

	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: mysql.TypeVarchar, Value: []byte("aa")}},
	}

	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(context.Background(), "", testEvent)
		require.Nil(t, err)
	}

	messages := encoder.Build()
	for _, msg := range messages {
		require.LessOrEqual(t, msg.Length(), 256)
	}
}

func TestCraftMaxBatchSize(t *testing.T) {
	t.Parallel()
	config := NewConfig(config.ProtocolCraft).WithMaxMessageBytes(10485760)
	config.maxBatchSize = 64
	encoder := newCraftEventBatchEncoderBuilder(config).Build()

	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: mysql.TypeVarchar, Value: []byte("aa")}},
	}

	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(context.Background(), "", testEvent)
		require.Nil(t, err)
	}

	messages := encoder.Build()
	sum := 0
	for _, msg := range messages {
		decoder, err := NewCraftEventBatchDecoder(msg.Value)
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

func TestDefaultCraftEventBatchCodec(t *testing.T) {
	config := NewConfig(config.ProtocolCraft).WithMaxMessageBytes(8192)
	config.maxBatchSize = 64
	testBatchCodec(t, newCraftEventBatchEncoderBuilder(config), NewCraftEventBatchDecoder)
}

func TestBuildCraftEventBatchEncoder(t *testing.T) {
	t.Parallel()
	config := NewConfig(config.ProtocolCraft)

	builder := &craftEventBatchEncoderBuilder{config: config}
	encoder, ok := builder.Build().(*CraftEventBatchEncoder)
	require.True(t, ok)
	require.Equal(t, config.maxBatchSize, encoder.maxBatchSize)
	require.Equal(t, config.maxMessageBytes, encoder.maxMessageBytes)
}
