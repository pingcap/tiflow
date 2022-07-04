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

package codec

import (
	"context"
	"sort"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

type columnsArray []*model.Column

func (a columnsArray) Len() int {
	return len(a)
}

func (a columnsArray) Less(i, j int) bool {
	return a[i].Name < a[j].Name
}

func (a columnsArray) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func sortColumnArrays(arrays ...[]*model.Column) {
	for _, array := range arrays {
		if array != nil {
			sort.Sort(columnsArray(array))
		}
	}
}

type batchTester struct {
	rowCases        [][]*model.RowChangedEvent
	ddlCases        [][]*model.DDLEvent
	resolvedTsCases [][]uint64
}

func newDefaultBatchTester() *batchTester {
	return &batchTester{
		rowCases:        codecRowCases,
		ddlCases:        codecDDLCases,
		resolvedTsCases: codecResolvedTSCases,
	}
}

func (s *batchTester) testBatchCodec(
	t *testing.T,
	encoderBuilder EncoderBuilder,
	newDecoder func(key []byte, value []byte) (EventBatchDecoder, error),
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
			sortColumnArrays(row.Columns, row.PreColumns, cs[index].Columns, cs[index].PreColumns)
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

	for _, cs := range s.rowCases {
		encoder := encoderBuilder.Build()

		for _, row := range cs {
			err := encoder.AppendRowChangedEvent(context.Background(), "", row, nil)
			require.Nil(t, err)
		}

		if len(cs) > 0 {
			res := encoder.Build()
			require.Len(t, res, 1)
			require.Equal(t, len(cs), res[0].GetRowsCount())
			decoder, err := newDecoder(res[0].Key, res[0].Value)
			require.Nil(t, err)
			checkRowDecoder(decoder, cs)
		}
	}
	for _, cs := range s.ddlCases {
		encoder := encoderBuilder.Build()
		for i, ddl := range cs {
			msg, err := encoder.EncodeDDLEvent(ddl)
			require.Nil(t, err)
			require.NotNil(t, msg)
			decoder, err := newDecoder(msg.Key, msg.Value)
			require.Nil(t, err)
			checkDDLDecoder(decoder, cs[i:i+1])

		}
	}

	for _, cs := range s.resolvedTsCases {
		encoder := encoderBuilder.Build()
		for i, ts := range cs {
			msg, err := encoder.EncodeCheckpointEvent(ts)
			require.Nil(t, err)
			require.NotNil(t, msg)
			decoder, err := newDecoder(msg.Key, msg.Value)
			require.Nil(t, err)
			checkTSDecoder(decoder, cs[i:i+1])
		}
	}
}

func TestBuildJSONEventBatchEncoder(t *testing.T) {
	t.Parallel()
	config := NewConfig(config.ProtocolOpen)
	builder := &openProtocolBatchEncoderBuilder{config: config}
	encoder, ok := builder.Build().(*OpenProtocolBatchEncoder)
	require.True(t, ok)
	require.Equal(t, config.maxBatchSize, encoder.maxBatchSize)
	require.Equal(t, config.maxMessageBytes, encoder.maxMessageBytes)
}

func TestMaxMessageBytes(t *testing.T) {
	t.Parallel()
	// the size of `testEvent` is 87
	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}

	ctx := context.Background()
	topic := ""
	// for a single message, the overhead is 36(maxRecordOverhead) + 8(versionHea) = 44, just can hold it.
	a := 87 + 44
	config := NewConfig(config.ProtocolOpen).WithMaxMessageBytes(a)
	encoder := newJSONEventBatchEncoderBuilder(config).Build()
	err := encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
	require.Nil(t, err)

	// cannot hold a single message
	config = config.WithMaxMessageBytes(a - 1)
	encoder = newJSONEventBatchEncoderBuilder(config).Build()
	err = encoder.AppendRowChangedEvent(ctx, topic, testEvent, nil)
	require.NotNil(t, err)

	// make sure each batch's `Length` not greater than `max-message-bytes`
	config = config.WithMaxMessageBytes(256)
	encoder = newJSONEventBatchEncoderBuilder(config).Build()
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
	config := NewConfig(config.ProtocolOpen).WithMaxMessageBytes(1048576)
	config.maxBatchSize = 64
	encoder := newJSONEventBatchEncoderBuilder(config).Build()

	testEvent := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
	}

	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(context.Background(), "", testEvent, nil)
		require.Nil(t, err)
	}

	messages := encoder.Build()
	sum := 0
	for _, msg := range messages {
		decoder, err := NewJSONEventBatchDecoder(msg.Key, msg.Value)
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

	cfg := NewConfig(config.ProtocolOpen)
	// Set the max batch size to 2, so that we can test the callback.
	cfg.maxBatchSize = 2
	builder := &openProtocolBatchEncoderBuilder{config: cfg}
	encoder, ok := builder.Build().(*OpenProtocolBatchEncoder)
	require.True(t, ok)
	require.Equal(t, cfg.maxBatchSize, encoder.maxBatchSize)

	count := 0

	row := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "a", Table: "b"},
		Columns:  []*model.Column{{Name: "col1", Type: 1, Value: "aa"}},
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
	config := NewConfig(config.ProtocolOpen).WithMaxMessageBytes(8192)
	config.maxBatchSize = 64
	tester := newDefaultBatchTester()
	tester.testBatchCodec(t, newJSONEventBatchEncoderBuilder(config), NewJSONEventBatchDecoder)
}
