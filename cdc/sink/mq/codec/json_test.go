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
	"sort"
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
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

func sortColumnsArrays(arrays ...[]*model.Column) {
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

func NewDefaultBatchTester() *batchTester {
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
			sortColumnsArrays(row.Columns, row.PreColumns, cs[index].Columns, cs[index].PreColumns)
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
			err := encoder.AppendRowChangedEvent(context.Background(), "", row)
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
	builder := &jsonEventBatchEncoderBuilder{config: config}
	encoder, ok := builder.Build().(*JSONEventBatchEncoder)
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
	// for a single message, the overhead is 36(maximumRecordOverhead) + 8(versionHea) = 44, just can hold it.
	a := 87 + 44
	config := NewConfig(config.ProtocolOpen).WithMaxMessageBytes(a)
	encoder := newJSONEventBatchEncoderBuilder(config).Build()
	err := encoder.AppendRowChangedEvent(ctx, topic, testEvent)
	require.Nil(t, err)

	// cannot hold a single message
	config = config.WithMaxMessageBytes(a - 1)
	encoder = newJSONEventBatchEncoderBuilder(config).Build()
	err = encoder.AppendRowChangedEvent(ctx, topic, testEvent)
	require.NotNil(t, err)

	// make sure each batch's `Length` not greater than `max-message-bytes`
	config = config.WithMaxMessageBytes(256)
	encoder = newJSONEventBatchEncoderBuilder(config).Build()
	for i := 0; i < 10000; i++ {
		err := encoder.AppendRowChangedEvent(ctx, topic, testEvent)
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
		err := encoder.AppendRowChangedEvent(context.Background(), "", testEvent)
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

func TestDefaultEventBatchCodec(t *testing.T) {
	config := NewConfig(config.ProtocolOpen).WithMaxMessageBytes(8192)
	config.maxBatchSize = 64
	tester := NewDefaultBatchTester()
	tester.testBatchCodec(t, newJSONEventBatchEncoderBuilder(config), NewJSONEventBatchDecoder)
}

func TestFormatCol(t *testing.T) {
	t.Parallel()
	row := &mqMessageRow{Update: map[string]column{"test": {
		Type:  mysql.TypeString,
		Value: "测",
	}}}
	rowEncode, err := row.Encode()
	require.Nil(t, err)
	row2 := new(mqMessageRow)
	err = row2.Decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)

	row = &mqMessageRow{Update: map[string]column{"test": {
		Type:  mysql.TypeBlob,
		Value: []byte("测"),
	}}}
	rowEncode, err = row.Encode()
	require.Nil(t, err)
	row2 = new(mqMessageRow)
	err = row2.Decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)
}

func TestNonBinaryStringCol(t *testing.T) {
	t.Parallel()
	col := &model.Column{
		Name:  "test",
		Type:  mysql.TypeString,
		Value: "value",
	}
	jsonCol := column{}
	jsonCol.FromSinkColumn(col)
	row := &mqMessageRow{Update: map[string]column{"test": jsonCol}}
	rowEncode, err := row.Encode()
	require.Nil(t, err)
	row2 := new(mqMessageRow)
	err = row2.Decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)
	jsonCol2 := row2.Update["test"]
	col2 := jsonCol2.ToSinkColumn("test")
	col2.Value = string(col2.Value.([]byte))
	require.Equal(t, col, col2)
}

func TestVarBinaryCol(t *testing.T) {
	t.Parallel()
	col := &model.Column{
		Name:  "test",
		Type:  mysql.TypeString,
		Flag:  model.BinaryFlag,
		Value: []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
	}
	jsonCol := column{}
	jsonCol.FromSinkColumn(col)
	row := &mqMessageRow{Update: map[string]column{"test": jsonCol}}
	rowEncode, err := row.Encode()
	require.Nil(t, err)
	row2 := new(mqMessageRow)
	err = row2.Decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)
	jsonCol2 := row2.Update["test"]
	col2 := jsonCol2.ToSinkColumn("test")
	require.Equal(t, col, col2)
}
