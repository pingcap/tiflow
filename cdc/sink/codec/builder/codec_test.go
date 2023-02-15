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

package builder

import (
	"bytes"
	"compress/zlib"
	"context"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sink/codec/craft"
	"github.com/pingcap/tiflow/cdc/sink/codec/internal"
	"github.com/pingcap/tiflow/cdc/sink/codec/open"
	"github.com/pingcap/tiflow/proto/benchmark"
	"github.com/stretchr/testify/require"
)

var (
	codecBenchmarkRowChanges = model.UnboundRowChangedEvents(internal.CodecRowCases[1])

	codecCraftEncodedRowChanges = []*common.Message{}
	codecJSONEncodedRowChanges  = []*common.Message{}
	codecPB1EncodedRowChanges   = []*common.Message{}
	codecPB2EncodedRowChanges   = []*common.Message{}

	codecTestSliceAllocator = craft.NewSliceAllocator(512)
)

func checkCompressedSize(messages []*common.Message) (int, int) {
	var buff bytes.Buffer
	writer := zlib.NewWriter(&buff)
	originalSize := 0
	for _, message := range messages {
		originalSize += len(message.Key) + len(message.Value)
		if len(message.Key) > 0 {
			_, _ = writer.Write(message.Key)
		}
		_, _ = writer.Write(message.Value)
	}
	writer.Close()
	return originalSize, buff.Len()
}

func encodeRowCase(t *testing.T, encoder codec.EventBatchEncoder, events []*model.RowChangedEvent) []*common.Message {
	msg, err := codecEncodeRowCase(encoder, events)
	require.Nil(t, err)
	return msg
}

func TestJsonVsCraftVsPB(t *testing.T) {
	t.Parallel()
	t.Logf("| case | craft size | json size | protobuf 1 size | protobuf 2 size | craft compressed | json compressed | protobuf 1 compressed | protobuf 2 compressed |")
	t.Logf("| :---- | :--------- | :-------- | :-------------- | :-------------- | :--------------- | :-------------- | :-------------------- | :-------------------- |")
	for i, bcs := range internal.CodecRowCases {
		if len(bcs) == 0 {
			continue
		}
		cs := model.UnboundRowChangedEvents(bcs)

		craftEncoder := craft.NewBatchEncoder()
		craftEncoder.(*craft.BatchEncoder).MaxMessageBytes = 8192
		craftEncoder.(*craft.BatchEncoder).MaxBatchSize = 64
		craftMessages := encodeRowCase(t, craftEncoder, cs)

		jsonEncoder := open.NewBatchEncoder()
		jsonEncoder.(*open.BatchEncoder).MaxMessageBytes = 8192
		jsonEncoder.(*open.BatchEncoder).MaxBatchSize = 64
		jsonMessages := encodeRowCase(t, jsonEncoder, cs)

		protobuf1Messages := codecEncodeRowChangedPB1ToMessage(cs)
		protobuf2Messages := codecEncodeRowChangedPB2ToMessage(cs)
		craftOriginal, craftCompressed := checkCompressedSize(craftMessages)
		jsonOriginal, jsonCompressed := checkCompressedSize(jsonMessages)
		protobuf1Original, protobuf1Compressed := checkCompressedSize(protobuf1Messages)
		protobuf2Original, protobuf2Compressed := checkCompressedSize(protobuf2Messages)
		t.Logf("| case %d | %d | %d (%d%%)+ | %d (%d%%)+ | %d (%d%%)+ | %d | %d (%d%%)+ | %d (%d%%)+ | %d (%d%%)+ |", i,
			craftOriginal, jsonOriginal, 100*jsonOriginal/craftOriginal-100,
			protobuf1Original, 100*protobuf1Original/craftOriginal-100,
			protobuf2Original, 100*protobuf2Original/craftOriginal-100,
			craftCompressed, jsonCompressed, 100*jsonCompressed/craftCompressed-100,
			protobuf1Compressed, 100*protobuf1Compressed/craftCompressed-100,
			protobuf2Compressed, 100*protobuf2Compressed/craftCompressed-100)
	}
}

func codecEncodeKeyPB(event *model.RowChangedEvent) []byte {
	key := &benchmark.Key{
		Ts:        event.CommitTs,
		Schema:    event.Table.Schema,
		Table:     event.Table.Table,
		RowId:     event.RowID,
		Partition: 0,
	}
	if b, err := key.Marshal(); err != nil {
		panic(err)
	} else {
		return b
	}
}

func codecEncodeColumnPB(column *model.Column, colv model.ColumnValue) *benchmark.Column {
	return &benchmark.Column{
		Name:  column.Name,
		Type:  uint32(column.Type),
		Flag:  uint32(column.Flag),
		Value: craft.EncodeTiDBType(codecTestSliceAllocator, column.Type, column.Flag, colv.Value),
	}
}

func codecEncodeColumnsPB(columns []*model.Column, colvals []model.ColumnValue) []*benchmark.Column {
	converted := make([]*benchmark.Column, len(columns))
	for i, column := range columns {
		converted[i] = codecEncodeColumnPB(column, colvals[i])
	}
	return converted
}

func codecEncodeRowChangedPB(event *model.RowChangedEvent) []byte {
	rowChanged := &benchmark.RowChanged{
		OldValue: codecEncodeColumnsPB(event.PreColumns, event.PreColumnValues),
		NewValue: codecEncodeColumnsPB(event.Columns, event.ColumnValues),
	}
	if b, err := rowChanged.Marshal(); err != nil {
		panic(err)
	} else {
		return b
	}
}

func codecEncodeRowChangedPB1ToMessage(events []*model.RowChangedEvent) []*common.Message {
	result := make([]*common.Message, len(events))
	for i, event := range events {
		result[i] = &common.Message{
			Key:   codecEncodeKeyPB(event),
			Value: codecEncodeRowChangedPB(event),
		}
	}
	return result
}

func codecEncodeRowChangedPB2ToMessage(events []*model.RowChangedEvent) []*common.Message {
	return []*common.Message{{
		Key:   codecEncodeKeysPB2(events),
		Value: codecEncodeRowChangedPB2(events),
	}}
}

func codecEncodeKeysPB2(events []*model.RowChangedEvent) []byte {
	converted := &benchmark.KeysColumnar{}

	for _, event := range events {
		converted.Ts = append(converted.Ts, event.CommitTs)
		converted.Schema = append(converted.Schema, event.Table.Schema)
		converted.Table = append(converted.Table, event.Table.Table)
		converted.RowId = append(converted.RowId, event.RowID)
		converted.Partition = append(converted.Partition, 0)
	}

	if b, err := converted.Marshal(); err != nil {
		panic(err)
	} else {
		return b
	}
}

func codecEncodeColumnsPB2(columns []*model.Column, colvals []model.ColumnValue) *benchmark.ColumnsColumnar {
	converted := &benchmark.ColumnsColumnar{
		Name:  make([]string, len(columns)),
		Type:  make([]uint32, len(columns)),
		Flag:  make([]uint32, len(columns)),
		Value: make([][]byte, len(columns)),
	}
	for i, column := range columns {
		converted.Name[i] = column.Name
		converted.Type[i] = uint32(column.Type)
		converted.Flag[i] = uint32(column.Flag)
		converted.Value[i] = craft.EncodeTiDBType(codecTestSliceAllocator, column.Type, column.Flag, colvals[i].Value)
	}
	return converted
}

func codecEncodeRowChangedPB2(events []*model.RowChangedEvent) []byte {
	rowChanged := &benchmark.RowChangedColumnar{}
	for _, event := range events {
		rowChanged.OldValue = append(rowChanged.OldValue, codecEncodeColumnsPB2(event.PreColumns, event.PreColumnValues))
		rowChanged.NewValue = append(rowChanged.NewValue, codecEncodeColumnsPB2(event.Columns, event.ColumnValues))
	}
	if b, err := rowChanged.Marshal(); err != nil {
		panic(err)
	} else {
		return b
	}
}

func codecEncodeRowCase(encoder codec.EventBatchEncoder, events []*model.RowChangedEvent) ([]*common.Message, error) {
	for _, event := range events {
		err := encoder.AppendRowChangedEvent(context.Background(), "", event, nil)
		if err != nil {
			return nil, err
		}
	}

	if len(events) > 0 {
		return encoder.Build(), nil
	}
	return nil, nil
}

func init() {
	var err error
	encoder := craft.NewBatchEncoder()
	encoder.(*craft.BatchEncoder).MaxMessageBytes = 8192
	encoder.(*craft.BatchEncoder).MaxBatchSize = 64
	if codecCraftEncodedRowChanges, err = codecEncodeRowCase(encoder, codecBenchmarkRowChanges); err != nil {
		panic(err)
	}

	encoder = open.NewBatchEncoder()
	encoder.(*open.BatchEncoder).MaxMessageBytes = 8192
	encoder.(*open.BatchEncoder).MaxBatchSize = 64
	if codecJSONEncodedRowChanges, err = codecEncodeRowCase(encoder, codecBenchmarkRowChanges); err != nil {
		panic(err)
	}
	codecPB1EncodedRowChanges = codecEncodeRowChangedPB1ToMessage(codecBenchmarkRowChanges)
	codecPB2EncodedRowChanges = codecEncodeRowChangedPB2ToMessage(codecBenchmarkRowChanges)
}

func BenchmarkCraftEncoding(b *testing.B) {
	allocator := craft.NewSliceAllocator(128)
	encoder := craft.NewBatchEncoderWithAllocator(allocator)
	encoder.(*craft.BatchEncoder).MaxMessageBytes = 8192
	encoder.(*craft.BatchEncoder).MaxBatchSize = 64
	for i := 0; i < b.N; i++ {
		_, _ = codecEncodeRowCase(encoder, codecBenchmarkRowChanges)
	}
}

func BenchmarkJsonEncoding(b *testing.B) {
	encoder := open.NewBatchEncoder()
	encoder.(*open.BatchEncoder).MaxMessageBytes = 8192
	encoder.(*open.BatchEncoder).MaxBatchSize = 64
	for i := 0; i < b.N; i++ {
		_, _ = codecEncodeRowCase(encoder, codecBenchmarkRowChanges)
	}
}

func BenchmarkProtobuf1Encoding(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = codecEncodeRowChangedPB1ToMessage(codecBenchmarkRowChanges)
	}
}

func BenchmarkProtobuf2Encoding(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = codecEncodeRowChangedPB2ToMessage(codecBenchmarkRowChanges)
	}
}

func BenchmarkCraftDecoding(b *testing.B) {
	allocator := craft.NewSliceAllocator(128)
	for i := 0; i < b.N; i++ {
		for _, message := range codecCraftEncodedRowChanges {
			if decoder, err := craft.NewBatchDecoderWithAllocator(
				message.Value, allocator); err != nil {
				panic(err)
			} else {
				for {
					if _, hasNext, err := decoder.HasNext(); err != nil {
						panic(err)
					} else if hasNext {
						_, _ = decoder.NextRowChangedEvent()
					} else {
						break
					}
				}
			}
		}
	}
}

func BenchmarkJsonDecoding(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, message := range codecJSONEncodedRowChanges {
			if decoder, err := open.NewBatchDecoder(message.Key, message.Value); err != nil {
				panic(err)
			} else {
				for {
					if _, hasNext, err := decoder.HasNext(); err != nil {
						panic(err)
					} else if hasNext {
						_, _ = decoder.NextRowChangedEvent()
					} else {
						break
					}
				}
			}
		}
	}
}

func codecDecodeRowChangedPB1(columns []*benchmark.Column) ([]*model.Column, []model.ColumnValue) {
	if len(columns) == 0 {
		return nil, nil
	}

	x := make([]*model.Column, len(columns))
	y := make([]model.ColumnValue, len(columns))
	for i, column := range columns {
		value, _ := craft.DecodeTiDBType(byte(column.Type), model.ColumnFlagType(column.Flag), column.Value)
		x[i] = &model.Column{
			Name: column.Name,
			Type: byte(column.Type),
			Flag: model.ColumnFlagType(column.Flag),
		}
		y[i] = model.ColumnValue{Value: value}
	}

	return x, y
}

func benchmarkProtobuf1Decoding() []*model.RowChangedEvent {
	result := make([]*model.RowChangedEvent, 0, 4)
	for _, message := range codecPB1EncodedRowChanges {
		key := &benchmark.Key{}
		if err := key.Unmarshal(message.Key); err != nil {
			panic(err)
		}
		value := &benchmark.RowChanged{}
		if err := value.Unmarshal(message.Value); err != nil {
			panic(err)
		}
		ev := &model.RowChangedEvent{}
		ev.PreColumns, ev.PreColumnValues = codecDecodeRowChangedPB1(value.OldValue)
		ev.Columns, ev.ColumnValues = codecDecodeRowChangedPB1(value.NewValue)
		ev.CommitTs = key.Ts
		ev.Table = &model.TableName{
			Schema: key.Schema,
			Table:  key.Table,
		}
		if key.Partition >= 0 {
			ev.Table.TableID = key.Partition
			ev.Table.IsPartition = true
		}
		result = append(result, ev)
	}
	return result
}

func BenchmarkProtobuf1Decoding(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, row := range benchmarkProtobuf1Decoding() {
			_ = row
		}
	}
}

func codecDecodeRowChangedPB2(columns *benchmark.ColumnsColumnar) ([]*model.Column, []model.ColumnValue) {
	if len(columns.Value) == 0 {
		return nil, nil
	}

	x := make([]*model.Column, len(columns.Value))
	y := make([]model.ColumnValue, len(columns.Value))
	for i, value := range columns.Value {
		v, _ := craft.DecodeTiDBType(byte(columns.Type[i]), model.ColumnFlagType(columns.Flag[i]), value)
		x[i] = &model.Column{
			Name: columns.Name[i],
			Type: byte(columns.Type[i]),
			Flag: model.ColumnFlagType(columns.Flag[i]),
		}
		y[i] = model.ColumnValue{Value: v}
	}
	return x, y
}

func benchmarkProtobuf2Decoding() []*model.RowChangedEvent {
	result := make([]*model.RowChangedEvent, 0, 4)
	for _, message := range codecPB2EncodedRowChanges {
		keys := &benchmark.KeysColumnar{}
		if err := keys.Unmarshal(message.Key); err != nil {
			panic(err)
		}
		values := &benchmark.RowChangedColumnar{}
		if err := values.Unmarshal(message.Value); err != nil {
			panic(err)
		}

		for i, ts := range keys.Ts {
			ev := &model.RowChangedEvent{}
			if len(values.OldValue) > i {
				ev.PreColumns, ev.PreColumnValues = codecDecodeRowChangedPB2(values.OldValue[i])
			}
			if len(values.NewValue) > i {
				ev.Columns, ev.ColumnValues = codecDecodeRowChangedPB2(values.NewValue[i])
			}
			ev.CommitTs = ts
			ev.Table = &model.TableName{
				Schema: keys.Schema[i],
				Table:  keys.Table[i],
			}
			if keys.Partition[i] >= 0 {
				ev.Table.TableID = keys.Partition[i]
				ev.Table.IsPartition = true
			}
			result = append(result, ev)
		}
	}
	return result
}

func BenchmarkProtobuf2Decoding(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, row := range benchmarkProtobuf2Decoding() {
			_ = row
		}
	}
}
