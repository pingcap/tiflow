// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.orglicensesLICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package craft

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	common2 "github.com/pingcap/tiflow/pkg/sink/codec/common"
)

// BatchEncoder encodes the events into the byte of a batch into craft binary format.
type BatchEncoder struct {
	rowChangedBuffer *RowChangedEventBuffer
	messageBuf       []*common2.Message
	callbackBuf      []func()

	// configs
	MaxMessageBytes int
	MaxBatchSize    int

	allocator *SliceAllocator
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (e *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*common2.Message, error) {
	return common2.NewResolvedMsg(
		config.ProtocolCraft, nil,
		NewResolvedEventEncoder(e.allocator, ts).Encode(), ts), nil
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (e *BatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	ev *model.RowChangedEvent,
	callback func(),
) error {
	rows, size := e.rowChangedBuffer.AppendRowChangedEvent(ev)
	if callback != nil {
		e.callbackBuf = append(e.callbackBuf, callback)
	}
	if size > e.MaxMessageBytes || rows >= e.MaxBatchSize {
		e.flush()
	}
	return nil
}

// EncodeDDLEvent implements the EventBatchEncoder interface
func (e *BatchEncoder) EncodeDDLEvent(ev *model.DDLEvent) (*common2.Message, error) {
	return common2.NewDDLMsg(config.ProtocolCraft,
		nil, NewDDLEventEncoder(e.allocator, ev).Encode(), ev), nil
}

// Build implements the EventBatchEncoder interface
func (e *BatchEncoder) Build() []*common2.Message {
	if e.rowChangedBuffer.Size() > 0 {
		// flush buffered data to message buffer
		e.flush()
	}
	ret := e.messageBuf
	e.messageBuf = make([]*common2.Message, 0, 2)
	return ret
}

func (e *BatchEncoder) flush() {
	headers := e.rowChangedBuffer.GetHeaders()
	ts := headers.GetTs(0)
	schema := headers.GetSchema(0)
	table := headers.GetTable(0)
	rowsCnt := e.rowChangedBuffer.RowsCount()
	message := common2.NewMsg(config.ProtocolCraft,
		nil, e.rowChangedBuffer.Encode(), ts, model.MessageTypeRow, &schema, &table)
	message.SetRowsCount(rowsCnt)
	if len(e.callbackBuf) != 0 && len(e.callbackBuf) == rowsCnt {
		callbacks := e.callbackBuf
		message.Callback = func() {
			for _, cb := range callbacks {
				cb()
			}
		}
		e.callbackBuf = make([]func(), 0)
	}
	e.messageBuf = append(e.messageBuf, message)
}

// NewBatchEncoder creates a new BatchEncoder.
func NewBatchEncoder() codec.EventBatchEncoder {
	// 64 is a magic number that come up with these assumptions and manual benchmark.
	// 1. Most table will not have more than 64 columns
	// 2. It only worth allocating slices in batch for slices that's small enough
	return NewBatchEncoderWithAllocator(NewSliceAllocator(64))
}

type batchEncoderBuilder struct {
	config *common2.Config
}

// Build a BatchEncoder
func (b *batchEncoderBuilder) Build() codec.EventBatchEncoder {
	encoder := NewBatchEncoder()
	encoder.(*BatchEncoder).MaxMessageBytes = b.config.MaxMessageBytes
	encoder.(*BatchEncoder).MaxBatchSize = b.config.MaxBatchSize
	return encoder
}

// NewBatchEncoderBuilder creates a craft batchEncoderBuilder.
func NewBatchEncoderBuilder(config *common2.Config) codec.EncoderBuilder {
	return &batchEncoderBuilder{config: config}
}

// NewBatchEncoderWithAllocator creates a new BatchEncoder with given allocator.
func NewBatchEncoderWithAllocator(allocator *SliceAllocator) codec.EventBatchEncoder {
	return &BatchEncoder{
		allocator:        allocator,
		messageBuf:       make([]*common2.Message, 0, 2),
		callbackBuf:      make([]func(), 0),
		rowChangedBuffer: NewRowChangedEventBuffer(allocator),
	}
}
