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
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/config"
)

// BatchEncoder encodes the events into the byte of a batch into craft binary format.
type BatchEncoder struct {
	rowChangedBuffer *RowChangedEventBuffer
	messageBuf       []*common.Message
	callbackBuf      []func()

	config *common.Config

	allocator *SliceAllocator
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (e *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	return common.NewResolvedMsg(
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
	rows, size := e.rowChangedBuffer.AppendRowChangedEvent(ev, e.config.DeleteOnlyHandleKeyColumns)
	if callback != nil {
		e.callbackBuf = append(e.callbackBuf, callback)
	}
	if size > e.config.MaxMessageBytes || rows >= e.config.MaxBatchSize {
		e.flush()
	}
	return nil
}

// EncodeDDLEvent implements the EventBatchEncoder interface
func (e *BatchEncoder) EncodeDDLEvent(ev *model.DDLEvent) (*common.Message, error) {
	return common.NewDDLMsg(config.ProtocolCraft,
		nil, NewDDLEventEncoder(e.allocator, ev).Encode(), ev), nil
}

// Build implements the EventBatchEncoder interface
func (e *BatchEncoder) Build() []*common.Message {
	if e.rowChangedBuffer.Size() > 0 {
		// flush buffered data to message buffer
		e.flush()
	}
	ret := e.messageBuf
	e.messageBuf = make([]*common.Message, 0, 2)
	return ret
}

func (e *BatchEncoder) flush() {
	headers := e.rowChangedBuffer.GetHeaders()
	ts := headers.GetTs(0)
	schema := headers.GetSchema(0)
	table := headers.GetTable(0)
	rowsCnt := e.rowChangedBuffer.RowsCount()
	message := common.NewMsg(config.ProtocolCraft,
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
func NewBatchEncoder(config *common.Config) codec.EventBatchEncoder {
	// 64 is a magic number that come up with these assumptions and manual benchmark.
	// 1. Most table will not have more than 64 columns
	// 2. It only worth allocating slices in batch for slices that's small enough
	return NewBatchEncoderWithAllocator(NewSliceAllocator(64), config)
}

type batchEncoderBuilder struct {
	config *common.Config
}

// Build a BatchEncoder
func (b *batchEncoderBuilder) Build() codec.EventBatchEncoder {
	return NewBatchEncoder(b.config)
}

// NewBatchEncoderBuilder creates a craft batchEncoderBuilder.
func NewBatchEncoderBuilder(config *common.Config) codec.EncoderBuilder {
	return &batchEncoderBuilder{config: config}
}

// NewBatchEncoderWithAllocator creates a new BatchEncoder with given allocator.
func NewBatchEncoderWithAllocator(allocator *SliceAllocator, config *common.Config) codec.EventBatchEncoder {
	return &BatchEncoder{
		allocator:        allocator,
		messageBuf:       make([]*common.Message, 0, 2),
		callbackBuf:      make([]func(), 0),
		rowChangedBuffer: NewRowChangedEventBuffer(allocator),
		config:           config,
	}
}
