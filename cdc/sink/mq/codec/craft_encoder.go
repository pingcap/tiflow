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

package codec

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec/craft"
	"github.com/pingcap/tiflow/pkg/config"
)

// craftBatchEncoder encodes the events into the byte of a batch into craft binary format.
type craftBatchEncoder struct {
	rowChangedBuffer *craft.RowChangedEventBuffer
	messageBuf       []*MQMessage
	callbackBuf      []func()

	// configs
	maxMessageBytes int
	maxBatchSize    int

	allocator *craft.SliceAllocator
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (e *craftBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	return newResolvedMsg(
		config.ProtocolCraft, nil,
		craft.NewResolvedEventEncoder(e.allocator, ts).Encode(), ts), nil
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (e *craftBatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	ev *model.RowChangedEvent,
	callback func(),
) error {
	rows, size := e.rowChangedBuffer.AppendRowChangedEvent(ev)
	if callback != nil {
		e.callbackBuf = append(e.callbackBuf, callback)
	}
	if size > e.maxMessageBytes || rows >= e.maxBatchSize {
		e.flush()
	}
	return nil
}

// EncodeDDLEvent implements the EventBatchEncoder interface
func (e *craftBatchEncoder) EncodeDDLEvent(ev *model.DDLEvent) (*MQMessage, error) {
	return newDDLMsg(config.ProtocolCraft,
		nil, craft.NewDDLEventEncoder(e.allocator, ev).Encode(), ev), nil
}

// Build implements the EventBatchEncoder interface
func (e *craftBatchEncoder) Build() []*MQMessage {
	if e.rowChangedBuffer.Size() > 0 {
		// flush buffered data to message buffer
		e.flush()
	}
	ret := e.messageBuf
	e.messageBuf = make([]*MQMessage, 0, 2)
	return ret
}

func (e *craftBatchEncoder) flush() {
	headers := e.rowChangedBuffer.GetHeaders()
	ts := headers.GetTs(0)
	schema := headers.GetSchema(0)
	table := headers.GetTable(0)
	rowsCnt := e.rowChangedBuffer.RowsCount()
	mqMessage := newMsg(config.ProtocolCraft,
		nil, e.rowChangedBuffer.Encode(), ts, model.MessageTypeRow, &schema, &table)
	mqMessage.SetRowsCount(rowsCnt)
	if len(e.callbackBuf) != 0 && len(e.callbackBuf) == rowsCnt {
		callbacks := e.callbackBuf
		mqMessage.Callback = func() {
			for _, cb := range callbacks {
				cb()
			}
		}
		e.callbackBuf = make([]func(), 0)
	}
	e.messageBuf = append(e.messageBuf, mqMessage)
}

// newCraftBatchEncoder creates a new craftBatchEncoder.
func newCraftBatchEncoder() EventBatchEncoder {
	// 64 is a magic number that come up with these assumptions and manual benchmark.
	// 1. Most table will not have more than 64 columns
	// 2. It only worth allocating slices in batch for slices that's small enough
	return newCraftBatchEncoderWithAllocator(craft.NewSliceAllocator(64))
}

type craftBatchEncoderBuilder struct {
	config *Config
}

// Build a craftBatchEncoder
func (b *craftBatchEncoderBuilder) Build() EventBatchEncoder {
	encoder := newCraftBatchEncoder()
	encoder.(*craftBatchEncoder).maxMessageBytes = b.config.maxMessageBytes
	encoder.(*craftBatchEncoder).maxBatchSize = b.config.maxBatchSize
	return encoder
}

func newCraftBatchEncoderBuilder(config *Config) EncoderBuilder {
	return &craftBatchEncoderBuilder{config: config}
}

// newCraftBatchEncoderWithAllocator creates a new craftBatchEncoder with given allocator.
func newCraftBatchEncoderWithAllocator(allocator *craft.SliceAllocator) EventBatchEncoder {
	return &craftBatchEncoder{
		allocator:        allocator,
		messageBuf:       make([]*MQMessage, 0, 2),
		callbackBuf:      make([]func(), 0),
		rowChangedBuffer: craft.NewRowChangedEventBuffer(allocator),
	}
}
