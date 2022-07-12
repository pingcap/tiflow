// Copyright 2021 PingCAP, Inc.
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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec/craft"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// CraftEventBatchEncoder encodes the events into the byte of a batch into craft binary format.
type CraftEventBatchEncoder struct {
	rowChangedBuffer *craft.RowChangedEventBuffer
	messageBuf       []*MQMessage

	// configs
	maxMessageBytes int
	maxBatchSize    int

	allocator *craft.SliceAllocator
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	return newResolvedMQMessage(config.ProtocolCraft, nil, craft.NewResolvedEventEncoder(e.allocator, ts).Encode(), ts), nil
}

func (e *CraftEventBatchEncoder) flush() {
	headers := e.rowChangedBuffer.GetHeaders()
	ts := headers.GetTs(0)
	schema := headers.GetSchema(0)
	table := headers.GetTable(0)
	rowsCnt := e.rowChangedBuffer.RowsCount()
	mqMessage := NewMQMessage(config.ProtocolCraft, nil, e.rowChangedBuffer.Encode(), ts, model.MessageTypeRow, &schema, &table)
	mqMessage.SetRowsCount(rowsCnt)
	e.messageBuf = append(e.messageBuf, mqMessage)
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) AppendRowChangedEvent(
	ctx context.Context,
	topic string,
	ev *model.RowChangedEvent,
) error {
	rows, size := e.rowChangedBuffer.AppendRowChangedEvent(ev)
	if size > e.maxMessageBytes || rows >= e.maxBatchSize {
		e.flush()
	}
	return nil
}

// EncodeDDLEvent implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) EncodeDDLEvent(ev *model.DDLEvent) (*MQMessage, error) {
	return newDDLMQMessage(config.ProtocolCraft, nil, craft.NewDDLEventEncoder(e.allocator, ev).Encode(), ev), nil
}

// Build implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) Build() []*MQMessage {
	if e.rowChangedBuffer.Size() > 0 {
		// flush buffered data to message buffer
		e.flush()
	}
	ret := e.messageBuf
	e.messageBuf = make([]*MQMessage, 0, 2)
	return ret
}

// NewCraftEventBatchEncoder creates a new CraftEventBatchEncoder.
func NewCraftEventBatchEncoder() EventBatchEncoder {
	// 64 is a magic number that come up with these assumptions and manual benchmark.
	// 1. Most table will not have more than 64 columns
	// 2. It only worth allocating slices in batch for slices that's small enough
	return NewCraftEventBatchEncoderWithAllocator(craft.NewSliceAllocator(64))
}

type craftEventBatchEncoderBuilder struct {
	config *Config
}

// Build a CraftEventBatchEncoder
func (b *craftEventBatchEncoderBuilder) Build() EventBatchEncoder {
	encoder := NewCraftEventBatchEncoder()
	encoder.(*CraftEventBatchEncoder).maxMessageBytes = b.config.maxMessageBytes
	encoder.(*CraftEventBatchEncoder).maxBatchSize = b.config.maxBatchSize
	return encoder
}

func newCraftEventBatchEncoderBuilder(config *Config) EncoderBuilder {
	return &craftEventBatchEncoderBuilder{config: config}
}

// NewCraftEventBatchEncoderWithAllocator creates a new CraftEventBatchEncoder with given allocator.
func NewCraftEventBatchEncoderWithAllocator(allocator *craft.SliceAllocator) EventBatchEncoder {
	return &CraftEventBatchEncoder{
		allocator:        allocator,
		messageBuf:       make([]*MQMessage, 0, 2),
		rowChangedBuffer: craft.NewRowChangedEventBuffer(allocator),
	}
}

// CraftEventBatchDecoder decodes the byte of a batch into the original messages.
type CraftEventBatchDecoder struct {
	headers *craft.Headers
	decoder *craft.MessageDecoder
	index   int

	allocator *craft.SliceAllocator
}

// HasNext implements the EventBatchDecoder interface
func (b *CraftEventBatchDecoder) HasNext() (model.MessageType, bool, error) {
	if b.index >= b.headers.Count() {
		return model.MessageTypeUnknown, false, nil
	}
	return b.headers.GetType(b.index), true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *CraftEventBatchDecoder) NextResolvedEvent() (uint64, error) {
	ty, hasNext, err := b.HasNext()
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !hasNext || ty != model.MessageTypeResolved {
		return 0, cerror.ErrCraftCodecInvalidData.GenWithStack("not found resolved event message")
	}
	ts := b.headers.GetTs(b.index)
	b.index++
	return ts, nil
}

// NextRowChangedEvent implements the EventBatchDecoder interface
func (b *CraftEventBatchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	ty, hasNext, err := b.HasNext()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !hasNext || ty != model.MessageTypeRow {
		return nil, cerror.ErrCraftCodecInvalidData.GenWithStack("not found row changed event message")
	}
	oldValue, newValue, err := b.decoder.RowChangedEvent(b.index)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ev := &model.RowChangedEvent{}
	if oldValue != nil {
		if ev.PreColumns, err = oldValue.ToModel(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	if newValue != nil {
		if ev.Columns, err = newValue.ToModel(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	ev.CommitTs = b.headers.GetTs(b.index)
	ev.Table = &model.TableName{
		Schema: b.headers.GetSchema(b.index),
		Table:  b.headers.GetTable(b.index),
	}
	partition := b.headers.GetPartition(b.index)
	if partition >= 0 {
		ev.Table.TableID = partition
		ev.Table.IsPartition = true
	}
	b.index++
	return ev, nil
}

// NextDDLEvent implements the EventBatchDecoder interface
func (b *CraftEventBatchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	ty, hasNext, err := b.HasNext()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !hasNext || ty != model.MessageTypeDDL {
		return nil, cerror.ErrCraftCodecInvalidData.GenWithStack("not found ddl event message")
	}
	ddlType, query, err := b.decoder.DDLEvent(b.index)
	if err != nil {
		return nil, errors.Trace(err)
	}
	event := &model.DDLEvent{
		CommitTs: b.headers.GetTs(b.index),
		Query:    query,
		Type:     ddlType,
		TableInfo: &model.SimpleTableInfo{
			Schema: b.headers.GetSchema(b.index),
			Table:  b.headers.GetTable(b.index),
		},
	}
	b.index++
	return event, nil
}

// NewCraftEventBatchDecoder creates a new CraftEventBatchDecoder.
func NewCraftEventBatchDecoder(bits []byte) (EventBatchDecoder, error) {
	return NewCraftEventBatchDecoderWithAllocator(bits, craft.NewSliceAllocator(64))
}

// NewCraftEventBatchDecoderWithAllocator creates a new CraftEventBatchDecoder with given allocator.
func NewCraftEventBatchDecoderWithAllocator(bits []byte, allocator *craft.SliceAllocator) (EventBatchDecoder, error) {
	decoder, err := craft.NewMessageDecoder(bits, allocator)
	if err != nil {
		return nil, errors.Trace(err)
	}
	headers, err := decoder.Headers()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &CraftEventBatchDecoder{
		headers:   headers,
		decoder:   decoder,
		allocator: allocator,
	}, nil
}
