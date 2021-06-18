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
	"math"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/codec/craft"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

// CraftEventBatchEncoder encodes the events into the byte of a batch into craft binary format.
type CraftEventBatchEncoder struct {
	rowChangedBuffer *craft.RowChangedEventBuffer
	messageBuf       []*MQMessage

	// configs
	maxMessageSize int
	maxBatchSize   int

	allocator *craft.SliceAllocator
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	return newResolvedMQMessage(ProtocolCraft, nil, craft.NewResolvedEventEncoder(e.allocator, ts).Encode(), ts), nil
}

func (e *CraftEventBatchEncoder) flush() {
	headers := e.rowChangedBuffer.GetHeaders()
	ts := headers.GetTs(0)
	schema := headers.GetSchema(0)
	table := headers.GetTable(0)
	e.messageBuf = append(e.messageBuf, NewMQMessage(ProtocolCraft, nil, e.rowChangedBuffer.Encode(), ts, model.MqMessageTypeRow, &schema, &table))
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) AppendRowChangedEvent(ev *model.RowChangedEvent) (EncoderResult, error) {
	rows, size := e.rowChangedBuffer.AppendRowChangedEvent(ev)
	if size > e.maxMessageSize || rows >= e.maxBatchSize {
		e.flush()
	}
	return EncoderNoOperation, nil
}

// AppendResolvedEvent is no-op
func (e *CraftEventBatchEncoder) AppendResolvedEvent(ts uint64) (EncoderResult, error) {
	return EncoderNoOperation, nil
}

// EncodeDDLEvent implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) EncodeDDLEvent(ev *model.DDLEvent) (*MQMessage, error) {
	return newDDLMQMessage(ProtocolCraft, nil, craft.NewDDLEventEncoder(e.allocator, ev).Encode(), ev), nil
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

// MixedBuild implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) MixedBuild(withVersion bool) []byte {
	panic("Only JsonEncoder supports mixed build")
}

// Size implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) Size() int {
	return e.rowChangedBuffer.Size()
}

// Reset implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) Reset() {
	e.rowChangedBuffer.Reset()
}

// SetParams reads relevant parameters for craft protocol
func (e *CraftEventBatchEncoder) SetParams(params map[string]string) error {
	var err error
	if maxMessageBytes, ok := params["max-message-bytes"]; ok {
		e.maxMessageSize, err = strconv.Atoi(maxMessageBytes)
		if err != nil {
			return cerror.ErrSinkInvalidConfig.Wrap(err)
		}
	} else {
		e.maxMessageSize = DefaultMaxMessageBytes
	}

	if e.maxMessageSize <= 0 || e.maxMessageSize > math.MaxInt32 {
		return cerror.ErrSinkInvalidConfig.Wrap(errors.Errorf("invalid max-message-bytes %d", e.maxMessageSize))
	}

	if maxBatchSize, ok := params["max-batch-size"]; ok {
		e.maxBatchSize, err = strconv.Atoi(maxBatchSize)
		if err != nil {
			return cerror.ErrSinkInvalidConfig.Wrap(err)
		}
	} else {
		e.maxBatchSize = DefaultMaxBatchSize
	}

	if e.maxBatchSize <= 0 || e.maxBatchSize > math.MaxUint16 {
		return cerror.ErrSinkInvalidConfig.Wrap(errors.Errorf("invalid max-batch-size %d", e.maxBatchSize))
	}
	return nil
}

// NewCraftEventBatchEncoder creates a new CraftEventBatchEncoder.
func NewCraftEventBatchEncoder() EventBatchEncoder {
	// 64 is a magic number that come up with these assumptions and manual benchmark.
	// 1. Most table will not have more than 64 columns
	// 2. It only worth allocating slices in batch for slices that's small enough
	return NewCraftEventBatchEncoderWithAllocator(craft.NewSliceAllocator(64))
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
func (b *CraftEventBatchDecoder) HasNext() (model.MqMessageType, bool, error) {
	if b.index >= b.headers.Count() {
		return model.MqMessageTypeUnknown, false, nil
	}
	return b.headers.GetType(b.index), true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *CraftEventBatchDecoder) NextResolvedEvent() (uint64, error) {
	ty, hasNext, err := b.HasNext()
	if err != nil {
		return 0, errors.Trace(err)
	}
	if !hasNext || ty != model.MqMessageTypeResolved {
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
	if !hasNext || ty != model.MqMessageTypeRow {
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
	if !hasNext || ty != model.MqMessageTypeDDL {
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
