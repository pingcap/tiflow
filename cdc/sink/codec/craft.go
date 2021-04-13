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
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

const (
	// CraftVersion1 represents the version of craft format
	CraftVersion1 uint64 = 1

	// default buffer size
	craftDefaultBufferCapacity = 512

	// Column group types
	craftColumnGroupTypeDelete = 0x3
	craftColumnGroupTypeOld    = 0x2
	craftColumnGroupTypeNew    = 0x1

	// Size tables index
	craftKeySizeTableIndex              = 0
	craftValueSizeTableIndex            = 1
	craftColumnGroupSizeTableStartIndex = 2
)

// CraftEventBatchEncoder encodes the events into the byte of a batch into craft binary format.
type CraftEventBatchEncoder struct {
	rowChangedBuffer *craftRowChangedEventBuffer
	messageBuf       []*MQMessage

	// configs
	maxMessageSize int
	maxBatchSize   int
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) EncodeCheckpointEvent(ts uint64) (*MQMessage, error) {
	return newResolvedMQMessage(ProtocolCraft, nil, newCraftResolvedEventEncoder(ts).encode(), ts), nil
}

func (e *CraftEventBatchEncoder) flush() {
	keys := e.rowChangedBuffer.getKeys()
	ts := keys.getTs(0)
	schema := keys.getSchema(0)
	table := keys.getTable(0)
	e.messageBuf = append(e.messageBuf, NewMQMessage(ProtocolCraft, nil, e.rowChangedBuffer.encode(), ts, model.MqMessageTypeRow, &schema, &table))
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) AppendRowChangedEvent(ev *model.RowChangedEvent) (EncoderResult, error) {
	rows, size := e.rowChangedBuffer.appendRowChangedEvent(ev)
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
	return newDDLMQMessage(ProtocolCraft, nil, newCraftDDLEventEncoder(ev).encode(), ev), nil
}

// Build implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) Build() []*MQMessage {
	if e.rowChangedBuffer.size() > 0 {
		// flush buffered data to message buffer
		e.flush()
	}
	ret := e.messageBuf
	e.messageBuf = make([]*MQMessage, 0)
	return ret
}

// MixedBuild implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) MixedBuild(withVersion bool) []byte {
	panic("Only JsonEncoder supports mixed build")
}

// Size implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) Size() int {
	return e.rowChangedBuffer.size()
}

// Reset implements the EventBatchEncoder interface
func (e *CraftEventBatchEncoder) Reset() {
	e.rowChangedBuffer.reset()
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
	return &CraftEventBatchEncoder{
		rowChangedBuffer: &craftRowChangedEventBuffer{
			keys: &craftColumnarKeys{},
		},
	}
}

// CraftEventBatchDecoder decodes the byte of a batch into the original messages.
type CraftEventBatchDecoder struct {
	keys    *craftColumnarKeys
	decoder *craftMessageDecoder
	index   int
}

// HasNext implements the EventBatchDecoder interface
func (b *CraftEventBatchDecoder) HasNext() (model.MqMessageType, bool, error) {
	if b.index >= b.keys.count {
		return model.MqMessageTypeUnknown, false, nil
	}
	return b.keys.getType(b.index), true, nil
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
	ts := b.keys.getTs(b.index)
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
		return nil, cerror.ErrCraftCodecInvalidData.GenWithStack("not found resolved event message")
	}
	old, new, err := b.decoder.decodeRowChangedEvent(b.index)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ev := &model.RowChangedEvent{}
	if old != nil {
		if ev.PreColumns, err = old.toModel(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	if new != nil {
		if ev.Columns, err = new.toModel(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	ev.CommitTs = b.keys.getTs(b.index)
	ev.Table = &model.TableName{
		Schema: b.keys.getSchema(b.index),
		Table:  b.keys.getTable(b.index),
	}
	partition := b.keys.getPartition(b.index)
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
		return nil, cerror.ErrCraftCodecInvalidData.GenWithStack("not found resolved event message")
	}
	ddlType, query, err := b.decoder.decodeDDLEvent(b.index)
	if err != nil {
		return nil, errors.Trace(err)
	}
	event := &model.DDLEvent{
		CommitTs: b.keys.getTs(b.index),
		Query:    query,
		Type:     ddlType,
		TableInfo: &model.SimpleTableInfo{
			Schema: b.keys.getSchema(b.index),
			Table:  b.keys.getTable(b.index),
		},
	}
	b.index++
	return event, nil
}

// NewCraftEventBatchDecoder creates a new CraftEventBatchDecoder.
func NewCraftEventBatchDecoder(bits []byte) (EventBatchDecoder, error) {
	decoder, err := newCraftMessageDecoder(bits)
	if err != nil {
		return nil, errors.Trace(err)
	}
	keys, err := decoder.decodeKeys()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &CraftEventBatchDecoder{
		keys:    keys,
		decoder: decoder,
	}, nil
}
