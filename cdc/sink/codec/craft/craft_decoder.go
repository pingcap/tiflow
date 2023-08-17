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
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// batchDecoder decodes the byte of a batch into the original messages.
type batchDecoder struct {
	headers *Headers
	decoder *MessageDecoder
	index   int

	allocator *SliceAllocator
}

// HasNext implements the EventBatchDecoder interface
func (b *batchDecoder) HasNext() (model.MessageType, bool, error) {
	if b.index >= b.headers.Count() {
		return model.MessageTypeUnknown, false, nil
	}
	return b.headers.GetType(b.index), true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *batchDecoder) NextResolvedEvent() (uint64, error) {
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
func (b *batchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
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
func (b *batchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
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
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema: b.headers.GetSchema(b.index),
				Table:  b.headers.GetTable(b.index),
			},
		},
	}
	b.index++
	return event, nil
}

func newBatchDecoder(bits []byte) (codec.EventBatchDecoder, error) {
	decoder := NewBatchDecoderWithAllocator(NewSliceAllocator(64))
	err := decoder.AddKeyValue(nil, bits)
	return decoder, err
}

// NewBatchDecoderWithAllocator creates a new batchDecoder with given allocator.
func NewBatchDecoderWithAllocator(
	allocator *SliceAllocator,
) codec.EventBatchDecoder {
	return &batchDecoder{
		allocator: allocator,
	}
}

// AddKeyValue implements the EventBatchDecoder interface
func (b *batchDecoder) AddKeyValue(_, value []byte) error {
	decoder, err := NewMessageDecoder(value, b.allocator)
	if err != nil {
		return errors.Trace(err)
	}
	headers, err := decoder.Headers()
	if err != nil {
		return errors.Trace(err)
	}
	b.decoder = decoder
	b.headers = headers

	return nil
}
