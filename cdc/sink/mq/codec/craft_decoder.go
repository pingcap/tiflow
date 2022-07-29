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
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec/craft"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// craftBatchDecoder decodes the byte of a batch into the original messages.
type craftBatchDecoder struct {
	headers *craft.Headers
	decoder *craft.MessageDecoder
	index   int

	allocator *craft.SliceAllocator
}

// HasNext implements the EventBatchDecoder interface
func (b *craftBatchDecoder) HasNext() (model.MessageType, bool, error) {
	if b.index >= b.headers.Count() {
		return model.MessageTypeUnknown, false, nil
	}
	return b.headers.GetType(b.index), true, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
func (b *craftBatchDecoder) NextResolvedEvent() (uint64, error) {
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
func (b *craftBatchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	ty, hasNext, err := b.HasNext()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !hasNext || ty != model.MessageTypeRow {
		return nil,
			cerror.ErrCraftCodecInvalidData.GenWithStack("not found row changed event message")
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
func (b *craftBatchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
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

// newCraftBatchDecoder creates a new craftBatchDecoder.
func newCraftBatchDecoder(bits []byte) (EventBatchDecoder, error) {
	return newCraftBatchDecoderWithAllocator(bits, craft.NewSliceAllocator(64))
}

// newCraftBatchDecoderWithAllocator creates a new craftBatchDecoder with given allocator.
func newCraftBatchDecoderWithAllocator(
	bits []byte, allocator *craft.SliceAllocator,
) (EventBatchDecoder, error) {
	decoder, err := craft.NewMessageDecoder(bits, allocator)
	if err != nil {
		return nil, errors.Trace(err)
	}
	headers, err := decoder.Headers()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &craftBatchDecoder{
		headers:   headers,
		decoder:   decoder,
		allocator: allocator,
	}, nil
}
