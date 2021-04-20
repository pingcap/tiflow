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

package craft

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
)

const (
	// Version1 represents the version of craft format
	Version1 uint64 = 1

	// default buffer size
	DefaultBufferCapacity = 1024

	// Column group types
	columnGroupTypeOld = 0x2
	columnGroupTypeNew = 0x1

	// Size tables index
	headerSizeTableIndex           = 0
	bodySizeTableIndex             = 1
	columnGroupSizeTableStartIndex = 2
)

/// Headers in columnar layout
type Headers struct {
	ts        []uint64
	ty        []uint64
	rowID     []int64
	partition []int64
	schema    []*string
	table     []*string

	count int
}

func (h *Headers) Count() int {
	return h.count
}

func (h *Headers) encode(bits []byte) []byte {
	bits = encodeDeltaUvarintChunk(bits, h.ts[:h.count])
	bits = encodeDeltaUvarintChunk(bits, h.ty[:h.count])
	bits = encodeDeltaVarintChunk(bits, h.rowID[:h.count])
	bits = encodeDeltaVarintChunk(bits, h.partition[:h.count])
	bits = encodeNullableStringChunk(bits, h.schema[:h.count])
	bits = encodeNullableStringChunk(bits, h.table[:h.count])
	return bits
}

func (h *Headers) appendHeader(allocator *SliceAllocator, ts, ty uint64, rowID, partition int64, schema, table *string) int {
	idx := h.count
	if idx+1 > len(h.ty) {
		size := newBufferSize(idx)
		h.ts = allocator.resizeUint64Slice(h.ts, size)
		h.ty = allocator.resizeUint64Slice(h.ty, size)
		h.rowID = allocator.resizeInt64Slice(h.rowID, size)
		h.partition = allocator.resizeInt64Slice(h.partition, size)
		h.schema = allocator.resizeNullableStringSlice(h.schema, size)
		h.table = allocator.resizeNullableStringSlice(h.table, size)
	}
	h.ts[idx] = ts
	h.ty[idx] = ty
	h.rowID[idx] = rowID
	h.partition[idx] = partition
	h.schema[idx] = schema
	h.table[idx] = table
	h.count++

	return 32 + len(*schema) + len(*table) /* 4 64-bits integers and two bytes array */
}

func (h *Headers) reset() {
	h.count = 0
}

func (h *Headers) GetType(index int) model.MqMessageType {
	return model.MqMessageType(h.ty[index])
}

func (h *Headers) GetTs(index int) uint64 {
	return h.ts[index]
}

func (h *Headers) GetPartition(index int) int64 {
	return h.partition[index]
}

func (h *Headers) GetSchema(index int) string {
	if h.schema[index] != nil {
		return *h.schema[index]
	}
	return ""
}

func (h *Headers) GetTable(index int) string {
	if h.table[index] != nil {
		return *h.table[index]
	}
	return ""
}

func decodeHeaders(bits []byte, numHeaders int, allocator *SliceAllocator) (*Headers, error) {
	var ts, ty []uint64
	var rowID, partition []int64
	var schema, table []*string
	var err error
	if bits, ts, err = decodeDeltaUvarintChunk(bits, numHeaders, allocator); err != nil {
		return nil, errors.Trace(err)
	}
	if bits, ty, err = decodeDeltaUvarintChunk(bits, numHeaders, allocator); err != nil {
		return nil, errors.Trace(err)
	}
	if bits, rowID, err = decodeDeltaVarintChunk(bits, numHeaders, allocator); err != nil {
		return nil, errors.Trace(err)
	}
	if bits, partition, err = decodeDeltaVarintChunk(bits, numHeaders, allocator); err != nil {
		return nil, errors.Trace(err)
	}
	if bits, schema, err = decodeNullableStringChunk(bits, numHeaders, allocator); err != nil {
		return nil, errors.Trace(err)
	}
	if _, table, err = decodeNullableStringChunk(bits, numHeaders, allocator); err != nil {
		return nil, errors.Trace(err)
	}
	return &Headers{
		ts:        ts,
		ty:        ty,
		rowID:     rowID,
		partition: partition,
		schema:    schema,
		table:     table,
		count:     numHeaders,
	}, nil
}

/// Column group in columnar layout
type columnGroup struct {
	ty     byte
	names  []string
	types  []uint64
	flags  []uint64
	values [][]byte
}

func (g *columnGroup) encode(bits []byte) []byte {
	bits = append(bits, g.ty)
	bits = encodeUvarint(bits, uint64(len(g.names)))
	bits = encodeStringChunk(bits, g.names)
	bits = encodeUvarintChunk(bits, g.types)
	bits = encodeUvarintChunk(bits, g.flags)
	bits = encodeNullableBytesChunk(bits, g.values)
	return bits
}

func (g *columnGroup) ToModel() ([]*model.Column, error) {
	columns := make([]*model.Column, len(g.names))
	for i, name := range g.names {
		ty := byte(g.types[i])
		flag := model.ColumnFlagType(g.flags[i])
		value, err := DecodeTiDBType(ty, flag, g.values[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		columns[i] = &model.Column{
			Name:  name,
			Type:  ty,
			Flag:  flag,
			Value: value,
		}
	}
	return columns, nil
}

func decodeColumnGroup(bits []byte, allocator *SliceAllocator) (*columnGroup, error) {
	var numColumns int
	bits, ty, err := decodeUint8(bits)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bits, numColumns, err = decodeUvarintLength(bits)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var names []string
	var values [][]byte
	var types, flags []uint64
	bits, names, err = decodeStringChunk(bits, numColumns, allocator)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bits, types, err = decodeUvarintChunk(bits, numColumns, allocator)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bits, flags, err = decodeUvarintChunk(bits, numColumns, allocator)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, values, err = decodeNullableBytesChunk(bits, numColumns, allocator)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &columnGroup{
		ty:     ty,
		names:  names,
		types:  types,
		flags:  flags,
		values: values,
	}, nil
}

func newColumnGroup(allocator *SliceAllocator, ty byte, columns []*model.Column) (int, *columnGroup) {
	l := len(columns)
	values := allocator.bytesSlice(l)
	names := allocator.stringSlice(l)
	types := allocator.uint64Slice(l)
	flags := allocator.uint64Slice(l)
	estimatedSize := 0
	idx := 0
	for _, col := range columns {
		if col == nil {
			continue
		}
		names[idx] = col.Name
		types[idx] = uint64(col.Type)
		flags[idx] = uint64(col.Flag)
		value := EncodeTiDBType(allocator, col.Type, col.Flag, col.Value)
		values[idx] = value
		estimatedSize += len(col.Name) + len(value) + 16 /* two 64-bits integers */
		idx++
	}
	if idx > 0 {
		return estimatedSize, &columnGroup{
			ty:     ty,
			names:  names[:idx],
			types:  types[:idx],
			flags:  flags[:idx],
			values: values[:idx],
		}
	}
	return estimatedSize, nil
}

/// Row changed message is basically an array of column groups
type rowChangedEvent = []*columnGroup

func newRowChangedMessage(allocator *SliceAllocator, ev *model.RowChangedEvent) (int, rowChangedEvent) {
	numGroups := 0
	if ev.PreColumns != nil {
		numGroups++
	}
	if ev.Columns != nil {
		numGroups++
	}
	groups := allocator.columnGroupSlice(numGroups)
	estimatedSize := 0
	if size, group := newColumnGroup(allocator, columnGroupTypeNew, ev.Columns); group != nil {
		groups[0] = group
		estimatedSize += size
	}
	if size, group := newColumnGroup(allocator, columnGroupTypeOld, ev.PreColumns); group != nil {
		groups[1] = group
		estimatedSize += size
	}
	return estimatedSize, rowChangedEvent(groups)
}

/// A buffer to save row changed events in batch
type RowChangedEventBuffer struct {
	headers *Headers

	events        []rowChangedEvent
	eventsCount   int
	estimatedSize int

	allocator *SliceAllocator
}

func NewRowChangedEventBuffer(allocator *SliceAllocator) *RowChangedEventBuffer {
	return &RowChangedEventBuffer{
		headers:   &Headers{},
		allocator: allocator,
	}
}

func (b *RowChangedEventBuffer) Encode() []byte {
	bits := NewMessageEncoder(b.allocator).encodeHeaders(b.headers).encodeRowChangeEvents(b.events[:b.eventsCount]).Encode()
	b.Reset()
	return bits
}

func (b *RowChangedEventBuffer) AppendRowChangedEvent(ev *model.RowChangedEvent) (rows, size int) {
	var partition int64 = -1
	if ev.Table.IsPartition {
		partition = ev.Table.TableID
	}

	var schema, table *string
	if len(ev.Table.Schema) > 0 {
		schema = &ev.Table.Schema
	}
	if len(ev.Table.Table) > 0 {
		table = &ev.Table.Table
	}

	b.estimatedSize += b.headers.appendHeader(
		b.allocator,
		ev.CommitTs,
		uint64(model.MqMessageTypeRow),
		ev.RowID,
		partition,
		schema,
		table,
	)
	if b.eventsCount+1 > len(b.events) {
		b.events = b.allocator.resizeRowChangedEventSlice(b.events, newBufferSize(b.eventsCount))
	}
	size, message := newRowChangedMessage(b.allocator, ev)
	b.events[b.eventsCount] = message
	b.eventsCount++
	b.estimatedSize += size
	return b.eventsCount, b.estimatedSize
}

func (b *RowChangedEventBuffer) Reset() {
	b.headers.reset()
	b.eventsCount = 0
	b.estimatedSize = 0
}

func (b *RowChangedEventBuffer) Size() int {
	return b.estimatedSize
}

func (b *RowChangedEventBuffer) GetHeaders() *Headers {
	return b.headers
}
