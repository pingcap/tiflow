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
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	// Version1 represents the version of craft format
	Version1 uint64 = 1

	// DefaultBufferCapacity is default buffer size
	DefaultBufferCapacity = 1024

	// Column group types
	columnGroupTypeOld = 0x2
	columnGroupTypeNew = 0x1

	// Size tables index
	metaSizeTableIndex             = 0
	bodySizeTableIndex             = 1
	columnGroupSizeTableStartIndex = 2

	// meta size table index
	headerSizeIndex         = 0
	termDictionarySizeIndex = 1
	maxMetaSizeIndex        = termDictionarySizeIndex

	nullInt64 = -1
)

var (
	oneNullInt64Slice           = []int64{nullInt64}
	oneNullStringSlice          = []*string{nil}
	emptyDecodingTermDictionary = &termDictionary{
		id: make([]string, 0),
	}
)

type termDictionary struct {
	term map[string]int
	id   []string
}

func newEncodingTermDictionaryWithSize(size int) *termDictionary {
	return &termDictionary{
		term: make(map[string]int),
		id:   make([]string, 0, size),
	}
}

func newEncodingTermDictionary() *termDictionary {
	return newEncodingTermDictionaryWithSize(8) // TODO, this number should be evaluated
}

func (d *termDictionary) encodeNullable(s *string) int64 {
	if s == nil {
		return nullInt64
	}
	return d.encode(*s)
}

func (d *termDictionary) encode(s string) int64 {
	id, ok := d.term[s]
	if !ok {
		id := len(d.id)
		d.term[s] = id
		d.id = append(d.id, s)
		return int64(id)
	}
	return int64(id)
}

func (d *termDictionary) encodeNullableChunk(array []*string) []int64 {
	result := make([]int64, len(array))
	for idx, s := range array {
		result[idx] = d.encodeNullable(s)
	}
	return result
}

func (d *termDictionary) encodeChunk(array []string) []int64 {
	result := make([]int64, len(array))
	for idx, s := range array {
		result[idx] = d.encode(s)
	}
	return result
}

func (d *termDictionary) decode(id int64) (string, error) {
	i := int(id)
	if len(d.id) <= i || i < 0 {
		return "", cerror.ErrCraftCodecInvalidData.GenWithStack("invalid term id")
	}
	return d.id[i], nil
}

func (d *termDictionary) decodeNullable(id int64) (*string, error) {
	if id == nullInt64 {
		return nil, nil
	}
	if id < nullInt64 {
		return nil, cerror.ErrCraftCodecInvalidData.GenWithStack("invalid term id")
	}
	s, err := d.decode(id)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (d *termDictionary) decodeChunk(array []int64) ([]string, error) {
	result := make([]string, len(array))
	for idx, id := range array {
		t, err := d.decode(id)
		if err != nil {
			return nil, err
		}
		result[idx] = t
	}
	return result, nil
}

func (d *termDictionary) decodeNullableChunk(array []int64) ([]*string, error) {
	result := make([]*string, len(array))
	for idx, id := range array {
		t, err := d.decodeNullable(id)
		if err != nil {
			return nil, err
		}
		result[idx] = t
	}
	return result, nil
}

func encodeTermDictionary(bits []byte, dict *termDictionary) []byte {
	if len(dict.id) == 0 {
		return bits
	}
	bits = encodeUvarint(bits, uint64(len(dict.id)))
	bits = encodeStringChunk(bits, dict.id)
	return bits
}

func decodeTermDictionary(bits []byte, allocator *SliceAllocator) ([]byte, *termDictionary, error) {
	newBits, l, err := decodeUvarint(bits)
	if err != nil {
		return bits, nil, err
	}
	newBits, id, err := decodeStringChunk(newBits, int(l), allocator)
	if err != nil {
		return bits, nil, err
	}
	return newBits, &termDictionary{id: id}, nil
}

// Headers in columnar layout
type Headers struct {
	ts        []uint64
	ty        []uint64
	partition []int64
	schema    []*string
	table     []*string

	count int
}

// Count returns number of headers
func (h *Headers) Count() int {
	return h.count
}

func (h *Headers) encode(bits []byte, dict *termDictionary) []byte {
	bits = encodeDeltaUvarintChunk(bits, h.ts[:h.count])
	bits = encodeUvarintChunk(bits, h.ty[:h.count])
	bits = encodeDeltaVarintChunk(bits, h.partition[:h.count])
	bits = encodeDeltaVarintChunk(bits, dict.encodeNullableChunk(h.schema[:h.count]))
	bits = encodeDeltaVarintChunk(bits, dict.encodeNullableChunk(h.table[:h.count]))
	return bits
}

func (h *Headers) appendHeader(allocator *SliceAllocator, ts, ty uint64, partition int64, schema, table *string) int {
	idx := h.count
	if idx+1 > len(h.ty) {
		size := newBufferSize(idx)
		h.ts = allocator.resizeUint64Slice(h.ts, size)
		h.ty = allocator.resizeUint64Slice(h.ty, size)
		h.partition = allocator.resizeInt64Slice(h.partition, size)
		h.schema = allocator.resizeNullableStringSlice(h.schema, size)
		h.table = allocator.resizeNullableStringSlice(h.table, size)
	}
	h.ts[idx] = ts
	h.ty[idx] = ty
	h.partition[idx] = partition
	h.schema[idx] = schema
	h.table[idx] = table
	h.count++

	return 32 + len(*schema) + len(*table) /* 4 64-bits integers and two bytes array */
}

func (h *Headers) reset() {
	h.count = 0
}

// GetType returns type of event at given index
func (h *Headers) GetType(index int) model.MessageType {
	return model.MessageType(h.ty[index])
}

// GetTs returns timestamp of event at given index
func (h *Headers) GetTs(index int) uint64 {
	return h.ts[index]
}

// GetPartition returns partition of event at given index
func (h *Headers) GetPartition(index int) int64 {
	return h.partition[index]
}

// GetSchema returns schema of event at given index
func (h *Headers) GetSchema(index int) string {
	if h.schema[index] != nil {
		return *h.schema[index]
	}
	return ""
}

// GetTable returns table of event at given index
func (h *Headers) GetTable(index int) string {
	if h.table[index] != nil {
		return *h.table[index]
	}
	return ""
}

func decodeHeaders(bits []byte, numHeaders int, allocator *SliceAllocator, dict *termDictionary) (*Headers, error) {
	var ts, ty []uint64
	var partition, tmp []int64
	var schema, table []*string
	var err error
	if bits, ts, err = decodeDeltaUvarintChunk(bits, numHeaders, allocator); err != nil {
		return nil, errors.Trace(err)
	}
	if bits, ty, err = decodeUvarintChunk(bits, numHeaders, allocator); err != nil {
		return nil, errors.Trace(err)
	}
	if bits, partition, err = decodeDeltaVarintChunk(bits, numHeaders, allocator); err != nil {
		return nil, errors.Trace(err)
	}
	if bits, tmp, err = decodeDeltaVarintChunk(bits, numHeaders, allocator); err != nil {
		return nil, errors.Trace(err)
	}
	if schema, err = dict.decodeNullableChunk(tmp); err != nil {
		return nil, errors.Trace(err)
	}
	if _, tmp, err = decodeDeltaVarintChunk(bits, numHeaders, allocator); err != nil {
		return nil, errors.Trace(err)
	}
	if table, err = dict.decodeNullableChunk(tmp); err != nil {
		return nil, errors.Trace(err)
	}
	return &Headers{
		ts:        ts,
		ty:        ty,
		partition: partition,
		schema:    schema,
		table:     table,
		count:     numHeaders,
	}, nil
}

// Column group in columnar layout
type columnGroup struct {
	ty     byte
	names  []string
	types  []uint64
	flags  []uint64
	values [][]byte
}

func (g *columnGroup) encode(bits []byte, dict *termDictionary) []byte {
	bits = append(bits, g.ty)
	bits = encodeUvarint(bits, uint64(len(g.names)))
	bits = encodeDeltaVarintChunk(bits, dict.encodeChunk(g.names))
	bits = encodeUvarintChunk(bits, g.types)
	bits = encodeUvarintChunk(bits, g.flags)
	bits = encodeNullableBytesChunk(bits, g.values)
	return bits
}

// ToModel converts column group into model
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

func decodeColumnGroup(bits []byte, allocator *SliceAllocator, dict *termDictionary) (*columnGroup, error) {
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
	var tmp []int64
	var values [][]byte
	var types, flags []uint64
	bits, tmp, err = decodeDeltaVarintChunk(bits, numColumns, allocator)
	if err != nil {
		return nil, errors.Trace(err)
	}
	names, err = dict.decodeChunk(tmp)
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

func newColumnGroup(allocator *SliceAllocator, ty byte, columns []*model.Column, onlyHandleKeyColumns bool) (int, *columnGroup) {
	l := len(columns)
	if l == 0 {
		return 0, nil
	}
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
		if onlyHandleKeyColumns && !col.Flag.IsHandleKey() {
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

// Row changed message is basically an array of column groups
type rowChangedEvent = []*columnGroup

func newRowChangedMessage(allocator *SliceAllocator, ev *model.RowChangedEvent, onlyHandleKeyColumns bool) (int, rowChangedEvent) {
	numGroups := 0
	if ev.PreColumns != nil {
		numGroups++
	}
	if ev.Columns != nil {
		numGroups++
	}
	groups := allocator.columnGroupSlice(numGroups)
	estimatedSize := 0
	idx := 0
	if size, group := newColumnGroup(
		allocator,
		columnGroupTypeNew,
		ev.GetColumns(),
		false); group != nil {
		groups[idx] = group
		idx++
		estimatedSize += size
	}
	onlyHandleKeyColumns = onlyHandleKeyColumns && ev.IsDelete()
	if size, group := newColumnGroup(
		allocator,
		columnGroupTypeOld,
		ev.GetPreColumns(),
		onlyHandleKeyColumns); group != nil {
		groups[idx] = group
		estimatedSize += size
	}
	return estimatedSize, groups
}

// RowChangedEventBuffer is a buffer to save row changed events in batch
type RowChangedEventBuffer struct {
	headers *Headers

	events        []rowChangedEvent
	eventsCount   int
	estimatedSize int

	allocator *SliceAllocator
}

// NewRowChangedEventBuffer creates new row changed event buffer with given allocator
func NewRowChangedEventBuffer(allocator *SliceAllocator) *RowChangedEventBuffer {
	return &RowChangedEventBuffer{
		headers:   &Headers{},
		allocator: allocator,
	}
}

// Encode row changed event buffer into bits
func (b *RowChangedEventBuffer) Encode() []byte {
	bits := NewMessageEncoder(b.allocator).encodeHeaders(b.headers).encodeRowChangeEvents(b.events[:b.eventsCount]).Encode()
	b.Reset()
	return bits
}

// AppendRowChangedEvent append a new event to buffer
func (b *RowChangedEventBuffer) AppendRowChangedEvent(ev *model.RowChangedEvent, onlyHandleKeyColumns bool) (rows, size int) {
	var partition int64 = -1
	if ev.TableInfo.IsPartitionTable() {
		partition = ev.GetTableID()
	}

	var schema, table *string
	if len(ev.TableInfo.GetSchemaName()) > 0 {
		schema = ev.TableInfo.GetSchemaNamePtr()
	}
	if len(ev.TableInfo.GetTableName()) > 0 {
		table = ev.TableInfo.GetTableNamePtr()
	}

	b.estimatedSize += b.headers.appendHeader(
		b.allocator,
		ev.CommitTs,
		uint64(model.MessageTypeRow),
		partition,
		schema,
		table,
	)
	if b.eventsCount+1 > len(b.events) {
		b.events = b.allocator.resizeRowChangedEventSlice(b.events, newBufferSize(b.eventsCount))
	}
	size, message := newRowChangedMessage(b.allocator, ev, onlyHandleKeyColumns)
	b.events[b.eventsCount] = message
	b.eventsCount++
	b.estimatedSize += size
	return b.eventsCount, b.estimatedSize
}

// Reset buffer
func (b *RowChangedEventBuffer) Reset() {
	b.headers.reset()
	b.eventsCount = 0
	b.estimatedSize = 0
}

// Size of buffer
func (b *RowChangedEventBuffer) Size() int {
	return b.estimatedSize
}

// RowsCount returns number of rows batched in this buffer.
func (b *RowChangedEventBuffer) RowsCount() int {
	return b.eventsCount
}

// GetHeaders returns headers of buffer
func (b *RowChangedEventBuffer) GetHeaders() *Headers {
	return b.headers
}
