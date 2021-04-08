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
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
)

/// Utility functions for buffer allocation
func newBufferSize(oldSize int) int {
	var newSize int
	if oldSize > 128 {
		newSize = oldSize + 128
	} else {
		if oldSize > 0 {
			newSize = oldSize * 2
		} else {
			newSize = 8
		}
	}
	return newSize
}

func newUint64Buffers(eachSize int) ([]uint64, []uint64) {
	buffer := make([]uint64, eachSize*2)
	return buffer[:eachSize], buffer[eachSize:]
}

func resizeUint64Buffer(buffer1, buffer2 []uint64) ([]uint64, []uint64) {
	newBuffer1, newBuffer2 := newUint64Buffers(newBufferSize(len(buffer1)))
	copy(newBuffer1, buffer1)
	copy(newBuffer2, buffer2)
	return newBuffer1, newBuffer2
}

func newInt64Buffers(eachSize int) ([]int64, []int64) {
	buffer := make([]int64, eachSize*2)
	return buffer[:eachSize], buffer[eachSize:]
}

func resizeInt64Buffer(buffer1, buffer2 []int64) ([]int64, []int64) {
	newBuffer1, newBuffer2 := newInt64Buffers(newBufferSize(len(buffer1)))
	copy(newBuffer1, buffer1)
	copy(newBuffer2, buffer2)
	return newBuffer1, newBuffer2
}

func newBytesBuffers(eachSize int) ([][]byte, [][]byte) {
	buffer := make([][]byte, eachSize*2)
	return buffer[:eachSize], buffer[eachSize:]
}

func resizeBytesBuffer(buffer1, buffer2 [][]byte) ([][]byte, [][]byte) {
	newBuffer1, newBuffer2 := newBytesBuffers(newBufferSize(len(buffer1)))
	copy(newBuffer1, buffer1)
	copy(newBuffer2, buffer2)
	return newBuffer1, newBuffer2
}

/// Keys in columnar layout
type craftColumnarKeys struct {
	ts        []uint64
	ty        []uint64
	rowID     []int64
	partition []int64
	schema    [][]byte
	table     [][]byte

	count int
}

func (k *craftColumnarKeys) encode(bits []byte) []byte {
	bits = encodeDeltaUvarintChunk(bits, k.ts[:k.count])
	bits = encodeDeltaUvarintChunk(bits, k.ty[:k.count])
	bits = encodeDeltaVarintChunk(bits, k.rowID[:k.count])
	bits = encodeDeltaVarintChunk(bits, k.partition[:k.count])
	bits = encodeNullableBytesChunk(bits, k.schema[:k.count])
	bits = encodeNullableBytesChunk(bits, k.table[:k.count])
	return bits
}

func (k *craftColumnarKeys) appendKey(ts, ty uint64, rowID, partition int64, schema, table []byte) int {
	idx := k.count
	if idx+1 > len(k.ty) {
		k.ts, k.ty = resizeUint64Buffer(k.ts, k.ty)
		k.rowID, k.partition = resizeInt64Buffer(k.rowID, k.partition)
		k.schema, k.table = resizeBytesBuffer(k.schema, k.table)
	}
	k.ts[idx] = ts
	k.ty[idx] = ty
	k.rowID[idx] = rowID
	k.partition[idx] = partition
	k.schema[idx] = schema
	k.table[idx] = table
	k.count++

	return 32 + len(schema) + len(table) /* 4 64-bits integers and two bytes array */
}

func (k *craftColumnarKeys) reset() {
	k.count = 0
}

func (k *craftColumnarKeys) getType(index int) model.MqMessageType {
	return model.MqMessageType(k.ty[index])
}

func (k *craftColumnarKeys) getTs(index int) uint64 {
	return k.ts[index]
}

func (k *craftColumnarKeys) getPartition(index int) int64 {
	return k.partition[index]
}

func (k *craftColumnarKeys) getSchema(index int) string {
	return string(k.schema[index])
}

func (k *craftColumnarKeys) getTable(index int) string {
	return string(k.table[index])
}

func decodeCraftColumnarKeys(bits []byte, numKeys int) (*craftColumnarKeys, error) {
	var ts, ty []uint64
	var rowID, partition []int64
	var schema, table [][]byte
	var err error
	if bits, ts, err = decodeDeltaUvarintChunk(bits, numKeys); err != nil {
		return nil, errors.Trace(err)
	}
	if bits, ty, err = decodeDeltaUvarintChunk(bits, numKeys); err != nil {
		return nil, errors.Trace(err)
	}
	if bits, rowID, err = decodeDeltaVarintChunk(bits, numKeys); err != nil {
		return nil, errors.Trace(err)
	}
	if bits, partition, err = decodeDeltaVarintChunk(bits, numKeys); err != nil {
		return nil, errors.Trace(err)
	}
	if bits, schema, err = decodeNullableBytesChunk(bits, numKeys); err != nil {
		return nil, errors.Trace(err)
	}
	if bits, table, err = decodeNullableBytesChunk(bits, numKeys); err != nil {
		return nil, errors.Trace(err)
	}
	return &craftColumnarKeys{
		ts:        ts,
		ty:        ty,
		rowID:     rowID,
		partition: partition,
		schema:    schema,
		table:     table,
		count:     numKeys,
	}, nil
}

/// Column group in columnar layout
type craftColumnarColumnGroup struct {
	ty     byte
	names  [][]byte
	types  []uint64
	flags  []uint64
	values [][]byte
}

func (g *craftColumnarColumnGroup) encode(bits []byte) []byte {
	bits = append(bits, g.ty)
	bits = encodeUvarint(bits, uint64(len(g.names)))
	bits = encodeBytesChunk(bits, g.names)
	bits = encodeUvarintChunk(bits, g.types)
	bits = encodeUvarintChunk(bits, g.flags)
	bits = encodeBytesChunk(bits, g.values)
	return bits
}

func (g *craftColumnarColumnGroup) toModel() ([]*model.Column, error) {
	columns := make([]*model.Column, len(g.names))
	for i, name := range g.names {
		ty := byte(g.types[i])
		flag := model.ColumnFlagType(g.flags[i])
		value, err := decodeTiDBType(ty, flag, g.values[i])
		if err != nil {
			return nil, errors.Trace(err)
		}
		columns[i] = &model.Column{
			Name:  string(name),
			Type:  ty,
			Flag:  flag,
			Value: value,
		}
	}
	return columns, nil
}

func newCraftColumnarColumnGroup(ty byte, columns []*model.Column) (int, *craftColumnarColumnGroup) {
	var names [][]byte
	var values [][]byte
	var types []uint64
	var flags []uint64
	estimatedSize := 0
	for _, col := range columns {
		if col == nil {
			continue
		}
		name := []byte(col.Name)
		names = append(names, name)
		types = append(types, uint64(col.Type))
		flags = append(flags, uint64(col.Flag))
		value := encodeTiDBType(col.Type, col.Flag, col.Value)
		values = append(values, value)
		estimatedSize += len(name) + len(value) + 8 /* two bytes array and two 64-bits integers */
	}
	if len(names) > 0 {
		return estimatedSize, &craftColumnarColumnGroup{
			ty:     ty,
			names:  names,
			types:  types,
			flags:  flags,
			values: values,
		}
	}
	return estimatedSize, nil
}

/// Row changed message is basically an array of column groups
type craftRowChangedEvent = []*craftColumnarColumnGroup

func newCraftRowChangedMessage(ev *model.RowChangedEvent) (int, craftRowChangedEvent) {
	var groups []*craftColumnarColumnGroup
	estimatedSize := 0
	if ev.IsDelete() {
		if size, group := newCraftColumnarColumnGroup(craftColumnGroupTypeDelete, ev.PreColumns); group != nil {
			groups = append(groups, group)
			estimatedSize += size
		}
	} else {
		if size, group := newCraftColumnarColumnGroup(craftColumnGroupTypeNew, ev.Columns); group != nil {
			groups = append(groups, group)
			estimatedSize += size
		}
		if size, group := newCraftColumnarColumnGroup(craftColumnGroupTypeOld, ev.PreColumns); group != nil {
			groups = append(groups, group)
			estimatedSize += size
		}
	}
	return estimatedSize, craftRowChangedEvent(groups)
}

/// A buffer to save row changed events in batch
type craftRowChangedEventBuffer struct {
	keys *craftColumnarKeys

	events        []craftRowChangedEvent
	eventsCount   int
	estimatedSize int
}

func (b *craftRowChangedEventBuffer) encode() []byte {
	bits := newCraftMessageEncoder().encodeKeys(b.keys).encodeRowChangeEvents(b.events[:b.eventsCount]).encode()
	b.reset()
	return bits
}

func (b *craftRowChangedEventBuffer) appendRowChangedEvent(ev *model.RowChangedEvent) (rows, size int) {
	var partition int64 = -1
	if ev.Table.IsPartition {
		partition = ev.Table.TableID
	}

	b.estimatedSize += b.keys.appendKey(
		ev.CommitTs,
		uint64(model.MqMessageTypeRow),
		ev.RowID,
		partition,
		[]byte(ev.Table.Schema),
		[]byte(ev.Table.Table),
	)
	if b.eventsCount+1 > len(b.events) {
		newSize := newBufferSize(b.eventsCount)
		events := make([]craftRowChangedEvent, newSize)
		copy(events, b.events)
		b.events = events
	}
	size, message := newCraftRowChangedMessage(ev)
	b.events[b.eventsCount] = message
	b.eventsCount++
	b.estimatedSize += size
	return b.eventsCount, b.estimatedSize
}

func (b *craftRowChangedEventBuffer) reset() {
	b.keys.reset()
	b.eventsCount = 0
	b.estimatedSize = 0
}

func (b *craftRowChangedEventBuffer) size() int {
	return b.estimatedSize
}

func (b *craftRowChangedEventBuffer) getKeys() *craftColumnarKeys {
	return b.keys
}
