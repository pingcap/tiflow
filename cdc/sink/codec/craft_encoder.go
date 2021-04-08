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
	"encoding/binary"
	"math"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
)

/// Primitive type encoders
func encodeFloat64(bits []byte, data float64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(data))
	return buf
}

func encodeVarint(bits []byte, data int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	l := binary.PutVarint(buf, data)
	buf = buf[:l]
	if bits == nil {
		return buf
	} else {
		return append(bits, buf...)
	}
}

func encodeUvarint(bits []byte, data uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	l := binary.PutUvarint(buf, data)
	buf = buf[:l]
	if bits == nil {
		return buf
	} else {
		return append(bits, buf...)
	}
}

func encodeUvarintReversed(bits []byte, data uint64) ([]byte, int) {
	buf := make([]byte, binary.MaxVarintLen64)
	i := 0
	for data >= 0x80 {
		buf[i] = byte(data) | 0x80
		data >>= 7
		i++
	}
	buf[i] = byte(data)
	for bi := i; bi >= 0; bi-- {
		bits = append(bits, buf[bi])
	}
	return bits, i + 1
}

func encodeBytes(bits []byte, data []byte) []byte {
	l := len(data)
	if bits == nil {
		bits = make([]byte, 0, binary.MaxVarintLen64+len(data))
	}
	bits = encodeUvarint(bits, uint64(l))
	return append(bits, data...)
}

func encodeString(bits []byte, data string) []byte {
	return encodeBytes(bits, []byte(data))
}

/// Chunk encoders
func encodeStringChunk(bits []byte, data []string) []byte {
	for _, s := range data {
		bits = encodeUvarint(bits, uint64(len(s)))
	}
	for _, s := range data {
		bits = append(bits, []byte(s)...)
	}
	return bits
}

func encodeNullableStringChunk(bits []byte, data []*string) []byte {
	for _, s := range data {
		var l int64 = -1
		if s != nil {
			l = int64(len(*s))
		}
		bits = encodeVarint(bits, l)
	}
	for _, s := range data {
		if s != nil {
			bits = append(bits, []byte(*s)...)
		}
	}
	return bits
}

func encodeBytesChunk(bits []byte, data [][]byte) []byte {
	for _, b := range data {
		bits = encodeUvarint(bits, uint64(len(b)))
	}
	for _, b := range data {
		bits = append(bits, b...)
	}
	return bits
}

func encodeNullableBytesChunk(bits []byte, data [][]byte) []byte {
	for _, b := range data {
		var l int64 = -1
		if b != nil {
			l = int64(len(b))
		}
		bits = encodeVarint(bits, l)
	}
	for _, b := range data {
		if b != nil {
			bits = append(bits, b...)
		}
	}
	return bits
}

func encodeVarintChunk(bits []byte, data []int64) []byte {
	for _, v := range data {
		bits = encodeVarint(bits, v)
	}
	return bits
}

func encodeUvarintChunk(bits []byte, data []uint64) []byte {
	for _, v := range data {
		bits = encodeUvarint(bits, v)
	}
	return bits
}

func encodeDeltaVarintChunk(bits []byte, data []int64) []byte {
	last := data[0]
	bits = encodeVarint(bits, last)
	for _, v := range data[1:] {
		bits = encodeVarint(bits, v-last)
		last = v
	}
	return bits
}

func encodeDeltaUvarintChunk(bits []byte, data []uint64) []byte {
	last := data[0]
	bits = encodeUvarint(bits, last)
	for _, v := range data[1:] {
		bits = encodeUvarint(bits, v-last)
		last = v
	}
	return bits
}

func encodeSizeTables(bits []byte, tables [][]uint64) []byte {
	size := len(bits)
	for _, table := range tables {
		bits = encodeUvarint(bits, uint64(len(table)))
		bits = encodeDeltaUvarintChunk(bits, table)
	}
	bits, _ = encodeUvarintReversed(bits, uint64(len(bits)-size))
	return bits
}

/// TiDB types encoder
func encodeTiDBType(ty byte, flag model.ColumnFlagType, value interface{}) []byte {
	if value == nil {
		return nil
	}
	switch ty {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeJSON, mysql.TypeNewDecimal:
		// value type for these mysql types are string
		return []byte(value.(string))
	case mysql.TypeEnum, mysql.TypeSet, mysql.TypeBit:
		// value type for thest mysql types are uint64
		return encodeUvarint(nil, value.(uint64))
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		// value type for these mysql types are []byte
		return value.([]byte)
	case mysql.TypeFloat, mysql.TypeDouble:
		// value type for these mysql types are float64
		return encodeFloat64(nil, value.(float64))
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeYear:
		// value type for these mysql types are int64 or uint64 depends on flags
		if flag.IsUnsigned() {
			return encodeUvarint(nil, value.(uint64))
		} else {
			return encodeVarint(nil, value.(int64))
		}
	case mysql.TypeUnspecified:
		fallthrough
	case mysql.TypeNull:
		fallthrough
	case mysql.TypeGeometry:
		return nil
	}
	return nil
}

/// Message encoder
type craftMessageEncoder struct {
	bits              []byte
	sizeTables        [][]uint64
	valuesStartOffset int
	valuesSizes       []uint64
	valuesSizesIndex  int
}

func newCraftMessageEncoder() *craftMessageEncoder {
	return &craftMessageEncoder{
		bits: encodeUvarint(make([]byte, 0, craftDefaultBufferCapacity), CraftVersion1),
	}
}

func (e *craftMessageEncoder) encodeValueSize() *craftMessageEncoder {
	e.valuesSizes[e.valuesSizesIndex] = uint64(len(e.bits) - e.valuesStartOffset)
	e.valuesSizesIndex++
	return e
}

func (e *craftMessageEncoder) encodeUvarint(u64 uint64) *craftMessageEncoder {
	e.bits = encodeUvarint(e.bits, u64)
	return e
}

func (e *craftMessageEncoder) encodeString(s string) *craftMessageEncoder {
	e.bits = encodeString(e.bits, s)
	return e
}

func (e *craftMessageEncoder) encodeKeys(keys *craftColumnarKeys) *craftMessageEncoder {
	e.bits = encodeUvarint(e.bits, uint64(keys.count))
	oldSize := len(e.bits)
	e.valuesSizes = make([]uint64, keys.count)
	e.bits = keys.encode(e.bits)
	e.valuesStartOffset = len(e.bits)
	e.sizeTables = append(e.sizeTables, []uint64{uint64(len(e.bits) - oldSize)}, e.valuesSizes)
	return e
}

func (e *craftMessageEncoder) encode() []byte {
	return encodeSizeTables(e.bits, e.sizeTables)
}

func (e *craftMessageEncoder) encodeRowChangeEvents(events []craftRowChangedEvent) *craftMessageEncoder {
	sizeTables := e.sizeTables
	for _, event := range events {
		columnGroupSizeTable := make([]uint64, len(event))
		for gi, group := range event {
			oldSize := len(e.bits)
			e.bits = group.encode(e.bits)
			columnGroupSizeTable[gi] = uint64(len(e.bits) - oldSize)
		}
		sizeTables = append(sizeTables, columnGroupSizeTable)
		e.encodeValueSize()
	}
	e.sizeTables = sizeTables
	return e
}

func newCraftResolvedEventEncoder(ts uint64) *craftMessageEncoder {
	return newCraftMessageEncoder().encodeKeys(&craftColumnarKeys{
		ts:        []uint64{uint64(ts)},
		ty:        []uint64{uint64(model.MqMessageTypeResolved)},
		rowID:     []int64{int64(-1)},
		partition: []int64{int64(-1)},
		schema:    [][]byte{nil},
		table:     [][]byte{nil},
		count:     1,
	}).encodeValueSize()
}

func newCraftDDLEventEncoder(ev *model.DDLEvent) *craftMessageEncoder {
	ty := uint64(ev.Type)
	query := ev.Query
	return newCraftMessageEncoder().encodeKeys(&craftColumnarKeys{
		ts:        []uint64{uint64(ev.CommitTs)},
		ty:        []uint64{uint64(model.MqMessageTypeDDL)},
		rowID:     []int64{int64(-1)},
		partition: []int64{int64(-1)},
		schema:    [][]byte{[]byte(ev.TableInfo.Schema)},
		table:     [][]byte{[]byte(ev.TableInfo.Table)},
		count:     1,
	}).encodeUvarint(ty).encodeString(query).encodeValueSize()
}
