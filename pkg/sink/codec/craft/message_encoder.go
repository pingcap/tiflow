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
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
)

// create byte slice from string without copying
func unsafeStringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}

// Primitive type encoders
func encodeFloat64(bits []byte, data float64) []byte {
	v := math.Float64bits(data)
	return append(bits, byte(v), byte(v>>8), byte(v>>16), byte(v>>24), byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}

func encodeVarint(bits []byte, data int64) []byte {
	udata := uint64(data) << 1
	if data < 0 {
		udata = ^udata
	}
	return encodeUvarint(bits, udata)
}

func encodeUvarint(bits []byte, data uint64) []byte {
	// Encode uint64 in varint format that is used in protobuf
	// Reference: https://developers.google.com/protocol-buffers/docs/encoding#varints
	for data >= 0x80 {
		bits = append(bits, byte(data)|0x80)
		data >>= 7
	}
	return append(bits, byte(data))
}

func encodeUvarintReversed(bits []byte, data uint64) ([]byte, int) {
	// Encode uint64 in varint format that is similar to protobuf but with bytes order reversed
	// Reference: https://developers.google.com/protocol-buffers/docs/encoding#varints
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

//nolint:unused,deadcode
func encodeBytes(bits []byte, data []byte) []byte {
	l := len(data)
	bits = encodeUvarint(bits, uint64(l))
	return append(bits, data...)
}

func encodeString(bits []byte, data string) []byte {
	l := len(data)
	bits = encodeUvarint(bits, uint64(l))
	return append(bits, data...)
}

// / Chunk encoders
func encodeStringChunk(bits []byte, data []string) []byte {
	for _, s := range data {
		bits = encodeUvarint(bits, uint64(len(s)))
	}
	for _, s := range data {
		bits = append(bits, s...)
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
			bits = append(bits, *s...)
		}
	}
	return bits
}

//nolint:unused,deadcode
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

func encodeSizeTables(bits []byte, tables [][]int64) []byte {
	size := len(bits)
	for _, table := range tables {
		bits = encodeUvarint(bits, uint64(len(table)))
		bits = encodeDeltaVarintChunk(bits, table)
	}
	bits, _ = encodeUvarintReversed(bits, uint64(len(bits)-size))
	return bits
}

// EncodeTiDBType encodes TiDB types
func EncodeTiDBType(allocator *SliceAllocator, ty byte, flag model.ColumnFlagType, value interface{}) []byte {
	if value == nil {
		return nil
	}
	switch ty {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeJSON, mysql.TypeNewDecimal:
		// value type for these mysql types are string
		return unsafeStringToBytes(value.(string))
	case mysql.TypeEnum, mysql.TypeSet, mysql.TypeBit:
		// value type for these mysql types are uint64
		return encodeUvarint(allocator.byteSlice(binary.MaxVarintLen64)[:0], value.(uint64))
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		// value type for these mysql types are []byte
		return value.([]byte)
	case mysql.TypeFloat:
		return encodeFloat64(allocator.byteSlice(4)[:0], float64(value.(float32)))
	case mysql.TypeDouble:
		// value type for these mysql types are float64
		return encodeFloat64(allocator.byteSlice(8)[:0], value.(float64))
	case mysql.TypeYear:
		// year is encoded as int64
		return encodeVarint(allocator.byteSlice(binary.MaxVarintLen64)[:0], value.(int64))
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		// value type for these mysql types are int64 or uint64 depends on flags
		if flag.IsUnsigned() {
			return encodeUvarint(allocator.byteSlice(binary.MaxVarintLen64)[:0], value.(uint64))
		}
		return encodeVarint(allocator.byteSlice(binary.MaxVarintLen64)[:0], value.(int64))
	case mysql.TypeUnspecified:
		fallthrough
	case mysql.TypeNull:
		fallthrough
	case mysql.TypeGeometry:
		return nil
	}
	return nil
}

// MessageEncoder is encoder for message
type MessageEncoder struct {
	bits           []byte
	sizeTables     [][]int64
	bodyLastOffset int
	bodySize       []int64
	bodySizeIndex  int
	metaSizeTable  []int64

	allocator *SliceAllocator
	dict      *termDictionary
}

// NewMessageEncoder creates a new encoder with given allocator
func NewMessageEncoder(allocator *SliceAllocator) *MessageEncoder {
	return &MessageEncoder{
		bits:      encodeUvarint(make([]byte, 0, DefaultBufferCapacity), Version1),
		allocator: allocator,
		dict:      newEncodingTermDictionary(),
	}
}

func (e *MessageEncoder) encodeBodySize() *MessageEncoder {
	e.bodySize[e.bodySizeIndex] = int64(len(e.bits) - e.bodyLastOffset)
	e.bodyLastOffset = len(e.bits)
	e.bodySizeIndex++
	return e
}

func (e *MessageEncoder) encodeUvarint(u64 uint64) *MessageEncoder {
	e.bits = encodeUvarint(e.bits, u64)
	return e
}

func (e *MessageEncoder) encodeString(s string) *MessageEncoder {
	e.bits = encodeString(e.bits, s)
	return e
}

func (e *MessageEncoder) encodeHeaders(headers *Headers) *MessageEncoder {
	oldSize := len(e.bits)
	e.bodySize = e.allocator.int64Slice(headers.count)
	e.bits = headers.encode(e.bits, e.dict)
	e.bodyLastOffset = len(e.bits)
	e.metaSizeTable = e.allocator.int64Slice(maxMetaSizeIndex + 1)
	e.metaSizeTable[headerSizeIndex] = int64(len(e.bits) - oldSize)
	e.sizeTables = append(e.sizeTables, e.metaSizeTable, e.bodySize)
	return e
}

// Encode message into bits
func (e *MessageEncoder) Encode() []byte {
	offset := len(e.bits)
	e.bits = encodeTermDictionary(e.bits, e.dict)
	e.metaSizeTable[termDictionarySizeIndex] = int64(len(e.bits) - offset)
	return encodeSizeTables(e.bits, e.sizeTables)
}

func (e *MessageEncoder) encodeRowChangeEvents(events []rowChangedEvent) *MessageEncoder {
	sizeTables := e.sizeTables
	for _, event := range events {
		columnGroupSizeTable := e.allocator.int64Slice(len(event))
		for gi, group := range event {
			oldSize := len(e.bits)
			e.bits = group.encode(e.bits, e.dict)
			columnGroupSizeTable[gi] = int64(len(e.bits) - oldSize)
		}
		sizeTables = append(sizeTables, columnGroupSizeTable)
		e.encodeBodySize()
	}
	e.sizeTables = sizeTables
	return e
}

// NewResolvedEventEncoder creates a new encoder with given allocator and timestamp
func NewResolvedEventEncoder(allocator *SliceAllocator, ts uint64) *MessageEncoder {
	return NewMessageEncoder(allocator).encodeHeaders(&Headers{
		ts:        allocator.oneUint64Slice(ts),
		ty:        allocator.oneUint64Slice(uint64(model.MessageTypeResolved)),
		partition: oneNullInt64Slice,
		schema:    oneNullStringSlice,
		table:     oneNullStringSlice,
		count:     1,
	}).encodeBodySize()
}

// NewDDLEventEncoder creates a new encoder with given allocator and timestamp
func NewDDLEventEncoder(allocator *SliceAllocator, ev *model.DDLEvent) *MessageEncoder {
	ty := uint64(ev.Type)
	query := ev.Query
	var schema, table *string
	if len(ev.TableInfo.TableName.Schema) > 0 {
		schema = &ev.TableInfo.TableName.Schema
	}
	if len(ev.TableInfo.TableName.Table) > 0 {
		table = &ev.TableInfo.TableName.Table
	}
	return NewMessageEncoder(allocator).encodeHeaders(&Headers{
		ts:        allocator.oneUint64Slice(ev.CommitTs),
		ty:        allocator.oneUint64Slice(uint64(model.MessageTypeDDL)),
		partition: oneNullInt64Slice,
		schema:    allocator.oneNullableStringSlice(schema),
		table:     allocator.oneNullableStringSlice(table),
		count:     1,
	}).encodeUvarint(ty).encodeString(query).encodeBodySize()
}
