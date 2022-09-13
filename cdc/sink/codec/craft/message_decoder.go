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

	"github.com/pingcap/errors"
	pmodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// / create string from byte slice without copying
func unsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// / Primitive type decoders
func decodeUint8(bits []byte) ([]byte, byte, error) {
	if len(bits) < 1 {
		return bits, 0, cerror.ErrCraftCodecInvalidData.GenWithStack("buffer underflow")
	}
	return bits[1:], bits[0], nil
}

func decodeVarint(bits []byte) ([]byte, int64, error) {
	x, rd := binary.Varint(bits)
	if rd < 0 {
		return bits, 0, cerror.ErrCraftCodecInvalidData.GenWithStack("invalid varint data")
	}
	return bits[rd:], x, nil
}

func decodeUvarint(bits []byte) ([]byte, uint64, error) {
	x, rd := binary.Uvarint(bits)
	if rd < 0 {
		return bits, 0, cerror.ErrCraftCodecInvalidData.GenWithStack("invalid uvarint data")
	}
	return bits[rd:], x, nil
}

func decodeUvarintReversed(bits []byte) (int, uint64, error) {
	// Decode uint64 in varint format that is similar to protobuf but with bytes order reversed
	// Reference: https://developers.google.com/protocol-buffers/docs/encoding#varints
	l := len(bits) - 1
	var x uint64
	var s uint
	i := 0
	for l >= 0 {
		b := bits[l]
		if b < 0x80 {
			if i >= binary.MaxVarintLen64 || i == binary.MaxVarintLen64-1 && b > 1 {
				return 0, 0, cerror.ErrCraftCodecInvalidData.GenWithStack("invalid reversed uvarint data")
			}
			return i + 1, x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
		i++
		l--
	}
	return i, x, nil
}

func decodeUvarintReversedLength(bits []byte) (int, int, error) {
	nb, x, err := decodeUvarintReversed(bits)
	if x > math.MaxInt32 {
		return 0, 0, cerror.ErrCraftCodecInvalidData.GenWithStack("length is greater than max int32")
	}
	return nb, int(x), err
}

func decodeUvarint32(bits []byte) ([]byte, int32, error) {
	newBits, x, err := decodeUvarint(bits)
	if err != nil {
		return bits, 0, errors.Trace(err)
	}
	if x > math.MaxInt32 {
		return bits, 0, cerror.ErrCraftCodecInvalidData.GenWithStack("length is greater than max int32")
	}
	return newBits, int32(x), nil
}

func decodeVarint32(bits []byte) ([]byte, int32, error) {
	newBits, x, err := decodeVarint(bits)
	if err != nil {
		return bits, 0, errors.Trace(err)
	}
	if x > math.MaxInt32 {
		return bits, 0, cerror.ErrCraftCodecInvalidData.GenWithStack("length is greater than max int32")
	}
	return newBits, int32(x), nil
}

func decodeUvarintLength(bits []byte) ([]byte, int, error) {
	bits, x, err := decodeUvarint32(bits)
	return bits, int(x), err
}

func decodeVarintLength(bits []byte) ([]byte, int, error) {
	bits, x, err := decodeVarint32(bits)
	return bits, int(x), err
}

func decodeFloat64(bits []byte) ([]byte, float64, error) {
	if len(bits) < 8 {
		return bits, 0, cerror.ErrCraftCodecInvalidData.GenWithStack("buffer underflow")
	}
	x := binary.LittleEndian.Uint64(bits)
	return bits[8:], math.Float64frombits(x), nil
}

func decodeBytes(bits []byte) ([]byte, []byte, error) {
	newBits, l, err := decodeUvarintLength(bits)
	if err != nil {
		return bits, nil, errors.Trace(err)
	}
	if len(newBits) < l {
		return bits, nil, cerror.ErrCraftCodecInvalidData.GenWithStack("buffer underflow")
	}
	return newBits[l:], newBits[:l], nil
}

func decodeString(bits []byte) ([]byte, string, error) {
	bits, bytes, err := decodeBytes(bits)
	if err == nil {
		return bits, unsafeBytesToString(bytes), nil
	}
	return bits, "", errors.Trace(err)
}

// Chunk decoders
func decodeStringChunk(bits []byte, size int, allocator *SliceAllocator) ([]byte, []string, error) {
	larray := allocator.intSlice(size)
	newBits := bits
	var bl int
	var err error
	for i := 0; i < size; i++ {
		newBits, bl, err = decodeUvarintLength(newBits)
		if err != nil {
			return bits, nil, errors.Trace(err)
		}
		larray[i] = bl
	}

	data := allocator.stringSlice(size)
	for i := 0; i < size; i++ {
		data[i] = unsafeBytesToString(newBits[:larray[i]])
		newBits = newBits[larray[i]:]
	}
	return newBits, data, nil
}

func decodeNullableStringChunk(bits []byte, size int, allocator *SliceAllocator) ([]byte, []*string, error) {
	larray := allocator.intSlice(size)
	newBits := bits
	var bl int
	var err error
	for i := 0; i < size; i++ {
		newBits, bl, err = decodeVarintLength(newBits)
		if err != nil {
			return bits, nil, errors.Trace(err)
		}
		larray[i] = bl
	}

	data := allocator.nullableStringSlice(size)
	for i := 0; i < size; i++ {
		if larray[i] == -1 {
			continue
		}
		s := unsafeBytesToString(newBits[:larray[i]])
		data[i] = &s
		newBits = newBits[larray[i]:]
	}
	return newBits, data, nil
}

//nolint:unused,deadcode
func decodeBytesChunk(bits []byte, size int, allocator *SliceAllocator) ([]byte, [][]byte, error) {
	return doDecodeBytesChunk(bits, size, decodeUvarintLength, allocator)
}

func doDecodeBytesChunk(bits []byte, size int, lengthDecoder func([]byte) ([]byte, int, error), allocator *SliceAllocator) ([]byte, [][]byte, error) {
	larray := allocator.intSlice(size)
	newBits := bits
	var bl int
	var err error
	for i := 0; i < size; i++ {
		newBits, bl, err = lengthDecoder(newBits)
		if err != nil {
			return bits, nil, errors.Trace(err)
		}
		larray[i] = bl
	}

	data := allocator.bytesSlice(size)
	for i := 0; i < size; i++ {
		if larray[i] != -1 {
			data[i] = newBits[:larray[i]]
			newBits = newBits[larray[i]:]
		}
	}
	return newBits, data, nil
}

func decodeNullableBytesChunk(bits []byte, size int, allocator *SliceAllocator) ([]byte, [][]byte, error) {
	return doDecodeBytesChunk(bits, size, decodeVarintLength, allocator)
}

func decodeVarintChunk(bits []byte, size int, allocator *SliceAllocator) ([]byte, []int64, error) {
	array := allocator.int64Slice(size)
	newBits := bits
	var i64 int64
	var err error
	for i := 0; i < size; i++ {
		newBits, i64, err = decodeVarint(newBits)
		if err != nil {
			return bits, nil, errors.Trace(err)
		}
		array[i] = i64
	}
	return newBits, array, nil
}

func decodeUvarintChunk(bits []byte, size int, allocator *SliceAllocator) ([]byte, []uint64, error) {
	array := allocator.uint64Slice(size)
	newBits := bits
	var u64 uint64
	var err error
	for i := 0; i < size; i++ {
		newBits, u64, err = decodeUvarint(newBits)
		if err != nil {
			return bits, nil, errors.Trace(err)
		}
		array[i] = u64
	}
	return newBits, array, nil
}

func decodeDeltaVarintChunk(bits []byte, size int, allocator *SliceAllocator) ([]byte, []int64, error) {
	array := allocator.int64Slice(size)
	newBits := bits
	var err error
	newBits, array[0], err = decodeVarint(newBits)
	if err != nil {
		return bits, nil, errors.Trace(err)
	}
	for i := 1; i < size; i++ {
		newBits, array[i], err = decodeVarint(newBits)
		if err != nil {
			return bits, nil, errors.Trace(err)
		}
		array[i] = array[i-1] + array[i]
	}
	return newBits, array, nil
}

func decodeDeltaUvarintChunk(bits []byte, size int, allocator *SliceAllocator) ([]byte, []uint64, error) {
	array := allocator.uint64Slice(size)
	newBits := bits
	var err error
	newBits, array[0], err = decodeUvarint(newBits)
	if err != nil {
		return bits, nil, errors.Trace(err)
	}
	for i := 1; i < size; i++ {
		newBits, array[i], err = decodeUvarint(newBits)
		if err != nil {
			return bits, nil, errors.Trace(err)
		}
		array[i] = array[i-1] + array[i]
	}
	return newBits, array, nil
}

// size tables are always at end of serialized data, there is no unread bytes to return
func decodeSizeTables(bits []byte, allocator *SliceAllocator) (int, [][]int64, error) {
	nb, size, _ := decodeUvarintReversedLength(bits)
	sizeOffset := len(bits) - nb
	tablesOffset := sizeOffset - size
	tables := bits[tablesOffset:sizeOffset]

	tableSize := size + nb
	var err error
	var table []int64
	result := make([][]int64, 0, 1)
	for len(tables) > 0 {
		tables, size, err = decodeUvarintLength(tables)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		tables, table, err = decodeDeltaVarintChunk(tables, size, allocator)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		result = append(result, table)
	}

	return tableSize, result, nil
}

// DecodeTiDBType decodes TiDB types.
func DecodeTiDBType(ty byte, flag model.ColumnFlagType, bits []byte) (interface{}, error) {
	if bits == nil {
		return nil, nil
	}
	switch ty {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeJSON, mysql.TypeNewDecimal:
		// value type for these mysql types are string
		return unsafeBytesToString(bits), nil
	case mysql.TypeEnum, mysql.TypeSet, mysql.TypeBit:
		// value type for thest mysql types are uint64
		_, u64, err := decodeUvarint(bits)
		return u64, err
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		// value type for these mysql types are []byte
		return bits, nil
	case mysql.TypeFloat, mysql.TypeDouble:
		// value type for these mysql types are float64
		_, f64, err := decodeFloat64(bits)
		return f64, err
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		// value type for these mysql types are int64 or uint64 depends on flags
		if flag.IsUnsigned() {
			_, u64, err := decodeUvarint(bits)
			return u64, err
		}
		_, i64, err := decodeVarint(bits)
		return i64, err
	case mysql.TypeYear:
		_, i64, err := decodeVarint(bits)
		return i64, err
	case mysql.TypeUnspecified:
		fallthrough
	case mysql.TypeNull:
		fallthrough
	case mysql.TypeGeometry:
		return nil, nil
	}
	return nil, nil
}

// MessageDecoder decoder
type MessageDecoder struct {
	bits            []byte
	sizeTables      [][]int64
	metaSizeTable   []int64
	bodyOffsetTable []int
	allocator       *SliceAllocator
	dict            *termDictionary
}

// NewMessageDecoder create a new message decode with bits and allocator
func NewMessageDecoder(bits []byte, allocator *SliceAllocator) (*MessageDecoder, error) {
	bits, version, err := decodeUvarint(bits)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if version < Version1 {
		return nil, cerror.ErrCraftCodecInvalidData.GenWithStack("unexpected craft version")
	}
	sizeTablesSize, sizeTables, err := decodeSizeTables(bits, allocator)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// truncate tailing size tables
	bits = bits[:len(bits)-sizeTablesSize]

	// get size table for each body element
	bodySizeTable := sizeTables[bodySizeTableIndex]
	// build body offset table from size of each body
	// offset table has number of bodies plus 1 elements
	// TODO check bodyOffsetTable size - 1
	bodyOffsetTable := make([]int, len(bodySizeTable)+1)

	// start offset of last body element
	start := 0
	for i, size := range bodySizeTable {
		bodyOffsetTable[i] = start
		start += int(size)
	}
	// start equals total size of body elements
	bodyOffsetTable[len(bodySizeTable)] = start

	// get meta data size table which contains size of headers and term dictionary
	metaSizeTable := sizeTables[metaSizeTableIndex]

	var dict *termDictionary
	termDictionaryOffset := int(metaSizeTable[headerSizeIndex]) + start
	if metaSizeTable[termDictionarySizeIndex] > 0 {
		// term dictionary offset starts from header size + body size
		termDictionaryEnd := termDictionaryOffset + int(metaSizeTable[termDictionarySizeIndex])
		_, dict, err = decodeTermDictionary(bits[termDictionaryOffset:termDictionaryEnd], allocator)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		dict = emptyDecodingTermDictionary
	}
	return &MessageDecoder{
		bits:            bits[:termDictionaryOffset],
		sizeTables:      sizeTables,
		metaSizeTable:   metaSizeTable,
		bodyOffsetTable: bodyOffsetTable,
		allocator:       allocator,
		dict:            dict,
	}, nil
}

// Headers decode headers of message
func (d *MessageDecoder) Headers() (*Headers, error) {
	var pairs, headersSize int
	var err error
	// get number of pairs from size of body size table
	pairs = len(d.sizeTables[bodySizeTableIndex])
	if err != nil {
		return nil, errors.Trace(err)
	}
	headersSize = int(d.metaSizeTable[headerSizeIndex])
	var headers *Headers
	headers, err = decodeHeaders(d.bits[:headersSize], pairs, d.allocator, d.dict)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// skip headers
	d.bits = d.bits[headersSize:]
	return headers, nil
}

func (d *MessageDecoder) bodyBits(index int) []byte {
	return d.bits[d.bodyOffsetTable[index]:d.bodyOffsetTable[index+1]]
}

// DDLEvent decode a DDL event
func (d *MessageDecoder) DDLEvent(index int) (pmodel.ActionType, string, error) {
	bits, ty, err := decodeUvarint(d.bodyBits(index))
	if err != nil {
		return pmodel.ActionNone, "", errors.Trace(err)
	}
	_, query, err := decodeString(bits)
	return pmodel.ActionType(ty), query, err
}

// RowChangedEvent decode a row changeded event
func (d *MessageDecoder) RowChangedEvent(index int) (preColumns, columns *columnGroup, err error) {
	bits := d.bodyBits(index)
	columnGroupSizeTable := d.sizeTables[columnGroupSizeTableStartIndex+index]
	columnGroupIndex := 0
	for len(bits) > 0 {
		columnGroupSize := columnGroupSizeTable[columnGroupIndex]
		columnGroup, err := decodeColumnGroup(bits[:columnGroupSize], d.allocator, d.dict)
		bits = bits[columnGroupSize:]
		columnGroupIndex++
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		switch columnGroup.ty {
		case columnGroupTypeOld:
			preColumns = columnGroup
		case columnGroupTypeNew:
			columns = columnGroup
		}
	}
	return preColumns, columns, nil
}
