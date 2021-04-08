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

	"github.com/pingcap/errors"
	pmodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

/// Primitive type decoders
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
	x := binary.BigEndian.Uint64(bits)
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
		return bits, string(bytes), nil
	}
	return bits, "", errors.Trace(err)
}

/// Chunk decoders
func decodeStringChunk(bits []byte, size int) ([]byte, []string, error) {
	newBits, data, err := decodeBytesChunk(bits, size)
	if err != nil {
		return bits, nil, errors.Trace(err)
	}
	result := make([]string, size)
	for i, d := range data {
		result[i] = string(d)
	}
	return newBits, result, nil
}

func decodeNullableStringChunk(bits []byte, size int) ([]byte, []*string, error) {
	newBits, data, err := decodeNullableBytesChunk(bits, size)
	if err != nil {
		return bits, nil, errors.Trace(err)
	}
	result := make([]*string, size)
	for i, d := range data {
		if d != nil {
			s := string(d)
			result[i] = &s
		}
	}
	return newBits, result, nil
}

func decodeBytesChunk(bits []byte, size int) ([]byte, [][]byte, error) {
	return doDecodeBytesChunk(bits, size, decodeUvarintLength)
}

func doDecodeBytesChunk(bits []byte, size int, lengthDecoder func([]byte) ([]byte, int, error)) ([]byte, [][]byte, error) {
	larray := make([]int, size)
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

	data := make([][]byte, size)
	for i := 0; i < size; i++ {
		if larray[i] != -1 {
			data[i] = newBits[:larray[i]]
			newBits = newBits[larray[i]:]
		}
	}
	return newBits, data, nil
}

func decodeNullableBytesChunk(bits []byte, size int) ([]byte, [][]byte, error) {
	return doDecodeBytesChunk(bits, size, decodeVarintLength)
}

func decodeVarintChunk(bits []byte, size int) ([]byte, []int64, error) {
	array := make([]int64, size)
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

func decodeUvarintChunk(bits []byte, size int) ([]byte, []uint64, error) {
	array := make([]uint64, size)
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

func decodeDeltaVarintChunk(bits []byte, size int) ([]byte, []int64, error) {
	array := make([]int64, size)
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

func decodeDeltaUvarintChunk(bits []byte, size int) ([]byte, []uint64, error) {
	array := make([]uint64, size)
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
func decodeSizeTables(bits []byte) (int, [][]uint64, error) {
	nb, size, _ := decodeUvarintReversedLength(bits)
	sizeOffset := len(bits) - nb
	tablesOffset := sizeOffset - size
	tables := bits[tablesOffset:sizeOffset]

	tableSize := size + nb
	var err error
	var table []uint64
	result := make([][]uint64, 0, 1)
	for len(tables) > 0 {
		tables, size, err = decodeUvarintLength(tables)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		tables, table, err = decodeDeltaUvarintChunk(tables, size)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		result = append(result, table)
	}

	return tableSize, result, nil
}

/// TiDB types decoder
func decodeTiDBType(ty byte, flag model.ColumnFlagType, bits []byte) (interface{}, error) {
	if bits == nil {
		return nil, nil
	}
	switch ty {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeJSON, mysql.TypeNewDecimal:
		// value type for these mysql types are string
		return string(bits), nil
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
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24, mysql.TypeYear:
		// value type for these mysql types are int64 or uint64 depends on flags
		if flag.IsUnsigned() {
			_, u64, err := decodeUvarint(bits)
			return u64, err
		}
		_, i64, err := decodeUvarint(bits)
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

// Message decoder
type craftMessageDecoder struct {
	bits            []byte
	sizeTables      [][]uint64
	valuesSizeTable []uint64
}

func newCraftMessageDecoder(bits []byte) (*craftMessageDecoder, error) {
	bits, version, err := decodeUvarint(bits)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if version < CraftVersion1 {
		return nil, cerror.ErrCraftCodecInvalidData.GenWithStack("unexpected craft version")
	}
	sizeTablesSize, sizeTables, err := decodeSizeTables(bits)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &craftMessageDecoder{
		bits:            bits[:len(bits)-sizeTablesSize],
		sizeTables:      sizeTables,
		valuesSizeTable: sizeTables[craftValueSizeTableIndex],
	}, nil
}

func (d *craftMessageDecoder) decodeKeys() (*craftColumnarKeys, error) {
	var pairs, keysSize int
	var err error
	d.bits, pairs, err = decodeUvarintLength(d.bits)
	if err != nil {
		return nil, errors.Trace(err)
	}
	keysSize = int(d.sizeTables[craftKeySizeTableIndex][0])
	var keys *craftColumnarKeys
	keys, err = decodeCraftColumnarKeys(d.bits[:keysSize], pairs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// skip keys
	d.bits = d.bits[keysSize:]
	return keys, nil
}

func (d *craftMessageDecoder) valueBits(index int) []byte {
	start := 0
	if index > 0 {
		start = int(d.valuesSizeTable[index-1])
	}
	return d.bits[start:int(d.valuesSizeTable[index])]
}

func (d *craftMessageDecoder) decodeDDLEvent(index int) (pmodel.ActionType, string, error) {
	bits, ty, err := decodeUvarint(d.valueBits(index))
	if err != nil {
		return pmodel.ActionNone, "", errors.Trace(err)
	}
	_, query, err := decodeString(bits)
	return pmodel.ActionType(ty), query, err
}

func decodeCraftColumnarColumnGroup(bits []byte) (*craftColumnarColumnGroup, error) {
	var numColumns int
	bits, ty, err := decodeUint8(bits)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bits, numColumns, err = decodeUvarintLength(bits)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var names, values [][]byte
	var types, flags []uint64
	bits, names, err = decodeBytesChunk(bits, numColumns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bits, types, err = decodeUvarintChunk(bits, numColumns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bits, flags, err = decodeUvarintChunk(bits, numColumns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, values, err = decodeBytesChunk(bits, numColumns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &craftColumnarColumnGroup{
		ty:     ty,
		names:  names,
		types:  types,
		flags:  flags,
		values: values,
	}, nil
}

func (d *craftMessageDecoder) decodeRowChangedEvent(index int) (preColumns, columns *craftColumnarColumnGroup, err error) {
	bits := d.valueBits(index)
	columnGroupSizeTable := d.sizeTables[craftColumnGroupSizeTableStartIndex+index]
	columnGroupIndex := 0
	for len(bits) > 0 {
		columnGroupSize := columnGroupSizeTable[columnGroupIndex]
		columnGroup, err := decodeCraftColumnarColumnGroup(bits[:columnGroupSize])
		bits = bits[columnGroupSize:]
		columnGroupIndex++
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		switch columnGroup.ty {
		case craftColumnGroupTypeDelete:
			fallthrough
		case craftColumnGroupTypeOld:
			preColumns = columnGroup
		case craftColumnGroupTypeNew:
			columns = columnGroup
		}
	}
	return preColumns, columns, nil
}
