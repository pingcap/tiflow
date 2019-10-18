// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package entry

import (
	"bytes"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

var (
	tablePrefix  = []byte{'t'}
	recordPrefix = []byte("_r")
	indexPrefix  = []byte("_i")
	metaPrefix   = []byte("m")
)

var (
	intLen            = 8
	tablePrefixLen    = len(tablePrefix)
	recordPrefixLen   = len(recordPrefix)
	indexPrefixLen    = len(indexPrefix)
	metaPrefixLen     = len(metaPrefix)
	prefixTableIdLen  = tablePrefixLen + intLen  /*tableId*/
	prefixRecordIdLen = recordPrefixLen + intLen /*recordId*/
	prefixIndexLen    = indexPrefixLen + intLen  /*indexId*/
)

// MetaType is for data structure meta/data flag.
type MetaType byte

const (
	UnknownMetaType MetaType = 0
	// StringMeta is the flag for string meta.
	StringMeta MetaType = 'S'
	// StringData is the flag for string data.
	StringData MetaType = 's'
	// HashMeta is the flag for hash meta.
	HashMeta MetaType = 'H'
	// HashData is the flag for hash data.
	HashData MetaType = 'h'
	// ListMeta is the flag for list meta.
	ListMeta MetaType = 'L'
	// ListData is the flag for list data.
	ListData MetaType = 'l'
)

type Meta interface {
	GetType() MetaType
}

type MetaHashData struct {
	key   string
	field []byte
}

func (d MetaHashData) GetType() MetaType {
	return HashData
}

type MetaListData struct {
	key   string
	index int64
}

func (d MetaListData) GetType() MetaType {
	return ListData
}

type Other struct {
	tp MetaType
}

func (d Other) GetType() MetaType {
	return d.tp
}

func decodeTableId(key []byte) (rest []byte, tableId int64, err error) {
	if len(key) < prefixTableIdLen || !bytes.HasPrefix(key, tablePrefix) {
		return nil, 0, errors.Errorf("invalid record key - %q", key)
	}
	key = key[tablePrefixLen:]
	rest, tableId, err = codec.DecodeInt(key)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return
}

func decodeRecordId(key []byte) (rest []byte, recordId int64, err error) {
	if len(key) < prefixRecordIdLen || !bytes.HasPrefix(key, recordPrefix) {
		return nil, 0, errors.Errorf("invalid record key - %q", key)
	}
	key = key[recordPrefixLen:]
	rest, recordId, err = codec.DecodeInt(key)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return
}

func decodeIndexKey(key []byte) (indexId int64, indexValue []types.Datum, err error) {
	if len(key) < prefixIndexLen || !bytes.HasPrefix(key, indexPrefix) {
		return 0, nil, errors.Errorf("invalid record key - %q", key)
	}
	key = key[indexPrefixLen:]
	key, indexId, err = codec.DecodeInt(key)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	indexValue, err = codec.Decode(key, 2)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	return
}

func decodeMetaKey(ek []byte) (Meta, error) {
	if !bytes.HasPrefix(ek, metaPrefix) {
		return nil, errors.New("invalid encoded hash data key prefix")
	}

	ek = ek[metaPrefixLen:]
	ek, rawKey, err := codec.DecodeBytes(ek, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	key := string(rawKey)

	ek, rawTp, err := codec.DecodeUint(ek)
	if err != nil {
		return nil, errors.Trace(err)
	}
	switch MetaType(rawTp) {
	case HashData:
		if len(ek) > 0 {
			var field []byte
			ek, field, err = codec.DecodeBytes(ek, nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return MetaHashData{key: key, field: field}, nil
		}
		if len(ek) > 0 {
			// TODO: warning hash key decode failure
			panic("hash key decode failure, should never happen")
		}
	case ListData:
		if len(ek) == 0 {
			panic("list key decode failure")
		}
		var index int64
		ek, index, err = codec.DecodeInt(ek)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return MetaListData{key: key, index: index}, nil
	// TODO decode other key
	default:
		return Other{tp: MetaType(rawTp)}, nil
	}
	return nil, fmt.Errorf("unknown meta type %v", rawTp)
}

// decodeRow decodes a byte slice into datums with a existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func decodeRow(b []byte) (map[int64]types.Datum, error) {
	row := make(map[int64]types.Datum)
	if b == nil {
		return row, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return row, nil
	}
	var err error
	var data []byte
	for len(b) > 0 {
		// Get col id.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, cid, err := codec.DecodeOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		id := cid.GetInt64()

		// Get col value.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, v, err := codec.DecodeOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}

		row[id] = v
	}
	return row, nil
}

// unflatten converts a raw datum to a column datum.
func unflatten(datum types.Datum, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	if datum.IsNull() {
		return datum, nil
	}
	switch ft.Tp {
	case mysql.TypeFloat:
		datum.SetFloat32(float32(datum.GetFloat64()))
		return datum, nil
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeYear, mysql.TypeInt24,
		mysql.TypeLong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob, mysql.TypeVarchar,
		mysql.TypeString:
		return datum, nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		var t types.Time
		t.Type = ft.Tp
		t.Fsp = int8(ft.Decimal)
		var err error
		err = t.FromPackedUint(datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		if ft.Tp == mysql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, loc)
			if err != nil {
				return datum, errors.Trace(err)
			}
		}
		datum.SetUint64(0)
		datum.SetMysqlTime(t)
		return datum, nil
	case mysql.TypeDuration: //duration should read fsp from column meta data
		dur := types.Duration{Duration: time.Duration(datum.GetInt64()), Fsp: int8(ft.Decimal)}
		datum.SetValue(dur)
		return datum, nil
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(ft.Elems, datum.GetUint64())
		if err != nil {
			enum = types.Enum{}
		}
		datum.SetValue(enum)
		return datum, nil
	case mysql.TypeSet:
		set, err := types.ParseSetValue(ft.Elems, datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetValue(set)
		return datum, nil
	case mysql.TypeBit:
		val := datum.GetUint64()
		byteSize := (ft.Flen + 7) >> 3
		datum.SetUint64(0)
		datum.SetMysqlBit(types.NewBinaryLiteralFromUint(val, byteSize))
	}
	return datum, nil
}
