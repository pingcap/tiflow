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

package cdc

import (
	"github.com/pingcap/errors"
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

func hasTablePrefix(key []byte) bool {
	return key[0] == tablePrefix[0]
}

func hasRecordPrefix(key []byte) bool {
	return key[0] == recordPrefix[0] && key[1] == recordPrefix[1]
}

func hasIndexPrefix(key []byte) bool {
	return key[0] == indexPrefix[0] && key[1] == indexPrefix[1]
}
func hasMetaPrefix(key []byte) bool {
	return key[0] == metaPrefix[0]
}

func decodeTableId(key []byte) (odd []byte, tableId int64, err error) {
	if len(key) < prefixTableIdLen || !hasTablePrefix(key) {
		return nil, 0, errors.Errorf("invalid record key - %q", key)
	}
	key = key[tablePrefixLen:]
	odd, tableId, err = codec.DecodeInt(key)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return
}

func decodeRecordId(key []byte) (odd []byte, recordId int64, err error) {
	if len(key) < prefixRecordIdLen || !hasRecordPrefix(key) {
		return nil, 0, errors.Errorf("invalid record key - %q", key)
	}
	key = key[recordPrefixLen:]
	odd, recordId, err = codec.DecodeInt(key)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return
}

func decodeIndexKey(key []byte) (indexId int64, indexValue []types.Datum, err error) {
	if len(key) < prefixIndexLen || !hasIndexPrefix(key) {
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

func decodeMetaKey(ek []byte) (key string, tp MetaType, field []byte, err error) {
	if !hasMetaPrefix(ek) {
		return "", UnknownMetaType, nil, errors.New("invalid encoded hash data key prefix")
	}

	ek = ek[metaPrefixLen:]
	ek, rawKey, err := codec.DecodeBytes(ek, nil)
	if err != nil {
		return "", UnknownMetaType, nil, errors.Trace(err)
	}
	key = string(rawKey)

	ek, rawTp, err := codec.DecodeUint(ek)
	if err != nil {
		return "", UnknownMetaType, nil, errors.Trace(err)
	}
	tp = MetaType(rawTp)
	if len(ek) > 0 {
		ek, field, err = codec.DecodeBytes(ek, nil)
	}
	if len(ek) > 0 {
		// TODO: warning hash key decode failure
		panic("hash key decode failure, should never happen")
	}
	return
}

// DecodeRowWithMap decodes a byte slice into datums with a existing row map.
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
		// Get col value.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		id := cid.GetInt64()
		_, v, err := codec.DecodeOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		row[id] = v
	}
	return row, nil
}
