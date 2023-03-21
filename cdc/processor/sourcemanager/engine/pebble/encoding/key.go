// Copyright 2021 PingCAP, Inc.
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

package encoding

import (
	"encoding/binary"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

const (
	typeDelete = iota + 1
	typeUpdate
	typeInsert
)

// DecodeKey decodes a key to uniqueID, tableID, startTs, CRTs.
func DecodeKey(key []byte) (uniqueID uint32, tableID uint64, startTs, CRTs uint64) {
	// uniqueID, tableID, CRTs, startTs, Key, Put/Delete
	// uniqueID
	uniqueID = binary.BigEndian.Uint32(key)
	// table ID
	tableID = binary.BigEndian.Uint64(key[4:])
	// CRTs
	CRTs = binary.BigEndian.Uint64(key[12:])
	if len(key) >= 28 {
		// startTs
		startTs = binary.BigEndian.Uint64(key[20:])
	}
	return
}

// DecodeCRTs decodes CRTs from the given key.
func DecodeCRTs(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[12:])
}

// EncodeTsKey encodes uniqueID, tableID, CRTs and StartTs.
// StartTs is optional.
func EncodeTsKey(uniqueID uint32, tableID uint64, CRTs uint64, startTs ...uint64) []byte {
	var buf []byte
	if len(startTs) == 0 {
		// uniqueID, tableID, CRTs.
		buf = make([]byte, 0, 4+8+8)
	} else if len(startTs) == 1 {
		// uniqueID, tableID, CRTs and startTs.
		buf = make([]byte, 0, 4+8+8+8)
	} else {
		log.Panic("EncodeTsKey retrieve one startTs at most")
	}

	uint64Buf := [8]byte{}
	// uniqueID
	binary.BigEndian.PutUint32(uint64Buf[:], uniqueID)
	buf = append(buf, uint64Buf[:4]...)
	// tableID
	binary.BigEndian.PutUint64(uint64Buf[:], tableID)
	buf = append(buf, uint64Buf[:]...)
	// CRTs
	binary.BigEndian.PutUint64(uint64Buf[:], CRTs)
	buf = append(buf, uint64Buf[:]...)
	if len(startTs) > 0 {
		// startTs
		binary.BigEndian.PutUint64(uint64Buf[:], startTs[0])
		buf = append(buf, uint64Buf[:]...)
	}
	return buf
}

// EncodeKey encodes a key according to event.
// Format: uniqueID, tableID, CRTs, startTs, delete/update/insert, Key.
func EncodeKey(uniqueID uint32, tableID uint64, event *model.PolymorphicEvent) []byte {
	if event.RawKV == nil {
		log.Panic("rawkv must not be nil", zap.Any("event", event))
	}
	// uniqueID, tableID, CRTs, startTs, Put/Delete, Key
	length := 4 + 8 + 8 + 8 + 2 + len(event.RawKV.Key)
	buf := make([]byte, 0, length)
	uint64Buf := [8]byte{}
	// uniqueID
	binary.BigEndian.PutUint32(uint64Buf[:], uniqueID)
	buf = append(buf, uint64Buf[:4]...)
	// table ID
	binary.BigEndian.PutUint64(uint64Buf[:], tableID)
	buf = append(buf, uint64Buf[:]...)
	// CRTs
	binary.BigEndian.PutUint64(uint64Buf[:], event.CRTs)
	buf = append(buf, uint64Buf[:]...)
	// startTs
	binary.BigEndian.PutUint64(uint64Buf[:], event.StartTs)
	buf = append(buf, uint64Buf[:]...)
	// Let Delete < Update < Insert
	binary.BigEndian.PutUint16(uint64Buf[:], getDMLOrder(event.RawKV))
	buf = append(buf, uint64Buf[:2]...)
	// key
	return append(buf, event.RawKV.Key...)
}

// getDMLOrder returns the order of the dml types: delete<update<insert
func getDMLOrder(rowKV *model.RawKVEntry) uint16 {
	if rowKV.OpType == model.OpTypeDelete {
		return typeDelete
	} else if rowKV.OldValue != nil {
		return typeUpdate
	}
	return typeInsert
}
