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
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

// DecodeKey decodes a key to uid, tableID, startTs, CRTs.
func DecodeKey(key []byte) (uid uint32, tableID uint64, startTs, CRTs uint64) {
	// uid, tableID, CRTs, startTs, Key, Put/Delete
	// uid
	uid = binary.BigEndian.Uint32(key)
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

// EncodeTsKey encodes uid, tableID, CRTs.
func EncodeTsKey(uid uint32, tableID uint64, ts uint64) []byte {
	// uid, tableID, CRTs.
	buf := make([]byte, 0, 4+8+8)
	uint64Buf := [8]byte{}
	// uid
	binary.BigEndian.PutUint32(uint64Buf[:], uid)
	buf = append(buf, uint64Buf[:4]...)
	// tableID
	binary.BigEndian.PutUint64(uint64Buf[:], tableID)
	buf = append(buf, uint64Buf[:]...)
	// CRTs
	binary.BigEndian.PutUint64(uint64Buf[:], ts)
	return append(buf, uint64Buf[:]...)
}

// EncodeKey encodes a key accroding to event.
// Format: uid, tableID, CRTs, startTs, Put/Delete, Key.
func EncodeKey(uid uint32, tableID uint64, event *model.PolymorphicEvent) []byte {
	if event.RawKV == nil {
		log.Panic("rawkv must not be nil", zap.Any("event", event))
	}
	// uid, tableID, CRTs, startTs, Put/Delete, Key
	length := 4 + 8 + 8 + 8 + 2 + len(event.RawKV.Key)
	buf := make([]byte, 0, length)
	uint64Buf := [8]byte{}
	// uid
	binary.BigEndian.PutUint32(uint64Buf[:], uid)
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
	// Let Delete < Put
	binary.BigEndian.PutUint16(uint64Buf[:], ^uint16(event.RawKV.OpType))
	buf = append(buf, uint64Buf[:2]...)
	// key
	return append(buf, event.RawKV.Key...)
}
