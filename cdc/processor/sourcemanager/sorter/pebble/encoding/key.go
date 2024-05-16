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
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/sorter"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	// Let Delete < Update < Insert.
	typeDelete = iota + 1
	typeUpdate
	typeInsert

	tsWindowLen int = 8
	uniqueIDLen int = 4
	tableIDLen  int = 8
	tsLen       int = 8
)

// TsWindow implements cdc/processor/sourcemanager/sorter.TsWindow.
type TsWindow struct {
	sizeInSeconds int
}

// DefaultTsWindow returns a TsWindow instance.
func DefaultTsWindow() TsWindow {
	return TsWindow{sizeInSeconds: 20}
}

// ExtractTsWindow implements cdc/processor/sourcemanager/sorter.TsWindow.
func (t TsWindow) ExtractTsWindow(ts uint64) uint64 {
	return uint64(oracle.ExtractPhysical(ts) / 1000 / 10)
}

// MinTsInWindow implements cdc/processor/sourcemanager/sorter.TsWindow.
func (t TsWindow) MinTsInWindow(tsWindow uint64) uint64 {
	return oracle.ComposeTS(int64(tsWindow)*10*1000, 0)
}

// DecodeKey decodes a key to uniqueID, tableID, startTs, CRTs.
func DecodeKey(key []byte) (uniqueID uint32, tableID uint64, startTs, CRTs uint64) {
	// TsWindow, uniqueID, tableID, CRTs, startTs, Key, Put/Delete
	offset := tsWindowLen

	// uniqueID
	uniqueID = binary.BigEndian.Uint32(key[offset:])
	offset += (uniqueIDLen)

	// table ID
	tableID = binary.BigEndian.Uint64(key[offset:])
	offset += tableIDLen

	// CRTs
	CRTs = binary.BigEndian.Uint64(key[offset:])
	offset += tsLen

	// startTs
	if len(key) > offset {
		startTs = binary.BigEndian.Uint64(key[offset:])
	}

	return
}

// DecodeCRTs decodes CRTs from the given key.
func DecodeCRTs(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[tsWindowLen+uniqueIDLen+tableIDLen:])
}

// EncodeTsKey encodes uniqueID, tsWindow, tableID, CRTs and StartTs.
// StartTs is optional.
func EncodeTsKey(uniqueID uint32, tableID uint64, CRTs uint64, startTs ...uint64) []byte {
	var buf []byte
	if len(startTs) == 0 {
		// tsWindow, uniqueID, tableID, CRTs.
		buf = make([]byte, tsWindowLen+uniqueIDLen+tableIDLen+tsLen)
	} else if len(startTs) == 1 {
		// tsWindow, uniqueID, tableID, CRTs and startTs.
		buf = make([]byte, tsWindowLen+uniqueIDLen+tableIDLen+2*tsLen)
	} else {
		log.Panic("EncodeTsKey retrieve one startTs at most")
	}
	encodeTsKey(buf, uniqueID, tableID, CRTs, startTs...)
	return buf
}

// EncodeTsKeyUpperBoundExcluded is like EncodeTsKey, but only used for generating a excluded upper bound.
func EncodeTsKeyUpperBoundExcluded(uniqueID uint32, tableID uint64, CRTs uint64, startTs uint64) []byte {
	originWindow := DefaultTsWindow().ExtractTsWindow(CRTs)
	pos := sorter.Position{StartTs: startTs, CommitTs: CRTs}
	pos = pos.Next()
	changedWindow := DefaultTsWindow().ExtractTsWindow(pos.CommitTs)

	buf := EncodeTsKey(uniqueID, tableID, pos.CommitTs, pos.StartTs)
	if changedWindow != originWindow {
		binary.BigEndian.PutUint64(buf[0:tsWindowLen], originWindow)
	}
	return buf
}

// EncodeKey encodes a key according to event.
// Format: tsWindow, uniqueID, tableID, CRTs, startTs, delete/update/insert, Key.
func EncodeKey(uniqueID uint32, tableID uint64, event *model.PolymorphicEvent) []byte {
	if event.RawKV == nil {
		log.Panic("rawkv must not be nil", zap.Any("event", event))
	}

	prefixLen := tsWindowLen + uniqueIDLen + tableIDLen + 2*tsLen
	keyLen := prefixLen + 2 + len(event.RawKV.Key)
	buf := make([]byte, keyLen)
	encodeTsKey(buf, uniqueID, tableID, event.CRTs, event.StartTs)

	binary.BigEndian.PutUint16(buf[prefixLen:], getDMLOrder(event.RawKV))
	copy(buf[prefixLen+2:], event.RawKV.Key)
	return buf
}

func encodeTsKey(buf []byte, uniqueID uint32, tableID uint64, CRTs uint64, startTs ...uint64) {
	offset := 0

	// tsWindow
	tsWindow := DefaultTsWindow().ExtractTsWindow(CRTs)
	binary.BigEndian.PutUint64(buf[offset:], tsWindow)
	offset += tsWindowLen

	// uniqueID
	binary.BigEndian.PutUint32(buf[offset:], uniqueID)
	offset += uniqueIDLen

	// tableID
	binary.BigEndian.PutUint64(buf[offset:], tableID)
	offset += tableIDLen

	// CRTs
	binary.BigEndian.PutUint64(buf[offset:], CRTs)
	offset += tsLen

	// startTs
	if len(startTs) > 0 {
		binary.BigEndian.PutUint64(buf[offset:], startTs[0])
	}
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
