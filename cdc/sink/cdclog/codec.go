// Copyright 2020 PingCAP, Inc.
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

package cdclog

import (
	"encoding/binary"
	"hash/crc32"
)

/*
record :=
  magic: uint32   // magic number of a record start
  length: uint64  // payload length
  checksum: uint32   // checksum of payload
  payload:  uint8[length]    // log data
*/

const recordMagic uint32 = 0x823a56e8
const headerLength int64 = 16 // 4 + 8 + 4 magic + length + checksum

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Record is the format in the log file
type Record struct {
	magic    uint32
	length   uint64
	checksum uint32
	payload  []byte
}

func (r *Record) recordLength() int64 {
	return headerLength + int64(len(r.payload))
}

func (r *Record) isValid() bool {
	return crc32.Checksum(r.payload, crcTable) == r.checksum
}

func encodeRecord(payload []byte) []byte {
	header := make([]byte, headerLength)
	binary.LittleEndian.PutUint32(header, recordMagic)
	binary.LittleEndian.PutUint64(header[4:], uint64(len(payload)))

	checksum := crc32.Checksum(payload, crcTable)
	binary.LittleEndian.PutUint32(header[4+8:], checksum)
	return append(header, payload...)
}
