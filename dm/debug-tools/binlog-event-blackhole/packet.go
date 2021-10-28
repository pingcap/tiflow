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

package main

import (
	"encoding/binary"
	"io"
	"os"
)

// registerSlaveCommand generates a COM_REGISTER_SLAVE command (with uninitialized header).
// ref https://dev.mysql.com/doc/internals/en/com-register-slave.html.
func registerSlaveCommand(username, password string, serverID uint32) []byte {
	hn, _ := os.Hostname()

	packet := make([]byte, 4+1+4+1+len(hn)+1+len(username)+1+len(password)+2+4+4)
	offset := 4 // 4 bytes header

	packet[offset] = 0x15 // COM_REGISTER_SLAVE
	offset++

	binary.LittleEndian.PutUint32(packet[offset:], serverID)
	offset += 4

	packet[offset] = uint8(len(hn))
	offset++
	n := copy(packet[offset:], hn)
	offset += n

	packet[offset] = uint8(len(username))
	offset++
	n = copy(packet[offset:], username)
	offset += n

	packet[offset] = uint8(len(password))
	offset++
	n = copy(packet[offset:], password)
	offset += n

	binary.LittleEndian.PutUint16(packet[offset:], 0)
	offset += 2

	binary.LittleEndian.PutUint32(packet[offset:], 0)
	offset += 4

	binary.LittleEndian.PutUint32(packet[offset:], 0)

	setCommandHeader(packet, 0)

	return packet
}

// dumpCommand generates a COM_BINLOG_DUMP command.
// ref https://dev.mysql.com/doc/internals/en/com-binlog-dump.html.
func dumpCommand(serverID uint32, name string, pos uint32) []byte {
	packet := make([]byte, 4+1+4+2+4+len(name))
	offset := 4 // 4 bytes header

	packet[offset] = 0x12 // COM_BINLOG_DUMP
	offset++

	binary.LittleEndian.PutUint32(packet[offset:], pos)
	offset += 4

	binary.LittleEndian.PutUint16(packet[offset:], 0x00) // BINLOG_DUMP_NEVER_STOP
	offset += 2

	binary.LittleEndian.PutUint32(packet[offset:], serverID)
	offset += 4

	copy(packet[offset:], name)

	setCommandHeader(packet, 0)

	return packet
}

// setCommandHeader set the header of the command inplace.
// ref https://dev.mysql.com/doc/internals/en/mysql-packet.html.
func setCommandHeader(packet []byte, seqID uint8) {
	// do not handle a payload which is larger than or equal to 2^24−1 bytes (16 MB) now.
	length := len(packet) - 4
	packet[0] = byte(length)
	packet[1] = byte(length >> 8)
	packet[2] = byte(length >> 16)

	packet[3] = seqID
}

// readPacket tries to read a MySQL packet (header & payload) from the Reader.
func readPacket(r io.Reader) ([]byte, []byte, error) {
	header := []byte{0, 0, 0, 0}
	_, err := io.ReadFull(r, header)
	if err != nil {
		return nil, nil, err
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	payload := make([]byte, length)
	_, err = io.ReadFull(r, payload)
	if err != nil {
		return nil, nil, err
	}

	return header, payload, nil
}
