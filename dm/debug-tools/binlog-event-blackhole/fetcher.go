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
	"context"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// registerSlave register a slave connection on the master.
func registerSlave(addr, username, password string, serverID uint32) (*client.Conn, error) {
	conn, err := client.Connect(addr, username, password, "", func(c *client.Conn) {
	})
	if err != nil {
		return nil, errors.Annotate(err, "connect to the master")
	}

	// takes as an indication that the client is checksum-aware.
	// ref https://dev.mysql.com/doc/refman/8.0/en/c-api-binary-log-functions.html.
	_, err = conn.Execute(`SET @master_binlog_checksum='NONE'`)
	if err != nil {
		return nil, errors.Annotate(err, `SET @master_binlog_checksum='NONE'`)
	}

	conn.ResetSequence()
	packet := registerSlaveCommand(username, password, serverID)
	err = conn.WritePacket(packet)
	if err != nil {
		return nil, errors.Annotatef(err, "write COM_REGISTER_SLAVE packet %v", packet)
	}

	_, err = conn.ReadOKPacket()
	if err != nil {
		return nil, errors.Annotate(err, "read OK packet")
	}

	return conn, nil
}

// startSync starts to sync binlog event from <name, pos>.
func startSync(conn *client.Conn, serverID uint32, name string, pos uint32) error {
	conn.ResetSequence()
	packet := dumpCommand(serverID, name, pos)
	err := conn.WritePacket(packet)
	return errors.Annotatef(err, "write COM_BINLOG_DUMP %v", packet)
}

// closeConn closes the connection to the master server.
func closeConn(conn *client.Conn) error {
	deadline := time.Now().Add(time.Millisecond)
	err := conn.SetReadDeadline(deadline)
	if err != nil {
		return errors.Annotatef(err, "set connection read deadline to %v", deadline)
	}

	return errors.Trace(conn.Close())
}

// readEventsWithGoMySQL reads binlog events from the master server with `go-mysql` pkg.
func readEventsWithGoMySQL(ctx context.Context, conn *client.Conn) (uint64, uint64, time.Duration, error) {
	var (
		eventCount atomic.Uint64
		byteCount  atomic.Uint64
		startTime  = time.Now()
	)
	for {
		select {
		case <-ctx.Done():
			return eventCount.Load(), byteCount.Load(), time.Since(startTime), nil
		default:
		}

		data, err := conn.ReadPacket()
		if err != nil {
			return eventCount.Load(), byteCount.Load(), time.Since(startTime), errors.Annotate(err, "read event packet")
		}

		switch data[0] {
		case 0x00: // OK_HEADER
			eventCount.Add(1)                    // got one more event
			byteCount.Add(4 + uint64(len(data))) // with 4 bytes packet header
			continue
		case 0xff: // ERR_HEADER
			return eventCount.Load(), byteCount.Load(), time.Since(startTime), errors.New("read event fail with 0xFF header")
		case 0xfe: // EOF_HEADER
			// Refer http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
			log.L().Warn("receive EOF packet, retrying")
			continue
		default:
			log.L().Warn("invalid stream header, retrying", zap.Uint8("header", data[0]))
			continue
		}
	}
}

// readEventsWithoutGoMySQL reads binlog events from master server without `go-mysql` pkg.
func readEventsWithoutGoMySQL(ctx context.Context, conn *client.Conn) (uint64, uint64, time.Duration, error) {
	var (
		eventCount atomic.Uint64
		byteCount  atomic.Uint64
		startTime  = time.Now()
	)
	for {
		select {
		case <-ctx.Done():
			return eventCount.Load(), byteCount.Load(), time.Since(startTime), nil
		default:
		}

		_, data, err := readPacket(conn)
		if err != nil {
			return eventCount.Load(), byteCount.Load(), time.Since(startTime), errors.Annotate(err, "read event packet")
		}

		switch data[0] {
		case 0x00: // OK_HEADER
			eventCount.Add(1)                    // got one more event
			byteCount.Add(4 + uint64(len(data))) // with 4 bytes packet header
			continue
		case 0xff: // ERR_HEADER
			return eventCount.Load(), byteCount.Load(), time.Since(startTime), errors.New("read event fail with 0xFF header")
		case 0xfe: // EOF_HEADER
			// Refer http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
			log.L().Warn("receive EOF packet, retrying")
			continue
		default:
			log.L().Warn("invalid stream header, retrying", zap.Uint8("header", data[0]))
			continue
		}
	}
}

// readDataOnly reads the binary data only and does not parse packet or binlog event.
func readDataOnly(ctx context.Context, conn *client.Conn) (uint64, time.Duration, error) {
	var (
		buf       = make([]byte, 10240)
		byteCount atomic.Uint64
		startTime = time.Now()
	)
	for {
		select {
		case <-ctx.Done():
			return byteCount.Load(), time.Since(startTime), nil
		default:
		}

		n, err := conn.Conn.Conn.Read(buf)
		if err != nil {
			return byteCount.Load(), time.Since(startTime), errors.Annotatef(err, "read binary data")
		}
		byteCount.Add(uint64(n))
	}
}
