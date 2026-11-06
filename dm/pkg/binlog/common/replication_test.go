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

package common

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/stretchr/testify/require"
)

func TestSetDefaultReplicationCfg(t *testing.T) {
	t.Parallel()
	syncCfg := replication.BinlogSyncerConfig{}

	retryCnt := 5
	SetDefaultReplicationCfg(&syncCfg, retryCnt)
	require.Equal(t, retryCnt, syncCfg.MaxReconnectAttempts)
	require.False(t, syncCfg.DisableRetrySync)
	require.False(t, syncCfg.UseDecimal)
	require.True(t, syncCfg.UseFloatWithTrailingZero)
	require.Equal(t, 16*1024*1024, syncCfg.ReadAheadBufferSize)

	retryCnt = 1
	SetDefaultReplicationCfg(&syncCfg, retryCnt)
	require.Equal(t, retryCnt, syncCfg.MaxReconnectAttempts)
	require.True(t, syncCfg.DisableRetrySync)
	require.Equal(t, 16*1024*1024, syncCfg.ReadAheadBufferSize)
}

// jsonbDouble encodes f as a JSONB DOUBLE scalar.
func jsonbDouble(f float64) []byte {
	buf := []byte{0x0b} // JSONB_DOUBLE
	return binary.LittleEndian.AppendUint64(buf, math.Float64bits(f))
}

// jsonbSmallObjectOneDouble encodes {"<key>": f} as a JSONB small object, key must be one byte.
func jsonbSmallObjectOneDouble(key byte, f float64) []byte {
	buf := []byte{
		0x00,       // JSONB_SMALL_OBJECT
		0x01, 0x00, // element count
		0x14, 0x00, // total size: 4 (header) + 4 (key entry) + 3 (value entry) + 1 (key) + 8 (value)
		0x0b, 0x00, // key offset
		0x01, 0x00, // key length
		0x0b,       // value type JSONB_DOUBLE
		0x0c, 0x00, // value offset
		key,
	}
	return binary.LittleEndian.AppendUint64(buf, math.Float64bits(f))
}

func decodeJSONColumns(t *testing.T, parser *replication.BinlogParser, values [][]byte) []string {
	t.Helper()
	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  11,
	}
	var (
		latestPos  uint32 = 4
		tableID    uint64 = 1
		columnType        = []byte{gmysql.MYSQL_TYPE_JSON}
	)
	rows := make([][]interface{}, 0, len(values))
	for _, v := range values {
		rows = append(rows, []interface{}{v})
	}

	formatDescEv, err := event.GenFormatDescriptionEvent(header, latestPos)
	require.NoError(t, err)
	tableMapEv, err := event.GenTableMapEvent(header, latestPos, tableID, []byte("db"), []byte("tbl"), columnType)
	require.NoError(t, err)
	rowsEv, err := event.GenRowsEvent(header, latestPos, replication.WRITE_ROWS_EVENTv2, tableID,
		event.RowFlagsEndOfStatement, rows, columnType, tableMapEv)
	require.NoError(t, err)

	var got []string
	onEvent := func(e *replication.BinlogEvent) error {
		if ev, ok := e.Event.(*replication.RowsEvent); ok {
			for _, row := range ev.Rows {
				got = append(got, row[0].(string))
			}
		}
		return nil
	}
	for _, raw := range [][]byte{formatDescEv.RawData, tableMapEv.RawData, rowsEv.RawData} {
		_, err = parser.ParseSingleEvent(bytes.NewReader(raw), onEvent)
		require.NoError(t, err)
	}
	return got
}

func TestSetDefaultBinlogParserCfgKeepsJSONDoubleType(t *testing.T) {
	t.Parallel()
	values := [][]byte{
		jsonbDouble(2500),
		jsonbDouble(0),
		jsonbDouble(12.5),
		jsonbDouble(1e15),
		jsonbSmallObjectOneDouble('a', 15),
	}

	parser := replication.NewBinlogParser()
	SetDefaultBinlogParserCfg(parser)
	got := decodeJSONColumns(t, parser, values)
	// whole-number doubles must keep ".0", otherwise the downstream parses them as JSON integers.
	require.Equal(t, []string{"2500.0", "0.0", "12.5", "1000000000000000.0", `{"a":15.0}`}, got)

	// without the option, go-mysql drops the trailing ".0".
	parser = replication.NewBinlogParser()
	parser.SetVerifyChecksum(true)
	got = decodeJSONColumns(t, parser, values)
	require.Equal(t, []string{"2500", "0", "12.5", "1000000000000000", `{"a":15}`}, got)
}
