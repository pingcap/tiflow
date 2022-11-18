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

package event

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/stretchr/testify/require"
)

func TestGenCommonFileHeader(t *testing.T) {
	t.Parallel()
	var (
		flavor          = gmysql.MySQLFlavor
		serverID uint32 = 101
		gSetStr         = "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454"
		gSet     gmysql.GTIDSet
	)
	gSet, err := gtid.ParserGTID(flavor, gSetStr)
	require.Nil(t, err)

	events, data, err := GenCommonFileHeader(flavor, serverID, gSet, true, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(events))
	require.Equal(t, replication.FORMAT_DESCRIPTION_EVENT, events[0].Header.EventType)
	require.Equal(t, replication.PREVIOUS_GTIDS_EVENT, events[1].Header.EventType)

	// write to file then parse it
	dir := t.TempDir()
	mysqlFilename := filepath.Join(dir, "mysql-bin-test.000001")
	mysqlFile, err := os.Create(mysqlFilename)
	require.Nil(t, err)
	defer mysqlFile.Close()

	_, err = mysqlFile.Write(data)
	require.Nil(t, err)

	count := 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		count++
		if count > 2 {
			t.Fatalf("too many binlog events got, current is %+v", e.Header)
		}
		require.Equal(t, events[count-1].Header, e.Header)
		require.Equal(t, events[count-1].Event, e.Event)
		require.Equal(t, events[count-1].RawData, e.RawData)
		return nil
	}

	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	err = parser2.ParseFile(mysqlFilename, 0, onEventFunc)
	require.Nil(t, err)

	// MariaDB
	flavor = gmysql.MariaDBFlavor
	gSetStr = "1-2-12,2-2-3,3-3-8,4-4-4"

	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	require.Nil(t, err)

	events, data, err = GenCommonFileHeader(flavor, serverID, gSet, true, 0)
	require.Nil(t, err)
	require.Equal(t, 2, len(events))
	require.Equal(t, replication.FORMAT_DESCRIPTION_EVENT, events[0].Header.EventType)
	require.Equal(t, replication.MARIADB_GTID_LIST_EVENT, events[1].Header.EventType)

	mariadbFilename := filepath.Join(dir, "mariadb-bin-test.000001")
	mariadbFile, err := os.Create(mariadbFilename)
	require.Nil(t, err)
	defer mariadbFile.Close()

	_, err = mariadbFile.Write(data)
	require.Nil(t, err)

	count = 0 // reset to 0
	err = parser2.ParseFile(mariadbFilename, 0, onEventFunc)
	require.Nil(t, err)
}

func TestGenCommonGTIDEvent(t *testing.T) {
	t.Parallel()
	var (
		flavor           = gmysql.MySQLFlavor
		serverID  uint32 = 101
		gSet      gmysql.GTIDSet
		latestPos uint32 = 123
	)

	// nil gSet, invalid
	gtidEv, err := GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	require.NotNil(t, err)
	require.Nil(t, gtidEv)

	// multi GTID in set, invalid
	gSetStr := "03fc0263-28c7-11e7-a653-6c0b84d59f30:1-123,05474d3c-28c7-11e7-8352-203db246dd3d:1-456,10b039fc-c843-11e7-8f6a-1866daf8d810:1-789"
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	require.Nil(t, err)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	require.NotNil(t, err)
	require.Nil(t, gtidEv)

	// multi intervals, invalid
	gSetStr = "03fc0263-28c7-11e7-a653-6c0b84d59f30:1-123:200-456"
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	require.Nil(t, err)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	require.NotNil(t, err)
	require.Nil(t, gtidEv)

	// interval > 1, invalid
	gSetStr = "03fc0263-28c7-11e7-a653-6c0b84d59f30:1-123"
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	require.Nil(t, err)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	require.NotNil(t, err)
	require.Nil(t, gtidEv)

	// valid
	gSetStr = "03fc0263-28c7-11e7-a653-6c0b84d59f30:123"
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	require.Nil(t, err)
	sid, err := ParseSID(gSetStr[:len(gSetStr)-4])
	require.Nil(t, err)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	require.Nil(t, err)
	require.NotNil(t, gtidEv)
	require.Equal(t, replication.GTID_EVENT, gtidEv.Header.EventType)

	// verify the body
	gtidEvBody1, ok := gtidEv.Event.(*replication.GTIDEvent)
	require.True(t, ok)
	require.NotNil(t, gtidEvBody1)
	require.Equal(t, sid.Bytes(), gtidEvBody1.SID)
	require.Equal(t, int64(123), gtidEvBody1.GNO)
	require.Equal(t, defaultGTIDFlags, gtidEvBody1.CommitFlag)
	require.Equal(t, defaultLastCommitted, gtidEvBody1.LastCommitted)
	require.Equal(t, defaultSequenceNumber, gtidEvBody1.SequenceNumber)

	// change flavor to MariaDB
	flavor = gmysql.MariaDBFlavor

	// GTID mismatch with flavor
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	require.NotNil(t, err)
	require.Nil(t, gtidEv)

	// multi GTID in set, invalid
	gSetStr = "1-2-3,4-5-6"
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	require.Nil(t, err)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	require.NotNil(t, err)
	require.Nil(t, gtidEv)

	// server_id mismatch, invalid
	gSetStr = "1-2-3"
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	require.Nil(t, err)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	require.NotNil(t, err)
	require.Nil(t, gtidEv)

	// valid
	gSetStr = fmt.Sprintf("1-%d-3", serverID)
	gSet, err = gtid.ParserGTID(flavor, gSetStr)
	require.Nil(t, err)
	gtidEv, err = GenCommonGTIDEvent(flavor, serverID, latestPos, gSet, false, 0)
	require.Nil(t, err)
	require.NotNil(t, gtidEv)
	require.Equal(t, replication.MARIADB_GTID_EVENT, gtidEv.Header.EventType)

	// verify the body, we
	gtidEvBody2, ok := gtidEv.Event.(*replication.MariadbGTIDEvent)
	require.True(t, ok)
	require.Equal(t, uint32(1), gtidEvBody2.GTID.DomainID)
	require.Equal(t, serverID, gtidEvBody2.GTID.ServerID)
	require.Equal(t, uint64(3), gtidEvBody2.GTID.SequenceNumber)
}

func TestGTIDIncrease(t *testing.T) {
	t.Parallel()
	var (
		flavor  = gmysql.MySQLFlavor
		gSetStr = "03fc0263-28c7-11e7-a653-6c0b84d59f30:123"
		gSetIn  gmysql.GTIDSet
		gSetOut gmysql.GTIDSet
	)

	// increase for MySQL
	gSetIn, err := gtid.ParserGTID(flavor, gSetStr)
	require.Nil(t, err)
	gSetOut, err = GTIDIncrease(flavor, gSetIn)
	require.Nil(t, err)
	require.NotNil(t, gSetOut)
	require.Equal(t, "03fc0263-28c7-11e7-a653-6c0b84d59f30:124", gSetOut.String())

	// increase for MariaDB
	flavor = gmysql.MariaDBFlavor
	gSetStr = "1-2-3"
	gSetIn, err = gtid.ParserGTID(flavor, gSetStr)
	require.Nil(t, err)
	gSetOut, err = GTIDIncrease(flavor, gSetIn)
	require.Nil(t, err)
	require.NotNil(t, gSetOut)
	require.Equal(t, "1-2-4", gSetOut.String())
}
