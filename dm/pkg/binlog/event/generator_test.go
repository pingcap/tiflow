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

func TestGenerateForMySQL(t *testing.T) {
	t.Parallel()
	var (
		flavor           = gmysql.MySQLFlavor
		serverID  uint32 = 101
		latestXID uint64 = 10
	)

	previousGTIDSetStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14,406a3f61-690d-11e7-87c5-6c92bf46f384:1-94321383,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495,686e1ab6-c47e-11e7-a42c-6c92bf46f384:1-34981190,03fc0263-28c7-11e7-a653-6c0b84d59f30:1-7041423,05474d3c-28c7-11e7-8352-203db246dd3d:1-170,10b039fc-c843-11e7-8f6a-1866daf8d810:1-308290454"
	previousGTIDSet, err := gtid.ParserGTID(flavor, previousGTIDSetStr)
	require.Nil(t, err)
	require.NotNil(t, previousGTIDSet)

	// mutil GTID in latestGTID
	latestGTIDStr := "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14"
	latestGTID, err := gtid.ParserGTID(flavor, latestGTIDStr)
	require.Nil(t, err)
	require.NotNil(t, latestGTID)
	_, err = NewGenerator(flavor, serverID, 0, latestGTID, previousGTIDSet, latestXID)
	require.NotNil(t, err)

	// latestGTID not one of the latest previousGTIDSet, UUID not found
	latestGTIDStr = "11111111-2343-11e7-be21-6c0b84d59f30:14"
	latestGTID, err = gtid.ParserGTID(flavor, latestGTIDStr)
	require.Nil(t, err)
	require.NotNil(t, latestGTID)
	_, err = NewGenerator(flavor, serverID, 0, latestGTID, previousGTIDSet, latestXID)
	require.NotNil(t, err)

	// latestGTID not one of the latest previousGTIDSet, interval mismatch
	latestGTIDStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:13"
	latestGTID, err = gtid.ParserGTID(flavor, latestGTIDStr)
	require.Nil(t, err)
	require.NotNil(t, latestGTID)
	_, err = NewGenerator(flavor, serverID, 0, latestGTID, previousGTIDSet, latestXID)
	require.NotNil(t, err)

	latestGTIDStr = "3ccc475b-2343-11e7-be21-6c0b84d59f30:14"
	latestGTID, err = gtid.ParserGTID(flavor, latestGTIDStr)
	require.Nil(t, err)
	require.NotNil(t, latestGTID)

	testGenerate(t, flavor, serverID, latestGTID, previousGTIDSet, latestXID)
}

func TestGenerateForMariaDB(t *testing.T) {
	t.Parallel()
	var (
		flavor           = gmysql.MariaDBFlavor
		serverID  uint32 = 101
		latestXID uint64 = 10
	)

	previousGTIDSetStr := "1-101-12,2-2-3,3-3-8,4-4-4"
	previousGTIDSet, err := gtid.ParserGTID(flavor, previousGTIDSetStr)
	require.Nil(t, err)
	require.NotNil(t, previousGTIDSet)

	// multi GTID in latestGTID
	latestGTIDStr := "1-101-12,2-2-23"
	latestGTID, err := gtid.ParserGTID(flavor, latestGTIDStr)
	require.Nil(t, err)
	require.NotNil(t, latestGTID)
	_, err = NewGenerator(flavor, serverID, 0, latestGTID, previousGTIDSet, latestXID)
	require.NotNil(t, err)

	// latestGTID not one of previousGTIDSet, domain-id mismatch
	latestGTIDStr = "5-101-12"
	latestGTID, err = gtid.ParserGTID(flavor, latestGTIDStr)
	require.Nil(t, err)
	require.NotNil(t, latestGTID)
	_, err = NewGenerator(flavor, serverID, 0, latestGTID, previousGTIDSet, latestXID)
	require.NotNil(t, err)

	// latestGTID not one of previousGTIDSet, sequence-number not equal
	latestGTIDStr = "1-101-13"
	latestGTID, err = gtid.ParserGTID(flavor, latestGTIDStr)
	require.Nil(t, err)
	require.NotNil(t, latestGTID)
	_, err = NewGenerator(flavor, serverID, 0, latestGTID, previousGTIDSet, latestXID)
	require.NotNil(t, err)

	latestGTIDStr = "1-101-12"
	latestGTID, err = gtid.ParserGTID(flavor, latestGTIDStr)
	require.Nil(t, err)
	require.NotNil(t, latestGTID)

	// server-id mismatch
	_, err = NewGenerator(flavor, 100, 0, latestGTID, previousGTIDSet, latestXID)
	require.NotNil(t, err)

	testGenerate(t, flavor, serverID, latestGTID, previousGTIDSet, latestXID)
}

func testGenerate(t *testing.T, flavor string, serverID uint32, latestGTID gmysql.GTIDSet, previousGTIDSet gmysql.GTIDSet, latestXID uint64) {
	t.Helper()
	// write some events to file
	dir := t.TempDir()
	filename := filepath.Join(dir, "mysql-bin-test.000001")
	f, err := os.Create(filename)
	require.Nil(t, err)
	defer f.Close()

	g, err := NewGenerator(flavor, serverID, 0, latestGTID, previousGTIDSet, latestXID)
	require.Nil(t, err)
	allEvents := make([]*replication.BinlogEvent, 0, 20)
	allEventTypes := make([]replication.EventType, 0, 50)

	// file header
	currentEvents, data, err := g.GenFileHeader(0)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)
	allEvents = append(allEvents, currentEvents...)
	allEventTypes = append(allEventTypes, replication.FORMAT_DESCRIPTION_EVENT, previousGTIDEventType(t, flavor))

	// CREATE DATABASE `db`
	schema := "db"
	currentEvents, data, err = g.GenCreateDatabaseEvents(schema)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)
	allEvents = append(allEvents, currentEvents...)
	allEventTypes = append(allEventTypes, gtidEventType(t, flavor), replication.QUERY_EVENT)

	// CREATE TABLE `db`.`tbl` (c1 INT, c2 TEXT)
	table := "tbl"
	query := fmt.Sprintf("CREATE TABLE `%s`.`%s` (c1 INT, c2 TEXT)", schema, table)
	currentEvents, data, err = g.GenCreateTableEvents(schema, query)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)
	allEvents = append(allEvents, currentEvents...)
	allEventTypes = append(allEventTypes, gtidEventType(t, flavor), replication.QUERY_EVENT)

	// INSERT INTO `db`.`tbl` VALUES (1, "string 1")
	var (
		tableID    uint64 = 8
		columnType        = []byte{gmysql.MYSQL_TYPE_LONG, gmysql.MYSQL_TYPE_STRING}
	)
	insertRows := make([][]interface{}, 0, 1)
	insertRows = append(insertRows, []interface{}{int32(1), "string 1"})
	dmlData := []*DMLData{
		{
			TableID:    tableID,
			Schema:     schema,
			Table:      table,
			ColumnType: columnType,
			Rows:       insertRows,
		},
	}
	eventType := replication.WRITE_ROWS_EVENTv2
	currentEvents, data, err = g.GenDMLEvents(eventType, dmlData, 0)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)
	allEvents = append(allEvents, currentEvents...)
	allEventTypes = append(allEventTypes, gtidEventType(t, flavor), replication.QUERY_EVENT, replication.TABLE_MAP_EVENT, eventType, replication.XID_EVENT)

	// INSERT INTO `db`.`tbl` VALUES (11, "string 11"), (12, "string 12")
	// INSERT INTO `db`.`tbl` VALUES (13, "string 13"),
	insertRows1 := make([][]interface{}, 0, 2)
	insertRows1 = append(insertRows1, []interface{}{int32(11), "string 11"}, []interface{}{int32(12), "string 12"})
	insertRows2 := make([][]interface{}, 0, 1)
	insertRows2 = append(insertRows2, []interface{}{int32(13), "string 13"})
	dmlData = []*DMLData{
		{
			TableID:    tableID,
			Schema:     schema,
			Table:      table,
			ColumnType: columnType,
			Rows:       insertRows1,
		},
		{
			TableID:    tableID,
			Schema:     schema,
			Table:      table,
			ColumnType: columnType,
			Rows:       insertRows2,
		},
	}
	currentEvents, data, err = g.GenDMLEvents(eventType, dmlData, 0)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)
	allEvents = append(allEvents, currentEvents...)
	allEventTypes = append(allEventTypes, gtidEventType(t, flavor), replication.QUERY_EVENT, replication.TABLE_MAP_EVENT, eventType, replication.TABLE_MAP_EVENT, eventType, replication.XID_EVENT)

	// UPDATE `db`.`tbl` SET c2="another string 11" WHERE c1=11
	// UPDATE `db`.`tbl` SET c1=120, c2="another string 120" WHERE C1=12
	updateRows1 := make([][]interface{}, 0, 2)
	updateRows1 = append(updateRows1, []interface{}{int32(11), "string 11"}, []interface{}{int32(11), "another string 11"})
	updateRows2 := make([][]interface{}, 0, 2)
	updateRows2 = append(updateRows2, []interface{}{int32(12), "string 12"}, []interface{}{int32(120), "another string 120"})
	dmlData = []*DMLData{
		{
			TableID:    tableID,
			Schema:     schema,
			Table:      table,
			ColumnType: columnType,
			Rows:       updateRows1,
		},
		{
			TableID:    tableID,
			Schema:     schema,
			Table:      table,
			ColumnType: columnType,
			Rows:       updateRows2,
		},
	}
	eventType = replication.UPDATE_ROWS_EVENTv2
	currentEvents, data, err = g.GenDMLEvents(eventType, dmlData, 0)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)
	allEvents = append(allEvents, currentEvents...)
	allEventTypes = append(allEventTypes, gtidEventType(t, flavor), replication.QUERY_EVENT, replication.TABLE_MAP_EVENT, eventType, replication.TABLE_MAP_EVENT, eventType, replication.XID_EVENT)

	// DELETE FROM `db`.`tbl` WHERE c1=13
	deleteRows := make([][]interface{}, 0, 1)
	deleteRows = append(deleteRows, []interface{}{int32(13), "string 13"})
	dmlData = []*DMLData{
		{
			TableID:    tableID,
			Schema:     schema,
			Table:      table,
			ColumnType: columnType,
			Rows:       deleteRows,
		},
	}
	eventType = replication.DELETE_ROWS_EVENTv2
	currentEvents, data, err = g.GenDMLEvents(eventType, dmlData, 0)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)
	allEvents = append(allEvents, currentEvents...)
	allEventTypes = append(allEventTypes, gtidEventType(t, flavor), replication.QUERY_EVENT, replication.TABLE_MAP_EVENT, eventType, replication.XID_EVENT)

	// ALTER TABLE
	query = fmt.Sprintf("ALTER TABLE `%s`.`%s` ADD COLUMN c3 INT", schema, table)
	currentEvents, data, err = g.GenDDLEvents(schema, query, 0)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)
	allEvents = append(allEvents, currentEvents...)
	allEventTypes = append(allEventTypes, gtidEventType(t, flavor), replication.QUERY_EVENT)

	// DROP TABLE `db`.`tbl`
	currentEvents, data, err = g.GenDropTableEvents(schema, table)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)
	allEvents = append(allEvents, currentEvents...)
	allEventTypes = append(allEventTypes, gtidEventType(t, flavor), replication.QUERY_EVENT)

	// DROP DATABASE `db`
	currentEvents, data, err = g.GenDropDatabaseEvents(schema)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)
	allEvents = append(allEvents, currentEvents...)
	allEventTypes = append(allEventTypes, gtidEventType(t, flavor), replication.QUERY_EVENT)

	// parse the file
	count := 0
	onEventFunc := func(e *replication.BinlogEvent) error {
		require.Equal(t, allEventTypes[count], e.Header.EventType)
		require.Equal(t, allEvents[count].RawData, e.RawData)
		count++
		return nil
	}

	parser2 := replication.NewBinlogParser()
	parser2.SetVerifyChecksum(true)
	err = parser2.ParseFile(filename, 0, onEventFunc)
	require.Nil(t, err)
}

func previousGTIDEventType(t *testing.T, flavor string) replication.EventType {
	t.Helper()
	switch flavor {
	case gmysql.MySQLFlavor:
		return replication.PREVIOUS_GTIDS_EVENT
	case gmysql.MariaDBFlavor:
		return replication.MARIADB_GTID_LIST_EVENT
	default:
		t.Fatalf("unsupported flavor %s", flavor)
		return replication.PREVIOUS_GTIDS_EVENT // hack for compiler
	}
}

func gtidEventType(t *testing.T, flavor string) replication.EventType {
	t.Helper()
	switch flavor {
	case gmysql.MySQLFlavor:
		return replication.GTID_EVENT
	case gmysql.MariaDBFlavor:
		return replication.MARIADB_GTID_EVENT
	default:
		t.Fatalf("unsupported flavor %s", flavor)
		return replication.GTID_EVENT // hack for compiler
	}
}
