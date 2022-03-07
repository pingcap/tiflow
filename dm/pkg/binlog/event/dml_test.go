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

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	. "github.com/pingcap/check"

	"github.com/pingcap/tiflow/dm/pkg/gtid"
)

var _ = Suite(&testDMLSuite{})

type testDMLSuite struct{}

func (t *testDMLSuite) TestGenDMLEvent(c *C) {
	var (
		serverID  uint32 = 101
		latestPos uint32 = 123
		xid       uint64 = 10
	)

	// test INSERT/UPDATE for MySQL
	flavor := gmysql.MySQLFlavor
	gSetStr := "03fc0263-28c7-11e7-a653-6c0b84d59f30:123"
	latestGTID, err := gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)

	// empty data
	result, err := GenDMLEvents(flavor, serverID, latestPos, latestGTID, replication.WRITE_ROWS_EVENTv2, xid, nil, true, false, 0)
	c.Assert(err, NotNil)
	c.Assert(result, IsNil)

	// single INSERT without batch
	insertRows1 := make([][]interface{}, 0, 1)
	insertRows1 = append(insertRows1, []interface{}{int32(11), "string column value"})
	insertDMLData := []*DMLData{
		{
			TableID:    11,
			Schema:     "db1",
			Table:      "tbl1",
			ColumnType: []byte{gmysql.MYSQL_TYPE_LONG, gmysql.MYSQL_TYPE_STRING},
			Rows:       insertRows1,
		},
	}
	eventType := replication.WRITE_ROWS_EVENTv2
	result, err = GenDMLEvents(flavor, serverID, latestPos, latestGTID, eventType, xid, insertDMLData, true, false, 0)
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.Events, HasLen, 3+2*len(insertDMLData))
	// simply check here, more check did in `event_test.go`
	c.Assert(result.Events[3].Header.EventType, Equals, eventType)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, "03fc0263-28c7-11e7-a653-6c0b84d59f30:124")

	latestPos = result.LatestPos // update latest pos
	latestGTID = result.LatestGTID
	xid++

	// multi INSERT with batch
	insertRows2 := make([][]interface{}, 0, 2)
	insertRows2 = append(insertRows2, []interface{}{int32(101), "string column value a"}, []interface{}{int32(102), "string column value b"})
	insertDMLData = append(insertDMLData, &DMLData{
		TableID:    12,
		Schema:     "db2",
		Table:      "tbl2",
		ColumnType: []byte{gmysql.MYSQL_TYPE_LONG, gmysql.MYSQL_TYPE_STRING},
		Rows:       insertRows2,
	})
	result, err = GenDMLEvents(flavor, serverID, latestPos, latestGTID, replication.WRITE_ROWS_EVENTv2, xid, insertDMLData, true, false, 0)
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.Events, HasLen, 3+2*len(insertDMLData)) // 2 more events for insertRows2
	c.Assert(result.Events[3+2].Header.EventType, Equals, eventType)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, "03fc0263-28c7-11e7-a653-6c0b84d59f30:125")

	latestPos = result.LatestPos // update latest pos
	latestGTID = result.LatestGTID
	xid++

	// single UPDATE
	updateRows := make([][]interface{}, 0, 2)
	updateRows = append(updateRows, []interface{}{int32(21), "old string"}, []interface{}{int32(21), "new string"})
	updateDMLData := []*DMLData{
		{
			TableID:    21,
			Schema:     "db3",
			Table:      "tbl3",
			ColumnType: []byte{gmysql.MYSQL_TYPE_LONG, gmysql.MYSQL_TYPE_STRING},
			Rows:       updateRows,
		},
	}
	eventType = replication.UPDATE_ROWS_EVENTv2
	result, err = GenDMLEvents(flavor, serverID, latestPos, latestGTID, eventType, xid, updateDMLData, true, false, 0)
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.Events, HasLen, 3+2*len(updateDMLData))
	c.Assert(result.Events[3].Header.EventType, Equals, eventType)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, "03fc0263-28c7-11e7-a653-6c0b84d59f30:126")

	latestPos = result.LatestPos // update latest pos
	xid++

	// test DELETE for MariaDB
	flavor = gmysql.MariaDBFlavor
	gSetStr = fmt.Sprintf("1-%d-3", serverID)
	latestGTID, err = gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)

	// single DELETE
	deleteRows := make([][]interface{}, 0, 1)
	deleteRows = append(deleteRows, []interface{}{int32(31), "string a"})
	deleteDMLData := []*DMLData{
		{
			TableID:    31,
			Schema:     "db4",
			Table:      "tbl4",
			ColumnType: []byte{gmysql.MYSQL_TYPE_LONG, gmysql.MYSQL_TYPE_STRING},
			Rows:       deleteRows,
		},
	}
	eventType = replication.DELETE_ROWS_EVENTv2
	result, err = GenDMLEvents(flavor, serverID, latestPos, latestGTID, eventType, xid, deleteDMLData, true, false, 0)
	c.Assert(err, IsNil)
	c.Assert(result, NotNil)
	c.Assert(result.Events, HasLen, 3+2*(len(deleteDMLData)))
	c.Assert(result.Events[3].Header.EventType, Equals, eventType)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, fmt.Sprintf("1-%d-4", serverID))
}
