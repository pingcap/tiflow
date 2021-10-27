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
	"bytes"
	"fmt"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/gtid"
)

var _ = Suite(&testDDLSuite{})

type testDDLSuite struct{}

func (t *testDDLSuite) TestGenDDLEvent(c *C) {
	var (
		serverID  uint32 = 101
		latestPos uint32 = 123
		schema           = "test_db"
		table            = "test_tbl"
	)

	// only some simple tests in this case and we can test parsing a binlog file including common header, DDL and DML in another case.

	// test CREATE/DROP DATABASE for MySQL
	flavor := gmysql.MySQLFlavor
	gSetStr := "03fc0263-28c7-11e7-a653-6c0b84d59f30:123"
	latestGTID, err := gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)

	// CREATE DATABASE
	result, err := GenCreateDatabaseEvents(flavor, serverID, latestPos, latestGTID, schema)
	c.Assert(err, IsNil)
	c.Assert(result.Events, HasLen, 2)
	// simply check here, more check did in `event_test.go`
	c.Assert(bytes.Contains(result.Data, []byte("CREATE DATABASE")), IsTrue)
	c.Assert(bytes.Contains(result.Data, []byte(schema)), IsTrue)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, "03fc0263-28c7-11e7-a653-6c0b84d59f30:124")

	latestPos = result.LatestPos // update latest pos
	latestGTID = result.LatestGTID

	// DROP DATABASE
	result, err = GenDropDatabaseEvents(flavor, serverID, latestPos, latestGTID, schema)
	c.Assert(err, IsNil)
	c.Assert(result.Events, HasLen, 2)
	c.Assert(bytes.Contains(result.Data, []byte("DROP DATABASE")), IsTrue)
	c.Assert(bytes.Contains(result.Data, []byte(schema)), IsTrue)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, "03fc0263-28c7-11e7-a653-6c0b84d59f30:125")

	latestPos = result.LatestPos // update latest pos

	// test CREATE/DROP table for MariaDB
	flavor = gmysql.MariaDBFlavor
	gSetStr = fmt.Sprintf("1-%d-3", serverID)
	latestGTID, err = gtid.ParserGTID(flavor, gSetStr)
	c.Assert(err, IsNil)

	// CREATE TABLE
	query := fmt.Sprintf("CREATE TABLE `%s` (c1 int)", table)
	result, err = GenCreateTableEvents(flavor, serverID, latestPos, latestGTID, schema, query)
	c.Assert(err, IsNil)
	c.Assert(result.Events, HasLen, 2)
	c.Assert(bytes.Contains(result.Data, []byte("CREATE TABLE")), IsTrue)
	c.Assert(bytes.Contains(result.Data, []byte(table)), IsTrue)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, fmt.Sprintf("1-%d-4", serverID))

	latestPos = result.LatestPos // update latest pos
	latestGTID = result.LatestGTID

	// DROP TABLE
	result, err = GenDropTableEvents(flavor, serverID, latestPos, latestGTID, schema, table)
	c.Assert(err, IsNil)
	c.Assert(result.Events, HasLen, 2)
	c.Assert(bytes.Contains(result.Data, []byte("DROP TABLE")), IsTrue)
	c.Assert(bytes.Contains(result.Data, []byte(table)), IsTrue)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, fmt.Sprintf("1-%d-5", serverID))

	latestPos = result.LatestPos // update latest pos
	latestGTID = result.LatestGTID

	// ALTER TABLE
	query = fmt.Sprintf("ALTER TABLE `%s`.`%s` CHANGE COLUMN `c2` `c2` decimal(10,3)", schema, table)
	result, err = GenDDLEvents(flavor, serverID, latestPos, latestGTID, schema, query)
	c.Assert(err, IsNil)
	c.Assert(result.Events, HasLen, 2)
	c.Assert(bytes.Contains(result.Data, []byte("ALTER TABLE")), IsTrue)
	c.Assert(bytes.Contains(result.Data, []byte(table)), IsTrue)
	c.Assert(result.LatestPos, Equals, latestPos+uint32(len(result.Data)))
	c.Assert(result.LatestGTID.String(), Equals, fmt.Sprintf("1-%d-6", serverID))
}
