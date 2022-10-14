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

package relay

import (
	"context"
	"fmt"
	"io"

	"github.com/DATA-DOG/go-sqlmock"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

var _ = Suite(&testUtilSuite{})

type testUtilSuite struct{}

func (t *testUtilSuite) TestIsNewServer(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), utils.DefaultDBTimeout)
	defer cancel()

	mockDB := conn.InitMockDB(c)
	baseDB, err := conn.DefaultDBProvider.Apply(getDBConfigForTest())
	c.Assert(err, IsNil)
	db := baseDB.DB

	flavor := gmysql.MySQLFlavor
	// no prevUUID, is new server.
	isNew, err := isNewServer(ctx, "", db, flavor)
	c.Assert(err, IsNil)
	c.Assert(isNew, IsTrue)

	// different server
	mockGetServerUUID(mockDB)
	isNew, err = isNewServer(ctx, "not-exists-uuid.000001", db, flavor)
	c.Assert(err, IsNil)
	c.Assert(isNew, IsTrue)

	// the same server
	mockGetServerUUID(mockDB)
	currUUID, err := utils.GetServerUUID(ctx, db, flavor)
	c.Assert(err, IsNil)

	mockGetServerUUID(mockDB)
	isNew, err = isNewServer(ctx, fmt.Sprintf("%s.000001", currUUID), db, flavor)
	c.Assert(err, IsNil)
	c.Assert(isNew, IsFalse)
	c.Assert(mockDB.ExpectationsWereMet(), IsNil)
}

func mockGetServerUUID(mockDB sqlmock.Sqlmock) {
	mockDB.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'server_uuid'").WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("server_uuid", "12e57f06-f360-11eb-8235-585cc2bc66c9"))
}

func mockGetRandomServerID(mockDB sqlmock.Sqlmock) {
	rows := sqlmock.NewRows([]string{"Server_id", "Host", "Port", "Master_id", "Slave_UUID"})
	rows.AddRow("2", "127.0.0.1", "3307", "1", "uuid2")
	mockDB.ExpectQuery("SHOW SLAVE HOSTS").WillReturnRows(rows)
	mockDB.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'server_id'").WillReturnRows(
		sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("server_id", "1"))
}

func (t *testUtilSuite) TestGetNextUUID(c *C) {
	UUIDs := []string{
		"b60868af-5a6f-11e9-9ea3-0242ac160006.000001",
		"7acfedb5-3008-4fa2-9776-6bac42b025fe.000002",
		"92ffd03b-813e-4391-b16a-177524e8d531.000003",
		"338513ce-b24e-4ff8-9ded-9ac5aa8f4d74.000004",
	}
	cases := []struct {
		currUUID       string
		UUIDs          []string
		nextUUID       string
		nextUUIDSuffix string
		errMsgReg      string
	}{
		{
			// empty current and UUID list
		},
		{
			// non-empty current UUID, but empty UUID list
			currUUID: "b60868af-5a6f-11e9-9ea3-0242ac160006.000001",
		},
		{
			// empty current UUID, but non-empty UUID list
			UUIDs: UUIDs,
		},
		{
			// current UUID in UUID list, has next UUID
			currUUID:       UUIDs[0],
			UUIDs:          UUIDs,
			nextUUID:       UUIDs[1],
			nextUUIDSuffix: UUIDs[1][len(UUIDs[1])-6:],
		},
		{
			// current UUID in UUID list, but has no next UUID
			currUUID: UUIDs[len(UUIDs)-1],
			UUIDs:    UUIDs,
		},
		{
			// current UUID not in UUID list
			currUUID: "40ed16c1-f6f7-4012-aa9b-d360261d2b22.666666",
			UUIDs:    UUIDs,
		},
		{
			// invalid next UUID in UUID list
			currUUID:  UUIDs[len(UUIDs)-1],
			UUIDs:     append(UUIDs, "invalid-uuid"),
			errMsgReg: ".*invalid-uuid.*",
		},
	}

	for _, cs := range cases {
		nu, nus, err := getNextRelaySubDir(cs.currUUID, cs.UUIDs)
		if len(cs.errMsgReg) > 0 {
			c.Assert(err, ErrorMatches, cs.errMsgReg)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(nu, Equals, cs.nextUUID)
		c.Assert(nus, Equals, cs.nextUUIDSuffix)
	}
}

func (t *testUtilSuite) TestIsIgnorableParseError(c *C) {
	cases := []struct {
		err       error
		ignorable bool
	}{
		{
			err:       nil,
			ignorable: false,
		},
		{
			err:       io.EOF,
			ignorable: true,
		},
		{
			err:       errors.Annotate(io.EOF, "annotated end of file"),
			ignorable: true,
		},
		{
			err:       errors.New("get event header err EOF xxxx"),
			ignorable: true,
		},
		{
			err:       errors.New("some other error"),
			ignorable: false,
		},
	}

	for _, cs := range cases {
		c.Assert(isIgnorableParseError(cs.err), Equals, cs.ignorable)
	}
}
