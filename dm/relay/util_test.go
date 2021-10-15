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

	"github.com/DATA-DOG/go-sqlmock"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/conn"
	"github.com/pingcap/dm/pkg/utils"
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
