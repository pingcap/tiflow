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

package utils

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/coreos/go-semver/semver"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/dm/pkg/gtid"
)

var _ = Suite(&testDBSuite{})

type testDBSuite struct{}

func (t *testDBSuite) TestGetFlavor(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	// MySQL
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'version';`).WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("version", "5.7.31-log"))
	flavor, err := GetFlavor(context.Background(), db)
	c.Assert(err, IsNil)
	c.Assert(flavor, Equals, "mysql")
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// MariaDB
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'version';`).WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("version", "10.13.1-MariaDB-1~wheezy"))
	flavor, err = GetFlavor(context.Background(), db)
	c.Assert(err, IsNil)
	c.Assert(flavor, Equals, "mariadb")
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// others
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'version';`).WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("version", "unknown"))
	flavor, err = GetFlavor(context.Background(), db)
	c.Assert(err, IsNil)
	c.Assert(flavor, Equals, "mysql") // as MySQL
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (t *testDBSuite) TestGetRandomServerID(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	t.createMockResult(mock, 1, []uint32{100, 101}, "mysql")
	serverID, err := GetRandomServerID(context.Background(), db)
	c.Assert(err, IsNil)
	c.Assert(serverID, Greater, uint32(0))
	c.Assert(mock.ExpectationsWereMet(), IsNil)
	c.Assert(serverID, Not(Equals), 1)
	c.Assert(serverID, Not(Equals), 100)
	c.Assert(serverID, Not(Equals), 101)
}

func TestGetPosAndGs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	db, mock, err := sqlmock.New()
	require.Nil(t, err)

	// 5 columns for MySQL
	rows := mock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).AddRow(
		"mysql-bin.000009", 11232, "do_db", "ignore_db", "074be7f4-f0f1-11ea-95bd-0242ac120002:1-699",
	)
	mock.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)

	pos, gs, err := GetPosAndGs(ctx, db, "mysql")
	require.Nil(t, err)
	require.Equal(t, pos, gmysql.Position{
		Name: "mysql-bin.000009",
		Pos:  11232,
	})
	require.Equal(t, gs.String(), "074be7f4-f0f1-11ea-95bd-0242ac120002:1-699")
	require.Nil(t, mock.ExpectationsWereMet())

	// 4 columns for MariaDB
	rows = mock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).AddRow(
		"mysql-bin.000009", 11232, "do_db", "ignore_db",
	)
	mock.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)
	rows = mock.NewRows([]string{"Variable_name", "Value"}).AddRow("gtid_binlog_pos", "1-2-100")
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'gtid_binlog_pos'`).WillReturnRows(rows)

	pos, gs, err = GetPosAndGs(ctx, db, "mariadb")
	require.Nil(t, err)
	require.Equal(t, pos, gmysql.Position{
		Name: "mysql-bin.000009",
		Pos:  11232,
	})
	require.Equal(t, gs.String(), "1-2-100")
	require.Nil(t, mock.ExpectationsWereMet())

	// some upstream (maybe a polarDB secondary node)
	rows = mock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"})
	mock.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)

	_, gs, err = GetPosAndGs(ctx, db, "mysql")
	require.Nil(t, gs)
	require.NotNil(t, err)
}

func TestGetBinlogDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	db, mock, err := sqlmock.New()
	require.Nil(t, err)

	// 5 columns for MySQL
	rows := mock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).AddRow(
		"mysql-bin.000009", 11232, "do_db", "ignore_db", "074be7f4-f0f1-11ea-95bd-0242ac120002:1-699",
	)
	mock.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)

	binlogDoDB, binlogIgnoreDB, err := GetBinlogDB(ctx, db, "mysql")
	require.Nil(t, err)
	require.Equal(t, binlogDoDB, "do_db")
	require.Equal(t, binlogIgnoreDB, "ignore_db")
	require.Nil(t, mock.ExpectationsWereMet())

	// 4 columns for MariaDB
	rows = mock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).AddRow(
		"mysql-bin.000009", 11232, "do_db", "ignore_db",
	)
	mock.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)
	rows = mock.NewRows([]string{"Variable_name", "Value"}).AddRow("gtid_binlog_pos", "1-2-100")
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'gtid_binlog_pos'`).WillReturnRows(rows)

	binlogDoDB, binlogIgnoreDB, err = GetBinlogDB(ctx, db, "mariadb")
	require.Nil(t, err)
	require.Equal(t, binlogDoDB, "do_db")
	require.Equal(t, binlogIgnoreDB, "ignore_db")
	require.Nil(t, mock.ExpectationsWereMet())
}

func (t *testDBSuite) TestGetMariaDBGtidDomainID(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	rows := mock.NewRows([]string{"Variable_name", "Value"}).AddRow("gtid_domain_id", 101)
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'gtid_domain_id'`).WillReturnRows(rows)

	dID, err := GetMariaDBGtidDomainID(ctx, db)
	c.Assert(err, IsNil)
	c.Assert(dID, Equals, uint32(101))
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (t *testDBSuite) TestGetServerUUID(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	// MySQL
	rows := mock.NewRows([]string{"Variable_name", "Value"}).AddRow("server_uuid", "074be7f4-f0f1-11ea-95bd-0242ac120002")
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'server_uuid'`).WillReturnRows(rows)
	uuid, err := GetServerUUID(ctx, db, "mysql")
	c.Assert(err, IsNil)
	c.Assert(uuid, Equals, "074be7f4-f0f1-11ea-95bd-0242ac120002")
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// MariaDB
	rows = mock.NewRows([]string{"Variable_name", "Value"}).AddRow("gtid_domain_id", 123)
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'gtid_domain_id'`).WillReturnRows(rows)
	rows = mock.NewRows([]string{"Variable_name", "Value"}).AddRow("server_id", 456)
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'server_id'`).WillReturnRows(rows)
	uuid, err = GetServerUUID(ctx, db, "mariadb")
	c.Assert(err, IsNil)
	c.Assert(uuid, Equals, "123-456")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (t *testDBSuite) TestGetServerUnixTS(c *C) {
	ctx := context.Background()

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	ts := time.Now().Unix()
	rows := sqlmock.NewRows([]string{"UNIX_TIMESTAMP()"}).AddRow(strconv.FormatInt(ts, 10))
	mock.ExpectQuery("SELECT UNIX_TIMESTAMP()").WillReturnRows(rows)

	ts2, err := GetServerUnixTS(ctx, db)
	c.Assert(err, IsNil)
	c.Assert(ts, Equals, ts2)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (t *testDBSuite) TestGetParser(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	var (
		DDL1 = `ALTER TABLE tbl ADD COLUMN c1 INT`
		DDL2 = `ALTER TABLE tbl ADD COLUMN 'c1' INT`
		DDL3 = `ALTER TABLE tbl ADD COLUMN "c1" INT`
	)

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	// no `ANSI_QUOTES`
	rows := mock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "")
	mock.ExpectQuery(`SHOW VARIABLES LIKE 'sql_mode'`).WillReturnRows(rows)
	p, err := GetParser(ctx, db)
	c.Assert(err, IsNil)
	_, err = p.ParseOneStmt(DDL1, "", "")
	c.Assert(err, IsNil)
	_, err = p.ParseOneStmt(DDL2, "", "")
	c.Assert(err, NotNil)
	_, err = p.ParseOneStmt(DDL3, "", "")
	c.Assert(err, NotNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)

	// `ANSI_QUOTES`
	rows = mock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery(`SHOW VARIABLES LIKE 'sql_mode'`).WillReturnRows(rows)
	p, err = GetParser(ctx, db)
	c.Assert(err, IsNil)
	_, err = p.ParseOneStmt(DDL1, "", "")
	c.Assert(err, IsNil)
	_, err = p.ParseOneStmt(DDL2, "", "")
	c.Assert(err, NotNil)
	_, err = p.ParseOneStmt(DDL3, "", "")
	c.Assert(err, IsNil)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (t *testDBSuite) TestGetGTID(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	rows := mock.NewRows([]string{"Variable_name", "Value"}).AddRow("GTID_MODE", "ON")
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'`).WillReturnRows(rows)
	mode, err := GetGTIDMode(ctx, db)
	c.Assert(err, IsNil)
	c.Assert(mode, Equals, "ON")
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func (t *testDBSuite) TestMySQLError(c *C) {
	err := newMysqlErr(tmysql.ErrNoSuchThread, "Unknown thread id: 111")
	c.Assert(IsNoSuchThreadError(err), Equals, true)

	err = newMysqlErr(tmysql.ErrMasterFatalErrorReadingBinlog, "binlog purged error")
	c.Assert(IsErrBinlogPurged(err), Equals, true)
}

func (t *testDBSuite) TestGetAllServerID(c *C) {
	testCases := []struct {
		masterID  uint32
		serverIDs []uint32
	}{
		{
			1,
			[]uint32{2, 3, 4},
		}, {
			2,
			[]uint32{},
		}, {
			4294967295, // max server-id.
			[]uint32{},
		},
	}

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	flavors := []string{gmysql.MariaDBFlavor, gmysql.MySQLFlavor}

	for _, testCase := range testCases {
		for _, flavor := range flavors {
			t.createMockResult(mock, testCase.masterID, testCase.serverIDs, flavor)
			serverIDs, err2 := GetAllServerID(context.Background(), db)
			c.Assert(err2, IsNil)

			for _, serverID := range testCase.serverIDs {
				_, ok := serverIDs[serverID]
				c.Assert(ok, IsTrue)
			}

			_, ok := serverIDs[testCase.masterID]
			c.Assert(ok, IsTrue)
		}
	}

	err = mock.ExpectationsWereMet()
	c.Assert(err, IsNil)
}

func (t *testDBSuite) createMockResult(mock sqlmock.Sqlmock, masterID uint32, serverIDs []uint32, flavor string) {
	expectQuery := mock.ExpectQuery("SHOW SLAVE HOSTS")

	host := "test"
	port := 3306
	slaveUUID := "test"

	if flavor == gmysql.MariaDBFlavor {
		rows := sqlmock.NewRows([]string{"Server_id", "Host", "Port", "Master_id"})
		for _, serverID := range serverIDs {
			rows.AddRow(serverID, host, port, masterID)
		}
		expectQuery.WillReturnRows(rows)
	} else {
		rows := sqlmock.NewRows([]string{"Server_id", "Host", "Port", "Master_id", "Slave_UUID"})
		for _, serverID := range serverIDs {
			rows.AddRow(serverID, host, port, masterID, slaveUUID)
		}
		expectQuery.WillReturnRows(rows)
	}

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'server_id'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("server_id", masterID))
}

func newMysqlErr(number uint16, message string) *mysql.MySQLError {
	return &mysql.MySQLError{
		Number:  number,
		Message: message,
	}
}

func (t *testDBSuite) TestTiDBVersion(c *C) {
	testCases := []struct {
		version string
		result  *semver.Version
		err     error
	}{
		{
			"wrong-version",
			semver.New("0.0.0"),
			errors.Errorf("not a valid TiDB version: %s", "wrong-version"),
		}, {
			"5.7.31-log",
			semver.New("0.0.0"),
			errors.Errorf("not a valid TiDB version: %s", "5.7.31-log"),
		}, {
			"5.7.25-TiDB-v3.1.2",
			semver.New("3.1.2"),
			nil,
		}, {
			"5.7.25-TiDB-v4.0.0-beta.2-1293-g0843f32c0-dirty",
			semver.New("4.0.00-beta.2"),
			nil,
		},
	}

	for _, tc := range testCases {
		tidbVer, err := ExtractTiDBVersion(tc.version)
		if tc.err != nil {
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Equals, tc.err.Error())
		} else {
			c.Assert(tidbVer, DeepEquals, tc.result)
		}
	}
}

func getGSetFromString(c *C, s string) gtid.Set {
	gSet, err := gtid.ParserGTID("mysql", s)
	c.Assert(err, IsNil)
	return gSet
}

func (t *testDBSuite) TestAddGSetWithPurged(c *C) {
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	mariaGTID, err := gtid.ParserGTID("mariadb", "1-2-100")
	c.Assert(err, IsNil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	c.Assert(err, IsNil)

	testCases := []struct {
		originGSet  gtid.Set
		purgedSet   gtid.Set
		expectedSet gtid.Set
		err         error
	}{
		{
			getGSetFromString(c, "3ccc475b-2343-11e7-be21-6c0b84d59f30:6-14"),
			getGSetFromString(c, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-5"),
			getGSetFromString(c, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14"),
			nil,
		}, {
			getGSetFromString(c, "3ccc475b-2343-11e7-be21-6c0b84d59f30:2-6"),
			getGSetFromString(c, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1"),
			getGSetFromString(c, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-6"),
			nil,
		}, {
			getGSetFromString(c, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-6"),
			getGSetFromString(c, "53bfca22-690d-11e7-8a62-18ded7a37b78:1-495"),
			getGSetFromString(c, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-6,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495"),
			nil,
		}, {
			getGSetFromString(c, "3ccc475b-2343-11e7-be21-6c0b84d59f30:6-14"),
			mariaGTID,
			nil,
			errors.New("invalid GTID format, must UUID:interval[:interval]"),
		},
	}

	for _, tc := range testCases {
		mock.ExpectQuery("select @@GLOBAL.gtid_purged").WillReturnRows(
			sqlmock.NewRows([]string{"@@GLOBAL.gtid_purged"}).AddRow(tc.purgedSet.String()))
		originSet := tc.originGSet.Clone()
		newSet, err := AddGSetWithPurged(ctx, originSet, conn)
		c.Assert(errors.ErrorEqual(err, tc.err), IsTrue)
		c.Assert(newSet, DeepEquals, tc.expectedSet)
		// make sure origin gSet hasn't changed
		c.Assert(originSet, DeepEquals, tc.originGSet)
	}
}

func (t *testDBSuite) TestGetMaxConnections(c *C) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)

	rows := mock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "151")
	mock.ExpectQuery(`SHOW VARIABLES LIKE 'max_connections'`).WillReturnRows(rows)
	maxConnections, err := GetMaxConnections(ctx, db)
	c.Assert(err, IsNil)
	c.Assert(maxConnections, Equals, 151)
	c.Assert(mock.ExpectationsWereMet(), IsNil)
}

func TestIsMariaDB(t *testing.T) {
	require.True(t, IsMariaDB("5.5.50-MariaDB-1~wheezy"))
	require.False(t, IsMariaDB("5.7.19-17-log"))
}

func TestCreateTableSQLToOneRow(t *testing.T) {
	input := "CREATE TABLE `t1` (\n  `id` bigint(20) NOT NULL,\n  `c1` varchar(20) DEFAULT NULL,\n  `c2` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`) /*T![clustered_index] NONCLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin"
	expected := "CREATE TABLE `t1` ( `id` bigint(20) NOT NULL, `c1` varchar(20) DEFAULT NULL, `c2` varchar(20) DEFAULT NULL, PRIMARY KEY (`id`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin"
	require.Equal(t, expected, CreateTableSQLToOneRow(input))
}
