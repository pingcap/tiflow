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
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/stretchr/testify/require"
)

func TestGetFlavor(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	// MySQL
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'version';`).WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("version", "5.7.31-log"))
	flavor, err := GetFlavor(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, "mysql", flavor)
	require.NoError(t, mock.ExpectationsWereMet())

	// MariaDB
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'version';`).WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("version", "10.13.1-MariaDB-1~wheezy"))
	flavor, err = GetFlavor(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, "mariadb", flavor)
	require.NoError(t, mock.ExpectationsWereMet())

	// others
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'version';`).WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("version", "unknown"))
	flavor, err = GetFlavor(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, "mysql", flavor) // as MySQL
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetRandomServerID(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	createMockResult(mock, 1, []uint32{100, 101}, "mysql")
	serverID, err := GetRandomServerID(context.Background(), db)
	require.NoError(t, err)
	require.Greater(t, serverID, uint32(0))
	require.NoError(t, mock.ExpectationsWereMet())
	require.NotEqual(t, 1, serverID)
	require.NotEqual(t, 100, serverID)
	require.NotEqual(t, 101, serverID)
}

func TestGetMariaDBGtidDomainID(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	rows := mock.NewRows([]string{"Variable_name", "Value"}).AddRow("gtid_domain_id", 101)
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'gtid_domain_id'`).WillReturnRows(rows)

	dID, err := GetMariaDBGtidDomainID(ctx, db)
	require.NoError(t, err)
	require.Equal(t, uint32(101), dID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetServerUUID(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	// MySQL
	rows := mock.NewRows([]string{"Variable_name", "Value"}).AddRow("server_uuid", "074be7f4-f0f1-11ea-95bd-0242ac120002")
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'server_uuid'`).WillReturnRows(rows)
	uuid, err := GetServerUUID(ctx, db, "mysql")
	require.NoError(t, err)
	require.Equal(t, "074be7f4-f0f1-11ea-95bd-0242ac120002", uuid)
	require.NoError(t, mock.ExpectationsWereMet())

	// MariaDB
	rows = mock.NewRows([]string{"Variable_name", "Value"}).AddRow("gtid_domain_id", 123)
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'gtid_domain_id'`).WillReturnRows(rows)
	rows = mock.NewRows([]string{"Variable_name", "Value"}).AddRow("server_id", 456)
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'server_id'`).WillReturnRows(rows)
	uuid, err = GetServerUUID(ctx, db, "mariadb")
	require.NoError(t, err)
	require.Equal(t, "123-456", uuid)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetServerUnixTS(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	ts := time.Now().Unix()
	rows := sqlmock.NewRows([]string{"UNIX_TIMESTAMP()"}).AddRow(strconv.FormatInt(ts, 10))
	mock.ExpectQuery("SELECT UNIX_TIMESTAMP()").WillReturnRows(rows)

	ts2, err := GetServerUnixTS(ctx, db)
	require.NoError(t, err)
	require.Equal(t, ts2, ts)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetParser(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	var (
		DDL1 = `ALTER TABLE tbl ADD COLUMN c1 INT`
		DDL2 = `ALTER TABLE tbl ADD COLUMN 'c1' INT`
		DDL3 = `ALTER TABLE tbl ADD COLUMN "c1" INT`
	)

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	// no `ANSI_QUOTES`
	rows := mock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "")
	mock.ExpectQuery(`SHOW VARIABLES LIKE 'sql_mode'`).WillReturnRows(rows)
	p, err := GetParser(ctx, db)
	require.NoError(t, err)
	_, err = p.ParseOneStmt(DDL1, "", "")
	require.NoError(t, err)
	_, err = p.ParseOneStmt(DDL2, "", "")
	require.Error(t, err)
	_, err = p.ParseOneStmt(DDL3, "", "")
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())

	// `ANSI_QUOTES`
	rows = mock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", "ANSI_QUOTES")
	mock.ExpectQuery(`SHOW VARIABLES LIKE 'sql_mode'`).WillReturnRows(rows)
	p, err = GetParser(ctx, db)
	require.NoError(t, err)
	_, err = p.ParseOneStmt(DDL1, "", "")
	require.NoError(t, err)
	_, err = p.ParseOneStmt(DDL2, "", "")
	require.Error(t, err)
	_, err = p.ParseOneStmt(DDL3, "", "")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetGTID(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	rows := mock.NewRows([]string{"Variable_name", "Value"}).AddRow("GTID_MODE", "ON")
	mock.ExpectQuery(`SHOW GLOBAL VARIABLES LIKE 'GTID_MODE'`).WillReturnRows(rows)
	mode, err := GetGTIDMode(ctx, db)
	require.NoError(t, err)
	require.Equal(t, "ON", mode)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestMySQLError(t *testing.T) {
	t.Parallel()

	err := newMysqlErr(tmysql.ErrNoSuchThread, "Unknown thread id: 111")
	require.Equal(t, true, IsNoSuchThreadError(err))

	err = newMysqlErr(tmysql.ErrMasterFatalErrorReadingBinlog, "binlog purged error")
	require.Equal(t, true, IsErrBinlogPurged(err))

	err = newMysqlErr(tmysql.ErrDupEntry, "Duplicate entry '123456' for key 'index'")
	require.Equal(t, true, IsErrDuplicateEntry(err))
}

func TestGetAllServerID(t *testing.T) {
	t.Parallel()

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
	require.NoError(t, err)

	flavors := []string{gmysql.MariaDBFlavor, gmysql.MySQLFlavor}

	for _, testCase := range testCases {
		for _, flavor := range flavors {
			createMockResult(mock, testCase.masterID, testCase.serverIDs, flavor)
			serverIDs, err2 := GetAllServerID(context.Background(), db)
			require.NoError(t, err2)

			for _, serverID := range testCase.serverIDs {
				_, ok := serverIDs[serverID]
				require.True(t, ok)
			}

			_, ok := serverIDs[testCase.masterID]
			require.True(t, ok)
		}
	}

	err = mock.ExpectationsWereMet()
	require.NoError(t, err)
}

func createMockResult(mock sqlmock.Sqlmock, masterID uint32, serverIDs []uint32, flavor string) {
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

func TestTiDBVersion(t *testing.T) {
	t.Parallel()

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
			require.Error(t, err)
			require.Equal(t, tc.err.Error(), err.Error())
		} else {
			require.Equal(t, tc.result, tidbVer)
		}
	}
}

func getGSetFromString(t *testing.T, s string) gmysql.GTIDSet {
	t.Helper()
	gSet, err := gtid.ParserGTID("mysql", s)
	require.NoError(t, err)
	return gSet
}

func TestAddGSetWithPurged(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mariaGTID, err := gtid.ParserGTID("mariadb", "1-2-100")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)

	testCases := []struct {
		originGSet  gmysql.GTIDSet
		purgedSet   gmysql.GTIDSet
		expectedSet gmysql.GTIDSet
		err         error
	}{
		{
			getGSetFromString(t, "3ccc475b-2343-11e7-be21-6c0b84d59f30:6-14"),
			getGSetFromString(t, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-5"),
			getGSetFromString(t, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-14"),
			nil,
		}, {
			getGSetFromString(t, "3ccc475b-2343-11e7-be21-6c0b84d59f30:2-6"),
			getGSetFromString(t, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1"),
			getGSetFromString(t, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-6"),
			nil,
		}, {
			getGSetFromString(t, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-6"),
			getGSetFromString(t, "53bfca22-690d-11e7-8a62-18ded7a37b78:1-495"),
			getGSetFromString(t, "3ccc475b-2343-11e7-be21-6c0b84d59f30:1-6,53bfca22-690d-11e7-8a62-18ded7a37b78:1-495"),
			nil,
		}, {
			getGSetFromString(t, "3ccc475b-2343-11e7-be21-6c0b84d59f30:6-14"),
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
		require.True(t, errors.ErrorEqual(err, tc.err))
		require.Equal(t, tc.expectedSet, newSet)
		// make sure origin gSet hasn't changed
		require.Equal(t, tc.originGSet, originSet)
	}
}

func TestGetMaxConnections(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	rows := mock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "151")
	mock.ExpectQuery(`SHOW VARIABLES LIKE 'max_connections'`).WillReturnRows(rows)
	maxConnections, err := GetMaxConnections(ctx, db)
	require.NoError(t, err)
	require.Equal(t, 151, maxConnections)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestIsMariaDB(t *testing.T) {
	t.Parallel()

	require.True(t, IsMariaDB("5.5.50-MariaDB-1~wheezy"))
	require.False(t, IsMariaDB("5.7.19-17-log"))
}

func TestCreateTableSQLToOneRow(t *testing.T) {
	t.Parallel()

	input := "CREATE TABLE `t1` (\n  `id` bigint(20) NOT NULL,\n  `c1` varchar(20) DEFAULT NULL,\n  `c2` varchar(20) DEFAULT NULL,\n  PRIMARY KEY (`id`) /*T![clustered_index] NONCLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin"
	expected := "CREATE TABLE `t1` ( `id` bigint(20) NOT NULL, `c1` varchar(20) DEFAULT NULL, `c2` varchar(20) DEFAULT NULL, PRIMARY KEY (`id`) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin"
	require.Equal(t, expected, CreateTableSQLToOneRow(input))
}

func TestGetSlaveServerID(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	cases := []struct {
		rows    *sqlmock.Rows
		results map[uint32]struct{}
	}{
		// For MySQL
		{
			sqlmock.NewRows([]string{"Server_id", "Host", "Port", "Master_id", "Slave_UUID"}).
				AddRow(192168010, "iconnect2", 3306, 192168011, "14cb6624-7f93-11e0-b2c0-c80aa9429562").
				AddRow(1921680101, "athena", 3306, 192168011, "07af4990-f41f-11df-a566-7ac56fdaf645"),
			map[uint32]struct{}{
				192168010: {}, 1921680101: {},
			},
		},
		// For MariaDB
		{
			sqlmock.NewRows([]string{"Server_id", "Host", "Port", "Master_id"}).
				AddRow(192168010, "iconnect2", 3306, 192168011).
				AddRow(1921680101, "athena", 3306, 192168011),
			map[uint32]struct{}{
				192168010: {}, 1921680101: {},
			},
		},
		// For MariaDB, with Server_id greater than 2^31, to test uint conversion
		{
			sqlmock.NewRows([]string{"Server_id", "Host", "Port", "Master_id"}).
				AddRow(2147483649, "iconnect2", 3306, 192168011).
				AddRow(2147483650, "athena", 3306, 192168011),
			map[uint32]struct{}{
				2147483649: {}, 2147483650: {},
			},
		},
	}

	for _, ca := range cases {
		mock.ExpectQuery("SHOW SLAVE HOSTS").WillReturnRows(ca.rows)
		results, err2 := GetSlaveServerID(context.Background(), db)
		require.NoError(t, err2)
		require.Equal(t, ca.results, results)
	}
}
