// Copyright 2021 PingCAP, Inc.
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

package conn

import (
	"context"

	. "github.com/pingcap/check"

	"github.com/pingcap/tiflow/dm/dm/config"
)

var _ = Suite(testUtilSuite{})

type testUtilSuite struct{}

<<<<<<< HEAD
func (s testUtilSuite) TestFetchTZSetting(c *C) {
	m := InitMockDB(c)

	m.ExpectQuery("SELECT cast\\(TIMEDIFF\\(NOW\\(6\\), UTC_TIMESTAMP\\(6\\)\\) as time\\);").
		WillReturnRows(m.NewRows([]string{""}).AddRow("01:00:00"))
	tz, err := FetchTimeZoneSetting(context.Background(), &config.DBConfig{})
	c.Assert(err, IsNil)
	c.Assert(tz, Equals, "+01:00")
=======
	cases := []struct {
		rows           *sqlmock.Rows
		binlogName     string
		pos            uint64
		binlogDoDB     string
		binlogIgnoreDB string
		gtidStr        string
		err            error
		flavor         string
	}{
		// For MySQL
		{
			sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
				AddRow("ON.000001", 4822, "", "", "85ab69d1-b21f-11e6-9c5e-64006a8978d2:1-46"),
			"ON.000001",
			4822,
			"",
			"",
			"85ab69d1-b21f-11e6-9c5e-64006a8978d2:1-46",
			nil,
			gmysql.MySQLFlavor,
		},
		// test unit64 position for MySQL
		{
			sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).
				AddRow("ON.000002", 429496729500, "", "", "85ab69d1-b21f-11e6-9c5e-64006a8978d2:1-500"),
			"ON.000002",
			429496729500,
			"",
			"",
			"85ab69d1-b21f-11e6-9c5e-64006a8978d2:1-500",
			nil,
			gmysql.MySQLFlavor,
		},
		// For MariaDB
		{
			sqlmock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB"}).
				AddRow("mariadb-bin.000016", 475, "", ""),
			"mariadb-bin.000016",
			475,
			"",
			"",
			"0-1-2",
			nil,
			gmysql.MariaDBFlavor,
		},
	}
	for _, ca := range cases {
		mock.ExpectQuery("SHOW MASTER STATUS").WillReturnRows(ca.rows)
		// For MariaDB
		if ca.flavor == gmysql.MariaDBFlavor {
			mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'gtid_binlog_pos'").WillReturnRows(
				sqlmock.NewRows([]string{"Variable_name", "Value"}).
					AddRow("gtid_binlog_pos", "0-1-2"),
			)
		}
		binlogName, pos, binlogDoDB, binlogIgnoreDB, gtidStr, err := GetMasterStatus(tctx, baseDB, ca.flavor)
		require.IsType(t, uint64(0), pos)
		require.NoError(t, err)
		require.Equal(t, ca.binlogName, binlogName)
		require.Equal(t, ca.pos, pos)
		require.Equal(t, ca.binlogDoDB, binlogDoDB)
		require.Equal(t, ca.binlogIgnoreDB, binlogIgnoreDB)
		require.Equal(t, ca.gtidStr, gtidStr)
		require.NoError(t, mock.ExpectationsWereMet())
	}
>>>>>>> 195353e150 (pkg(dm): modify the type of pos returned by function GetMasterStatus (#7815))
}
