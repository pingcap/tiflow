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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
)

func TestGetBinlogDB(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultDBTimeout)
	defer cancel()

	tctx := tcontext.NewContext(ctx, log.L())

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	baseDB := NewBaseDB(db)

	// 5 columns for MySQL
	rows := mock.NewRows([]string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}).AddRow(
		"mysql-bin.000009", 11232, "do_db", "ignore_db", "074be7f4-f0f1-11ea-95bd-0242ac120002:1-699",
	)
	mock.ExpectQuery(`SHOW MASTER STATUS`).WillReturnRows(rows)

	binlogDoDB, binlogIgnoreDB, err := GetBinlogDB(tctx, baseDB, "mysql")
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

	binlogDoDB, binlogIgnoreDB, err = GetBinlogDB(tctx, baseDB, "mariadb")
	require.Nil(t, err)
	require.Equal(t, binlogDoDB, "do_db")
	require.Equal(t, binlogIgnoreDB, "ignore_db")
	require.Nil(t, mock.ExpectationsWereMet())
}

func TestGetMasterStatus(t *testing.T) {
	ctx := context.Background()
	tctx := tcontext.NewContext(ctx, log.L())

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	baseDB := NewBaseDB(db)

	cases := []struct {
		rows           *sqlmock.Rows
		binlogName     string
		pos            uint32
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
		require.NoError(t, err)
		require.Equal(t, ca.binlogName, binlogName)
		require.Equal(t, ca.pos, pos)
		require.Equal(t, ca.binlogDoDB, binlogDoDB)
		require.Equal(t, ca.binlogIgnoreDB, binlogIgnoreDB)
		require.Equal(t, ca.gtidStr, gtidStr)
		require.NoError(t, mock.ExpectationsWereMet())
	}
}
