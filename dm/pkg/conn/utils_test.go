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
