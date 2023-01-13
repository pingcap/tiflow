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

package binlog

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/stretchr/testify/require"
)

func TestGetBinaryLogs(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	ctx := tcontext.Background()
	baseDB := conn.NewBaseDBForTest(db)

	cases := []struct {
		rows  *sqlmock.Rows
		sizes FileSizes
	}{
		{
			sqlmock.NewRows([]string{"Log_name", "File_size"}).
				AddRow("mysql-bin.000001", 52119).
				AddRow("mysql-bin.000002", 114),
			[]binlogSize{
				{
					"mysql-bin.000001", 52119,
				},
				{
					"mysql-bin.000002", 114,
				},
			},
		},
		{
			sqlmock.NewRows([]string{"Log_name", "File_size", "Encrypted"}).
				AddRow("mysql-bin.000001", 52119, "No").
				AddRow("mysql-bin.000002", 114, "No"),
			[]binlogSize{
				{
					"mysql-bin.000001", 52119,
				},
				{
					"mysql-bin.000002", 114,
				},
			},
		},
	}

	for _, ca := range cases {
		mock.ExpectQuery("SHOW BINARY LOGS").WillReturnRows(ca.rows)
		sizes, err2 := GetBinaryLogs(ctx, baseDB)
		require.NoError(t, err2)
		require.Equal(t, ca.sizes, sizes)
		require.NoError(t, mock.ExpectationsWereMet())
	}

	mock.ExpectQuery("SHOW BINARY LOGS").WillReturnError(&mysql.MySQLError{
		Number:  1227,
		Message: "Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation",
	})
	_, err2 := GetBinaryLogs(ctx, baseDB)
	require.Error(t, err2)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestBinlogSizesAfter(t *testing.T) {
	t.Parallel()
	sizes := FileSizes{
		{name: "mysql-bin.999999", size: 1},
		{name: "mysql-bin.1000000", size: 2},
		{name: "mysql-bin.1000001", size: 4},
	}

	cases := []struct {
		position gmysql.Position
		expected int64
	}{
		{
			gmysql.Position{Name: "mysql-bin.999999", Pos: 0},
			7,
		},
		{
			gmysql.Position{Name: "mysql-bin.1000000", Pos: 1},
			5,
		},
		{
			gmysql.Position{Name: "mysql-bin.1000001", Pos: 3},
			1,
		},
	}

	for _, ca := range cases {
		require.Equal(t, ca.expected, sizes.After(ca.position))
	}
}
