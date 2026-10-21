// Copyright 2026 PingCAP, Inc.
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

package cputil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/stretchr/testify/require"
)

var positionColumns = []string{"binlog_name", "binlog_pos", "binlog_gtid", "is_global"}

func TestLoadSyncerCheckpoint(t *testing.T) {
	for _, details := range []bool{false, true} {
		for _, flavor := range []string{mysql.MySQLFlavor, mysql.MariaDBFlavor} {
			t.Run(fmt.Sprintf("%s/details=%t", flavor, details), func(t *testing.T) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				gtidStr := "938bc44f-4acc-11ed-a147-0242ac110003:1-100"
				if flavor == mysql.MariaDBFlavor {
					gtidStr = "0-1-100"
				}
				projection := "binlog_name, binlog_pos, binlog_gtid, is_global"
				rows := sqlmock.NewRows(positionColumns).
					AddRow("mysql-bin.000010", 200, gtidStr, true).
					AddRow("mysql-bin.000010", 300, nil, false)
				if details {
					projection = "cp_schema, cp_table, binlog_name, binlog_pos, binlog_gtid, exit_safe_binlog_name, exit_safe_binlog_pos, exit_safe_binlog_gtid, table_info, is_global"
					rows = sqlmock.NewRows([]string{"schema", "table", "name", "pos", "gtid", "exitName", "exitPos", "exitGTID", "tableInfo", "global"}).
						AddRow("", "", "mysql-bin.000010", 200, gtidStr, "mysql-bin.000011", 400, gtidStr, "null", true).
						AddRow("db", "tbl", "mysql-bin.000010", 300, nil, "", 0, nil, `{"id":42}`, false)
				}
				// No metadata writes are expected in either projection.
				mock.ExpectQuery(regexp.QuoteMeta("SELECT " + projection + " FROM `meta`.`task_syncer_checkpoint` WHERE id = ?")).
					WithArgs("source-1").WillReturnRows(rows).RowsWillBeClosed()
				var points []SyncerCheckpointRow
				query := func(query string, args ...interface{}) (*sql.Rows, error) {
					return db.QueryContext(context.Background(), query, args...)
				}
				err = LoadSyncerCheckpoint(query, "`meta`.`task_syncer_checkpoint`", "source-1", flavor, details,
					func(row SyncerCheckpointRow) error {
						points = append(points, row)
						return nil
					})
				require.NoError(t, err)
				require.Len(t, points, 2)
				require.True(t, points[0].IsGlobal)
				require.Equal(t, mysql.Position{Name: "mysql-bin.000010", Pos: 200}, points[0].Location.Position)
				require.Equal(t, gtidStr, points[0].Location.GTIDSetStr())
				require.False(t, points[1].IsGlobal)
				require.True(t, gtid.CheckGTIDSetEmpty(points[1].Location.GetGTID()))
				if details {
					require.Equal(t, "mysql-bin.000011", points[0].ExitSafeBinlogName)
					require.Equal(t, uint32(400), points[0].ExitSafeBinlogPos)
					require.Equal(t, sql.NullString{String: gtidStr, Valid: true}, points[0].ExitSafeBinlogGTID)
					require.Equal(t, "db", points[1].Schema)
					require.Equal(t, "tbl", points[1].Table)
					require.Equal(t, `{"id":42}`, string(points[1].TableInfo))
				} else {
					require.Nil(t, points[1].TableInfo)
				}
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}
}

func TestLoadSyncerCheckpointErrors(t *testing.T) {
	sentinel := errors.New("stop loading checkpoint")
	cases := []struct {
		name     string
		rows     *sqlmock.Rows
		queryErr error
		visitErr error
		wantErr  string
	}{
		{name: "empty", rows: sqlmock.NewRows(positionColumns)},
		{name: "query error", queryErr: sentinel, wantErr: sentinel.Error()},
		{name: "missing table", queryErr: &dmysql.MySQLError{Number: 1146, Message: "missing table"}, wantErr: "missing table"},
		{name: "invalid global GTID", rows: sqlmock.NewRows(positionColumns).AddRow("bin.000001", 100, "invalid", true), wantErr: "invalid"},
		{name: "invalid table GTID", rows: sqlmock.NewRows(positionColumns).AddRow("bin.000001", 100, "invalid", false), wantErr: "invalid"},
		{name: "scan error", rows: sqlmock.NewRows(positionColumns).AddRow("bin.000001", "invalid", "", true), wantErr: "invalid"},
		{name: "row error", rows: sqlmock.NewRows(positionColumns).AddRow("bin.000001", 100, "", true).RowError(0, sentinel), wantErr: sentinel.Error()},
		{name: "visitor error", rows: sqlmock.NewRows(positionColumns).AddRow("bin.000001", 100, "", true), visitErr: sentinel, wantErr: sentinel.Error()},
		// Retain ownership of rows even when a caller's query adapter injects an error.
		{name: "rows and query error", rows: sqlmock.NewRows(positionColumns).AddRow("bin.000001", 100, "", true), queryErr: sentinel, wantErr: sentinel.Error()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			query := mock.ExpectQuery("SELECT .* FROM `cp` WHERE id = \\?").WithArgs("source-1")
			if tc.rows == nil {
				query.WillReturnError(tc.queryErr)
			} else {
				query.WillReturnRows(tc.rows).RowsWillBeClosed()
			}
			queryFn := func(query string, args ...interface{}) (*sql.Rows, error) {
				rows, err := db.QueryContext(context.Background(), query, args...)
				if tc.rows != nil && tc.queryErr != nil {
					err = tc.queryErr
				}
				return rows, err
			}
			err = LoadSyncerCheckpoint(queryFn, "`cp`", "source-1", mysql.MySQLFlavor, false,
				func(SyncerCheckpointRow) error { return tc.visitErr })
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
			if tc.name == "missing table" {
				require.True(t, conn.IsMySQLError(err, 1146), "preserve the cause for caller-specific missing-table handling")
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestIsFreshSyncerCheckpoint(t *testing.T) {
	for _, flavor := range []string{mysql.MySQLFlavor, mysql.MariaDBFlavor} {
		for _, enableGTID := range []bool{false, true} {
			zero := binlog.MustZeroLocation(flavor)
			require.True(t, IsFreshSyncerCheckpoint(zero, false, flavor, enableGTID))
			require.False(t, IsFreshSyncerCheckpoint(zero, true, flavor, enableGTID))
			position := zero.Clone()
			position.Position = mysql.Position{Name: "bin.000001", Pos: 100}
			require.False(t, IsFreshSyncerCheckpoint(position, false, flavor, enableGTID))
		}
		value := "938bc44f-4acc-11ed-a147-0242ac110003:1-100"
		if flavor == mysql.MariaDBFlavor {
			value = "0-1-100"
		}
		set, err := gtid.ParserGTID(flavor, value)
		require.NoError(t, err)
		require.False(t, IsFreshSyncerCheckpoint(binlog.NewLocation(binlog.MinPosition, set), false, flavor, true))
	}
}
