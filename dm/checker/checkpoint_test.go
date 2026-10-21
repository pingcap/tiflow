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

package checker

import (
	"context"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	gmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	pkgchecker "github.com/pingcap/tiflow/dm/pkg/checker"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/stretchr/testify/require"
)

const checkpointQuery = "SELECT binlog_name, binlog_pos, binlog_gtid, is_global FROM `dm_meta`.`test_syncer_checkpoint` WHERE id = ?"

func TestMetaPositionForCheck(t *testing.T) {
	const uuid = "938bc44f-4acc-11ed-a147-0242ac110003"
	configured := config.Meta{BinLogName: "mysql-bin.000010", BinLogPos: 200, BinLogGTID: uuid + ":1-100"}
	cases := []struct {
		name       string
		global     *config.Meta
		table      bool
		enableGTID bool
		queryErr   error
		expected   *config.Meta
	}{
		{name: "no checkpoint rows"},
		{name: "missing table", queryErr: &gmysql.MySQLError{Number: mysql.ErrNoSuchTable}},
		{name: "missing schema", queryErr: &gmysql.MySQLError{Number: mysql.ErrBadDB}},
		{name: "fresh global point", global: &config.Meta{BinLogPos: 4}},
		{name: "fresh GTID point", enableGTID: true, global: &config.Meta{BinLogPos: 4}},
		{name: "relay filename", global: &config.Meta{BinLogName: "mysql-bin|000001.000020", BinLogPos: 300}, expected: &config.Meta{BinLogName: "mysql-bin.000020", BinLogPos: 300}},
		{name: "file position", global: &config.Meta{BinLogName: "mysql-bin.000020", BinLogPos: 300}},
		{name: "older file position", global: &config.Meta{BinLogName: "mysql-bin.000001", BinLogPos: 100}},
		{name: "newer GTID", enableGTID: true, global: &config.Meta{BinLogGTID: uuid + ":1-200"}},
		{name: "older GTID", enableGTID: true, global: &config.Meta{BinLogGTID: uuid + ":1-50"}},
		{name: "incomparable GTID", enableGTID: true, global: &config.Meta{BinLogGTID: uuid + ":101-200"}},
		{name: "table point makes task non-fresh", table: true, expected: &config.Meta{BinLogPos: 4}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			cfg := &config.SubTaskConfig{
				Name: "test", SourceID: "source-1", MetaSchema: "dm_meta", Mode: config.ModeIncrement,
				Flavor: "mysql", Meta: &configured, SyncerConfig: config.SyncerConfig{EnableGTID: tc.enableGTID},
			}
			instance := &mysqlInstance{cfg: cfg, targetDB: &conn.BaseDB{DB: db}}
			query := mock.ExpectQuery(regexp.QuoteMeta(checkpointQuery)).WithArgs("source-1")
			expected := tc.expected
			if tc.queryErr != nil {
				query.WillReturnError(tc.queryErr)
			} else {
				rows := sqlmock.NewRows([]string{"binlog_name", "binlog_pos", "binlog_gtid", "is_global"})
				if tc.global != nil {
					rows.AddRow(tc.global.BinLogName, tc.global.BinLogPos, tc.global.BinLogGTID, true)
					if expected == nil && (tc.global.BinLogName != "" || tc.global.BinLogGTID != "") {
						expected = tc.global
					}
				}
				if tc.table {
					rows.AddRow("mysql-bin.000020", 300, "", false)
				}
				query.WillReturnRows(rows).RowsWillBeClosed()
			}
			if expected == nil {
				expected = &configured
			}
			meta, _, err := (&Checker{}).metaPositionForCheck(context.Background(), instance)
			require.NoError(t, err)
			require.Equal(t, expected, meta)
			require.Same(t, &configured, cfg.Meta, "do not replace the persisted task meta")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMetaPositionForCheckErrors(t *testing.T) {
	cases := []struct {
		name     string
		queryErr error
		rows     *sqlmock.Rows
	}{
		{name: "permission denied", queryErr: &gmysql.MySQLError{Number: mysql.ErrTableaccessDenied}},
		{name: "connection error", queryErr: errors.New("connection failed")},
		{name: "invalid checkpoint GTID", rows: sqlmock.NewRows([]string{"name", "pos", "gtid", "global"}).AddRow("bin.000001", 100, "invalid", true)},
		{name: "invalid table checkpoint GTID", rows: sqlmock.NewRows([]string{"name", "pos", "gtid", "global"}).AddRow("bin.000001", 100, "invalid", false)},
		{name: "scan error", rows: sqlmock.NewRows([]string{"name", "pos", "gtid", "global"}).AddRow("bin.000001", "invalid", "", true)},
		{name: "table removed during read", rows: sqlmock.NewRows([]string{"name", "pos", "gtid", "global"}).AddRow("bin.000001", 100, "", true).RowError(0, &gmysql.MySQLError{Number: mysql.ErrNoSuchTable})},
		{name: "row error", rows: sqlmock.NewRows([]string{"name", "pos", "gtid", "global"}).AddRow("bin.000001", 100, "", true).RowError(0, errors.New("read failed"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			query := mock.ExpectQuery(regexp.QuoteMeta(checkpointQuery)).WithArgs("source-1")
			if tc.queryErr != nil {
				query.WillReturnError(tc.queryErr)
			} else {
				query.WillReturnRows(tc.rows).RowsWillBeClosed()
			}
			cfg := &config.SubTaskConfig{Name: "test", SourceID: "source-1", MetaSchema: "dm_meta", Mode: config.ModeIncrement, Flavor: "mysql"}
			_, _, err = (&Checker{}).metaPositionForCheck(context.Background(), &mysqlInstance{cfg: cfg, targetDB: &conn.BaseDB{DB: db}})
			require.Error(t, err, "must not silently fall back to configured meta")
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestMetaPositionForCheckSkipCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mode       string
		removeMeta bool
		binlogName string
	}{
		{name: "remove-meta", mode: config.ModeIncrement, removeMeta: true},
		{name: "all mode", mode: config.ModeAll},
		{name: "start-time", mode: config.ModeIncrement, binlogName: binlog.FakeBinlogName},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.SubTaskConfig{Mode: tc.mode, Meta: &config.Meta{BinLogName: tc.binlogName}}
			c := NewChecker(nil, nil, 0, 0, WithRemoveMeta(tc.removeMeta))
			// No DB: these paths must not even attempt to load an old checkpoint.
			meta, _, err := c.metaPositionForCheck(context.Background(), &mysqlInstance{cfg: cfg})
			require.NoError(t, err)
			require.Same(t, cfg.Meta, meta)
		})
	}
}

func TestCheckpointMetaPositionChecking(t *testing.T) {
	for _, removeMeta := range []bool{false, true} {
		for _, enableGTID := range []bool{false, true} {
			cfg := &config.SubTaskConfig{
				Name: "test", SourceID: "source-1", MetaSchema: "dm_meta", Mode: config.ModeIncrement,
				Meta:         &config.Meta{BinLogName: "mysql-bin.000001", BinLogPos: 100, BinLogGTID: "ignored-invalid-gtid"},
				SyncerConfig: config.SyncerConfig{EnableGTID: enableGTID},
			}
			expected := cfg.Meta
			mock := initMockDB(t)
			if !removeMeta {
				expected = &config.Meta{BinLogName: "mysql-bin.000010", BinLogPos: 200, BinLogGTID: "938bc44f-4acc-11ed-a147-0242ac110003:1-100"}
				mock.ExpectQuery(regexp.QuoteMeta(checkpointQuery)).WithArgs("source-1").WillReturnRows(
					sqlmock.NewRows([]string{"name", "pos", "gtid", "global"}).AddRow(expected.BinLogName, expected.BinLogPos, expected.BinLogGTID, true)).RowsWillBeClosed()
			}
			c := NewChecker([]*config.SubTaskConfig{cfg}, map[string]string{config.MetaPositionChecking: ""}, 0, 0, WithRemoveMeta(removeMeta))
			require.NoError(t, c.Init(context.Background()))
			require.Len(t, c.checkList, 1)
			inst := c.instances[0]
			require.Equal(t, pkgchecker.NewMetaPositionChecker(inst.sourceDB, inst.cfg.From, enableGTID, expected), c.checkList[0])
			require.Equal(t, "mysql-bin.000001", cfg.Meta.BinLogName)
			require.NoError(t, mock.ExpectationsWereMet())
			c.Close()
		}
	}
}

func TestCheckpointWithoutGTID(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta(checkpointQuery)).WithArgs("source-1").WillReturnRows(
		sqlmock.NewRows([]string{"name", "pos", "gtid", "global"}).AddRow("mysql-bin.000010", 200, nil, true)).RowsWillBeClosed()
	cfg := &config.SubTaskConfig{
		Name: "test", SourceID: "source-1", MetaSchema: "dm_meta", Mode: config.ModeIncrement,
		Flavor: "mysql", SyncerConfig: config.SyncerConfig{EnableGTID: true},
	}
	meta, enableGTID, err := (&Checker{}).metaPositionForCheck(context.Background(), &mysqlInstance{cfg: cfg, targetDB: &conn.BaseDB{DB: db}})
	require.NoError(t, err)
	require.False(t, enableGTID, "syncer will read the file position to recover the GTID")
	require.Equal(t, &config.Meta{BinLogName: "mysql-bin.000010", BinLogPos: 200}, meta)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCheckSyncConfigRemoveMeta(t *testing.T) {
	for _, removeMeta := range []bool{false, true} {
		cfg := &config.SubTaskConfig{
			Name: "test", SourceID: "source-1", MetaSchema: "dm_meta", Mode: config.ModeIncrement,
			Meta:                &config.Meta{BinLogName: "mysql-bin.000001", BinLogPos: 100},
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.MetaPositionChecking: {}}),
		}
		mock := initMockDB(t)
		stop := "checkpoint read"
		if removeMeta {
			// Reach the upstream check without reading or deleting any checkpoint.
			stop = "configured position check"
			mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnError(errors.New(stop))
		} else {
			mock.ExpectQuery(regexp.QuoteMeta(checkpointQuery)).WithArgs("source-1").WillReturnError(errors.New(stop))
		}
		_, err := CheckSyncConfig(context.Background(), []*config.SubTaskConfig{cfg}, 10, 10, WithRemoveMeta(removeMeta))
		require.ErrorContains(t, err, stop)
		require.NoError(t, mock.ExpectationsWereMet())
	}
}
