// Copyright 2023 PingCAP, Inc.
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

package sql

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	ormUtil "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func newMockDB(t *testing.T) (*sql.DB, *gorm.DB, sqlmock.Sqlmock) {
	backendDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)

	mock.ExpectQuery("SELECT VERSION()").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.35-log"))
	db, err := ormUtil.NewGormDB(backendDB, "mysql")
	require.NoError(t, err)
	return backendDB, db, mock
}

func TestAutoMigrateTables(t *testing.T) {
	t.Parallel()
	t.Skip("skip this test since it's unstable")

	backendDB, db, mock := newMockDB(t)
	defer backendDB.Close()

	mockSchmaFn := func(_ string) {
		mock.ExpectQuery("SELECT SCHEMA_NAME from Information_schema.SCHEMATA " +
			"where SCHEMA_NAME LIKE ? ORDER BY SCHEMA_NAME=? DESC limit 1").WillReturnRows(
			sqlmock.NewRows([]string{"SCHEMA_NAME"}))
	}

	mockSchmaFn("upstream")
	mock.ExpectExec("CREATE TABLE `upstream` (`id` bigint(20) unsigned,`endpoints` text NOT NULL," +
		"`config` text,`version` bigint(20) unsigned NOT NULL,`update_at` datetime(6) NOT NULL," +
		"PRIMARY KEY (`id`))").
		WillReturnResult(sqlmock.NewResult(0, 0))

	mockSchmaFn("changefeed_info")
	mock.ExpectExec("CREATE TABLE `changefeed_info` (`uuid` bigint(20) unsigned,`namespace` varchar(128) NOT NULL," +
		"`id` varchar(128) NOT NULL,`upstream_id` bigint(20) unsigned NOT NULL,`sink_uri` text NOT NULL," +
		"`start_ts` bigint(20) unsigned NOT NULL,`target_ts` bigint(20) unsigned NOT NULL,`config` longtext NOT NULL," +
		"`removed_at` datetime(6),`version` bigint(20) unsigned NOT NULL,`update_at` datetime(6) NOT NULL," +
		"PRIMARY KEY (`uuid`),UNIQUE INDEX `namespace` (`namespace`,`id`),INDEX `upstream_id` (`upstream_id`))").
		WillReturnResult(sqlmock.NewResult(0, 0))

	mockSchmaFn("changefeed_state")
	mock.ExpectExec("CREATE TABLE `changefeed_state` (`changefeed_uuid` bigint(20) unsigned,`state` text NOT NULL," +
		"`warning` text,`error` text,`version` bigint(20) unsigned NOT NULL,`update_at` datetime(6) NOT NULL," +
		"PRIMARY KEY (`changefeed_uuid`))").
		WillReturnResult(sqlmock.NewResult(0, 0))

	mockSchmaFn("schedule")
	mock.ExpectExec("CREATE TABLE `schedule` (`changefeed_uuid` bigint(20) unsigned,`owner` varchar(128)," +
		"`owner_state` text NOT NULL,`processors` text,`task_position` text NOT NULL,`version` bigint(20) unsigned NOT NULL," +
		"`update_at` datetime(6) NOT NULL,PRIMARY KEY (`changefeed_uuid`))").
		WillReturnResult(sqlmock.NewResult(0, 0))

	mockSchmaFn("progress")
	mock.ExpectExec("CREATE TABLE `progress` (`capture_id` varchar(128),`progress` longtext," +
		"`version` bigint(20) unsigned NOT NULL,`update_at` datetime(6) NOT NULL,PRIMARY KEY (`capture_id`))").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := AutoMigrate(db)
	require.NoError(t, err)
}
