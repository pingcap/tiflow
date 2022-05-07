// Copyright 2022 PingCAP, Inc.
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

package checkpoint

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/errors"
	dmconfig "github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/lib"
)

func TestTableName(t *testing.T) {
	t.Parallel()
	jobCfg := &config.JobCfg{Name: "test", MetaSchema: "meta"}
	require.Equal(t, loadTableName(jobCfg), "`meta`.`test_lightning_checkpoint_list`")
	require.Equal(t, syncTableName(jobCfg), "`meta`.`test_syncer_checkpoint`")
	require.Equal(t, shardMetaName(jobCfg), "`meta`.`test_syncer_sharding_meta`")
	require.Equal(t, onlineDDLName(jobCfg), "`meta`.`test_onlineddl`")
}

func TestCheckpoint(t *testing.T) {
	jobCfg := &config.JobCfg{Name: "test", MetaSchema: "meta", TaskMode: dmconfig.ModeAll}
	db, mock, err := conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectExec(regexp.QuoteMeta(fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, "`meta`"))).WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t, createMetaDatabase(context.Background(), jobCfg, conn.NewBaseDB(db)))

	mock.ExpectExec(regexp.QuoteMeta(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		task_name varchar(255) NOT NULL,
		source_name varchar(255) NOT NULL,
		status varchar(10) NOT NULL DEFAULT 'init' COMMENT 'init,running,finished',
		PRIMARY KEY (task_name, source_name)
	);`, "`meta`.`test_lightning_checkpoint_list`"))).WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t, createLoadCheckpointTable(context.Background(), jobCfg, conn.NewBaseDB(db)))

	mock.ExpectExec(regexp.QuoteMeta(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id VARCHAR(32) NOT NULL,
		cp_schema VARCHAR(128) NOT NULL,
		cp_table VARCHAR(128) NOT NULL,
		binlog_name VARCHAR(128),
		binlog_pos INT UNSIGNED,
		binlog_gtid TEXT,
		exit_safe_binlog_name VARCHAR(128) DEFAULT '',
		exit_safe_binlog_pos INT UNSIGNED DEFAULT 0,
		exit_safe_binlog_gtid TEXT,
		table_info JSON NOT NULL,
		is_global BOOLEAN,
		create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE KEY uk_id_schema_table (id, cp_schema, cp_table)
	)`, "`meta`.`test_syncer_checkpoint`"))).WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t, createSyncCheckpointTable(context.Background(), jobCfg, conn.NewBaseDB(db)))

	mock.ExpectExec(regexp.QuoteMeta("DROP TABLE IF EXISTS `meta`.`test_lightning_checkpoint_list`")).WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t, dropLoadCheckpointTable(context.Background(), jobCfg, conn.NewBaseDB(db)))

	mock.ExpectExec(regexp.QuoteMeta("DROP TABLE IF EXISTS `meta`.`test_syncer_checkpoint`")).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("DROP TABLE IF EXISTS `meta`.`test_syncer_sharding_meta`")).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("DROP TABLE IF EXISTS `meta`.`test_onlineddl`")).WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t, dropSyncCheckpointTable(context.Background(), jobCfg, conn.NewBaseDB(db)))
}

func TestCheckpointLifeCycle(t *testing.T) {
	jobCfg := &config.JobCfg{Name: "test", MetaSchema: "meta", TaskMode: dmconfig.ModeAll}
	checkpointAgent := NewAgentImpl(jobCfg)
	require.Equal(t, checkpointAgent.GetConfig(), jobCfg)
	jobCfg2 := &config.JobCfg{Name: "test2", MetaSchema: "meta", TaskMode: dmconfig.ModeAll}
	checkpointAgent.UpdateConfig(jobCfg2)
	require.Equal(t, checkpointAgent.GetConfig(), jobCfg2)
	require.NotEqual(t, jobCfg, jobCfg2)

	// create meta database error
	_, mock, err := conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectExec(".*").WillReturnError(errors.New("invalid connection"))
	require.Error(t, checkpointAgent.Init(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())

	// create load checkpoint error
	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnError(errors.New("invalid connection"))
	require.Error(t, checkpointAgent.Init(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())

	// create sync checkpoint error
	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnError(errors.New("invalid connection"))
	require.Error(t, checkpointAgent.Init(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())

	// create all checkpoint tables
	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t, checkpointAgent.Init(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())

	// create load checkpoint only
	jobCfg.TaskMode = dmconfig.ModeFull
	checkpointAgent.UpdateConfig(jobCfg)
	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t, checkpointAgent.Init(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())

	// create sync checkpoint only
	jobCfg.TaskMode = dmconfig.ModeIncrement
	checkpointAgent.UpdateConfig(jobCfg)
	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	require.NoError(t, checkpointAgent.Init(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())

	// drop load checkpoint error
	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectExec(".*").WillReturnError(errors.New("invalid connection"))
	require.Error(t, checkpointAgent.Remove(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())

	// drop sync checkpoint error
	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnError(errors.New("invalid connection"))
	require.Error(t, checkpointAgent.Remove(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())

	// drop shard-meta error
	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnError(errors.New("invalid connection"))
	require.Error(t, checkpointAgent.Remove(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())

	// drop online-ddl error
	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(".*").WillReturnError(errors.New("invalid connection"))
	require.Error(t, checkpointAgent.Remove(context.Background()))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestIsFresh(t *testing.T) {
	source1 := "source1"
	jobCfg := &config.JobCfg{
		Name:       "test",
		MetaSchema: "meta",
		TaskMode:   dmconfig.ModeAll,
		Upstreams: []*config.UpstreamCfg{
			{
				MySQLInstance: dmconfig.MySQLInstance{
					SourceID: source1,
				},
				DBCfg: &dmconfig.DBConfig{},
			},
		},
	}
	taskCfg := jobCfg.ToTaskConfigs()[source1]
	checkpointAgent := NewAgentImpl(jobCfg)

	isFresh, err := checkpointAgent.IsFresh(context.Background(), lib.WorkerDMDump, &metadata.Task{Cfg: taskCfg})
	require.NoError(t, err)
	require.True(t, isFresh)

	loadTableName := "`meta`.`test_lightning_checkpoint_list`"
	syncTableName := "`meta`.`test_syncer_checkpoint`"
	query := fmt.Sprintf("SELECT status FROM %s WHERE `task_name` = ? AND `source_name` = ?", loadTableName)
	_, mock, err := conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs("test", source1).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("init"))
	isFresh, err = checkpointAgent.IsFresh(context.Background(), lib.WorkerDMLoad, &metadata.Task{Cfg: taskCfg})
	require.NoError(t, err)
	require.True(t, isFresh)

	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs("test", source1).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("running"))
	isFresh, err = checkpointAgent.IsFresh(context.Background(), lib.WorkerDMLoad, &metadata.Task{Cfg: taskCfg})
	require.NoError(t, err)
	require.False(t, isFresh)

	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs("test", source1).WillReturnError(sql.ErrNoRows)
	isFresh, err = checkpointAgent.IsFresh(context.Background(), lib.WorkerDMLoad, &metadata.Task{Cfg: taskCfg})
	require.NoError(t, err)
	require.True(t, isFresh)

	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs("test", source1).WillReturnError(errors.New("invalid connection"))
	isFresh, err = checkpointAgent.IsFresh(context.Background(), lib.WorkerDMLoad, &metadata.Task{Cfg: taskCfg})
	require.Error(t, err)
	require.False(t, isFresh)

	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs("test", source1).WillReturnError(errors.New("invalid connection"))
	isFresh, err = checkpointAgent.IsFresh(context.Background(), lib.WorkerDMLoad, &metadata.Task{Cfg: taskCfg})
	require.Error(t, err)
	require.False(t, isFresh)

	query = fmt.Sprintf("SELECT 1 FROM %s WHERE `id` = ? AND `is_global` = true", syncTableName)
	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(source1).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	isFresh, err = checkpointAgent.IsFresh(context.Background(), lib.WorkerDMSync, &metadata.Task{Cfg: taskCfg})
	require.NoError(t, err)
	require.False(t, isFresh)

	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(source1).WillReturnError(sql.ErrNoRows)
	isFresh, err = checkpointAgent.IsFresh(context.Background(), lib.WorkerDMSync, &metadata.Task{Cfg: taskCfg})
	require.NoError(t, err)
	require.True(t, isFresh)

	_, mock, err = conn.InitMockDBFull()
	require.NoError(t, err)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(source1).WillReturnError(errors.New("invalid connection"))
	isFresh, err = checkpointAgent.IsFresh(context.Background(), lib.WorkerDMSync, &metadata.Task{Cfg: taskCfg})
	require.Error(t, err)
	require.False(t, isFresh)
}
