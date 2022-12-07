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

package checker

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	gmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/parser/mysql"
	router "github.com/pingcap/tidb/util/table-router"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/stretchr/testify/require"
)

var (
	schema     = "db_1"
	tb1        = "t_1"
	tb2        = "t_2"
	metaSchema = "dm_meta"
	taskName   = "test"
)

func ignoreExcept(itemMap map[string]struct{}) []string {
	items := []string{
		config.DumpPrivilegeChecking,
		config.ReplicationPrivilegeChecking,
		config.VersionChecking,
		config.ServerIDChecking,
		config.BinlogEnableChecking,
		config.BinlogFormatChecking,
		config.BinlogRowImageChecking,
		config.TableSchemaChecking,
		config.ShardTableSchemaChecking,
		config.ShardAutoIncrementIDChecking,
		config.OnlineDDLChecking,
		config.BinlogDBChecking,
		config.TargetDBPrivilegeChecking,
		config.LightningFreeSpaceChecking,
		config.LightningDownstreamVersionChecking,
		config.LightningRegionDistributionChecking,
		config.LightningEmptyRegionChecking,
	}
	ignoreCheckingItems := make([]string, 0, len(items)-len(itemMap))
	for _, i := range items {
		if _, ok := itemMap[i]; !ok {
			ignoreCheckingItems = append(ignoreCheckingItems, i)
		}
	}
	return ignoreCheckingItems
}

func TestIgnoreAllCheckingItems(t *testing.T) {
	msg, err := CheckSyncConfig(context.Background(), nil, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.NoError(t, err)
	require.Len(t, msg, 0)

	result, err := RunCheckOnConfigs(context.Background(), nil, false)
	require.NoError(t, err)
	require.Nil(t, result)

	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: []string{config.AllChecking},
		},
	}
	msg, err = CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.NoError(t, err)
	require.Len(t, msg, 0)

	result, err = RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestDumpPrivilegeChecking(t *testing.T) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.DumpPrivilegeChecking: {}}),
		},
	}

	// test not enough privileges

	mock := initMockDB(t)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT USAGE ON *.* TO 'haha'@'%'"))
	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.Regexp(t, "(.|\n)*lack.*Select(.|\n)*", err.Error())
	require.Len(t, msg, 0)

	mock = initMockDB(t)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT USAGE ON *.* TO 'haha'@'%'"))
	result, err := RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Summary.Failed)
	require.Contains(t, result.Results[0].Errors[0].ShortErr, "lack of Select privilege")

	// test dumpWholeInstance

	mock = initMockDB(t)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT SELECT ON db.* TO 'haha'@'%'"))
	result, err = RunCheckOnConfigs(context.Background(), cfgs, true)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Summary.Failed)
	require.Contains(t, result.Results[0].Errors[0].ShortErr, "lack of Select global (*.*) privilege")

	// happy path

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
			AddRow("GRANT RELOAD,SELECT ON *.* TO 'haha'@'%'"))
	}, cfgs)
}

func TestReplicationPrivilegeChecking(t *testing.T) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.ReplicationPrivilegeChecking: {}}),
		},
	}

	// test not enough privileges

	mock := initMockDB(t)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT USAGE ON *.* TO 'haha'@'%'"))
	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.Regexp(t, "(.|\n)*lack.*REPLICATION SLAVE(.|\n)*", err.Error())
	require.Regexp(t, "(.|\n)*lack.*REPLICATION CLIENT(.|\n)*", err.Error())
	require.Len(t, msg, 0)

	mock = initMockDB(t)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT USAGE ON *.* TO 'haha'@'%'"))
	result, err := RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Summary.Failed)
	require.Contains(t, result.Results[0].Errors[0].ShortErr, "lack of REPLICATION SLAVE global (*.*) privilege")
	require.Contains(t, result.Results[0].Errors[0].ShortErr, "lack of REPLICATION CLIENT global (*.*) privilege")

	// happy path

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
			AddRow("GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO 'haha'@'%'"))
	}, cfgs)
}

func TestTargetDBPrivilegeChecking(t *testing.T) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.TargetDBPrivilegeChecking: {}}),
		},
	}

	// test not enough privileges

	mock := initMockDB(t)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT SELECT,UPDATE,CREATE,DELETE,INSERT,ALTER ON *.* TO 'test'@'%'"))
	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.NoError(t, err)
	require.Contains(t, msg, "lack of Drop global (*.*) privilege; lack of Index global (*.*) privilege; ")

	mock = initMockDB(t)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT SELECT,UPDATE,CREATE,DELETE,INSERT,ALTER ON *.* TO 'test'@'%'"))
	result, err := RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.Equal(t, int64(1), result.Summary.Warning)
	require.Contains(t, result.Results[0].Errors[0].ShortErr, "lack of Drop global (*.*) privilege")
	require.Contains(t, result.Results[0].Errors[0].ShortErr, "lack of Index global (*.*) privilege")

	// happy path

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
			AddRow("GRANT SELECT,UPDATE,CREATE,DELETE,INSERT,ALTER,INDEX,DROP ON *.* TO 'test'@'%'"))
	}, cfgs)

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
			AddRow("GRANT ALL PRIVILEGES ON *.* TO 'test'@'%'"))
	}, cfgs)
}

func TestVersionChecking(t *testing.T) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.VersionChecking: {}}),
		},
	}

	// happy path

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("version", "5.7.26-log"))
	}, cfgs)

	// MariaDB should have a warning

	mock := initMockDB(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "10.1.29-MariaDB"))
	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.NoError(t, err)
	require.Contains(t, msg, "Migrating from MariaDB is still experimental")

	mock = initMockDB(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "10.1.29-MariaDB"))
	result, err := RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.True(t, result.Summary.Passed)
	require.Equal(t, int64(1), result.Summary.Warning)
	require.Contains(t, result.Results[0].Errors[0].ShortErr, "Migrating from MariaDB is still experimental.")

	// too low MySQL version

	mock = initMockDB(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.5.26-log"))
	msg, err = CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.NoError(t, err)
	require.Regexp(t, "(.|\n)*version suggested at least .* but got 5.5.26(.|\n)*", msg)

	mock = initMockDB(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.5.26-log"))
	result, err = RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.True(t, result.Summary.Passed)
	require.Equal(t, int64(1), result.Summary.Warning)
	require.Regexp(t, "version suggested at least .* but got 5.5.26", result.Results[0].Errors[0].ShortErr)
}

func TestServerIDChecking(t *testing.T) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.ServerIDChecking: {}}),
		},
	}

	// not explicit server ID

	mock := initMockDB(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'server_id'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("server_id", "0"))
	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.NoError(t, err)
	require.Contains(t, msg, "Set server_id greater than 0")

	mock = initMockDB(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'server_id'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("server_id", "0"))
	result, err := RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.True(t, result.Summary.Passed)
	require.Contains(t, result.Results[0].Instruction, "Set server_id greater than 0")

	// happy path

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'server_id'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("server_id", "1"))
	}, cfgs)
}

func TestBinlogEnableChecking(t *testing.T) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.BinlogEnableChecking: {}}),
		},
	}

	// forget to turn on binlog

	mock := initMockDB(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'log_bin'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin", "OFF"))
	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.ErrorContains(t, err, "log_bin is OFF, and should be ON")
	require.Len(t, msg, 0)

	mock = initMockDB(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'log_bin'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin", "OFF"))
	result, err := RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.False(t, result.Summary.Passed)
	require.Contains(t, result.Results[0].Errors[0].ShortErr, "log_bin is OFF, and should be ON")

	// happy path

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'log_bin'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("log_bin", "ON"))
	}, cfgs)
}

func TestBinlogFormatChecking(t *testing.T) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.BinlogFormatChecking: {}}),
		},
	}

	// binlog_format is not correct

	mock := initMockDB(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_format'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("binlog_format", "STATEMENT"))
	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.ErrorContains(t, err, "binlog_format is STATEMENT, and should be ROW")
	require.Len(t, msg, 0)

	mock = initMockDB(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_format'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("binlog_format", "STATEMENT"))
	result, err := RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.Contains(t, result.Results[0].Errors[0].ShortErr, "binlog_format is STATEMENT, and should be ROW")

	// happy path

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_format'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("binlog_format", "ROW"))
	}, cfgs)
}

func TestBinlogRowImageChecking(t *testing.T) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.BinlogRowImageChecking: {}}),
		},
	}

	// binlog_row_image is not correct

	mock := initMockDB(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.26-log"))
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("binlog_row_image", "MINIMAL"))
	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.ErrorContains(t, err, "binlog_row_image is MINIMAL, and should be FULL")
	require.Len(t, msg, 0)

	mock = initMockDB(t)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.26-log"))
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("binlog_row_image", "MINIMAL"))
	result, err := RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.Contains(t, result.Results[0].Errors[0].ShortErr, "binlog_row_image is MINIMAL, and should be FULL")

	// happy path

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("version", "10.1.29-MariaDB"))
		mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("binlog_row_image", "FULL"))
	}, cfgs)
}

func TestTableSchemaChecking(t *testing.T) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.TableSchemaChecking: {}}),
		},
	}
	errNoSuchTable := &gmysql.MySQLError{Number: 1146, Message: "Table 'xxx' doesn't exist"}

	createTable1 := `CREATE TABLE %s (
				  id int(11) DEFAULT NULL,
				  b int(11) DEFAULT NULL
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`
	createTable2 := `CREATE TABLE %s (
				  id int(11) DEFAULT NULL,
				  b int(11) DEFAULT NULL,
				  UNIQUE KEY id (id)
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`

	// no PK/UK should raise error per table

	mock := initMockDB(t)
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "2"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE `db_1`.`t_1`").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE `db_1`.`t_1`").WillReturnError(errNoSuchTable) // downstream connection mock
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE `db_1`.`t_2`").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	mock.ExpectQuery("SHOW CREATE TABLE `db_1`.`t_2`").WillReturnError(errNoSuchTable) // downstream connection mock

	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.NoError(t, err)
	require.Contains(t, msg, "primary/unique key does not exist")
	require.NoError(t, mock.ExpectationsWereMet())

	mock = initMockDB(t)
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "2"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE `db_1`.`t_1`").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE `db_1`.`t_1`").WillReturnError(errNoSuchTable) // downstream connection mock
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE `db_1`.`t_2`").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	mock.ExpectQuery("SHOW CREATE TABLE `db_1`.`t_2`").WillReturnError(errNoSuchTable) // downstream connection mock

	result, err := RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.Contains(t, result.Results[0].Errors[0].ShortErr, "primary/unique key does not exist")
	require.Contains(t, result.Results[0].Errors[1].ShortErr, "primary/unique key does not exist")
	require.NoError(t, mock.ExpectationsWereMet())

	// happy path

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.MatchExpectationsInOrder(false)
		mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "2"))
		mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
		mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
		mock.ExpectQuery("SHOW CREATE TABLE `db_1`.`t_1`").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb1)))
		mock.ExpectQuery("SHOW CREATE TABLE `db_1`.`t_1`").WillReturnError(errNoSuchTable) // downstream connection mock
		mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
		mock.ExpectQuery("SHOW CREATE TABLE `db_1`.`t_2`").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb2)))
		mock.ExpectQuery("SHOW CREATE TABLE `db_1`.`t_2`").WillReturnError(errNoSuchTable) // downstream connection mock
	}, cfgs)
}

func TestShardTableSchemaChecking(t *testing.T) {
	cfgs := []*config.SubTaskConfig{
		{
			MetaSchema: metaSchema,
			Name:       taskName,
			ShardMode:  config.ShardPessimistic,
			RouteRules: []*router.TableRule{
				{
					SchemaPattern: schema,
					TargetSchema:  "db",
					TablePattern:  "t_*",
					TargetTable:   "t",
				},
			},
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.ShardTableSchemaChecking: {}}),
		},
	}

	createTable1 := `CREATE TABLE %s (
				  id int(11) DEFAULT NULL,
				  b int(11) DEFAULT NULL
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`
	createTable2 := `CREATE TABLE %s (
  					id int(11) DEFAULT NULL,
  					c int(11) DEFAULT NULL
					) ENGINE=InnoDB DEFAULT CHARSET=latin1`
	errNoSuchTable := &gmysql.MySQLError{Number: mysql.ErrNoSuchTable}
	createTableSQL := "SHOW CREATE TABLE `%s`.`%s`"

	// test different column definition

	mock := initMockDB(t)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, metaSchema, cputil.LoaderCheckpoint(taskName))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, metaSchema, cputil.LightningCheckpoint(taskName))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, metaSchema, cputil.SyncerCheckpoint(taskName))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "2"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb2)))
	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.ErrorContains(t, err, "different column definition")
	require.Len(t, msg, 0)

	mock = initMockDB(t)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, metaSchema, cputil.LoaderCheckpoint(taskName))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, metaSchema, cputil.LightningCheckpoint(taskName))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, metaSchema, cputil.SyncerCheckpoint(taskName))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "2"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb2)))
	result, err := RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.Contains(t, result.Results[0].Errors[0].ShortErr, "different column definition")

	// test success check

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.ExpectQuery(fmt.Sprintf(createTableSQL, metaSchema, cputil.LoaderCheckpoint(taskName))).WillReturnError(errNoSuchTable)
		mock.ExpectQuery(fmt.Sprintf(createTableSQL, metaSchema, cputil.LightningCheckpoint(taskName))).WillReturnError(errNoSuchTable)
		mock.ExpectQuery(fmt.Sprintf(createTableSQL, metaSchema, cputil.SyncerCheckpoint(taskName))).WillReturnError(errNoSuchTable)
		mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
		mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
		mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "2"))
		mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
		mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
		mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
		mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	}, cfgs)

	// test existing checkpoint

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.ExpectQuery(fmt.Sprintf(createTableSQL, metaSchema, cputil.LoaderCheckpoint(taskName))).WillReturnRows(sqlmock.
			NewRows([]string{"Table", "Create Table"}).AddRow(cputil.LoaderCheckpoint(taskName), ""))
		mock.ExpectQuery(fmt.Sprintf(createTableSQL, metaSchema, cputil.LightningCheckpoint(taskName))).WillReturnRows(sqlmock.
			NewRows([]string{"Table", "Create Table"}).AddRow(cputil.LightningCheckpoint(taskName), ""))
		mock.ExpectQuery(fmt.Sprintf(createTableSQL, metaSchema, cputil.SyncerCheckpoint(taskName))).WillReturnRows(sqlmock.
			NewRows([]string{"Table", "Create Table"}).AddRow(cputil.SyncerCheckpoint(taskName), ""))
	}, cfgs)
}

func TestShardAutoIncrementIDChecking(t *testing.T) {
	cfgs := []*config.SubTaskConfig{
		{
			ShardMode: config.ShardPessimistic,
			RouteRules: []*router.TableRule{
				{
					SchemaPattern: schema,
					TargetSchema:  "db",
					TablePattern:  "t_*",
					TargetTable:   "t",
				},
			},
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.ShardTableSchemaChecking: {}, config.ShardAutoIncrementIDChecking: {}}),
		},
	}

	createTable1 := `CREATE TABLE %s (
				  id int(11) NOT NULL AUTO_INCREMENT,
				  b int(11) DEFAULT NULL,
				PRIMARY KEY (id),
				UNIQUE KEY u_b(b)
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`

	createTable2 := `CREATE TABLE %s (
				  id int(11) NOT NULL,
				  b int(11) DEFAULT NULL,
				INDEX (id),
				UNIQUE KEY u_b(b)
				) ENGINE=InnoDB DEFAULT CHARSET=latin1`

	errNoSuchTable := &gmysql.MySQLError{Number: mysql.ErrNoSuchTable}
	createTableSQL := "SHOW CREATE TABLE `%s`.`%s`"
	mock := initMockDB(t)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LoaderCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LightningCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.SyncerCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "2"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.NoError(t, err)
	require.Regexp(t, "(.|\n)*sourceID  table .* of sharding .* have auto-increment key(.|\n)*", msg)

	mock = initMockDB(t)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LoaderCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LightningCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.SyncerCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "2"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	result, err := RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.Regexp(t, "sourceID  table .* of sharding .* have auto-increment key", result.Results[0].Errors[0].ShortErr)

	// happy path

	checkHappyPath(t, func() {
		mock := initMockDB(t)
		mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LoaderCheckpoint(""))).WillReturnError(errNoSuchTable)
		mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LightningCheckpoint(""))).WillReturnError(errNoSuchTable)
		mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.SyncerCheckpoint(""))).WillReturnError(errNoSuchTable)
		mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
		mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb1)))
		mock.ExpectQuery("SHOW VARIABLES LIKE 'max_connections'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("max_connections", "2"))
		mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
		mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb1)))
		mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
		mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb2)))
	}, cfgs)
}

func TestSameTargetTableDetection(t *testing.T) {
	cfgs := []*config.SubTaskConfig{
		{
			RouteRules: []*router.TableRule{
				{
					SchemaPattern: schema,
					TargetSchema:  "db",
					TablePattern:  tb1,
					TargetTable:   "t",
				}, {
					SchemaPattern: schema,
					TargetSchema:  "db",
					TablePattern:  tb2,
					TargetTable:   "T",
				},
			},
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.TableSchemaChecking: {}}),
		},
	}

	createTable1 := `CREATE TABLE %s (
				  	id int(11) NOT NULL AUTO_INCREMENT,
  					b int(11) DEFAULT NULL,
					PRIMARY KEY (id),
					UNIQUE KEY u_b(b)
					) ENGINE=InnoDB DEFAULT CHARSET=latin1`
	errNoSuchTable := &gmysql.MySQLError{Number: mysql.ErrNoSuchTable}
	createTableSQL := "SHOW CREATE TABLE `%s`.`%s`"

	mock := initMockDB(t)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LoaderCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LightningCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.SyncerCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LoaderCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LightningCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.SyncerCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.ErrorContains(t, err, "same table name in case-insensitive")
	require.Len(t, msg, 0)

	mock = initMockDB(t)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LoaderCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LightningCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.SyncerCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LoaderCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.LightningCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery(fmt.Sprintf(createTableSQL, "", cputil.SyncerCheckpoint(""))).WillReturnError(errNoSuchTable)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	_, err = RunCheckOnConfigs(context.Background(), cfgs, false)
	require.ErrorContains(t, err, "same table name in case-insensitive")
}

func initMockDB(t *testing.T) sqlmock.Sqlmock {
	t.Helper()

	mock, err := conn.MockDefaultDBProvider()
	require.NoError(t, err)
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	return mock
}

func checkHappyPath(t *testing.T, pre func(), cfgs []*config.SubTaskConfig) {
	t.Helper()

	pre()
	msg, err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	require.NoError(t, err)
	require.Equal(t, CheckTaskSuccess, msg)

	pre()
	result, err := RunCheckOnConfigs(context.Background(), cfgs, false)
	require.NoError(t, err)
	require.True(t, result.Summary.Passed)
	require.Equal(t, int64(0), result.Summary.Warning)
}
