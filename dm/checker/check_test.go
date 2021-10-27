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
	router "github.com/pingcap/tidb-tools/pkg/table-router"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/pkg/conn"

	tc "github.com/pingcap/check"
)

func TestChecker(t *testing.T) {
	tc.TestingT(t)
}

type testCheckerSuite struct{}

var _ = tc.Suite(&testCheckerSuite{})

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
	}
	ignoreCheckingItems := make([]string, 0, len(items)-len(itemMap))
	for _, i := range items {
		if _, ok := itemMap[i]; !ok {
			ignoreCheckingItems = append(ignoreCheckingItems, i)
		}
	}
	return ignoreCheckingItems
}

func (s *testCheckerSuite) TestIgnoreAllCheckingItems(c *tc.C) {
	c.Assert(CheckSyncConfig(context.Background(), nil, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)

	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: []string{config.AllChecking},
		},
	}
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)
}

// nolint:dupl
func (s *testCheckerSuite) TestDumpPrivilegeChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.DumpPrivilegeChecking: {}}),
		},
	}
	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT USAGE ON *.* TO 'haha'@'%'"))
	err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	c.Assert(err, tc.ErrorMatches, "(.|\n)*lack.*RELOAD(.|\n)*")
	c.Assert(err, tc.ErrorMatches, "(.|\n)*lack.*Select(.|\n)*")

	mock = conn.InitMockDB(c)

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT RELOAD,SELECT ON *.* TO 'haha'@'%'"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)
}

// nolint:dupl
func (s *testCheckerSuite) TestReplicationPrivilegeChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.ReplicationPrivilegeChecking: {}}),
		},
	}
	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT USAGE ON *.* TO 'haha'@'%'"))
	err := CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt)
	c.Assert(err, tc.ErrorMatches, "(.|\n)*lack.*REPLICATION SLAVE(.|\n)*")
	c.Assert(err, tc.ErrorMatches, "(.|\n)*lack.*REPLICATION CLIENT(.|\n)*")

	mock = conn.InitMockDB(c)

	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for User"}).
		AddRow("GRANT REPLICATION SLAVE,REPLICATION CLIENT ON *.* TO 'haha'@'%'"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)
}

func (s *testCheckerSuite) TestVersionChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.VersionChecking: {}}),
		},
	}

	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.26-log"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)

	mock = conn.InitMockDB(c)

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "10.1.29-MariaDB"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)

	mock = conn.InitMockDB(c)

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.5.26-log"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.ErrorMatches, "(.|\n)*version required at least .* but got 5.5.26(.|\n)*")

	mock = conn.InitMockDB(c)

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "10.0.0-MariaDB"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.ErrorMatches, "(.|\n)*version required at least .* but got 10.0.0(.|\n)*")
}

func (s *testCheckerSuite) TestServerIDChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.ServerIDChecking: {}}),
		},
	}

	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'server_id'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("server_id", "0"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.ErrorMatches, "(.|\n)*please set server_id greater than 0(.|\n)*")

	mock = conn.InitMockDB(c)

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'server_id'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("server_id", "1"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)
}

func (s *testCheckerSuite) TestBinlogEnableChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.BinlogEnableChecking: {}}),
		},
	}

	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'log_bin'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin", "OFF"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.ErrorMatches, "(.|\n)*log_bin is OFF, and should be ON(.|\n)*")

	mock = conn.InitMockDB(c)

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'log_bin'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("log_bin", "ON"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)
}

func (s *testCheckerSuite) TestBinlogFormatChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.BinlogFormatChecking: {}}),
		},
	}

	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_format'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("binlog_format", "STATEMENT"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.ErrorMatches, "(.|\n)*binlog_format is STATEMENT, and should be ROW(.|\n)*")

	mock = conn.InitMockDB(c)

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_format'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("binlog_format", "ROW"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)
}

func (s *testCheckerSuite) TestBinlogRowImageChecking(c *tc.C) {
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.BinlogRowImageChecking: {}}),
		},
	}

	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.26-log"))
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("binlog_row_image", "MINIMAL"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.ErrorMatches, "(.|\n)*binlog_row_image is MINIMAL, and should be FULL(.|\n)*")

	mock = conn.InitMockDB(c)

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "10.1.29-MariaDB"))
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("binlog_row_image", "FULL"))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)
}

func (s *testCheckerSuite) TestTableSchemaChecking(c *tc.C) {
	var (
		schema = "db_1"
		tb1    = "t_1"
		tb2    = "t_2"
	)
	cfgs := []*config.SubTaskConfig{
		{
			IgnoreCheckingItems: ignoreExcept(map[string]struct{}{config.TableSchemaChecking: {}}),
		},
	}

	createTable1 := `CREATE TABLE %s (
  					id int(11) DEFAULT NULL,
  					b int(11) DEFAULT NULL
					) ENGINE=InnoDB DEFAULT CHARSET=latin1`
	createTable2 := `CREATE TABLE %s (
  					id int(11) DEFAULT NULL,
  					b int(11) DEFAULT NULL,
  					UNIQUE KEY id (id)
					) ENGINE=InnoDB DEFAULT CHARSET=latin1`

	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.ErrorMatches, "(.|\n)*primary/unique key does not exist(.|\n)*")

	mock = conn.InitMockDB(c)

	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb1)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb2)))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)
}

func (s *testCheckerSuite) TestShardTableSchemaChecking(c *tc.C) {
	var (
		schema = "db_1"
		tb1    = "t_1"
		tb2    = "t_2"
	)
	cfgs := []*config.SubTaskConfig{
		{
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

	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb2)))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.ErrorMatches, "(.|\n)*different column definition(.|\n)*")

	mock = conn.InitMockDB(c)

	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)
}

func (s *testCheckerSuite) TestShardAutoIncrementIDChecking(c *tc.C) {
	var (
		schema = "db_1"
		tb1    = "t_1"
		tb2    = "t_2"
	)
	cfgs := []*config.SubTaskConfig{
		{
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

	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.ErrorMatches, "(.|\n)*instance  table .* of sharding .* have auto-increment key(.|\n)*")

	mock = conn.InitMockDB(c)

	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable2, tb2)))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.IsNil)
}

func (s *testCheckerSuite) TestSameTargetTableDetection(c *tc.C) {
	var (
		schema = "db_1"
		tb1    = "t_1"
		tb2    = "t_2"
	)
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

	mock := conn.InitMockDB(c)
	mock.ExpectQuery("SHOW DATABASES").WillReturnRows(sqlmock.NewRows([]string{"DATABASE"}).AddRow(schema))
	mock.ExpectQuery("SHOW FULL TABLES").WillReturnRows(sqlmock.NewRows([]string{"Tables_in_" + schema, "Table_type"}).AddRow(tb1, "BASE TABLE").AddRow(tb2, "BASE TABLE"))
	mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb1)))
	mock.ExpectQuery("SHOW CREATE TABLE .*").WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow(tb1, fmt.Sprintf(createTable1, tb2)))
	c.Assert(CheckSyncConfig(context.Background(), cfgs, common.DefaultErrorCnt, common.DefaultWarnCnt), tc.ErrorMatches, "(.|\n)*same table name in case-insensitive(.|\n)*")
}
