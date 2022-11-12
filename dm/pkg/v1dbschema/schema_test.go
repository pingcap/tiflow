// Copyright 2020 PingCAP, Inc.
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

package v1dbschema

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
)

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testSchema struct {
	host     string
	port     int
	user     string
	password string
	db       *conn.BaseDB
	mockDB   sqlmock.Sqlmock
}

var _ = Suite(&testSchema{})

func (t *testSchema) SetUpSuite(c *C) {
	t.setUpDBConn(c)
}

func (t *testSchema) TestTearDown(c *C) {
	t.db.Close()
}

func (t *testSchema) setUpDBConn(c *C) {
	t.host = os.Getenv("MYSQL_HOST")
	if t.host == "" {
		t.host = "127.0.0.1"
	}
	t.port, _ = strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if t.port == 0 {
		t.port = 3306
	}
	t.user = os.Getenv("MYSQL_USER")
	if t.user == "" {
		t.user = "root"
	}
	t.password = os.Getenv("MYSQL_PSWD")

	cfg := &config.DBConfig{
		Host:     t.host,
		Port:     t.port,
		User:     t.user,
		Password: t.password,
		Session:  map[string]string{"sql_log_bin": "off"}, // do not enable binlog to break other unit test cases.
	}
	cfg.Adjust()

	var err error
	t.mockDB = conn.InitMockDB(c)
	t.db, err = conn.DefaultDBProvider.Apply(cfg)
	c.Assert(err, IsNil)
}

func (t *testSchema) TestSchemaV106ToV20x(c *C) {
	var (
		tctx = tcontext.Background()
		cfg  = &config.SubTaskConfig{
			Name:       "test",
			SourceID:   "mysql-replica-01",
			ServerID:   429523137,
			MetaSchema: "dm_meta_v106_test",
			From: config.DBConfig{
				Host:     t.host,
				Port:     t.port,
				User:     t.user,
				Password: t.password,
			},
		}
		endGS, _ = gtid.ParserGTID(gmysql.MySQLFlavor, "ccb992ad-a557-11ea-ba6a-0242ac140002:1-16")
	)

	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/v1dbschema/MockGetGTIDsForPos", `return("ccb992ad-a557-11ea-ba6a-0242ac140002:10-16")`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/v1dbschema/MockGetGTIDsForPos")
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/dm/pkg/utils/GetGTIDPurged", `return("ccb992ad-a557-11ea-ba6a-0242ac140002:1-9")`), IsNil)
	//nolint:errcheck
	defer failpoint.Disable("github.com/pingcap/tiflow/dm/pkg/utils/GetGTIDPurged")

	// update schema without GTID enabled.
	// mock updateSyncerCheckpoint
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec("ALTER TABLE `dm_meta_v106_test`.`test_syncer_checkpoint` ADD COLUMN binlog_gtid TEXT AFTER binlog_pos").WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectExec("ALTER TABLE `dm_meta_v106_test`.`test_syncer_checkpoint` ADD COLUMN table_info JSON NOT NULL AFTER binlog_gtid").WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	// mock updateSyncerOnlineDDLMeta
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec("UPDATE `dm_meta_v106_test`.`test_onlineddl`.*").WithArgs(cfg.SourceID, fmt.Sprint(cfg.ServerID)).WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	c.Assert(UpdateSchema(tctx, t.db, cfg), IsNil)
	c.Assert(t.mockDB.ExpectationsWereMet(), IsNil)

	// update schema with GTID enabled.
	cfg.EnableGTID = true
	// reset mockDB conn because last UpdateSchema would close the conn.
	t.setUpDBConn(c)
	// mock updateSyncerCheckpoint
	t.mockDB.ExpectQuery("SELECT binlog_name, binlog_pos FROM `dm_meta_v106_test`.`test_syncer_checkpoint`.*").
		WithArgs(cfg.SourceID, true).WillReturnRows(sqlmock.NewRows([]string{"binlog_name", "binlog_pos"}).
		AddRow("mysql-bin.000001", "0"))
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec("ALTER TABLE `dm_meta_v106_test`.`test_syncer_checkpoint` ADD COLUMN binlog_gtid TEXT AFTER binlog_pos").WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectExec("ALTER TABLE `dm_meta_v106_test`.`test_syncer_checkpoint` ADD COLUMN table_info JSON NOT NULL AFTER binlog_gtid").WithArgs().WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec("UPDATE `dm_meta_v106_test`.`test_syncer_checkpoint` SET binlog_gtid.*").
		WithArgs(endGS.String(), cfg.SourceID, true).WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	// mock updateSyncerOnlineDDLMeta
	t.mockDB.ExpectBegin()
	t.mockDB.ExpectExec("UPDATE `dm_meta_v106_test`.`test_onlineddl`.*").WithArgs(cfg.SourceID, fmt.Sprint(cfg.ServerID)).WillReturnResult(sqlmock.NewErrorResult(nil))
	t.mockDB.ExpectCommit()
	c.Assert(UpdateSchema(tctx, t.db, cfg), IsNil)
	c.Assert(t.mockDB.ExpectationsWereMet(), IsNil)
}
