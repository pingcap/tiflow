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

package unit

import (
	"context"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

var _ = check.Suite(&testUnitSuite{})

type testUnitSuite struct{}

func (t *testUnitSuite) TestIsCtxCanceledProcessErr(c *check.C) {
	err := NewProcessError(context.Canceled)
	c.Assert(IsCtxCanceledProcessErr(err), check.IsTrue)

	err = NewProcessError(errors.New("123"))
	c.Assert(IsCtxCanceledProcessErr(err), check.IsFalse)

	terr := terror.ErrDBBadConn
	err = NewProcessError(terror.ErrDBBadConn)
	c.Assert(err.GetErrCode(), check.Equals, int32(terr.Code()))
	c.Assert(err.GetErrClass(), check.Equals, terr.Class().String())
	c.Assert(err.GetErrLevel(), check.Equals, terr.Level().String())
	c.Assert(err.GetMessage(), check.Equals, terr.Message())
	c.Assert(err.GetRawCause(), check.Equals, "")
}

func (t *testUnitSuite) TestJoinProcessErrors(c *check.C) {
	errs := []*pb.ProcessError{
		NewProcessError(terror.ErrDBDriverError.Generate()),
		NewProcessError(terror.ErrSyncUnitDMLStatementFound.Generate()),
	}

	c.Assert(JoinProcessErrors(errs), check.Equals,
		`ErrCode:10001 ErrClass:"database" ErrScope:"not-set" ErrLevel:"high" Message:"database driver error" Workaround:"Please check the database connection and the database config in configuration file." , ErrCode:36014 ErrClass:"sync-unit" ErrScope:"internal" ErrLevel:"high" Message:"only support ROW format binlog, unexpected DML statement found in query event" `)
}

func TestIsResumableError(t *testing.T) {
	testCases := []struct {
		err       error
		resumable bool
	}{
		// only DM new error is checked
		{&tmysql.SQLError{Code: 1105, Message: "unsupported modify column length 20 is less than origin 40", State: tmysql.DefaultMySQLState}, true},
		{&tmysql.SQLError{Code: 1105, Message: "unsupported drop integer primary key", State: tmysql.DefaultMySQLState}, true},
		{&tmysql.SQLError{Code: 1072, Message: "column c id 3 does not exist, this column may have been updated by other DDL ran in parallel", State: tmysql.DefaultMySQLState}, true},
		{terror.ErrDBExecuteFailed.Generate("file test.t3.sql: execute statement failed: USE `test_abc`;: context canceled"), true},
		{terror.ErrDBExecuteFailed.Delegate(&tmysql.SQLError{Code: 1105, Message: "unsupported modify column length 20 is less than origin 40", State: tmysql.DefaultMySQLState}, "alter table t modify col varchar(20)"), false},
		{terror.ErrDBExecuteFailed.Delegate(&tmysql.SQLError{Code: 1105, Message: "unsupported drop integer primary key", State: tmysql.DefaultMySQLState}, "alter table t drop column id"), false},
		{terror.ErrDBExecuteFailed.Delegate(&tmysql.SQLError{Code: 1067, Message: "Invalid default value for 'ct'", State: tmysql.DefaultMySQLState}, "CREATE TABLE `tbl` (`c1` int(11) NOT NULL,`ct` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '创建时间',PRIMARY KEY (`c1`)) ENGINE=InnoDB DEFAULT CHARSET=latin1"), false},
		{terror.ErrDBExecuteFailed.Delegate(errors.New("Error 1062: Duplicate entry '5' for key 'PRIMARY'")), false},
		{terror.ErrDBExecuteFailed.Delegate(errors.New("INSERT INTO `db`.`tbl` (`c1`,`c2`) VALUES (?,?);: Error 1406: Data too long for column 'c2' at row 1")), false},
		// real error is generated by `Delegate` and multiple `Annotatef`, we use `New` to simplify it
		{terror.ErrParserParseRelayLog.New("parse relay log file bin.000018 from offset 555 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file bin.000018 from offset 0 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004/bin.000018: binlog checksum mismatch, data may be corrupted"), false},
		{terror.ErrParserParseRelayLog.New("parse relay log file bin.000018 from offset 500 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file bin.000018 from offset 0 in dir /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004: parse relay log file /home/tidb/deploy/relay_log/d2e831df-b4ec-11e9-9237-0242ac110008.000004/bin.000018: get event err EOF, need 1567488104 but got 316323"), false},
		{terror.ErrSyncUnitDDLWrongSequence.Generate("wrong sequence", "right sequence"), false},
		{terror.ErrSyncerShardDDLConflict.Generate("conflict DDL", "conflict"), true},
		// others
		{nil, true},
		{errors.New("unknown error"), true},
		{terror.ErrNotSet.Delegate(&tmysql.SQLError{Code: 1236, Message: "Could not find first log file name in binary log index file", State: tmysql.DefaultMySQLState}), false},
		{terror.ErrNotSet.Delegate(&tmysql.SQLError{Code: 1236, Message: "The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, but the master has purged binary logs containing GTIDs that the slave requires", State: tmysql.DefaultMySQLState}), false},
		{terror.ErrLoadLightningRuntime.Delegate(common.ErrDBConnect), false},
	}

	for _, tc := range testCases {
		err := NewProcessError(tc.err)
		require.Equal(t, tc.resumable, IsResumableError(err))
	}
}
