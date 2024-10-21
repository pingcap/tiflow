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

package syncer

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/check"
	"github.com/pingcap/tidb/pkg/util/filter"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/stretchr/testify/require"
)

var _ = check.Suite(&testJobSuite{})

type testJobSuite struct{}

func (t *testJobSuite) TestJobTypeString(c *check.C) {
	testCases := []struct {
		tp  opType
		str string
	}{
		{
			null,
			"",
		}, {
			dml,
			"dml",
		}, {
			ddl,
			"ddl",
		}, {
			xid,
			"xid",
		}, {
			flush,
			"flush",
		}, {
			skip,
			"skip",
		}, {
			rotate,
			"rotate",
		},
	}

	for _, testCase := range testCases {
		tpStr := testCase.tp.String()
		c.Assert(tpStr, check.Equals, testCase.str)
	}
}

func TestJob(t *testing.T) {
	t.Parallel()

	ddlInfo := &ddlInfo{
		sourceTables: []*filter.Table{{Schema: "test1", Name: "t1"}},
		targetTables: []*filter.Table{{Schema: "test2", Name: "t2"}},
	}
	table := &cdcmodel.TableName{Schema: "test", Table: "t1"}
	location := binlog.MustZeroLocation(mysql.MySQLFlavor)
	ec := &eventContext{startLocation: location, endLocation: location, lastLocation: location, safeMode: true}
	qec := &queryEventContext{
		eventContext:    ec,
		originSQL:       "create database test",
		needHandleDDLs:  []string{"create database test"},
		shardingDDLInfo: ddlInfo,
	}

	schema := "create table test.tb(id int primary key, col1 int unique not null)"
	ti := mockTableInfo(t, schema)

	exampleChange := sqlmodel.NewRowChange(table, nil, nil, []interface{}{2, 2}, ti, nil, nil)

	testCases := []struct {
		job    *job
		jobStr string
	}{
		{
			newDMLJob(exampleChange, ec),
			"tp: dml, flushSeq: 0, dml: [type: ChangeInsert, source table: test.t1, target table: test.t1, preValues: [], postValues: [2 2]], safemode: true, ddls: [], last_location: position: (, 4), gtid-set: , start_location: position: (, 4), gtid-set: , current_location: position: (, 4), gtid-set: ",
		}, {
			newDDLJob(qec),
			"tp: ddl, flushSeq: 0, dml: [], safemode: false, ddls: [create database test], last_location: position: (, 4), gtid-set: , start_location: position: (, 4), gtid-set: , current_location: position: (, 4), gtid-set: ",
		}, {
			newXIDJob(binlog.MustZeroLocation(mysql.MySQLFlavor), binlog.MustZeroLocation(mysql.MySQLFlavor), binlog.MustZeroLocation(mysql.MySQLFlavor)),
			"tp: xid, flushSeq: 0, dml: [], safemode: false, ddls: [], last_location: position: (, 4), gtid-set: , start_location: position: (, 4), gtid-set: , current_location: position: (, 4), gtid-set: ",
		}, {
			newFlushJob(16, 1),
			"tp: flush, flushSeq: 1, dml: [], safemode: false, ddls: [], last_location: position: (, 0), gtid-set: , start_location: position: (, 0), gtid-set: , current_location: position: (, 0), gtid-set: ",
		}, {
			newSkipJob(ec),
			"tp: skip, flushSeq: 0, dml: [], safemode: false, ddls: [], last_location: position: (, 4), gtid-set: , start_location: position: (, 0), gtid-set: , current_location: position: (, 0), gtid-set: ",
		},
	}

	for _, testCase := range testCases {
		require.Equal(t, testCase.jobStr, testCase.job.String())
	}
}

func (t *testJobSuite) TestQueueBucketName(c *check.C) {
	name := queueBucketName(0)
	c.Assert(name, check.Equals, "q_0")

	name = queueBucketName(8)
	c.Assert(name, check.Equals, "q_0")

	name = queueBucketName(9)
	c.Assert(name, check.Equals, "q_1")
}
