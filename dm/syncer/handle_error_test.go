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

package syncer

import (
	"context"
	"fmt"

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/conn"
	"github.com/pingcap/dm/syncer/dbconn"
)

func (s *testSyncerSuite) TestHandleError(c *C) {
	var (
		syncer = NewSyncer(s.cfg, nil)
		task   = "test"
		ctx    = context.Background()
		cases  = []struct {
			req    pb.HandleWorkerErrorRequest
			errMsg string
		}{
			{
				req:    pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace, Task: task, BinlogPos: "", Sqls: []string{""}},
				errMsg: fmt.Sprintf("source '%s' has no error", syncer.cfg.SourceID),
			},
			{
				req:    pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace, Task: task, BinlogPos: "wrong_binlog_pos", Sqls: []string{""}},
				errMsg: ".*invalid --binlog-pos .* in handle-error operation.*",
			},
			{
				req:    pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace, Task: task, BinlogPos: "mysql-bin.000001:2345", Sqls: []string{"wrong_sql"}},
				errMsg: ".* sql wrong_sql: .*",
			},
			{
				req:    pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace, Task: task, BinlogPos: "mysql-bin.000001:2345", Sqls: []string{"alter table tb add column a int;"}},
				errMsg: ".*without schema name not valid.*",
			},
			{
				req:    pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace, Task: task, BinlogPos: "mysql-bin.000001:2345", Sqls: []string{"insert into db.tb values(1,2);"}},
				errMsg: ".*only support replace with DDL currently.*",
			},
			{
				req:    pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace, Task: task, BinlogPos: "mysql-bin.000001:2345", Sqls: []string{"alter table db.tb add column a int;"}},
				errMsg: "",
			},
			{
				req:    pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Skip, Task: task, BinlogPos: "mysql-bin.000001:2345", Sqls: []string{}},
				errMsg: "",
			},
		}
	)
	mockDB := conn.InitMockDB(c)
	var err error
	syncer.fromDB, err = dbconn.NewUpStreamConn(s.cfg.From) // used to get parser
	c.Assert(err, IsNil)

	for _, cs := range cases {
		err := syncer.HandleError(ctx, &cs.req)
		if len(cs.errMsg) == 0 {
			c.Assert(err, IsNil)
		} else {
			c.Assert(err, NotNil)
			c.Assert(err.Error(), Matches, cs.errMsg)
		}
	}
	c.Assert(mockDB.ExpectationsWereMet(), IsNil)
}
