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
	"testing"

	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/syncer/binlogstream"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/stretchr/testify/require"
)

func TestHandleError(t *testing.T) {
	t.Parallel()

	var (
		cfg    = genDefaultSubTaskConfig4Test()
		syncer = NewSyncer(cfg, nil, nil)
		task   = "test"
		ctx    = context.Background()
		cases  = []struct {
			req    *pb.HandleWorkerErrorRequest
			errMsg string
		}{
			{
				req:    &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace, Task: task, BinlogPos: "", Sqls: []string{""}},
				errMsg: fmt.Sprintf("source '%s' has no error", syncer.cfg.SourceID),
			},
			{
				req:    &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace, Task: task, BinlogPos: "wrong_binlog_pos", Sqls: []string{""}},
				errMsg: ".*invalid --binlog-pos .* in handle-error operation.*",
			},
			{
				req:    &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace, Task: task, BinlogPos: "mysql-bin.000001:2345", Sqls: []string{"wrong_sql"}},
				errMsg: ".* sql wrong_sql: .*",
			},
			{
				req:    &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace, Task: task, BinlogPos: "mysql-bin.000001:2345", Sqls: []string{"alter table tb add column a int;"}},
				errMsg: ".*without schema name not valid.*",
			},
			{
				req:    &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace, Task: task, BinlogPos: "mysql-bin.000001:2345", Sqls: []string{"insert into db.tb values(1,2);"}},
				errMsg: ".*only support replace or inject with DDL currently.*",
			},
			{
				req:    &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Replace, Task: task, BinlogPos: "mysql-bin.000001:2345", Sqls: []string{"alter table db.tb add column a int;"}},
				errMsg: "",
			},
			{
				req:    &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Inject, Task: task, BinlogPos: "", Sqls: []string{""}},
				errMsg: fmt.Sprintf("source '%s' has no error", syncer.cfg.SourceID),
			},
			{
				req:    &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Inject, Task: task, BinlogPos: "wrong_binlog_pos", Sqls: []string{""}},
				errMsg: ".*invalid --binlog-pos .* in handle-error operation.*",
			},
			{
				req:    &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Inject, Task: task, BinlogPos: "mysql-bin.000001:2345", Sqls: []string{"wrong_sql"}},
				errMsg: ".* sql wrong_sql: .*",
			},
			{
				req:    &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Inject, Task: task, BinlogPos: "mysql-bin.000001:2345", Sqls: []string{"alter table db.tb add column a int;"}},
				errMsg: "",
			},
			{
				req:    &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_Skip, Task: task, BinlogPos: "mysql-bin.000001:2345", Sqls: []string{}},
				errMsg: "",
			},
			{
				req:    &pb.HandleWorkerErrorRequest{Op: pb.ErrorOp_List, Task: task, BinlogPos: "mysql-bin.000001:2344", Sqls: []string{}},
				errMsg: "",
			},
		}
	)
	mockDB, err := conn.MockDefaultDBProvider()
	require.NoError(t, err)
	syncer.fromDB, err = dbconn.NewUpStreamConn(&cfg.From) // used to get parser
	require.NoError(t, err)
	syncer.streamerController = binlogstream.NewStreamerController4Test(nil, nil)

	for _, cs := range cases {
		commandsJSON, err := syncer.HandleError(ctx, cs.req)
		if cs.req.Op == pb.ErrorOp_List {
			require.Equal(t, "[{\"op\":1,\"task\":\"test\",\"binlogPos\":\"(mysql-bin.000001, 2345)\"}]", commandsJSON)
		}
		if len(cs.errMsg) == 0 {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.Regexp(t, cs.errMsg, err.Error())
		}
	}
	require.NoError(t, mockDB.ExpectationsWereMet())
}
