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

package e2e_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/tests/integration_tests/util"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/client"
	"github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/jobmaster/dm"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	engineModel "github.com/pingcap/tiflow/engine/model"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
)

func TestDMJob(t *testing.T) {
	ctx := context.Background()
	masterClient, err := client.NewMasterClient(ctx, []string{"127.0.0.1:10245"})
	require.NoError(t, err)

	mysqlCfg := util.DBConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "123456",
	}
	tidbCfg := util.DBConfig{
		Host:     "127.0.0.1",
		Port:     4000,
		User:     "root",
		Password: "",
	}

	mysql, err := util.CreateDB(mysqlCfg)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, util.CloseDB(mysql))
	}()
	tidb, err := util.CreateDB(tidbCfg)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, util.CloseDB(tidb))
	}()

	// clean up
	_, err = tidb.Exec("drop database if exists dm_meta")
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		testSimpleAllModeTask(t, masterClient, mysql, tidb, "test1")
	}()
	go func() {
		defer wg.Done()
		testSimpleAllModeTask(t, masterClient, mysql, tidb, "test2")
	}()
	wg.Wait()

	// test metrics for syncer
	jobIDs := map[string]struct{}{}
	metricsURLs := []string{
		"http://127.0.0.1:11241/metrics",
		"http://127.0.0.1:11242/metrics",
		"http://127.0.0.1:11243/metrics",
	}
	re := regexp.MustCompile(`job_id="(.{36})"`)
	for _, metricsURL := range metricsURLs {
		resp, err := http.Get(metricsURL)
		require.NoError(t, err)
		content, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		matched := re.FindAllSubmatch(content, -1)
		for _, m := range matched {
			jobIDs[string(m[1])] = struct{}{}
		}
	}

	require.Equal(t, 2, len(jobIDs))
}

// testSimpleAllModeTask extracts the common logic for a DM "all" mode task,
// `db` should not contain special character.
func testSimpleAllModeTask(
	t *testing.T,
	client client.MasterClient,
	mysql, tidb *sql.DB,
	db string,
) {
	ctx := context.Background()
	noError := func(_ interface{}, err error) {
		require.NoError(t, err)
	}

	noError(tidb.Exec("drop database if exists " + db))
	noError(mysql.Exec("drop database if exists " + db))

	// full phase
	noError(mysql.Exec("create database " + db))
	noError(mysql.Exec("create table " + db + ".t1(c int primary key)"))
	noError(mysql.Exec("insert into " + db + ".t1 values(1)"))

	dmJobCfg, err := ioutil.ReadFile("./dm-job.yaml")
	require.NoError(t, err)
	dmJobCfg = bytes.ReplaceAll(dmJobCfg, []byte("<placeholder>"), []byte(db))
	var resp *enginepb.SubmitJobResponse
	require.Eventually(t, func() bool {
		resp, err = client.SubmitJob(ctx, &enginepb.SubmitJobRequest{
			Tp:     int32(engineModel.JobTypeDM),
			Config: dmJobCfg,
		})
		return err == nil && resp.Err == nil
	}, time.Second*5, time.Millisecond*100)

	// check full phase
	waitRow := func(where string) {
		require.Eventually(t, func() bool {
			rs, err := tidb.Query("select 1 from " + db + ".t1 where " + where)
			if err != nil {
				t.Logf("query error: %v", err)
				return false
			}
			if !rs.Next() {
				t.Log("no rows")
				return false
			}
			if rs.Next() {
				t.Log("more than one row")
				return false
			}
			return true
		}, 30*time.Second, 500*time.Millisecond)
	}
	waitRow("c = 1")

	// incremental phase
	noError(mysql.Exec("insert into " + db + ".t1 values(2)"))
	waitRow("c = 2")

	// imitate an error that can auto resume
	noError(tidb.Exec("drop table " + db + ".t1"))
	noError(mysql.Exec("insert into " + db + ".t1 values(3)"))
	time.Sleep(time.Second)
	noError(tidb.Exec("create table " + db + ".t1(c int primary key)"))
	time.Sleep(time.Second)

	// check auto resume
	waitRow("c = 3")

	// check query status
	source1 := "mysql-replica-01"
	source2 := "mysql-replica-02"
	resp2, err := queryStatus(ctx, client, resp.JobId, []string{source1, source2}, t)
	require.NoError(t, err)
	require.Nil(t, resp2.Err)
	var jobStatus dm.JobStatus
	require.NoError(t, json.Unmarshal([]byte(resp2.JsonRet), &jobStatus))
	require.Equal(t, resp.JobId, jobStatus.JobMasterID)
	require.Contains(t, string(jobStatus.TaskStatus[source1].Status.Status), "totalEvents")
	require.Contains(t, jobStatus.TaskStatus[source2].Status.ErrorMsg, fmt.Sprintf("task %s for job not found", source2))

	// pause task
	resp2, err = operateTask(ctx, client, resp.JobId, nil, dmpkg.Pause, t)
	require.NoError(t, err)
	require.Nil(t, resp2.Err)
	require.Equal(t, "null", resp2.JsonRet)

	// eventually paused
	require.Eventually(t, func() bool {
		resp2, err = queryStatus(ctx, client, resp.JobId, []string{source1}, t)
		require.NoError(t, err)
		require.Nil(t, resp2.Err)
		require.NoError(t, json.Unmarshal([]byte(resp2.JsonRet), &jobStatus))
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StagePaused
	}, time.Second*10, time.Second)

	// binlog schema list
	binlogSchemaReq := &dmpkg.BinlogSchemaRequest{
		Op:      pb.SchemaOp_ListSchema,
		Sources: []string{source1},
	}
	resp2, err = binlogSchema(ctx, client, resp.JobId, binlogSchemaReq, t)
	require.NoError(t, err)
	require.Nil(t, resp2.Err)
	var binlogSchemaResp dmpkg.BinlogSchemaResponse
	require.NoError(t, json.Unmarshal([]byte(resp2.JsonRet), &binlogSchemaResp))
	require.Equal(t, "", binlogSchemaResp.ErrorMsg)
	require.Len(t, binlogSchemaResp.Results, 1)
	require.Equal(t, fmt.Sprintf(`["%s"]`, db), binlogSchemaResp.Results[source1].Msg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].ErrorMsg)

	// resume task
	resp2, err = operateTask(ctx, client, resp.JobId, nil, dmpkg.Resume, t)
	require.NoError(t, err)
	require.Nil(t, resp2.Err)
	require.Equal(t, "null", resp2.JsonRet)

	// eventually resumed
	require.Eventually(t, func() bool {
		resp2, err = queryStatus(ctx, client, resp.JobId, []string{source1}, t)
		require.NoError(t, err)
		require.Nil(t, resp2.Err)
		require.NoError(t, json.Unmarshal([]byte(resp2.JsonRet), &jobStatus))
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StageRunning
	}, time.Second*10, time.Second)

	// get job cfg
	resp2, err = getJobCfg(ctx, client, resp.JobId, t)
	require.NoError(t, err)
	require.Nil(t, resp2.Err)
	require.Contains(t, resp2.JsonRet, `"flavor":"mysql"`)

	noError(mysql.Exec("alter table " + db + ".t1 add column new_col int unique"))
	noError(mysql.Exec("insert into " + db + ".t1 values(4,4)"))

	// eventually error
	require.Eventually(t, func() bool {
		resp2, err = queryStatus(ctx, client, resp.JobId, []string{source1}, t)
		require.NoError(t, err)
		require.Nil(t, resp2.Err)
		require.NoError(t, json.Unmarshal([]byte(resp2.JsonRet), &jobStatus))
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StageError &&
			strings.Contains(jobStatus.TaskStatus[source1].Status.Result.Errors[0].RawCause,
				`unsupported add column 'new_col' constraint UNIQUE KEY when altering`)
	}, time.Second*10, time.Second)

	// binlog replace
	binlogReq := &dmpkg.BinlogRequest{
		Op: pb.ErrorOp_Replace,
		Sqls: []string{
			"alter table " + db + ".t1 add column new_col int;",
			"alter table " + db + ".t1 add unique(new_col);",
		},
	}
	resp2, err = binlog(ctx, client, resp.JobId, binlogReq, t)
	require.NoError(t, err)
	require.Nil(t, resp2.Err)
	var binlogResp dmpkg.BinlogResponse
	require.NoError(t, json.Unmarshal([]byte(resp2.JsonRet), &binlogResp))
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].ErrorMsg)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	waitRow("new_col = 4")

	// binlog replace again
	resp2, err = binlog(ctx, client, resp.JobId, binlogReq, t)
	require.NoError(t, err)
	require.Nil(t, resp2.Err)
	require.NoError(t, json.Unmarshal([]byte(resp2.JsonRet), &binlogResp))
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	require.Equal(t, fmt.Sprintf("source '%s' has no error", source1), binlogResp.Results[source1].ErrorMsg)
}

func queryStatus(ctx context.Context, client client.MasterClient, jobID string, tasks []string, t *testing.T) (*enginepb.DebugJobResponse, error) {
	var args struct {
		Tasks []string
	}
	args.Tasks = tasks
	jsonArg, err := json.Marshal(args)
	require.NoError(t, err)
	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return client.DebugJob(ctx2, &enginepb.DebugJobRequest{JobId: jobID, Command: dmpkg.QueryStatus, JsonArg: string(jsonArg)})
}

func operateTask(ctx context.Context, client client.MasterClient, jobID string, tasks []string, op dmpkg.OperateType, t *testing.T) (*enginepb.DebugJobResponse, error) {
	var args struct {
		Tasks []string
		Op    dmpkg.OperateType
	}
	args.Tasks = tasks
	args.Op = op
	jsonArg, err := json.Marshal(args)
	require.NoError(t, err)
	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return client.DebugJob(ctx2, &enginepb.DebugJobRequest{JobId: jobID, Command: dmpkg.OperateTask, JsonArg: string(jsonArg)})
}

func getJobCfg(ctx context.Context, client client.MasterClient, jobID string, t *testing.T) (*enginepb.DebugJobResponse, error) {
	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return client.DebugJob(ctx2, &enginepb.DebugJobRequest{JobId: jobID, Command: dmpkg.GetJobCfg})
}

func binlog(ctx context.Context, client client.MasterClient, jobID string, req *dmpkg.BinlogRequest, t *testing.T) (*enginepb.DebugJobResponse, error) {
	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	jsonArg, err := json.Marshal(req)
	require.NoError(t, err)
	return client.DebugJob(ctx2, &enginepb.DebugJobRequest{JobId: jobID, Command: dmpkg.Binlog, JsonArg: string(jsonArg)})
}

func binlogSchema(ctx context.Context, client client.MasterClient, jobID string, req *dmpkg.BinlogSchemaRequest, t *testing.T) (*enginepb.DebugJobResponse, error) {
	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	jsonArg, err := json.Marshal(req)
	require.NoError(t, err)
	return client.DebugJob(ctx2, &enginepb.DebugJobRequest{JobId: jobID, Command: dmpkg.BinlogSchema, JsonArg: string(jsonArg)})
}
