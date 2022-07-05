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

	"github.com/pingcap/tiflow/tests/integration_tests/util"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/client"
	"github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/jobmaster/dm"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/openapi"
	engineModel "github.com/pingcap/tiflow/engine/model"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
)

const (
	baseURL = "http://127.0.0.1:10245/api/v1/jobs/%s"
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

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
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
	jobStatus, err := queryStatus(ctx, httpClient, resp.JobId, "", t)
	require.NoError(t, err)
	require.Equal(t, resp.JobId, jobStatus.JobMasterID)
	require.Contains(t, string(jobStatus.TaskStatus[source1].Status.Status), "totalEvents")
	jobStatus, err = queryStatus(ctx, httpClient, resp.JobId, source2, t)
	require.NoError(t, err)
	require.Contains(t, jobStatus.TaskStatus[source2].Status.ErrorMsg, fmt.Sprintf("task %s for job not found", source2))

	// pause task
	err = operateTask(ctx, httpClient, resp.JobId, source1, dmpkg.Pause, t)
	require.NoError(t, err)

	// eventually paused
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(ctx, httpClient, resp.JobId, source1, t)
		require.NoError(t, err)
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StagePaused
	}, time.Second*10, time.Second)

	// binlog schema list
	binlogSchemaResp, err := getBinlogSchema(ctx, httpClient, resp.JobId, source1, "", "", t)
	require.NoError(t, err)
	require.Len(t, binlogSchemaResp.Results, 1)
	require.Equal(t, fmt.Sprintf(`["%s"]`, db), binlogSchemaResp.Results[source1].Msg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].ErrorMsg)

	// resume task
	err = operateTask(ctx, httpClient, resp.JobId, source1, dmpkg.Resume, t)
	require.NoError(t, err)

	// eventually resumed
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(ctx, httpClient, resp.JobId, source1, t)
		require.NoError(t, err)
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StageRunning
	}, time.Second*10, time.Second)

	// get job cfg
	jobCfg, err := getJobCfg(ctx, *httpClient, resp.JobId, t)
	require.NoError(t, err)
	require.Contains(t, jobCfg, `flavor: mysql`)

	noError(mysql.Exec("alter table " + db + ".t1 add column new_col int unique"))
	noError(mysql.Exec("insert into " + db + ".t1 values(4,4)"))

	// eventually error
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(ctx, httpClient, resp.JobId, source1, t)
		require.NoError(t, err)
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StageError &&
			strings.Contains(jobStatus.TaskStatus[source1].Status.Result.Errors[0].RawCause,
				`unsupported add column 'new_col' constraint UNIQUE KEY when altering`)
	}, time.Second*10, time.Second)

	// binlog replace
	binlogReq := &openapi.SetBinlogOperatorRequest{
		Op: openapi.SetBinlogOperatorRequestOpReplace,
		Sqls: &[]string{
			"alter table " + db + ".t1 add column new_col int;",
			"alter table " + db + ".t1 add unique(new_col);",
		},
	}
	binlogResp, err := setBinlogOperator(ctx, httpClient, resp.JobId, source1, binlogReq, t)
	require.NoError(t, err)
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].ErrorMsg)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	waitRow("new_col = 4")

	// binlog replace again
	binlogResp, err = setBinlogOperator(ctx, httpClient, resp.JobId, source1, binlogReq, t)
	require.NoError(t, err)
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	require.Equal(t, fmt.Sprintf("source '%s' has no error", source1), binlogResp.Results[source1].ErrorMsg)

	// binlog get
	binlogResp, err = getBinlogOperator(ctx, httpClient, resp.JobId, source1, "", t)
	require.NoError(t, err)
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	require.Equal(t, fmt.Sprintf("source '%s' has no error", source1), binlogResp.Results[source1].ErrorMsg)

	// binlog delete
	binlogResp, err = deleteBinlogOperator(ctx, httpClient, resp.JobId, source1, t)
	require.NoError(t, err)
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	require.Equal(t, fmt.Sprintf("source '%s' has no error", source1), binlogResp.Results[source1].ErrorMsg)

	// pause task
	err = operateTask(ctx, httpClient, resp.JobId, source1, dmpkg.Pause, t)
	require.NoError(t, err)
	// eventually paused
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(ctx, httpClient, resp.JobId, source1, t)
		require.NoError(t, err)
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StagePaused
	}, time.Second*10, time.Second)
	// set binlog schema
	fromSource := true
	binlogSchemaReq := &openapi.SetBinlogSchemaRequest{
		Database:   db,
		Table:      "t1",
		FromSource: &fromSource,
	}
	binlogSchemaResp, err = setBinlogSchema(ctx, httpClient, resp.JobId, source1, binlogSchemaReq, t)
	require.NoError(t, err)
	require.Equal(t, "", binlogSchemaResp.ErrorMsg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].ErrorMsg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].Msg)
	// get new binlog schema
	binlogSchemaResp, err = getBinlogSchema(ctx, httpClient, resp.JobId, source1, db, "t1", t)
	require.NoError(t, err)
	require.Len(t, binlogSchemaResp.Results, 1)
	require.Equal(t, "CREATE TABLE `t1` ( `c` int(11) NOT NULL, `new_col` int(11) DEFAULT NULL, PRIMARY KEY (`c`) /*T![clustered_index] CLUSTERED */, UNIQUE KEY `new_col` (`new_col`)) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin", binlogSchemaResp.Results[source1].Msg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].ErrorMsg)
}

func queryStatus(ctx context.Context, client *http.Client, jobID string, task string, t *testing.T) (*dm.JobStatus, error) {
	url := fmt.Sprintf(baseURL+"/status", jobID)
	if task != "" {
		url += fmt.Sprintf("/tasks/%s", task)
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var jobStatus dm.JobStatus
	err = json.Unmarshal(respBody, &jobStatus)
	return &jobStatus, err
}

func operateTask(ctx context.Context, client *http.Client, jobID string, task string, op dmpkg.OperateType, t *testing.T) error {
	operateTaskReq := &openapi.OperateTaskRequest{}
	switch op {
	case dmpkg.Pause:
		operateTaskReq.Op = openapi.OperateTaskRequestOpPause
	case dmpkg.Resume:
		operateTaskReq.Op = openapi.OperateTaskRequestOpResume
	}

	bs, err := json.Marshal(operateTaskReq)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(baseURL+"/status/tasks/%s", jobID, task), bytes.NewReader(bs))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	_, err = client.Do(req)
	return err
}

func getJobCfg(ctx context.Context, client http.Client, jobID string, t *testing.T) (string, error) {
	resp, err := client.Get(fmt.Sprintf(baseURL+"/config", jobID))
	if err != nil {
		return "", err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	var jobCfg string
	err = json.Unmarshal(respBody, &jobCfg)
	return jobCfg, err
}

func getBinlogOperator(ctx context.Context, client *http.Client, jobID string, task string, binlogPos string, t *testing.T) (*dmpkg.BinlogResponse, error) {
	url := fmt.Sprintf(baseURL+"/binlog/tasks/%s", jobID, task)
	if binlogPos != "" {
		url += "?binlog_pos=" + binlogPos
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var binlogResp dmpkg.BinlogResponse
	err = json.Unmarshal(respBody, &binlogResp)
	return &binlogResp, err
}

func setBinlogOperator(ctx context.Context, client *http.Client, jobID string, task string, req *openapi.SetBinlogOperatorRequest, t *testing.T) (*dmpkg.BinlogResponse, error) {
	bs, err := json.Marshal(req)
	require.NoError(t, err)
	resp, err := client.Post(fmt.Sprintf(baseURL+"/binlog/tasks/%s", jobID, task), "application/json", bytes.NewReader(bs))
	require.NoError(t, err)
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var binlogResp dmpkg.BinlogResponse
	err = json.Unmarshal(respBody, &binlogResp)
	return &binlogResp, err
}

func deleteBinlogOperator(ctx context.Context, client *http.Client, jobID string, task string, t *testing.T) (*dmpkg.BinlogResponse, error) {
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf(baseURL+"/binlog/tasks/%s", jobID, task), nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var binlogResp dmpkg.BinlogResponse
	err = json.Unmarshal(respBody, &binlogResp)
	return &binlogResp, err
}

func getBinlogSchema(ctx context.Context, client *http.Client, jobID string, task string, schema string, table string, t *testing.T) (*dmpkg.BinlogSchemaResponse, error) {
	url := fmt.Sprintf(baseURL+"/schema/tasks/%s", jobID, task)
	if schema != "" {
		url += "?database=" + schema
	}
	if table != "" {
		url += "&table=" + table
	}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var binlogSchemaResp dmpkg.BinlogSchemaResponse
	err = json.Unmarshal(respBody, &binlogSchemaResp)
	return &binlogSchemaResp, err
}

func setBinlogSchema(ctx context.Context, client *http.Client, jobID string, task string, req *openapi.SetBinlogSchemaRequest, t *testing.T) (*dmpkg.BinlogSchemaResponse, error) {
	url := fmt.Sprintf(baseURL+"/schema/tasks/%s", jobID, task)
	bs, err := json.Marshal(req)
	require.NoError(t, err)
	binlogSchemaReq, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(bs))
	require.NoError(t, err)
	binlogSchemaReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(binlogSchemaReq)
	require.NoError(t, err)
	respBody, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	var binlogSchemaResp dmpkg.BinlogSchemaResponse
	err = json.Unmarshal(respBody, &binlogSchemaResp)
	return &binlogSchemaResp, err
}
