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
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/jobmaster/dm"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/openapi"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/test/e2e"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/tests/integration_tests/util"
	"github.com/stretchr/testify/require"
)

const (
	masterAddr = "127.0.0.1:10245"
	baseURL    = "http://" + masterAddr + "/api/v1/jobs/%s"
	tenantID   = "e2e-test"
	projectID  = "project-dm"
)

func TestDMJob(t *testing.T) {
	mysqlCfg := util.DBConfig{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "",
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
		testSimpleAllModeTask(t, mysql, tidb, "test1")
	}()
	go func() {
		defer wg.Done()
		testSimpleAllModeTask(t, mysql, tidb, "test2")
	}()
	wg.Wait()

	// executor metrics
	metricsURLs := []string{
		"http://127.0.0.1:11241/metrics",
		"http://127.0.0.1:11242/metrics",
		"http://127.0.0.1:11243/metrics",
	}

	testMetrics := func(re *regexp.Regexp) {
		jobIDs := map[string]struct{}{}

		ctx := context.Background()
		cli, err := httputil.NewClient(nil)
		require.NoError(t, err)
		for _, metricsURL := range metricsURLs {
			resp, err := cli.Get(ctx, metricsURL)
			require.NoError(t, err)
			content, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			matched := re.FindAllSubmatch(content, -1)
			for _, m := range matched {
				jobIDs[string(m[1])] = struct{}{}
			}
			require.NoError(t, resp.Body.Close())
		}
		require.Equal(t, 2, len(jobIDs))
	}

	// test metrics for syncer
	re := regexp.MustCompile(`syncer.*\{job_id="(.{36})"`)
	testMetrics(re)
	// test metrics for dm_worker_task_state: 2 running all job
	re = regexp.MustCompile(`dm_worker_task_state.*\{job_id="(.{36})".*2`)
	testMetrics(re)
}

// testSimpleAllModeTask extracts the common logic for a DM "all" mode task,
// `db` should not contain special character.
func testSimpleAllModeTask(
	t *testing.T,
	mysql, tidb *sql.DB,
	db string,
) {
	ctx := context.Background()
	noError := func(_ interface{}, err error) {
		require.NoError(t, err)
	}

	httpClient, err := httputil.NewClient(nil)
	require.NoError(t, err)

	noError(tidb.Exec("drop database if exists " + db))
	noError(mysql.Exec("drop database if exists " + db))

	// full phase
	noError(mysql.Exec("create database " + db))
	noError(mysql.Exec("create table " + db + ".t1(c int primary key)"))
	noError(mysql.Exec("insert into " + db + ".t1 values(1)"))

	dmJobCfg, err := os.ReadFile("./dm-job.yaml")
	require.NoError(t, err)
	// start full job
	dmJobCfg = bytes.ReplaceAll(dmJobCfg, []byte("<placeholder>"), []byte(db))
	dmJobCfg = bytes.ReplaceAll(dmJobCfg, []byte("task-mode: all"), []byte("task-mode: full"))
	var jobID string
	require.Eventually(t, func() bool {
		var err error
		jobID, err = e2e.CreateJobViaHTTP(ctx, masterAddr, tenantID, projectID,
			pb.Job_DM, dmJobCfg)
		return err == nil
	}, time.Second*5, time.Millisecond*100)

	// check full phase
	waitRow := func(where string, db string) {
		require.Eventually(t, func() bool {
			//nolint:sqlclosecheck
			rs, err := tidb.Query("select 1 from " + db + ".t1 where " + where)
			if err != nil {
				t.Logf("query error: %v", err)
				return false
			}
			defer func(rs *sql.Rows) {
				_ = rs.Close()
			}(rs)
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
	waitRow("c = 1", db)

	// load finished and job exits
	// TODO: check load status after framework supports it
	// TODO: check checkpoint deleted after frameworker support StopImpl
	require.Eventually(t, func() bool {
		job, err := e2e.QueryJobViaHTTP(ctx, masterAddr, tenantID, projectID, jobID)
		return err == nil && job.State == pb.Job_Finished
	}, time.Second*30, time.Millisecond*100)

	source1 := "mysql-replica-01"
	source2 := "mysql-replica-02"

	// start incremental job
	dmJobCfg = bytes.ReplaceAll(dmJobCfg, []byte("task-mode: full"), []byte("task-mode: incremental"))
	require.Eventually(t, func() bool {
		var err error
		jobID, err = e2e.CreateJobViaHTTP(ctx, masterAddr, tenantID, projectID, pb.Job_DM, dmJobCfg)
		return err == nil
	}, time.Second*5, time.Millisecond*100)

	var jobStatus *dm.JobStatus
	// wait job online
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(ctx, httpClient, jobID, []string{source1, source2})
		return err == nil && jobStatus.JobID == jobID
	}, time.Second*5, time.Millisecond*100)
	require.Contains(t, string(jobStatus.TaskStatus[source1].Status.Status), "totalEvents")
	require.Contains(t, jobStatus.TaskStatus[source2].Status.ErrorMsg, fmt.Sprintf("task %s for job not found", source2))

	// incremental phase
	noError(mysql.Exec("insert into " + db + ".t1 values(2)"))
	waitRow("c = 2", db)

	// imitate an error that can auto resume
	noError(tidb.Exec("drop table " + db + ".t1"))
	noError(mysql.Exec("insert into " + db + ".t1 values(3)"))
	time.Sleep(time.Second)
	noError(tidb.Exec("create table " + db + ".t1(c int primary key)"))
	time.Sleep(time.Second)

	// check auto resume
	waitRow("c = 3", db)

	// pause task
	err = operateJob(ctx, httpClient, jobID, []string{source1}, dmpkg.Pause)
	require.NoError(t, err)

	// eventually paused
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(ctx, httpClient, jobID, nil)
		for _, task := range jobStatus.TaskStatus {
			require.Greater(t, task.Status.IoTotalBytes, uint64(0))
			require.Greater(t, task.Status.DumpIoTotalBytes, uint64(0))
		}
		require.NoError(t, err)
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StagePaused
	}, time.Second*10, time.Second)

	// binlog schema list
	binlogSchemaResp, err := getBinlogSchema(ctx, httpClient, jobID, source1, "", "")
	require.NoError(t, err)
	require.Len(t, binlogSchemaResp.Results, 1)
	require.Equal(t, fmt.Sprintf(`["%s"]`, db), binlogSchemaResp.Results[source1].Msg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].ErrorMsg)

	// resume task
	err = operateJob(ctx, httpClient, jobID, nil, dmpkg.Resume)
	require.NoError(t, err)

	// eventually resumed
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(ctx, httpClient, jobID, []string{source1})
		require.NoError(t, err)
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StageRunning
	}, time.Second*10, time.Second)

	// get job cfg
	jobCfg, err := getJobCfg(ctx, httpClient, jobID)
	require.NoError(t, err)
	require.Contains(t, jobCfg, `flavor: mysql`)
	require.Contains(t, jobCfg, `tidb_txn_mode: optimistic`)

	noError(mysql.Exec("alter table " + db + ".t1 add column new_col int unique"))
	noError(mysql.Exec("insert into " + db + ".t1 values(4,4)"))

	// eventually error
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(ctx, httpClient, jobID, nil)
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
	binlogResp, err := setBinlogOperator(ctx, httpClient, jobID, source1, binlogReq)
	require.NoError(t, err)
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].ErrorMsg)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	waitRow("new_col = 4", db)

	// binlog replace again
	binlogResp, err = setBinlogOperator(ctx, httpClient, jobID, source1, binlogReq)
	require.NoError(t, err)
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	require.Equal(t, fmt.Sprintf("source '%s' has no error", source1), binlogResp.Results[source1].ErrorMsg)

	// binlog get
	binlogResp, err = getBinlogOperator(ctx, httpClient, jobID, source1, "")
	require.NoError(t, err)
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	require.Equal(t, fmt.Sprintf("source '%s' has no error", source1), binlogResp.Results[source1].ErrorMsg)

	// binlog delete
	binlogResp, err = deleteBinlogOperator(ctx, httpClient, jobID, source1)
	require.NoError(t, err)
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	require.Equal(t, fmt.Sprintf("source '%s' has no error", source1), binlogResp.Results[source1].ErrorMsg)

	// pause task
	err = operateJob(ctx, httpClient, jobID, []string{source1}, dmpkg.Pause)
	require.NoError(t, err)
	// eventually paused
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(ctx, httpClient, jobID, nil)
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
	binlogSchemaResp, err = setBinlogSchema(ctx, httpClient, jobID, source1, binlogSchemaReq)
	require.NoError(t, err)
	require.Equal(t, "", binlogSchemaResp.ErrorMsg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].ErrorMsg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].Msg)
	// get new binlog schema
	binlogSchemaResp, err = getBinlogSchema(ctx, httpClient, jobID, source1, db, "t1")
	require.NoError(t, err)
	require.Len(t, binlogSchemaResp.Results, 1)
	require.Equal(t, "CREATE TABLE `t1` ( `c` int(11) NOT NULL, `new_col` int(11) DEFAULT NULL, PRIMARY KEY (`c`) /*T![clustered_index] CLUSTERED */, UNIQUE KEY `new_col` (`new_col`)) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin", binlogSchemaResp.Results[source1].Msg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].ErrorMsg)

	// update with new balist
	newDB := "new_" + db
	dmJobCfg = bytes.ReplaceAll(dmJobCfg, []byte(fmt.Sprintf(`["%s"]`, db)), []byte(fmt.Sprintf(`["%s", "%s"]`, db, newDB)))
	err = updateJobCfg(ctx, httpClient, jobID, string(dmJobCfg))
	require.NoError(t, err)
	// get new config
	jobCfg, err = getJobCfg(ctx, httpClient, jobID)
	require.NoError(t, err)
	require.Contains(t, jobCfg, newDB)
	require.Contains(t, jobCfg, `mod-revision: 1`)
	// eventually apply new config, task still paused
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(ctx, httpClient, jobID, nil)
		return err == nil && !jobStatus.TaskStatus[source1].ConfigOutdated && jobStatus.TaskStatus[source1].Status.Stage == metadata.StagePaused
	}, time.Second*30, time.Second)

	// resume task
	err = operateJob(ctx, httpClient, jobID, nil, dmpkg.Resume)
	require.NoError(t, err)
	// eventually resumed
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(ctx, httpClient, jobID, []string{source1})
		require.NoError(t, err)
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StageRunning
	}, time.Second*10, time.Second)

	noError(mysql.Exec("create database " + newDB))
	noError(mysql.Exec("create table " + newDB + ".t1(c int primary key)"))
	noError(mysql.Exec("insert into " + newDB + ".t1 values(1)"))
	waitRow("c = 1", newDB)
}

func queryStatus(ctx context.Context, client *httputil.Client, jobID string, tasks []string) (*dm.JobStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()
	u := fmt.Sprintf(baseURL+"/status", jobID)
	v := url.Values{}
	for _, task := range tasks {
		v.Add("tasks", task)
	}
	u += "?" + v.Encode()
	resp, err := client.Get(ctx, u)
	if err != nil {
		return nil, err
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("status code %d, body %s", resp.StatusCode, string(respBody))
	}

	var jobStatus dm.JobStatus
	err = json.Unmarshal(respBody, &jobStatus)
	return &jobStatus, err
}

func operateJob(
	ctx context.Context, client *httputil.Client, jobID string, tasks []string,
	op dmpkg.OperateType,
) error {
	operateJobReq := &openapi.OperateJobRequest{
		Tasks: &tasks,
	}
	switch op {
	case dmpkg.Pause:
		operateJobReq.Op = openapi.OperateJobRequestOpPause
	case dmpkg.Resume:
		operateJobReq.Op = openapi.OperateJobRequestOpResume
	}

	url := fmt.Sprintf(baseURL+"/status", jobID)
	header := http.Header{"Content-Type": {"application/json"}}
	bs, err := json.Marshal(operateJobReq)
	if err != nil {
		return err
	}
	_, err = client.DoRequest(ctx, url, http.MethodPut, header, bytes.NewReader(bs))
	return err
}

func getJobCfg(ctx context.Context, client *httputil.Client, jobID string) (string, error) {
	resp, err := client.Get(ctx, fmt.Sprintf(baseURL+"/config", jobID))
	if err != nil {
		return "", err
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	err = resp.Body.Close()
	if err != nil {
		return "", err
	}
	var jobCfg string
	err = json.Unmarshal(respBody, &jobCfg)
	return jobCfg, err
}

func updateJobCfg(ctx context.Context, client *httputil.Client, jobID string, cfg string) error {
	url := fmt.Sprintf(baseURL+"/config", jobID)
	req := openapi.UpdateJobConfigRequest{
		Config: cfg,
	}
	bs, err := json.Marshal(req)
	if err != nil {
		return err
	}
	header := http.Header{"Content-Type": {"application/json"}}
	_, err = client.DoRequest(ctx, url, http.MethodPut, header, bytes.NewReader(bs))
	return err
}

func getBinlogOperator(ctx context.Context, client *httputil.Client, jobID string,
	task string, binlogPos string,
) (*dmpkg.BinlogResponse, error) {
	u := fmt.Sprintf(baseURL+"/binlog/tasks/%s", jobID, task)
	v := url.Values{}
	if binlogPos != "" {
		v.Add("binlog_pos", binlogPos)
	}
	u += "?" + v.Encode()
	resp, err := client.Get(ctx, u)
	if err != nil {
		return nil, err
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	var binlogResp dmpkg.BinlogResponse
	err = json.Unmarshal(respBody, &binlogResp)
	return &binlogResp, err
}

func setBinlogOperator(ctx context.Context, client *httputil.Client, jobID string,
	task string, req *openapi.SetBinlogOperatorRequest,
) (*dmpkg.BinlogResponse, error) {
	bs, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf(baseURL+"/binlog/tasks/%s", jobID, task)
	header := http.Header{"Content-Type": {"application/json"}}
	respBody, err := client.DoRequest(ctx, url, http.MethodPost, header, bytes.NewReader(bs))
	if err != nil {
		return nil, err
	}

	var binlogResp dmpkg.BinlogResponse
	err = json.Unmarshal(respBody, &binlogResp)
	return &binlogResp, err
}

func deleteBinlogOperator(ctx context.Context, client *httputil.Client, jobID string,
	task string,
) (*dmpkg.BinlogResponse, error) {
	url := fmt.Sprintf(baseURL+"/binlog/tasks/%s", jobID, task)
	respBody, err := client.DoRequest(ctx, url, http.MethodDelete, nil, nil)
	if err != nil {
		return nil, err
	}
	var binlogResp dmpkg.BinlogResponse
	err = json.Unmarshal(respBody, &binlogResp)
	return &binlogResp, err
}

func getBinlogSchema(ctx context.Context, client *httputil.Client, jobID string,
	task string, schema string, table string,
) (*dmpkg.BinlogSchemaResponse, error) {
	u := fmt.Sprintf(baseURL+"/schema/tasks/%s", jobID, task)
	v := url.Values{}
	if schema != "" {
		v.Add("database", schema)
	}
	if table != "" {
		v.Add("table", table)
	}
	u += "?" + v.Encode()
	resp, err := client.Get(ctx, u)
	if err != nil {
		return nil, err
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	err = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	var binlogSchemaResp dmpkg.BinlogSchemaResponse
	err = json.Unmarshal(respBody, &binlogSchemaResp)
	return &binlogSchemaResp, err
}

func setBinlogSchema(
	ctx context.Context, client *httputil.Client, jobID string, task string,
	req *openapi.SetBinlogSchemaRequest,
) (*dmpkg.BinlogSchemaResponse, error) {
	url := fmt.Sprintf(baseURL+"/schema/tasks/%s", jobID, task)
	bs, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	header := http.Header{"Content-Type": {"application/json"}}
	respBody, err := client.DoRequest(ctx, url, http.MethodPut, header, bytes.NewReader(bs))
	if err != nil {
		return nil, err
	}
	var binlogSchemaResp dmpkg.BinlogSchemaResponse
	err = json.Unmarshal(respBody, &binlogSchemaResp)
	return &binlogSchemaResp, err
}
