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
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
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
		content, err := io.ReadAll(resp.Body)
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
		Timeout: 3 * time.Second,
	}

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
	var resp *enginepb.SubmitJobResponse
	require.Eventually(t, func() bool {
		resp, err = client.SubmitJob(ctx, &enginepb.SubmitJobRequest{
			Tp:     int32(engineModel.JobTypeDM),
			Config: dmJobCfg,
		})
		return err == nil && resp.Err == nil
	}, time.Second*5, time.Millisecond*100)

	// check full phase
	waitRow := func(where string, db string) {
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
	waitRow("c = 1", db)

	// check load finished
	source1 := "mysql-replica-01"
	source2 := "mysql-replica-02"
	require.Eventually(t, func() bool {
		jobStatus, err := queryStatus(httpClient, resp.JobId, []string{source1})
		return err == nil && jobStatus.TaskStatus[source1].Status.Stage == metadata.StageFinished
	}, time.Second*30, time.Millisecond*100)

	// start incremental job via updateJobConfig
	binlogName, binlogPos, err := getMasterStatus(tcontext.Background(), conn.NewBaseDB(mysql), gmysql.MySQLFlavor)
	require.NoError(t, err)
	dmJobCfg = bytes.ReplaceAll(dmJobCfg, []byte("task-mode: full"), []byte("task-mode: incremental"))
	dmJobCfg = bytes.ReplaceAll(dmJobCfg, []byte("binlog-name: ON.000001"), []byte(fmt.Sprintf("binlog-name: %s", binlogName)))
	dmJobCfg = bytes.ReplaceAll(dmJobCfg, []byte("binlog-pos: 4"), []byte(fmt.Sprintf("binlog-pos: %d", binlogPos)))
	err = updateJobCfg(httpClient, resp.JobId, string(dmJobCfg))
	require.NoError(t, err)

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

	jobStatus, err := queryStatus(httpClient, resp.JobId, []string{source1, source2})
	require.NoError(t, err)
	require.Equal(t, resp.JobId, jobStatus.JobMasterID)
	require.Contains(t, string(jobStatus.TaskStatus[source1].Status.Status), "totalEvents")
	require.Contains(t, jobStatus.TaskStatus[source2].Status.ErrorMsg, fmt.Sprintf("task %s for job not found", source2))

	// pause task
	err = operateJob(httpClient, resp.JobId, []string{source1}, dmpkg.Pause)
	require.NoError(t, err)

	// eventually paused
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(httpClient, resp.JobId, nil)
		require.NoError(t, err)
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StagePaused
	}, time.Second*10, time.Second)

	// binlog schema list
	binlogSchemaResp, err := getBinlogSchema(httpClient, resp.JobId, source1, "", "")
	require.NoError(t, err)
	require.Len(t, binlogSchemaResp.Results, 1)
	require.Equal(t, fmt.Sprintf(`["%s"]`, db), binlogSchemaResp.Results[source1].Msg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].ErrorMsg)

	// resume task
	err = operateJob(httpClient, resp.JobId, nil, dmpkg.Resume)
	require.NoError(t, err)

	// eventually resumed
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(httpClient, resp.JobId, []string{source1})
		require.NoError(t, err)
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StageRunning
	}, time.Second*10, time.Second)

	// get job cfg
	jobCfg, err := getJobCfg(httpClient, resp.JobId)
	require.NoError(t, err)
	require.Contains(t, jobCfg, `flavor: mysql`)
	require.Contains(t, jobCfg, `tidb_txn_mode: optimistic`)

	noError(mysql.Exec("alter table " + db + ".t1 add column new_col int unique"))
	noError(mysql.Exec("insert into " + db + ".t1 values(4,4)"))

	// eventually error
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(httpClient, resp.JobId, nil)
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
	binlogResp, err := setBinlogOperator(httpClient, resp.JobId, source1, binlogReq)
	require.NoError(t, err)
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].ErrorMsg)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	waitRow("new_col = 4", db)

	// binlog replace again
	binlogResp, err = setBinlogOperator(httpClient, resp.JobId, source1, binlogReq)
	require.NoError(t, err)
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	require.Equal(t, fmt.Sprintf("source '%s' has no error", source1), binlogResp.Results[source1].ErrorMsg)

	// binlog get
	binlogResp, err = getBinlogOperator(httpClient, resp.JobId, source1, "")
	require.NoError(t, err)
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	require.Equal(t, fmt.Sprintf("source '%s' has no error", source1), binlogResp.Results[source1].ErrorMsg)

	// binlog delete
	binlogResp, err = deleteBinlogOperator(httpClient, resp.JobId, source1)
	require.NoError(t, err)
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Len(t, binlogResp.Results, 1)
	require.Equal(t, "", binlogResp.Results[source1].Msg)
	require.Equal(t, fmt.Sprintf("source '%s' has no error", source1), binlogResp.Results[source1].ErrorMsg)

	// pause task
	err = operateJob(httpClient, resp.JobId, []string{source1}, dmpkg.Pause)
	require.NoError(t, err)
	// eventually paused
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(httpClient, resp.JobId, nil)
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
	binlogSchemaResp, err = setBinlogSchema(httpClient, resp.JobId, source1, binlogSchemaReq)
	require.NoError(t, err)
	require.Equal(t, "", binlogSchemaResp.ErrorMsg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].ErrorMsg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].Msg)
	// get new binlog schema
	binlogSchemaResp, err = getBinlogSchema(httpClient, resp.JobId, source1, db, "t1")
	require.NoError(t, err)
	require.Len(t, binlogSchemaResp.Results, 1)
	require.Equal(t, "CREATE TABLE `t1` ( `c` int(11) NOT NULL, `new_col` int(11) DEFAULT NULL, PRIMARY KEY (`c`) /*T![clustered_index] CLUSTERED */, UNIQUE KEY `new_col` (`new_col`)) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin", binlogSchemaResp.Results[source1].Msg)
	require.Equal(t, "", binlogSchemaResp.Results[source1].ErrorMsg)

	// update with new balist
	newDB := "new_" + db
	dmJobCfg = bytes.ReplaceAll(dmJobCfg, []byte(fmt.Sprintf(`["%s"]`, db)), []byte(fmt.Sprintf(`["%s", "%s"]`, db, newDB)))
	err = updateJobCfg(httpClient, resp.JobId, string(dmJobCfg))
	require.NoError(t, err)
	// get new config
	jobCfg, err = getJobCfg(httpClient, resp.JobId)
	require.NoError(t, err)
	require.Contains(t, jobCfg, newDB)
	require.Contains(t, jobCfg, `mod_revision: 2`)
	// eventually apply new config, task still paused
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(httpClient, resp.JobId, nil)
		return err == nil && jobStatus.TaskStatus[source1].CfgModRevison == 2 && jobStatus.TaskStatus[source1].Status.Stage == metadata.StagePaused
	}, time.Second*30, time.Second)

	// resume task
	err = operateJob(httpClient, resp.JobId, nil, dmpkg.Resume)
	require.NoError(t, err)
	// eventually resumed
	require.Eventually(t, func() bool {
		jobStatus, err = queryStatus(httpClient, resp.JobId, []string{source1})
		require.NoError(t, err)
		return jobStatus.TaskStatus[source1].Status.Stage == metadata.StageRunning
	}, time.Second*10, time.Second)

	noError(mysql.Exec("create database " + newDB))
	noError(mysql.Exec("create table " + newDB + ".t1(c int primary key)"))
	noError(mysql.Exec("insert into " + newDB + ".t1 values(1)"))
	waitRow("c = 1", newDB)
}

func queryStatus(client *http.Client, jobID string, tasks []string) (*dm.JobStatus, error) {
	u := fmt.Sprintf(baseURL+"/status", jobID)
	v := url.Values{}
	for _, task := range tasks {
		v.Add("tasks", task)
	}
	u += "?" + v.Encode()
	resp, err := client.Get(u)
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

func operateJob(client *http.Client, jobID string, tasks []string, op dmpkg.OperateType) error {
	operateJobReq := &openapi.OperateJobRequest{
		Tasks: &tasks,
	}
	switch op {
	case dmpkg.Pause:
		operateJobReq.Op = openapi.OperateJobRequestOpPause
	case dmpkg.Resume:
		operateJobReq.Op = openapi.OperateJobRequestOpResume
	}

	bs, err := json.Marshal(operateJobReq)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPut, fmt.Sprintf(baseURL+"/status", jobID), bytes.NewReader(bs))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	_, err = client.Do(req)
	return err
}

func getJobCfg(client *http.Client, jobID string) (string, error) {
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

func updateJobCfg(client *http.Client, jobID string, cfg string) error {
	url := fmt.Sprintf(baseURL+"/config", jobID)
	req := openapi.UpdateJobConfigRequest{
		Config: cfg,
	}
	bs, err := json.Marshal(req)
	if err != nil {
		return err
	}
	updateCfgReq, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(bs))
	if err != nil {
		return err
	}
	updateCfgReq.Header.Set("Content-Type", "application/json")
	_, err = client.Do(updateCfgReq)
	return err
}

func getBinlogOperator(client *http.Client, jobID string, task string, binlogPos string) (*dmpkg.BinlogResponse, error) {
	u := fmt.Sprintf(baseURL+"/binlog/tasks/%s", jobID, task)
	v := url.Values{}
	if binlogPos != "" {
		v.Add("binlog_pos", binlogPos)
	}
	u += "?" + v.Encode()
	resp, err := client.Get(u)
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

func setBinlogOperator(client *http.Client, jobID string, task string, req *openapi.SetBinlogOperatorRequest) (*dmpkg.BinlogResponse, error) {
	bs, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := client.Post(fmt.Sprintf(baseURL+"/binlog/tasks/%s", jobID, task), "application/json", bytes.NewReader(bs))
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

func deleteBinlogOperator(client *http.Client, jobID string, task string) (*dmpkg.BinlogResponse, error) {
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf(baseURL+"/binlog/tasks/%s", jobID, task), nil)
	if err != nil {
		return nil, err
	}
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

func getBinlogSchema(client *http.Client, jobID string, task string, schema string, table string) (*dmpkg.BinlogSchemaResponse, error) {
	u := fmt.Sprintf(baseURL+"/schema/tasks/%s", jobID, task)
	v := url.Values{}
	if schema != "" {
		v.Add("database", schema)
	}
	if table != "" {
		v.Add("table", table)
	}
	u += "?" + v.Encode()
	resp, err := client.Get(u)
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

func setBinlogSchema(client *http.Client, jobID string, task string, req *openapi.SetBinlogSchemaRequest) (*dmpkg.BinlogSchemaResponse, error) {
	url := fmt.Sprintf(baseURL+"/schema/tasks/%s", jobID, task)
	bs, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	binlogSchemaReq, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(bs))
	if err != nil {
		return nil, err
	}
	binlogSchemaReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(binlogSchemaReq)
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

func getMasterStatus(ctx *tcontext.Context, db *conn.BaseDB, flavor string) (string, uint32, error) {
	binlogName, pos, _, _, _, err := conn.GetMasterStatus(ctx, db, flavor)
	return binlogName, pos, err
}
