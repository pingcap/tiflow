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

package dm

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	dmmaster "github.com/pingcap/tiflow/dm/dm/master"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/openapi"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/pkg/meta/kvclient/mock"
	"github.com/stretchr/testify/require"
)

const (
	baseURL = "/api/v1/jobs/job-id/"
)

func terrorHTTPErrorHandler(c *gin.Context) {
	c.Next()
	gErr := c.Errors.Last()
	if gErr == nil {
		return
	}
	c.IndentedJSON(http.StatusBadRequest, openapi.ErrorWithMessage{ErrorMsg: gErr.Error()})
}

func equalError(t *testing.T, expected string, body *bytes.Buffer) {
	var e openapi.ErrorWithMessage
	json.Unmarshal(body.Bytes(), &e)
	require.Equal(t, expected, e.ErrorMsg)
}

func TestInitOpenAPI(t *testing.T) {
	var (
		mockBaseJobmaster = &MockBaseJobmaster{}
		mockMessageAgent  = &dmpkg.MockMessageAgent{}
		jm                = &JobMaster{
			workerID:      "jobmaster-worker-id",
			BaseJobMaster: mockBaseJobmaster,
			metadata:      metadata.NewMetaData(mockBaseJobmaster.JobMasterID(), mock.NewMetaMock()),
			messageAgent:  mockMessageAgent,
		}
	)
	jm.taskManager = NewTaskManager(nil, jm.metadata.JobStore(), jm.messageAgent)
	jm.workerManager = NewWorkerManager(nil, jm.metadata.JobStore(), nil, jm.messageAgent, nil)

	engine := gin.New()
	engine.Use(terrorHTTPErrorHandler)
	apiGroup := engine.Group(baseURL)
	jm.initOpenAPI(apiGroup)

	funcBackup := dmmaster.CheckAndAdjustSourceConfigFunc
	dmmaster.CheckAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock
	defer func() {
		dmmaster.CheckAndAdjustSourceConfigFunc = funcBackup
	}()

	testDMAPIGetJobConfig(t, engine, jm)
	testDMAPIGetJobStatus(t, engine, jm)
	testDMAPIOperateTask(t, engine, jm)
	testDMAPIGetBinlogOperator(t, engine, jm, mockMessageAgent)
	testDMAPISetBinlogOperator(t, engine, jm, mockMessageAgent)
	testDMAPIDeleteBinlogOperator(t, engine, jm, mockMessageAgent)
	testDMAPIGetSchema(t, engine, jm, mockMessageAgent)
}

func testDMAPIGetJobConfig(t *testing.T, engine *gin.Engine, jm *JobMaster) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/v1/jobs/job-not-exist/config", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusNotFound, w.Code)

	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"config", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)
	equalError(t, "state not found", w.Body)

	jobCfg := &config.JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))
	require.NoError(t, jm.taskManager.OperateTask(context.Background(), dmpkg.Create, jobCfg, nil))
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"config", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)

	var cfgStr string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &cfgStr))
	bs, err := jobCfg.Yaml()
	require.NoError(t, err)
	require.Equal(t, sortString(string(bs)), sortString(cfgStr))

	require.NoError(t, jm.taskManager.OperateTask(context.Background(), dmpkg.Delete, nil, nil))
}

func testDMAPIGetJobStatus(t *testing.T, engine *gin.Engine, jm *JobMaster) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/v1/jobs/job-not-exist/status", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusNotFound, w.Code)

	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"status", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)
	equalError(t, "state not found", w.Body)

	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"status/tasks/"+"task", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)
	equalError(t, "state not found", w.Body)

	require.NoError(t, jm.taskManager.OperateTask(context.Background(), dmpkg.Create, &config.JobCfg{}, nil))
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"status", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)

	jobStatus := JobStatus{
		JobMasterID: "dm-jobmaster-id",
		WorkerID:    "jobmaster-worker-id",
		TaskStatus:  map[string]TaskStatus{},
	}
	var jobStatus2 JobStatus
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &jobStatus2))
	require.Equal(t, jobStatus, jobStatus2)

	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"status/tasks/"+"task1", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &jobStatus2))
	require.Equal(t, "task task1 for job not found", jobStatus2.TaskStatus["task1"].Status.ErrorMsg)

	require.NoError(t, jm.taskManager.OperateTask(context.Background(), dmpkg.Delete, nil, nil))
}

func testDMAPIOperateTask(t *testing.T, engine *gin.Engine, jm *JobMaster) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("PUT", "/api/v1/jobs/job-not-exist/status", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusNotFound, w.Code)

	w = httptest.NewRecorder()
	r = httptest.NewRequest("PUT", baseURL+"status/tasks/"+"task", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)
	equalError(t, "unsupport op type '' for operate task", w.Body)

	req := openapi.OperateTaskRequest{
		Op: openapi.OperateTaskRequestOpPause,
	}
	bs, err := json.Marshal(req)
	require.NoError(t, err)
	w = httptest.NewRecorder()
	r = httptest.NewRequest("PUT", baseURL+"status/tasks/"+"task", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)
	equalError(t, "state not found", w.Body)

	jobCfg := &config.JobCfg{}
	require.NoError(t, jobCfg.DecodeFile(jobTemplatePath))
	require.NoError(t, jm.taskManager.OperateTask(context.Background(), dmpkg.Create, jobCfg, nil))
	req.Op = openapi.OperateTaskRequestOpResume
	bs, err = json.Marshal(req)
	require.NoError(t, err)
	w = httptest.NewRecorder()
	r = httptest.NewRequest("PUT", baseURL+"status/tasks/"+"mysql-replica-01", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)

	require.NoError(t, jm.taskManager.OperateTask(context.Background(), dmpkg.Delete, nil, nil))
}

func testDMAPIGetBinlogOperator(t *testing.T, engine *gin.Engine, jm *JobMaster, messageAgent *dmpkg.MockMessageAgent) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/v1/jobs/job-not-exist/binlog", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusNotFound, w.Code)

	messageAgent.On("SendRequest").Return(nil, errors.New("binlog operator not found")).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"binlog/tasks/"+"task1"+"?binlog_pos='mysql-bin.000001,4'", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	var binlogResp dmpkg.BinlogResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &binlogResp))
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Equal(t, "binlog operator not found", binlogResp.Results["task1"].ErrorMsg)
}

func testDMAPISetBinlogOperator(t *testing.T, engine *gin.Engine, jm *JobMaster, messageAgent *dmpkg.MockMessageAgent) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/v1/jobs/job-not-exist/binlog", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusNotFound, w.Code)

	req := openapi.SetBinlogOperatorRequest{
		Op: "wrong-op",
	}
	bs, err := json.Marshal(req)
	require.NoError(t, err)
	w = httptest.NewRecorder()
	r = httptest.NewRequest("POST", baseURL+"binlog/tasks/"+"task1", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)
	equalError(t, "unsupport op type '' for set binlog operator", w.Body)

	pos := "mysql-bin.000001,4"
	sqls := []string{"ALTER TABLE tb ADD COLUMN c int(11) UNIQUE;"}
	req = openapi.SetBinlogOperatorRequest{
		BinlogPos: &pos,
		Op:        openapi.SetBinlogOperatorRequestOpReplace,
		Sqls:      &sqls,
	}
	bs, err = json.Marshal(req)
	require.NoError(t, err)
	messageAgent.On("SendRequest").Return(&dmpkg.CommonTaskResponse{
		Msg: "binlog operator set success",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("POST", baseURL+"binlog/tasks/"+"task1", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	var binlogResp dmpkg.BinlogResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &binlogResp))
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Equal(t, "binlog operator set success", binlogResp.Results["task1"].Msg)

	req = openapi.SetBinlogOperatorRequest{
		Op: openapi.SetBinlogOperatorRequestOpSkip,
	}
	bs, err = json.Marshal(req)
	require.NoError(t, err)
	messageAgent.On("SendRequest").Return(&dmpkg.CommonTaskResponse{
		ErrorMsg: "no binlog error",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("POST", baseURL+"binlog/tasks/"+"task1", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &binlogResp))
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Equal(t, "no binlog error", binlogResp.Results["task1"].ErrorMsg)

	req = openapi.SetBinlogOperatorRequest{
		Op: openapi.SetBinlogOperatorRequestOpInject,
	}
	bs, err = json.Marshal(req)
	require.NoError(t, err)
	messageAgent.On("SendRequest").Return(&dmpkg.CommonTaskResponse{
		ErrorMsg: "no binlog error",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("POST", baseURL+"binlog/tasks/"+"task1", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &binlogResp))
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Equal(t, "no binlog error", binlogResp.Results["task1"].ErrorMsg)
}

func testDMAPIDeleteBinlogOperator(t *testing.T, engine *gin.Engine, jm *JobMaster, messageAgent *dmpkg.MockMessageAgent) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("DELETE", "/api/v1/jobs/job-not-exist/binlog", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusNotFound, w.Code)

	messageAgent.On("SendRequest").Return(nil, errors.New("binlog operator not found")).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("DELETE", baseURL+"binlog/tasks/"+"task1"+"?binlog_pos='mysql-bin.000001,4'", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	var binlogResp dmpkg.BinlogResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &binlogResp))
	require.Equal(t, "", binlogResp.ErrorMsg)
	require.Equal(t, "binlog operator not found", binlogResp.Results["task1"].ErrorMsg)
}

func testDMAPIGetSchema(t *testing.T, engine *gin.Engine, jm *JobMaster, messageAgent *dmpkg.MockMessageAgent) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/v1/jobs/job-not-exist/schema", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusNotFound, w.Code)

	messageAgent.On("SendRequest").Return(&dmpkg.CommonTaskResponse{
		Msg: "targets",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"schema/tasks/"+"task1"+"?target=true", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	var binlogSchemaResp dmpkg.BinlogSchemaResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &binlogSchemaResp))
	require.Equal(t, "", binlogSchemaResp.ErrorMsg)
	require.Equal(t, "targets", binlogSchemaResp.Results["task1"].Msg)

	messageAgent.On("SendRequest").Return(&dmpkg.CommonTaskResponse{
		Msg: "tables",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"schema/tasks/"+"task1"+"?database='db'", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &binlogSchemaResp))
	require.Equal(t, "", binlogSchemaResp.ErrorMsg)
	require.Equal(t, "tables", binlogSchemaResp.Results["task1"].Msg)

	messageAgent.On("SendRequest").Return(&dmpkg.CommonTaskResponse{
		Msg: "table",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"schema/tasks/"+"task1"+"?database='db'&table='tb'", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &binlogSchemaResp))
	require.Equal(t, "", binlogSchemaResp.ErrorMsg)
	require.Equal(t, "table", binlogSchemaResp.Results["task1"].Msg)

	messageAgent.On("SendRequest").Return(&dmpkg.CommonTaskResponse{
		Msg: "databases",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"schema/tasks/"+"task1", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &binlogSchemaResp))
	require.Equal(t, "", binlogSchemaResp.ErrorMsg)
	require.Equal(t, "databases", binlogSchemaResp.Results["task1"].Msg)

	messageAgent.On("SendRequest").Return(&dmpkg.CommonTaskResponse{
		Msg: "databases",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"schema/tasks/"+"task1"+"?table='tb'", nil)
	engine.ServeHTTP(w, r)
	require.Equal(t, http.StatusBadRequest, w.Code)
	equalError(t, "invalid query parameter for get schema", w.Body)
}
