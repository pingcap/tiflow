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
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/dm/checker"
	dmconfig "github.com/pingcap/tiflow/dm/config"
	dmmaster "github.com/pingcap/tiflow/dm/master"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/openapi"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/meta/mock"
	engineOpenAPI "github.com/pingcap/tiflow/engine/pkg/openapi"
	"github.com/pingcap/tiflow/engine/pkg/promutil"
	"github.com/pingcap/tiflow/pkg/errors"
	tmock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	baseURL = "/api/v1/jobs/job-id/"
)

func TestDMOpenAPISuite(t *testing.T) {
	suite.Run(t, new(testDMOpenAPISuite))
}

type testDMOpenAPISuite struct {
	suite.Suite
	jm              *JobMaster
	engine          *gin.Engine
	messageAgent    *dmpkg.MockMessageAgent
	checkpointAnget *MockCheckpointAgent
	funcBackup      func(ctx context.Context, cfg *dmconfig.SourceConfig) error
}

func (t *testDMOpenAPISuite) SetupSuite() {
	var (
		mockBaseJobmaster   = &MockBaseJobmaster{t: t.T()}
		mockMessageAgent    = &dmpkg.MockMessageAgent{}
		mockCheckpointAgent = &MockCheckpointAgent{}
		jm                  = &JobMaster{
			BaseJobMaster:   mockBaseJobmaster,
			metadata:        metadata.NewMetaData(mock.NewMetaMock(), log.L()),
			messageAgent:    mockMessageAgent,
			checkpointAgent: mockCheckpointAgent,
		}
	)
	jm.taskManager = NewTaskManager("test-job", nil, jm.metadata.JobStore(), jm.messageAgent, jm.Logger(), promutil.NewFactory4Test(t.T().TempDir()))
	jm.workerManager = NewWorkerManager(mockBaseJobmaster.ID(), nil, jm.metadata.JobStore(), jm.metadata.UnitStateStore(), nil, jm.messageAgent, nil, jm.Logger(),
		resModel.ResourceTypeLocalFile)
	jm.initialized.Store(true)

	engine := gin.New()
	apiGroup := engine.Group(baseURL)
	jm.initOpenAPI(apiGroup)

	t.funcBackup = dmmaster.CheckAndAdjustSourceConfigFunc
	t.jm = jm
	t.engine = engine
	t.messageAgent = mockMessageAgent
	t.checkpointAnget = mockCheckpointAgent
	dmmaster.CheckAndAdjustSourceConfigFunc = checkAndNoAdjustSourceConfigMock
}

func (t *testDMOpenAPISuite) TearDownSuite() {
	dmmaster.CheckAndAdjustSourceConfigFunc = t.funcBackup
}

func (t *testDMOpenAPISuite) TestDMAPIGetJobConfig() {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/v1/jobs/job-not-exist/config", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusNotFound, w.Code)

	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"config", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusInternalServerError, w.Code)
	equalError(t.T(), "state not found", w.Body)

	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	require.NoError(t.T(), t.jm.taskManager.OperateTask(context.Background(), dmpkg.Create, jobCfg, nil))
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"config", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)

	var cfgStr string
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &cfgStr))
	bs, err := jobCfg.Yaml()
	require.NoError(t.T(), err)
	require.Equal(t.T(), sortString(string(bs)), sortString(cfgStr))

	require.NoError(t.T(), t.jm.taskManager.OperateTask(context.Background(), dmpkg.Delete, nil, nil))
}

func (t *testDMOpenAPISuite) TestDMAPIUpdateJobCfg() {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("PUT", "/api/v1/jobs/job-not-exist/config", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusNotFound, w.Code)

	req := openapi.UpdateJobConfigRequest{}
	bs, err := json.Marshal(req)
	require.NoError(t.T(), err)
	w = httptest.NewRecorder()
	r = httptest.NewRequest("PUT", baseURL+"config", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusInternalServerError, w.Code)

	cfgBytes, err := os.ReadFile(jobTemplatePath)
	require.NoError(t.T(), err)
	req = openapi.UpdateJobConfigRequest{Config: string(cfgBytes)}
	bs, err = json.Marshal(req)
	require.NoError(t.T(), err)
	w = httptest.NewRecorder()
	r = httptest.NewRequest("PUT", baseURL+"config", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusInternalServerError, w.Code)

	require.NoError(t.T(), t.jm.taskManager.OperateTask(context.Background(), dmpkg.Create, &config.JobCfg{}, nil))
	t.checkpointAnget.On("Update").Return(nil)
	verDB := conn.InitVersionDB()
	verDB.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version'").WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version", "5.7.25-TiDB-v6.1.0"))
	checker.CheckSyncConfigFunc = func(_ context.Context, _ []*dmconfig.SubTaskConfig, _, _ int64) (string, error) {
		return "check pass", nil
	}
	w = httptest.NewRecorder()
	r = httptest.NewRequest("PUT", baseURL+"config", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)
}

func (t *testDMOpenAPISuite) TestDMAPIGetJobStatus() {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/v1/jobs/job-not-exist/status", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusNotFound, w.Code)

	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"status", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusInternalServerError, w.Code)
	equalError(t.T(), "state not found", w.Body)

	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"status"+"?tasks=task", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusInternalServerError, w.Code)
	equalError(t.T(), "state not found", w.Body)

	require.NoError(t.T(), t.jm.taskManager.OperateTask(context.Background(), dmpkg.Create, &config.JobCfg{}, nil))
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"status", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)

	jobStatus := JobStatus{
		JobID:      "dm-jobmaster-id",
		TaskStatus: map[string]TaskStatus{},
	}
	var jobStatus2 JobStatus
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &jobStatus2))
	require.Equal(t.T(), jobStatus, jobStatus2)

	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"status"+"?tasks=task1&tasks=task2", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &jobStatus2))
	require.Equal(t.T(), "task task1 for job not found", jobStatus2.TaskStatus["task1"].Status.ErrorMsg)
	require.Equal(t.T(), "task task2 for job not found", jobStatus2.TaskStatus["task2"].Status.ErrorMsg)

	require.NoError(t.T(), t.jm.taskManager.OperateTask(context.Background(), dmpkg.Delete, nil, nil))
}

func (t *testDMOpenAPISuite) TestDMAPIOperateJob() {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("PUT", "/api/v1/jobs/job-not-exist/status", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusNotFound, w.Code)

	w = httptest.NewRecorder()
	r = httptest.NewRequest("PUT", baseURL+"status", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusInternalServerError, w.Code)
	equalError(t.T(), "unsupported op type '' for operate task", w.Body)

	tasks := []string{"task"}
	req := openapi.OperateJobRequest{
		Op:    openapi.OperateJobRequestOpPause,
		Tasks: &tasks,
	}
	bs, err := json.Marshal(req)
	require.NoError(t.T(), err)
	w = httptest.NewRecorder()
	r = httptest.NewRequest("PUT", baseURL+"status", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusInternalServerError, w.Code)
	equalError(t.T(), "state not found", w.Body)

	jobCfg := &config.JobCfg{}
	require.NoError(t.T(), jobCfg.DecodeFile(jobTemplatePath))
	require.NoError(t.T(), t.jm.taskManager.OperateTask(context.Background(), dmpkg.Create, jobCfg, nil))
	req.Op = openapi.OperateJobRequestOpResume
	req.Tasks = nil
	bs, err = json.Marshal(req)
	require.NoError(t.T(), err)
	w = httptest.NewRecorder()
	r = httptest.NewRequest("PUT", baseURL+"status", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)

	require.NoError(t.T(), t.jm.taskManager.OperateTask(context.Background(), dmpkg.Delete, nil, nil))
}

func (t *testDMOpenAPISuite) TestDMAPIGetBinlogOperator() {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/v1/jobs/job-not-exist/binlog", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusNotFound, w.Code)

	t.messageAgent.On("SendRequest", tmock.Anything, tmock.Anything, tmock.Anything, tmock.Anything).Return(nil, errors.New("binlog operator not found")).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"binlog/tasks/"+"task1"+"?binlog_pos='mysql-bin.000001,4'", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)
	var binlogResp dmpkg.BinlogResponse
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &binlogResp))
	require.Equal(t.T(), "", binlogResp.ErrorMsg)
	require.Equal(t.T(), "binlog operator not found", binlogResp.Results["task1"].ErrorMsg)
}

func (t *testDMOpenAPISuite) TestDMAPISetBinlogOperator() {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/v1/jobs/job-not-exist/binlog", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusNotFound, w.Code)

	req := openapi.SetBinlogOperatorRequest{
		Op: "wrong-op",
	}
	bs, err := json.Marshal(req)
	require.NoError(t.T(), err)
	w = httptest.NewRecorder()
	r = httptest.NewRequest("POST", baseURL+"binlog/tasks/"+"task1", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusInternalServerError, w.Code)
	equalError(t.T(), "unsupported op type 'wrong-op' for set binlog operator", w.Body)

	pos := "mysql-bin.000001,4"
	sqls := []string{"ALTER TABLE tb ADD COLUMN c int(11) UNIQUE;"}
	req = openapi.SetBinlogOperatorRequest{
		BinlogPos: &pos,
		Op:        openapi.SetBinlogOperatorRequestOpReplace,
		Sqls:      &sqls,
	}
	bs, err = json.Marshal(req)
	require.NoError(t.T(), err)
	t.messageAgent.On("SendRequest", tmock.Anything, tmock.Anything, tmock.Anything, tmock.Anything).Return(&dmpkg.CommonTaskResponse{
		Msg: "binlog operator set success",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("POST", baseURL+"binlog/tasks/"+"task1", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusCreated, w.Code)
	var binlogResp dmpkg.BinlogResponse
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &binlogResp))
	require.Equal(t.T(), "", binlogResp.ErrorMsg)
	require.Equal(t.T(), "binlog operator set success", binlogResp.Results["task1"].Msg)

	req = openapi.SetBinlogOperatorRequest{
		Op: openapi.SetBinlogOperatorRequestOpSkip,
	}
	bs, err = json.Marshal(req)
	require.NoError(t.T(), err)
	t.messageAgent.On("SendRequest", tmock.Anything, tmock.Anything, tmock.Anything, tmock.Anything).Return(&dmpkg.CommonTaskResponse{
		ErrorMsg: "no binlog error",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("POST", baseURL+"binlog/tasks/"+"task1", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusCreated, w.Code)
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &binlogResp))
	require.Equal(t.T(), "", binlogResp.ErrorMsg)
	require.Equal(t.T(), "no binlog error", binlogResp.Results["task1"].ErrorMsg)

	req = openapi.SetBinlogOperatorRequest{
		Op: openapi.SetBinlogOperatorRequestOpInject,
	}
	bs, err = json.Marshal(req)
	require.NoError(t.T(), err)
	t.messageAgent.On("SendRequest", tmock.Anything, tmock.Anything, tmock.Anything, tmock.Anything).Return(&dmpkg.CommonTaskResponse{
		ErrorMsg: "no binlog error",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("POST", baseURL+"binlog/tasks/"+"task1", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusCreated, w.Code)
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &binlogResp))
	require.Equal(t.T(), "", binlogResp.ErrorMsg)
	require.Equal(t.T(), "no binlog error", binlogResp.Results["task1"].ErrorMsg)
}

func (t *testDMOpenAPISuite) TestDMAPIDeleteBinlogOperator() {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("DELETE", "/api/v1/jobs/job-not-exist/binlog", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusNotFound, w.Code)

	t.messageAgent.On("SendRequest", tmock.Anything, tmock.Anything, tmock.Anything, tmock.Anything).Return(nil, errors.New("binlog operator not found")).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("DELETE", baseURL+"binlog/tasks/"+"task1"+"?binlog_pos='mysql-bin.000001,4'", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)
	var binlogResp dmpkg.BinlogResponse
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &binlogResp))
	require.Equal(t.T(), "", binlogResp.ErrorMsg)
	require.Equal(t.T(), "binlog operator not found", binlogResp.Results["task1"].ErrorMsg)

	t.messageAgent.On("SendRequest", tmock.Anything, tmock.Anything, tmock.Anything, tmock.Anything).Return(&dmpkg.CommonTaskResponse{}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("DELETE", baseURL+"binlog/tasks/"+"task1"+"?binlog_pos='mysql-bin.000001,4'", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusNoContent, w.Code)
}

func (t *testDMOpenAPISuite) TestDMAPIGetSchema() {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/v1/jobs/job-not-exist/schema", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusNotFound, w.Code)

	t.messageAgent.On("SendRequest", tmock.Anything, tmock.Anything, tmock.Anything, tmock.Anything).Return(&dmpkg.CommonTaskResponse{
		Msg: "targets",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"schema/tasks/"+"task1"+"?target=true", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)
	var binlogSchemaResp dmpkg.BinlogSchemaResponse
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &binlogSchemaResp))
	require.Equal(t.T(), "", binlogSchemaResp.ErrorMsg)
	require.Equal(t.T(), "targets", binlogSchemaResp.Results["task1"].Msg)

	t.messageAgent.On("SendRequest", tmock.Anything, tmock.Anything, tmock.Anything, tmock.Anything).Return(&dmpkg.CommonTaskResponse{
		Msg: "tables",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"schema/tasks/"+"task1"+"?database='db'", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &binlogSchemaResp))
	require.Equal(t.T(), "", binlogSchemaResp.ErrorMsg)
	require.Equal(t.T(), "tables", binlogSchemaResp.Results["task1"].Msg)

	t.messageAgent.On("SendRequest", tmock.Anything, tmock.Anything, tmock.Anything, tmock.Anything).Return(&dmpkg.CommonTaskResponse{
		Msg: "table",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"schema/tasks/"+"task1"+"?database='db'&table='tb'", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &binlogSchemaResp))
	require.Equal(t.T(), "", binlogSchemaResp.ErrorMsg)
	require.Equal(t.T(), "table", binlogSchemaResp.Results["task1"].Msg)

	t.messageAgent.On("SendRequest", tmock.Anything, tmock.Anything, tmock.Anything, tmock.Anything).Return(&dmpkg.CommonTaskResponse{
		Msg: "databases",
	}, nil).Once()
	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"schema/tasks/"+"task1", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &binlogSchemaResp))
	require.Equal(t.T(), "", binlogSchemaResp.ErrorMsg)
	require.Equal(t.T(), "databases", binlogSchemaResp.Results["task1"].Msg)

	w = httptest.NewRecorder()
	r = httptest.NewRequest("GET", baseURL+"schema/tasks/"+"task1"+"?table='tb'", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusInternalServerError, w.Code)
	equalError(t.T(), "invalid query params for get schema", w.Body)
}

func (t *testDMOpenAPISuite) TestDMAPISetSchema() {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("PUT", "/api/v1/jobs/job-not-exist/schema", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusNotFound, w.Code)

	t.messageAgent.On("SendRequest", tmock.Anything, tmock.Anything, tmock.Anything, tmock.Anything).Return(nil, errors.New("task not paused")).Once()
	from := true
	req := openapi.SetBinlogSchemaRequest{Database: "db", Table: "tb", FromSource: &from}
	bs, err := json.Marshal(req)
	require.NoError(t.T(), err)
	w = httptest.NewRecorder()
	r = httptest.NewRequest("PUT", baseURL+"schema/tasks/"+"task1", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)
	var binlogSchemaResp dmpkg.BinlogSchemaResponse
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &binlogSchemaResp))
	require.Equal(t.T(), "", binlogSchemaResp.ErrorMsg)
	require.Equal(t.T(), "task not paused", binlogSchemaResp.Results["task1"].ErrorMsg)

	t.messageAgent.On("SendRequest", tmock.Anything, tmock.Anything, tmock.Anything, tmock.Anything).Return(&dmpkg.CommonTaskResponse{
		Msg: "success",
	}, nil).Once()
	req = openapi.SetBinlogSchemaRequest{Database: "db", Table: "tb", FromTarget: &from}
	bs, err = json.Marshal(req)
	require.NoError(t.T(), err)
	w = httptest.NewRecorder()
	r = httptest.NewRequest("PUT", baseURL+"schema/tasks/"+"task1", bytes.NewReader(bs))
	r.Header.Set("Content-Type", "application/json")
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), http.StatusOK, w.Code)
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &binlogSchemaResp))
	require.Equal(t.T(), "", binlogSchemaResp.ErrorMsg)
	require.Equal(t.T(), "", binlogSchemaResp.Results["task1"].ErrorMsg)
	require.Equal(t.T(), "success", binlogSchemaResp.Results["task1"].Msg)
}

func (t *testDMOpenAPISuite) TestJobMasterNotInitialized() {
	t.jm.initialized.Store(false)
	defer t.jm.initialized.Store(true)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", baseURL+"config", nil)
	t.engine.ServeHTTP(w, r)
	require.Equal(t.T(), errors.HTTPStatusCode(errors.ErrJobNotRunning), w.Code)
	var httpErr engineOpenAPI.HTTPError
	require.NoError(t.T(), json.Unmarshal(w.Body.Bytes(), &httpErr))
	require.Equal(t.T(), string(errors.ErrJobNotRunning.RFCCode()), httpErr.Code)
}

func equalError(t *testing.T, expected string, body *bytes.Buffer) {
	var httpErr engineOpenAPI.HTTPError
	json.Unmarshal(body.Bytes(), &httpErr)
	require.Equal(t, expected, httpErr.Message)
}

func TestHTTPErrorHandler(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		err     error
		code    string
		message string
	}{
		{
			errors.New("unknown error"),
			string(errors.ErrUnknown.RFCCode()),
			"unknown error",
		},
		{
			errors.ErrDeserializeConfig.GenWithStackByArgs(),
			string(errors.ErrDeserializeConfig.RFCCode()),
			errors.ErrDeserializeConfig.GetMsg(),
		},
		{
			terror.ErrDBBadConn.Generate(),
			"DM:ErrDBBadConn",
			terror.ErrDBBadConn.Generate().Error(),
		},
		{
			terror.ErrDBInvalidConn.Generate(),
			"DM:ErrDBInvalidConn",
			terror.ErrDBInvalidConn.Generate().Error(),
		},
	}

	for _, tc := range testCases {
		engine := gin.New()
		engine.Use(httpErrorHandler())
		engine.GET("/test", func(c *gin.Context) {
			c.Error(tc.err)
		})

		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/test", nil)
		engine.ServeHTTP(w, r)
		require.Equal(t, errors.HTTPStatusCode(tc.err), w.Code)

		var httpErr engineOpenAPI.HTTPError
		err := json.Unmarshal(w.Body.Bytes(), &httpErr)
		require.NoError(t, err)
		require.Equal(t, tc.code, httpErr.Code)
		require.Equal(t, tc.message, httpErr.Message)
	}
}
