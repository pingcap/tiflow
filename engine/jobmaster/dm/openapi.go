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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/openapi"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	engineOpenAPI "github.com/pingcap/tiflow/engine/pkg/openapi"
	"github.com/pingcap/tiflow/pkg/errors"
)

// errCodePrefix is the prefix that attach to terror's error code string.
// e.g. codeDBBadConn -> DM:ErrDBBadConn, codeDBInvalidConn -> DM:ErrDBInvalidConn
// This is used to distinguish with other components' error code, such as DFLOW and CDC.
// TODO: replace all terror usage with pkg/errors, need further discussion.
const errCodePrefix = "DM:Err"

func (jm *JobMaster) initOpenAPI(router *gin.RouterGroup) {
	router.Use(httpErrorHandler())

	router.Use(func(c *gin.Context) {
		if !jm.initialized.Load() {
			_ = c.Error(errors.ErrJobNotRunning.GenWithStackByArgs(jm.ID()))
			c.Abort()
			return
		}
	})

	wrapper := openapi.ServerInterfaceWrapper{
		Handler: jm,
	}
	// copy from openapi.RegisterHandlersWithOptions
	router.DELETE("/binlog/tasks/:task-name", wrapper.DMAPIDeleteBinlogOperator)

	router.GET("/binlog/tasks/:task-name", wrapper.DMAPIGetBinlogOperator)

	router.POST("/binlog/tasks/:task-name", wrapper.DMAPISetBinlogOperator)

	router.GET("/config", wrapper.DMAPIGetJobConfig)

	router.PUT("/config", wrapper.DMAPIUpdateJobConfig)

	router.GET("/schema/tasks/:task-name", wrapper.DMAPIGetSchema)

	router.PUT("/schema/tasks/:task-name", wrapper.DMAPISetSchema)

	router.GET("/status", wrapper.DMAPIGetJobStatus)

	router.PUT("/status", wrapper.DMAPIOperateJob)
}

// DMAPIGetJobStatus implements the api of get job status.
func (jm *JobMaster) DMAPIGetJobStatus(c *gin.Context, params openapi.DMAPIGetJobStatusParams) {
	var tasks []string
	if params.Tasks != nil {
		tasks = *params.Tasks
	}
	resp, err := jm.QueryJobStatus(c.Request.Context(), tasks)
	if err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, resp)
}

// TODO: extract terror from worker response
func httpErrorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		gErr := c.Errors.Last()
		if gErr == nil {
			return
		}

		// Adapt to the error format of engine openapi.
		var httpErr *engineOpenAPI.HTTPError
		var tErr *terror.Error
		if errors.As(gErr.Err, &tErr) {
			code := errCodePrefix + tErr.Code().String()
			message := tErr.Error()
			httpErr = &engineOpenAPI.HTTPError{
				Code:    code,
				Message: message,
			}
		} else {
			httpErr = engineOpenAPI.NewHTTPError(gErr.Err)
		}
		c.JSON(errors.HTTPStatusCode(gErr.Err), httpErr)
	}
}

// DMAPIGetJobConfig implements the api of get job config.
func (jm *JobMaster) DMAPIGetJobConfig(c *gin.Context) {
	cfg, err := jm.GetJobCfg(c.Request.Context())
	if err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}
	bs, err := cfg.Yaml()
	if err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, string(bs))
}

// DMAPIUpdateJobConfig implements the api of update job config.
func (jm *JobMaster) DMAPIUpdateJobConfig(c *gin.Context) {
	var req openapi.UpdateJobConfigRequest
	if err := c.Bind(&req); err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}

	var jobCfg config.JobCfg
	if err := jobCfg.Decode([]byte(req.Config)); err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}

	if err := jm.UpdateJobCfg(c.Request.Context(), &jobCfg); err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusOK)
}

// DMAPIOperateJob implements the api of operate job.
func (jm *JobMaster) DMAPIOperateJob(c *gin.Context) {
	var req openapi.OperateJobRequest
	if err := c.Bind(&req); err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}

	var op dmpkg.OperateType
	switch req.Op {
	case openapi.OperateJobRequestOpPause:
		op = dmpkg.Pause
	case openapi.OperateJobRequestOpResume:
		op = dmpkg.Resume
	default:
		// nolint:errcheck
		_ = c.Error(errors.Errorf("unsupported op type '%s' for operate task", req.Op))
		return
	}

	var tasks []string
	if req.Tasks != nil {
		tasks = *req.Tasks
	}
	err := jm.operateTask(c.Request.Context(), op, nil, tasks)
	if err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusOK)
}

// DMAPIGetBinlogOperator implements the api of get binlog operator.
// TODO: pagination support if needed
func (jm *JobMaster) DMAPIGetBinlogOperator(c *gin.Context, taskName string, params openapi.DMAPIGetBinlogOperatorParams) {
	req := &dmpkg.BinlogRequest{
		Op:      pb.ErrorOp_List,
		Sources: []string{taskName},
	}
	if params.BinlogPos != nil {
		req.BinlogPos = *params.BinlogPos
	}
	resp, err := jm.Binlog(c.Request.Context(), req)
	if err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPISetBinlogOperator implements the api of set binlog operator.
func (jm *JobMaster) DMAPISetBinlogOperator(c *gin.Context, taskName string) {
	var req openapi.SetBinlogOperatorRequest
	if err := c.Bind(&req); err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}

	r := &dmpkg.BinlogRequest{
		Sources: []string{taskName},
	}
	if req.BinlogPos != nil {
		r.BinlogPos = *req.BinlogPos
	}
	if req.Sqls != nil {
		r.Sqls = *req.Sqls
	}
	switch req.Op {
	case openapi.SetBinlogOperatorRequestOpInject:
		r.Op = pb.ErrorOp_Inject
	case openapi.SetBinlogOperatorRequestOpSkip:
		r.Op = pb.ErrorOp_Skip
	case openapi.SetBinlogOperatorRequestOpReplace:
		r.Op = pb.ErrorOp_Replace
	default:
		// nolint:errcheck
		_ = c.Error(errors.Errorf("unsupported op type '%s' for set binlog operator", req.Op))
		return
	}
	resp, err := jm.Binlog(c.Request.Context(), r)
	if err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusCreated, resp)
}

// DMAPIDeleteBinlogOperator implements the api of delete binlog operator.
func (jm *JobMaster) DMAPIDeleteBinlogOperator(c *gin.Context, taskName string, params openapi.DMAPIDeleteBinlogOperatorParams) {
	req := &dmpkg.BinlogRequest{
		Op:      pb.ErrorOp_Revert,
		Sources: []string{taskName},
	}
	if params.BinlogPos != nil {
		req.BinlogPos = *params.BinlogPos
	}
	resp, err := jm.Binlog(c.Request.Context(), req)
	if err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}

	if resp.ErrorMsg != "" {
		c.IndentedJSON(http.StatusOK, resp)
		return
	}
	for _, r := range resp.Results {
		if r.ErrorMsg != "" {
			c.IndentedJSON(http.StatusOK, resp)
			return
		}
	}
	c.Status(http.StatusNoContent)
}

// DMAPIGetSchema implements the api of get schema.
func (jm *JobMaster) DMAPIGetSchema(c *gin.Context, taskname string, params openapi.DMAPIGetSchemaParams) {
	var (
		op       pb.SchemaOp
		database string
		table    string
	)
	if params.Database != nil {
		database = *params.Database
	}
	if params.Table != nil {
		table = *params.Table
	}
	switch {
	case params.Target != nil && *params.Target:
		op = pb.SchemaOp_ListMigrateTargets
	case database != "" && table != "":
		op = pb.SchemaOp_GetSchema
	case database != "" && table == "":
		op = pb.SchemaOp_ListTable
	case database == "" && table == "":
		op = pb.SchemaOp_ListSchema
	default:
		// nolint:errcheck
		_ = c.Error(errors.New("invalid query params for get schema"))
		return
	}

	r := &dmpkg.BinlogSchemaRequest{
		Op:       op,
		Sources:  []string{taskname},
		Database: database,
		Table:    table,
	}
	resp := jm.BinlogSchema(c.Request.Context(), r)
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPISetSchema implements the api of set schema.
func (jm *JobMaster) DMAPISetSchema(c *gin.Context, taskName string) {
	var (
		req        openapi.SetBinlogSchemaRequest
		fromSource bool
		fromTarget bool
	)
	if err := c.Bind(&req); err != nil {
		// nolint:errcheck
		_ = c.Error(err)
		return
	}
	if req.FromSource != nil {
		fromSource = *req.FromSource
	}
	if req.FromTarget != nil {
		fromTarget = *req.FromTarget
	}
	r := &dmpkg.BinlogSchemaRequest{
		Op:         pb.SchemaOp_SetSchema,
		Sources:    []string{taskName},
		Database:   req.Database,
		Table:      req.Table,
		Schema:     req.Sql,
		FromSource: fromSource,
		FromTarget: fromTarget,
	}
	resp := jm.BinlogSchema(c.Request.Context(), r)
	c.IndentedJSON(http.StatusOK, resp)
}
