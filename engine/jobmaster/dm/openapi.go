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
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/openapi"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
)

func (jm *JobMaster) initOpenAPI(router *gin.RouterGroup) {
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

	router.GET("/status/tasks/:task-name", wrapper.DMAPIGetTaskStatus)

	router.PUT("/status/tasks/:task-name", wrapper.DMAPIOperateTask)
}

// DMAPIGetJobStatus implements the api of get job status.
func (jm *JobMaster) DMAPIGetJobStatus(c *gin.Context) {
	resp, err := jm.QueryJobStatus(c.Request.Context(), nil)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPIGetTaskStatus implements the api of get task status.
func (jm *JobMaster) DMAPIGetTaskStatus(c *gin.Context, taskName string) {
	resp, err := jm.QueryJobStatus(c.Request.Context(), []string{taskName})
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPIGetJobConfig implements the api of get job config.
func (jm *JobMaster) DMAPIGetJobConfig(c *gin.Context) {
	cfg, err := jm.GetJobCfg(c.Request.Context())
	if err != nil {
		_ = c.Error(err)
		return
	}
	bs, err := cfg.Yaml()
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, string(bs))
}

// DMAPIUpdateJobConfig implements the api of update job config.
func (jm *JobMaster) DMAPIUpdateJobConfig(c *gin.Context) {
	// TODO: support update job config
}

// DMAPIOperateTask implements the api of operate task.
func (jm *JobMaster) DMAPIOperateTask(c *gin.Context, taskName string) {
	var req openapi.OperateTaskRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}

	var op dmpkg.OperateType
	switch req.Op {
	case openapi.OperateTaskRequestOpPause:
		op = dmpkg.Pause
	case openapi.OperateTaskRequestOpResume:
		op = dmpkg.Resume
	default:
		_ = c.Error(errors.Errorf("unsupport op type '%s' for operate task", req.Op))
		return
	}

	err := jm.OperateTask(c.Request.Context(), op, nil, []string{taskName})
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusOK)
}

// DMAPIGetBinlogOperator implements the api of get binlog operator.
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
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPISetBinlogOperator implements the api of set binlog operator.
func (jm *JobMaster) DMAPISetBinlogOperator(c *gin.Context, taskName string) {
	var req openapi.SetBinlogOperatorRequest
	if err := c.Bind(&req); err != nil {
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
		_ = c.Error(errors.New("unsupport op type '' for set binlog operator"))
		return
	}
	resp, err := jm.Binlog(c.Request.Context(), r)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, resp)
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
		_ = c.Error(err)
		return
	}
	// TODO: use no content response, now return content because delete may failed.
	c.IndentedJSON(http.StatusOK, resp)
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
