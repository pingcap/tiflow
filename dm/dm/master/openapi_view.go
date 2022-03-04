// Copyright 2021 PingCAP, Inc.
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

// this file implement all of the APIs of the DataMigration service.

package master

import (
	"encoding/json"
	"fmt"
	"net/http"

	ginmiddleware "github.com/deepmap/oapi-codegen/pkg/gin-middleware"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

const (
	docJSONBasePath = "/api/v1/dm.json"
)

// redirectRequestToLeaderMW a middleware auto redirect request to leader.
// because the leader has some data in memory, only the leader can process the request.
func (s *Server) redirectRequestToLeaderMW() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx2 := c.Request.Context()
		isLeader, _ := s.isLeaderAndNeedForward(ctx2)
		if isLeader {
			c.Next()
		} else {
			// nolint:dogsled
			_, _, leaderOpenAPIAddr, err := s.election.LeaderInfo(ctx2)
			if err != nil {
				_ = c.AbortWithError(http.StatusBadRequest, err)
				return
			}
			c.Redirect(http.StatusTemporaryRedirect, fmt.Sprintf("http://%s%s", leaderOpenAPIAddr, c.Request.RequestURI))
			c.AbortWithStatus(http.StatusTemporaryRedirect)
		}
	}
}

// InitOpenAPIHandles init openapi handlers.
func (s *Server) InitOpenAPIHandles() error {
	swagger, err := openapi.GetSwagger()
	if err != nil {
		return err
	}
	// disables swagger server name validation. it seems to work poorly
	swagger.Servers = nil
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	// middlewares
	r.Use(gin.Recovery())
	r.Use(openapi.ZapLogger(log.L().WithFields(zap.String("component", "openapi")).Logger))
	r.Use(s.redirectRequestToLeaderMW())
	r.Use(terrorHTTPErrorHandler())
	// use validation middleware to check all requests against the OpenAPI schema.
	r.Use(ginmiddleware.OapiRequestValidator(swagger))
	// register handlers
	openapi.RegisterHandlers(r, s)
	s.openapiHandles = r
	return nil
}

// GetDocJSON url is:(GET /api/v1/dm.json).
func (s *Server) GetDocJSON(c *gin.Context) {
	swagger, err := openapi.GetSwagger()
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, swagger)
}

// GetDocHTML url is:(GET /api/v1/docs).
func (s *Server) GetDocHTML(c *gin.Context) {
	html, err := openapi.GetSwaggerHTML(openapi.NewSwaggerConfig(docJSONBasePath, ""))
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.Writer.WriteHeader(http.StatusOK)
	_, err = c.Writer.Write([]byte(html))
	if err != nil {
		_ = c.Error(err)
	}
}

// DMAPIGetClusterMasterList get cluster master node list url is:(GET /api/v1/cluster/masters).
func (s *Server) DMAPIGetClusterMasterList(c *gin.Context) {
	newCtx := c.Request.Context()
	memberMasters, err := s.listMemberMaster(newCtx, nil)
	if err != nil {
		_ = c.Error(err)
		return
	}
	masterCnt := len(memberMasters.Master.Masters)
	masters := make([]openapi.ClusterMaster, masterCnt)
	for idx, master := range memberMasters.Master.Masters {
		masters[idx] = openapi.ClusterMaster{
			Name:   master.GetName(),
			Alive:  master.GetAlive(),
			Addr:   master.GetPeerURLs()[0],
			Leader: master.GetName() == s.cfg.Name, // only leader can handle request
		}
	}
	resp := &openapi.GetClusterMasterListResponse{Total: masterCnt, Data: masters}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPIOfflineMasterNode offline master node url is: (DELETE /api/v1/cluster/masters/{master-name}).
func (s *Server) DMAPIOfflineMasterNode(c *gin.Context, masterName string) {
	newCtx := c.Request.Context()
	if err := s.deleteMasterByName(newCtx, masterName); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusNoContent)
}

// DMAPIGetClusterWorkerList get cluster worker node list url is: (GET /api/v1/cluster/workers).
func (s *Server) DMAPIGetClusterWorkerList(c *gin.Context) {
	memberWorkers := s.listMemberWorker(nil)
	workerCnt := len(memberWorkers.Worker.Workers)
	workers := make([]openapi.ClusterWorker, workerCnt)
	for idx, worker := range memberWorkers.Worker.Workers {
		workers[idx] = openapi.ClusterWorker{
			Name:            worker.GetName(),
			Addr:            worker.GetAddr(),
			BoundSourceName: worker.GetSource(),
			BoundStage:      worker.GetStage(),
		}
	}
	resp := &openapi.GetClusterWorkerListResponse{Total: workerCnt, Data: workers}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPIOfflineWorkerNode offline worker node url is: (DELETE /api/v1/cluster/workers/{worker-name}).
func (s *Server) DMAPIOfflineWorkerNode(c *gin.Context, workerName string) {
	if err := s.scheduler.RemoveWorker(workerName); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusNoContent)
}

// DMAPICreateSource url is:(POST /api/v1/sources).
func (s *Server) DMAPICreateSource(c *gin.Context) {
	var createSourceReq openapi.Source
	if err := c.Bind(&createSourceReq); err != nil {
		_ = c.Error(err)
		return
	}
	cfg := config.OpenAPISourceToSourceCfg(createSourceReq)

	ctx := c.Request.Context()
	if err := checkAndAdjustSourceConfigFunc(ctx, cfg); err != nil {
		_ = c.Error(err)
		return
	}
	// TODO support specify worker name
	if err := s.createSource(ctx, cfg); err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusCreated, createSourceReq)
}

// DMAPIGetSourceList url is:(GET /api/v1/sources).
func (s *Server) DMAPIGetSourceList(c *gin.Context, params openapi.DMAPIGetSourceListParams) {
	ctx := c.Request.Context()
	// todo support filter
	sourceList := s.listSource(ctx, nil)
	// fill status
	if params.WithStatus != nil && *params.WithStatus {
		for idx := range sourceList {
			sourceStatusList, err := s.getSourceStatus(ctx, sourceList[idx].SourceName)
			if err != nil {
				_ = c.Error(err)
				return
			}
			sourceList[idx].StatusList = &sourceStatusList
		}
	}
	resp := openapi.GetSourceListResponse{Total: len(sourceList), Data: sourceList}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPIDeleteSource url is:(DELETE /api/v1/sources).
func (s *Server) DMAPIDeleteSource(c *gin.Context, sourceName string, params openapi.DMAPIDeleteSourceParams) {
	ctx := c.Request.Context()
	var force bool
	// force means delete source and stop all task of this source
	if params.Force != nil && *params.Force {
		force = *params.Force
	}
	if err := s.deleteSource(ctx, sourceName, force); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusNoContent)
}

// DMAPIStartRelay url is:(POST /api/v1/sources/{source-id}/relay).
func (s *Server) DMAPIStartRelay(c *gin.Context, sourceName string) {
	var req openapi.StartRelayRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	sourceCfg := s.scheduler.GetSourceCfgByID(sourceName)
	if sourceCfg == nil {
		_ = c.Error(terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName))
		return
	}
	needUpdate := false
	// update relay related in source cfg
	if req.RelayBinlogName != nil && sourceCfg.RelayBinLogName != *req.RelayBinlogName {
		sourceCfg.RelayBinLogName = *req.RelayBinlogName
		needUpdate = true
	}
	if req.RelayBinlogGtid != nil && sourceCfg.RelayBinlogGTID != *req.RelayBinlogGtid {
		sourceCfg.RelayBinlogGTID = *req.RelayBinlogGtid
		needUpdate = true
	}
	if req.RelayDir != nil && sourceCfg.RelayDir != *req.RelayDir {
		sourceCfg.RelayDir = *req.RelayDir
		needUpdate = true
	}
	if needUpdate {
		// update current source relay config before start relay
		if err := s.scheduler.UpdateSourceCfg(sourceCfg); err != nil {
			_ = c.Error(err)
			return
		}
	}
	if err := s.scheduler.StartRelay(sourceName, req.WorkerNameList); err != nil {
		_ = c.Error(err)
	}
}

// DMAPIStopRelay url is:(DELETE /api/v1/sources/{source-id}/relay).
func (s *Server) DMAPIStopRelay(c *gin.Context, sourceName string) {
	var req openapi.StopRelayRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	if err := s.scheduler.StopRelay(sourceName, req.WorkerNameList); err != nil {
		_ = c.Error(err)
	}
}

// DMAPIPauseRelay pause relay log function for the data source url is: (POST /api/v1/sources/{source-name}/pause-relay).
func (s *Server) DMAPIPauseRelay(c *gin.Context, sourceName string) {
	if err := s.scheduler.UpdateExpectRelayStage(pb.Stage_Paused, sourceName); err != nil {
		_ = c.Error(err)
	}
}

// DMAPIResumeRelay resume relay log function for the data source url is: (POST /api/v1/sources/{source-name}/resume-relay).
func (s *Server) DMAPIResumeRelay(c *gin.Context, sourceName string) {
	if err := s.scheduler.UpdateExpectRelayStage(pb.Stage_Running, sourceName); err != nil {
		_ = c.Error(err)
	}
}

func (s *Server) getBaseDBBySourceName(sourceName string) (*conn.BaseDB, error) {
	sourceCfg := s.scheduler.GetSourceCfgByID(sourceName)
	if sourceCfg == nil {
		return nil, terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	dbCfg := sourceCfg.GenerateDBConfig()
	return conn.DefaultDBProvider.Apply(dbCfg)
}

// DMAPIGetSourceSchemaList get source schema list url is: (GET /api/v1/sources/{source-name}/schemas).
func (s *Server) DMAPIGetSourceSchemaList(c *gin.Context, sourceName string) {
	baseDB, err := s.getBaseDBBySourceName(sourceName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	defer baseDB.Close()
	schemaList, err := utils.GetSchemaList(c.Request.Context(), baseDB.DB)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, schemaList)
}

// DMAPIGetSourceTableList get source table list url is: (GET /api/v1/sources/{source-name}/schemas/{schema-name}).
func (s *Server) DMAPIGetSourceTableList(c *gin.Context, sourceName string, schemaName string) {
	baseDB, err := s.getBaseDBBySourceName(sourceName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	defer baseDB.Close()
	tableList, err := utils.GetTableList(c.Request.Context(), baseDB.DB, schemaName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, tableList)
}

// DMAPIGetSourceStatus url is: (GET /api/v1/sources/{source-id}/status).
func (s *Server) DMAPIGetSourceStatus(c *gin.Context, sourceName string) {
	sourceCfg := s.scheduler.GetSourceCfgByID(sourceName)
	if sourceCfg == nil {
		_ = c.Error(terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName))
		return
	}
	var resp openapi.GetSourceStatusResponse
	worker := s.scheduler.GetWorkerBySource(sourceName)
	// current this source not bound to any worker
	if worker == nil {
		resp.Data = append(resp.Data, openapi.SourceStatus{SourceName: sourceName})
		resp.Total = len(resp.Data)
		c.IndentedJSON(http.StatusOK, resp)
		return
	}
	sourceStatusList, err := s.getSourceStatus(c.Request.Context(), sourceName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	resp.Data = append(resp.Data, sourceStatusList...)
	resp.Total = len(resp.Data)
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPITransferSource transfer source  another free worker url is: (POST /api/v1/sources/{source-name}/transfer).
func (s *Server) DMAPITransferSource(c *gin.Context, sourceName string) {
	var req openapi.WorkerNameRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	if err := s.transferSource(c.Request.Context(), sourceName, req.WorkerName); err != nil {
		_ = c.Error(err)
	}
}

// DMAPIStartTask url is:(POST /api/v1/tasks).
func (s *Server) DMAPIStartTask(c *gin.Context) {
	var req openapi.CreateTaskRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	task := &req.Task
	if err := task.Adjust(); err != nil {
		_ = c.Error(err)
		return
	}
	// prepare target db config
	newCtx := c.Request.Context()
	toDBCfg := config.GetTargetDBCfgFromOpenAPITask(task)
	if adjustDBErr := adjustTargetDB(newCtx, toDBCfg); adjustDBErr != nil {
		_ = c.Error(terror.WithClass(adjustDBErr, terror.ClassDMMaster))
		return
	}
	// prepare source db config source name -> source config
	sourceCfgMap := make(map[string]*config.SourceConfig)
	for _, cfg := range task.SourceConfig.SourceConf {
		if sourceCfg := s.scheduler.GetSourceCfgByID(cfg.SourceName); sourceCfg != nil {
			sourceCfgMap[cfg.SourceName] = sourceCfg
		} else {
			_ = c.Error(terror.ErrOpenAPITaskSourceNotFound.Generatef("source name %s", cfg.SourceName))
			return
		}
	}
	// generate sub task configs
	subTaskConfigList, err := config.OpenAPITaskToSubTaskConfigs(task, toDBCfg, sourceCfgMap)
	if err != nil {
		_ = c.Error(err)
		return
	}
	// check subtask config
	msg, err := s.checkTask(newCtx, subTaskConfigList, common.DefaultErrorCnt, common.DefaultWarnCnt)
	if err != nil {
		_ = c.Error(terror.WithClass(err, terror.ClassDMMaster))
		return
	}
	if len(msg) != 0 {
		// TODO: return warning msg with http.StatusCreated and task together
		log.L().Warn("openapi pre-check warning before start task", zap.String("warning", msg))
	}

	if createErr := s.createTask(newCtx, subTaskConfigList); createErr != nil {
		_ = c.Error(terror.WithClass(createErr, terror.ClassDMMaster))
		return
	}
	var sourceNameList []string
	// specify only start task on partial sources
	if req.SourceNameList != nil {
		sourceNameList = *req.SourceNameList
	} else {
		sourceNameList = s.getTaskSourceNameList(task.Name)
	}
	if startErr := s.startTask(newCtx, task.Name, sourceNameList, req.RemoveMeta, nil); startErr != nil {
		_ = c.Error(terror.WithClass(startErr, terror.ClassDMMaster))
		return
	}
	c.IndentedJSON(http.StatusCreated, task)
}

// DMAPIDeleteTask url is:(DELETE /api/v1/tasks).
func (s *Server) DMAPIDeleteTask(c *gin.Context, taskName string, params openapi.DMAPIDeleteTaskParams) {
	var sourceNameList []string
	if params.SourceNameList != nil {
		sourceNameList = *params.SourceNameList
	} else {
		sourceNameList = s.getTaskSourceNameList(taskName)
	}
	if len(sourceNameList) == 0 {
		_ = c.Error(terror.ErrSchedulerTaskNotExist.Generate(taskName))
		return
	}

	ctx := c.Request.Context()
	if err := s.stopTask(ctx, taskName, sourceNameList); err != nil {
		_ = c.Error(err)
		return
	}
	if err := s.deleteTask(ctx, taskName); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusNoContent)
}

// DMAPIGetTaskList url is:(GET /api/v1/tasks).
func (s *Server) DMAPIGetTaskList(c *gin.Context, params openapi.DMAPIGetTaskListParams) {
	ctx := c.Request.Context()
	taskList := s.listTask(ctx, nil)
	// fill status
	if params.WithStatus != nil && *params.WithStatus {
		// fill status for every task
		for idx := range taskList {
			subTaskStatusList, err := s.getTaskStatus(ctx, taskList[idx].Name, s.getTaskSourceNameList(taskList[idx].Name))
			if err != nil {
				_ = c.Error(err)
				return
			}
			taskList[idx].StatusList = &subTaskStatusList
		}
	}
	resp := openapi.GetTaskListResponse{Total: len(taskList), Data: taskList}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPIGetTaskStatus url is:(GET /api/v1/tasks/{task-name}/status).
func (s *Server) DMAPIGetTaskStatus(c *gin.Context, taskName string, params openapi.DMAPIGetTaskStatusParams) {
	var sourceNameList []string
	// specify only start task on partial sources
	if params.SourceNameList != nil {
		sourceNameList = *params.SourceNameList
	} else {
		sourceNameList = s.getTaskSourceNameList(taskName)
	}
	if len(sourceNameList) == 0 {
		_ = c.Error(terror.ErrSchedulerTaskNotExist.Generate(taskName))
		return
	}
	// 2. get status from workers
	subTaskStatusList, err := s.getTaskStatus(c.Request.Context(), taskName, sourceNameList)
	if err != nil {
		_ = c.Error(err)
		return
	}
	resp := openapi.GetTaskStatusResponse{Total: len(subTaskStatusList), Data: subTaskStatusList}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPIPauseTask pause task url is: (POST /api/v1/tasks/{task-name}/pause).
func (s *Server) DMAPIPauseTask(c *gin.Context, taskName string) {
	var sourceNameList openapi.SchemaNameList
	if err := c.Bind(&sourceNameList); err != nil {
		_ = c.Error(err)
		return
	}
	if len(sourceNameList) == 0 {
		sourceNameList = s.getTaskSourceNameList(taskName)
	}
	ctx := c.Request.Context()
	if err := s.stopTask(ctx, taskName, sourceNameList); err != nil {
		_ = c.Error(err)
	}
}

// DMAPIResumeTask resume task url is: (POST /api/v1/tasks/{task-name}/resume).
func (s *Server) DMAPIResumeTask(c *gin.Context, taskName string) {
	var sourceNameList openapi.SchemaNameList
	if err := c.Bind(&sourceNameList); err != nil {
		_ = c.Error(err)
		return
	}
	if len(sourceNameList) == 0 {
		sourceNameList = s.getTaskSourceNameList(taskName)
	}
	ctx := c.Request.Context()
	if err := s.startTask(ctx, taskName, sourceNameList, false, nil); err != nil {
		_ = c.Error(err)
	}
}

// DMAPIGetSchemaListByTaskAndSource get task source schema list url is: (GET /api/v1/tasks/{task-name}/sources/{source-name}/schemas).
func (s *Server) DMAPIGetSchemaListByTaskAndSource(c *gin.Context, taskName string, sourceName string) {
	worker := s.scheduler.GetWorkerBySource(sourceName)
	if worker == nil {
		_ = c.Error(terror.ErrWorkerNoStart)
		return
	}
	workerReq := workerrpc.Request{
		Type: workerrpc.CmdOperateSchema,
		OperateSchema: &pb.OperateWorkerSchemaRequest{
			Op:     pb.SchemaOp_ListSchema,
			Task:   taskName,
			Source: sourceName,
		},
	}
	newCtx := c.Request.Context()
	resp, err := worker.SendRequest(newCtx, &workerReq, s.cfg.RPCTimeout)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if !resp.OperateSchema.Result {
		_ = c.Error(terror.ErrOpenAPICommonError.New(resp.OperateSchema.Msg))
		return
	}
	schemaList := openapi.SchemaNameList{}
	if err := json.Unmarshal([]byte(resp.OperateSchema.Msg), &schemaList); err != nil {
		_ = c.Error(terror.ErrSchemaTrackerUnMarshalJSON.Delegate(err, resp.OperateSchema.Msg))
		return
	}
	c.IndentedJSON(http.StatusOK, schemaList)
}

// DMAPIGetTableListByTaskAndSource get task source table list url is: (GET /api/v1/tasks/{task-name}/sources/{source-name}/schemas/{schema-name}).
func (s *Server) DMAPIGetTableListByTaskAndSource(c *gin.Context, taskName string, sourceName string, schemaName string) {
	worker := s.scheduler.GetWorkerBySource(sourceName)
	if worker == nil {
		_ = c.Error(terror.ErrWorkerNoStart)
		return
	}
	workerReq := workerrpc.Request{
		Type: workerrpc.CmdOperateSchema,
		OperateSchema: &pb.OperateWorkerSchemaRequest{
			Op:       pb.SchemaOp_ListTable,
			Task:     taskName,
			Source:   sourceName,
			Database: schemaName,
		},
	}
	newCtx := c.Request.Context()
	resp, err := worker.SendRequest(newCtx, &workerReq, s.cfg.RPCTimeout)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if !resp.OperateSchema.Result {
		_ = c.Error(terror.ErrOpenAPICommonError.New(resp.OperateSchema.Msg))
		return
	}
	tableList := openapi.TableNameList{}
	if err := json.Unmarshal([]byte(resp.OperateSchema.Msg), &tableList); err != nil {
		_ = c.Error(terror.ErrSchemaTrackerUnMarshalJSON.Delegate(err, resp.OperateSchema.Msg))
		return
	}
	c.IndentedJSON(http.StatusOK, tableList)
}

// DMAPIGetTableStructure get task source table structure url is: (GET /api/v1/tasks/{task-name}/sources/{source-name}/schemas/{schema-name}/{table-name}).
func (s *Server) DMAPIGetTableStructure(c *gin.Context, taskName string, sourceName string, schemaName string, tableName string) {
	worker := s.scheduler.GetWorkerBySource(sourceName)
	if worker == nil {
		_ = c.Error(terror.ErrWorkerNoStart)
		return
	}
	workerReq := workerrpc.Request{
		Type: workerrpc.CmdOperateSchema,
		OperateSchema: &pb.OperateWorkerSchemaRequest{
			Op:       pb.SchemaOp_GetSchema,
			Task:     taskName,
			Source:   sourceName,
			Database: schemaName,
			Table:    tableName,
		},
	}
	newCtx := c.Request.Context()
	resp, err := worker.SendRequest(newCtx, &workerReq, s.cfg.RPCTimeout)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if !resp.OperateSchema.Result {
		_ = c.Error(terror.ErrOpenAPICommonError.New(resp.OperateSchema.Msg))
		return
	}
	taskTableStruct := openapi.GetTaskTableStructureResponse{
		SchemaCreateSql: &resp.OperateSchema.Msg,
		SchemaName:      &schemaName,
		TableName:       tableName,
	}
	c.IndentedJSON(http.StatusOK, taskTableStruct)
}

// DMAPIDeleteTableStructure delete task source table structure url is: (DELETE /api/v1/tasks/{task-name}/sources/{source-name}/schemas/{schema-name}/{table-name}).
func (s *Server) DMAPIDeleteTableStructure(c *gin.Context, taskName string, sourceName string, schemaName string, tableName string) {
	worker := s.scheduler.GetWorkerBySource(sourceName)
	if worker == nil {
		_ = c.Error(terror.ErrWorkerNoStart)
		return
	}
	workerReq := workerrpc.Request{
		Type: workerrpc.CmdOperateSchema,
		OperateSchema: &pb.OperateWorkerSchemaRequest{
			Op:       pb.SchemaOp_RemoveSchema,
			Task:     taskName,
			Source:   sourceName,
			Database: schemaName,
			Table:    tableName,
		},
	}
	newCtx := c.Request.Context()
	resp, err := worker.SendRequest(newCtx, &workerReq, s.cfg.RPCTimeout)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if !resp.OperateSchema.Result {
		_ = c.Error(terror.ErrOpenAPICommonError.New(resp.OperateSchema.Msg))
		return
	}
	c.Status(http.StatusNoContent)
}

// DMAPIOperateTableStructure operate task source table structure url is: (PUT /api/v1/tasks/{task-name}/sources/{source-name}/schemas/{schema-name}/{table-name}).
func (s *Server) DMAPIOperateTableStructure(c *gin.Context, taskName string, sourceName string, schemaName string, tableName string) {
	var req openapi.OperateTaskTableStructureRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	worker := s.scheduler.GetWorkerBySource(sourceName)
	if worker == nil {
		_ = c.Error(terror.ErrWorkerNoStart)
		return
	}
	opReq := &pb.OperateWorkerSchemaRequest{
		Op:       pb.SchemaOp_SetSchema,
		Task:     taskName,
		Source:   sourceName,
		Database: schemaName,
		Table:    tableName,
		Schema:   req.SqlContent,
		Sync:     *req.Sync,
		Flush:    *req.Flush,
	}
	if req.Sync != nil {
		opReq.Sync = *req.Sync
	}
	if req.Flush != nil {
		opReq.Flush = *req.Flush
	}
	workerReq := workerrpc.Request{Type: workerrpc.CmdOperateSchema, OperateSchema: opReq}
	newCtx := c.Request.Context()
	resp, err := worker.SendRequest(newCtx, &workerReq, s.cfg.RPCTimeout)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if !resp.OperateSchema.Result {
		_ = c.Error(terror.ErrOpenAPICommonError.New(resp.OperateSchema.Msg))
		return
	}
}

// DMAPIImportTaskTemplate create task_config_template url is: (POST /api/v1/tasks/templates/import).
func (s *Server) DMAPIImportTaskTemplate(c *gin.Context) {
	var req openapi.TaskTemplateRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	resp := openapi.TaskTemplateResponse{
		FailedTaskList: []struct {
			ErrorMsg string `json:"error_msg"`
			TaskName string `json:"task_name"`
		}{},
		SuccessTaskList: []string{},
	}
	for _, task := range config.SubTaskConfigsToOpenAPITask(s.scheduler.GetSubTaskCfgs()) {
		if err := ha.PutOpenAPITaskTemplate(s.etcdClient, task, req.Overwrite); err != nil {
			resp.FailedTaskList = append(resp.FailedTaskList, struct {
				ErrorMsg string `json:"error_msg"`
				TaskName string `json:"task_name"`
			}{
				ErrorMsg: err.Error(),
				TaskName: task.Name,
			})
		} else {
			resp.SuccessTaskList = append(resp.SuccessTaskList, task.Name)
		}
	}
	c.IndentedJSON(http.StatusAccepted, resp)
}

// DMAPICreateTaskTemplate create task_config_template url is: (POST /api/tasks/templates).
func (s *Server) DMAPICreateTaskTemplate(c *gin.Context) {
	task := &openapi.Task{}
	if err := c.Bind(task); err != nil {
		_ = c.Error(err)
		return
	}
	if err := task.Adjust(); err != nil {
		_ = c.Error(err)
		return
	}
	// prepare target db config
	newCtx := c.Request.Context()
	toDBCfg := config.GetTargetDBCfgFromOpenAPITask(task)
	if adjustDBErr := adjustTargetDB(newCtx, toDBCfg); adjustDBErr != nil {
		_ = c.Error(terror.WithClass(adjustDBErr, terror.ClassDMMaster))
		return
	}
	if err := ha.PutOpenAPITaskTemplate(s.etcdClient, *task, false); err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusCreated, task)
}

// DMAPIGetTaskTemplateList get task_config_template list url is: (GET /api/v1/tasks/templates).
func (s *Server) DMAPIGetTaskTemplateList(c *gin.Context) {
	TaskConfigList, err := ha.GetAllOpenAPITaskTemplate(s.etcdClient)
	if err != nil {
		_ = c.Error(err)
		return
	}
	taskList := make([]openapi.Task, len(TaskConfigList))
	for i, TaskConfig := range TaskConfigList {
		taskList[i] = *TaskConfig
	}
	resp := openapi.GetTaskListResponse{Total: len(TaskConfigList), Data: taskList}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPIDeleteTaskTemplate delete task_config_template url is: (DELETE /api/v1/tasks/templates/{task-name}).
func (s *Server) DMAPIDeleteTaskTemplate(c *gin.Context, taskName string) {
	if err := ha.DeleteOpenAPITaskTemplate(s.etcdClient, taskName); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusNoContent)
}

// DMAPIGetTaskTemplate get task_config_template url is: (GET /api/v1/tasks/templates/{task-name}).
func (s *Server) DMAPIGetTaskTemplate(c *gin.Context, taskName string) {
	task, err := ha.GetOpenAPITaskTemplate(s.etcdClient, taskName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if task == nil {
		_ = c.Error(terror.ErrOpenAPITaskConfigNotExist.Generate(taskName))
		return
	}
	c.IndentedJSON(http.StatusOK, task)
}

// DMAPUpdateTaskTemplate update task_config_template url is: (PUT /api/v1/tasks/templates/{task-name}).
func (s *Server) DMAPUpdateTaskTemplate(c *gin.Context, taskName string) {
	task := &openapi.Task{}
	if err := c.Bind(task); err != nil {
		_ = c.Error(err)
		return
	}
	if err := task.Adjust(); err != nil {
		_ = c.Error(err)
		return
	}
	newCtx := c.Request.Context()
	toDBCfg := config.GetTargetDBCfgFromOpenAPITask(task)
	if adjustDBErr := adjustTargetDB(newCtx, toDBCfg); adjustDBErr != nil {
		_ = c.Error(terror.WithClass(adjustDBErr, terror.ClassDMMaster))
		return
	}
	if err := ha.UpdateOpenAPITaskTemplate(s.etcdClient, *task); err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, task)
}

// DMAPIGetClusterInfo return cluster id of dm cluster.
func (s *Server) DMAPIGetClusterInfo(c *gin.Context) {
	r := &openapi.GetClusterInfoResponse{}
	r.ClusterId = s.ClusterID()
	c.IndentedJSON(http.StatusOK, r)
}

func terrorHTTPErrorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		gErr := c.Errors.Last()
		if gErr == nil {
			return
		}
		var code int
		var msg string
		if tErr, ok := gErr.Err.(*terror.Error); ok {
			code = int(tErr.Code())
			msg = tErr.Error()
		} else {
			msg = gErr.Error()
		}
		c.IndentedJSON(http.StatusBadRequest, openapi.ErrorWithMessage{ErrorMsg: msg, ErrorCode: code})
	}
}
