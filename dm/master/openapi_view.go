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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"

	ginmiddleware "github.com/deepmap/oapi-codegen/pkg/gin-middleware"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/master/workerrpc"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
)

const (
	docJSONBasePath = "/api/v1/dm.json"
)

// reverseRequestToLeaderMW reverses request to leader.
func (s *Server) reverseRequestToLeaderMW(tlsCfg *tls.Config) gin.HandlerFunc {
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

			failpoint.Inject("MockNotSetTls", func() {
				tlsCfg = nil
			})
			// simpleProxy just reverses to leader host
			simpleProxy := httputil.ReverseProxy{
				Director: func(req *http.Request) {
					if tlsCfg != nil {
						req.URL.Scheme = "https"
					} else {
						req.URL.Scheme = "http"
					}
					req.URL.Host = leaderOpenAPIAddr
					req.Host = leaderOpenAPIAddr
				},
			}
			if tlsCfg != nil {
				transport := http.DefaultTransport.(*http.Transport).Clone()
				transport.TLSClientConfig = tlsCfg
				simpleProxy.Transport = transport
			}
			log.L().Info("reverse request to leader", zap.String("Request URL", c.Request.URL.String()), zap.String("leader", leaderOpenAPIAddr), zap.Bool("hasTLS", tlsCfg != nil))
			simpleProxy.ServeHTTP(c.Writer, c.Request)
			c.Abort()
		}
	}
}

// InitOpenAPIHandles init openapi handlers.
func (s *Server) InitOpenAPIHandles(tlsCfg *tls.Config) error {
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
	r.Use(s.reverseRequestToLeaderMW(tlsCfg))
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
	var masterURL string
	if info, err := s.getClusterInfo(c.Request.Context()); err != nil {
		_ = c.Error(err)
		return
	} else if info.Topology != nil && info.Topology.MasterTopologyList != nil && len(*info.Topology.MasterTopologyList) > 0 {
		masterTopos := *info.Topology.MasterTopologyList
		protocol := "http"
		if useTLS.Load() {
			protocol = "https"
		}
		hostPort := net.JoinHostPort(masterTopos[0].Host, strconv.Itoa(masterTopos[0].Port))
		masterURL = fmt.Sprintf("%s://%s", protocol, hostPort)
	}
	swagger, err := openapi.GetSwagger()
	if err != nil {
		_ = c.Error(err)
		return
	} else if masterURL != "" {
		for idx := range swagger.Servers {
			swagger.Servers[idx].URL = masterURL
		}
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

// DMAPIGetClusterInfo return cluster id of dm cluster url is: (GET /api/v1/cluster/info).
func (s *Server) DMAPIGetClusterInfo(c *gin.Context) {
	info, err := s.getClusterInfo(c.Request.Context())
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, info)
}

// DMAPIGetClusterInfo return cluster id of dm cluster url is: (PUT /api/v1/cluster/info).
func (s *Server) DMAPIUpdateClusterInfo(c *gin.Context) {
	var req openapi.ClusterTopology
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	info, err := s.updateClusterInfo(c.Request.Context(), &req)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, info)
}

// DMAPICreateSource url is:(POST /api/v1/sources).
func (s *Server) DMAPICreateSource(c *gin.Context) {
	var req openapi.CreateSourceRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	ctx := c.Request.Context()
	source, err := s.createSource(ctx, req)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusCreated, source)
}

// DMAPIGetSourceList url is:(GET /api/v1/sources).
func (s *Server) DMAPIGetSourceList(c *gin.Context, params openapi.DMAPIGetSourceListParams) {
	ctx := c.Request.Context()
	sourceList, err := s.listSource(ctx, params)
	if err != nil {
		_ = c.Error(err)
		return
	}
	resp := openapi.GetSourceListResponse{Total: len(sourceList), Data: sourceList}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPIGetSource url is:(GET /api/v1/sources/{source-name}).
func (s *Server) DMAPIGetSource(c *gin.Context, sourceName string, params openapi.DMAPIGetSourceParams) {
	ctx := c.Request.Context()
	source, err := s.getSource(ctx, sourceName, params)
	if err != nil {
		if terror.ErrSchedulerSourceCfgNotExist.Equal(err) {
			c.Status(http.StatusNotFound)
			return
		}
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, source)
}

// DMAPIGetSourceStatus url is: (GET /api/v1/sources/{source-id}/status).
func (s *Server) DMAPIGetSourceStatus(c *gin.Context, sourceName string) {
	ctx := c.Request.Context()
	withStatus := true
	source, err := s.getSource(ctx, sourceName, openapi.DMAPIGetSourceParams{WithStatus: &withStatus})
	if err != nil {
		_ = c.Error(err)
		return
	}
	var resp openapi.GetSourceStatusResponse
	// current this source not bound to any worker
	if worker := s.scheduler.GetWorkerBySource(sourceName); worker == nil {
		resp.Data = append(resp.Data, openapi.SourceStatus{SourceName: sourceName})
		resp.Total = len(resp.Data)
		c.IndentedJSON(http.StatusOK, resp)
		return
	}
	resp.Data = append(resp.Data, *source.StatusList...)
	resp.Total = len(resp.Data)
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPIUpdateSource url is:(PUT /api/v1/sources/{source-name}).
func (s *Server) DMAPIUpdateSource(c *gin.Context, sourceName string) {
	var req openapi.UpdateSourceRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	source, err := s.updateSource(c.Request.Context(), sourceName, req)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, source)
}

// DMAPIDeleteSource url is:(DELETE /api/v1/sources/{source-name}).
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

// DMAPIEnableSource url is:(POST /api/v1/sources/{source-name}/enable).
func (s *Server) DMAPIEnableSource(c *gin.Context, sourceName string) {
	ctx := c.Request.Context()
	if _, err := s.getSource(ctx, sourceName, openapi.DMAPIGetSourceParams{}); err != nil {
		_ = c.Error(err)
		return
	}
	if err := s.enableSource(ctx, sourceName); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusOK)
}

// DMAPIDisableSource url is:(POST /api/v1/sources/{source-name}/disable).
func (s *Server) DMAPIDisableSource(c *gin.Context, sourceName string) {
	ctx := c.Request.Context()
	if _, err := s.getSource(ctx, sourceName, openapi.DMAPIGetSourceParams{}); err != nil {
		_ = c.Error(err)
		return
	}
	if err := s.disableSource(ctx, sourceName); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusOK)
}

// DMAPITransferSource transfer source to another free worker url is: (POST /api/v1/sources/{source-name}/transfer).
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

// DMAPIGetSourceSchemaList get source schema list url is: (GET /api/v1/sources/{source-name}/schemas).
func (s *Server) DMAPIGetSourceSchemaList(c *gin.Context, sourceName string) {
	baseDB, err := s.getBaseDBBySourceName(sourceName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	defer baseDB.Close()
	schemaList, err := dbutil.GetSchemas(c.Request.Context(), baseDB.DB)
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
	tableList, err := dbutil.GetTables(c.Request.Context(), baseDB.DB, schemaName)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, tableList)
}

// DMAPIEnableRelay url is:(POST /api/v1/relay/enable).
func (s *Server) DMAPIEnableRelay(c *gin.Context, sourceName string) {
	var req openapi.EnableRelayRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	if err := s.enableRelay(c.Request.Context(), sourceName, req); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusOK)
}

// DMAPIEnableRelay url is:(POST /api/v1/relay/disable).
func (s *Server) DMAPIDisableRelay(c *gin.Context, sourceName string) {
	var req openapi.DisableRelayRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	if err := s.disableRelay(c.Request.Context(), sourceName, req); err != nil {
		_ = c.Error(err)
	}
	c.Status(http.StatusOK)
}

// DMAPIPurgeRelay url is:(POST /api/v1/relay/purge).
func (s *Server) DMAPIPurgeRelay(c *gin.Context, sourceName string) {
	var req openapi.PurgeRelayRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	if err := s.purgeRelay(c.Request.Context(), sourceName, req); err != nil {
		_ = c.Error(err)
	}
	c.Status(http.StatusOK)
}

func (s *Server) getBaseDBBySourceName(sourceName string) (*conn.BaseDB, error) {
	sourceCfg := s.scheduler.GetSourceCfgByID(sourceName)
	if sourceCfg == nil {
		return nil, terror.ErrSchedulerSourceCfgNotExist.Generate(sourceName)
	}
	dbCfg := sourceCfg.GenerateDBConfig()
	return conn.GetUpstreamDB(dbCfg)
}

// DMAPICreateTask url is:(POST /api/v1/tasks).
func (s *Server) DMAPICreateTask(c *gin.Context) {
	var req openapi.CreateTaskRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	res, err := s.createTask(c.Request.Context(), req)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusCreated, res)
}

// DMAPIUpdateTask url is: (PUT /api/v1/tasks/{task-name}).
func (s *Server) DMAPIUpdateTask(c *gin.Context, taskName string) {
	var req openapi.UpdateTaskRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	res, err := s.updateTask(c.Request.Context(), req)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, res)
}

// DMAPIDeleteTask url is:(DELETE /api/v1/tasks).
func (s *Server) DMAPIDeleteTask(c *gin.Context, taskName string, params openapi.DMAPIDeleteTaskParams) {
	ctx := c.Request.Context()
	var force bool
	if params.Force != nil && *params.Force {
		force = *params.Force
	}
	if err := s.deleteTask(ctx, taskName, force); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusNoContent)
}

// DMAPIGetTask url is:(GET /api/v1/tasks/{task-name}).
func (s *Server) DMAPIGetTask(c *gin.Context, taskName string, params openapi.DMAPIGetTaskParams) {
	ctx := c.Request.Context()
	task, err := s.getTask(ctx, taskName, params)
	if err != nil {
		if terror.ErrSchedulerSourceCfgNotExist.Equal(err) {
			c.Status(http.StatusNotFound)
			return
		}
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, task)
}

// DMAPIGetTaskStatus url is:(GET /api/v1/tasks/{task-name}/status).
func (s *Server) DMAPIGetTaskStatus(c *gin.Context, taskName string, params openapi.DMAPIGetTaskStatusParams) {
	ctx := c.Request.Context()
	withStatus := true
	task, err := s.getTask(ctx, taskName, openapi.DMAPIGetTaskParams{WithStatus: &withStatus})
	if err != nil {
		_ = c.Error(err)
		return
	}
	resp := openapi.GetTaskStatusResponse{Total: len(*task.StatusList), Data: *task.StatusList}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPIGetTaskList url is:(GET /api/v1/tasks).
func (s *Server) DMAPIGetTaskList(c *gin.Context, params openapi.DMAPIGetTaskListParams) {
	ctx := c.Request.Context()
	taskList, err := s.listTask(ctx, params)
	if err != nil {
		_ = c.Error(err)
		return
	}
	resp := openapi.GetTaskListResponse{Total: len(taskList), Data: taskList}
	c.IndentedJSON(http.StatusOK, resp)
}

// DMAPIStartTask url is: (POST /api/v1/tasks/{task-name}/start).
func (s *Server) DMAPIStartTask(c *gin.Context, taskName string) {
	var req openapi.StartTaskRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	ctx := c.Request.Context()
	if err := s.startTask(ctx, taskName, req); err != nil {
		_ = c.Error(err)
	}
	c.Status(http.StatusOK)
}

// DMAPIStopTask url is: (POST /api/v1/tasks/{task-name}/stop).
func (s *Server) DMAPIStopTask(c *gin.Context, taskName string) {
	var req openapi.StopTaskRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	ctx := c.Request.Context()
	if err := s.stopTask(ctx, taskName, req); err != nil {
		_ = c.Error(err)
	}
	c.Status(http.StatusOK)
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

// DMAPIGetTaskMigrateTargets get task migrate targets list url is: (GET /api/v1/tasks/{task-name}/sources/{source-name}/migrate_targets).
func (s *Server) DMAPIGetTaskMigrateTargets(c *gin.Context, taskName string, sourceName string, params openapi.DMAPIGetTaskMigrateTargetsParams) {
	worker := s.scheduler.GetWorkerBySource(sourceName)
	if worker == nil {
		_ = c.Error(terror.ErrWorkerNoStart)
		return
	}
	var schemaPattern, tablePattern string
	if params.SchemaPattern != nil {
		schemaPattern = *params.SchemaPattern
	}
	if params.TablePattern != nil {
		tablePattern = *params.TablePattern
	}
	workerReq := workerrpc.Request{
		Type: workerrpc.CmdOperateSchema,
		OperateSchema: &pb.OperateWorkerSchemaRequest{
			Op:     pb.SchemaOp_ListMigrateTargets,
			Task:   taskName,
			Source: sourceName,
			Schema: schemaPattern,
			Table:  tablePattern,
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
	targets := []openapi.TaskMigrateTarget{}
	if err := json.Unmarshal([]byte(resp.OperateSchema.Msg), &targets); err != nil {
		_ = c.Error(terror.ErrSchemaTrackerUnMarshalJSON.Delegate(err, resp.OperateSchema.Msg))
		return
	}
	c.IndentedJSON(http.StatusOK, openapi.GetTaskMigrateTargetsResponse{Data: targets, Total: len(targets)})
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

// DMAPIConvertTask turns task into the format of a configuration file or vice versa url is: (POST /api/v1/tasks/,).
func (s *Server) DMAPIConvertTask(c *gin.Context) {
	var req openapi.ConverterTaskRequest
	if err := c.Bind(&req); err != nil {
		_ = c.Error(err)
		return
	}
	if req.Task == nil && req.TaskConfigFile == nil {
		_ = c.Error(terror.ErrOpenAPICommonError.Generate("request body is invalid one of `task` or `task_config_file` must be entered."))
		return
	}
	task, taskCfg, err := s.convertTaskConfig(c.Request.Context(), req)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, openapi.ConverterTaskResponse{Task: *task, TaskConfigFile: taskCfg.String()})
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
	for _, task := range config.SubTaskConfigsToOpenAPITaskList(s.scheduler.GetALlSubTaskCfgs()) {
		if err := ha.PutOpenAPITaskTemplate(s.etcdClient, *task, req.Overwrite); err != nil {
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
	if adjustDBErr := AdjustTargetDB(newCtx, toDBCfg); adjustDBErr != nil {
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
	if adjustDBErr := AdjustTargetDB(newCtx, toDBCfg); adjustDBErr != nil {
		_ = c.Error(terror.WithClass(adjustDBErr, terror.ClassDMMaster))
		return
	}
	if err := ha.UpdateOpenAPITaskTemplate(s.etcdClient, *task); err != nil {
		_ = c.Error(err)
		return
	}
	c.IndentedJSON(http.StatusOK, task)
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
