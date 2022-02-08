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

package api

import (
	"bufio"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	// apiOpVarChangefeedState is the key of changefeed state in HTTP API
	apiOpVarChangefeedState = "state"
	// apiOpVarChangefeedID is the key of changefeed ID in HTTP API
	apiOpVarChangefeedID = "changefeed_id"
	// apiOpVarCaptureID is the key of capture ID in HTTP API
	apiOpVarCaptureID = "capture_id"
	// forWardFromCapture is a header to be set when a request is forwarded from another capture
	forWardFromCapture = "TiCDC-ForwardFromCapture"
	// getOwnerRetryMaxTime is the retry max time to get an owner
	getOwnerRetryMaxTime = 3
)

// openAPI provides capture APIs.
type openAPI struct {
	capture *capture.Capture
	// use for unit test only
	testStatusProvider owner.StatusProvider
}

func NewOpenAPI(c *capture.Capture) openAPI {
	return openAPI{capture: c}
}

// NewOpenAPI4Test return a openAPI for test
func NewOpenAPI4Test(c *capture.Capture, p owner.StatusProvider) openAPI {
	return openAPI{capture: c, testStatusProvider: p}
}

func (h *openAPI) statusProvider() owner.StatusProvider {
	if h.testStatusProvider != nil {
		return h.testStatusProvider
	}
	return h.capture.StatusProvider()
}

// RegisterOpenAPIRoutes registers routes for OpenAPI
func RegisterOpenAPIRoutes(router *gin.Engine, api openAPI) {
	v1 := router.Group("/api/v1")

	v1.Use(logMiddleware())
	v1.Use(errorHandleMiddleware())

	// common API
	v1.GET("/status", api.ServerStatus)
	v1.GET("/health", api.Health)
	v1.POST("/log", SetLogLevel)

	// changefeed API
	changefeedGroup := v1.Group("/changefeeds")
	changefeedGroup.GET("", api.ListChangefeed)
	changefeedGroup.GET("/:changefeed_id", api.GetChangefeed)
	changefeedGroup.POST("", api.CreateChangefeed)
	changefeedGroup.PUT("/:changefeed_id", api.UpdateChangefeed)
	changefeedGroup.POST("/:changefeed_id/pause", api.PauseChangefeed)
	changefeedGroup.POST("/:changefeed_id/resume", api.ResumeChangefeed)
	changefeedGroup.DELETE("/:changefeed_id", api.RemoveChangefeed)
	changefeedGroup.POST("/:changefeed_id/tables/rebalance_table", api.RebalanceTable)
	changefeedGroup.POST("/:changefeed_id/tables/move_table", api.MoveTable)

	// owner API
	ownerGroup := v1.Group("/owner")
	ownerGroup.POST("/resign", api.ResignOwner)

	// processor API
	processorGroup := v1.Group("/processors")
	processorGroup.GET("", api.ListProcessor)
	processorGroup.GET("/:changefeed_id/:capture_id", api.GetProcessor)

	// capture API
	captureGroup := v1.Group("/captures")
	captureGroup.GET("", api.ListCapture)
}

// ListChangefeed lists all changgefeeds in cdc cluster
// @Summary List changefeed
// @Description list all changefeeds in cdc cluster
// @Tags changefeed
// @Accept json
// @Produce json
// @Param state query string false "state"
// @Success 200 {array} model.ChangefeedCommonInfo
// @Failure 500 {object} model.HTTPError
// @Router /api/v1/changefeeds [get]
func (h *openAPI) ListChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	ctx := c.Request.Context()
	state := c.Query(apiOpVarChangefeedState)
	// get all changefeed status
	statuses, err := h.statusProvider().GetAllChangeFeedStatuses(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}
	// get all changefeed infos
	infos, err := h.statusProvider().GetAllChangeFeedInfo(ctx)
	if err != nil {
		// this call will return a parsedError generated by the error we passed in
		// so it is no need to check the parsedError
		_ = c.Error(err)
		return
	}

	resps := make([]*model.ChangefeedCommonInfo, 0)
	for cfID, cfStatus := range statuses {
		cfInfo, exist := infos[cfID]
		if !exist {
			// If a changefeed info does not exists, skip it
			continue
		}

		if !cfInfo.State.IsNeeded(state) {
			continue
		}

		resp := &model.ChangefeedCommonInfo{
			ID: cfID,
		}

		if cfInfo != nil {
			resp.FeedState = cfInfo.State
			resp.RunningError = cfInfo.Error
		}

		if cfStatus != nil {
			resp.CheckpointTSO = cfStatus.CheckpointTs
			tm := oracle.GetTimeFromTS(cfStatus.CheckpointTs)
			resp.CheckpointTime = model.JSONTime(tm)
		}

		resps = append(resps, resp)
	}
	c.IndentedJSON(http.StatusOK, resps)
}

// GetChangefeed get detailed info of a changefeed
// @Summary Get changefeed
// @Description get detail information of a changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Success 200 {object} model.ChangefeedDetail
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v1/changefeeds/{changefeed_id} [get]
func (h *openAPI) GetChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	ctx := c.Request.Context()
	changefeedID := c.Param(apiOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID))
		return
	}

	info, err := h.statusProvider().GetChangeFeedInfo(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	status, err := h.statusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	processorInfos, err := h.statusProvider().GetAllTaskStatuses(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	taskStatus := make([]model.CaptureTaskStatus, 0, len(processorInfos))
	for captureID, status := range processorInfos {
		tables := make([]int64, 0)
		for tableID := range status.Tables {
			tables = append(tables, tableID)
		}
		taskStatus = append(taskStatus, model.CaptureTaskStatus{CaptureID: captureID, Tables: tables, Operation: status.Operation})
	}

	changefeedDetail := &model.ChangefeedDetail{
		ID:             changefeedID,
		SinkURI:        info.SinkURI,
		CreateTime:     model.JSONTime(info.CreateTime),
		StartTs:        info.StartTs,
		TargetTs:       info.TargetTs,
		CheckpointTSO:  status.CheckpointTs,
		CheckpointTime: model.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
		ResolvedTs:     status.ResolvedTs,
		Engine:         info.Engine,
		FeedState:      info.State,
		TaskStatus:     taskStatus,
	}

	c.IndentedJSON(http.StatusOK, changefeedDetail)
}

// CreateChangefeed creates a changefeed
// @Summary Create changefeed
// @Description create a new changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed body model.ChangefeedConfig true "changefeed config"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/changefeeds [post]
func (h *openAPI) CreateChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	ctx := c.Request.Context()
	var changefeedConfig model.ChangefeedConfig
	if err := c.BindJSON(&changefeedConfig); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.Wrap(err))
		return
	}

	info, err := verifyCreateChangefeedConfig(c, changefeedConfig, h.capture)
	if err != nil {
		_ = c.Error(err)
		return
	}

	infoStr, err := info.Marshal()
	if err != nil {
		_ = c.Error(err)
		return
	}

	err = h.capture.EtcdClient.CreateChangefeedInfo(ctx, info, changefeedConfig.ID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	log.Info("Create changefeed successfully!", zap.String("id", changefeedConfig.ID), zap.String("changefeed", infoStr))
	c.Status(http.StatusAccepted)
}

// PauseChangefeed pauses a changefeed
// @Summary Pause a changefeed
// @Description Pause a changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v1/changefeeds/{changefeed_id}/pause [post]
func (h *openAPI) PauseChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	ctx := c.Request.Context()

	changefeedID := c.Param(apiOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID))
		return
	}
	// check if the changefeed exists
	_, err := h.statusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	job := model.AdminJob{
		CfID: changefeedID,
		Type: model.AdminStop,
	}

	_ = h.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.EnqueueJob(job)
		return nil
	})

	c.Status(http.StatusAccepted)
}

// ResumeChangefeed resumes a changefeed
// @Summary Resume a changefeed
// @Description Resume a changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed-id path string true "changefeed_id"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/changefeeds/{changefeed_id}/resume [post]
func (h *openAPI) ResumeChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	ctx := c.Request.Context()
	changefeedID := c.Param(apiOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID))
		return
	}
	// check if the changefeed exists
	_, err := h.statusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	job := model.AdminJob{
		CfID: changefeedID,
		Type: model.AdminResume,
	}

	_ = h.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.EnqueueJob(job)
		return nil
	})

	c.Status(http.StatusAccepted)
}

// UpdateChangefeed updates a changefeed
// @Summary Update a changefeed
// @Description Update a changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Param target_ts body integer false "changefeed target ts"
// @Param sink_uri body string false "sink uri"
// @Param filter_rules body []string false "filter rules"
// @Param ignore_txn_start_ts body integer false "ignore transaction start ts"
// @Param mounter_worker_num body integer false "mounter worker nums"
// @Param sink_config body config.SinkConfig false "sink config"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v1/changefeeds/{changefeed_id} [put]
func (h *openAPI) UpdateChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	ctx := c.Request.Context()
	changefeedID := c.Param(apiOpVarChangefeedID)

	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID))
		return
	}
	info, err := h.statusProvider().GetChangeFeedInfo(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if info.State != model.StateStopped {
		_ = c.Error(cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs("can only update changefeed config when it is stopped"))
		return
	}

	// can only update target-ts, sink-uri
	// filter_rules, ignore_txn_start_ts, mounter_worker_num, sink_config
	var changefeedConfig model.ChangefeedConfig
	if err = c.BindJSON(&changefeedConfig); err != nil {
		_ = c.Error(err)
		return
	}

	newInfo, err := verifyUpdateChangefeedConfig(ctx, changefeedConfig, info)
	if err != nil {
		_ = c.Error(err)
		return
	}

	err = h.capture.EtcdClient.SaveChangeFeedInfo(ctx, newInfo, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	c.Status(http.StatusAccepted)
}

// RemoveChangefeed removes a changefeed
// @Summary Remove a changefeed
// @Description Remove a changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id path string true "changefeed_id"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/changefeeds/{changefeed_id} [delete]
func (h *openAPI) RemoveChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	ctx := c.Request.Context()
	changefeedID := c.Param(apiOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID))
		return
	}
	// check if the changefeed exists
	_, err := h.statusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	job := model.AdminJob{
		CfID: changefeedID,
		Type: model.AdminRemove,
	}

	_ = h.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.EnqueueJob(job)
		return nil
	})

	c.Status(http.StatusAccepted)
}

// RebalanceTable rebalances tables
// @Summary rebalance tables
// @Description rebalance all tables of a changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id path string true "changefeed_id"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v1/changefeeds/{changefeed_id}/tables/rebalance_table [post]
func (h *openAPI) RebalanceTable(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	ctx := c.Request.Context()
	changefeedID := c.Param(apiOpVarChangefeedID)

	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID))
		return
	}
	// check if the changefeed exists
	_, err := h.statusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	_ = h.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.TriggerRebalance(changefeedID)
		return nil
	})

	c.Status(http.StatusAccepted)
}

// MoveTable moves a table to target capture
// @Summary move table
// @Description move one table to the target capture
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed_id path string true "changefeed_id"
// @Param table_id body integer true "table_id"
// @Param capture_id body string true "capture_id"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v1/changefeeds/{changefeed_id}/tables/move_table [post]
func (h *openAPI) MoveTable(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	ctx := c.Request.Context()
	changefeedID := c.Param(apiOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID))
		return
	}
	// check if the changefeed exists
	_, err := h.statusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	data := struct {
		CaptureID string `json:"capture_id"`
		TableID   int64  `json:"table_id"`
	}{}
	err = c.BindJSON(&data)
	if err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.Wrap(err))
		return
	}

	if err := model.ValidateChangefeedID(data.CaptureID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid capture_id: %s", data.CaptureID))
		return
	}

	_ = h.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.ManualSchedule(changefeedID, data.CaptureID, data.TableID)
		return nil
	})

	c.Status(http.StatusAccepted)
}

// ResignOwner makes the current owner resign
// @Summary notify the owner to resign
// @Description notify the current owner to resign
// @Tags owner
// @Accept json
// @Produce json
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/owner/resign [post]
func (h *openAPI) ResignOwner(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	_ = h.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.AsyncStop()
		return nil
	})

	c.Status(http.StatusAccepted)
}

// GetProcessor gets the detailed info of a processor
// @Summary Get processor detail information
// @Description get the detail information of a processor
// @Tags processor
// @Accept json
// @Produce json
// @Success 200 {object} model.ProcessorDetail
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/processors/{changefeed_id}/{capture_id} [get]
func (h *openAPI) GetProcessor(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	ctx := c.Request.Context()

	changefeedID := c.Param(apiOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID))
		return
	}

	captureID := c.Param(apiOpVarCaptureID)
	if err := model.ValidateChangefeedID(captureID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid capture_id: %s", captureID))
		return
	}

	statuses, err := h.statusProvider().GetAllTaskStatuses(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	status, exist := statuses[captureID]
	if !exist {
		_ = c.Error(cerror.ErrCaptureNotExist.GenWithStackByArgs(captureID))
		return
	}

	positions, err := h.statusProvider().GetTaskPositions(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	position, exist := positions[captureID]
	// Note: for the case that no tables are attached to a newly created changefeed,
	//       we just do not report an error.
	var processorDetail model.ProcessorDetail
	if exist {
		processorDetail = model.ProcessorDetail{
			CheckPointTs: position.CheckPointTs,
			ResolvedTs:   position.ResolvedTs,
			Count:        position.Count,
			Error:        position.Error,
		}
		tables := make([]int64, 0)
		for tableID := range status.Tables {
			tables = append(tables, tableID)
		}
		processorDetail.Tables = tables
	}
	c.IndentedJSON(http.StatusOK, &processorDetail)
}

// ListProcessor lists all processors in the TiCDC cluster
// @Summary List processors
// @Description list all processors in the TiCDC cluster
// @Tags processor
// @Accept json
// @Produce json
// @Success 200 {array} model.ProcessorCommonInfo
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/processors [get]
func (h *openAPI) ListProcessor(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	ctx := c.Request.Context()
	infos, err := h.statusProvider().GetProcessors(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}
	resps := make([]*model.ProcessorCommonInfo, len(infos))
	for i, info := range infos {
		resp := &model.ProcessorCommonInfo{CfID: info.CfID, CaptureID: info.CaptureID}
		resps[i] = resp
	}
	c.IndentedJSON(http.StatusOK, resps)
}

// ListCapture lists all captures
// @Summary List captures
// @Description list all captures in cdc cluster
// @Tags capture
// @Accept json
// @Produce json
// @Success 200 {array} model.Capture
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/captures [get]
func (h *openAPI) ListCapture(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	ctx := c.Request.Context()
	captureInfos, err := h.statusProvider().GetCaptures(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}

	ownerID := h.capture.Info().ID

	captures := make([]*model.Capture, 0, len(captureInfos))
	for _, c := range captureInfos {
		isOwner := c.ID == ownerID
		captures = append(captures,
			&model.Capture{ID: c.ID, IsOwner: isOwner, AdvertiseAddr: c.AdvertiseAddr})
	}

	c.IndentedJSON(http.StatusOK, captures)
}

// ServerStatus gets the status of server(capture)
// @Summary Get server status
// @Description get the status of a server(capture)
// @Tags common
// @Accept json
// @Produce json
// @Success 200 {object} model.ServerStatus
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v1/status [get]
func (h *openAPI) ServerStatus(c *gin.Context) {
	status := model.ServerStatus{
		Version: version.ReleaseVersion,
		GitHash: version.GitHash,
		Pid:     os.Getpid(),
	}
	status.ID = h.capture.Info().ID
	status.IsOwner = h.capture.IsOwner()
	c.IndentedJSON(http.StatusOK, status)
}

// Health check if cdc cluster is health
// @Summary Check if CDC cluster is health
// @Description check if CDC cluster is health
// @Tags common
// @Accept json
// @Produce json
// @Success 200
// @Failure 500 {object} model.HTTPError
// @Router	/api/v1/health [get]
func (h *openAPI) Health(c *gin.Context) {
	ctx := c.Request.Context()

	if _, err := h.capture.GetOwner(ctx); err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}
	c.Status(http.StatusOK)
}

// SetLogLevel changes TiCDC log level dynamically.
// @Summary Change TiCDC log level
// @Description change TiCDC log level dynamically
// @Tags common
// @Accept json
// @Produce json
// @Param log_level body string true "log level"
// @Success 200
// @Failure 400 {object} model.HTTPError
// @Router	/api/v1/log [post]
func SetLogLevel(c *gin.Context) {
	// get json data from request body
	data := struct {
		Level string `json:"log_level"`
	}{}
	err := c.BindJSON(&data)
	if err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid log level: %s", err.Error()))
		return
	}

	err = logutil.SetLogLevel(data.Level)
	if err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("fail to change log level: %s", data.Level))
		return
	}
	log.Warn("log level changed", zap.String("level", data.Level))
	c.Status(http.StatusOK)
}

// forwardToOwner forward an request to owner
func (h *openAPI) forwardToOwner(c *gin.Context) {
	ctx := c.Request.Context()
	// every request can only forward to owner one time
	if len(c.GetHeader(forWardFromCapture)) != 0 {
		_ = c.Error(cerror.ErrRequestForwardErr.FastGenByArgs())
		return
	}
	c.Header(forWardFromCapture, h.capture.Info().ID)

	var owner *model.CaptureInfo
	// get owner
	err := retry.Do(ctx, func() error {
		o, err := h.capture.GetOwner(ctx)
		if err != nil {
			log.Info("get owner failed, retry later", zap.Error(err))
			return err
		}
		owner = o
		return nil
	}, retry.WithBackoffBaseDelay(300), retry.WithMaxTries(getOwnerRetryMaxTime))
	if err != nil {
		_ = c.Error(err)
		return
	}

	tslConfig, err := config.GetGlobalServerConfig().Security.ToTLSConfigWithVerify()
	if err != nil {
		_ = c.Error(err)
		return
	}

	// init a request
	req, _ := http.NewRequest(c.Request.Method, c.Request.RequestURI, c.Request.Body)
	req.URL.Host = owner.AdvertiseAddr
	if tslConfig != nil {
		req.URL.Scheme = "https"
	} else {
		req.URL.Scheme = "http"
	}
	for k, v := range c.Request.Header {
		for _, vv := range v {
			req.Header.Add(k, vv)
		}
	}

	// forward to owner
	cli := httputil.NewClient(tslConfig)
	resp, err := cli.Do(req)
	if err != nil {
		_ = c.Error(err)
		return
	}

	// write header
	for k, values := range resp.Header {
		for _, v := range values {
			c.Header(k, v)
		}
	}

	// write status code
	c.Status(resp.StatusCode)

	// write response body
	defer resp.Body.Close()
	_, err = bufio.NewReader(resp.Body).WriteTo(c.Writer)
	if err != nil {
		_ = c.Error(err)
		return
	}
}
