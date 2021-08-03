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

package capture

import (
	"bufio"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/br/pkg/httputil"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/owner"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	chttputil "github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	// APIOpVarChangefeedState is the key of changefeed state in HTTP API
	APIOpVarChangefeedState = "state"
	// APIOpVarChangefeedID is the key of changefeed ID in HTTP API
	APIOpVarChangefeedID = "changefeed_id"
	// APIOpVarCaptureID is the key of capture ID in HTTP API
	APIOpVarCaptureID = "capture_id"
	// APIOpVarTableID is the key of table ID in HTTP API
	APIOpVarTableID = "table_id"
	// ForwardFromCapture is a header to be set when a request is forwarded from another capture
	ForwardFromCapture = "TiCDC-ForwardFromCapture"
)

// HTTPHandler is a  HTTPHandler of capture
type HTTPHandler struct {
	capture *Capture
}

// NewHTTPHandler return a HTTPHandler for OpenAPI
func NewHTTPHandler(capture *Capture) HTTPHandler {
	return HTTPHandler{
		capture: capture,
	}
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
func (h *HTTPHandler) ListChangefeed(c *gin.Context) {
	state := c.Query(APIOpVarChangefeedState)
	statuses, err := h.capture.etcdClient.GetAllChangeFeedStatus(c.Request.Context())
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}
	changefeedIDs := make(map[string]struct{}, len(statuses))
	for cid := range statuses {
		changefeedIDs[cid] = struct{}{}
	}

	resps := make([]*model.ChangefeedCommonInfo, 0)
	for changefeedID := range changefeedIDs {

		cfInfo, err := h.capture.etcdClient.GetChangeFeedInfo(c.Request.Context(), changefeedID)
		if err != nil {
			// If a changefeed does not exists, skip it
			if cerror.ErrChangeFeedNotExists.Equal(err) {
				continue
			}
			c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
			return
		}

		if !chttputil.IsListState(state, cfInfo.State) {
			continue
		}

		cfStatus, _, err := h.capture.etcdClient.GetChangeFeedStatus(c.Request.Context(), changefeedID)
		if err != nil {
			// If a changefeed does not exists, skip it
			if cerror.ErrChangeFeedNotExists.Equal(err) {
				continue
			}
			c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
			return
		}

		resp := &model.ChangefeedCommonInfo{
			ID: changefeedID,
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
func (h *HTTPHandler) GetChangefeed(c *gin.Context) {
	changefeedID := c.Param(APIOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		c.IndentedJSON(http.StatusBadRequest,
			model.NewHTTPError(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID)))
		return
	}

	info, err := h.capture.etcdClient.GetChangeFeedInfo(c, changefeedID)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}

	status, _, err := h.capture.etcdClient.GetChangeFeedStatus(c, changefeedID)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}

	processorInfos, err := h.capture.etcdClient.GetAllTaskStatus(c, changefeedID)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
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
func (h *HTTPHandler) CreateChangefeed(c *gin.Context) {
	// TODO
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
func (h *HTTPHandler) PauseChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	changefeedID := c.Param(APIOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		c.IndentedJSON(http.StatusBadRequest,
			model.NewHTTPError(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID)))
		return
	}
	// check if the changefeed exists && check if the etcdClient work well
	_, _, err := h.capture.etcdClient.GetChangeFeedStatus(c, changefeedID)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
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
func (h *HTTPHandler) ResumeChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}

	changefeedID := c.Param(APIOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		c.IndentedJSON(http.StatusBadRequest,
			model.NewHTTPError(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID)))
		return
	}
	// check if the changefeed exists && check if the etcdClient work well
	_, _, err := h.capture.etcdClient.GetChangeFeedStatus(c, changefeedID)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
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
// @Param changefeed body model.ChangefeedConfig true "update changefeed"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v1/changefeeds/{changefeed_id} [put]
func (h *HTTPHandler) UpdateChangefeed(c *gin.Context) {
	// TODO
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
func (h *HTTPHandler) RemoveChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}
	changefeedID := c.Param(APIOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		c.IndentedJSON(http.StatusBadRequest,
			model.NewHTTPError(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID)))
		return
	}
	// check if the changefeed exists && check if the etcdClient work well
	_, _, err := h.capture.etcdClient.GetChangeFeedStatus(c, changefeedID)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
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
func (h *HTTPHandler) RebalanceTable(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
		return
	}
	changefeedID := c.Param(APIOpVarChangefeedID)

	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		c.IndentedJSON(http.StatusBadRequest,
			model.NewHTTPError(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID)))
		return
	}
	// check if the changefeed exists
	_, _, err := h.capture.etcdClient.GetChangeFeedStatus(c, changefeedID)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
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
// @Param capture_id body string true "target capture_id"
// @Success 202
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v1/changefeeds/{changefeed_id}/tables/move_table [post]
func (h *HTTPHandler) MoveTable(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
	}

	changefeedID := c.Param(APIOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		c.IndentedJSON(http.StatusBadRequest,
			model.NewHTTPError(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID)))
		return
	}
	// check if the changefeed exists
	_, _, err := h.capture.etcdClient.GetChangeFeedStatus(c, changefeedID)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}

	captureID := c.PostForm(APIOpVarCaptureID)
	if err := model.ValidateChangefeedID(captureID); err != nil {
		c.IndentedJSON(http.StatusBadRequest,
			model.NewHTTPError(cerror.ErrAPIInvalidParam.GenWithStack("invalid capture_id: %s", captureID)))
		return
	}

	tableIDStr := c.PostForm(APIOpVarTableID)
	tableID, err := strconv.ParseInt(tableIDStr, 10, 64)
	if err != nil {
		c.IndentedJSON(http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid table_id: %s", tableIDStr))
		return
	}

	_ = h.capture.OperateOwnerUnderLock(func(owner *owner.Owner) error {
		owner.ManualSchedule(changefeedID, captureID, tableID)
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
func (h *HTTPHandler) ResignOwner(c *gin.Context) {
	if !h.capture.IsOwner() {
		h.forwardToOwner(c)
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
func (h *HTTPHandler) GetProcessor(c *gin.Context) {
	changefeedID := c.Param(APIOpVarChangefeedID)
	if err := model.ValidateChangefeedID(changefeedID); err != nil {
		c.IndentedJSON(http.StatusBadRequest,
			model.NewHTTPError(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedID)))
		return
	}

	captureID := c.Param(APIOpVarCaptureID)
	if err := model.ValidateChangefeedID(captureID); err != nil {
		c.IndentedJSON(http.StatusBadRequest,
			model.NewHTTPError(cerror.ErrAPIInvalidParam.GenWithStack("invalid capture_id: %s", changefeedID)))
		return
	}

	_, status, err := h.capture.etcdClient.GetTaskStatus(c, changefeedID, captureID)
	if err != nil && cerror.ErrTaskStatusNotExists.Equal(err) {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}

	_, position, err := h.capture.etcdClient.GetTaskPosition(c, changefeedID, captureID)
	if err != nil && cerror.ErrTaskPositionNotExists.Equal(err) {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}

	processorDetail := &model.ProcessorDetail{CheckPointTs: position.CheckPointTs, ResolvedTs: position.ResolvedTs, Error: position.Error}
	tables := make([]int64, 0)
	for tableID := range status.Tables {
		tables = append(tables, tableID)
	}
	processorDetail.Tables = tables
	c.IndentedJSON(http.StatusOK, processorDetail)
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
func (h *HTTPHandler) ListProcessor(c *gin.Context) {
	infos, err := h.capture.etcdClient.GetProcessors(c)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
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
func (h *HTTPHandler) ListCapture(c *gin.Context) {
	_, raw, err := h.capture.etcdClient.GetCaptures(c)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}

	ownerID, err := h.capture.etcdClient.GetOwnerID(c, kv.CaptureOwnerKey)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}

	captures := make([]*model.Capture, 0, len(raw))
	for _, c := range raw {
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
func (h *HTTPHandler) ServerStatus(c *gin.Context) {
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
func (h *HTTPHandler) Health(c *gin.Context) {
	if _, err := h.capture.GetOwner(c); err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}
	c.Status(http.StatusOK)
}

// forwardToOwner forward an request to owner
func (h *HTTPHandler) forwardToOwner(c *gin.Context) {
	// every request can only forward to owner one time
	if len(c.GetHeader(ForwardFromCapture)) != 0 {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(cerror.ErrRequestForwardErr.FastGenByArgs()))
		return
	}
	c.Header(ForwardFromCapture, h.capture.Info().ID)

	owner, err := h.capture.GetOwner(c)
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
	}

	tslConfig, err := config.GetGlobalServerConfig().Security.ToTLSConfigWithVerify()
	if err != nil {
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
	}

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
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
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
		c.IndentedJSON(http.StatusInternalServerError, model.NewHTTPError(err))
		return
	}
}
