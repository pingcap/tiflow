// Copyright 2020 PingCAP, Inc.
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

package owner

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/api/middleware"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type commonResp struct {
	Status  bool   `json:"status"`
	Message string `json:"message"`
}

// ChangefeedResp holds the most common usage information for a changefeed
type ChangefeedResp struct {
	FeedState    string              `json:"state"`
	TSO          uint64              `json:"tso"`
	Checkpoint   string              `json:"checkpoint"`
	RunningError *model.RunningError `json:"error"`
}

// MarshalJSON use to marshal ChangefeedResp
func (c ChangefeedResp) MarshalJSON() ([]byte, error) {
	// alias the original type to prevent recursive call of MarshalJSON
	type Alias ChangefeedResp
	if c.FeedState == string(model.StateNormal) {
		c.RunningError = nil
	}
	return json.Marshal(struct {
		Alias
	}{
		Alias: Alias(c),
	})
}

// ownerAPI provides owner APIs.
type ownerAPI struct {
	capture capture.Capture
}

// RegisterOwnerAPIRoutes registers routes for owner APIs.
func RegisterOwnerAPIRoutes(router *gin.Engine, capture capture.Capture) {
	ownerAPI := ownerAPI{capture: capture}
	owner := router.Group("/capture/owner")

	owner.Use(middleware.ErrorHandleMiddleware())
	owner.Use(middleware.LogMiddleware())

	owner.POST("/resign", gin.WrapF(ownerAPI.handleResignOwner))
	owner.POST("/admin", gin.WrapF(ownerAPI.handleChangefeedAdmin))
	owner.POST("/rebalance_trigger", gin.WrapF(ownerAPI.handleRebalanceTrigger))
	owner.POST("/move_table", gin.WrapF(ownerAPI.handleMoveTable))
	owner.POST("/changefeed/query", gin.WrapF(ownerAPI.handleChangefeedQuery))
}

func handleOwnerResp(w http.ResponseWriter, err error) {
	if err != nil {
		if errors.Cause(err) == concurrency.ErrElectionNotLeader {
			api.WriteError(w, http.StatusBadRequest, err)
			return
		}
		api.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	api.WriteData(w, commonResp{Status: true})
}

func (h *ownerAPI) handleResignOwner(w http.ResponseWriter, req *http.Request) {
	if h.capture == nil {
		// for test only
		handleOwnerResp(w, concurrency.ErrElectionNotLeader)
		return
	}
	o, err := h.capture.GetOwner()
	if o != nil {
		o.AsyncStop()
	}
	handleOwnerResp(w, err)
}

func (h *ownerAPI) handleChangefeedAdmin(w http.ResponseWriter, req *http.Request) {
	if h.capture == nil {
		// for test only
		handleOwnerResp(w, concurrency.ErrElectionNotLeader)
		return
	}

	err := req.ParseForm()
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	typeStr := req.Form.Get(api.OpVarAdminJob)
	typ, err := strconv.ParseInt(typeStr, 10, 64)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid admin job type: %s", typeStr))
		return
	}
	job := model.AdminJob{
		CfID: model.DefaultChangeFeedID(req.Form.Get(api.OpVarChangefeedID)),
		Type: model.AdminJobType(typ),
	}

	err = api.HandleOwnerJob(req.Context(), h.capture, job)
	handleOwnerResp(w, err)
}

func (h *ownerAPI) handleRebalanceTrigger(w http.ResponseWriter, req *http.Request) {
	if h.capture == nil {
		// for test only
		handleOwnerResp(w, concurrency.ErrElectionNotLeader)
		return
	}

	err := req.ParseForm()
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	changefeedID := model.DefaultChangeFeedID(req.Form.Get(api.OpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		api.WriteError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed id: %s", changefeedID.ID))
		return
	}

	err = api.HandleOwnerBalance(req.Context(), h.capture, changefeedID)
	handleOwnerResp(w, err)
}

func (h *ownerAPI) handleMoveTable(w http.ResponseWriter, req *http.Request) {
	if h.capture == nil {
		// for test only
		handleOwnerResp(w, concurrency.ErrElectionNotLeader)
		return
	}

	err := req.ParseForm()
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError,
			cerror.WrapError(cerror.ErrInternalServerError, err))
		return
	}
	changefeedID := model.DefaultChangeFeedID(req.Form.Get(api.OpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		api.WriteError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed id: %s", changefeedID.ID))
		return
	}
	to := req.Form.Get(api.OpVarTargetCaptureID)
	if err := model.ValidateChangefeedID(to); err != nil {
		api.WriteError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid target capture id: %s", to))
		return
	}
	tableIDStr := req.Form.Get(api.OpVarTableID)
	tableID, err := strconv.ParseInt(tableIDStr, 10, 64)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid tableID: %s", tableIDStr))
		return
	}

	err = api.HandleOwnerScheduleTable(
		req.Context(), h.capture, changefeedID, to, tableID)
	handleOwnerResp(w, err)
}

func (h *ownerAPI) handleChangefeedQuery(w http.ResponseWriter, req *http.Request) {
	if h.capture == nil {
		// for test only
		handleOwnerResp(w, concurrency.ErrElectionNotLeader)
		return
	}

	err := req.ParseForm()
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	changefeedID := model.DefaultChangeFeedID(req.Form.Get(api.OpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		api.WriteError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed id: %s", changefeedID.ID))
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfInfo, err := h.capture.GetEtcdClient().GetChangeFeedInfo(ctx, changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		api.WriteError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed id: %s", changefeedID))
		return
	}
	cfStatus, _, err := h.capture.GetEtcdClient().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		api.WriteError(w, http.StatusBadRequest, err)
		return
	}

	resp := &ChangefeedResp{}
	if cfInfo != nil {
		resp.FeedState = string(cfInfo.State)
		resp.RunningError = cfInfo.Error
	}
	if cfStatus != nil {
		resp.TSO = cfStatus.CheckpointTs
		tm := oracle.GetTimeFromTS(cfStatus.CheckpointTs)
		resp.Checkpoint = tm.Format("2006-01-02 15:04:05.000")
	}
	api.WriteData(w, resp)
}

// HandleAdminLogLevel handles requests to set the log level.
func HandleAdminLogLevel(w http.ResponseWriter, r *http.Request) {
	var level string
	data, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		api.WriteError(w, http.StatusInternalServerError, err)
		return
	}
	err = json.Unmarshal(data, &level)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("invalid log level: %s", err))
		return
	}

	err = logutil.SetLogLevel(level)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest,
			cerror.ErrAPIInvalidParam.GenWithStack("fail to change log level: %s", err))
		return
	}
	log.Warn("log level changed", zap.String("level", level))

	api.WriteData(w, struct{}{})
}
