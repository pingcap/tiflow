// Copyright 2023 PingCAP, Inc.
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

package v2

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// getProcessor gets the detailed info of a processor
// @Summary Get processor detail information
// @Description get the detail information of a processor
// @Tags processor,v2
// @Produce json
// @Success 200 {object} ProcessorDetail
// @Failure 500,400 {object} model.HTTPError
// @Param   changefeed_id   path    string  true  "changefeed ID"
// @Param   capture_id   path    string  true  "capture ID"
// @Router	/api/v2/processors/{changefeed_id}/{capture_id} [get]
func (h *OpenAPIV2) getProcessor(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedID := model.DefaultChangeFeedID(c.Param(apiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(
			cerror.ErrAPIInvalidParam.GenWithStack(
				"invalid changefeed_id: %s",
				changefeedID.ID,
			),
		)
		return
	}

	captureID := c.Param(apiOpVarCaptureID)
	if err := model.ValidateChangefeedID(captureID); err != nil {
		_ = c.Error(
			cerror.ErrAPIInvalidParam.GenWithStack(
				"invalid capture_id: %s",
				captureID,
			),
		)
		return
	}

	info, err := h.capture.StatusProvider().GetChangeFeedInfo(
		ctx,
		changefeedID,
	)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if !info.State.IsRunning() {
		_ = c.Error(
			cerror.WrapError(
				cerror.ErrAPIInvalidParam,
				fmt.Errorf("changefeed in abnormal state: %s, "+
					"can'duration get processor of an abnormal changefeed",
					string(info.State),
				),
			),
		)
		return
	}

	// check if this captureID exist
	procInfos, err := h.capture.StatusProvider().GetProcessors(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}
	var found bool
	for _, info := range procInfos {
		if info.CaptureID == captureID {
			found = true
			break
		}
	}
	if !found {
		_ = c.Error(cerror.ErrCaptureNotExist.GenWithStackByArgs(captureID))
		return
	}

	statuses, err := h.capture.StatusProvider().GetAllTaskStatuses(
		ctx,
		changefeedID,
	)
	if err != nil {
		_ = c.Error(err)
		return
	}
	status, captureExist := statuses[captureID]

	// Note: for the case that no tables are attached to a newly created
	// changefeed, we just do not report an error.
	var processorDetail ProcessorDetail
	if captureExist {
		tables := make([]int64, 0)
		for tableID := range status.Tables {
			tables = append(tables, tableID)
		}
		processorDetail.Tables = tables
	}
	c.JSON(http.StatusOK, &processorDetail)
}

// listProcessors lists all processors in the TiCDC cluster
// @Summary List processors
// @Description list all processors in the TiCDC cluster
// @Tags processor,v2
// @Produce json
// @Success 200 {array} ProcessorCommonInfo
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/processors [get]
func (h *OpenAPIV2) listProcessors(c *gin.Context) {
	ctx := c.Request.Context()
	infos, err := h.capture.StatusProvider().GetProcessors(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}
	prcInfos := make([]ProcessorCommonInfo, len(infos))
	for i, info := range infos {
		resp := ProcessorCommonInfo{
			Namespace:    info.CfID.Namespace,
			ChangeFeedID: info.CfID.ID,
			CaptureID:    info.CaptureID,
		}
		prcInfos[i] = resp
	}
	resp := &ListResponse[ProcessorCommonInfo]{
		Total: len(prcInfos),
		Items: prcInfos,
	}

	c.JSON(http.StatusOK, resp)
}
