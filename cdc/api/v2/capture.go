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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/api/middleware"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const apiOpVarCaptureID = "capture_id"

// drainCapture remove all tables at the given capture.
func (h *OpenAPIV2) drainCapture(c *gin.Context) {
	if !h.capture.IsOwner() {
		middleware.ForwardToOwnerMiddleware(h.capture)(c)
		return
	}
	captureID := c.Param(apiOpVarCaptureID)

	ctx := c.Request.Context()
	captures, err := h.capture.StatusProvider().GetCaptures(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}

	// drain capture only work if there is at least two alive captures,
	// it cannot work properly if it has only one capture.
	if len(captures) <= 1 {
		_ = c.Error(cerror.ErrSchedulerRequestFailed.
			GenWithStackByArgs("only one capture alive"))
		return
	}

	target := captureID
	checkCaptureFound := func() bool {
		// make sure the target capture exist
		for _, capture := range captures {
			if capture.ID == target {
				return true
			}
		}
		return false
	}

	if !checkCaptureFound() {
		_ = c.Error(cerror.ErrCaptureNotExist.GenWithStackByArgs(target))
		return
	}

	// only owner handle api request, so this must be the owner.
	ownerInfo, err := h.capture.Info()
	if err != nil {
		_ = c.Error(err)
		return
	}

	if ownerInfo.ID == target {
		_ = c.Error(cerror.ErrSchedulerRequestFailed.
			GenWithStackByArgs("cannot drain the owner"))
		return
	}

	resp, err := api.HandleOwnerDrainCapture(ctx, h.capture, target)
	if err != nil {
		_ = c.AbortWithError(http.StatusServiceUnavailable, err)
		return
	}

	c.JSON(http.StatusAccepted, resp)
}

// drainCapture remove all tables at the given capture.
func (h *OpenAPIV2) listCaptures(c *gin.Context) {
	ctx := c.Request.Context()
	captureInfos, err := h.capture.StatusProvider().GetCaptures(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}
	info, err := h.capture.Info()
	if err != nil {
		_ = c.Error(err)
		return
	}
	ownerID := info.ID

	etcdClient, err := h.capture.GetEtcdClient()
	if err != nil {
		_ = c.Error(err)
		return
	}

	captures := make([]model.Capture, 0, len(captureInfos))
	for _, c := range captureInfos {
		isOwner := c.ID == ownerID
		captures = append(captures,
			model.Capture{
				ID:            c.ID,
				IsOwner:       isOwner,
				AdvertiseAddr: c.AdvertiseAddr,
				ClusterID:     etcdClient.GetClusterID(),
			})
	}
	resp := &ListResponse[model.Capture]{
		Total: len(captureInfos),
		Items: captures,
	}
	c.JSON(http.StatusOK, resp)
}
