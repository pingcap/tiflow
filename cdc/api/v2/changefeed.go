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

package v2

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/ngaut/log"
	"github.com/pingcap/tiflow/cdc/api/validator"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// CreateChangefeed creates a changefeed
// @Summary Create changefeed
// @Description create a new changefeed
// @Tags changefeed
// @Accept json
// @Produce json
// @Param changefeed body model.ChangeFeedInfo true "changefeed config"
// @Success 200 {object} model.ChangeFeedInfo
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/changefeeds [post]
func (h *OpenAPIV2) CreateChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	var info model.ChangeFeedInfo
	if err := c.BindJSON(&info); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.Wrap(err))
		return
	}
	verifiedInfo, err := validator.VerifyCreateChangefeedInfo(ctx, info, h.capture)
	if err != nil {
		_ = c.Error(err)
		return
	}

	infoStr, err := info.Marshal()
	if err != nil {
		_ = c.Error(err)
		return
	}

	err = h.capture.EtcdClient.CreateChangefeedInfo(ctx, verifiedInfo,
		model.DefaultChangeFeedID(verifiedInfo.ID))
	if err != nil {
		_ = c.Error(err)
		return
	}

	log.Info("Create changefeed successfully!", zap.String("id", verifiedInfo.ID), zap.String("changefeed", infoStr))
	c.Status(http.StatusOK)
}

// VerifyTable verify tables of a changefeed
// @Summary Verify tables
// @Description Verify all tables of a changefeed, check if these tables are ineligible or eligible
// @Tags changefeed
// @Accept json
// @Produce json
// @Param replicaConfig body config.replicaConfig true "replicaConfig config"
// @Success 200 {object} Tables
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/verify-table [post]
func (h *OpenAPIV2) VerifyTable(c *gin.Context) {

}
