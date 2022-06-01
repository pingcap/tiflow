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
// @Param changefeed body map[string]interface{} true "changefeed info"
// @Success 200 {object} map[string]interface{}
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/changefeeds [post]
func (h *OpenAPIV2) CreateChangefeed(c *gin.Context) {
	ctx := c.Request.Context()

	info, err := model.GetDeFaultChangefeedInfo()
	if err != nil {
		_ = c.Error(err)
		return
	}

	if err := c.BindJSON(info); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.Wrap(err))
		return
	}

	err = validator.VerifyCreateChangefeedInfo(ctx, info, h.capture)
	if err != nil {
		_ = c.Error(err)
		return
	}

	infoStr, err := info.Marshal()
	if err != nil {
		_ = c.Error(err)
		return
	}

	err = h.capture.EtcdClient.CreateChangefeedInfo(ctx, info,
		model.DefaultChangeFeedID(info.ID))
	if err != nil {
		_ = c.Error(err)
		return
	}

	log.Info("Create changefeed successfully!", zap.String("id", info.ID), zap.String("changefeed", infoStr))
	c.Status(http.StatusOK)
}
