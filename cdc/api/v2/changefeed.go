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
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// CreateChangefeed handles create changefeed request
func (h *OpenAPIV2) CreateChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		api.ForwardToOwner(c, h.capture)
		return
	}

	ctx := c.Request.Context()

	config := &ChangefeedConfig{ReplicaConfig: GetDefaultReplicaConfig()}

	if err := c.BindJSON(&config); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.Wrap(err))
		return
	}

	info, err := VerifyCreateChangefeedConfig(ctx, config, h.capture)
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
	c.JSON(http.StatusOK, info)
}
