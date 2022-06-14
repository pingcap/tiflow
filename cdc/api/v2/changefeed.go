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
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

const apiOpVarChangefeedID = "changefeed_id"

// CreateChangefeed handles create changefeed request,
// it returns the changefeed's changefeedInfo that it just created
func (h *OpenAPIV2) CreateChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		api.ForwardToOwner(c, h.capture)
		return
	}

	ctx := c.Request.Context()

	config := &ChangefeedConfig{ReplicaConfig: GetDefaultReplicaConfig()}

	if err := c.BindJSON(&config); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	if len(config.PDAddrs) == 0 {
		up := h.capture.UpstreamManager.GetDefaultUpstream()
		config.PDAddrs = up.PdEndpoints
		config.KeyPath = up.SecurityConfig.KeyPath
		config.CAPath = up.SecurityConfig.CAPath
		config.CertPath = up.SecurityConfig.CertPath
	}

	info, err := verifyCreateChangefeedConfig(ctx, config, h.capture)
	if err != nil {
		_ = c.Error(err)
		return
	}
	upstreamInfo := &model.UpstreamInfo{
		ID:            info.UpstreamID,
		PDEndpoints:   strings.Join(config.PDAddrs, ","),
		KeyPath:       config.KeyPath,
		CertPath:      config.CertPath,
		CAPath:        config.CAPath,
		CertAllowedCN: config.CertAllowedCN,
	}
	infoStr, err := info.Marshal()
	if err != nil {
		_ = c.Error(err)
		return
	}

	err = h.capture.EtcdClient.CreateChangefeedInfo(ctx,
		upstreamInfo,
		info,
		model.DefaultChangeFeedID(info.ID))
	if err != nil {
		_ = c.Error(err)
		return
	}

	log.Info("Create changefeed successfully!",
		zap.String("id", info.ID),
		zap.String("changefeed", infoStr))
	c.JSON(http.StatusOK, info)
}

// VerifyTable verify table, return ineligibleTables and EligibleTables.
func (h *OpenAPIV2) VerifyTable(c *gin.Context) {
	cfg := getDefaultVerifyTableConfig()
	if err := c.BindJSON(cfg); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	if len(cfg.PDAddrs) == 0 {
		up := h.capture.UpstreamManager.GetDefaultUpstream()
		cfg.PDAddrs = up.PdEndpoints
		cfg.KeyPath = up.SecurityConfig.KeyPath
		cfg.CAPath = up.SecurityConfig.CAPath
		cfg.CertPath = up.SecurityConfig.CertPath
	}
	credential := &security.Credential{
		CAPath:        cfg.CAPath,
		CertPath:      cfg.CertPath,
		KeyPath:       cfg.KeyPath,
		CertAllowedCN: make([]string, 0),
	}
	if len(cfg.CertAllowedCN) != 0 {
		credential.CertAllowedCN = cfg.CertAllowedCN
	}

	kvStore, err := kv.CreateTiStore(strings.Join(cfg.PDAddrs, ","), credential)
	if err != nil {
		_ = c.Error(err)
		return
	}
	replicaCfg := cfg.ReplicaConfig.ToInternalReplicaConfig()
	ineligibleTables, eligibleTables, err := entry.VerifyTables(replicaCfg, kvStore, cfg.StartTs)
	if err != nil {
		_ = c.Error(err)
		return
	}
	toAPIModelFunc := func(tbls []model.TableName) []TableName {
		var apiModles []TableName
		for _, tbl := range tbls {
			apiModles = append(apiModles, TableName{
				Schema:      tbl.Schema,
				Table:       tbl.Table,
				TableID:     tbl.TableID,
				IsPartition: tbl.IsPartition,
			})
		}
		return apiModles
	}
	tables := &Tables{
		IneligibleTables: toAPIModelFunc(ineligibleTables),
		EligibleTables:   toAPIModelFunc(eligibleTables),
	}
	c.JSON(http.StatusOK, tables)
}

// UpdateChangefeed handles update changefeed request,
// it returns the updated changefeedInfo
// Can only update a changefeed's: TargetTs, SinkURI,
// ReplicaConfig, PDAddrs, CAPath, CertPath, KeyPath,
// SyncPointEnabled, SyncPointInterval
func (h *OpenAPIV2) UpdateChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		api.ForwardToOwner(c, h.capture)
		return
	}

	ctx := c.Request.Context()

	changefeedID := model.DefaultChangeFeedID(c.Param(apiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}

	cfInfo, err := h.capture.StatusProvider().GetChangeFeedInfo(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	if cfInfo.State != model.StateStopped {
		_ = c.Error(cerror.ErrChangefeedUpdateRefused.
			GenWithStackByArgs("can only update changefeed config when it is stopped"))
		return
	}

	if cfInfo.Namespace == "" {
		cfInfo.Namespace = model.DefaultNamespace
	}
	if cfInfo.UpstreamID == 0 {
		up := h.capture.UpstreamManager.GetDefaultUpstream()
		cfInfo.UpstreamID = up.ID
	}
	upInfo, err := h.capture.EtcdClient.GetUpstreamInfo(ctx, cfInfo.UpstreamID, cfInfo.Namespace)
	if err != nil {
		_ = c.Error(err)
		return
	}

	changefeedConfig := new(ChangefeedConfig)
	changefeedConfig.SyncPointEnabled = cfInfo.SyncPointEnabled
	changefeedConfig.ReplicaConfig = ToAPIReplicaConfig(cfInfo.Config)
	if err = c.BindJSON(&changefeedConfig); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	newCfInfo, newUpInfo, err := verifyUpdateChangefeedConfig(ctx, changefeedConfig, cfInfo, upInfo)
	if err != nil {
		_ = c.Error(err)
		return
	}

	if newCfInfo != nil {
		err = h.capture.EtcdClient.SaveChangeFeedInfo(ctx, newCfInfo, changefeedID)
		if err != nil {
			_ = c.Error(err)
			return
		}
	}

	if newUpInfo != nil {
		err = h.capture.EtcdClient.SaveUpstreamInfo(ctx, upInfo, changefeedID.Namespace)
		if err != nil {
			_ = c.Error(err)
			return
		}
	}

	c.JSON(http.StatusOK, newCfInfo)
}

// GetChangeFeedMetaInfo handles get changefeed's meta info request
// This API for cdc cli use only.
func (h *OpenAPIV2) GetChangeFeedMetaInfo(c *gin.Context) {
	if !h.capture.IsOwner() {
		api.ForwardToOwner(c, h.capture)
		return
	}
	ctx := c.Request.Context()

	changefeedID := model.DefaultChangeFeedID(c.Param(apiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}
	info, err := h.capture.StatusProvider().GetChangeFeedInfo(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, info)
}
