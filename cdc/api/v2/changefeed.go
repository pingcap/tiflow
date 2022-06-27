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
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
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
	credential := &security.Credential{
		CAPath:   config.CAPath,
		CertPath: config.CertPath,
		KeyPath:  config.KeyPath,
	}
	if len(config.CertAllowedCN) != 0 {
		credential.CertAllowedCN = config.CertAllowedCN
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pdClient, err := getPDClient(timeoutCtx, config.PDAddrs, credential)
	if err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	defer pdClient.Close()

	// verify tables
	kvStorage, err := kv.CreateTiStore(strings.Join(config.PDAddrs, ","), credential)
	if err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrInternalServerError, err))
		return
	}
	// We should not close kvStorage since all kvStorage in cdc is the same one.
	// defer kvStorage.Close()

	info, err := verifyCreateChangefeedConfig(
		ctx,
		config,
		pdClient,
		h.capture.StatusProvider(),
		h.capture.EtcdClient.GetEnsureGCServiceID(gc.EnsureGCServiceCreating),
		kvStorage)
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
	c.JSON(http.StatusCreated, toAPIModel(info))
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

	if err := h.verifyUpstream(ctx, changefeedConfig, cfInfo); err != nil {
		return
	}

	log.Info("Old ChangeFeed and Upstream Info",
		zap.String("changefeedInfo", cfInfo.String()),
		zap.Any("upstreamInfo", upInfo))

	newCfInfo, newUpInfo, err := verifyUpdateChangefeedConfig(ctx, changefeedConfig, cfInfo, upInfo)
	if err != nil {
		_ = c.Error(err)
		return
	}

	log.Info("New ChangeFeed and Upstream Info",
		zap.String("changefeedInfo", newCfInfo.String()),
		zap.Any("upstreamInfo", newUpInfo))

	err = h.capture.EtcdClient.UpdateChangefeedAndUpstream(ctx, newUpInfo, newCfInfo, changefeedID)
	if err != nil {
		_ = c.Error(err)
	}

	c.JSON(http.StatusOK, newCfInfo)
}

func (h *OpenAPIV2) verifyUpstream(ctx context.Context,
	changefeedConfig *ChangefeedConfig,
	cfInfo *model.ChangeFeedInfo,
) error {
	if len(changefeedConfig.PDAddrs) != 0 {
		// check if the upstream cluster id changed
		timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		pd, err := getPDClient(timeoutCtx, changefeedConfig.PDAddrs, &security.Credential{
			CAPath:        changefeedConfig.CAPath,
			CertPath:      changefeedConfig.CertPath,
			KeyPath:       changefeedConfig.KeyPath,
			CertAllowedCN: changefeedConfig.CertAllowedCN,
		})
		if err != nil {
			return err
		}
		defer pd.Close()
		if pd.GetClusterID(ctx) != cfInfo.UpstreamID {
			return cerror.ErrUpstreamMissMatch.
				GenWithStackByArgs(cfInfo.UpstreamID, pd.GetClusterID(ctx))
		}
	}
	return nil
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
	c.JSON(http.StatusOK, toAPIModel(info))
}

// ResumeChangefeed handles update changefeed request.
func (h *OpenAPIV2) ResumeChangefeed(c *gin.Context) {
	if !h.capture.IsOwner() {
		api.ForwardToOwner(c, h.capture)
		return
	}

	ctx := c.Request.Context()
	changefeedID := model.DefaultChangeFeedID(c.Param(apiOpVarChangefeedID))
	err := model.ValidateChangefeedID(changefeedID.ID)
	if err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}

	var overwriteCheckpointTs uint64
	overwriteCheckpointTsStr, ok := c.GetQuery("overwrite_checkpoint_ts")
	if ok {
		overwriteCheckpointTs, err = strconv.ParseUint(overwriteCheckpointTsStr, 10, 64)
		if err != nil {
			_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid start_ts:% s",
				overwriteCheckpointTsStr))
			return
		}
	}

	cfInfo, err := h.capture.StatusProvider().GetChangeFeedInfo(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	var upstream *upstream.Upstream
	if cfInfo.UpstreamID == 0 {
		upstream = h.capture.UpstreamManager.GetDefaultUpstream()
	} else {
		upstream, ok = h.capture.UpstreamManager.Get(cfInfo.UpstreamID)
		if !ok {
			_ = c.Error(cerror.WrapError(cerror.ErrUpstreamNotFound,
				fmt.Errorf("upstream %d not found", cfInfo.UpstreamID)))
			return
		}
	}

	if err := verifyResumeChangefeed(
		ctx,
		h.capture,
		upstream,
		changefeedID,
		overwriteCheckpointTs); err != nil {
		_ = c.Error(err)
		return
	}

	job := model.AdminJob{
		CfID:                  changefeedID,
		Type:                  model.AdminResume,
		OverwriteCheckpointTs: overwriteCheckpointTs,
	}

	if err := api.HandleOwnerJob(ctx, h.capture, job); err != nil {
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusAccepted)
}

func toAPIModel(info *model.ChangeFeedInfo) *ChangeFeedInfo {
	var runningError *RunningError
	if info.Error != nil {
		runningError = &RunningError{
			Addr:    info.Error.Addr,
			Code:    info.Error.Code,
			Message: info.Error.Message,
		}
	}
	apiInfoModel := &ChangeFeedInfo{
		UpstreamID:        info.UpstreamID,
		Namespace:         info.Namespace,
		ID:                info.ID,
		SinkURI:           info.SinkURI,
		CreateTime:        info.CreateTime,
		StartTs:           info.StartTs,
		TargetTs:          info.TargetTs,
		AdminJobType:      info.AdminJobType,
		Engine:            info.Engine,
		Config:            ToAPIReplicaConfig(info.Config),
		State:             info.State,
		Error:             runningError,
		SyncPointEnabled:  info.SyncPointEnabled,
		SyncPointInterval: info.SyncPointInterval,
		CreatorVersion:    info.CreatorVersion,
	}
	return apiInfoModel
}
