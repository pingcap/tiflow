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
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

const apiOpVarChangefeedID = "changefeed_id"

// createChangefeed handles create changefeed request,
// it returns the changefeed's changefeedInfo that it just created
func (h *OpenAPIV2) createChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	cfg := &ChangefeedConfig{ReplicaConfig: GetDefaultReplicaConfig()}

	if err := c.BindJSON(&cfg); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	if len(cfg.PDAddrs) == 0 {
		up, err := getCaptureDefaultUpstream(h.capture)
		if err != nil {
			_ = c.Error(err)
			return
		}
		cfg.PDConfig = getUpstreamPDConfig(up)
	}
	credential := cfg.PDConfig.toCredential()

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pdClient, err := h.helpers.getPDClient(timeoutCtx, cfg.PDAddrs, credential)
	if err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIGetPDClientFailed, err))
		return
	}
	defer pdClient.Close()

	// verify tables todo: del kvstore
	kvStorage, err := h.helpers.createTiStore(cfg.PDAddrs, credential)
	if err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrNewStore, err))
		return
	}
	etcdClient, err := h.capture.GetEtcdClient()
	if err != nil {
		_ = c.Error(err)
		return
	}
	// We should not close kvStorage since all kvStorage in cdc is the same one.
	// defer kvStorage.Close()
	// TODO: We should get a kvStorage from upstream instead of creating a new one
	info, err := h.helpers.verifyCreateChangefeedConfig(
		ctx,
		cfg,
		pdClient,
		h.capture.StatusProvider(),
		etcdClient.GetEnsureGCServiceID(gc.EnsureGCServiceCreating),
		kvStorage)
	if err != nil {
		_ = c.Error(err)
		return
	}
	needRemoveGCSafePoint := false
	defer func() {
		if !needRemoveGCSafePoint {
			return
		}
		err := gc.UndoEnsureChangefeedStartTsSafety(
			ctx,
			pdClient,
			etcdClient.GetEnsureGCServiceID(gc.EnsureGCServiceCreating),
			model.DefaultChangeFeedID(cfg.ID),
		)
		if err != nil {
			_ = c.Error(err)
			return
		}
	}()
	upstreamInfo := &model.UpstreamInfo{
		ID:            info.UpstreamID,
		PDEndpoints:   strings.Join(cfg.PDAddrs, ","),
		KeyPath:       cfg.KeyPath,
		CertPath:      cfg.CertPath,
		CAPath:        cfg.CAPath,
		CertAllowedCN: cfg.CertAllowedCN,
	}
	infoStr, err := info.Marshal()
	if err != nil {
		needRemoveGCSafePoint = true
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	o, err := h.capture.GetOwner()
	if err != nil {
		needRemoveGCSafePoint = true
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	err = o.ValidateChangefeed(info)
	if err != nil {
		needRemoveGCSafePoint = true
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}

	err = etcdClient.CreateChangefeedInfo(ctx,
		upstreamInfo,
		info,
		model.DefaultChangeFeedID(info.ID))
	if err != nil {
		needRemoveGCSafePoint = true
		_ = c.Error(err)
		return
	}

	log.Info("Create changefeed successfully!",
		zap.String("id", info.ID),
		zap.String("changefeed", infoStr))
	c.JSON(http.StatusCreated, toAPIModel(info, true))
}

// verifyTable verify table, return ineligibleTables and EligibleTables.
func (h *OpenAPIV2) verifyTable(c *gin.Context) {
	cfg := getDefaultVerifyTableConfig()
	if err := c.BindJSON(cfg); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	if len(cfg.PDAddrs) == 0 {
		up, err := getCaptureDefaultUpstream(h.capture)
		if err != nil {
			_ = c.Error(err)
			return
		}
		cfg.PDConfig = getUpstreamPDConfig(up)
	}
	credential := cfg.PDConfig.toCredential()

	kvStore, err := h.helpers.createTiStore(cfg.PDAddrs, credential)
	if err != nil {
		_ = c.Error(err)
		return
	}
	replicaCfg := cfg.ReplicaConfig.ToInternalReplicaConfig()
	ineligibleTables, eligibleTables, err := h.helpers.
		getVerfiedTables(replicaCfg, kvStore, cfg.StartTs)
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

// updateChangefeed handles update changefeed request,
// it returns the updated changefeedInfo
// Can only update a changefeed's: TargetTs, SinkURI,
// ReplicaConfig, PDAddrs, CAPath, CertPath, KeyPath,
// SyncPointEnabled, SyncPointInterval
func (h *OpenAPIV2) updateChangefeed(c *gin.Context) {
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
	cfStatus, err := h.capture.StatusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	etcdClient, err := h.capture.GetEtcdClient()
	if err != nil {
		_ = c.Error(err)
		return
	}
	cfInfo.Namespace = changefeedID.Namespace
	cfInfo.ID = changefeedID.ID
	upInfo, err := etcdClient.GetUpstreamInfo(ctx, cfInfo.UpstreamID,
		cfInfo.Namespace)
	if err != nil {
		_ = c.Error(err)
		return
	}

	updateCfConfig := &ChangefeedConfig{}
	updateCfConfig.ReplicaConfig = ToAPIReplicaConfig(cfInfo.Config)
	if err = c.BindJSON(updateCfConfig); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}

	if err = h.helpers.verifyUpstream(ctx, updateCfConfig, cfInfo); err != nil {
		_ = c.Error(errors.Trace(err))
		return
	}

	log.Info("Old ChangeFeed and Upstream Info",
		zap.String("changefeedInfo", cfInfo.String()),
		zap.Any("upstreamInfo", upInfo))

	var pdAddrs []string
	var credentials *security.Credential
	if upInfo != nil {
		pdAddrs = strings.Split(upInfo.PDEndpoints, ",")
		credentials = &security.Credential{
			CAPath:        upInfo.CAPath,
			CertPath:      upInfo.CertPath,
			KeyPath:       upInfo.KeyPath,
			CertAllowedCN: upInfo.CertAllowedCN,
		}
	}
	if len(updateCfConfig.PDAddrs) != 0 {
		pdAddrs = updateCfConfig.PDAddrs
		credentials = updateCfConfig.PDConfig.toCredential()
	}

	storage, err := h.helpers.createTiStore(pdAddrs, credentials)
	if err != nil {
		_ = c.Error(errors.Trace(err))
	}
	newCfInfo, newUpInfo, err := h.helpers.
		verifyUpdateChangefeedConfig(ctx, updateCfConfig, cfInfo, upInfo, storage, cfStatus.CheckpointTs)
	if err != nil {
		_ = c.Error(errors.Trace(err))
		return
	}

	log.Info("New ChangeFeed and Upstream Info",
		zap.String("changefeedInfo", newCfInfo.String()),
		zap.Any("upstreamInfo", newUpInfo))

	err = etcdClient.UpdateChangefeedAndUpstream(ctx, newUpInfo, newCfInfo,
		changefeedID)
	if err != nil {
		_ = c.Error(errors.Trace(err))
		return
	}
	c.JSON(http.StatusOK, toAPIModel(newCfInfo, true))
}

// getChangeFeedMetaInfo returns the metaInfo of a changefeed
func (h *OpenAPIV2) getChangeFeedMetaInfo(c *gin.Context) {
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
	c.JSON(http.StatusOK, toAPIModel(info, false))
}

// resumeChangefeed handles update changefeed request.
func (h *OpenAPIV2) resumeChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedID := model.DefaultChangeFeedID(c.Param(apiOpVarChangefeedID))
	err := model.ValidateChangefeedID(changefeedID.ID)
	if err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}

	_, err = h.capture.StatusProvider().GetChangeFeedInfo(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	cfg := new(ResumeChangefeedConfig)
	if err := c.BindJSON(&cfg); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}

	if len(cfg.PDAddrs) == 0 {
		up, err := getCaptureDefaultUpstream(h.capture)
		if err != nil {
			_ = c.Error(err)
			return
		}
		cfg.PDConfig = getUpstreamPDConfig(up)
	}
	credential := cfg.PDConfig.toCredential()

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pdClient, err := h.helpers.getPDClient(timeoutCtx, cfg.PDAddrs, credential)
	if err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	defer pdClient.Close()

	etcdClient, err := h.capture.GetEtcdClient()
	if err != nil {
		_ = c.Error(err)
		return
	}
	if err := h.helpers.verifyResumeChangefeedConfig(
		ctx,
		pdClient,
		etcdClient.GetEnsureGCServiceID(gc.EnsureGCServiceResuming),
		changefeedID,
		cfg.OverwriteCheckpointTs); err != nil {
		_ = c.Error(err)
		return
	}
	needRemoveGCSafePoint := false
	defer func() {
		if !needRemoveGCSafePoint {
			return
		}
		err := gc.UndoEnsureChangefeedStartTsSafety(
			ctx,
			pdClient,
			etcdClient.GetEnsureGCServiceID(gc.EnsureGCServiceResuming),
			changefeedID,
		)
		if err != nil {
			_ = c.Error(err)
			return
		}
	}()

	job := model.AdminJob{
		CfID:                  changefeedID,
		Type:                  model.AdminResume,
		OverwriteCheckpointTs: cfg.OverwriteCheckpointTs,
	}

	if err := api.HandleOwnerJob(ctx, h.capture, job); err != nil {
		if cfg.OverwriteCheckpointTs > 0 {
			needRemoveGCSafePoint = true
		}
		_ = c.Error(err)
		return
	}
	c.Status(http.StatusOK)
}

func toAPIModel(info *model.ChangeFeedInfo, maskSinkURI bool) *ChangeFeedInfo {
	var runningError *RunningError
	if info.Error != nil {
		runningError = &RunningError{
			Addr:    info.Error.Addr,
			Code:    info.Error.Code,
			Message: info.Error.Message,
		}
	}

	sinkURI := info.SinkURI
	var err error
	if maskSinkURI {
		sinkURI, err = util.MaskSinkURI(sinkURI)
		if err != nil {
			log.Error("failed to mask sink URI", zap.Error(err))
		}
	}

	apiInfoModel := &ChangeFeedInfo{
		UpstreamID:     info.UpstreamID,
		Namespace:      info.Namespace,
		ID:             info.ID,
		SinkURI:        sinkURI,
		CreateTime:     info.CreateTime,
		StartTs:        info.StartTs,
		TargetTs:       info.TargetTs,
		AdminJobType:   info.AdminJobType,
		Engine:         info.Engine,
		Config:         ToAPIReplicaConfig(info.Config),
		State:          info.State,
		Error:          runningError,
		CreatorVersion: info.CreatorVersion,
	}
	return apiInfoModel
}

func getCaptureDefaultUpstream(cp capture.Capture) (*upstream.Upstream, error) {
	upManager, err := cp.GetUpstreamManager()
	if err != nil {
		return nil, errors.Trace(err)
	}
	up, err := upManager.GetDefaultUpstream()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return up, nil
}

func getUpstreamPDConfig(up *upstream.Upstream) PDConfig {
	return PDConfig{
		PDAddrs:  up.PdEndpoints,
		KeyPath:  up.SecurityConfig.KeyPath,
		CAPath:   up.SecurityConfig.CAPath,
		CertPath: up.SecurityConfig.CertPath,
	}
}
