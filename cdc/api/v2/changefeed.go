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
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
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
		up := h.capture.GetUpstreamManager().GetDefaultUpstream()
		cfg.PDAddrs = up.PdEndpoints
		cfg.KeyPath = up.SecurityConfig.KeyPath
		cfg.CAPath = up.SecurityConfig.CAPath
		cfg.CertPath = up.SecurityConfig.CertPath
	}
	credential := &security.Credential{
		CAPath:   cfg.CAPath,
		CertPath: cfg.CertPath,
		KeyPath:  cfg.KeyPath,
	}
	if len(cfg.CertAllowedCN) != 0 {
		credential.CertAllowedCN = cfg.CertAllowedCN
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pdClient, err := h.helpers.getPDClient(timeoutCtx, cfg.PDAddrs, credential)
	if err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	defer pdClient.Close()

	// verify tables todo: del kvstore
	kvStorage, err := h.helpers.callKVCreateTiStore(cfg.PDAddrs, credential)
	if err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrInternalServerError, err))
		return
	}
	// We should not close kvStorage since all kvStorage in cdc is the same one.
	// defer kvStorage.Close()
	// TODO: We should get a kvStorage from upstream instead of creating a new one
	info, err := h.helpers.verifyCreateChangefeedConfig(ctx, cfg, pdClient,
		h.capture.StatusProvider(), h.capture.GetEtcdClient().GetEnsureGCServiceID(),
		kvStorage)
	if err != nil {
		_ = c.Error(err)
		return
	}
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
		_ = c.Error(err)
		return
	}

	err = h.capture.GetEtcdClient().CreateChangefeedInfo(ctx,
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

// verifyTable verify table, return ineligibleTables and EligibleTables.
func (h *OpenAPIV2) verifyTable(c *gin.Context) {
	cfg := getDefaultVerifyTableConfig()
	if err := c.BindJSON(cfg); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}
	if len(cfg.PDAddrs) == 0 {
		up := h.capture.GetUpstreamManager().GetDefaultUpstream()
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

	kvStore, err := h.helpers.callKVCreateTiStore(cfg.PDAddrs, credential)
	if err != nil {
		_ = c.Error(err)
		return
	}
	replicaCfg := cfg.ReplicaConfig.ToInternalReplicaConfig()
	ineligibleTables, eligibleTables, err := h.helpers.
		callEntryVerifTables(replicaCfg, kvStore, cfg.StartTs)
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
	upInfo, err := h.capture.GetEtcdClient().
		GetUpstreamInfo(ctx, cfInfo.UpstreamID, cfInfo.Namespace)
	if err != nil {
		_ = c.Error(err)
		return
	}

	updateCfConfig := &ChangefeedConfig{}
	updateCfConfig.SyncPointEnabled = cfInfo.SyncPointEnabled
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

	newCfInfo, newUpInfo, err := h.helpers.
		verifyUpdateChangefeedConfig(ctx, updateCfConfig, cfInfo, upInfo)
	if err != nil {
		_ = c.Error(errors.Trace(err))
		return
	}

	log.Info("New ChangeFeed and Upstream Info",
		zap.String("changefeedInfo", newCfInfo.String()),
		zap.Any("upstreamInfo", newUpInfo))

	err = h.capture.GetEtcdClient().
		UpdateChangefeedAndUpstream(ctx, newUpInfo, newCfInfo, changefeedID)
	if err != nil {
		_ = c.Error(errors.Trace(err))
		return
	}
	c.JSON(http.StatusOK, newCfInfo)
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
	c.JSON(http.StatusOK, toAPIModel(info))
}

// getPDClient returns a PDClient given the PD cluster addresses and a credential
func (APIV2HelpersImpl) getPDClient(ctx context.Context,
	pdAddrs []string,
	credential *security.Credential,
) (pd.Client, error) {
	grpcTLSOption, err := credential.ToGRPCDialOption()
	if err != nil {
		return nil, errors.Trace(err)
	}

	pdClient, err := pd.NewClientWithContext(
		ctx, pdAddrs, credential.PDSecurityOption(),
		pd.WithGRPCDialOptions(
			grpcTLSOption,
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		))
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrAPIGetPDClientFailed, errors.Trace(err))
	}
	return pdClient, nil
}

// callKVCreateTiStore wrap the callKVCreateTiStore method to increase testability
func (h APIV2HelpersImpl) callKVCreateTiStore(pdAddrs []string,
	credential *security.Credential,
) (tidbkv.Storage, error) {
	return kv.CreateTiStore(strings.Join(pdAddrs, ","), credential)
}

func (h APIV2HelpersImpl) callEntryVerifTables(replicaConfig *config.ReplicaConfig,
	storage tidbkv.Storage, startTs uint64) (ineligibleTables,
	eligibleTables []model.TableName, err error,
) {
	return entry.VerifyTables(replicaConfig, storage, startTs)
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
