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
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	// apiOpVarChangefeedState is the key of changefeed state in HTTP API
	apiOpVarChangefeedState = "state"
	// apiOpVarChangefeedID is the key of changefeed ID in HTTP API
	apiOpVarChangefeedID = "changefeed_id"
	// timeout for pd client
	timeout = 30 * time.Second
)

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
	// We should not close kvStorage since all kvStorage in cdc is the same one.
	// defer kvStorage.Close()
	// TODO: We should get a kvStorage from upstream instead of creating a new one
	info, err := h.helpers.verifyCreateChangefeedConfig(
		ctx,
		cfg,
		pdClient,
		h.capture.StatusProvider(),
		h.capture.GetEtcdClient().GetEnsureGCServiceID(gc.EnsureGCServiceCreating),
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
			h.capture.GetEtcdClient().GetEnsureGCServiceID(gc.EnsureGCServiceCreating),
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
	o, err := h.capture.GetOwner()
	// cannot create changefeed if there are running lightning/restore tasks
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

	err = h.capture.GetEtcdClient().CreateChangefeedInfo(ctx, upstreamInfo, info)
	if err != nil {
		needRemoveGCSafePoint = true
		_ = c.Error(err)
		return
	}

	log.Info("Create changefeed successfully!",
		zap.String("id", info.ID),
		zap.String("changefeed", info.String()))
	c.JSON(http.StatusOK, toAPIModel(info,
		info.StartTs, info.StartTs,
		nil, true))
}

// listChangeFeeds lists all changgefeeds in cdc cluster
// @Summary List changefeed
// @Description list all changefeeds in cdc cluster
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param state query string false "state"
// @Success 200 {array} ChangefeedCommonInfo
// @Failure 500 {object} model.HTTPError
// @Router /api/v2/changefeeds [get]
func (h *OpenAPIV2) listChangeFeeds(c *gin.Context) {
	ctx := c.Request.Context()
	state := c.Query(apiOpVarChangefeedState)
	statuses, err := h.capture.StatusProvider().GetAllChangeFeedStatuses(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}

	infos, err := h.capture.StatusProvider().GetAllChangeFeedInfo(ctx)
	if err != nil {
		_ = c.Error(err)
		return
	}

	commonInfos := make([]ChangefeedCommonInfo, 0)
	changefeeds := make([]model.ChangeFeedID, 0)

	for cfID := range statuses {
		changefeeds = append(changefeeds, cfID)
	}
	sort.Slice(changefeeds, func(i, j int) bool {
		if changefeeds[i].Namespace == changefeeds[j].Namespace {
			return changefeeds[i].ID < changefeeds[j].ID
		}

		return changefeeds[i].Namespace < changefeeds[j].Namespace
	})

	for _, cfID := range changefeeds {
		cfInfo, exist := infos[cfID]
		if !exist {
			continue
		}
		cfStatus := statuses[cfID]

		if !cfInfo.State.IsNeeded(state) {
			// if the value of `state` is not 'all', only return changefeed
			// with state 'normal', 'stopped', 'failed'
			continue
		}

		// return the common info only.
		commonInfo := &ChangefeedCommonInfo{
			UpstreamID: cfInfo.UpstreamID,
			Namespace:  cfID.Namespace,
			ID:         cfID.ID,
			FeedState:  cfInfo.State,
		}

		if cfInfo.Error != nil {
			commonInfo.RunningError = cfInfo.Error
		} else {
			commonInfo.RunningError = cfInfo.Warning
		}
		log.Info("List changefeed successfully!", zap.Any("runningError", commonInfo.RunningError))

		// if the state is normal, we shall not return the error info
		// because changefeed will is retrying. errors will confuse the users
		if commonInfo.FeedState == model.StateNormal {
			commonInfo.RunningError = nil
		}

		if cfStatus != nil {
			commonInfo.CheckpointTSO = cfStatus.CheckpointTs
			tm := oracle.GetTimeFromTS(cfStatus.CheckpointTs)
			commonInfo.CheckpointTime = model.JSONTime(tm)
		}

		commonInfos = append(commonInfos, *commonInfo)
	}
	resp := &ListResponse[ChangefeedCommonInfo]{
		Total: len(commonInfos),
		Items: commonInfos,
	}

	c.JSON(http.StatusOK, resp)
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

	oldCfInfo, err := h.capture.StatusProvider().GetChangeFeedInfo(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}
	switch oldCfInfo.State {
	case model.StateStopped, model.StateFailed:
	default:
		_ = c.Error(
			cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs(
				"can only update changefeed config when it is stopped or failed",
			),
		)
		return
	}
	cfStatus, err := h.capture.StatusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	etcdClient := h.capture.GetEtcdClient()
	oldCfInfo.Namespace = changefeedID.Namespace
	oldCfInfo.ID = changefeedID.ID
	OldUpInfo, err := etcdClient.GetUpstreamInfo(ctx, oldCfInfo.UpstreamID,
		oldCfInfo.Namespace)
	if err != nil {
		_ = c.Error(err)
		return
	}

	updateCfConfig := &ChangefeedConfig{}
	if err = c.BindJSON(updateCfConfig); err != nil {
		_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
		return
	}

	if err = h.helpers.verifyUpstream(ctx, updateCfConfig, oldCfInfo); err != nil {
		_ = c.Error(errors.Trace(err))
		return
	}

	log.Info("Old ChangeFeed and Upstream Info",
		zap.String("changefeedInfo", oldCfInfo.String()),
		zap.Any("upstreamInfo", OldUpInfo))

	upManager, err := h.capture.GetUpstreamManager()
	if err != nil {
		_ = c.Error(err)
		return
	}

	var storage tidbkv.Storage
	// if PDAddrs is not empty, use it to create a new kvstore
	// Note: upManager is nil in some unit test cases
	if len(updateCfConfig.PDAddrs) != 0 || upManager == nil {
		pdAddrs := updateCfConfig.PDAddrs
		credentials := updateCfConfig.PDConfig.toCredential()
		storage, err = h.helpers.createTiStore(pdAddrs, credentials)
		if err != nil {
			_ = c.Error(errors.Trace(err))
		}
	} else { // get the upstream of the changefeed to get the kvstore
		up, ok := upManager.Get(oldCfInfo.UpstreamID)
		if !ok {
			_ = c.Error(errors.New(fmt.Sprintf("upstream %d not found", oldCfInfo.UpstreamID)))
			return
		}
		storage = up.KVStorage
	}

	newCfInfo, newUpInfo, err := h.helpers.verifyUpdateChangefeedConfig(ctx,
		updateCfConfig, oldCfInfo, OldUpInfo, storage, cfStatus.CheckpointTs)
	if err != nil {
		_ = c.Error(errors.Trace(err))
		return
	}

	log.Info("New ChangeFeed and Upstream Info",
		zap.String("changefeedInfo", newCfInfo.String()),
		zap.Any("upstreamInfo", newUpInfo))

	err = h.capture.GetEtcdClient().
		UpdateChangefeedAndUpstream(ctx, newUpInfo, newCfInfo)
	if err != nil {
		_ = c.Error(errors.Trace(err))
		return
	}
	c.JSON(http.StatusOK, toAPIModel(newCfInfo,
		cfStatus.ResolvedTs, cfStatus.CheckpointTs, nil, true))
}

// getChangefeed get detailed info of a changefeed
// @Summary Get changefeed
// @Description get detail information of a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Success 200 {object} ChangeFeedInfo
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v2/changefeeds/{changefeed_id} [get]
func (h *OpenAPIV2) getChangeFeed(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedID := model.DefaultChangeFeedID(c.Param(apiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(
			cerror.ErrAPIInvalidParam.GenWithStack(
				"invalid changefeed_id: %s",
				changefeedID.ID,
			))
		return
	}
	cfInfo, err := h.capture.StatusProvider().GetChangeFeedInfo(
		ctx,
		changefeedID,
	)
	if err != nil {
		_ = c.Error(err)
		return
	}

	status, err := h.capture.StatusProvider().GetChangeFeedStatus(
		ctx,
		changefeedID,
	)
	if err != nil {
		_ = c.Error(err)
		return
	}

	taskStatus := make([]model.CaptureTaskStatus, 0)
	if cfInfo.State == model.StateNormal {
		processorInfos, err := h.capture.StatusProvider().GetAllTaskStatuses(
			ctx,
			changefeedID,
		)
		if err != nil {
			_ = c.Error(err)
			return
		}
		for captureID, status := range processorInfos {
			tables := make([]int64, 0)
			for tableID := range status.Tables {
				tables = append(tables, tableID)
			}
			taskStatus = append(taskStatus,
				model.CaptureTaskStatus{
					CaptureID: captureID, Tables: tables,
					Operation: status.Operation,
				})
		}
	}
	detail := toAPIModel(cfInfo, status.ResolvedTs,
		status.CheckpointTs, taskStatus, true)
	c.JSON(http.StatusOK, detail)
}

// deleteChangefeed handles delete changefeed request
// RemoveChangefeed removes a changefeed
// @Summary Remove a changefeed
// @Description Remove a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id path string true "changefeed_id"
// @Success 200 {object} EmptyResponse
// @Failure 500,400 {object} model.HTTPError
// @Router	/api/v2/changefeeds/{changefeed_id} [delete]
func (h *OpenAPIV2) deleteChangefeed(c *gin.Context) {
	ctx := c.Request.Context()
	changefeedID := model.DefaultChangeFeedID(c.Param(apiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}
	_, err := h.capture.StatusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		if cerror.ErrChangeFeedNotExists.Equal(err) {
			c.JSON(http.StatusOK, &EmptyResponse{})
			return
		}
		_ = c.Error(err)
		return
	}

	job := model.AdminJob{
		CfID: changefeedID,
		Type: model.AdminRemove,
	}

	if err := api.HandleOwnerJob(ctx, h.capture, job); err != nil {
		_ = c.Error(err)
		return
	}

	// Owner needs at least two ticks to remove a changefeed,
	// we need to wait for it.
	err = retry.Do(ctx, func() error {
		_, err := h.capture.StatusProvider().GetChangeFeedStatus(ctx, changefeedID)
		if err != nil {
			if strings.Contains(err.Error(), "ErrChangeFeedNotExists") {
				return nil
			}
			return err
		}
		return cerror.ErrChangeFeedDeletionUnfinished.GenWithStackByArgs(changefeedID)
	},
		retry.WithMaxTries(100),         // max retry duration is 1 minute
		retry.WithBackoffBaseDelay(600), // default owner tick interval is 200ms
		retry.WithIsRetryableErr(cerror.IsRetryableError))

	if err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, &EmptyResponse{})
}

// todo: remove this API
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
	status, err := h.capture.StatusProvider().GetChangeFeedStatus(
		ctx,
		changefeedID,
	)
	if err != nil {
		_ = c.Error(err)
		return
	}
	taskStatus := make([]model.CaptureTaskStatus, 0)
	if info.State == model.StateNormal {
		processorInfos, err := h.capture.StatusProvider().GetAllTaskStatuses(
			ctx,
			changefeedID,
		)
		if err != nil {
			_ = c.Error(err)
			return
		}
		for captureID, status := range processorInfos {
			tables := make([]int64, 0)
			for tableID := range status.Tables {
				tables = append(tables, tableID)
			}
			taskStatus = append(taskStatus,
				model.CaptureTaskStatus{
					CaptureID: captureID, Tables: tables,
					Operation: status.Operation,
				})
		}
	}
	c.JSON(http.StatusOK, toAPIModel(info, status.ResolvedTs, status.CheckpointTs,
		taskStatus, false))
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
	status, err := h.capture.StatusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	var pdClient pd.Client
	// if PDAddrs is empty, use the default pdClient
	if len(cfg.PDAddrs) == 0 {
		up, err := getCaptureDefaultUpstream(h.capture)
		if err != nil {
			_ = c.Error(err)
			return
		}
		pdClient = up.PDClient
	} else {
		credential := cfg.PDConfig.toCredential()
		timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		pdClient, err = h.helpers.getPDClient(timeoutCtx, cfg.PDAddrs, credential)
		if err != nil {
			_ = c.Error(cerror.WrapError(cerror.ErrAPIInvalidParam, err))
			return
		}
		defer pdClient.Close()
	}
	// If there is no overrideCheckpointTs, then check whether the currentCheckpointTs is smaller than gc safepoint or not.
	newCheckpointTs := status.CheckpointTs
	if cfg.OverwriteCheckpointTs != 0 {
		newCheckpointTs = cfg.OverwriteCheckpointTs
	}
	if err := h.helpers.verifyResumeChangefeedConfig(
		ctx,
		pdClient,
		h.capture.GetEtcdClient().GetEnsureGCServiceID(gc.EnsureGCServiceResuming),
		changefeedID,
		newCheckpointTs); err != nil {
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
			h.capture.GetEtcdClient().GetEnsureGCServiceID(gc.EnsureGCServiceResuming),
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
		needRemoveGCSafePoint = true
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, &EmptyResponse{})
}

// pauseChangefeed handles pause changefeed request
// PauseChangefeed pauses a changefeed
// @Summary Pause a changefeed
// @Description Pause a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Success 200 {object} EmptyResponse
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v2/changefeeds/{changefeed_id}/pause [post]
func (h *OpenAPIV2) pauseChangefeed(c *gin.Context) {
	ctx := c.Request.Context()

	changefeedID := model.DefaultChangeFeedID(c.Param(apiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}
	// check if the changefeed exists
	_, err := h.capture.StatusProvider().GetChangeFeedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	job := model.AdminJob{
		CfID: changefeedID,
		Type: model.AdminStop,
	}

	if err := api.HandleOwnerJob(ctx, h.capture, job); err != nil {
		_ = c.Error(err)
		return
	}
	c.JSON(http.StatusOK, &EmptyResponse{})
}

func (h *OpenAPIV2) status(c *gin.Context) {
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
	status, err := h.capture.StatusProvider().GetChangeFeedStatus(
		ctx,
		changefeedID,
	)
	if err != nil {
		_ = c.Error(err)
		return
	}
	var lastError *RunningError
	if info.Error != nil &&
		oracle.GetTimeFromTS(status.CheckpointTs).Before(info.Error.Time) {
		lastError = &RunningError{
			Time:    &info.Error.Time,
			Addr:    info.Error.Addr,
			Code:    info.Error.Code,
			Message: info.Error.Message,
		}
	}
	var lastWarning *RunningError
	if info.Warning != nil &&
		oracle.GetTimeFromTS(status.CheckpointTs).Before(info.Warning.Time) {
		lastWarning = &RunningError{
			Time:    &info.Warning.Time,
			Addr:    info.Warning.Addr,
			Code:    info.Warning.Code,
			Message: info.Warning.Message,
		}
	}

	c.JSON(http.StatusOK, &ChangefeedStatus{
		State:        string(info.State),
		CheckpointTs: status.CheckpointTs,
		ResolvedTs:   status.ResolvedTs,
		LastError:    lastError,
		LastWarning:  lastWarning,
	})
}

// synced get the synced status of a changefeed
// @Summary Get synced status
// @Description get the synced status of a changefeed
// @Tags changefeed,v2
// @Accept json
// @Produce json
// @Param changefeed_id  path  string  true  "changefeed_id"
// @Param namespace query string false "default"
// @Success 200 {object} SyncedStatus
// @Failure 500,400 {object} model.HTTPError
// @Router /api/v2/changefeeds/{changefeed_id}/synced [get]
func (h *OpenAPIV2) synced(c *gin.Context) {
	ctx := c.Request.Context()

	changefeedID := model.DefaultChangeFeedID(c.Param(apiOpVarChangefeedID))
	if err := model.ValidateChangefeedID(changefeedID.ID); err != nil {
		_ = c.Error(cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s",
			changefeedID.ID))
		return
	}

	status, err := h.capture.StatusProvider().GetChangeFeedSyncedStatus(ctx, changefeedID)
	if err != nil {
		_ = c.Error(err)
		return
	}

	log.Info("Get changefeed synced status:", zap.Any("status", status), zap.Any("changefeedID", changefeedID))

	cfg := &ChangefeedConfig{ReplicaConfig: GetDefaultReplicaConfig()}
	if (status.SyncedCheckInterval != 0) && (status.CheckpointInterval != 0) {
		cfg.ReplicaConfig.SyncedStatus.CheckpointInterval = status.CheckpointInterval
		cfg.ReplicaConfig.SyncedStatus.SyncedCheckInterval = status.SyncedCheckInterval
	}

	// try to get pd client to get pd time, and determine synced status based on the pd time
	if len(cfg.PDAddrs) == 0 {
		up, err := getCaptureDefaultUpstream(h.capture)
		if err != nil {
			_ = c.Error(err)
			return
		}
		cfg.PDConfig = getUpstreamPDConfig(up)
	}
	credential := cfg.PDConfig.toCredential()

	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	pdClient, err := h.helpers.getPDClient(timeoutCtx, cfg.PDAddrs, credential)
	if err != nil {
		// case 1. we can't get pd client, pd may be unavailable.
		//         if pullerResolvedTs - checkpointTs > checkpointInterval, data is not synced
		//         otherwise, if pd is unavailable, we decide data whether is synced based on
		//         the time difference between current time and lastSyncedTs.
		var message string
		if (oracle.ExtractPhysical(status.PullerResolvedTs) - oracle.ExtractPhysical(status.CheckpointTs)) >
			cfg.ReplicaConfig.SyncedStatus.CheckpointInterval*1000 {
			message = fmt.Sprintf("%s. Besides the data is not finish syncing", err.Error())
		} else {
			message = fmt.Sprintf("%s. You should check the pd status first. If pd status is normal, means we don't finish sync data. "+
				"If pd is offline, please check whether we satisfy the condition that "+
				"the time difference from lastSyncedTs to the current time from the time zone of pd is greater than %v secs. "+
				"If it's satisfied, means the data syncing is totally finished", err, cfg.ReplicaConfig.SyncedStatus.SyncedCheckInterval)
		}
		c.JSON(http.StatusOK, SyncedStatus{
			Synced:           false,
			SinkCheckpointTs: model.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
			PullerResolvedTs: model.JSONTime(oracle.GetTimeFromTS(status.PullerResolvedTs)),
			LastSyncedTs:     model.JSONTime(oracle.GetTimeFromTS(status.LastSyncedTs)),
			NowTs:            model.JSONTime(time.Unix(0, 0)),
			Info:             message,
		})
		return
	}
	defer pdClient.Close()
	// get time from pd
	physicalNow, _, _ := pdClient.GetTS(ctx)

	// We can normally get pd time. Thus we determine synced status based on physicalNow, lastSyncedTs, checkpointTs and pullerResolvedTs
	if (physicalNow-oracle.ExtractPhysical(status.LastSyncedTs) > cfg.ReplicaConfig.SyncedStatus.SyncedCheckInterval*1000) &&
		(physicalNow-oracle.ExtractPhysical(status.CheckpointTs) < cfg.ReplicaConfig.SyncedStatus.CheckpointInterval*1000) {
		// case 2: If physcialNow - lastSyncedTs > SyncedCheckInterval && physcialNow - CheckpointTs < CheckpointInterval
		//         --> reach strict synced status
		c.JSON(http.StatusOK, SyncedStatus{
			Synced:           true,
			SinkCheckpointTs: model.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
			PullerResolvedTs: model.JSONTime(oracle.GetTimeFromTS(status.PullerResolvedTs)),
			LastSyncedTs:     model.JSONTime(oracle.GetTimeFromTS(status.LastSyncedTs)),
			NowTs:            model.JSONTime(time.Unix(physicalNow/1e3, 0)),
			Info:             "Data syncing is finished",
		})
		return
	}

	if physicalNow-oracle.ExtractPhysical(status.LastSyncedTs) > cfg.ReplicaConfig.SyncedStatus.SyncedCheckInterval*1000 {
		// case 3: If physcialNow - lastSyncedTs > SyncedCheckInterval && physcialNow - CheckpointTs > CheckpointInterval
		//         we should consider the situation that pd or tikv region is not healthy to block the advancing resolveTs.
		//         if pullerResolvedTs - checkpointTs > CheckpointInterval-->  data is not synced
		//         otherwise, if pd & tikv is healthy --> data is not synced
		//                    if not healthy --> data is synced
		var message string
		if (oracle.ExtractPhysical(status.PullerResolvedTs) - oracle.ExtractPhysical(status.CheckpointTs)) <
			cfg.ReplicaConfig.SyncedStatus.CheckpointInterval*1000 {
			message = fmt.Sprintf("Please check whether PD is online and TiKV Regions are all available. " +
				"If PD is offline or some TiKV regions are not available, it means that the data syncing process is complete. " +
				"To check whether TiKV regions are all available, " +
				"you can view 'TiKV-Details' > 'Resolved-Ts' > 'Max Leader Resolved TS gap' on Grafana. " +
				"If the gap is large, such as a few minutes, it means that some regions in TiKV are unavailable. " +
				"Otherwise, if the gap is small and PD is online, it means the data syncing is incomplete, so please wait")
		} else {
			message = "The data syncing is not finished, please wait"
		}
		c.JSON(http.StatusOK, SyncedStatus{
			Synced:           false,
			SinkCheckpointTs: model.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
			PullerResolvedTs: model.JSONTime(oracle.GetTimeFromTS(status.PullerResolvedTs)),
			LastSyncedTs:     model.JSONTime(oracle.GetTimeFromTS(status.LastSyncedTs)),
			NowTs:            model.JSONTime(time.Unix(physicalNow/1e3, 0)),
			Info:             message,
		})
		return
	}

	// case	4: If physcialNow - lastSyncedTs < SyncedCheckInterval --> data is not synced
	c.JSON(http.StatusOK, SyncedStatus{
		Synced:           false,
		SinkCheckpointTs: model.JSONTime(oracle.GetTimeFromTS(status.CheckpointTs)),
		PullerResolvedTs: model.JSONTime(oracle.GetTimeFromTS(status.PullerResolvedTs)),
		LastSyncedTs:     model.JSONTime(oracle.GetTimeFromTS(status.LastSyncedTs)),
		NowTs:            model.JSONTime(time.Unix(physicalNow/1e3, 0)),
		Info:             "The data syncing is not finished, please wait",
	})
}

func toAPIModel(
	info *model.ChangeFeedInfo,
	resolvedTs uint64,
	checkpointTs uint64,
	taskStatus []model.CaptureTaskStatus,
	maskSinkURI bool,
) *ChangeFeedInfo {
	var runningError *RunningError

	// if the state is normal, we shall not return the error info
	// because changefeed will is retrying. errors will confuse the users
	if info.State != model.StateNormal && info.Error != nil {
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
		Config:         ToAPIReplicaConfig(info.Config),
		State:          info.State,
		Error:          runningError,
		CreatorVersion: info.CreatorVersion,
		CheckpointTs:   checkpointTs,
		ResolvedTs:     resolvedTs,
		CheckpointTime: model.JSONTime(oracle.GetTimeFromTS(checkpointTs)),
		TaskStatus:     taskStatus,
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
