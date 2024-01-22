// Copyright 2021 PingCAP, Inc.
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

package v1

import (
	"context"
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/sink/validator"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/r3labs/diff"
	"github.com/tikv/client-go/v2/oracle"
)

// verifyCreateChangefeedConfig verifies ChangefeedConfig for create a changefeed
func verifyCreateChangefeedConfig(
	ctx context.Context,
	changefeedConfig model.ChangefeedConfig,
	capture capture.Capture,
) (*model.ChangeFeedInfo, error) {
	upManager, err := capture.GetUpstreamManager()
	if err != nil {
		return nil, err
	}
	up, err := upManager.GetDefaultUpstream()
	if err != nil {
		return nil, err
	}

	// verify sinkURI
	if changefeedConfig.SinkURI == "" {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("sink-uri is empty, can't not create a changefeed without sink-uri")
	}

	// verify changefeedID
	if err := model.ValidateChangefeedID(changefeedConfig.ID); err != nil {
		return nil, cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedConfig.ID)
	}
	// check if the changefeed exists
	cfStatus, err := capture.StatusProvider().GetChangeFeedStatus(ctx,
		model.DefaultChangeFeedID(changefeedConfig.ID))
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return nil, err
	}
	if cfStatus != nil {
		return nil, cerror.ErrChangeFeedAlreadyExists.GenWithStackByArgs(changefeedConfig.ID)
	}

	// verify start-ts
	if changefeedConfig.StartTS == 0 {
		ts, logical, err := up.PDClient.GetTS(ctx)
		if err != nil {
			return nil, cerror.ErrPDEtcdAPIError.GenWithStackByArgs("fail to get ts from pd client")
		}
		changefeedConfig.StartTS = oracle.ComposeTS(ts, logical)
	}

	// Ensure the start ts is valid in the next 1 hour.
	const ensureTTL = 60 * 60
	if err := gc.EnsureChangefeedStartTsSafety(
		ctx,
		up.PDClient,
		capture.GetEtcdClient().GetEnsureGCServiceID(gc.EnsureGCServiceCreating),
		model.DefaultChangeFeedID(changefeedConfig.ID),
		ensureTTL, changefeedConfig.StartTS); err != nil {
		if !cerror.ErrStartTsBeforeGC.Equal(err) {
			return nil, cerror.ErrPDEtcdAPIError.Wrap(err)
		}
		return nil, err
	}

	// verify target-ts
	if changefeedConfig.TargetTS > 0 && changefeedConfig.TargetTS <= changefeedConfig.StartTS {
		return nil, cerror.ErrTargetTsBeforeStartTs.GenWithStackByArgs(changefeedConfig.TargetTS, changefeedConfig.StartTS)
	}

	// init replicaConfig
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.ForceReplicate = changefeedConfig.ForceReplicate
	if changefeedConfig.MounterWorkerNum != 0 {
		replicaConfig.Mounter.WorkerNum = changefeedConfig.MounterWorkerNum
	}
	if changefeedConfig.SinkConfig != nil {
		replicaConfig.Sink = changefeedConfig.SinkConfig
	}
	if len(changefeedConfig.IgnoreTxnStartTs) != 0 {
		replicaConfig.Filter.IgnoreTxnStartTs = changefeedConfig.IgnoreTxnStartTs
	}
	if len(changefeedConfig.FilterRules) != 0 {
		replicaConfig.Filter.Rules = changefeedConfig.FilterRules
	}
	// verify replicaConfig
	sinkURIParsed, err := url.Parse(changefeedConfig.SinkURI)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	err = replicaConfig.ValidateAndAdjust(sinkURIParsed)
	if err != nil {
		return nil, err
	}

	captureInfos, err := capture.StatusProvider().GetCaptures(ctx)
	if err != nil {
		return nil, err
	}
	// set sortEngine and EnableOldValue
	cdcClusterVer, err := version.GetTiCDCClusterVersion(model.ListVersionsFromCaptureInfos(captureInfos))
	if err != nil {
		return nil, err
	}
	sortEngine := model.SortUnified
	if !cdcClusterVer.ShouldEnableOldValueByDefault() {
		replicaConfig.EnableOldValue = false
		log.Warn("The TiCDC cluster is built from unknown branch or less than 5.0.0-rc, the old-value are disabled by default.")
		if !cdcClusterVer.ShouldEnableUnifiedSorterByDefault() {
			sortEngine = model.SortInMemory
		}
	}

	// init ChangefeedInfo
	info := &model.ChangeFeedInfo{
		Namespace:      model.DefaultNamespace,
		ID:             changefeedConfig.ID,
		UpstreamID:     up.ID,
		SinkURI:        changefeedConfig.SinkURI,
		CreateTime:     time.Now(),
		StartTs:        changefeedConfig.StartTS,
		TargetTs:       changefeedConfig.TargetTS,
		Config:         replicaConfig,
		Engine:         sortEngine,
		State:          model.StateNormal,
		CreatorVersion: version.ReleaseVersion,
		Epoch:          owner.GenerateChangefeedEpoch(ctx, up.PDClient),
	}
	f, err := filter.NewFilter(replicaConfig, "")
	if err != nil {
		return nil, err
	}
	tableInfos, ineligibleTables, _, err := entry.VerifyTables(f,
		up.KVStorage, changefeedConfig.StartTS)
	if err != nil {
		return nil, err
	}
	err = f.Verify(tableInfos)
	if err != nil {
		return nil, err
	}
	if !replicaConfig.ForceReplicate && !changefeedConfig.IgnoreIneligibleTable {
		if len(ineligibleTables) != 0 {
			return nil, cerror.ErrTableIneligible.GenWithStackByArgs(ineligibleTables)
		}
	}

	tz, err := util.GetTimezone(changefeedConfig.TimeZone)
	if err != nil {
		return nil, cerror.ErrAPIInvalidParam.Wrap(errors.Annotatef(err, "invalid timezone:%s", changefeedConfig.TimeZone))
	}
	ctx = contextutil.PutTimezoneInCtx(ctx, tz)
	if err := validator.Validate(ctx, info.SinkURI, info.Config, up.PDClock); err != nil {
		return nil, err
	}

	return info, nil
}

// VerifyUpdateChangefeedConfig verify ChangefeedConfig for update a changefeed
func VerifyUpdateChangefeedConfig(ctx context.Context,
	changefeedConfig model.ChangefeedConfig,
	oldInfo *model.ChangeFeedInfo,
) (*model.ChangeFeedInfo, error) {
	newInfo, err := oldInfo.Clone()
	if err != nil {
		return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs(err.Error())
	}
	// verify target_ts
	if changefeedConfig.TargetTS != 0 {
		if changefeedConfig.TargetTS <= newInfo.StartTs {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStack("can not update target-ts:%d less than start-ts:%d", changefeedConfig.TargetTS, newInfo.StartTs)
		}
		newInfo.TargetTs = changefeedConfig.TargetTS
	}

	// verify rules
	if len(changefeedConfig.FilterRules) != 0 {
		newInfo.Config.Filter.Rules = changefeedConfig.FilterRules
		_, err = filter.VerifyTableRules(newInfo.Config.Filter)
		if err != nil {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs(err.Error())
		}
	}

	var sinkConfigUpdated, sinkURIUpdated bool
	if len(changefeedConfig.IgnoreTxnStartTs) != 0 {
		newInfo.Config.Filter.IgnoreTxnStartTs = changefeedConfig.IgnoreTxnStartTs
	}
	if changefeedConfig.MounterWorkerNum != 0 {
		newInfo.Config.Mounter.WorkerNum = changefeedConfig.MounterWorkerNum
	}
	if changefeedConfig.SinkConfig != nil {
		sinkConfigUpdated = true
		newInfo.Config.Sink = changefeedConfig.SinkConfig
	}
	if changefeedConfig.SinkURI != "" {
		sinkURIUpdated = true
		newInfo.SinkURI = changefeedConfig.SinkURI
	}

	if sinkConfigUpdated || sinkURIUpdated {
		// check sink config is compatible with sinkURI
		newCfg := newInfo.Config.Sink
		oldCfg := oldInfo.Config.Sink
		err := newCfg.CheckCompatibilityWithSinkURI(oldCfg, newInfo.SinkURI)
		if err != nil {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
		}

		if err := validator.Validate(ctx, newInfo.SinkURI, newInfo.Config, nil); err != nil {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
		}
	}

	if !diff.Changed(oldInfo, newInfo) {
		return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs("changefeed config is the same with the old one, do nothing")
	}

	return newInfo, nil
}
