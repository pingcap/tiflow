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

package capture

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
<<<<<<< HEAD:cdc/capture/http_validator.go
	"github.com/pingcap/tiflow/cdc/sink"
=======
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/sinkv2/validator"
>>>>>>> 0867f80e5f (cdc: add changefeed epoch to prevent unexpected state (#8268)):cdc/api/v1/validator.go
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/r3labs/diff"
	"github.com/tikv/client-go/v2/oracle"
)

// verifyCreateChangefeedConfig verify ChangefeedConfig for create a changefeed
func verifyCreateChangefeedConfig(ctx context.Context, changefeedConfig model.ChangefeedConfig, capture *Capture) (*model.ChangeFeedInfo, error) {
	// verify sinkURI
	if changefeedConfig.SinkURI == "" {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("sink-uri is empty, can't not create a changefeed without sink-uri")
	}

	// verify changefeedID
	if err := model.ValidateChangefeedID(changefeedConfig.ID); err != nil {
		return nil, cerror.ErrAPIInvalidParam.GenWithStack("invalid changefeed_id: %s", changefeedConfig.ID)
	}
	// check if the changefeed exists
	cfStatus, err := capture.owner.StatusProvider().GetChangeFeedStatus(ctx, changefeedConfig.ID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return nil, err
	}
	if cfStatus != nil {
		return nil, cerror.ErrChangeFeedAlreadyExists.GenWithStackByArgs(changefeedConfig.ID)
	}

	// verify start-ts
	if changefeedConfig.StartTS == 0 {
		ts, logical, err := capture.pdClient.GetTS(ctx)
		if err != nil {
			return nil, cerror.ErrPDEtcdAPIError.GenWithStackByArgs("fail to get ts from pd client")
		}
		changefeedConfig.StartTS = oracle.ComposeTS(ts, logical)
	}

	// Ensure the start ts is valid in the next 1 hour.
	const ensureTTL = 60 * 60
	if err := gc.EnsureChangefeedStartTsSafety(
		ctx,
		capture.pdClient,
		gc.EnsureGCServiceCreating,
		changefeedConfig.ID,
		ensureTTL,
		changefeedConfig.StartTS); err != nil {
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

	captureInfos, err := capture.owner.StatusProvider().GetCaptures(ctx)
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
<<<<<<< HEAD:cdc/capture/http_validator.go
		SinkURI:           changefeedConfig.SinkURI,
		Opts:              make(map[string]string),
		CreateTime:        time.Now(),
		StartTs:           changefeedConfig.StartTS,
		TargetTs:          changefeedConfig.TargetTS,
		Config:            replicaConfig,
		Engine:            sortEngine,
		State:             model.StateNormal,
		SyncPointEnabled:  false,
		SyncPointInterval: 10 * time.Minute,
		CreatorVersion:    version.ReleaseVersion,
=======
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
>>>>>>> 0867f80e5f (cdc: add changefeed epoch to prevent unexpected state (#8268)):cdc/api/v1/validator.go
	}

	if !replicaConfig.ForceReplicate && !changefeedConfig.IgnoreIneligibleTable {
		ineligibleTables, _, err := VerifyTables(replicaConfig, capture.kvStorage, changefeedConfig.StartTS)
		if err != nil {
			return nil, err
		}
		if len(ineligibleTables) != 0 {
			return nil, cerror.ErrTableIneligible.GenWithStackByArgs(ineligibleTables)
		}
	}

	tz, err := util.GetTimezone(changefeedConfig.TimeZone)
	if err != nil {
		return nil, cerror.ErrAPIInvalidParam.Wrap(errors.Annotatef(err, "invalid timezone:%s", changefeedConfig.TimeZone))
	}
	ctx = util.PutTimezoneInCtx(ctx, tz)
	if err := sink.Validate(ctx, info.SinkURI, info.Config, info.Opts); err != nil {
		return nil, err
	}

	return info, nil
}

// verifyUpdateChangefeedConfig verify ChangefeedConfig for update a changefeed
func verifyUpdateChangefeedConfig(ctx context.Context, changefeedConfig model.ChangefeedConfig, oldInfo *model.ChangeFeedInfo) (*model.ChangeFeedInfo, error) {
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
		_, err = filter.VerifyRules(newInfo.Config)
		if err != nil {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs(err.Error())
		}
	}

	if len(changefeedConfig.IgnoreTxnStartTs) != 0 {
		newInfo.Config.Filter.IgnoreTxnStartTs = changefeedConfig.IgnoreTxnStartTs
	}

	if changefeedConfig.MounterWorkerNum != 0 {
		newInfo.Config.Mounter.WorkerNum = changefeedConfig.MounterWorkerNum
	}

	if changefeedConfig.SinkConfig != nil {
		newInfo.Config.Sink = changefeedConfig.SinkConfig
	}

	// verify sink_uri
	if changefeedConfig.SinkURI != "" {
		newInfo.SinkURI = changefeedConfig.SinkURI
		if err := sink.Validate(ctx, changefeedConfig.SinkURI, newInfo.Config, newInfo.Opts); err != nil {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
		}
	}

	if !diff.Changed(oldInfo, newInfo) {
		return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs("changefeed config is the same with the old one, do nothing")
	}

	return newInfo, nil
}

// VerifyTables catalog tables specified by ReplicaConfig into
// eligible (has an unique index or primary key) and ineligible tables.
func VerifyTables(replicaConfig *config.ReplicaConfig, storage tidbkv.Storage, startTs uint64) (ineligibleTables, eligibleTables []model.TableName, err error) {
	filter, err := filter.NewFilter(replicaConfig)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	meta, err := kv.GetSnapshotMeta(storage, startTs)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	snap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, startTs, false /* explicitTables */)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for _, tableInfo := range snap.Tables() {
		if filter.ShouldIgnoreTable(tableInfo.TableName.Schema, tableInfo.TableName.Table) {
			continue
		}
		// Sequence is not supported yet, TiCDC needs to filter all sequence tables.
		// See https://github.com/pingcap/tiflow/issues/4559
		if tableInfo.IsSequence() {
			continue
		}
		if !tableInfo.IsEligible(false /* forceReplicate */) {
			ineligibleTables = append(ineligibleTables, tableInfo.TableName)
		} else {
			eligibleTables = append(eligibleTables, tableInfo.TableName)
		}
	}
	return
}
