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
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/txnutil/gc"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	tidbkv "github.com/pingcap/tidb/kv"
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
	// check if the changefeed exists && check if the etcdClient work well
	cfStatus, _, err := capture.etcdClient.GetChangeFeedStatus(ctx, changefeedConfig.ID)
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

	// Ensure the start ts is validate in the next 1 hour.
	const ensureTTL = 60 * 60
	if err := gc.EnsureChangefeedStartTsSafety(
		ctx, capture.pdClient, changefeedConfig.ID, ensureTTL, changefeedConfig.StartTS); err != nil {
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

	_, captureInfos, err := capture.etcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}
	// set sortEngine and EnableOldValue
	cdcClusterVer, err := version.GetTiCDCClusterVersion(captureInfos)
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
	}

	if !replicaConfig.ForceReplicate && !changefeedConfig.IgnoreIneligibleTable {
		ineligibleTables, _, err := verifyTables(replicaConfig, capture.kvStorage, changefeedConfig.StartTS)
		if err != nil {
			return nil, err
		}
		if len(ineligibleTables) != 0 {
			return nil, cerror.ErrTableIneligible.GenWithStackByArgs(ineligibleTables)
		}
	}

	tz, err := util.GetTimezone(changefeedConfig.TimeZone)
	if err != nil {
		return nil, errors.Annotate(err, "invalid timezone:"+changefeedConfig.TimeZone)
	}
	ctx = util.PutTimezoneInCtx(ctx, tz)
	err = verifySink(ctx, info.SinkURI, info.Config, info.Opts)
	if err != nil {
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
		err = verifySink(ctx, changefeedConfig.SinkURI, newInfo.Config, newInfo.Opts)
		if err != nil {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
		}
	}

	if !diff.Changed(oldInfo, newInfo) {
		return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs("changefeed config is the same with the old one, do nothing")
	}

	return newInfo, nil
}

func verifySink(ctx context.Context, sinkURI string, cfg *config.ReplicaConfig, opts map[string]string) error {
	sinkFilter, err := filter.NewFilter(cfg)
	if err != nil {
		return err
	}
	errCh := make(chan error)
	// TODO: find a better way to verify a sinkURI is valid
	s, err := sink.NewSink(ctx, "sink-verify", sinkURI, sinkFilter, cfg, opts, errCh)
	if err != nil {
		return err
	}
	err = s.Close(ctx)
	if err != nil {
		return err
	}
	select {
	case err = <-errCh:
		if err != nil {
			return err
		}
	default:
	}
	return nil
}

func verifyTables(replicaConfig *config.ReplicaConfig, storage tidbkv.Storage, startTs uint64) (ineligibleTables, eligibleTables []model.TableName, err error) {
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
		if !tableInfo.IsEligible(false /* forceReplicate */) {
			ineligibleTables = append(ineligibleTables, tableInfo.TableName)
		} else {
			eligibleTables = append(eligibleTables, tableInfo.TableName)
		}
	}
	return
}
