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
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/r3labs/diff"
	"github.com/tikv/client-go/v2/oracle"
)

// TODO: Rename the file and move it to other place. Clean the duplicate codes in ticdc/cmd/util.go
func verifyUpdateChangefeedParameters(ctx context.Context, oldInfo *model.ChangeFeedInfo, config model.ChangefeedConfig) (*model.ChangeFeedInfo, error) {
	newInfo, err := oldInfo.Clone()
	if err != nil {
		return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs(err.Error())
	}
	// verify target_ts
	if config.TargetTS != 0 {
		if config.TargetTS <= newInfo.StartTs {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStack("can not update target-ts:%d less than start-ts:%d", config.TargetTS, newInfo.StartTs)
		}
		newInfo.TargetTs = config.TargetTS
	}

	// verify rules
	if len(config.FilterRules) != 0 {
		newInfo.Config.Filter.Rules = config.FilterRules
		_, err = filter.VerifyRules(newInfo.Config)
		if err != nil {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs(err.Error())
		}
	}

	if len(config.IgnoreTxnStartTs) != 0 {
		newInfo.Config.Filter.IgnoreTxnStartTs = config.IgnoreTxnStartTs
	}

	if config.MounterWorkerNum != 0 {
		newInfo.Config.Mounter.WorkerNum = config.MounterWorkerNum
	}

	if config.SinkConfig != nil {
		newInfo.Config.Sink = config.SinkConfig
	}

	// verify sink_uri
	if config.SinkURI != "" {
		newInfo.SinkURI = config.SinkURI
		err = verifySink(ctx, config.SinkURI, newInfo.Config, newInfo.Opts)
		if err != nil {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
		}
	}

	if !diff.Changed(oldInfo, newInfo) {
		return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs("changefeed config is the same with the old one, do nothing")
	}

	return newInfo, nil
}

func verifyChangefeedCreateParameters(ctx context.Context, capture *Capture, changefeedConfig model.ChangefeedConfig) (*model.ChangeFeedInfo, error) {
	// verify sinkURI
	if changefeedConfig.SinkURI == "" {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("sink-uri is empty, can't not create a changefeed without sink-uri")
	}

	// verify changefeedID
	if err := model.ValidateChangefeedID(changefeedConfig.ID); err != nil {
		return nil, err
	}
	_, _, err := capture.etcdClient.GetChangeFeedStatus(ctx, changefeedConfig.ID)
	if err != nil && err == cerror.ErrChangeFeedAlreadyExists {
		return nil, err
	}

	// verify start-ts
	if changefeedConfig.StartTS == 0 {
		ts, logical, err := capture.pdClient.GetTS(ctx)
		if err != nil {
			return nil, cerror.ErrPDEtcdAPIError.GenWithStackByArgs("fail to get ts from pd client")
		}
		changefeedConfig.StartTS = oracle.ComposeTS(ts, logical)
	}

	if err = util.CheckSafetyOfStartTs(ctx, capture.pdClient, changefeedConfig.ID, changefeedConfig.StartTS); err != nil {
		if err != cerror.ErrStartTsBeforeGC {
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

	// set sortEngine and EnableOldValue
	_, captureInfos, err := capture.etcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, cerror.ErrPDEtcdAPIError.Wrap(err)
	}
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
		ineligibleTables, _, err := verifyTables(capture, replicaConfig, changefeedConfig.StartTS)
		if err != nil {
			return nil, err
		}
		if len(ineligibleTables) != 0 {
			return nil, cerror.ErrTableIneligible.GenWithStackByArgs(ineligibleTables)
		}
	}

	err = verifySink(ctx, info.SinkURI, info.Config, info.Opts)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func verifyTables(capture *Capture, replicaConfig *config.ReplicaConfig, startTs uint64) (ineligibleTables, eligibleTables []model.TableName, err error) {
	meta, err := kv.GetSnapshotMeta(capture.kvStorage, startTs)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	filter, err := filter.NewFilter(replicaConfig)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	snap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, startTs, false /* explicitTables */)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for tID, tableName := range snap.CloneTables() {
		tableInfo, exist := snap.TableByID(tID)
		if !exist {
			return nil, nil, errors.NotFoundf("table %d", tID)
		}
		if filter.ShouldIgnoreTable(tableName.Schema, tableName.Table) {
			continue
		}
		if !tableInfo.IsEligible(false /* forceReplicate */) {
			ineligibleTables = append(ineligibleTables, tableName)
		} else {
			eligibleTables = append(eligibleTables, tableName)
		}
	}
	return
}

func verifySink(ctx context.Context, sinkURI string, cfg *config.ReplicaConfig, opts map[string]string) error {
	filter, err := filter.NewFilter(cfg)
	if err != nil {
		return err
	}
	errCh := make(chan error)
	s, err := sink.NewSink(ctx, "cli-verify", sinkURI, filter, cfg, opts, errCh)
	if err != nil {
		return err
	}
	err = s.Close()
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
