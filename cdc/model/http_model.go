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

package model

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/r3labs/diff"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

// JSONTime used to wrap time into json format
type JSONTime time.Time

// MarshalJSON use to specify the time format
func (t JSONTime) MarshalJSON() ([]byte, error) {
	stamp := fmt.Sprintf("\"%s\"", time.Time(t).Format("2006-01-02 15:04:05.000"))
	return []byte(stamp), nil
}

// HTTPError of cdc http api
type HTTPError struct {
	Error string `json:"error_msg"`
	Code  string `json:"error_code"`
}

// NewHTTPError wrap a err into HTTPError
func NewHTTPError(err error) HTTPError {
	errCode, _ := cerror.RFCCode(err)
	return HTTPError{
		Error: err.Error(),
		Code:  string(errCode),
	}
}

// ServerStatus holds some common information of a server
type ServerStatus struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
	ID      string `json:"id"`
	Pid     int    `json:"pid"`
	IsOwner bool   `json:"is_owner"`
}

// ChangefeedCommonInfo holds some common usage information of a changefeed
type ChangefeedCommonInfo struct {
	ID             string        `json:"id"`
	FeedState      FeedState     `json:"state"`
	CheckpointTSO  uint64        `json:"checkpoint_tso"`
	CheckpointTime JSONTime      `json:"checkpoint_time"`
	RunningError   *RunningError `json:"error"`
}

// ChangefeedDetail holds detail info of a changefeed
type ChangefeedDetail struct {
	ID             string              `json:"id"`
	SinkURI        string              `json:"sink_uri"`
	CreateTime     JSONTime            `json:"create_time"`
	StartTs        uint64              `json:"start_ts"`
	TargetTs       uint64              `json:"target_ts"`
	CheckpointTSO  uint64              `json:"checkpoint_tso"`
	CheckpointTime JSONTime            `json:"checkpoint_time"`
	Engine         SortEngine          `json:"sort_engine"`
	FeedState      FeedState           `json:"state"`
	RunningError   *RunningError       `json:"error"`
	ErrorHis       []int64             `json:"error_history"`
	CreatorVersion string              `json:"creator_version"`
	TaskStatus     []CaptureTaskStatus `json:"task_status"`
}

// ChangefeedConfig use to create a changefeed
type ChangefeedConfig struct {
	ID       string `json:"changefeed_id"`
	StartTS  uint64 `json:"start_ts"`
	TargetTS uint64 `json:"target_ts"`
	SinkURI  string `json:"sink_uri"`
	// timezone used when checking sink uri
	TimeZone string `json:"timezone" default:"system"`
	// if true, force to replicate some ineligible tables
	ForceReplicate        bool               `json:"force_replicate" default:"false"`
	IgnoreIneligibleTable bool               `json:"ignore_ineligible_table" default:"false"`
	FilterRules           []string           `json:"filter_rules"`
	IgnoreTxnStartTs      []uint64           `json:"ignore_txn_start_ts"`
	MounterWorkerNum      int                `json:"mounter_worker_num" default:"16"`
	SinkConfig            *config.SinkConfig `json:"sink_config"`
}

// VerifyCreateChangefeedConfig verify ChangefeedConfig for create a changefeed
func (changefeedConfig *ChangefeedConfig) VerifyCreateChangefeedConfig(ctx context.Context, kvStorage tidbkv.Storage, etcdClient *kv.CDCEtcdClient, pdClient pd.Client) (*ChangeFeedInfo, error) {
	// verify sinkURI
	if changefeedConfig.SinkURI == "" {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("sink-uri is empty, can't not create a changefeed without sink-uri")
	}

	// verify changefeedID
	if err := ValidateChangefeedID(changefeedConfig.ID); err != nil {
		return nil, err
	}
	_, _, err := etcdClient.GetChangeFeedStatus(ctx, changefeedConfig.ID)
	if err != nil && err == cerror.ErrChangeFeedAlreadyExists {
		return nil, err
	}

	// verify start-ts
	if changefeedConfig.StartTS == 0 {
		ts, logical, err := pdClient.GetTS(ctx)
		if err != nil {
			return nil, cerror.ErrPDEtcdAPIError.GenWithStackByArgs("fail to get ts from pd client")
		}
		changefeedConfig.StartTS = oracle.ComposeTS(ts, logical)
	}

	if err = util.CheckSafetyOfStartTs(ctx, pdClient, changefeedConfig.ID, changefeedConfig.StartTS); err != nil {
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
	_, captureInfos, err := etcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, cerror.ErrPDEtcdAPIError.Wrap(err)
	}
	cdcClusterVer, err := version.GetTiCDCClusterVersion(captureInfos)
	if err != nil {
		return nil, err
	}
	sortEngine := SortUnified
	if !cdcClusterVer.ShouldEnableOldValueByDefault() {
		replicaConfig.EnableOldValue = false
		log.Warn("The TiCDC cluster is built from unknown branch or less than 5.0.0-rc, the old-value are disabled by default.")
		if !cdcClusterVer.ShouldEnableUnifiedSorterByDefault() {
			sortEngine = SortInMemory
		}
	}

	// init ChangefeedInfo
	info := &ChangeFeedInfo{
		SinkURI:           changefeedConfig.SinkURI,
		Opts:              make(map[string]string),
		CreateTime:        time.Now(),
		StartTs:           changefeedConfig.StartTS,
		TargetTs:          changefeedConfig.TargetTS,
		Config:            replicaConfig,
		Engine:            sortEngine,
		State:             StateNormal,
		SyncPointEnabled:  false,
		SyncPointInterval: 10 * time.Minute,
		CreatorVersion:    version.ReleaseVersion,
	}

	if !replicaConfig.ForceReplicate && !changefeedConfig.IgnoreIneligibleTable {
		ineligibleTables, _, err := changefeedConfig.verifyTables(kvStorage, replicaConfig, changefeedConfig.StartTS)
		if err != nil {
			return nil, err
		}
		if len(ineligibleTables) != 0 {
			return nil, cerror.ErrTableIneligible.GenWithStackByArgs(ineligibleTables)
		}
	}

	err = changefeedConfig.verifySink(ctx, info.SinkURI, info.Config, info.Opts)
	if err != nil {
		return nil, err
	}

	return info, nil
}

// VerifyUpdateChangefeedConfig verify ChangefeedConfig for update a changefeed
func (changefeedConfig *ChangefeedConfig) VerifyUpdateChangefeedConfig(ctx context.Context, oldInfo *ChangeFeedInfo) (*ChangeFeedInfo, error) {
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
		err = changefeedConfig.verifySink(ctx, changefeedConfig.SinkURI, newInfo.Config, newInfo.Opts)
		if err != nil {
			return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
		}
	}

	if !diff.Changed(oldInfo, newInfo) {
		return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs("changefeed config is the same with the old one, do nothing")
	}

	return newInfo, nil
}

func (changefeedConfig *ChangefeedConfig) verifySink(ctx context.Context, sinkURI string, cfg *config.ReplicaConfig, opts map[string]string) error {
	filter, err := filter.NewFilter(cfg)
	if err != nil {
		return err
	}
	errCh := make(chan error)
	// TODO: find a better way to verify a sinkURI is valid
	s, err := sink.NewSink(ctx, "sink-verify", sinkURI, filter, cfg, opts, errCh)
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

func (changefeedConfig *ChangefeedConfig) verifyTables(kvStorage tidbkv.Storage, replicaConfig *config.ReplicaConfig, startTs uint64) (ineligibleTables, eligibleTables []TableName, err error) {
	meta, err := kv.GetSnapshotMeta(kvStorage, startTs)
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

// ProcessorCommonInfo holds the common info of a processor
type ProcessorCommonInfo struct {
	CfID      string `json:"changefeed_id"`
	CaptureID string `json:"capture_id"`
}

// ProcessorDetail holds the detail info of a processor
type ProcessorDetail struct {
	// The maximum event CommitTs that has been synchronized.
	CheckPointTs uint64 `json:"checkpoint_ts"`
	// The event that satisfies CommitTs <= ResolvedTs can be synchronized.
	ResolvedTs uint64 `json:"resolved_ts"`
	// all table ids that this processor are replicating
	Tables []int64 `json:"table_ids"`
	// Error code when error happens
	Error *RunningError `json:"error"`
}

// CaptureTaskStatus holds TaskStatus of a capture
type CaptureTaskStatus struct {
	CaptureID string `json:"capture_id"`
	// Table list, containing tables that processor should process
	Tables    []int64                     `json:"table_ids"`
	Operation map[TableID]*TableOperation `json:"table_operations"`
}

// Capture holds common information of a capture in cdc
type Capture struct {
	ID            string `json:"id"`
	IsOwner       bool   `json:"is_owner"`
	AdvertiseAddr string `json:"address"`
}
