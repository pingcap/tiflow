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
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/capture"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

// VerifyCreateChangefeedConfig verifies ChangefeedConfig and
// returns a  changefeedInfo for create a changefeed.
func VerifyCreateChangefeedConfig(
	ctx context.Context,
	cfg *ChangefeedConfig,
	capture *capture.Capture,
) (*model.ChangeFeedInfo, error) {
	// verify credential and pdAddrs
	if len(cfg.PDAddrs) == 0 {
		return nil, cerror.ErrAPIInvalidParam.GenWithStackByCause(
			"can not create a changefeed with empty pdAddrs")
	}
	credential := security.Credential{
		CAPath:   cfg.CAPath,
		CertPath: cfg.CertPath,
		KeyPath:  cfg.KeyPath,
	}
	grpcTLSOption, err := credential.ToGRPCDialOption()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(cfg.CertAllowedCN) != 0 {
		credential.CertAllowedCN = cfg.CertAllowedCN
	}

	pdClient, err := pd.NewClientWithContext(
		ctx, cfg.PDAddrs, credential.PDSecurityOption(),
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
		return nil, errors.Trace(err)
	}
	defer pdClient.Close()

	// verify sinkURI
	if cfg.SinkURI == "" {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs(
			"sink-uri is empty, can't not create a changefeed without sink-uri")
	}

	// verify changefeedID
	if cfg.ID == "" {
		cfg.ID = uuid.New().String()
	}
	if err := model.ValidateChangefeedID(cfg.ID); err != nil {
		return nil, cerror.ErrAPIInvalidParam.GenWithStack(
			"invalid changefeed_id: %s", cfg.ID)
	}
	cfStatus, err := capture.StatusProvider().GetChangeFeedStatus(ctx,
		model.DefaultChangeFeedID(cfg.ID))
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return nil, err
	}
	if cfStatus != nil {
		return nil, cerror.ErrChangeFeedAlreadyExists.GenWithStackByArgs(cfg.ID)
	}

	// verify start-ts
	if cfg.StartTs == 0 {
		ts, logical, err := pdClient.GetTS(ctx)
		if err != nil {
			return nil, cerror.ErrPDEtcdAPIError.GenWithStackByArgs(
				"fail to get ts from pd client")
		}
		cfg.StartTs = oracle.ComposeTS(ts, logical)
	}

	// Ensure the start ts is valid in the next 1 hour.
	const ensureTTL = 60 * 60
	if err := gc.EnsureChangefeedStartTsSafety(
		ctx,
		pdClient,
		capture.EtcdClient.GetEnsureGCServiceID(),
		model.DefaultChangeFeedID(cfg.ID),
		ensureTTL, cfg.StartTs); err != nil {
		if !cerror.ErrStartTsBeforeGC.Equal(err) {
			return nil, cerror.ErrPDEtcdAPIError.Wrap(err)
		}
		return nil, err
	}

	// verify target-ts
	if cfg.TargetTs > 0 && cfg.TargetTs <= cfg.StartTs {
		return nil, cerror.ErrTargetTsBeforeStartTs.GenWithStackByArgs(
			cfg.TargetTs, cfg.StartTs)
	}

	// fill replica config
	replicaCfg, err := fillReplicaConfig(cfg.ReplicaConfig)
	if err != nil {
		return nil, err
	}

	// verify tables
	kvStorage, err := kv.CreateTiStore(strings.Join(cfg.PDAddrs, ","), &credential)
	if err != nil {
		return nil, err
	}
	// We should not close kvStorage since all kvStorage in cdc is the same one.
	// defer kvStorage.Close()

	if !replicaCfg.ForceReplicate && !cfg.ReplicaConfig.IgnoreIneligibleTable {
		ineligibleTables, _, err := entry.VerifyTables(replicaCfg, kvStorage, cfg.StartTs)
		if err != nil {
			return nil, err
		}
		if len(ineligibleTables) != 0 {
			return nil, cerror.ErrTableIneligible.GenWithStackByArgs(ineligibleTables)
		}
	}

	// verify sink
	if err := sink.Validate(ctx, cfg.SinkURI, replicaCfg, map[string]string{}); err != nil {
		return nil, err
	}

	return &model.ChangeFeedInfo{
		UpstreamID:        pdClient.GetClusterID(ctx),
		Namespace:         cfg.Namespace,
		ID:                cfg.ID,
		SinkURI:           cfg.SinkURI,
		Opts:              make(map[string]string),
		CreateTime:        time.Now(),
		StartTs:           cfg.StartTs,
		TargetTs:          cfg.TargetTs,
		Engine:            model.SortUnified,
		Config:            replicaCfg,
		State:             model.StateNormal,
		SyncPointEnabled:  cfg.SyncPointEnabled,
		SyncPointInterval: cfg.SyncPointInterval,
		CreatorVersion:    version.ReleaseVersion,
	}, nil
}

func fillReplicaConfig(cfg *ReplicaConfig) (*config.ReplicaConfig, error) {
	res := config.GetDefaultReplicaConfig()
	res.CaseSensitive = cfg.CaseSensitive
	res.EnableOldValue = cfg.EnableOldValue
	res.ForceReplicate = cfg.ForceReplicate
	res.CheckGCSafePoint = cfg.CheckGCSafePoint
	if cfg.Filter != nil {
		res.Filter = &config.FilterConfig{
			Rules:                 cfg.Filter.Rules,
			MySQLReplicationRules: cfg.Filter.MySQLReplicationRules,
			IgnoreTxnStartTs:      cfg.Filter.IgnoreTxnStartTs,
			DDLAllowlist:          cfg.Filter.DDLAllowlist,
		}
	}
	if cfg.Consistent != nil {
		res.Consistent = &config.ConsistentConfig{
			Level:             cfg.Consistent.Level,
			MaxLogSize:        cfg.Consistent.MaxLogSize,
			FlushIntervalInMs: cfg.Consistent.FlushIntervalInMs,
			Storage:           cfg.Consistent.Storage,
		}
	}
	if cfg.Sink != nil {
		var dispatchRules []*config.DispatchRule
		for _, rule := range cfg.Sink.DispatchRules {
			dispatchRules = append(dispatchRules, &config.DispatchRule{
				Matcher:        rule.Matcher,
				DispatcherRule: "",
				PartitionRule:  rule.PartitionRule,
				TopicRule:      rule.TopicRule,
			})
		}
		var columnSelectors []*config.ColumnSelector
		for _, selector := range cfg.Sink.ColumnSelectors {
			columnSelectors = append(columnSelectors, &config.ColumnSelector{
				Matcher: selector.Matcher,
				Columns: selector.Columns,
			})
		}
		res.Sink = &config.SinkConfig{
			DispatchRules:   dispatchRules,
			Protocol:        cfg.Sink.Protocol,
			ColumnSelectors: columnSelectors,
			SchemaRegistry:  cfg.Sink.SchemaRegistry,
		}
	}
	return res, nil
}
