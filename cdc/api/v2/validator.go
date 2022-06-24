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
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/sink"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/r3labs/diff"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

// verifyCreateChangefeedConfig verifies ChangefeedConfig and
// returns a  changefeedInfo for create a changefeed.
func verifyCreateChangefeedConfig(
	ctx context.Context,
	cfg *ChangefeedConfig,
	pdClient pd.Client,
	statusProvider owner.StatusProvider,
	ensureGCServiceID string,
	kvStorage tidbkv.Storage,
) (*model.ChangeFeedInfo, error) {
	// verify sinkURI
	if cfg.SinkURI == "" {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs(
			"sink_uri is empty, can't not create a changefeed without sink_uri")
	}

	// verify changefeedID
	if cfg.ID == "" {
		cfg.ID = uuid.New().String()
	}
	if err := model.ValidateChangefeedID(cfg.ID); err != nil {
		return nil, cerror.ErrAPIInvalidParam.GenWithStack(
			"invalid changefeed_id: %s", cfg.ID)
	}
	if cfg.Namespace == "" {
		cfg.Namespace = model.DefaultNamespace
	}

	if err := model.ValidateNamespace(cfg.Namespace); err != nil {
		return nil, cerror.ErrAPIInvalidParam.GenWithStack(
			"invalid namespace: %s", cfg.Namespace)
	}

	cfStatus, err := statusProvider.GetChangeFeedStatus(ctx,
		model.DefaultChangeFeedID(cfg.ID))
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return nil, err
	}
	if cfStatus != nil {
		return nil, cerror.ErrChangeFeedAlreadyExists.GenWithStackByArgs(cfg.ID)
	}

	// verify start ts
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
		ensureGCServiceID,
		model.DefaultChangeFeedID(cfg.ID),
		ensureTTL, cfg.StartTs); err != nil {
		if !cerror.ErrStartTsBeforeGC.Equal(err) {
			return nil, cerror.ErrPDEtcdAPIError.Wrap(err)
		}
		return nil, err
	}

	// verify target ts
	if cfg.TargetTs > 0 && cfg.TargetTs <= cfg.StartTs {
		return nil, cerror.ErrTargetTsBeforeStartTs.GenWithStackByArgs(
			cfg.TargetTs, cfg.StartTs)
	}

	// fill replicaConfig
	replicaCfg := cfg.ReplicaConfig.ToInternalReplicaConfig()
	// verify replicaConfig
	err = replicaCfg.Validate()
	if err != nil {
		return nil, err
	}
	if !replicaCfg.EnableOldValue {
		sinkURIParsed, err := url.Parse(cfg.SinkURI)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}

		protocol := sinkURIParsed.Query().Get(config.ProtocolKey)
		if protocol != "" {
			replicaCfg.Sink.Protocol = protocol
		}
		for _, fp := range config.ForceEnableOldValueProtocols {
			if replicaCfg.Sink.Protocol == fp {
				log.Warn(
					"Attempting to replicate without old value enabled. "+
						"CDC will enable old value and continue.",
					zap.String("protocol", replicaCfg.Sink.Protocol))
				replicaCfg.EnableOldValue = true
				break
			}
		}

		if replicaCfg.ForceReplicate {
			return nil, cerror.ErrOldValueNotEnabled.GenWithStackByArgs(
				"if use force replicate, old value feature must be enabled")
		}
	}
	_, err = filter.VerifyRules(replicaCfg.Filter)
	if err != nil {
		return nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs(err.Error())
	}

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
	if err := sink.Validate(ctx, cfg.SinkURI, replicaCfg); err != nil {
		return nil, err
	}

	return &model.ChangeFeedInfo{
		UpstreamID:        pdClient.GetClusterID(ctx),
		Namespace:         cfg.Namespace,
		ID:                cfg.ID,
		SinkURI:           cfg.SinkURI,
		CreateTime:        time.Now(),
		StartTs:           cfg.StartTs,
		TargetTs:          cfg.TargetTs,
		Engine:            cfg.Engine,
		Config:            replicaCfg,
		State:             model.StateNormal,
		SyncPointEnabled:  cfg.SyncPointEnabled,
		SyncPointInterval: cfg.SyncPointInterval,
		CreatorVersion:    version.ReleaseVersion,
	}, nil
}

func getPDClient(ctx context.Context,
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
		return nil, errors.Trace(err)
	}
	return pdClient, nil
}

// verifyUpdateChangefeedConfig verifies config to update
// a changefeed and returns a changefeedInfo
func verifyUpdateChangefeedConfig(ctx context.Context,
	cfg *ChangefeedConfig, oldInfo *model.ChangeFeedInfo,
	oldUpInfo *model.UpstreamInfo,
) (*model.ChangeFeedInfo, *model.UpstreamInfo, error) {
	newInfo, err := oldInfo.Clone()
	if err != nil {
		return nil, nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs(err.Error())
	}

	// verify TargetTs
	if cfg.TargetTs != 0 {
		if cfg.TargetTs <= newInfo.StartTs {
			return nil, nil, cerror.ErrChangefeedUpdateRefused.GenWithStack(
				"can not update target_ts:%d less than start_ts:%d",
				cfg.TargetTs, newInfo.StartTs)
		}
		newInfo.TargetTs = cfg.TargetTs
	}

	// verify syncPoint
	newInfo.SyncPointEnabled = cfg.SyncPointEnabled
	if cfg.SyncPointInterval != 0 {
		newInfo.SyncPointInterval = cfg.SyncPointInterval
	}

	if cfg.ReplicaConfig != nil {
		newInfo.Config = cfg.ReplicaConfig.ToInternalReplicaConfig()
	}

	// verify SinkURI
	if cfg.SinkURI != "" {
		newInfo.SinkURI = cfg.SinkURI
		if err := sink.Validate(ctx, newInfo.SinkURI, newInfo.Config); err != nil {
			return nil, nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
		}
	}
	if cfg.Engine != "" {
		newInfo.Engine = cfg.Engine
	}
	newUpInfo, err := oldUpInfo.Clone()
	if err != nil {
		return nil, nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs(err.Error())
	}
	if cfg.PDAddrs != nil {
		newUpInfo.PDEndpoints = strings.Join(cfg.PDAddrs, ",")
	}
	if cfg.CAPath != "" {
		newUpInfo.CAPath = cfg.CAPath
	}
	if cfg.CertPath != "" {
		newUpInfo.CertPath = cfg.CertPath
	}
	if cfg.KeyPath != "" {
		newUpInfo.KeyPath = cfg.KeyPath
	}
	if cfg.CertAllowedCN != nil {
		newUpInfo.CertAllowedCN = cfg.CertAllowedCN
	}
	changefeedInfoChanged := diff.Changed(oldInfo, newInfo)
	upstreamInfoChanged := diff.Changed(oldUpInfo, newUpInfo)

	if !changefeedInfoChanged && !upstreamInfoChanged {
		return nil, nil, cerror.ErrChangefeedUpdateRefused.
			GenWithStackByArgs("changefeed config is the same with the old one, do nothing")
	}
	return newInfo, newUpInfo, nil
}

func verifyResumeChangefeed(ctx context.Context,
	changefeedID model.ChangeFeedID,
	checkpointTs uint64,
	upstream *upstream.Upstream,
) error {
	// TODO: gcSafePoint needs a force update here.
	// At present, gcSafePoint is updated in gcManager.TryUpdateGCSafePoint every 1min.
	// So when we try to resume the changefeed, gcManager.lastSafePointTs may not updated timely,
	// And gcManager.CheckStaleCheckpointTs may not return error expectfully
	err := upstream.GCManager.CheckStaleCheckpointTs(ctx, changefeedID, checkpointTs)
	if err != nil {
		return err
	}

	return nil
}
