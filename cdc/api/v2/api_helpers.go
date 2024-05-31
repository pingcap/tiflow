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
	"crypto/tls"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/transformer/columnselector"
	"github.com/pingcap/tiflow/cdc/sink/validator"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/r3labs/diff"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

// RegisterImportTaskPrefix denotes the key prefix associated with the entries
// containning import/restore information in the embedded Etcd of the
// upstream PD.
const RegisterImportTaskPrefix = "/tidb/brie/import"

// APIV2Helpers is a collections of helper functions of OpenAPIV2.
// Defining it as an interface to make APIs more testable.
// Note: Every method in this interface should check the context before returning.
// If the context is canceled, the method should return an error immediately.
type APIV2Helpers interface {
	// verifyCreateChangefeedConfig verifies the changefeedConfig,
	// and yield a valid changefeedInfo or error
	verifyCreateChangefeedConfig(
		ctx context.Context,
		cfg *ChangefeedConfig,
		pdClient pd.Client,
		provider owner.StatusProvider,
		ensureGCServiceID string,
		kvStorage tidbkv.Storage,
	) (*model.ChangeFeedInfo, error)

	// verifyUpdateChangefeedConfig verifies the changefeed update config,
	// and returns a pair of valid changefeedInfo & upstreamInfo
	verifyUpdateChangefeedConfig(
		ctx context.Context,
		cfg *ChangefeedConfig,
		oldInfo *model.ChangeFeedInfo,
		oldUpInfo *model.UpstreamInfo,
		kvStorage tidbkv.Storage,
		checkpointTs uint64,
	) (*model.ChangeFeedInfo, *model.UpstreamInfo, error)

	// verifyUpstream verifies the upstreamConfig
	verifyUpstream(
		ctx context.Context,
		changefeedConfig *ChangefeedConfig,
		cfInfo *model.ChangeFeedInfo,
	) error

	verifyResumeChangefeedConfig(
		ctx context.Context,
		pdClient pd.Client,
		gcServiceID string,
		changefeedID model.ChangeFeedID,
		overrideCheckpointTs uint64,
	) error

	// getPDClient returns a PDClient given the PD cluster addresses and a credential
	getPDClient(
		ctx context.Context,
		pdAddrs []string,
		credential *security.Credential,
	) (pd.Client, error)

	// getEtcdClient returns an Etcd client given the PD endpoints and
	// tls config
	getEtcdClient(
		ctx context.Context,
		pdAddrs []string,
		tlsConfig *tls.Config,
	) (*clientv3.Client, error)

	// getKVCreateTiStore wraps kv.createTiStore method to increase testability
	createTiStore(
		ctx context.Context,
		pdAddrs []string,
		credential *security.Credential,
	) (tidbkv.Storage, error)

	// getVerifiedTables wraps entry.VerifyTables to increase testability
	getVerifiedTables(
		ctx context.Context,
		replicaConfig *config.ReplicaConfig,
		storage tidbkv.Storage, startTs uint64,
		scheme string, topic string, protocol config.Protocol,
	) (ineligibleTables,
		eligibleTables []model.TableName, err error,
	)
}

// APIV2HelpersImpl is an implementation of AVIV2Helpers interface
type APIV2HelpersImpl struct{}

// verifyCreateChangefeedConfig verifies ChangefeedConfig and
// returns a changefeedInfo for create a changefeed.
func (APIV2HelpersImpl) verifyCreateChangefeedConfig(
	ctx context.Context,
	cfg *ChangefeedConfig,
	pdClient pd.Client,
	provider owner.StatusProvider,
	ensureGCServiceID string,
	kvStorage tidbkv.Storage,
) (*model.ChangeFeedInfo, error) {
	// verify sinkURI
	if cfg.SinkURI == "" {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs(
			"sink_uri is empty, cannot create a changefeed without sink_uri")
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

	exists, err := provider.IsChangefeedExists(ctx,
		model.ChangeFeedID{Namespace: cfg.Namespace, ID: cfg.ID})
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return nil, err
	}
	if exists {
		return nil, cerror.ErrChangeFeedAlreadyExists.GenWithStackByArgs(cfg.ID)
	}

	ts, logical, err := pdClient.GetTS(ctx)
	if err != nil {
		return nil, cerror.ErrPDEtcdAPIError.GenWithStackByArgs("fail to get ts from pd client")
	}
	currentTSO := oracle.ComposeTS(ts, logical)
	// verify start ts
	if cfg.StartTs == 0 {
		cfg.StartTs = currentTSO
	} else if cfg.StartTs > currentTSO {
		return nil, cerror.ErrAPIInvalidParam.GenWithStack(
			"invalid start-ts %v, larger than current tso %v", cfg.StartTs, currentTSO)
	}
	// Ensure the start ts is valid in the next 3600 seconds, aka 1 hour
	const ensureTTL = 60 * 60
	if err = gc.EnsureChangefeedStartTsSafety(
		ctx,
		pdClient,
		ensureGCServiceID,
		model.ChangeFeedID{Namespace: cfg.Namespace, ID: cfg.ID},
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
	sinkURIParsed, err := url.Parse(cfg.SinkURI)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	err = replicaCfg.ValidateAndAdjust(sinkURIParsed)
	if err != nil {
		return nil, err
	}

	f, err := filter.NewFilter(replicaCfg, "")
	if err != nil {
		return nil, errors.Cause(err)
	}
	tableInfos, ineligibleTables, _, err := entry.VerifyTables(f, kvStorage, cfg.StartTs)
	if err != nil {
		return nil, errors.Cause(err)
	}
	err = f.Verify(tableInfos)
	if err != nil {
		return nil, errors.Cause(err)
	}
	if !replicaCfg.ForceReplicate && !cfg.ReplicaConfig.IgnoreIneligibleTable {
		if err != nil {
			return nil, err
		}
		if len(ineligibleTables) != 0 {
			return nil, cerror.ErrTableIneligible.GenWithStackByArgs(ineligibleTables)
		}
	}

	// verify sink
	if err = validator.Validate(ctx,
		model.ChangeFeedID{Namespace: cfg.Namespace, ID: cfg.ID},
		cfg.SinkURI, replicaCfg, nil,
	); err != nil {
		return nil, err
	}

	return &model.ChangeFeedInfo{
		UpstreamID:     pdClient.GetClusterID(ctx),
		Namespace:      cfg.Namespace,
		ID:             cfg.ID,
		SinkURI:        cfg.SinkURI,
		CreateTime:     time.Now(),
		StartTs:        cfg.StartTs,
		TargetTs:       cfg.TargetTs,
		Config:         replicaCfg,
		State:          model.StateNormal,
		CreatorVersion: version.ReleaseVersion,
		Epoch:          owner.GenerateChangefeedEpoch(ctx, pdClient),
	}, nil
}

// verifyUpstream verifies the upstream config before updating a changefeed
func (h APIV2HelpersImpl) verifyUpstream(
	ctx context.Context,
	changefeedConfig *ChangefeedConfig,
	cfInfo *model.ChangeFeedInfo,
) error {
	if len(changefeedConfig.PDAddrs) == 0 {
		return nil
	}
	// check if the upstream cluster id changed
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pdClient, err := h.getPDClient(timeoutCtx, changefeedConfig.PDAddrs, &security.Credential{
		CAPath:        changefeedConfig.CAPath,
		CertPath:      changefeedConfig.CertPath,
		KeyPath:       changefeedConfig.KeyPath,
		CertAllowedCN: changefeedConfig.CertAllowedCN,
	})
	if err != nil {
		return err
	}
	defer pdClient.Close()
	if pdClient.GetClusterID(ctx) != cfInfo.UpstreamID {
		return cerror.ErrUpstreamMissMatch.
			GenWithStackByArgs(cfInfo.UpstreamID, pdClient.GetClusterID(ctx))
	}

	return nil
}

// verifyUpdateChangefeedConfig verifies config to update
// a changefeed and returns a changefeedInfo
func (APIV2HelpersImpl) verifyUpdateChangefeedConfig(
	ctx context.Context,
	cfg *ChangefeedConfig,
	oldInfo *model.ChangeFeedInfo,
	oldUpInfo *model.UpstreamInfo,
	kvStorage tidbkv.Storage,
	checkpointTs uint64,
) (*model.ChangeFeedInfo, *model.UpstreamInfo, error) {
	// update changefeed info
	newInfo, err := oldInfo.Clone()
	if err != nil {
		return nil, nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByArgs(err.Error())
	}

	var configUpdated, sinkURIUpdated bool
	if cfg.TargetTs != 0 {
		if cfg.TargetTs <= newInfo.StartTs {
			return nil, nil, cerror.ErrChangefeedUpdateRefused.GenWithStack(
				"can not update target_ts:%d less than start_ts:%d",
				cfg.TargetTs, newInfo.StartTs)
		}
		newInfo.TargetTs = cfg.TargetTs
	}
	if cfg.ReplicaConfig != nil {
		configUpdated = true
		newInfo.Config = cfg.ReplicaConfig.ToInternalReplicaConfig()
	}
	if cfg.SinkURI != "" {
		sinkURIUpdated = true
		newInfo.SinkURI = cfg.SinkURI
	}

	// verify changefeed info
	f, err := filter.NewFilter(newInfo.Config, "")
	if err != nil {
		return nil, nil, cerror.ErrChangefeedUpdateRefused.
			GenWithStackByArgs(errors.Cause(err).Error())
	}
	tableInfos, _, _, err := entry.VerifyTables(f, kvStorage, checkpointTs)
	if err != nil {
		return nil, nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
	}
	err = f.Verify(tableInfos)
	if err != nil {
		return nil, nil, cerror.ErrChangefeedUpdateRefused.
			GenWithStackByArgs(errors.Cause(err).Error())
	}

	if configUpdated || sinkURIUpdated {
		log.Info("config or sink uri updated, check the compatibility",
			zap.Bool("configUpdated", configUpdated),
			zap.Bool("sinkURIUpdated", sinkURIUpdated))
		// check sink config is compatible with sinkURI
		newCfg := newInfo.Config.Sink
		oldCfg := oldInfo.Config.Sink
		err := newCfg.CheckCompatibilityWithSinkURI(oldCfg, newInfo.SinkURI)
		if err != nil {
			return nil, nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
		}

		// use the sinkURI to validate and adjust the new config
		sinkURI := oldInfo.SinkURI
		if sinkURIUpdated {
			sinkURI = newInfo.SinkURI
		}
		sinkURIParsed, err := url.Parse(sinkURI)
		if err != nil {
			return nil, nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
		}
		err = newInfo.Config.ValidateAndAdjust(sinkURIParsed)
		if err != nil {
			return nil, nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
		}

		if err := validator.Validate(ctx,
			model.ChangeFeedID{Namespace: cfg.Namespace, ID: cfg.ID},
			newInfo.SinkURI, newInfo.Config, nil); err != nil {
			return nil, nil, cerror.ErrChangefeedUpdateRefused.GenWithStackByCause(err)
		}
	}

	// update and verify up info
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

// verifyResumeChangefeedConfig verifies the changefeed config before resuming a changefeed
// overrideCheckpointTs is the checkpointTs of the changefeed that specified by the user.
// or it is the checkpointTs of the changefeed before it is paused.
// we need to check weather the resuming changefeed is gc safe or not.
func (APIV2HelpersImpl) verifyResumeChangefeedConfig(
	ctx context.Context,
	pdClient pd.Client,
	gcServiceID string,
	changefeedID model.ChangeFeedID,
	overrideCheckpointTs uint64,
) error {
	if overrideCheckpointTs == 0 {
		return nil
	}

	ts, logical, err := pdClient.GetTS(ctx)
	if err != nil {
		return cerror.ErrPDEtcdAPIError.GenWithStackByArgs("fail to get ts from pd client")
	}
	currentTSO := oracle.ComposeTS(ts, logical)
	if overrideCheckpointTs > currentTSO {
		return cerror.ErrAPIInvalidParam.GenWithStack(
			"invalid checkpoint-ts %v, larger than current tso %v", overrideCheckpointTs, currentTSO)
	}

	// 1h is enough for resuming a changefeed.
	gcTTL := int64(60 * 60)
	err = gc.EnsureChangefeedStartTsSafety(
		ctx,
		pdClient,
		gcServiceID,
		changefeedID,
		gcTTL, overrideCheckpointTs)
	if err != nil {
		if !cerror.ErrStartTsBeforeGC.Equal(err) {
			return cerror.ErrPDEtcdAPIError.Wrap(err)
		}
		return err
	}

	return nil
}

// getPDClient returns a PDClient given the PD cluster addresses and a credential
func (APIV2HelpersImpl) getPDClient(
	ctx context.Context,
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
		),
		pd.WithForwardingOption(config.EnablePDForwarding))
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrAPIGetPDClientFailed, errors.Trace(err))
	}
	return pdClient, nil
}

func (h APIV2HelpersImpl) getEtcdClient(
	ctx context.Context,
	pdAddrs []string,
	tlsCfg *tls.Config,
) (*clientv3.Client, error) {
	conf := config.GetGlobalServerConfig()
	grpcTLSOption, err := conf.Security.ToGRPCDialOption()
	if err != nil {
		return nil, err
	}
	logConfig := &logutil.DefaultZapLoggerConfig
	logConfig.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	res, err := clientv3.New(
		clientv3.Config{
			Endpoints:   pdAddrs,
			TLS:         tlsCfg,
			LogConfig:   logConfig,
			DialTimeout: 5 * time.Second,
			DialOptions: []grpc.DialOption{
				grpcTLSOption,
				grpc.WithBlock(),
				grpc.WithConnectParams(
					grpc.ConnectParams{
						Backoff: backoff.Config{
							BaseDelay:  time.Second,
							Multiplier: 1.1,
							Jitter:     0.1,
							MaxDelay:   3 * time.Second,
						},
						MinConnectTimeout: 3 * time.Second,
					},
				),
			},
		},
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ctx.Err() != nil {
		return nil, errors.Trace(ctx.Err())
	}
	return res, nil
}

// getTiStore wrap the kv.createTiStore method to increase testability
func (h APIV2HelpersImpl) createTiStore(
	ctx context.Context,
	pdAddrs []string,
	credential *security.Credential,
) (tidbkv.Storage, error) {
	res, err := kv.CreateTiStore(strings.Join(pdAddrs, ","), credential)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if ctx.Err() != nil {
		return nil, errors.Trace(ctx.Err())
	}
	return res, nil
}

func (h APIV2HelpersImpl) getVerifiedTables(
	ctx context.Context,
	replicaConfig *config.ReplicaConfig,
	storage tidbkv.Storage, startTs uint64,
	scheme string, topic string, protocol config.Protocol,
) ([]model.TableName, []model.TableName, error) {
	f, err := filter.NewFilter(replicaConfig, "")
	if err != nil {
		return nil, nil, err
	}
	tableInfos, ineligibleTables, eligibleTables, err := entry.
		VerifyTables(f, storage, startTs)
	if err != nil {
		return nil, nil, err
	}

	if !sink.IsMQScheme(scheme) {
		return ineligibleTables, eligibleTables, nil
	}

	eventRouter, err := dispatcher.NewEventRouter(replicaConfig, protocol, topic, scheme)
	if err != nil {
		return nil, nil, err
	}
	err = eventRouter.VerifyTables(tableInfos)
	if err != nil {
		return nil, nil, err
	}

	selectors, err := columnselector.New(replicaConfig)
	if err != nil {
		return nil, nil, err
	}
	err = selectors.VerifyTables(tableInfos, eventRouter)
	if err != nil {
		return nil, nil, err
	}

	if ctx.Err() != nil {
		return nil, nil, errors.Trace(ctx.Err())
	}

	return ineligibleTables, eligibleTables, nil
}
