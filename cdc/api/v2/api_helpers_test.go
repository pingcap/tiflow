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
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestVerifyCreateChangefeedConfig(t *testing.T) {
	ctx := context.Background()
	pdClient := &mockPDClient{}
	helper := entry.NewSchemaTestHelper(t)
	helper.Tk().MustExec("use test;")
	storage := helper.Storage()
	provider := &mockStatusProvider{}
	cfg := &ChangefeedConfig{}
	h := &APIV2HelpersImpl{}
	cfInfo, err := h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.Nil(t, cfInfo)
	require.NotNil(t, err)
	cfg.SinkURI = "blackhole://"
	// repliconfig is nil
	require.Panics(t, func() {
		_, _ = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	})
	cfg.ReplicaConfig = GetDefaultReplicaConfig()
	cfg.ReplicaConfig.ForceReplicate = true
	cfg.ReplicaConfig.EnableOldValue = false
	cfg.SinkURI = "mysql://"
	// disable old value but force replicate, and using mysql sink.
	cfInfo, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	require.Nil(t, cfInfo)
	cfg.ReplicaConfig.ForceReplicate = false
	cfg.ReplicaConfig.IgnoreIneligibleTable = true
	cfg.SinkURI = "blackhole://"
	cfInfo, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.Nil(t, err)
	require.NotNil(t, cfInfo)
	require.NotEqual(t, "", cfInfo.ID)
	require.Equal(t, model.DefaultNamespace, cfInfo.Namespace)
	require.NotEqual(t, 0, cfInfo.Epoch)

	cfg.ID = "abdc/sss"
	cfInfo, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	cfg.ID = ""
	cfg.Namespace = "abdc/sss"
	cfInfo, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	cfg.ID = ""
	cfg.Namespace = ""
	// changefeed already exists
	provider.changefeedStatus = &model.ChangeFeedStatusForAPI{}
	cfInfo, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	provider.changefeedStatus = nil
	provider.err = cerror.ErrChangeFeedNotExists.GenWithStackByArgs("aaa")
	cfInfo, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.Nil(t, err)
	require.Equal(t, uint64(123), cfInfo.UpstreamID)
	cfg.TargetTs = 3
	cfg.StartTs = 4
	cfInfo, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	cfg.TargetTs = 6
	cfg.ReplicaConfig.EnableOldValue = false
	cfg.SinkURI = "aaab://"
	cfInfo, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	cfg.SinkURI = string([]byte{0x7f, ' '})
	cfInfo, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)

	cfg.StartTs = 0
	// use blackhole to workaround
	cfg.SinkURI = "blackhole://127.0.0.1:9092/test?protocol=avro"
	cfg.ReplicaConfig.EnableOldValue = true
	cfg.ReplicaConfig.ForceReplicate = false
	cfInfo, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NoError(t, err)
	require.False(t, cfInfo.Config.EnableOldValue)

	cfg.ReplicaConfig.ForceReplicate = true
	cfInfo, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.Error(t, cerror.ErrOldValueNotEnabled, err)
}

func TestVerifyUpdateChangefeedConfig(t *testing.T) {
	ctx := context.Background()
	cfg := &ChangefeedConfig{}
	oldInfo := &model.ChangeFeedInfo{
		Config: config.GetDefaultReplicaConfig(),
	}
	oldUpInfo := &model.UpstreamInfo{}
	helper := entry.NewSchemaTestHelper(t)
	helper.Tk().MustExec("use test;")
	storage := helper.Storage()
	h := &APIV2HelpersImpl{}
	newCfInfo, newUpInfo, err := h.verifyUpdateChangefeedConfig(ctx, cfg, oldInfo, oldUpInfo, storage, 0)
	require.NotNil(t, err)
	require.Nil(t, newCfInfo)
	require.Nil(t, newUpInfo)
	// namespace and id can not be updated
	cfg.Namespace = "abc"
	cfg.ID = "1234"
	newCfInfo, newUpInfo, err = h.verifyUpdateChangefeedConfig(ctx, cfg, oldInfo, oldUpInfo, storage, 0)
	require.NotNil(t, err)
	require.Nil(t, newCfInfo)
	require.Nil(t, newUpInfo)
	cfg.StartTs = 2
	cfg.TargetTs = 10
	cfg.ReplicaConfig = ToAPIReplicaConfig(config.GetDefaultReplicaConfig())
	cfg.ReplicaConfig.SyncPointInterval = &JSONDuration{30 * time.Second}
	cfg.PDAddrs = []string{"a", "b"}
	cfg.CertPath = "p1"
	cfg.CAPath = "p2"
	cfg.KeyPath = "p3"
	cfg.SinkURI = "blackhole://"
	cfg.CertAllowedCN = []string{"c", "d"}
	newCfInfo, newUpInfo, err = h.verifyUpdateChangefeedConfig(ctx, cfg, oldInfo, oldUpInfo, storage, 0)
	require.Nil(t, err)
	// startTs can not be updated
	newCfInfo.Config.Sink.TxnAtomicity = ""
	require.Equal(t, uint64(0), newCfInfo.StartTs)
	require.Equal(t, uint64(10), newCfInfo.TargetTs)
	require.Equal(t, 30*time.Second, newCfInfo.Config.SyncPointInterval)
	require.Equal(t, cfg.ReplicaConfig.ToInternalReplicaConfig(), newCfInfo.Config)
	require.Equal(t, "a,b", newUpInfo.PDEndpoints)
	require.Equal(t, "p1", newUpInfo.CertPath)
	require.Equal(t, "p2", newUpInfo.CAPath)
	require.Equal(t, "p3", newUpInfo.KeyPath)
	require.Equal(t, []string{"c", "d"}, newUpInfo.CertAllowedCN)
	require.Equal(t, "blackhole://", newCfInfo.SinkURI)
	oldInfo.StartTs = 10
	cfg.TargetTs = 9
	newCfInfo, newUpInfo, err = h.verifyUpdateChangefeedConfig(ctx, cfg, oldInfo, oldUpInfo, storage, 0)
	require.NotNil(t, err)

	cfg.StartTs = 0
	cfg.TargetTs = 0
	cfg.ReplicaConfig.EnableOldValue = true
	cfg.SinkURI = "blackhole://127.0.0.1:9092/test?protocol=avro"
	newCfInfo, newUpInfo, err = h.verifyUpdateChangefeedConfig(ctx, cfg, oldInfo, oldUpInfo, storage, 0)
	require.NoError(t, err)
	require.False(t, newCfInfo.Config.EnableOldValue)

	cfg.ReplicaConfig.ForceReplicate = true
	newCfInfo, newUpInfo, err = h.verifyUpdateChangefeedConfig(ctx, cfg, oldInfo, oldUpInfo, storage, 0)
	require.Error(t, cerror.ErrOldValueNotEnabled, err)
}
