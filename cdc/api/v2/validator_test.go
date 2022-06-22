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

	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/owner"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

type mockPdClient struct {
	pd.Client
	logicTime int64
	timestamp int64
}

type mockStatusProvider struct {
	owner.StatusProvider
	changefeedStatus *model.ChangeFeedStatus
	err              error
}

// GetChangeFeedStatus returns a changefeeds' runtime status.
func (m *mockStatusProvider) GetChangeFeedStatus(ctx context.Context,
	changefeedID model.ChangeFeedID,
) (*model.ChangeFeedStatus, error) {
	return m.changefeedStatus, m.err
}

func (m *mockPdClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string,
	ttl int64, safePoint uint64,
) (uint64, error) {
	return safePoint, nil
}

func (m *mockPdClient) GetTS(ctx context.Context) (int64, int64, error) {
	return m.logicTime, m.timestamp, nil
}

func (m *mockPdClient) GetClusterID(ctx context.Context) uint64 {
	return 123
}

type mockStorage struct {
	tidbkv.Storage
}

func TestVerifyCreateChangefeedConfig(t *testing.T) {
	ctx := context.Background()
	pdClient := &mockPdClient{}
	storage := &mockStorage{}
	provider := &mockStatusProvider{}
	cfg := &ChangefeedConfig{}
	cfInfo, err := verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.Nil(t, cfInfo)
	require.NotNil(t, err)
	cfg.SinkURI = "blackhole://"
	// repliconfig is nil
	require.Panics(t, func() {
		_, _ = verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	})
	cfg.ReplicaConfig = GetDefaultReplicaConfig()
	cfg.ReplicaConfig.ForceReplicate = true
	cfg.ReplicaConfig.EnableOldValue = false
	// disable old value but force replicate
	cfInfo, err = verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	require.Nil(t, cfInfo)
	cfg.ReplicaConfig.ForceReplicate = false
	cfg.ReplicaConfig.IgnoreIneligibleTable = true
	cfInfo, err = verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.Nil(t, err)
	require.NotNil(t, cfInfo)
	require.NotEqual(t, "", cfInfo.ID)
	require.Equal(t, model.DefaultNamespace, cfInfo.Namespace)

	cfg.ID = "abdc/sss"
	cfInfo, err = verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	cfg.ID = ""
	cfg.Namespace = "abdc/sss"
	cfInfo, err = verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	cfg.ID = ""
	cfg.Namespace = ""
	// changefeed already exists
	provider.changefeedStatus = &model.ChangeFeedStatus{}
	cfInfo, err = verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	provider.changefeedStatus = nil
	provider.err = cerror.ErrChangeFeedNotExists.GenWithStackByArgs("aaa")
	cfInfo, err = verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.Nil(t, err)
	require.Equal(t, uint64(123), cfInfo.UpstreamID)
	cfg.TargetTs = 3
	cfg.StartTs = 4
	cfInfo, err = verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	cfg.TargetTs = 6
	cfg.ReplicaConfig.EnableOldValue = false
	cfg.SinkURI = "aaab://"
	cfInfo, err = verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	cfg.SinkURI = string([]byte{0x7f, ' '})
	cfInfo, err = verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.NotNil(t, err)
	cfg.SinkURI = "blackhole://sss?protocol=canal"
	cfg.ReplicaConfig.EnableOldValue = false
	cfInfo, err = verifyCreateChangefeedConfig(ctx, cfg, pdClient, provider, "en", storage)
	require.Nil(t, err)
	require.True(t, cfInfo.Config.EnableOldValue)
}

func TestVerifyUpdateChangefeedConfig(t *testing.T) {
	ctx := context.Background()
	cfg := &ChangefeedConfig{}
	oldInfo := &model.ChangeFeedInfo{}
	oldUpInfo := &model.UpstreamInfo{}
	newCfInfo, newUpInfo, err := verifyUpdateChangefeedConfig(ctx, cfg, oldInfo, oldUpInfo)
	require.NotNil(t, err)
	require.Nil(t, newCfInfo)
	require.Nil(t, newUpInfo)
	// namespace and id can not be updated
	cfg.Namespace = "abc"
	cfg.ID = "1234"
	newCfInfo, newUpInfo, err = verifyUpdateChangefeedConfig(ctx, cfg, oldInfo, oldUpInfo)
	require.NotNil(t, err)
	require.Nil(t, newCfInfo)
	require.Nil(t, newUpInfo)
	cfg.StartTs = 2
	cfg.TargetTs = 10
	cfg.Engine = model.SortInMemory
	cfg.ReplicaConfig = ToAPIReplicaConfig(config.GetDefaultReplicaConfig())
	cfg.SyncPointEnabled = true
	cfg.SyncPointInterval = 10 * time.Second
	cfg.PDAddrs = []string{"a", "b"}
	cfg.CertPath = "p1"
	cfg.CAPath = "p2"
	cfg.KeyPath = "p3"
	cfg.SinkURI = "blackhole://"
	cfg.CertAllowedCN = []string{"c", "d"}
	newCfInfo, newUpInfo, err = verifyUpdateChangefeedConfig(ctx, cfg, oldInfo, oldUpInfo)
	require.Nil(t, err)
	// startTs can not be updated
	require.Equal(t, uint64(0), newCfInfo.StartTs)
	require.Equal(t, uint64(10), newCfInfo.TargetTs)
	require.Equal(t, model.SortInMemory, newCfInfo.Engine)
	require.Equal(t, true, newCfInfo.SyncPointEnabled)
	require.Equal(t, 10*time.Second, newCfInfo.SyncPointInterval)
	require.Equal(t, config.GetDefaultReplicaConfig(), newCfInfo.Config)
	require.Equal(t, "a,b", newUpInfo.PDEndpoints)
	require.Equal(t, "p1", newUpInfo.CertPath)
	require.Equal(t, "p2", newUpInfo.CAPath)
	require.Equal(t, "p3", newUpInfo.KeyPath)
	require.Equal(t, []string{"c", "d"}, newUpInfo.CertAllowedCN)
	require.Equal(t, "blackhole://", newCfInfo.SinkURI)
	oldInfo.StartTs = 10
	cfg.TargetTs = 9
	newCfInfo, newUpInfo, err = verifyUpdateChangefeedConfig(ctx, cfg, oldInfo, oldUpInfo)
	require.NotNil(t, err)
}
