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

	"github.com/golang/mock/gomock"
	mock_controller "github.com/pingcap/tiflow/cdc/controller/mock"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestVerifyCreateChangefeedConfig(t *testing.T) {
	ctx := context.Background()
	pdClient := &mockPDClient{}
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test;")
	storage := helper.Storage()
	ctrl := mock_controller.NewMockController(gomock.NewController(t))
	cfg := &ChangefeedConfig{}
	h := &APIV2HelpersImpl{}
	cfInfo, err := h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	require.Nil(t, cfInfo)
	require.NotNil(t, err)
	cfg.SinkURI = "blackhole://"
	ctrl.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(false, nil)
	// repliconfig is nil
	require.Panics(t, func() {
		_, _ = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	})
	cfg.ReplicaConfig = GetDefaultReplicaConfig()
	cfg.ReplicaConfig.ForceReplicate = false
	cfg.ReplicaConfig.IgnoreIneligibleTable = true
	cfg.SinkURI = "blackhole://"
	ctrl.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(false, nil)
	cfInfo, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	require.Nil(t, err)
	require.NotNil(t, cfInfo)
	require.NotEqual(t, "", cfInfo.ID)
	require.Equal(t, model.DefaultNamespace, cfInfo.Namespace)
	require.NotEqual(t, 0, cfInfo.Epoch)

	// invalid changefeed id or namespace id
	cfg.ID = "abdc/sss"
	_, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	require.NotNil(t, err)
	cfg.ID = ""
	cfg.Namespace = "abdc/sss"
	_, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	require.NotNil(t, err)
	cfg.ID = ""
	cfg.Namespace = ""
	// changefeed already exists
	ctrl.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(true, nil)
	_, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	require.NotNil(t, err)
	ctrl.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(false, cerror.ErrChangeFeedNotExists.GenWithStackByArgs("aaa"))
	_, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	require.Nil(t, err)
	require.Equal(t, uint64(123), cfInfo.UpstreamID)
	cfg.TargetTs = 3
	cfg.StartTs = 4
	ctrl.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(false, nil)
	_, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	require.NotNil(t, err)
	cfg.TargetTs = 6
	cfg.SinkURI = "aaab://"
	ctrl.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(false, nil)
	_, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	require.NotNil(t, err)
	cfg.SinkURI = string([]byte{0x7f, ' '})
	ctrl.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(false, nil)
	_, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	require.NotNil(t, err)

	cfg.StartTs = 0
	// use blackhole to workaround
	cfg.SinkURI = "blackhole://127.0.0.1:9092/test?protocol=avro"
	cfg.ReplicaConfig.ForceReplicate = false
	ctrl.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(false, nil)
	_, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	require.NoError(t, err)

	cfg.ReplicaConfig.ForceReplicate = true
	ctrl.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(false, nil)
	_, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	require.Error(t, cerror.ErrOldValueNotEnabled, err)

	// invalid start-ts, in the future
	cfg.StartTs = 1000000000000000000
	ctrl.EXPECT().IsChangefeedExists(gomock.Any(), gomock.Any()).Return(false, nil)
	_, err = h.verifyCreateChangefeedConfig(ctx, cfg, pdClient, ctrl, "en", storage)
	require.Error(t, cerror.ErrAPIInvalidParam, err)
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
	require.Equal(t, uint64(0), newCfInfo.StartTs)
	require.Equal(t, uint64(10), newCfInfo.TargetTs)
	require.Equal(t, 30*time.Second, util.GetOrZero(newCfInfo.Config.SyncPointInterval))
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
}
