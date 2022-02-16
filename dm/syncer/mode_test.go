package syncer

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

import (
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/dm/config"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	mode "github.com/pingcap/tiflow/dm/syncer/safe-mode"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/integration"
	"go.uber.org/zap"
)

func TestEnableSafeModeInitializationPhase(t *testing.T) {
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)
	etcdTestCli := mockCluster.RandClient()

	l := log.With(zap.String("unit test", "TestEnableSafeModeInitializationPhase"))
	s := &Syncer{
		tctx:     tcontext.Background().WithLogger(l),
		safeMode: mode.NewSafeMode(), cli: etcdTestCli,
		cfg: &config.SubTaskConfig{Name: "test", SourceID: "test"},
	}

	duration, err := time.ParseDuration("2s")
	require.NoError(t, err)

	s.cliArgs = &config.TaskCliArgs{SafeModeDuration: duration.String()}
	s.enableSafeModeInitializationPhase(s.tctx)
	time.Sleep(time.Second) // wait for enableSafeModeInitializationPhase running
	require.True(t, s.safeMode.Enable())

	time.Sleep(duration) // wait for enableSafeModeInitializationPhase exit
	require.False(t, s.safeMode.Enable())
	require.Equal(t, s.cliArgs.SafeModeDuration, "")
}
