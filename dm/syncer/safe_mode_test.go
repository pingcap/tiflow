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
package syncer

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	mode "github.com/pingcap/tiflow/dm/syncer/safe-mode"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/integration"
	"go.uber.org/zap"
)

type mockCheckpointForSafeMode struct {
	CheckPoint

	safeModeExitPoint *binlog.Location
}

func (c *mockCheckpointForSafeMode) SafeModeExitPoint() *binlog.Location {
	return c.safeModeExitPoint
}

func TestEnableSafeModeInitializationPhase(t *testing.T) {
	mockCluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer mockCluster.Terminate(t)
	etcdTestCli := mockCluster.RandClient()

	l := log.With(zap.String("unit test", "TestEnableSafeModeInitializationPhase"))
	s := &Syncer{
		tctx:     tcontext.Background().WithLogger(l),
		safeMode: mode.NewSafeMode(), cli: etcdTestCli,
		cfg: &config.SubTaskConfig{
			Name: "test", SourceID: "test",
			SyncerConfig: config.SyncerConfig{CheckpointFlushInterval: 1},
		},
	}

	// test enable by task cliArgs
	duration, err := time.ParseDuration("2s")
	require.NoError(t, err)
	s.cliArgs = &config.TaskCliArgs{SafeModeDuration: duration.String()}
	s.enableSafeModeInitializationPhase(s.tctx)
	time.Sleep(time.Second) // wait for enableSafeModeInitializationPhase running
	require.True(t, s.safeMode.Enable())
	time.Sleep(duration) // wait for enableSafeModeInitializationPhase exit
	require.False(t, s.safeMode.Enable())
	s.Lock()
	require.Equal(t, s.cliArgs.SafeModeDuration, "")
	s.Unlock()

	// test enable by config
	s.cliArgs = nil
	s.cfg.SafeMode = true
	mockCheckpoint := &mockCheckpointForSafeMode{}
	s.checkpoint = mockCheckpoint
	s.enableSafeModeInitializationPhase(s.tctx)
	require.True(t, s.safeMode.Enable())

	// test enable by SafeModeExitPoint
	s.cfg.SafeMode = false
	mockCheckpoint.safeModeExitPoint = &binlog.Location{Position: mysql.Position{Name: "mysql-bin.000123", Pos: 123}}
	s.enableSafeModeInitializationPhase(s.tctx)
	require.True(t, s.safeMode.Enable())

	// test enable by initPhaseSeconds
	s.enableSafeModeInitializationPhase(s.tctx)
	time.Sleep(time.Second) // wait for enableSafeModeInitializationPhase running
	require.True(t, s.safeMode.Enable())
	time.Sleep(time.Second * 2) // wait for enableSafeModeInitializationPhase exit
	require.False(t, s.safeMode.Enable())
}
