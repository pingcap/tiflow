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
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	mode "github.com/pingcap/tiflow/dm/syncer/safe-mode"
)

type mockCheckpointForSafeMode struct {
	CheckPoint

	safeModeExitPoint *binlog.Location
}

func (c *mockCheckpointForSafeMode) SafeModeExitPoint() *binlog.Location {
	return c.safeModeExitPoint
}

func TestEnableSafeModeInitializationPhase(t *testing.T) {
	integration.BeforeTestExternal(t)
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

	// test enable by task cliArgs (disable is tested in it test)
	duration, err := time.ParseDuration("2s")
	require.NoError(t, err)
	s.cliArgs = &config.TaskCliArgs{SafeModeDuration: duration.String()}
	s.enableSafeModeInitializationPhase(s.tctx)
	require.True(t, s.safeMode.Enable())
	s.Lock()
	require.Nil(t, s.exitSafeModeTS) // not meet the first binlog
	firstBinlogTS := int64(1)
	require.NoError(t, s.initSafeModeExitTS(firstBinlogTS))
	require.NotNil(t, s.exitSafeModeTS) // not meet the first binlog
	require.Equal(t, int64(3), *s.exitSafeModeTS)
	require.Equal(t, firstBinlogTS, *s.firstMeetBinlogTS)
	s.Unlock()
	require.NoError(t, s.checkAndExitSafeModeByBinlogTS(s.tctx, *s.exitSafeModeTS)) // not exit when binlog TS == exit TS
	require.True(t, s.safeMode.Enable())
	require.NoError(t, s.checkAndExitSafeModeByBinlogTS(s.tctx, *s.exitSafeModeTS+int64(1))) // exit when binlog TS > exit TS
	require.False(t, s.safeMode.Enable())
	s.Lock()
	require.Nil(t, s.exitSafeModeTS)
	require.Equal(t, "", s.cliArgs.SafeModeDuration)
	s.Unlock()

	// test enable by config
	s.cliArgs = nil
	s.cfg.SafeMode = true
	mockCheckpoint := &mockCheckpointForSafeMode{}
	s.checkpoint = mockCheckpoint
	s.enableSafeModeInitializationPhase(s.tctx)
	require.True(t, s.safeMode.Enable())

	// test enable by SafeModeExitPoint (disable is tested in it test)
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
