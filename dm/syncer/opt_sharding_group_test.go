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
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/shardddl"
)

type optShardingGroupSuite struct {
	suite.Suite
	cfg *config.SubTaskConfig
}

func (s *optShardingGroupSuite) SetupSuite() {
	s.cfg = &config.SubTaskConfig{
		SourceID:   "mysql-replica-01",
		MetaSchema: "test",
		Name:       "checkpoint_ut",
	}
	require.NoError(s.T(), log.InitLogger(&log.Config{}))
}

func TestOptShardingGroupSuite(t *testing.T) {
	suite.Run(t, new(optShardingGroupSuite))
}

func (s *optShardingGroupSuite) TestLowestFirstPosInOptGroups() {
	s.T().Parallel()
	var (
		db1tbl     = "`db1`.`tbl`"
		db2tbl     = "`db2`.`tbl`"
		db3tbl     = "`db3`.`tbl`"
		sourceTbls = []string{"`db1`.`tbl1`", "`db1`.`tbl2`", "`db2`.`tbl1`", "`db2`.`tbl2`", "`db3`.`tbl1`"}
		targetTbls = []string{db1tbl, db1tbl, db2tbl, db2tbl, db3tbl}
		positions  = []binlog.Location{pos11, pos12, pos21, pos22, pos3}
	)

	k := NewOptShardingGroupKeeper(tcontext.Background(), s.cfg)
	for i := range sourceTbls {
		k.appendConflictTable(utils.UnpackTableID(sourceTbls[i]), utils.UnpackTableID(targetTbls[i]), positions[i], "", false)
	}

	require.Equal(s.T(), pos21.Position, k.lowestFirstLocationInGroups().Position)
	k.resolveGroup(utils.UnpackTableID(db2tbl))
	k.addShardingReSync(&ShardingReSync{
		targetTable:  utils.UnpackTableID(db2tbl),
		currLocation: pos21,
	})
	// should still be pos21, because it's added to shardingReSyncs
	require.Equal(s.T(), pos21.Position, k.lowestFirstLocationInGroups().Position)
	k.removeShardingReSync(&ShardingReSync{targetTable: utils.UnpackTableID(db2tbl)})
	// should be pos11 now, pos21 is totally resolved
	require.Equal(s.T(), pos11.Position, k.lowestFirstLocationInGroups().Position)
}

func (s *optShardingGroupSuite) TestSync() {
	s.T().Parallel()
	var (
		db1tbl     = "`db1`.`tbl`"
		db2tbl     = "`db2`.`tbl`"
		db3tbl     = "`db3`.`tbl`"
		sourceTbls = []string{"`db1`.`tbl1`", "`db1`.`tbl2`", "`db2`.`tbl1`", "`db2`.`tbl2`", "`db3`.`tbl1`"}
		targetTbls = []string{db1tbl, db1tbl, db2tbl, db2tbl, db3tbl}
		positions  = []binlog.Location{pos11, pos12, pos21, pos22, pos3}
		logger     = log.L()
		err        error
	)

	k := NewOptShardingGroupKeeper(tcontext.Background(), s.cfg)
	for i := range sourceTbls {
		k.appendConflictTable(utils.UnpackTableID(sourceTbls[i]), utils.UnpackTableID(targetTbls[i]), positions[i], "", false)
	}

	shardingReSyncCh := make(chan *ShardingReSync, 10)

	syncer := Syncer{
		osgk:       k,
		tctx:       tcontext.Background().WithLogger(logger),
		optimist:   shardddl.NewOptimist(&logger, nil, "", ""),
		checkpoint: &mockCheckpoint{},
	}
	syncer.schemaTracker, err = schema.NewTracker(context.Background(), s.cfg.Name, defaultTestSessionCfg, syncer.downstreamTrackConn)
	require.NoError(s.T(), err)

	// case 1: mock receive resolved stage from dm-master when syncing other tables
	require.Equal(s.T(), pos21.Position, k.lowestFirstLocationInGroups().Position)
	require.True(s.T(), k.tableInConflict(utils.UnpackTableID(db2tbl)))
	require.True(s.T(), k.inConflictStage(utils.UnpackTableID(sourceTbls[3]), utils.UnpackTableID(db2tbl)))
	syncer.resolveOptimisticDDL(&eventContext{
		shardingReSyncCh: &shardingReSyncCh,
		currentLocation:  &endPos3,
	}, utils.UnpackTableID(sourceTbls[2]), utils.UnpackTableID(db2tbl))
	require.False(s.T(), k.tableInConflict(utils.UnpackTableID(db2tbl)))
	require.False(s.T(), k.inConflictStage(utils.UnpackTableID(sourceTbls[3]), utils.UnpackTableID(db2tbl)))
	require.Len(s.T(), shardingReSyncCh, 1)
	shardingResync := <-shardingReSyncCh
	expectedShardingResync := &ShardingReSync{
		currLocation:   pos21,
		latestLocation: endPos3,
		targetTable:    utils.UnpackTableID(db2tbl),
		allResolved:    true,
	}
	require.Equal(s.T(), expectedShardingResync, shardingResync)
	// the ShardingResync is not removed from osgk, so lowest location is still pos21
	require.Equal(s.T(), pos21.Position, k.lowestFirstLocationInGroups().Position)
	k.removeShardingReSync(shardingResync)

	// case 2: mock receive resolved stage from dm-master in handleQueryEventOptimistic
	require.Equal(s.T(), pos11.Position, k.lowestFirstLocationInGroups().Position)
	require.True(s.T(), k.tableInConflict(utils.UnpackTableID(db1tbl)))
	require.True(s.T(), k.inConflictStage(utils.UnpackTableID(sourceTbls[0]), utils.UnpackTableID(db1tbl)))
	syncer.resolveOptimisticDDL(&eventContext{
		shardingReSyncCh: &shardingReSyncCh,
		currentLocation:  &endPos12,
	}, utils.UnpackTableID(sourceTbls[1]), utils.UnpackTableID(db1tbl))
	require.False(s.T(), k.tableInConflict(utils.UnpackTableID(db1tbl)))
	require.False(s.T(), k.inConflictStage(utils.UnpackTableID(sourceTbls[0]), utils.UnpackTableID(db1tbl)))
	require.Len(s.T(), shardingReSyncCh, 1)
	shardingResync = <-shardingReSyncCh
	expectedShardingResync = &ShardingReSync{
		currLocation:   pos11,
		latestLocation: endPos12,
		targetTable:    utils.UnpackTableID(db1tbl),
		allResolved:    true,
	}
	require.Equal(s.T(), expectedShardingResync, shardingResync)
	require.Equal(s.T(), pos11.Position, k.lowestFirstLocationInGroups().Position)
	k.removeShardingReSync(shardingResync)

	// case 3: mock drop table, should resolve conflict stage
	require.Equal(s.T(), pos3.Position, k.lowestFirstLocationInGroups().Position)
	require.True(s.T(), k.tableInConflict(utils.UnpackTableID(db3tbl)))
	require.True(s.T(), k.inConflictStage(utils.UnpackTableID(sourceTbls[4]), utils.UnpackTableID(db3tbl)))
	k.RemoveGroup(utils.UnpackTableID(db3tbl), []string{sourceTbls[4]})
	require.False(s.T(), k.tableInConflict(utils.UnpackTableID(db3tbl)))
	require.False(s.T(), k.inConflictStage(utils.UnpackTableID(sourceTbls[4]), utils.UnpackTableID(db3tbl)))
	require.Len(s.T(), shardingReSyncCh, 0)
}
