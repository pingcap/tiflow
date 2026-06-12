// Copyright 2019 PingCAP, Inc.
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
	"fmt"
	"sort"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/stretchr/testify/suite"
)

var (
	targetTbl = &filter.Table{
		Schema: "target_db",
		Name:   "tbl",
	}
	target     = targetTbl.String()
	sourceTbl1 = &filter.Table{Schema: "db1", Name: "tbl1"}
	sourceTbl2 = &filter.Table{Schema: "db1", Name: "tbl2"}
	sourceTbl3 = &filter.Table{Schema: "db1", Name: "tbl3"}
	sourceTbl4 = &filter.Table{Schema: "db1", Name: "tbl4"}
	source1    = sourceTbl1.String()
	source2    = sourceTbl2.String()
	source3    = sourceTbl3.String()
	source4    = sourceTbl4.String()
	pos11      = binlog.Location{Position: mysql.Position{Name: "mysql-bin.000002", Pos: 123}}
	endPos11   = binlog.Location{Position: mysql.Position{Name: "mysql-bin.000002", Pos: 456}}
	pos12      = binlog.Location{Position: mysql.Position{Name: "mysql-bin.000002", Pos: 789}}
	endPos12   = binlog.Location{Position: mysql.Position{Name: "mysql-bin.000002", Pos: 999}}
	pos21      = binlog.Location{Position: mysql.Position{Name: "mysql-bin.000001", Pos: 123}}
	endPos21   = binlog.Location{Position: mysql.Position{Name: "mysql-bin.000001", Pos: 456}}
	pos22      = binlog.Location{Position: mysql.Position{Name: "mysql-bin.000001", Pos: 789}}
	endPos22   = binlog.Location{Position: mysql.Position{Name: "mysql-bin.000001", Pos: 999}}
	pos3       = binlog.Location{Position: mysql.Position{Name: "mysql-bin.000003", Pos: 123}}
	endPos3    = binlog.Location{Position: mysql.Position{Name: "mysql-bin.000003", Pos: 456}}
	ddls1      = []string{"DUMMY DDL"}
	ddls2      = []string{"ANOTHER DUMMY DDL"}
)

type testShardingGroupSuite struct {
	suite.Suite
	cfg *config.SubTaskConfig
}

func (t *testShardingGroupSuite) SetupSuite() {
	t.cfg = &config.SubTaskConfig{
		SourceID:   "mysql-replica-01",
		MetaSchema: "test",
		Name:       "checkpoint_ut",
	}
}

func (t *testShardingGroupSuite) TestLowestFirstPosInGroups() {
	k := NewShardingGroupKeeper(tcontext.Background(), t.cfg, nil)

	g1 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{"db1.tbl1", "db1.tbl2"}, nil, false, "", false)
	// nolint:dogsled
	_, _, _, err := g1.TrySync("db1.tbl1", pos11, endPos11, ddls1)
	t.Require().NoError(err)

	// lowest
	g2 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{"db2.tbl1", "db2.tbl2"}, nil, false, "", false)
	// nolint:dogsled
	_, _, _, err = g2.TrySync("db2.tbl1", pos21, endPos21, ddls1)
	t.Require().NoError(err)

	g3 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{"db3.tbl1", "db3.tbl2"}, nil, false, "", false)
	// nolint:dogsled
	_, _, _, err = g3.TrySync("db3.tbl1", pos3, endPos3, ddls1)
	t.Require().NoError(err)

	k.groups["db1.tbl"] = g1
	k.groups["db2.tbl"] = g2
	k.groups["db3.tbl"] = g3

	t.Require().Equal(pos21.Position, k.lowestFirstLocationInGroups().Position)
}

func (t *testShardingGroupSuite) TestMergeAndLeave() {
	k := NewShardingGroupKeeper(tcontext.Background(), t.cfg, nil)
	g1 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{source1, source2}, nil, false, "", false)
	t.Require().Equal(map[string]bool{source1: false, source2: false}, g1.Sources())

	needShardingHandle, synced, remain, err := g1.Merge([]string{source3})
	t.Require().NoError(err)
	t.Require().False(needShardingHandle)
	t.Require().False(synced)
	t.Require().Equal(3, remain)

	// repeat merge has no side effect
	needShardingHandle, synced, remain, err = g1.Merge([]string{source3})
	t.Require().NoError(err)
	t.Require().False(needShardingHandle)
	t.Require().False(synced)
	t.Require().Equal(3, remain)

	err = g1.Leave([]string{source1})
	t.Require().NoError(err)
	t.Require().Equal(map[string]bool{source3: false, source2: false}, g1.Sources())

	// repeat leave has no side effect
	err = g1.Leave([]string{source1})
	t.Require().NoError(err)
	t.Require().Equal(map[string]bool{source3: false, source2: false}, g1.Sources())

	ddls := []string{"DUMMY DDL"}
	pos1 := mysql.Position{Name: "mysql-bin.000002", Pos: 123}
	endPos1 := mysql.Position{Name: "mysql-bin.000002", Pos: 456}
	// nolint:dogsled
	_, _, _, err = g1.TrySync(source1, binlog.Location{Position: pos1}, binlog.Location{Position: endPos1}, ddls)
	t.Require().NoError(err)

	// nolint:dogsled
	_, _, _, err = g1.Merge([]string{source1})
	t.Require().True(terror.ErrSyncUnitAddTableInSharding.Equal(err))
	err = g1.Leave([]string{source2})
	t.Require().True(terror.ErrSyncUnitDropSchemaTableInSharding.Equal(err))
}

func (t *testShardingGroupSuite) TestSync() {
	k := NewShardingGroupKeeper(tcontext.Background(), t.cfg, nil)
	g1 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{source1, source2}, nil, false, "", false)
	synced, active, remain, err := g1.TrySync(source1, pos11, endPos11, ddls1)
	t.Require().NoError(err)
	t.Require().False(synced)
	t.Require().True(active)
	t.Require().Equal(1, remain)
	synced, active, remain, err = g1.TrySync(source1, pos12, endPos12, ddls2)
	t.Require().NoError(err)
	t.Require().False(synced)
	t.Require().False(active)
	t.Require().Equal(1, remain)

	t.Require().Equal(&pos11, g1.FirstLocationUnresolved())
	t.Require().Equal(&endPos11, g1.FirstEndPosUnresolved())
	loc, err := g1.ActiveDDLFirstLocation()
	t.Require().NoError(err)
	t.Require().Equal(pos11, loc)

	// not call `TrySync` for source2, beforeActiveDDL is always true
	beforeActiveDDL := g1.CheckSyncing(source2, pos21)
	t.Require().True(beforeActiveDDL)

	info := g1.UnresolvedGroupInfo()
	shouldBe := &pb.ShardingGroup{Target: "", DDLs: ddls1, FirstLocation: pos11.String(), Synced: []string{source1}, Unsynced: []string{source2}}
	t.Require().Equal(shouldBe, info)

	// simple sort for [][]string{[]string{"db1", "tbl2"}, []string{"db1", "tbl1"}}
	tbls1 := g1.Tables()
	tbls2 := g1.UnresolvedTables()
	if tbls1[0].Name != tbls2[0].Name {
		tbls1[0], tbls1[1] = tbls1[1], tbls1[0]
	}
	t.Require().Equal(tbls2, tbls1)

	// sync first DDL for source2, synced but not resolved
	synced, active, remain, err = g1.TrySync(source2, pos21, endPos21, ddls1)
	t.Require().NoError(err)
	t.Require().True(synced)
	t.Require().True(active)
	t.Require().Equal(0, remain)

	// active DDL is at pos21
	beforeActiveDDL = g1.CheckSyncing(source2, pos21)
	t.Require().True(beforeActiveDDL)

	info = g1.UnresolvedGroupInfo()
	sort.Strings(info.Synced)
	shouldBe = &pb.ShardingGroup{Target: "", DDLs: ddls1, FirstLocation: pos11.String(), Synced: []string{source1, source2}, Unsynced: []string{}}
	t.Require().Equal(shouldBe, info)

	resolved := g1.ResolveShardingDDL()
	t.Require().False(resolved)

	// next active DDL not present
	beforeActiveDDL = g1.CheckSyncing(source2, pos21)
	t.Require().True(beforeActiveDDL)

	synced, active, remain, err = g1.TrySync(source2, pos22, endPos22, ddls2)
	t.Require().NoError(err)
	t.Require().True(synced)
	t.Require().True(active)
	t.Require().Equal(0, remain)
	resolved = g1.ResolveShardingDDL()
	t.Require().True(resolved)

	// caller should reset sharding group if DDL is successful executed
	g1.Reset()

	info = g1.UnresolvedGroupInfo()
	t.Require().Nil(info)
	t.Require().Nil(g1.UnresolvedTables())
}

func (t *testShardingGroupSuite) TestTableID() {
	originTables := []*filter.Table{
		{Schema: "db", Name: "table"},
		{Schema: `d"b`, Name: `t"able"`},
		{Schema: "d`b", Name: "t`able"},
	}
	for _, originTable := range originTables {
		// ignore isSchemaOnly
		tableID := utils.GenTableID(originTable)
		table := utils.UnpackTableID(tableID)
		t.Require().Equal(originTable, table)
	}
}

func (t *testShardingGroupSuite) TestKeeper() {
	k := NewShardingGroupKeeper(tcontext.Background(), t.cfg, nil)
	k.clear()
	db, mock, err := sqlmock.New()
	t.Require().NoError(err)
	dbConn, err := db.Conn(context.Background())
	t.Require().NoError(err)
	k.db = conn.NewBaseDBForTest(db)
	k.dbConn = dbconn.NewDBConn(t.cfg, conn.NewBaseConnForTest(dbConn, &retry.FiniteRetryStrategy{}))
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS `%s`", t.cfg.MetaSchema)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.*", dbutil.TableName(t.cfg.MetaSchema, cputil.SyncerShardMeta(t.cfg.Name)))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	t.Require().Nil(k.prepare())

	// test meta

	mock.ExpectQuery(" SELECT `target_table_id`, `source_table_id`, `active_index`, `is_global`, `data` FROM `test`.`checkpoint_ut_syncer_sharding_meta`.*").
		WillReturnRows(sqlmock.NewRows([]string{"target_table_id", "source_table_id", "active_index", "is_global", "data"}))
	meta, err := k.LoadShardMeta(mysql.MySQLFlavor, false)
	t.Require().NoError(err)
	t.Require().Len(meta, 0)
	mock.ExpectQuery(" SELECT `target_table_id`, `source_table_id`, `active_index`, `is_global`, `data` FROM `test`.`checkpoint_ut_syncer_sharding_meta`.*").
		WillReturnRows(sqlmock.NewRows([]string{"target_table_id", "source_table_id", "active_index", "is_global", "data"}).
			AddRow(target, "", 0, true, "[{\"ddls\":[\"DUMMY DDL\"],\"source\":\"`db1`.`tbl1`\",\"first-position\":{\"Name\":\"mysql-bin.000002\",\"Pos\":123},\"first-gtid-set\":\"\"},{\"ddls\":[\"ANOTHER DUMMY DDL\"],\"source\":\"`db1`.`tbl1`\",\"first-position\":{\"Name\":\"mysql-bin.000002\",\"Pos\":789},\"first-gtid-set\":\"\"}]").
			AddRow(target, source1, 0, false, "[{\"ddls\":[\"DUMMY DDL\"],\"source\":\"`db1`.`tbl1`\",\"first-position\":{\"Name\":\"mysql-bin.000002\",\"Pos\":123},\"first-gtid-set\":\"\"},{\"ddls\":[\"ANOTHER DUMMY DDL\"],\"source\":\"`db1`.`tbl1`\",\"first-position\":{\"Name\":\"mysql-bin.000002\",\"Pos\":789},\"first-gtid-set\":\"\"}]"))

	meta, err = k.LoadShardMeta(mysql.MySQLFlavor, false)
	t.Require().NoError(err)
	t.Require().Len(meta, 1) // has meta of `target`

	// test AddGroup and LeaveGroup

	needShardingHandle, group, synced, remain, err := k.AddGroup(targetTbl, []string{source1}, nil, true)
	t.Require().NoError(err)
	t.Require().False(needShardingHandle)
	t.Require().NotNil(group)
	t.Require().False(synced)
	t.Require().Equal(0, remain) // first time doesn't return `remain`

	needShardingHandle, group, synced, remain, err = k.AddGroup(targetTbl, []string{source2}, nil, true)
	t.Require().NoError(err)
	t.Require().False(needShardingHandle)
	t.Require().NotNil(group)
	t.Require().False(synced)
	t.Require().Equal(2, remain)

	// test LeaveGroup
	// nolint:dogsled
	_, _, _, remain, err = k.AddGroup(targetTbl, []string{source3}, nil, true)
	t.Require().NoError(err)
	t.Require().Equal(3, remain)
	// nolint:dogsled
	_, _, _, remain, err = k.AddGroup(targetTbl, []string{source4}, nil, true)
	t.Require().NoError(err)
	t.Require().Equal(4, remain)
	t.Require().Nil(k.LeaveGroup(targetTbl, []string{source3, source4}))

	// test TrySync and InSyncing

	needShardingHandle, group, synced, active, remain, err := k.TrySync(sourceTbl1, targetTbl, pos12, endPos12, ddls1)
	t.Require().NoError(err)
	t.Require().True(needShardingHandle)
	t.Require().Equal(map[string]bool{source1: true, source2: false}, group.sources)
	t.Require().False(synced)
	t.Require().True(active)
	t.Require().Equal(1, remain)

	t.Require().False(k.InSyncing(sourceTbl1, &filter.Table{Schema: targetTbl.Schema, Name: "wrong table"}, pos11))
	loc, err := k.ActiveDDLFirstLocation(targetTbl)
	t.Require().NoError(err)
	// position before active DDL, not in syncing
	t.Require().Equal(-1, binlog.CompareLocation(endPos11, loc, false))
	t.Require().False(k.InSyncing(sourceTbl1, targetTbl, endPos11))
	// position at/after active DDL, in syncing
	t.Require().Equal(0, binlog.CompareLocation(pos12, loc, false))
	t.Require().False(k.InSyncing(sourceTbl1, targetTbl, pos12))
	t.Require().Equal(1, binlog.CompareLocation(endPos12, loc, false))
	t.Require().True(k.InSyncing(sourceTbl1, targetTbl, endPos12))

	needShardingHandle, group, synced, active, remain, err = k.TrySync(sourceTbl2, targetTbl, pos21, endPos21, ddls1)
	t.Require().NoError(err)
	t.Require().True(needShardingHandle)
	t.Require().Equal(map[string]bool{source1: true, source2: true}, group.sources)
	t.Require().True(synced)
	t.Require().True(active)
	t.Require().Equal(0, remain)

	unresolvedTarget, unresolvedTables := k.UnresolvedTables()
	t.Require().Equal(map[string]bool{target: true}, unresolvedTarget)
	// simple re-order
	if unresolvedTables[0].Name > unresolvedTables[1].Name {
		unresolvedTables[0], unresolvedTables[1] = unresolvedTables[1], unresolvedTables[0]
	}
	t.Require().Equal([]*filter.Table{sourceTbl1, sourceTbl2}, unresolvedTables)

	unresolvedGroups := k.UnresolvedGroups()
	t.Require().Len(unresolvedGroups, 1)
	g := unresolvedGroups[0]
	t.Require().Len(g.Unsynced, 0)
	t.Require().Equal(ddls1, g.DDLs)
	t.Require().Equal(pos12.String(), g.FirstLocation)

	sqls, args := k.PrepareFlushSQLs(unresolvedTarget)
	t.Require().Len(sqls, 0)
	t.Require().Len(args, 0)

	reset, err := k.ResolveShardingDDL(targetTbl)
	t.Require().NoError(err)
	t.Require().True(reset)

	k.ResetGroups()

	unresolvedTarget, unresolvedTables = k.UnresolvedTables()
	t.Require().Len(unresolvedTarget, 0)
	t.Require().Len(unresolvedTables, 0)
}
