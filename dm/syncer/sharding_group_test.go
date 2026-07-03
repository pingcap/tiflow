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
	. "github.com/pingcap/check"
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
)

var _ = Suite(&testShardingGroupSuite{})

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
	cfg *config.SubTaskConfig
}

func (t *testShardingGroupSuite) SetUpSuite(c *C) {
	t.cfg = &config.SubTaskConfig{
		SourceID:   "mysql-replica-01",
		MetaSchema: "test",
		Name:       "checkpoint_ut",
	}
}

func (t *testShardingGroupSuite) TestLowestFirstPosInGroups(c *C) {
	k := NewShardingGroupKeeper(tcontext.Background(), t.cfg, nil)

	g1 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{"db1.tbl1", "db1.tbl2"}, nil, false, "", false)
	// nolint:dogsled
	_, _, _, err := g1.TrySync("db1.tbl1", pos11, endPos11, ddls1)
	c.Assert(err, IsNil)

	// lowest
	g2 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{"db2.tbl1", "db2.tbl2"}, nil, false, "", false)
	// nolint:dogsled
	_, _, _, err = g2.TrySync("db2.tbl1", pos21, endPos21, ddls1)
	c.Assert(err, IsNil)

	g3 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{"db3.tbl1", "db3.tbl2"}, nil, false, "", false)
	// nolint:dogsled
	_, _, _, err = g3.TrySync("db3.tbl1", pos3, endPos3, ddls1)
	c.Assert(err, IsNil)

	k.groups["db1.tbl"] = g1
	k.groups["db2.tbl"] = g2
	k.groups["db3.tbl"] = g3

	c.Assert(k.lowestFirstLocationInGroups().Position, DeepEquals, pos21.Position)
}

func (t *testShardingGroupSuite) TestMergeAndLeave(c *C) {
	k := NewShardingGroupKeeper(tcontext.Background(), t.cfg, nil)
	g1 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{source1, source2}, nil, false, "", false)
	c.Assert(g1.Sources(), DeepEquals, map[string]bool{source1: false, source2: false})

	needShardingHandle, synced, remain, err := g1.Merge([]string{source3})
	c.Assert(err, IsNil)
	c.Assert(needShardingHandle, IsFalse)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 3)

	// repeat merge has no side effect
	needShardingHandle, synced, remain, err = g1.Merge([]string{source3})
	c.Assert(err, IsNil)
	c.Assert(needShardingHandle, IsFalse)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 3)

	err = g1.Leave([]string{source1})
	c.Assert(err, IsNil)
	c.Assert(g1.Sources(), DeepEquals, map[string]bool{source3: false, source2: false})

	// repeat leave has no side effect
	err = g1.Leave([]string{source1})
	c.Assert(err, IsNil)
	c.Assert(g1.Sources(), DeepEquals, map[string]bool{source3: false, source2: false})

	ddls := []string{"DUMMY DDL"}
	pos1 := mysql.Position{Name: "mysql-bin.000002", Pos: 123}
	endPos1 := mysql.Position{Name: "mysql-bin.000002", Pos: 456}
	// nolint:dogsled
	_, _, _, err = g1.TrySync(source1, binlog.Location{Position: pos1}, binlog.Location{Position: endPos1}, ddls)
	c.Assert(err, IsNil)

	// nolint:dogsled
	_, _, _, err = g1.Merge([]string{source1})
	c.Assert(terror.ErrSyncUnitAddTableInSharding.Equal(err), IsTrue)
	err = g1.Leave([]string{source2})
	c.Assert(terror.ErrSyncUnitDropSchemaTableInSharding.Equal(err), IsTrue)
}

func (t *testShardingGroupSuite) TestSync(c *C) {
	k := NewShardingGroupKeeper(tcontext.Background(), t.cfg, nil)
	g1 := NewShardingGroup(k.cfg.SourceID, k.shardMetaSchema, k.shardMetaTable, []string{source1, source2}, nil, false, "", false)
	synced, active, remain, err := g1.TrySync(source1, pos11, endPos11, ddls1)
	c.Assert(err, IsNil)
	c.Assert(synced, IsFalse)
	c.Assert(active, IsTrue)
	c.Assert(remain, Equals, 1)
	synced, active, remain, err = g1.TrySync(source1, pos12, endPos12, ddls2)
	c.Assert(err, IsNil)
	c.Assert(synced, IsFalse)
	c.Assert(active, IsFalse)
	c.Assert(remain, Equals, 1)

	c.Assert(g1.FirstLocationUnresolved(), DeepEquals, &pos11)
	c.Assert(g1.FirstEndPosUnresolved(), DeepEquals, &endPos11)
	loc, err := g1.ActiveDDLFirstLocation()
	c.Assert(err, IsNil)
	c.Assert(loc, DeepEquals, pos11)

	// not call `TrySync` for source2, beforeActiveDDL is always true
	beforeActiveDDL := g1.CheckSyncing(source2, pos21)
	c.Assert(beforeActiveDDL, IsTrue)

	info := g1.UnresolvedGroupInfo()
	shouldBe := &pb.ShardingGroup{Target: "", DDLs: ddls1, FirstLocation: pos11.String(), Synced: []string{source1}, Unsynced: []string{source2}}
	c.Assert(info, DeepEquals, shouldBe)

	// simple sort for [][]string{[]string{"db1", "tbl2"}, []string{"db1", "tbl1"}}
	tbls1 := g1.Tables()
	tbls2 := g1.UnresolvedTables()
	if tbls1[0].Name != tbls2[0].Name {
		tbls1[0], tbls1[1] = tbls1[1], tbls1[0]
	}
	c.Assert(tbls1, DeepEquals, tbls2)

	// sync first DDL for source2, synced but not resolved
	synced, active, remain, err = g1.TrySync(source2, pos21, endPos21, ddls1)
	c.Assert(err, IsNil)
	c.Assert(synced, IsTrue)
	c.Assert(active, IsTrue)
	c.Assert(remain, Equals, 0)

	// active DDL is at pos21
	beforeActiveDDL = g1.CheckSyncing(source2, pos21)
	c.Assert(beforeActiveDDL, IsTrue)

	info = g1.UnresolvedGroupInfo()
	sort.Strings(info.Synced)
	shouldBe = &pb.ShardingGroup{Target: "", DDLs: ddls1, FirstLocation: pos11.String(), Synced: []string{source1, source2}, Unsynced: []string{}}
	c.Assert(info, DeepEquals, shouldBe)

	resolved := g1.ResolveShardingDDL()
	c.Assert(resolved, IsFalse)

	// next active DDL not present
	beforeActiveDDL = g1.CheckSyncing(source2, pos21)
	c.Assert(beforeActiveDDL, IsTrue)

	synced, active, remain, err = g1.TrySync(source2, pos22, endPos22, ddls2)
	c.Assert(err, IsNil)
	c.Assert(synced, IsTrue)
	c.Assert(active, IsTrue)
	c.Assert(remain, Equals, 0)
	resolved = g1.ResolveShardingDDL()
	c.Assert(resolved, IsTrue)

	// caller should reset sharding group if DDL is successful executed
	g1.Reset()

	info = g1.UnresolvedGroupInfo()
	c.Assert(info, IsNil)
	c.Assert(g1.UnresolvedTables(), IsNil)
}

func (t *testShardingGroupSuite) TestTableID(c *C) {
	originTables := []*filter.Table{
		{Schema: "db", Name: "table"},
		{Schema: `d"b`, Name: `t"able"`},
		{Schema: "d`b", Name: "t`able"},
	}
	for _, originTable := range originTables {
		// ignore isSchemaOnly
		tableID := utils.GenTableID(originTable)
		table := utils.UnpackTableID(tableID)
		c.Assert(table, DeepEquals, originTable)
	}
}

func (t *testShardingGroupSuite) TestKeeper(c *C) {
	k := NewShardingGroupKeeper(tcontext.Background(), t.cfg, nil)
	k.clear()
	db, mock, err := sqlmock.New()
	c.Assert(err, IsNil)
	dbConn, err := db.Conn(context.Background())
	c.Assert(err, IsNil)
	k.db = conn.NewBaseDBForTest(db)
	k.dbConn = dbconn.NewDBConn(t.cfg, conn.NewBaseConnForTest(dbConn, &retry.FiniteRetryStrategy{}))
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS `%s`", t.cfg.MetaSchema)).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.*", dbutil.TableName(t.cfg.MetaSchema, cputil.SyncerShardMeta(t.cfg.Name)))).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	c.Assert(k.prepare(), IsNil)

	// test meta

	mock.ExpectQuery(" SELECT `target_table_id`, `source_table_id`, `active_index`, `is_global`, `data` FROM `test`.`checkpoint_ut_syncer_sharding_meta`.*").
		WillReturnRows(sqlmock.NewRows([]string{"target_table_id", "source_table_id", "active_index", "is_global", "data"}))
	meta, err := k.LoadShardMeta(mysql.MySQLFlavor, false)
	c.Assert(err, IsNil)
	c.Assert(meta, HasLen, 0)
	mock.ExpectQuery(" SELECT `target_table_id`, `source_table_id`, `active_index`, `is_global`, `data` FROM `test`.`checkpoint_ut_syncer_sharding_meta`.*").
		WillReturnRows(sqlmock.NewRows([]string{"target_table_id", "source_table_id", "active_index", "is_global", "data"}).
			AddRow(target, "", 0, true, "[{\"ddls\":[\"DUMMY DDL\"],\"source\":\"`db1`.`tbl1`\",\"first-position\":{\"Name\":\"mysql-bin.000002\",\"Pos\":123},\"first-gtid-set\":\"\"},{\"ddls\":[\"ANOTHER DUMMY DDL\"],\"source\":\"`db1`.`tbl1`\",\"first-position\":{\"Name\":\"mysql-bin.000002\",\"Pos\":789},\"first-gtid-set\":\"\"}]").
			AddRow(target, source1, 0, false, "[{\"ddls\":[\"DUMMY DDL\"],\"source\":\"`db1`.`tbl1`\",\"first-position\":{\"Name\":\"mysql-bin.000002\",\"Pos\":123},\"first-gtid-set\":\"\"},{\"ddls\":[\"ANOTHER DUMMY DDL\"],\"source\":\"`db1`.`tbl1`\",\"first-position\":{\"Name\":\"mysql-bin.000002\",\"Pos\":789},\"first-gtid-set\":\"\"}]"))

	meta, err = k.LoadShardMeta(mysql.MySQLFlavor, false)
	c.Assert(err, IsNil)
	c.Assert(meta, HasLen, 1) // has meta of `target`

	// test AddGroup and LeaveGroup

	needShardingHandle, group, synced, remain, err := k.AddGroup(targetTbl, []string{source1}, nil, true)
	c.Assert(err, IsNil)
	c.Assert(needShardingHandle, IsFalse)
	c.Assert(group, NotNil)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 0) // first time doesn't return `remain`

	needShardingHandle, group, synced, remain, err = k.AddGroup(targetTbl, []string{source2}, nil, true)
	c.Assert(err, IsNil)
	c.Assert(needShardingHandle, IsFalse)
	c.Assert(group, NotNil)
	c.Assert(synced, IsFalse)
	c.Assert(remain, Equals, 2)

	// test LeaveGroup
	// nolint:dogsled
	_, _, _, remain, err = k.AddGroup(targetTbl, []string{source3}, nil, true)
	c.Assert(err, IsNil)
	c.Assert(remain, Equals, 3)
	// nolint:dogsled
	_, _, _, remain, err = k.AddGroup(targetTbl, []string{source4}, nil, true)
	c.Assert(err, IsNil)
	c.Assert(remain, Equals, 4)
	c.Assert(k.LeaveGroup(targetTbl, []string{source3, source4}), IsNil)

	// test TrySync and InSyncing

	needShardingHandle, group, synced, active, remain, err := k.TrySync(sourceTbl1, targetTbl, pos12, endPos12, ddls1)
	c.Assert(err, IsNil)
	c.Assert(needShardingHandle, IsTrue)
	c.Assert(group.sources, DeepEquals, map[string]bool{source1: true, source2: false})
	c.Assert(synced, IsFalse)
	c.Assert(active, IsTrue)
	c.Assert(remain, Equals, 1)

	c.Assert(k.InSyncing(sourceTbl1, &filter.Table{Schema: targetTbl.Schema, Name: "wrong table"}, pos11), IsFalse)
	loc, err := k.ActiveDDLFirstLocation(targetTbl)
	c.Assert(err, IsNil)
	// position before active DDL, not in syncing
	c.Assert(binlog.CompareLocation(endPos11, loc, false), Equals, -1)
	c.Assert(k.InSyncing(sourceTbl1, targetTbl, endPos11), IsFalse)
	// position at/after active DDL, in syncing
	c.Assert(binlog.CompareLocation(pos12, loc, false), Equals, 0)
	c.Assert(k.InSyncing(sourceTbl1, targetTbl, pos12), IsFalse)
	c.Assert(binlog.CompareLocation(endPos12, loc, false), Equals, 1)
	c.Assert(k.InSyncing(sourceTbl1, targetTbl, endPos12), IsTrue)

	needShardingHandle, group, synced, active, remain, err = k.TrySync(sourceTbl2, targetTbl, pos21, endPos21, ddls1)
	c.Assert(err, IsNil)
	c.Assert(needShardingHandle, IsTrue)
	c.Assert(group.sources, DeepEquals, map[string]bool{source1: true, source2: true})
	c.Assert(synced, IsTrue)
	c.Assert(active, IsTrue)
	c.Assert(remain, Equals, 0)

	unresolvedTarget, unresolvedTables := k.UnresolvedTables()
	c.Assert(unresolvedTarget, DeepEquals, map[string]bool{target: true})
	// simple re-order
	if unresolvedTables[0].Name > unresolvedTables[1].Name {
		unresolvedTables[0], unresolvedTables[1] = unresolvedTables[1], unresolvedTables[0]
	}
	c.Assert(unresolvedTables, DeepEquals, []*filter.Table{sourceTbl1, sourceTbl2})

	unresolvedGroups := k.UnresolvedGroups()
	c.Assert(unresolvedGroups, HasLen, 1)
	g := unresolvedGroups[0]
	c.Assert(g.Unsynced, HasLen, 0)
	c.Assert(g.DDLs, DeepEquals, ddls1)
	c.Assert(g.FirstLocation, DeepEquals, pos12.String())

	sqls, args := k.PrepareFlushSQLs(unresolvedTarget)
	c.Assert(sqls, HasLen, 0)
	c.Assert(args, HasLen, 0)

	reset, err := k.ResolveShardingDDL(targetTbl)
	c.Assert(err, IsNil)
	c.Assert(reset, IsTrue)

	k.ResetGroups()

	unresolvedTarget, unresolvedTables = k.UnresolvedTables()
	c.Assert(unresolvedTarget, HasLen, 0)
	c.Assert(unresolvedTables, HasLen, 0)
}
