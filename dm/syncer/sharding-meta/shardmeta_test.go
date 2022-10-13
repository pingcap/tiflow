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

package shardmeta

import (
	"fmt"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
)

var _ = check.Suite(&testShardMetaSuite{})

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

type testShardMetaSuite struct{}

func (t *testShardMetaSuite) TestShardingMeta(c *check.C) {
	var (
		active     bool
		err        error
		sqls       []string
		args       [][]interface{}
		location   binlog.Location
		filename   = "mysql-bin.000001"
		table1     = "table1"
		table2     = "table2"
		table3     = "table3"
		metaSchema = "dm_meta"
		metaTable  = "test_syncer_sharding_meta"
		sourceID   = "mysql-replica-01"
		tableID    = "`target_db`.`target_table`"
		meta       = NewShardingMeta(metaSchema, metaTable, false)
		items      = []*DDLItem{
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 1000}}, []string{"ddl1"}, table1),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 1200}}, []string{"ddl2-1,ddl2-2"}, table1),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 1400}}, []string{"ddl3"}, table1),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 1600}}, []string{"ddl1"}, table2),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 1800}}, []string{"ddl2-1,ddl2-2"}, table2),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 2000}}, []string{"ddl3"}, table2),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 2200}}, []string{"ddl1"}, table3),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 2400}}, []string{"ddl2-1,ddl2-2"}, table3),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 2600}}, []string{"ddl3"}, table3),
		}
	)

	// 1st round sharding DDL sync
	for i := 0; i < 7; i++ {
		active, err = meta.AddItem(items[i])
		c.Assert(err, check.IsNil)
		if i%3 == 0 {
			c.Assert(active, check.IsTrue)
		} else {
			c.Assert(active, check.IsFalse)
		}
	}

	c.Assert(meta.GetGlobalItems(), check.DeepEquals, []*DDLItem{items[0], items[1], items[2]})
	c.Assert(meta.GetGlobalActiveDDL(), check.DeepEquals, items[0])
	c.Assert(meta.GetActiveDDLItem(table1), check.DeepEquals, items[0])
	c.Assert(meta.GetActiveDDLItem(table2), check.DeepEquals, items[3])
	c.Assert(meta.GetActiveDDLItem(table3), check.DeepEquals, items[6])
	c.Assert(meta.InSequenceSharding(), check.IsTrue)
	location, err = meta.ActiveDDLFirstLocation()
	c.Assert(err, check.IsNil)
	c.Assert(location.Position, check.DeepEquals, items[0].FirstLocation.Position)

	// find synced in shrading group, and call ShardingMeta.ResolveShardingDDL
	c.Assert(meta.ResolveShardingDDL(), check.IsFalse)

	c.Assert(meta.GetGlobalActiveDDL(), check.DeepEquals, items[1])
	c.Assert(meta.GetActiveDDLItem(table1), check.DeepEquals, items[1])
	c.Assert(meta.GetActiveDDLItem(table2), check.DeepEquals, items[4])
	c.Assert(meta.GetActiveDDLItem(table3), check.IsNil)
	c.Assert(meta.InSequenceSharding(), check.IsTrue)
	location, err = meta.ActiveDDLFirstLocation()
	c.Assert(err, check.IsNil)
	c.Assert(location.Position, check.DeepEquals, items[1].FirstLocation.Position)

	sqls, args = meta.FlushData(sourceID, tableID)
	c.Assert(sqls, check.HasLen, 4)
	c.Assert(args, check.HasLen, 4)
	for _, stmt := range sqls {
		c.Assert(stmt, check.Matches, "INSERT INTO .*")
	}
	for _, arg := range args {
		c.Assert(arg, check.HasLen, 8)
		c.Assert(arg[3], check.Equals, 1)
	}

	// 2nd round sharding DDL sync
	for i := 0; i < 8; i++ {
		if i%3 == 0 {
			continue
		}
		active, err = meta.AddItem(items[i])
		c.Assert(err, check.IsNil)
		if i%3 == 1 {
			c.Assert(active, check.IsTrue)
		} else {
			c.Assert(active, check.IsFalse)
		}
	}

	c.Assert(meta.GetGlobalActiveDDL(), check.DeepEquals, items[1])
	c.Assert(meta.GetActiveDDLItem(table1), check.DeepEquals, items[1])
	c.Assert(meta.GetActiveDDLItem(table2), check.DeepEquals, items[4])
	c.Assert(meta.GetActiveDDLItem(table3), check.DeepEquals, items[7])
	c.Assert(meta.InSequenceSharding(), check.IsTrue)
	location, err = meta.ActiveDDLFirstLocation()
	c.Assert(err, check.IsNil)
	c.Assert(location.Position, check.DeepEquals, items[1].FirstLocation.Position)

	// find synced in shrading group, and call ShardingMeta.ResolveShardingDDL
	c.Assert(meta.ResolveShardingDDL(), check.IsFalse)

	c.Assert(meta.GetGlobalActiveDDL(), check.DeepEquals, items[2])
	c.Assert(meta.GetActiveDDLItem(table1), check.DeepEquals, items[2])
	c.Assert(meta.GetActiveDDLItem(table2), check.DeepEquals, items[5])
	c.Assert(meta.GetActiveDDLItem(table3), check.IsNil)
	c.Assert(meta.InSequenceSharding(), check.IsTrue)
	location, err = meta.ActiveDDLFirstLocation()
	c.Assert(err, check.IsNil)
	c.Assert(location.Position, check.DeepEquals, items[2].FirstLocation.Position)

	sqls, args = meta.FlushData(sourceID, tableID)
	c.Assert(sqls, check.HasLen, 4)
	c.Assert(args, check.HasLen, 4)
	for _, stmt := range sqls {
		c.Assert(stmt, check.Matches, "INSERT INTO .*")
	}
	for _, arg := range args {
		c.Assert(arg, check.HasLen, 8)
		c.Assert(arg[3], check.Equals, 2)
	}

	// 3rd round sharding DDL sync
	for i := 0; i < 9; i++ {
		if i%3 != 2 {
			continue
		}
		active, err = meta.AddItem(items[i])
		c.Assert(err, check.IsNil)
		if i%3 == 2 {
			c.Assert(active, check.IsTrue)
		} else {
			c.Assert(active, check.IsFalse)
		}
	}
	c.Assert(meta.GetGlobalActiveDDL(), check.DeepEquals, items[2])
	c.Assert(meta.GetActiveDDLItem(table1), check.DeepEquals, items[2])
	c.Assert(meta.GetActiveDDLItem(table2), check.DeepEquals, items[5])
	c.Assert(meta.GetActiveDDLItem(table3), check.DeepEquals, items[8])
	c.Assert(meta.InSequenceSharding(), check.IsTrue)
	location, err = meta.ActiveDDLFirstLocation()
	c.Assert(err, check.IsNil)
	c.Assert(location.Position, check.DeepEquals, items[2].FirstLocation.Position)

	// find synced in shrading group, and call ShardingMeta.ResolveShardingDDL
	c.Assert(meta.ResolveShardingDDL(), check.IsTrue)

	c.Assert(meta.GetGlobalActiveDDL(), check.IsNil)
	c.Assert(meta.GetActiveDDLItem(table1), check.IsNil)
	c.Assert(meta.GetActiveDDLItem(table2), check.IsNil)
	c.Assert(meta.GetActiveDDLItem(table3), check.IsNil)
	c.Assert(meta.InSequenceSharding(), check.IsFalse)
	_, err = meta.ActiveDDLFirstLocation()
	c.Assert(err, check.ErrorMatches, fmt.Sprintf("\\[.*\\], Message: activeIdx %d larger than length of global DDLItems: .*", meta.ActiveIdx()))

	sqls, args = meta.FlushData(sourceID, tableID)
	c.Assert(sqls, check.HasLen, 1)
	c.Assert(args, check.HasLen, 1)
	c.Assert(sqls[0], check.Matches, "DELETE FROM .*")
	c.Assert(args[0], check.DeepEquals, []interface{}{sourceID, tableID})
}

func (t *testShardMetaSuite) TestShardingMetaWrongSequence(c *check.C) {
	var (
		active   bool
		err      error
		filename = "mysql-bin.000001"
		table1   = "table1"
		table2   = "table2"
		meta     = NewShardingMeta("", "", false)
		items    = []*DDLItem{
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 1000}}, []string{"ddl1"}, table1),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 1200}}, []string{"ddl2"}, table1),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 1400}}, []string{"ddl3"}, table1),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 1600}}, []string{"ddl1"}, table2),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 1800}}, []string{"ddl3"}, table2),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 2000}}, []string{"ddl2"}, table2),
		}
	)

	// 1st round sharding DDL sync
	for i := 0; i < 4; i++ {
		active, err = meta.AddItem(items[i])
		c.Assert(err, check.IsNil)
		if i%3 == 0 {
			c.Assert(active, check.IsTrue)
		} else {
			c.Assert(active, check.IsFalse)
		}
	}
	// find synced in shrading group, and call ShardingMeta.ResolveShardingDDL
	c.Assert(meta.ResolveShardingDDL(), check.IsFalse)

	// 2nd round sharding DDL sync
	for i := 0; i < 4; i++ {
		if i%3 == 0 {
			continue
		}
		active, err = meta.AddItem(items[i])
		c.Assert(err, check.IsNil)
		if i%3 == 1 {
			c.Assert(active, check.IsTrue)
		} else {
			c.Assert(active, check.IsFalse)
		}
	}
	active, err = meta.AddItem(items[4])
	c.Assert(active, check.IsFalse)
	c.Assert(err, check.ErrorMatches, "\\[.*\\], Message: detect inconsistent DDL sequence from source .*, right DDL sequence should be .*")
}

func (t *testShardMetaSuite) TestFlushLoadMeta(c *check.C) {
	var (
		active     bool
		err        error
		filename   = "mysql-bin.000001"
		table1     = "table1"
		table2     = "table2"
		metaSchema = "dm_meta"
		metaTable  = "test_syncer_sharding_meta"
		sourceID   = "mysql-replica-01"
		tableID    = "`target_db`.`target_table`"
		meta       = NewShardingMeta(metaSchema, metaTable, false)
		loadedMeta = NewShardingMeta(metaSchema, metaTable, false)
		items      = []*DDLItem{
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 1000}}, []string{"ddl1"}, table1),
			NewDDLItem(binlog.Location{Position: mysql.Position{Name: filename, Pos: 1200}}, []string{"ddl1"}, table2),
		}
	)
	for _, item := range items {
		active, err = meta.AddItem(item)
		c.Assert(err, check.IsNil)
		c.Assert(active, check.IsTrue)
	}
	sqls, args := meta.FlushData(sourceID, tableID)
	c.Assert(sqls, check.HasLen, 3)
	c.Assert(args, check.HasLen, 3)
	for _, arg := range args {
		c.Assert(arg, check.HasLen, 8)
		c.Assert(loadedMeta.RestoreFromData(arg[2].(string), arg[3].(int), arg[4].(bool), []byte(arg[5].(string)), mysql.MySQLFlavor), check.IsNil)
	}
	c.Assert(loadedMeta.activeIdx, check.Equals, meta.activeIdx)
	c.Assert(loadedMeta.global.String(), check.Equals, meta.global.String())
	c.Assert(loadedMeta.tableName, check.Equals, meta.tableName)
	c.Assert(len(loadedMeta.sources), check.Equals, len(meta.sources))
	for table, source := range loadedMeta.sources {
		c.Assert(source.String(), check.Equals, meta.sources[table].String())
	}
}
