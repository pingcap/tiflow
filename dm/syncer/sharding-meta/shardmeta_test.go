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
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/stretchr/testify/require"
)

func TestShardingMeta(t *testing.T) {
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
		require.NoError(t, err)
		if i%3 == 0 {
			require.True(t, active)
		} else {
			require.False(t, active)
		}
	}

	require.Equal(t, []*DDLItem{items[0], items[1], items[2]}, meta.GetGlobalItems())
	require.Equal(t, items[0], meta.GetGlobalActiveDDL())
	require.Equal(t, items[0], meta.GetActiveDDLItem(table1))
	require.Equal(t, items[3], meta.GetActiveDDLItem(table2))
	require.Equal(t, items[6], meta.GetActiveDDLItem(table3))
	require.True(t, meta.InSequenceSharding())
	location, err = meta.ActiveDDLFirstLocation()
	require.NoError(t, err)
	require.Equal(t, items[0].FirstLocation.Position, location.Position)

	// find synced in shrading group, and call ShardingMeta.ResolveShardingDDL
	require.False(t, meta.ResolveShardingDDL())

	require.Equal(t, items[1], meta.GetGlobalActiveDDL())
	require.Equal(t, items[1], meta.GetActiveDDLItem(table1))
	require.Equal(t, items[4], meta.GetActiveDDLItem(table2))
	require.Nil(t, meta.GetActiveDDLItem(table3))
	require.True(t, meta.InSequenceSharding())
	location, err = meta.ActiveDDLFirstLocation()
	require.NoError(t, err)
	require.Equal(t, items[1].FirstLocation.Position, location.Position)

	sqls, args = meta.FlushData(sourceID, tableID)
	require.Len(t, sqls, 4)
	require.Len(t, args, 4)
	for _, stmt := range sqls {
		require.Regexp(t, "INSERT INTO .*", stmt)
	}
	for _, arg := range args {
		require.Len(t, arg, 8)
		require.Equal(t, 1, arg[3])
	}

	// 2nd round sharding DDL sync
	for i := 0; i < 8; i++ {
		if i%3 == 0 {
			continue
		}
		active, err = meta.AddItem(items[i])
		require.NoError(t, err)
		if i%3 == 1 {
			require.True(t, active)
		} else {
			require.False(t, active)
		}
	}

	require.Equal(t, items[1], meta.GetGlobalActiveDDL())
	require.Equal(t, items[1], meta.GetActiveDDLItem(table1))
	require.Equal(t, items[4], meta.GetActiveDDLItem(table2))
	require.Equal(t, items[7], meta.GetActiveDDLItem(table3))
	require.True(t, meta.InSequenceSharding())
	location, err = meta.ActiveDDLFirstLocation()
	require.NoError(t, err)
	require.Equal(t, items[1].FirstLocation.Position, location.Position)

	// find synced in shrading group, and call ShardingMeta.ResolveShardingDDL
	require.False(t, meta.ResolveShardingDDL())

	require.Equal(t, items[2], meta.GetGlobalActiveDDL())
	require.Equal(t, items[2], meta.GetActiveDDLItem(table1))
	require.Equal(t, items[5], meta.GetActiveDDLItem(table2))
	require.Nil(t, meta.GetActiveDDLItem(table3))
	require.True(t, meta.InSequenceSharding())
	location, err = meta.ActiveDDLFirstLocation()
	require.NoError(t, err)
	require.Equal(t, items[2].FirstLocation.Position, location.Position)

	sqls, args = meta.FlushData(sourceID, tableID)
	require.Len(t, sqls, 4)
	require.Len(t, args, 4)
	for _, stmt := range sqls {
		require.Regexp(t, "INSERT INTO .*", stmt)
	}
	for _, arg := range args {
		require.Len(t, arg, 8)
		require.Equal(t, 2, arg[3])
	}

	// 3rd round sharding DDL sync
	for i := 0; i < 9; i++ {
		if i%3 != 2 {
			continue
		}
		active, err = meta.AddItem(items[i])
		require.NoError(t, err)
		if i%3 == 2 {
			require.True(t, active)
		} else {
			require.False(t, active)
		}
	}
	require.Equal(t, items[2], meta.GetGlobalActiveDDL())
	require.Equal(t, items[2], meta.GetActiveDDLItem(table1))
	require.Equal(t, items[5], meta.GetActiveDDLItem(table2))
	require.Equal(t, items[8], meta.GetActiveDDLItem(table3))
	require.True(t, meta.InSequenceSharding())
	location, err = meta.ActiveDDLFirstLocation()
	require.NoError(t, err)
	require.Equal(t, items[2].FirstLocation.Position, location.Position)

	// find synced in shrading group, and call ShardingMeta.ResolveShardingDDL
	require.True(t, meta.ResolveShardingDDL())

	require.Nil(t, meta.GetGlobalActiveDDL())
	require.Nil(t, meta.GetActiveDDLItem(table1))
	require.Nil(t, meta.GetActiveDDLItem(table2))
	require.Nil(t, meta.GetActiveDDLItem(table3))
	require.False(t, meta.InSequenceSharding())
	_, err = meta.ActiveDDLFirstLocation()
	require.Error(t, err)
	require.Regexp(t, fmt.Sprintf("\\[.*\\], Message: activeIdx %d larger than length of global DDLItems: .*", meta.ActiveIdx()), err.Error())

	sqls, args = meta.FlushData(sourceID, tableID)
	require.Len(t, sqls, 1)
	require.Len(t, args, 1)
	require.Regexp(t, "DELETE FROM .*", sqls[0])
	require.Equal(t, []interface{}{sourceID, tableID}, args[0])
}

func TestShardingMetaWrongSequence(t *testing.T) {
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
		require.NoError(t, err)
		if i%3 == 0 {
			require.True(t, active)
		} else {
			require.False(t, active)
		}
	}
	// find synced in shrading group, and call ShardingMeta.ResolveShardingDDL
	require.False(t, meta.ResolveShardingDDL())

	// 2nd round sharding DDL sync
	for i := 0; i < 4; i++ {
		if i%3 == 0 {
			continue
		}
		active, err = meta.AddItem(items[i])
		require.NoError(t, err)
		if i%3 == 1 {
			require.True(t, active)
		} else {
			require.False(t, active)
		}
	}
	active, err = meta.AddItem(items[4])
	require.False(t, active)
	require.Error(t, err)
	require.Regexp(t, "\\[.*\\], Message: detect inconsistent DDL sequence from source .*, right DDL sequence should be .*", err.Error())
}

func TestFlushLoadMeta(t *testing.T) {
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
		require.NoError(t, err)
		require.True(t, active)
	}
	sqls, args := meta.FlushData(sourceID, tableID)
	require.Len(t, sqls, 3)
	require.Len(t, args, 3)
	for _, arg := range args {
		require.Len(t, arg, 8)
		require.NoError(t, loadedMeta.RestoreFromData(arg[2].(string), arg[3].(int), arg[4].(bool), []byte(arg[5].(string)), mysql.MySQLFlavor))
	}
	require.Equal(t, meta.activeIdx, loadedMeta.activeIdx)
	require.Equal(t, meta.global.String(), loadedMeta.global.String())
	require.Equal(t, meta.tableName, loadedMeta.tableName)
	require.Equal(t, len(meta.sources), len(loadedMeta.sources))
	for table, source := range loadedMeta.sources {
		require.Equal(t, meta.sources[table].String(), source.String())
	}
}
