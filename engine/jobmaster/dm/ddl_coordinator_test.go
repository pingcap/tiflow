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

package dm

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/log"
	dmconfig "github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	"github.com/pingcap/tiflow/engine/pkg/meta/mock"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestDDLCoordinator(t *testing.T) {
	var (
		checkpointAgent = &MockCheckpointAgent{}
		metaClient      = mock.NewMetaMock()
		jobStore        = metadata.NewJobStore(metaClient, log.L())
		ddlCoordinator  = NewDDLCoordinator("", metaClient, checkpointAgent, jobStore, log.L())

		tb1         = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		tb2         = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb2"}
		tb3         = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb3"}
		targetTable = metadata.TargetTable{Schema: "schema", Table: "tb"}
		tables      = map[metadata.TargetTable][]metadata.SourceTable{
			targetTable: {tb1},
		}
	)

	require.EqualError(t, ddlCoordinator.Reset(context.Background()), "state not found")
	ddls, conflictStage, err := ddlCoordinator.Coordinate(context.Background(), nil)
	require.EqualError(t, err, "state not found")
	require.Len(t, ddls, 0)
	require.Equal(t, optimism.ConflictError, conflictStage)

	jobCfg := &config.JobCfg{Upstreams: []*config.UpstreamCfg{{MySQLInstance: dmconfig.MySQLInstance{SourceID: "source"}}}}
	require.NoError(t, jobStore.Put(context.Background(), metadata.NewJob(jobCfg)))
	require.NoError(t, ddlCoordinator.Reset(context.Background()))
	ddls, conflictStage, err = ddlCoordinator.Coordinate(context.Background(), nil)
	require.EqualError(t, err, "coordinate error with non-shard-mode")
	require.Len(t, ddls, 0)
	require.Equal(t, optimism.ConflictError, conflictStage)

	jobCfg.ShardMode = dmconfig.ShardOptimistic
	require.NoError(t, jobStore.UpdateConfig(context.Background(), jobCfg))
	checkpointAgent.On("FetchAllDoTables").Return(nil, context.DeadlineExceeded).Once()
	require.Error(t, ddlCoordinator.Reset(context.Background()))

	checkpointAgent.On("FetchAllDoTables").Return(tables, nil).Once()
	require.NoError(t, ddlCoordinator.Reset(context.Background()))
	require.Contains(t, ddlCoordinator.tables, targetTable)
	require.Len(t, ddlCoordinator.tables[targetTable], 1)

	item := &metadata.DDLItem{
		SourceTable: tb2,
		DDLs:        []string{genCreateStmt("col1 int", "col2 int")},
		Tables:      []string{genCreateStmt("col1 int", "col2 int")},
		TargetTable: targetTable,
		Type:        metadata.CreateTable,
	}
	checkpointAgent.On("FetchTableStmt").Return("", context.DeadlineExceeded).Once()
	ddls, conflictStage, err = ddlCoordinator.Coordinate(context.Background(), item)
	require.EqualError(t, err, "fetch table stmt from checkpoint failed, sourceTable: {source schema tb1}, err: context deadline exceeded")
	require.Len(t, ddls, 0)
	require.Equal(t, optimism.ConflictError, conflictStage)
	require.Len(t, ddlCoordinator.tables[targetTable], 1)

	checkpointAgent.On("FetchTableStmt").Return(genCreateStmt("col1 int", "col2 int"), nil).Once()
	ddls, conflictStage, err = ddlCoordinator.Coordinate(context.Background(), item)
	require.NoError(t, err)
	require.Equal(t, item.DDLs, ddls)
	require.Equal(t, optimism.ConflictNone, conflictStage)
	require.Len(t, ddlCoordinator.tables, 1)

	item = &metadata.DDLItem{
		SourceTable: tb1,
		DDLs:        []string{"alter table tb1 modify column col2 varhar(255)"},
		Tables:      []string{genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 int", "col2 varchar(255)")},
		TargetTable: targetTable,
		Type:        metadata.OtherDDL,
	}
	checkpointAgent.On("FetchTableStmt").Return(genCreateStmt("col1 int", "col2 int"), nil).Twice()
	ddls, conflictStage, err = ddlCoordinator.Coordinate(context.Background(), item)
	require.NoError(t, err)
	require.Len(t, ddls, 0)
	require.Equal(t, optimism.ConflictSkipWaitRedirect, conflictStage)

	item = &metadata.DDLItem{
		SourceTable: tb2,
		DDLs:        []string{"alter table tb1 modify column col1 varhar(255)"},
		Tables:      []string{genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 varchar(255)", "col2 int")},
		TargetTable: targetTable,
		Type:        metadata.OtherDDL,
	}
	ddls, conflictStage, err = ddlCoordinator.Coordinate(context.Background(), item)
	require.EqualError(t, err, fmt.Sprintf("conflict detected for table %v", item.SourceTable))
	require.Len(t, ddls, 0)
	require.Equal(t, optimism.ConflictDetected, conflictStage)

	item = &metadata.DDLItem{
		SourceTable: tb1,
		DDLs:        []string{"drop table tb1"},
		TargetTable: targetTable,
		Type:        metadata.DropTable,
	}
	ddls, conflictStage, err = ddlCoordinator.Coordinate(context.Background(), item)
	require.NoError(t, err)
	require.Equal(t, item.DDLs, ddls)
	require.Equal(t, optimism.ConflictNone, conflictStage)
	require.Contains(t, ddlCoordinator.tables, targetTable)
	require.Len(t, ddlCoordinator.tables[targetTable], 1)

	item = &metadata.DDLItem{
		SourceTable: tb2,
		DDLs:        []string{"alter table tb1 modify column col1 varhar(255)"},
		Tables:      []string{genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 varchar(255)", "col2 int")},
		TargetTable: targetTable,
		Type:        metadata.OtherDDL,
	}
	ddls, conflictStage, err = ddlCoordinator.Coordinate(context.Background(), item)
	require.NoError(t, err)
	require.Equal(t, item.DDLs, ddls)
	require.Equal(t, optimism.ConflictNone, conflictStage)

	item = &metadata.DDLItem{
		SourceTable: tb2,
		DDLs:        []string{"drop table tb2"},
		TargetTable: targetTable,
		Type:        metadata.DropTable,
	}
	checkpointAgent.On("FetchTableStmt").Return(genCreateStmt("col1 int", "col2 int"), nil).Once()
	ddls, conflictStage, err = ddlCoordinator.Coordinate(context.Background(), item)
	require.NoError(t, err)
	require.Equal(t, item.DDLs, ddls)
	require.Equal(t, optimism.ConflictNone, conflictStage)
	require.Len(t, ddlCoordinator.tables, 0)

	item = &metadata.DDLItem{
		SourceTable: tb3,
		DDLs:        []string{genCreateStmt("col1 varchar(255)", "col2 int")},
		Tables:      []string{genCreateStmt("col1 varchar(255)", "col2 int")},
		TargetTable: targetTable,
		Type:        metadata.CreateTable,
	}
	ddls, conflictStage, err = ddlCoordinator.Coordinate(context.Background(), item)
	require.NoError(t, err)
	require.Equal(t, item.DDLs, ddls)
	require.Equal(t, optimism.ConflictNone, conflictStage)
	require.Contains(t, ddlCoordinator.tables, targetTable)
	require.Len(t, ddlCoordinator.tables[targetTable], 1)

	require.NoError(t, ddlCoordinator.ClearMetadata(context.Background()))

	checkpointAgent.AssertExpectations(t)
}

func TestShowDDLLocks(t *testing.T) {
	ddlCoordinator := &DDLCoordinator{
		shardGroups: map[metadata.TargetTable]*shardGroup{
			{Schema: "db", Table: "tb"}: {
				normalTables: map[metadata.SourceTable]string{
					{Source: "source1", Schema: "db", Table: "tb"}: genCreateStmt("col1 int", "col2 int"),
					{Source: "source2", Schema: "db", Table: "tb"}: genCreateStmt("col1 int", "col2 int"),
				},
				conflictTables: map[metadata.SourceTable]string{
					{Source: "source2", Schema: "db", Table: "tb"}: genCreateStmt("col1 int", "col2 varchar(255)"),
				},
			},
			{Schema: "database", Table: "table"}: {
				normalTables: map[metadata.SourceTable]string{
					{Source: "source1", Schema: "database1", Table: "table1"}: genCreateStmt("col1 int", "col2 int", "col3 int"),
					{Source: "source1", Schema: "database2", Table: "table2"}: genCreateStmt("col1 int", "col2 int"),
				},
				conflictTables: map[metadata.SourceTable]string{
					{Source: "source1", Schema: "database1", Table: "table1"}: genCreateStmt("col1 int", "col2 varchar(255)", "col3 int"),
				},
			},
		},
	}
	resp := ddlCoordinator.ShowDDLLocks(context.Background())
	bs, err := json.MarshalIndent(resp, "", "\t")
	require.NoError(t, err)
	expectedResp := `{
	"Locks": {
		"{\"Schema\":\"database\",\"Table\":\"table\"}": {
			"ShardTables": {
				"{\"Source\":\"source1\",\"Schema\":\"database1\",\"Table\":\"table1\"}": {
					"Current": "CREATE TABLE tbl(col1 int, col2 int, col3 int)",
					"Next": "CREATE TABLE tbl(col1 int, col2 varchar(255), col3 int)"
				},
				"{\"Source\":\"source1\",\"Schema\":\"database2\",\"Table\":\"table2\"}": {
					"Current": "CREATE TABLE tbl(col1 int, col2 int)",
					"Next": ""
				}
			}
		},
		"{\"Schema\":\"db\",\"Table\":\"tb\"}": {
			"ShardTables": {
				"{\"Source\":\"source1\",\"Schema\":\"db\",\"Table\":\"tb\"}": {
					"Current": "CREATE TABLE tbl(col1 int, col2 int)",
					"Next": ""
				},
				"{\"Source\":\"source2\",\"Schema\":\"db\",\"Table\":\"tb\"}": {
					"Current": "CREATE TABLE tbl(col1 int, col2 int)",
					"Next": "CREATE TABLE tbl(col1 int, col2 varchar(255))"
				}
			}
		}
	}
}`
	require.Equal(t, expectedResp, string(bs))
}

func TestHandle(t *testing.T) {
	var (
		tb1        = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		tb2        = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb2"}
		tb         = metadata.TargetTable{Schema: "schema", Table: "tb"}
		tableAgent = &MockCheckpointAgent{}
		g          = &shardGroup{
			normalTables: map[metadata.SourceTable]string{
				tb1: "",
			},
			conflictTables:      make(map[metadata.SourceTable]string),
			droppedColumnsStore: metadata.NewDroppedColumnsStore(mock.NewMetaMock(), tb),
			tableAgent:          tableAgent,
		}
	)

	item := &metadata.DDLItem{
		SourceTable: tb2,
		DDLs:        []string{genCreateStmt("col1 int", "col2 int")},
		Tables:      []string{genCreateStmt("col1 int", "col2 int")},
	}

	ddls, conflictStage, err := g.handle(context.Background(), item)
	require.EqualError(t, err, fmt.Sprintf("unknown ddl type %v", item.Type))
	require.Len(t, ddls, 0)
	require.Equal(t, optimism.ConflictError, conflictStage)
	require.True(t, g.isResolved(context.Background()))

	item.Type = metadata.CreateTable
	ddls, conflictStage, err = g.handle(context.Background(), item)
	require.NoError(t, err)
	require.Equal(t, item.DDLs, ddls)
	require.Equal(t, optimism.ConflictNone, conflictStage)
	require.True(t, g.isResolved(context.Background()))

	item = &metadata.DDLItem{
		SourceTable: tb1,
		DDLs:        []string{"alter table tb1 modify column col2 varhar(255)"},
		Tables:      []string{genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 int", "col2 varchar(255)")},
		Type:        metadata.OtherDDL,
	}
	ddls, conflictStage, err = g.handle(context.Background(), item)
	require.NoError(t, err)
	require.Len(t, ddls, 0)
	require.Equal(t, optimism.ConflictSkipWaitRedirect, conflictStage)
	require.False(t, g.isResolved(context.Background()))

	item = &metadata.DDLItem{
		SourceTable: tb1,
		DDLs:        []string{"drop table tb1"},
		Type:        metadata.DropTable,
	}
	ddls, conflictStage, err = g.handle(context.Background(), item)
	require.NoError(t, err)
	require.Equal(t, item.DDLs, ddls)
	require.Equal(t, optimism.ConflictNone, conflictStage)
	require.True(t, g.isResolved(context.Background()))

	item = &metadata.DDLItem{
		SourceTable: tb2,
		DDLs:        []string{"alter table tb1 drop column col2"},
		Tables:      []string{genCreateStmt("col1 int", "col2 int", "col3 int"), genCreateStmt("col1 int", "col3 int")},
		Type:        metadata.OtherDDL,
	}
	ddls, conflictStage, err = g.handle(context.Background(), item)
	require.NoError(t, err)
	require.Equal(t, item.DDLs, ddls)
	require.Equal(t, optimism.ConflictNone, conflictStage)
	require.False(t, g.isResolved(context.Background()))

	item = &metadata.DDLItem{
		SourceTable: tb2,
		DDLs:        []string{"drop table tb2"},
		Type:        metadata.DropTable,
	}
	tableAgent.On("FetchTableStmt").Return(genCreateStmt("col1 int", "col3 int"), nil).Once()
	ddls, conflictStage, err = g.handle(context.Background(), item)
	require.NoError(t, err)
	require.Equal(t, item.DDLs, ddls)
	require.Equal(t, optimism.ConflictNone, conflictStage)
	require.True(t, g.isResolved(context.Background()))
}

func TestHandleCreateTable(t *testing.T) {
	var (
		tb1 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		g   = &shardGroup{
			normalTables: make(map[metadata.SourceTable]string),
		}
	)

	item := &metadata.DDLItem{
		SourceTable: tb1,
		Tables:      []string{genCreateStmt("col1 int", "col2 int")},
	}
	// tb1 create table
	g.handleCreateTable(item)
	require.Len(t, g.normalTables, 1)
	require.Equal(t, item.Tables[0], g.normalTables[tb1])

	// tb1 idempotent
	g.handleCreateTable(item)
	require.Len(t, g.normalTables, 1)
	require.Equal(t, item.Tables[0], g.normalTables[tb1])

	// new create table will override old one
	item.Tables[0] = genCreateStmt("col1 int", "col2 int", "col3 int")
	g.handleCreateTable(item)
	require.Len(t, g.normalTables, 1)
	require.Equal(t, item.Tables[0], g.normalTables[tb1])
}

func TestHandleDropTable(t *testing.T) {
	var (
		tb1 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		tb2 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb2"}
		g   = &shardGroup{
			normalTables: map[metadata.SourceTable]string{
				tb1: genCreateStmt("col1 int", "col2 int"),
				tb2: "",
			},
			droppedColumnsStore: metadata.NewDroppedColumnsStore(mock.NewMetaMock(), metadata.TargetTable{Schema: "schema", Table: "tb"}),
		}
	)

	item := &metadata.DDLItem{
		SourceTable: metadata.SourceTable{},
		Tables:      []string{genCreateStmt("col1 int", "col2 int")},
	}
	g.handleDropTable(context.Background(), item)
	require.Len(t, g.normalTables, 2)

	item.SourceTable = tb1
	g.handleDropTable(context.Background(), item)
	require.Len(t, g.normalTables, 1)

	item.SourceTable = tb2
	g.handleDropTable(context.Background(), item)
	require.Len(t, g.normalTables, 0)

	item.SourceTable = tb1
	g.handleDropTable(context.Background(), item)
	require.Len(t, g.normalTables, 0)
}

func TestHandleDDLs(t *testing.T) {
	var (
		tb1 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		tb2 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb2"}
		tb3 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb3"}
		tb  = metadata.TargetTable{Schema: "schema", Table: "tb"}
		g   = &shardGroup{
			normalTables: map[metadata.SourceTable]string{
				tb1: "",
				tb3: genCreateStmt("col1 int", "col2 int"),
			},
			conflictTables:      make(map[metadata.SourceTable]string),
			droppedColumnsStore: metadata.NewDroppedColumnsStore(mock.NewMetaMock(), tb),
		}
	)

	item := &metadata.DDLItem{
		SourceTable: tb2,
		DDLs:        []string{"alter table tb add column col2 int"},
		Tables:      []string{genCreateStmt("col1 int"), genCreateStmt("col1 int", "col2 int")},
	}
	ddls, conflictStage, err := g.handleDDLs(context.Background(), item)
	require.NoError(t, err)
	require.Len(t, ddls, 1)
	require.Equal(t, item.DDLs, ddls)
	require.Equal(t, optimism.ConflictNone, conflictStage)

	item = &metadata.DDLItem{
		SourceTable: tb2,
		DDLs:        []string{"alter table tb modify column col1 varchar(255)"},
		Tables:      []string{genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 varchar(255)", "col2 int")},
	}
	ddls, conflictStage, err = g.handleDDLs(context.Background(), item)
	require.NoError(t, err)
	require.Len(t, ddls, 0)
	require.Equal(t, optimism.ConflictSkipWaitRedirect, conflictStage)

	item = &metadata.DDLItem{
		SourceTable: tb3,
		DDLs:        []string{"alter table tb modify column col2 varchar(255)"},
		Tables:      []string{genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 int", "col2 varchar(255)")},
	}
	ddls, conflictStage, err = g.handleDDLs(context.Background(), item)
	require.EqualError(t, err, fmt.Sprintf("conflict detected for table %v", item.SourceTable))
	require.Len(t, ddls, 0)
	require.Equal(t, optimism.ConflictDetected, conflictStage)
}

func TestHandleDDL(t *testing.T) {
	var (
		tb1 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		tb2 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb2"}
		tb3 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb3"}
		g   = &shardGroup{
			normalTables: map[metadata.SourceTable]string{
				tb1: "",
				tb2: genCreateStmt("col1 int"),
				tb3: genCreateStmt("col1 int", "col2 int"),
			},
			conflictTables: make(map[metadata.SourceTable]string),
		}
	)

	// tb2 add col2
	schemaChanged, conflictStage := g.handleDDL(tb2, genCreateStmt("col1 int"), genCreateStmt("col1 int", "col2 int"))
	require.True(t, schemaChanged)
	require.Equal(t, conflictStage, optimism.ConflictNone)
	// idempotent
	schemaChanged, conflictStage = g.handleDDL(tb2, genCreateStmt("col1 int"), genCreateStmt("col1 int", "col2 int"))
	require.True(t, schemaChanged)
	require.Equal(t, conflictStage, optimism.ConflictNone)

	// tb2 modify col1
	schemaChanged, conflictStage = g.handleDDL(tb2, genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 varchar(255), col2 int"))
	require.False(t, schemaChanged)
	require.Equal(t, conflictStage, optimism.ConflictSkipWaitRedirect)
	// idempotent
	schemaChanged, conflictStage = g.handleDDL(tb2, genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 varchar(255), col2 int"))
	require.False(t, schemaChanged)
	require.Equal(t, conflictStage, optimism.ConflictSkipWaitRedirect)

	// tb3 modify col1
	schemaChanged, conflictStage = g.handleDDL(tb3, genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 varchar(255), col2 int"))
	require.True(t, schemaChanged)
	require.Equal(t, conflictStage, optimism.ConflictNone)
	// tb2 idempotent
	schemaChanged, conflictStage = g.handleDDL(tb2, genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 varchar(255), col2 int"))
	require.True(t, schemaChanged)
	require.Equal(t, conflictStage, optimism.ConflictNone)

	// tb3 add column with wrong name
	schemaChanged, conflictStage = g.handleDDL(tb3, genCreateStmt("col1 varchar(255), col2 int"), genCreateStmt("col1 varchar(255), col2 int, col3 int"))
	require.True(t, schemaChanged)
	require.Equal(t, conflictStage, optimism.ConflictNone)
	// tb3 rename column
	schemaChanged, conflictStage = g.handleDDL(tb3, genCreateStmt("col1 varchar(255), col2 int, col3 int"), genCreateStmt("col1 varchar(255), col2 int, col4 int"))
	require.False(t, schemaChanged)
	require.Equal(t, conflictStage, optimism.ConflictSkipWaitRedirect)
	// tb2 add column with true name directly
	schemaChanged, conflictStage = g.handleDDL(tb2, genCreateStmt("col1 varchar(255), col2 int"), genCreateStmt("col1 varchar(255), col2 int, col4 int"))
	require.True(t, schemaChanged)
	require.Equal(t, conflictStage, optimism.ConflictNone)
	// tb3 idempotent
	// NOTE: rename column will be executed but retrun column already exists
	// data may insistent
	schemaChanged, conflictStage = g.handleDDL(tb3, genCreateStmt("col1 varchar(255), col2 int, col3 int"), genCreateStmt("col1 varchar(255), col2 int, col4 int"))
	require.True(t, schemaChanged)
	require.Equal(t, conflictStage, optimism.ConflictNone)

	// tb3 add column not null no default
	schemaChanged, conflictStage = g.handleDDL(tb3, genCreateStmt("col1 varchar(255), col2 int, col4 int"), genCreateStmt("col1 varchar(255), col2 int, col4 int, col5 int not null"))
	require.False(t, schemaChanged)
	require.Equal(t, conflictStage, optimism.ConflictSkipWaitRedirect)
	// tb2 rename column
	schemaChanged, conflictStage = g.handleDDL(tb2, genCreateStmt("col1 varchar(255), col2 int, col4 int"), genCreateStmt("col1 varchar(255), col3 int, col4 int"))
	require.False(t, schemaChanged)
	require.Equal(t, conflictStage, optimism.ConflictDetected)
}

func TestJoinTables(t *testing.T) {
	var (
		tb1 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		tb2 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb2"}
		tb3 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb3"}
		g   = &shardGroup{
			normalTables: map[metadata.SourceTable]string{
				tb1: "",
				tb2: genCreateStmt("col1 int"),
				tb3: genCreateStmt("col1 int", "col2 int"),
			},
			conflictTables: make(map[metadata.SourceTable]string),
		}
	)

	// no conflict
	joined, err := g.joinTables(normal)
	require.NoError(t, err)
	require.Equal(t, "CREATE TABLE `tbl`(`col1` INT(11), `col2` INT(11)) CHARSET UTF8MB4 COLLATE UTF8MB4_BIN", joined.String())
	joined, err = g.joinTables(final)
	require.NoError(t, err)
	require.Equal(t, "CREATE TABLE `tbl`(`col1` INT(11), `col2` INT(11)) CHARSET UTF8MB4 COLLATE UTF8MB4_BIN", joined.String())

	// has conflict
	g.conflictTables[tb3] = genCreateStmt("col1 varchar(255)", "col2 int")
	joined, err = g.joinTables(normal)
	require.NoError(t, err)
	require.Equal(t, "CREATE TABLE `tbl`(`col1` INT(11), `col2` INT(11)) CHARSET UTF8MB4 COLLATE UTF8MB4_BIN", joined.String())
	joined, err = g.joinTables(conflict)
	require.NoError(t, err)
	require.Equal(t, "CREATE TABLE `tbl`(`col1` VARCHAR(255) CHARACTER SET UTF8MB4 COLLATE utf8mb4_bin, `col2` INT(11)) CHARSET UTF8MB4 COLLATE UTF8MB4_BIN", joined.String())
	_, err = g.joinTables(final)
	require.Error(t, err)
}

func TestNoConflictForTables(t *testing.T) {
	var (
		tb1 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		tb2 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb2"}
		tb3 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb3"}
		g   = &shardGroup{
			normalTables: map[metadata.SourceTable]string{
				tb1: "",
				tb2: genCreateStmt("col1 int"),
				tb3: genCreateStmt("col1 int", "col2 int"),
			},
			conflictTables: make(map[metadata.SourceTable]string),
		}
	)
	require.True(t, g.noConflictForTables(conflict))
	require.True(t, g.noConflictForTables(final))

	// tb2 modify col1
	g.conflictTables[tb2] = genCreateStmt("col1 varchar(255)")
	require.True(t, g.noConflictForTables(conflict))
	require.False(t, g.noConflictForTables(final))

	// tb3 modify col2
	g.conflictTables[tb2] = genCreateStmt("col1 int", "col2 varchar(255)")
	require.False(t, g.noConflictForTables(conflict))
	require.False(t, g.noConflictForTables(final))

	// tb2 rename col1 to col3, tb3 rename col1 to col4
	g.conflictTables[tb2] = genCreateStmt("col3 int")
	g.conflictTables[tb3] = genCreateStmt("col4 int", "col2 int")
	require.False(t, g.noConflictForTables(conflict))
	require.False(t, g.noConflictForTables(final))
}

func TestNoConflictWithOneNormalTable(t *testing.T) {
	var (
		tb1 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		tb2 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb2"}
		tb3 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb3"}
		g   = &shardGroup{
			normalTables: map[metadata.SourceTable]string{
				tb1: "",
				tb2: genCreateStmt("col1 int"),
				tb3: genCreateStmt("col1 int", "col2 int"),
			},
			conflictTables: make(map[metadata.SourceTable]string),
		}
	)
	require.True(t, g.noConflictForTables(conflict))
	require.True(t, g.noConflictForTables(final))

	// tb2 rename col1
	prevTable := genCmpTable(genCreateStmt("col1 int"))
	postTable := genCmpTable(genCreateStmt("col3 int"))
	require.False(t, g.noConflictWithOneNormalTable(tb2, prevTable, postTable))

	// tb2 modify col1
	prevTable = genCmpTable(genCreateStmt("col1 int"))
	postTable = genCmpTable(genCreateStmt("col1 varchar(255)"))
	require.False(t, g.noConflictWithOneNormalTable(tb2, prevTable, postTable))

	// tb2 add not null no default col
	prevTable = genCmpTable(genCreateStmt("col1 int"))
	postTable = genCmpTable(genCreateStmt("col1 int", "col3 int not null"))
	require.False(t, g.noConflictWithOneNormalTable(tb2, prevTable, postTable))

	// tb3 rename col2
	prevTable = genCmpTable(genCreateStmt("col1 int", "col2 int"))
	postTable = genCmpTable(genCreateStmt("col1 int", "col4 int"))
	require.False(t, g.noConflictWithOneNormalTable(tb3, prevTable, postTable))

	// tb2 modify col1 forcely
	g.normalTables[tb2] = genCreateStmt("col1 varchar(255)")
	// tb3 modify col1
	prevTable = genCmpTable(genCreateStmt("col1 int", "col2 int"))
	postTable = genCmpTable(genCreateStmt("col1 varchar(255)", "col2 int"))
	require.True(t, g.noConflictWithOneNormalTable(tb3, prevTable, postTable))

	// tb2 rename col1 forcely
	g.normalTables[tb2] = genCreateStmt("col3 int")
	// tb3 rename col1
	prevTable = genCmpTable(genCreateStmt("col1 int", "col2 int"))
	postTable = genCmpTable(genCreateStmt("col3 int", "col2 int"))
	require.True(t, g.noConflictWithOneNormalTable(tb3, prevTable, postTable))

	// tb2 add not null no default col forcely
	g.normalTables[tb2] = genCreateStmt("col1 int", "col3 int not null")
	// tb3 add not null no default col
	prevTable = genCmpTable(genCreateStmt("col1 int", "col2 int"))
	postTable = genCmpTable(genCreateStmt("col1 int", "col2 int", "col3 int not null"))
	require.True(t, g.noConflictWithOneNormalTable(tb3, prevTable, postTable))
}

func TestAllTableSmaller(t *testing.T) {
	var (
		tb1 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		tb2 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb2"}
		tb3 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb3"}
		g   = &shardGroup{
			normalTables: map[metadata.SourceTable]string{
				tb1: "",
				tb2: genCreateStmt("col1 int"),
				tb3: genCreateStmt("col1 int", "col2 int"),
			},
			conflictTables: make(map[metadata.SourceTable]string),
		}
	)
	require.True(t, g.allTableSmaller(conflict))
	require.True(t, g.allTableSmaller(final))

	// tb2 modify col1
	g.conflictTables[tb2] = genCreateStmt("col1 varchar(255)")
	require.True(t, g.allTableSmaller(conflict))
	require.False(t, g.allTableSmaller(final))

	// tb3 modify col1
	g.conflictTables[tb3] = genCreateStmt("col1 varchar(255)", "col2 int")
	require.True(t, g.allTableSmaller(conflict))
	require.True(t, g.allTableSmaller(final))

	g.resolveTables()

	// tb3 rename col2
	g.conflictTables[tb3] = genCreateStmt("col3 varchar(255)", "col2 int")
	require.True(t, g.allTableSmaller(conflict))
	require.False(t, g.allTableSmaller(final))
	// tb2 rename col2
	g.conflictTables[tb2] = genCreateStmt("col3 varchar(255)")
	require.True(t, g.allTableSmaller(conflict))
	require.True(t, g.allTableSmaller(final))

	g.resolveTables()

	// tb3 add not null no default
	g.conflictTables[tb3] = genCreateStmt("col3 varchar(255)", "col2 int", "col4 int not null")
	require.True(t, g.allTableSmaller(conflict))
	require.False(t, g.allTableSmaller(final))
	// tb2 add not null no default
	g.conflictTables[tb2] = genCreateStmt("col3 varchar(255)", "col4 int not null")
	require.True(t, g.allTableSmaller(conflict))
	require.True(t, g.allTableSmaller(final))

	g.resolveTables()

	// tb2 modify column
	g.conflictTables[tb2] = genCreateStmt("col3 int", "col4 int not null")
	require.True(t, g.allTableSmaller(conflict))
	require.False(t, g.allTableSmaller(final))
	// tb3 rename column
	g.conflictTables[tb3] = genCreateStmt("col3 varchar(255)", "col2 int", "col5 int not null")
	require.False(t, g.allTableSmaller(conflict))
	require.False(t, g.allTableSmaller(final))
}

func TestAllTableLarger(t *testing.T) {
	var (
		tb1 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		tb2 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb2"}
		tb3 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb3"}
		g   = &shardGroup{
			normalTables: map[metadata.SourceTable]string{
				tb1: "",
				tb2: genCreateStmt("col1 int"),
				tb3: genCreateStmt("col1 int", "col2 int"),
			},
			conflictTables: make(map[metadata.SourceTable]string),
		}
	)
	require.True(t, g.allTableLarger(conflict))
	require.True(t, g.allTableLarger(final))

	// tb2 modify col1
	g.conflictTables[tb2] = genCreateStmt("col1 varchar(255)")
	require.True(t, g.allTableLarger(conflict))
	require.False(t, g.allTableLarger(final))

	// tb3 modify col1
	g.conflictTables[tb3] = genCreateStmt("col1 varchar(255)", "col2 int")
	require.True(t, g.allTableLarger(conflict))
	require.True(t, g.allTableLarger(final))

	g.resolveTables()

	// tb3 rename col2
	g.conflictTables[tb3] = genCreateStmt("col3 varchar(255)", "col2 int")
	require.True(t, g.allTableLarger(conflict))
	require.False(t, g.allTableLarger(final))
	// tb2 rename col2
	g.conflictTables[tb2] = genCreateStmt("col3 varchar(255)")
	require.True(t, g.allTableLarger(conflict))
	require.True(t, g.allTableLarger(final))

	g.resolveTables()

	// tb3 add not null no default
	g.conflictTables[tb3] = genCreateStmt("col3 varchar(255)", "col2 int", "col4 int not null")
	require.True(t, g.allTableLarger(conflict))
	require.False(t, g.allTableLarger(final))
	// tb2 add not null no default
	g.conflictTables[tb2] = genCreateStmt("col3 varchar(255)", "col4 int not null")
	require.True(t, g.allTableLarger(conflict))
	require.True(t, g.allTableLarger(final))

	g.resolveTables()

	// tb2 modify column
	g.conflictTables[tb2] = genCreateStmt("col3 int", "col4 int not null")
	require.True(t, g.allTableLarger(conflict))
	require.False(t, g.allTableLarger(final))
	// tb3 rename column
	g.conflictTables[tb3] = genCreateStmt("col3 varchar(255)", "col2 int", "col5 int not null")
	require.False(t, g.allTableLarger(conflict))
	require.False(t, g.allTableLarger(final))
	// tb3 modify another column
	g.conflictTables[tb3] = genCreateStmt("col3 varchar(255)", "col2 int", "col4 varchar(255) not null")
	require.False(t, g.allTableLarger(conflict))
	require.False(t, g.allTableLarger(final))
}

func TestDroppedColumn(t *testing.T) {
	var (
		tb1 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		tb2 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb2"}
		tb3 = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb3"}
		g   = &shardGroup{
			normalTables: map[metadata.SourceTable]string{
				tb1: genCreateStmt("col1 int", "col2 int"),
				tb2: genCreateStmt("col1 int", "col2 int"),
				tb3: genCreateStmt("col1 int", "col2 int", "col3 int"),
			},
			conflictTables:      make(map[metadata.SourceTable]string),
			droppedColumnsStore: metadata.NewDroppedColumnsStore(mock.NewMetaMock(), metadata.TargetTable{Schema: "schema", Table: "table"}),
		}
	)

	// drop column
	col, err := g.checkAddDroppedColumn(context.Background(), tb1, "ALTER TABLE `tb1` DROP COLUMN `col1`", genCreateStmt("col1 int", "col2 int"), genCreateStmt("col2 int"), []string{"col1"})
	require.NoError(t, err)
	require.Equal(t, "col1", col)

	// add column with a larger field len
	col, err = g.checkAddDroppedColumn(context.Background(), tb1, "ALTER TABLE `tb3` ADD COLUMN `col3` BIGINT", genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 int", "col2 int", "col3 bigint"), nil)
	require.Contains(t, err.Error(), "add columns with different field lengths")
	require.Equal(t, "", col)

	// add column with a smaller field len
	col, err = g.checkAddDroppedColumn(context.Background(), tb1, "ALTER TABLE `tb3` ADD COLUMN `col3` SMALLINT", genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 int", "col2 int", "col3 smallint"), nil)
	require.Contains(t, err.Error(), "add columns with different field lengths")
	require.Equal(t, "", col)

	// add dropped column
	g.droppedColumnsStore.AddDroppedColumns(context.Background(), []string{"col3"}, tb2)
	col, err = g.checkAddDroppedColumn(context.Background(), tb2, "ALTER TABLE `tb2` ADD COLUMN `col3` INT", genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 int", "col2 int", "col3 int"), nil)
	require.Contains(t, err.Error(), "wasn't fully dropped in downstream")
	require.Equal(t, "", col)

	col, err = g.checkAddDroppedColumn(context.Background(), tb1, "ALTER TABLE `tb1` ADD INDEX uk(col1)", genCreateStmt("col1 int", "col2 int"), genCreateStmt("col1 int", "col2 int"), nil)
	require.NoError(t, err)
	require.Equal(t, "", col)
}

func TestGCDroppedColumns(t *testing.T) {
	var (
		tb1        = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb1"}
		tb2        = metadata.SourceTable{Source: "source", Schema: "schema", Table: "tb2"}
		tableAgent = &MockCheckpointAgent{}
		g          = &shardGroup{
			normalTables: map[metadata.SourceTable]string{
				tb1: genCreateStmt("col1 int", "col2 int"),
				tb2: genCreateStmt("col1 int", "col2 int", "col3 int"),
			},
			conflictTables:      make(map[metadata.SourceTable]string),
			droppedColumnsStore: metadata.NewDroppedColumnsStore(mock.NewMetaMock(), metadata.TargetTable{Schema: "schema", Table: "table"}),
			tableAgent:          tableAgent,
		}
	)

	require.NoError(t, g.gcDroppedColumns(context.Background()))

	// add dropped column
	g.droppedColumnsStore.AddDroppedColumns(context.Background(), []string{"col3"}, tb1)
	g.droppedColumnsStore.AddDroppedColumns(context.Background(), []string{"col2"}, tb1)

	require.NoError(t, g.gcDroppedColumns(context.Background()))

	g.normalTables[tb2] = genCreateStmt("col1 int", "col2 int")
	tableAgent.On("FetchTableStmt").Return("", errors.New("table info not found")).Once()
	tableAgent.On("FetchTableStmt").Return(genCreateStmt("col1 int", "col2 int", "col3 int"), nil).Once()
	require.NoError(t, g.gcDroppedColumns(context.Background()))

	tableAgent.On("FetchTableStmt").Return(genCreateStmt("col1 int", "col2 int"), nil).Twice()
	require.NoError(t, g.gcDroppedColumns(context.Background()))
	state, err := g.droppedColumnsStore.Get(context.Background())
	require.NoError(t, err)
	droppedColumns := state.(*metadata.DroppedColumns)
	require.Len(t, droppedColumns.Cols, 1)
	require.Contains(t, droppedColumns.Cols, "col2")

	g.normalTables[tb1] = genCreateStmt("col1 int", "col3 int")
	g.normalTables[tb2] = genCreateStmt("col1 int", "col3 int")
	tableAgent.On("FetchTableStmt").Return(genCreateStmt("col1 int", "col3 int"), nil).Twice()
	require.NoError(t, g.gcDroppedColumns(context.Background()))
	state, err = g.droppedColumnsStore.Get(context.Background())
	require.Equal(t, errors.Cause(err), metadata.ErrStateNotFound)
	require.Nil(t, state)

	tableAgent.AssertExpectations(t)
}

func genCreateStmt(cols ...string) string {
	str := "CREATE TABLE tbl("
	for idx, col := range cols {
		if idx == 0 {
			str += col
		} else {
			str += ", " + col
		}
	}
	str += ")"
	return str
}
