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
	"strings"
	"sync"

	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/schemacmp"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

// Workflow:
//
// Worker1                    Jobmaster                    DDLCoordinator                        TiDB
//   │                            │                              │                                │
//   │                            │ Init                         │                                │
//   │                            ├─────────────────────────────►│ Reset                          │
//   │                            │                              ├───────────────────────────────►│
//   │                            │                              │ FetchAllDoTables               │
//   │ DDLCoordinateRequest       │                              │                                │
//   ├───────────────────────────►│ Coordinate                   │                                │
//   │ AlterTableDropColumn       ├─────────────────────────────►│ CreateShardGroup               │
//   │                            │                              ├───────────────────────────────►│
//   │                            │                              │ FetchAllDoTables               │
//   │                            │                              │                                │
//   │                            │                              ├───┐                            │
//   │                            │                              │   │ Handle                     │
//   │                            │                              │   │                            │
//   │                            │                              │   │ AddDroppedColumn           │
//   │                            │ Response                     │◄──┘                            │
//   │ DDLCoordinateResponse      │◄─────────────────────────────┤                                │
//   │◄───────────────────────────┤ Skip                         │                                │
//   │ Skip                       │                              │                                │
//   │                            │                              │                                │
//   │                            │                              │                                │
//   │ DDLCoordinateRequest       │                              │                                │
//   ├───────────────────────────►│ Coordinate                   │                                │
//   │ AlterTableDropColumn       ├─────────────────────────────►│                                │
//   │                            │                              ├───┐                            │
//   │                            │                              │   │ Handle                     │
//   │                            │                              │   │                            │
//   │                            │                              │   │ AddDroppedColumn           │
//   │                            │ Response                     │◄──┘                            │
//   │ DDLCoordinateResponse      │◄─────────────────────────────┤                                │
//   │◄───────────────────────────┤ Execute                      │                                │
//   │ Execute                    │                              │                                │
//   │                            │                              │                                │
//   │                            │                              │                                │
//   │ DDLCoordinateRequest       │                              │                                │
//   ├───────────────────────────►│                              │                                │
//   │ AlterTableRenameColumn     │ Coordinate                   │                                │
//   │                            ├─────────────────────────────►│                                │
//   │                            │                              ├───┐                            │
//   │                            │                              │   │ GCDroppedColumn            │
//   │                            │                              │   ├───────────────────────────►│
//   │                            │                              │   │                            │
//   │                            │                              │   │ Handle                     │
//   │                            │ Response                     │◄──┘                            │
//   │                            │◄─────────────────────────────┤                                │
//   │ DDLCoordinateResponse      │ SkipAndWaitRedirect          │                                │
//   │◄───────────────────────────┤                              │                                │
//   │ SkipAndWaitRedirect        │                              │                                │
//   │                            │                              │                                │
//   │                            ├───┐                          │                                │
//   │                            │   │ Jobmaster failover       │                                │
//   │                            │   │                          │                                │
//   │                            │   │ Recover                  │                                │
//   │                            │   ├─────────────────────────►│ Reset                          │
//   │                            │   │                          ├───────────────────────────────►│
//   │ RestartAllWorkers          │◄──┘                          │ FetchAllDoTables               │
//   │◄───────────────────────────┤                              │                                │
//   │                            │                              │                                │
//   ├───┐                        │                              │                                │
//   │   │ Restart From Checkpoint│                              │                                │
//   │◄──┘                        │                              │                                │
//   │                            │                              │                                │
//   │ DDLCoordinateRequest       │                              │                                │
//   ├───────────────────────────►│ Coordinate                   │                                │
//   │ AlterTableRenameColumn     ├─────────────────────────────►│                                │
//   │                            │                              ├───┐                            │
//   │                            │                              │   │ Handle                     │
//   │                            │ Response                     │◄──┘                            │
//   │                            │◄─────────────────────────────┤                                │
//   │ DDLCoordinateResponse      │ SkipAndWaitRedirect          │                                │
//   │◄───────────────────────────┤                              │                                │
//   │ SkipAndWaitRedirect        │                              │                                │
//   │                            │                              │                                │
//   │                            │                              │ Worker2 Rename Column          │
//   │                            │                              │◄────────────────────────────   │
//   │  DDLRedirectRequest        │                              │                                │
//   │◄───────────────────────────┼──────────────────────────────┤                                │
//   │────────────────────────────┼─────────────────────────────►│                                │
//   │  DDLRedirectResponse       │                              │                                │
//   │                            │                              │                                │
//   ├────┐                       │                              │                                │
//   │    │                       │                              │                                │
//   │    │ Redirect              │                              │                                │
//   │    │                       │                              │                                │
//   │◄───┘                       │                              │                                │
//   │                            │                              │                                │

type tableType int

const (
	// normal represents the type of shardGroup.normalTables.
	normal tableType = iota
	// conflict represents the type of shardGroup.conflictTables.
	conflict
	// final represents the type of shardGroup.conflictTables if exist else shardGroup.normalTables.
	final
)

// TableAgent defines an interface for checkpoint.
type TableAgent interface {
	FetchAllDoTables(ctx context.Context, cfg *config.JobCfg) (map[metadata.TargetTable][]metadata.SourceTable, error)
	FetchTableStmt(ctx context.Context, jobID string, cfg *config.JobCfg, sourceTable metadata.SourceTable) (string, error)
}

// DDLCoordinator is a coordinator for ddl.
type DDLCoordinator struct {
	mu          sync.RWMutex
	tables      map[metadata.TargetTable]map[metadata.SourceTable]struct{}
	shardGroups map[metadata.TargetTable]*shardGroup
	logger      *zap.Logger

	kvClient   metaModel.KVClient
	tableAgent TableAgent
	jobID      string
	jobStore   *metadata.JobStore
}

// NewDDLCoordinator creates a new DDLCoordinator.
func NewDDLCoordinator(jobID string, kvClient metaModel.KVClient, tableAgent TableAgent, jobStore *metadata.JobStore, pLogger *zap.Logger) *DDLCoordinator {
	return &DDLCoordinator{
		tableAgent:  tableAgent,
		tables:      make(map[metadata.TargetTable]map[metadata.SourceTable]struct{}),
		jobID:       jobID,
		kvClient:    kvClient,
		shardGroups: make(map[metadata.TargetTable]*shardGroup),
		jobStore:    jobStore,
		logger:      pLogger.With(zap.String("component", "ddl_coordinator")),
	}
}

// ClearMetadata clears metadata.
func (c *DDLCoordinator) ClearMetadata(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return metadata.DelAllDroppedColumns(ctx, c.kvClient)
}

// Reset resets the ddl coordinator.
func (c *DDLCoordinator) Reset(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	jobCfg, err := c.jobStore.GetJobCfg(ctx)
	if err != nil {
		return err
	}
	if jobCfg.ShardMode == "" {
		c.logger.Info("non-shard-mode, skip reset")
		return nil
	}
	c.logger.Info("reset ddl coordinator")

	// fetch all tables which need to be coordinated.
	tables, err := c.tableAgent.FetchAllDoTables(ctx, jobCfg)
	if err != nil {
		return err
	}

	for targetTable, sourceTables := range tables {
		c.tables[targetTable] = make(map[metadata.SourceTable]struct{}, 0)
		for _, sourceTable := range sourceTables {
			c.tables[targetTable][sourceTable] = struct{}{}
		}
	}
	return nil
}

// Coordinate coordinates ddls.
func (c *DDLCoordinator) Coordinate(ctx context.Context, item *metadata.DDLItem) ([]string, optimism.ConflictStage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	jobCfg, err := c.jobStore.GetJobCfg(ctx)
	if err != nil {
		return nil, optimism.ConflictError, err
	} else if jobCfg.ShardMode == "" {
		return nil, optimism.ConflictError, errors.New("coordinate error with non-shard-mode")
	}

	// create shard group if not exists.
	g, err := c.loadOrCreateShardGroup(ctx, item.TargetTable, jobCfg)
	if err != nil {
		return nil, optimism.ConflictError, err
	}

	ddls, conflictStage, err := g.handle(ctx, item)
	if err != nil {
		return ddls, conflictStage, err
	}

	// if all source table is deleted or the shard group is resolved, we should remove the shard group.
	if g.isResolved(ctx) {
		c.removeShardGroup(ctx, item.TargetTable)
	}

	// handle table level ddl
	switch item.Type {
	case metadata.CreateTable:
		tables, ok := c.tables[item.TargetTable]
		if !ok {
			c.tables[item.TargetTable] = make(map[metadata.SourceTable]struct{}, 0)
			tables = c.tables[item.TargetTable]
		}
		tables[item.SourceTable] = struct{}{}
	case metadata.DropTable:
		delete(c.tables[item.TargetTable], item.SourceTable)
		if len(c.tables[item.TargetTable]) == 0 {
			delete(c.tables, item.TargetTable)
		}
	}
	return ddls, conflictStage, err
}

// ShowDDLLocks show ddl locks.
func (c *DDLCoordinator) ShowDDLLocks(ctx context.Context) ShowDDLLocksResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ddlLocks := make(map[metadata.TargetTable]DDLLock)
	for targetTable, g := range c.shardGroups {
		tbs := g.showTables()
		ddlLocks[targetTable] = DDLLock{ShardTables: tbs}
	}
	return ShowDDLLocksResponse{Locks: ddlLocks}
}

func (c *DDLCoordinator) loadOrCreateShardGroup(ctx context.Context, targetTable metadata.TargetTable, jobCfg *config.JobCfg) (*shardGroup, error) {
	if g, ok := c.shardGroups[targetTable]; ok {
		return g, nil
	}

	newGroup, err := newShardGroup(ctx, c.jobID, jobCfg, targetTable, c.tables[targetTable], c.kvClient, c.tableAgent)
	if err != nil {
		return nil, err
	}
	c.shardGroups[targetTable] = newGroup
	return newGroup, nil
}

func (c *DDLCoordinator) removeShardGroup(ctx context.Context, targetTable metadata.TargetTable) {
	if g, ok := c.shardGroups[targetTable]; ok {
		if err := g.clear(ctx); err != nil {
			c.logger.Error("clear shard group failed", zap.Error(err))
		}
	}
	delete(c.shardGroups, targetTable)
}

type shardGroup struct {
	// normalTables represents upstream table info record in checkpoint.
	normalTables map[metadata.SourceTable]string
	// conflictTables represents upstream table info after executing conflict DDL.
	conflictTables      map[metadata.SourceTable]string
	tableAgent          TableAgent
	droppedColumnsStore *metadata.DroppedColumnsStore
	id                  frameModel.MasterID
	cfg                 *config.JobCfg
}

func newShardGroup(ctx context.Context, id frameModel.MasterID, cfg *config.JobCfg, targetTable metadata.TargetTable, sourceTables map[metadata.SourceTable]struct{}, kvClient metaModel.KVClient, tableAgent TableAgent) (*shardGroup, error) {
	g := &shardGroup{
		tableAgent:          tableAgent,
		normalTables:        make(map[metadata.SourceTable]string),
		conflictTables:      make(map[metadata.SourceTable]string),
		droppedColumnsStore: metadata.NewDroppedColumnsStore(kvClient, targetTable),
		id:                  id,
		cfg:                 cfg,
	}
	for sourceTable := range sourceTables {
		stmt, err := g.tableAgent.FetchTableStmt(ctx, id, cfg, sourceTable)
		// NOTE: There are cases where the source table exists but the table info does not,
		// such as a table is created upstream when the coordinator starts, but the dm-worker is not yet synchronized
		// we skip these tables in handleDDL.
		// TODO: better error handling
		if err != nil && !strings.HasPrefix(err.Error(), "table info not found") {
			return nil, errors.Errorf("fetch table stmt from checkpoint failed, sourceTable: %s, err: %v", sourceTable, err)
		}
		g.normalTables[sourceTable] = stmt
	}
	return g, nil
}

func (g *shardGroup) handle(ctx context.Context, item *metadata.DDLItem) ([]string, optimism.ConflictStage, error) {
	// nolint:errcheck
	g.gcDroppedColumns(ctx)

	var (
		ddls          []string
		err           error
		conflictStage optimism.ConflictStage = optimism.ConflictNone
	)

	switch item.Type {
	case metadata.CreateTable:
		g.handleCreateTable(item)
		ddls = append(ddls, item.DDLs...)
	case metadata.DropTable:
		g.handleDropTable(ctx, item)
		ddls = append(ddls, item.DDLs...)
	case metadata.OtherDDL:
		ddls, conflictStage, err = g.handleDDLs(ctx, item)
	default:
		return nil, optimism.ConflictError, errors.Errorf("unknown ddl type %v", item.Type)
	}

	return ddls, conflictStage, err
}

// handleCreateTable handles create table ddl.
// add new source table to shard group.
func (g *shardGroup) handleCreateTable(item *metadata.DDLItem) {
	stmt, ok := g.normalTables[item.SourceTable]
	if ok && stmt != "" {
		log.L().Warn("create table already exists", zap.Any("source table", item.SourceTable))
	}

	g.normalTables[item.SourceTable] = item.Tables[0]
}

// handleDropTable handles drop table ddl.
// remove source table from shard group.
// mark shard group as deleted if all source tables are deleted.
func (g *shardGroup) handleDropTable(ctx context.Context, item *metadata.DDLItem) {
	_, ok := g.normalTables[item.SourceTable]
	if !ok {
		log.L().Warn("drop table does not exist", zap.Any("source table", item.SourceTable))
		return
	}

	delete(g.normalTables, item.SourceTable)
	delete(g.conflictTables, item.SourceTable)
	// nolint:errcheck
	g.droppedColumnsStore.DelDroppedColumnForTable(ctx, item.SourceTable)
}

// handleDDLs handles ddl.
func (g *shardGroup) handleDDLs(ctx context.Context, item *metadata.DDLItem) (newDDLs []string, conflictStage optimism.ConflictStage, err error) {
	stmt, ok := g.normalTables[item.SourceTable]
	if !ok || stmt == "" {
		log.L().Warn("table does not exist", zap.Any("source table", item.SourceTable))
		g.normalTables[item.SourceTable] = item.Tables[0]
	}

	dropCols := make([]string, 0, len(item.DDLs))

	// handle ddls one by one
	for idx, ddl := range item.DDLs {
		prevTableStmt, postTableStmt := g.getTableForOneDDL(item, idx)
		schemaChanged, conflictStage := g.handleDDL(item.SourceTable, prevTableStmt, postTableStmt)

		switch conflictStage {
		case optimism.ConflictDetected:
			return nil, optimism.ConflictDetected, errors.Errorf("conflict detected for table %v", item.SourceTable)
		case optimism.ConflictSkipWaitRedirect:
			return newDDLs, optimism.ConflictSkipWaitRedirect, nil
		case optimism.ConflictNone:
			if col, err := g.checkAddDroppedColumn(ctx, item.SourceTable, ddl, prevTableStmt, postTableStmt, dropCols); err != nil {
				return nil, optimism.ConflictError, err
			} else if len(col) != 0 {
				dropCols = append(dropCols, col)
			}
		case optimism.ConflictResolved:
		}
		if schemaChanged {
			newDDLs = append(newDDLs, ddl)
		}
	}

	if len(dropCols) > 0 {
		if err := g.droppedColumnsStore.AddDroppedColumns(ctx, dropCols, item.SourceTable); err != nil {
			return nil, optimism.ConflictError, err
		}
	}

	return newDDLs, optimism.ConflictNone, nil
}

func (g *shardGroup) handleDDL(sourceTable metadata.SourceTable, prevTableStmt, postTableStmt string) (bool, optimism.ConflictStage) {
	// for a new ddl, we ignore the original conflict table, just use the normal table and new ddl to calculate the ConfictStage.
	delete(g.conflictTables, sourceTable)

	prevTable := genCmpTable(prevTableStmt)
	postTable := genCmpTable(postTableStmt)

	currTable := genCmpTable(g.normalTables[sourceTable])
	// handle idempotent ddl
	idempotent := false
	if cmp, err := prevTable.Compare(currTable); err != nil || cmp != 0 {
		if cmp, err := postTable.Compare(currTable); err == nil && cmp == 0 {
			idempotent = true
		}
		// this usually happened when worker restarts and the shard group is not reset.
		log.L().Warn("prev-table not equal table saved in master", zap.Stringer("master-table", currTable), zap.Stringer("prev-table", prevTable))
		g.normalTables[sourceTable] = prevTableStmt
	}

	tableCmp, tableErr := prevTable.Compare(postTable)
	// Normal DDL
	if tableErr == nil {
		oldJoined, oldErr := g.joinTables(normal)

		g.normalTables[sourceTable] = postTableStmt
		newJoined, newErr := g.joinTables(normal)
		// normal DDL can be sync if no error
		if newErr == nil {
			// if a normal DDL let all final tables become no conflict
			// return ConflictNone
			if len(g.conflictTables) > 0 && g.noConflictForTables(final) {
				log.L().Info("all conflict resolved for the DDL", zap.Any("source table", sourceTable), zap.String("prevTable", prevTableStmt), zap.String("postTable", postTableStmt))
				g.resolveTables()
				return true, optimism.ConflictNone
			}

			// should not happened
			if oldErr != nil {
				return true, optimism.ConflictNone
			}
			joinedCmp, joinedErr := oldJoined.Compare(newJoined)
			// return schema changed in 2 cases
			// oldJoined != newJoined
			// prevTable < postTable
			return (joinedErr != nil || joinedCmp != 0) || tableCmp < 0, optimism.ConflictNone
		}
	}

	log.L().Info("found conflict for DDL", zap.Any("source table", sourceTable), zap.String("prevTable", prevTableStmt), zap.String("postTable", postTableStmt), log.ShortError(tableErr))

	if idempotent || g.noConflictWithOneNormalTable(sourceTable, prevTable, postTable) {
		log.L().Info("directly return conflict DDL", zap.Bool("idempotent", idempotent), zap.Any("source", sourceTable), zap.Stringer("prevTable", prevTable), zap.Stringer("postTable", postTable))
		g.normalTables[sourceTable] = postTableStmt
		return true, optimism.ConflictNone
	}

	// meet conflict DDL
	g.normalTables[sourceTable] = prevTableStmt
	g.conflictTables[sourceTable] = postTableStmt

	// if any conflict happened between conflict DDLs, return error
	// e.g. tb1: "ALTER TABLE RENAME a TO b", tb2: "ALTER TABLE RENAME c TO d"
	if !g.noConflictForTables(conflict) {
		log.L().Error("conflict happened with other conflict tables", zap.Any("source table", sourceTable), zap.String("prevTable", prevTableStmt), zap.String("postTable", postTableStmt))
		return false, optimism.ConflictDetected
	}

	if g.noConflictForTables(final) {
		log.L().Info("all conflict resolved for the DDL", zap.Any("source table", sourceTable), zap.String("prevTable", prevTableStmt), zap.String("postTable", postTableStmt))
		g.resolveTables()
		return true, optimism.ConflictNone
	}
	log.L().Info("conflict hasn't been resolved", zap.Any("source table", sourceTable), zap.Stringer("prevTable", prevTable), zap.Stringer("postTable", postTable))
	return false, optimism.ConflictSkipWaitRedirect
}

// joinTables join tables by tableType.
func (g *shardGroup) joinTables(tp tableType) (schemacmp.Table, error) {
	var (
		joined     schemacmp.Table
		err        error
		firstTable = true
	)

	for sourceTable := range g.normalTables {
		tableStmt, ok := g.getTableBySourceTable(sourceTable, tp)
		if !ok || tableStmt == "" {
			continue
		}
		table := genCmpTable(tableStmt)

		if firstTable {
			joined = table
			firstTable = false
			continue
		}
		joined, err = joined.Join(table)
		if err != nil {
			return joined, err
		}
	}
	return joined, nil
}

// noConflictForTables checks if there is no conflict for tables by tableType(conflictTable/finalTable).
// if there is conflict for conflictTables, we should report error to user.
// if there is no conflict for finalTables, we should report conflict resolved to worker.
func (g *shardGroup) noConflictForTables(tp tableType) bool {
	if _, err := g.joinTables(tp); err != nil {
		return false
	}
	if !g.allTableSmaller(tp) {
		return false
	}
	if !g.allTableLarger(tp) {
		return false
	}
	return true
}

func (g *shardGroup) noConflictWithOneNormalTable(sourceTable metadata.SourceTable, prevTable, postTable schemacmp.Table) bool {
	for st, ti := range g.normalTables {
		if st == sourceTable {
			continue
		}
		if ti == "" {
			continue
		}
		t := genCmpTable(ti)

		// judge joined no error
		joined, err := postTable.Join(t)
		if err != nil {
			continue
		}

		// judge this normal table is smaller(same as allTableSmaller)
		if _, err = joined.Compare(prevTable); err == nil {
			continue
		}

		// judge this normal table is larger(same as allTableLarger)
		if joined, err = prevTable.Join(t); err != nil {
			if _, err = t.Compare(postTable); err == nil {
				return true
			}
		}
		if cmp, err := joined.Compare(postTable); err != nil || cmp < 0 {
			continue
		}

		return true
	}
	return false
}

// see dm/pkg/shardddl/optimism/lock.go:allTableSmaller for more detail
func (g *shardGroup) allTableSmaller(tp tableType) bool {
	var (
		joined schemacmp.Table
		err    error
	)
	joined, err = g.joinTables(tp)

	if err != nil {
		return false
	}

	for sourceTable := range g.conflictTables {
		t := genCmpTable(g.normalTables[sourceTable])
		if _, err = joined.Compare(t); err == nil {
			return false
		}
	}
	return true
}

// see dm/pkg/shardddl/optimism/lock.go:allTableLarger for more detail
func (g *shardGroup) allTableLarger(tp tableType) bool {
	for sourceTable, conflictTableStmt := range g.conflictTables {
		conflictTable := genCmpTable(conflictTableStmt)
		// for every conflict table's prev_table
		normalTable := genCmpTable(g.normalTables[sourceTable])

		for s := range g.normalTables {
			// for every judge table
			judgeTableStmt, ok := g.getTableBySourceTable(s, tp)
			if !ok || judgeTableStmt == "" {
				continue
			}
			judgeTable := genCmpTable(judgeTableStmt)

			joined, err := normalTable.Join(judgeTable)
			if err != nil {
				// modify column
				if _, err := judgeTable.Join(conflictTable); err != nil {
					return false
				}
			} else if cmp, err := joined.Compare(conflictTable); err != nil || cmp < 0 {
				return false
			}
		}
	}
	return true
}

func (g *shardGroup) resolveTables() {
	for sourceTable, conflictStmt := range g.conflictTables {
		g.normalTables[sourceTable] = conflictStmt
	}
	g.conflictTables = make(map[metadata.SourceTable]string)
	// TODO: redirect for conflict worker.
}

func (g *shardGroup) getTableForOneDDL(item *metadata.DDLItem, idx int) (string, string) {
	return item.Tables[idx], item.Tables[idx+1]
}

func (g *shardGroup) getTableBySourceTable(st metadata.SourceTable, tp tableType) (string, bool) {
	var (
		stmt string
		ok   bool
	)
	switch tp {
	case normal:
		stmt, ok = g.normalTables[st]
	case conflict:
		stmt, ok = g.conflictTables[st]
	case final:
		stmt, ok = g.conflictTables[st]
		if !ok {
			stmt, ok = g.normalTables[st]
		}
	}
	return stmt, ok
}

func (g *shardGroup) checkAddDroppedColumn(ctx context.Context, sourceTable metadata.SourceTable, ddl string, prevTableStmt, postTableStmt string, newDroppedColumns []string) (string, error) {
	currTable := g.normalTables[sourceTable]
	defer func() {
		g.normalTables[sourceTable] = currTable
	}()

	g.normalTables[sourceTable] = prevTableStmt
	oldJoined, err := g.joinTables(normal)
	if err != nil {
		// nolint:nilerr
		return "", nil
	}

	postTable := genCmpTable(postTableStmt)
	g.normalTables[sourceTable] = postTableStmt
	newJoined, err := g.joinTables(normal)
	if err != nil {
		// nolint:nilerr
		return "", nil
	}

	cmp, err := oldJoined.Compare(newJoined)
	if err != nil {
		// nolint:nilerr
		return "", nil
	}

	if cmp <= 0 {
		if col, err2 := optimism.AddDifferentFieldLenColumns("", ddl, oldJoined, newJoined); err2 != nil {
			// check for add column with a larger field len
			return "", err2
		} else if _, err2 = optimism.AddDifferentFieldLenColumns("", ddl, postTable, newJoined); err2 != nil {
			// check for add column with a smaller field len
			return "", err2
		} else if len(col) > 0 && (g.droppedColumnsStore.HasDroppedColumn(ctx, col, sourceTable) || slices.Contains(newDroppedColumns, col)) {
			return "", errors.Errorf("add column %s that wasn't fully dropped in downstream. ddl: %s", col, ddl)
		}
	}

	if cmp >= 0 {
		if col, err2 := optimism.GetColumnName("", ddl, ast.AlterTableDropColumn); err2 != nil {
			return "", err2
		} else if len(col) > 0 {
			return col, nil
		}
	}
	return "", nil
}

func (g *shardGroup) gcDroppedColumns(ctx context.Context) error {
	state, err := g.droppedColumnsStore.Get(ctx)
	if err != nil {
		if errors.Cause(err) == metadata.ErrStateNotFound {
			return nil
		}
		return err
	}

	droppedColumns := state.(*metadata.DroppedColumns)
	cacheStmts := make(map[metadata.SourceTable]string)
OutLoop:
	for col := range droppedColumns.Cols {
		// firstly, check the tables recorded in the ddl coordinator
		for _, tbStmt := range g.normalTables {
			if tbStmt == "" {
				continue
			}
			if cols := getColumnNames(tbStmt); slices.Contains(cols, col) {
				continue OutLoop
			}
		}

		// secondly, check the tables from checkpoint
		for sourceTable := range g.normalTables {
			tbStmt, ok := cacheStmts[sourceTable]
			if !ok {
				tbStmt, err = g.tableAgent.FetchTableStmt(ctx, g.id, g.cfg, sourceTable)
				if err != nil {
					if strings.HasPrefix(err.Error(), "table info not found") {
						continue
					}
					return err
				}
				cacheStmts[sourceTable] = tbStmt
			}

			if cols := getColumnNames(tbStmt); slices.Contains(cols, col) {
				continue OutLoop
			}
		}
		if err := g.droppedColumnsStore.DelDroppedColumn(ctx, col); err != nil {
			return err
		}
	}
	return nil
}

// isResolved means all tables in the group are resolved.
// 1. no conflict ddls waiting
// 2. all dropped column has done
// 3. all shard tables stmts are same.
func (g *shardGroup) isResolved(ctx context.Context) bool {
	if len(g.conflictTables) != 0 {
		return false
	}
	if _, err := g.droppedColumnsStore.Get(ctx); errors.Cause(err) != metadata.ErrStateNotFound {
		return false
	}

	var (
		prevTable schemacmp.Table
		first     = true
	)
	for _, tbStmt := range g.normalTables {
		if tbStmt == "" {
			continue
		}
		if first {
			prevTable = genCmpTable(tbStmt)
			first = false
			continue
		}
		currTable := genCmpTable(tbStmt)
		if cmp, err := prevTable.Compare(currTable); err != nil || cmp != 0 {
			return false
		}
		prevTable = currTable
	}
	return true
}

func (g *shardGroup) clear(ctx context.Context) error {
	return g.droppedColumnsStore.Delete(ctx)
}

func (g *shardGroup) showTables() map[metadata.SourceTable]ShardTable {
	tables := make(map[metadata.SourceTable]ShardTable, 0)
	for sourceTable, stmt := range g.normalTables {
		tables[sourceTable] = ShardTable{
			Current: stmt,
			Next:    g.conflictTables[sourceTable],
		}
	}
	// show dropped columns if needed.
	return tables
}

func genCmpTable(createStmt string) schemacmp.Table {
	p := parser.New()
	stmtNode, _ := p.ParseOneStmt(createStmt, "", "")
	ti, _ := ddl.BuildTableInfoFromAST(stmtNode.(*ast.CreateTableStmt))
	ti.State = model.StatePublic
	return schemacmp.Encode(ti)
}

// getColumnNames and return columns' names for create table stmt.
func getColumnNames(createStmt string) []string {
	p := parser.New()
	stmtNode, _ := p.ParseOneStmt(createStmt, "", "")
	s := stmtNode.(*ast.CreateTableStmt)

	cols := make([]string, 0, len(s.Cols))
	for _, col := range s.Cols {
		cols = append(cols, col.Name.Name.O)
	}
	return cols
}
