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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/schemacmp"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/config"
	"github.com/pingcap/tiflow/engine/jobmaster/dm/metadata"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"go.uber.org/zap"
)

type tableType int

const (
	normal tableType = iota
	conflict
	final
)

// TableAgent defines an interface for checkpoint.
type TableAgent interface {
	FetchAllDoTables(ctx context.Context, cfg *config.JobCfg) (map[metadata.TargetTable][]metadata.SourceTable, error)
	FetchTableStmt(ctx context.Context, jobID string, cfg *config.JobCfg, sourceTable metadata.SourceTable) (string, error)
}

// DDLCoordinator is a coordinator for ddl.
type DDLCoordinator struct {
	mu          sync.Mutex
	tables      map[metadata.TargetTable]map[metadata.SourceTable]struct{}
	shardGroups map[metadata.TargetTable]*shardGroup
	pendings    sync.WaitGroup
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

// Reset resets the shard group.
func (c *DDLCoordinator) Reset(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// wait all pending ddls coordinated.
	c.pendings.Wait()

	jobCfg, err := c.jobStore.GetJobCfg(ctx)
	if err != nil {
		return err
	}
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

func (c *DDLCoordinator) loadOrCreateShardGroup(ctx context.Context, targetTable metadata.TargetTable) (*shardGroup, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if g, ok := c.shardGroups[targetTable]; ok {
		return g, nil
	}

	jobCfg, err := c.jobStore.GetJobCfg(ctx)
	if err != nil {
		return nil, err
	}
	newGroup, err := newShardGroup(ctx, c.jobID, jobCfg, targetTable, c.tables[targetTable], c.kvClient, c.tableAgent)
	if err != nil {
		return nil, err
	}
	c.shardGroups[targetTable] = newGroup
	return newGroup, nil
}

func (c *DDLCoordinator) removeShardGroup(targetTable metadata.TargetTable) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.shardGroups, targetTable)
}

// Coordinate coordinates ddls.
func (c *DDLCoordinator) Coordinate(ctx context.Context, item *metadata.DDLItem) ([]string, optimism.ConflictStage, error) {
	c.mu.Lock()
	c.pendings.Add(1)
	defer c.pendings.Done()
	c.mu.Unlock()

	// create shard group if not exists.
	g, err := c.loadOrCreateShardGroup(ctx, item.TargetTable)
	if err != nil {
		return nil, optimism.ConflictError, err
	}

	ddls, conflictStage, deleted, err := g.handle(ctx, item)
	if err != nil {
		return ddls, conflictStage, err
	}

	// if all source table is deleted, we should remove the shard group.
	if deleted {
		c.removeShardGroup(item.TargetTable)
	}

	// handle table level ddl
	switch item.Type {
	case metadata.CreateTable:
		c.mu.Lock()
		tables, ok := c.tables[item.TargetTable]
		if !ok {
			c.tables[item.TargetTable] = make(map[metadata.SourceTable]struct{}, 0)
			tables = c.tables[item.TargetTable]
		}
		tables[item.SourceTable] = struct{}{}
		c.mu.Unlock()
	case metadata.DropTable:
		c.mu.Lock()
		delete(c.tables[item.TargetTable], item.SourceTable)
		if len(c.tables[item.TargetTable]) == 0 {
			delete(c.tables, item.TargetTable)
		}
		c.mu.Unlock()
	}
	return ddls, conflictStage, err
}

// ShowDDLLocks show ddl locks.
func (c *DDLCoordinator) ShowDDLLocks(ctx context.Context) ShowDDLLocksResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	ddlLocks := make(map[metadata.TargetTable]DDLLock)
	for targetTable, g := range c.shardGroups {
		tbs := g.showTables()
		ddlLocks[targetTable] = DDLLock{TableStmts: tbs}
	}
	return ShowDDLLocksResponse{Locks: ddlLocks}
}

type shardGroup struct {
	mu               sync.Mutex
	normalTables     map[metadata.SourceTable]string
	conflictTables   map[metadata.SourceTable]string
	tableAgent       TableAgent
	dropColumnsStore *metadata.DropColumnsStore
	deleted          bool
}

func newShardGroup(ctx context.Context, id frameModel.MasterID, cfg *config.JobCfg, targetTable metadata.TargetTable, sourceTables map[metadata.SourceTable]struct{}, kvClient metaModel.KVClient, tableAgent TableAgent) (*shardGroup, error) {
	g := &shardGroup{
		tableAgent:       tableAgent,
		normalTables:     make(map[metadata.SourceTable]string),
		conflictTables:   make(map[metadata.SourceTable]string),
		dropColumnsStore: metadata.NewDropColumnsStore(kvClient, targetTable),
	}
	for sourceTable := range sourceTables {
		stmt, err := g.tableAgent.FetchTableStmt(ctx, id, cfg, sourceTable)
		// NOTE: There are cases where the source table exists but the table info does not,
		// such as a table is created upstream when the coordinator starts, but the dm-worker is not yet synchronized
		// we skip these tables in handleDDL.
		// TODO: better error handling
		if err != nil && !strings.HasPrefix(err.Error(), "table info not found") {
			return nil, err
		}
		g.normalTables[sourceTable] = stmt
	}
	return g, nil
}

func (g *shardGroup) handle(ctx context.Context, item *metadata.DDLItem) ([]string, optimism.ConflictStage, bool, error) {
	// each group only processes one item at a time
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.deleted {
		return nil, optimism.ConflictError, false, errors.Errorf("shard group for target table %v is deleted", item.TargetTable)
	}

	var (
		ddls          []string
		needDeleted   bool
		err           error
		conflictStage optimism.ConflictStage = optimism.ConflictNone
	)

	switch item.Type {
	case metadata.CreateTable:
		g.handleCreateTable(ctx, item)
		ddls = append(ddls, item.DDLs...)
	case metadata.DropTable:
		needDeleted = g.handleDropTable(ctx, item)
		ddls = append(ddls, item.DDLs...)
	case metadata.OtherDDL:
		ddls, conflictStage, err = g.handleDDLs(ctx, item)
	default:
		return nil, optimism.ConflictError, false, errors.Errorf("unknown ddl type %v", item.Type)
	}

	if g.isResolved() {
		if err := g.clear(ctx); err != nil {
			return nil, optimism.ConflictError, false, err
		}
		needDeleted = true
	}
	return ddls, conflictStage, needDeleted, err
}

// handleCreateTable handles create table ddl.
// add new source table to shard group.
func (g *shardGroup) handleCreateTable(ctx context.Context, item *metadata.DDLItem) {
	stmt, ok := g.normalTables[item.SourceTable]
	if ok && stmt != "" {
		log.L().Warn("create table already exists", zap.Any("source table", item.SourceTable))
	}

	g.normalTables[item.SourceTable] = item.Tables[0]
}

// handleDropTable handles drop table ddl.
// remove source table from shard group.
// mark shard group as deleted if all source tables are deleted.
func (g *shardGroup) handleDropTable(ctx context.Context, item *metadata.DDLItem) bool {
	_, ok := g.normalTables[item.SourceTable]
	if !ok {
		log.L().Warn("drop table does not exist", zap.Any("source table", item.SourceTable))
		return false
	}

	delete(g.normalTables, item.SourceTable)
	delete(g.conflictTables, item.SourceTable)
	if len(g.normalTables) == 0 {
		g.deleted = true
		return true
	}
	return false
}

func (g *shardGroup) checkAddDropColumn(ctx context.Context, sourceTable metadata.SourceTable, ddl string, prevTableStmt, postTableStmt string, newDropColumns []string) (string, error) {
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
		} else if len(col) > 0 && (g.isDroppedColumn(ctx, col, sourceTable) || contains(newDropColumns, col)) {
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
			if col, err := g.checkAddDropColumn(ctx, item.SourceTable, ddl, prevTableStmt, postTableStmt, dropCols); err != nil {
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
		if err := g.dropColumnsStore.AddDropColumns(ctx, dropCols, item.SourceTable); err != nil {
			return nil, optimism.ConflictError, err
		}
	}

	return newDDLs, optimism.ConflictNone, nil
}

func (g *shardGroup) handleDDL(sourceTable metadata.SourceTable, prevTableStmt, postTableStmt string) (bool, optimism.ConflictStage) {
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

func (g *shardGroup) isDroppedColumn(ctx context.Context, col string, sourceTable metadata.SourceTable) bool {
	state, err := g.dropColumnsStore.Get(ctx)
	if err != nil {
		return false
	}
	dropColumns := state.(*metadata.DropColumns)
	if tbs, ok := dropColumns.Cols[col]; ok {
		if _, ok := tbs[sourceTable]; ok {
			return true
		}
	}
	return false
}

func (g *shardGroup) isResolved() bool {
	if len(g.conflictTables) != 0 {
		return false
	}

	var previousTable string
	for _, tbStmt := range g.normalTables {
		if tbStmt == "" {
			continue
		}
		if previousTable == "" {
			previousTable = tbStmt
			continue
		}
		lhs := genCmpTable(previousTable)
		rhs := genCmpTable(tbStmt)
		if cmp, err := lhs.Compare(rhs); err != nil || cmp != 0 {
			return false
		}
		previousTable = tbStmt
	}
	return true
}

func (g *shardGroup) clear(ctx context.Context) error {
	return g.dropColumnsStore.Delete(ctx)
}

func (g *shardGroup) showTables() map[metadata.SourceTable]TableStmt {
	g.mu.Lock()
	defer g.mu.Unlock()
	tables := make(map[metadata.SourceTable]TableStmt, 0)
	for sourceTable, stmt := range g.normalTables {
		tables[sourceTable] = TableStmt{
			Current: stmt,
			Pending: g.conflictTables[sourceTable],
		}
	}
	return tables
}

func genCmpTable(createStmt string) schemacmp.Table {
	p, _ := utils.GetParserFromSQLModeStr(mysql.DefaultSQLMode)
	stmtNode, _ := p.ParseOneStmt(createStmt, "", "")
	ti, _ := ddl.BuildTableInfoFromAST(stmtNode.(*ast.CreateTableStmt))
	ti.State = model.StatePublic
	return schemacmp.Encode(ti)
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
