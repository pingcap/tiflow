// Copyright 2021 PingCAP, Inc.
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

package checker

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/schemacmp"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

const (
	// AutoIncrementKeyChecking is an identification for auto increment key checking.
	AutoIncrementKeyChecking = "auto-increment key checking"
)

type checkItem struct {
	upstreamTable   filter.Table
	downstreamTable filter.Table
	sourceID        string
}

// hold information of incompatibility option.
type incompatibilityOption struct {
	state       State
	tableID     string
	instruction string
	errMessage  string
}

// String returns raw text of this incompatibility option.
func (o *incompatibilityOption) String() string {
	var text bytes.Buffer

	if len(o.errMessage) > 0 {
		fmt.Fprintf(&text, "information: %s\n", o.errMessage)
	}

	if len(o.instruction) > 0 {
		fmt.Fprintf(&text, "instruction: %s\n", o.instruction)
	}

	return text.String()
}

// TablesChecker checks compatibility of table structures, there are differences between MySQL and TiDB.
// In generally we need to check definitions of columns, constraints and table options.
// Because of the early TiDB engineering design, we did not have a complete list of check items, which are all based on experience now.
type TablesChecker struct {
	upstreamDBs  map[string]*conn.BaseDB
	downstreamDB *conn.BaseDB
	// sourceID -> downstream table -> upstream tables
	tableMap map[string]map[filter.Table][]filter.Table
	// downstream table -> extended column names
	extendedColumnPerTable map[filter.Table][]string
	dumpThreads            int
	// a simple cache for downstream table structure
	// filter.Table -> *ast.CreateTableStmt
	// if the value is nil, it means the downstream table is not created yet
	downstreamTables sync.Map
}

// NewTablesChecker returns a RealChecker.
func NewTablesChecker(
	upstreamDBs map[string]*conn.BaseDB,
	downstreamDB *conn.BaseDB,
	tableMap map[string]map[filter.Table][]filter.Table,
	extendedColumnPerTable map[filter.Table][]string,
	dumpThreads int,
) RealChecker {
	if dumpThreads == 0 {
		dumpThreads = 1
	}
	c := &TablesChecker{
		upstreamDBs:            upstreamDBs,
		downstreamDB:           downstreamDB,
		tableMap:               tableMap,
		extendedColumnPerTable: extendedColumnPerTable,
		dumpThreads:            dumpThreads,
	}
	log.L().Logger.Debug("check table structure", zap.Int("channel pool size", dumpThreads))
	return c
}

type tablesCheckerWorker struct {
	c                *TablesChecker
	downstreamParser *parser.Parser

	lastSourceID   string
	upstreamParser *parser.Parser
}

func (w *tablesCheckerWorker) handle(ctx context.Context, checkItem *checkItem) ([]*incompatibilityOption, error) {
	var (
		err   error
		ret   = make([]*incompatibilityOption, 0, 1)
		table = checkItem.upstreamTable
	)
	log.L().Logger.Debug("checking table", zap.String("db", table.Schema), zap.String("table", table.Name))
	if w.lastSourceID == "" || w.lastSourceID != checkItem.sourceID {
		w.lastSourceID = checkItem.sourceID
		w.upstreamParser, err = dbutil.GetParserForDB(ctx, w.c.upstreamDBs[w.lastSourceID].DB)
		if err != nil {
			return nil, err
		}
	}
	db := w.c.upstreamDBs[checkItem.sourceID].DB
	upstreamSQL, err := dbutil.GetCreateTableSQL(ctx, db, table.Schema, table.Name)
	if err != nil {
		// continue if table was deleted when checking
		if isMySQLError(err, mysql.ErrNoSuchTable) {
			return nil, nil
		}
		return nil, err
	}

	upstreamStmt, err := getCreateTableStmt(w.upstreamParser, upstreamSQL)
	if err != nil {
		opt := &incompatibilityOption{
			state:      StateWarning,
			tableID:    dbutil.TableName(table.Schema, table.Name),
			errMessage: err.Error(),
		}
		ret = append(ret, opt)
		// nolint:nilerr
		return ret, nil
	}

	downstreamStmt, ok := w.c.downstreamTables.Load(checkItem.downstreamTable)
	if !ok {
		sql, err2 := dbutil.GetCreateTableSQL(
			ctx,
			w.c.downstreamDB.DB,
			checkItem.downstreamTable.Schema,
			checkItem.downstreamTable.Name,
		)
		if err2 != nil && !isMySQLError(err2, mysql.ErrNoSuchTable) {
			return nil, err2
		}
		if sql == "" {
			downstreamStmt = (*ast.CreateTableStmt)(nil)
		} else {
			downstreamStmt, err2 = getCreateTableStmt(w.downstreamParser, sql)
			if err2 != nil {
				opt := &incompatibilityOption{
					state:      StateWarning,
					tableID:    dbutil.TableName(table.Schema, table.Name),
					errMessage: err2.Error(),
				}
				ret = append(ret, opt)
			}
		}
		w.c.downstreamTables.Store(checkItem.downstreamTable, downstreamStmt)
	}

	downstreamTable := filter.Table{
		Schema: checkItem.downstreamTable.Schema,
		Name:   checkItem.downstreamTable.Name,
	}
	opts := w.c.checkAST(
		upstreamStmt,
		downstreamStmt.(*ast.CreateTableStmt),
		w.c.extendedColumnPerTable[downstreamTable],
	)
	for _, opt := range opts {
		opt.tableID = table.String()
		ret = append(ret, opt)
	}
	log.L().Logger.Debug("finish checking table", zap.String("db", table.Schema), zap.String("table", table.Name))
	return ret, nil
}

// Check implements RealChecker interface.
func (c *TablesChecker) Check(ctx context.Context) *Result {
	r := &Result{
		Name:  c.Name(),
		Desc:  "check compatibility of table structure",
		State: StateSuccess,
	}

	startTime := time.Now()
	sourceIDs := maps.Keys(c.tableMap)
	concurrency, err := GetConcurrency(ctx, sourceIDs, c.upstreamDBs, c.dumpThreads)
	if err != nil {
		markCheckError(r, err)
		return r
	}

	everyOptHandler, finalHandler := c.handleOpts(r)

	pool := NewWorkerPoolWithContext[*checkItem, []*incompatibilityOption](
		ctx, everyOptHandler,
	)

	for i := 0; i < concurrency; i++ {
		worker := &tablesCheckerWorker{c: c}
		worker.downstreamParser, err = dbutil.GetParserForDB(ctx, c.downstreamDB.DB)
		if err != nil {
			markCheckError(r, err)
			return r
		}
		pool.Go(worker.handle)
	}

	dispatchTableItemWithDownstreamTable(c.tableMap, pool)

	if err := pool.Wait(); err != nil {
		markCheckError(r, err)
		return r
	}
	finalHandler()

	log.L().Logger.Info("check table structure over", zap.Duration("spend time", time.Since(startTime)))
	return r
}

// Name implements RealChecker interface.
func (c *TablesChecker) Name() string {
	return "table structure compatibility check"
}

// handleOpts returns a handler that should be called on every
// incompatibilityOption, and a second handler that should be called once after
// all incompatibilityOption.
func (c *TablesChecker) handleOpts(r *Result) (func(options []*incompatibilityOption), func()) {
	// extract same instruction from Errors to Result.Instruction
	resultInstructions := map[string]struct{}{}

	return func(options []*incompatibilityOption) {
			for _, opt := range options {
				tableMsg := "table " + opt.tableID + " "
				switch opt.state {
				case StateWarning:
					if r.State != StateFailure {
						r.State = StateWarning
					}
					e := NewError(tableMsg + opt.errMessage)
					e.Severity = StateWarning
					if _, ok := resultInstructions[opt.instruction]; !ok && opt.instruction != "" {
						resultInstructions[opt.instruction] = struct{}{}
					}
					r.Errors = append(r.Errors, e)
				case StateFailure:
					r.State = StateFailure
					e := NewError(tableMsg + opt.errMessage)
					if _, ok := resultInstructions[opt.instruction]; !ok && opt.instruction != "" {
						resultInstructions[opt.instruction] = struct{}{}
					}
					r.Errors = append(r.Errors, e)
				}
			}
		}, func() {
			instructionSlice := make([]string, 0, len(resultInstructions))
			for k := range resultInstructions {
				instructionSlice = append(instructionSlice, k)
			}
			r.Instruction += strings.Join(instructionSlice, "; ")
		}
}

func (c *TablesChecker) checkAST(
	upstreamStmt *ast.CreateTableStmt,
	downstreamStmt *ast.CreateTableStmt,
	extendedCols []string,
) []*incompatibilityOption {
	var options []*incompatibilityOption

	// check columns
	for _, def := range upstreamStmt.Cols {
		option := c.checkColumnDef(def)
		if option != nil {
			options = append(options, option)
		}
	}
	// check constrains
	for _, cst := range upstreamStmt.Constraints {
		option := c.checkConstraint(cst)
		if option != nil {
			options = append(options, option)
		}
	}
	// check primary/unique key
	hasUnique := false
	for _, cst := range upstreamStmt.Constraints {
		if c.checkUnique(cst) {
			hasUnique = true
			break
		}
	}
	if !hasUnique {
		options = append(options, &incompatibilityOption{
			state:       StateWarning,
			instruction: "You need to set primary/unique keys for the table. Otherwise replication efficiency might become very low and exactly-once replication cannot be guaranteed.",
			errMessage:  "primary/unique key does not exist",
		})
	}

	if downstreamStmt == nil {
		if len(extendedCols) > 0 {
			options = append(options, &incompatibilityOption{
				state:       StateFailure,
				instruction: "You need to create a table with extended columns before replication.",
				errMessage:  fmt.Sprintf("upstream table %s who has extended columns %v does not exist in downstream table", upstreamStmt.Table.Name, extendedCols),
			})
		}
		return options
	}

	options = append(options, c.checkTableStructurePair(upstreamStmt, downstreamStmt, extendedCols)...)
	return options
}

func (c *TablesChecker) checkColumnDef(def *ast.ColumnDef) *incompatibilityOption {
	return nil
}

func (c *TablesChecker) checkConstraint(cst *ast.Constraint) *incompatibilityOption {
	if cst.Tp == ast.ConstraintForeignKey {
		return &incompatibilityOption{
			state:       StateWarning,
			instruction: "TiDB does not support foreign key constraints. See the document: https://docs.pingcap.com/tidb/stable/mysql-compatibility#unsupported-features",
			errMessage:  fmt.Sprintf("Foreign Key %s is parsed but ignored by TiDB.", cst.Name),
		}
	}

	return nil
}

func (c *TablesChecker) checkUnique(cst *ast.Constraint) bool {
	switch cst.Tp {
	case ast.ConstraintPrimaryKey, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		return true
	}
	return false
}

func (c *TablesChecker) checkTableStructurePair(
	upstream *ast.CreateTableStmt,
	downstream *ast.CreateTableStmt,
	extendedCols []string,
) []*incompatibilityOption {
	//nolint: prealloc
	var options []*incompatibilityOption

	// check charset of upstream/downstream tables
	upstreamCharset := getCharset(upstream)
	downstreamCharset := getCharset(downstream)
	if upstreamCharset != "" && downstreamCharset != "" &&
		!strings.EqualFold(upstreamCharset, downstreamCharset) &&
		!strings.EqualFold(downstreamCharset, mysql.UTF8MB4Charset) {
		options = append(options, &incompatibilityOption{
			state:       StateWarning,
			instruction: "Ensure that you use the same charsets for both upstream and downstream databases. Different charsets might cause data inconsistency.",
			errMessage: fmt.Sprintf("charset is not same, upstream: (%s %s), downstream: (%s %s)",
				upstream.Table.Name.O, upstreamCharset,
				downstream.Table.Name.O, downstreamCharset),
		})
	}

	// check collation
	upstreamCollation := getCollation(upstream)
	downstreamCollation := getCollation(downstream)
	if upstreamCollation != "" && downstreamCollation != "" &&
		!strings.EqualFold(upstreamCollation, downstreamCollation) {
		options = append(options, &incompatibilityOption{
			state:       StateWarning,
			instruction: "Ensure that you use the same collations for both upstream and downstream databases. Otherwise the query results from the two databases might be inconsistent.",
			errMessage: fmt.Sprintf("collation is not same, upstream: (%s %s), downstream: (%s %s)",
				upstream.Table.Name.O, upstreamCollation,
				downstream.Table.Name.O, downstreamCollation),
		})
	}

	// check PK/UK
	upstreamPKUK := getPKAndUK(upstream)
	downstreamPKUK := getPKAndUK(downstream)
	// the number of PK/UK should be small, we use a simple but slow algorithm for now
	for idxNameUp, s := range upstreamPKUK {
		for idxNameDown, s2 := range downstreamPKUK {
			if stringSetEqual(s, s2) {
				delete(upstreamPKUK, idxNameUp)
				delete(downstreamPKUK, idxNameDown)
				break
			}
		}
	}
	for idxName, cols := range upstreamPKUK {
		options = append(options, &incompatibilityOption{
			state:       StateWarning,
			instruction: "Ensure that you use the same index columns for both upstream and downstream databases. Otherwise the migration job might fail or data inconsistency might occur.",
			errMessage: fmt.Sprintf("upstream has more PK or NOT NULL UK than downstream, index name: %s, columns: %v",
				idxName, utils.SetToSlice(cols)),
		})
	}
	for idxName, cols := range downstreamPKUK {
		options = append(options, &incompatibilityOption{
			state:       StateWarning,
			instruction: "Ensure that you use the same index columns for both upstream and downstream databases. Otherwise the migration job might fail or data inconsistency might occur.",
			errMessage: fmt.Sprintf("downstream has more PK or NOT NULL UK than upstream, table name: %s, index name: %s, columns: %v",
				downstream.Table.Name.O, idxName, utils.SetToSlice(cols)),
		})
	}

	// check columns
	upstreamCols := getColumnsAndIgnorable(upstream)
	downstreamCols := getColumnsAndIgnorable(downstream)
	for col := range upstreamCols {
		if _, ok := downstreamCols[col]; ok {
			delete(upstreamCols, col)
			delete(downstreamCols, col)
		}
	}

	upstreamDupCols := make([]string, 0, len(extendedCols))
	downstreamMissingCols := make([]string, 0, len(extendedCols))
	for _, col := range extendedCols {
		if _, ok := upstreamCols[col]; ok {
			upstreamDupCols = append(upstreamDupCols, col)
		}
		if _, ok := downstreamCols[col]; !ok {
			downstreamMissingCols = append(downstreamMissingCols, col)
		}
		delete(upstreamCols, col)
	}
	if len(upstreamDupCols) > 0 {
		options = append(options, &incompatibilityOption{
			state:       StateFailure,
			instruction: "DM automatically fills the values of extended columns. You need to remove these columns or change configuration.",
			errMessage:  fmt.Sprintf("upstream table must not contain extended column %v", upstreamDupCols),
		})
	}
	if len(downstreamMissingCols) > 0 {
		options = append(options, &incompatibilityOption{
			state:       StateFailure,
			instruction: "You need to manually add extended columns to the downstream table.",
			errMessage:  fmt.Sprintf("downstream table must contain extended columns %v", downstreamMissingCols),
		})
	}
	if len(upstreamDupCols) > 0 || len(downstreamMissingCols) > 0 {
		return options
	}

	if len(upstreamCols) > 0 {
		options = append(options, &incompatibilityOption{
			state:       StateWarning,
			instruction: "Ensure that the column numbers are the same between upstream and downstream databases. Otherwise the migration job may fail.",
			errMessage: fmt.Sprintf("upstream has more columns than downstream, columns: %v",
				maps.Keys(upstreamCols)),
		})
	}
	for col, ignorable := range downstreamCols {
		if ignorable {
			delete(downstreamCols, col)
		}
	}
	if len(downstreamCols) > 0 {
		options = append(options, &incompatibilityOption{
			state:       StateWarning,
			instruction: "Ensure that the column numbers are the same between upstream and downstream databases. Otherwise the migration job may fail.",
			errMessage: fmt.Sprintf("downstream has more columns than upstream that require values to insert records, table name: %s, columns: %v",
				downstream.Table.Name.O, maps.Keys(downstreamCols)),
		})
	}

	return options
}

// ShardingTablesChecker checks consistency of table structures of one sharding group
// * check whether they have same column list
// * check whether they have auto_increment key.
type ShardingTablesChecker struct {
	targetTableID                string
	dbs                          map[string]*conn.BaseDB
	tableMap                     map[string][]filter.Table // sourceID => {[table1, table2, ...]}
	checkAutoIncrementPrimaryKey bool
	firstCreateTableStmtNode     *ast.CreateTableStmt
	firstTable                   filter.Table
	firstSourceID                string
	inCh                         chan *checkItem
	reMu                         sync.Mutex
	dumpThreads                  int
}

// NewShardingTablesChecker returns a RealChecker.
func NewShardingTablesChecker(
	targetTableID string,
	dbs map[string]*conn.BaseDB,
	tableMap map[string][]filter.Table,
	checkAutoIncrementPrimaryKey bool,
	dumpThreads int,
) RealChecker {
	if dumpThreads == 0 {
		dumpThreads = 1
	}
	c := &ShardingTablesChecker{
		targetTableID:                targetTableID,
		dbs:                          dbs,
		tableMap:                     tableMap,
		checkAutoIncrementPrimaryKey: checkAutoIncrementPrimaryKey,
		dumpThreads:                  dumpThreads,
	}
	c.inCh = make(chan *checkItem, dumpThreads)

	return c
}

// Check implements RealChecker interface.
func (c *ShardingTablesChecker) Check(ctx context.Context) *Result {
	r := &Result{
		Name:  c.Name(),
		Desc:  "check consistency of sharding table structures",
		State: StateSuccess,
		Extra: fmt.Sprintf("sharding %s,", c.targetTableID),
	}

	startTime := time.Now()
	log.L().Logger.Info("start to check sharding tables")

	for sourceID, tables := range c.tableMap {
		c.firstSourceID = sourceID
		c.firstTable = tables[0]
		break
	}
	db, ok := c.dbs[c.firstSourceID]
	if !ok {
		markCheckError(r, errors.NotFoundf("client for sourceID %s", c.firstSourceID))
		return r
	}

	p, err := dbutil.GetParserForDB(ctx, db.DB)
	if err != nil {
		r.Extra = fmt.Sprintf("fail to get parser for sourceID %s on sharding %s", c.firstSourceID, c.targetTableID)
		markCheckError(r, err)
		return r
	}
	r.Extra = fmt.Sprintf("sourceID %s on sharding %s", c.firstSourceID, c.targetTableID)
	statement, err := dbutil.GetCreateTableSQL(ctx, db.DB, c.firstTable.Schema, c.firstTable.Name)
	if err != nil {
		markCheckError(r, err)
		return r
	}

	c.firstCreateTableStmtNode, err = getCreateTableStmt(p, statement)
	if err != nil {
		markCheckErrorFromParser(r, err)
		return r
	}

	sourceIDs := maps.Keys(c.tableMap)
	concurrency, err := GetConcurrency(ctx, sourceIDs, c.dbs, c.dumpThreads)
	if err != nil {
		markCheckError(r, err)
		return r
	}
	eg, checkCtx := errgroup.WithContext(ctx)
	for i := 0; i < concurrency; i++ {
		eg.Go(func() error {
			return c.checkShardingTable(checkCtx, r)
		})
	}

	dispatchTableItem(checkCtx, c.tableMap, c.inCh)
	if err := eg.Wait(); err != nil {
		markCheckError(r, err)
	}

	log.L().Logger.Info("check sharding table structure over", zap.Duration("spend time", time.Since(startTime)))
	return r
}

func (c *ShardingTablesChecker) checkShardingTable(ctx context.Context, r *Result) error {
	var (
		sourceID string
		p        *parser.Parser
		err      error
	)
	for {
		select {
		case <-ctx.Done():
			return nil
		case checkItem, ok := <-c.inCh:
			if !ok {
				return nil
			}
			table := checkItem.upstreamTable
			if len(sourceID) == 0 || sourceID != checkItem.sourceID {
				sourceID = checkItem.sourceID
				p, err = dbutil.GetParserForDB(ctx, c.dbs[sourceID].DB)
				if err != nil {
					c.reMu.Lock()
					r.Extra = fmt.Sprintf("fail to get parser for sourceID %s on sharding %s", sourceID, c.targetTableID)
					c.reMu.Unlock()
					return err
				}
			}

			statement, err := dbutil.GetCreateTableSQL(ctx, c.dbs[sourceID].DB, table.Schema, table.Name)
			if err != nil {
				// continue if table was deleted when checking
				if isMySQLError(err, mysql.ErrNoSuchTable) {
					continue
				}
				return err
			}

			ctStmt, err := getCreateTableStmt(p, statement)
			if err != nil {
				c.reMu.Lock()
				markCheckErrorFromParser(r, err)
				c.reMu.Unlock()
				continue
			}

			if has := hasAutoIncrementKey(ctStmt); has {
				c.reMu.Lock()
				if r.State == StateSuccess {
					r.State = StateWarning
					r.Instruction = "If happen conflict, please handle it by yourself. You can refer to https://docs.pingcap.com/tidb-data-migration/stable/shard-merge-best-practices/#handle-conflicts-between-primary-keys-or-unique-indexes-across-multiple-sharded-tables"
					r.Extra = AutoIncrementKeyChecking
				}
				r.Errors = append(r.Errors, NewError("sourceID %s table %v of sharding %s have auto-increment key, please make sure them don't conflict in target table!", sourceID, table, c.targetTableID))
				c.reMu.Unlock()
			}

			if checkErr := c.checkConsistency(ctStmt, table.String(), sourceID); checkErr != nil {
				c.reMu.Lock()
				r.State = StateFailure
				r.Errors = append(r.Errors, checkErr)
				r.Extra = fmt.Sprintf("error on sharding %s", c.targetTableID)
				r.Instruction = "please set same table structure for sharding tables"
				c.reMu.Unlock()
				// shouldn't return error
				// it's feasible to check more sharding tables and
				// able to inform users of as many as possible incompatible tables
			}
		}
	}
}

func hasAutoIncrementKey(stmt *ast.CreateTableStmt) bool {
	for _, col := range stmt.Cols {
		for _, opt := range col.Options {
			if opt.Tp == ast.ColumnOptionAutoIncrement {
				return true
			}
		}
	}
	return false
}

type briefColumnInfo struct {
	name         string
	tp           string
	isUniqueKey  bool
	isPrimaryKey bool
}

func (c *briefColumnInfo) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s %s", c.name, c.tp)
	if c.isPrimaryKey {
		fmt.Fprintln(&buf, " primary key")
	} else if c.isUniqueKey {
		fmt.Fprintln(&buf, " unique key")
	}

	return buf.String()
}

type briefColumnInfos []*briefColumnInfo

func (cs briefColumnInfos) String() string {
	colStrs := make([]string, 0, len(cs))
	for _, col := range cs {
		colStrs = append(colStrs, col.String())
	}

	return strings.Join(colStrs, "\n")
}

func (c *ShardingTablesChecker) checkConsistency(other *ast.CreateTableStmt, otherTable, othersourceID string) *Error {
	selfColumnList := getBriefColumnList(c.firstCreateTableStmtNode)
	otherColumnList := getBriefColumnList(other)

	if len(selfColumnList) != len(otherColumnList) {
		e := NewError("column length mismatch (self: %d vs other: %d)", len(selfColumnList), len(otherColumnList))
		getColumnNames := func(infos briefColumnInfos) []string {
			ret := make([]string, 0, len(infos))
			for _, info := range infos {
				ret = append(ret, info.name)
			}
			return ret
		}
		e.Self = fmt.Sprintf("sourceID %s table %v columns %v", c.firstSourceID, c.firstTable, getColumnNames(selfColumnList))
		e.Other = fmt.Sprintf("sourceID %s table %s columns %v", othersourceID, otherTable, getColumnNames(otherColumnList))
		return e
	}

	for i := range selfColumnList {
		if *selfColumnList[i] != *otherColumnList[i] {
			e := NewError("different column definition")
			e.Self = fmt.Sprintf("sourceID %s table %s column %s", c.firstSourceID, c.firstTable, selfColumnList[i])
			e.Other = fmt.Sprintf("sourceID %s table %s column %s", othersourceID, otherTable, otherColumnList[i])
			return e
		}
	}

	return nil
}

func getBriefColumnList(stmt *ast.CreateTableStmt) briefColumnInfos {
	columnList := make(briefColumnInfos, 0, len(stmt.Cols))

	for _, col := range stmt.Cols {
		bc := &briefColumnInfo{
			name: col.Name.Name.L,
			tp:   col.Tp.String(),
		}

		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionPrimaryKey:
				bc.isPrimaryKey = true
			case ast.ColumnOptionUniqKey:
				bc.isUniqueKey = true
			}
		}

		columnList = append(columnList, bc)
	}

	return columnList
}

// Name implements Checker interface.
func (c *ShardingTablesChecker) Name() string {
	return fmt.Sprintf("sharding table %s consistency checking", c.targetTableID)
}

// OptimisticShardingTablesChecker checks consistency of table structures of one sharding group in optimistic shard.
// * check whether they have compatible column list.
type OptimisticShardingTablesChecker struct {
	targetTableID string
	dbs           map[string]*conn.BaseDB
	tableMap      map[string][]filter.Table // sourceID => [table1, table2, ...]
	reMu          sync.Mutex
	joinedMu      sync.Mutex
	inCh          chan *checkItem
	dumpThreads   int
	joined        *schemacmp.Table
}

// NewOptimisticShardingTablesChecker returns a RealChecker.
func NewOptimisticShardingTablesChecker(
	targetTableID string,
	dbs map[string]*conn.BaseDB,
	tableMap map[string][]filter.Table,
	dumpThreads int,
) RealChecker {
	if dumpThreads == 0 {
		dumpThreads = 1
	}
	c := &OptimisticShardingTablesChecker{
		targetTableID: targetTableID,
		dbs:           dbs,
		tableMap:      tableMap,
		dumpThreads:   dumpThreads,
	}
	c.inCh = make(chan *checkItem, dumpThreads)
	return c
}

// Name implements Checker interface.
func (c *OptimisticShardingTablesChecker) Name() string {
	return fmt.Sprintf("optimistic sharding table %s consistency checking", c.targetTableID)
}

// Check implements RealChecker interface.
func (c *OptimisticShardingTablesChecker) Check(ctx context.Context) *Result {
	r := &Result{
		Name:  c.Name(),
		Desc:  "check consistency of sharding table structures for Optimistic Sharding Merge",
		State: StateSuccess,
		Extra: fmt.Sprintf("sharding %s", c.targetTableID),
	}

	startTime := time.Now()
	sourceIDs := maps.Keys(c.tableMap)
	concurrency, err := GetConcurrency(ctx, sourceIDs, c.dbs, c.dumpThreads)
	if err != nil {
		markCheckError(r, err)
		return r
	}
	eg, checkCtx := errgroup.WithContext(ctx)
	for i := 0; i < concurrency; i++ {
		eg.Go(func() error {
			return c.checkTable(checkCtx, r)
		})
	}

	dispatchTableItem(checkCtx, c.tableMap, c.inCh)
	if err := eg.Wait(); err != nil {
		markCheckError(r, err)
	}

	log.L().Logger.Info("check optimistic sharding table structure over", zap.Duration("spend time", time.Since(startTime)))
	return r
}

func (c *OptimisticShardingTablesChecker) checkTable(ctx context.Context, r *Result) error {
	var (
		sourceID string
		p        *parser.Parser
		err      error
	)
	for {
		select {
		case <-ctx.Done():
			return nil
		case checkItem, ok := <-c.inCh:
			if !ok {
				return nil
			}
			table := checkItem.upstreamTable
			if len(sourceID) == 0 || sourceID != checkItem.sourceID {
				sourceID = checkItem.sourceID
				p, err = dbutil.GetParserForDB(ctx, c.dbs[sourceID].DB)
				if err != nil {
					c.reMu.Lock()
					r.Extra = fmt.Sprintf("fail to get parser for sourceID %s on sharding %s", sourceID, c.targetTableID)
					c.reMu.Unlock()
					return err
				}
			}

			statement, err := dbutil.GetCreateTableSQL(ctx, c.dbs[sourceID].DB, table.Schema, table.Name)
			if err != nil {
				// continue if table was deleted when checking
				if isMySQLError(err, mysql.ErrNoSuchTable) {
					continue
				}
				return err
			}

			ctStmt, err := getCreateTableStmt(p, statement)
			if err != nil {
				c.reMu.Lock()
				markCheckErrorFromParser(r, err)
				c.reMu.Unlock()
				continue
			}

			if has := hasAutoIncrementKey(ctStmt); has {
				c.reMu.Lock()
				if r.State == StateSuccess {
					r.State = StateWarning
					r.Instruction = "If happen conflict, please handle it by yourself. You can refer to https://docs.pingcap.com/tidb-data-migration/stable/shard-merge-best-practices/#handle-conflicts-between-primary-keys-or-unique-indexes-across-multiple-sharded-tables"
					r.Extra = AutoIncrementKeyChecking
				}
				r.Errors = append(r.Errors, NewError("sourceID %s table %v of sharding %s have auto-increment key, please make sure them don't conflict in target table!", sourceID, table, c.targetTableID))
				c.reMu.Unlock()
			}

			ti, err := dbutil.GetTableInfoBySQL(statement, p)
			if err != nil {
				return err
			}
			encodeTi := schemacmp.Encode(ti)
			c.joinedMu.Lock()
			if c.joined == nil {
				c.joined = &encodeTi
				c.joinedMu.Unlock()
				continue
			}
			newJoined, err2 := c.joined.Join(encodeTi)
			if err2 != nil {
				// NOTE: conflict detected.
				c.reMu.Lock()
				r.Extra = fmt.Sprintf("fail to join table info %s with %s", c.joined, encodeTi)
				c.reMu.Unlock()
				c.joinedMu.Unlock()
				return err2
			}
			c.joined = &newJoined
			c.joinedMu.Unlock()
		}
	}
}

func dispatchTableItem(ctx context.Context, tableMap map[string][]filter.Table, inCh chan *checkItem) {
	for sourceID, tables := range tableMap {
		for _, table := range tables {
			select {
			case <-ctx.Done():
				log.L().Logger.Warn("ctx canceled before input tables completely")
				return
			case inCh <- &checkItem{upstreamTable: table, sourceID: sourceID}:
			}
		}
	}
	close(inCh)
}

func dispatchTableItemWithDownstreamTable(
	tableMaps map[string]map[filter.Table][]filter.Table,
	pool *WorkerPool[*checkItem, []*incompatibilityOption],
) {
	for sourceID, tableMap := range tableMaps {
		for downTable, upTables := range tableMap {
			for _, upTable := range upTables {
				ok := pool.PutJob(&checkItem{
					upstreamTable:   upTable,
					downstreamTable: downTable,
					sourceID:        sourceID,
				})
				if !ok {
					return
				}
			}
		}
	}
}

// GetConcurrency gets the concurrency of workers that we can randomly dispatch
// tasks on any sources to any of them, where each task needs a SQL connection.
func GetConcurrency(ctx context.Context, sourceIDs []string, dbs map[string]*conn.BaseDB, dumpThreads int) (int, error) {
	concurrency := dumpThreads
	for _, sourceID := range sourceIDs {
		db, ok := dbs[sourceID]
		if !ok {
			return 0, errors.NotFoundf("SQL connection for sourceID %s", sourceID)
		}
		maxConnections, err := conn.GetMaxConnections(tcontext.NewContext(ctx, log.L()), db)
		if err != nil {
			return 0, err
		}
		concurrency = int(math.Min(float64(concurrency), float64((maxConnections+1)/2)))
	}
	return concurrency, nil
}
