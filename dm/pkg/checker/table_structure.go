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
	"database/sql"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tidb/util/schemacmp"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

const (
	// AutoIncrementKeyChecking is an identification for auto increment key checking.
	AutoIncrementKeyChecking = "auto-increment key checking"
)

type checkItem struct {
	table    *filter.Table
	sourceID string
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
	dbs         map[string]*sql.DB
	tableMap    map[string][]*filter.Table // sourceID => {[table1, table2, ...]}
	reMu        sync.Mutex
	inCh        chan *checkItem
	optCh       chan *incompatibilityOption
	wg          sync.WaitGroup
	dumpThreads int
}

// NewTablesChecker returns a RealChecker.
func NewTablesChecker(dbs map[string]*sql.DB, tableMap map[string][]*filter.Table, dumpThreads int) RealChecker {
	if dumpThreads == 0 {
		dumpThreads = 1
	}
	c := &TablesChecker{
		dbs:         dbs,
		tableMap:    tableMap,
		dumpThreads: dumpThreads,
	}
	log.L().Logger.Debug("check table structure", zap.Int("channel pool size", dumpThreads))
	c.inCh = make(chan *checkItem, dumpThreads)
	c.optCh = make(chan *incompatibilityOption, dumpThreads)
	return c
}

// Check implements RealChecker interface.
func (c *TablesChecker) Check(ctx context.Context) *Result {
	r := &Result{
		Name:  c.Name(),
		Desc:  "check compatibility of table structure",
		State: StateSuccess,
	}

	startTime := time.Now()
	concurrency, err := getConcurrency(ctx, c.tableMap, c.dbs, c.dumpThreads)
	if err != nil {
		markCheckError(r, err)
		return r
	}
	eg, checkCtx := errgroup.WithContext(ctx)
	for i := 0; i < concurrency; i++ {
		eg.Go(func() error {
			return c.checkTable(checkCtx)
		})
	}
	// start consuming results before dispatching
	// or the dispatching thread could be blocked when
	// the output channel is full.
	c.wg.Add(1)
	go c.handleOpts(ctx, r)
	dispatchTableItem(checkCtx, c.tableMap, c.inCh)
	if err := eg.Wait(); err != nil {
		c.reMu.Lock()
		markCheckError(r, err)
		c.reMu.Unlock()
	}
	close(c.optCh)
	c.wg.Wait()

	log.L().Logger.Info("check table structure over", zap.Duration("spend time", time.Since(startTime)))
	return r
}

// Name implements RealChecker interface.
func (c *TablesChecker) Name() string {
	return "table structure compatibility check"
}

func (c *TablesChecker) handleOpts(ctx context.Context, r *Result) {
	defer c.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case opt, ok := <-c.optCh:
			if !ok {
				return
			}
			tableMsg := "table " + opt.tableID + " "
			c.reMu.Lock()
			switch opt.state {
			case StateWarning:
				if r.State != StateFailure {
					r.State = StateWarning
				}
				e := NewError(tableMsg + opt.errMessage)
				e.Severity = StateWarning
				e.Instruction = opt.instruction
				r.Errors = append(r.Errors, e)
			case StateFailure:
				r.State = StateFailure
				e := NewError(tableMsg + opt.errMessage)
				e.Instruction = opt.instruction
				r.Errors = append(r.Errors, e)
			}
			c.reMu.Unlock()
		}
	}
}

func (c *TablesChecker) checkTable(ctx context.Context) error {
	var (
		sourceID string
		p        *parser.Parser
		err      error
	)
	for {
		select {
		case <-ctx.Done():
			return context.Canceled
		case checkItem, ok := <-c.inCh:
			if !ok {
				return nil
			}
			table := checkItem.table
			log.L().Logger.Debug("checking table", zap.String("db", table.Schema), zap.String("table", table.Name))
			if len(sourceID) == 0 || sourceID != checkItem.sourceID {
				sourceID = checkItem.sourceID
				p, err = dbutil.GetParserForDB(ctx, c.dbs[sourceID])
				if err != nil {
					return err
				}
			}
			db := c.dbs[checkItem.sourceID]
			statement, err := dbutil.GetCreateTableSQL(ctx, db, table.Schema, table.Name)
			if err != nil {
				// continue if table was deleted when checking
				if isMySQLError(err, mysql.ErrNoSuchTable) {
					continue
				}
				return err
			}

			ctStmt, err := getCreateTableStmt(p, statement)
			if err != nil {
				return err
			}
			opts := c.checkAST(ctStmt)
			for _, opt := range opts {
				opt.tableID = table.String()
				c.optCh <- opt
			}
			log.L().Logger.Debug("finish checking table", zap.String("db", table.Schema), zap.String("table", table.Name))
		}
	}
}

func (c *TablesChecker) checkAST(st *ast.CreateTableStmt) []*incompatibilityOption {
	var options []*incompatibilityOption

	// check columns
	for _, def := range st.Cols {
		option := c.checkColumnDef(def)
		if option != nil {
			options = append(options, option)
		}
	}
	// check constrains
	for _, cst := range st.Constraints {
		option := c.checkConstraint(cst)
		if option != nil {
			options = append(options, option)
		}
	}
	// check primary/unique key
	hasUnique := false
	for _, cst := range st.Constraints {
		if c.checkUnique(cst) {
			hasUnique = true
			break
		}
	}
	if !hasUnique {
		options = append(options, &incompatibilityOption{
			state:       StateFailure,
			instruction: "please set primary/unique key for the table",
			errMessage:  "primary/unique key does not exist",
		})
	}

	// check options
	// TODO: in fact, this doesn't work
	// unsupported character report an error in `ParseOneStmt`
	for _, opt := range st.Options {
		option := c.checkTableOption(opt)
		if option != nil {
			options = append(options, option)
		}
	}
	return options
}

func (c *TablesChecker) checkColumnDef(def *ast.ColumnDef) *incompatibilityOption {
	return nil
}

func (c *TablesChecker) checkConstraint(cst *ast.Constraint) *incompatibilityOption {
	if cst.Tp == ast.ConstraintForeignKey {
		return &incompatibilityOption{
			state:       StateWarning,
			instruction: "please ref document: https://docs.pingcap.com/tidb/stable/mysql-compatibility#unsupported-features",
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

func (c *TablesChecker) checkTableOption(opt *ast.TableOption) *incompatibilityOption {
	if opt.Tp == ast.TableOptionCharset {
		// Check charset
		cs := strings.ToLower(opt.StrValue)
		if cs != "binary" && !charset.ValidCharsetAndCollation(cs, "") {
			return &incompatibilityOption{
				state:       StateFailure,
				instruction: "https://docs.pingcap.com/tidb/stable/mysql-compatibility#unsupported-features",
				errMessage:  fmt.Sprintf("unsupport charset %s", opt.StrValue),
			}
		}
	}
	return nil
}

// ShardingTablesChecker checks consistency of table structures of one sharding group
// * check whether they have same column list
// * check whether they have auto_increment key.
type ShardingTablesChecker struct {
	targetTableID                string
	dbs                          map[string]*sql.DB
	tableMap                     map[string][]*filter.Table // sourceID => {[table1, table2, ...]}
	checkAutoIncrementPrimaryKey bool
	firstCreateTableStmtNode     *ast.CreateTableStmt
	firstTable                   *filter.Table
	firstSourceID                string
	inCh                         chan *checkItem
	reMu                         sync.Mutex
	dumpThreads                  int
}

// NewShardingTablesChecker returns a RealChecker.
func NewShardingTablesChecker(targetTableID string, dbs map[string]*sql.DB, tableMap map[string][]*filter.Table, checkAutoIncrementPrimaryKey bool, dumpThreads int) RealChecker {
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

	p, err := dbutil.GetParserForDB(ctx, db)
	if err != nil {
		r.Extra = fmt.Sprintf("fail to get parser for sourceID %s on sharding %s", c.firstSourceID, c.targetTableID)
		markCheckError(r, err)
		return r
	}
	r.Extra = fmt.Sprintf("sourceID %s on sharding %s", c.firstSourceID, c.targetTableID)
	statement, err := dbutil.GetCreateTableSQL(ctx, db, c.firstTable.Schema, c.firstTable.Name)
	if err != nil {
		markCheckError(r, err)
		return r
	}

	c.firstCreateTableStmtNode, err = getCreateTableStmt(p, statement)
	if err != nil {
		markCheckError(r, err)
		return r
	}

	concurrency, err := getConcurrency(ctx, c.tableMap, c.dbs, c.dumpThreads)
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
			table := checkItem.table
			if len(sourceID) == 0 || sourceID != checkItem.sourceID {
				sourceID = checkItem.sourceID
				p, err = dbutil.GetParserForDB(ctx, c.dbs[sourceID])
				if err != nil {
					c.reMu.Lock()
					r.Extra = fmt.Sprintf("fail to get parser for sourceID %s on sharding %s", sourceID, c.targetTableID)
					c.reMu.Unlock()
					return err
				}
			}

			statement, err := dbutil.GetCreateTableSQL(ctx, c.dbs[sourceID], table.Schema, table.Name)
			if err != nil {
				// continue if table was deleted when checking
				if isMySQLError(err, mysql.ErrNoSuchTable) {
					continue
				}
				return err
			}

			ctStmt, err := getCreateTableStmt(p, statement)
			if err != nil {
				return err
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
	dbs           map[string]*sql.DB
	tableMap      map[string][]*filter.Table // sourceID => [table1, table2, ...]
	reMu          sync.Mutex
	joinedMu      sync.Mutex
	inCh          chan *checkItem
	dumpThreads   int
	joined        *schemacmp.Table
}

// NewOptimisticShardingTablesChecker returns a RealChecker.
func NewOptimisticShardingTablesChecker(targetTableID string, dbs map[string]*sql.DB, tableMap map[string][]*filter.Table, dumpThreads int) RealChecker {
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
	concurrency, err := getConcurrency(ctx, c.tableMap, c.dbs, c.dumpThreads)
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
			table := checkItem.table
			if len(sourceID) == 0 || sourceID != checkItem.sourceID {
				sourceID = checkItem.sourceID
				p, err = dbutil.GetParserForDB(ctx, c.dbs[sourceID])
				if err != nil {
					c.reMu.Lock()
					r.Extra = fmt.Sprintf("fail to get parser for sourceID %s on sharding %s", sourceID, c.targetTableID)
					c.reMu.Unlock()
					return err
				}
			}

			statement, err := dbutil.GetCreateTableSQL(ctx, c.dbs[sourceID], table.Schema, table.Name)
			if err != nil {
				// continue if table was deleted when checking
				if isMySQLError(err, mysql.ErrNoSuchTable) {
					continue
				}
				return err
			}

			ctStmt, err := getCreateTableStmt(p, statement)
			if err != nil {
				return err
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

func dispatchTableItem(ctx context.Context, tableMap map[string][]*filter.Table, inCh chan *checkItem) {
	for sourceID, tables := range tableMap {
		for _, table := range tables {
			select {
			case <-ctx.Done():
				log.L().Logger.Warn("ctx canceled before input tables completely")
				return
			case inCh <- &checkItem{table, sourceID}:
			}
		}
	}
	close(inCh)
}

func getConcurrency(ctx context.Context, tableMap map[string][]*filter.Table, dbs map[string]*sql.DB, dumpThreads int) (int, error) {
	concurrency := dumpThreads
	for sourceID := range tableMap {
		db, ok := dbs[sourceID]
		if !ok {
			return 0, errors.NotFoundf("client for sourceID %s", sourceID)
		}
		maxConnections, err := utils.GetMaxConnections(ctx, db)
		if err != nil {
			return 0, err
		}
		concurrency = int(math.Min(float64(concurrency), float64((maxConnections+1)/2)))
	}
	return concurrency, nil
}
