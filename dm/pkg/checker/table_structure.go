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

	"github.com/pingcap/errors"
	column "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

const (
	// AutoIncrementKeyChecking is an identification for auto increment key checking.
	AutoIncrementKeyChecking = "auto-increment key checking"
	maxThreadNum             = 32
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
	dbs      map[string]*sql.DB
	tableMap map[string][]*filter.Table //
}

// NewTablesChecker returns a RealChecker.
func NewTablesChecker(dbs map[string]*sql.DB, tableMap map[string][]*filter.Table) RealChecker {
	c := &TablesChecker{
		dbs:      dbs,
		tableMap: tableMap,
	}
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
	var (
		inCh          = make(chan *checkItem, maxThreadNum)
		checkWg       sync.WaitGroup
		reMu          sync.Mutex
		firstSourceID string
	)

	for sourceID := range c.tableMap {
		firstSourceID = sourceID
		break
	}

	checkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	checkFunc := func() {
		defer checkWg.Done()
		sourceID := firstSourceID
		p, err := dbutil.GetParserForDB(ctx, c.dbs[sourceID])
		if err != nil {
			reMu.Lock()
			markCheckError(r, err)
			reMu.Unlock()
			return
		}
		for {
			select {
			case <-checkCtx.Done():
				return
			case checkItem, ok := <-inCh:
				if !ok {
					return
				}
				table := checkItem.table
				if sourceID != checkItem.sourceID {
					sourceID = checkItem.sourceID
					p, err = dbutil.GetParserForDB(ctx, c.dbs[sourceID])
					if err != nil {
						reMu.Lock()
						markCheckError(r, err)
						reMu.Unlock()
						return
					}
				}
				db := c.dbs[checkItem.sourceID]
				statement, err := dbutil.GetCreateTableSQL(ctx, db, table.Schema, table.Name)
				if err != nil {
					// continue if table was deleted when checking
					if isMySQLError(err, mysql.ErrNoSuchTable) {
						continue
					}
					reMu.Lock()
					markCheckError(r, err)
					reMu.Unlock()
					cancel()
					break
				}

				opts := c.checkCreateSQL(p, statement)
				for _, opt := range opts {
					opt.tableID = table.String()
					tableMsg := "table " + opt.tableID + " "
					reMu.Lock()
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
					reMu.Unlock()
				}
			}
		}
	}

	concurrency := maxThreadNum
	for sourceID := range c.tableMap {
		db, ok := c.dbs[sourceID]
		if !ok {
			markCheckError(r, errors.NotFoundf("client for sourceID %s", sourceID))
			return r
		}
		maxConnections, err := utils.GetMaxConnections(ctx, db)
		if err != nil {
			markCheckError(r, err)
			return r
		}
		concurrency = int(math.Min(float64(concurrency), float64(maxConnections/2)))
	}

	for i := 0; i < concurrency; i++ {
		checkWg.Add(1)
		go checkFunc()
	}

outer:
	for sourceID, tables := range c.tableMap {
		for _, table := range tables {
			select {
			case inCh <- &checkItem{table, sourceID}:
			case <-checkCtx.Done():
				log.L().Logger.Warn("ctx canceled before input tables completely")
				break outer
			}
		}
	}

	close(inCh)
	checkWg.Wait()
	log.L().Logger.Info("check table structure over", zap.String("spend time", time.Since(startTime).String()))
	return r
}

// Name implements RealChecker interface.
func (c *TablesChecker) Name() string {
	return "table structure compatibility check"
}

func (c *TablesChecker) checkCreateSQL(p *parser.Parser, statement string) []*incompatibilityOption {
	stmt, err := p.ParseOneStmt(statement, "", "")
	if err != nil {
		return []*incompatibilityOption{
			{
				state:      StateFailure,
				errMessage: errors.Annotatef(err, "statement: %s", statement).Error(),
			},
		}
	}
	// Analyze ast
	return c.checkAST(stmt)
}

func (c *TablesChecker) checkAST(stmt ast.StmtNode) []*incompatibilityOption {
	st, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		return []*incompatibilityOption{
			{
				state:      StateFailure,
				errMessage: fmt.Sprintf("Expect CreateTableStmt but got %T", stmt),
			},
		}
	}

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
	mapping                      map[string]*column.Mapping
	checkAutoIncrementPrimaryKey bool
}

// NewShardingTablesChecker returns a RealChecker.
func NewShardingTablesChecker(targetTableID string, dbs map[string]*sql.DB, tableMap map[string][]*filter.Table, mapping map[string]*column.Mapping, checkAutoIncrementPrimaryKey bool) RealChecker {
	return &ShardingTablesChecker{
		targetTableID:                targetTableID,
		dbs:                          dbs,
		tableMap:                     tableMap,
		mapping:                      mapping,
		checkAutoIncrementPrimaryKey: checkAutoIncrementPrimaryKey,
	}
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
	var (
		stmtNode      *ast.CreateTableStmt
		firstTable    *filter.Table
		firstSourceID string
		inCh          = make(chan *checkItem, maxThreadNum)
		checkWg       sync.WaitGroup
		reMu          sync.Mutex
	)

	for sourceID, tables := range c.tableMap {
		firstSourceID = sourceID
		firstTable = tables[0]
		break
	}
	db, ok := c.dbs[firstSourceID]
	if !ok {
		markCheckError(r, errors.NotFoundf("client for sourceID %s", firstSourceID))
		return r
	}

	parser2, err := dbutil.GetParserForDB(ctx, db)
	if err != nil {
		r.Extra = fmt.Sprintf("fail to get parser for sourceID %s on sharding %s", firstSourceID, c.targetTableID)
		markCheckError(r, err)
		return r
	}
	r.Extra = fmt.Sprintf("sourceID %s on sharding %s", firstSourceID, c.targetTableID)
	statement, err := dbutil.GetCreateTableSQL(ctx, db, firstTable.Schema, firstTable.Name)
	if err != nil {
		markCheckError(r, err)
		return r
	}

	stmt, err := parser2.ParseOneStmt(statement, "", "")
	if err != nil {
		markCheckError(r, errors.Annotatef(err, "statement %s", statement))
		return r
	}

	stmtNode, ok = stmt.(*ast.CreateTableStmt)
	if !ok {
		markCheckError(r, errors.Errorf("Expect CreateTableStmt but got %T", stmt))
		return r
	}

	checkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	checkFunc := func() {
		var (
			sourceID = firstSourceID
			p        *parser.Parser
			err      error
		)
		defer func() {
			checkWg.Done()
			if err != nil {
				cancel()
				reMu.Lock()
				markCheckError(r, err)
				reMu.Unlock()
			}
		}()
		p, err = dbutil.GetParserForDB(ctx, c.dbs[sourceID])
		if err != nil {
			return
		}
		for {
			select {
			case <-checkCtx.Done():
				return
			case checkItem, ok := <-inCh:
				if !ok {
					return
				}
				table := checkItem.table
				var err2 error
				if sourceID != checkItem.sourceID {
					sourceID = checkItem.sourceID
					p, err2 = dbutil.GetParserForDB(ctx, c.dbs[sourceID])
					if err2 != nil {
						reMu.Lock()
						r.Extra = fmt.Sprintf("fail to get parser for sourceID %s on sharding %s", sourceID, c.targetTableID)
						reMu.Unlock()
						err = err2
						return
					}
				}

				statement, err2 := dbutil.GetCreateTableSQL(ctx, c.dbs[sourceID], table.Schema, table.Name)
				if err2 != nil {
					// continue if table was deleted when checking
					if isMySQLError(err2, mysql.ErrNoSuchTable) {
						continue
					}
					err = err2
					return
				}

				info, err2 := dbutil.GetTableInfoBySQL(statement, p)
				if err2 != nil {
					err = errors.Annotatef(err2, "statement: %s", statement)
					return
				}
				stmt, err2 := p.ParseOneStmt(statement, "", "")
				if err2 != nil {
					err = errors.Annotatef(err2, "statement: %s", statement)
					return
				}

				ctStmt, ok := stmt.(*ast.CreateTableStmt)
				if !ok {
					err = errors.Errorf("Expect CreateTableStmt but got %T", stmt)
					return
				}

				if c.checkAutoIncrementPrimaryKey {
					passed := c.checkAutoIncrementKey(sourceID, table.Schema, table.Name, ctStmt, info, r)
					if !passed {
						return
					}
				}

				checkErr := c.checkConsistency(stmtNode, ctStmt, firstTable.String(), table.String(), firstSourceID, sourceID)
				if checkErr != nil {
					reMu.Lock()
					r.State = StateFailure
					r.Errors = append(r.Errors, checkErr)
					r.Extra = fmt.Sprintf("error on sharding %s", c.targetTableID)
					r.Instruction = "please set same table structure for sharding tables"
					reMu.Unlock()
					return
				}
			}
		}
	}

	concurrency := maxThreadNum
	for sourceID := range c.tableMap {
		db, ok := c.dbs[sourceID]
		if !ok {
			markCheckError(r, errors.NotFoundf("client for sourceID %s", sourceID))
			return r
		}
		maxConnections, err := utils.GetMaxConnections(ctx, db)
		if err != nil {
			markCheckError(r, err)
			return r
		}
		concurrency = int(math.Min(float64(concurrency), float64(maxConnections/2)))
	}

	for i := 0; i < concurrency; i++ {
		checkWg.Add(1)
		go checkFunc()
	}

outer:
	for sourceID, tables := range c.tableMap {
		for _, table := range tables {
			select {
			case inCh <- &checkItem{table, sourceID}:
			case <-checkCtx.Done():
				log.L().Logger.Warn("ctx canceled before input tables completely")
				break outer
			}
		}
	}
	close(inCh)

	checkWg.Wait()
	log.L().Logger.Info("check sharding table structure over", zap.String("spend time", time.Since(startTime).String()))
	return r
}

func (c *ShardingTablesChecker) checkAutoIncrementKey(sourceID, schema, table string, ctStmt *ast.CreateTableStmt, info *model.TableInfo, r *Result) bool {
	autoIncrementKeys := c.findAutoIncrementKey(ctStmt, info)
	for columnName, isBigInt := range autoIncrementKeys {
		hasMatchedRule := false
		if cm, ok1 := c.mapping[sourceID]; ok1 {
			ruleSet := cm.Selector.Match(schema, table)
			for _, rule := range ruleSet {
				r, ok2 := rule.(*column.Rule)
				if !ok2 {
					continue
				}

				if r.Expression == column.PartitionID && r.TargetColumn == columnName {
					hasMatchedRule = true
					break
				}
			}

			if hasMatchedRule && !isBigInt {
				r.State = StateFailure
				r.Errors = append(r.Errors, NewError("sourceID %s table `%s`.`%s` of sharding %s have auto-increment key %s and column mapping, but type of %s should be bigint", sourceID, schema, table, c.targetTableID, columnName, columnName))
				r.Instruction = "please set auto-increment key type to bigint"
				r.Extra = AutoIncrementKeyChecking
				return false
			}
		}

		if !hasMatchedRule {
			r.State = StateFailure
			r.Errors = append(r.Errors, NewError("sourceID %s table `%s`.`%s` of sharding %s have auto-increment key %s and column mapping, but type of %s should be bigint", sourceID, schema, table, c.targetTableID, columnName, columnName))
			r.Instruction = "please handle it by yourself"
			r.Extra = AutoIncrementKeyChecking
			return false
		}
	}

	return true
}

func (c *ShardingTablesChecker) findAutoIncrementKey(stmt *ast.CreateTableStmt, info *model.TableInfo) map[string]bool {
	autoIncrementKeys := make(map[string]bool)
	autoIncrementCols := make(map[string]bool)

	for _, col := range stmt.Cols {
		var (
			hasAutoIncrementOpt bool
			isUnique            bool
		)
		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionAutoIncrement:
				hasAutoIncrementOpt = true
			case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey:
				isUnique = true
			}
		}

		if hasAutoIncrementOpt {
			if isUnique {
				autoIncrementKeys[col.Name.Name.O] = col.Tp.Tp == mysql.TypeLonglong
			} else {
				autoIncrementCols[col.Name.Name.O] = col.Tp.Tp == mysql.TypeLonglong
			}
		}
	}

	for _, index := range info.Indices {
		if index.Unique || index.Primary {
			if len(index.Columns) == 1 {
				if isBigInt, ok := autoIncrementCols[index.Columns[0].Name.O]; ok {
					autoIncrementKeys[index.Columns[0].Name.O] = isBigInt
				}
			}
		}
	}

	return autoIncrementKeys
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

func (c *ShardingTablesChecker) checkConsistency(self, other *ast.CreateTableStmt, selfTable, otherTable, selfsourceID, othersourceID string) *Error {
	selfColumnList := getBriefColumnList(self)
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
		e.Self = fmt.Sprintf("sourceID %s table %s columns %v", selfsourceID, selfTable, getColumnNames(selfColumnList))
		e.Other = fmt.Sprintf("sourceID %s table %s columns %v", othersourceID, otherTable, getColumnNames(otherColumnList))
		return e
	}

	for i := range selfColumnList {
		if *selfColumnList[i] != *otherColumnList[i] {
			e := NewError("different column definition")
			e.Self = fmt.Sprintf("sourceID %s table %s column %s", selfsourceID, selfTable, selfColumnList[i])
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
