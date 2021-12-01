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
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/dm/pkg/binlog/event"
	tcontext "github.com/pingcap/ticdc/dm/pkg/context"
	parserpkg "github.com/pingcap/ticdc/dm/pkg/parser"
	"github.com/pingcap/ticdc/dm/pkg/terror"
	"github.com/pingcap/ticdc/dm/pkg/utils"
	"github.com/pingcap/ticdc/dm/syncer/metrics"
)

func parseOneStmt(qec *queryEventContext) (stmt ast.StmtNode, err error) {
	// We use Parse not ParseOneStmt here, because sometimes we got a commented out ddl which can't be parsed
	// by ParseOneStmt(it's a limitation of tidb parser.)
	qec.tctx.L().Info("parse ddl", zap.String("event", "query"), zap.Stringer("query event context", qec))
	stmts, err := parserpkg.Parse(qec.p, qec.originSQL, "", "")
	if err != nil {
		// log error rather than fatal, so other defer can be executed
		qec.tctx.L().Error("parse ddl", zap.String("event", "query"), zap.Stringer("query event context", qec))
		return nil, terror.ErrSyncerParseDDL.Delegate(err, qec.originSQL)
	}
	if len(stmts) == 0 {
		return nil, nil
	}
	return stmts[0], nil
}

// processOneDDL processes already split ddl as following step:
// 1. generate ddl info;
// 2. skip sql by skipQueryEvent;
// 3. apply online ddl if onlineDDL is not nil:
//    * specially, if skip, apply empty string;
func (s *Syncer) processOneDDL(qec *queryEventContext, sql string) ([]string, error) {
	ddlInfo, err := s.genDDLInfo(qec.p, qec.ddlSchema, sql, qec.eventStatusVars)
	if err != nil {
		return nil, err
	}

	if s.onlineDDL != nil {
		if err = s.onlineDDL.CheckRegex(ddlInfo.originStmt, qec.ddlSchema, s.SourceTableNamesFlavor); err != nil {
			return nil, err
		}
	}

	qec.tctx.L().Debug("will skip query event", zap.String("event", "query"), zap.String("statement", sql), zap.Stringer("ddlInfo", ddlInfo))
	shouldSkip, err := s.skipQueryEvent(qec, ddlInfo)
	if err != nil {
		return nil, err
	}
	if shouldSkip {
		metrics.SkipBinlogDurationHistogram.WithLabelValues("query", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(qec.startTime).Seconds())
		qec.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.Stringer("query event context", qec))
		if s.onlineDDL == nil || len(ddlInfo.originDDL) != 0 {
			return nil, nil
		}
	}

	if s.onlineDDL == nil {
		return []string{ddlInfo.originDDL}, nil
	}
	// filter and save ghost table ddl
	sqls, err := s.onlineDDL.Apply(qec.tctx, ddlInfo.sourceTables, ddlInfo.originDDL, ddlInfo.originStmt, qec.p)
	if err != nil {
		return nil, err
	}
	// represent saved in onlineDDL.Storage
	if len(sqls) == 0 {
		return nil, nil
	}
	// represent this sql is not online DDL.
	if sqls[0] == sql {
		return sqls, nil
	}

	if qec.onlineDDLTable == nil {
		qec.onlineDDLTable = ddlInfo.sourceTables[0]
	} else if qec.onlineDDLTable.String() != ddlInfo.sourceTables[0].String() {
		return nil, terror.ErrSyncerUnitOnlineDDLOnMultipleTable.Generate(qec.originSQL)
	}
	return sqls, nil
}

// genDDLInfo generates ddl info by given sql.
func (s *Syncer) genDDLInfo(p *parser.Parser, schema, sql string, statusVars []byte) (*ddlInfo, error) {
	s.tctx.L().Debug("begin generate ddl info", zap.String("event", "query"), zap.String("statement", sql))
	stmt, err := p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "ddl %s", sql)
	}

	sourceTables, err := parserpkg.FetchDDLTables(schema, stmt, s.SourceTableNamesFlavor)
	if err != nil {
		return nil, err
	}

	targetTables := make([]*filter.Table, 0, len(sourceTables))
	for i := range sourceTables {
		renamedTable := s.route(sourceTables[i])
		targetTables = append(targetTables, renamedTable)
	}

	ddlInfo := &ddlInfo{
		originDDL:    sql,
		originStmt:   stmt,
		sourceTables: sourceTables,
		targetTables: targetTables,
	}

	adjustCollation(s.tctx, ddlInfo, statusVars)
	routedDDL, err := parserpkg.RenameDDLTable(ddlInfo.originStmt, ddlInfo.targetTables)
	ddlInfo.routedDDL = routedDDL
	return ddlInfo, err
}

func (s *Syncer) dropSchemaInSharding(tctx *tcontext.Context, sourceSchema string) error {
	sources := make(map[string][]*filter.Table)
	sgs := s.sgk.Groups()
	for name, sg := range sgs {
		if sg.IsSchemaOnly {
			// in sharding group leave handling, we always process schema group,
			// we can ignore schema only group here
			continue
		}
		tables := sg.Tables()
		for _, table := range tables {
			if table.Schema != sourceSchema {
				continue
			}
			sources[name] = append(sources[name], table)
		}
	}
	// delete from sharding group firstly
	for name, tables := range sources {
		targetTable := utils.UnpackTableID(name)
		sourceTableIDs := make([]string, 0, len(tables))
		for _, table := range tables {
			sourceTableIDs = append(sourceTableIDs, utils.GenTableID(table))
		}
		err := s.sgk.LeaveGroup(targetTable, sourceTableIDs)
		if err != nil {
			return err
		}
	}
	// delete from checkpoint
	for _, tables := range sources {
		for _, table := range tables {
			// refine clear them later if failed
			// now it doesn't have problems
			if err1 := s.checkpoint.DeleteTablePoint(tctx, table); err1 != nil {
				s.tctx.L().Error("fail to delete checkpoint", zap.Stringer("table", table))
			}
		}
	}
	return nil
}

func (s *Syncer) clearOnlineDDL(tctx *tcontext.Context, targetTable *filter.Table) error {
	group := s.sgk.Group(targetTable)
	if group == nil {
		return nil
	}

	// return [[schema, table]...]
	tables := group.Tables()

	for _, table := range tables {
		s.tctx.L().Info("finish online ddl", zap.Stringer("table", table))
		err := s.onlineDDL.Finish(tctx, table)
		if err != nil {
			return terror.Annotatef(err, "finish online ddl on %v", table)
		}
	}

	return nil
}

// adjustCollation adds collation for create database and check create table.
func adjustCollation(tctx *tcontext.Context, ddlInfo *ddlInfo, statusVars []byte) {
	// check create table has collation
	if createStmt, ok := ddlInfo.originStmt.(*ast.CreateTableStmt); ok {
		// create table like old table has no table_options
		// See https://dev.mysql.com/doc/refman/5.7/en/create-table.html
		if createStmt.ReferTable != nil {
			return
		}
		for _, tableOption := range createStmt.Options {
			// already have 'Collation'
			if tableOption.Tp == ast.TableOptionCollate {
				return
			}
		}
		tctx.L().Warn("detect create table risk which use implicit collation", zap.String("originSQL", ddlInfo.originDDL))
	} else if createStmt, ok := ddlInfo.originStmt.(*ast.CreateDatabaseStmt); ok {
		// collation is in create database syntax create_option
		// See https://dev.mysql.com/doc/refman/8.0/en/create-database.html
		hasCharset := false
		for _, createOption := range createStmt.Options {
			// already have 'Collation'
			if createOption.Tp == ast.DatabaseOptionCollate {
				return
			}
			if createOption.Tp == ast.DatabaseOptionCharset {
				hasCharset = true
			}
		}

		// just has charset, can not add collation by server collation
		if hasCharset {
			tctx.L().Warn("detect create database risk which use explicit charset and implicit collation", zap.String("originSQL", ddlInfo.originDDL))
			return
		}

		// add collation by server collation from binlog statusVars
		collation, err := event.GetServerCollationByStatusVars(statusVars)
		if err != nil {
			tctx.L().Warn("found error when get collation_server from binlog status_vars", zap.Error(err))
		}
		// add collation
		tctx.L().Warn("detect create database risk which use implicit charset and collation, we will add collation by binlog status_vars", zap.String("originSQL", ddlInfo.originDDL), zap.String("collation", collation))
		createStmt.Options = append(createStmt.Options, &ast.DatabaseOption{Tp: ast.DatabaseOptionCollate, Value: collation})
	}
}

type ddlInfo struct {
	originDDL    string
	routedDDL    string
	originStmt   ast.StmtNode
	sourceTables []*filter.Table
	targetTables []*filter.Table
}

func (d *ddlInfo) String() string {
	sourceTables := make([]string, 0, len(d.sourceTables))
	targetTables := make([]string, 0, len(d.targetTables))
	for i := range d.sourceTables {
		sourceTables = append(sourceTables, d.sourceTables[i].String())
		targetTables = append(targetTables, d.targetTables[i].String())
	}
	return fmt.Sprintf("{originDDL: %s, routedDDL: %s, sourceTables: %s, targetTables: %s}",
		d.originDDL, d.routedDDL, strings.Join(sourceTables, ","), strings.Join(targetTables, ","))
}
