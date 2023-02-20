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

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/filter"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	parserpkg "github.com/pingcap/tiflow/dm/pkg/parser"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
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
//   - specially, if skip, apply empty string;
func (s *Syncer) processOneDDL(qec *queryEventContext, sql string) ([]string, error) {
	ddlInfo, err := s.genDDLInfo(qec, sql)
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
func (s *Syncer) genDDLInfo(qec *queryEventContext, sql string) (*ddlInfo, error) {
	s.tctx.L().Debug("begin generate ddl info", zap.String("event", "query"), zap.String("statement", sql))
	stmt, err := qec.p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "ddl %s", sql)
	}

	sourceTables, err := parserpkg.FetchDDLTables(qec.ddlSchema, stmt, s.SourceTableNamesFlavor)
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

	// "strict" will adjust collation
	if s.cfg.CollationCompatible == config.StrictCollationCompatible {
		adjustCollation(s.tctx, ddlInfo, qec.eventStatusVars, s.charsetAndDefaultCollation, s.idAndCollationMap)
	}

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
func adjustCollation(tctx *tcontext.Context, ddlInfo *ddlInfo, statusVars []byte, charsetAndDefaultCollationMap map[string]string, idAndCollationMap map[int]string) {
	switch createStmt := ddlInfo.originStmt.(type) {
	case *ast.CreateTableStmt:
		if createStmt.ReferTable != nil {
			return
		}
		adjustColumnsCollation(tctx, createStmt, charsetAndDefaultCollationMap)
		var justCharset string
		for _, tableOption := range createStmt.Options {
			// already have 'Collation'
			if tableOption.Tp == ast.TableOptionCollate {
				return
			}
			if tableOption.Tp == ast.TableOptionCharset {
				justCharset = tableOption.StrValue
			}
		}
		if justCharset == "" {
			tctx.L().Warn("detect create table risk which use implicit charset and collation", zap.String("originSQL", ddlInfo.originDDL))
			return
		}
		// just has charset, can add collation by charset and default collation map
		collation, ok := charsetAndDefaultCollationMap[strings.ToLower(justCharset)]
		if !ok {
			tctx.L().Warn("not found charset default collation.", zap.String("originSQL", ddlInfo.originDDL), zap.String("charset", strings.ToLower(justCharset)))
			return
		}
		tctx.L().Info("detect create table risk which use explicit charset and implicit collation, we will add collation by INFORMATION_SCHEMA.COLLATIONS", zap.String("originSQL", ddlInfo.originDDL), zap.String("collation", collation))
		createStmt.Options = append(createStmt.Options, &ast.TableOption{Tp: ast.TableOptionCollate, StrValue: collation})

	case *ast.CreateDatabaseStmt:
		var justCharset, collation string
		var ok bool
		var err error
		for _, createOption := range createStmt.Options {
			// already have 'Collation'
			if createOption.Tp == ast.DatabaseOptionCollate {
				return
			}
			if createOption.Tp == ast.DatabaseOptionCharset {
				justCharset = createOption.Value
			}
		}

		// just has charset, can add collation by charset and default collation map
		if justCharset != "" {
			collation, ok = charsetAndDefaultCollationMap[strings.ToLower(justCharset)]
			if !ok {
				tctx.L().Warn("not found charset default collation.", zap.String("originSQL", ddlInfo.originDDL), zap.String("charset", strings.ToLower(justCharset)))
				return
			}
			tctx.L().Info("detect create database risk which use explicit charset and implicit collation, we will add collation by INFORMATION_SCHEMA.COLLATIONS", zap.String("originSQL", ddlInfo.originDDL), zap.String("collation", collation))
		} else {
			// has no charset and collation
			// add collation by server collation from binlog statusVars
			collation, err = event.GetServerCollationByStatusVars(statusVars, idAndCollationMap)
			if err != nil {
				tctx.L().Error("can not get charset server collation from binlog statusVars.", zap.Error(err), zap.String("originSQL", ddlInfo.originDDL))
			}
			if collation == "" {
				tctx.L().Error("get server collation from binlog statusVars is nil.", zap.Error(err), zap.String("originSQL", ddlInfo.originDDL))
				return
			}
			// add collation
			tctx.L().Info("detect create database risk which use implicit charset and collation, we will add collation by binlog status_vars", zap.String("originSQL", ddlInfo.originDDL), zap.String("collation", collation))
		}
		createStmt.Options = append(createStmt.Options, &ast.DatabaseOption{Tp: ast.DatabaseOptionCollate, Value: collation})
	}
}

// adjustColumnsCollation adds column's collation.
func adjustColumnsCollation(tctx *tcontext.Context, createStmt *ast.CreateTableStmt, charsetAndDefaultCollationMap map[string]string) {
ColumnLoop:
	for _, col := range createStmt.Cols {
		for _, options := range col.Options {
			// already have 'Collation'
			if options.Tp == ast.ColumnOptionCollate {
				continue ColumnLoop
			}
		}
		fieldType := col.Tp
		// already have 'Collation'
		if fieldType.GetCollate() != "" {
			continue
		}
		if fieldType.GetCharset() != "" {
			// just have charset
			collation, ok := charsetAndDefaultCollationMap[strings.ToLower(fieldType.GetCharset())]
			if !ok {
				tctx.L().Warn("not found charset default collation for column.", zap.String("table", createStmt.Table.Name.String()), zap.String("column", col.Name.String()), zap.String("charset", strings.ToLower(fieldType.GetCharset())))
				continue
			}
			col.Options = append(col.Options, &ast.ColumnOption{Tp: ast.ColumnOptionCollate, StrValue: collation})
		}
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
