// Copyright 2020 PingCAP, Inc.
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/tidb-tools/pkg/filter"
	ddl2 "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/pkg/quotes"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// OperateSchema operates schema for an upstream table.
func (s *Syncer) OperateSchema(ctx context.Context, req *pb.OperateWorkerSchemaRequest) (msg string, err error) {
	sourceTable := &filter.Table{
		Schema: req.Database,
		Name:   req.Table,
	}
	switch req.Op {
	case pb.SchemaOp_ListSchema:
		allSchema := s.schemaTracker.AllSchemas()
		schemaList := make([]string, len(allSchema))
		for i, schema := range allSchema {
			schemaList[i] = schema.Name.String()
		}
		schemaListJSON, err := json.Marshal(schemaList)
		if err != nil {
			return "", terror.ErrSchemaTrackerMarshalJSON.Delegate(err, schemaList)
		}
		return string(schemaListJSON), err
	case pb.SchemaOp_ListTable:
		tables, err := s.schemaTracker.ListSchemaTables(req.Database)
		if err != nil {
			return "", err
		}
		tableListJSON, err := json.Marshal(tables)
		if err != nil {
			return "", terror.ErrSchemaTrackerMarshalJSON.Delegate(err, tables)
		}
		return string(tableListJSON), err
	case pb.SchemaOp_GetSchema:
		// when task is paused, schemaTracker is closed. We get the table structure from checkpoint.
		ti := s.checkpoint.GetTableInfo(req.Database, req.Table)
		if ti == nil {
			s.tctx.L().Info("table schema is not in checkpoint, fetch from downstream",
				zap.String("table", sourceTable.String()))
			targetTable := s.route(sourceTable)
			result, err2 := dbconn.GetTableCreateSQL(s.tctx.WithContext(ctx), s.downstreamTrackConn, targetTable.String())
			result = strings.Replace(result, fmt.Sprintf("CREATE TABLE %s", quotes.QuoteName(targetTable.Name)), fmt.Sprintf("CREATE TABLE %s", quotes.QuoteName(sourceTable.Name)), 1)
			return utils.CreateTableSQLToOneRow(result), err2
		}

		result := bytes.NewBuffer(make([]byte, 0, 512))
		err2 := executor.ConstructResultOfShowCreateTable(s.sessCtx, ti, autoid.Allocators{}, result)
		return utils.CreateTableSQLToOneRow(result.String()), err2

	case pb.SchemaOp_SetSchema:
		// for set schema, we must ensure it's a valid `CREATE TABLE` statement.
		// if want to update the one in checkpoint, it should wait for the flush of checkpoint.
		parser2, err := s.fromDB.GetParser(ctx)
		if err != nil {
			return "", err
		}
		node, err := parser2.ParseOneStmt(req.Schema, "", "")
		if err != nil {
			return "", terror.ErrSchemaTrackerInvalidCreateTableStmt.Delegate(err, req.Schema)
		}
		stmt, ok := node.(*ast.CreateTableStmt)
		if !ok {
			return "", terror.ErrSchemaTrackerInvalidCreateTableStmt.Generate(req.Schema)
		}
		// ensure correct table name.
		stmt.Table.Schema = model.NewCIStr(req.Database)
		stmt.Table.Name = model.NewCIStr(req.Table)
		stmt.IfNotExists = false // we must ensure drop the previous one.

		var newCreateSQLBuilder strings.Builder
		restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &newCreateSQLBuilder)
		if err = stmt.Restore(restoreCtx); err != nil {
			return "", terror.ErrSchemaTrackerRestoreStmtFail.Delegate(err)
		}
		newSQL := newCreateSQLBuilder.String()

		s.exprFilterGroup.ResetExprs(sourceTable)

		if !req.Flush {
			s.tctx.L().Info("overwrite --flush to true for operate-schema")
		}

		ti, err2 := ddl2.BuildTableInfoFromAST(stmt)
		if err2 != nil {
			return "", terror.ErrSchemaTrackerRestoreStmtFail.Delegate(err2)
		}

		log.L().Info("flush table info", zap.String("table info", newSQL))
		err = s.checkpoint.FlushPointWithTableInfo(tcontext.NewContext(ctx, log.L()), sourceTable, ti)
		if err != nil {
			return "", err
		}

		if req.Sync {
			if s.cfg.ShardMode != config.ShardOptimistic {
				log.L().Warn("ignore --sync flag", zap.String("shard mode", s.cfg.ShardMode))
				break
			}
			targetTable := s.route(sourceTable)
			// use new table info as tableInfoBefore, we can also use the origin table from schemaTracker
			info := s.optimist.ConstructInfo(req.Database, req.Table, targetTable.Schema, targetTable.Name, []string{""}, ti, []*model.TableInfo{ti})
			info.IgnoreConflict = true
			log.L().Info("sync info with operate-schema", zap.String("info", info.ShortString()))
			_, err = s.optimist.PutInfo(info)
			if err != nil {
				return "", err
			}
		}

	case pb.SchemaOp_RemoveSchema:
		// as the doc says, `operate-schema remove` will let DM-worker use table structure in checkpoint, which does not
		// need further actions.
		return "", nil
	}
	return "", nil
}
