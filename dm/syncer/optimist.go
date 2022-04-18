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
	"context"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// initOptimisticShardDDL initializes the shard DDL support in the optimistic mode.
func (s *Syncer) initOptimisticShardDDL(ctx context.Context) error {
	// fetch tables from source and filter them
	sourceTables, err := s.fromDB.FetchAllDoTables(ctx, s.baList)
	if err != nil {
		return err
	}

	// convert according to router rules.
	// downstream-schema -> downstream-table -> upstream-schema -> upstream-table.
	// TODO: refine to downstream-ID -> upstream-ID
	mapper := make(map[string]map[string]map[string]map[string]struct{})
	for upSchema, UpTables := range sourceTables {
		for _, upTable := range UpTables {
			up := &filter.Table{Schema: upSchema, Name: upTable}
			down := s.route(up)
			downSchema, downTable := down.Schema, down.Name
			if _, ok := mapper[downSchema]; !ok {
				mapper[downSchema] = make(map[string]map[string]map[string]struct{})
			}
			if _, ok := mapper[downSchema][downTable]; !ok {
				mapper[downSchema][downTable] = make(map[string]map[string]struct{})
			}
			if _, ok := mapper[downSchema][downTable][upSchema]; !ok {
				mapper[downSchema][downTable][upSchema] = make(map[string]struct{})
			}
			mapper[downSchema][downTable][upSchema][upTable] = struct{}{}
		}
	}

	return s.optimist.Init(mapper)
}

// handleQueryEventOptimistic handles QueryEvent in the optimistic shard DDL mode.
func (s *Syncer) handleQueryEventOptimistic(qec *queryEventContext) error {
	// interrupted after flush old checkpoint and before track DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err := handleFlushCheckpointStage(1, val.(int), "before track DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	var (
		upTable   *filter.Table
		downTable *filter.Table

		isDBDDL  bool
		tiBefore *model.TableInfo
		tiAfter  *model.TableInfo
		tisAfter []*model.TableInfo
		err      error

		trackInfos = qec.trackInfos
	)

	err = s.execError.Load()
	if err != nil {
		qec.tctx.L().Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	switch trackInfos[0].originStmt.(type) {
	case *ast.CreateDatabaseStmt, *ast.DropDatabaseStmt, *ast.AlterDatabaseStmt:
		isDBDDL = true
	}

	for _, trackInfo := range trackInfos {
		// check whether do shard DDL for multi upstream tables.
		if upTable != nil && upTable.String() != "``" && upTable.String() != trackInfo.sourceTables[0].String() {
			return terror.ErrSyncerUnitDDLOnMultipleTable.Generate(qec.originSQL)
		}
		upTable = trackInfo.sourceTables[0]
		downTable = trackInfo.targetTables[0]
	}

	if !isDBDDL {
		if _, ok := trackInfos[0].originStmt.(*ast.CreateTableStmt); !ok {
			tiBefore, err = s.getTableInfo(qec.tctx, upTable, downTable)
			if err != nil {
				return err
			}
		}
	}

	for _, trackInfo := range trackInfos {
		if err = s.trackDDL(qec.ddlSchema, trackInfo, qec.eventContext); err != nil {
			return err
		}
		if !isDBDDL {
			tiAfter, err = s.getTableInfo(qec.tctx, upTable, downTable)
			if err != nil {
				return err
			}
			tisAfter = append(tisAfter, tiAfter)
		}
	}

	// in optimistic mode, don't `saveTablePoint` before execute DDL,
	// because it has no `UnresolvedTables` to prevent the flush of this checkpoint.

	info := s.optimist.ConstructInfo(upTable.Schema, upTable.Name, downTable.Schema, downTable.Name, qec.needHandleDDLs, tiBefore, tisAfter)

	var (
		rev    int64
		skipOp bool
		op     optimism.Operation
	)
	switch trackInfos[0].originStmt.(type) {
	case *ast.CreateDatabaseStmt, *ast.AlterDatabaseStmt:
		// need to execute the DDL to the downstream, but do not do the coordination with DM-master.
		op.DDLs = qec.needHandleDDLs
		skipOp = true
	case *ast.DropDatabaseStmt:
		skipOp = true
		s.osgk.RemoveSchema(upTable.Schema)
	case *ast.CreateTableStmt:
		info.TableInfoBefore = tiAfter // for `CREATE TABLE`, we use tiAfter as tiBefore.
		rev, err = s.optimist.PutInfoAddTable(info)
		if err != nil {
			return err
		}
	case *ast.DropTableStmt:
		// no operation exist for `DROP TABLE` now.
		_, err = s.optimist.DeleteInfoRemoveTable(info)
		if err != nil {
			return err
		}
		skipOp = true
		s.osgk.RemoveGroup(downTable, []string{utils.GenTableID(upTable)})
	default:
		rev, err = s.optimist.PutInfo(info)
		if err != nil {
			return err
		}
	}

	if !skipOp {
		s.tctx.L().Info("putted a shard DDL info into etcd", zap.Stringer("info", info))
		op, err = s.optimist.GetOperation(qec.tctx.Ctx, info, rev+1)
		if err != nil {
			return err
		}
		s.tctx.L().Info("got a shard DDL lock operation", zap.Stringer("operation", op))
	}

	if op.ConflictStage == optimism.ConflictDetected {
		return terror.ErrSyncerShardDDLConflict.Generate(qec.needHandleDDLs, op.ConflictMsg)
	}

	// if this ddl is a ConflictSkipWaitRedirect ddl, we should skip all this worker's following ddls/dmls until the lock is resolved.
	// To do this, we append this table to osgk to prevent the following ddl/dmls from being executed.
	if op.ConflictStage == optimism.ConflictSkipWaitRedirect {
		first := s.osgk.appendConflictTable(upTable, downTable, qec.startLocation.Clone(), s.cfg.Flavor, s.cfg.EnableGTID)
		if first {
			s.optimist.GetRedirectOperation(qec.tctx.Ctx, info, op.Revision+1)
		}
		// This conflicted ddl is not executed in downstream, so we need to revert tableInfo in schemaTracker to `tiBefore`.
		err = s.schemaTracker.DropTable(upTable)
		if err != nil {
			s.tctx.L().Error("fail to drop table to rollback table in schema tracker", zap.Stringer("table", upTable))
			return err
		} else {
			err = s.schemaTracker.CreateTableIfNotExists(upTable, tiBefore)
			if err != nil {
				s.tctx.L().Error("fail to recreate table to rollback table in schema tracker", zap.Stringer("table", upTable))
				return err
			}
		}
		s.tctx.L().Info("skip conflict ddls in optimistic shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
		return nil
	}

	// updated needHandleDDLs to DDLs received from DM-master.
	qec.needHandleDDLs = op.DDLs

	s.tctx.L().Info("start to handle ddls in optimistic shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

	// interrupted after track DDL and before execute DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(2, val.(int), "before execute DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	qec.shardingDDLInfo = trackInfos[0]
	job := newDDLJob(qec)
	_, err = s.handleJobFunc(job)
	if err != nil {
		return err
	}

	err = s.execError.Load()
	if err != nil {
		s.tctx.L().Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	if qec.onlineDDLTable != nil {
		s.tctx.L().Info("finish online ddl and clear online ddl metadata in optimistic shard mode",
			zap.String("event", "query"),
			zap.Strings("ddls", qec.needHandleDDLs),
			zap.String("raw statement", qec.originSQL),
			zap.Stringer("table", qec.onlineDDLTable))
		err = s.onlineDDL.Finish(qec.tctx, qec.onlineDDLTable)
		if err != nil {
			return terror.Annotatef(err, "finish online ddl on %v", qec.onlineDDLTable)
		}
	}

	s.resolveOptimisticDDL(qec.eventContext, getDDLJobSourceTable(job), job.targetTable, op.ConflictStage)

	s.tctx.L().Info("finish to handle ddls in optimistic shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	return nil
}

func (s *Syncer) resolveOptimisticDDL(ec *eventContext, sourceTable, targetTable *filter.Table, stage optimism.ConflictStage) {
	if sourceTable != nil && targetTable != nil {
		if (stage == optimism.ConflictNone && s.osgk.tableInConflict(targetTable)) ||
			s.osgk.inConflictStage(sourceTable, targetTable) {
			// in the following two situations we should resolve this ddl lock at now
			// 1. after this worker's ddl, the ddl lock is resolved
			// 2. other worker has resolved this ddl lock, receives resolve command from master
			group, redirectLocation := s.osgk.resolveGroup(targetTable)
			if len(group) > 0 {
				s.optimist.DoneRedirectOperation(utils.GenTableID(targetTable))
				for tableID, location := range group {
					s.saveTablePoint(utils.UnpackTableID(tableID), location)
				}
				resync := &ShardingReSync{
					currLocation:   redirectLocation,
					latestLocation: ec.currentLocation.Clone(),
					targetTable:    targetTable,
					allResolved:    true,
				}
				s.osgk.tctx.L().Info("sending resync operation in optimistic shard mode",
					zap.Stringer("shardingResync", resync))
				*ec.shardingReSyncCh <- resync
			}
		}
	} else {
		s.osgk.tctx.L().Warn("invalid resolveOptimistic deploy without sourceTable/targetTable in optimistic shard mode",
			zap.Bool("emptySourceTable", sourceTable == nil),
			zap.Bool("emptyTargetTable", targetTable == nil))
	}
}
