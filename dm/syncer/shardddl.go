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

package syncer

import (
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/log"
	parserpkg "github.com/pingcap/tiflow/dm/pkg/parser"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"go.uber.org/zap"
)

type ShardDDLStrategy interface {
	preFilter(ddlInfo *ddlInfo, qec *queryEventContext, sourceTable *filter.Table, targetTable *filter.Table) (bool, error)
	handleDDL(qec *queryEventContext) error
}

type ShardDDL struct {
	logger log.Logger

	strategy ShardDDLStrategy

	// TODO: Take it out.
	s *Syncer
}

type Normal struct {
	ShardDDLStrategy

	logger log.Logger

	s *Syncer
}

type Pessimist struct {
	ShardDDLStrategy

	logger log.Logger

	s *Syncer
}

type Optimist struct {
	ShardDDLStrategy

	logger log.Logger

	s *Syncer
}

// NewShardDDL creates a new ShardDDL instance.
func NewShardDDL(pLogger *log.Logger, syncer *Syncer) *ShardDDL {
	shardDDL := &ShardDDL{
		logger: pLogger.WithFields(zap.String("component", "ddl")),
		s:      syncer,
	}
	switch syncer.cfg.ShardMode {
	case config.ShardPessimistic:
		shardDDL.strategy = &Pessimist{logger: shardDDL.logger, s: syncer}
	case config.ShardOptimistic:
		shardDDL.strategy = &Optimist{logger: shardDDL.logger, s: syncer}
	default:
		shardDDL.strategy = &Normal{logger: shardDDL.logger, s: syncer}
	}
	return shardDDL
}

func (ddl *ShardDDL) HandleQueryEvent(ev *replication.QueryEvent, ec eventContext, originSQL string) (err error) {
	if originSQL == "BEGIN" {
		failpoint.Inject("NotUpdateLatestGTID", func(_ failpoint.Value) {
			// directly return nil without update latest GTID here
			failpoint.Return(nil)
		})
		// GTID event: GTID_NEXT = xxx:11
		// Query event: BEGIN (GTID set = xxx:1-11)
		// Rows event: ... (GTID set = xxx:1-11)  if we update lastLocation below,
		//                                        otherwise that is xxx:1-10 when dealing with table checkpoints
		// Xid event: GTID set = xxx:1-11  this event is related to global checkpoint
		*ec.lastLocation = *ec.currentLocation
		return nil
	}

	codec, err := event.GetCharsetCodecByStatusVars(ev.StatusVars)
	if err != nil {
		ddl.logger.Error("get charset codec failed, will treat query as utf8", zap.Error(err))
	} else if codec != nil {
		converted, err2 := codec.NewDecoder().String(originSQL)
		if err2 != nil {
			ddl.logger.Error("convert query string failed, will treat query as utf8", zap.Error(err2))
		} else {
			originSQL = converted
		}
	}

	qec := &queryEventContext{
		eventContext:    &ec,
		ddlSchema:       string(ev.Schema),
		originSQL:       utils.TrimCtrlChars(originSQL),
		splitDDLs:       make([]string, 0),
		appliedDDLs:     make([]string, 0),
		sourceTbls:      make(map[string]map[string]struct{}),
		eventStatusVars: ev.StatusVars,
	}

	defer func() {
		if err == nil {
			return
		}
		// why not `skipSQLByPattern` at beginning, but at defer?
		// it is in order to track every ddl except for the one that will cause error.
		// if `skipSQLByPattern` at beginning, some ddl should be tracked may be skipped.
		needSkip, err2 := ddl.s.skipSQLByPattern(qec.originSQL)
		if err2 != nil {
			err = err2
			return
		}
		if !needSkip {
			return
		}
		// don't return error if filter success
		ddl.s.metricsProxies.SkipBinlogDurationHistogram.WithLabelValues("query", ddl.s.cfg.Name, ddl.s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		ddl.logger.Warn("skip event", zap.String("event", "query"), zap.Stringer("query event context", qec))
		*ec.lastLocation = *ec.currentLocation // before record skip location, update lastLocation
		err = ddl.s.recordSkipSQLsLocation(&ec)
	}()

	qec.p, err = event.GetParserForStatusVars(ev.StatusVars)
	if err != nil {
		ddl.logger.Warn("found error when get sql_mode from binlog status_vars", zap.Error(err))
	}

	qec.timezone, err = event.GetTimezoneByStatusVars(ev.StatusVars, ddl.s.upstreamTZStr)
	if err != nil {
		ddl.logger.Warn("found error when get timezone from binlog status_vars", zap.Error(err))
	}

	qec.timestamp = ec.header.Timestamp

	stmt, err := parseOneStmt(qec)
	if err != nil {
		return err
	}

	if node, ok := stmt.(ast.DMLNode); ok {
		// if DML can be ignored, we do not report an error
		table, err2 := getTableByDML(node)
		if err2 == nil {
			if len(table.Schema) == 0 {
				table.Schema = qec.ddlSchema
			}
			ignore, err2 := ddl.s.skipRowsEvent(table, replication.QUERY_EVENT)
			if err2 == nil && ignore {
				return nil
			}
		}
		return terror.Annotatef(terror.ErrSyncUnitDMLStatementFound.Generate(), "query %s", qec.originSQL)
	}

	if _, ok := stmt.(ast.DDLNode); !ok {
		return nil
	}

	if qec.shardingReSync != nil {
		qec.shardingReSync.currLocation = *qec.currentLocation
		if binlog.CompareLocation(qec.shardingReSync.currLocation, qec.shardingReSync.latestLocation, ddl.s.cfg.EnableGTID) >= 0 {
			ddl.logger.Info("re-replicate shard group was completed", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
			return qec.closeShardingResync()
		} else if ddl.s.cfg.ShardMode != config.ShardOptimistic {
			// in re-syncing, we can simply skip all DDLs.
			// for pessimistic shard mode,
			// all ddls have been added to sharding DDL sequence
			// only update lastPos when the query is a real DDL
			*qec.lastLocation = qec.shardingReSync.currLocation
			ddl.logger.Debug("skip event in re-replicating sharding group", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
			return nil
		}
		// optimistic shard mode handle situation will be handled through table point after
		// we split ddls and handle the appliedDDLs
	}

	ddl.logger.Info("ready to split ddl", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	*qec.lastLocation = *qec.currentLocation // update lastLocation, because we have checked `isDDL`

	// TiDB can't handle multi schema change DDL, so we split it here.
	qec.splitDDLs, err = parserpkg.SplitDDL(stmt, qec.ddlSchema)
	if err != nil {
		return err
	}

	// for DDL, we don't apply operator until we try to execute it. so can handle sharding cases
	// We use default parser because inside function where need parser, sqls are came from parserpkg.SplitDDL, which is StringSingleQuotes, KeyWordUppercase and NameBackQuotes
	// TODO: save stmt, tableName to avoid parse the sql to get them again
	qec.p = parser.New()
	for _, sql := range qec.splitDDLs {
		sqls, err2 := ddl.s.processOneDDL(qec, sql)
		if err2 != nil {
			ddl.logger.Error("fail to process ddl", zap.String("event", "query"), zap.Stringer("queryEventContext", qec), log.ShortError(err2))
			return err2
		}
		qec.appliedDDLs = append(qec.appliedDDLs, sqls...)
	}
	ddl.logger.Info("resolve sql", zap.String("event", "query"), zap.Strings("appliedDDLs", qec.appliedDDLs), zap.Stringer("queryEventContext", qec))

	ddl.s.metricsProxies.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageGenQuery, ddl.s.cfg.Name, ddl.s.cfg.WorkerName, ddl.s.cfg.SourceID).Observe(time.Since(qec.startTime).Seconds())

	/*
		we construct a application transaction for ddl. we save checkpoint after we execute all ddls
		Here's a brief discussion for implement:
		* non sharding table: make no difference
		* sharding table - we limit one ddl event only contains operation for same table
		  * drop database / drop table / truncate table: we ignore these operations
		  * create database / create table / create index / drop index / alter table:
			operation is only for same table,  make no difference
		  * rename table
			* online ddl: we would ignore rename ghost table,  make no difference
			* other rename: we don't allow user to execute more than one rename operation in one ddl event, then it would make no difference
	*/

	qec.needHandleDDLs = make([]string, 0, len(qec.appliedDDLs))
	qec.trackInfos = make([]*ddlInfo, 0, len(qec.appliedDDLs))

	// handle one-schema change DDL
	for _, sql := range qec.appliedDDLs {
		if len(sql) == 0 {
			continue
		}
		// We use default parser because sqls are came from above *Syncer.splitAndFilterDDL, which is StringSingleQuotes, KeyWordUppercase and NameBackQuotes
		ddlInfo, err2 := ddl.s.genDDLInfo(qec, sql)
		if err2 != nil {
			return err2
		}
		sourceTable := ddlInfo.sourceTables[0]
		targetTable := ddlInfo.targetTables[0]
		if len(ddlInfo.routedDDL) == 0 {
			ddl.s.metricsProxies.SkipBinlogDurationHistogram.WithLabelValues("query", ddl.s.cfg.Name, ddl.s.cfg.SourceID).Observe(time.Since(qec.startTime).Seconds())
			ddl.logger.Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", qec.ddlSchema))
			continue
		}

		// DDL is sequentially synchronized in this syncer's main process goroutine
		// filter DDL that is older or same as table checkpoint, to avoid sync again for already synced DDLs
		if ddl.s.checkpoint.IsOlderThanTablePoint(sourceTable, *qec.currentLocation) {
			ddl.logger.Info("filter obsolete DDL", zap.String("event", "query"), zap.String("statement", sql), log.WrapStringerField("location", qec.currentLocation))
			continue
		}

		// pre-filter of sharding
		if filter, err := ddl.strategy.preFilter(ddlInfo, qec, sourceTable, targetTable); err != nil {
			return err
		} else if filter {
			continue
		}

		qec.needHandleDDLs = append(qec.needHandleDDLs, ddlInfo.routedDDL)
		qec.trackInfos = append(qec.trackInfos, ddlInfo)
		// TODO: current table checkpoints will be deleted in track ddls, but created and updated in flush checkpoints,
		//       we should use a better mechanism to combine these operations
		if ddl.s.cfg.ShardMode == "" {
			recordSourceTbls(qec.sourceTbls, ddlInfo.originStmt, sourceTable)
		}
	}

	ddl.logger.Info("prepare to handle ddls", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	if len(qec.needHandleDDLs) == 0 {
		ddl.logger.Info("skip event, need handled ddls is empty", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
		return ddl.s.recordSkipSQLsLocation(qec.eventContext)
	}

	// interrupted before flush old checkpoint.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(0, val.(int), "before flush old checkpoint")
		if err != nil {
			failpoint.Return(err)
		}
	})

	// flush previous DMLs and checkpoint if needing to handle the DDL.
	// NOTE: do this flush before operations on shard groups which may lead to skip a table caused by `UnresolvedTables`.
	if err = ddl.s.flushJobs(); err != nil {
		return err
	}

	switch ddl.s.cfg.ShardMode {
	case "":
		return ddl.strategy.handleDDL(qec)
	case config.ShardOptimistic:
		return ddl.strategy.handleDDL(qec)
	case config.ShardPessimistic:
		return ddl.strategy.handleDDL(qec)
	}
	return errors.Errorf("unsupported shard-mode %s, should not happened", ddl.s.cfg.ShardMode)
}

func (ddl *Normal) preFilter(*ddlInfo, *queryEventContext, *filter.Table, *filter.Table) (bool, error) {
	return false, nil
}

func (ddl *Normal) handleDDL(qec *queryEventContext) error {
	ddl.logger.Info("start to handle ddls in normal mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

	// interrupted after flush old checkpoint and before track DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err := handleFlushCheckpointStage(1, val.(int), "before track DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	// run trackDDL before add ddl job to make sure checkpoint can be flushed
	for _, trackInfo := range qec.trackInfos {
		if err := ddl.s.trackDDL(qec.ddlSchema, trackInfo, qec.eventContext); err != nil {
			return err
		}
	}

	// interrupted after track DDL and before execute DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err := handleFlushCheckpointStage(2, val.(int), "before execute DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	job := newDDLJob(qec)
	_, err := ddl.s.handleJobFunc(job)
	if err != nil {
		return err
	}

	// when add ddl job, will execute ddl and then flush checkpoint.
	// if execute ddl failed, the execError will be set to that error.
	// return nil here to avoid duplicate error message
	err = ddl.s.execError.Load()
	if err != nil {
		ddl.logger.Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	ddl.logger.Info("finish to handle ddls in normal mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

	if qec.onlineDDLTable != nil {
		ddl.logger.Info("finish online ddl and clear online ddl metadata in normal mode",
			zap.String("event", "query"),
			zap.Strings("ddls", qec.needHandleDDLs),
			zap.String("raw statement", qec.originSQL),
			zap.Stringer("table", qec.onlineDDLTable))
		err2 := ddl.s.onlineDDL.Finish(qec.tctx, qec.onlineDDLTable)
		if err2 != nil {
			return terror.Annotatef(err2, "finish online ddl on %v", qec.onlineDDLTable)
		}
	}

	return nil
}

func (ddl *Pessimist) preFilter(ddlInfo *ddlInfo, qec *queryEventContext, sourceTable *filter.Table, targetTable *filter.Table) (bool, error) {
	switch ddlInfo.originStmt.(type) {
	case *ast.DropDatabaseStmt:
		err := ddl.s.dropSchemaInSharding(qec.tctx, sourceTable.Schema)
		if err != nil {
			return false, err
		}
		return true, nil
	case *ast.DropTableStmt:
		sourceTableID := utils.GenTableID(sourceTable)
		err := ddl.s.sgk.LeaveGroup(targetTable, []string{sourceTableID})
		if err != nil {
			return false, err
		}
		err = ddl.s.checkpoint.DeleteTablePoint(qec.tctx, sourceTable)
		if err != nil {
			return false, err
		}
		return true, nil
	case *ast.TruncateTableStmt:
		ddl.logger.Info("filter truncate table statement in shard group", zap.String("event", "query"), zap.String("statement", ddlInfo.routedDDL))
		return true, nil
	}

	// in sharding mode, we only support to do one ddl in one event
	if qec.shardingDDLInfo == nil {
		qec.shardingDDLInfo = ddlInfo
	} else if qec.shardingDDLInfo.sourceTables[0].String() != sourceTable.String() {
		return false, terror.ErrSyncerUnitDDLOnMultipleTable.Generate(qec.originSQL)
	}

	return false, nil
}

func (ddl *Pessimist) handleDDL(qec *queryEventContext) error {
	var (
		err                error
		needShardingHandle bool
		group              *ShardingGroup
		synced             bool
		active             bool
		remain             int

		ddlInfo        = qec.shardingDDLInfo
		sourceTableID  = utils.GenTableID(ddlInfo.sourceTables[0])
		needHandleDDLs = qec.needHandleDDLs
		// for sharding DDL, the firstPos should be the `Pos` of the binlog, not the `End_log_pos`
		// so when restarting before sharding DDLs synced, this binlog can be re-sync again to trigger the TrySync
		startLocation   = qec.startLocation
		currentLocation = qec.currentLocation
	)

	var annotate string
	switch ddlInfo.originStmt.(type) {
	case *ast.CreateDatabaseStmt:
		// for CREATE DATABASE, we do nothing. when CREATE TABLE under this DATABASE, sharding groups will be added
	case *ast.CreateTableStmt:
		// for CREATE TABLE, we add it to group
		needShardingHandle, group, synced, remain, err = ddl.s.sgk.AddGroup(ddlInfo.targetTables[0], []string{sourceTableID}, nil, true)
		if err != nil {
			return err
		}
		annotate = "add table to shard group"
	default:
		needShardingHandle, group, synced, active, remain, err = ddl.s.sgk.TrySync(ddlInfo.sourceTables[0], ddlInfo.targetTables[0], *startLocation, *qec.currentLocation, needHandleDDLs)
		if err != nil {
			return err
		}
		annotate = "try to sync table in shard group"
		// meets DDL that will not be processed in sequence sharding
		if !active {
			ddl.logger.Info("skip in-activeDDL",
				zap.String("event", "query"),
				zap.Stringer("queryEventContext", qec),
				zap.String("sourceTableID", sourceTableID),
				zap.Bool("in-sharding", needShardingHandle),
				zap.Bool("is-synced", synced),
				zap.Int("unsynced", remain))
			return nil
		}
	}

	ddl.logger.Info(annotate,
		zap.String("event", "query"),
		zap.Stringer("queryEventContext", qec),
		zap.String("sourceTableID", sourceTableID),
		zap.Bool("in-sharding", needShardingHandle),
		zap.Bool("is-synced", synced),
		zap.Int("unsynced", remain))

	// interrupted after flush old checkpoint and before track DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(1, val.(int), "before track DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	for _, trackInfo := range qec.trackInfos {
		if err = ddl.s.trackDDL(qec.ddlSchema, trackInfo, qec.eventContext); err != nil {
			return err
		}
	}

	if needShardingHandle {
		ddl.s.metricsProxies.UnsyncedTableGauge.WithLabelValues(ddl.s.cfg.Name, ddlInfo.targetTables[0].String(), ddl.s.cfg.SourceID).Set(float64(remain))
		err = ddl.s.safeMode.IncrForTable(qec.tctx, ddlInfo.targetTables[0]) // try enable safe-mode when starting syncing for sharding group
		if err != nil {
			return err
		}

		// save checkpoint in memory, don't worry, if error occurred, we can rollback it
		// for non-last sharding DDL's table, this checkpoint will be used to skip binlog event when re-syncing
		// NOTE: when last sharding DDL executed, all this checkpoints will be flushed in the same txn
		ddl.logger.Info("save table checkpoint for source",
			zap.String("event", "query"),
			zap.String("sourceTableID", sourceTableID),
			zap.Stringer("start location", startLocation),
			log.WrapStringerField("end location", currentLocation))
		ddl.s.saveTablePoint(ddlInfo.sourceTables[0], *currentLocation)
		if !synced {
			ddl.logger.Info("source shard group is not synced",
				zap.String("event", "query"),
				zap.String("sourceTableID", sourceTableID),
				zap.Stringer("start location", startLocation),
				log.WrapStringerField("end location", currentLocation))
			return nil
		}

		ddl.logger.Info("source shard group is synced",
			zap.String("event", "query"),
			zap.String("sourceTableID", sourceTableID),
			zap.Stringer("start location", startLocation),
			log.WrapStringerField("end location", currentLocation))
		err = ddl.s.safeMode.DescForTable(qec.tctx, ddlInfo.targetTables[0]) // try disable safe-mode after sharding group synced
		if err != nil {
			return err
		}
		// maybe multi-groups' sharding DDL synced in this for-loop (one query-event, multi tables)
		if cap(*qec.shardingReSyncCh) < len(needHandleDDLs) {
			*qec.shardingReSyncCh = make(chan *ShardingReSync, len(needHandleDDLs))
		}
		firstEndLocation := group.FirstEndPosUnresolved()
		if firstEndLocation == nil {
			return terror.ErrSyncerUnitFirstEndPosNotFound.Generate(sourceTableID)
		}

		allResolved, err2 := ddl.s.sgk.ResolveShardingDDL(ddlInfo.targetTables[0])
		if err2 != nil {
			return err2
		}
		*qec.shardingReSyncCh <- &ShardingReSync{
			currLocation:   *firstEndLocation,
			latestLocation: *currentLocation,
			targetTable:    ddlInfo.targetTables[0],
			allResolved:    allResolved,
		}

		// Don't send new DDLInfo to dm-master until all local sql jobs finished
		// since jobWg is flushed by flushJobs before, we don't wait here any more

		// NOTE: if we need singleton Syncer (without dm-master) to support sharding DDL sync
		// we should add another config item to differ, and do not save DDLInfo, and not wait for ddlExecInfo

		// construct & send shard DDL info into etcd, DM-master will handle it.
		shardInfo := ddl.s.pessimist.ConstructInfo(ddlInfo.targetTables[0].Schema, ddlInfo.targetTables[0].Name, needHandleDDLs)
		rev, err2 := ddl.s.pessimist.PutInfo(qec.tctx.Ctx, shardInfo)
		if err2 != nil {
			return err2
		}
		ddl.s.metricsProxies.Metrics.ShardLockResolving.Set(1) // block and wait DDL lock to be synced
		ddl.logger.Info("putted shard DDL info", zap.Stringer("info", shardInfo), zap.Int64("revision", rev))

		shardOp, err2 := ddl.s.pessimist.GetOperation(qec.tctx.Ctx, shardInfo, rev+1)
		ddl.s.metricsProxies.Metrics.ShardLockResolving.Set(0)
		if err2 != nil {
			return err2
		}

		if shardOp.Exec {
			failpoint.Inject("ShardSyncedExecutionExit", func() {
				ddl.logger.Warn("exit triggered", zap.String("failpoint", "ShardSyncedExecutionExit"))
				//nolint:errcheck
				ddl.s.flushCheckPoints()
				utils.OsExit(1)
			})
			failpoint.Inject("SequenceShardSyncedExecutionExit", func() {
				group := ddl.s.sgk.Group(ddlInfo.targetTables[0])
				if group != nil {
					// exit in the first round sequence sharding DDL only
					if group.meta.ActiveIdx() == 1 {
						ddl.logger.Warn("exit triggered", zap.String("failpoint", "SequenceShardSyncedExecutionExit"))
						//nolint:errcheck
						ddl.s.flushCheckPoints()
						utils.OsExit(1)
					}
				}
			})

			ddl.logger.Info("execute DDL job",
				zap.String("event", "query"),
				zap.Stringer("queryEventContext", qec),
				zap.String("sourceTableID", sourceTableID),
				zap.Stringer("operation", shardOp))
		} else {
			ddl.logger.Info("ignore DDL job",
				zap.String("event", "query"),
				zap.Stringer("queryEventContext", qec),
				zap.String("sourceTableID", sourceTableID),
				zap.Stringer("operation", shardOp))
		}
	}

	ddl.logger.Info("start to handle ddls in shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

	// interrupted after track DDL and before execute DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(2, val.(int), "before execute DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	job := newDDLJob(qec)
	_, err = ddl.s.handleJobFunc(job)
	if err != nil {
		return err
	}

	err = ddl.s.execError.Load()
	if err != nil {
		ddl.logger.Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	if qec.onlineDDLTable != nil {
		err = ddl.s.clearOnlineDDL(qec.tctx, ddlInfo.targetTables[0])
		if err != nil {
			return err
		}
	}

	ddl.logger.Info("finish to handle ddls in shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	return nil
}

func (ddl *Optimist) preFilter(ddlInfo *ddlInfo, qec *queryEventContext, sourceTable *filter.Table, targetTable *filter.Table) (bool, error) {
	if ddl.s.osgk.inConflictStage(sourceTable, targetTable) {
		// if in unsync stage and not before active DDL, filter it
		// if in sharding re-sync stage and not before active DDL (the next DDL to be synced), filter it
		ddl.logger.Info("replicate sharding DDL, filter Conflicted table's ddl events",
			zap.String("event", "query"),
			zap.Stringer("source", sourceTable),
			log.WrapStringerField("location", qec.currentLocation))
		return true, nil
	} else if qec.shardingReSync != nil && qec.shardingReSync.targetTable.String() != targetTable.String() {
		// in re-syncing, ignore non current sharding group's events
		ddl.logger.Info("skip event in re-replicating shard group", zap.String("event", "query"), zap.Stringer("re-shard", qec.shardingReSync))
		return true, nil
	}
	switch ddlInfo.originStmt.(type) {
	case *ast.TruncateTableStmt:
		ddl.logger.Info("filter truncate table statement in shard group", zap.String("event", "query"), zap.String("statement", ddlInfo.routedDDL))
		return true, nil
	case *ast.RenameTableStmt:
		return false, terror.ErrSyncerUnsupportedStmt.Generate("RENAME TABLE", config.ShardOptimistic)
	}
	return false, nil
}

func (ddl *Optimist) handleDDL(qec *queryEventContext) error {
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

	err = ddl.s.execError.Load()
	if err != nil {
		ddl.logger.Error("error detected when executing SQL job", log.ShortError(err))
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
			tiBefore, err = ddl.s.getTableInfo(qec.tctx, upTable, downTable)
			if err != nil {
				return err
			}
		}
	}

	for _, trackInfo := range trackInfos {
		if err = ddl.s.trackDDL(qec.ddlSchema, trackInfo, qec.eventContext); err != nil {
			return err
		}
		if !isDBDDL {
			tiAfter, err = ddl.s.getTableInfo(qec.tctx, upTable, downTable)
			if err != nil {
				return err
			}
			tisAfter = append(tisAfter, tiAfter)
		}
	}

	// in optimistic mode, don't `saveTablePoint` before execute DDL,
	// because it has no `UnresolvedTables` to prevent the flush of this checkpoint.

	info := ddl.s.optimist.ConstructInfo(upTable.Schema, upTable.Name, downTable.Schema, downTable.Name, qec.needHandleDDLs, tiBefore, tisAfter)

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
		ddl.s.osgk.RemoveSchema(upTable.Schema)
	case *ast.CreateTableStmt:
		// need to execute the DDL to the downstream, but do not do the coordination with DM-master.
		op.DDLs = qec.needHandleDDLs
		skipOp = true
		if err = ddl.s.checkpoint.FlushPointsWithTableInfos(qec.tctx, []*filter.Table{upTable}, []*model.TableInfo{tiAfter}); err != nil {
			ddl.logger.Error("failed to flush create table info", zap.Stringer("table", upTable), zap.Strings("ddls", qec.needHandleDDLs), log.ShortError(err))
		}
		if _, err = ddl.s.optimist.AddTable(info); err != nil {
			return err
		}
	case *ast.DropTableStmt:
		skipOp = true
		if _, err = ddl.s.optimist.RemoveTable(info); err != nil {
			return err
		}
		ddl.s.osgk.RemoveGroup(downTable, []string{utils.GenTableID(upTable)})
	default:
		rev, err = ddl.s.optimist.PutInfo(info)
		if err != nil {
			return err
		}
	}

	ddl.logger.Info("putted a shard DDL info into etcd", zap.Stringer("info", info))
	if !skipOp {
		for {
			op, err = ddl.s.optimist.GetOperation(qec.tctx.Ctx, info, rev+1)
			if err != nil {
				return err
			}
			ddl.logger.Info("got a shard DDL lock operation", zap.Stringer("operation", op))
			if op.ConflictStage != optimism.ConflictDetected {
				break
			}
			rev = op.Revision
			ddl.logger.Info("operation conflict detected, waiting for resolve", zap.Stringer("info", info))
		}
	}

	switch op.ConflictStage {
	case optimism.ConflictError:
		return terror.ErrSyncerShardDDLConflict.Generate(qec.needHandleDDLs, op.ConflictMsg)
	// if this ddl is a ConflictSkipWaitRedirect ddl, we should skip all this worker's following ddls/dmls until the lock is resolved.
	// To do this, we append this table to osgk to prevent the following ddl/dmls from being executed.
	// conflict location must be the start location for current received ddl event.
	case optimism.ConflictSkipWaitRedirect:
		first := ddl.s.osgk.appendConflictTable(upTable, downTable, qec.startLocation.Clone(), ddl.s.cfg.Flavor, ddl.s.cfg.EnableGTID)
		if first {
			ddl.s.optimist.GetRedirectOperation(qec.tctx.Ctx, info, op.Revision+1)
		}
		// This conflicted ddl is not executed in downstream, so we need to revert tableInfo in schemaTracker to `tiBefore`.
		err = ddl.s.schemaTracker.DropTable(upTable)
		if err != nil {
			ddl.logger.Error("fail to drop table to rollback table in schema tracker", zap.Stringer("table", upTable))
		} else {
			err = ddl.s.schemaTracker.CreateTableIfNotExists(upTable, tiBefore)
			if err != nil {
				ddl.logger.Error("fail to recreate table to rollback table in schema tracker", zap.Stringer("table", upTable))
			} else {
				ddl.logger.Info("skip conflict ddls in optimistic shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
			}
		}
		return err
	}

	// updated needHandleDDLs to DDLs received from DM-master.
	qec.needHandleDDLs = op.DDLs

	ddl.logger.Info("start to handle ddls in optimistic shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

	// interrupted after track DDL and before execute DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(2, val.(int), "before execute DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	qec.shardingDDLInfo = trackInfos[0]
	job := newDDLJob(qec)
	_, err = ddl.s.handleJobFunc(job)
	if err != nil {
		return err
	}

	err = ddl.s.execError.Load()
	if err != nil {
		ddl.logger.Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	if qec.onlineDDLTable != nil {
		ddl.logger.Info("finish online ddl and clear online ddl metadata in optimistic shard mode",
			zap.String("event", "query"),
			zap.Strings("ddls", qec.needHandleDDLs),
			zap.String("raw statement", qec.originSQL),
			zap.Stringer("table", qec.onlineDDLTable))
		err = ddl.s.onlineDDL.Finish(qec.tctx, qec.onlineDDLTable)
		if err != nil {
			return terror.Annotatef(err, "finish online ddl on %v", qec.onlineDDLTable)
		}
	}

	// we don't resolveOptimisticDDL here because it may cause correctness problem
	// There are two cases if we receive ConflictNone here:
	// 1. This shard table is the only shard table on this worker. We don't need to redirect in this case.
	// 2. This shard table isn't the only shard table. The conflicted table before will receive a redirection event.
	// If we resolveOptimisticDDL here, if this ddl event is idempotent, it may falsely resolve the conflict which
	// has a totally different ddl.

	ddl.logger.Info("finish to handle ddls in optimistic shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	return nil
}
