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
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/failpoint"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	tidbddl "github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/filter"
	tidbmock "github.com/pingcap/tidb/pkg/util/mock"
	regexprrouter "github.com/pingcap/tidb/pkg/util/regexpr-router"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	parserpkg "github.com/pingcap/tiflow/dm/pkg/parser"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	onlineddl "github.com/pingcap/tiflow/dm/syncer/online-ddl-tools"
	sm "github.com/pingcap/tiflow/dm/syncer/safe-mode"
	"github.com/pingcap/tiflow/dm/syncer/shardddl"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type shardDDLStrategy interface {
	// when preFilter returns true, it means we should skip this DDL
	preFilter(ddlInfo *ddlInfo, qec *queryEventContext, sourceTable *filter.Table, targetTable *filter.Table) (bool, error)
	// handle DDL handles query event
	handleDDL(qec *queryEventContext) error
}

type DDLWorker struct {
	logger log.Logger

	strategy shardDDLStrategy

	binlogFilter               *bf.BinlogEvent
	metricsProxies             *metrics.Proxies
	name                       string
	workerName                 string
	sourceID                   string
	enableGTID                 bool
	shardMode                  string
	upstreamTZStr              string
	onlineDDL                  onlineddl.OnlinePlugin
	checkpoint                 CheckPoint
	tableRouter                *regexprrouter.RouteTable
	sourceTableNamesFlavor     conn.LowerCaseTableNamesFlavor
	collationCompatible        string
	charsetAndDefaultCollation map[string]string
	idAndCollationMap          map[int]string
	baList                     *filter.Filter

	getTableInfo            func(tctx *tcontext.Context, sourceTable, targetTable *filter.Table) (*model.TableInfo, error)
	getDBInfoFromDownstream func(tctx *tcontext.Context, sourceTable, targetTable *filter.Table) (*model.DBInfo, error)
	recordSkipSQLsLocation  func(ec *eventContext) error
	trackDDL                func(usedSchema string, trackInfo *ddlInfo, ec *eventContext) error
	saveTablePoint          func(table *filter.Table, location binlog.Location)
	flushJobs               func() error
}

// NewDDLWorker creates a new DDLWorker instance.
func NewDDLWorker(pLogger *log.Logger, syncer *Syncer) *DDLWorker {
	ddlWorker := &DDLWorker{
		logger:                     pLogger.WithFields(zap.String("component", "ddl")),
		binlogFilter:               syncer.binlogFilter,
		metricsProxies:             syncer.metricsProxies,
		name:                       syncer.cfg.Name,
		workerName:                 syncer.cfg.WorkerName,
		sourceID:                   syncer.cfg.SourceID,
		enableGTID:                 syncer.cfg.EnableGTID,
		shardMode:                  syncer.cfg.ShardMode,
		upstreamTZStr:              syncer.upstreamTZStr,
		onlineDDL:                  syncer.onlineDDL,
		checkpoint:                 syncer.checkpoint,
		tableRouter:                syncer.tableRouter,
		sourceTableNamesFlavor:     syncer.SourceTableNamesFlavor,
		collationCompatible:        syncer.cfg.CollationCompatible,
		charsetAndDefaultCollation: syncer.charsetAndDefaultCollation,
		idAndCollationMap:          syncer.idAndCollationMap,
		baList:                     syncer.baList,
		recordSkipSQLsLocation:     syncer.recordSkipSQLsLocation,
		trackDDL:                   syncer.trackDDL,
		saveTablePoint:             syncer.saveTablePoint,
		flushJobs:                  syncer.flushJobs,
		getTableInfo:               syncer.getTableInfo,
		getDBInfoFromDownstream:    syncer.getDBInfoFromDownstream,
	}
	switch syncer.cfg.ShardMode {
	case config.ShardPessimistic:
		ddlWorker.strategy = NewPessimistDDL(&ddlWorker.logger, syncer)
	case config.ShardOptimistic:
		ddlWorker.strategy = NewOptimistDDL(&ddlWorker.logger, syncer)
	default:
		ddlWorker.strategy = NewNormalDDL(&ddlWorker.logger, syncer)
	}
	return ddlWorker
}

type Normal struct {
	logger log.Logger

	trackDDL      func(usedSchema string, trackInfo *ddlInfo, ec *eventContext) error
	onlineDDL     onlineddl.OnlinePlugin
	handleJobFunc func(*job) (bool, error)
	execError     *atomic.Error
}

func NewNormalDDL(pLogger *log.Logger, syncer *Syncer) *Normal {
	return &Normal{
		logger:        pLogger.WithFields(zap.String("mode", "normal")),
		trackDDL:      syncer.trackDDL,
		onlineDDL:     syncer.onlineDDL,
		handleJobFunc: syncer.handleJobFunc,
		execError:     &syncer.execError,
	}
}

type Pessimist struct {
	logger         log.Logger
	sgk            *ShardingGroupKeeper
	checkpoint     CheckPoint
	onlineDDL      onlineddl.OnlinePlugin
	metricsProxies *metrics.Proxies
	name           string
	sourceID       string

	execError        *atomic.Error
	safeMode         *sm.SafeMode
	trackDDL         func(usedSchema string, trackInfo *ddlInfo, ec *eventContext) error
	handleJobFunc    func(*job) (bool, error)
	flushCheckPoints func() error
	saveTablePoint   func(table *filter.Table, location binlog.Location)
	pessimist        *shardddl.Pessimist // shard DDL pessimist
}

func NewPessimistDDL(pLogger *log.Logger, syncer *Syncer) *Pessimist {
	return &Pessimist{
		logger:           pLogger.WithFields(zap.String("mode", "pessimist")),
		sgk:              syncer.sgk,
		checkpoint:       syncer.checkpoint,
		onlineDDL:        syncer.onlineDDL,
		metricsProxies:   syncer.metricsProxies,
		name:             syncer.cfg.Name,
		sourceID:         syncer.cfg.SourceID,
		execError:        &syncer.execError,
		safeMode:         syncer.safeMode,
		trackDDL:         syncer.trackDDL,
		handleJobFunc:    syncer.handleJobFunc,
		flushCheckPoints: syncer.flushCheckPoints,
		saveTablePoint:   syncer.saveTablePoint,
		pessimist:        syncer.pessimist,
	}
}

type Optimist struct {
	logger log.Logger

	schemaTracker *schema.Tracker
	flavor        string
	enableGTID    bool
	onlineDDL     onlineddl.OnlinePlugin
	checkpoint    CheckPoint
	trackDDL      func(usedSchema string, trackInfo *ddlInfo, ec *eventContext) error
	handleJobFunc func(*job) (bool, error)
	osgk          *OptShardingGroupKeeper // optimistic ddl's keeper to keep all sharding (sub) group in this syncer
	getTableInfo  func(tctx *tcontext.Context, sourceTable, targetTable *filter.Table) (*model.TableInfo, error)
	execError     *atomic.Error
	optimist      *shardddl.Optimist // shard DDL optimist
	strict        bool
}

func NewOptimistDDL(pLogger *log.Logger, syncer *Syncer) *Optimist {
	return &Optimist{
		logger:        pLogger.WithFields(zap.String("mode", "optimist")),
		schemaTracker: syncer.schemaTracker,
		flavor:        syncer.cfg.Flavor,
		enableGTID:    syncer.cfg.EnableGTID,
		onlineDDL:     syncer.onlineDDL,
		checkpoint:    syncer.checkpoint,
		trackDDL:      syncer.trackDDL,
		handleJobFunc: syncer.handleJobFunc,
		osgk:          syncer.osgk,
		getTableInfo:  syncer.getTableInfo,
		execError:     &syncer.execError,
		optimist:      syncer.optimist,
		strict:        syncer.cfg.StrictOptimisticShardMode,
	}
}

func (ddl *DDLWorker) HandleQueryEvent(ev *replication.QueryEvent, ec eventContext, originSQL string) (err error) {
	if originSQL == "BEGIN" {
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
		needSkip, err2 := skipSQLByPattern(ddl.binlogFilter, qec.originSQL)
		if err2 != nil {
			err = err2
			return
		}
		if !needSkip {
			return
		}
		// don't return error if filter success
		ddl.metricsProxies.SkipBinlogDurationHistogram.WithLabelValues("query", ddl.name, ddl.sourceID).Observe(time.Since(ec.startTime).Seconds())
		ddl.logger.Warn("skip event", zap.String("event", "query"), zap.Stringer("query event context", qec))
		err = ddl.recordSkipSQLsLocation(&ec)
	}()

	qec.p, err = event.GetParserForStatusVars(ev.StatusVars)
	if err != nil {
		ddl.logger.Warn("found error when getting sql_mode from binlog status_vars", zap.Error(err))
	}

	qec.timezone, err = event.GetTimezoneByStatusVars(ev.StatusVars, ddl.upstreamTZStr)
	// no timezone information retrieved and upstream timezone not previously set
	if err != nil && ddl.upstreamTZStr == "" {
		ddl.logger.Warn("found error when getting timezone from binlog status_vars", zap.Error(err))
	}

	qec.timestamp = ec.header.Timestamp

	stmt, err := parseOneStmt(qec)
	if err != nil {
		return err
	}

	if _, ok := stmt.(ast.DDLNode); !ok {
		ddl.logger.Info("ddl that dm doesn't handle, skip it", zap.String("event", "query"),
			zap.Stringer("queryEventContext", qec))
		return ddl.recordSkipSQLsLocation(qec.eventContext)
	}

	if qec.shardingReSync != nil {
		qec.shardingReSync.currLocation = qec.endLocation
		// TODO: refactor this, see https://github.com/pingcap/tiflow/issues/6691
		// for optimistic ddl, we can resync idemponent ddl.
		cmp := binlog.CompareLocation(qec.shardingReSync.currLocation, qec.shardingReSync.latestLocation, ddl.enableGTID)
		if cmp > 0 || (cmp == 0 && ddl.shardMode != config.ShardOptimistic) {
			ddl.logger.Info("re-replicate shard group was completed", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
			return qec.closeShardingResync()
		} else if ddl.shardMode != config.ShardOptimistic {
			ddl.logger.Debug("skip event in re-replicating sharding group", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
			return nil
		}
		// optimistic shard mode handle situation will be handled through table point after
		// we split ddls and handle the appliedDDLs
	}

	ddl.logger.Info("ready to split ddl", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

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
		sqls, err2 := ddl.processOneDDL(qec, sql)
		if err2 != nil {
			ddl.logger.Error("fail to process ddl", zap.String("event", "query"), zap.Stringer("queryEventContext", qec), log.ShortError(err2))
			return err2
		}
		qec.appliedDDLs = append(qec.appliedDDLs, sqls...)
	}
	ddl.logger.Info("resolve sql", zap.String("event", "query"), zap.Strings("appliedDDLs", qec.appliedDDLs), zap.Stringer("queryEventContext", qec))

	ddl.metricsProxies.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageGenQuery, ddl.name, ddl.workerName, ddl.sourceID).Observe(time.Since(qec.startTime).Seconds())

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
		ddlInfo, err2 := ddl.genDDLInfo(qec, sql)
		if err2 != nil {
			return err2
		}
		sourceTable := ddlInfo.sourceTables[0]
		targetTable := ddlInfo.targetTables[0]
		if len(ddlInfo.routedDDL) == 0 {
			ddl.metricsProxies.SkipBinlogDurationHistogram.WithLabelValues("query", ddl.name, ddl.sourceID).Observe(time.Since(qec.startTime).Seconds())
			ddl.logger.Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", qec.ddlSchema))
			continue
		}

		// DDL is sequentially synchronized in this syncer's main process goroutine
		// filter DDL that is older or same as table checkpoint, to avoid sync again for already synced DDLs
		if ddl.checkpoint.IsOlderThanTablePoint(sourceTable, qec.endLocation) {
			ddl.logger.Info("filter obsolete DDL", zap.String("event", "query"), zap.String("statement", sql), log.WrapStringerField("location", qec.endLocation))
			continue
		}

		// pre-filter of sharding
		if filter, err2 := ddl.strategy.preFilter(ddlInfo, qec, sourceTable, targetTable); err2 != nil {
			return err2
		} else if filter {
			continue
		}

		qec.needHandleDDLs = append(qec.needHandleDDLs, ddlInfo.routedDDL)
		qec.trackInfos = append(qec.trackInfos, ddlInfo)
		// TODO: current table checkpoints will be deleted in track ddls, but created and updated in flush checkpoints,
		//       we should use a better mechanism to combine these operations
		if ddl.shardMode == "" {
			recordSourceTbls(qec.sourceTbls, ddlInfo.stmtCache, sourceTable)
		}
	}

	ddl.logger.Info("prepare to handle ddls", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	if len(qec.needHandleDDLs) == 0 {
		ddl.logger.Info("skip event, need handled ddls is empty", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
		return ddl.recordSkipSQLsLocation(qec.eventContext)
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
	if err = ddl.flushJobs(); err != nil {
		return err
	}

	return ddl.strategy.handleDDL(qec)
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
		if err := ddl.trackDDL(qec.ddlSchema, trackInfo, qec.eventContext); err != nil {
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
	_, err := ddl.handleJobFunc(job)
	if err != nil {
		return err
	}

	// when add ddl job, will execute ddl and then flush checkpoint.
	// if execute ddl failed, the execError will be set to that error.
	// return nil here to avoid duplicate error message
	err = ddl.execError.Load()
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
		err2 := ddl.onlineDDL.Finish(qec.tctx, qec.onlineDDLTable)
		if err2 != nil {
			return terror.Annotatef(err2, "finish online ddl on %v", qec.onlineDDLTable)
		}
	}

	return nil
}

func (ddl *Pessimist) preFilter(ddlInfo *ddlInfo, qec *queryEventContext, sourceTable *filter.Table, targetTable *filter.Table) (bool, error) {
	switch ddlInfo.stmtCache.(type) {
	case *ast.DropDatabaseStmt:
		err := ddl.dropSchemaInSharding(qec.tctx, sourceTable.Schema)
		if err != nil {
			return false, err
		}
		return true, nil
	case *ast.DropTableStmt:
		sourceTableID := utils.GenTableID(sourceTable)
		err := ddl.sgk.LeaveGroup(targetTable, []string{sourceTableID})
		if err != nil {
			return false, err
		}
		err = ddl.checkpoint.DeleteTablePoint(qec.tctx, sourceTable)
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
		startLocation = qec.startLocation
		endLocation   = qec.endLocation
	)

	var annotate string
	switch ddlInfo.stmtCache.(type) {
	case *ast.CreateDatabaseStmt:
		// for CREATE DATABASE, we do nothing. when CREATE TABLE under this DATABASE, sharding groups will be added
	case *ast.CreateTableStmt:
		// for CREATE TABLE, we add it to group
		needShardingHandle, group, synced, remain, err = ddl.sgk.AddGroup(ddlInfo.targetTables[0], []string{sourceTableID}, nil, true)
		if err != nil {
			return err
		}
		annotate = "add table to shard group"
	default:
		needShardingHandle, group, synced, active, remain, err = ddl.sgk.TrySync(ddlInfo.sourceTables[0], ddlInfo.targetTables[0], startLocation, qec.endLocation, needHandleDDLs)
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
		if err = ddl.trackDDL(qec.ddlSchema, trackInfo, qec.eventContext); err != nil {
			return err
		}
	}

	if needShardingHandle {
		ddl.metricsProxies.UnsyncedTableGauge.WithLabelValues(ddl.name, ddlInfo.targetTables[0].String(), ddl.sourceID).Set(float64(remain))
		err = ddl.safeMode.IncrForTable(qec.tctx, ddlInfo.targetTables[0]) // try enable safe-mode when starting syncing for sharding group
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
			log.WrapStringerField("end location", endLocation))
		ddl.saveTablePoint(ddlInfo.sourceTables[0], endLocation)
		if !synced {
			ddl.logger.Info("source shard group is not synced",
				zap.String("event", "query"),
				zap.String("sourceTableID", sourceTableID),
				zap.Stringer("start location", startLocation),
				log.WrapStringerField("end location", endLocation))
			return nil
		}

		ddl.logger.Info("source shard group is synced",
			zap.String("event", "query"),
			zap.String("sourceTableID", sourceTableID),
			zap.Stringer("start location", startLocation),
			log.WrapStringerField("end location", endLocation))
		err = ddl.safeMode.DescForTable(qec.tctx, ddlInfo.targetTables[0]) // try disable safe-mode after sharding group synced
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

		allResolved, err2 := ddl.sgk.ResolveShardingDDL(ddlInfo.targetTables[0])
		if err2 != nil {
			return err2
		}
		*qec.shardingReSyncCh <- &ShardingReSync{
			currLocation:   *firstEndLocation,
			latestLocation: endLocation,
			targetTable:    ddlInfo.targetTables[0],
			allResolved:    allResolved,
		}

		// Don't send new DDLInfo to dm-master until all local sql jobs finished
		// since jobWg is flushed by flushJobs before, we don't wait here any more

		// NOTE: if we need singleton Syncer (without dm-master) to support sharding DDL sync
		// we should add another config item to differ, and do not save DDLInfo, and not wait for ddlExecInfo

		// construct & send shard DDL info into etcd, DM-master will handle it.
		shardInfo := ddl.pessimist.ConstructInfo(ddlInfo.targetTables[0].Schema, ddlInfo.targetTables[0].Name, needHandleDDLs)
		rev, err2 := ddl.pessimist.PutInfo(qec.tctx.Ctx, shardInfo)
		if err2 != nil {
			return err2
		}
		ddl.metricsProxies.Metrics.ShardLockResolving.Set(1) // block and wait DDL lock to be synced
		ddl.logger.Info("putted shard DDL info", zap.Stringer("info", shardInfo), zap.Int64("revision", rev))

		shardOp, err2 := ddl.pessimist.GetOperation(qec.tctx.Ctx, shardInfo, rev+1)
		ddl.metricsProxies.Metrics.ShardLockResolving.Set(0)
		if err2 != nil {
			return err2
		}

		if shardOp.Exec {
			failpoint.Inject("ShardSyncedExecutionExit", func() {
				ddl.logger.Warn("exit triggered", zap.String("failpoint", "ShardSyncedExecutionExit"))
				//nolint:errcheck
				ddl.flushCheckPoints()
				utils.OsExit(1)
			})
			failpoint.Inject("SequenceShardSyncedExecutionExit", func() {
				group := ddl.sgk.Group(ddlInfo.targetTables[0])
				if group != nil {
					// exit in the first round sequence sharding DDL only
					if group.meta.ActiveIdx() == 1 {
						ddl.logger.Warn("exit triggered", zap.String("failpoint", "SequenceShardSyncedExecutionExit"))
						//nolint:errcheck
						ddl.flushCheckPoints()
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
	_, err = ddl.handleJobFunc(job)
	if err != nil {
		return err
	}

	err = ddl.execError.Load()
	if err != nil {
		ddl.logger.Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	if qec.onlineDDLTable != nil {
		err = ddl.clearOnlineDDL(qec.tctx, ddlInfo.targetTables[0])
		if err != nil {
			return err
		}
	}

	ddl.logger.Info("finish to handle ddls in shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	return nil
}

func (ddl *Optimist) preFilter(ddlInfo *ddlInfo, qec *queryEventContext, sourceTable *filter.Table, targetTable *filter.Table) (bool, error) {
	if ddl.osgk.inConflictStage(sourceTable, targetTable) {
		// if in unsync stage and not before active DDL, filter it
		// if in sharding re-sync stage and not before active DDL (the next DDL to be synced), filter it
		ddl.logger.Info("replicate sharding DDL, filter Conflicted table's ddl events",
			zap.String("event", "query"),
			zap.Stringer("source", sourceTable),
			log.WrapStringerField("location", qec.endLocation))
		return true, nil
	} else if qec.shardingReSync != nil && qec.shardingReSync.targetTable.String() != targetTable.String() {
		// in re-syncing, ignore non current sharding group's events
		ddl.logger.Info("skip event in re-replicating shard group", zap.String("event", "query"), zap.Stringer("re-shard", qec.shardingReSync))
		return true, nil
	}
	switch ddlInfo.stmtCache.(type) {
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

	err = ddl.execError.Load()
	if err != nil {
		ddl.logger.Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	switch trackInfos[0].stmtCache.(type) {
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
		if _, ok := trackInfos[0].stmtCache.(*ast.CreateTableStmt); !ok {
			tiBefore, err = ddl.getTableInfo(qec.tctx, upTable, downTable)
			if err != nil {
				return err
			}
		}
	}

	for _, trackInfo := range trackInfos {
		if err = ddl.trackDDL(qec.ddlSchema, trackInfo, qec.eventContext); err != nil {
			return err
		}
		if !isDBDDL {
			tiAfter, err = ddl.getTableInfo(qec.tctx, upTable, downTable)
			if err != nil {
				return err
			}
			tisAfter = append(tisAfter, tiAfter)
		}
	}

	// in optimistic mode, don't `saveTablePoint` before execute DDL,
	// because it has no `UnresolvedTables` to prevent the flush of this checkpoint.

	info := ddl.optimist.ConstructInfo(upTable.Schema, upTable.Name, downTable.Schema, downTable.Name, qec.needHandleDDLs, tiBefore, tisAfter)

	var (
		rev    int64
		skipOp bool
		op     optimism.Operation
	)
	switch trackInfos[0].stmtCache.(type) {
	case *ast.CreateDatabaseStmt, *ast.AlterDatabaseStmt:
		// need to execute the DDL to the downstream, but do not do the coordination with DM-master.
		op.DDLs = qec.needHandleDDLs
		skipOp = true
	case *ast.DropDatabaseStmt:
		skipOp = true
		ddl.osgk.RemoveSchema(upTable.Schema)
	case *ast.CreateTableStmt:
		// need to execute the DDL to the downstream, but do not do the coordination with DM-master.
		op.DDLs = qec.needHandleDDLs
		skipOp = true
		if err = ddl.checkpoint.FlushPointsWithTableInfos(qec.tctx, []*filter.Table{upTable}, []*model.TableInfo{tiAfter}); err != nil {
			ddl.logger.Error("failed to flush create table info", zap.Stringer("table", upTable), zap.Strings("ddls", qec.needHandleDDLs), log.ShortError(err))
		}
		if _, err = ddl.optimist.AddTable(info); err != nil {
			return err
		}
	case *ast.DropTableStmt:
		skipOp = true
		if _, err = ddl.optimist.RemoveTable(info); err != nil {
			return err
		}
		ddl.osgk.RemoveGroup(downTable, []string{utils.GenTableID(upTable)})
	default:
		rev, err = ddl.optimist.PutInfo(info)
		if err != nil {
			return err
		}
	}

	ddl.logger.Info("putted a shard DDL info into etcd", zap.Stringer("info", info))
	if !skipOp {
		for {
			op, err = ddl.optimist.GetOperation(qec.tctx.Ctx, info, rev+1)
			if err != nil {
				return err
			}
			ddl.logger.Info("got a shard DDL lock operation", zap.Stringer("operation", op))
			if op.ConflictStage != optimism.ConflictDetected {
				break
			}
			if ddl.strict {
				return terror.ErrSyncerShardDDLConflict.Generate(qec.needHandleDDLs, op.ConflictMsg)
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
		if ddl.strict {
			return terror.ErrSyncerShardDDLConflict.Generate(qec.needHandleDDLs, "")
		}
		// TODO: check if we don't need Clone for startLocation
		first := ddl.osgk.appendConflictTable(upTable, downTable, qec.startLocation.Clone(), ddl.flavor, ddl.enableGTID)
		if first {
			ddl.optimist.GetRedirectOperation(qec.tctx.Ctx, info, op.Revision+1)
		}
		// This conflicted ddl is not executed in downstream, so we need to revert tableInfo in schemaTracker to `tiBefore`.
		err = ddl.schemaTracker.DropTable(upTable)
		if err != nil {
			ddl.logger.Error("fail to drop table to rollback table in schema tracker", zap.Stringer("table", upTable))
		} else {
			err = ddl.schemaTracker.CreateTableIfNotExists(upTable, tiBefore)
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
	_, err = ddl.handleJobFunc(job)
	if err != nil {
		return err
	}

	err = ddl.execError.Load()
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
		err = ddl.onlineDDL.Finish(qec.tctx, qec.onlineDDLTable)
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

// copy from https://github.com/pingcap/tidb/blob/fc4f8a1d8f5342cd01f78eb460e47d78d177ed20/ddl/column.go#L366
func (ddl *DDLWorker) needChangeColumnData(oldCol, newCol *table.Column) bf.EventType {
	toUnsigned := mysql.HasUnsignedFlag(newCol.GetFlag())
	originUnsigned := mysql.HasUnsignedFlag(oldCol.GetFlag())
	needTruncationOrToggleSign := func() bool {
		return (newCol.GetFlen() > 0 && (newCol.GetFlen() < oldCol.GetFlen() || newCol.GetDecimal() < oldCol.GetDecimal())) ||
			(toUnsigned != originUnsigned)
	}
	// Ignore the potential max display length represented by integer's flen, use default flen instead.
	defaultOldColFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(oldCol.GetType())
	defaultNewColFlen, _ := mysql.GetDefaultFieldLengthAndDecimal(newCol.GetType())
	needTruncationOrToggleSignForInteger := func() bool {
		return (defaultNewColFlen > 0 && defaultNewColFlen < defaultOldColFlen) || (toUnsigned != originUnsigned)
	}

	// Deal with the same type.
	if oldCol.GetType() == newCol.GetType() {
		switch oldCol.GetType() {
		case mysql.TypeNewDecimal:
			// Since type decimal will encode the precision, frac, negative(signed) and wordBuf into storage together, there is no short
			// cut to eliminate data reorg change for column type change between decimal.
			if oldCol.GetFlen() != newCol.GetFlen() || oldCol.GetDecimal() != newCol.GetDecimal() || toUnsigned != originUnsigned {
				return bf.PrecisionDecrease
			}
			return bf.AlterTable
		case mysql.TypeEnum, mysql.TypeSet:
			if tidbddl.IsElemsChangedToModifyColumn(oldCol.GetElems(), newCol.GetElems()) {
				return bf.ValueRangeDecrease
			}
			return bf.AlterTable
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			if toUnsigned != originUnsigned {
				return bf.PrecisionDecrease
			}
			return bf.AlterTable
		case mysql.TypeString:
			// Due to the behavior of padding \x00 at binary type, always change column data when binary length changed
			if types.IsBinaryStr(&oldCol.FieldType) {
				if newCol.GetFlen() != oldCol.GetFlen() {
					return bf.PrecisionDecrease
				}
			}
			return bf.AlterTable
		}

		if needTruncationOrToggleSign() {
			return bf.ValueRangeDecrease
		}
		return bf.AlterTable
	}

	if tidbddl.ConvertBetweenCharAndVarchar(oldCol.GetType(), newCol.GetType()) {
		return bf.ModifyColumn
	}

	// Deal with the different type.
	switch oldCol.GetType() {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		switch newCol.GetType() {
		case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			if needTruncationOrToggleSign() {
				return bf.ModifyColumn
			}
			return bf.AlterTable
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		switch newCol.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
			if needTruncationOrToggleSignForInteger() {
				return bf.ValueRangeDecrease
			}
			return bf.AlterTable
		}
	}

	//  The rest is considered as ModifyColumn.
	return bf.ModifyColumn
}

func (ddl *DDLWorker) handleModifyColumn(qec *queryEventContext, info *ddlInfo, spec *ast.AlterTableSpec) (bf.EventType, error) {
	// return AlterTable if any error happened
	// avoid panic, won't happen
	if len(info.sourceTables) == 0 || len(info.targetTables) == 0 {
		return bf.AlterTable, nil
	}
	if len(spec.NewColumns) == 0 || spec.NewColumns[0].Tp == nil {
		return bf.AlterTable, nil
	}

	// get table info and db info
	ti, err := ddl.getTableInfo(qec.tctx, info.sourceTables[0], info.targetTables[0])
	if err != nil || ti == nil {
		return bf.AlterTable, err
	}
	tbl := tables.MockTableFromMeta(ti)
	di, err := ddl.getDBInfoFromDownstream(qec.tctx, info.sourceTables[0], info.targetTables[0])
	if err != nil || di == nil {
		return bf.AlterTable, err
	}

	// get old and new column
	oldColumnName := spec.OldColumnName
	if spec.Tp == ast.AlterTableModifyColumn {
		oldColumnName = spec.NewColumns[0].Name
	}
	oldCol := table.FindCol(tbl.Cols(), oldColumnName.Name.L)
	if oldCol == nil {
		return bf.AlterTable, nil
	}
	newCol := table.ToColumn(&model.ColumnInfo{
		ID:                    oldCol.ID,
		Offset:                oldCol.Offset,
		State:                 oldCol.State,
		OriginDefaultValue:    oldCol.OriginDefaultValue,
		OriginDefaultValueBit: oldCol.OriginDefaultValueBit,
		FieldType:             *spec.NewColumns[0].Tp,
		Name:                  spec.NewColumns[0].Name.Name,
		Version:               oldCol.Version,
	})

	// handle charset and collation
	if err := tidbddl.ProcessColumnCharsetAndCollation(tidbmock.NewContext(), oldCol, newCol, ti, spec.NewColumns[0], di); err != nil {
		ddl.logger.Warn("process column charset and collation failed", zap.Error(err))
		return bf.AlterTable, err
	}
	// handle column options
	if err := tidbddl.ProcessColumnOptions(tidbmock.NewContext(), newCol, spec.NewColumns[0].Options); err != nil {
		ddl.logger.Warn("process column options failed", zap.Error(err))
		return bf.AlterTable, err
	}

	if et := ddl.needChangeColumnData(oldCol, newCol); et != bf.AlterTable {
		return et, nil
	}
	switch {
	case mysql.HasAutoIncrementFlag(oldCol.GetFlag()) && !mysql.HasAutoIncrementFlag(newCol.GetFlag()):
		return bf.RemoveAutoIncrement, nil
	case mysql.HasPriKeyFlag(oldCol.GetFlag()) && !mysql.HasPriKeyFlag(newCol.GetFlag()):
		return bf.DropPrimaryKey, nil
	case mysql.HasUniKeyFlag(oldCol.GetFlag()) && !mysql.HasUniKeyFlag(newCol.GetFlag()):
		return bf.DropUniqueKey, nil
	case oldCol.GetDefaultValue() != newCol.GetDefaultValue():
		return bf.ModifyDefaultValue, nil
	case oldCol.GetCharset() != newCol.GetCharset():
		return bf.ModifyCharset, nil
	case oldCol.GetCollate() != newCol.GetCollate():
		return bf.ModifyCollation, nil
	case spec.Position != nil && spec.Position.Tp != ast.ColumnPositionNone:
		return bf.ModifyColumnsOrder, nil
	case oldCol.Name.L != newCol.Name.L:
		return bf.RenameColumn, nil
	default:
		return bf.AlterTable, nil
	}
}

// AstToDDLEvent returns filter.DDLEvent.
func (ddl *DDLWorker) AstToDDLEvent(qec *queryEventContext, info *ddlInfo) (et bf.EventType) {
	defer func() {
		ddl.logger.Info("get ddl event type", zap.String("event_type", string(et)))
	}()
	node := info.stmtCache
	switch n := node.(type) {
	case *ast.AlterTableStmt:
		validSpecs, err := tidbddl.ResolveAlterTableSpec(tidbmock.NewContext(), n.Specs)
		if err != nil {
			break
		}

		for _, spec := range validSpecs {
			switch spec.Tp {
			case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
				et, err := ddl.handleModifyColumn(qec, info, spec)
				if err != nil {
					ddl.logger.Warn("handle modify column failed", zap.Error(err))
				}
				return et
			case ast.AlterTableRenameColumn:
				return bf.RenameColumn
			case ast.AlterTableRenameIndex:
				return bf.RenameIndex
			case ast.AlterTableRenameTable:
				return bf.RenameTable
			case ast.AlterTableDropColumn:
				return bf.DropColumn
			case ast.AlterTableDropIndex:
				return bf.DropIndex
			case ast.AlterTableDropPartition:
				return bf.DropTablePartition
			case ast.AlterTableDropPrimaryKey:
				return bf.DropPrimaryKey
			case ast.AlterTableTruncatePartition:
				return bf.TruncateTablePartition
			case ast.AlterTableAlterColumn:
				return bf.ModifyDefaultValue
			case ast.AlterTableAddConstraint:
				return bf.ModifyConstraint
			case ast.AlterTableOption:
				for _, opt := range spec.Options {
					switch opt.Tp {
					case ast.TableOptionCharset:
						return bf.ModifyCharset
					case ast.TableOptionCollate:
						return bf.ModifyCollation
					case ast.TableOptionEngine:
						return bf.ModifyStorageEngine
					}
				}
			case ast.AlterTableReorganizePartition:
				return bf.ReorganizePartition
			case ast.AlterTableRebuildPartition:
				return bf.RebuildPartition
			case ast.AlterTableCoalescePartitions:
				return bf.CoalescePartition
			case ast.AlterTableExchangePartition:
				return bf.ExchangePartition
			}
		}
	}
	return bf.AstToDDLEvent(node)
}

// skipQueryEvent if skip by binlog-filter:
// * track the ddlInfo;
// * changes ddlInfo.originDDL to empty string.
func (ddl *DDLWorker) skipQueryEvent(qec *queryEventContext, ddlInfo *ddlInfo) (bool, error) {
	if utils.IsBuildInSkipDDL(qec.originSQL) {
		return true, nil
	}
	et := ddl.AstToDDLEvent(qec, ddlInfo)
	// get real tables before apply block-allow list
	realTables := make([]*filter.Table, 0, len(ddlInfo.sourceTables))
	for _, table := range ddlInfo.sourceTables {
		realTableName := table.Name
		if ddl.onlineDDL != nil {
			realTableName = ddl.onlineDDL.RealName(table.Name)
		}
		realTables = append(realTables, &filter.Table{
			Schema: table.Schema,
			Name:   realTableName,
		})
	}
	for _, table := range realTables {
		ddl.logger.Debug("query event info", zap.String("event", "query"), zap.String("origin sql", qec.originSQL), zap.Stringer("table", table), zap.Stringer("ddl info", ddlInfo))
		if skipByTable(ddl.baList, table) {
			ddl.logger.Debug("skip event by balist")
			return true, nil
		}
		needSkip, err := skipByFilter(ddl.binlogFilter, table, et, qec.originSQL)
		if err != nil {
			return needSkip, err
		}

		if needSkip {
			ddl.logger.Debug("skip event by binlog filter")
			// In the case of online-ddl, if the generated table is skipped, track ddl will failed.
			err := ddl.trackDDL(qec.ddlSchema, ddlInfo, qec.eventContext)
			if err != nil {
				ddl.logger.Warn("track ddl failed", zap.Stringer("ddl info", ddlInfo))
			}
			ddl.saveTablePoint(table, qec.lastLocation)
			ddl.logger.Warn("track skipped ddl and return empty string", zap.String("origin sql", qec.originSQL), zap.Stringer("ddl info", ddlInfo))
			ddlInfo.originDDL = ""
			return true, nil
		}
	}
	return false, nil
}

// processOneDDL processes already split ddl as following step:
// 1. generate ddl info;
// 2. skip sql by skipQueryEvent;
// 3. apply online ddl if onlineDDL is not nil:
//   - specially, if skip, apply empty string;
func (ddl *DDLWorker) processOneDDL(qec *queryEventContext, sql string) ([]string, error) {
	ddlInfo, err := ddl.genDDLInfo(qec, sql)
	if err != nil {
		return nil, err
	}

	if ddl.onlineDDL != nil {
		if err = ddl.onlineDDL.CheckRegex(ddlInfo.stmtCache, qec.ddlSchema, ddl.sourceTableNamesFlavor); err != nil {
			return nil, err
		}
	}

	qec.tctx.L().Debug("will check skip query event", zap.String("event", "query"), zap.String("statement", sql), zap.Stringer("ddlInfo", ddlInfo))
	shouldSkip, err := ddl.skipQueryEvent(qec, ddlInfo)
	if err != nil {
		return nil, err
	}
	if shouldSkip {
		ddl.metricsProxies.SkipBinlogDurationHistogram.WithLabelValues("query", ddl.name, ddl.sourceID).Observe(time.Since(qec.startTime).Seconds())
		qec.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.Stringer("query event context", qec))
		if ddl.onlineDDL == nil || len(ddlInfo.originDDL) != 0 {
			return nil, nil
		}
	}

	if ddl.onlineDDL == nil {
		return []string{ddlInfo.originDDL}, nil
	}
	// filter and save ghost table ddl
	sqls, err := ddl.onlineDDL.Apply(qec.tctx, ddlInfo.sourceTables, ddlInfo.originDDL, ddlInfo.stmtCache, qec.p)
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
func (ddl *DDLWorker) genDDLInfo(qec *queryEventContext, sql string) (*ddlInfo, error) {
	stmt, err := qec.p.ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, terror.Annotatef(terror.ErrSyncerUnitParseStmt.New(err.Error()), "ddl %s", sql)
	}
	// get another stmt, one for representing original ddl, one for letting other function modify it.
	stmt2, _ := qec.p.ParseOneStmt(sql, "", "")

	sourceTables, err := parserpkg.FetchDDLTables(qec.ddlSchema, stmt, ddl.sourceTableNamesFlavor)
	if err != nil {
		return nil, err
	}

	targetTables := make([]*filter.Table, 0, len(sourceTables))
	for i := range sourceTables {
		renamedTable := route(ddl.tableRouter, sourceTables[i])
		targetTables = append(targetTables, renamedTable)
	}

	ddlInfo := &ddlInfo{
		originDDL:    sql,
		originStmt:   stmt,
		stmtCache:    stmt2,
		sourceTables: sourceTables,
		targetTables: targetTables,
	}

	// "strict" will adjust collation
	if ddl.collationCompatible == config.StrictCollationCompatible {
		ddl.adjustCollation(ddlInfo, qec.eventStatusVars, ddl.charsetAndDefaultCollation, ddl.idAndCollationMap)
	}

	routedDDL, err := parserpkg.RenameDDLTable(ddlInfo.stmtCache, ddlInfo.targetTables)
	ddlInfo.routedDDL = routedDDL
	return ddlInfo, err
}

func (ddl *Pessimist) dropSchemaInSharding(tctx *tcontext.Context, sourceSchema string) error {
	sources := make(map[string][]*filter.Table)
	sgs := ddl.sgk.Groups()
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
		err := ddl.sgk.LeaveGroup(targetTable, sourceTableIDs)
		if err != nil {
			return err
		}
	}
	// delete from checkpoint
	for _, tables := range sources {
		for _, table := range tables {
			// refine clear them later if failed
			// now it doesn't have problems
			if err1 := ddl.checkpoint.DeleteTablePoint(tctx, table); err1 != nil {
				ddl.logger.Error("fail to delete checkpoint", zap.Stringer("table", table))
			}
		}
	}
	return nil
}

func (ddl *Pessimist) clearOnlineDDL(tctx *tcontext.Context, targetTable *filter.Table) error {
	group := ddl.sgk.Group(targetTable)
	if group == nil {
		return nil
	}

	// return [[schema, table]...]
	tables := group.Tables()

	for _, table := range tables {
		ddl.logger.Info("finish online ddl", zap.Stringer("table", table))
		err := ddl.onlineDDL.Finish(tctx, table)
		if err != nil {
			return terror.Annotatef(err, "finish online ddl on %v", table)
		}
	}

	return nil
}

// adjustCollation adds collation for create database and check create table.
func (ddl *DDLWorker) adjustCollation(ddlInfo *ddlInfo, statusVars []byte, charsetAndDefaultCollationMap map[string]string, idAndCollationMap map[int]string) {
	switch createStmt := ddlInfo.stmtCache.(type) {
	case *ast.CreateTableStmt:
		if createStmt.ReferTable != nil {
			return
		}
		ddl.adjustColumnsCollation(createStmt, charsetAndDefaultCollationMap)
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
			ddl.logger.Warn("detect create table risk which use implicit charset and collation", zap.String("originSQL", ddlInfo.originDDL))
			return
		}
		// just has charset, can add collation by charset and default collation map
		collation, ok := charsetAndDefaultCollationMap[strings.ToLower(justCharset)]
		if !ok {
			ddl.logger.Warn("not found charset default collation.", zap.String("originSQL", ddlInfo.originDDL), zap.String("charset", strings.ToLower(justCharset)))
			return
		}
		ddl.logger.Info("detect create table risk which use explicit charset and implicit collation, we will add collation by INFORMATION_SCHEMA.COLLATIONS", zap.String("originSQL", ddlInfo.originDDL), zap.String("collation", collation))
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
				ddl.logger.Warn("not found charset default collation.", zap.String("originSQL", ddlInfo.originDDL), zap.String("charset", strings.ToLower(justCharset)))
				return
			}
			ddl.logger.Info("detect create database risk which use explicit charset and implicit collation, we will add collation by INFORMATION_SCHEMA.COLLATIONS", zap.String("originSQL", ddlInfo.originDDL), zap.String("collation", collation))
		} else {
			// has no charset and collation
			// add collation by server collation from binlog statusVars
			collation, err = event.GetServerCollationByStatusVars(statusVars, idAndCollationMap)
			if err != nil {
				ddl.logger.Error("can not get charset server collation from binlog statusVars.", zap.Error(err), zap.String("originSQL", ddlInfo.originDDL))
			}
			if collation == "" {
				ddl.logger.Error("get server collation from binlog statusVars is nil.", zap.Error(err), zap.String("originSQL", ddlInfo.originDDL))
				return
			}
			// add collation
			ddl.logger.Info("detect create database risk which use implicit charset and collation, we will add collation by binlog status_vars", zap.String("originSQL", ddlInfo.originDDL), zap.String("collation", collation))
		}
		createStmt.Options = append(createStmt.Options, &ast.DatabaseOption{Tp: ast.DatabaseOptionCollate, Value: collation})
	}
}

// adjustColumnsCollation adds column's collation.
func (ddl *DDLWorker) adjustColumnsCollation(createStmt *ast.CreateTableStmt, charsetAndDefaultCollationMap map[string]string) {
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
				ddl.logger.Warn("not found charset default collation for column.", zap.String("table", createStmt.Table.Name.String()), zap.String("column", col.Name.String()), zap.String("charset", strings.ToLower(fieldType.GetCharset())))
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
	stmtCache    ast.StmtNode
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
