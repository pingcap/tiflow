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
	"bytes"
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	cm "github.com/pingcap/tidb-tools/pkg/column-mapping"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tidb/util/filter"
	regexprrouter "github.com/pingcap/tidb/util/regexpr-router"
	router "github.com/pingcap/tidb/util/table-router"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/optimism"
	"github.com/pingcap/tiflow/dm/syncer/binlogstream"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/binlog/reader"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	fr "github.com/pingcap/tiflow/dm/pkg/func-rollback"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	"github.com/pingcap/tiflow/dm/pkg/log"
	parserpkg "github.com/pingcap/tiflow/dm/pkg/parser"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/shardddl/pessimism"
	"github.com/pingcap/tiflow/dm/pkg/storage"
	"github.com/pingcap/tiflow/dm/pkg/streamer"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	operator "github.com/pingcap/tiflow/dm/syncer/err-operator"
	onlineddl "github.com/pingcap/tiflow/dm/syncer/online-ddl-tools"
	sm "github.com/pingcap/tiflow/dm/syncer/safe-mode"
	"github.com/pingcap/tiflow/dm/syncer/shardddl"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
)

var (
	waitTime = 10 * time.Millisecond

	// MaxDDLConnectionTimeoutMinute also used by SubTask.ExecuteDDL.
	MaxDDLConnectionTimeoutMinute = 5

	maxDMLConnectionTimeout = "5m"
	maxDDLConnectionTimeout = fmt.Sprintf("%dm", MaxDDLConnectionTimeoutMinute)

	maxDMLConnectionDuration, _ = time.ParseDuration(maxDMLConnectionTimeout)

	defaultMaxPauseOrStopWaitTime = 10 * time.Second

	adminQueueName     = "admin queue"
	defaultBucketCount = 8
)

const (
	skipJobIdx = iota
	ddlJobIdx
	workerJobTSArrayInitSize // size = skip + ddl
)

// waitXIDStatus represents the status for waiting XID event when pause/stop task.
type waitXIDStatus int64

const (
	noWait waitXIDStatus = iota
	waiting
	waitComplete
)

// Syncer can sync your MySQL data to another MySQL database.
type Syncer struct {
	sync.RWMutex

	tctx *tcontext.Context // this ctx only used for logger.

	// this ctx derives from a background ctx and was initialized in s.Run, it is used for some background tasks in s.Run
	// when this ctx cancelled, syncer will shutdown all background running jobs (except the syncDML and syncDDL) and not wait transaction end.
	runCtx    *tcontext.Context
	runCancel context.CancelFunc
	// this ctx only used for syncDML and syncDDL and only cancelled when ungraceful stop.
	syncCtx    *tcontext.Context
	syncCancel context.CancelFunc
	// control all goroutines that started in S.Run
	runWg sync.WaitGroup

	cfg            *config.SubTaskConfig
	syncCfg        replication.BinlogSyncerConfig
	cliArgs        *config.TaskCliArgs
	metricsProxies *metrics.Proxies

	sgk  *ShardingGroupKeeper    // keeper to keep all sharding (sub) group in this syncer
	osgk *OptShardingGroupKeeper // optimistic ddl's keeper to keep all sharding (sub) group in this syncer

	pessimist *shardddl.Pessimist // shard DDL pessimist
	optimist  *shardddl.Optimist  // shard DDL optimist
	cli       *clientv3.Client

	binlogType         binlogstream.BinlogType
	streamerController *binlogstream.StreamerController

	jobWg sync.WaitGroup // counts ddl/flush/asyncFlush job in-flight in s.dmlJobCh and s.ddlJobCh

	schemaTracker *schema.Tracker

	fromDB   *dbconn.UpStreamConn
	fromConn *dbconn.DBConn

	toDB                *conn.BaseDB
	toDBConns           []*dbconn.DBConn
	ddlDB               *conn.BaseDB
	ddlDBConn           *dbconn.DBConn
	downstreamTrackConn *dbconn.DBConn

	dmlJobCh            chan *job
	ddlJobCh            chan *job
	jobsClosed          atomic.Bool
	jobsChanLock        sync.Mutex
	waitXIDJob          atomic.Int64
	isTransactionEnd    bool
	waitTransactionLock sync.Mutex

	tableRouter     *regexprrouter.RouteTable
	binlogFilter    *bf.BinlogEvent
	columnMapping   *cm.Mapping
	baList          *filter.Filter
	exprFilterGroup *ExprFilterGroup
	sessCtx         sessionctx.Context

	running atomic.Bool
	closed  atomic.Bool

	start    atomic.Time
	lastTime atomic.Time

	// safeMode is used to track if we need to generate dml with safe-mode
	// For each binlog event, we will set the current value into eventContext because
	// the status of this track may change over time.
	safeMode *sm.SafeMode

	upstreamTZ *time.Location
	timezone   *time.Location

	binlogSizeCount     atomic.Int64
	lastBinlogSizeCount atomic.Int64

	lastCount atomic.Int64
	count     atomic.Int64
	totalTps  atomic.Int64
	tps       atomic.Int64

	filteredInsert atomic.Int64
	filteredUpdate atomic.Int64
	filteredDelete atomic.Int64

	checkpoint            CheckPoint
	checkpointFlushWorker *checkpointFlushWorker
	onlineDDL             onlineddl.OnlinePlugin

	// record process error rather than log.Fatal
	runFatalChan chan *pb.ProcessError
	// record whether error occurred when execute SQLs
	execError atomic.Error

	readerHub              *streamer.ReaderHub
	recordedActiveRelayLog bool

	errOperatorHolder *operator.Holder

	isReplacingOrInjectingErr bool // true if we are in replace or inject events by handle-error

	currentLocationMu struct {
		sync.RWMutex
		currentLocation binlog.Location // use to calc remain binlog size
	}

	errLocation struct {
		sync.RWMutex
		startLocation *binlog.Location
		endLocation   *binlog.Location
		isQueryEvent  bool
	}

	handleJobFunc func(*job) (bool, error)
	flushSeq      int64

	// `lower_case_table_names` setting of upstream db
	SourceTableNamesFlavor utils.LowerCaseTableNamesFlavor

	tsOffset                  atomic.Int64    // time offset between upstream and syncer, DM's timestamp - MySQL's timestamp
	secondsBehindMaster       atomic.Int64    // current task delay second behind upstream
	workerJobTSArray          []*atomic.Int64 // worker's sync job TS array, note that idx=0 is skip idx and idx=1 is ddl idx,sql worker job idx=(queue id + 2)
	lastCheckpointFlushedTime time.Time

	firstMeetBinlogTS *int64
	exitSafeModeTS    *int64 // TS(in binlog header) need to exit safe mode.

	locations *locationRecorder
	// initial executed binlog location, set once for each instance of syncer.
	initExecutedLoc *binlog.Location

	relay                      relay.Process
	charsetAndDefaultCollation map[string]string
	idAndCollationMap          map[int]string
}

// NewSyncer creates a new Syncer.
func NewSyncer(cfg *config.SubTaskConfig, etcdClient *clientv3.Client, relay relay.Process) *Syncer {
	logFields := []zap.Field{
		zap.String("task", cfg.Name),
		zap.String("unit", "binlog replication"),
	}
	var logger log.Logger
	if cfg.FrameworkLogger != nil {
		logger = log.Logger{Logger: cfg.FrameworkLogger.With(logFields...)}
	} else {
		logger = log.With(logFields...)
	}

	syncer := &Syncer{
		pessimist: shardddl.NewPessimist(&logger, etcdClient, cfg.Name, cfg.SourceID),
		optimist:  shardddl.NewOptimist(&logger, etcdClient, cfg.Name, cfg.SourceID),
	}
	syncer.cfg = cfg
	syncer.tctx = tcontext.Background().WithLogger(logger)
	syncer.jobsClosed.Store(true) // not open yet
	syncer.waitXIDJob.Store(int64(noWait))
	syncer.isTransactionEnd = true
	syncer.closed.Store(false)
	syncer.lastBinlogSizeCount.Store(0)
	syncer.binlogSizeCount.Store(0)
	syncer.lastCount.Store(0)
	syncer.count.Store(0)
	syncer.handleJobFunc = syncer.handleJob
	syncer.cli = etcdClient

	syncer.checkpoint = NewRemoteCheckPoint(syncer.tctx, cfg, syncer.metricsProxies, syncer.checkpointID())

	syncer.binlogType = binlogstream.RelayToBinlogType(relay)
	syncer.errOperatorHolder = operator.NewHolder(&logger)
	syncer.readerHub = streamer.GetReaderHub()

	if cfg.ShardMode == config.ShardPessimistic {
		// only need to sync DDL in sharding mode
		syncer.sgk = NewShardingGroupKeeper(syncer.tctx, cfg, syncer.metricsProxies)
	} else if cfg.ShardMode == config.ShardOptimistic {
		syncer.osgk = NewOptShardingGroupKeeper(syncer.tctx, cfg)
	}
	syncer.recordedActiveRelayLog = false
	syncer.workerJobTSArray = make([]*atomic.Int64, cfg.WorkerCount+workerJobTSArrayInitSize)
	for i := range syncer.workerJobTSArray {
		syncer.workerJobTSArray[i] = atomic.NewInt64(0)
	}
	syncer.lastCheckpointFlushedTime = time.Time{}
	syncer.relay = relay
	syncer.locations = &locationRecorder{}
	return syncer
}

func (s *Syncer) refreshCliArgs() {
	if s.cli == nil {
		// for dummy syncer in ut
		return
	}
	cliArgs, err := ha.GetTaskCliArgs(s.cli, s.cfg.Name, s.cfg.SourceID)
	if err != nil {
		s.tctx.L().Error("failed to get task cli args", zap.Error(err))
	}
	s.Lock()
	s.cliArgs = cliArgs
	s.Unlock()
}

func (s *Syncer) newJobChans() {
	chanSize := calculateChanSize(s.cfg.QueueSize, s.cfg.WorkerCount, s.cfg.Compact)
	s.dmlJobCh = make(chan *job, chanSize)
	s.ddlJobCh = make(chan *job, s.cfg.QueueSize)
	s.jobsClosed.Store(false)
}

func (s *Syncer) closeJobChans() {
	s.jobsChanLock.Lock()
	defer s.jobsChanLock.Unlock()
	if s.jobsClosed.Load() {
		return
	}
	close(s.dmlJobCh)
	close(s.ddlJobCh)
	s.jobsClosed.Store(true)
}

// Type implements Unit.Type.
func (s *Syncer) Type() pb.UnitType {
	return pb.UnitType_Sync
}

// Init initializes syncer for a sync task, but not start Process.
// if fail, it should not call s.Close.
// some check may move to checker later.
func (s *Syncer) Init(ctx context.Context) (err error) {
	rollbackHolder := fr.NewRollbackHolder("syncer")
	defer func() {
		if err != nil {
			rollbackHolder.RollbackReverseOrder()
		}
	}()

	tctx := s.tctx.WithContext(ctx)
	s.upstreamTZ, err = str2TimezoneOrFromDB(tctx, "", &s.cfg.From)
	if err != nil {
		return
	}
	s.timezone, err = str2TimezoneOrFromDB(tctx, s.cfg.Timezone, &s.cfg.To)
	if err != nil {
		return
	}

	s.syncCfg, err = subtaskCfg2BinlogSyncerCfg(s.cfg, s.timezone)
	if err != nil {
		return err
	}

	err = s.createDBs(ctx)
	if err != nil {
		return err
	}
	rollbackHolder.Add(fr.FuncRollback{Name: "close-DBs", Fn: s.closeDBs})

	if s.cfg.CollationCompatible == config.StrictCollationCompatible {
		s.charsetAndDefaultCollation, s.idAndCollationMap, err = dbconn.GetCharsetAndCollationInfo(tctx, s.fromConn)
		if err != nil {
			return err
		}
	}

	s.streamerController = binlogstream.NewStreamerController(s.syncCfg, s.cfg.EnableGTID, s.fromDB, s.cfg.RelayDir, s.timezone, s.relay)

	s.baList, err = filter.New(s.cfg.CaseSensitive, s.cfg.BAList)
	if err != nil {
		return terror.ErrSyncerUnitGenBAList.Delegate(err)
	}

	s.binlogFilter, err = bf.NewBinlogEvent(s.cfg.CaseSensitive, s.cfg.FilterRules)
	if err != nil {
		return terror.ErrSyncerUnitGenBinlogEventFilter.Delegate(err)
	}

	vars := map[string]string{
		"time_zone": s.timezone.String(),
	}
	s.sessCtx = utils.NewSessionCtx(vars)
	s.exprFilterGroup = NewExprFilterGroup(s.tctx, s.sessCtx, s.cfg.ExprFilter)

	if len(s.cfg.ColumnMappingRules) > 0 {
		s.columnMapping, err = cm.NewMapping(s.cfg.CaseSensitive, s.cfg.ColumnMappingRules)
		if err != nil {
			return terror.ErrSyncerUnitGenColumnMapping.Delegate(err)
		}
	}

	if s.cfg.OnlineDDL {
		s.onlineDDL, err = onlineddl.NewRealOnlinePlugin(tctx, s.cfg, s.metricsProxies)
		if err != nil {
			return err
		}
		rollbackHolder.Add(fr.FuncRollback{Name: "close-onlineDDL", Fn: s.closeOnlineDDL})
	}

	err = s.genRouter()
	if err != nil {
		return err
	}

	var schemaMap map[string]string
	var tableMap map[string]map[string]string
	if s.SourceTableNamesFlavor == utils.LCTableNamesSensitive {
		// TODO: we should avoid call this function multi times
		allTables, err1 := utils.FetchAllDoTables(ctx, s.fromDB.BaseDB.DB, s.baList)
		if err1 != nil {
			return err1
		}
		schemaMap, tableMap = buildLowerCaseTableNamesMap(s.tctx.L(), allTables)
	}

	switch s.cfg.ShardMode {
	case config.ShardPessimistic:
		err = s.sgk.Init()
		if err != nil {
			return err
		}
		err = s.initShardingGroups(ctx, true)
		if err != nil {
			return err
		}
		rollbackHolder.Add(fr.FuncRollback{Name: "close-sharding-group-keeper", Fn: s.sgk.Close})
	case config.ShardOptimistic:
		if err = s.initOptimisticShardDDL(ctx); err != nil {
			return err
		}
	}

	err = s.checkpoint.Init(tctx)
	if err != nil {
		return err
	}

	rollbackHolder.Add(fr.FuncRollback{Name: "close-checkpoint", Fn: s.checkpoint.Close})

	err = s.checkpoint.Load(tctx)
	if err != nil {
		return err
	}
	if s.SourceTableNamesFlavor == utils.LCTableNamesSensitive {
		if err = s.checkpoint.CheckAndUpdate(ctx, schemaMap, tableMap); err != nil {
			return err
		}

		if s.onlineDDL != nil {
			if err = s.onlineDDL.CheckAndUpdate(s.tctx, schemaMap, tableMap); err != nil {
				return err
			}
		}
	}

	// when Init syncer, set active relay log info
	if s.cfg.Meta == nil || s.cfg.Meta.BinLogName != binlog.FakeBinlogName {
		err = s.setInitActiveRelayLog(ctx)
		if err != nil {
			return err
		}
		rollbackHolder.Add(fr.FuncRollback{Name: "remove-active-realylog", Fn: s.removeActiveRelayLog})
	}
	s.reset()

	metricProxies := metrics.DefaultMetricsProxies
	if s.cfg.MetricsFactory != nil {
		metricProxies = &metrics.Proxies{}
		metricProxies.Init(s.cfg.MetricsFactory)
	}
	s.metricsProxies = metricProxies.CacheForOneTask(s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID)

	return nil
}

// buildLowerCaseTableNamesMap build a lower case schema map and lower case table map for all tables
// Input: map of schema --> list of tables
// Output: schema names map: lower_case_schema_name --> schema_name
//         tables names map: lower_case_schema_name --> lower_case_table_name --> table_name
// Note: the result will skip the schemas and tables that their lower_case_name are the same.
func buildLowerCaseTableNamesMap(logger log.Logger, tables map[string][]string) (map[string]string, map[string]map[string]string) {
	schemaMap := make(map[string]string)
	tablesMap := make(map[string]map[string]string)
	lowerCaseSchemaSet := make(map[string]string)
	for schema, tableNames := range tables {
		lcSchema := strings.ToLower(schema)
		// track if there are multiple schema names with the same lower case name.
		// just skip this kind of schemas.
		if rawSchema, ok := lowerCaseSchemaSet[lcSchema]; ok {
			delete(schemaMap, lcSchema)
			delete(tablesMap, lcSchema)
			logger.Warn("skip check schema with same lower case value",
				zap.Strings("schemas", []string{schema, rawSchema}))
			continue
		}
		lowerCaseSchemaSet[lcSchema] = schema

		if lcSchema != schema {
			schemaMap[lcSchema] = schema
		}
		tblsMap := make(map[string]string)
		lowerCaseTableSet := make(map[string]string)
		for _, tb := range tableNames {
			lcTbl := strings.ToLower(tb)
			if rawTbl, ok := lowerCaseTableSet[lcTbl]; ok {
				delete(tblsMap, lcTbl)
				logger.Warn("skip check tables with same lower case value", zap.String("schema", schema),
					zap.Strings("table", []string{tb, rawTbl}))
				continue
			}
			if lcTbl != tb {
				tblsMap[lcTbl] = tb
			}
		}
		if len(tblsMap) > 0 {
			tablesMap[lcSchema] = tblsMap
		}
	}
	return schemaMap, tablesMap
}

// initShardingGroups initializes sharding groups according to source MySQL, filter rules and router rules
// NOTE: now we don't support modify router rules after task has started.
func (s *Syncer) initShardingGroups(ctx context.Context, needCheck bool) error {
	// fetch tables from source and filter them
	sourceTables, err := s.fromDB.FetchAllDoTables(ctx, s.baList)
	if err != nil {
		return err
	}

	// convert according to router rules
	// target-ID -> source-IDs
	mapper := make(map[string][]string, len(sourceTables))
	for schema, tables := range sourceTables {
		for _, table := range tables {
			sourceTable := &filter.Table{Schema: schema, Name: table}
			targetTable := s.route(sourceTable)
			targetID := utils.GenTableID(targetTable)
			sourceID := utils.GenTableID(sourceTable)
			_, ok := mapper[targetID]
			if !ok {
				mapper[targetID] = make([]string, 0, len(tables))
			}
			mapper[targetID] = append(mapper[targetID], sourceID)
		}
	}

	loadMeta, err2 := s.sgk.LoadShardMeta(s.cfg.Flavor, s.cfg.EnableGTID)
	if err2 != nil {
		return err2
	}
	if needCheck && s.SourceTableNamesFlavor == utils.LCTableNamesSensitive {
		// try fix persistent data before init
		schemaMap, tableMap := buildLowerCaseTableNamesMap(s.tctx.L(), sourceTables)
		if err2 = s.sgk.CheckAndFix(loadMeta, schemaMap, tableMap); err2 != nil {
			return err2
		}
	}

	// add sharding group
	for targetID, sourceIDs := range mapper {
		targetTable := utils.UnpackTableID(targetID)
		_, _, _, _, err := s.sgk.AddGroup(targetTable, sourceIDs, loadMeta[targetID], false)
		if err != nil {
			return err
		}
	}

	shardGroup := s.sgk.Groups()
	s.tctx.L().Debug("initial sharding groups", zap.Int("shard group length", len(shardGroup)), zap.Reflect("shard group", shardGroup))

	return nil
}

// IsFreshTask implements Unit.IsFreshTask.
func (s *Syncer) IsFreshTask(ctx context.Context) (bool, error) {
	globalPoint := s.checkpoint.GlobalPoint()
	tablePoint := s.checkpoint.TablePoint()
	// doesn't have neither GTID nor binlog pos
	return binlog.IsFreshPosition(globalPoint, s.cfg.Flavor, s.cfg.EnableGTID) && len(tablePoint) == 0, nil
}

func (s *Syncer) reset() {
	if s.streamerController != nil {
		s.streamerController.Close()
	}
	// create new job chans
	s.newJobChans()
	s.checkpointFlushWorker = &checkpointFlushWorker{
		input:              make(chan *checkpointFlushTask, 16),
		cp:                 s.checkpoint,
		execError:          &s.execError,
		afterFlushFn:       s.afterFlushCheckpoint,
		updateJobMetricsFn: s.updateJobMetrics,
	}

	s.execError.Store(nil)
	s.setErrLocation(nil, nil, false)
	s.isReplacingOrInjectingErr = false
	s.waitXIDJob.Store(int64(noWait))
	s.isTransactionEnd = true
	s.flushSeq = 0
	s.firstMeetBinlogTS = nil
	s.exitSafeModeTS = nil
	switch s.cfg.ShardMode {
	case config.ShardPessimistic:
		// every time start to re-sync from resume, we reset status to make it like a fresh syncing
		s.sgk.ResetGroups()
		s.pessimist.Reset()
	case config.ShardOptimistic:
		s.optimist.Reset()
	}
}

func (s *Syncer) resetDBs(tctx *tcontext.Context) error {
	var err error

	for i := 0; i < len(s.toDBConns); i++ {
		err = s.toDBConns[i].ResetConn(tctx)
		if err != nil {
			return terror.WithScope(err, terror.ScopeDownstream)
		}
	}

	if s.onlineDDL != nil {
		err = s.onlineDDL.ResetConn(tctx)
		if err != nil {
			return terror.WithScope(err, terror.ScopeDownstream)
		}
	}

	if s.sgk != nil {
		err = s.sgk.dbConn.ResetConn(tctx)
		if err != nil {
			return terror.WithScope(err, terror.ScopeDownstream)
		}
	}

	err = s.ddlDBConn.ResetConn(tctx)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}

	err = s.downstreamTrackConn.ResetConn(tctx)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}

	err = s.checkpoint.ResetConn(tctx)
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}

	return nil
}

// Process implements the dm.Unit interface.
func (s *Syncer) Process(ctx context.Context, pr chan pb.ProcessResult) {
	s.metricsProxies.Metrics.SyncerExitWithErrorCounter.Add(0)

	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create new done chan
	// use lock of Syncer to avoid Close while Process
	s.Lock()
	if s.isClosed() {
		s.Unlock()
		return
	}
	s.Unlock()

	runFatalChan := make(chan *pb.ProcessError, s.cfg.WorkerCount+1)
	s.runFatalChan = runFatalChan
	var (
		errs   = make([]*pb.ProcessError, 0, 2)
		errsMu sync.Mutex
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err, ok := <-runFatalChan
			if !ok {
				return
			}
			cancel() // cancel s.Run
			s.metricsProxies.Metrics.SyncerExitWithErrorCounter.Inc()
			errsMu.Lock()
			errs = append(errs, err)
			errsMu.Unlock()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-newCtx.Done() // ctx or newCtx
	}()

	err := s.Run(newCtx)
	if err != nil {
		// returned error rather than sent to runFatalChan
		// cancel goroutines created in s.Run
		cancel()
	}
	close(runFatalChan) // Run returned, all potential fatal sent to s.runFatalChan
	wg.Wait()           // wait for receive all fatal from s.runFatalChan

	if err != nil {
		if utils.IsContextCanceledError(err) {
			s.tctx.L().Info("filter out error caused by user cancel", log.ShortError(err))
		} else {
			s.tctx.L().Debug("unit syncer quits with error", zap.Error(err))
			s.metricsProxies.Metrics.SyncerExitWithErrorCounter.Inc()
			errsMu.Lock()
			errs = append(errs, unit.NewProcessError(err))
			errsMu.Unlock()
		}
	}

	isCanceled := false
	select {
	case <-ctx.Done():
		isCanceled = true
	default:
	}

	pr <- pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

func (s *Syncer) getTableInfo(tctx *tcontext.Context, sourceTable, targetTable *filter.Table) (*model.TableInfo, error) {
	ti, err := s.schemaTracker.GetTableInfo(sourceTable)
	if err == nil {
		return ti, nil
	}
	if !schema.IsTableNotExists(err) {
		return nil, terror.ErrSchemaTrackerCannotGetTable.Delegate(err, sourceTable)
	}

	if err = s.schemaTracker.CreateSchemaIfNotExists(sourceTable.Schema); err != nil {
		return nil, terror.ErrSchemaTrackerCannotCreateSchema.Delegate(err, sourceTable.Schema)
	}

	// if the table does not exist (IsTableNotExists(err)), continue to fetch the table from downstream and create it.
	err = s.trackTableInfoFromDownstream(tctx, sourceTable, targetTable)
	if err != nil {
		return nil, err
	}

	ti, err = s.schemaTracker.GetTableInfo(sourceTable)
	if err != nil {
		return nil, terror.ErrSchemaTrackerCannotGetTable.Delegate(err, sourceTable)
	}
	return ti, nil
}

// trackTableInfoFromDownstream tries to track the table info from the downstream. It will not overwrite existing table.
func (s *Syncer) trackTableInfoFromDownstream(tctx *tcontext.Context, sourceTable, targetTable *filter.Table) error {
	// TODO: Switch to use the HTTP interface to retrieve the TableInfo directly if HTTP port is available
	// use parser for downstream.
	parser2, err := dbconn.GetParserForConn(tctx, s.ddlDBConn)
	if err != nil {
		return terror.ErrSchemaTrackerCannotParseDownstreamTable.Delegate(err, targetTable, sourceTable)
	}

	createSQL, err := dbconn.GetTableCreateSQL(tctx, s.ddlDBConn, targetTable.String())
	if err != nil {
		return terror.ErrSchemaTrackerCannotFetchDownstreamTable.Delegate(err, targetTable, sourceTable)
	}

	// rename the table back to original.
	var createNode ast.StmtNode
	createNode, err = parser2.ParseOneStmt(createSQL, "", "")
	if err != nil {
		return terror.ErrSchemaTrackerCannotParseDownstreamTable.Delegate(err, targetTable, sourceTable)
	}
	createStmt := createNode.(*ast.CreateTableStmt)
	createStmt.IfNotExists = true
	createStmt.Table.Schema = model.NewCIStr(sourceTable.Schema)
	createStmt.Table.Name = model.NewCIStr(sourceTable.Name)

	// schema tracker sets non-clustered index, so can't handle auto_random.
	if v, _ := s.schemaTracker.GetSystemVar(schema.TiDBClusteredIndex); v == "OFF" {
		for _, col := range createStmt.Cols {
			for i, opt := range col.Options {
				if opt.Tp == ast.ColumnOptionAutoRandom {
					// col.Options is unordered
					col.Options[i] = col.Options[len(col.Options)-1]
					col.Options = col.Options[:len(col.Options)-1]
					break
				}
			}
		}
	}

	var newCreateSQLBuilder strings.Builder
	restoreCtx := format.NewRestoreCtx(format.DefaultRestoreFlags, &newCreateSQLBuilder)
	if err = createStmt.Restore(restoreCtx); err != nil {
		return terror.ErrSchemaTrackerCannotParseDownstreamTable.Delegate(err, targetTable, sourceTable)
	}
	newCreateSQL := newCreateSQLBuilder.String()
	tctx.L().Debug("reverse-synchronized table schema",
		zap.Stringer("sourceTable", sourceTable),
		zap.Stringer("targetTable", targetTable),
		zap.String("sql", newCreateSQL),
	)
	if err = s.schemaTracker.Exec(tctx.Ctx, sourceTable.Schema, newCreateSQL); err != nil {
		return terror.ErrSchemaTrackerCannotCreateTable.Delegate(err, sourceTable)
	}

	return nil
}

var dmlMetric = map[sqlmodel.RowChangeType]string{
	sqlmodel.RowChangeInsert: "insert",
	sqlmodel.RowChangeUpdate: "update",
	sqlmodel.RowChangeDelete: "delete",
}

func (s *Syncer) updateJobMetrics(isFinished bool, queueBucket string, j *job) {
	tp := j.tp
	targetTable := j.targetTable
	count := 1
	if tp == ddl {
		count = len(j.ddls)
	}

	m := s.metricsProxies.AddedJobsTotal
	if isFinished {
		s.count.Add(int64(count))
		m = s.metricsProxies.FinishedJobsTotal
	}
	switch tp {
	case dml:
		m.WithLabelValues(dmlMetric[j.dml.Type()], s.cfg.Name, queueBucket, s.cfg.SourceID, s.cfg.WorkerName, targetTable.Schema, targetTable.Name).Add(float64(count))
	case ddl, flush, asyncFlush, conflict, compact:
		m.WithLabelValues(tp.String(), s.cfg.Name, queueBucket, s.cfg.SourceID, s.cfg.WorkerName, targetTable.Schema, targetTable.Name).Add(float64(count))
	case skip, xid:
		// ignore skip/xid jobs
	default:
		s.tctx.L().Warn("unknown job operation type", zap.Stringer("type", j.tp))
	}
}

func (s *Syncer) calcReplicationLag(headerTS int64) int64 {
	return time.Now().Unix() - s.tsOffset.Load() - headerTS
}

// updateReplicationJobTS store job TS, it is called after every batch dml job / one skip job / one ddl job is added and committed.
func (s *Syncer) updateReplicationJobTS(job *job, jobIdx int) {
	// when job is nil mean no job in this bucket, need do reset this bucket job ts to 0
	if job == nil {
		s.workerJobTSArray[jobIdx].Store(0)
	} else {
		s.workerJobTSArray[jobIdx].Store(int64(job.eventHeader.Timestamp))
	}
}

func (s *Syncer) updateReplicationLagMetric() {
	var lag int64
	var minTS int64

	for idx := range s.workerJobTSArray {
		if ts := s.workerJobTSArray[idx].Load(); ts != int64(0) {
			if minTS == int64(0) || ts < minTS {
				minTS = ts
			}
		}
	}
	if minTS != int64(0) {
		lag = s.calcReplicationLag(minTS)
	}

	s.metricsProxies.Metrics.ReplicationLagHistogram.Observe(float64(lag))
	s.metricsProxies.Metrics.ReplicationLagGauge.Set(float64(lag))
	s.secondsBehindMaster.Store(lag)

	failpoint.Inject("ShowLagInLog", func(v failpoint.Value) {
		minLag := v.(int)
		if int(lag) >= minLag {
			s.tctx.L().Info("ShowLagInLog", zap.Int64("lag", lag))
		}
	})

	// reset skip job TS in case of skip job TS is never updated
	if minTS == s.workerJobTSArray[skipJobIdx].Load() {
		s.workerJobTSArray[skipJobIdx].Store(0)
	}
}

func (s *Syncer) saveTablePoint(table *filter.Table, location binlog.Location) {
	ti, err := s.schemaTracker.GetTableInfo(table)
	if err != nil && table.Name != "" {
		// TODO: if we RENAME tb1 TO tb2, the tracker will remove TableInfo of tb1 but we still save the table
		// checkpoint for tb1. We can delete the table checkpoint in future.
		s.tctx.L().Warn("table info missing from schema tracker",
			zap.Stringer("table", table),
			zap.Stringer("location", location),
			zap.Error(err))
	}
	s.checkpoint.SaveTablePoint(table, location, ti)
}

// only used in tests.
var (
	lastLocationForTest              binlog.Location
	lastLocationNumForTest           int
	waitJobsDoneForTest              bool
	failExecuteSQLForTest            bool
	failOnceForTest                  atomic.Bool
	waitBeforeRunExitDurationForTest time.Duration
)

// TODO: move to syncer/job.go
// addJob adds one job to DML queue or DDL queue according to its type.
// Caller should prepare all needed jobs before calling this function, addJob should not generate any new jobs.
// There should not be a second way to send jobs to DML queue or DDL queue.
func (s *Syncer) addJob(job *job) {
	failpoint.Inject("countJobFromOneEvent", func() {
		if job.tp == dml {
			if job.currentLocation.Position.Compare(lastLocationForTest.Position) == 0 {
				lastLocationNumForTest++
			} else {
				lastLocationForTest = job.currentLocation
				lastLocationNumForTest = 1
			}
			// trigger a flush after see one job
			if lastLocationNumForTest == 1 {
				waitJobsDoneForTest = true
				s.tctx.L().Info("meet the first job of an event", zap.Any("binlog position", lastLocationForTest))
			}
			// mock a execution error after see two jobs.
			if lastLocationNumForTest == 2 {
				failExecuteSQLForTest = true
				s.tctx.L().Info("meet the second job of an event", zap.Any("binlog position", lastLocationForTest))
			}
		}
	})
	failpoint.Inject("countJobFromOneGTID", func() {
		if job.tp == dml {
			if binlog.CompareLocation(job.currentLocation, lastLocationForTest, true) == 0 {
				lastLocationNumForTest++
			} else {
				lastLocationForTest = job.currentLocation
				lastLocationNumForTest = 1
			}
			// trigger a flush after see one job
			if lastLocationNumForTest == 1 {
				waitJobsDoneForTest = true
				s.tctx.L().Info("meet the first job of a GTID", zap.Any("binlog position", lastLocationForTest))
			}
			// mock a execution error after see two jobs.
			if lastLocationNumForTest == 2 {
				failExecuteSQLForTest = true
				s.tctx.L().Info("meet the second job of a GTID", zap.Any("binlog position", lastLocationForTest))
			}
		}
	})

	// avoid job.type data race with compactor.run()
	// simply copy the opType for performance, though copy a new job in compactor is better
	tp := job.tp
	switch tp {
	case flush:
		s.jobWg.Add(1)
		s.dmlJobCh <- job
	case asyncFlush:
		s.jobWg.Add(1)
		s.dmlJobCh <- job
	case ddl:
		s.updateJobMetrics(false, adminQueueName, job)
		s.jobWg.Add(1)
		startTime := time.Now()
		s.ddlJobCh <- job
		s.metricsProxies.AddJobDurationHistogram.WithLabelValues("ddl", s.cfg.Name, adminQueueName, s.cfg.SourceID).Observe(time.Since(startTime).Seconds())
	case dml:
		failpoint.Inject("SkipDML", func(val failpoint.Value) {
			// first col should be an int and primary key, every row with pk <= val will be skipped
			skippedIDUpperBound := val.(int)
			firstColVal, _ := strconv.Atoi(fmt.Sprintf("%v", job.dml.RowValues()[0]))
			if firstColVal <= skippedIDUpperBound {
				failpoint.Goto("skip_dml")
			}
		})
		s.dmlJobCh <- job
		failpoint.Label("skip_dml")
		failpoint.Inject("checkCheckpointInMiddleOfTransaction", func() {
			s.tctx.L().Info("receive dml job", zap.Any("dml job", job))
			time.Sleep(500 * time.Millisecond)
		})
	case gc:
		s.dmlJobCh <- job
	default:
		s.tctx.L().DPanic("unhandled job type", zap.Stringer("job", job))
	}
}

// flushIfOutdated checks whether syncer should flush now because last flushing is outdated.
func (s *Syncer) flushIfOutdated() error {
	if !s.checkpoint.LastFlushOutdated() {
		return nil
	}

	if s.cfg.Experimental.AsyncCheckpointFlush {
		jobSeq := s.getFlushSeq()
		s.tctx.L().Info("Start to async flush current checkpoint to downstream based on flush interval", zap.Int64("job sequence", jobSeq))
		j := newAsyncFlushJob(s.cfg.WorkerCount, jobSeq)
		s.addJob(j)
		s.flushCheckPointsAsync(j)
		return nil
	}
	return s.flushJobs()
}

// TODO: move to syncer/job.go
// handleJob will do many actions based on job type.
func (s *Syncer) handleJob(job *job) (added2Queue bool, err error) {
	skipCheckFlush := false
	defer func() {
		if !skipCheckFlush && err == nil {
			err = s.flushIfOutdated()
		}
	}()

	// 1. handle jobs that not needed to be sent to queue

	s.waitTransactionLock.Lock()
	defer s.waitTransactionLock.Unlock()

	failpoint.Inject("checkCheckpointInMiddleOfTransaction", func() {
		if waitXIDStatus(s.waitXIDJob.Load()) == waiting {
			s.tctx.L().Info("not receive xid job yet", zap.Any("next job", job))
		}
	})

	if waitXIDStatus(s.waitXIDJob.Load()) == waitComplete && job.tp != flush {
		s.tctx.L().Info("All jobs is completed before syncer close, the coming job will be reject", zap.Any("job", job))
		return
	}

	switch job.tp {
	case xid:
		s.waitXIDJob.CAS(int64(waiting), int64(waitComplete))
		s.saveGlobalPoint(job.location)
		s.isTransactionEnd = true
		return
	case skip:
		s.updateReplicationJobTS(job, skipJobIdx)
		return
	}

	// 2. send the job to queue

	s.addJob(job)
	added2Queue = true

	// 3. after job is sent to queue

	switch job.tp {
	case dml:
		// DMl events will generate many jobs and only caller knows all jobs has been sent, so many logics are done by
		// caller
		s.isTransactionEnd = false
		skipCheckFlush = true
		return
	case ddl:
		s.jobWg.Wait()

		// skip rest logic when downstream error
		if s.execError.Load() != nil {
			// nolint:nilerr
			return
		}
		s.updateReplicationJobTS(nil, ddlJobIdx) // clear ddl job ts because this ddl is already done.
		failpoint.Inject("ExitAfterDDLBeforeFlush", func() {
			s.tctx.L().Warn("exit triggered", zap.String("failpoint", "ExitAfterDDLBeforeFlush"))
			utils.OsExit(1)
		})
		// interrupted after executed DDL and before save checkpoint.
		failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
			err = handleFlushCheckpointStage(3, val.(int), "before save checkpoint")
			if err != nil {
				failpoint.Return()
			}
		})
		// save global checkpoint for DDL
		s.saveGlobalPoint(job.location)
		for sourceSchema, tbs := range job.sourceTbls {
			if len(sourceSchema) == 0 {
				continue
			}
			for _, sourceTable := range tbs {
				s.saveTablePoint(sourceTable, job.location)
			}
		}
		// reset sharding group after checkpoint saved
		s.resetShardingGroup(job.targetTable)

		// interrupted after save checkpoint and before flush checkpoint.
		failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
			err = handleFlushCheckpointStage(4, val.(int), "before flush checkpoint")
			if err != nil {
				failpoint.Return()
			}
		})
		skipCheckFlush = true
		err = s.flushCheckPoints()
		return
	case flush:
		s.jobWg.Wait()
		skipCheckFlush = true
		err = s.flushCheckPoints()
		return
	case asyncFlush:
		skipCheckFlush = true
	}
	// nolint:nakedret
	return
}

func (s *Syncer) saveGlobalPoint(globalLocation binlog.Location) {
	if s.cfg.ShardMode == config.ShardPessimistic {
		globalLocation = s.sgk.AdjustGlobalLocation(globalLocation)
	} else if s.cfg.ShardMode == config.ShardOptimistic {
		globalLocation = s.osgk.AdjustGlobalLocation(globalLocation)
	}
	s.checkpoint.SaveGlobalPoint(globalLocation)
}

func (s *Syncer) resetShardingGroup(table *filter.Table) {
	if s.cfg.ShardMode == config.ShardPessimistic {
		// for DDL sharding group, reset group after checkpoint saved
		group := s.sgk.Group(table)
		if group != nil {
			group.Reset()
		}
	}
}

// flushCheckPoints synchronously flushes previous saved checkpoint in memory to persistent storage, like TiDB
// we flush checkpoints in four cases:
//   1. DDL executed
//   2. pausing / stopping the sync (driven by `s.flushJobs`)
//   3. IsFreshTask return true
//   4. Heartbeat event received
// but when error occurred, we can not flush checkpoint, otherwise data may lost
// and except rejecting to flush the checkpoint, we also need to rollback the checkpoint saved before
// this should be handled when `s.Run` returned
//
// we may need to refactor the concurrency model to make the work-flow more clear later.
func (s *Syncer) flushCheckPoints() error {
	err := s.execError.Load()
	// TODO: for now, if any error occurred (including user canceled), checkpoint won't be updated. But if we have put
	// optimistic shard info, DM-master may resolved the optimistic lock and let other worker execute DDL. So after this
	// worker resume, it can not execute the DML/DDL in old binlog because of downstream table structure mismatching.
	// We should find a way to (compensating) implement a transaction containing interaction with both etcd and SQL.
	if err != nil && (terror.ErrDBExecuteFailed.Equal(err) || terror.ErrDBUnExpect.Equal(err)) {
		s.tctx.L().Warn("error detected when executing SQL job, skip sync flush checkpoints",
			zap.Stringer("checkpoint", s.checkpoint),
			zap.Error(err))
		return nil
	}

	snapshotInfo, exceptTables, shardMetaSQLs, shardMetaArgs := s.createCheckpointSnapshot(true)

	if snapshotInfo == nil {
		s.tctx.L().Debug("checkpoint has no change, skip sync flush checkpoint")
		return nil
	}

	syncFlushErrCh := make(chan error, 1)

	task := &checkpointFlushTask{
		snapshotInfo:   snapshotInfo,
		exceptTables:   exceptTables,
		shardMetaSQLs:  shardMetaSQLs,
		shardMetaArgs:  shardMetaArgs,
		asyncflushJob:  nil,
		syncFlushErrCh: syncFlushErrCh,
	}
	s.checkpointFlushWorker.Add(task)

	return <-syncFlushErrCh
}

// flushCheckPointsAsync asynchronous flushes checkpoint.
func (s *Syncer) flushCheckPointsAsync(asyncFlushJob *job) {
	err := s.execError.Load()
	// TODO: for now, if any error occurred (including user canceled), checkpoint won't be updated. But if we have put
	// optimistic shard info, DM-master may resolved the optimistic lock and let other worker execute DDL. So after this
	// worker resume, it can not execute the DML/DDL in old binlog because of downstream table structure mismatching.
	// We should find a way to (compensating) implement a transaction containing interaction with both etcd and SQL.
	if err != nil && (terror.ErrDBExecuteFailed.Equal(err) || terror.ErrDBUnExpect.Equal(err)) {
		s.tctx.L().Warn("error detected when executing SQL job, skip async flush checkpoints",
			zap.Stringer("checkpoint", s.checkpoint),
			zap.Error(err))
		return
	}

	snapshotInfo, exceptTables, shardMetaSQLs, shardMetaArgs := s.createCheckpointSnapshot(false)

	if snapshotInfo == nil {
		s.tctx.L().Debug("checkpoint has no change, skip async flush checkpoint", zap.Int64("job seq", asyncFlushJob.flushSeq))
		return
	}

	task := &checkpointFlushTask{
		snapshotInfo:   snapshotInfo,
		exceptTables:   exceptTables,
		shardMetaSQLs:  shardMetaSQLs,
		shardMetaArgs:  shardMetaArgs,
		asyncflushJob:  asyncFlushJob,
		syncFlushErrCh: nil,
	}
	s.checkpointFlushWorker.Add(task)
}

func (s *Syncer) createCheckpointSnapshot(isSyncFlush bool) (*SnapshotInfo, []*filter.Table, []string, [][]interface{}) {
	snapshotInfo := s.checkpoint.Snapshot(isSyncFlush)
	if snapshotInfo == nil {
		return nil, nil, nil, nil
	}

	var (
		exceptTableIDs map[string]bool
		exceptTables   []*filter.Table
		shardMetaSQLs  []string
		shardMetaArgs  [][]interface{}
	)
	if s.cfg.ShardMode == config.ShardPessimistic {
		// flush all checkpoints except tables which are unresolved for sharding DDL for the pessimistic mode.
		// NOTE: for the optimistic mode, because we don't handle conflicts automatically (or no re-direct supported),
		// so we can simply flush checkpoint for all tables now, and after re-direct supported this should be updated.
		exceptTableIDs, exceptTables = s.sgk.UnresolvedTables()
		s.tctx.L().Info("flush checkpoints except for these tables", zap.Reflect("tables", exceptTables))

		shardMetaSQLs, shardMetaArgs = s.sgk.PrepareFlushSQLs(exceptTableIDs)
		s.tctx.L().Info("prepare flush sqls", zap.Strings("shard meta sqls", shardMetaSQLs), zap.Reflect("shard meta arguments", shardMetaArgs))
	}

	return snapshotInfo, exceptTables, shardMetaSQLs, shardMetaArgs
}

func (s *Syncer) afterFlushCheckpoint(task *checkpointFlushTask) error {
	// add a gc job to let causality module gc outdated kvs.
	if task.asyncflushJob != nil {
		s.tctx.L().Info("after async flushed checkpoint, gc stale causality keys", zap.Int64("flush job seq", task.asyncflushJob.flushSeq))
		s.addJob(newGCJob(task.asyncflushJob.flushSeq))
	} else {
		s.tctx.L().Info("after sync flushed checkpoint, gc all causality keys")
		s.addJob(newGCJob(math.MaxInt64))
	}

	// update current active relay log after checkpoint flushed
	err := s.updateActiveRelayLog(task.snapshotInfo.globalPos.Position)
	if err != nil {
		return err
	}

	now := time.Now()
	if !s.lastCheckpointFlushedTime.IsZero() {
		duration := now.Sub(s.lastCheckpointFlushedTime).Seconds()
		s.metricsProxies.Metrics.FlushCheckPointsTimeInterval.Observe(duration)
	}
	s.lastCheckpointFlushedTime = now

	s.logAndClearFilteredStatistics()

	if s.cliArgs != nil && s.cliArgs.StartTime != "" && s.cli != nil {
		clone := *s.cliArgs
		clone.StartTime = ""
		err2 := ha.PutTaskCliArgs(s.cli, s.cfg.Name, []string{s.cfg.SourceID}, clone)
		if err2 != nil {
			s.tctx.L().Error("failed to clean start-time in task cli args", zap.Error(err2))
		} else {
			s.Lock()
			s.cliArgs.StartTime = ""
			s.Unlock()
		}
	}
	return nil
}

func (s *Syncer) logAndClearFilteredStatistics() {
	filteredInsert := s.filteredInsert.Swap(0)
	filteredUpdate := s.filteredUpdate.Swap(0)
	filteredDelete := s.filteredDelete.Swap(0)
	if filteredInsert > 0 || filteredUpdate > 0 || filteredDelete > 0 {
		s.tctx.L().Info("after last flushing checkpoint, DM has ignored row changes by expression filter",
			zap.Int64("number of filtered insert", filteredInsert),
			zap.Int64("number of filtered update", filteredUpdate),
			zap.Int64("number of filtered delete", filteredDelete))
	}
}

// DDL synced one by one, so we only need to process one DDL at a time.
func (s *Syncer) syncDDL(queueBucket string, db *dbconn.DBConn, ddlJobChan chan *job) {
	defer s.runWg.Done()

	var err error
	for {
		ddlJob, ok := <-ddlJobChan
		if !ok {
			return
		}
		// add this ddl ts beacause we start to exec this ddl.
		s.updateReplicationJobTS(ddlJob, ddlJobIdx)

		failpoint.Inject("BlockDDLJob", func(v failpoint.Value) {
			t := v.(int) // sleep time
			s.tctx.L().Info("BlockDDLJob", zap.Any("job", ddlJob), zap.Int("sleep time", t))
			time.Sleep(time.Second * time.Duration(t))
		})

		var (
			ignore           = false
			shardPessimistOp *pessimism.Operation
		)
		switch s.cfg.ShardMode {
		case config.ShardPessimistic:
			shardPessimistOp = s.pessimist.PendingOperation()
			if shardPessimistOp != nil && !shardPessimistOp.Exec {
				ignore = true
				s.tctx.L().Info("ignore shard DDLs in pessimistic shard mode", zap.Strings("ddls", ddlJob.ddls))
			}
		case config.ShardOptimistic:
			if len(ddlJob.ddls) == 0 {
				ignore = true
				s.tctx.L().Info("ignore shard DDLs in optimistic mode", zap.Stringer("info", s.optimist.PendingInfo()))
			}
		}

		failpoint.Inject("ExecDDLError", func() {
			s.tctx.L().Warn("execute ddl error", zap.Strings("DDL", ddlJob.ddls), zap.String("failpoint", "ExecDDLError"))
			err = terror.ErrDBUnExpect.Delegate(errors.Errorf("execute ddl %v error", ddlJob.ddls))
			failpoint.Goto("bypass")
		})

		if !ignore {
			var affected int
			affected, err = db.ExecuteSQLWithIgnore(s.syncCtx, s.metricsProxies, errorutil.IsIgnorableMySQLDDLError, ddlJob.ddls)
			if err != nil {
				err = s.handleSpecialDDLError(s.syncCtx, err, ddlJob.ddls, affected, db)
				err = terror.WithScope(err, terror.ScopeDownstream)
			}
		}
		failpoint.Label("bypass")
		failpoint.Inject("SafeModeExit", func(val failpoint.Value) {
			if intVal, ok := val.(int); ok && (intVal == 2 || intVal == 3) {
				s.tctx.L().Warn("mock safe mode error", zap.Strings("DDL", ddlJob.ddls), zap.String("failpoint", "SafeModeExit"))
				if intVal == 2 {
					err = terror.ErrWorkerDDLLockInfoNotFound.Generatef("DDL info not found")
				} else {
					err = terror.ErrDBExecuteFailed.Delegate(errors.Errorf("execute ddl %v error", ddlJob.ddls))
				}
			}
		})
		// If downstream has error (which may cause by tracker is more compatible than downstream), we should stop handling
		// this job, set `s.execError` to let caller of `addJob` discover error
		if err != nil {
			s.execError.Store(err)
			if !utils.IsContextCanceledError(err) {
				err = s.handleEventError(err, ddlJob.startLocation, ddlJob.currentLocation, true, ddlJob.originSQL)
				s.runFatalChan <- unit.NewProcessError(err)
			}
			s.jobWg.Done()
			continue
		}

		switch s.cfg.ShardMode {
		case config.ShardPessimistic:
			// for sharding DDL syncing, send result back
			shardInfo := s.pessimist.PendingInfo()
			switch {
			case shardInfo == nil:
				// no need to do the shard DDL handle for `CREATE DATABASE/TABLE` now.
				s.tctx.L().Warn("skip shard DDL handle in pessimistic shard mode", zap.Strings("ddl", ddlJob.ddls))
			case shardPessimistOp == nil:
				err = terror.ErrWorkerDDLLockOpNotFound.Generate(shardInfo)
			default:
				err = s.pessimist.DoneOperationDeleteInfo(*shardPessimistOp, *shardInfo)
			}
		case config.ShardOptimistic:
			shardInfo := s.optimist.PendingInfo()
			switch {
			case shardInfo == nil:
				// no need to do the shard DDL handle for `DROP DATABASE/TABLE` now.
				// but for `CREATE DATABASE` and `ALTER DATABASE` we execute it to the downstream directly without `shardInfo`.
				if ignore { // actually ignored.
					s.tctx.L().Warn("skip shard DDL handle in optimistic shard mode", zap.Strings("ddl", ddlJob.ddls))
				}
			case s.optimist.PendingOperation() == nil:
				err = terror.ErrWorkerDDLLockOpNotFound.Generate(shardInfo)
			default:
				err = s.optimist.DoneOperation(*(s.optimist.PendingOperation()))
			}
		}
		if err != nil {
			if s.execError.Load() == nil {
				s.execError.Store(err)
			}
			if !utils.IsContextCanceledError(err) {
				err = s.handleEventError(err, ddlJob.startLocation, ddlJob.currentLocation, true, ddlJob.originSQL)
				s.runFatalChan <- unit.NewProcessError(err)
			}
			s.jobWg.Done()
			continue
		}
		s.jobWg.Done()
		s.updateJobMetrics(true, queueBucket, ddlJob)
	}
}

func (s *Syncer) successFunc(queueID int, statementsCnt int, jobs []*job) {
	queueBucket := queueBucketName(queueID)
	if len(jobs) > 0 {
		// NOTE: we can use the first job of job queue to calculate lag because when this job committed,
		// every event before this job's event in this queue has already commit.
		// and we can use this job to maintain the oldest binlog event ts among all workers.
		j := jobs[0]
		switch j.tp {
		case ddl:
			s.metricsProxies.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageDDLExec, s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(time.Since(j.jobAddTime).Seconds())
		case dml:
			s.metricsProxies.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageDMLExec, s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(time.Since(j.jobAddTime).Seconds())
			// metric only increases by 1 because dm batches sql jobs in a single transaction.
			s.metricsProxies.Metrics.FinishedTransactionTotal.Inc()
		}
	}

	for _, sqlJob := range jobs {
		s.updateJobMetrics(true, queueBucket, sqlJob)
	}
	s.updateReplicationJobTS(nil, dmlWorkerJobIdx(queueID))
	s.metricsProxies.ReplicationTransactionBatch.WithLabelValues(s.cfg.WorkerName, s.cfg.Name, s.cfg.SourceID, queueBucket, "statements").Observe(float64(statementsCnt))
	s.metricsProxies.ReplicationTransactionBatch.WithLabelValues(s.cfg.WorkerName, s.cfg.Name, s.cfg.SourceID, queueBucket, "rows").Observe(float64(len(jobs)))
}

func (s *Syncer) fatalFunc(job *job, err error) {
	s.execError.Store(err)
	if !utils.IsContextCanceledError(err) {
		err = s.handleEventError(err, job.startLocation, job.currentLocation, false, "")
		s.runFatalChan <- unit.NewProcessError(err)
	}
}

// DML synced with causality.
func (s *Syncer) syncDML() {
	defer s.runWg.Done()

	dmlJobCh := s.dmlJobCh
	if s.cfg.Compact {
		dmlJobCh = compactorWrap(dmlJobCh, s)
	}
	causalityCh := causalityWrap(dmlJobCh, s)
	flushCh := dmlWorkerWrap(causalityCh, s)

	for range flushCh {
		s.jobWg.Done()
	}
}

func (s *Syncer) waitBeforeRunExit(ctx context.Context) {
	defer s.runWg.Done()
	failpoint.Inject("checkCheckpointInMiddleOfTransaction", func() {
		s.tctx.L().Info("incr maxPauseOrStopWaitTime time ")
		defaultMaxPauseOrStopWaitTime = time.Minute * 10
	})
	select {
	case <-ctx.Done(): // hijack the root context from s.Run to wait for the transaction to end.
		s.tctx.L().Info("received subtask's done, try graceful stop")
		needToExitTime := time.Now()
		s.waitTransactionLock.Lock()

		failpoint.Inject("checkWaitDuration", func(_ failpoint.Value) {
			s.isTransactionEnd = false
		})
		if s.isTransactionEnd {
			s.waitXIDJob.Store(int64(waitComplete))
			s.waitTransactionLock.Unlock()
			s.tctx.L().Info("the last job is transaction end, done directly")
			s.runCancel()
			return
		}
		s.waitXIDJob.Store(int64(waiting))
		s.waitTransactionLock.Unlock()
		s.refreshCliArgs()

		waitDuration := defaultMaxPauseOrStopWaitTime
		if s.cliArgs != nil && s.cliArgs.WaitTimeOnStop != "" {
			waitDuration, _ = time.ParseDuration(s.cliArgs.WaitTimeOnStop)
		}
		prepareForWaitTime := time.Since(needToExitTime)
		failpoint.Inject("recordAndIgnorePrepareTime", func() {
			prepareForWaitTime = time.Duration(0)
		})
		waitDuration -= prepareForWaitTime
		failpoint.Inject("recordAndIgnorePrepareTime", func() {
			waitBeforeRunExitDurationForTest = waitDuration
		})
		if waitDuration.Seconds() <= 0 {
			s.tctx.L().Info("wait transaction end timeout, exit now")
			s.runCancel()
			return
		}
		failpoint.Inject("checkWaitDuration", func(val failpoint.Value) {
			if testDuration, testError := time.ParseDuration(val.(string)); testError == nil {
				if testDuration.Seconds() == waitDuration.Seconds() {
					panic("success check wait_time_on_stop !!!")
				} else {
					s.tctx.L().Error("checkWaitDuration fail", zap.Duration("testDuration", testDuration), zap.Duration("waitDuration", waitDuration))
				}
			} else {
				s.tctx.L().Error("checkWaitDuration error", zap.Error(testError))
			}
		})

		select {
		case <-s.runCtx.Ctx.Done():
			s.tctx.L().Info("syncer run exit so runCtx done")
		case <-time.After(waitDuration):
			s.tctx.L().Info("wait transaction end timeout, exit now")
			s.runCancel()
		}
	case <-s.runCtx.Ctx.Done(): // when no graceful stop, run ctx will canceled first.
		s.tctx.L().Info("received ungraceful exit ctx, exit now")
	}
}

func (s *Syncer) updateTSOffsetCronJob(ctx context.Context) {
	defer s.runWg.Done()
	// temporarily hard code there. if this Proxies works well add this to config file.
	ticker := time.NewTicker(time.Minute * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if utErr := s.updateTSOffset(ctx); utErr != nil {
				s.tctx.L().Error("get server unix ts err", zap.Error(utErr))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *Syncer) updateLagCronJob(ctx context.Context) {
	defer s.runWg.Done()
	// temporarily hard code there. if this Proxies works well add this to config file.
	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.updateReplicationLagMetric()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Syncer) updateTSOffset(ctx context.Context) error {
	t1 := time.Now()
	ts, tsErr := s.fromDB.GetServerUnixTS(ctx)
	rtt := time.Since(t1).Seconds()
	if tsErr == nil {
		s.tsOffset.Store(time.Now().Unix() - ts - int64(rtt/2))
	}
	return tsErr
}

// Run starts running for sync, we should guarantee it can rerun when paused.
func (s *Syncer) Run(ctx context.Context) (err error) {
	runCtx, runCancel := context.WithCancel(context.Background())
	s.runCtx, s.runCancel = tcontext.NewContext(runCtx, s.tctx.L()), runCancel
	syncCtx, syncCancel := context.WithCancel(context.Background())
	s.syncCtx, s.syncCancel = tcontext.NewContext(syncCtx, s.tctx.L()), syncCancel
	defer func() {
		s.runCancel()
		s.closeJobChans()
		s.checkpointFlushWorker.Close()
		s.runWg.Wait()
		// s.syncCancel won't be called when normal exit, this call just to follow the best practice of use context.
		s.syncCancel()
	}()

	// we should start this goroutine as soon as possible, because it's the only goroutine that cancel syncer.Run
	s.runWg.Add(1)
	go func() {
		s.waitBeforeRunExit(ctx)
	}()

	// before sync run, we get the ts offset from upstream first
	if utErr := s.updateTSOffset(ctx); utErr != nil {
		return utErr
	}

	// some initialization that can't be put in Syncer.Init
	fresh, err := s.IsFreshTask(s.runCtx.Ctx)
	if err != nil {
		return err
	}

	// task command line arguments have the highest priority
	skipLoadMeta := false
	s.refreshCliArgs()
	if s.cliArgs != nil && s.cliArgs.StartTime != "" {
		err = s.setGlobalPointByTime(s.runCtx, s.cliArgs.StartTime)
		if terror.ErrConfigStartTimeTooLate.Equal(err) {
			return err
		}
		skipLoadMeta = err == nil
	}

	// some initialization that can't be put in Syncer.Init
	if fresh && !skipLoadMeta {
		// for fresh task, we try to load checkpoints from meta (file or config item)
		err = s.checkpoint.LoadMeta(runCtx)
		if err != nil {
			return err
		}
	}

	var (
		flushCheckpoint bool
		delLoadTask     bool
		cleanDumpFile   = s.cfg.CleanDumpFile
		freshAndAllMode bool
	)
	flushCheckpoint, err = s.adjustGlobalPointGTID(s.runCtx)
	if err != nil {
		return err
	}
	if fresh && s.cfg.Mode == config.ModeAll {
		delLoadTask = true
		flushCheckpoint = true
		freshAndAllMode = true
	}

	if s.cfg.Mode == config.ModeIncrement || !fresh {
		cleanDumpFile = false
	}

	s.runWg.Add(1)
	go s.syncDML()
	s.runWg.Add(1)
	go func() {
		defer s.runWg.Done()
		// also need to use a different ctx. checkpointFlushWorker worker will be closed in the first defer
		s.checkpointFlushWorker.Run(s.tctx)
	}()
	s.runWg.Add(1)
	go s.syncDDL(adminQueueName, s.ddlDBConn, s.ddlJobCh)
	s.runWg.Add(1)
	go s.updateLagCronJob(s.runCtx.Ctx)
	s.runWg.Add(1)
	go s.updateTSOffsetCronJob(s.runCtx.Ctx)

	// some prepare work before the binlog event loop:
	// 1. first we flush checkpoint as needed, so in next resume we won't go to Load unit.
	// 2. then since we are confident that Load unit is done we can delete the load task etcd KV.
	//    TODO: we can't handle panic between 1. and 2., or fail to delete the load task etcd KV.
	// 3. then we initiate schema tracker
	// 4. - when it's a fresh task, load the table structure from dump files into schema tracker.
	//      if it's also a optimistic sharding task, also load the table structure into checkpoints because shard tables
	//      may not have same table structure so we can't fetch the downstream table structure for them lazily.
	//    - when it's a resumed task, load the table structure from checkpoints into schema tracker.
	//    TODO: we can't handle failure between 1. and 4. After 1. it's not a fresh task.
	// 5. finally clean the dump files

	if flushCheckpoint {
		if err = s.flushCheckPoints(); err != nil {
			s.tctx.L().Warn("fail to flush checkpoints when starting task", zap.Error(err))
			return err
		}
	}
	if delLoadTask {
		if err = s.delLoadTask(); err != nil {
			s.tctx.L().Error("error when del load task in etcd", zap.Error(err))
		}
	}

	s.schemaTracker, err = schema.NewTracker(ctx, s.cfg.Name, s.cfg.To.Session, s.downstreamTrackConn, s.tctx.L())
	if err != nil {
		return terror.ErrSchemaTrackerInit.Delegate(err)
	}

	if freshAndAllMode {
		err = s.loadTableStructureFromDump(ctx)
		if err != nil {
			s.tctx.L().Warn("error happened when load table structure from dump files", zap.Error(err))
			cleanDumpFile = false
		}
		if s.cfg.ShardMode == config.ShardOptimistic {
			s.flushOptimisticTableInfos(s.runCtx)
		}
	} else {
		err = s.checkpoint.LoadIntoSchemaTracker(ctx, s.schemaTracker)
		if err != nil {
			return err
		}
	}

	if cleanDumpFile {
		s.tctx.L().Info("try to remove all dump files")
		if err = storage.RemoveAll(ctx, s.cfg.Dir, nil); err != nil {
			s.tctx.L().Warn("error when remove loaded dump folder", zap.String("data folder", s.cfg.Dir), zap.Error(err))
		}
	}

	failpoint.Inject("AdjustGTIDExit", func() {
		s.tctx.L().Warn("exit triggered", zap.String("failpoint", "AdjustGTIDExit"))
		s.streamerController.Close()
		utils.OsExit(1)
	})

	// startLocation is the start location for current received event
	// currentLocation is the end location for current received event (End_log_pos in `show binlog events` for mysql)
	// lastLocation is the end location for last received and fully executed (ROTATE / QUERY / XID) event
	// we use startLocation to replace and skip binlog event of specified position
	// we use currentLocation and update table checkpoint in sharding ddl
	// we use lastLocation to update global checkpoint and table checkpoint
	var (
		currentLocation = s.checkpoint.GlobalPoint() // also init to global checkpoint
		startLocation   = s.checkpoint.GlobalPoint()
		lastLocation    = s.checkpoint.GlobalPoint()

		currentGTID string
	)
	s.tctx.L().Info("replicate binlog from checkpoint", zap.Stringer("checkpoint", lastLocation))

	if s.streamerController.IsClosed() {
		s.locations.reset(lastLocation)
		err = s.streamerController.Start(s.runCtx, lastLocation)
		if err != nil {
			return terror.Annotate(err, "fail to restart streamer controller")
		}
	}
	// syncing progress with sharding DDL group
	// 1. use the global streamer to sync regular binlog events
	// 2. sharding DDL synced for some sharding groups
	//    * record first pos, last pos, target schema, target table as re-sync info
	// 3. use the re-sync info recorded in step.2 to create a new streamer
	// 4. use the new streamer re-syncing for this sharding group
	// 5. in sharding group's re-syncing
	//    * ignore other tables' binlog events
	//    * compare last pos with current binlog's pos to determine whether re-sync completed
	// 6. use the global streamer to continue the syncing
	var (
		shardingReSyncCh        = make(chan *ShardingReSync, 10)
		shardingReSync          *ShardingReSync
		savedGlobalLastLocation binlog.Location
		traceSource             = fmt.Sprintf("%s.syncer.%s", s.cfg.SourceID, s.cfg.Name)
		lastEvent               *replication.BinlogEvent
	)

	// this is second defer func in syncer.Run so in this time checkpointFlushWorker are still running
	defer func() {
		if err1 := recover(); err1 != nil {
			failpoint.Inject("ExitAfterSaveOnlineDDL", func() {
				s.tctx.L().Info("force panic")
				panic("ExitAfterSaveOnlineDDL")
			})
			s.tctx.L().Error("panic log", zap.Reflect("error message", err1), zap.Stack("stack"))
			err = terror.ErrSyncerUnitPanic.Generate(err1)
		}

		var (
			err2            error
			exitSafeModeLoc = currentLocation.Clone()
		)
		saveExitSafeModeLoc := func(loc binlog.Location) {
			if binlog.CompareLocation(loc, exitSafeModeLoc, s.cfg.EnableGTID) > 0 {
				exitSafeModeLoc = loc.Clone()
			}
		}
		saveExitSafeModeLoc(savedGlobalLastLocation)
		if s.cfg.ShardMode == config.ShardOptimistic {
			if shardingReSync != nil {
				saveExitSafeModeLoc(shardingReSync.latestLocation)
			}
			for _, latestLoc := range s.osgk.getShardingResyncs() {
				saveExitSafeModeLoc(latestLoc)
			}
		}
		s.checkpoint.SaveSafeModeExitPoint(&exitSafeModeLoc)

		// flush all jobs before exit
		if err2 = s.flushJobs(); err2 != nil {
			s.tctx.L().Warn("failed to flush jobs when exit task", zap.Error(err2))
		}

		// if any execute error, flush safemode exit point
		if err2 = s.execError.Load(); err2 != nil && (terror.ErrDBExecuteFailed.Equal(err2) || terror.ErrDBUnExpect.Equal(err2)) {
			if err2 = s.checkpoint.FlushSafeModeExitPoint(s.tctx); err2 != nil {
				s.tctx.L().Warn("failed to flush safe mode checkpoints when exit task", zap.Error(err2))
			}
		}
	}()

	now := time.Now()
	s.start.Store(now)
	s.lastTime.Store(now)

	s.initInitExecutedLoc()
	s.running.Store(true)
	defer s.running.Store(false)

	// safeMode makes syncer reentrant.
	// we make each operator reentrant to make syncer reentrant.
	// `replace` and `delete` are naturally reentrant.
	// use `delete`+`replace` to represent `update` can make `update`  reentrant.
	// but there are no ways to make `update` idempotent,
	// if we start syncer at an early position, database must bear a period of inconsistent state,
	// it's eventual consistency.
	s.safeMode = sm.NewSafeMode()
	s.enableSafeModeInitializationPhase(s.runCtx)

	closeShardingResync := func() error {
		if shardingReSync == nil {
			return nil
		}

		// if remaining DDLs in sequence, redirect global stream to the next sharding DDL position
		if !shardingReSync.allResolved {
			nextLocation, err2 := s.sgk.ActiveDDLFirstLocation(shardingReSync.targetTable)
			if err2 != nil {
				return err2
			}

			currentLocation = nextLocation
			lastLocation = nextLocation
		} else {
			currentLocation = savedGlobalLastLocation
			lastLocation = savedGlobalLastLocation // restore global last pos
		}
		// if suffix>0, we are replacing error
		s.isReplacingOrInjectingErr = currentLocation.Suffix != 0

		s.locations.reset(currentLocation)
		err3 := s.streamerController.ResetReplicationSyncer(s.tctx, currentLocation)
		if err3 != nil {
			return err3
		}

		shardingReSync = nil
		return nil
	}

	maybeSkipNRowsEvent := func(n int) error {
		if n == 0 {
			return nil
		}

		for i := 0; i < n; {
			e, err1 := s.getEvent(s.runCtx, currentLocation)
			if err1 != nil {
				return err
			}
			switch e.Event.(type) {
			case *replication.GTIDEvent, *replication.MariadbGTIDEvent:
				gtidStr, err2 := event.GetGTIDStr(e)
				if err2 != nil {
					return err2
				}
				if currentGTID != gtidStr {
					s.tctx.L().Error("after recover GTID-based replication, the first GTID is not same as broken one. May meet duplicate entry or corrupt data if table has no PK/UK.",
						zap.String("last GTID", currentGTID),
						zap.String("GTID after reset", gtidStr),
					)
					return nil
				}
			case *replication.RowsEvent:
				i++
			}
		}
		s.tctx.L().Info("discard event already consumed", zap.Int("count", n),
			zap.Any("cur_loc", currentLocation))
		return nil
	}

	advanceCurrentLocationGtidSet := func(e *replication.BinlogEvent) error {
		if _, ok := e.Event.(*replication.MariadbGTIDEvent); ok {
			gtidSet, err2 := gtid.ParserGTID(s.cfg.Flavor, currentGTID)
			if err2 != nil {
				return err2
			}
			if currentLocation.GetGTID().Contain(gtidSet) {
				return nil
			}
		}

		// clone currentLocation's gtid set to avoid its gtid is transport to table checkpoint
		// currently table checkpoint will save location's gtid set with shallow copy
		newGTID := currentLocation.GetGTID().Clone()
		err2 := newGTID.Update(currentGTID)
		if err2 != nil {
			return terror.Annotatef(err2, "fail to update GTID %s", currentGTID)
		}
		err2 = currentLocation.SetGTID(newGTID)
		if err2 != nil {
			return terror.Annotatef(err2, "fail to set GTID %s", newGTID)
		}
		return nil
	}

	// eventIndex is the rows event index in this transaction, it's used to avoiding read duplicate event in gtid mode
	eventIndex := 0
	// affectedSourceTables is used for gtid mode to update table point's gtid set after receiving a xid event,
	// which means this whole event is finished
	affectedSourceTables := make(map[string]map[string]struct{}, 0)
	// the relay log file may be truncated(not end with an RotateEvent), in this situation, we may read some rows events
	// and then read from the gtid again, so we force enter safe-mode for one more transaction to avoid failure due to
	// conflict
	for {
		if s.execError.Load() != nil {
			return nil
		}
		s.currentLocationMu.Lock()
		s.currentLocationMu.currentLocation = currentLocation
		s.currentLocationMu.Unlock()

		failpoint.Inject("FakeRedirect", func(val failpoint.Value) {
			if len(shardingReSyncCh) == 0 && shardingReSync == nil {
				if strVal, ok := val.(string); ok {
					pos, gtidSet, err2 := s.fromDB.GetMasterStatus(ctx, s.cfg.Flavor)
					if err2 != nil {
						s.tctx.L().Error("fail to get master status in failpoint FakeRedirect", zap.Error(err2))
						os.Exit(1)
					}
					loc := binlog.NewLocation(pos, gtidSet)
					s.tctx.L().Info("fake redirect", zap.Stringer("currentLocation", currentLocation), zap.Stringer("latestLocation", loc))
					resync := &ShardingReSync{
						currLocation:   currentLocation.Clone(),
						latestLocation: loc,
						targetTable:    utils.UnpackTableID(strVal),
						allResolved:    true,
					}
					shardingReSyncCh <- resync
				}
			}
		})
		// fetch from sharding resync channel if needed, and redirect global
		// stream to current binlog position recorded by ShardingReSync
		if len(shardingReSyncCh) > 0 && shardingReSync == nil {
			// some sharding groups need to re-syncing
			shardingReSync = <-shardingReSyncCh
			s.tctx.L().Debug("starts to handle new shardingResync operation", zap.Stringer("shardingResync", shardingReSync))
			savedGlobalLastLocation = lastLocation // save global last location
			lastLocation = shardingReSync.currLocation

			// remove sharding resync global checkpoint location limit from optimistic sharding group keeper
			if s.cfg.ShardMode == config.ShardOptimistic {
				s.osgk.removeShardingReSync(shardingReSync)
			}

			currentLocation = shardingReSync.currLocation
			// if suffix>0, we are replacing error
			s.isReplacingOrInjectingErr = currentLocation.Suffix != 0
			s.locations.reset(shardingReSync.currLocation)
			err = s.streamerController.ResetReplicationSyncer(s.runCtx, shardingReSync.currLocation)
			if err != nil {
				return err
			}

			failpoint.Inject("ReSyncExit", func() {
				s.tctx.L().Warn("exit triggered", zap.String("failpoint", "ReSyncExit"))
				utils.OsExit(1)
			})
		}
		var e *replication.BinlogEvent
		// for position mode, we can redirect at any time
		// for gtid mode, we can redirect only when current location related gtid's transaction is totally executed and
		//  next gtid is just updated (because we check if we can end resync by currLoc >= latestLoc)
		if s.cfg.ShardMode == config.ShardOptimistic {
			canRedirect := true
			if s.cfg.EnableGTID {
				canRedirect = safeToRedirect(lastEvent)
			} else if lastEvent != nil {
				if _, ok := lastEvent.Event.(*replication.QueryEvent); ok {
					if op := s.optimist.PendingOperation(); op != nil && op.ConflictStage == optimism.ConflictSkipWaitRedirect {
						canRedirect = false // don't redirect here at least after a row event
					}
				}
			}
			if canRedirect {
				op, targetTableID := s.optimist.PendingRedirectOperation()
				if op != nil {
					// for optimistic sharding mode, if a new sharding group is resolved when syncer is redirecting,
					// instead of using the currentLocation, the next redirection should share the same latestLocation with the current shardingResync.
					// This is to avoid syncer syncs to current shardingResync.latestLocation before,
					// we may miss some rows events if we don't check the row events between currentLocation and shardingResync.latestLocation.
					// TODO: This will cause a potential performance issue. If we have multiple tables not resolved after a huge amount of binlogs but resolved in a short time,
					//   	current implementation will cause syncer to redirect and replay the binlogs in this segment several times. One possible solution is to
					//      interrupt current resync once syncer meets a new redirect operation, force other tables to be resolved together in the interrupted shardingResync.
					//      If we want to do this, we also need to remove the target table check at https://github.com/pingcap/tiflow/blob/af849add84bf26feb2628d3e1e4344830b915fd9/dm/syncer/syncer.go#L2489
					endLocation := &currentLocation
					if shardingReSync != nil {
						endLocation = &shardingReSync.latestLocation
					}
					resolved := s.resolveOptimisticDDL(&eventContext{
						shardingReSyncCh: &shardingReSyncCh,
						currentLocation:  endLocation,
					}, &filter.Table{Schema: op.UpSchema, Name: op.UpTable}, utils.UnpackTableID(targetTableID))
					// if resolved and targetTableID == shardingResync.TargetTableID, we should resolve this resync before we continue
					if resolved && shardingReSync != nil && targetTableID == shardingReSync.targetTable.String() {
						err = closeShardingResync()
						if err != nil {
							return err
						}
					}
					continue
				}
			}
		}

		startTime := time.Now()
		e, err = s.getEvent(s.runCtx, currentLocation)

		failpoint.Inject("SafeModeExit", func(val failpoint.Value) {
			if intVal, ok := val.(int); ok && intVal == 1 {
				s.tctx.L().Warn("fail to get event", zap.String("failpoint", "SafeModeExit"))
				err = errors.New("connect: connection refused")
			}
		})
		failpoint.Inject("GetEventErrorInTxn", func(val failpoint.Value) {
			if intVal, ok := val.(int); ok && intVal == eventIndex && failOnceForTest.CAS(false, true) {
				err = errors.New("failpoint triggered")
				s.tctx.L().Warn("failed to get event", zap.Int("event_index", eventIndex),
					zap.Any("cur_pos", currentLocation), zap.Any("las_pos", lastLocation),
					zap.Any("pos", e.Header.LogPos), log.ShortError(err))
			}
		})
		failpoint.Inject("SleepInTxn", func(val failpoint.Value) {
			if intVal, ok := val.(int); ok && intVal == eventIndex && failOnceForTest.CAS(false, true) {
				s.tctx.L().Warn("start to sleep 6s and continue for this event", zap.Int("event_index", eventIndex),
					zap.Any("cur_pos", currentLocation), zap.Any("las_pos", lastLocation),
					zap.Any("pos", e.Header.LogPos))
				time.Sleep(6 * time.Second)
			}
		})
		switch {
		case err == context.Canceled:
			s.tctx.L().Info("binlog replication main routine quit(context canceled)!", zap.Stringer("last location", lastLocation))
			return nil
		case err == context.DeadlineExceeded:
			s.tctx.L().Info("deadline exceeded when fetching binlog event")
			continue
		// TODO: if we can maintain the lastLocation inside streamerController, no need to expose this logic to syncer
		case isDuplicateServerIDError(err):
			// if the server id is already used, need to use a new server id
			s.tctx.L().Info("server id is already used by another slave, will change to a new server id and get event again")
			err1 := s.streamerController.UpdateServerIDAndResetReplication(s.tctx, lastLocation)
			if err1 != nil {
				return err1
			}
			continue
		case err == relay.ErrorMaybeDuplicateEvent:
			s.tctx.L().Warn("read binlog met a truncated file, will skip events that has been consumed")
			err = maybeSkipNRowsEvent(eventIndex)
			if err == nil {
				continue
			}
			s.tctx.L().Warn("skip duplicate rows event failed", zap.Error(err))
		}

		if err != nil {
			s.tctx.L().Error("fail to fetch binlog", log.ShortError(err))

			if isConnectionRefusedError(err) {
				return err
			}

			// TODO: if we can maintain the lastLocation inside streamerController, no need to expose this logic to syncer
			if s.streamerController.CanRetry(err) {
				// GlobalPoint is the last finished transaction location
				err = s.streamerController.ResetReplicationSyncer(s.tctx, s.checkpoint.GlobalPoint())
				if err != nil {
					return err
				}
				s.tctx.L().Info("reset replication binlog puller", zap.Any("pos", s.checkpoint.GlobalPoint()))
				if err = maybeSkipNRowsEvent(eventIndex); err != nil {
					return err
				}
				continue
			}

			return terror.ErrSyncerGetEvent.Generate(err)
		}

		failpoint.Inject("IgnoreSomeTypeEvent", func(val failpoint.Value) {
			if e.Header.EventType.String() == val.(string) {
				s.tctx.L().Debug("IgnoreSomeTypeEvent", zap.Reflect("event", e))
				failpoint.Continue()
			}
		})

		// time duration for reading an event from relay log or upstream master.
		s.metricsProxies.Metrics.BinlogReadDurationHistogram.Observe(time.Since(startTime).Seconds())
		startTime = time.Now() // reset start time for the next metric.

		s.metricsProxies.Metrics.BinlogSyncerPosGauge.Set(float64(e.Header.LogPos))
		index, err := binlog.GetFilenameIndex(lastLocation.Position.Name)
		if err != nil {
			s.tctx.L().Warn("fail to get index number of binlog file, may because only specify GTID and hasn't saved according binlog position", log.ShortError(err))
		} else {
			s.metricsProxies.Metrics.BinlogSyncerFileGauge.Set(float64(index))
		}
		s.binlogSizeCount.Add(int64(e.Header.EventSize))
		s.metricsProxies.Metrics.BinlogEventSizeHistogram.Observe(float64(e.Header.EventSize))

		failpoint.Inject("ProcessBinlogSlowDown", nil)

		s.tctx.L().Debug("receive binlog event", zap.Reflect("header", e.Header))

		// support QueryEvent and RowsEvent
		// we calculate startLocation and endLocation(currentLocation) for Query event here
		// set startLocation empty for other events to avoid misuse
		startLocation = binlog.Location{}
		if _, ok := e.Event.(*replication.GenericEvent); !ok {
			lastEvent = e
		}
		switch ev := e.Event.(type) {
		case *replication.QueryEvent, *replication.RowsEvent:
			startLocation = binlog.NewLocation(
				mysql.Position{
					Name: lastLocation.Position.Name,
					Pos:  e.Header.LogPos - e.Header.EventSize,
				},
				lastLocation.GetGTID(),
			)
			startLocation.Suffix = currentLocation.Suffix

			endSuffix := startLocation.Suffix
			if s.isReplacingOrInjectingErr {
				endSuffix++
			}
			currentLocation = binlog.NewLocation(
				mysql.Position{
					Name: lastLocation.Position.Name,
					Pos:  e.Header.LogPos,
				},
				currentLocation.GetGTID(),
			)
			currentLocation.Suffix = endSuffix

			// TODO: can be removed in the future
			if queryEvent, ok := ev.(*replication.QueryEvent); ok {
				err = currentLocation.SetGTID(queryEvent.GSet)
				if err != nil {
					return terror.Annotatef(err, "fail to record GTID %v", queryEvent.GSet)
				}
			}

			if !s.isReplacingOrInjectingErr {
				apply, op := s.errOperatorHolder.MatchAndApply(startLocation, currentLocation, e)
				if apply {
					if op == pb.ErrorOp_Replace || op == pb.ErrorOp_Inject {
						s.isReplacingOrInjectingErr = true
						// revert currentLocation to startLocation
						currentLocation = startLocation
					} else if op == pb.ErrorOp_Skip {
						queryEvent := ev.(*replication.QueryEvent)
						ec := eventContext{
							tctx:            s.tctx,
							header:          e.Header,
							startLocation:   &startLocation,
							currentLocation: &currentLocation,
							lastLocation:    &lastLocation,
						}
						var sourceTbls map[string]map[string]struct{}
						sourceTbls, err = s.trackOriginDDL(queryEvent, ec)
						if err != nil {
							s.tctx.L().Warn("failed to track query when handle-error skip", zap.Error(err), zap.ByteString("sql", queryEvent.Query))
						}

						s.saveGlobalPoint(currentLocation)
						for sourceSchema, tableMap := range sourceTbls {
							if sourceSchema == "" {
								continue
							}
							for sourceTable := range tableMap {
								s.saveTablePoint(&filter.Table{Schema: sourceSchema, Name: sourceTable}, currentLocation)
							}
						}
						err = s.flushJobs()
						if err != nil {
							s.tctx.L().Warn("failed to flush jobs when handle-error skip", zap.Error(err))
						} else {
							s.tctx.L().Info("flush jobs when handle-error skip")
						}
					}
					// skip the current event
					continue
				}
			}
			// set endLocation.Suffix=0 of last replace or inject event
			if currentLocation.Suffix > 0 && e.Header.EventSize > 0 {
				currentLocation.Suffix = 0
				s.isReplacingOrInjectingErr = false
				s.locations.reset(currentLocation)
				if !s.errOperatorHolder.IsInject(startLocation) {
					// replace operator need redirect to currentLocation
					if err = s.streamerController.ResetReplicationSyncer(s.runCtx, currentLocation); err != nil {
						return err
					}
				}
			}
		}

		// check pass SafeModeExitLoc and try disable safe mode, but not in sharding or replacing error
		safeModeExitLoc := s.checkpoint.SafeModeExitPoint()
		if safeModeExitLoc != nil && !s.isReplacingOrInjectingErr && shardingReSync == nil {
			// TODO: for RowsEvent (in fact other than QueryEvent), `currentLocation` is updated in `handleRowsEvent`
			// so here the meaning of `currentLocation` is the location of last event
			if binlog.CompareLocation(currentLocation, *safeModeExitLoc, s.cfg.EnableGTID) > 0 {
				s.checkpoint.SaveSafeModeExitPoint(nil)
				// must flush here to avoid the following situation:
				// 1. quit safe mode
				// 2. push forward and replicate some sqls after safeModeExitPoint to downstream
				// 3. quit because of network error, fail to flush global checkpoint and new safeModeExitPoint to downstream
				// 4. restart again, quit safe mode at safeModeExitPoint, but some sqls after this location have already been replicated to the downstream
				if err = s.checkpoint.FlushSafeModeExitPoint(s.runCtx); err != nil {
					return err
				}
				if err = s.safeMode.Add(s.runCtx, -1); err != nil {
					return err
				}
			}
		}

		// set exitSafeModeTS when meet first binlog
		if s.firstMeetBinlogTS == nil && s.cliArgs != nil && s.cliArgs.SafeModeDuration != "" && int64(e.Header.Timestamp) != 0 && e.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT {
			if checkErr := s.initSafeModeExitTS(int64(e.Header.Timestamp)); checkErr != nil {
				return checkErr
			}
		}
		// check if need to exit safe mode by binlog header TS
		if s.exitSafeModeTS != nil && !s.isReplacingOrInjectingErr && shardingReSync == nil {
			if checkErr := s.checkAndExitSafeModeByBinlogTS(s.runCtx, int64(e.Header.Timestamp)); checkErr != nil {
				return checkErr
			}
		}
		ec := eventContext{
			tctx:                s.runCtx,
			header:              e.Header,
			startLocation:       &startLocation,
			currentLocation:     &currentLocation,
			lastLocation:        &lastLocation,
			shardingReSync:      shardingReSync,
			closeShardingResync: closeShardingResync,
			traceSource:         traceSource,
			safeMode:            s.safeMode.Enable(),
			startTime:           startTime,
			shardingReSyncCh:    &shardingReSyncCh,
		}

		var originSQL string // show origin sql when error, only ddl now
		var err2 error
		var sourceTable *filter.Table

		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			err2 = s.handleRotateEvent(ev, ec)
		case *replication.RowsEvent:
			eventIndex++
			s.metricsProxies.Metrics.BinlogEventRowHistogram.Observe(float64(len(ev.Rows)))
			sourceTable, err2 = s.handleRowsEvent(ev, ec)
			if sourceTable != nil && err2 == nil && s.cfg.EnableGTID {
				if _, ok := affectedSourceTables[sourceTable.Schema]; !ok {
					affectedSourceTables[sourceTable.Schema] = make(map[string]struct{})
				}
				affectedSourceTables[sourceTable.Schema][sourceTable.Name] = struct{}{}
			}
		case *replication.QueryEvent:
			originSQL = strings.TrimSpace(string(ev.Query))
			err2 = s.handleQueryEvent(ev, ec, originSQL)
		case *replication.XIDEvent:
			// reset eventIndex and force safeMode flag here.
			eventIndex = 0
			currentLocation.Position.Pos = e.Header.LogPos
			for schemaName, tableMap := range affectedSourceTables {
				for table := range tableMap {
					s.saveTablePoint(&filter.Table{Schema: schemaName, Name: table}, currentLocation)
				}
			}
			affectedSourceTables = make(map[string]map[string]struct{})
			if shardingReSync != nil {
				shardingReSync.currLocation.Position.Pos = e.Header.LogPos
				shardingReSync.currLocation.Suffix = currentLocation.Suffix
				err = shardingReSync.currLocation.SetGTID(ev.GSet)
				if err != nil {
					return terror.Annotatef(err, "fail to record GTID %v", ev.GSet)
				}

				// only need compare binlog position?
				lastLocation = shardingReSync.currLocation
				if binlog.CompareLocation(shardingReSync.currLocation, shardingReSync.latestLocation, s.cfg.EnableGTID) >= 0 {
					s.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "XID"), zap.Stringer("re-shard", shardingReSync))
					err = closeShardingResync()
					if err != nil {
						return terror.Annotatef(err, "shard group current location %s", shardingReSync.currLocation)
					}
					continue
				}
			}

			s.tctx.L().Debug("", zap.String("event", "XID"), zap.Stringer("last location", lastLocation), log.WrapStringerField("location", currentLocation))
			lastLocation.Position.Pos = e.Header.LogPos // update lastPos
			err = lastLocation.SetGTID(ev.GSet)
			if err != nil {
				return terror.Annotatef(err, "fail to record GTID %v", ev.GSet)
			}

			job := newXIDJob(currentLocation, startLocation, currentLocation)
			_, err2 = s.handleJobFunc(job)
		case *replication.GenericEvent:
			if e.Header.EventType == replication.HEARTBEAT_EVENT {
				// flush checkpoint even if there are no real binlog events
				if s.checkpoint.LastFlushOutdated() {
					s.tctx.L().Info("meet heartbeat event and then flush jobs")
					err2 = s.flushJobs()
				}
			}
		case *replication.GTIDEvent, *replication.MariadbGTIDEvent:
			currentGTID, err2 = event.GetGTIDStr(e)
			if err2 != nil {
				return err2
			}
			currentLocation.Position.Pos = e.Header.LogPos // update currentLocation's position here to make sure optimism redirection can end correctly
			if s.cfg.EnableGTID {
				// TODO: add mariaDB integration test
				err2 = advanceCurrentLocationGtidSet(e)
				if err2 != nil {
					return err2
				}
			}
		}
		if err2 != nil {
			if err := s.handleEventError(err2, startLocation, currentLocation, e.Header.EventType == replication.QUERY_EVENT, originSQL); err != nil {
				return err
			}
		}
		if waitXIDStatus(s.waitXIDJob.Load()) == waitComplete {
			// already wait until XID event, we can stop sync now, s.runcancel will be called in defer func
			return nil
		}
	}
}

func (s *Syncer) initSafeModeExitTS(firstBinlogTS int64) error {
	// see more in https://github.com/pingcap/tiflow/pull/4601#discussion_r814446628
	duration, err := time.ParseDuration(s.cliArgs.SafeModeDuration)
	if err != nil {
		return err
	}
	s.firstMeetBinlogTS = &firstBinlogTS
	exitTS := firstBinlogTS + int64(duration.Seconds())
	s.exitSafeModeTS = &exitTS
	s.tctx.L().Info("safe-mode will disable by task cli args", zap.Int64("ts", exitTS))
	return nil
}

func (s *Syncer) checkAndExitSafeModeByBinlogTS(ctx *tcontext.Context, ts int64) error {
	if s.exitSafeModeTS != nil && ts > *s.exitSafeModeTS {
		if err := s.safeMode.Add(ctx, -1); err != nil {
			return err
		}
		s.exitSafeModeTS = nil
		if !s.safeMode.Enable() {
			s.tctx.L().Info("safe-mode disable by task cli args", zap.Int64("ts in binlog", ts))
		}
		// delete cliArgs in etcd
		clone := *s.cliArgs
		clone.SafeModeDuration = ""
		if err2 := ha.PutTaskCliArgs(s.cli, s.cfg.Name, []string{s.cfg.SourceID}, clone); err2 != nil {
			s.tctx.L().Error("failed to clean safe-mode-duration in task cli args", zap.Error(err2))
		} else {
			s.Lock()
			s.cliArgs.SafeModeDuration = ""
			s.Unlock()
		}
	}
	return nil
}

type eventContext struct {
	tctx                *tcontext.Context
	header              *replication.EventHeader
	startLocation       *binlog.Location
	currentLocation     *binlog.Location
	lastLocation        *binlog.Location
	shardingReSync      *ShardingReSync
	closeShardingResync func() error
	traceSource         string
	// safeMode is the value of syncer.safeMode when process this event
	// syncer.safeMode's value may change on the fly, e.g. after event by pass the safeModeExitPoint
	safeMode         bool
	startTime        time.Time
	shardingReSyncCh *chan *ShardingReSync
}

// TODO: Further split into smaller functions and group common arguments into a context struct.
func (s *Syncer) handleRotateEvent(ev *replication.RotateEvent, ec eventContext) error {
	failpoint.Inject("MakeFakeRotateEvent", func(val failpoint.Value) {
		ec.header.LogPos = 0
		ev.NextLogName = []byte(val.(string))
		ec.tctx.L().Info("MakeFakeRotateEvent", zap.String("fake file name", string(ev.NextLogName)))
	})

	if utils.IsFakeRotateEvent(ec.header) {
		if fileName := string(ev.NextLogName); mysql.CompareBinlogFileName(fileName, ec.lastLocation.Position.Name) <= 0 {
			// NOTE A fake rotate event is also generated when a master-slave switch occurs upstream, and the binlog filename may be rolled back in this case
			// when the DM is updating based on the GTID, we also update the filename of the lastLocation
			if s.cfg.EnableGTID {
				ec.lastLocation.Position.Name = fileName
			}
			return nil // not rotate to the next binlog file, ignore it
		}
		// when user starts a new task with GTID and no binlog file name, we can't know active relay log at init time
		// at this case, we update active relay log when receive fake rotate event
		if !s.recordedActiveRelayLog {
			if err := s.updateActiveRelayLog(mysql.Position{
				Name: string(ev.NextLogName),
				Pos:  uint32(ev.Position),
			}); err != nil {
				ec.tctx.L().Warn("failed to update active relay log, will try to update when flush checkpoint",
					zap.ByteString("NextLogName", ev.NextLogName),
					zap.Uint64("Position", ev.Position),
					zap.Error(err))
			} else {
				s.recordedActiveRelayLog = true
			}
		}
	}

	*ec.currentLocation = binlog.NewLocation(
		mysql.Position{
			Name: string(ev.NextLogName),
			Pos:  uint32(ev.Position),
		},
		ec.currentLocation.GetGTID(),
	)

	if binlog.CompareLocation(*ec.currentLocation, *ec.lastLocation, s.cfg.EnableGTID) >= 0 {
		*ec.lastLocation = *ec.currentLocation
	}

	if ec.shardingReSync != nil {
		if binlog.CompareLocation(*ec.currentLocation, ec.shardingReSync.currLocation, s.cfg.EnableGTID) > 0 {
			ec.shardingReSync.currLocation = *ec.currentLocation
		}
		if binlog.CompareLocation(ec.shardingReSync.currLocation, ec.shardingReSync.latestLocation, s.cfg.EnableGTID) >= 0 {
			ec.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "rotate"), zap.Stringer("re-shard", ec.shardingReSync))
			err := ec.closeShardingResync()
			if err != nil {
				return err
			}
		} else {
			ec.tctx.L().Debug("re-replicate shard group", zap.String("event", "rotate"), log.WrapStringerField("location", ec.currentLocation), zap.Reflect("re-shard", ec.shardingReSync))
		}
		return nil
	}

	ec.tctx.L().Info("", zap.String("event", "rotate"), log.WrapStringerField("location", ec.currentLocation))
	return nil
}

// getFlushSeq is used for assigning an auto-incremental sequence number for each flush job.
func (s *Syncer) getFlushSeq() int64 {
	s.flushSeq++
	return s.flushSeq
}

// handleRowsEvent returns (affected table, error), affected table means this table's event is committed to handleJob.
func (s *Syncer) handleRowsEvent(ev *replication.RowsEvent, ec eventContext) (*filter.Table, error) {
	sourceTable := &filter.Table{
		Schema: string(ev.Table.Schema),
		Name:   string(ev.Table.Table),
	}
	targetTable := s.route(sourceTable)

	*ec.currentLocation = binlog.NewLocation(
		mysql.Position{
			Name: ec.lastLocation.Position.Name,
			Pos:  ec.header.LogPos,
		},
		ec.currentLocation.GetGTID(),
	)

	if ec.shardingReSync != nil {
		ec.shardingReSync.currLocation = *ec.currentLocation
		if binlog.CompareLocation(ec.shardingReSync.currLocation, ec.shardingReSync.latestLocation, s.cfg.EnableGTID) >= 0 {
			ec.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "row"), zap.Stringer("re-shard", ec.shardingReSync))
			return nil, ec.closeShardingResync()
		}
		if ec.shardingReSync.targetTable.String() != targetTable.String() {
			// in re-syncing, ignore non current sharding group's events
			ec.tctx.L().Debug("skip event in re-replicating shard group", zap.String("event", "row"), zap.Stringer("re-shard", ec.shardingReSync))
			return nil, nil
		}
	}

	// For DML position before table checkpoint, ignore it. When the position equals to table checkpoint, this event may
	// be partially replicated to downstream, we rely on safe-mode to handle it.
	if s.checkpoint.IsOlderThanTablePoint(sourceTable, *ec.currentLocation) {
		ec.tctx.L().Debug("ignore obsolete event that is old than table checkpoint",
			zap.String("event", "row"),
			log.WrapStringerField("location", ec.currentLocation),
			zap.Stringer("source table", sourceTable))
		return nil, nil
	}

	ec.tctx.L().Debug("",
		zap.String("event", "row"),
		zap.Stringer("source table", sourceTable),
		zap.Stringer("target table", targetTable),
		log.WrapStringerField("location", ec.currentLocation),
		zap.Reflect("raw event data", ev.Rows))

	needSkip, err := s.skipRowsEvent(sourceTable, ec.header.EventType)
	if err != nil {
		return nil, err
	}
	if needSkip {
		s.metricsProxies.SkipBinlogDurationHistogram.WithLabelValues("rows", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		// for RowsEvent, we should record lastLocation rather than currentLocation
		return nil, s.recordSkipSQLsLocation(&ec)
	}

	if (s.cfg.ShardMode == config.ShardPessimistic && s.sgk.InSyncing(sourceTable, targetTable, *ec.currentLocation)) ||
		(s.cfg.ShardMode == config.ShardOptimistic && s.osgk.inConflictStage(sourceTable, targetTable)) {
		// if in unsync stage and not before active DDL, filter it
		// if in sharding re-sync stage and not before active DDL (the next DDL to be synced), filter it
		ec.tctx.L().Debug("replicate sharding DDL, filter Rows event",
			zap.String("event", "row"),
			zap.Stringer("source", sourceTable),
			log.WrapStringerField("location", ec.currentLocation))
		return nil, nil
	}

	// TODO(csuzhangxc): check performance of `getTable` from schema tracker.
	tableInfo, err := s.getTableInfo(ec.tctx, sourceTable, targetTable)
	if err != nil {
		return nil, terror.WithScope(err, terror.ScopeDownstream)
	}
	originRows, err := s.mappingDML(sourceTable, tableInfo, ev.Rows)
	if err != nil {
		return nil, err
	}
	if err2 := checkLogColumns(ev.SkippedColumns); err2 != nil {
		return nil, err2
	}

	extRows := generateExtendColumn(originRows, s.tableRouter, sourceTable, s.cfg.SourceID)

	var dmls []*sqlmodel.RowChange

	param := &genDMLParam{
		targetTable:     targetTable,
		originalData:    originRows,
		sourceTableInfo: tableInfo,
		sourceTable:     sourceTable,
		extendData:      extRows,
	}

	switch ec.header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		exprFilter, err2 := s.exprFilterGroup.GetInsertExprs(sourceTable, tableInfo)
		if err2 != nil {
			return nil, err2
		}

		param.safeMode = ec.safeMode
		dmls, err = s.genAndFilterInsertDMLs(ec.tctx, param, exprFilter)
		if err != nil {
			return nil, terror.Annotatef(err, "gen insert sqls failed, sourceTable: %v, targetTable: %v", sourceTable, targetTable)
		}
		s.metricsProxies.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageGenWriteRows, s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())

	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		oldExprFilter, newExprFilter, err2 := s.exprFilterGroup.GetUpdateExprs(sourceTable, tableInfo)
		if err2 != nil {
			return nil, err2
		}

		param.safeMode = ec.safeMode
		dmls, err = s.genAndFilterUpdateDMLs(ec.tctx, param, oldExprFilter, newExprFilter)
		if err != nil {
			return nil, terror.Annotatef(err, "gen update sqls failed, sourceTable: %v, targetTable: %v", sourceTable, targetTable)
		}
		s.metricsProxies.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageGenUpdateRows, s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())

	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		exprFilter, err2 := s.exprFilterGroup.GetDeleteExprs(sourceTable, tableInfo)
		if err2 != nil {
			return nil, err2
		}

		dmls, err = s.genAndFilterDeleteDMLs(ec.tctx, param, exprFilter)
		if err != nil {
			return nil, terror.Annotatef(err, "gen delete sqls failed, sourceTable: %v, targetTable: %v", sourceTable, targetTable)
		}
		s.metricsProxies.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageGenDeleteRows, s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())

	default:
		ec.tctx.L().Debug("ignoring unrecognized event", zap.String("event", "row"), zap.Stringer("type", ec.header.EventType))
		return nil, nil
	}

	startTime := time.Now()

	metricTp := ""
	for i := range dmls {
		metricTp = dmlMetric[dmls[i].Type()]
		job := newDMLJob(dmls[i], &ec)
		added2Queue, err2 := s.handleJobFunc(job)
		if err2 != nil || !added2Queue {
			return nil, err2
		}
	}
	s.metricsProxies.DispatchBinlogDurationHistogram.WithLabelValues(metricTp, s.cfg.Name, s.cfg.SourceID).Observe(time.Since(startTime).Seconds())

	if len(sourceTable.Schema) != 0 && !s.cfg.EnableGTID {
		// when in position-based replication, now events before table checkpoint is sent to queue. But in GTID-based
		// replication events may share one GTID, so event whose end position is equal to table checkpoint may not be
		// sent to queue. We may need event index in GTID to resolve it.
		// when in gtid-based replication, we can't save gtid to table point because this transaction may not
		// have been fully replicated to downstream. So we don't save table point here but record the affected tables and returns
		s.saveTablePoint(sourceTable, *ec.currentLocation)
	}

	failpoint.Inject("flushFirstJob", func() {
		if waitJobsDoneForTest {
			s.tctx.L().Info("trigger flushFirstJob")
			waitJobsDoneForTest = false

			err2 := s.flushJobs()
			if err2 != nil {
				s.tctx.L().DPanic("flush checkpoint failed", zap.Error(err2))
			}
			failpoint.Return(nil, nil)
		}
	})

	return sourceTable, s.flushIfOutdated()
}

type queryEventContext struct {
	*eventContext

	p         *parser.Parser // used parser
	ddlSchema string         // used schema
	originSQL string         // before split
	// split multi-schema change DDL into multiple one schema change DDL due to TiDB's limitation
	splitDDLs      []string // after split before online ddl
	appliedDDLs    []string // after onlineDDL apply if onlineDDL != nil
	needHandleDDLs []string // after route

	shardingDDLInfo *ddlInfo
	trackInfos      []*ddlInfo
	sourceTbls      map[string]map[string]struct{} // db name -> tb name
	onlineDDLTable  *filter.Table
	eventStatusVars []byte // binlog StatusVars
}

func (qec *queryEventContext) String() string {
	var startLocation, currentLocation, lastLocation string
	if qec.startLocation != nil {
		startLocation = qec.startLocation.String()
	}
	if qec.currentLocation != nil {
		currentLocation = qec.currentLocation.String()
	}
	if qec.lastLocation != nil {
		lastLocation = qec.lastLocation.String()
	}
	var needHandleDDLs, shardingReSync string
	if qec.needHandleDDLs != nil {
		needHandleDDLs = strings.Join(qec.needHandleDDLs, ",")
	}
	if qec.shardingReSync != nil {
		shardingReSync = qec.shardingReSync.String()
	}
	trackInfos := make([]string, 0, len(qec.trackInfos))
	for _, trackInfo := range qec.trackInfos {
		trackInfos = append(trackInfos, trackInfo.String())
	}
	return fmt.Sprintf("{schema: %s, originSQL: %s, startLocation: %s, currentLocation: %s, lastLocation: %s, re-sync: %s, needHandleDDLs: %s, trackInfos: %s}",
		qec.ddlSchema, qec.originSQL, startLocation, currentLocation, lastLocation, shardingReSync, needHandleDDLs, strings.Join(trackInfos, ","))
}

// generateExtendColumn generate extended columns by extractor.
func generateExtendColumn(data [][]interface{}, r *regexprrouter.RouteTable, table *filter.Table, sourceID string) [][]interface{} {
	extendCol, extendVal := r.FetchExtendColumn(table.Schema, table.Name, sourceID)
	if len(extendCol) == 0 {
		return nil
	}

	rows := make([][]interface{}, len(data))
	for i := range data {
		rows[i] = data[i]
		for _, v := range extendVal {
			rows[i] = append(rows[i], v)
		}
	}
	return rows
}

func (s *Syncer) handleQueryEvent(ev *replication.QueryEvent, ec eventContext, originSQL string) (err error) {
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
		s.tctx.L().Error("get charset codec failed, will treat query as utf8", zap.Error(err))
	} else if codec != nil {
		converted, err2 := codec.NewDecoder().String(originSQL)
		if err2 != nil {
			s.tctx.L().Error("convert query string failed, will treat query as utf8", zap.Error(err2))
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
		needSkip, err2 := s.skipSQLByPattern(qec.originSQL)
		if err2 != nil {
			err = err2
			return
		}
		if !needSkip {
			return
		}
		// don't return error if filter success
		s.metricsProxies.SkipBinlogDurationHistogram.WithLabelValues("query", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(ec.startTime).Seconds())
		ec.tctx.L().Warn("skip event", zap.String("event", "query"), zap.Stringer("query event context", qec))
		*ec.lastLocation = *ec.currentLocation // before record skip location, update lastLocation
		err = s.recordSkipSQLsLocation(&ec)
	}()

	qec.p, err = event.GetParserForStatusVars(ev.StatusVars)
	if err != nil {
		s.tctx.L().Warn("found error when get sql_mode from binlog status_vars", zap.Error(err))
	}

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
			ignore, err2 := s.skipRowsEvent(table, replication.QUERY_EVENT)
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
		if binlog.CompareLocation(qec.shardingReSync.currLocation, qec.shardingReSync.latestLocation, s.cfg.EnableGTID) >= 0 {
			qec.tctx.L().Info("re-replicate shard group was completed", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
			return qec.closeShardingResync()
		} else if s.cfg.ShardMode != config.ShardOptimistic {
			// in re-syncing, we can simply skip all DDLs.
			// for pessimistic shard mode,
			// all ddls have been added to sharding DDL sequence
			// only update lastPos when the query is a real DDL
			*qec.lastLocation = qec.shardingReSync.currLocation
			qec.tctx.L().Debug("skip event in re-replicating sharding group", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
			return nil
		}
		// optimistic shard mode handle situation will be handled through table point after
		// we split ddls and handle the appliedDDLs
	}

	qec.tctx.L().Info("ready to split ddl", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
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
		sqls, err2 := s.processOneDDL(qec, sql)
		if err2 != nil {
			qec.tctx.L().Error("fail to process ddl", zap.String("event", "query"), zap.Stringer("queryEventContext", qec), log.ShortError(err2))
			return err2
		}
		qec.appliedDDLs = append(qec.appliedDDLs, sqls...)
	}
	qec.tctx.L().Info("resolve sql", zap.String("event", "query"), zap.Strings("appliedDDLs", qec.appliedDDLs), zap.Stringer("queryEventContext", qec))

	s.metricsProxies.BinlogEventCost.WithLabelValues(metrics.BinlogEventCostStageGenQuery, s.cfg.Name, s.cfg.WorkerName, s.cfg.SourceID).Observe(time.Since(qec.startTime).Seconds())

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
		ddlInfo, err2 := s.genDDLInfo(qec, sql)
		if err2 != nil {
			return err2
		}
		sourceTable := ddlInfo.sourceTables[0]
		targetTable := ddlInfo.targetTables[0]
		if len(ddlInfo.routedDDL) == 0 {
			s.metricsProxies.SkipBinlogDurationHistogram.WithLabelValues("query", s.cfg.Name, s.cfg.SourceID).Observe(time.Since(qec.startTime).Seconds())
			qec.tctx.L().Warn("skip event", zap.String("event", "query"), zap.String("statement", sql), zap.String("schema", qec.ddlSchema))
			continue
		}

		// DDL is sequentially synchronized in this syncer's main process goroutine
		// filter DDL that is older or same as table checkpoint, to avoid sync again for already synced DDLs
		if s.checkpoint.IsOlderThanTablePoint(sourceTable, *qec.currentLocation) {
			qec.tctx.L().Info("filter obsolete DDL", zap.String("event", "query"), zap.String("statement", sql), log.WrapStringerField("location", qec.currentLocation))
			continue
		}

		// pre-filter of sharding
		if s.cfg.ShardMode == config.ShardPessimistic {
			switch ddlInfo.originStmt.(type) {
			case *ast.DropDatabaseStmt:
				err = s.dropSchemaInSharding(qec.tctx, sourceTable.Schema)
				if err != nil {
					return err
				}
				continue
			case *ast.DropTableStmt:
				sourceTableID := utils.GenTableID(sourceTable)
				err = s.sgk.LeaveGroup(targetTable, []string{sourceTableID})
				if err != nil {
					return err
				}
				err = s.checkpoint.DeleteTablePoint(qec.tctx, sourceTable)
				if err != nil {
					return err
				}
				continue
			case *ast.TruncateTableStmt:
				qec.tctx.L().Info("filter truncate table statement in shard group", zap.String("event", "query"), zap.String("statement", ddlInfo.routedDDL))
				continue
			}

			// in sharding mode, we only support to do one ddl in one event
			if qec.shardingDDLInfo == nil {
				qec.shardingDDLInfo = ddlInfo
			} else if qec.shardingDDLInfo.sourceTables[0].String() != sourceTable.String() {
				return terror.ErrSyncerUnitDDLOnMultipleTable.Generate(qec.originSQL)
			}
		} else if s.cfg.ShardMode == config.ShardOptimistic {
			if s.osgk.inConflictStage(sourceTable, targetTable) {
				// if in unsync stage and not before active DDL, filter it
				// if in sharding re-sync stage and not before active DDL (the next DDL to be synced), filter it
				ec.tctx.L().Info("replicate sharding DDL, filter Conflicted table's ddl events",
					zap.String("event", "query"),
					zap.Stringer("source", sourceTable),
					log.WrapStringerField("location", ec.currentLocation))
				continue
			} else if ec.shardingReSync != nil && ec.shardingReSync.targetTable.String() != targetTable.String() {
				// in re-syncing, ignore non current sharding group's events
				ec.tctx.L().Info("skip event in re-replicating shard group", zap.String("event", "query"), zap.Stringer("re-shard", ec.shardingReSync))
				continue
			}
			switch ddlInfo.originStmt.(type) {
			case *ast.TruncateTableStmt:
				qec.tctx.L().Info("filter truncate table statement in shard group", zap.String("event", "query"), zap.String("statement", ddlInfo.routedDDL))
				continue
			case *ast.RenameTableStmt:
				return terror.ErrSyncerUnsupportedStmt.Generate("RENAME TABLE", config.ShardOptimistic)
			}
		}

		qec.needHandleDDLs = append(qec.needHandleDDLs, ddlInfo.routedDDL)
		qec.trackInfos = append(qec.trackInfos, ddlInfo)
		// TODO: current table checkpoints will be deleted in track ddls, but created and updated in flush checkpoints,
		//       we should use a better mechanism to combine these operations
		if s.cfg.ShardMode == "" {
			recordSourceTbls(qec.sourceTbls, ddlInfo.originStmt, sourceTable)
		}
	}

	qec.tctx.L().Info("prepare to handle ddls", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	if len(qec.needHandleDDLs) == 0 {
		qec.tctx.L().Info("skip event, need handled ddls is empty", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
		return s.recordSkipSQLsLocation(qec.eventContext)
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
	if err = s.flushJobs(); err != nil {
		return err
	}

	switch s.cfg.ShardMode {
	case "":
		return s.handleQueryEventNoSharding(qec)
	case config.ShardOptimistic:
		return s.handleQueryEventOptimistic(qec)
	case config.ShardPessimistic:
		return s.handleQueryEventPessimistic(qec)
	}
	return errors.Errorf("unsupported shard-mode %s, should not happened", s.cfg.ShardMode)
}

func (s *Syncer) handleQueryEventNoSharding(qec *queryEventContext) error {
	qec.tctx.L().Info("start to handle ddls in normal mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

	// interrupted after flush old checkpoint and before track DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err := handleFlushCheckpointStage(1, val.(int), "before track DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	// run trackDDL before add ddl job to make sure checkpoint can be flushed
	for _, trackInfo := range qec.trackInfos {
		if err := s.trackDDL(qec.ddlSchema, trackInfo, qec.eventContext); err != nil {
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
	_, err := s.handleJobFunc(job)
	if err != nil {
		return err
	}

	// when add ddl job, will execute ddl and then flush checkpoint.
	// if execute ddl failed, the execError will be set to that error.
	// return nil here to avoid duplicate error message
	err = s.execError.Load()
	if err != nil {
		qec.tctx.L().Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	qec.tctx.L().Info("finish to handle ddls in normal mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

	if qec.onlineDDLTable != nil {
		qec.tctx.L().Info("finish online ddl and clear online ddl metadata in normal mode",
			zap.String("event", "query"),
			zap.Strings("ddls", qec.needHandleDDLs),
			zap.String("raw statement", qec.originSQL),
			zap.Stringer("table", qec.onlineDDLTable))
		err2 := s.onlineDDL.Finish(qec.tctx, qec.onlineDDLTable)
		if err2 != nil {
			return terror.Annotatef(err2, "finish online ddl on %v", qec.onlineDDLTable)
		}
	}

	return nil
}

func (s *Syncer) handleQueryEventPessimistic(qec *queryEventContext) error {
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
		needShardingHandle, group, synced, remain, err = s.sgk.AddGroup(ddlInfo.targetTables[0], []string{sourceTableID}, nil, true)
		if err != nil {
			return err
		}
		annotate = "add table to shard group"
	default:
		needShardingHandle, group, synced, active, remain, err = s.sgk.TrySync(ddlInfo.sourceTables[0], ddlInfo.targetTables[0], *startLocation, *qec.currentLocation, needHandleDDLs)
		if err != nil {
			return err
		}
		annotate = "try to sync table in shard group"
		// meets DDL that will not be processed in sequence sharding
		if !active {
			qec.tctx.L().Info("skip in-activeDDL",
				zap.String("event", "query"),
				zap.Stringer("queryEventContext", qec),
				zap.String("sourceTableID", sourceTableID),
				zap.Bool("in-sharding", needShardingHandle),
				zap.Bool("is-synced", synced),
				zap.Int("unsynced", remain))
			return nil
		}
	}

	qec.tctx.L().Info(annotate,
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
		if err = s.trackDDL(qec.ddlSchema, trackInfo, qec.eventContext); err != nil {
			return err
		}
	}

	if needShardingHandle {
		s.metricsProxies.UnsyncedTableGauge.WithLabelValues(s.cfg.Name, ddlInfo.targetTables[0].String(), s.cfg.SourceID).Set(float64(remain))
		err = s.safeMode.IncrForTable(qec.tctx, ddlInfo.targetTables[0]) // try enable safe-mode when starting syncing for sharding group
		if err != nil {
			return err
		}

		// save checkpoint in memory, don't worry, if error occurred, we can rollback it
		// for non-last sharding DDL's table, this checkpoint will be used to skip binlog event when re-syncing
		// NOTE: when last sharding DDL executed, all this checkpoints will be flushed in the same txn
		qec.tctx.L().Info("save table checkpoint for source",
			zap.String("event", "query"),
			zap.String("sourceTableID", sourceTableID),
			zap.Stringer("start location", startLocation),
			log.WrapStringerField("end location", currentLocation))
		s.saveTablePoint(ddlInfo.sourceTables[0], *currentLocation)
		if !synced {
			qec.tctx.L().Info("source shard group is not synced",
				zap.String("event", "query"),
				zap.String("sourceTableID", sourceTableID),
				zap.Stringer("start location", startLocation),
				log.WrapStringerField("end location", currentLocation))
			return nil
		}

		qec.tctx.L().Info("source shard group is synced",
			zap.String("event", "query"),
			zap.String("sourceTableID", sourceTableID),
			zap.Stringer("start location", startLocation),
			log.WrapStringerField("end location", currentLocation))
		err = s.safeMode.DescForTable(qec.tctx, ddlInfo.targetTables[0]) // try disable safe-mode after sharding group synced
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

		allResolved, err2 := s.sgk.ResolveShardingDDL(ddlInfo.targetTables[0])
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
		shardInfo := s.pessimist.ConstructInfo(ddlInfo.targetTables[0].Schema, ddlInfo.targetTables[0].Name, needHandleDDLs)
		rev, err2 := s.pessimist.PutInfo(qec.tctx.Ctx, shardInfo)
		if err2 != nil {
			return err2
		}
		s.metricsProxies.Metrics.ShardLockResolving.Set(1) // block and wait DDL lock to be synced
		qec.tctx.L().Info("putted shard DDL info", zap.Stringer("info", shardInfo), zap.Int64("revision", rev))

		shardOp, err2 := s.pessimist.GetOperation(qec.tctx.Ctx, shardInfo, rev+1)
		s.metricsProxies.Metrics.ShardLockResolving.Set(0)
		if err2 != nil {
			return err2
		}

		if shardOp.Exec {
			failpoint.Inject("ShardSyncedExecutionExit", func() {
				qec.tctx.L().Warn("exit triggered", zap.String("failpoint", "ShardSyncedExecutionExit"))
				//nolint:errcheck
				s.flushCheckPoints()
				utils.OsExit(1)
			})
			failpoint.Inject("SequenceShardSyncedExecutionExit", func() {
				group := s.sgk.Group(ddlInfo.targetTables[0])
				if group != nil {
					// exit in the first round sequence sharding DDL only
					if group.meta.ActiveIdx() == 1 {
						qec.tctx.L().Warn("exit triggered", zap.String("failpoint", "SequenceShardSyncedExecutionExit"))
						//nolint:errcheck
						s.flushCheckPoints()
						utils.OsExit(1)
					}
				}
			})

			qec.tctx.L().Info("execute DDL job",
				zap.String("event", "query"),
				zap.Stringer("queryEventContext", qec),
				zap.String("sourceTableID", sourceTableID),
				zap.Stringer("operation", shardOp))
		} else {
			qec.tctx.L().Info("ignore DDL job",
				zap.String("event", "query"),
				zap.Stringer("queryEventContext", qec),
				zap.String("sourceTableID", sourceTableID),
				zap.Stringer("operation", shardOp))
		}
	}

	qec.tctx.L().Info("start to handle ddls in shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))

	// interrupted after track DDL and before execute DDL.
	failpoint.Inject("FlushCheckpointStage", func(val failpoint.Value) {
		err = handleFlushCheckpointStage(2, val.(int), "before execute DDL")
		if err != nil {
			failpoint.Return(err)
		}
	})

	job := newDDLJob(qec)
	_, err = s.handleJobFunc(job)
	if err != nil {
		return err
	}

	err = s.execError.Load()
	if err != nil {
		qec.tctx.L().Error("error detected when executing SQL job", log.ShortError(err))
		// nolint:nilerr
		return nil
	}

	if qec.onlineDDLTable != nil {
		err = s.clearOnlineDDL(qec.tctx, ddlInfo.targetTables[0])
		if err != nil {
			return err
		}
	}

	qec.tctx.L().Info("finish to handle ddls in shard mode", zap.String("event", "query"), zap.Stringer("queryEventContext", qec))
	return nil
}

// trackDDL tracks ddl in schemaTracker.
func (s *Syncer) trackDDL(usedSchema string, trackInfo *ddlInfo, ec *eventContext) error {
	var (
		srcTables    = trackInfo.sourceTables
		targetTables = trackInfo.targetTables
		srcTable     = srcTables[0]
	)

	// Make sure the needed tables are all loaded into the schema tracker.
	var (
		shouldExecDDLOnSchemaTracker bool
		shouldSchemaExist            bool
		shouldTableExistNum          int  // tableNames[:shouldTableExistNum] should exist
		shouldRefTableExistNum       int  // tableNames[1:shouldTableExistNum] should exist, since first one is "caller table"
		shouldReTrackDownstreamIndex bool // retrack downstreamIndex
	)

	switch node := trackInfo.originStmt.(type) {
	case *ast.CreateDatabaseStmt:
		shouldExecDDLOnSchemaTracker = true
	case *ast.AlterDatabaseStmt:
		shouldExecDDLOnSchemaTracker = true
		shouldSchemaExist = true
	case *ast.DropDatabaseStmt:
		shouldExecDDLOnSchemaTracker = true
		shouldReTrackDownstreamIndex = true
		if s.cfg.ShardMode == "" {
			if err := s.checkpoint.DeleteSchemaPoint(ec.tctx, srcTable.Schema); err != nil {
				return err
			}
		}
	case *ast.RecoverTableStmt:
		shouldExecDDLOnSchemaTracker = true
		shouldSchemaExist = true
	case *ast.CreateTableStmt, *ast.CreateViewStmt:
		shouldExecDDLOnSchemaTracker = true
		shouldSchemaExist = true
		// for CREATE TABLE LIKE/AS, the reference tables should exist
		shouldRefTableExistNum = len(srcTables)
	case *ast.DropTableStmt:
		shouldExecDDLOnSchemaTracker = true
		shouldReTrackDownstreamIndex = true
		if err := s.checkpoint.DeleteTablePoint(ec.tctx, srcTable); err != nil {
			return err
		}
	case *ast.RenameTableStmt, *ast.CreateIndexStmt, *ast.DropIndexStmt, *ast.RepairTableStmt:
		shouldExecDDLOnSchemaTracker = true
		shouldSchemaExist = true
		shouldTableExistNum = 1
		shouldReTrackDownstreamIndex = true
	case *ast.AlterTableStmt:
		shouldSchemaExist = true
		shouldReTrackDownstreamIndex = true
		// for DDL that adds FK, since TiDB doesn't fully support it yet, we simply ignore execution of this DDL.
		switch {
		case len(node.Specs) == 1 && node.Specs[0].Constraint != nil && node.Specs[0].Constraint.Tp == ast.ConstraintForeignKey:
			shouldTableExistNum = 1
			shouldExecDDLOnSchemaTracker = false
		case node.Specs[0].Tp == ast.AlterTableRenameTable:
			shouldTableExistNum = 1
			shouldExecDDLOnSchemaTracker = true
		default:
			shouldTableExistNum = len(srcTables)
			shouldExecDDLOnSchemaTracker = true
		}
	case *ast.LockTablesStmt, *ast.UnlockTablesStmt, *ast.CleanupTableLockStmt, *ast.TruncateTableStmt:
		break
	default:
		ec.tctx.L().DPanic("unhandled DDL type cannot be tracked", zap.Stringer("type", reflect.TypeOf(trackInfo.originStmt)))
	}

	if shouldReTrackDownstreamIndex {
		s.schemaTracker.RemoveDownstreamSchema(s.tctx, targetTables)
	}

	if shouldSchemaExist {
		if err := s.schemaTracker.CreateSchemaIfNotExists(srcTable.Schema); err != nil {
			return terror.ErrSchemaTrackerCannotCreateSchema.Delegate(err, srcTable.Schema)
		}
	}
	for i := 0; i < shouldTableExistNum; i++ {
		if _, err := s.getTableInfo(ec.tctx, srcTables[i], targetTables[i]); err != nil {
			return err
		}
	}
	// skip getTable before in above loop
	// nolint:ifshort
	start := 1
	if shouldTableExistNum > start {
		start = shouldTableExistNum
	}
	for i := start; i < shouldRefTableExistNum; i++ {
		if err := s.schemaTracker.CreateSchemaIfNotExists(srcTables[i].Schema); err != nil {
			return terror.ErrSchemaTrackerCannotCreateSchema.Delegate(err, srcTables[i].Schema)
		}
		if _, err := s.getTableInfo(ec.tctx, srcTables[i], targetTables[i]); err != nil {
			return err
		}
	}

	if shouldExecDDLOnSchemaTracker {
		if err := s.schemaTracker.Exec(ec.tctx.Ctx, usedSchema, trackInfo.originDDL); err != nil {
			if ignoreTrackerDDLError(err) {
				ec.tctx.L().Warn("will ignore a DDL error when tracking",
					zap.String("schema", usedSchema),
					zap.String("statement", trackInfo.originDDL),
					log.WrapStringerField("location", ec.currentLocation),
					log.ShortError(err))
				return nil
			}
			ec.tctx.L().Error("cannot track DDL",
				zap.String("schema", usedSchema),
				zap.String("statement", trackInfo.originDDL),
				log.WrapStringerField("location", ec.currentLocation),
				log.ShortError(err))
			return terror.ErrSchemaTrackerCannotExecDDL.Delegate(err, trackInfo.originDDL)
		}
		s.exprFilterGroup.ResetExprs(srcTable)
	}

	return nil
}

func (s *Syncer) trackOriginDDL(ev *replication.QueryEvent, ec eventContext) (map[string]map[string]struct{}, error) {
	originSQL := strings.TrimSpace(string(ev.Query))
	if originSQL == "BEGIN" || originSQL == "" || utils.IsBuildInSkipDDL(originSQL) {
		return nil, nil
	}
	var err error
	qec := &queryEventContext{
		eventContext:    &ec,
		ddlSchema:       string(ev.Schema),
		originSQL:       utils.TrimCtrlChars(originSQL),
		splitDDLs:       make([]string, 0),
		appliedDDLs:     make([]string, 0),
		sourceTbls:      make(map[string]map[string]struct{}),
		eventStatusVars: ev.StatusVars,
	}
	qec.p, err = event.GetParserForStatusVars(ev.StatusVars)
	if err != nil {
		s.tctx.L().Warn("found error when get sql_mode from binlog status_vars", zap.Error(err))
	}
	stmt, err := parseOneStmt(qec)
	if err != nil {
		// originSQL can't be parsed => can't be tracked by schema tracker
		// we can use operate-schema to set a compatible schema after this
		return nil, err
	}

	if _, ok := stmt.(ast.DDLNode); !ok {
		return nil, nil
	}

	// TiDB can't handle multi schema change DDL, so we split it here.
	qec.splitDDLs, err = parserpkg.SplitDDL(stmt, qec.ddlSchema)
	if err != nil {
		return nil, err
	}

	affectedTbls := make(map[string]map[string]struct{})
	for _, sql := range qec.splitDDLs {
		ddlInfo, err := s.genDDLInfo(qec, sql)
		if err != nil {
			return nil, err
		}
		sourceTable := ddlInfo.sourceTables[0]
		switch ddlInfo.originStmt.(type) {
		case *ast.DropDatabaseStmt:
			delete(affectedTbls, sourceTable.Schema)
		case *ast.DropTableStmt:
			if affectedTable, ok := affectedTbls[sourceTable.Schema]; ok {
				delete(affectedTable, sourceTable.Name)
			}
		default:
			if _, ok := affectedTbls[sourceTable.Schema]; !ok {
				affectedTbls[sourceTable.Schema] = make(map[string]struct{})
			}
			affectedTbls[sourceTable.Schema][sourceTable.Name] = struct{}{}
		}
		err = s.trackDDL(qec.ddlSchema, ddlInfo, qec.eventContext)
		if err != nil {
			return nil, err
		}
	}

	return affectedTbls, nil
}

func (s *Syncer) genRouter() error {
	s.tableRouter, _ = regexprrouter.NewRegExprRouter(s.cfg.CaseSensitive, []*router.TableRule{})
	for _, rule := range s.cfg.RouteRules {
		err := s.tableRouter.AddRule(rule)
		if err != nil {
			return terror.ErrSyncerUnitGenTableRouter.Delegate(err)
		}
	}
	return nil
}

func (s *Syncer) loadTableStructureFromDump(ctx context.Context) error {
	logger := s.tctx.L()
	files, err := storage.CollectDirFiles(ctx, s.cfg.LoaderConfig.Dir, nil)
	if err != nil {
		logger.Warn("fail to get dump files", zap.Error(err))
		return err
	}
	var dbs, tables []string
	var tableFiles [][2]string // [db, filename]
	for f := range files {
		if db, ok := utils.GetDBFromDumpFilename(f); ok {
			dbs = append(dbs, db)
			continue
		}
		if db, table, ok := utils.GetTableFromDumpFilename(f); ok {
			cols, _ := s.tableRouter.FetchExtendColumn(db, table, s.cfg.SourceID)
			if len(cols) > 0 {
				continue
			}
			tables = append(tables, dbutil.TableName(db, table))
			tableFiles = append(tableFiles, [2]string{db, f})
			continue
		}
	}
	logger.Info("fetch table structure from dump files",
		zap.Strings("database", dbs),
		zap.Any("tables", tables))
	for _, db := range dbs {
		if err = s.schemaTracker.CreateSchemaIfNotExists(db); err != nil {
			return err
		}
	}

	var firstErr error
	setFirstErr := func(err error) {
		if firstErr == nil {
			firstErr = err
		}
	}

	for _, dbAndFile := range tableFiles {
		db, file := dbAndFile[0], dbAndFile[1]
		content, err2 := storage.ReadFile(ctx, s.cfg.LoaderConfig.Dir, file, nil)
		if err2 != nil {
			logger.Warn("fail to read file for creating table in schema tracker",
				zap.String("db", db),
				zap.String("path", s.cfg.LoaderConfig.Dir),
				zap.String("file", file),
				zap.Error(err))
			setFirstErr(err2)
			continue
		}
		stmts := bytes.Split(content, []byte(";"))
		for _, stmt := range stmts {
			stmt = bytes.TrimSpace(stmt)
			if len(stmt) == 0 || bytes.HasPrefix(stmt, []byte("/*")) {
				continue
			}
			err = s.schemaTracker.Exec(ctx, db, string(stmt))
			if err != nil {
				logger.Warn("fail to create table for dump files",
					zap.Any("path", s.cfg.LoaderConfig.Dir),
					zap.Any("file", file),
					zap.ByteString("statement", stmt),
					zap.Error(err))
				setFirstErr(err)
			}
			// TODO: we should save table checkpoint here, but considering when
			// the first time of flushing checkpoint, user may encounter https://github.com/pingcap/tiflow/issues/5010
			// we should fix that problem first.
		}
	}
	return firstErr
}

func (s *Syncer) createDBs(ctx context.Context) error {
	var err error
	dbCfg := s.cfg.From
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout)
	fromDB, fromConns, err := dbconn.CreateConns(s.tctx, s.cfg, &dbCfg, 1)
	if err != nil {
		return err
	}
	s.fromDB = &dbconn.UpStreamConn{BaseDB: fromDB}
	s.fromConn = fromConns[0]
	conn, err := s.fromDB.BaseDB.GetBaseConn(ctx)
	if err != nil {
		return err
	}
	lcFlavor, err := utils.FetchLowerCaseTableNamesSetting(ctx, conn.DBConn)
	if err != nil {
		return err
	}
	s.SourceTableNamesFlavor = lcFlavor

	hasSQLMode := false
	// get sql_mode from upstream db
	if s.cfg.To.Session == nil {
		s.cfg.To.Session = make(map[string]string)
	} else {
		for k := range s.cfg.To.Session {
			if strings.ToLower(k) == "sql_mode" {
				hasSQLMode = true
				break
			}
		}
	}
	if !hasSQLMode {
		sqlMode, err2 := utils.GetGlobalVariable(ctx, s.fromDB.BaseDB.DB, "sql_mode")
		if err2 != nil {
			s.tctx.L().Warn("cannot get sql_mode from upstream database, the sql_mode will be assigned \"IGNORE_SPACE, NO_AUTO_VALUE_ON_ZERO, ALLOW_INVALID_DATES\"", log.ShortError(err2))
		}
		sqlModes, err3 := utils.AdjustSQLModeCompatible(sqlMode)
		if err3 != nil {
			s.tctx.L().Warn("cannot adjust sql_mode compatible, the sql_mode will be assigned  stay the same", log.ShortError(err3))
		}
		s.cfg.To.Session["sql_mode"] = sqlModes
	}

	dbCfg = s.cfg.To
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().
		SetReadTimeout(maxDMLConnectionTimeout).
		SetMaxIdleConns(s.cfg.WorkerCount)

	s.toDB, s.toDBConns, err = dbconn.CreateConns(s.tctx, s.cfg, &dbCfg, s.cfg.WorkerCount)
	if err != nil {
		dbconn.CloseUpstreamConn(s.tctx, s.fromDB) // release resources acquired before return with error
		return err
	}
	// baseConn for ddl
	dbCfg = s.cfg.To
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDDLConnectionTimeout)

	var ddlDBConns []*dbconn.DBConn
	s.ddlDB, ddlDBConns, err = dbconn.CreateConns(s.tctx, s.cfg, &dbCfg, 2)
	if err != nil {
		dbconn.CloseUpstreamConn(s.tctx, s.fromDB)
		dbconn.CloseBaseDB(s.tctx, s.toDB)
		return err
	}
	s.ddlDBConn = ddlDBConns[0]
	s.downstreamTrackConn = ddlDBConns[1]
	printServerVersion(s.tctx, s.fromDB.BaseDB, "upstream")
	printServerVersion(s.tctx, s.toDB, "downstream")

	return nil
}

// closeBaseDB closes all opened DBs, rollback for createConns.
func (s *Syncer) closeDBs() {
	dbconn.CloseUpstreamConn(s.tctx, s.fromDB)
	dbconn.CloseBaseDB(s.tctx, s.toDB)
	dbconn.CloseBaseDB(s.tctx, s.ddlDB)
}

// record skip ddl/dml sqls' position
// make newJob's sql argument empty to distinguish normal sql and skips sql.
func (s *Syncer) recordSkipSQLsLocation(ec *eventContext) error {
	job := newSkipJob(ec)
	_, err := s.handleJobFunc(job)
	return err
}

// flushJobs add a flush job and wait for all jobs finished.
// NOTE: currently, flush job is always sync operation.
func (s *Syncer) flushJobs() error {
	flushJobSeq := s.getFlushSeq()
	s.tctx.L().Info("flush all jobs", zap.Stringer("global checkpoint", s.checkpoint), zap.Int64("flush job seq", flushJobSeq))
	job := newFlushJob(s.cfg.WorkerCount, flushJobSeq)
	_, err := s.handleJobFunc(job)
	return err
}

func (s *Syncer) route(table *filter.Table) *filter.Table {
	if table.Schema == "" {
		return table
	}
	targetSchema, targetTable, err := s.tableRouter.Route(table.Schema, table.Name)
	if err != nil {
		s.tctx.L().Error("fail to route table", zap.Stringer("table", table), zap.Error(err)) // log the error, but still continue
	}
	if targetSchema == "" {
		return table
	}
	if targetTable == "" {
		targetTable = table.Name
	}

	return &filter.Table{Schema: targetSchema, Name: targetTable}
}

func (s *Syncer) IsRunning() bool {
	return s.running.Load()
}

func (s *Syncer) isClosed() bool {
	return s.closed.Load()
}

// Close closes syncer.
func (s *Syncer) Close() {
	s.Lock()
	defer s.Unlock()

	if s.isClosed() {
		return
	}
	s.stopSync()
	s.closeDBs()
	s.checkpoint.Close()
	if err := s.schemaTracker.Close(); err != nil {
		s.tctx.L().Error("fail to close schema tracker", log.ShortError(err))
	}
	if s.sgk != nil {
		s.sgk.Close()
	}
	s.closeOnlineDDL()
	// when closing syncer by `stop-task`, remove active relay log from hub
	s.removeActiveRelayLog()
	s.metricsProxies.RemoveLabelValuesWithTaskInMetrics(s.cfg.Name)

	s.runWg.Wait()
	s.closed.Store(true)
}

// Kill kill syncer without graceful.
func (s *Syncer) Kill() {
	s.tctx.L().Warn("kill syncer without graceful")
	s.runCancel()
	s.syncCancel()
	s.Close()
}

// stopSync stops stream and rollbacks checkpoint. Now it's used by Close() and Pause().
func (s *Syncer) stopSync() {
	// before re-write workflow for s.syncer, simply close it
	// when resuming, re-create s.syncer

	if s.streamerController != nil {
		s.streamerController.Close()
	}

	// try to rollback checkpoints, if they already flushed, no effect, this operation should call before close schemaTracker
	prePos := s.checkpoint.GlobalPoint()
	s.checkpoint.Rollback()
	currPos := s.checkpoint.GlobalPoint()
	if binlog.CompareLocation(prePos, currPos, s.cfg.EnableGTID) != 0 {
		s.tctx.L().Warn("rollback global checkpoint", zap.Stringer("previous position", prePos), zap.Stringer("current position", currPos))
	}
}

func (s *Syncer) closeOnlineDDL() {
	if s.onlineDDL != nil {
		s.onlineDDL.Close()
		s.onlineDDL = nil
	}
}

// Pause implements Unit.Pause.
func (s *Syncer) Pause() {
	if s.isClosed() {
		s.tctx.L().Warn("try to pause, but already closed")
		return
	}
	s.stopSync()
	if err := s.schemaTracker.Close(); err != nil {
		s.tctx.L().Error("fail to close schema tracker", log.ShortError(err))
	}
}

// Resume resumes the paused process.
func (s *Syncer) Resume(ctx context.Context, pr chan pb.ProcessResult) {
	if s.isClosed() {
		s.tctx.L().Warn("try to resume, but already closed")
		return
	}

	// continue the processing
	s.reset()
	// reset database conns
	err := s.resetDBs(s.tctx.WithContext(ctx))
	if err != nil {
		pr <- pb.ProcessResult{
			IsCanceled: false,
			Errors: []*pb.ProcessError{
				unit.NewProcessError(err),
			},
		}
		return
	}
	s.Process(ctx, pr)
}

// CheckCanUpdateCfg check if task config can be updated.
// 1. task must not in a pessimistic ddl state.
// 2. only balist, route/filter rules and syncerConfig can be updated at this moment.
// 3. some config fields from sourceCfg also can be updated, see more in func `copyConfigFromSource`.
func (s *Syncer) CheckCanUpdateCfg(newCfg *config.SubTaskConfig) error {
	s.RLock()
	defer s.RUnlock()
	// can't update when in sharding merge
	if s.cfg.ShardMode == config.ShardPessimistic {
		_, tables := s.sgk.UnresolvedTables()
		if len(tables) > 0 {
			return terror.ErrSyncerUnitUpdateConfigInSharding.Generate(tables)
		}
	}

	oldCfg, err := s.cfg.Clone()
	if err != nil {
		return err
	}
	oldCfg.BAList = newCfg.BAList
	oldCfg.BWList = newCfg.BWList
	oldCfg.RouteRules = newCfg.RouteRules
	oldCfg.FilterRules = newCfg.FilterRules
	oldCfg.SyncerConfig = newCfg.SyncerConfig
	oldCfg.To.Session = newCfg.To.Session // session is adjusted in `createDBs`

	// support fields that changed in func `copyConfigFromSource`
	oldCfg.From = newCfg.From
	oldCfg.Flavor = newCfg.Flavor
	oldCfg.ServerID = newCfg.ServerID
	oldCfg.RelayDir = newCfg.RelayDir
	oldCfg.UseRelay = newCfg.UseRelay
	oldCfg.EnableGTID = newCfg.EnableGTID
	oldCfg.CaseSensitive = newCfg.CaseSensitive

	if oldCfg.String() != newCfg.String() {
		s.tctx.L().Warn("can not update cfg", zap.Stringer("old cfg", oldCfg), zap.Stringer("new cfg", newCfg))
		return terror.ErrWorkerUpdateSubTaskConfig.Generatef("can't update subtask config for syncer because new config contains some fields that should not be changed, task: %s", s.cfg.Name)
	}
	return nil
}

// Update implements Unit.Update
// now, only support to update config for routes, filters, column-mappings, block-allow-list
// now no config diff implemented, so simply re-init use new config.
func (s *Syncer) Update(ctx context.Context, cfg *config.SubTaskConfig) error {
	s.Lock()
	defer s.Unlock()
	if s.cfg.ShardMode == config.ShardPessimistic {
		_, tables := s.sgk.UnresolvedTables()
		if len(tables) > 0 {
			return terror.ErrSyncerUnitUpdateConfigInSharding.Generate(tables)
		}
	}

	var (
		err              error
		oldBaList        *filter.Filter
		oldTableRouter   *regexprrouter.RouteTable
		oldBinlogFilter  *bf.BinlogEvent
		oldColumnMapping *cm.Mapping
	)

	defer func() {
		if err == nil {
			return
		}
		if oldBaList != nil {
			s.baList = oldBaList
		}
		if oldTableRouter != nil {
			s.tableRouter = oldTableRouter
		}
		if oldBinlogFilter != nil {
			s.binlogFilter = oldBinlogFilter
		}
		if oldColumnMapping != nil {
			s.columnMapping = oldColumnMapping
		}
	}()

	// update block-allow-list
	oldBaList = s.baList
	s.baList, err = filter.New(cfg.CaseSensitive, cfg.BAList)
	if err != nil {
		return terror.ErrSyncerUnitGenBAList.Delegate(err)
	}

	// update route
	oldTableRouter = s.tableRouter
	s.tableRouter, err = regexprrouter.NewRegExprRouter(cfg.CaseSensitive, cfg.RouteRules)
	if err != nil {
		return terror.ErrSyncerUnitGenTableRouter.Delegate(err)
	}

	// update binlog filter
	oldBinlogFilter = s.binlogFilter
	s.binlogFilter, err = bf.NewBinlogEvent(cfg.CaseSensitive, cfg.FilterRules)
	if err != nil {
		return terror.ErrSyncerUnitGenBinlogEventFilter.Delegate(err)
	}

	// update column-mappings
	oldColumnMapping = s.columnMapping
	s.columnMapping, err = cm.NewMapping(cfg.CaseSensitive, cfg.ColumnMappingRules)
	if err != nil {
		return terror.ErrSyncerUnitGenColumnMapping.Delegate(err)
	}

	switch s.cfg.ShardMode {
	case config.ShardPessimistic:
		// re-init sharding group
		err = s.sgk.Init()
		if err != nil {
			return err
		}

		err = s.initShardingGroups(context.Background(), false) // FIXME: fix context when re-implementing `Update`
		if err != nil {
			return err
		}
	case config.ShardOptimistic:
		err = s.initOptimisticShardDDL(context.Background()) // FIXME: fix context when re-implementing `Update`
		if err != nil {
			return err
		}
	}

	// update l.cfg
	s.cfg.BAList = cfg.BAList
	s.cfg.RouteRules = cfg.RouteRules
	s.cfg.FilterRules = cfg.FilterRules
	s.cfg.ColumnMappingRules = cfg.ColumnMappingRules

	// update timezone
	if s.timezone == nil {
		s.timezone, err = str2TimezoneOrFromDB(s.tctx.WithContext(ctx), s.cfg.Timezone, &s.cfg.To)
		return err
	}
	// update syncer config
	s.cfg.SyncerConfig = cfg.SyncerConfig

	// updated fileds that changed in func `copyConfigFromSource`
	s.cfg.From = cfg.From
	s.cfg.Flavor = cfg.Flavor
	s.cfg.ServerID = cfg.ServerID
	s.cfg.RelayDir = cfg.RelayDir
	s.cfg.UseRelay = cfg.UseRelay
	s.cfg.EnableGTID = cfg.EnableGTID
	s.cfg.CaseSensitive = cfg.CaseSensitive
	return nil
}

// checkpointID returns ID which used for checkpoint table.
func (s *Syncer) checkpointID() string {
	if len(s.cfg.SourceID) > 0 {
		return s.cfg.SourceID
	}
	return strconv.FormatUint(uint64(s.cfg.ServerID), 10)
}

// UpdateFromConfig updates config for `From`.
func (s *Syncer) UpdateFromConfig(cfg *config.SubTaskConfig) error {
	s.Lock()
	defer s.Unlock()
	s.fromDB.BaseDB.Close()

	s.cfg.From = cfg.From

	var err error
	s.cfg.From.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout)
	s.fromDB, err = dbconn.NewUpStreamConn(&s.cfg.From)
	if err != nil {
		s.tctx.L().Error("fail to create baseConn connection", log.ShortError(err))
		return err
	}

	s.syncCfg, err = subtaskCfg2BinlogSyncerCfg(s.cfg, s.timezone)
	if err != nil {
		return err
	}

	if s.streamerController != nil {
		s.streamerController.UpdateSyncCfg(s.syncCfg, s.fromDB)
	}
	return nil
}

// ShardDDLOperation returns the current pending to handle shard DDL lock operation.
func (s *Syncer) ShardDDLOperation() *pessimism.Operation {
	return s.pessimist.PendingOperation()
}

func (s *Syncer) setErrLocation(startLocation, endLocation *binlog.Location, isQueryEventEvent bool) {
	s.errLocation.Lock()
	defer s.errLocation.Unlock()

	s.errLocation.isQueryEvent = isQueryEventEvent
	if s.errLocation.startLocation == nil || startLocation == nil {
		s.errLocation.startLocation = startLocation
	} else if binlog.CompareLocation(*startLocation, *s.errLocation.startLocation, s.cfg.EnableGTID) < 0 {
		s.errLocation.startLocation = startLocation
	}

	if s.errLocation.endLocation == nil || endLocation == nil {
		s.errLocation.endLocation = endLocation
	} else if binlog.CompareLocation(*endLocation, *s.errLocation.endLocation, s.cfg.EnableGTID) < 0 {
		s.errLocation.endLocation = endLocation
	}
}

func (s *Syncer) getErrLocation() (*binlog.Location, bool) {
	s.errLocation.Lock()
	defer s.errLocation.Unlock()
	return s.errLocation.startLocation, s.errLocation.isQueryEvent
}

func (s *Syncer) handleEventError(err error, startLocation, endLocation binlog.Location, isQueryEvent bool, originSQL string) error {
	if err == nil {
		return nil
	}
	s.setErrLocation(&startLocation, &endLocation, isQueryEvent)
	if len(originSQL) > 0 {
		return terror.Annotatef(err, "startLocation: [%s], endLocation: [%s], origin SQL: [%s]", startLocation, endLocation, originSQL)
	}
	return terror.Annotatef(err, "startLocation: [%s], endLocation: [%s]", startLocation, endLocation)
}

// getEvent gets an event from streamerController or errOperatorHolder.
func (s *Syncer) getEvent(tctx *tcontext.Context, startLocation binlog.Location) (*replication.BinlogEvent, error) {
	// next event is a replace or inject event
	if s.isReplacingOrInjectingErr {
		s.tctx.L().Info("try to get replace or inject event", zap.Stringer("location", startLocation))
		return s.errOperatorHolder.GetEvent(startLocation)
	}

	e, err := s.streamerController.GetEvent(tctx)
	if err == nil {
		s.locations.update(e)
	}
	return e, err
}

func (s *Syncer) adjustGlobalPointGTID(tctx *tcontext.Context) (bool, error) {
	location := s.checkpoint.GlobalPoint()
	// situations that don't need to adjust
	// 1. GTID is not enabled
	// 2. location already has GTID position
	// 3. location is totally new, has no position info
	// 4. location is too early thus not a COMMIT location, which happens when it's reset by other logic
	if !s.cfg.EnableGTID || location.GTIDSetStr() != "" || location.Position.Name == "" || location.Position.Pos == 4 {
		return false, nil
	}
	// set enableGTID to false for new streamerController
	streamerController := binlogstream.NewStreamerController(s.syncCfg, false, s.fromDB, s.cfg.RelayDir, s.timezone, s.relay)

	endPos := binlog.AdjustPosition(location.Position)
	startPos := mysql.Position{
		Name: endPos.Name,
		Pos:  0,
	}
	startLocation := location.Clone()
	startLocation.Position = startPos

	err := streamerController.Start(tctx, startLocation)
	if err != nil {
		return false, err
	}
	defer streamerController.Close()

	gs, err := reader.GetGTIDsForPosFromStreamer(tctx.Context(), streamerController.GetStreamer(), endPos)
	if err != nil {
		s.tctx.L().Warn("fail to get gtids for global location", zap.Stringer("pos", location), zap.Error(err))
		return false, err
	}
	dbConn, err := s.fromDB.BaseDB.GetBaseConn(tctx.Context())
	if err != nil {
		s.tctx.L().Warn("fail to build connection", zap.Stringer("pos", location), zap.Error(err))
		return false, err
	}
	gs, err = utils.AddGSetWithPurged(tctx.Context(), gs, dbConn.DBConn)
	if err != nil {
		s.tctx.L().Warn("fail to merge purged gtidSet", zap.Stringer("pos", location), zap.Error(err))
		return false, err
	}
	err = location.SetGTID(gs)
	if err != nil {
		s.tctx.L().Warn("fail to set gtid for global location", zap.Stringer("pos", location),
			zap.String("adjusted_gtid", gs.String()), zap.Error(err))
		return false, err
	}
	s.saveGlobalPoint(location)
	// redirect streamer for new gtid set location
	err = s.streamerController.ResetReplicationSyncer(tctx, location)
	if err != nil {
		s.tctx.L().Warn("fail to redirect streamer for global location", zap.Stringer("pos", location),
			zap.String("adjusted_gtid", gs.String()), zap.Error(err))
		return false, err
	}
	return true, nil
}

// delLoadTask is called when finish restoring data, to delete load worker in etcd.
func (s *Syncer) delLoadTask() error {
	if s.cli == nil {
		return nil
	}
	_, _, err := ha.DelLoadTask(s.cli, s.cfg.Name, s.cfg.SourceID)
	if err != nil {
		return err
	}
	s.tctx.Logger.Info("delete load worker in etcd for all mode", zap.String("task", s.cfg.Name), zap.String("source", s.cfg.SourceID))
	return nil
}

// DM originally cached s.cfg.QueueSize * s.cfg.WorkerCount dml jobs in memory in 2.0.X.
// Now if compact: false, dmlJobCh and dmlWorker will both cached s.cfg.QueueSize * s.cfg.WorkerCount/2 jobs.
// If compact: true, dmlJobCh, compactor buffer, compactor output channel and dmlWorker will all cached s.cfg.QueueSize * s.cfg.WorkerCount/4 jobs.
func calculateChanSize(queueSize, workerCount int, compact bool) int {
	chanSize := queueSize * workerCount / 2
	if compact {
		chanSize /= 2
	}
	return chanSize
}

func (s *Syncer) flushOptimisticTableInfos(tctx *tcontext.Context) {
	tbls := s.optimist.Tables()
	sourceTables := make([]*filter.Table, 0, len(tbls))
	tableInfos := make([]*model.TableInfo, 0, len(tbls))
	for _, tbl := range tbls {
		sourceTable := tbl[0]
		targetTable := tbl[1]
		tableInfo, err := s.getTableInfo(tctx, &sourceTable, &targetTable)
		if err != nil {
			tctx.L().Error("failed to get table  infos", log.ShortError(err))
			continue
		}
		sourceTables = append(sourceTables, &sourceTable)
		tableInfos = append(tableInfos, tableInfo)
	}
	if err := s.checkpoint.FlushPointsWithTableInfos(tctx, sourceTables, tableInfos); err != nil {
		tctx.L().Error("failed to flush table points with table infos", log.ShortError(err))
	}
}

func (s *Syncer) setGlobalPointByTime(tctx *tcontext.Context, timeStr string) error {
	t, err := utils.ParseStartTimeInLoc(timeStr, s.upstreamTZ)
	if err != nil {
		return err
	}

	var (
		loc   *binlog.Location
		posTp binlog.PosType
	)

	if s.relay != nil {
		subDir := s.relay.Status(nil).(*pb.RelayStatus).RelaySubDir
		relayDir := path.Join(s.cfg.RelayDir, subDir)
		finder := binlog.NewLocalBinlogPosFinder(tctx, s.cfg.EnableGTID, s.cfg.Flavor, relayDir)
		loc, posTp, err = finder.FindByTimestamp(t.Unix())
	} else {
		finder := binlog.NewRemoteBinlogPosFinder(tctx, s.fromDB.BaseDB.DB, s.syncCfg, s.cfg.EnableGTID)
		loc, posTp, err = finder.FindByTimestamp(t.Unix())
	}
	if err != nil {
		s.tctx.L().Error("fail to find binlog position by timestamp",
			zap.Time("time", t),
			zap.Error(err))
		return err
	}

	switch posTp {
	case binlog.InRangeBinlogPos:
		s.tctx.L().Info("find binlog position by timestamp",
			zap.String("time", timeStr),
			zap.Stringer("pos", loc))
	case binlog.BelowLowerBoundBinlogPos:
		s.tctx.L().Warn("fail to find binlog location by timestamp because the timestamp is too early, will use the earliest binlog location",
			zap.String("time", timeStr),
			zap.Any("location", loc))
	case binlog.AboveUpperBoundBinlogPos:
		return terror.ErrConfigStartTimeTooLate.Generate(timeStr)
	}

	err = s.checkpoint.DeleteAllTablePoint(tctx)
	if err != nil {
		return err
	}
	s.checkpoint.SaveGlobalPointForcibly(*loc)
	s.tctx.L().Info("Will replicate from the specified time, the location recorded in checkpoint and config file will be ignored",
		zap.String("time", timeStr),
		zap.Any("locationOfTheTime", loc))
	return nil
}

func (s *Syncer) getFlushedGlobalPoint() binlog.Location {
	return s.checkpoint.FlushedGlobalPoint()
}

func (s *Syncer) getInitExecutedLoc() binlog.Location {
	s.RLock()
	defer s.RUnlock()
	return s.initExecutedLoc.Clone()
}

func (s *Syncer) initInitExecutedLoc() {
	s.Lock()
	defer s.Unlock()
	if s.initExecutedLoc == nil {
		p := s.checkpoint.GlobalPoint()
		s.initExecutedLoc = &p
	}
}

func (s *Syncer) getTrackedTableInfo(table *filter.Table) (*model.TableInfo, error) {
	return s.schemaTracker.GetTableInfo(table)
}

func (s *Syncer) getDownStreamTableInfo(tctx *tcontext.Context, tableID string, originTI *model.TableInfo) (*schema.DownstreamTableInfo, error) {
	return s.schemaTracker.GetDownStreamTableInfo(tctx, tableID, originTI)
}

func (s *Syncer) getTableInfoFromCheckpoint(table *filter.Table) *model.TableInfo {
	return s.checkpoint.GetTableInfo(table.Schema, table.Name)
}
