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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/filter"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/config/dbconfig"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/pingcap/tiflow/dm/syncer/binlogstream"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/pingcap/tiflow/dm/syncer/metrics"
	"github.com/pingcap/tiflow/dm/unit"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	validatorStatusInterval = time.Minute

	moreColumnInBinlogMsg            = "binlog has more columns than current table"
	tableWithoutPrimaryKeyMsg        = "no primary key"
	tableNotSyncedOrDropped          = "table is not synced or dropped"
	downstreamPKColumnOutOfBoundsMsg = "primary key column of downstream table out of range of binlog event row"
)

type validateTableInfo struct {
	targetTable         *filter.Table
	srcTableInfo        *model.TableInfo
	downstreamTableInfo *schema.DownstreamTableInfo

	message string
}

type rowChangeJobType int

func (r rowChangeJobType) String() string {
	switch r {
	case rowInsert:
		return "row-insert"
	case rowUpdated:
		return "row-update"
	case rowDeleted:
		return "row-delete"
	case flushCheckpoint:
		return "flush"
	default:
		return "unknown"
	}
}

const (
	rowInsert rowChangeJobType = iota
	rowUpdated
	rowDeleted
	flushCheckpoint

	rowChangeTypeCount  = 3
	errorStateTypeCount = 4 // pb.ValidateErrorState_*

	validatorDmctlOpTimeout = 5 * time.Second
)

// to make ut easier, we define it as a var, so we can change it.
var markErrorRowDelay = config.DefaultValidatorRowErrorDelay

// change of table
// binlog changes are clustered into table changes
// the validator validates changes of table-grain at a time.
type tableChangeJob struct {
	jobs map[string]*rowValidationJob
}

func newTableChangeJob() *tableChangeJob {
	return &tableChangeJob{jobs: make(map[string]*rowValidationJob)}
}

// return true if it's new added row job.
func (tc *tableChangeJob) addOrUpdate(job *rowValidationJob) bool {
	if val, ok := tc.jobs[job.Key]; ok {
		val.row = job.row
		val.size = job.size
		val.Tp = job.Tp
		val.FirstValidateTS = 0
		val.FailedCnt = 0 // clear failed count
		return false
	}
	tc.jobs[job.Key] = job
	return true
}

// change of a row.
type rowValidationJob struct {
	Key string
	Tp  rowChangeJobType
	row *sqlmodel.RowChange

	// estimated memory size taken by this row, we use binlog size of the row to estimated it now.
	// the memory taken for a row change job is more than this size
	size int32
	wg   *sync.WaitGroup
	// timestamp of first validation of this row. will reset when merge row changes.
	// if the job is loaded from meta, it's reset too, in case validator stopped for a long time,
	// then those failed row change maybe marked as error row immediately.
	FirstValidateTS int64
	FailedCnt       int
}

type tableValidateStatus struct {
	source  filter.Table
	target  filter.Table
	stage   pb.Stage // either Running or Stopped
	message string
}

func (vs *tableValidateStatus) String() string {
	return fmt.Sprintf("source=%s, target=%s, stage=%s, message=%s",
		vs.source, vs.target, vs.stage, vs.message)
}

func (vs *tableValidateStatus) stopped(msg string) {
	vs.stage = pb.Stage_Stopped
	vs.message = msg
}

// DataValidator is used to continuously validate incremental data migrated to downstream by dm.
// validator can be start when there's syncer unit in the subtask and validation mode is not none,
// it's terminated when the subtask is terminated.
// stage of validator is independent of subtask, pause/resume subtask doesn't affect the stage of validator.
//
// validator can be in running or stopped stage
// - in running when it's started with subtask or started later on the fly.
// - in stopped when validation stop is executed.
//
// for each subtask, before it's closed/killed, only one DataValidator object is created,
// on "dmctl validation stop/start", will call Stop and Start on the same object.
type DataValidator struct {
	// used to sync Stop and Start operations.
	sync.RWMutex

	cfg    *config.SubTaskConfig
	syncer *Syncer
	// whether validator starts together with subtask
	startWithSubtask bool

	wg           sync.WaitGroup
	errProcessWg sync.WaitGroup
	errChan      chan error
	ctx          context.Context
	cancel       context.CancelFunc
	tctx         *tcontext.Context

	L                  log.Logger
	fromDB             *conn.BaseDB
	toDB               *conn.BaseDB
	upstreamTZ         *time.Location
	timezone           *time.Location
	syncCfg            replication.BinlogSyncerConfig
	streamerController *binlogstream.StreamerController
	persistHelper      *validatorPersistHelper

	validateInterval time.Duration
	checkInterval    time.Duration
	workers          []*validateWorker
	workerCnt        int

	// whether we start to mark failed rows as error rows
	// if it's false, we don't mark failed row change as error to reduce false-positive
	// it's set to true when validator reached the progress of syncer once or after markErrorRowDelay
	markErrorStarted atomic.Bool

	// fields in this field block are guarded by stateMutex
	stateMutex  sync.RWMutex
	stage       pb.Stage // only Running or Stopped is allowed for validator
	flushedLoc  *binlog.Location
	result      pb.ProcessResult
	tableStatus map[string]*tableValidateStatus

	processedRowCounts   []atomic.Int64 // all processed row count since the beginning of validator
	pendingRowCounts     []atomic.Int64
	newErrorRowCount     atomic.Int64
	processedBinlogSize  atomic.Int64
	pendingRowSize       atomic.Int64 // accumulation of rowValidationJob.size
	lastFlushTime        time.Time
	location             *binlog.Location
	loadedPendingChanges map[string]*tableChangeJob

	vmetric *metrics.ValidatorMetrics
}

func NewContinuousDataValidator(cfg *config.SubTaskConfig, syncerObj *Syncer, startWithSubtask bool) *DataValidator {
	v := &DataValidator{
		cfg:              cfg,
		syncer:           syncerObj,
		startWithSubtask: startWithSubtask,
		vmetric:          metrics.NewValidatorMetrics(cfg.Name, cfg.SourceID),
	}
	v.L = log.With(zap.String("task", cfg.Name), zap.String("unit", "continuous validator"))

	v.setStage(pb.Stage_Stopped)
	v.workerCnt = cfg.ValidatorCfg.WorkerCount
	v.processedRowCounts = make([]atomic.Int64, rowChangeTypeCount)
	v.validateInterval = cfg.ValidatorCfg.ValidateInterval.Duration
	v.checkInterval = cfg.ValidatorCfg.CheckInterval.Duration
	v.persistHelper = newValidatorCheckpointHelper(v)
	v.pendingRowCounts = make([]atomic.Int64, rowChangeTypeCount)

	return v
}

// reset state on start/restart.
func (v *DataValidator) reset() {
	v.errChan = make(chan error, 10)
	v.workers = []*validateWorker{}

	v.markErrorStarted.Store(false)
	v.resetResult()
	for i := range v.processedRowCounts {
		v.processedRowCounts[i].Store(0)
	}
	for i := range v.pendingRowCounts {
		v.pendingRowCounts[i].Store(0)
	}
	v.newErrorRowCount.Store(0)
	v.processedBinlogSize.Store(0)
	v.pendingRowSize.Store(0)
	v.initTableStatus(map[string]*tableValidateStatus{})
}

func (v *DataValidator) initialize() error {
	v.ctx, v.cancel = context.WithCancel(context.Background())
	v.tctx = tcontext.NewContext(v.ctx, v.L)
	v.reset()

	newCtx, cancelFunc := v.tctx.WithTimeout(unit.DefaultInitTimeout)
	defer cancelFunc()

	var err error
	defer func() {
		if err == nil {
			return
		}
		dbconn.CloseBaseDB(newCtx, v.fromDB)
		dbconn.CloseBaseDB(newCtx, v.toDB)
		v.cancel()
	}()

	dbCfg := v.cfg.From
	dbCfg.RawDBCfg = dbconfig.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout).SetMaxIdleConns(1)
	v.fromDB, err = conn.GetUpstreamDB(&dbCfg)
	if err != nil {
		return err
	}

	dbCfg = v.cfg.To
	// worker count + checkpoint connection, others concurrent access can create it on the fly
	dbCfg.RawDBCfg = dbconfig.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout).SetMaxIdleConns(v.workerCnt + 1)
	v.toDB, err = conn.GetDownstreamDB(&dbCfg)
	if err != nil {
		return err
	}

	if err = v.persistHelper.init(newCtx); err != nil {
		return err
	}

	var defaultUpstreamTZ string
	failpoint.Inject("ValidatorMockUpstreamTZ", func() {
		defaultUpstreamTZ = "UTC"
	})
	v.upstreamTZ, _, err = str2TimezoneOrFromDB(newCtx, defaultUpstreamTZ, conn.UpstreamDBConfig(&v.cfg.From))
	if err != nil {
		return err
	}
	v.timezone, _, err = str2TimezoneOrFromDB(newCtx, v.cfg.Timezone, conn.DownstreamDBConfig(&v.cfg.To))
	if err != nil {
		return err
	}

	v.syncCfg, err = subtaskCfg2BinlogSyncerCfg(v.cfg, v.timezone, v.syncer.baList)
	if err != nil {
		return err
	}

	v.streamerController = binlogstream.NewStreamerController(
		v.syncCfg,
		v.cfg.EnableGTID,
		&dbconn.UpStreamConn{BaseDB: v.fromDB},
		v.cfg.RelayDir,
		v.timezone,
		nil,
		v.L,
	)
	return nil
}

func (v *DataValidator) routineWrapper(fn func()) {
	defer func() {
		if err := recover(); err != nil {
			v.L.Error("panic", zap.Any("err", err))
			v.sendError(terror.ErrValidatorPanic.Generate(err))
		}
	}()

	fn()
}

func (v *DataValidator) Start(expect pb.Stage) {
	v.Lock()
	defer v.Unlock()

	v.L.Info("starting", zap.Any("cfg", v.cfg.ValidatorCfg),
		zap.String("start-time", v.cfg.ValidatorCfg.StartTime),
		zap.Bool("start with subtask", v.startWithSubtask),
		zap.Any("expect", expect))
	if v.Stage() == pb.Stage_Running {
		v.L.Info("already started")
		return
	}

	if expect != pb.Stage_Running {
		v.L.Info("expect stage is not running", zap.Any("expect", expect))
		return
	}

	if err := v.initialize(); err != nil {
		v.fillResult(err)
		return
	}

	v.wg.Add(1)
	go v.routineWrapper(v.doValidate)

	v.wg.Add(1)
	go v.routineWrapper(v.printStatusRoutine)

	v.wg.Add(1)
	go utils.GoLogWrapper(v.L, v.markErrorStartedRoutine)

	// routineWrapper relies on errorProcessRoutine to handle panic errors,
	// so just wrap it using a common wrapper.
	v.errProcessWg.Add(1)
	go utils.GoLogWrapper(v.L, v.errorProcessRoutine)

	v.setStage(pb.Stage_Running)
	v.L.Info("started")
}

func (v *DataValidator) markErrorStartedRoutine() {
	defer v.wg.Done()

	select {
	case <-v.ctx.Done():
	case <-time.After(markErrorRowDelay):
		if !v.markErrorStarted.Load() {
			v.L.Info("mark markErrorStarted=true after error row delay")
			v.markErrorStarted.Store(true)
		}
	}
}

func (v *DataValidator) printStatusRoutine() {
	defer v.wg.Done()
	var (
		prevProcessedBinlogSize = v.processedBinlogSize.Load()
		prevTime                = time.Now()
	)
	for {
		select {
		case <-v.ctx.Done():
			return
		case <-time.After(validatorStatusInterval):
			processed := v.getProcessedRowCounts()
			pending := []int64{
				v.pendingRowCounts[rowInsert].Load(),
				v.pendingRowCounts[rowUpdated].Load(),
				v.pendingRowCounts[rowDeleted].Load(),
			}
			currProcessedBinlogSize := v.processedBinlogSize.Load()
			currTime := time.Now()
			interval := time.Since(prevTime)
			speed := float64((currProcessedBinlogSize-prevProcessedBinlogSize)>>20) / interval.Seconds()
			prevProcessedBinlogSize = currProcessedBinlogSize
			prevTime = currTime
			counts, err := v.getErrorRowCount(validatorDmctlOpTimeout)
			if err == nil {
				v.vmetric.ErrorCount.Set(float64(counts[pb.ValidateErrorState_NewErr]))
			} else {
				v.L.Warn("failed to get error row count", zap.Error(err))
			}
			v.L.Info("validator status",
				zap.Int64s("processed(i, u, d)", processed),
				zap.Int64s("pending(i, u, d)", pending),
				zap.Int64("new error rows(not flushed)", v.newErrorRowCount.Load()),
				zap.String("binlog process speed", fmt.Sprintf("%.2f MB/s", speed)),
			)
		}
	}
}

func (v *DataValidator) fillResult(err error) {
	// when met a non-retryable error, we'll call stopInner, then v.ctx is cancelled,
	// don't set IsCanceled in this case
	isCanceled := false
	if v.getResultErrCnt() == 0 {
		select {
		case <-v.ctx.Done():
			isCanceled = true
		default:
		}
	}

	var processErr *pb.ProcessError
	if utils.IsContextCanceledError(err) {
		v.L.Info("filter out context cancelled error", log.ShortError(err))
	} else {
		v.L.Error("error during validation", zap.Error(err))
		processErr = unit.NewProcessError(err)
	}
	v.addResultError(processErr, isCanceled)
}

func (v *DataValidator) errorProcessRoutine() {
	defer v.errProcessWg.Done()

	var (
		stopped bool
		wg      sync.WaitGroup
	)

	for err := range v.errChan {
		v.fillResult(err)

		if errors.Cause(err) != context.Canceled && !stopped {
			stopped = true
			wg.Add(1)
			go func() {
				defer wg.Done()
				v.stopInner()
			}()
		}
	}
	wg.Wait()
}

func (v *DataValidator) waitSyncerSynced(currLoc binlog.Location) error {
	syncLoc := v.syncer.getFlushedGlobalPoint()
	cmp := binlog.CompareLocation(currLoc, syncLoc, v.cfg.EnableGTID)
	if cmp >= 0 && !v.markErrorStarted.Load() {
		v.markErrorStarted.Store(true)
		v.L.Info("validator progress reached syncer")
	}
	if cmp <= 0 {
		return nil
	}

	for {
		select {
		case <-v.ctx.Done():
			return v.ctx.Err()
		case <-time.After(v.checkInterval):
			syncLoc = v.syncer.getFlushedGlobalPoint()
			cmp = binlog.CompareLocation(currLoc, syncLoc, v.cfg.EnableGTID)
			if cmp <= 0 {
				return nil
			}
			v.L.Debug("wait syncer synced", zap.Reflect("loc", currLoc))
		}
	}
}

func (v *DataValidator) updateValidatorBinlogMetric(currLoc binlog.Location) {
	v.vmetric.BinlogPos.Set(float64(currLoc.Position.Pos))
	index, err := utils.GetFilenameIndex(currLoc.Position.Name)
	if err != nil {
		v.L.Warn("fail to record validator binlog file index", zap.Error(err))
	} else {
		v.vmetric.BinlogFile.Set(float64(index))
	}
}

func (v *DataValidator) updateValidatorBinlogLag(currLoc binlog.Location) {
	syncerLoc := v.syncer.getFlushedGlobalPoint()
	index, err := utils.GetFilenameIndex(currLoc.Position.Name)
	if err != nil {
		v.L.Warn("fail to record validator binlog file index", zap.Error(err))
	}
	if syncerLoc.Position.Name == currLoc.Position.Name {
		// same file: record the log pos latency
		v.vmetric.LogPosLatency.Set(float64(syncerLoc.Position.Pos - currLoc.Position.Pos))
		v.vmetric.LogFileLatency.Set(float64(0))
	} else {
		var syncerLogIdx int64
		v.vmetric.LogPosLatency.Set(float64(0))
		syncerLogIdx, err = utils.GetFilenameIndex(syncerLoc.Position.Name)
		if err == nil {
			v.vmetric.LogFileLatency.Set(float64(syncerLogIdx - index))
		} else {
			v.vmetric.LogFileLatency.Set(float64(0))
			v.L.Warn("fail to get syncer's log file index", zap.Error(err))
		}
	}
}

func (v *DataValidator) waitSyncerRunning() error {
	if v.syncer.IsRunning() {
		return nil
	}
	v.L.Info("wait until syncer running")
	for {
		select {
		case <-v.ctx.Done():
			return v.ctx.Err()
		case <-time.After(v.checkInterval):
			if v.syncer.IsRunning() {
				v.L.Info("syncer is running, wait finished")
				return nil
			}
		}
	}
}

func (v *DataValidator) getInitialBinlogPosition() (binlog.Location, error) {
	var location binlog.Location
	timeStr := v.cfg.ValidatorCfg.StartTime
	switch {
	case timeStr != "":
		// already check it when set it, will not check it again
		t, _ := utils.ParseStartTimeInLoc(timeStr, v.upstreamTZ)
		finder := binlog.NewRemoteBinlogPosFinder(v.tctx, v.fromDB, v.syncCfg, v.cfg.EnableGTID)
		loc, posTp, err := finder.FindByTimestamp(t.Unix())
		if err != nil {
			v.L.Error("fail to find binlog position by timestamp",
				zap.Time("time", t), zap.Error(err))
			return location, err
		}
		v.L.Info("find binlog pos by timestamp", zap.String("time", timeStr),
			zap.Any("loc", loc), zap.Stringer("pos type", posTp))

		if posTp == binlog.AboveUpperBoundBinlogPos {
			return location, terror.ErrConfigStartTimeTooLate.Generate(timeStr)
		}
		location = *loc
		v.L.Info("do validate from timestamp", zap.Any("loc", location))
	case v.startWithSubtask:
		// in extreme case, this loc may still not be the first binlog location of this task:
		//   syncer synced some binlog and flush checkpoint, but validator still not has chance to run, then fail-over
		location = v.syncer.getInitExecutedLoc()
		v.L.Info("do validate from init executed loc of syncer", zap.Any("loc", location))
	default:
		location = v.syncer.getFlushedGlobalPoint()
		v.L.Info("do validate from current loc of syncer", zap.Any("loc", location))
	}
	return location, nil
}

// doValidate: runs in a separate goroutine.
func (v *DataValidator) doValidate() {
	defer v.wg.Done()

	if err := v.waitSyncerRunning(); err != nil {
		// no need to wrapped it in error_list, since err can be context.Canceled only.
		v.sendError(err)
		return
	}

	if err := v.loadPersistedData(); err != nil {
		v.sendError(terror.ErrValidatorLoadPersistedData.Delegate(err))
		return
	}

	var location binlog.Location
	if v.location != nil {
		location = *v.location
		v.L.Info("do validate from checkpoint", zap.Any("loc", location))
	} else {
		// validator always uses remote binlog streamer now.
		var err error
		location, err = v.getInitialBinlogPosition()
		if err != nil {
			v.sendError(err)
			return
		}
		// when relay log enabled, binlog name may contain uuid suffix, so need to extract the real location
		location.Position.Name = utils.ExtractRealName(location.Position.Name)
		// persist current location to make sure we start from the same location
		// if fail-over happens before we flush checkpoint and data.
		err = v.persistHelper.persist(v.tctx, location)
		if err != nil {
			v.sendError(terror.ErrValidatorPersistData.Delegate(err))
			return
		}
	}
	// it's for test, some fields in streamerController is mocked, cannot call Start
	if v.streamerController.IsClosed() {
		err := v.streamerController.Start(v.tctx, location)
		if err != nil {
			v.sendError(terror.Annotate(err, "fail to start streamer controller"))
			return
		}
	}

	v.startValidateWorkers()
	defer func() {
		for _, worker := range v.workers {
			worker.close()
		}
	}()

	// we don't flush checkpoint&data on exist, since checkpoint and pending data may not correspond with each other.
	locationForFlush := location.CloneWithFlavor(v.cfg.Flavor)
	v.lastFlushTime = time.Now()
	for {
		e, _, err := v.streamerController.GetEvent(v.tctx)
		if err != nil {
			switch {
			case err == context.Canceled:
				return
			case err == context.DeadlineExceeded:
				v.L.Info("deadline exceeded when fetching binlog event")
				continue
			case isDuplicateServerIDError(err):
				// if the server id is already used, need to use a new server id
				v.L.Info("server id is already used by another slave, will change to a new server id and get event again")
				err1 := v.streamerController.UpdateServerIDAndResetReplication(v.tctx, locationForFlush)
				if err1 != nil {
					v.sendError(terror.Annotate(err1, "fail to update UpdateServerIDAndResetReplication"))
					return
				}
				continue
			case err == relay.ErrorMaybeDuplicateEvent:
				continue
			case isConnectionRefusedError(err):
				v.sendError(terror.ErrValidatorGetEvent.Delegate(err))
				return
			default:
				if v.streamerController.CanRetry(err) {
					err = v.streamerController.ResetReplicationSyncer(v.tctx, locationForFlush)
					if err != nil {
						v.sendError(terror.Annotate(err, "fail to reset replication"))
						return
					}
					continue
				}
				v.sendError(terror.ErrValidatorGetEvent.Delegate(err))
				return
			}
		}

		currEndLoc := v.streamerController.GetCurEndLocation()
		locationForFlush = v.streamerController.GetTxnEndLocation()

		// wait until syncer synced current event
		err = v.waitSyncerSynced(currEndLoc)
		if err != nil {
			// no need to wrap it in error_list, since err can be context.Canceled only.
			v.sendError(err)
			return
		}
		failpoint.Inject("mockValidatorDelay", func(val failpoint.Value) {
			if sec, ok := val.(int); ok {
				v.L.Info("mock validator delay", zap.Int("second", sec))
				time.Sleep(time.Duration(sec) * time.Second)
			}
		})
		// update validator metric
		v.updateValidatorBinlogMetric(currEndLoc)
		v.updateValidatorBinlogLag(currEndLoc)
		v.processedBinlogSize.Add(int64(e.Header.EventSize))

		switch ev := e.Event.(type) {
		case *replication.RowsEvent:
			if err = v.processRowsEvent(e.Header, ev); err != nil {
				v.L.Warn("failed to process event: ", zap.Reflect("error", err))
				v.sendError(terror.ErrValidatorProcessRowEvent.Delegate(err))
				return
			}
		case *replication.XIDEvent:
			if err = v.checkAndPersistCheckpointAndData(locationForFlush); err != nil {
				v.sendError(terror.ErrValidatorPersistData.Delegate(err))
				return
			}
		case *replication.QueryEvent:
			if err = v.checkAndPersistCheckpointAndData(locationForFlush); err != nil {
				v.sendError(terror.ErrValidatorPersistData.Delegate(err))
				return
			}
		case *replication.GenericEvent:
			if e.Header.EventType == replication.HEARTBEAT_EVENT {
				if err = v.checkAndPersistCheckpointAndData(locationForFlush); err != nil {
					v.sendError(terror.ErrValidatorPersistData.Delegate(err))
					return
				}
			}
		}
	}
}

func (v *DataValidator) Stop() {
	v.stopInner()
	v.errProcessWg.Wait()
	metrics.RemoveValidatorLabelValuesWithTask(v.cfg.Name)
}

func (v *DataValidator) stopInner() {
	v.Lock()
	defer v.Unlock()
	v.L.Info("stopping")
	if v.Stage() != pb.Stage_Running {
		v.L.Warn("not started")
		return
	}

	v.cancel()
	v.streamerController.Close()
	v.fromDB.Close()
	v.toDB.Close()

	v.wg.Wait()
	// we want to record all errors, so we need to wait all error sender goroutines to stop
	// before closing this error chan.
	close(v.errChan)

	v.setStage(pb.Stage_Stopped)
	v.L.Info("stopped")
}

func (v *DataValidator) startValidateWorkers() {
	v.wg.Add(v.workerCnt)
	v.workers = make([]*validateWorker, v.workerCnt)
	for i := 0; i < v.workerCnt; i++ {
		worker := newValidateWorker(v, i)
		v.workers[i] = worker
		// worker handles panic in validateTableChange, so we can see it in `dmctl validation status`,
		// for other panics we just log it.
		go utils.GoLogWrapper(v.L, func() {
			defer v.wg.Done()
			worker.run()
		})
	}

	for _, tblChange := range v.loadedPendingChanges {
		for key, row := range tblChange.jobs {
			v.dispatchRowChange(key, row)
		}
	}
}

func (v *DataValidator) dispatchRowChange(key string, row *rowValidationJob) {
	hashVal := int(utils.GenHashKey(key)) % v.workerCnt
	v.workers[hashVal].rowChangeCh <- row

	v.L.Debug("dispatch row change job", zap.Any("table", row.row.GetSourceTable()),
		zap.Stringer("type", row.Tp), zap.String("key", key), zap.Int("worker id", hashVal))
}

func (v *DataValidator) genValidateTableInfo(sourceTable *filter.Table, columnCount int) (*validateTableInfo, error) {
	targetTable := v.syncer.route(sourceTable)
	// there are 2 cases tracker may drop table:
	// 1. checkpoint rollback, tracker may recreate tables and drop non-needed tables
	// 2. when operate-schema set/remove
	// in case 1, we add another layer synchronization to make sure we don't get a dropped table when recreation.
	// 	for non-needed tables, we will not validate them.
	// in case 2, validator should be paused
	res := &validateTableInfo{targetTable: targetTable}
	var (
		tableInfo *model.TableInfo
		err       error
	)
	tableInfo, err = v.syncer.getTrackedTableInfo(sourceTable)
	if err != nil {
		switch {
		case schema.IsTableNotExists(err):
			// not a table need to sync
			res.message = tableNotSyncedOrDropped
			return res, nil
		case terror.ErrSchemaTrackerIsClosed.Equal(err):
			// schema tracker is closed
			// try to get table schema from checkpoint
			tableInfo = v.syncer.getTableInfoFromCheckpoint(sourceTable)
			if tableInfo == nil {
				// get table schema from checkpoint failed
				return res, errors.Annotate(err, "fail to get table info from checkpoint")
			}
		default:
			return res, err
		}
	}
	if len(tableInfo.Columns) < columnCount {
		res.message = moreColumnInBinlogMsg
		return res, nil
	}

	tableID := utils.GenTableID(targetTable)
	downstreamTableInfo, err := v.syncer.getDownStreamTableInfo(v.tctx, tableID, tableInfo)
	if err != nil {
		// todo: might be connection error, then return error, or downstream table not exists, then set state to stopped.
		return res, err
	}
	pk := downstreamTableInfo.WhereHandle.UniqueNotNullIdx
	if pk == nil {
		res.message = tableWithoutPrimaryKeyMsg
		return res, nil
	}
	// offset of pk column is adjusted using source table info, the offsets should stay in range of ev.ColumnCount.
	for _, col := range pk.Columns {
		if col.Offset >= columnCount {
			res.message = downstreamPKColumnOutOfBoundsMsg
			return res, nil
		}
	}
	// if current TI has more columns, clone and strip columns
	if len(tableInfo.Columns) > columnCount {
		tableInfo = tableInfo.Clone()
		tableInfo.Columns = tableInfo.Columns[:columnCount]
	}

	res.srcTableInfo = tableInfo
	res.downstreamTableInfo = downstreamTableInfo
	return res, nil
}

func (v *DataValidator) processRowsEvent(header *replication.EventHeader, ev *replication.RowsEvent) error {
	sourceTable := &filter.Table{
		Schema: string(ev.Table.Schema),
		Name:   string(ev.Table.Table),
	}

	failpoint.Inject("ValidatorPanic", func() {})

	if err := checkLogColumns(ev.SkippedColumns); err != nil {
		return terror.Annotate(err, sourceTable.String())
	}

	needSkip, err := v.syncer.skipRowsEvent(sourceTable, header.EventType)
	if err != nil {
		return err
	}
	if needSkip {
		return nil
	}

	fullTableName := sourceTable.String()
	state, ok := v.getTableStatus(fullTableName)
	if ok && state.stage == pb.Stage_Stopped {
		return nil
	}

	validateTbl, err := v.genValidateTableInfo(sourceTable, int(ev.ColumnCount))
	if err != nil {
		return terror.Annotate(err, "failed to get table info")
	}

	targetTable := validateTbl.targetTable
	if state == nil {
		state = &tableValidateStatus{
			source: *sourceTable,
			target: *targetTable,
			stage:  pb.Stage_Running,
		}

		v.L.Info("put table status", zap.Stringer("state", state))
		v.putTableStatus(fullTableName, state)
	}
	if validateTbl.message != "" {
		v.L.Warn("stop validating table", zap.String("table", sourceTable.String()),
			zap.String("reason", validateTbl.message))
		state.stopped(validateTbl.message)
		return nil
	}

	tableInfo, downstreamTableInfo := validateTbl.srcTableInfo, validateTbl.downstreamTableInfo

	changeType := getRowChangeType(header.EventType)

	step := 1
	if changeType == rowUpdated {
		step = 2
	}
	estimatedRowSize := int32(header.EventSize) / int32(len(ev.Rows))
	for i := 0; i < len(ev.Rows); i += step {
		var beforeImage, afterImage []interface{}
		switch changeType {
		case rowInsert:
			afterImage = ev.Rows[i]
		case rowUpdated:
			beforeImage, afterImage = ev.Rows[i], ev.Rows[i+1]
		default: // rowDeleted
			beforeImage = ev.Rows[i]
		}

		rowChange := sqlmodel.NewRowChange(
			&cdcmodel.TableName{Schema: sourceTable.Schema, Table: sourceTable.Name},
			&cdcmodel.TableName{Schema: targetTable.Schema, Table: targetTable.Name},
			beforeImage, afterImage,
			tableInfo, downstreamTableInfo.TableInfo,
			nil,
		)
		rowChange.SetWhereHandle(downstreamTableInfo.WhereHandle)
		size := estimatedRowSize
		if changeType == rowUpdated && rowChange.IsIdentityUpdated() {
			delRow, insRow := rowChange.SplitUpdate()
			delRowKey := genRowKey(delRow)
			v.dispatchRowChange(delRowKey, &rowValidationJob{Key: delRowKey, Tp: rowDeleted, row: delRow, size: size})
			v.processedRowCounts[rowDeleted].Inc()

			insRowKey := genRowKey(insRow)
			v.dispatchRowChange(insRowKey, &rowValidationJob{Key: insRowKey, Tp: rowInsert, row: insRow, size: size})
			v.processedRowCounts[rowInsert].Inc()
		} else {
			rowKey := genRowKey(rowChange)
			if changeType == rowUpdated {
				size *= 2
			}
			v.dispatchRowChange(rowKey, &rowValidationJob{Key: rowKey, Tp: changeType, row: rowChange, size: size})
			v.processedRowCounts[changeType].Inc()
		}
	}
	return nil
}

func (v *DataValidator) checkAndPersistCheckpointAndData(loc binlog.Location) error {
	metaFlushInterval := v.cfg.ValidatorCfg.MetaFlushInterval.Duration
	if time.Since(v.lastFlushTime) > metaFlushInterval {
		v.lastFlushTime = time.Now()
		if err := v.persistCheckpointAndData(loc); err != nil {
			v.L.Warn("failed to flush checkpoint: ", zap.Error(err))
			if isRetryableValidateError(err) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (v *DataValidator) persistCheckpointAndData(loc binlog.Location) error {
	var wg sync.WaitGroup
	wg.Add(v.workerCnt)
	flushJob := &rowValidationJob{
		Tp: flushCheckpoint,
		wg: &wg,
	}
	for i, worker := range v.workers {
		v.L.Debug("dispatch flush job", zap.Int("worker id", i))
		worker.rowChangeCh <- flushJob
	}
	wg.Wait()

	v.L.Info("persist checkpoint and intermediate data",
		zap.Int64("pending size", v.getPendingRowSize()),
		zap.Int64("pending count", v.getAllPendingRowCount()),
		zap.Int64("new error", v.newErrorRowCount.Load()))

	err := v.persistHelper.persist(v.tctx, loc)
	if err != nil {
		return err
	}

	// reset errors after save
	for _, worker := range v.workers {
		worker.resetErrorRows()
	}
	v.newErrorRowCount.Store(0)
	v.setFlushedLoc(&loc)
	return nil
}

func (v *DataValidator) loadPersistedData() error {
	data, err := v.persistHelper.loadPersistedDataRetry(v.tctx)
	if err != nil {
		return err
	}
	// table info of pending change is not persisted in order to save space, so need to init them after load.
	pendingChanges := make(map[string]*tableChangeJob)
	for _, tblChange := range data.pendingChanges {
		// todo: if table is dropped since last run, we should skip rows related to this table & update table status
		// see https://github.com/pingcap/tiflow/pull/4881#discussion_r834093316
		sourceTable := tblChange.sourceTable
		validateTbl, err2 := v.genValidateTableInfo(sourceTable, tblChange.columnCount)
		if err2 != nil {
			return terror.Annotate(err2, "failed to get table info on load")
		}
		if validateTbl.message != "" {
			return errors.New("failed to get table info " + validateTbl.message)
		}
		pendingTblChange := newTableChangeJob()
		// aggregate using target table just as worker did.
		pendingChanges[validateTbl.targetTable.String()] = pendingTblChange
		for _, row := range tblChange.rows {
			var beforeImage, afterImage []interface{}
			switch row.Tp {
			case rowInsert:
				afterImage = row.Data
			case rowUpdated:
				// set both to row.Data, since we only save one image on persist in order to save space
				beforeImage, afterImage = row.Data, row.Data
			default:
				// rowDeleted
				beforeImage = row.Data
			}
			pendingTblChange.jobs[row.Key] = &rowValidationJob{
				Key: row.Key,
				Tp:  row.Tp,
				row: sqlmodel.NewRowChange(
					&cdcmodel.TableName{Schema: sourceTable.Schema, Table: sourceTable.Name},
					&cdcmodel.TableName{Schema: validateTbl.targetTable.Schema, Table: validateTbl.targetTable.Name},
					beforeImage, afterImage,
					validateTbl.srcTableInfo, validateTbl.downstreamTableInfo.TableInfo,
					nil,
				),
				size:      row.Size,
				FailedCnt: row.FailedCnt,
			}
		}
	}

	v.location = data.checkpoint
	v.setProcessedRowCounts(data.processedRowCounts)
	v.loadedPendingChanges = pendingChanges
	v.persistHelper.setRevision(data.rev)
	v.initTableStatus(data.tableStatus)

	return nil
}

func (v *DataValidator) incrErrorRowCount(cnt int) {
	v.newErrorRowCount.Add(int64(cnt))
}

func (v *DataValidator) getWorkers() []*validateWorker {
	return v.workers
}

func (v *DataValidator) Started() bool {
	v.stateMutex.RLock()
	defer v.stateMutex.RUnlock()
	return v.stage == pb.Stage_Running
}

func (v *DataValidator) Stage() pb.Stage {
	v.stateMutex.RLock()
	defer v.stateMutex.RUnlock()
	return v.stage
}

func (v *DataValidator) setStage(stage pb.Stage) {
	v.stateMutex.Lock()
	defer v.stateMutex.Unlock()
	v.stage = stage
}

func (v *DataValidator) getFlushedLoc() *binlog.Location {
	v.stateMutex.RLock()
	defer v.stateMutex.RUnlock()
	return v.flushedLoc
}

func (v *DataValidator) setFlushedLoc(loc *binlog.Location) {
	v.stateMutex.Lock()
	defer v.stateMutex.Unlock()
	if loc == nil {
		v.flushedLoc = nil
		return
	}
	clone := loc.Clone()
	v.flushedLoc = &clone
}

func (v *DataValidator) getResult() pb.ProcessResult {
	v.stateMutex.RLock()
	defer v.stateMutex.RUnlock()
	return v.result
}

func (v *DataValidator) addResultError(err *pb.ProcessError, cancelled bool) {
	v.stateMutex.Lock()
	defer v.stateMutex.Unlock()
	if err != nil {
		v.result.Errors = append(v.result.Errors, err)
	}
	v.result.IsCanceled = cancelled
}

func (v *DataValidator) getResultErrCnt() int {
	v.stateMutex.Lock()
	defer v.stateMutex.Unlock()
	return len(v.result.Errors)
}

func (v *DataValidator) resetResult() {
	v.stateMutex.Lock()
	defer v.stateMutex.Unlock()
	v.result.Reset()
}

func (v *DataValidator) initTableStatus(m map[string]*tableValidateStatus) {
	v.stateMutex.Lock()
	defer v.stateMutex.Unlock()
	v.tableStatus = m
}

func (v *DataValidator) getTableStatus(fullTableName string) (*tableValidateStatus, bool) {
	v.stateMutex.RLock()
	defer v.stateMutex.RUnlock()
	res, ok := v.tableStatus[fullTableName]
	return res, ok
}

// return snapshot of the current table status.
func (v *DataValidator) getTableStatusMap() map[string]*tableValidateStatus {
	v.stateMutex.RLock()
	defer v.stateMutex.RUnlock()
	tblStatus := make(map[string]*tableValidateStatus)
	for key, tblStat := range v.tableStatus {
		stat := &tableValidateStatus{}
		*stat = *tblStat // deep copy
		tblStatus[key] = stat
	}
	return tblStatus
}

func (v *DataValidator) putTableStatus(name string, status *tableValidateStatus) {
	v.stateMutex.Lock()
	defer v.stateMutex.Unlock()
	v.tableStatus[name] = status
}

func (v *DataValidator) isMarkErrorStarted() bool {
	return v.markErrorStarted.Load()
}

func (v *DataValidator) getProcessedRowCounts() []int64 {
	return []int64{
		v.processedRowCounts[rowInsert].Load(),
		v.processedRowCounts[rowUpdated].Load(),
		v.processedRowCounts[rowDeleted].Load(),
	}
}

func (v *DataValidator) setProcessedRowCounts(counts []int64) {
	v.processedRowCounts[rowInsert].Store(counts[rowInsert])
	v.processedRowCounts[rowUpdated].Store(counts[rowUpdated])
	v.processedRowCounts[rowDeleted].Store(counts[rowDeleted])
}

func (v *DataValidator) addPendingRowCount(tp rowChangeJobType, cnt int64) {
	v.pendingRowCounts[tp].Add(cnt)
}

func (v *DataValidator) getAllPendingRowCount() int64 {
	return v.pendingRowCounts[rowInsert].Load() +
		v.pendingRowCounts[rowUpdated].Load() +
		v.pendingRowCounts[rowDeleted].Load()
}

func (v *DataValidator) addPendingRowSize(size int64) {
	v.pendingRowSize.Add(size)
}

func (v *DataValidator) getPendingRowSize() int64 {
	return v.pendingRowSize.Load()
}

func (v *DataValidator) sendError(err error) {
	v.errChan <- err
}

func (v *DataValidator) getNewErrorRowCount() int64 {
	return v.newErrorRowCount.Load()
}

// getRowChangeType should be called only when the event type is RowsEvent.
func getRowChangeType(t replication.EventType) rowChangeJobType {
	switch t {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return rowInsert
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return rowUpdated
	default:
		// replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return rowDeleted
	}
}

func genRowKey(row *sqlmodel.RowChange) string {
	vals := row.RowStrIdentity()
	return genRowKeyByString(vals)
}

func genRowKeyByString(pkValues []string) string {
	// in the scenario below, the generated key may not be unique, but it's rare
	// suppose a table with multiple column primary key: (v1, v2)
	// for below case, the generated key is the same:
	// 	 (aaa\t, bbb) and (aaa, \tbbb), the joint values both are "aaa\t\tbbb"
	join := strings.Join(pkValues, "\t")
	// if the key is too long, need to make sure it can be stored into database
	if len(join) > maxRowKeyLength {
		sum := sha256.Sum256([]byte(join))
		return hex.EncodeToString(sum[:])
	}
	return join
}

func (v *DataValidator) GetValidatorTableStatus(filterStatus pb.Stage) []*pb.ValidationTableStatus {
	tblStatus := v.getTableStatusMap()

	result := make([]*pb.ValidationTableStatus, 0)
	for _, tblStat := range tblStatus {
		returnAll := filterStatus == pb.Stage_InvalidStage
		if returnAll || tblStat.stage == filterStatus {
			result = append(result, &pb.ValidationTableStatus{
				Source:   v.cfg.SourceID,
				SrcTable: tblStat.source.String(),
				DstTable: tblStat.target.String(),
				Stage:    tblStat.stage,
				Message:  tblStat.message,
			})
		}
	}
	return result
}

func (v *DataValidator) GetValidatorError(errState pb.ValidateErrorState) ([]*pb.ValidationError, error) {
	// todo: validation error in workers cannot be returned
	// because the errID is only allocated when the error rows are flushed
	// user cannot handle errorRows without errID
	var (
		toDB  *conn.BaseDB
		err   error
		dbCfg dbconfig.DBConfig
	)
	ctx, cancel := context.WithTimeout(context.Background(), validatorDmctlOpTimeout)
	tctx := tcontext.NewContext(ctx, v.L)
	defer cancel()
	failpoint.Inject("MockValidationQuery", func() {
		toDB = v.persistHelper.db
		failpoint.Return(v.persistHelper.loadError(tctx, toDB, errState))
	})
	dbCfg = v.cfg.To
	dbCfg.RawDBCfg = dbconfig.DefaultRawDBConfig().SetMaxIdleConns(1)
	toDB, err = conn.GetDownstreamDB(&dbCfg)
	if err != nil {
		v.L.Warn("failed to create downstream db", zap.Error(err))
		return nil, err
	}
	defer dbconn.CloseBaseDB(tctx, toDB)
	ret, err := v.persistHelper.loadError(tctx, toDB, errState)
	if err != nil {
		v.L.Warn("fail to load validator error", zap.Error(err))
		return nil, err
	}
	return ret, nil
}

func (v *DataValidator) OperateValidatorError(validateOp pb.ValidationErrOp, errID uint64, isAll bool) error {
	var (
		toDB  *conn.BaseDB
		err   error
		dbCfg dbconfig.DBConfig
	)
	ctx, cancel := context.WithTimeout(context.Background(), validatorDmctlOpTimeout)
	tctx := tcontext.NewContext(ctx, v.L)
	defer cancel()
	failpoint.Inject("MockValidationQuery", func() {
		toDB = v.persistHelper.db
		failpoint.Return(v.persistHelper.operateError(tctx, toDB, validateOp, errID, isAll))
	})
	dbCfg = v.cfg.To
	dbCfg.RawDBCfg = dbconfig.DefaultRawDBConfig().SetMaxIdleConns(1)
	toDB, err = conn.GetDownstreamDB(&dbCfg)
	if err != nil {
		return err
	}
	defer dbconn.CloseBaseDB(tctx, toDB)
	return v.persistHelper.operateError(tctx, toDB, validateOp, errID, isAll)
}

func (v *DataValidator) getErrorRowCount(timeout time.Duration) ([errorStateTypeCount]int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	tctx := tcontext.NewContext(ctx, v.L)

	// use a separate db to get error count, since validator maybe stopped or initializing
	dbCfg := v.cfg.To
	dbCfg.RawDBCfg = dbconfig.DefaultRawDBConfig().SetMaxIdleConns(1)
	countMap := map[pb.ValidateErrorState]int64{}
	toDB, err := conn.GetDownstreamDB(&dbCfg)
	if err != nil {
		v.L.Warn("failed to create downstream db", zap.Error(err))
	} else {
		defer dbconn.CloseBaseDB(tctx, toDB)
		countMap, err = v.persistHelper.loadErrorCount(tctx, toDB)
		if err != nil {
			v.L.Warn("failed to load error count", zap.Error(err))
		}
	}
	var errorRowCount [errorStateTypeCount]int64
	errorRowCount[pb.ValidateErrorState_NewErr] = countMap[pb.ValidateErrorState_NewErr]
	errorRowCount[pb.ValidateErrorState_IgnoredErr] = countMap[pb.ValidateErrorState_IgnoredErr]
	errorRowCount[pb.ValidateErrorState_ResolvedErr] = countMap[pb.ValidateErrorState_ResolvedErr]

	errorRowCount[pb.ValidateErrorState_NewErr] += v.newErrorRowCount.Load()

	return errorRowCount, err
}

func (v *DataValidator) GetValidatorStatus() *pb.ValidationStatus {
	var extraMsg string
	errorRowCount, err := v.getErrorRowCount(validatorDmctlOpTimeout)
	if err != nil {
		// nolint:nilerr
		extraMsg = fmt.Sprintf(" (failed to load error count from meta db: %s)", err.Error())
	}
	// if we print those state in a structured way, there would be at least 9 lines for each subtask,
	// which is hard to read, so print them into one line.
	template := "insert/update/delete: %d/%d/%d"
	processedRowCounts := v.getProcessedRowCounts()
	processedRows := fmt.Sprintf(template, processedRowCounts[rowInsert],
		processedRowCounts[rowUpdated], processedRowCounts[rowDeleted])
	pendingRows := fmt.Sprintf(template, v.pendingRowCounts[rowInsert].Load(),
		v.pendingRowCounts[rowUpdated].Load(), v.pendingRowCounts[rowDeleted].Load())
	errorRows := fmt.Sprintf("new/ignored/resolved: %d/%d/%d%s",
		errorRowCount[pb.ValidateErrorState_NewErr], errorRowCount[pb.ValidateErrorState_IgnoredErr],
		errorRowCount[pb.ValidateErrorState_ResolvedErr], extraMsg)

	result := v.getResult()
	returnedResult := &result
	if !result.IsCanceled && len(result.Errors) == 0 {
		// no need to show if validator is running normally
		returnedResult = nil
	}

	flushedLoc := v.getFlushedLoc()
	var validatorBinlog, validatorBinlogGtid string
	if flushedLoc != nil {
		validatorBinlog = flushedLoc.Position.String()
		if flushedLoc.GetGTID() != nil {
			validatorBinlogGtid = flushedLoc.GetGTID().String()
		}
	}
	return &pb.ValidationStatus{
		Task:                v.cfg.Name,
		Source:              v.cfg.SourceID,
		Mode:                v.cfg.ValidatorCfg.Mode,
		Stage:               v.Stage(),
		Result:              returnedResult,
		ValidatorBinlog:     validatorBinlog,
		ValidatorBinlogGtid: validatorBinlogGtid,
		ProcessedRowsStatus: processedRows,
		PendingRowsStatus:   pendingRows,
		ErrorRowsStatus:     errorRows,
	}
}
