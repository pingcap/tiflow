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
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

const (
	checkInterval           = 5 * time.Second
	validationInterval      = 10 * time.Second
	validatorStatusInterval = 30 * time.Second

	moreColumnInBinlogMsg     = "binlog has more columns than current table"
	tableWithoutPrimaryKeyMsg = "no primary key"
)

var errNoPrimaryKey = errors.New("no primary key")

type validateTableInfo struct {
	Source     *filter.Table
	Info       *model.TableInfo
	PrimaryKey *model.IndexInfo
	Target     *filter.Table // target table after route
}

type rowChangeType int

const (
	rowInsert rowChangeType = iota
	rowDeleted
	rowUpdated
	flushCheckpoint

	rowChangeTypeCount  = 3
	errorStateTypeCount = 5 // pb.ValidateErrorState_*
)

// change of table
// binlog changes are clustered into table changes
// the validator validates changes of table-grain at a time.
type tableChange struct {
	table *validateTableInfo
	rows  map[string]*rowChange
}

func newTableChange(table *validateTableInfo) *tableChange {
	return &tableChange{table: table, rows: make(map[string]*rowChange)}
}

// change of a row.
// todo: this struct and some logic on it may reuse RowChange in pkg/sqlmodel
// todo: maybe we can use reuse it later.
type rowChange struct {
	table *validateTableInfo
	Key   string        `json:"key"`
	Data  []interface{} `json:"data"`
	Tp    rowChangeType `json:"tp"`
	wg    *sync.WaitGroup

	// timestamp of first validation of this row
	// will reset when merge row changes
	FirstValidateTS int64 `json:"first-validate-ts"`
	FailedCnt       int   `json:"failed-cnt"` // failed count
}

type tableValidateStatus struct {
	source  filter.Table
	target  filter.Table
	stage   pb.Stage // either Running or Stopped
	message string
}

// DataValidator
// validator can be start when there's syncer unit in the subtask and validation mode is not none,
// it's terminated when the subtask is terminated.
// stage of validator is independent of subtask, pause/resume subtask doesn't affect the stage of validator.
//
// validator can be in running or stopped stage
// - in running when it's started with subtask or started later on the fly.
// - in stopped when validation stop is executed.
type DataValidator struct {
	sync.RWMutex
	cfg    *config.SubTaskConfig
	syncer *Syncer
	// whether validator starts together with subtask
	startWithSubtask bool

	stage        pb.Stage
	wg           sync.WaitGroup
	errProcessWg sync.WaitGroup
	errChan      chan error
	ctx          context.Context
	cancel       context.CancelFunc
	tctx         *tcontext.Context

	L                  log.Logger
	fromDB             *conn.BaseDB
	toDB               *conn.BaseDB
	toDBConns          []*dbconn.DBConn
	timezone           *time.Location
	syncCfg            replication.BinlogSyncerConfig
	streamerController *StreamerController
	persistHelper      *validatorPersistHelper

	result           pb.ProcessResult
	validateInterval time.Duration
	workers          []*validateWorker
	workerCnt        int

	// whether the validation progress ever reached syncer
	// if it's false, we don't mark failed row change as error to reduce false-positive
	reachedSyncer atomic.Bool

	processedRowCounts   []atomic.Int64
	pendingRowCounts     []atomic.Int64
	errorRowCounts       []atomic.Int64
	lastFlushTime        time.Time
	tableStatus          map[string]*tableValidateStatus
	location             *binlog.Location
	loadedPendingChanges map[string]*tableChange
}

func NewContinuousDataValidator(cfg *config.SubTaskConfig, syncerObj *Syncer, startWithSubtask bool) *DataValidator {
	v := &DataValidator{
		cfg:              cfg,
		syncer:           syncerObj,
		startWithSubtask: startWithSubtask,
		stage:            pb.Stage_Stopped,
	}
	v.L = log.With(zap.String("task", cfg.Name), zap.String("unit", "continuous validator"))

	v.workerCnt = cfg.ValidatorCfg.WorkerCount
	v.processedRowCounts = make([]atomic.Int64, rowChangeTypeCount)
	v.validateInterval = validationInterval
	v.persistHelper = newValidatorCheckpointHelper(v)
	v.tableStatus = make(map[string]*tableValidateStatus)

	return v
}

func (v *DataValidator) initialize() error {
	v.reachedSyncer.Store(false)
	v.ctx, v.cancel = context.WithCancel(context.Background())
	v.tctx = tcontext.NewContext(v.ctx, v.L)
	v.result.Reset()
	// todo: enhance error handling
	v.errChan = make(chan error, 10)
	v.pendingRowCounts = make([]atomic.Int64, rowChangeTypeCount)
	v.errorRowCounts = make([]atomic.Int64, errorStateTypeCount)

	if err := v.persistHelper.init(v.tctx); err != nil {
		return err
	}

	newCtx, cancelFunc := context.WithTimeout(v.ctx, unit.DefaultInitTimeout)
	defer cancelFunc()
	tctx := tcontext.NewContext(newCtx, v.L)

	var err error
	defer func() {
		if err == nil {
			return
		}
		dbconn.CloseBaseDB(tctx, v.fromDB)
		dbconn.CloseBaseDB(tctx, v.toDB)
		v.cancel()
	}()

	dbCfg := v.cfg.From
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout)
	v.fromDB, _, err = dbconn.CreateConns(tctx, v.cfg, &dbCfg, 0)
	if err != nil {
		return err
	}

	dbCfg = v.cfg.To
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout).SetMaxIdleConns(v.workerCnt)
	v.toDB, v.toDBConns, err = dbconn.CreateConns(tctx, v.cfg, &dbCfg, v.workerCnt)
	if err != nil {
		return err
	}

	v.timezone, err = str2TimezoneOrFromDB(tctx, v.cfg.Timezone, &v.cfg.To)
	if err != nil {
		return err
	}

	v.syncCfg, err = subtaskCfg2BinlogSyncerCfg(v.cfg, v.timezone)
	if err != nil {
		return err
	}

	v.streamerController = NewStreamerController(v.syncCfg, v.cfg.EnableGTID, &dbconn.UpStreamConn{BaseDB: v.fromDB}, v.cfg.RelayDir, v.timezone, nil)
	return nil
}

func (v *DataValidator) Start(expect pb.Stage) {
	v.Lock()
	defer v.Unlock()
	failpoint.Inject("MockValidationQuery", func() {
		v.stage = pb.Stage_Running
		failpoint.Return()
	})

	v.L.Info("starting")
	if v.stage == pb.Stage_Running {
		v.L.Info("already started")
		return
	}

	if err := v.initialize(); err != nil {
		v.fillResult(err, false)
		return
	}

	if expect != pb.Stage_Running {
		return
	}

	v.wg.Add(1)
	go v.doValidate()

	v.wg.Add(1)
	go v.printStatusRoutine()

	v.errProcessWg.Add(1)
	go v.errorProcessRoutine()

	v.stage = pb.Stage_Running
	v.L.Info("started")
}

func (v *DataValidator) printStatusRoutine() {
	defer v.wg.Done()
	for {
		select {
		case <-v.ctx.Done():
			return
		case <-time.After(validatorStatusInterval):
			processed := []int64{
				v.processedRowCounts[rowInsert].Load(),
				v.processedRowCounts[rowUpdated].Load(),
				v.processedRowCounts[rowDeleted].Load(),
			}
			pending := []int64{
				v.pendingRowCounts[rowInsert].Load(),
				v.pendingRowCounts[rowUpdated].Load(),
				v.pendingRowCounts[rowDeleted].Load(),
			}
			errorCounts := []int64{
				v.errorRowCounts[pb.ValidateErrorState_NewErr].Load(),
				v.errorRowCounts[pb.ValidateErrorState_IgnoredErr].Load(),
				v.errorRowCounts[pb.ValidateErrorState_ResolvedErr].Load(),
			}
			v.L.Info("validator status",
				zap.Int64s("processed(i, u, d)", processed),
				zap.Int64s("pending(i, u, d)", pending),
				zap.Int64s("error(new, ignored, resolved)", errorCounts),
			)
		}
	}
}

func (v *DataValidator) fillResult(err error, needLock bool) {
	if needLock {
		v.Lock()
		defer v.Unlock()
	}

	// todo: if error, validation is stopped( and cancelled), and we may receive a cancelled error.
	// todo: do we need this cancel field?
	isCanceled := false
	select {
	case <-v.ctx.Done():
		isCanceled = true
	default:
	}
	v.result.IsCanceled = isCanceled

	if utils.IsContextCanceledError(err) {
		v.L.Info("filter out context cancelled error", log.ShortError(err))
	} else {
		v.L.Error("error during validation", zap.Error(err))
		v.result.Errors = append(v.result.Errors, unit.NewProcessError(err))
	}
}

func (v *DataValidator) errorProcessRoutine() {
	defer v.errProcessWg.Done()
	for err := range v.errChan {
		v.fillResult(err, true)

		if errors.Cause(err) != context.Canceled {
			// todo: need a better way to handle err(auto resuming on some error, etc.)
			v.stopInner()
		}
	}
}

func (v *DataValidator) waitSyncerSynced(currLoc binlog.Location) error {
	syncLoc := v.syncer.getFlushedGlobalPoint()
	cmp := binlog.CompareLocation(currLoc, syncLoc, v.cfg.EnableGTID)
	if cmp <= 0 {
		return nil
	}

	v.reachedSyncer.Store(true)
	for {
		select {
		case <-v.ctx.Done():
			return v.ctx.Err()
		case <-time.After(checkInterval):
			syncLoc = v.syncer.getFlushedGlobalPoint()
			cmp = binlog.CompareLocation(currLoc, syncLoc, v.cfg.EnableGTID)
			if cmp <= 0 {
				return nil
			}
			v.L.Debug("wait syncer synced", zap.Reflect("loc", currLoc))
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
		case <-time.After(checkInterval):
			if v.syncer.IsRunning() {
				v.L.Info("syncer is running, wait finished")
				return nil
			}
		}
	}
}

// doValidate: runs in a separate goroutine.
func (v *DataValidator) doValidate() {
	defer v.wg.Done()

	if err := v.waitSyncerRunning(); err != nil {
		v.errChan <- terror.Annotate(err, "failed to wait syncer running")
		return
	}

	if err := v.loadPersistedData(v.tctx); err != nil {
		v.errChan <- terror.Annotate(err, "failed to load persisted data")
		return
	}

	var location binlog.Location
	if v.location != nil {
		location = *v.location
	} else {
		if v.startWithSubtask {
			// in extreme case, this loc may still not be the first binlog location of this task:
			//   syncer synced some binlog and flush checkpoint, but validator still not has chance to run, then fail-over
			location = v.syncer.getInitExecutedLoc()
		} else {
			location = v.syncer.getFlushedGlobalPoint()
		}
		// persist current location to make sure we start from the same location
		// if fail-over happens before we flush checkpoint and data.
		err := v.persistHelper.persist(location)
		if err != nil {
			v.errChan <- terror.Annotate(err, "failed to persist checkpoint")
			return
		}
	}
	// it's for test, some fields in streamerController is mocked, cannot call Start
	if v.streamerController.IsClosed() {
		err := v.streamerController.Start(v.tctx, location)
		if err != nil {
			v.errChan <- terror.Annotate(err, "fail to start streamer controller")
			return
		}
	}

	v.startValidateWorkers()
	defer func() {
		for _, worker := range v.workers {
			worker.close()
		}
	}()

	// when gtid is enabled:
	// gtid of currLoc need to be updated when meet gtid event, so we can compare with syncer
	// gtid of locationForFlush is updated when we meet xid event, if error happened, we start sync from this location
	//
	// some events maybe processed again, but it doesn't matter for validation
	// todo: maybe we can use curStartLocation/curEndLocation in locationRecorder
	currLoc := location.CloneWithFlavor(v.cfg.Flavor)
	// we don't flush checkpoint&data on exist, since checkpoint and pending data may not correspond with each other.
	locationForFlush := currLoc.Clone()
	v.lastFlushTime = time.Now()
	for {
		e, err := v.streamerController.GetEvent(v.tctx)
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
					v.errChan <- terror.Annotate(err1, "fail to update UpdateServerIDAndResetReplication")
					return
				}
				continue
			case err == relay.ErrorMaybeDuplicateEvent:
				continue
			case isConnectionRefusedError(err):
				v.errChan <- terror.Annotate(err, "fail to get event")
				return
			default:
				if v.streamerController.CanRetry(err) {
					err = v.streamerController.ResetReplicationSyncer(v.tctx, locationForFlush)
					if err != nil {
						v.errChan <- terror.Annotate(err, "fail to reset replication")
						return
					}
					continue
				}
				v.errChan <- terror.Annotate(err, "fail to get binlog from stream controller")
				return
			}
		}

		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			currLoc.Position.Name = string(ev.NextLogName)
			currLoc.Position.Pos = uint32(ev.Position)
		case *replication.GTIDEvent, *replication.MariadbGTIDEvent:
			currLoc.Position.Pos = e.Header.LogPos
			gtidStr, _ := event.GetGTIDStr(e)
			if err = currLoc.Update(gtidStr); err != nil {
				v.errChan <- terror.Annotate(err, "failed to update gtid set")
				return
			}
		default:
			currLoc.Position.Pos = e.Header.LogPos
		}

		// wait until syncer synced current event
		err = v.waitSyncerSynced(currLoc)
		if err != nil {
			v.errChan <- terror.Annotate(err, "failed to wait syncer")
			return
		}

		switch ev := e.Event.(type) {
		case *replication.RowsEvent:
			if err = v.processRowsEvent(e.Header, ev); err != nil {
				v.L.Warn("failed to process event: ", zap.Reflect("error", err))
				v.errChan <- terror.Annotate(err, "failed to process event")
				return
			}
			// update on success processed
			locationForFlush.Position = currLoc.Position
		case *replication.XIDEvent:
			locationForFlush.Position = currLoc.Position
			err = locationForFlush.SetGTID(ev.GSet)
			if err != nil {
				v.errChan <- terror.Annotate(err, "failed to set gtid")
				return
			}
			if err = v.checkAndFlushCheckpoint(locationForFlush); err != nil {
				v.errChan <- terror.Annotate(err, "failed to flush checkpoint")
				return
			}
		case *replication.GenericEvent:
			if e.Header.EventType == replication.HEARTBEAT_EVENT {
				if err = v.checkAndFlushCheckpoint(locationForFlush); err != nil {
					v.errChan <- terror.Annotate(err, "failed to flush checkpoint")
					return
				}
			}
		default:
			locationForFlush.Position = currLoc.Position
		}
	}
}

func (v *DataValidator) Stop() {
	v.stopInner()
	v.errProcessWg.Wait()
}

func (v *DataValidator) stopInner() {
	v.Lock()
	defer v.Unlock()
	v.L.Info("stopping")
	if v.stage != pb.Stage_Running {
		v.L.Warn("not started")
		return
	}

	v.cancel()
	v.streamerController.Close()
	v.fromDB.Close()
	v.toDB.Close()
	v.persistHelper.close()

	v.wg.Wait()
	close(v.errChan) // close error chan after all possible sender goroutines stopped

	v.stage = pb.Stage_Stopped
	v.L.Info("stopped")
}

func (v *DataValidator) Started() bool {
	v.RLock()
	defer v.RUnlock()
	return v.stage == pb.Stage_Running
}

func (v *DataValidator) Stage() pb.Stage {
	v.RLock()
	defer v.RUnlock()
	return v.stage
}

func (v *DataValidator) startValidateWorkers() {
	v.wg.Add(v.workerCnt)
	v.workers = make([]*validateWorker, v.workerCnt)
	for i := 0; i < v.workerCnt; i++ {
		worker := newValidateWorker(v, i)
		v.workers[i] = worker
		go func() {
			v.wg.Done()
			worker.run()
		}()
	}

	for _, tblChange := range v.loadedPendingChanges {
		for key, row := range tblChange.rows {
			v.dispatchRowChange(key, row)
		}
	}
}

func (v *DataValidator) dispatchRowChange(key string, row *rowChange) {
	hashVal := int(utils.GenHashKey(key)) % v.workerCnt
	v.workers[hashVal].rowChangeCh <- row
}

func (v *DataValidator) genValidateTableInfo(sourceTable *filter.Table) (*validateTableInfo, error) {
	targetTable := v.syncer.route(sourceTable)
	// there are 2 cases tracker may drop table:
	// 1. checkpoint rollback, tracker may recreate tables and drop non-needed tables
	// 2. when operate-schema
	// in case 1, we add another layer synchronization to make sure we don't get a dropped table when recreation.
	// 	for non-needed tables, we will not validate them.
	// in case 2, validator should be paused
	tableInfo, err := v.syncer.getTrackedTableInfo(sourceTable)
	if err != nil {
		return nil, err
	}
	table := &validateTableInfo{
		Source: sourceTable,
		Info:   tableInfo,
		Target: targetTable,
	}

	var primaryIdx *model.IndexInfo
	for _, idx := range tableInfo.Indices {
		if idx.Primary {
			primaryIdx = idx
		}
	}
	// todo: PKIsHandle = true when table has primary key like "id int primary key CLUSTERED", since schema-tracker(we get from it)
	// todo: only use downstream DDL when the task is incremental only, will support this case later.
	if primaryIdx == nil {
		// return partial initialized result
		return table, errNoPrimaryKey
	}
	table.PrimaryKey = primaryIdx

	columnMap := make(map[string]*model.ColumnInfo)
	for _, col := range tableInfo.Columns {
		columnMap[col.Name.O] = col
	}

	return table, nil
}

func (v *DataValidator) processRowsEvent(header *replication.EventHeader, ev *replication.RowsEvent) error {
	v.Lock() // mutex lock to protect tableStatus and maybe other fields
	defer v.Unlock()
	sourceTable := &filter.Table{
		Schema: string(ev.Table.Schema),
		Name:   string(ev.Table.Table),
	}
	fullTableName := sourceTable.String()
	if state, ok := v.tableStatus[fullTableName]; ok && state.stage == pb.Stage_Stopped {
		return nil
	}

	if err := checkLogColumns(ev.SkippedColumns); err != nil {
		return errors.Errorf("unexpected skipped columns for table %s", sourceTable.String())
	}

	needSkip, err := v.syncer.skipRowsEvent(sourceTable, header.EventType)
	if err != nil {
		return err
	}
	if needSkip {
		return nil
	}

	table, err := v.genValidateTableInfo(sourceTable)
	if err != nil {
		if schema.IsTableNotExists(err) {
			// not a table need to sync
			return nil
		} else if err == errNoPrimaryKey {
			v.tableStatus[fullTableName] = &tableValidateStatus{
				source:  *sourceTable,
				target:  *table.Target,
				stage:   pb.Stage_Stopped,
				message: tableWithoutPrimaryKeyMsg,
			}
			return nil
		}
		return terror.Annotate(err, "failed to get table info")
	}

	if len(table.Info.Columns) < int(ev.ColumnCount) {
		v.tableStatus[fullTableName] = &tableValidateStatus{
			source:  *sourceTable,
			target:  *table.Target,
			stage:   pb.Stage_Stopped,
			message: moreColumnInBinlogMsg,
		}
		return nil
	}

	if _, ok := v.tableStatus[fullTableName]; !ok {
		v.tableStatus[fullTableName] = &tableValidateStatus{
			source: *sourceTable,
			target: *table.Target,
			stage:  pb.Stage_Running,
		}
	}

	changeType := getRowChangeType(header.EventType)

	step := 1
	if changeType == rowUpdated {
		step = 2
	}
	for i := 0; i < len(ev.Rows); i += step {
		row := ev.Rows[i]
		pkCols := table.PrimaryKey.Columns
		pkValue := make([]string, len(pkCols))
		for idx, col := range pkCols {
			pkValue[idx] = genColData(row[col.Offset])
		}
		key := genRowKey(pkValue)

		if changeType == rowUpdated {
			afterRowChangeType := changeType
			afterRow := ev.Rows[i+1]
			afterPkValue := make([]string, len(pkCols))
			for idx, col := range pkCols {
				afterPkValue[idx] = genColData(afterRow[col.Offset])
			}
			afterKey := genRowKey(afterPkValue)
			if afterKey != key {
				// TODO: may reuse IsIdentityUpdated/SplitUpdate of RowChange in pkg/sqlmodel
				v.dispatchRowChange(key, &rowChange{
					table: table,
					Key:   key,
					Data:  row,
					Tp:    rowDeleted,
				})
				v.processedRowCounts[rowDeleted].Inc()
				afterRowChangeType = rowInsert
			}
			v.dispatchRowChange(afterKey, &rowChange{
				table: table,
				Key:   afterKey,
				Data:  afterRow,
				Tp:    afterRowChangeType,
			})
			v.processedRowCounts[afterRowChangeType].Inc()
		} else {
			v.dispatchRowChange(key, &rowChange{
				table: table,
				Key:   key,
				Data:  row,
				Tp:    changeType,
			})
			v.processedRowCounts[changeType].Inc()
		}
	}
	return nil
}

func (v *DataValidator) checkAndFlushCheckpoint(loc binlog.Location) error {
	metaFlushInterval := v.cfg.ValidatorCfg.MetaFlushInterval.Duration
	if time.Since(v.lastFlushTime) > metaFlushInterval {
		v.lastFlushTime = time.Now()
		if err := v.flushCheckpointAndData(loc); err != nil {
			v.L.Warn("failed to flush checkpoint: ", zap.Error(err))
			return err
		}
	}
	return nil
}

func (v *DataValidator) flushCheckpointAndData(loc binlog.Location) error {
	var wg sync.WaitGroup
	wg.Add(v.workerCnt)
	flushJob := &rowChange{
		Tp: flushCheckpoint,
		wg: &wg,
	}
	for _, worker := range v.workers {
		worker.rowChangeCh <- flushJob
	}
	wg.Wait()

	err := v.persistHelper.persist(loc)
	if err != nil {
		return err
	}
	return nil
}

func (v *DataValidator) loadPersistedData(tctx *tcontext.Context) error {
	var err error
	v.location, err = v.persistHelper.loadCheckpoint(tctx)
	if err != nil {
		return err
	}

	var rev int64
	v.loadedPendingChanges, rev, err = v.persistHelper.loadPendingChange(tctx)
	if err != nil {
		return err
	}
	// table info of pending change is not persisted in order to save space, so need to init them after load.
	for _, tblChange := range v.loadedPendingChanges {
		tblChange.table, err = v.genValidateTableInfo(tblChange.table.Source)
		// todo: if table is dropped since last run, we should skip rows related to this table & update table status
		// see https://github.com/pingcap/tiflow/pull/4881#discussion_r834093316
		if err != nil {
			return err
		}
		for _, row := range tblChange.rows {
			row.table = tblChange.table
		}
	}

	v.persistHelper.setRevision(rev)

	v.tableStatus, err = v.persistHelper.loadTableStatus(tctx)
	if err != nil {
		return err
	}

	countMap, err := v.persistHelper.loadErrorCount()
	if err != nil {
		return err
	}
	for i := range v.errorRowCounts {
		v.errorRowCounts[i].Store(countMap[pb.ValidateErrorState(i)])
	}
	return nil
}

func (v *DataValidator) incrErrorRowCount(status pb.ValidateErrorState, cnt int) {
	v.errorRowCounts[status].Add(int64(cnt))
}

func (v *DataValidator) hasReachedSyncer() bool {
	return v.reachedSyncer.Load()
}

func (v *DataValidator) addPendingRowCount(tp rowChangeType, cnt int64) {
	v.pendingRowCounts[tp].Add(cnt)
}

// getRowChangeType should be called only when the event type is RowsEvent.
func getRowChangeType(t replication.EventType) rowChangeType {
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

func genRowKey(pkValues []string) string {
	// TODO: in scenario below, the generated key may not unique, but it's rare
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

func genColData(v interface{}) string {
	switch dv := v.(type) {
	case []byte:
		return string(dv)
	case string:
		return dv
	}
	s := fmt.Sprintf("%v", v)
	return s
}

func (v *DataValidator) GetValidationStatus() []*pb.ValidationStatus {
	failpoint.Inject("MockValidationQuery", func() {
		failpoint.Return([]*pb.ValidationStatus{
			{
				SrcTable:         "`testdb1`.`testtable1`",
				DstTable:         "`dstdb`.`dsttable`",
				ValidationStatus: pb.Stage_Running.String(),
			},
			{
				SrcTable:         "`testdb2`.`testtable2`",
				DstTable:         "`dstdb`.`dsttable`",
				ValidationStatus: pb.Stage_Stopped.String(),
				Message:          tableWithoutPrimaryKeyMsg,
			},
		})
	})
	v.RLock()
	defer v.RUnlock()
	result := make([]*pb.ValidationStatus, 0)
	for _, tblStat := range v.tableStatus {
		result = append(result, &pb.ValidationStatus{
			SrcTable:         tblStat.source.String(),
			DstTable:         tblStat.target.String(),
			ValidationStatus: tblStat.stage.String(),
			Message:          tblStat.message,
		})
	}
	return result
}

func (v *DataValidator) GetValidatorError(errState pb.ValidateErrorState) []*pb.ValidationError {
	failpoint.Inject("MockValidationQuery", func() {
		failpoint.Return(
			[]*pb.ValidationError{
				{Id: "1"}, {Id: "2"},
			},
		)
	})
	// todo: validation error in workers cannot be returned
	// because the errID is only allocated when the error rows are flushed
	// user cannot handle errorRows without errID
	ret, err := v.persistHelper.loadError(errState)
	if err != nil {
		v.L.Warn("fail to load validator error", zap.Error(err))
	}
	return ret
}

func (v *DataValidator) OperateValidatorError(validateOp pb.ValidationErrOp, errID uint64, isAll bool) error {
	failpoint.Inject("MockValidationOperation", func() {
		failpoint.Return(nil)
	})
	return v.persistHelper.operateError(validateOp, errID, isAll)
}

// return snapshot of the current table status.
func (v *DataValidator) getTableStatus() map[string]*tableValidateStatus {
	v.RLock()
	defer v.RUnlock()
	tblStatus := make(map[string]*tableValidateStatus)
	for key, tblStat := range v.tableStatus {
		stat := &tableValidateStatus{}
		*stat = *tblStat // deep copy
		tblStatus[key] = stat
	}
	return tblStatus
}
