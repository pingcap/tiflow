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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/parser/model"
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
	pkIndices  []int         // TODO: can we use offset of in indexColumn? may remove this field in that case
}

type rowChangeType int

const (
	rowInsert rowChangeType = iota
	rowDeleted
	rowUpdated
	flushCheckpoint
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
	changeEventCount []atomic.Int64
	workerCnt        int

	// whether the validation progress ever reached syncer
	// if it's false, we don't mark failed row change as error to reduce false-positive
	reachedSyncer atomic.Bool

	tableStatus          map[string]*tableValidateStatus
	location             *binlog.Location
	loadedPendingChanges map[string]*tableChange
}

func NewContinuousDataValidator(cfg *config.SubTaskConfig, syncerObj *Syncer) *DataValidator {
	v := &DataValidator{
		cfg:    cfg,
		syncer: syncerObj,
		stage:  pb.Stage_Stopped,
	}
	v.L = log.With(zap.String("task", cfg.Name), zap.String("unit", "continuous validator"))

	v.workerCnt = cfg.ValidatorCfg.WorkerCount
	v.changeEventCount = make([]atomic.Int64, 3)
	v.workers = make([]*validateWorker, v.workerCnt)
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
			// todo: status about pending row changes
			v.L.Info("processed event status",
				zap.Int64("insert", v.changeEventCount[rowInsert].Load()),
				zap.Int64("update", v.changeEventCount[rowUpdated].Load()),
				zap.Int64("delete", v.changeEventCount[rowDeleted].Load()),
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
	syncLoc := v.syncer.checkpoint.FlushedGlobalPoint()
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
			syncLoc = v.syncer.checkpoint.FlushedGlobalPoint()
			cmp = binlog.CompareLocation(currLoc, syncLoc, v.cfg.EnableGTID)
			if cmp <= 0 {
				return nil
			}
			v.L.Debug("wait syncer synced", zap.Reflect("loc", currLoc))
		}
	}
}

func (v *DataValidator) waitSyncerRunning() error {
	if v.syncer.IsSchemaLoaded() {
		return nil
	}
	v.L.Info("wait until syncer running")
	for {
		select {
		case <-v.ctx.Done():
			return v.ctx.Err()
		case <-time.After(checkInterval):
			if v.syncer.IsSchemaLoaded() {
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

	// todo: if validator starts on task start, we need to make sure the location we got is the init location of syncer
	// todo: right now, there's change they have a gap
	var location binlog.Location
	if v.location != nil {
		location = *v.location
	} else {
		location = v.syncer.checkpoint.FlushedGlobalPoint()
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

	metaFlushInterval := v.cfg.ValidatorCfg.MetaFlushInterval.Duration
	// when gtid is enabled:
	// gtid of currLoc need to be updated when meet gtid event, so we can compare with syncer
	// gtid of locationForFlush is updated when we meet xid event, if error happened, we start sync from this location
	//
	// some events maybe processed again, but it doesn't matter for validation
	// todo: maybe we can use curStartLocation/curEndLocation in locationRecorder
	currLoc := location.CloneWithFlavor(v.cfg.Flavor)
	locationForFlush := currLoc.Clone()
	lastFlushCheckpointTime := time.Now()
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
			// TODO: if this routine block on get event, should make sure checkpoint is flushed normally
			if time.Since(lastFlushCheckpointTime) > metaFlushInterval {
				lastFlushCheckpointTime = time.Now()
				if err = v.flushCheckpointAndData(locationForFlush); err != nil {
					v.L.Warn("failed to flush checkpoint: ", zap.Error(err))
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
	// todo: syncer will change schema in schemaTracker, will there be data race?
	// todo: what if table is dropped while validator falls behind?
	tableInfo, err := v.syncer.schemaTracker.GetTableInfo(sourceTable)
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
	pkIndices := make([]int, len(primaryIdx.Columns))
	for i, col := range primaryIdx.Columns {
		pkIndices[i] = columnMap[col.Name.O].Offset
	}
	table.pkIndices = pkIndices

	return table, nil
}

func (v *DataValidator) processRowsEvent(header *replication.EventHeader, ev *replication.RowsEvent) error {
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
	v.changeEventCount[changeType].Inc()

	step := 1
	if changeType == rowUpdated {
		step = 2
	}
	for i := 0; i < len(ev.Rows); i += step {
		row := ev.Rows[i]
		pkValue := make([]string, len(table.pkIndices))
		for _, idx := range table.pkIndices {
			pkValue[idx] = genColData(row[idx])
		}
		key := genRowKey(pkValue)

		if changeType == rowUpdated {
			afterRowChangeType := changeType
			afterRow := ev.Rows[i+1]
			afterPkValue := make([]string, len(table.pkIndices))
			for _, idx := range table.pkIndices {
				afterPkValue[idx] = genColData(afterRow[idx])
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
				afterRowChangeType = rowInsert
			}
			v.dispatchRowChange(afterKey, &rowChange{
				table: table,
				Key:   afterKey,
				Data:  afterRow,
				Tp:    afterRowChangeType,
			})
		} else {
			v.dispatchRowChange(key, &rowChange{
				table: table,
				Key:   key,
				Data:  row,
				Tp:    changeType,
			})
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
	return nil
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
	return strings.Join(pkValues, "-")
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
