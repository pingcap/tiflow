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
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

const (
	checkInterval      = 5 * time.Second
	validationInterval = 10 * time.Second
)

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
)

// change of table
// binlog changes are clustered into table changes
// the validator validates changes of table-grain at a time.
type tableChange struct {
	table *validateTableInfo
	rows  map[string]*rowChange
}

// change of a row.
// todo: this struct and some logic on it may reuse RowChange in pkg/sqlmodel
// todo: maybe we can use reuse it later.
type rowChange struct {
	table      *validateTableInfo
	key        string
	pkValues   []string // todo: might be removed in later pr
	data       []interface{}
	tp         rowChangeType
	lastMeetTS int64 // the last meet timestamp(in seconds)
	failedCnt  int   // failed count
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

	result           pb.ProcessResult
	validateInterval time.Duration
	workers          []*validateWorker
	changeEventCount []atomic.Int64
	workerCnt        int

	// such as table without primary key
	unsupportedTable map[string]string
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

	v.unsupportedTable = make(map[string]string)

	return v
}

func (v *DataValidator) initialize() error {
	v.ctx, v.cancel = context.WithCancel(context.Background())
	v.tctx = tcontext.NewContext(v.ctx, v.L)
	v.result.Reset()
	// todo: enhance error handling
	v.errChan = make(chan error, 10)

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
}

func (v *DataValidator) printStatusRoutine() {
	defer v.wg.Done()
	for {
		select {
		case <-v.ctx.Done():
			return
		case <-time.After(checkInterval):
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
			v.L.Info("wait syncer synced", zap.Reflect("loc", currLoc))
		}
	}
}

func (v *DataValidator) waitSyncerRunning() error {
	if v.syncer.IsRunning() {
		return nil
	}
	for {
		select {
		case <-v.ctx.Done():
			return v.ctx.Err()
		case <-time.After(checkInterval):
			if v.syncer.IsRunning() {
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

	// todo: if validator starts on task start, we need to make sure the location we got is the init location of syncer
	// todo: right now, there's change they have a gap
	location := v.syncer.checkpoint.FlushedGlobalPoint()
	// it's for test, some fields in streamerController is mocked, cannot call Start
	if v.streamerController.IsClosed() {
		err := v.streamerController.Start(v.tctx, location)
		if err != nil {
			v.errChan <- terror.Annotate(err, "fail to start streamer controller")
			return
		}
	}

	v.L.Info("start continuous validation")
	v.startValidateWorkers()
	defer func() {
		for _, worker := range v.workers {
			worker.close()
		}
	}()

	currLoc := location.CloneWithFlavor(v.cfg.Flavor)
	for {
		e, err := v.streamerController.GetEvent(v.tctx)
		if err != nil {
			v.errChan <- terror.Annotate(err, "fail to get binlog from stream controller")
			return
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

		if ev, ok := e.Event.(*replication.RowsEvent); ok {
			if err = v.processRowsEvent(e.Header, ev); err != nil {
				v.L.Warn("failed to process event: ", zap.Reflect("error", err))
				v.errChan <- terror.Annotate(err, "failed to process event")
				return
			}
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
	if v.stage != pb.Stage_Running {
		v.L.Warn("not started")
		return
	}

	v.cancel()
	v.streamerController.Close()
	v.fromDB.Close()
	v.toDB.Close()

	v.wg.Wait()
	close(v.errChan) // close error chan after all possible sender goroutines stopped

	v.stage = pb.Stage_Stopped
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
}

func (v *DataValidator) dispatchRowChange(key string, row *rowChange) {
	hashVal := int(utils.GenHashKey(key)) % v.workerCnt
	v.workers[hashVal].rowChangeCh <- row
}

func (v *DataValidator) processRowsEvent(header *replication.EventHeader, ev *replication.RowsEvent) error {
	sourceTable := &filter.Table{
		Schema: string(ev.Table.Schema),
		Name:   string(ev.Table.Table),
	}
	fullTableName := sourceTable.String()
	if _, ok := v.unsupportedTable[fullTableName]; ok {
		return nil
	}

	needSkip, err := v.syncer.skipRowsEvent(sourceTable, header.EventType)
	if err != nil {
		return err
	}
	if needSkip {
		return nil
	}

	targetTable := v.syncer.route(sourceTable)
	// todo: syncer will change schema in schemaTracker, will there be data race?
	// todo: what if table is dropped while validator falls behind?
	tableInfo, err := v.syncer.schemaTracker.GetTableInfo(sourceTable)
	if err != nil {
		if schema.IsTableNotExists(err) {
			// not a table need to sync
			return nil
		}
		return terror.Annotate(err, "failed to get table info")
	}

	if len(tableInfo.Columns) < int(ev.ColumnCount) {
		v.unsupportedTable[fullTableName] = "binlog has more columns than current table"
		return nil
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
		// todo: for table without primary index, need to record in the failed table, will add it later together with checkpoint
		v.unsupportedTable[fullTableName] = "without primary key"
		return nil
	}

	table := &validateTableInfo{
		Source:     sourceTable,
		Info:       tableInfo,
		PrimaryKey: primaryIdx,
		Target:     targetTable,
	}

	for _, cols := range ev.SkippedColumns {
		if len(cols) > 0 {
			err := errors.Errorf("unexpected skipped columns for table %s", sourceTable.String())
			return err
		}
	}

	changeType := getRowChangeType(header.EventType)
	v.changeEventCount[changeType].Inc()

	columnMap := make(map[string]*model.ColumnInfo)
	for _, col := range tableInfo.Columns {
		columnMap[col.Name.O] = col
	}
	pk := table.PrimaryKey
	pkIndices := make([]int, len(pk.Columns))
	for i, col := range pk.Columns {
		pkIndices[i] = columnMap[col.Name.O].Offset
	}
	table.pkIndices = pkIndices

	step := 1
	if changeType == rowUpdated {
		step = 2
	}
	for i := 0; i < len(ev.Rows); i += step {
		row := ev.Rows[i]
		pkValue := make([]string, len(pk.Columns))
		for _, idx := range pkIndices {
			pkValue[idx] = genColData(row[idx])
		}
		key := genRowKey(pkValue)

		if changeType == rowUpdated {
			afterRowChangeType := changeType
			afterRow := ev.Rows[i+1]
			afterPkValue := make([]string, len(pk.Columns))
			for _, idx := range pkIndices {
				afterPkValue[idx] = genColData(afterRow[idx])
			}
			afterKey := genRowKey(afterPkValue)
			if afterKey != key {
				// TODO: may reuse IsIdentityUpdated/SplitUpdate of RowChange in pkg/sqlmodel
				v.dispatchRowChange(key, &rowChange{
					table:      table,
					key:        key,
					pkValues:   pkValue,
					data:       row,
					tp:         rowDeleted,
					lastMeetTS: int64(header.Timestamp),
				})
				afterRowChangeType = rowInsert
			}
			v.dispatchRowChange(afterKey, &rowChange{
				table:      table,
				key:        afterKey,
				pkValues:   afterPkValue,
				data:       afterRow,
				tp:         afterRowChangeType,
				lastMeetTS: int64(header.Timestamp),
			})
		} else {
			v.dispatchRowChange(key, &rowChange{
				table:      table,
				key:        key,
				pkValues:   pkValue,
				data:       row,
				tp:         changeType,
				lastMeetTS: int64(header.Timestamp),
			})
		}
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
