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
	"database/sql"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/parser/model"
	tidbmysql "github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

const (
	checkInterval        = 5 * time.Second
	retryInterval        = 5 * time.Second
	validationInterval   = 10 * time.Second
	defaultBatchRowCount = 500
	defaultWorkerCnt     = 5
)

type validateTableInfo struct {
	Schema     string
	Name       string
	Info       *model.TableInfo
	PrimaryKey *model.IndexInfo
	ColumnMap  map[string]*model.ColumnInfo
	Target     *filter.Table // target table after route
}

type RowDataIterator interface {
	// Next seeks the next row data, it used when compared rows.
	Next() (map[string]*dbutil.ColumnData, error)
	// Close release the resource.
	Close()
}

type RowDataIteratorImpl struct {
	Rows *sql.Rows
}

func (r *RowDataIteratorImpl) Next() (map[string]*dbutil.ColumnData, error) {
	for r.Rows.Next() {
		rowData, err := dbutil.ScanRow(r.Rows)
		return rowData, err
	}
	return nil, nil
}

func (r *RowDataIteratorImpl) Close() {
	r.Rows.Close()
}

type RowChangeIteratorImpl struct {
	Rows []map[string]*dbutil.ColumnData
	Idx  int
}

func (b *RowChangeIteratorImpl) Next() (map[string]*dbutil.ColumnData, error) {
	if b.Idx >= len(b.Rows) {
		return nil, nil
	}
	row := b.Rows[b.Idx]
	b.Idx++
	return row, nil
}

func (b *RowChangeIteratorImpl) Close() {
	// skip: nothing to do
}

type rowChangeType int

const (
	rowInvalidChange rowChangeType = iota
	rowInsert
	rowDeleted
	rowUpdated
)

// change of table
// binlog changes are clustered into table changes
// the validator validates changes of table-grain at a time
type tableChange struct {
	table *validateTableInfo
	rows  map[string]*rowChange
}

// change of a row
type rowChange struct {
	pk         []string
	data       []interface{}
	theType    rowChangeType
	lastMeetTs int64 // the last meet timestamp(in seconds)
	retryCnt   int   // retry count
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

	L                  log.Logger
	fromDB             *conn.BaseDB
	fromDBConn         *dbconn.DBConn
	toDB               *conn.BaseDB
	toDBConn           *dbconn.DBConn
	timezone           *time.Location
	syncCfg            replication.BinlogSyncerConfig
	streamerController *StreamerController

	result        pb.ProcessResult
	batchRowCnt   int
	retryInterval time.Duration

	failedRowCnt       atomic.Int64
	accumulatedChanges map[string]*tableChange
	pendingRowCnt      atomic.Int64
	rowsEventChan      chan *replication.BinlogEvent // unbuffered is enough
	pendingChangeCh    chan map[string]*tableChange
	changeEventCount   []int
	validationTimer    *time.Timer
	tables             map[string]*validateTableInfo
	workerCnt          int
	pendingChangeChs   map[int]chan map[string]*tableChange // replace pendingChangeCh
	failedChangesMap   map[int]map[string]*tableChange      // replace failedChanges

	// such as table without primary key
	unsupportedTable map[string]string
}

func NewContinuousDataValidator(cfg *config.SubTaskConfig, syncerObj *Syncer) *DataValidator {
	c := &DataValidator{
		cfg:     cfg,
		syncer:  syncerObj,
		stage:   pb.Stage_Stopped,
		errChan: make(chan error, 1),
	}
	c.L = log.With(zap.String("task", cfg.Name), zap.String("unit", "continuous validator"))

	c.workerCnt = defaultWorkerCnt
	c.pendingChangeChs = make(map[int]chan map[string]*tableChange)
	c.failedChangesMap = make(map[int]map[string]*tableChange)
	for i := 0; i < c.workerCnt; i++ {
		c.pendingChangeChs[i] = make(chan map[string]*tableChange)
		c.failedChangesMap[i] = make(map[string]*tableChange)
	}
	c.batchRowCnt = defaultBatchRowCount
	c.validationTimer = time.NewTimer(validationInterval)
	c.rowsEventChan = make(chan *replication.BinlogEvent)
	c.pendingChangeCh = make(chan map[string]*tableChange)
	c.tables = make(map[string]*validateTableInfo)
	c.changeEventCount = make([]int, 4)
	c.accumulatedChanges = make(map[string]*tableChange)
	c.retryInterval = retryInterval

	return c
}

func (v *DataValidator) initialize() error {
	newCtx, cancelFunc := context.WithTimeout(v.ctx, unit.DefaultInitTimeout)
	defer cancelFunc()
	tctx := tcontext.NewContext(newCtx, v.L)

	var err error
	defer func() {
		if err == nil {
			return
		}
		if v.fromDB != nil {
			v.fromDB.Close()
		}
		if v.toDB != nil {
			v.toDB.Close()
		}
	}()

	dbCfg := v.cfg.From
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout)
	var fromDBConns, toDBConns []*dbconn.DBConn
	v.fromDB, fromDBConns, err = dbconn.CreateConns(tctx, v.cfg, &dbCfg, 1)
	if err != nil {
		return err
	}
	v.fromDBConn = fromDBConns[0]

	v.toDB, toDBConns, err = dbconn.CreateConns(tctx, v.cfg, &dbCfg, 1)
	if err != nil {
		return err
	}
	v.toDBConn = toDBConns[0]

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

	v.ctx, v.cancel = context.WithCancel(context.Background())

	if err := v.initialize(); err != nil {
		v.fillResult(err, false)
		return
	}

	if expect != pb.Stage_Running {
		return
	}

	v.wg.Add(1)
	go func() {
		defer v.wg.Done()
		v.doValidate()
	}()

	v.errProcessWg.Add(1)
	go v.errorProcessRoutine()

	v.stage = pb.Stage_Running
}

func (v *DataValidator) fillResult(err error, needLock bool) {
	if needLock {
		v.Lock()
		defer v.Unlock()
	}

	var errs []*pb.ProcessError
	if utils.IsContextCanceledError(err) {
		v.L.Info("filter out context cancelled error", log.ShortError(err))
	} else {
		errs = append(errs, unit.NewProcessError(err))
	}

	isCanceled := false
	select {
	case <-v.ctx.Done():
		isCanceled = true
	default:
	}

	v.result = pb.ProcessResult{
		IsCanceled: isCanceled,
		Errors:     errs,
	}
}

func (v *DataValidator) errorProcessRoutine() {
	v.errProcessWg.Done()
	for {
		select {
		case err := <-v.errChan:
			v.fillResult(err, true)

			if errors.Cause(err) != context.Canceled {
				// todo: need a better way to handle err(auto resuming on some error, etc.)
				v.stopInner()
			}
		case <-v.ctx.Done():
			return
		}
	}
}

func (v *DataValidator) waitSyncerSynced(loc *binlog.Location, event *replication.BinlogEvent) error {
	// TODO
	return nil
}

func (v *DataValidator) waitSyncerRunning() error {
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

// doValidate: runs in a separate goroutine
func (v *DataValidator) doValidate() {
	if err := v.waitSyncerRunning(); err != nil {
		v.errChan <- terror.Annotate(err, "failed to wait syncer running")
		return
	}

	tctx := tcontext.NewContext(v.ctx, v.L)
	// todo: syncer may change replication location(start from timestamp, sharding resync), how validator react?
	location := v.syncer.checkpoint.GlobalPoint()
	err := v.streamerController.Start(tctx, location)
	if err != nil {
		v.errChan <- terror.Annotate(err, "fail to start streamer controller")
		return
	}

	v.L.Info("start continuous validation")
	v.wg.Add(2)
	go v.rowsEventProcessRoutine()
	go v.validateTaskDispatchRoutine()

	var currLoc binlog.Location
	for {
		e, err := v.streamerController.GetEvent(tctx)
		if err != nil {
			v.errChan <- terror.Annotate(err, "fail to get binlog from stream controller")
			return
		}

		if !utils.IsFakeRotateEvent(e.Header) {
			// wait until syncer synced that event
			err := v.waitSyncerSynced(&currLoc, e)
			if err != nil {
				v.errChan <- terror.Annotate(err, "failed to wait syncer")
				return
			}
		}

		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			currLoc.Position.Name = string(ev.NextLogName)
			currLoc.Position.Pos = uint32(ev.Position)
		case *replication.QueryEvent:
			// TODO not processed now
			currLoc.Position.Pos = e.Header.LogPos
			err2 := currLoc.SetGTID(ev.GSet)
			if err2 != nil {
				v.errChan <- terror.Annotate(err2, "failed to set gtid")
				return
			}
		case *replication.RowsEvent:
			currLoc.Position.Pos = e.Header.LogPos
			select {
			case v.rowsEventChan <- e:
			case <-v.ctx.Done():
				return
			}
		case *replication.XIDEvent:
			currLoc.Position.Pos = e.Header.LogPos
			err2 := currLoc.SetGTID(ev.GSet)
			if err2 != nil {
				v.errChan <- terror.Annotate(err2, "failed to set gtid")
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

func (v *DataValidator) rowsEventProcessRoutine() {
	defer v.wg.Done()
	for {
		select {
		case <-v.ctx.Done():
			v.L.Debug("validator row event process unit done")
			return
		case e := <-v.rowsEventChan:
			if err := v.processEventRows(e.Header, e.Event.(*replication.RowsEvent)); err != nil {
				v.L.Warn("failed to process event: ", zap.Reflect("error", err))
				v.errChan <- terror.Annotate(err, "failed to process event")
				return
			}
		case <-v.validationTimer.C:
			rowCount := v.getRowCount(v.accumulatedChanges)
			if rowCount > 0 {
				v.pendingChangeCh <- v.accumulatedChanges
				v.accumulatedChanges = make(map[string]*tableChange)
			}
			v.validationTimer.Reset(validationInterval)
		}
	}
}

func (v *DataValidator) validateTableChangeWorkerRoutine(workerID int) {
	defer v.wg.Done()
	for {
		select {
		case change := <-v.pendingChangeChs[workerID]:
			// 1. validate table change
			// 2. update failed rows
			// 3. update pending row count
			// 4. update failed row cnt
			failed := v.validateTableChange(change)
			deltaFailed := v.updateFailedChangesByWorker(change, failed, workerID)
			v.pendingRowCnt.Sub(int64(v.getRowCount(change))) // atomic value
			v.failedRowCnt.Add(int64(deltaFailed))            // atomic value
		case <-v.ctx.Done():
			v.L.Debug("validator worker done", zap.Int("workerID", workerID))
			return
		case <-time.After(v.retryInterval):
			retryChange := v.failedChangesMap[workerID]
			failed := v.validateTableChange(retryChange)
			deltaFailed := v.updateFailedChangesByWorker(retryChange, failed, workerID)
			v.failedRowCnt.Add(int64(deltaFailed))
		}
	}
}

func (v *DataValidator) validateTaskDispatchRoutine() {
	defer v.wg.Done()
	// start workers here
	v.wg.Add(v.workerCnt)
	for i := 0; i < v.workerCnt; i++ {
		go v.validateTableChangeWorkerRoutine(i)
	}
	for {
		select {
		case change := <-v.pendingChangeCh:
			v.dispatchRowChange(change) // dispatch change to worker
		case <-v.ctx.Done():
			v.L.Debug("validator task dispatch done")
			return
		}
	}
}

func hashTablePk(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (v *DataValidator) dispatchRowChange(change map[string]*tableChange) {
	dispatchMap := make(map[int]map[string]*tableChange, 0)
	for tableName := range change {
		// for every table
		for _, curRowChange := range change[tableName].rows {
			// for every row in the table
			// 1. join primary key by '-'
			// 2. hash (tableName, primaryKey) to hashVal
			// 3. dispatch the row change to dispatchMap[hashVal][tableName]
			pk := strings.Join(curRowChange.pk, "-")
			hashKey := tableName + "," + pk
			hashVal := int(hashTablePk(hashKey)) % v.workerCnt
			if _, ok := dispatchMap[hashVal]; !ok {
				dispatchMap[hashVal] = make(map[string]*tableChange, 0)
			}
			if _, ok := dispatchMap[hashVal][tableName]; !ok {
				dispatchMap[hashVal][tableName] = &tableChange{
					table: change[tableName].table,
					rows:  make(map[string]*rowChange, 0),
				}
			}
			dispatchMap[hashVal][tableName].rows[pk] = curRowChange
		}
	}
	for hashVal := range dispatchMap {
		v.pendingChangeChs[hashVal] <- dispatchMap[hashVal]
	}
}

func (v *DataValidator) processEventRows(header *replication.EventHeader, ev *replication.RowsEvent) error {
	sourceTable := &filter.Table{
		Schema: string(ev.Table.Schema),
		Name:   string(ev.Table.Table),
	}
	fullTableName := sourceTable.String()
	if _, ok := v.unsupportedTable[fullTableName]; ok {
		return nil
	}

	targetTable := v.syncer.route(sourceTable)
	tableInfo, err := v.syncer.schemaTracker.GetTableInfo(sourceTable)
	if err != nil {
		if schema.IsTableNotExists(err) {
			// not a table need to sync
			return nil
		}
		return terror.Annotate(err, "failed to get table info")
	}

	columnMap := make(map[string]*model.ColumnInfo)
	for _, col := range tableInfo.Columns {
		columnMap[col.Name.O] = col
	}
	var primaryIdx *model.IndexInfo
	for _, idx := range tableInfo.Indices {
		if idx.Primary {
			primaryIdx = idx
		}
	}
	if primaryIdx == nil {
		// todo: for table without primary index, need to record in the failed table, will add it later together with checkpoint
		v.unsupportedTable[fullTableName] = "without primary key"
		return nil
	}

	table := &validateTableInfo{
		Schema:     sourceTable.Schema,
		Name:       sourceTable.Name,
		Info:       tableInfo,
		PrimaryKey: primaryIdx,
		ColumnMap:  columnMap,
		Target:     targetTable,
	}

	for _, cols := range ev.SkippedColumns {
		if len(cols) > 0 {
			err := errors.Errorf("unexpected skipped columns for table `%s`.`%s`", table.Schema, table.Name)
			return err
		}
	}

	changeType := getRowChangeType(header.EventType)
	v.changeEventCount[changeType]++

	pk := table.PrimaryKey
	pkIndices := make([]int, len(pk.Columns))
	for i, col := range pk.Columns {
		pkIndices[i] = table.ColumnMap[col.Name.O].Offset
	}

	rowCount := v.getRowCount(v.accumulatedChanges)
	change := v.accumulatedChanges[fullTableName]

	updateRowChange := func(key string, row *rowChange) {
		if val, ok := change.rows[key]; ok {
			val.data = row.data
			val.theType = row.theType
			val.lastMeetTs = row.lastMeetTs
			val.retryCnt = row.retryCnt
		} else {
			change.rows[key] = row
			rowCount++
			v.pendingRowCnt.Inc()
		}
	}

	step := 1
	if changeType == rowUpdated {
		step = 2
	}
	for i := 0; i < len(ev.Rows); i += step {
		if change == nil {
			// no change of this table
			change = &tableChange{
				table: table,
				rows:  make(map[string]*rowChange),
			}
			v.accumulatedChanges[fullTableName] = change
		}
		row := ev.Rows[i]
		pkValue := make([]string, len(pk.Columns))
		for _, idx := range pkIndices {
			pkValue[idx] = fmt.Sprintf("%v", row[idx])
		}
		key := strings.Join(pkValue, "-")

		if changeType == rowUpdated {
			afterRowChangeType := changeType
			afterRow := ev.Rows[i+1]
			afterPkValue := make([]string, len(pk.Columns))
			for _, idx := range pkIndices {
				afterPkValue[idx] = fmt.Sprintf("%v", afterRow[idx])
			}
			afterKey := strings.Join(afterPkValue, "-")
			if afterKey != key {
				// convert to delete and insert
				updateRowChange(key, &rowChange{
					pk:         pkValue,
					data:       row,
					theType:    rowDeleted,
					lastMeetTs: int64(header.Timestamp),
				})
				afterRowChangeType = rowInsert
			}
			updateRowChange(afterKey, &rowChange{
				pk:         afterPkValue,
				data:       afterRow,
				theType:    afterRowChangeType,
				lastMeetTs: int64(header.Timestamp),
			})
		} else {
			updateRowChange(key, &rowChange{
				pk:         pkValue,
				data:       row,
				theType:    changeType,
				lastMeetTs: int64(header.Timestamp),
			})
		}

		if rowCount >= v.batchRowCnt {
			v.pendingChangeCh <- v.accumulatedChanges
			v.accumulatedChanges = make(map[string]*tableChange)

			if !v.validationTimer.Stop() {
				<-v.validationTimer.C
			}
			v.validationTimer.Reset(validationInterval)

			rowCount = 0
			change = nil
		}
	}
	return nil
}

func (v *DataValidator) getRowCount(c map[string]*tableChange) int {
	res := 0
	for _, val := range c {
		res += len(val.rows)
	}
	return res
}

// getRowChangeType should be called only when the event type is RowsEvent
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

func (v *DataValidator) validateTableChange(tableChanges map[string]*tableChange) map[string]*tableChange {
	failedChanges := make(map[string]*tableChange)
	for k, val := range tableChanges {
		var insertUpdateChanges, deleteChanges []*rowChange
		for _, r := range val.rows {
			if r.theType == rowDeleted {
				deleteChanges = append(deleteChanges, r)
			} else {
				insertUpdateChanges = append(insertUpdateChanges, r)
			}
		}
		rows := make(map[string]*rowChange, 0)
		if len(insertUpdateChanges) > 0 {
			failedRows := v.validateChanges(val.table, insertUpdateChanges, false)
			for _, pk := range failedRows {
				key := strings.Join(pk, "-")
				rows[key] = val.rows[key]
			}
		}
		if len(deleteChanges) > 0 {
			failedRows := v.validateChanges(val.table, deleteChanges, true)
			for _, pk := range failedRows {
				key := strings.Join(pk, "-")
				rows[key] = val.rows[key]
			}
		}
		if len(rows) > 0 {
			failedChanges[k] = &tableChange{
				table: val.table,
				rows:  rows,
			}
		}
	}
	return failedChanges
}

func (v *DataValidator) validateChanges(table *validateTableInfo, rows []*rowChange, deleteChange bool) [][]string {
	pkValues := make([][]string, 0, len(rows))
	for _, r := range rows {
		pkValues = append(pkValues, r.pk)
	}
	cond := &Cond{Table: table, PkValues: pkValues}
	var failedRows [][]string
	var err error
	if deleteChange {
		failedRows, err = v.validateDeletedRows(cond)
	} else {
		failedRows, err = v.validateInsertAndUpdateRows(rows, cond)
	}
	if err != nil {
		v.L.Warn("fail to validate row changes of table", zap.Error(err))
		return [][]string{}
	}
	return failedRows
}

// remove previous failed rows related to current batch of rows
// This function updates the failed rows every time after validating
func (v *DataValidator) updateFailedChangesByWorker(all, failed map[string]*tableChange, workerID int) int {
	failedChanges := v.failedChangesMap[workerID]
	deltaFailed := 0
	for k, val := range all {
		// remove from all
		prevFailed := failedChanges[k]
		if prevFailed == nil {
			continue
		}
		for _, r := range val.rows {
			key := strings.Join(r.pk, "-")
			if _, ok := prevFailed.rows[key]; ok {
				delete(prevFailed.rows, key)
				deltaFailed--
			}
		}
	}
	for k, val := range failed {
		// add from failed
		prevFailed := failedChanges[k]
		if prevFailed == nil {
			prevFailed = &tableChange{
				table: val.table,
				rows:  make(map[string]*rowChange),
			}
			failedChanges[k] = prevFailed
		}

		for _, r := range val.rows {
			key := strings.Join(r.pk, "-")
			prevFailed.rows[key] = r
			deltaFailed++
		}
	}
	return deltaFailed
}

func (v *DataValidator) validateDeletedRows(cond *Cond) ([][]string, error) {
	downstreamRowsIterator, err := getRowsFrom(cond, v.toDBConn)
	if err != nil {
		return [][]string{}, err
	}
	defer downstreamRowsIterator.Close()

	var failedRows [][]string
	for {
		data, err := downstreamRowsIterator.Next()
		if err != nil {
			return nil, err
		}
		if data == nil {
			break
		}
		failedRows = append(failedRows, getPKValues(data, cond))
	}
	return failedRows, nil
}

func (v *DataValidator) validateInsertAndUpdateRows(rows []*rowChange, cond *Cond) ([][]string, error) {
	var failedRows [][]string
	var upstreamRowsIterator, downstreamRowsIterator RowDataIterator
	var err error
	upstreamRowsIterator, err = getRowChangeIterator(cond.Table, rows)
	if err != nil {
		return nil, err
	}
	defer upstreamRowsIterator.Close()
	downstreamRowsIterator, err = getRowsFrom(cond, v.toDBConn)
	if err != nil {
		return nil, err
	}
	defer downstreamRowsIterator.Close()

	var lastUpstreamData, lastDownstreamData map[string]*dbutil.ColumnData

	tableInfo := cond.Table.Info
	_, orderKeyCols := dbutil.SelectUniqueOrderKey(tableInfo)
	for {
		if lastUpstreamData == nil {
			lastUpstreamData, err = upstreamRowsIterator.Next()
			if err != nil {
				return nil, err
			}
		}

		if lastDownstreamData == nil {
			lastDownstreamData, err = downstreamRowsIterator.Next()
			if err != nil {
				return nil, err
			}
		}

		// may have deleted on upstream and haven't synced to downstream,
		// we mark this as success as we'll check the delete-event later
		// or downstream removed the pk and added more data by other clients, skip it.
		if lastUpstreamData == nil && lastDownstreamData != nil {
			v.L.Debug("more data on downstream, may come from other client, skip it")
			break
		}

		if lastDownstreamData == nil {
			// target lack some data, should insert the last source datas
			for lastUpstreamData != nil {
				failedRows = append(failedRows, getPKValues(lastUpstreamData, cond))

				lastUpstreamData, err = upstreamRowsIterator.Next()
				if err != nil {
					return nil, err
				}
			}
			break
		}

		eq, cmp, err := v.compareData(lastUpstreamData, lastDownstreamData, orderKeyCols, tableInfo.Columns)
		if err != nil {
			return nil, err
		}
		if eq {
			lastDownstreamData = nil
			lastUpstreamData = nil
			continue
		}

		switch cmp {
		case 1:
			// may have deleted on upstream and haven't synced to downstream,
			// we mark this as success as we'll check the delete-event later
			// or downstream removed the pk and added more data by other clients, skip it.
			v.L.Debug("more data on downstream, may come from other client, skip it", zap.Reflect("data", lastDownstreamData))
			lastDownstreamData = nil
		case -1:
			failedRows = append(failedRows, getPKValues(lastUpstreamData, cond))
			lastUpstreamData = nil
		case 0:
			failedRows = append(failedRows, getPKValues(lastUpstreamData, cond))
			lastUpstreamData = nil
			lastDownstreamData = nil
		}
	}
	return failedRows, nil
}

func getRowsFrom(cond *Cond, conn *dbconn.DBConn) (RowDataIterator, error) {
	tctx := tcontext.NewContext(context.Background(), log.L())
	fullTableName := dbutil.TableName(cond.Table.Schema, cond.Table.Name)
	orderKeys, _ := dbutil.SelectUniqueOrderKey(cond.Table.Info)
	columnNames := make([]string, 0, len(cond.Table.Info.Columns))
	for _, col := range cond.Table.ColumnMap {
		columnNames = append(columnNames, dbutil.ColumnName(col.Name.O))
	}
	columns := strings.Join(columnNames, ", ")
	rowsQuery := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM %s WHERE %s ORDER BY %s",
		columns, fullTableName, cond.GetWhere(), strings.Join(orderKeys, ","))
	rows, err := conn.QuerySQL(tctx, rowsQuery, cond.GetArgs()...)
	if err != nil {
		return nil, err
	}
	newRowDataIter := &RowDataIteratorImpl{
		Rows: rows,
	}
	return newRowDataIter, nil
}

func getRowChangeIterator(table *validateTableInfo, rows []*rowChange) (RowDataIterator, error) {
	sort.Slice(rows, func(i, j int) bool {
		left, right := rows[i], rows[j]
		for idx := range left.pk {
			if left.pk[idx] != right.pk[idx] {
				return left.pk[idx] < right.pk[idx]
			}
		}
		return false
	})
	it := &RowChangeIteratorImpl{}
	for _, r := range rows {
		colMap := make(map[string]*dbutil.ColumnData)
		for _, c := range table.Info.Columns {
			var colData []byte
			if r.data[c.Offset] != nil {
				colData = []byte(fmt.Sprintf("%v", r.data[c.Offset]))
			}
			colMap[c.Name.O] = &dbutil.ColumnData{
				Data:   colData,
				IsNull: r.data[c.Offset] == nil,
			}
		}
		it.Rows = append(it.Rows, colMap)
	}
	return it, nil
}

func getPKValues(data map[string]*dbutil.ColumnData, cond *Cond) []string {
	var pkValues []string
	for _, pkColumn := range cond.Table.PrimaryKey.Columns {
		// TODO primary key cannot be null, if we uses unique key should make sure all columns are not null
		pkValues = append(pkValues, string(data[pkColumn.Name.O].Data))
	}
	return pkValues
}

func (v *DataValidator) compareData(map1, map2 map[string]*dbutil.ColumnData, orderKeyCols, columns []*model.ColumnInfo) (equal bool, cmp int32, err error) {
	var (
		data1, data2 *dbutil.ColumnData
		str1, str2   string
		key          string
		ok           bool
	)

	equal = true

	defer func() {
		if equal || err != nil {
			return
		}

		if cmp == 0 {
			v.L.Warn("find different row", zap.String("column", key), zap.String("row1", rowToString(map1)), zap.String("row2", rowToString(map2)))
		} else if cmp > 0 {
			v.L.Warn("target had superfluous data", zap.String("row", rowToString(map2)))
		} else {
			v.L.Warn("target lack data", zap.String("row", rowToString(map1)))
		}
	}()

	for _, column := range columns {
		if data1, ok = map1[column.Name.O]; !ok {
			return false, 0, errors.Errorf("upstream doesn't have key %s", key)
		}
		if data2, ok = map2[column.Name.O]; !ok {
			return false, 0, errors.Errorf("downstream doesn't have key %s", key)
		}
		str1 = string(data1.Data)
		str2 = string(data2.Data)
		if column.FieldType.Tp == tidbmysql.TypeFloat || column.FieldType.Tp == tidbmysql.TypeDouble {
			if data1.IsNull == data2.IsNull && data1.IsNull {
				continue
			}

			num1, err1 := strconv.ParseFloat(str1, 64)
			num2, err2 := strconv.ParseFloat(str2, 64)
			if err1 != nil || err2 != nil {
				err = errors.Errorf("convert %s, %s to float failed, err1: %v, err2: %v", str1, str2, err1, err2)
				return
			}
			if math.Abs(num1-num2) <= 1e-6 {
				continue
			}
		} else {
			if (str1 == str2) && (data1.IsNull == data2.IsNull) {
				continue
			}
		}

		equal = false
		break

	}
	if equal {
		return
	}

	// Not Equal. Compare orderkeycolumns.
	for _, col := range orderKeyCols {
		if data1, ok = map1[col.Name.O]; !ok {
			err = errors.Errorf("upstream doesn't have key %s", col.Name.O)
			return
		}
		if data2, ok = map2[col.Name.O]; !ok {
			err = errors.Errorf("downstream doesn't have key %s", col.Name.O)
			return
		}

		if NeedQuotes(col.FieldType.Tp) {
			strData1 := string(data1.Data)
			strData2 := string(data2.Data)

			if len(strData1) == len(strData2) && strData1 == strData2 {
				continue
			}

			if strData1 < strData2 {
				cmp = -1
			} else {
				cmp = 1
			}
			break
		} else if data1.IsNull || data2.IsNull {
			if data1.IsNull && data2.IsNull {
				continue
			}

			if data1.IsNull {
				cmp = -1
			} else {
				cmp = 1
			}
			break
		} else {
			num1, err1 := strconv.ParseFloat(string(data1.Data), 64)
			num2, err2 := strconv.ParseFloat(string(data2.Data), 64)
			if err1 != nil || err2 != nil {
				err = errors.Errorf("convert %s, %s to float failed, err1: %v, err2: %v", string(data1.Data), string(data2.Data), err1, err2)
				return
			}

			if num1 == num2 {
				continue
			}

			if num1 < num2 {
				cmp = -1
			} else {
				cmp = 1
			}
			break
		}
	}

	return
}

func NeedQuotes(tp byte) bool {
	return !(dbutil.IsNumberType(tp) || dbutil.IsFloatType(tp))
}

func rowToString(row map[string]*dbutil.ColumnData) string {
	var s strings.Builder
	s.WriteString("{ ")
	for key, val := range row {
		if val.IsNull {
			s.WriteString(fmt.Sprintf("%s: IsNull, ", key))
		} else {
			s.WriteString(fmt.Sprintf("%s: %s, ", key, val.Data))
		}
	}
	s.WriteString(" }")

	return s.String()
}
