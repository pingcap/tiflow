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
	"hash/fnv"
	"sort"

	"github.com/pingcap/errors"

	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	tidbmysql "github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
)

const (
	defaultDelay       = 5 * time.Second
	retryInterval      = 5 * time.Second
	validationInterval = time.Second
	batchRowCount      = 200
	defaultWorkerCnt   = 5
)

type rowChangeType int

// table info
type TableDiff struct {
	// Schema represents the database name.
	Schema string `json:"schema"`

	// Table represents the table name.
	Table string `json:"table"`

	// Info is the parser.TableInfo, include some meta infos for this table.
	// It used for TiDB/MySQL/MySQL Shard sources.
	Info *model.TableInfo `json:"info"`

	PrimaryKey *model.IndexInfo
	ColumnMap  map[string]*model.ColumnInfo
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
	table *TableDiff
	rows  map[string]*rowChange
}

// change of a row
type rowChange struct {
	pk         []string
	data       []interface{}
	theType    rowChangeType
	lastMeetTs int64 // the last meet timestamp(in seconds)
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

	stage  pb.Stage
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	L                  log.Logger
	fromDB             *conn.BaseDB
	fromDBConn         *dbconn.DBConn
	toDB               *conn.BaseDB
	toDBConn           *dbconn.DBConn
	timezone           *time.Location
	syncCfg            replication.BinlogSyncerConfig
	streamerController *StreamerController

	result pb.ProcessResult

	failedRowCnt       atomic.Int64
	accumulatedChanges map[string]*tableChange
	pendingRowCnt      atomic.Int64
	rowsEventChan      chan *replication.BinlogEvent // unbuffered is enough
	pendingChangeCh    chan map[string]*tableChange
	changeEventCount   []int
	validationTimer    *time.Timer
	diffTables         map[string]*TableDiff
	workerCnt          int
	pendingChangeChs   map[int]chan map[string]*tableChange // replace pendingChangeCh
	failedChangesMap   map[int]map[string]*tableChange      // replace failedChanges
}

func NewContinuousDataValidator(cfg *config.SubTaskConfig, syncerObj *Syncer) *DataValidator {
	c := &DataValidator{
		cfg:    cfg,
		syncer: syncerObj,
		stage:  pb.Stage_Stopped,
	}
	c.L = log.With(zap.String("task", cfg.Name), zap.String("unit", "continuous validator"))
	return c
}

func (v *DataValidator) initialize() error {
	newCtx, cancelFunc := context.WithTimeout(v.ctx, unit.DefaultInitTimeout)
	defer cancelFunc()
	tctx := tcontext.NewContext(newCtx, v.L)

	var err error
	defer func() {
		if err != nil && v.fromDB != nil {
			v.fromDB.Close()
		}
	}()

	dbCfg := v.cfg.From
	dbCfg.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxDMLConnectionTimeout)
	// v.fromDB, err = dbconn.CreateBaseDB(&dbCfg)
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

	v.workerCnt = defaultWorkerCnt
	for i := 0; i < v.workerCnt; i++ {
		v.pendingChangeChs[i] = make(chan map[string]*tableChange)
		v.failedChangesMap[i] = make(map[string]*tableChange)
	}
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

func (v *DataValidator) doValidate() {
	tctx := tcontext.NewContext(v.ctx, v.L)
	err := v.streamerController.Start(tctx, lastLocation)
	if err != nil {
		v.fillResult(terror.Annotate(err, "fail to restart streamer controller"), true)
		return
	}

	v.L.Info("start continuous validation")
	v.wg.Add(2)
	go v.rowsEventProcessRoutine()
	go v.validateTaskDispatchRoutine()
	var latestPos mysql.Position
	for {
		e, err := v.streamerController.GetEvent(tctx)
		if err != nil {
			v.L.Fatal("getting event from streamer controller failed")
			return
		}
		// todo: configuring the time or use checkpoint
		eventTime := time.Unix(int64(e.Header.Timestamp), 0)
		lag := time.Since(eventTime)
		if lag < defaultDelay {
			time.Sleep(defaultDelay - lag)
		}

		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			latestPos.Name = string(ev.NextLogName)
		case *replication.QueryEvent:
			// TODO not processed now
		case *replication.RowsEvent:
			select {
			case v.rowsEventChan <- e:
			case <-v.ctx.Done():
				return
			}
		}
		latestPos.Pos = e.Header.LogPos
	}
}

func (v *DataValidator) Stop() {
	v.Lock()
	defer v.Unlock()
	if v.stage != pb.Stage_Running {
		v.L.Warn("not started")
		return
	}

	v.streamerController.Close()
	v.fromDB.Close()
	v.toDB.Close()

	if v.cancel != nil {
		v.cancel()
	}
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
	v.wg.Done()
	for {
		select {
		case <-v.ctx.Done():
			return
		case e := <-v.rowsEventChan:
			if err := v.processEventRows(e.Header, e.Event.(*replication.RowsEvent)); err != nil {
				v.L.Warn("failed to process event: ", zap.Reflect("error", err))
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

func (v *DataValidator) workerValidateTableChange(workerID int) {
	v.wg.Done()
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
			return
		case <-time.After(retryInterval):
			retryChange := v.failedChangesMap[workerID]
			failed := v.validateTableChange(retryChange)
			deltaFailed := v.updateFailedChangesByWorker(retryChange, failed, workerID)
			v.failedRowCnt.Add(int64(deltaFailed))
		}
	}
}

func (v *DataValidator) validateTaskDispatchRoutine() {
	v.wg.Done()
	// start workers here
	v.wg.Add(v.workerCnt)
	for i := 0; i < v.workerCnt; i++ {
		go v.workerValidateTableChange(i)
	}
	for {
		select {
		case change := <-v.pendingChangeCh:
			v.dispatchRowChange(change) // dispatch change to worker
		case <-v.ctx.Done():
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
	schemaName, tableName := string(ev.Table.Schema), string(ev.Table.Table)
	fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
	var table *TableDiff
	if _, ok := v.diffTables[fullTableName]; !ok {
		// table not found in cache
		// try getting table info from the upstream
		tctx := tcontext.NewContext(v.ctx, v.L)
		fullTableQueryName := fmt.Sprintf("`%s`.`%s`", schemaName, tableName)
		createSQL, err := dbconn.GetTableCreateSQL(tctx, v.fromDBConn, fullTableQueryName)
		if err != nil {
			// get create table stmt failed
			return err
		}
		parser, err := utils.GetParser(v.ctx, v.fromDB.DB)
		if err != nil {
			return err
		}
		tableInfo, err := dbutil.GetTableInfoBySQL(createSQL, parser)
		if err != nil {
			return err
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
		v.diffTables[fullTableName] = &TableDiff{
			Schema:     schemaName,
			Table:      tableName,
			Info:       tableInfo,
			PrimaryKey: primaryIdx,
			ColumnMap:  columnMap,
		}
		table = v.diffTables[fullTableName]
	}
	if table == nil {
		return nil
	}
	if table.PrimaryKey == nil {
		errMsg := fmt.Sprintf("no primary key for %s.%s", table.Schema, table.Table)
		return errors.New(errMsg)
	}
	for _, cols := range ev.SkippedColumns {
		if len(cols) > 0 {
			return errors.New("")
		}
	}
	changeType := getRowChangeType(header.EventType)
	if changeType == rowInvalidChange {
		v.L.Info("ignoring unrecognized event", zap.Reflect("event header", header))
		return nil
	}
	v.changeEventCount[changeType]++

	init, step := 0, 1
	if changeType == rowUpdated {
		init, step = 1, 2
	}
	pk := table.PrimaryKey
	pkIndices := make([]int, len(pk.Columns))
	for i, col := range pk.Columns {
		pkIndices[i] = table.ColumnMap[col.Name.O].Offset
	}

	rowCount := v.getRowCount(v.accumulatedChanges)
	change := v.accumulatedChanges[fullTableName]
	for i := init; i < len(ev.Rows); i += step {
		row := ev.Rows[i]
		pkValue := make([]string, len(pk.Columns))
		for _, idx := range pkIndices {
			pkValue[idx] = fmt.Sprintf("%v", row[idx])
		}

		if change == nil {
			// no change of this table
			change = &tableChange{
				table: table,
				rows:  make(map[string]*rowChange),
			}
			v.accumulatedChanges[fullTableName] = change
		}
		key := strings.Join(pkValue, "-")
		val, ok := change.rows[key]
		if !ok {
			// this row hasn't been changed before
			val = &rowChange{pk: pkValue}
			change.rows[key] = val
			rowCount++
			v.pendingRowCnt.Inc()
		}
		val.data = row
		val.theType = changeType
		val.lastMeetTs = int64(header.Timestamp)

		if rowCount >= batchRowCount {
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

func getRowChangeType(t replication.EventType) rowChangeType {
	switch t {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return rowInsert
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return rowUpdated
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return rowDeleted
	default:
		return rowInvalidChange
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

func (v *DataValidator) validateChanges(table *TableDiff, rows []*rowChange, deleteChange bool) [][]string {
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
		panic(err)
	}
	return failedRows
}

// remove previous failed rows related to current batch of rows
// e.g. Assuming that one row was modified twice and successfully migrated to downstream.
// the validator might get false negative result when validating the first update binlog record
// but it must finally get true positive after validating the second update record.
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
			delete(prevFailed.rows, key)
			deltaFailed--
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
		// todo: cancel all routine
		return nil, errors.Trace(err)
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
		return nil, errors.New("")
	}
	defer upstreamRowsIterator.Close()
	downstreamRowsIterator, err = getRowsFrom(cond, v.toDBConn)
	if err != nil {
		// todo: cancel all routine
		return nil, errors.New("")
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
			return nil, errors.New("")
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
	fullTableName := dbutil.TableName(cond.Table.Schema, cond.Table.Table)
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

func getRowChangeIterator(table *TableDiff, rows []*rowChange) (RowDataIterator, error) {
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
			return false, 0, errors.Errorf("upstream don't have key %s", key)
		}
		if data2, ok = map2[column.Name.O]; !ok {
			return false, 0, errors.Errorf("downstream don't have key %s", key)
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
			err = errors.Errorf("don't have key %s", col.Name.O)
			return
		}
		if data2, ok = map2[col.Name.O]; !ok {
			err = errors.Errorf("don't have key %s", col.Name.O)
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
