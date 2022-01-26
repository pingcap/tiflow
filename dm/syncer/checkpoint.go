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
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	"github.com/pingcap/tiflow/dm/pkg/dumpling"
	fr "github.com/pingcap/tiflow/dm/pkg/func-rollback"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/parser/model"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

/*
variants about checkpoint:
1. update global checkpoint for DDL/XID event from any stream (global and sharding streaming)
2. update table checkpoint for DDL/DML event from any stream (global and sharding streaming)
3. position of global/table checkpoint increases monotonically
4. global checkpoint <= min checkpoint of all unsolved sharding tables
5. max checkpoint of all tables >= global checkpoint
*/

var (
	globalCpSchema       = "" // global checkpoint's cp_schema
	globalCpTable        = "" // global checkpoint's cp_table
	maxCheckPointTimeout = "1m"
	batchFlushPoints     = 100
)

type tablePoint struct {
	location binlog.Location
	ti       *model.TableInfo
}

func (b *tablePoint) String() string {
	return fmt.Sprintf("location(%v), tableInfo(ID: %d, Name:%s, ColNum: %d, IdxNum: %d, PKIsHandle: %t)", b.location, b.ti.ID, b.ti.Name, len(b.ti.Columns), len(b.ti.Indices), b.ti.PKIsHandle)
}

type binlogPoint struct {
	sync.RWMutex

	savedPoint   tablePoint
	flushedPoint tablePoint // point which flushed persistently
	enableGTID   bool
}

func newBinlogPoint(location, flushedLocation binlog.Location, ti, flushedTI *model.TableInfo, enableGTID bool) *binlogPoint {
	return &binlogPoint{
		savedPoint: tablePoint{
			location: location,
			ti:       ti,
		},
		flushedPoint: tablePoint{
			location: flushedLocation,
			ti:       flushedTI,
		},
		enableGTID: enableGTID,
	}
}

func (b *binlogPoint) save(location binlog.Location, ti *model.TableInfo) error {
	b.Lock()
	defer b.Unlock()

	if binlog.CompareLocation(location, b.savedPoint.location, b.enableGTID) < 0 {
		// support to save equal location, but not older location
		return terror.ErrCheckpointSaveInvalidPos.Generate(location, b.savedPoint.location)
	}

	b.savedPoint.location = location
	b.savedPoint.ti = ti
	return nil
}

func (b *binlogPoint) flush() {
	b.flushBy(b.savedPoint)
}

func (b *binlogPoint) flushBy(tp tablePoint) {
	b.Lock()
	defer b.Unlock()
	b.flushedPoint = tp
}

func (b *binlogPoint) rollback(schemaTracker *schema.Tracker, schema string) (isSchemaChanged bool) {
	b.Lock()
	defer b.Unlock()

	// set suffix to 0 when we meet error
	b.flushedPoint.location.ResetSuffix()
	b.savedPoint.location = b.flushedPoint.location
	if b.savedPoint.ti == nil {
		return // for global checkpoint, no need to rollback the schema.
	}

	// NOTE: no `Equal` function for `model.TableInfo` exists now, so we compare `pointer` directly,
	// and after a new DDL applied to the schema, the returned pointer of `model.TableInfo` changed now.
	trackedTi, _ := schemaTracker.GetTableInfo(&filter.Table{Schema: schema, Name: b.savedPoint.ti.Name.O}) // ignore the returned error, only compare `trackerTi` is enough.
	// may three versions of schema exist:
	// - the one tracked in the TiDB-with-mockTiKV.
	// - the one in the checkpoint but not flushed.
	// - the one in the checkpoint and flushed.
	// if any of them are not equal, then we rollback them:
	// - set the one in the checkpoint but not flushed to the one flushed.
	// - set the one tracked to the one in the checkpoint by the caller of this method (both flushed and not flushed are the same now)
	if isSchemaChanged = (trackedTi != b.savedPoint.ti) || (b.savedPoint.ti != b.flushedPoint.ti); isSchemaChanged {
		b.savedPoint.ti = b.flushedPoint.ti
	}
	return
}

func (b *binlogPoint) outOfDate() bool {
	return b.outOfDateBy(b.savedPoint.location)
}

func (b *binlogPoint) outOfDateBy(pos binlog.Location) bool {
	b.RLock()
	defer b.RUnlock()

	return binlog.CompareLocation(pos, b.flushedPoint.location, b.enableGTID) > 0
}

// MySQLLocation returns point as binlog.Location.
func (b *binlogPoint) MySQLLocation() binlog.Location {
	b.RLock()
	defer b.RUnlock()
	return b.savedPoint.location
}

// FlushedMySQLLocation returns flushed point as binlog.Location.
func (b *binlogPoint) FlushedMySQLLocation() binlog.Location {
	b.RLock()
	defer b.RUnlock()
	return b.flushedPoint.location
}

// TableInfo returns the table schema associated at the current binlog position.
func (b *binlogPoint) TableInfo() *model.TableInfo {
	b.RLock()
	defer b.RUnlock()
	return b.savedPoint.ti
}

func (b *binlogPoint) String() string {
	b.RLock()
	defer b.RUnlock()

	return fmt.Sprintf("%v(flushed %v)", b.savedPoint, b.flushedPoint)
}

// SnapshotInfo contains:
// - checkpoint snapshot id, it's for retrieving checkpoint snapshot in flush phase
// - global checkpoint position, it's for updating current active relay log after checkpoint flush.
type SnapshotInfo struct {
	// the snapshot id
	id int
	// global checkpoint position.
	globalPos binlog.Location
}

// CheckPoint represents checkpoints status for syncer
// including global binlog's checkpoint and every table's checkpoint
// when save checkpoint, we must differ saving in memory from saving (flushing) to DB (or file) permanently
// for sharding merging, we must save checkpoint in memory to support skip when re-syncing for the special streamer
// but before all DDLs for a sharding group to be synced and executed, we should not save checkpoint permanently
// because, when restarting to continue the sync, all sharding DDLs must try-sync again.
type CheckPoint interface {
	// Init initializes the CheckPoint
	Init(tctx *tcontext.Context) error

	// Close closes the CheckPoint
	Close()

	// ResetConn resets database connections owned by the Checkpoint
	ResetConn(tctx *tcontext.Context) error

	// Clear clears all checkpoints
	Clear(tctx *tcontext.Context) error

	// Load loads all checkpoints saved by CheckPoint
	Load(tctx *tcontext.Context) error

	// LoadMeta loads checkpoints from meta config item or file
	LoadMeta() error

	// SaveTablePoint saves checkpoint for specified table in memory
	SaveTablePoint(table *filter.Table, point binlog.Location, ti *model.TableInfo)

	// DeleteTablePoint deletes checkpoint for specified table in memory and storage
	DeleteTablePoint(tctx *tcontext.Context, table *filter.Table) error

	// DeleteSchemaPoint deletes checkpoint for specified schema
	DeleteSchemaPoint(tctx *tcontext.Context, sourceSchema string) error

	// IsOlderThanTablePoint checks whether job's checkpoint is older than previous saved checkpoint
	IsOlderThanTablePoint(table *filter.Table, point binlog.Location, isDDL bool) bool

	// SaveGlobalPoint saves the global binlog stream's checkpoint
	// corresponding to Meta.Save
	SaveGlobalPoint(point binlog.Location)

	// Snapshot make a snapshot of current checkpoint
	Snapshot(isSyncFlush bool) *SnapshotInfo

	// FlushGlobalPointsExcept flushes the global checkpoint and tables'
	// checkpoints except exceptTables, it also flushes SQLs with Args providing
	// by extraSQLs and extraArgs. Currently extraSQLs contain shard meta only.
	// @exceptTables: [[schema, table]... ]
	// corresponding to Meta.Flush
	FlushPointsExcept(tctx *tcontext.Context, snapshotID int, exceptTables []*filter.Table, extraSQLs []string, extraArgs [][]interface{}) error

	// FlushPointsWithTableInfos flushed the table points with given table infos
	FlushPointsWithTableInfos(tctx *tcontext.Context, tables []*filter.Table, tis []*model.TableInfo) error

	// FlushSafeModeExitPoint flushed the global checkpoint's with given table info
	FlushSafeModeExitPoint(tctx *tcontext.Context) error

	// GlobalPoint returns the global binlog stream's checkpoint
	// corresponding to Meta.Pos and Meta.GTID
	GlobalPoint() binlog.Location

	// GlobalPointSaveTime return the global point saved time, used for test only
	GlobalPointSaveTime() time.Time

	// SaveSafeModeExitPoint saves the pointer to location which indicates safe mode exit
	// this location is used when dump unit can't assure consistency
	SaveSafeModeExitPoint(point *binlog.Location)

	// SafeModeExitPoint returns the location where safe mode could safely turn off after
	SafeModeExitPoint() *binlog.Location

	// TablePoint returns all table's stream checkpoint
	TablePoint() map[string]map[string]binlog.Location

	// FlushedGlobalPoint returns the flushed global binlog stream's checkpoint
	// corresponding to to Meta.Pos and gtid
	FlushedGlobalPoint() binlog.Location

	// CheckGlobalPoint checks whether we should save global checkpoint
	// corresponding to Meta.Check
	CheckGlobalPoint() bool

	// CheckLastSnapshotCreationTime checks whether we should async flush checkpoint since last time async flush
	CheckLastSnapshotCreationTime() bool

	// GetFlushedTableInfo gets flushed table info
	// use for lazy create table in schemaTracker
	GetFlushedTableInfo(table *filter.Table) *model.TableInfo

	// Rollback rolls global checkpoint and all table checkpoints back to flushed checkpoints
	Rollback(schemaTracker *schema.Tracker)

	// String return text of global position
	String() string

	// CheckAndUpdate check the checkpoint data consistency and try to fix them if possible
	CheckAndUpdate(ctx context.Context, schemas map[string]string, tables map[string]map[string]string) error
}

// remoteCheckpointSnapshot contains info needed to flush checkpoint to downstream by FlushPointsExcept method.
type remoteCheckpointSnapshot struct {
	id                  int
	globalPoint         *tablePoint
	globalPointSaveTime time.Time
	points              map[string]map[string]tablePoint
}

// RemoteCheckPoint implements CheckPoint
// which using target database to store info
// NOTE: now we sync from relay log, so not add GTID support yet
// it's not thread-safe.
type RemoteCheckPoint struct {
	sync.RWMutex

	cfg *config.SubTaskConfig

	db        *conn.BaseDB
	dbConn    *dbconn.DBConn
	tableName string // qualified table name: schema is set through task config, table is task name
	id        string // checkpoint ID, now it is `source-id`

	// source-schema -> source-table -> checkpoint
	// used to filter the synced binlog when re-syncing for sharding group
	points map[string]map[string]*binlogPoint

	// global binlog checkpoint
	// after restarted, we can continue to re-sync from this point
	// if there are sharding groups waiting for DDL syncing or in DMLs re-syncing
	//   this global checkpoint is min(next-binlog-pos, min(all-syncing-sharding-group-first-pos))
	// else
	//   this global checkpoint is next-binlog-pos
	globalPoint              *binlogPoint
	globalPointSaveTime      time.Time
	lastSnapshotCreationTime time.Time

	// safeModeExitPoint is set in RemoteCheckPoint.Load (from downstream DB) and LoadMeta (from metadata file).
	// it is unset (set nil) in RemoteCheckPoint.Clear, and when syncer's stream pass its location.
	// it is flushed along with globalPoint which called by Syncer.flushCheckPoints.
	// this variable is mainly used to decide status of safe mode, so it is access when
	//  - init safe mode
	//  - checking in sync and if passed, unset it
	safeModeExitPoint          *binlog.Location
	needFlushSafeModeExitPoint atomic.Bool

	logCtx *tcontext.Context
	// these fields are used for async flush checkpoint
	snapshots   []*remoteCheckpointSnapshot
	snapshotSeq int
}

// NewRemoteCheckPoint creates a new RemoteCheckPoint.
func NewRemoteCheckPoint(tctx *tcontext.Context, cfg *config.SubTaskConfig, id string) CheckPoint {
	cp := &RemoteCheckPoint{
		cfg:         cfg,
		tableName:   dbutil.TableName(cfg.MetaSchema, cputil.SyncerCheckpoint(cfg.Name)),
		id:          id,
		points:      make(map[string]map[string]*binlogPoint),
		globalPoint: newBinlogPoint(binlog.NewLocation(cfg.Flavor), binlog.NewLocation(cfg.Flavor), nil, nil, cfg.EnableGTID),
		logCtx:      tcontext.Background().WithLogger(tctx.L().WithFields(zap.String("component", "remote checkpoint"))),
		snapshots:   make([]*remoteCheckpointSnapshot, 0),
		snapshotSeq: 0,
	}

	return cp
}

// Snapshot make a snapshot of checkpoint and return the snapshot info.
func (cp *RemoteCheckPoint) Snapshot(isSyncFlush bool) *SnapshotInfo {
	cp.RLock()
	defer cp.RUnlock()

	// make snapshot is visit in single thread, so depend on rlock should be enough
	cp.snapshotSeq++
	id := cp.snapshotSeq

	tableCheckPoints := make(map[string]map[string]tablePoint, len(cp.points))
	for s, tableCps := range cp.points {
		tableCpSnapshots := make(map[string]tablePoint)
		for tbl, point := range tableCps {
			if point.outOfDate() {
				tableCpSnapshots[tbl] = point.savedPoint
			}
		}
		if len(tableCpSnapshots) > 0 {
			tableCheckPoints[s] = tableCpSnapshots
		}
	}

	flushGlobalPoint := cp.globalPoint.outOfDate() || cp.globalPointSaveTime.IsZero() || (isSyncFlush && cp.needFlushSafeModeExitPoint.Load())

	// if there is no change on both table points and global point, just return an empty snapshot
	if len(tableCheckPoints) == 0 && !flushGlobalPoint {
		return nil
	}

	snapshot := &remoteCheckpointSnapshot{
		id:     id,
		points: tableCheckPoints,
	}

	globalPoint := &tablePoint{
		location: cp.globalPoint.savedPoint.location.Clone(),
		ti:       cp.globalPoint.savedPoint.ti,
	}
	if flushGlobalPoint {
		snapshot.globalPoint = globalPoint
		snapshot.globalPointSaveTime = time.Now()
	}

	cp.snapshots = append(cp.snapshots, snapshot)
	cp.lastSnapshotCreationTime = time.Now()
	return &SnapshotInfo{
		id:        id,
		globalPos: globalPoint.location,
	}
}

// Init implements CheckPoint.Init.
func (cp *RemoteCheckPoint) Init(tctx *tcontext.Context) (err error) {
	var db *conn.BaseDB
	var dbConns []*dbconn.DBConn

	rollbackHolder := fr.NewRollbackHolder("syncer")
	defer func() {
		if err != nil {
			rollbackHolder.RollbackReverseOrder()
		}
	}()

	checkPointDB := cp.cfg.To
	checkPointDB.RawDBCfg = config.DefaultRawDBConfig().SetReadTimeout(maxCheckPointTimeout)
	db, dbConns, err = dbconn.CreateConns(tctx, cp.cfg, &checkPointDB, 1)
	if err != nil {
		return
	}
	cp.db = db
	cp.dbConn = dbConns[0]
	rollbackHolder.Add(fr.FuncRollback{Name: "CloseRemoteCheckPoint", Fn: cp.Close})

	err = cp.prepare(tctx)

	return
}

// Close implements CheckPoint.Close.
func (cp *RemoteCheckPoint) Close() {
	dbconn.CloseBaseDB(cp.logCtx, cp.db)
}

// ResetConn implements CheckPoint.ResetConn.
func (cp *RemoteCheckPoint) ResetConn(tctx *tcontext.Context) error {
	return cp.dbConn.ResetConn(tctx)
}

// Clear implements CheckPoint.Clear.
func (cp *RemoteCheckPoint) Clear(tctx *tcontext.Context) error {
	cp.Lock()
	defer cp.Unlock()

	// delete all checkpoints
	// use a new context apart from syncer, to make sure when syncer call `cancel` checkpoint could update
	tctx2, cancel := tctx.WithContext(context.Background()).WithTimeout(maxDMLConnectionDuration)
	defer cancel()
	_, err := cp.dbConn.ExecuteSQL(
		tctx2,
		[]string{`DELETE FROM ` + cp.tableName + ` WHERE id = ?`},
		[]interface{}{cp.id},
	)
	if err != nil {
		return err
	}

	cp.globalPoint = newBinlogPoint(binlog.NewLocation(cp.cfg.Flavor), binlog.NewLocation(cp.cfg.Flavor), nil, nil, cp.cfg.EnableGTID)
	cp.globalPointSaveTime = time.Time{}
	cp.lastSnapshotCreationTime = time.Time{}
	cp.points = make(map[string]map[string]*binlogPoint)
	cp.snapshots = make([]*remoteCheckpointSnapshot, 0)
	cp.safeModeExitPoint = nil

	return nil
}

// SaveTablePoint implements CheckPoint.SaveTablePoint.
func (cp *RemoteCheckPoint) SaveTablePoint(table *filter.Table, point binlog.Location, ti *model.TableInfo) {
	cp.Lock()
	defer cp.Unlock()
	cp.saveTablePoint(table, point, ti)
}

// saveTablePoint saves single table's checkpoint without mutex.Lock.
func (cp *RemoteCheckPoint) saveTablePoint(sourceTable *filter.Table, location binlog.Location, ti *model.TableInfo) {
	if binlog.CompareLocation(cp.globalPoint.savedPoint.location, location, cp.cfg.EnableGTID) > 0 {
		panic(fmt.Sprintf("table checkpoint %+v less than global checkpoint %+v", location, cp.globalPoint))
	}

	// we save table checkpoint while we meet DDL or DML
	cp.logCtx.L().Debug("save table checkpoint", zap.Stringer("location", location), zap.Stringer("table", sourceTable))
	mSchema, ok := cp.points[sourceTable.Schema]
	if !ok {
		mSchema = make(map[string]*binlogPoint)
		cp.points[sourceTable.Schema] = mSchema
	}
	point, ok := mSchema[sourceTable.Name]
	if !ok {
		mSchema[sourceTable.Name] = newBinlogPoint(location, binlog.NewLocation(cp.cfg.Flavor), ti, nil, cp.cfg.EnableGTID)
	} else if err := point.save(location, ti); err != nil {
		cp.logCtx.L().Error("fail to save table point", zap.Stringer("table", sourceTable), log.ShortError(err))
	}
}

// SaveSafeModeExitPoint implements CheckPoint.SaveSafeModeExitPoint
// shouldn't call concurrently (only called before loop in Syncer.Run and in loop to reset).
func (cp *RemoteCheckPoint) SaveSafeModeExitPoint(point *binlog.Location) {
	if cp.safeModeExitPoint == nil || point == nil ||
		binlog.CompareLocation(*point, *cp.safeModeExitPoint, cp.cfg.EnableGTID) > 0 {
		cp.safeModeExitPoint = point
		cp.needFlushSafeModeExitPoint.Store(true)
	}
}

// SafeModeExitPoint implements CheckPoint.SafeModeExitPoint.
func (cp *RemoteCheckPoint) SafeModeExitPoint() *binlog.Location {
	return cp.safeModeExitPoint
}

// DeleteTablePoint implements CheckPoint.DeleteTablePoint.
func (cp *RemoteCheckPoint) DeleteTablePoint(tctx *tcontext.Context, table *filter.Table) error {
	cp.Lock()
	defer cp.Unlock()
	sourceSchema, sourceTable := table.Schema, table.Name
	mSchema, ok := cp.points[sourceSchema]
	if !ok {
		return nil
	}
	_, ok = mSchema[sourceTable]
	if !ok {
		return nil
	}

	// use a new context apart from syncer, to make sure when syncer call `cancel` checkpoint could update
	tctx2, cancel := tctx.WithContext(context.Background()).WithTimeout(maxDMLConnectionDuration)
	defer cancel()
	cp.logCtx.L().Info("delete table checkpoint", zap.String("schema", sourceSchema), zap.String("table", sourceTable))
	_, err := cp.dbConn.ExecuteSQL(
		tctx2,
		[]string{`DELETE FROM ` + cp.tableName + ` WHERE id = ? AND cp_schema = ? AND cp_table = ?`},
		[]interface{}{cp.id, sourceSchema, sourceTable},
	)
	if err != nil {
		return err
	}
	delete(mSchema, sourceTable)
	return nil
}

// DeleteSchemaPoint implements CheckPoint.DeleteSchemaPoint.
func (cp *RemoteCheckPoint) DeleteSchemaPoint(tctx *tcontext.Context, sourceSchema string) error {
	cp.Lock()
	defer cp.Unlock()
	_, ok := cp.points[sourceSchema]
	if !ok {
		return nil
	}

	// use a new context apart from syncer, to make sure when syncer call `cancel` checkpoint could update
	tctx2, cancel := tctx.WithContext(context.Background()).WithTimeout(maxDMLConnectionDuration)
	defer cancel()
	cp.logCtx.L().Info("delete schema checkpoint", zap.String("schema", sourceSchema))
	_, err := cp.dbConn.ExecuteSQL(
		tctx2,
		[]string{`DELETE FROM ` + cp.tableName + ` WHERE id = ? AND cp_schema = ?`},
		[]interface{}{cp.id, sourceSchema},
	)
	if err != nil {
		return err
	}
	delete(cp.points, sourceSchema)
	return nil
}

// IsOlderThanTablePoint implements CheckPoint.IsOlderThanTablePoint.
// This function is used to skip old binlog events. Table checkpoint is saved after dispatching a binlog event.
// - For GTID based and position based replication, DML handling is different. When using position based, each event has
//   unique position so we have confident to skip event which is <= table checkpoint. When using GTID based, there may
//   be more than one event with same GTID, so we can only skip event which is < table checkpoint.
// - DDL will not have unique position or GTID, so we can always skip events <= table checkpoint.
func (cp *RemoteCheckPoint) IsOlderThanTablePoint(table *filter.Table, location binlog.Location, isDDL bool) bool {
	cp.RLock()
	defer cp.RUnlock()
	sourceSchema, sourceTable := table.Schema, table.Name
	mSchema, ok := cp.points[sourceSchema]
	if !ok {
		return false
	}
	point, ok := mSchema[sourceTable]
	if !ok {
		return false
	}
	oldLocation := point.MySQLLocation()
	cp.logCtx.L().Debug("compare table location whether is newer", zap.Stringer("location", location), zap.Stringer("old location", oldLocation))

	if isDDL || !cp.cfg.EnableGTID {
		return binlog.CompareLocation(location, oldLocation, cp.cfg.EnableGTID) <= 0
	}
	return binlog.CompareLocation(location, oldLocation, cp.cfg.EnableGTID) < 0
}

// SaveGlobalPoint implements CheckPoint.SaveGlobalPoint.
func (cp *RemoteCheckPoint) SaveGlobalPoint(location binlog.Location) {
	cp.Lock()
	defer cp.Unlock()

	cp.logCtx.L().Debug("save global checkpoint", zap.Stringer("location", location))
	if err := cp.globalPoint.save(location, nil); err != nil {
		cp.logCtx.L().Error("fail to save global checkpoint", log.ShortError(err))
	}
}

// FlushPointsExcept implements CheckPoint.FlushSnapshotPointsExcept.
func (cp *RemoteCheckPoint) FlushPointsExcept(
	tctx *tcontext.Context,
	snapshotID int,
	exceptTables []*filter.Table,
	extraSQLs []string,
	extraArgs [][]interface{},
) error {
	cp.Lock()

	if len(cp.snapshots) == 0 || cp.snapshots[0].id != snapshotID {
		cp.logCtx.Logger.DPanic("snapshot not found", zap.Int("id", snapshotID))
	}
	snapshotCp := cp.snapshots[0]
	cp.snapshots = cp.snapshots[1:]

	// convert slice to map
	excepts := make(map[string]map[string]struct{})
	for _, schemaTable := range exceptTables {
		schema, table := schemaTable.Schema, schemaTable.Name
		m, ok := excepts[schema]
		if !ok {
			m = make(map[string]struct{})
			excepts[schema] = m
		}
		m[table] = struct{}{}
	}

	sqls := make([]string, 0, 100)
	args := make([][]interface{}, 0, 100)

	if snapshotCp.globalPoint != nil {
		locationG := snapshotCp.globalPoint.location
		sqlG, argG := cp.genUpdateSQL(globalCpSchema, globalCpTable, locationG, cp.safeModeExitPoint, nil, true)
		sqls = append(sqls, sqlG)
		args = append(args, argG)
	}

	type tableCpSnapshotTuple struct {
		tableCp         *binlogPoint // current table checkpoint location
		snapshotTableCP tablePoint   // table checkpoint snapshot location
	}

	points := make([]*tableCpSnapshotTuple, 0, 100)

	for schema, mSchema := range snapshotCp.points {
		schemaCp := cp.points[schema]
		for table, point := range mSchema {
			if _, ok1 := excepts[schema]; ok1 {
				if _, ok2 := excepts[schema][table]; ok2 {
					continue
				}
			}
			tableCP := schemaCp[table]
			if tableCP.outOfDateBy(point.location) {
				tiBytes, err := json.Marshal(point.ti)
				if err != nil {
					return terror.ErrSchemaTrackerCannotSerialize.Delegate(err, schema, table)
				}

				sql2, arg := cp.genUpdateSQL(schema, table, point.location, nil, tiBytes, false)
				sqls = append(sqls, sql2)
				args = append(args, arg)

				points = append(points, &tableCpSnapshotTuple{
					tableCp:         tableCP,
					snapshotTableCP: point,
				})
			}
		}
	}
	for i := range extraSQLs {
		sqls = append(sqls, extraSQLs[i])
		args = append(args, extraArgs[i])
	}

	cp.Unlock()

	// use a new context apart from syncer, to make sure when syncer call `cancel` checkpoint could update
	tctx2, cancel := tctx.WithContext(context.Background()).WithTimeout(maxDMLConnectionDuration)
	defer cancel()
	_, err := cp.dbConn.ExecuteSQL(tctx2, sqls, args...)
	if err != nil {
		return err
	}

	if snapshotCp.globalPoint != nil {
		cp.globalPoint.flushBy(*snapshotCp.globalPoint)
		cp.Lock()
		cp.globalPointSaveTime = snapshotCp.globalPointSaveTime
		cp.Unlock()
	}

	for _, point := range points {
		point.tableCp.flushBy(point.snapshotTableCP)
	}
	cp.needFlushSafeModeExitPoint.Store(false)
	return nil
}

// FlushPointsWithTableInfos implements CheckPoint.FlushPointsWithTableInfos.
func (cp *RemoteCheckPoint) FlushPointsWithTableInfos(tctx *tcontext.Context, tables []*filter.Table, tis []*model.TableInfo) error {
	cp.Lock()
	defer cp.Unlock()
	// should not happened
	if len(tables) != len(tis) {
		return errors.Errorf("the length of the tables is not equal to the length of the table infos, left: %d, right: %d", len(tables), len(tis))
	}

	for i := 0; i < len(tables); i += batchFlushPoints {
		end := i + batchFlushPoints
		if end > len(tables) {
			end = len(tables)
		}

		sqls := make([]string, 0, batchFlushPoints)
		args := make([][]interface{}, 0, batchFlushPoints)
		points := make([]*binlogPoint, 0, batchFlushPoints)
		for j := i; j < end; j++ {
			table := tables[j]
			ti := tis[j]
			sourceSchema, sourceTable := table.Schema, table.Name

			var point *binlogPoint
			// if point already in memory, use it
			if tablePoints, ok := cp.points[sourceSchema]; ok {
				if p, ok2 := tablePoints[sourceTable]; ok2 {
					point = p
				}
			}
			// create new point
			if point == nil {
				cp.saveTablePoint(table, cp.globalPoint.MySQLLocation(), ti)
				point = cp.points[sourceSchema][sourceTable]
			}
			tiBytes, err := json.Marshal(ti)
			if err != nil {
				return terror.ErrSchemaTrackerCannotSerialize.Delegate(err, sourceSchema, sourceTable)
			}
			location := point.MySQLLocation()
			sql, arg := cp.genUpdateSQL(sourceSchema, sourceTable, location, nil, tiBytes, false)
			sqls = append(sqls, sql)
			args = append(args, arg)
			points = append(points, point)
		}
		// use a new context apart from syncer, to make sure when syncer call `cancel` checkpoint could update
		tctx2, cancel := tctx.WithContext(context.Background()).WithTimeout(utils.DefaultDBTimeout)
		defer cancel()
		_, err := cp.dbConn.ExecuteSQL(tctx2, sqls, args...)
		if err != nil {
			return err
		}

		for _, point := range points {
			point.flush()
		}
	}
	return nil
}

// FlushSafeModeExitPoint implements CheckPoint.FlushSafeModeExitPoint.
func (cp *RemoteCheckPoint) FlushSafeModeExitPoint(tctx *tcontext.Context) error {
	cp.RLock()
	defer cp.RUnlock()

	sqls := make([]string, 1)
	args := make([][]interface{}, 1)

	// use FlushedGlobalPoint here to avoid update global checkpoint
	locationG := cp.FlushedGlobalPoint()
	sqls[0], args[0] = cp.genUpdateSQL(globalCpSchema, globalCpTable, locationG, cp.safeModeExitPoint, nil, true)

	// use a new context apart from syncer, to make sure when syncer call `cancel` checkpoint could update
	tctx2, cancel := tctx.WithContext(context.Background()).WithTimeout(maxDMLConnectionDuration)
	defer cancel()
	_, err := cp.dbConn.ExecuteSQL(tctx2, sqls, args...)
	if err != nil {
		return err
	}

	cp.needFlushSafeModeExitPoint.Store(false)
	return nil
}

// GlobalPoint implements CheckPoint.GlobalPoint.
func (cp *RemoteCheckPoint) GlobalPoint() binlog.Location {
	cp.RLock()
	defer cp.RUnlock()

	return cp.globalPoint.MySQLLocation()
}

// GlobalPointSaveTime implements CheckPoint.GlobalPointSaveTime.
func (cp *RemoteCheckPoint) GlobalPointSaveTime() time.Time {
	cp.RLock()
	defer cp.RUnlock()

	return cp.globalPointSaveTime
}

// TablePoint implements CheckPoint.TablePoint.
func (cp *RemoteCheckPoint) TablePoint() map[string]map[string]binlog.Location {
	cp.RLock()
	defer cp.RUnlock()

	tablePoint := make(map[string]map[string]binlog.Location)
	for schema, tables := range cp.points {
		tablePoint[schema] = make(map[string]binlog.Location)
		for table, point := range tables {
			tablePoint[schema][table] = point.MySQLLocation()
		}
	}
	return tablePoint
}

// FlushedGlobalPoint implements CheckPoint.FlushedGlobalPoint.
func (cp *RemoteCheckPoint) FlushedGlobalPoint() binlog.Location {
	cp.RLock()
	defer cp.RUnlock()

	return cp.globalPoint.FlushedMySQLLocation()
}

// String implements CheckPoint.String.
func (cp *RemoteCheckPoint) String() string {
	cp.RLock()
	defer cp.RUnlock()

	return cp.globalPoint.String()
}

// CheckGlobalPoint implements CheckPoint.CheckGlobalPoint.
func (cp *RemoteCheckPoint) CheckGlobalPoint() bool {
	cp.RLock()
	defer cp.RUnlock()
	return time.Since(cp.globalPointSaveTime) >= time.Duration(cp.cfg.CheckpointFlushInterval)*time.Second
}

// CheckLastSnapshotCreationTime implements CheckPoint.CheckLastSnapshotCreationTime.
func (cp *RemoteCheckPoint) CheckLastSnapshotCreationTime() bool {
	cp.RLock()
	defer cp.RUnlock()
	return time.Since(cp.lastSnapshotCreationTime) >= time.Duration(cp.cfg.CheckpointFlushInterval)*time.Second
}

// Rollback implements CheckPoint.Rollback.
func (cp *RemoteCheckPoint) Rollback(schemaTracker *schema.Tracker) {
	cp.RLock()
	defer cp.RUnlock()
	cp.globalPoint.rollback(schemaTracker, "")
	for schemaName, mSchema := range cp.points {
		for tableName, point := range mSchema {
			table := &filter.Table{
				Schema: schemaName,
				Name:   tableName,
			}
			logger := cp.logCtx.L().WithFields(zap.Stringer("table", table))
			logger.Debug("try to rollback checkpoint", log.WrapStringerField("checkpoint", point))
			from := point.MySQLLocation()
			if point.rollback(schemaTracker, schemaName) {
				logger.Info("rollback checkpoint", zap.Stringer("from", from), zap.Stringer("to", point.FlushedMySQLLocation()))
				// schema changed
				if err := schemaTracker.DropTable(table); err != nil {
					logger.Warn("failed to drop table from schema tracker", log.ShortError(err))
				}
				if point.savedPoint.ti != nil {
					// TODO: Figure out how to recover from errors.
					if err := schemaTracker.CreateSchemaIfNotExists(schemaName); err != nil {
						logger.Error("failed to rollback schema on schema tracker: cannot create schema", log.ShortError(err))
					}
					if err := schemaTracker.CreateTableIfNotExists(table, point.savedPoint.ti); err != nil {
						logger.Error("failed to rollback schema on schema tracker: cannot create table", log.ShortError(err))
					}
				}
			}
		}
	}

	// drop any tables in the tracker if no corresponding checkpoint exists.
	for _, schema := range schemaTracker.AllSchemas() {
		_, ok1 := cp.points[schema.Name.O]
		for _, table := range schema.Tables {
			var ok2 bool
			if ok1 {
				_, ok2 = cp.points[schema.Name.O][table.Name.O]
			}
			if !ok2 {
				err := schemaTracker.DropTable(&filter.Table{Schema: schema.Name.O, Name: table.Name.O})
				cp.logCtx.L().Info("drop table in schema tracker because no checkpoint exists", zap.String("schema", schema.Name.O), zap.String("table", table.Name.O), log.ShortError(err))
			}
		}
	}
}

func (cp *RemoteCheckPoint) prepare(tctx *tcontext.Context) error {
	if err := cp.createSchema(tctx); err != nil {
		return err
	}

	return cp.createTable(tctx)
}

func (cp *RemoteCheckPoint) createSchema(tctx *tcontext.Context) error {
	// TODO(lance6716): change ColumnName to IdentName or something
	sql2 := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", dbutil.ColumnName(cp.cfg.MetaSchema))
	args := make([]interface{}, 0)
	_, err := cp.dbConn.ExecuteSQL(tctx, []string{sql2}, [][]interface{}{args}...)
	cp.logCtx.L().Info("create checkpoint schema", zap.String("statement", sql2))
	return err
}

func (cp *RemoteCheckPoint) createTable(tctx *tcontext.Context) error {
	sqls := []string{
		`CREATE TABLE IF NOT EXISTS ` + cp.tableName + ` (
			id VARCHAR(32) NOT NULL,
			cp_schema VARCHAR(128) NOT NULL,
			cp_table VARCHAR(128) NOT NULL,
			binlog_name VARCHAR(128),
			binlog_pos INT UNSIGNED,
			binlog_gtid TEXT,
			exit_safe_binlog_name VARCHAR(128) DEFAULT '',
			exit_safe_binlog_pos INT UNSIGNED DEFAULT 0,
			exit_safe_binlog_gtid TEXT,
			table_info JSON NOT NULL,
			is_global BOOLEAN,
			create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
			update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uk_id_schema_table (id, cp_schema, cp_table)
		)`,
	}
	_, err := cp.dbConn.ExecuteSQL(tctx, sqls)
	cp.logCtx.L().Info("create checkpoint table", zap.Strings("statements", sqls))
	return err
}

// Load implements CheckPoint.Load.
func (cp *RemoteCheckPoint) Load(tctx *tcontext.Context) error {
	cp.Lock()
	defer cp.Unlock()

	query := `SELECT cp_schema, cp_table, binlog_name, binlog_pos, binlog_gtid, exit_safe_binlog_name, exit_safe_binlog_pos, exit_safe_binlog_gtid, table_info, is_global FROM ` + cp.tableName + ` WHERE id = ?`
	rows, err := cp.dbConn.QuerySQL(tctx, query, cp.id)
	defer func() {
		if rows != nil {
			rows.Close()
		}
	}()

	failpoint.Inject("LoadCheckpointFailed", func(val failpoint.Value) {
		err = tmysql.NewErr(uint16(val.(int)))
		log.L().Warn("Load failed", zap.String("failpoint", "LoadCheckpointFailed"), zap.Error(err))
	})

	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}

	// checkpoints in DB have higher priority
	// if don't want to use checkpoint in DB, set `remove-meta` to `true`
	var (
		cpSchema              string
		cpTable               string
		binlogName            string
		binlogPos             uint32
		binlogGTIDSet         sql.NullString
		exitSafeBinlogName    string
		exitSafeBinlogPos     uint32
		exitSafeBinlogGTIDSet sql.NullString
		tiBytes               []byte
		isGlobal              bool
	)
	for rows.Next() {
		err := rows.Scan(&cpSchema, &cpTable, &binlogName, &binlogPos, &binlogGTIDSet, &exitSafeBinlogName, &exitSafeBinlogPos, &exitSafeBinlogGTIDSet, &tiBytes, &isGlobal)
		if err != nil {
			return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
		}

		gset, err := gtid.ParserGTID(cp.cfg.Flavor, binlogGTIDSet.String) // default to "".
		if err != nil {
			return err
		}

		location := binlog.InitLocation(
			mysql.Position{
				Name: binlogName,
				Pos:  binlogPos,
			},
			gset,
		)
		if isGlobal {
			// Use IsFreshPosition here to make sure checkpoint can be updated if gset is empty
			if !binlog.IsFreshPosition(location, cp.cfg.Flavor, cp.cfg.EnableGTID) {
				cp.globalPoint = newBinlogPoint(location, location, nil, nil, cp.cfg.EnableGTID)
				cp.logCtx.L().Info("fetch global checkpoint from DB", log.WrapStringerField("global checkpoint", cp.globalPoint))
			}

			if cp.cfg.EnableGTID {
				// gtid set default is "", but upgrade may cause NULL value
				if exitSafeBinlogGTIDSet.Valid && exitSafeBinlogGTIDSet.String != "" {
					gset2, err2 := gtid.ParserGTID(cp.cfg.Flavor, exitSafeBinlogGTIDSet.String)
					if err2 != nil {
						return err2
					}
					exitSafeModeLoc := binlog.InitLocation(
						mysql.Position{
							Name: exitSafeBinlogName,
							Pos:  exitSafeBinlogPos,
						},
						gset2,
					)
					cp.SaveSafeModeExitPoint(&exitSafeModeLoc)
				}
			} else {
				if exitSafeBinlogName != "" {
					exitSafeModeLoc := binlog.Location{
						Position: mysql.Position{
							Name: exitSafeBinlogName,
							Pos:  exitSafeBinlogPos,
						},
					}
					cp.SaveSafeModeExitPoint(&exitSafeModeLoc)
				}
			}
			continue // skip global checkpoint
		}

		var ti *model.TableInfo
		if !bytes.Equal(tiBytes, []byte("null")) {
			// only create table if `table_info` is not `null`.
			if err = json.Unmarshal(tiBytes, &ti); err != nil {
				return terror.ErrSchemaTrackerInvalidJSON.Delegate(err, cpSchema, cpTable)
			}
		}

		mSchema, ok := cp.points[cpSchema]
		if !ok {
			mSchema = make(map[string]*binlogPoint)
			cp.points[cpSchema] = mSchema
		}
		mSchema[cpTable] = newBinlogPoint(location, location, ti, ti, cp.cfg.EnableGTID)
	}

	return terror.WithScope(terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError), terror.ScopeDownstream)
}

// CheckAndUpdate check the checkpoint data consistency and try to fix them if possible.
func (cp *RemoteCheckPoint) CheckAndUpdate(ctx context.Context, schemas map[string]string, tables map[string]map[string]string) error {
	cp.Lock()
	hasChange := false
	for lcSchema, tableMap := range tables {
		tableCps, ok := cp.points[lcSchema]
		if !ok {
			continue
		}
		for lcTable, table := range tableMap {
			tableCp, ok := tableCps[lcTable]
			if !ok {
				continue
			}
			tableCps[table] = tableCp
			delete(tableCps, lcTable)
			hasChange = true
		}
	}
	for lcSchema, schema := range schemas {
		if tableCps, ok := cp.points[lcSchema]; ok {
			cp.points[schema] = tableCps
			delete(cp.points, lcSchema)
			hasChange = true
		}
	}
	cp.Unlock()

	if hasChange {
		tctx := tcontext.NewContext(ctx, log.L())
		cpID := cp.Snapshot(true)
		if cpID != nil {
			return cp.FlushPointsExcept(tctx, cpID.id, nil, nil, nil)
		}
	}
	return nil
}

// LoadMeta implements CheckPoint.LoadMeta.
func (cp *RemoteCheckPoint) LoadMeta() error {
	cp.Lock()
	defer cp.Unlock()

	var (
		location        *binlog.Location
		safeModeExitLoc *binlog.Location
		err             error
	)
	switch cp.cfg.Mode {
	case config.ModeAll:
		// NOTE: syncer must continue the syncing follow loader's tail, so we parse mydumper's output
		// refine when master / slave switching added and checkpoint mechanism refactored
		location, safeModeExitLoc, err = cp.parseMetaData()
		if err != nil {
			return err
		}
	case config.ModeIncrement:
		// load meta from task config
		if cp.cfg.Meta == nil {
			cp.logCtx.L().Warn("didn't set meta in increment task-mode")
			cp.globalPoint = newBinlogPoint(binlog.NewLocation(cp.cfg.Flavor), binlog.NewLocation(cp.cfg.Flavor), nil, nil, cp.cfg.EnableGTID)
			return nil
		}
		gset, err := gtid.ParserGTID(cp.cfg.Flavor, cp.cfg.Meta.BinLogGTID)
		if err != nil {
			return err
		}

		loc := binlog.InitLocation(
			mysql.Position{
				Name: cp.cfg.Meta.BinLogName,
				Pos:  cp.cfg.Meta.BinLogPos,
			},
			gset,
		)
		location = &loc
	default:
		// should not go here (syncer is only used in `all` or `incremental` mode)
		return terror.ErrCheckpointInvalidTaskMode.Generate(cp.cfg.Mode)
	}

	// if meta loaded, we will start syncing from meta's pos
	if location != nil {
		cp.globalPoint = newBinlogPoint(*location, *location, nil, nil, cp.cfg.EnableGTID)
		cp.logCtx.L().Info("loaded checkpoints from meta", log.WrapStringerField("global checkpoint", cp.globalPoint))
	}
	if safeModeExitLoc != nil {
		cp.SaveSafeModeExitPoint(safeModeExitLoc)
		cp.logCtx.L().Info("set SafeModeExitLoc from meta", zap.Stringer("SafeModeExitLoc", safeModeExitLoc))
	}

	return nil
}

// genUpdateSQL generates SQL and arguments for update checkpoint.
func (cp *RemoteCheckPoint) genUpdateSQL(cpSchema, cpTable string, location binlog.Location, safeModeExitLoc *binlog.Location, tiBytes []byte, isGlobal bool) (string, []interface{}) {
	// use `INSERT INTO ... ON DUPLICATE KEY UPDATE` rather than `REPLACE INTO`
	// to keep `create_time`, `update_time` correctly
	sql2 := `INSERT INTO ` + cp.tableName + `
		(id, cp_schema, cp_table, binlog_name, binlog_pos, binlog_gtid, exit_safe_binlog_name, exit_safe_binlog_pos, exit_safe_binlog_gtid, table_info, is_global) VALUES
		(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			binlog_name = VALUES(binlog_name),
			binlog_pos = VALUES(binlog_pos),
			binlog_gtid = VALUES(binlog_gtid),
			exit_safe_binlog_name = VALUES(exit_safe_binlog_name),
			exit_safe_binlog_pos = VALUES(exit_safe_binlog_pos),
			exit_safe_binlog_gtid = VALUES(exit_safe_binlog_gtid),
			table_info = VALUES(table_info),
			is_global = VALUES(is_global);
	`

	if isGlobal {
		cpSchema = globalCpSchema
		cpTable = globalCpTable
	}

	if len(tiBytes) == 0 {
		tiBytes = []byte("null")
	}

	var (
		exitSafeName    string
		exitSafePos     uint32
		exitSafeGTIDStr string
	)
	if safeModeExitLoc != nil {
		exitSafeName = safeModeExitLoc.Position.Name
		exitSafePos = safeModeExitLoc.Position.Pos
		exitSafeGTIDStr = safeModeExitLoc.GTIDSetStr()
	}

	// convert tiBytes to string to get a readable log
	args := []interface{}{
		cp.id, cpSchema, cpTable, location.Position.Name, location.Position.Pos, location.GTIDSetStr(),
		exitSafeName, exitSafePos, exitSafeGTIDStr, string(tiBytes), isGlobal,
	}
	return sql2, args
}

func (cp *RemoteCheckPoint) parseMetaData() (*binlog.Location, *binlog.Location, error) {
	// `metadata` is mydumper's output meta file name
	filename := path.Join(cp.cfg.Dir, "metadata")

	loc, loc2, err := dumpling.ParseMetaData(filename, cp.cfg.Flavor)
	if err != nil {
		toPrint, err2 := os.ReadFile(filename)
		if err2 != nil {
			toPrint = []byte(err2.Error())
		}
		err = terror.ErrParseMydumperMeta.Generate(err, toPrint)
	}

	return loc, loc2, err
}

// GetFlushedTableInfo implements CheckPoint.GetFlushedTableInfo.
func (cp *RemoteCheckPoint) GetFlushedTableInfo(table *filter.Table) *model.TableInfo {
	cp.Lock()
	defer cp.Unlock()

	if tables, ok := cp.points[table.Schema]; ok {
		if point, ok2 := tables[table.Name]; ok2 {
			return point.flushedPoint.ti
		}
	}
	return nil
}
