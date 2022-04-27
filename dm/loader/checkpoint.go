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

package loader

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/cputil"
	fr "github.com/pingcap/tiflow/dm/pkg/func-rollback"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"

	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"go.uber.org/zap"
)

// CheckPoint represents checkpoint status.
type CheckPoint interface {
	// Load loads all checkpoints recorded before.
	// because of no checkpoints updated in memory when error occurred
	// when resuming, Load will be called again to load checkpoints
	Load(tctx *tcontext.Context) error

	// GetRestoringFileInfo get restoring data files for table
	GetRestoringFileInfo(db, table string) map[string][]int64

	// GetAllRestoringFileInfo return all restoring files position
	GetAllRestoringFileInfo() map[string][]int64

	// IsTableCreated checks if db / table was created. set `table` to "" when check db
	IsTableCreated(db, table string) bool

	// IsTableFinished query if table has finished
	IsTableFinished(db, table string) bool

	// CalcProgress calculate which table has finished and which table partial restored
	CalcProgress(allFiles map[string]Tables2DataFiles) error

	// Init initialize checkpoint data in tidb
	Init(tctx *tcontext.Context, filename string, endpos int64) error

	// ResetConn resets database connections owned by the Checkpoint
	ResetConn(tctx *tcontext.Context) error

	// Close closes the CheckPoint
	Close()

	// Clear clears all recorded checkpoints
	Clear(tctx *tcontext.Context) error

	// Count returns recorded checkpoints' count
	Count(tctx *tcontext.Context) (int, error)

	// GenSQL generates sql to update checkpoint to DB
	GenSQL(filename string, offset int64) string

	// UpdateOffset keeps `cp.restoringFiles` in memory same with checkpoint in DB,
	// should be called after update checkpoint in DB
	UpdateOffset(filename string, offset int64) error

	// AllFinished returns `true` when all restoring job are finished
	AllFinished() bool
}

// RemoteCheckPoint implements CheckPoint by saving status in remote database system, mostly in TiDB.
// it's not thread-safe.
type RemoteCheckPoint struct {
	// used to protect database operation with `conn`.
	// if more operations need to be protected, add another mutex or rename this one.
	connMutex sync.Mutex

	db             *conn.BaseDB
	conn           *DBConn
	id             string
	schema         string
	tableName      string // tableName contains schema name
	restoringFiles struct {
		sync.RWMutex
		pos map[string]map[string]FilePosSet // schema -> table -> FilePosSet(filename -> [cur, end])
	}
	finishedTables map[string]struct{}
	logger         log.Logger
}

func newRemoteCheckPoint(tctx *tcontext.Context, cfg *config.SubTaskConfig, id string) (CheckPoint, error) {
	var err error
	var db *conn.BaseDB
	var dbConns []*DBConn

	rollbackHolder := fr.NewRollbackHolder("loader")
	defer func() {
		if err != nil {
			rollbackHolder.RollbackReverseOrder()
		}
	}()

	db, dbConns, err = createConns(tctx, cfg, cfg.Name, cfg.SourceID, 1)
	if err != nil {
		return nil, err
	}

	cp := &RemoteCheckPoint{
		db:             db,
		conn:           dbConns[0],
		id:             id,
		finishedTables: make(map[string]struct{}),
		schema:         dbutil.ColumnName(cfg.MetaSchema),
		tableName:      dbutil.TableName(cfg.MetaSchema, cputil.LoaderCheckpoint(cfg.Name)),
		logger:         tctx.L().WithFields(zap.String("component", "remote checkpoint")),
	}
	cp.restoringFiles.pos = make(map[string]map[string]FilePosSet)
	rollbackHolder.Add(fr.FuncRollback{Name: "CloseRemoteCheckPoint", Fn: cp.Close})

	err = cp.prepare(tctx)
	if err != nil {
		return nil, err
	}

	return cp, nil
}

func (cp *RemoteCheckPoint) prepare(tctx *tcontext.Context) error {
	// create schema
	if err := cp.createSchema(tctx); err != nil {
		return err
	}
	// create table
	return cp.createTable(tctx)
}

func (cp *RemoteCheckPoint) createSchema(tctx *tcontext.Context) error {
	sql2 := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", cp.schema)
	cp.connMutex.Lock()
	err := cp.conn.executeSQL(tctx, []string{sql2})
	cp.connMutex.Unlock()
	return terror.WithScope(err, terror.ScopeDownstream)
}

func (cp *RemoteCheckPoint) createTable(tctx *tcontext.Context) error {
	createTable := `CREATE TABLE IF NOT EXISTS %s (
		id char(32) NOT NULL,
		filename varchar(255) NOT NULL,
		cp_schema varchar(128) NOT NULL,
		cp_table varchar(128) NOT NULL,
		offset bigint NOT NULL,
		end_pos bigint NOT NULL,
		create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
		update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		UNIQUE KEY uk_id_f (id,filename)
	);
`
	sql2 := fmt.Sprintf(createTable, cp.tableName)
	cp.connMutex.Lock()
	err := cp.conn.executeSQL(tctx, []string{sql2})
	cp.connMutex.Unlock()
	return terror.WithScope(err, terror.ScopeDownstream)
}

// Load implements CheckPoint.Load.
func (cp *RemoteCheckPoint) Load(tctx *tcontext.Context) error {
	begin := time.Now()
	defer func() {
		cp.logger.Info("load checkpoint", zap.Duration("cost time", time.Since(begin)))
	}()

	query := fmt.Sprintf("SELECT `filename`,`cp_schema`,`cp_table`,`offset`,`end_pos` from %s where `id`=?", cp.tableName)
	cp.connMutex.Lock()
	rows, err := cp.conn.querySQL(tctx, query, cp.id)
	cp.connMutex.Unlock()
	if err != nil {
		return terror.WithScope(err, terror.ScopeDownstream)
	}
	defer rows.Close()

	var (
		filename string
		schema   string
		table    string
		offset   int64
		endPos   int64
	)

	cp.restoringFiles.Lock()
	defer cp.restoringFiles.Unlock()
	cp.restoringFiles.pos = make(map[string]map[string]FilePosSet) // reset to empty
	for rows.Next() {
		err := rows.Scan(&filename, &schema, &table, &offset, &endPos)
		if err != nil {
			return terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
		}

		if _, ok := cp.restoringFiles.pos[schema]; !ok {
			cp.restoringFiles.pos[schema] = make(map[string]FilePosSet)
		}
		tables := cp.restoringFiles.pos[schema]
		if _, ok := tables[table]; !ok {
			tables[table] = make(map[string][]int64)
		}
		restoringFiles := tables[table]
		restoringFiles[filename] = []int64{offset, endPos}
	}

	return terror.WithScope(terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError), terror.ScopeDownstream)
}

// GetRestoringFileInfo implements CheckPoint.GetRestoringFileInfo.
func (cp *RemoteCheckPoint) GetRestoringFileInfo(db, table string) map[string][]int64 {
	cp.restoringFiles.RLock()
	defer cp.restoringFiles.RUnlock()
	results := make(map[string][]int64)
	if tables, ok := cp.restoringFiles.pos[db]; ok {
		if restoringFiles, ok := tables[table]; ok {
			// make a copy of restoringFiles, and its slice value
			for k, v := range restoringFiles {
				results[k] = make([]int64, len(v))
				copy(results[k], v)
			}
			return results
		}
	}
	return results
}

// GetAllRestoringFileInfo implements CheckPoint.GetAllRestoringFileInfo.
func (cp *RemoteCheckPoint) GetAllRestoringFileInfo() map[string][]int64 {
	cp.restoringFiles.RLock()
	defer cp.restoringFiles.RUnlock()
	results := make(map[string][]int64)
	for _, tables := range cp.restoringFiles.pos {
		for _, files := range tables {
			for file, pos := range files {
				results[file] = make([]int64, len(pos))
				copy(results[file], pos)
			}
		}
	}
	return results
}

// IsTableCreated implements CheckPoint.IsTableCreated.
func (cp *RemoteCheckPoint) IsTableCreated(db, table string) bool {
	cp.restoringFiles.RLock()
	defer cp.restoringFiles.RUnlock()
	tables, ok := cp.restoringFiles.pos[db]
	if !ok {
		return false
	}
	if table == "" {
		return true
	}
	_, ok = tables[table]
	return ok
}

// IsTableFinished implements CheckPoint.IsTableFinished.
func (cp *RemoteCheckPoint) IsTableFinished(db, table string) bool {
	key := strings.Join([]string{db, table}, ".")
	if _, ok := cp.finishedTables[key]; ok {
		return true
	}
	return false
}

// CalcProgress implements CheckPoint.CalcProgress.
func (cp *RemoteCheckPoint) CalcProgress(allFiles map[string]Tables2DataFiles) error {
	cp.restoringFiles.RLock()
	defer cp.restoringFiles.RUnlock()
	cp.finishedTables = make(map[string]struct{}) // reset to empty
	for db, tables := range cp.restoringFiles.pos {
		dbTables, ok := allFiles[db]
		if !ok {
			return terror.ErrCheckpointDBNotExistInFile.Generate(db)
		}

		for table, restoringFiles := range tables {
			files, ok := dbTables[table]
			if !ok {
				return terror.ErrCheckpointTableNotExistInFile.Generate(table, db)
			}

			restoringCount := len(restoringFiles)
			totalCount := len(files)

			t := strings.Join([]string{db, table}, ".")
			if restoringCount == totalCount {
				// compare offset.
				if cp.allFilesFinished(restoringFiles) {
					cp.finishedTables[t] = struct{}{}
				}
			} else if restoringCount > totalCount {
				return terror.ErrCheckpointRestoreCountGreater.Generate(table)
			}
		}
	}

	cp.logger.Info("calculate checkpoint finished.", zap.Any("finished tables", cp.finishedTables))
	return nil
}

func (cp *RemoteCheckPoint) allFilesFinished(files map[string][]int64) bool {
	for file, pos := range files {
		if len(pos) != 2 {
			cp.logger.Error("unexpected checkpoint record", zap.String("data file", file), zap.Int64s("position", pos))
			return false
		}
		if pos[0] != pos[1] {
			return false
		}
	}
	return true
}

// AllFinished implements CheckPoint.AllFinished.
func (cp *RemoteCheckPoint) AllFinished() bool {
	cp.restoringFiles.RLock()
	defer cp.restoringFiles.RUnlock()
	for _, tables := range cp.restoringFiles.pos {
		for _, restoringFiles := range tables {
			if !cp.allFilesFinished(restoringFiles) {
				return false
			}
		}
	}
	return true
}

// Init implements CheckPoint.Init.
func (cp *RemoteCheckPoint) Init(tctx *tcontext.Context, filename string, endPos int64) error {
	// fields[0] -> db name, fields[1] -> table name
	schema, table, err := getDBAndTableFromFilename(filename)
	if err != nil {
		return terror.ErrCheckpointInvalidTableFile.Generate(filename)
	}
	sql2 := fmt.Sprintf("INSERT INTO %s (`id`, `filename`, `cp_schema`, `cp_table`, `offset`, `end_pos`) VALUES(?,?,?,?,?,?)", cp.tableName)
	cp.logger.Info("initial checkpoint record",
		zap.String("sql", sql2),
		zap.String("id", cp.id),
		zap.String("filename", filename),
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int64("offset", 0),
		zap.Int64("end position", endPos))
	args := []interface{}{cp.id, filename, schema, table, 0, endPos}
	cp.connMutex.Lock()
	err = cp.conn.executeSQL(tctx, []string{sql2}, args)
	cp.connMutex.Unlock()
	if err != nil {
		if isErrDupEntry(err) {
			cp.logger.Warn("checkpoint record already exists, skip it.", zap.String("id", cp.id), zap.String("filename", filename))
			return nil
		}
		return terror.WithScope(terror.Annotate(err, "initialize checkpoint"), terror.ScopeDownstream)
	}
	// checkpoint not exists and no error, cache endPos in memory
	cp.restoringFiles.Lock()
	defer cp.restoringFiles.Unlock()
	if _, ok := cp.restoringFiles.pos[schema]; !ok {
		cp.restoringFiles.pos[schema] = make(map[string]FilePosSet)
	}
	tables := cp.restoringFiles.pos[schema]
	if _, ok := tables[table]; !ok {
		tables[table] = make(map[string][]int64)
	}
	restoringFiles := tables[table]
	if _, ok := restoringFiles[filename]; !ok {
		restoringFiles[filename] = []int64{0, endPos}
	}
	return nil
}

// ResetConn implements CheckPoint.ResetConn.
func (cp *RemoteCheckPoint) ResetConn(tctx *tcontext.Context) error {
	cp.connMutex.Lock()
	defer cp.connMutex.Unlock()
	return cp.conn.resetConn(tctx)
}

// Close implements CheckPoint.Close.
func (cp *RemoteCheckPoint) Close() {
	if err := cp.db.Close(); err != nil {
		cp.logger.Error("close checkpoint db", log.ShortError(err))
	}
}

// GenSQL implements CheckPoint.GenSQL.
func (cp *RemoteCheckPoint) GenSQL(filename string, offset int64) string {
	sql := fmt.Sprintf("UPDATE %s SET `offset`=%d WHERE `id` ='%s' AND `filename`='%s';",
		cp.tableName, offset, cp.id, filename)
	return sql
}

// UpdateOffset implements CheckPoint.UpdateOffset.
func (cp *RemoteCheckPoint) UpdateOffset(filename string, offset int64) error {
	cp.restoringFiles.Lock()
	defer cp.restoringFiles.Unlock()
	db, table, err := getDBAndTableFromFilename(filename)
	if err != nil {
		return terror.Annotatef(terror.ErrLoadTaskCheckPointNotMatch.Generate(err), "wrong filename=%s", filename)
	}

	if _, ok := cp.restoringFiles.pos[db]; ok {
		if _, ok := cp.restoringFiles.pos[db][table]; ok {
			if _, ok := cp.restoringFiles.pos[db][table][filename]; ok {
				cp.restoringFiles.pos[db][table][filename][0] = offset
				return nil
			}
		}
	}
	return terror.ErrLoadTaskCheckPointNotMatch.Generatef("db=%s table=%s not in checkpoint", db, filename)
}

// Clear implements CheckPoint.Clear.
func (cp *RemoteCheckPoint) Clear(tctx *tcontext.Context) error {
	sql2 := fmt.Sprintf("DELETE FROM %s WHERE `id` = '%s'", cp.tableName, cp.id)
	cp.connMutex.Lock()
	err := cp.conn.executeSQL(tctx, []string{sql2})
	cp.connMutex.Unlock()
	return terror.WithScope(err, terror.ScopeDownstream)
}

// Count implements CheckPoint.Count.
func (cp *RemoteCheckPoint) Count(tctx *tcontext.Context) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(id) FROM %s WHERE `id` = ?", cp.tableName)
	cp.connMutex.Lock()
	rows, err := cp.conn.querySQL(tctx, query, cp.id)
	cp.connMutex.Unlock()
	if err != nil {
		return 0, terror.WithScope(err, terror.ScopeDownstream)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return 0, terror.WithScope(terror.DBErrorAdapt(err, terror.ErrDBDriverError), terror.ScopeDownstream)
		}
	}
	if rows.Err() != nil {
		return 0, terror.WithScope(terror.DBErrorAdapt(rows.Err(), terror.ErrDBDriverError), terror.ScopeDownstream)
	}
	cp.logger.Debug("checkpoint record", zap.Int("count", count))
	return count, nil
}

func (cp *RemoteCheckPoint) String() string {
	cp.restoringFiles.RLock()
	defer cp.restoringFiles.RUnlock()
	result := make(map[string][]int64)
	for _, tables := range cp.restoringFiles.pos {
		for _, files := range tables {
			for file, set := range files {
				result[file] = set
			}
		}
	}
	bytes, err := json.Marshal(result)
	if err != nil {
		return err.Error()
	}
	return string(bytes)
}

type lightingLoadStatus int

const (
	lightningStatusInit lightingLoadStatus = iota
	lightningStatusRunning
	lightningStatusFinished
)

func (s lightingLoadStatus) String() string {
	switch s {
	case lightningStatusInit:
		return "init"
	case lightningStatusRunning:
		return "running"
	case lightningStatusFinished:
		return "finished"
	default:
		panic(fmt.Sprintf("unknown lightning load stauts '%d'", s))
	}
}

func parseLightningLoadStatus(s string) lightingLoadStatus {
	switch s {
	case "running":
		return lightningStatusRunning
	case "finished":
		return lightningStatusFinished
	case "init":
		return lightningStatusInit
	default:
		log.L().Warn("unknown lightning load status, will fallback to init", zap.String("status", s))
		return lightningStatusInit
	}
}

type LightningCheckpointList struct {
	db         *conn.BaseDB
	schema     string
	tableName  string
	taskName   string
	sourceName string
	logger     log.Logger
}

func NewLightningCheckpointList(db *conn.BaseDB, taskName, sourceName, metaSchema string) *LightningCheckpointList {
	return &LightningCheckpointList{
		db:         db,
		schema:     dbutil.ColumnName(metaSchema),
		tableName:  dbutil.TableName(metaSchema, cputil.LightningCheckpoint(taskName)),
		taskName:   taskName,
		sourceName: sourceName,
		logger:     log.L().WithFields(zap.String("component", "lightning checkpoint database list")),
	}
}

func (cp *LightningCheckpointList) Prepare(ctx context.Context) error {
	connection, err := cp.db.GetBaseConn(ctx)
	if err != nil {
		return terror.WithScope(terror.Annotate(err, "initialize connection when prepare"), terror.ScopeDownstream)
	}
	defer conn.CloseBaseConnWithoutErr(cp.db, connection)

	createSchema := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", cp.schema)
	tctx := tcontext.NewContext(ctx, log.With(zap.String("job", "lightning-checkpoint")))
	_, err = connection.ExecuteSQL(tctx, nil, "lightning-checkpoint", []string{createSchema})
	if err != nil {
		return err
	}
	createTable := `CREATE TABLE IF NOT EXISTS %s (
		task_name varchar(255) NOT NULL,
		source_name varchar(255) NOT NULL,
		status varchar(10) NOT NULL DEFAULT 'init' COMMENT 'init,running,finished',
		PRIMARY KEY (task_name, source_name)
	);
`
	sql2 := fmt.Sprintf(createTable, cp.tableName)
	_, err = connection.ExecuteSQL(tctx, nil, "lightning-checkpoint", []string{sql2})
	return terror.WithScope(err, terror.ScopeDownstream)
}

func (cp *LightningCheckpointList) RegisterCheckPoint(ctx context.Context) error {
	connection, err := cp.db.GetBaseConn(ctx)
	if err != nil {
		return terror.WithScope(terror.Annotate(err, "initialize connection"), terror.ScopeDownstream)
	}
	defer conn.CloseBaseConnWithoutErr(cp.db, connection)

	sql := fmt.Sprintf("INSERT IGNORE INTO %s (`task_name`, `source_name`) VALUES (?, ?)", cp.tableName)
	cp.logger.Info("initial checkpoint record",
		zap.String("task", cp.taskName),
		zap.String("source", cp.sourceName))
	args := []interface{}{cp.taskName, cp.sourceName}
	tctx := tcontext.NewContext(ctx, log.With(zap.String("job", "lightning-checkpoint")))
	_, err = connection.ExecuteSQL(tctx, nil, "lightning-checkpoint", []string{sql}, args)
	if err != nil {
		return terror.WithScope(terror.Annotate(err, "initialize checkpoint"), terror.ScopeDownstream)
	}
	return nil
}

func (cp *LightningCheckpointList) UpdateStatus(ctx context.Context, status lightingLoadStatus) error {
	connection, err := cp.db.GetBaseConn(ctx)
	if err != nil {
		return terror.WithScope(terror.Annotate(err, "initialize connection"), terror.ScopeDownstream)
	}
	defer conn.CloseBaseConnWithoutErr(cp.db, connection)

	sql := fmt.Sprintf("UPDATE %s set status = ? WHERE `task_name` = ? AND `source_name` = ?", cp.tableName)
	cp.logger.Info("update lightning loader status",
		zap.String("task", cp.taskName), zap.String("source", cp.sourceName),
		zap.Stringer("status", status))
	tctx := tcontext.NewContext(ctx, log.With(zap.String("job", "lightning-checkpoint")))
	_, err = connection.ExecuteSQL(tctx, nil, "lightning-checkpoint", []string{sql},
		[]interface{}{status.String(), cp.taskName, cp.sourceName})
	if err != nil {
		return terror.WithScope(terror.Annotate(err, "update lightning status"), terror.ScopeDownstream)
	}
	return nil
}

func (cp *LightningCheckpointList) taskStatus(ctx context.Context) (lightingLoadStatus, error) {
	connection, err := cp.db.GetBaseConn(ctx)
	if err != nil {
		return lightningStatusInit, terror.WithScope(terror.Annotate(err, "initialize connection"), terror.ScopeDownstream)
	}
	defer conn.CloseBaseConnWithoutErr(cp.db, connection)

	query := fmt.Sprintf("SELECT status FROM %s WHERE `task_name` = ? AND `source_name` = ?", cp.tableName)
	tctx := tcontext.NewContext(ctx, log.With(zap.String("job", "lightning-checkpoint")))
	rows, err := connection.QuerySQL(tctx, query, cp.taskName, cp.sourceName)
	if err != nil {
		return lightningStatusInit, err
	}
	defer rows.Close()
	if rows.Next() {
		var status string
		if err = rows.Scan(&status); err != nil {
			return lightningStatusInit, terror.WithScope(err, terror.ScopeDownstream)
		}
		return parseLightningLoadStatus(status), nil
	}
	// status row doesn't exist, return default value
	return lightningStatusInit, nil
}

// Close implements CheckPoint.Close.
func (cp *LightningCheckpointList) Close() {
	if err := cp.db.Close(); err != nil {
		cp.logger.Error("close checkpoint list db", log.ShortError(err))
	}
}
