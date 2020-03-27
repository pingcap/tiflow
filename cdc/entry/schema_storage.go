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

package entry

import (
	"container/list"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pingcap/ticdc/pkg/retry"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.uber.org/zap"
)

// Storage stores the source TiDB all schema infomations
// schema infomations could be changed by drainer init and ddls appear
type Storage struct {
	tableIDToName  map[int64]TableName
	tableNameToID  map[TableName]int64
	schemaNameToID map[string]int64

	schemas map[int64]*timodel.DBInfo
	tables  map[int64]*TableInfo

	truncateTableID   map[int64]struct{}
	ineligibleTableID map[int64]struct{}

	schemaMetaVersion int64
	lastHandledTs     uint64
	resolvedTs        *uint64

	currentJob *list.Element
	jobList    *jobList

	version2SchemaTable map[int64]TableName
	currentVersion      int64
}

// TableName specify a Schema name and Table name
type TableName struct {
	Schema string `toml:"db-name" json:"db-name"`
	Table  string `toml:"tbl-name" json:"tbl-name"`
}

// String implements fmt.Stringer interface.
func (t TableName) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
}

// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	*timodel.TableInfo
	ColumnsOffset map[int64]int
	IndicesOffset map[int64]int
	UniqueColumns map[int64]struct{}
	handleColID   int64

	// if the table of this row only has one unique index(includes primary key),
	// IndieMarkCol will be set to the name of the unique index
	IndieMarkCol string
	rowColInfos  []rowcodec.ColInfo
}

// WrapTableInfo creates a TableInfo from a timodel.TableInfo
func WrapTableInfo(info *timodel.TableInfo) *TableInfo {
	columnsOffset := make(map[int64]int, len(info.Columns))
	for i, col := range info.Columns {
		columnsOffset[col.ID] = i
	}
	indicesOffset := make(map[int64]int, len(info.Indices))
	for i, idx := range info.Indices {
		indicesOffset[idx.ID] = i
	}

	ti := &TableInfo{
		TableInfo:     info,
		ColumnsOffset: columnsOffset,
		IndicesOffset: indicesOffset,
		UniqueColumns: make(map[int64]struct{}),
	}

	uniqueIndexNum := 0

	if ti.PKIsHandle {
		for _, col := range ti.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				// Prepend to make sure the primary key ends up at the front
				ti.UniqueColumns[col.ID] = struct{}{}
				break
			}
		}
		uniqueIndexNum++
	}

	for _, idx := range ti.Indices {
		if ti.IsIndexUnique(idx) {
			for _, col := range idx.Columns {
				ti.UniqueColumns[ti.Columns[col.Offset].ID] = struct{}{}
			}
		}
		if idx.Primary || idx.Unique {
			uniqueIndexNum++
		}
	}

	// this table has only one unique column
	if uniqueIndexNum == 1 && len(ti.UniqueColumns) == 1 {
		for col := range ti.UniqueColumns {
			info, _ := ti.GetColumnInfo(col)
			ti.IndieMarkCol = info.Name.O
		}
	}

	return ti
}

// GetColumnInfo returns the column info by ID
func (ti *TableInfo) GetColumnInfo(colID int64) (info *timodel.ColumnInfo, exist bool) {
	colOffset, exist := ti.ColumnsOffset[colID]
	if !exist {
		return nil, false
	}
	return ti.Columns[colOffset], true
}

// GetIndexInfo returns the index info by ID
func (ti *TableInfo) GetIndexInfo(indexID int64) (info *timodel.IndexInfo, exist bool) {
	indexOffset, exist := ti.IndicesOffset[indexID]
	if !exist {
		return nil, false
	}
	return ti.Indices[indexOffset], true
}

// GetRowColInfos returns all column infos for rowcodec
func (ti *TableInfo) GetRowColInfos() (int64, []rowcodec.ColInfo) {
	if len(ti.rowColInfos) != 0 {
		return ti.handleColID, ti.rowColInfos
	}
	handleColID := int64(-1)
	reqCols := make([]rowcodec.ColInfo, len(ti.Columns))
	for i, col := range ti.Columns {
		isPK := (ti.PKIsHandle && mysql.HasPriKeyFlag(col.Flag)) || col.ID == timodel.ExtraHandleID
		if isPK {
			handleColID = col.ID
		}
		reqCols[i] = rowcodec.ColInfo{
			ID:         col.ID,
			Tp:         int32(col.Tp),
			Flag:       int32(col.Flag),
			Flen:       col.Flen,
			Decimal:    col.Decimal,
			Elems:      col.Elems,
			IsPKHandle: isPK,
		}
	}
	ti.rowColInfos = reqCols
	ti.handleColID = handleColID
	return ti.handleColID, ti.rowColInfos
}

// IsColWritable returns is the col is writeable
func (ti *TableInfo) IsColWritable(col *timodel.ColumnInfo) bool {
	return col.State == timodel.StatePublic && !col.IsGenerated()
}

// GetUniqueKeys returns all unique keys of the table as a slice of column names
func (ti *TableInfo) GetUniqueKeys() [][]string {
	var uniqueKeys [][]string
	if ti.PKIsHandle {
		for _, col := range ti.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				// Prepend to make sure the primary key ends up at the front
				uniqueKeys = [][]string{{col.Name.O}}
				break
			}
		}
	}
	for _, idx := range ti.Indices {
		if ti.IsIndexUnique(idx) {
			colNames := make([]string, 0, len(idx.Columns))
			for _, col := range idx.Columns {
				colNames = append(colNames, col.Name.O)
			}
			if idx.Primary {
				uniqueKeys = append([][]string{colNames}, uniqueKeys...)
			} else {
				uniqueKeys = append(uniqueKeys, colNames)
			}
		}
	}
	return uniqueKeys
}

// IsColumnUnique returns whether the column is unique
func (ti *TableInfo) IsColumnUnique(colID int64) bool {
	_, exist := ti.UniqueColumns[colID]
	return exist
}

// ExistTableUniqueColumn returns whether the table has the unique column
func (ti *TableInfo) ExistTableUniqueColumn() bool {
	return len(ti.UniqueColumns) != 0
}

// IsIndexUnique returns whether the index is unique
func (ti *TableInfo) IsIndexUnique(indexInfo *timodel.IndexInfo) bool {
	if indexInfo.Primary {
		return true
	}
	if indexInfo.Unique {
		for _, col := range indexInfo.Columns {
			if !mysql.HasNotNullFlag(ti.Columns[col.Offset].Flag) {
				return false
			}
		}
		return true
	}
	return false
}

// Clone clones the TableInfo
func (ti *TableInfo) Clone() *TableInfo {
	return WrapTableInfo(ti.TableInfo.Clone())
}

// newStorage returns the Schema object
func newStorage(resolvedTs *uint64, jobList *jobList) *Storage {
	s := NewSingleStorage()
	s.resolvedTs = resolvedTs
	s.currentJob = jobList.Head()
	s.jobList = jobList
	return s
}

// NewSingleStorage creates a new single storage
func NewSingleStorage() *Storage {
	s := &Storage{
		version2SchemaTable: make(map[int64]TableName),
		truncateTableID:     make(map[int64]struct{}),
		ineligibleTableID:   make(map[int64]struct{}),
	}

	s.tableIDToName = make(map[int64]TableName)
	s.tableNameToID = make(map[TableName]int64)
	s.schemas = make(map[int64]*timodel.DBInfo)
	s.schemaNameToID = make(map[string]int64)
	s.tables = make(map[int64]*TableInfo)

	return s
}

// String implements fmt.Stringer interface.
func (s *Storage) String() string {
	mp := map[string]interface{}{
		"tableIDToName":     s.tableIDToName,
		"schemaNameToID":    s.schemaNameToID,
		"schemas":           s.schemas,
		"tables":            s.tables,
		"schemaMetaVersion": s.schemaMetaVersion,
		"resolvedTs":        *s.resolvedTs,
		"cjob":              s.currentJob.Value,
	}

	data, err := json.MarshalIndent(mp, "\t", "\t")
	if err != nil {
		panic(err)
	}

	return string(data)
}

// SchemaMetaVersion returns the current schemaversion in drainer
func (s *Storage) SchemaMetaVersion() int64 {
	return s.schemaMetaVersion
}

// GetTableNameByID looks up a TableName with the given table id
func (s *Storage) GetTableNameByID(id int64) (TableName, bool) {
	name, ok := s.tableIDToName[id]
	return name, ok
}

// GetTableIDByName returns the tableID by table schemaName and tableName
func (s *Storage) GetTableIDByName(schemaName string, tableName string) (int64, bool) {
	id, ok := s.tableNameToID[TableName{
		Schema: schemaName,
		Table:  tableName,
	}]
	return id, ok
}

// GetTableByName queries a table by name,
// the second returned value is false if no table with the specified name is found.
func (s *Storage) GetTableByName(schema, table string) (info *TableInfo, ok bool) {
	id, ok := s.GetTableIDByName(schema, table)
	if !ok {
		return nil, ok
	}
	return s.TableByID(id)
}

// SchemaByID returns the DBInfo by schema id
func (s *Storage) SchemaByID(id int64) (val *timodel.DBInfo, ok bool) {
	val, ok = s.schemas[id]
	return
}

// SchemaByTableID returns the schema ID by table ID
func (s *Storage) SchemaByTableID(tableID int64) (*timodel.DBInfo, bool) {
	tn, ok := s.tableIDToName[tableID]
	if !ok {
		return nil, false
	}
	schemaID, ok := s.schemaNameToID[tn.Schema]
	if !ok {
		return nil, false
	}
	return s.SchemaByID(schemaID)
}

// TableByID returns the TableInfo by table id
func (s *Storage) TableByID(id int64) (val *TableInfo, ok bool) {
	val, ok = s.tables[id]
	return
}

// DropSchema deletes the given DBInfo
func (s *Storage) DropSchema(id int64) (string, error) {
	schema, ok := s.schemas[id]
	if !ok {
		return "", errors.NotFoundf("schema %d", id)
	}

	for _, table := range schema.Tables {
		delete(s.tables, table.ID)
		tableName := s.tableIDToName[table.ID]
		delete(s.tableIDToName, table.ID)
		delete(s.tableNameToID, tableName)
	}

	delete(s.schemas, id)
	delete(s.schemaNameToID, schema.Name.O)

	return schema.Name.O, nil
}

// CreateSchema adds new DBInfo
func (s *Storage) CreateSchema(db *timodel.DBInfo) error {
	if _, ok := s.schemas[db.ID]; ok {
		return errors.AlreadyExistsf("schema %s(%d)", db.Name, db.ID)
	}

	s.schemas[db.ID] = db
	s.schemaNameToID[db.Name.O] = db.ID

	log.Debug("create schema success, schema id", zap.String("name", db.Name.O), zap.Int64("id", db.ID))
	return nil
}

// DropTable deletes the given TableInfo
func (s *Storage) DropTable(id int64) (string, error) {
	table, ok := s.tables[id]
	if !ok {
		return "", errors.NotFoundf("table %d", id)
	}
	err := s.removeTable(id)
	if err != nil {
		return "", errors.Trace(err)
	}

	delete(s.tables, id)
	tableName := s.tableIDToName[id]
	delete(s.tableIDToName, id)
	delete(s.tableNameToID, tableName)
	delete(s.ineligibleTableID, id)

	log.Debug("drop table success", zap.String("name", table.Name.O), zap.Int64("id", id))
	return table.Name.O, nil
}

// CreateTable creates new TableInfo
func (s *Storage) CreateTable(schema *timodel.DBInfo, table *timodel.TableInfo) error {
	_, ok := s.tables[table.ID]
	if ok {
		return errors.AlreadyExistsf("table %s.%s", schema.Name, table.Name)
	}

	schema.Tables = append(schema.Tables, table)
	tbl := WrapTableInfo(table)
	s.tables[table.ID] = tbl
	if !tbl.ExistTableUniqueColumn() {
		log.Warn("this table is not eligible to replicate", zap.String("tableName", table.Name.O), zap.Int64("tableID", table.ID))
		s.ineligibleTableID[table.ID] = struct{}{}
	}
	s.tableIDToName[table.ID] = TableName{Schema: schema.Name.O, Table: table.Name.O}
	s.tableNameToID[s.tableIDToName[table.ID]] = table.ID

	log.Debug("create table success", zap.String("name", schema.Name.O+"."+table.Name.O), zap.Int64("id", table.ID))
	return nil
}

// ReplaceTable replace the table by new tableInfo
func (s *Storage) ReplaceTable(table *timodel.TableInfo) error {
	_, ok := s.tables[table.ID]
	if !ok {
		return errors.NotFoundf("table %s(%d)", table.Name, table.ID)
	}
	tbl := WrapTableInfo(table)
	s.tables[table.ID] = tbl
	if !tbl.ExistTableUniqueColumn() {
		log.Warn("this table is not eligible to replicate", zap.String("tableName", table.Name.O), zap.Int64("tableID", table.ID))
		s.ineligibleTableID[table.ID] = struct{}{}
	}
	return nil
}

func (s *Storage) removeTable(tableID int64) error {
	schema, ok := s.SchemaByTableID(tableID)
	if !ok {
		return errors.NotFoundf("table(%d)'s schema", tableID)
	}

	for i := range schema.Tables {
		if schema.Tables[i].ID == tableID {
			copy(schema.Tables[i:], schema.Tables[i+1:])
			schema.Tables = schema.Tables[:len(schema.Tables)-1]
			return nil
		}
	}
	return nil
}

// HandlePreviousDDLJobIfNeed apply all jobs with FinishedTS less or equals `commitTs`.
func (s *Storage) HandlePreviousDDLJobIfNeed(commitTs uint64) error {
	err := retry.Run(10*time.Millisecond, 25,
		func() error {
			err := s.handlePreviousDDLJobIfNeed(commitTs)
			if errors.Cause(err) != model.ErrUnresolved {
				return backoff.Permanent(err)
			}
			return err
		})
	switch err.(type) {
	case *backoff.PermanentError:
		return errors.Annotate(err, "timeout")
	default:
		return err
	}
}

func (s *Storage) handlePreviousDDLJobIfNeed(commitTs uint64) error {
	resolvedTs := atomic.LoadUint64(s.resolvedTs)
	if commitTs > resolvedTs {
		return errors.Annotatef(model.ErrUnresolved, "waiting resolved ts of ddl puller, resolvedTs(%d), commitTs(%d)", resolvedTs, commitTs)
	}
	currentJob, jobs := s.jobList.FetchNextJobs(s.currentJob, commitTs)
	for _, job := range jobs {
		if SkipJob(job) {
			log.Info("skip DDL job because the job isn't synced and done", zap.Stringer("job", job))
			continue
		}
		if job.BinlogInfo.FinishedTS <= s.lastHandledTs {
			log.Debug("skip DDL job because the job is already handled", zap.Stringer("job", job))
			continue
		}
		_, _, _, err := s.HandleDDL(job)
		if err != nil {
			return errors.Annotatef(err, "handle ddl job %v failed, the schema info: %s", job, s)
		}
	}
	s.currentJob = currentJob
	return nil
}

// HandleDDL has four return values,
// the first value[string]: the schema name
// the second value[string]: the table name
// the third value[string]: the sql that is corresponding to the job
// the fourth value[error]: the handleDDL execution's err
func (s *Storage) HandleDDL(job *timodel.Job) (schemaName string, tableName string, sql string, err error) {
	log.Debug("handle job: ", zap.String("sql query", job.Query), zap.Stringer("job", job))

	if SkipJob(job) {
		return "", "", "", nil
	}

	sql = job.Query
	if sql == "" {
		return "", "", "", errors.Errorf("[ddl job sql miss]%+v", job)
	}

	switch job.Type {
	case timodel.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		schema := job.BinlogInfo.DBInfo.Clone()

		err := s.CreateSchema(schema)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: ""}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O

	case timodel.ActionModifySchemaCharsetAndCollate:
		db := job.BinlogInfo.DBInfo.Clone()
		if _, ok := s.schemas[db.ID]; !ok {
			return "", "", "", errors.NotFoundf("schema %s(%d)", db.Name, db.ID)
		}

		s.schemas[db.ID] = db
		s.schemaNameToID[db.Name.O] = db.ID
		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: db.Name.O, Table: ""}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = db.Name.O

	case timodel.ActionDropSchema:
		schemaName, err = s.DropSchema(job.SchemaID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schemaName, Table: ""}
		s.currentVersion = job.BinlogInfo.SchemaVersion

	case timodel.ActionRenameTable:
		// ignore schema doesn't support rename ddl
		_, ok := s.SchemaByTableID(job.TableID)
		if !ok {
			return "", "", "", errors.NotFoundf("table(%d) or it's schema", job.TableID)
		}
		// first drop the table
		_, err := s.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}
		// create table
		table := job.BinlogInfo.TableInfo.Clone()
		schema, ok := s.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err = s.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: table.Name.O}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O
		tableName = table.Name.O

	case timodel.ActionCreateTable, timodel.ActionCreateView, timodel.ActionRecoverTable:
		table := job.BinlogInfo.TableInfo.Clone()
		if table == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		schema, ok := s.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err := s.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: table.Name.O}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O
		tableName = table.Name.O

	case timodel.ActionDropTable, timodel.ActionDropView:
		schema, ok := s.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		tableName, err = s.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: tableName}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O

	case timodel.ActionTruncateTable:
		schema, ok := s.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		// job.TableID is the old table id, different from table.ID
		_, err := s.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		table := job.BinlogInfo.TableInfo.Clone()
		if table == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}

		err = s.CreateTable(schema, table)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: table.Name.O}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O
		tableName = table.Name.O
		s.truncateTableID[job.TableID] = struct{}{}

	default:
		binlogInfo := job.BinlogInfo
		if binlogInfo == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}
		tbInfo := binlogInfo.TableInfo
		if tbInfo == nil {
			return "", "", "", errors.NotFoundf("table %d", job.TableID)
		}
		tbInfo = tbInfo.Clone()

		schema, ok := s.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		err := s.ReplaceTable(tbInfo)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: tbInfo.Name.O}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O
		tableName = tbInfo.Name.O
	}
	s.lastHandledTs = job.BinlogInfo.FinishedTS
	return
}

// CloneTables return a clone of the existing tables.
func (s *Storage) CloneTables() map[uint64]TableName {
	mp := make(map[uint64]TableName, len(s.tableIDToName))

	for id, table := range s.tableIDToName {
		mp[uint64(id)] = table
	}

	return mp
}

// Clone clones Storage
func (s *Storage) Clone() *Storage {
	n := &Storage{
		tableIDToName:  make(map[int64]TableName),
		tableNameToID:  make(map[TableName]int64),
		schemaNameToID: make(map[string]int64),

		schemas: make(map[int64]*timodel.DBInfo),
		tables:  make(map[int64]*TableInfo),

		truncateTableID:     make(map[int64]struct{}),
		ineligibleTableID:   make(map[int64]struct{}),
		version2SchemaTable: make(map[int64]TableName),
	}
	for k, v := range s.tableIDToName {
		n.tableIDToName[k] = v
	}
	for k, v := range s.tableNameToID {
		n.tableNameToID[k] = v
	}
	for k, v := range s.schemaNameToID {
		n.schemaNameToID[k] = v
	}
	for k, v := range s.schemas {
		n.schemas[k] = v.Clone()
	}
	for k, v := range s.tables {
		n.tables[k] = v.Clone()
	}
	for k, v := range s.truncateTableID {
		n.truncateTableID[k] = v
	}
	for k, v := range s.ineligibleTableID {
		n.ineligibleTableID[k] = v
	}
	for k, v := range s.version2SchemaTable {
		n.version2SchemaTable[k] = v
	}
	n.schemaMetaVersion = s.schemaMetaVersion
	n.lastHandledTs = s.lastHandledTs
	n.resolvedTs = s.resolvedTs
	n.jobList = s.jobList
	n.currentJob = s.currentJob
	n.currentVersion = s.currentVersion
	return n
}

// IsTruncateTableID returns true if the table id have been truncated by truncate table DDL
func (s *Storage) IsTruncateTableID(id int64) bool {
	_, ok := s.truncateTableID[id]
	return ok
}

// IsIneligibleTableID returns true if the table is ineligible
func (s *Storage) IsIneligibleTableID(id int64) bool {
	_, ok := s.ineligibleTableID[id]
	return ok
}

// FillSchemaName fills the schema name in ddl job
func (s *Storage) FillSchemaName(job *timodel.Job) error {
	var schemaName string
	if job.Type != timodel.ActionCreateSchema {
		dbInfo, exist := s.SchemaByID(job.SchemaID)
		if !exist {
			return errors.NotFoundf("schema %d not found", job.SchemaID)
		}
		schemaName = dbInfo.Name.O
	} else {
		schemaName = job.BinlogInfo.DBInfo.Name.O
	}
	job.SchemaName = schemaName
	return nil
}

// SkipJob skip the job should not be executed
// TiDB write DDL Binlog for every DDL Job, we must ignore jobs that are cancelled or rollback
// For older version TiDB, it write DDL Binlog in the txn that the state of job is changed to *synced*
// Now, it write DDL Binlog in the txn that the state of job is changed to *done* (before change to *synced*)
// At state *done*, it will be always and only changed to *synced*.
func SkipJob(job *timodel.Job) bool {
	switch job.Type {
	case timodel.ActionSetTiFlashReplica, timodel.ActionUpdateTiFlashReplicaStatus:
		return true
	}
	return !job.IsSynced() && !job.IsDone()
}
