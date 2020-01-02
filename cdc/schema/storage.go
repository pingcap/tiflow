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

package schema

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"go.uber.org/zap"
)

// Storage stores the source TiDB all schema infomations
// schema infomations could be changed by drainer init and ddls appear
type Storage struct {
	tableIDToName  map[int64]TableName
	tableNameToID  map[TableName]int64
	schemaNameToID map[string]int64

	schemas map[int64]*model.DBInfo
	tables  map[int64]*TableInfo

	truncateTableID map[int64]struct{}

	schemaMetaVersion int64
	lastHandledTs     uint64

	jobs                []*model.Job
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
	*model.TableInfo
	ColumnsOffset map[int64]int
	IndicesOffset map[int64]int
}

// WrapTableInfo creates a TableInfo from a model.TableInfo
func WrapTableInfo(info *model.TableInfo) *TableInfo {
	columnsOffset := make(map[int64]int, len(info.Columns))
	for i, col := range info.Columns {
		columnsOffset[col.ID] = i
	}
	indicesOffset := make(map[int64]int, len(info.Indices))
	for i, idx := range info.Indices {
		indicesOffset[idx.ID] = i
	}
	return &TableInfo{
		TableInfo:     info,
		ColumnsOffset: columnsOffset,
		IndicesOffset: indicesOffset,
	}
}

// GetColumnInfo returns the column info by ID
func (ti *TableInfo) GetColumnInfo(colID int64) (info *model.ColumnInfo, exist bool) {
	colOffset, exist := ti.ColumnsOffset[colID]
	if !exist {
		return nil, false
	}
	return ti.Columns[colOffset], true
}

// GetIndexInfo returns the index info by ID
func (ti *TableInfo) GetIndexInfo(indexID int64) (info *model.IndexInfo, exist bool) {
	indexOffset, exist := ti.IndicesOffset[indexID]
	if !exist {
		return nil, false
	}
	return ti.Indices[indexOffset], true
}

// WritableColumns returns all public and non-generated columns
func (ti *TableInfo) WritableColumns() []*model.ColumnInfo {
	cols := make([]*model.ColumnInfo, 0, len(ti.Columns))
	for _, col := range ti.Columns {
		if col.State == model.StatePublic && !col.IsGenerated() {
			cols = append(cols, col)
		}
	}
	return cols
}

// GetUniqueKeys returns all unique keys of the table as a slice of column names
func (ti *TableInfo) GetUniqueKeys() [][]string {
	var uniqueKeys [][]string
	for _, idx := range ti.Indices {
		if idx.Primary || idx.Unique {
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
	if ti.PKIsHandle {
		for _, col := range ti.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				// Prepend to make sure the primary key ends up at the front
				uniqueKeys = append([][]string{{col.Name.O}}, uniqueKeys...)
				break
			}
		}
	}
	return uniqueKeys
}

// NewStorage returns the Schema object
func NewStorage(jobs []*model.Job) (*Storage, error) {
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].BinlogInfo.FinishedTS < jobs[j].BinlogInfo.FinishedTS
	})

	s := &Storage{
		version2SchemaTable: make(map[int64]TableName),
		truncateTableID:     make(map[int64]struct{}),
		jobs:                jobs,
	}

	s.tableIDToName = make(map[int64]TableName)
	s.tableNameToID = make(map[TableName]int64)
	s.schemas = make(map[int64]*model.DBInfo)
	s.schemaNameToID = make(map[string]int64)
	s.tables = make(map[int64]*TableInfo)

	return s, nil
}

// String implements fmt.Stringer interface.
func (s *Storage) String() string {
	mp := map[string]interface{}{
		"tableIDToName":  s.tableIDToName,
		"tableNameToID":  s.tableNameToID,
		"schemaNameToID": s.schemaNameToID,
		// "schemas":           s.schemas,
		// "tables":            s.tables,
		"schemaMetaVersion": s.schemaMetaVersion,
	}

	data, _ := json.MarshalIndent(mp, "\t", "\t")

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
func (s *Storage) SchemaByID(id int64) (val *model.DBInfo, ok bool) {
	val, ok = s.schemas[id]
	return
}

// SchemaByTableID returns the schema ID by table ID
func (s *Storage) SchemaByTableID(tableID int64) (*model.DBInfo, bool) {
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
func (s *Storage) CreateSchema(db *model.DBInfo) error {
	if _, ok := s.schemas[db.ID]; ok {
		return errors.AlreadyExistsf("schema %s(%d)", db.Name, db.ID)
	}

	s.schemas[db.ID] = db
	s.schemaNameToID[db.Name.O] = db.ID

	log.Debug("create schema failed, schema id", zap.String("name", db.Name.O), zap.Int64("id", db.ID))
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

	log.Debug("drop table success", zap.String("name", table.Name.O), zap.Int64("id", id))
	return table.Name.O, nil
}

// CreateTable creates new TableInfo
func (s *Storage) CreateTable(schema *model.DBInfo, table *model.TableInfo) error {
	_, ok := s.tables[table.ID]
	if ok {
		return errors.AlreadyExistsf("table %s.%s", schema.Name, table.Name)
	}

	schema.Tables = append(schema.Tables, table)
	s.tables[table.ID] = WrapTableInfo(table)
	s.tableIDToName[table.ID] = TableName{Schema: schema.Name.O, Table: table.Name.O}
	s.tableNameToID[s.tableIDToName[table.ID]] = table.ID

	log.Debug("create table success", zap.String("name", schema.Name.O+"."+table.Name.O), zap.Int64("id", table.ID))
	return nil
}

// ReplaceTable replace the table by new tableInfo
func (s *Storage) ReplaceTable(table *model.TableInfo) error {
	_, ok := s.tables[table.ID]
	if !ok {
		return errors.NotFoundf("table %s(%d)", table.Name, table.ID)
	}

	s.tables[table.ID] = WrapTableInfo(table)

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

// AddJob adds a DDL job to the schema storage
func (s *Storage) AddJob(job *model.Job) {
	if len(s.jobs) == 0 || s.jobs[len(s.jobs)-1].BinlogInfo.FinishedTS < job.BinlogInfo.FinishedTS {
		s.jobs = append(s.jobs, job)
		return
	}

	log.Debug("skip job in AddJob")
}

// HandlePreviousDDLJobIfNeed apply all jobs with FinishedTS less or equals `commitTs`.
func (s *Storage) HandlePreviousDDLJobIfNeed(commitTs uint64) error {
	var i int
	var job *model.Job
	for i, job = range s.jobs {
		if skipJob(job) {
			log.Info("skip DDL job because the job isn't synced and done", zap.Stringer("job", job))
			continue
		}

		if job.BinlogInfo.FinishedTS > commitTs {
			break
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

	s.jobs = s.jobs[i:]

	return nil
}

// HandleDDL has four return values,
// the first value[string]: the schema name
// the second value[string]: the table name
// the third value[string]: the sql that is corresponding to the job
// the fourth value[error]: the handleDDL execution's err
func (s *Storage) HandleDDL(job *model.Job) (schemaName string, tableName string, sql string, err error) {
	log.Debug("handle job: ", zap.String("sql query", job.Query), zap.Stringer("job", job))

	if skipJob(job) {
		return "", "", "", nil
	}

	sql = job.Query
	if sql == "" {
		return "", "", "", errors.Errorf("[ddl job sql miss]%+v", job)
	}

	switch job.Type {
	case model.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		schema := job.BinlogInfo.DBInfo

		err := s.CreateSchema(schema)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schema.Name.O, Table: ""}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = schema.Name.O

	case model.ActionModifySchemaCharsetAndCollate:
		db := job.BinlogInfo.DBInfo
		if _, ok := s.schemas[db.ID]; !ok {
			return "", "", "", errors.NotFoundf("schema %s(%d)", db.Name, db.ID)
		}

		s.schemas[db.ID] = db
		s.schemaNameToID[db.Name.O] = db.ID
		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: db.Name.O, Table: ""}
		s.currentVersion = job.BinlogInfo.SchemaVersion
		schemaName = db.Name.O

	case model.ActionDropSchema:
		schemaName, err = s.DropSchema(job.SchemaID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		s.version2SchemaTable[job.BinlogInfo.SchemaVersion] = TableName{Schema: schemaName, Table: ""}
		s.currentVersion = job.BinlogInfo.SchemaVersion

	case model.ActionRenameTable:
		// ignore schema doesn't support reanme ddl
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
		table := job.BinlogInfo.TableInfo
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

	case model.ActionCreateTable, model.ActionCreateView, model.ActionRecoverTable:
		table := job.BinlogInfo.TableInfo
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

	case model.ActionDropTable, model.ActionDropView:
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

	case model.ActionTruncateTable:
		schema, ok := s.SchemaByID(job.SchemaID)
		if !ok {
			return "", "", "", errors.NotFoundf("schema %d", job.SchemaID)
		}

		// job.TableID is the old table id, different from table.ID
		_, err := s.DropTable(job.TableID)
		if err != nil {
			return "", "", "", errors.Trace(err)
		}

		table := job.BinlogInfo.TableInfo
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

// IsTruncateTableID returns true if the table id have been truncated by truncate table DDL
func (s *Storage) IsTruncateTableID(id int64) bool {
	_, ok := s.truncateTableID[id]
	return ok
}

// TiDB write DDL Binlog for every DDL Job, we must ignore jobs that are cancelled or rollback
// For older version TiDB, it write DDL Binlog in the txn that the state of job is changed to *synced*
// Now, it write DDL Binlog in the txn that the state of job is changed to *done* (before change to *synced*)
// At state *done*, it will be always and only changed to *synced*.
func skipJob(job *model.Job) bool {
	return !job.IsSynced() && !job.IsDone()
}
