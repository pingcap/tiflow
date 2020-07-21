// Copyright 2020 PingCAP, Inc.
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
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/retry"
	timeta "github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/util/rowcodec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// schemaSnapshot stores the source TiDB all schema information
// schemaSnapshot is a READ ONLY struct
type schemaSnapshot struct {
	tableNameToID  map[model.TableName]int64
	schemaNameToID map[string]int64

	schemas        map[int64]*timodel.DBInfo
	tables         map[int64]*TableInfo
	partitionTable map[int64]*TableInfo

	truncateTableID   map[int64]struct{}
	ineligibleTableID map[int64]struct{}

	currentTs uint64
}

// SingleSchemaSnapshot is a single schema snapshot independent of schema storage
type SingleSchemaSnapshot = schemaSnapshot

// HandleDDL handles the ddl job
func (s *SingleSchemaSnapshot) HandleDDL(job *timodel.Job) error {
	return s.handleDDL(job)
}

// NewSingleSchemaSnapshotFromMeta creates a new single schema snapshot from a tidb meta
func NewSingleSchemaSnapshotFromMeta(meta *timeta.Meta, currentTs uint64) (*SingleSchemaSnapshot, error) {
	return newSchemaSnapshotFromMeta(meta, currentTs)
}

func newEmptySchemaSnapshot() *schemaSnapshot {
	return &schemaSnapshot{
		tableNameToID:  make(map[model.TableName]int64),
		schemaNameToID: make(map[string]int64),

		schemas:        make(map[int64]*timodel.DBInfo),
		tables:         make(map[int64]*TableInfo),
		partitionTable: make(map[int64]*TableInfo),

		truncateTableID:   make(map[int64]struct{}),
		ineligibleTableID: make(map[int64]struct{}),
	}
}

func newSchemaSnapshotFromMeta(meta *timeta.Meta, currentTs uint64) (*schemaSnapshot, error) {
	snap := newEmptySchemaSnapshot()
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, dbinfo := range dbinfos {
		snap.schemas[dbinfo.ID] = dbinfo
		snap.schemaNameToID[dbinfo.Name.O] = dbinfo.ID
	}
	for schemaID, dbinfo := range snap.schemas {
		tableInfos, err := meta.ListTables(schemaID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dbinfo.Tables = make([]*timodel.TableInfo, 0, len(tableInfos))
		for _, tableInfo := range tableInfos {
			dbinfo.Tables = append(dbinfo.Tables, tableInfo)
			tableInfo := WrapTableInfo(dbinfo.ID, dbinfo.Name.O, currentTs, tableInfo)
			snap.tables[tableInfo.ID] = tableInfo
			snap.tableNameToID[model.TableName{Schema: dbinfo.Name.O, Table: tableInfo.Name.O}] = tableInfo.ID
			isEligible := tableInfo.IsEligible()
			if !isEligible {
				snap.ineligibleTableID[tableInfo.ID] = struct{}{}
			}
			if pi := tableInfo.GetPartitionInfo(); pi != nil {
				for _, partition := range pi.Definitions {
					snap.partitionTable[partition.ID] = tableInfo
					if !isEligible {
						snap.ineligibleTableID[partition.ID] = struct{}{}
					}
				}
			}
		}
	}
	snap.currentTs = currentTs
	return snap, nil
}

func (s *schemaSnapshot) PrintStatus(logger func(msg string, fields ...zap.Field)) {
	logger("[SchemaSnap] Start to print status", zap.Uint64("currentTs", s.currentTs))
	for id, dbInfo := range s.schemas {
		logger("[SchemaSnap] --> Schemas", zap.Int64("schemaID", id), zap.Reflect("dbInfo", dbInfo))
		// check schemaNameToID
		if schemaID, exist := s.schemaNameToID[dbInfo.Name.O]; !exist || schemaID != id {
			logger("[SchemaSnap] ----> schemaNameToID item lost", zap.String("name", dbInfo.Name.O), zap.Int64("schemaNameToID", s.schemaNameToID[dbInfo.Name.O]))
		}
	}
	if len(s.schemaNameToID) != len(s.schemas) {
		logger("[SchemaSnap] schemaNameToID length mismatch schemas")
		for schemaName, schemaID := range s.schemaNameToID {
			logger("[SchemaSnap] --> schemaNameToID", zap.String("schemaName", schemaName), zap.Int64("schemaID", schemaID))
		}
	}
	for id, tableInfo := range s.tables {
		logger("[SchemaSnap] --> Tables", zap.Int64("tableID", id), zap.Stringer("tableInfo", tableInfo))
		// check tableNameToID
		if tableID, exist := s.tableNameToID[tableInfo.TableName]; !exist || tableID != id {
			logger("[SchemaSnap] ----> tableNameToID item lost", zap.Stringer("name", tableInfo.TableName), zap.Int64("tableNameToID", s.tableNameToID[tableInfo.TableName]))
		}
	}
	if len(s.tableNameToID) != len(s.tables) {
		logger("[SchemaSnap] tableNameToID length mismatch tables")
		for tableName, tableID := range s.tableNameToID {
			logger("[SchemaSnap] --> tableNameToID", zap.Stringer("tableName", tableName), zap.Int64("tableID", tableID))
		}
	}
	for pid, table := range s.partitionTable {
		logger("[SchemaSnap] --> Partitions", zap.Int64("partitionID", pid), zap.Int64("tableID", table.ID))
	}
	truncateTableID := make([]int64, 0, len(s.truncateTableID))
	for id := range s.truncateTableID {
		truncateTableID = append(truncateTableID, id)
	}
	logger("[SchemaSnap] TruncateTableIDs", zap.Int64s("ids", truncateTableID))

	ineligibleTableID := make([]int64, 0, len(s.ineligibleTableID))
	for id := range s.ineligibleTableID {
		ineligibleTableID = append(ineligibleTableID, id)
	}
	logger("[SchemaSnap] IneligibleTableIDs", zap.Int64s("ids", ineligibleTableID))
}

// Clone clones Storage
func (s *schemaSnapshot) Clone() *schemaSnapshot {
	n := &schemaSnapshot{
		tableNameToID:  make(map[model.TableName]int64, len(s.tableNameToID)),
		schemaNameToID: make(map[string]int64, len(s.schemaNameToID)),

		schemas:        make(map[int64]*timodel.DBInfo, len(s.schemas)),
		tables:         make(map[int64]*TableInfo, len(s.tables)),
		partitionTable: make(map[int64]*TableInfo, len(s.partitionTable)),

		truncateTableID:   make(map[int64]struct{}, len(s.truncateTableID)),
		ineligibleTableID: make(map[int64]struct{}, len(s.ineligibleTableID)),
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
	for k, v := range s.partitionTable {
		n.partitionTable[k] = v.Clone()
	}
	for k, v := range s.truncateTableID {
		n.truncateTableID[k] = v
	}
	for k, v := range s.ineligibleTableID {
		n.ineligibleTableID[k] = v
	}
	return n
}

// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	*timodel.TableInfo
	SchemaID         int64
	TableName        model.TableName
	TableInfoVersion uint64
	columnsOffset    map[int64]int
	indicesOffset    map[int64]int
	uniqueColumns    map[int64]struct{}
	handleColID      int64

	// if the table of this row only has one unique index(includes primary key),
	// IndieMarkCol will be set to the name of the unique index
	IndieMarkCol string
	rowColInfos  []rowcodec.ColInfo
}

// WrapTableInfo creates a TableInfo from a timodel.TableInfo
func WrapTableInfo(schemaID int64, schemaName string, version uint64, info *timodel.TableInfo) *TableInfo {
	ti := &TableInfo{
		TableInfo:        info,
		SchemaID:         schemaID,
		TableName:        model.TableName{Schema: schemaName, Table: info.Name.O},
		TableInfoVersion: version,
		columnsOffset:    make(map[int64]int, len(info.Columns)),
		indicesOffset:    make(map[int64]int, len(info.Indices)),
		uniqueColumns:    make(map[int64]struct{}),
		handleColID:      -1,
		rowColInfos:      make([]rowcodec.ColInfo, len(info.Columns)),
	}

	uniqueIndexNum := 0

	for i, col := range ti.Columns {
		ti.columnsOffset[col.ID] = i
		isPK := (ti.PKIsHandle && mysql.HasPriKeyFlag(col.Flag)) || col.ID == timodel.ExtraHandleID
		if isPK {
			ti.handleColID = col.ID
			ti.uniqueColumns[col.ID] = struct{}{}
			uniqueIndexNum++
		}
		ti.rowColInfos[i] = rowcodec.ColInfo{
			ID:         col.ID,
			IsPKHandle: isPK,
			Ft:         rowcodec.FieldTypeFromModelColumn(col),
		}
	}

	for i, idx := range ti.Indices {
		ti.indicesOffset[idx.ID] = i
		if ti.IsIndexUnique(idx) {
			for _, col := range idx.Columns {
				ti.uniqueColumns[ti.Columns[col.Offset].ID] = struct{}{}
			}
		}
		if idx.Primary || idx.Unique {
			uniqueIndexNum++
		}
	}

	// this table has only one unique column
	if uniqueIndexNum == 1 && len(ti.uniqueColumns) == 1 {
		for col := range ti.uniqueColumns {
			info, _ := ti.GetColumnInfo(col)
			if !info.IsGenerated() {
				ti.IndieMarkCol = info.Name.O
			}
		}
	}

	return ti
}

// GetColumnInfo returns the column info by ID
func (ti *TableInfo) GetColumnInfo(colID int64) (info *timodel.ColumnInfo, exist bool) {
	colOffset, exist := ti.columnsOffset[colID]
	if !exist {
		return nil, false
	}
	return ti.Columns[colOffset], true
}

func (ti *TableInfo) String() string {
	return fmt.Sprintf("TableInfo, ID: %d, Name:%s, ColNum: %d, IdxNum: %d, PKIsHandle: %t", ti.ID, ti.TableName, len(ti.Columns), len(ti.Indices), ti.PKIsHandle)
}

// GetIndexInfo returns the index info by ID
func (ti *TableInfo) GetIndexInfo(indexID int64) (info *timodel.IndexInfo, exist bool) {
	indexOffset, exist := ti.indicesOffset[indexID]
	if !exist {
		return nil, false
	}
	return ti.Indices[indexOffset], true
}

// GetRowColInfos returns all column infos for rowcodec
func (ti *TableInfo) GetRowColInfos() (int64, []rowcodec.ColInfo) {
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
	_, exist := ti.uniqueColumns[colID]
	return exist
}

// ExistTableUniqueColumn returns whether the table has the unique column
func (ti *TableInfo) ExistTableUniqueColumn() bool {
	return len(ti.uniqueColumns) != 0
}

// IsEligible returns whether the table is a eligible table
func (ti *TableInfo) IsEligible() bool {
	if ti.IsView() {
		return true
	}
	return ti.ExistTableUniqueColumn()
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
	return WrapTableInfo(ti.SchemaID, ti.TableName.Schema, ti.TableInfoVersion, ti.TableInfo.Clone())
}

// GetTableNameByID looks up a TableName with the given table id
func (s *schemaSnapshot) GetTableNameByID(id int64) (model.TableName, bool) {
	tableInfo, ok := s.tables[id]
	if !ok {
		// Try partition, it could be a partition table.
		partInfo, ok := s.partitionTable[id]
		if !ok {
			return model.TableName{}, false
		}
		// Must exists an table that contains the partition.
		tableInfo = s.tables[partInfo.ID]
	}
	return tableInfo.TableName, true
}

// GetTableIDByName returns the tableID by table schemaName and tableName
func (s *schemaSnapshot) GetTableIDByName(schemaName string, tableName string) (int64, bool) {
	id, ok := s.tableNameToID[model.TableName{
		Schema: schemaName,
		Table:  tableName,
	}]
	return id, ok
}

// GetTableByName queries a table by name,
// the second returned value is false if no table with the specified name is found.
func (s *schemaSnapshot) GetTableByName(schema, table string) (info *TableInfo, ok bool) {
	id, ok := s.GetTableIDByName(schema, table)
	if !ok {
		return nil, ok
	}
	return s.TableByID(id)
}

// SchemaByID returns the DBInfo by schema id
func (s *schemaSnapshot) SchemaByID(id int64) (val *timodel.DBInfo, ok bool) {
	val, ok = s.schemas[id]
	return
}

// SchemaByTableID returns the schema ID by table ID
func (s *schemaSnapshot) SchemaByTableID(tableID int64) (*timodel.DBInfo, bool) {
	tableInfo, ok := s.tables[tableID]
	if !ok {
		return nil, false
	}
	schemaID, ok := s.schemaNameToID[tableInfo.TableName.Schema]
	if !ok {
		return nil, false
	}
	return s.SchemaByID(schemaID)
}

// TableByID returns the TableInfo by table id
func (s *schemaSnapshot) TableByID(id int64) (val *TableInfo, ok bool) {
	val, ok = s.tables[id]
	return
}

// PhysicalTableByID returns the TableInfo by table id or partition ID.
func (s *schemaSnapshot) PhysicalTableByID(id int64) (val *TableInfo, ok bool) {
	val, ok = s.tables[id]
	if !ok {
		val, ok = s.partitionTable[id]
	}
	return
}

// IsTruncateTableID returns true if the table id have been truncated by truncate table DDL
func (s *schemaSnapshot) IsTruncateTableID(id int64) bool {
	_, ok := s.truncateTableID[id]
	return ok
}

// IsIneligibleTableID returns true if the table is ineligible
func (s *schemaSnapshot) IsIneligibleTableID(id int64) bool {
	_, ok := s.ineligibleTableID[id]
	return ok
}

// FillSchemaName fills the schema name in ddl job
func (s *schemaSnapshot) FillSchemaName(job *timodel.Job) error {
	if job.Type == timodel.ActionCreateSchema ||
		job.Type == timodel.ActionDropSchema {
		job.SchemaName = job.BinlogInfo.DBInfo.Name.O
		return nil
	}
	dbInfo, exist := s.SchemaByID(job.SchemaID)
	if !exist {
		return errors.NotFoundf("schema %d not found", job.SchemaID)
	}
	job.SchemaName = dbInfo.Name.O
	return nil
}

func (s *schemaSnapshot) dropSchema(id int64) error {
	schema, ok := s.schemas[id]
	if !ok {
		return errors.NotFoundf("schema %d", id)
	}

	for _, table := range schema.Tables {
		if pi := table.GetPartitionInfo(); pi != nil {
			for _, partition := range pi.Definitions {
				delete(s.partitionTable, partition.ID)
			}
		}
		tableName := s.tables[table.ID].TableName
		if pi := table.GetPartitionInfo(); pi != nil {
			for _, partition := range pi.Definitions {
				delete(s.partitionTable, partition.ID)
			}
		}
		delete(s.tables, table.ID)
		delete(s.tableNameToID, tableName)
	}

	delete(s.schemas, id)
	delete(s.schemaNameToID, schema.Name.O)

	return nil
}

func (s *schemaSnapshot) createSchema(db *timodel.DBInfo) error {
	if _, ok := s.schemas[db.ID]; ok {
		return errors.AlreadyExistsf("schema %s(%d)", db.Name, db.ID)
	}

	s.schemas[db.ID] = db.Clone()
	s.schemaNameToID[db.Name.O] = db.ID

	log.Debug("create schema success, schema id", zap.String("name", db.Name.O), zap.Int64("id", db.ID))
	return nil
}

func (s *schemaSnapshot) replaceSchema(db *timodel.DBInfo) error {
	if _, ok := s.schemas[db.ID]; !ok {
		return errors.NotFoundf("schema %s(%d)", db.Name, db.ID)
	}
	s.schemas[db.ID] = db.Clone()
	s.schemaNameToID[db.Name.O] = db.ID
	return nil
}

func (s *schemaSnapshot) dropTable(id int64) error {
	table, ok := s.tables[id]
	if !ok {
		return errors.NotFoundf("table %d", id)
	}
	schema, ok := s.SchemaByTableID(id)
	if !ok {
		return errors.NotFoundf("table(%d)'s schema", id)
	}

	for i := range schema.Tables {
		if schema.Tables[i].ID == id {
			copy(schema.Tables[i:], schema.Tables[i+1:])
			schema.Tables = schema.Tables[:len(schema.Tables)-1]
			break
		}
	}

	tableName := s.tables[id].TableName
	delete(s.tables, id)
	if pi := table.GetPartitionInfo(); pi != nil {
		for _, partition := range pi.Definitions {
			delete(s.partitionTable, partition.ID)
			delete(s.ineligibleTableID, partition.ID)
		}
	}
	delete(s.tableNameToID, tableName)
	delete(s.ineligibleTableID, id)

	log.Debug("drop table success", zap.String("name", table.Name.O), zap.Int64("id", id))
	return nil
}

func (s *schemaSnapshot) updatePartition(tbl *TableInfo) error {
	id := tbl.ID
	table, ok := s.tables[id]
	if !ok {
		return errors.NotFoundf("table %d", id)
	}
	oldPi := table.GetPartitionInfo()
	if oldPi == nil {
		return errors.NotFoundf("table %d is not a partition table, truncate partition failed", id)
	}
	oldIDs := make(map[int64]struct{}, len(oldPi.Definitions))
	for _, p := range oldPi.Definitions {
		oldIDs[p.ID] = struct{}{}
	}

	newPi := tbl.GetPartitionInfo()
	if newPi == nil {
		return errors.NotFoundf("table %d is not a partition table, truncate partition failed", id)
	}
	s.tables[id] = tbl
	for _, partition := range newPi.Definitions {
		// update table info.
		if _, ok := s.partitionTable[partition.ID]; ok {
			log.Debug("add table partition success", zap.String("name", tbl.Name.O), zap.Int64("tid", id), zap.Reflect("add partition id", partition.ID))
		}
		s.partitionTable[partition.ID] = tbl
		if !tbl.ExistTableUniqueColumn() {
			s.ineligibleTableID[partition.ID] = struct{}{}
		}
		delete(oldIDs, partition.ID)
	}

	// drop old partition.
	for pid := range oldIDs {
		s.truncateTableID[pid] = struct{}{}
		delete(s.partitionTable, pid)
		delete(s.ineligibleTableID, pid)
		log.Debug("drop table partition success", zap.String("name", tbl.Name.O), zap.Int64("tid", id), zap.Reflect("truncated partition id", pid))
	}

	return nil
}

func (s *schemaSnapshot) createTable(table *TableInfo) error {
	schema, ok := s.schemas[table.SchemaID]
	if !ok {
		return errors.NotFoundf("table's schema(%d)", table.SchemaID)
	}
	_, ok = s.tables[table.ID]
	if ok {
		return errors.AlreadyExistsf("table %s.%s", schema.Name, table.Name)
	}

	schema.Tables = append(schema.Tables, table.TableInfo)

	s.tables[table.ID] = table
	if !table.IsEligible() {
		log.Warn("this table is not eligible to replicate", zap.String("tableName", table.Name.O), zap.Int64("tableID", table.ID))
		s.ineligibleTableID[table.ID] = struct{}{}
	}
	if pi := table.GetPartitionInfo(); pi != nil {
		for _, partition := range pi.Definitions {
			s.partitionTable[partition.ID] = table
			if !table.IsEligible() {
				s.ineligibleTableID[partition.ID] = struct{}{}
			}
		}
	}
	s.tableNameToID[table.TableName] = table.ID

	log.Debug("create table success", zap.String("name", schema.Name.O+"."+table.Name.O), zap.Int64("id", table.ID))
	return nil
}

// ReplaceTable replace the table by new tableInfo
func (s *schemaSnapshot) replaceTable(table *TableInfo) error {
	_, ok := s.tables[table.ID]
	if !ok {
		return errors.NotFoundf("table %s(%d)", table.Name, table.ID)
	}
	s.tables[table.ID] = table
	if !table.IsEligible() {
		log.Warn("this table is not eligible to replicate", zap.String("tableName", table.Name.O), zap.Int64("tableID", table.ID))
		s.ineligibleTableID[table.ID] = struct{}{}
	}
	if pi := table.GetPartitionInfo(); pi != nil {
		for _, partition := range pi.Definitions {
			s.partitionTable[partition.ID] = table
			if !table.IsEligible() {
				s.ineligibleTableID[partition.ID] = struct{}{}
			}
		}
	}

	return nil
}

func (s *schemaSnapshot) handleDDL(job *timodel.Job) error {
	if err := s.FillSchemaName(job); err != nil {
		return errors.Trace(err)
	}
	log.Debug("handle job: ", zap.String("sql query", job.Query), zap.Stringer("job", job))
	getWrapTableInfo := func(job *timodel.Job) *TableInfo {
		return WrapTableInfo(job.SchemaID, job.SchemaName,
			job.BinlogInfo.FinishedTS,
			job.BinlogInfo.TableInfo.Clone())
	}
	switch job.Type {
	case timodel.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		err := s.createSchema(job.BinlogInfo.DBInfo)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionModifySchemaCharsetAndCollate:
		err := s.replaceSchema(job.BinlogInfo.DBInfo)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionDropSchema:
		err := s.dropSchema(job.SchemaID)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionRenameTable:
		// first drop the table
		err := s.dropTable(job.TableID)
		if err != nil {
			return errors.Trace(err)
		}
		// create table
		err = s.createTable(getWrapTableInfo(job))
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionCreateTable, timodel.ActionCreateView, timodel.ActionRecoverTable:
		err := s.createTable(getWrapTableInfo(job))
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionDropTable, timodel.ActionDropView:
		err := s.dropTable(job.TableID)
		if err != nil {
			return errors.Trace(err)
		}

	case timodel.ActionTruncateTable:
		// job.TableID is the old table id, different from table.ID
		err := s.dropTable(job.TableID)
		if err != nil {
			return errors.Trace(err)
		}

		err = s.createTable(getWrapTableInfo(job))
		if err != nil {
			return errors.Trace(err)
		}

		s.truncateTableID[job.TableID] = struct{}{}
	case timodel.ActionTruncateTablePartition, timodel.ActionAddTablePartition, timodel.ActionDropTablePartition:
		err := s.updatePartition(getWrapTableInfo(job))
		if err != nil {
			return errors.Trace(err)
		}
	default:
		binlogInfo := job.BinlogInfo
		if binlogInfo == nil {
			log.Warn("ignore a invalid DDL job", zap.Reflect("job", job))
			return nil
		}
		tbInfo := binlogInfo.TableInfo
		if tbInfo == nil {
			log.Warn("ignore a invalid DDL job", zap.Reflect("job", job))
			return nil
		}
		err := s.replaceTable(getWrapTableInfo(job))
		if err != nil {
			return errors.Trace(err)
		}
	}
	s.currentTs = job.BinlogInfo.FinishedTS
	return nil
}

// CloneTables return a clone of the existing tables.
func (s *schemaSnapshot) CloneTables() map[model.TableID]model.TableName {
	mp := make(map[model.TableID]model.TableName, len(s.tables))

	for id, table := range s.tables {
		mp[id] = table.TableName
	}

	return mp
}

// SchemaStorage stores the schema information with multi-version
type SchemaStorage struct {
	snaps      []*schemaSnapshot
	snapsMu    sync.RWMutex
	gcTs       uint64
	resolvedTs uint64

	filter *filter.Filter
}

// NewSchemaStorage creates a new schema storage
func NewSchemaStorage(meta *timeta.Meta, startTs uint64, filter *filter.Filter) (*SchemaStorage, error) {
	var snap *schemaSnapshot
	var err error
	if meta == nil {
		snap = newEmptySchemaSnapshot()
	} else {
		snap, err = newSchemaSnapshotFromMeta(meta, startTs)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	schema := &SchemaStorage{
		snaps:      []*schemaSnapshot{snap},
		resolvedTs: startTs,
		filter:     filter,
	}
	return schema, nil
}

func (s *SchemaStorage) getSnapshot(ts uint64) (*schemaSnapshot, error) {
	gcTs := atomic.LoadUint64(&s.gcTs)
	if ts < gcTs {
		return nil, errors.Errorf("can not found schema snapshot, the specified ts(%d) is less than gcTS(%d)", ts, gcTs)
	}
	resolvedTs := atomic.LoadUint64(&s.resolvedTs)
	if ts > resolvedTs {
		return nil, errors.Annotatef(model.ErrUnresolved, "can not found schema snapshot, the specified ts(%d) is more than resolvedTs(%d)", ts, resolvedTs)
	}
	s.snapsMu.RLock()
	defer s.snapsMu.RUnlock()
	i := sort.Search(len(s.snaps), func(i int) bool {
		return s.snaps[i].currentTs > ts
	})
	if i <= 0 {
		return nil, errors.Errorf("can not found schema snapshot, ts: %d", ts)
	}
	return s.snaps[i-1], nil
}

// GetSnapshot returns the snapshot which of ts is specified
func (s *SchemaStorage) GetSnapshot(ctx context.Context, ts uint64) (*schemaSnapshot, error) {
	var snap *schemaSnapshot
	err := retry.Run(10*time.Millisecond, 25,
		func() error {
			select {
			case <-ctx.Done():
				return backoff.Permanent(errors.Trace(ctx.Err()))
			default:
			}
			var err error
			snap, err = s.getSnapshot(ts)
			if errors.Cause(err) != model.ErrUnresolved {
				return backoff.Permanent(err)
			}
			return err
		})
	switch err.(type) {
	case *backoff.PermanentError:
		return nil, errors.Annotate(err, "timeout")
	default:
		return snap, err
	}
}

// GetLastSnapshot returns the last snapshot
func (s *SchemaStorage) GetLastSnapshot() *schemaSnapshot {
	s.snapsMu.RLock()
	defer s.snapsMu.RUnlock()
	return s.snaps[len(s.snaps)-1]
}

// HandleDDLJob creates a new snapshot in storage and handles the ddl job
func (s *SchemaStorage) HandleDDLJob(job *timodel.Job) error {
	if s.skipJob(job) {
		s.AdvanceResolvedTs(job.BinlogInfo.FinishedTS)
		return nil
	}
	s.snapsMu.Lock()
	defer s.snapsMu.Unlock()
	var snap *schemaSnapshot
	if len(s.snaps) > 0 {
		lastSnap := s.snaps[len(s.snaps)-1]
		if job.BinlogInfo.FinishedTS <= lastSnap.currentTs {
			log.Debug("ignore foregone DDL job", zap.Reflect("job", job))
			return nil
		}
		snap = lastSnap.Clone()
	} else {
		snap = newEmptySchemaSnapshot()
	}
	if err := snap.handleDDL(job); err != nil {
		return errors.Trace(err)
	}
	s.snaps = append(s.snaps, snap)
	s.AdvanceResolvedTs(job.BinlogInfo.FinishedTS)
	return nil
}

// AdvanceResolvedTs advances the resolved
func (s *SchemaStorage) AdvanceResolvedTs(ts uint64) {
	var swapped bool
	for !swapped {
		oldResolvedTs := atomic.LoadUint64(&s.resolvedTs)
		if ts < oldResolvedTs {
			return
		}
		swapped = atomic.CompareAndSwapUint64(&s.resolvedTs, oldResolvedTs, ts)
	}
}

// DoGC removes snaps which of ts less than this specified ts
func (s *SchemaStorage) DoGC(ts uint64) {
	s.snapsMu.Lock()
	defer s.snapsMu.Unlock()
	var startIdx int
	for i, snap := range s.snaps {
		if snap.currentTs > ts {
			break
		}
		startIdx = i
	}
	if startIdx == 0 {
		return
	}
	if log.GetLevel() == zapcore.DebugLevel {
		log.Debug("Do GC in schema storage")
		for i := 0; i < startIdx; i++ {
			s.snaps[i].PrintStatus(log.Debug)
		}
	}
	s.snaps = s.snaps[startIdx:]
	atomic.StoreUint64(&s.gcTs, s.snaps[0].currentTs)
	log.Info("finished gc in schema storage", zap.Uint64("gcTs", s.snaps[0].currentTs))
}

// SkipJob skip the job should not be executed
// TiDB write DDL Binlog for every DDL Job, we must ignore jobs that are cancelled or rollback
// For older version TiDB, it write DDL Binlog in the txn that the state of job is changed to *synced*
// Now, it write DDL Binlog in the txn that the state of job is changed to *done* (before change to *synced*)
// At state *done*, it will be always and only changed to *synced*.
func (s *SchemaStorage) skipJob(job *timodel.Job) bool {
	if s.filter != nil && s.filter.ShouldDiscardDDL(job.Type) {
		log.Info("discard the ddl job", zap.Int64("jobID", job.ID), zap.String("query", job.Query))
		return true
	}
	return !job.IsSynced() && !job.IsDone()
}
