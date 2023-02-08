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

package schema

import (
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/ddl"
	timeta "github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// Snapshot stores the source TiDB all schema information.
// If no special comments, all public methods are thread-safe.
type Snapshot struct {
	inner  snapshot
	rwlock *sync.RWMutex
}

// PreTableInfo returns the table info which will be overwritten by the specified job
func (s *Snapshot) PreTableInfo(job *timodel.Job) (*model.TableInfo, error) {
	switch job.Type {
	case timodel.ActionCreateSchema, timodel.ActionModifySchemaCharsetAndCollate, timodel.ActionDropSchema:
		return nil, nil
	case timodel.ActionCreateTable, timodel.ActionCreateView, timodel.ActionRecoverTable:
		// no pre table info
		return nil, nil
	case timodel.ActionRenameTable, timodel.ActionDropTable, timodel.ActionDropView, timodel.ActionTruncateTable:
		// get the table will be dropped
		table, ok := s.PhysicalTableByID(job.TableID)
		if !ok {
			return nil, cerror.ErrSchemaStorageTableMiss.GenWithStackByArgs(job.TableID)
		}
		return table, nil
	case timodel.ActionRenameTables:
		// DDL on multiple tables, ignore pre table info
		return nil, nil
	default:
		binlogInfo := job.BinlogInfo
		if binlogInfo == nil {
			log.Warn("ignore a invalid DDL job", zap.Any("job", job))
			return nil, nil
		}
		tbInfo := binlogInfo.TableInfo
		if tbInfo == nil {
			log.Warn("ignore a invalid DDL job", zap.Any("job", job))
			return nil, nil
		}
		tableID := tbInfo.ID
		table, ok := s.PhysicalTableByID(tableID)
		if !ok {
			return nil, cerror.ErrSchemaStorageTableMiss.GenWithStackByArgs(job.TableID)
		}
		return table, nil
	}
}

// FillSchemaName fills the schema name in ddl job.
func (s *Snapshot) FillSchemaName(job *timodel.Job) error {
	if job.Type == timodel.ActionRenameTables {
		// DDLs on multiple schema or tables, ignore them.
		return nil
	}
	if job.Type == timodel.ActionCreateSchema ||
		job.Type == timodel.ActionDropSchema {
		job.SchemaName = job.BinlogInfo.DBInfo.Name.O
		return nil
	}
	dbInfo, exist := s.SchemaByID(job.SchemaID)
	if !exist {
		return cerror.ErrSnapshotSchemaNotFound.GenWithStackByArgs(job.SchemaID)
	}
	job.SchemaName = dbInfo.Name.O
	return nil
}

// GetSchemaVersion returns the schema version of the meta.
func GetSchemaVersion(meta *timeta.Meta) (int64, error) {
	// After we get the schema version at startTs, if the diff corresponding to that version does not exist,
	// it means that the job is not committed yet, so we should subtract one from the version, i.e., version--.
	version, err := meta.GetSchemaVersion()
	if err != nil {
		return 0, errors.Trace(err)
	}
	diff, err := meta.GetSchemaDiff(version)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if diff == nil {
		version--
	}
	return version, nil
}

// NewSingleSnapshotFromMeta creates a new single schema snapshot from a tidb meta
func NewSingleSnapshotFromMeta(meta *timeta.Meta, currentTs uint64, forceReplicate bool) (*Snapshot, error) {
	// meta is nil only in unit tests
	if meta == nil {
		snap := NewEmptySnapshot(forceReplicate)
		snap.InitPreExistingTables()
		snap.inner.currentTs = currentTs
		return snap, nil
	}
	return NewSnapshotFromMeta(meta, currentTs, forceReplicate)
}

// NewSnapshotFromMeta creates a schema snapshot from meta.
func NewSnapshotFromMeta(meta *timeta.Meta, currentTs uint64, forceReplicate bool) (*Snapshot, error) {
	snap := NewEmptySnapshot(forceReplicate)
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMetaListDatabases, err)
	}
	// `tag` is used to reverse sort all versions in the generated snapshot.
	tag := negative(currentTs)

	for _, dbinfo := range dbinfos {
		vid := newVersionedID(dbinfo.ID, tag)
		vid.target = dbinfo
		snap.inner.schemas.ReplaceOrInsert(vid)

		vname := newVersionedEntityName(-1, dbinfo.Name.O, tag) // -1 means the entity is a schema.
		vname.target = dbinfo.ID
		snap.inner.schemaNameToID.ReplaceOrInsert(vname)

		tableInfos, err := meta.ListTables(dbinfo.ID)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMetaListDatabases, err)
		}
		for _, tableInfo := range tableInfos {
			tableInfo := model.WrapTableInfo(dbinfo.ID, dbinfo.Name.O, currentTs, tableInfo)
			snap.inner.tables.ReplaceOrInsert(versionedID{
				id:     tableInfo.ID,
				tag:    tag,
				target: tableInfo,
			})
			snap.inner.tableNameToID.ReplaceOrInsert(versionedEntityName{
				prefix: dbinfo.ID,
				entity: tableInfo.Name.O,
				tag:    tag,
				target: tableInfo.ID,
			})

			ineligible := !tableInfo.IsEligible(forceReplicate)
			if ineligible {
				snap.inner.ineligibleTables.ReplaceOrInsert(versionedID{id: tableInfo.ID, tag: tag})
			}
			if pi := tableInfo.GetPartitionInfo(); pi != nil {
				for _, partition := range pi.Definitions {
					vid := newVersionedID(partition.ID, tag)
					vid.target = tableInfo
					snap.inner.partitions.ReplaceOrInsert(vid)
					if ineligible {
						snap.inner.ineligibleTables.ReplaceOrInsert(versionedID{id: partition.ID, tag: tag})
					}
				}
			}
		}
	}

	snap.inner.currentTs = currentTs
	return snap, nil
}

// NewEmptySnapshot creates an empty schema snapshot.
func NewEmptySnapshot(forceReplicate bool) *Snapshot {
	inner := snapshot{
		tableNameToID:    btree.NewG[versionedEntityName](16, versionedEntityNameLess),
		schemaNameToID:   btree.NewG[versionedEntityName](16, versionedEntityNameLess),
		schemas:          btree.NewG[versionedID](16, versionedIDLess),
		tables:           btree.NewG[versionedID](16, versionedIDLess),
		partitions:       btree.NewG[versionedID](16, versionedIDLess),
		truncatedTables:  btree.NewG[versionedID](16, versionedIDLess),
		ineligibleTables: btree.NewG[versionedID](16, versionedIDLess),
		forceReplicate:   forceReplicate,
		currentTs:        0,
	}

	return &Snapshot{inner: inner, rwlock: new(sync.RWMutex)}
}

// these constants imitate TiDB's session.InitDDLJobTables in an empty Snapshot.
const (
	mysqlDBID      = int64(1)
	dummyTS        = uint64(1)
	mdlCreateTable = "create table mysql.tidb_mdl_info(job_id BIGINT NOT NULL PRIMARY KEY, version BIGINT NOT NULL, table_ids text(65535));"
)

// InitPreExistingTables initializes the pre-existing tables in an empty Snapshot.
// Since v6.2.0, tables of concurrent DDL will be directly written as meta KV in
// TiKV, without being written to history DDL jobs. So the Snapshot which is not
// build from meta needs this method to handle history DDL.
// Since v6.5.0, Backfill tables is written as meta KV in TiKV, so the Snapshot
// which is not build from meta needs this method to handle history DDL.
// See:https://github.com/pingcap/tidb/pull/39616
func (s *Snapshot) InitPreExistingTables() {
	ddlJobTableIDs := [...]int64{ddl.JobTableID, ddl.ReorgTableID, ddl.HistoryTableID}
	backfillTableIDs := [...]int64{ddl.BackfillTableID, ddl.BackfillHistoryTableID}

	mysqlDBInfo := &timodel.DBInfo{
		ID:      mysqlDBID,
		Name:    timodel.NewCIStr(mysql.SystemDB),
		Charset: mysql.UTF8MB4Charset,
		Collate: mysql.UTF8MB4DefaultCollation,
		State:   timodel.StatePublic,
	}
	_ = s.inner.createSchema(mysqlDBInfo, dummyTS)

	p := parser.New()
	for i, table := range session.DDLJobTables {
		stmt, _ := p.ParseOneStmt(table.SQL, "", "")
		tblInfo, _ := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
		tblInfo.State = timodel.StatePublic
		tblInfo.ID = ddlJobTableIDs[i]
		wrapped := model.WrapTableInfo(mysqlDBID, mysql.SystemDB, dummyTS, tblInfo)
		_ = s.inner.createTable(wrapped, dummyTS)
	}

	for i, table := range session.BackfillTables {
		stmt, _ := p.ParseOneStmt(table.SQL, "", "")
		tblInfo, _ := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
		tblInfo.State = timodel.StatePublic
		tblInfo.ID = backfillTableIDs[i]
		wrapped := model.WrapTableInfo(mysqlDBID, mysql.SystemDB, dummyTS, tblInfo)
		_ = s.inner.createTable(wrapped, dummyTS)
	}

	stmt, _ := p.ParseOneStmt(mdlCreateTable, "", "")
	tblInfo, _ := ddl.BuildTableInfoFromAST(stmt.(*ast.CreateTableStmt))
	tblInfo.State = timodel.StatePublic
	tblInfo.ID = ddl.MDLTableID
	wrapped := model.WrapTableInfo(mysqlDBID, mysql.SystemDB, dummyTS, tblInfo)
	_ = s.inner.createTable(wrapped, dummyTS)
}

// Copy creates a new schema snapshot based on the given one. The copied one shares same internal
// data structures with the old one to save memory usage.
func (s *Snapshot) Copy() *Snapshot {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	return &Snapshot{inner: s.inner, rwlock: s.rwlock}
}

// PrintStatus prints the schema snapshot.
func (s *Snapshot) PrintStatus(logger func(msg string, fields ...zap.Field)) {
	logger("[SchemaSnap] Start to print status", zap.Uint64("currentTs", s.CurrentTs()))

	availableSchemas := make(map[int64]string, s.inner.schemas.Len())
	s.IterSchemas(func(dbInfo *timodel.DBInfo) {
		availableSchemas[dbInfo.ID] = dbInfo.Name.O
		logger("[SchemaSnap] --> Schemas", zap.Int64("schemaID", dbInfo.ID), zap.Reflect("dbInfo", dbInfo))
		// check schemaNameToID
		id, ok := s.inner.schemaIDByName(dbInfo.Name.O)
		if !ok || id != dbInfo.ID {
			logger("[SchemaSnap] ----> schemaNameToID item lost", zap.String("name", dbInfo.Name.O), zap.Int64("schemaNameToID", id))
		}
	})
	s.IterSchemaNames(func(schema string, target int64) {
		if _, ok := availableSchemas[target]; !ok {
			logger("[SchemaSnap] ----> schemas item lost", zap.String("name", schema), zap.Int64("schema", target))
		}
	})

	availableTables := make(map[int64]struct{}, s.inner.tables.Len())
	s.IterTables(true, func(tableInfo *model.TableInfo) {
		availableTables[tableInfo.ID] = struct{}{}
		logger("[SchemaSnap] --> Tables", zap.Int64("tableID", tableInfo.ID),
			zap.Stringer("tableInfo", tableInfo),
			zap.Bool("ineligible", s.inner.isIneligibleTableID(tableInfo.ID)))
		id, ok := s.inner.tableIDByName(tableInfo.TableName.Schema, tableInfo.TableName.Table)
		if !ok || id != tableInfo.ID {
			logger("[SchemaSnap] ----> tableNameToID item lost", zap.Stringer("name", tableInfo.TableName), zap.Int64("tableNameToID", id))
		}
	})
	s.IterTableNames(func(schemaID int64, table string, target int64) {
		if _, ok := availableTables[target]; !ok {
			name := fmt.Sprintf("%s.%s", availableSchemas[schemaID], table)
			logger("[SchemaSnap] ----> tables item lost", zap.String("name", name), zap.Int64("table", target))
		}
	})

	s.IterPartitions(true, func(pid int64, table *model.TableInfo) {
		logger("[SchemaSnap] --> Partitions", zap.Int64("partitionID", pid), zap.Int64("tableID", table.ID),
			zap.Bool("ineligible", s.inner.isIneligibleTableID(pid)))
	})
}

// IterSchemas iterates all schemas in the snapshot.
func (s *Snapshot) IterSchemas(f func(i *timodel.DBInfo)) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	s.inner.iterSchemas(f)
}

// IterSchemaNames iterates all schema names in the snapshot.
func (s *Snapshot) IterSchemaNames(f func(schema string, target int64)) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	s.inner.iterSchemaNames(f)
}

// IterTables iterates all tables in the snapshot.
func (s *Snapshot) IterTables(includeIneligible bool, f func(i *model.TableInfo)) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	s.inner.iterTables(includeIneligible, f)
}

// IterTableNames iterates all table names in the snapshot.
func (s *Snapshot) IterTableNames(f func(schema int64, table string, target int64)) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	s.inner.iterTableNames(f)
}

// IterPartitions iterates all partitions in the snapshot.
func (s *Snapshot) IterPartitions(includeIneligible bool, f func(id int64, i *model.TableInfo)) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	s.inner.iterPartitions(includeIneligible, f)
}

// SchemaByID returns the DBInfo by schema id.
// The second returned value is false if no schema with the specified id is found.
// NOTE: The returned table info should always be READ-ONLY!
func (s *Snapshot) SchemaByID(id int64) (*timodel.DBInfo, bool) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	return s.inner.schemaByID(id)
}

// PhysicalTableByID returns the TableInfo by table id or partition id.
// The second returned value is false if no table with the specified id is found.
// NOTE: The returned table info should always be READ-ONLY!
func (s *Snapshot) PhysicalTableByID(id int64) (*model.TableInfo, bool) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	return s.inner.physicalTableByID(id)
}

// SchemaIDByName gets the schema id from the given schema name.
func (s *Snapshot) SchemaIDByName(schema string) (int64, bool) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	return s.inner.schemaIDByName(schema)
}

// TableIDByName returns the tableID by table schemaName and tableName.
// The second returned value is false if no table with the specified name is found.
func (s *Snapshot) TableIDByName(schema string, table string) (int64, bool) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	return s.inner.tableIDByName(schema, table)
}

// TableByName queries a table by name,
// The second returned value is false if no table with the specified name is found.
// NOTE: The returned table info should always be READ-ONLY!
func (s *Snapshot) TableByName(schema, table string) (*model.TableInfo, bool) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	return s.inner.tableByName(schema, table)
}

// SchemaByTableID returns the schema ID by table ID.
func (s *Snapshot) SchemaByTableID(tableID int64) (*timodel.DBInfo, bool) {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	tableInfo, ok := s.inner.physicalTableByID(tableID)
	if !ok {
		return nil, false
	}
	return s.inner.schemaByID(tableInfo.SchemaID)
}

// IsTruncateTableID returns true if the table id have been truncated by truncate table DDL.
func (s *Snapshot) IsTruncateTableID(id int64) bool {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	tag, ok := s.inner.tableTagByID(id, true)
	return ok && s.inner.truncatedTables.Has(newVersionedID(id, tag))
}

// IsIneligibleTableID returns true if the table is ineligible.
func (s *Snapshot) IsIneligibleTableID(id int64) bool {
	s.rwlock.RLock()
	defer s.rwlock.RUnlock()
	return s.inner.isIneligibleTableID(id)
}

// HandleDDL handles the given job.
func (s *Snapshot) HandleDDL(job *timodel.Job) error {
	if err := s.FillSchemaName(job); err != nil {
		return errors.Trace(err)
	}
	return s.DoHandleDDL(job)
}

// CurrentTs returns the finish timestamp of the schema snapshot.
func (s *Snapshot) CurrentTs() uint64 {
	return s.inner.currentTs
}

// Drop drops the snapshot. It must be called when GC some snapshots.
// Drop a snapshot will also drop all snapshots with a less timestamp.
func (s *Snapshot) Drop() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	s.inner.drop()
}

// DoHandleDDL is like HandleDDL but doesn't fill schema name into job.
// NOTE: it's public because some tests in the upper package need this.
func (s *Snapshot) DoHandleDDL(job *timodel.Job) error {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	getWrapTableInfo := func(job *timodel.Job) *model.TableInfo {
		return model.WrapTableInfo(job.SchemaID, job.SchemaName,
			job.BinlogInfo.FinishedTS,
			job.BinlogInfo.TableInfo)
	}
	switch job.Type {
	case timodel.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		err := s.inner.createSchema(job.BinlogInfo.DBInfo, job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionModifySchemaCharsetAndCollate:
		err := s.inner.replaceSchema(job.BinlogInfo.DBInfo, job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionDropSchema:
		err := s.inner.dropSchema(job.SchemaID, job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionRenameTable:
		// first drop the table
		err := s.inner.dropTable(job.TableID, job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
		// create table
		err = s.inner.createTable(getWrapTableInfo(job), job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionRenameTables:
		err := s.inner.renameTables(job, job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionCreateTable, timodel.ActionCreateView, timodel.ActionRecoverTable:
		err := s.inner.createTable(getWrapTableInfo(job), job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionDropTable, timodel.ActionDropView:
		err := s.inner.dropTable(job.TableID, job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}

	case timodel.ActionTruncateTable:
		// job.TableID is the old table id, different from table.ID
		err := s.inner.truncateTable(job.TableID, getWrapTableInfo(job), job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionTruncateTablePartition,
		timodel.ActionAddTablePartition,
		timodel.ActionDropTablePartition:
		err := s.inner.updatePartition(getWrapTableInfo(job), job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionExchangeTablePartition:
		err := s.inner.exchangePartition(getWrapTableInfo(job), job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	default:
		binlogInfo := job.BinlogInfo
		if binlogInfo == nil {
			log.Warn("ignore a invalid DDL job", zap.Any("job", job))
			return nil
		}
		if binlogInfo.TableInfo == nil {
			log.Warn("ignore a invalid DDL job", zap.Any("job", job))
			return nil
		}
		err := s.inner.replaceTable(getWrapTableInfo(job), job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if s.inner.currentTs != job.BinlogInfo.FinishedTS {
		panic("HandleDDL should update currentTs")
	}
	return nil
}

// TableCount counts tables in the snapshot. It's only for tests.
func (s *Snapshot) TableCount(includeIneligible bool,
	filter func(schema, table string) bool,
) (count int) {
	s.IterTables(includeIneligible, func(i *model.TableInfo) {
		if filter(i.TableName.Schema, i.TableName.Table) {
			count++
		}
	})
	return
}

// SchemaCount counts schemas in the snapshot. It's only for tests.
func (s *Snapshot) SchemaCount() (count int) {
	s.IterSchemas(func(i *timodel.DBInfo) { count += 1 })
	return
}

// DumpToString dumps the snapshot to a string.
func (s *Snapshot) DumpToString() string {
	schemas := make([]string, 0, s.inner.schemas.Len())
	s.IterSchemas(func(dbInfo *timodel.DBInfo) {
		schemas = append(schemas, fmt.Sprintf("%v", dbInfo))
	})

	tables := make([]string, 0, s.inner.tables.Len())
	s.IterTables(true, func(tbInfo *model.TableInfo) {
		tables = append(tables, fmt.Sprintf("%v", tbInfo))
	})

	partitions := make([]string, 0, s.inner.partitions.Len())
	s.IterPartitions(true, func(id int64, _ *model.TableInfo) {
		partitions = append(partitions, fmt.Sprintf("%d", id))
	})

	schemaNames := make([]string, 0, s.inner.schemaNameToID.Len())
	s.IterSchemaNames(func(schema string, target int64) {
		schemaNames = append(schemaNames, fmt.Sprintf("%s:%d", schema, target))
	})

	tableNames := make([]string, 0, s.inner.tableNameToID.Len())
	s.IterTableNames(func(schemaID int64, table string, target int64) {
		schema, _ := s.inner.schemaByID(schemaID)
		tableNames = append(tableNames, fmt.Sprintf("%s.%s:%d", schema.Name.O, table, target))
	})
	return fmt.Sprintf("%s\n%s\n%s\n%s\n%s",
		strings.Join(schemas, "\t"),
		strings.Join(tables, "\t"),
		strings.Join(partitions, "\t"),
		strings.Join(schemaNames, "\t"),
		strings.Join(tableNames, "\t"))
}

type snapshot struct {
	// map[versionedEntityName] -> int64
	// The ID can be `-1` which means the table is deleted.
	tableNameToID *btree.BTreeG[versionedEntityName]

	// map[versionedEntityName] -> int64
	// The ID can be `-1` which means the table is deleted.
	schemaNameToID *btree.BTreeG[versionedEntityName]

	// map[versionedID] -> *timodel.DBInfo
	// The target can be `nil` which means the entity is deleted.
	schemas *btree.BTreeG[versionedID]

	// map[versionedID] -> *model.TableInfo
	// The target can be `nil` which means the entity is deleted.
	tables *btree.BTreeG[versionedID]

	// map[versionedID] -> *model.TableInfo
	partitions *btree.BTreeG[versionedID]

	// map[versionedID] -> struct{}
	truncatedTables *btree.BTreeG[versionedID]

	// map[versionedID] -> struct{}
	// Partitions and tables share ineligibleTables because their IDs won't conflict.
	ineligibleTables *btree.BTreeG[versionedID]

	// if forceReplicate is true, treat ineligible tables as eligible.
	forceReplicate bool

	currentTs uint64
}

func (s *snapshot) schemaByID(id int64) (val *timodel.DBInfo, ok bool) {
	tag := negative(s.currentTs)
	start := versionedID{id: id, tag: tag, target: nil}
	end := versionedID{id: id, tag: negative(uint64(0)), target: nil}
	s.schemas.AscendRange(start, end, func(i versionedID) bool {
		val = targetToDBInfo(i.target)
		ok = val != nil
		return false
	})
	return
}

func (s *snapshot) physicalTableByID(id int64) (tableInfo *model.TableInfo, ok bool) {
	tag := negative(s.currentTs)
	start := versionedID{id: id, tag: tag, target: nil}
	end := versionedID{id: id, tag: negative(uint64(0)), target: nil}
	s.tables.AscendRange(start, end, func(i versionedID) bool {
		tableInfo = targetToTableInfo(i.target)
		ok = tableInfo != nil
		return false
	})
	if !ok {
		// Try partition, it could be a partition table.
		s.partitions.AscendRange(start, end, func(i versionedID) bool {
			tableInfo = targetToTableInfo(i.target)
			ok = tableInfo != nil
			return false
		})
	}
	return
}

func (s *snapshot) schemaIDByName(schema string) (id int64, ok bool) {
	tag := negative(s.currentTs)
	start := newVersionedEntityName(-1, schema, tag)
	end := newVersionedEntityName(-1, schema, negative(uint64(0)))
	s.schemaNameToID.AscendRange(start, end, func(i versionedEntityName) bool {
		id = i.target
		ok = id >= 0 // negative values are treated as invalid.
		return false
	})
	return
}

func (s *snapshot) tableIDByName(schema string, table string) (id int64, ok bool) {
	var prefix int64
	prefix, ok = s.schemaIDByName(schema)
	if ok {
		tag := negative(s.currentTs)
		start := newVersionedEntityName(prefix, table, tag)
		end := newVersionedEntityName(prefix, table, negative(uint64(0)))
		s.tableNameToID.AscendRange(start, end, func(i versionedEntityName) bool {
			id = i.target
			ok = id >= 0 // negative values are treated as invalid.
			return false
		})
	}
	return
}

func (s *snapshot) tableByName(schema, table string) (info *model.TableInfo, ok bool) {
	id, ok := s.tableIDByName(schema, table)
	if !ok {
		return nil, ok
	}
	return s.physicalTableByID(id)
}

func (s *snapshot) isIneligibleTableID(id int64) (ok bool) {
	tag, ok := s.tableTagByID(id, false)
	return ok && s.ineligibleTables.Has(newVersionedID(id, tag))
}

func (s *snapshot) tableTagByID(id int64, nilAcceptable bool) (foundTag uint64, ok bool) {
	tag := negative(s.currentTs)
	start := newVersionedID(id, tag)
	end := newVersionedID(id, negative(uint64(0)))
	s.tables.AscendRange(start, end, func(i versionedID) bool {
		tableInfo := targetToTableInfo(i.target)
		if nilAcceptable || tableInfo != nil {
			foundTag = i.tag
			ok = true
		}
		return false
	})
	if !ok {
		// Try partition, it could be a partition table.
		s.partitions.AscendRange(start, end, func(i versionedID) bool {
			tableInfo := targetToTableInfo(i.target)
			if nilAcceptable || tableInfo != nil {
				foundTag = i.tag
				ok = true
			}
			return false
		})
	}
	return
}

// dropSchema removes a schema from the snapshot.
// Tables in the schema will also be dropped.
func (s *snapshot) dropSchema(id int64, currentTs uint64) error {
	dbInfo, ok := s.schemaByID(id)
	if !ok {
		return cerror.ErrSnapshotSchemaNotFound.GenWithStackByArgs(id)
	}
	tag := negative(currentTs)
	s.schemas.ReplaceOrInsert(newVersionedID(id, tag))
	s.schemaNameToID.ReplaceOrInsert(newVersionedEntityName(-1, dbInfo.Name.O, tag))
	for _, id := range s.tablesInSchema(dbInfo.Name.O) {
		tbInfo, _ := s.physicalTableByID(id)
		s.doDropTable(tbInfo, currentTs)
	}
	s.currentTs = currentTs
	log.Debug("drop schema success", zap.String("name", dbInfo.Name.O), zap.Int64("id", dbInfo.ID))
	return nil
}

// Create a new schema in the snapshot. `dbInfo` will be deeply copied.
func (s *snapshot) createSchema(dbInfo *timodel.DBInfo, currentTs uint64) error {
	x, ok := s.schemaByID(dbInfo.ID)
	if ok {
		return cerror.ErrSnapshotSchemaExists.GenWithStackByArgs(x.Name, x.ID)
	}
	if id, ok := s.schemaIDByName(dbInfo.Name.O); ok {
		return cerror.ErrSnapshotSchemaExists.GenWithStackByArgs(dbInfo.Name.O, id)
	}
	s.doCreateSchema(dbInfo, currentTs)
	s.currentTs = currentTs
	log.Debug("create schema success", zap.String("name", dbInfo.Name.O), zap.Int64("id", dbInfo.ID))
	return nil
}

// Replace a schema. dbInfo will be deeply copied.
// Callers should ensure `dbInfo` information not conflict with other schemas.
func (s *snapshot) replaceSchema(dbInfo *timodel.DBInfo, currentTs uint64) error {
	old, ok := s.schemaByID(dbInfo.ID)
	if !ok {
		return cerror.ErrSnapshotSchemaNotFound.GenWithStack("schema %s(%d) not found", dbInfo.Name, dbInfo.ID)
	}
	s.doCreateSchema(dbInfo, currentTs)
	if old.Name.O != dbInfo.Name.O {
		tag := negative(currentTs)
		s.schemaNameToID.ReplaceOrInsert(newVersionedEntityName(-1, old.Name.O, tag))
	}
	s.currentTs = currentTs
	log.Debug("replace schema success", zap.String("name", dbInfo.Name.O), zap.Int64("id", dbInfo.ID))
	return nil
}

func (s *snapshot) doCreateSchema(dbInfo *timodel.DBInfo, currentTs uint64) {
	tag := negative(currentTs)
	vid := newVersionedID(dbInfo.ID, tag)
	vid.target = dbInfo.Clone()
	s.schemas.ReplaceOrInsert(vid)
	vname := newVersionedEntityName(-1, dbInfo.Name.O, tag)
	vname.target = dbInfo.ID
	s.schemaNameToID.ReplaceOrInsert(vname)
}

// dropTable removes a table(NOT partition) from the snapshot.
func (s *snapshot) dropTable(id int64, currentTs uint64) error {
	tbInfo, ok := s.physicalTableByID(id)
	if !ok {
		return cerror.ErrSnapshotTableNotFound.GenWithStackByArgs(id)
	}
	s.doDropTable(tbInfo, currentTs)
	s.currentTs = currentTs
	log.Debug("drop table success",
		zap.String("schema", tbInfo.TableName.Schema),
		zap.String("table", tbInfo.TableName.Table),
		zap.Int64("id", tbInfo.ID))
	return nil
}

func (s *snapshot) doDropTable(tbInfo *model.TableInfo, currentTs uint64) {
	tag := negative(currentTs)
	s.tables.ReplaceOrInsert(newVersionedID(tbInfo.ID, tag))
	s.tableNameToID.ReplaceOrInsert(newVersionedEntityName(tbInfo.SchemaID, tbInfo.TableName.Table, tag))
	if pi := tbInfo.GetPartitionInfo(); pi != nil {
		for _, partition := range pi.Definitions {
			s.partitions.ReplaceOrInsert(newVersionedID(partition.ID, tag))
		}
	}
}

// truncateTable truncate the table with the given ID, and replace it with a new `tbInfo`.
// NOTE: after a table is truncated:
//   - physicalTableByID(id) will return nil;
//   - IsTruncateTableID(id) should return true.
func (s *snapshot) truncateTable(id int64, tbInfo *model.TableInfo, currentTs uint64) (err error) {
	old, ok := s.physicalTableByID(id)
	if !ok {
		return cerror.ErrSnapshotTableNotFound.GenWithStackByArgs(id)
	}
	s.doDropTable(old, currentTs)
	s.doCreateTable(tbInfo, currentTs)
	s.truncatedTables.ReplaceOrInsert(newVersionedID(id, negative(currentTs)))
	s.currentTs = currentTs
	log.Debug("truncate table success",
		zap.String("schema", tbInfo.TableName.Schema),
		zap.String("table", tbInfo.TableName.Table),
		zap.Int64("id", tbInfo.ID))
	return
}

// Create a new table in the snapshot. `tbInfo` will be deeply copied.
func (s *snapshot) createTable(tbInfo *model.TableInfo, currentTs uint64) error {
	if _, ok := s.schemaByID(tbInfo.SchemaID); !ok {
		return cerror.ErrSnapshotSchemaNotFound.GenWithStack("table's schema(%d)", tbInfo.SchemaID)
	}
	if _, ok := s.physicalTableByID(tbInfo.ID); ok {
		return cerror.ErrSnapshotTableExists.GenWithStackByArgs(tbInfo.TableName.Schema, tbInfo.TableName.Table)
	}
	s.doCreateTable(tbInfo, currentTs)
	s.currentTs = currentTs
	log.Debug("create table success", zap.Int64("id", tbInfo.ID),
		zap.String("name", fmt.Sprintf("%s.%s", tbInfo.TableName.Schema, tbInfo.TableName.Table)))
	return nil
}

// ReplaceTable replace the table by new tableInfo
func (s *snapshot) replaceTable(tbInfo *model.TableInfo, currentTs uint64) error {
	if _, ok := s.schemaByID(tbInfo.SchemaID); !ok {
		return cerror.ErrSnapshotSchemaNotFound.GenWithStack("table's schema(%d)", tbInfo.SchemaID)
	}
	if _, ok := s.physicalTableByID(tbInfo.ID); !ok {
		return cerror.ErrSnapshotTableNotFound.GenWithStack("table %s(%d)", tbInfo.Name, tbInfo.ID)
	}
	s.doCreateTable(tbInfo, currentTs)
	s.currentTs = currentTs
	log.Debug("replace table success", zap.String("name", tbInfo.Name.O), zap.Int64("id", tbInfo.ID))
	return nil
}

func (s *snapshot) doCreateTable(tbInfo *model.TableInfo, currentTs uint64) {
	tbInfo = tbInfo.Clone()
	tag := negative(currentTs)
	vid := newVersionedID(tbInfo.ID, tag)
	vid.target = tbInfo
	s.tables.ReplaceOrInsert(vid)

	vname := newVersionedEntityName(tbInfo.SchemaID, tbInfo.TableName.Table, tag)
	vname.target = tbInfo.ID
	s.tableNameToID.ReplaceOrInsert(vname)

	ineligible := !tbInfo.IsEligible(s.forceReplicate)
	if ineligible {
		// Sequence is not supported yet, and always ineligible.
		// Skip Warn to avoid confusion.
		// See https://github.com/pingcap/tiflow/issues/4559
		if !tbInfo.IsSequence() {
			log.Warn("this table is ineligible to replicate",
				zap.String("tableName", tbInfo.Name.O), zap.Int64("tableID", tbInfo.ID))
		}
		s.ineligibleTables.ReplaceOrInsert(newVersionedID(tbInfo.ID, tag))
	}
	if pi := tbInfo.GetPartitionInfo(); pi != nil {
		for _, partition := range pi.Definitions {
			vid := newVersionedID(partition.ID, tag)
			vid.target = tbInfo
			s.partitions.ReplaceOrInsert(vid)
			if ineligible {
				s.ineligibleTables.ReplaceOrInsert(newVersionedID(partition.ID, tag))
			}
		}
	}
}

// updatePartition updates partition info for `tbInfo`.
func (s *snapshot) updatePartition(tbInfo *model.TableInfo, currentTs uint64) error {
	oldTbInfo, ok := s.physicalTableByID(tbInfo.ID)
	if !ok {
		return cerror.ErrSnapshotTableNotFound.GenWithStackByArgs(tbInfo.ID)
	}
	oldPi := oldTbInfo.GetPartitionInfo()
	if oldPi == nil {
		return cerror.ErrSnapshotTableNotFound.GenWithStack("table %d is not a partition table", tbInfo.ID)
	}
	newPi := tbInfo.GetPartitionInfo()
	if newPi == nil {
		return cerror.ErrSnapshotTableNotFound.GenWithStack("table %d is not a partition table", tbInfo.ID)
	}

	tag := negative(currentTs)
	vid := newVersionedID(tbInfo.ID, tag)
	vid.target = tbInfo.Clone()
	s.tables.ReplaceOrInsert(vid)
	ineligible := !tbInfo.IsEligible(s.forceReplicate)
	if ineligible {
		s.ineligibleTables.ReplaceOrInsert(newVersionedID(tbInfo.ID, tag))
	}
	for _, partition := range oldPi.Definitions {
		s.partitions.ReplaceOrInsert(newVersionedID(partition.ID, tag))
	}
	for _, partition := range newPi.Definitions {
		vid := newVersionedID(partition.ID, tag)
		vid.target = tbInfo
		s.partitions.ReplaceOrInsert(vid)
		if ineligible {
			s.ineligibleTables.ReplaceOrInsert(newVersionedID(partition.ID, tag))
		}
	}
	s.currentTs = currentTs

	log.Debug("adjust partition success",
		zap.String("schema", tbInfo.TableName.Schema),
		zap.String("table", tbInfo.TableName.Table),
		zap.Any("partitions", newPi.Definitions),
	)
	return nil
}

// exchangePartition find the partition's id in the old table info of targetTable,
// and find the sourceTable's id in the new table info of targetTable.
// Then set sourceTable's id to the partition's id, which make the exchange happen in snapshot.
// Finally, update both the targetTable's info and the sourceTable's info in snapshot.
func (s *snapshot) exchangePartition(targetTable *model.TableInfo, currentTS uint64) error {
	var sourceTable *model.TableInfo
	oldTable, ok := s.physicalTableByID(targetTable.ID)
	if !ok {
		return cerror.ErrSnapshotTableNotFound.GenWithStackByArgs(targetTable.ID)
	}

	oldPartitions := oldTable.GetPartitionInfo()
	if oldPartitions == nil {
		return cerror.ErrSnapshotTableNotFound.
			GenWithStack("table %d is not a partitioned table", oldTable.ID)
	}

	newPartitions := targetTable.GetPartitionInfo()
	if newPartitions == nil {
		return cerror.ErrSnapshotTableNotFound.
			GenWithStack("table %d is not a partitioned table", targetTable.ID)
	}

	oldIDs := make(map[int64]struct{}, len(oldPartitions.Definitions))
	for _, p := range oldPartitions.Definitions {
		oldIDs[p.ID] = struct{}{}
	}

	newIDs := make(map[int64]struct{}, len(oldPartitions.Definitions))
	for _, p := range newPartitions.Definitions {
		newIDs[p.ID] = struct{}{}
	}

	// 1. find the source table info
	var diff []int64
	for id := range newIDs {
		if _, ok := oldIDs[id]; !ok {
			diff = append(diff, id)
		}
	}
	if len(diff) != 1 {
		return cerror.ErrExchangePartition.
			GenWithStackByArgs(fmt.Sprintf("The exchanged source table number must be 1, but found %v", diff))
	}
	sourceTable, ok = s.physicalTableByID(diff[0])
	if !ok {
		return cerror.ErrSnapshotTableNotFound.GenWithStackByArgs(diff[0])
	}

	// 3.find the exchanged partition info
	diff = diff[:0]
	for id := range oldIDs {
		if _, ok := newIDs[id]; !ok {
			diff = append(diff, id)
		}
	}
	if len(diff) != 1 {
		return cerror.ErrExchangePartition.
			GenWithStackByArgs(fmt.Sprintf("The exchanged source table number must be 1, but found %v", diff))
	}

	exchangedPartitionID := diff[0]
	// 4.update the targetTable
	err := s.updatePartition(targetTable, currentTS)
	if err != nil {
		return errors.Trace(err)
	}

	newSourceTable := sourceTable.Clone()
	// 5.update the sourceTable
	err = s.dropTable(sourceTable.ID, currentTS)
	if err != nil {
		return errors.Trace(err)
	}
	newSourceTable.ID = exchangedPartitionID
	err = s.createTable(newSourceTable, currentTS)
	if err != nil {
		return errors.Trace(err)
	}

	log.Info("handle exchange partition success",
		zap.String("sourceTable", sourceTable.TableName.String()),
		zap.Int64("exchangedPartition", exchangedPartitionID),
		zap.String("targetTable", targetTable.TableName.String()),
		zap.Any("partition", targetTable.GetPartitionInfo().Definitions))
	return nil
}

func (s *snapshot) renameTables(job *timodel.Job, currentTs uint64) error {
	var oldSchemaIDs, newSchemaIDs, oldTableIDs []int64
	var newTableNames, oldSchemaNames []*timodel.CIStr
	err := job.DecodeArgs(&oldSchemaIDs, &newSchemaIDs, &newTableNames, &oldTableIDs, &oldSchemaNames)
	if err != nil {
		return errors.Trace(err)
	}
	if len(job.BinlogInfo.MultipleTableInfos) < len(newTableNames) {
		return cerror.ErrInvalidDDLJob.GenWithStackByArgs(job.ID)
	}
	// NOTE: should handle failures in halfway better.
	for _, tableID := range oldTableIDs {
		if err := s.dropTable(tableID, currentTs); err != nil {
			return errors.Trace(err)
		}
	}
	for i, tableInfo := range job.BinlogInfo.MultipleTableInfos {
		newSchema, ok := s.schemaByID(newSchemaIDs[i])
		if !ok {
			return cerror.ErrSnapshotSchemaNotFound.GenWithStackByArgs(newSchemaIDs[i])
		}
		newSchemaName := newSchema.Name.O
		tbInfo := model.WrapTableInfo(newSchemaIDs[i], newSchemaName, job.BinlogInfo.FinishedTS, tableInfo)
		err = s.createTable(tbInfo, currentTs)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *snapshot) iterTables(includeIneligible bool, f func(i *model.TableInfo)) {
	tag := negative(s.currentTs)
	var tableID int64 = -1
	s.tables.Ascend(func(x versionedID) bool {
		if x.id != tableID && x.tag >= tag {
			tableID = x.id
			if x.target != nil && (includeIneligible ||
				!s.ineligibleTables.Has(newVersionedID(x.id, x.tag))) {
				f(targetToTableInfo(x.target))
			}
		}
		return true
	})
}

func (s *snapshot) iterPartitions(includeIneligible bool, f func(id int64, i *model.TableInfo)) {
	tag := negative(s.currentTs)
	var partitionID int64 = -1
	s.partitions.Ascend(func(x versionedID) bool {
		if x.id != partitionID && x.tag >= tag {
			partitionID = x.id
			if x.target != nil && (includeIneligible ||
				!s.ineligibleTables.Has(newVersionedID(x.id, x.tag))) {
				f(partitionID, targetToTableInfo(x.target))
			}
		}
		return true
	})
}

func (s *snapshot) iterSchemas(f func(i *timodel.DBInfo)) {
	tag := negative(s.currentTs)
	var schemaID int64 = -1
	s.schemas.Ascend(func(x versionedID) bool {
		if x.id != schemaID && x.tag >= tag {
			schemaID = x.id
			if x.target != nil {
				f(targetToDBInfo(x.target))
			}
		}
		return true
	})
}

func (s *snapshot) iterTableNames(f func(schema int64, table string, target int64)) {
	tag := negative(s.currentTs)
	var prefix int64 = -1
	entity := ""
	s.tableNameToID.Ascend(func(x versionedEntityName) bool {
		if (x.prefix != prefix || x.entity != entity) && x.tag >= tag {
			prefix = x.prefix
			entity = x.entity
			if x.target > 0 {
				f(prefix, entity, x.target)
			}
		}
		return true
	})
}

func (s *snapshot) iterSchemaNames(f func(schema string, target int64)) {
	tag := negative(s.currentTs)
	entity := ""
	s.schemaNameToID.Ascend(func(x versionedEntityName) bool {
		if x.entity != entity && x.tag >= tag {
			entity = x.entity
			if x.target > 0 {
				f(entity, x.target)
			}
		}
		return true
	})
}

func (s *snapshot) tablesInSchema(schema string) (tables []int64) {
	schemaID, ok := s.schemaIDByName(schema)
	if !ok {
		return
	}
	start := newVersionedEntityName(schemaID, "", 0)
	end := newVersionedEntityName(schemaID+1, "", 0)
	tag := negative(s.currentTs)
	currTable := ""
	s.tableNameToID.AscendRange(start, end, func(x versionedEntityName) bool {
		if x.tag >= tag && x.entity != currTable {
			currTable = x.entity
			if x.target > 0 {
				tables = append(tables, x.target)
			}
		}
		return true
	})
	return
}

func (s *snapshot) drop() {
	tag := negative(s.currentTs)

	schemas := make([]versionedID, 0, s.schemas.Len())
	var schemaID int64 = -1
	schemaDroped := false
	s.schemas.Ascend(func(x versionedID) bool {
		if x.tag >= tag {
			if x.id != schemaID {
				schemaID = x.id
				schemaDroped = false
			}
			if schemaDroped || x.target == nil {
				schemas = append(schemas, newVersionedID(x.id, x.tag))
			}
			schemaDroped = true
		}
		return true
	})
	for _, vid := range schemas {
		s.schemas.Delete(vid)
	}

	tables := make([]versionedID, 0, s.tables.Len())
	var tableID int64 = -1
	tableDroped := false
	s.tables.Ascend(func(x versionedID) bool {
		if x.tag >= tag {
			if x.id != tableID {
				tableID = x.id
				tableDroped = false
			}
			if tableDroped || x.target == nil {
				tables = append(tables, newVersionedID(x.id, x.tag))
			}
			tableDroped = true
		}
		return true
	})
	for _, vid := range tables {
		x, _ := s.tables.Delete(vid)
		info := targetToTableInfo(x.target)
		if info != nil {
			ineligible := !info.IsEligible(s.forceReplicate)
			if ineligible {
				s.ineligibleTables.Delete(vid)
			}
		} else {
			// Maybe the table is truncated.
			s.truncatedTables.Delete(vid)
		}
	}

	partitions := make([]versionedID, 0, s.partitions.Len())
	var partitionID int64 = -1
	partitionDroped := false
	s.partitions.Ascend(func(x versionedID) bool {
		if x.tag >= tag {
			if x.id != partitionID {
				partitionID = x.id
				partitionDroped = false
			}
			if partitionDroped || x.target == nil {
				partitions = append(partitions, newVersionedID(x.id, x.tag))
			}
			partitionDroped = true
		}
		return true
	})
	for _, vid := range partitions {
		x, _ := s.partitions.Delete(vid)
		info := targetToTableInfo(x.target)
		if info != nil {
			ineligible := !info.IsEligible(s.forceReplicate)
			if ineligible {
				s.ineligibleTables.Delete(vid)
			}
		}
	}

	schemaNames := make([]versionedEntityName, 0, s.schemaNameToID.Len())
	schemaName := ""
	schemaNameDroped := false
	s.schemaNameToID.Ascend(func(x versionedEntityName) bool {
		if x.tag >= tag {
			if x.entity != schemaName {
				schemaName = x.entity
				schemaNameDroped = false
			}
			if schemaNameDroped || x.target < 0 {
				schemaNames = append(schemaNames, newVersionedEntityName(x.prefix, x.entity, x.tag))
			}
			schemaNameDroped = true
		}
		return true
	})
	for _, vname := range schemaNames {
		s.schemaNameToID.Delete(vname)
	}

	tableNames := make([]versionedEntityName, 0, s.tableNameToID.Len())
	schemaID = -1
	tableName := ""
	tableNameDroped := false
	s.tableNameToID.Ascend(func(x versionedEntityName) bool {
		if x.tag >= tag {
			if x.prefix != schemaID || x.entity != tableName {
				schemaID = x.prefix
				tableName = x.entity
				tableNameDroped = false
			}
			if tableNameDroped || x.target < 0 {
				tableNames = append(tableNames, newVersionedEntityName(x.prefix, x.entity, x.tag))
			}
			tableNameDroped = true
		}
		return true
	})
	for _, vname := range tableNames {
		s.tableNameToID.Delete(vname)
	}
}

// Entity(schema or table) name with finish timestamp of the associated DDL job.
type versionedEntityName struct {
	prefix int64 // schema ID if the entity is a table, or -1 if it's a schema.
	entity string
	tag    uint64 // A transform of timestamp to reverse sort versions.
	// the associated entity id, negative values are treated as invalid.
	target int64
}

// ID with finish timestamp of the associated DDL job.
type versionedID struct {
	id  int64
	tag uint64 // A transform of timestamp to reverse sort versions.
	// the associated entity pointer.
	target interface{}
}

func versionedEntityNameLess(v1, v2 versionedEntityName) bool {
	return v1.prefix < v2.prefix || (v1.prefix == v2.prefix && v1.entity < v2.entity) ||
		(v1.prefix == v2.prefix && v1.entity == v2.entity && v1.tag < v2.tag)
}

func versionedIDLess(v1, v2 versionedID) bool {
	return v1.id < v2.id || (v1.id == v2.id && v1.tag < v2.tag)
}

// negative transforms `x` for reverse sorting based on it.
func negative(x uint64) uint64 {
	return math.MaxUint64 - x
}

// newVersionedEntityName creates an instance with target -1, which means it's deleted from
// the associated snapshot.
func newVersionedEntityName(prefix int64, entity string, tag uint64) versionedEntityName {
	var target int64 = -1
	return versionedEntityName{prefix, entity, tag, target}
}

// newVersionedID creates an instance with target nil, which means it's deleted from the
// associated snapshot.
func newVersionedID(id int64, tag uint64) versionedID {
	var target interface{} = nil
	return versionedID{id, tag, target}
}

func targetToTableInfo(target interface{}) *model.TableInfo {
	if target == nil {
		return nil
	}
	return target.(*model.TableInfo)
}

func targetToDBInfo(target interface{}) *timodel.DBInfo {
	if target == nil {
		return nil
	}
	return target.(*timodel.DBInfo)
}
