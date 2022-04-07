package schema

import (
	"fmt"
	"unsafe"

	"github.com/google/btree"
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"

	timeta "github.com/pingcap/tidb/meta"
	timodel "github.com/pingcap/tidb/parser/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// PreTableInfo returns the table info which will be overwritten by the specified job
func (s *SchemaSnapshot) PreTableInfo(job *timodel.Job) (*model.TableInfo, error) {
	switch job.Type {
	case timodel.ActionCreateSchema, timodel.ActionModifySchemaCharsetAndCollate, timodel.ActionDropSchema:
		return nil, nil
	case timodel.ActionCreateTable, timodel.ActionCreateView, timodel.ActionRecoverTable:
		// no pre table info
		return nil, nil
	case timodel.ActionRenameTable, timodel.ActionDropTable, timodel.ActionDropView, timodel.ActionTruncateTable:
		// get the table will be dropped
		table, ok := s.TableByID(job.TableID)
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
			log.Warn("ignore a invalid DDL job", zap.Reflect("job", job))
			return nil, nil
		}
		tbInfo := binlogInfo.TableInfo
		if tbInfo == nil {
			log.Warn("ignore a invalid DDL job", zap.Reflect("job", job))
			return nil, nil
		}
		tableID := tbInfo.ID
		table, ok := s.TableByID(tableID)
		if !ok {
			return nil, cerror.ErrSchemaStorageTableMiss.GenWithStackByArgs(job.TableID)
		}
		return table, nil
	}
}

// NewSingleSchemaSnapshotFromMeta creates a new single schema snapshot from a tidb meta
func NewSingleSchemaSnapshotFromMeta(meta *timeta.Meta, currentTs uint64, forceReplicate bool) (*SchemaSnapshot, error) {
	// meta is nil only in unit tests
	if meta == nil {
		snap := NewEmptySchemaSnapshot(forceReplicate)
		snap.currentTs = currentTs
		return snap, nil
	}
	return NewSchemaSnapshotFromMeta(meta, currentTs, forceReplicate)
}

// SchemaSnapshot stores the source TiDB all schema information and it should be READ-ONLY!
type SchemaSnapshot struct {
	// map[versionedEntityName] -> int64
	// The ID can be `-1` which means the table is deleted.
	tableNameToID *btree.BTree

	// map[versionedEntityName] -> int64
	// The ID can be `-1` which means the table is deleted.
	schemaNameToID *btree.BTree

	// map[versionedID] -> *timodel.DBInfo
	// The target can be `nil` which means the entity is deleted.
	schemas *btree.BTree

	// map[versionedID] -> *model.TableInfo
	// The target can be `nil` which means the entity is deleted.
	tables *btree.BTree

	// map[versionedID] -> *model.TableInfo
	partitionTables *btree.BTree

	// map[versionedID] -> struct{}
	truncatedTables *btree.BTree

	// map[versionedID] -> struct{}
	ineligibleTables *btree.BTree

	// if forceReplicate is true, treat ineligible tables as eligible.
	forceReplicate bool

	currentTs uint64
}

func NewEmptySchemaSnapshot(forceReplicate bool) *SchemaSnapshot {
	return &SchemaSnapshot{
		tableNameToID:    btree.New(16),
		schemaNameToID:   btree.New(16),
		schemas:          btree.New(16),
		tables:           btree.New(16),
		partitionTables:  btree.New(16),
		truncatedTables:  btree.New(16),
		ineligibleTables: btree.New(16),
		forceReplicate:   forceReplicate,
		currentTs:        0,
	}
}

func NewSchemaSnapshotFromMeta(meta *timeta.Meta, currentTs uint64, forceReplicate bool) (*SchemaSnapshot, error) {
	snap := NewEmptySchemaSnapshot(forceReplicate)
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMetaListDatabases, err)
	}
	tag := negative(currentTs)

	for _, dbinfo := range dbinfos {
		vid := newVersionedID(dbinfo.ID, tag)
		vid.target = unsafe.Pointer(dbinfo)
		snap.schemas.ReplaceOrInsert(vid)

		vname := newVersionedEntityName(dbinfo.Name.O, "", tag)
		vname.target = dbinfo.ID
		snap.schemaNameToID.ReplaceOrInsert(vname)

		tableInfos, err := meta.ListTables(dbinfo.ID)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMetaListDatabases, err)
		}
		for _, tableInfo := range tableInfos {
			tableInfo := model.WrapTableInfo(dbinfo.ID, dbinfo.Name.O, currentTs, tableInfo)
			snap.tables.ReplaceOrInsert(versionedID{
				id:     tableInfo.ID,
				tag:    tag,
				target: unsafe.Pointer(tableInfo),
			})
			snap.tableNameToID.ReplaceOrInsert(versionedEntityName{
				schema: dbinfo.Name.O,
				table:  tableInfo.Name.O,
				tag:    tag,
				target: tableInfo.ID,
			})

			isEligible := tableInfo.IsEligible(forceReplicate)
			if !isEligible {
				snap.ineligibleTables.ReplaceOrInsert(versionedID{id: tableInfo.ID, tag: tag})
			}
			if pi := tableInfo.GetPartitionInfo(); pi != nil {
				for _, partition := range pi.Definitions {
					vid := newVersionedID(partition.ID, tag)
					vid.target = unsafe.Pointer(tableInfo)
					snap.partitionTables.ReplaceOrInsert(vid)
					if !isEligible {
						snap.ineligibleTables.ReplaceOrInsert(versionedID{id: partition.ID, tag: tag})
					}
				}
			}
		}
	}

	snap.currentTs = currentTs
	return snap, nil
}

func (s *SchemaSnapshot) Copy() *SchemaSnapshot {
	return &SchemaSnapshot{
		tableNameToID:    s.tableNameToID,
		schemaNameToID:   s.schemaNameToID,
		schemas:          s.schemas,
		tables:           s.tables,
		partitionTables:  s.partitionTables,
		truncatedTables:  s.truncatedTables,
		ineligibleTables: s.ineligibleTables,
		forceReplicate:   s.forceReplicate,
		currentTs:        s.currentTs,
	}
}

func (s *SchemaSnapshot) PrintStatus(logger func(msg string, fields ...zap.Field)) {
	/*************
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
	for pid, table := range s.partitionTables {
		logger("[SchemaSnap] --> Partitions", zap.Int64("partitionID", pid), zap.Int64("tableID", table.ID))
	}
	truncatedTables := make([]int64, 0, len(s.truncatedTables))
	for id := range s.truncatedTables {
		truncatedTables = append(truncatedTables, id)
	}
	logger("[SchemaSnap] TruncateTableIDs", zap.Int64s("ids", truncatedTables))

	ineligibleTables := make([]int64, 0, len(s.ineligibleTables))
	for id := range s.ineligibleTables {
		ineligibleTables = append(ineligibleTables, id)
	}
	logger("[SchemaSnap] IneligibleTableIDs", zap.Int64s("ids", ineligibleTables))
	*************/
}

// SchemaByID returns the DBInfo by schema id.
// The second returned value is false if no schema with the specified id is found.
// NOTE: The returned table info should always be READ-ONLY!
func (s *SchemaSnapshot) SchemaByID(id int64) (val *timodel.DBInfo, ok bool) {
	tag := negative(s.currentTs)
	start := versionedID{id: id, tag: tag, target: nil}
	end := versionedID{id: id, tag: negative(uint64(0)), target: nil}
	s.schemas.AscendRange(start, end, func(i btree.Item) bool {
		val = (*timodel.DBInfo)(i.(versionedID).target)
		ok = val != nil
		return false
	})
	return
}

// TableByID returns the TableInfo by table id or partition id.
// The second returned value is false if no table with the specified id is found.
// NOTE: The returned table info should always be READ-ONLY!
func (s *SchemaSnapshot) TableByID(id int64) (tableInfo *model.TableInfo, ok bool) {
	tag := negative(s.currentTs)
	start := versionedID{id: id, tag: tag, target: nil}
	end := versionedID{id: id, tag: negative(uint64(0)), target: nil}
	s.tables.AscendRange(start, end, func(i btree.Item) bool {
		tableInfo = (*model.TableInfo)(i.(versionedID).target)
		ok = tableInfo != nil
		return false
	})
	if !ok {
		// Try partition, it could be a partition table.
		s.partitionTables.AscendRange(start, end, func(i btree.Item) bool {
			tableInfo = (*model.TableInfo)(i.(versionedID).target)
			ok = tableInfo != nil
			return false
		})
	}
	return
}

// TableIDByName returns the tableID by table schemaName and tableName.
// The second returned value is false if no table with the specified name is found.
// NOTE: The returned table info should always be READ-ONLY!
func (s *SchemaSnapshot) TableIDByName(schema string, table string) (id int64, ok bool) {
	tag := negative(s.currentTs)
	start := newVersionedEntityName(schema, table, tag)
	end := newVersionedEntityName(schema, table, negative(uint64(0)))
	s.tableNameToID.AscendRange(start, end, func(i btree.Item) bool {
		id = i.(versionedEntityName).target
		ok = id >= 0 // negative values are treated as invalid.
		return false
	})
	return
}

// GetTableByName queries a table by name,
// The second returned value is false if no table with the specified name is found.
// NOTE: The returned table info should always be READ-ONLY!
func (s *SchemaSnapshot) TableByName(schema, table string) (info *model.TableInfo, ok bool) {
	id, ok := s.TableIDByName(schema, table)
	if !ok {
		return nil, ok
	}
	return s.TableByID(id)
}

// SchemaByTableID returns the schema ID by table ID.
func (s *SchemaSnapshot) SchemaByTableID(tableID int64) (*timodel.DBInfo, bool) {
	tableInfo, ok := s.TableByID(tableID)
	if !ok {
		return nil, false
	}
	return s.SchemaByID(tableInfo.SchemaID)
}

// IsTruncateTableID returns true if the table id have been truncated by truncate table DDL.
func (s *SchemaSnapshot) IsTruncateTableID(id int64) bool {
	tag, ok := s.tableTagByID(id)
	return ok && s.truncatedTables.Get(newVersionedID(id, tag)) != nil
}

// IsIneligibleTableID returns true if the table is ineligible.
func (s *SchemaSnapshot) IsIneligibleTableID(id int64) (ok bool) {
	tag, ok := s.tableTagByID(id)
	return ok && s.ineligibleTables.Get(newVersionedID(id, tag)) != nil
}

func (s *SchemaSnapshot) tableTagByID(id int64) (foundTag uint64, ok bool) {
	tag := negative(s.currentTs)
	start := newVersionedID(id, tag)
	end := newVersionedID(id, negative(uint64(0)))
	s.tables.AscendRange(start, end, func(i btree.Item) bool {
		tableInfo := (*timodel.TableInfo)(i.(versionedID).target)
		if tableInfo != nil {
			foundTag = i.(versionedID).tag
			ok = true
		}
		return false
	})
	if !ok {
		// Try partition, it could be a partition table.
		s.partitionTables.AscendRange(start, end, func(i btree.Item) bool {
			tableInfo := (*timodel.TableInfo)(i.(versionedID).target)
			if tableInfo != nil {
				foundTag = i.(versionedID).tag
				ok = true
			}
			return false
		})
	}
	return
}

// FillSchemaName fills the schema name in ddl job.
func (s *SchemaSnapshot) FillSchemaName(job *timodel.Job) error {
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

func (s *SchemaSnapshot) dropSchema(id int64, currentTs uint64) error {
	dbInfo, ok := s.SchemaByID(id)
	if !ok {
		return cerror.ErrSnapshotSchemaNotFound.GenWithStackByArgs(id)
	}
	tag := negative(currentTs)
	s.schemas.ReplaceOrInsert(newVersionedID(id, tag))
	s.schemaNameToID.ReplaceOrInsert(newVersionedEntityName(dbInfo.Name.O, "", tag))
	s.currentTs = currentTs
	log.Debug("drop schema success", zap.String("name", dbInfo.Name.O), zap.Int64("id", dbInfo.ID))
	return nil
}

// Create a new schema in the snapshot. `dbInfo` will be deep copied.
func (s *SchemaSnapshot) createSchema(dbInfo *timodel.DBInfo, currentTs uint64) error {
	x, ok := s.SchemaByID(dbInfo.ID)
	if ok {
		return cerror.ErrSnapshotSchemaExists.GenWithStackByArgs(x.Name, x.ID)
	}
	s.doCreateSchema(dbInfo, currentTs)
	log.Debug("create schema success", zap.String("name", dbInfo.Name.O), zap.Int64("id", dbInfo.ID))
	return nil
}

func (s *SchemaSnapshot) replaceSchema(dbInfo *timodel.DBInfo, currentTs uint64) error {
	if _, ok := s.SchemaByID(dbInfo.ID); !ok {
		return cerror.ErrSnapshotSchemaNotFound.GenWithStack("schema %s(%d) not found", dbInfo.Name, dbInfo.ID)
	}
	s.doCreateSchema(dbInfo, currentTs)
	log.Debug("replace schema success", zap.String("name", dbInfo.Name.O), zap.Int64("id", dbInfo.ID))
	return nil
}

func (s *SchemaSnapshot) doCreateSchema(dbInfo *timodel.DBInfo, currentTs uint64) {
	tag := negative(currentTs)
	vid := newVersionedID(dbInfo.ID, tag)
	vid.target = unsafe.Pointer(dbInfo.Clone())
	s.schemas.ReplaceOrInsert(vid)
	vname := newVersionedEntityName(dbInfo.Name.O, "", tag)
	vname.target = dbInfo.ID
	s.schemaNameToID.ReplaceOrInsert(vname)
	s.currentTs = currentTs
}

func (s *SchemaSnapshot) dropTable(id int64, currentTs uint64) error {
	tbInfo, ok := s.TableByID(id)
	if !ok {
		return cerror.ErrSnapshotTableNotFound.GenWithStackByArgs(id)
	}
	tag := negative(currentTs)
	s.tables.ReplaceOrInsert(newVersionedID(id, tag))
	s.tableNameToID.ReplaceOrInsert(newVersionedEntityName(tbInfo.TableName.Schema, tbInfo.TableName.Table, tag))
	if pi := tbInfo.GetPartitionInfo(); pi != nil {
		for _, partition := range pi.Definitions {
			s.partitionTables.ReplaceOrInsert(newVersionedID(partition.ID, tag))
		}
	}
	s.currentTs = currentTs
	log.Debug("drop table success",
		zap.String("schema", tbInfo.TableName.Schema),
		zap.String("table", tbInfo.TableName.Table),
		zap.Int64("id", tbInfo.ID))
	return nil
}

func (s *SchemaSnapshot) truncateTable(id int64, tbInfo *model.TableInfo, currentTs uint64) (err error) {
	old, ok := s.TableByID(id)
	if !ok {
		return cerror.ErrSnapshotTableNotFound.GenWithStackByArgs(id)
	}
	tag := negative(currentTs)
	s.tables.ReplaceOrInsert(newVersionedID(id, tag))
	s.tableNameToID.ReplaceOrInsert(newVersionedEntityName(old.TableName.Schema, old.TableName.Table, tag))

	s.doCreateTable(tbInfo, currentTs)
	s.truncatedTables.ReplaceOrInsert(newVersionedID(tbInfo.ID, tag))

	log.Debug("truncate table success",
		zap.String("schema", tbInfo.TableName.Schema),
		zap.String("table", tbInfo.TableName.Table),
		zap.Int64("id", tbInfo.ID))
	return
}

// Create a new table in the snapshot. `tbInfo` will be deep copied.
func (s *SchemaSnapshot) createTable(tbInfo *model.TableInfo, currentTs uint64) error {
	if _, ok := s.SchemaByID(tbInfo.SchemaID); !ok {
		return cerror.ErrSnapshotSchemaNotFound.GenWithStack("table's schema(%d)", tbInfo.SchemaID)
	}
	if _, ok := s.TableByID(tbInfo.ID); ok {
		return cerror.ErrSnapshotTableExists.GenWithStackByArgs(tbInfo.TableName.Schema, tbInfo.TableName.Table)
	}
	s.doCreateTable(tbInfo, currentTs)
	log.Debug("create table success", zap.Int64("id", tbInfo.ID),
		zap.String("name", fmt.Sprintf("%s.%s", tbInfo.TableName.Schema, tbInfo.TableName.Table)))
	return nil
}

// ReplaceTable replace the table by new tableInfo
func (s *SchemaSnapshot) replaceTable(tbInfo *model.TableInfo, currentTs uint64) error {
	if _, ok := s.SchemaByID(tbInfo.SchemaID); !ok {
		return cerror.ErrSnapshotSchemaNotFound.GenWithStack("table's schema(%d)", tbInfo.SchemaID)
	}
	if _, ok := s.TableByID(tbInfo.ID); !ok {
		return cerror.ErrSnapshotTableNotFound.GenWithStack("table %s(%d)", tbInfo.Name, tbInfo.ID)
	}
	s.doCreateTable(tbInfo, currentTs)
	log.Debug("replace table success", zap.String("name", tbInfo.Name.O), zap.Int64("id", tbInfo.ID))
	return nil
}

func (s *SchemaSnapshot) doCreateTable(tbInfo *model.TableInfo, currentTs uint64) {
	tbInfo = tbInfo.Clone()
	tag := negative(currentTs)
	vid := newVersionedID(tbInfo.ID, tag)
	vid.target = unsafe.Pointer(tbInfo)
	s.tables.ReplaceOrInsert(vid)

	vname := newVersionedEntityName(tbInfo.TableName.Schema, tbInfo.TableName.Table, tag)
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
			vid.target = unsafe.Pointer(tbInfo)
			s.partitionTables.ReplaceOrInsert(vid)
			if ineligible {
				s.ineligibleTables.ReplaceOrInsert(newVersionedID(partition.ID, tag))
			}
		}
	}
	s.currentTs = currentTs
}

// updatePartition updates partition info for `tbInfo`.
func (s *SchemaSnapshot) updatePartition(tbInfo *model.TableInfo, currentTs uint64) error {
	oldTbInfo, ok := s.TableByID(tbInfo.ID)
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
	vid.target = unsafe.Pointer(tbInfo.Clone())
	s.tables.ReplaceOrInsert(vid)
	ineligible := !tbInfo.IsEligible(s.forceReplicate)
	if ineligible {
		s.ineligibleTables.ReplaceOrInsert(newVersionedID(tbInfo.ID, tag))
	}
	for _, partition := range newPi.Definitions {
		vid := newVersionedID(partition.ID, tag)
		vid.target = unsafe.Pointer(tbInfo)
		s.partitionTables.ReplaceOrInsert(vid)
		if ineligible {
			s.ineligibleTables.ReplaceOrInsert(newVersionedID(partition.ID, tag))
		}
	}
	// TODO: is it necessary to print changes detailly?
	log.Debug("adjust partition success",
		zap.String("schema", tbInfo.TableName.Schema),
		zap.String("table", tbInfo.TableName.Table))
	return nil
}

func (s *SchemaSnapshot) HandleDDL(job *timodel.Job) error {
	if err := s.FillSchemaName(job); err != nil {
		return errors.Trace(err)
	}
	getWrapTableInfo := func(job *timodel.Job) *model.TableInfo {
		return model.WrapTableInfo(job.SchemaID, job.SchemaName,
			job.BinlogInfo.FinishedTS,
			job.BinlogInfo.TableInfo)
	}
	switch job.Type {
	case timodel.ActionCreateSchema:
		// get the DBInfo from job rawArgs
		err := s.createSchema(job.BinlogInfo.DBInfo, job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionModifySchemaCharsetAndCollate:
		err := s.replaceSchema(job.BinlogInfo.DBInfo, job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionDropSchema:
		err := s.dropSchema(job.SchemaID, job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionRenameTable:
		// first drop the table
		err := s.dropTable(job.TableID, job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
		// create table
		err = s.createTable(getWrapTableInfo(job), job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionRenameTables:
		err := s.renameTables(job, job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionCreateTable, timodel.ActionCreateView, timodel.ActionRecoverTable:
		err := s.createTable(getWrapTableInfo(job), job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionDropTable, timodel.ActionDropView:
		err := s.dropTable(job.TableID, job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}

	case timodel.ActionTruncateTable:
		// job.TableID is the old table id, different from table.ID
		err := s.truncateTable(job.TableID, getWrapTableInfo(job), job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	case timodel.ActionTruncateTablePartition, timodel.ActionAddTablePartition, timodel.ActionDropTablePartition:
		err := s.updatePartition(getWrapTableInfo(job), job.BinlogInfo.FinishedTS)
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
		err := s.replaceTable(getWrapTableInfo(job), job.BinlogInfo.FinishedTS)
		if err != nil {
			return errors.Trace(err)
		}
	}
	s.currentTs = job.BinlogInfo.FinishedTS
	return nil
}

func (s *SchemaSnapshot) renameTables(job *timodel.Job, currentTs uint64) error {
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
		newSchema, ok := s.SchemaByID(newSchemaIDs[i])
		if !ok {
			return cerror.ErrSnapshotSchemaNotFound.GenWithStackByArgs(newSchemaIDs[i])
		}
		newSchemaName := newSchema.Name.L
		tbInfo := model.WrapTableInfo(newSchemaIDs[i], newSchemaName, job.BinlogInfo.FinishedTS, tableInfo)
		err = s.createTable(tbInfo, currentTs)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *SchemaSnapshot) CurrentTs() uint64 {
	return s.currentTs
}

// TableCount counts tables in the snapshot.
func (s *SchemaSnapshot) TableCount() (count int) {
	s.IterTables(true, func(i *model.TableInfo) { count += 1 })
	return
}

// TableCount counts ineligible tables in the snapshot.
func (s *SchemaSnapshot) IneligibleTableCount() (count int) {
	s.IterTables(true, func(i *model.TableInfo) {
		if !i.IsEligible(s.forceReplicate) {
			count += 1
		}
	})
	return
}

// IterTables iterates all table in the snapshot.
func (s *SchemaSnapshot) IterTables(includeIneligible bool, f func(i *model.TableInfo)) {
	tag := negative(s.currentTs)
	for _, tables := range [2]*btree.BTree{s.tables, s.partitionTables} {
		var tableID int64 = -1
		tables.Ascend(func(i btree.Item) bool {
			x := i.(versionedID)
			if x.id != tableID && x.tag >= tag {
				tableID = x.id
				if x.target != nil && (includeIneligible || s.ineligibleTables.Get(newVersionedID(x.id, x.tag)) == nil) {
					f((*model.TableInfo)(x.target))
				}
			}
			return true
		})
	}
	return
}

func (s *SchemaSnapshot) IterSchemas(f func(i *timodel.DBInfo)) {
	tag := negative(s.currentTs)
	var schemaID int64 = -1
	s.schemas.Ascend(func(i btree.Item) bool {
		x := i.(versionedID)
		if x.id != schemaID && x.tag >= tag {
			schemaID = x.id
			if x.target != nil {
				f((*timodel.DBInfo)(x.target))
			}
		}
		return true
	})
}

////////////////////////////////////////////////////////////////

// Entity(schema or table) name with finish timestamp of the associated DDL job.
type versionedEntityName struct {
	schema string
	table  string
	// tag is `uint64.MAX - finish_timestamp`.
	tag uint64
	// the associated entity id, negative values are treated as invalid.
	target int64
}

// ID with finish timestamp of the associated DDL job.
type versionedID struct {
	id  int64
	tag uint64
	// the associated entity pointer.
	target unsafe.Pointer
}

func (v1 versionedEntityName) Less(than btree.Item) bool {
	v2 := than.(versionedEntityName)
	return v1.schema < v2.schema || (v1.schema == v2.schema && v1.table < v2.table)
}

func (v1 versionedID) Less(than btree.Item) bool {
	v2 := than.(versionedID)
	return v1.id < v2.id
}

func negative(x uint64) uint64 {
	return ^x
}

func newVersionedEntityName(schema string, table string, tag uint64) versionedEntityName {
	var target int64 = -1
	return versionedEntityName{schema, table, tag, target}
}

func newVersionedID(id int64, tag uint64) versionedID {
	var target unsafe.Pointer = unsafe.Pointer(nil)
	return versionedID{id, tag, target}
}

// TidySchemaSnapshot is for tests.
func TidySchemaSnapshot(snap *SchemaSnapshot) {
	snap.IterSchemas(func(dbInfo *timodel.DBInfo) {
		if len(dbInfo.Tables) == 0 {
			dbInfo.Tables = nil
		}
	})
	snap.IterTables(true, func(tableInfo *model.TableInfo) {
		tableInfo.TableInfoVersion = 0
		if len(tableInfo.Columns) == 0 {
			tableInfo.Columns = nil
		}
		if len(tableInfo.Indices) == 0 {
			tableInfo.Indices = nil
		}
		if len(tableInfo.ForeignKeys) == 0 {
			tableInfo.ForeignKeys = nil
		}
	})

	// the snapshot from meta doesn't know which ineligible tables that have existed in history
	// so we delete the ineligible tables which are already not exist
	// FIXME: correct it.
	// for tableID := range snap.ineligibleTableID {
	// 	if _, ok := snap.tables[tableID]; !ok {
	// 		delete(snap.ineligibleTableID, tableID)
	// 	}
	// }
	// the snapshot from meta doesn't know which tables are truncated, so we just ignore it

	snap.truncatedTables.Clear(true)
}
