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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	schema "github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SchemaStorage stores the schema information with multi-version
type SchemaStorage interface {
	// GetSnapshot returns the nearest snapshot which currentTs is less than or
	// equal to the ts.
	// It may block caller when ts is larger than the resolvedTs of SchemaStorage.
	GetSnapshot(ctx context.Context, ts uint64) (*schema.Snapshot, error)
	// GetLastSnapshot returns the last snapshot
	GetLastSnapshot() *schema.Snapshot
	// HandleDDLJob creates a new snapshot in storage and handles the ddl job
	HandleDDLJob(job *timodel.Job) error

	// AllPhysicalTables returns the table IDs of all tables and partition tables.
	AllPhysicalTables(ctx context.Context, ts model.Ts) ([]model.TableID, error)

	// AllTables returns table info of all tables that are being replicated.
	AllTables(ctx context.Context, ts model.Ts) ([]*model.TableInfo, error)

	// BuildDDLEvents by parsing the DDL job
	BuildDDLEvents(ctx context.Context, job *timodel.Job) (ddlEvents []*model.DDLEvent, err error)

	// IsIneligibleTable returns whether the table is ineligible.
	// Ineligible means that the table does not have a primary key or unique key.
	IsIneligibleTable(ctx context.Context, tableID model.TableID, ts model.Ts) (bool, error)

	// AdvanceResolvedTs advances the resolved ts
	AdvanceResolvedTs(ts uint64)
	// ResolvedTs returns the resolved ts of the schema storage
	ResolvedTs() uint64
	// DoGC removes snaps that are no longer needed at the specified TS.
	// It returns the TS from which the oldest maintained snapshot is valid.
	DoGC(ts uint64) (lastSchemaTs uint64)
}

type schemaStorage struct {
	snaps   []*schema.Snapshot
	snapsMu sync.RWMutex

	gcTs          uint64
	resolvedTs    uint64
	schemaVersion int64

	filter filter.Filter

	forceReplicate bool

	id   model.ChangeFeedID
	role util.Role
}

// NewSchemaStorage creates a new schema storage
func NewSchemaStorage(
	storage tidbkv.Storage, startTs uint64,
	forceReplicate bool, id model.ChangeFeedID,
	role util.Role, filter filter.Filter,
) (SchemaStorage, error) {
	var (
		snap    *schema.Snapshot
		version int64
		err     error
	)
	// storage may be nil in some unit test cases.
	if storage == nil {
		snap = schema.NewEmptySnapshot(forceReplicate)
	} else {
		meta := kv.GetSnapshotMeta(storage, startTs)
		snap, err = schema.NewSnapshotFromMeta(id, meta, startTs, forceReplicate, filter)
		if err != nil {
			return nil, errors.Trace(err)
		}
		version, err = schema.GetSchemaVersion(meta)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return &schemaStorage{
		snaps:          []*schema.Snapshot{snap},
		resolvedTs:     startTs,
		forceReplicate: forceReplicate,
		filter:         filter,
		id:             id,
		schemaVersion:  version,
		role:           role,
	}, nil
}

// getSnapshot returns the snapshot which currentTs is less than(but most close to)
// or equal to the ts.
func (s *schemaStorage) getSnapshot(ts uint64) (*schema.Snapshot, error) {
	gcTs := atomic.LoadUint64(&s.gcTs)
	if ts < gcTs {
		// Unexpected error, caller should fail immediately.
		return nil, cerror.ErrSchemaStorageGCed.GenWithStackByArgs(ts, gcTs)
	}
	resolvedTs := atomic.LoadUint64(&s.resolvedTs)
	if ts > resolvedTs {
		// Caller should retry.
		return nil, cerror.ErrSchemaStorageUnresolved.GenWithStackByArgs(ts, resolvedTs)
	}
	s.snapsMu.RLock()
	defer s.snapsMu.RUnlock()
	// Here we search for the first snapshot whose currentTs is larger than ts.
	// So the result index -1 is the snapshot we want.
	i := sort.Search(len(s.snaps), func(i int) bool {
		return s.snaps[i].CurrentTs() > ts
	})
	// i == 0 has two meanings:
	// 1. The schema storage is empty.
	// 2. The ts is smaller than the first snapshot.
	// In both cases, we should return an error.
	if i == 0 {
		// Unexpected error, caller should fail immediately.
		return nil, cerror.ErrSchemaSnapshotNotFound.GenWithStackByArgs(ts)
	}
	return s.snaps[i-1], nil
}

// GetSnapshot returns the snapshot which of ts is specified
func (s *schemaStorage) GetSnapshot(ctx context.Context, ts uint64) (*schema.Snapshot, error) {
	var snap *schema.Snapshot

	// The infinite retry here is a temporary solution to the `ErrSchemaStorageUnresolved` caused by
	// DDL puller lagging too much.
	startTime := time.Now()
	logTime := startTime
	err := retry.Do(ctx, func() error {
		var err error
		snap, err = s.getSnapshot(ts)
		now := time.Now()
		if now.Sub(logTime) >= 30*time.Second && isRetryable(err) {
			log.Warn("GetSnapshot is taking too long, DDL puller stuck?",
				zap.Error(err),
				zap.Uint64("ts", ts),
				zap.Duration("duration", now.Sub(startTime)),
				zap.String("namespace", s.id.Namespace),
				zap.String("changefeed", s.id.ID),
				zap.String("role", s.role.String()))
			logTime = now
		}
		return err
	}, retry.WithBackoffBaseDelay(10), retry.WithIsRetryableErr(isRetryable))

	return snap, err
}

func isRetryable(err error) bool {
	return cerror.IsRetryableError(err) && cerror.ErrSchemaStorageUnresolved.Equal(err)
}

// GetLastSnapshot returns the last snapshot
func (s *schemaStorage) GetLastSnapshot() *schema.Snapshot {
	s.snapsMu.RLock()
	defer s.snapsMu.RUnlock()
	return s.snaps[len(s.snaps)-1]
}

// HandleDDLJob creates a new snapshot in storage and handles the ddl job
func (s *schemaStorage) HandleDDLJob(job *timodel.Job) error {
	if s.skipJob(job) {
		s.schemaVersion = job.BinlogInfo.SchemaVersion
		s.AdvanceResolvedTs(job.BinlogInfo.FinishedTS)
		return nil
	}
	s.snapsMu.Lock()
	defer s.snapsMu.Unlock()
	var snap *schema.Snapshot
	if len(s.snaps) > 0 {
		lastSnap := s.snaps[len(s.snaps)-1]
		// We use schemaVersion to check if an already-executed DDL job is processed for a second time.
		// Unexecuted DDL jobs should have largest schemaVersions.
		if job.BinlogInfo.FinishedTS <= lastSnap.CurrentTs() || job.BinlogInfo.SchemaVersion <= s.schemaVersion {
			log.Info("schemaStorage: ignore foregone DDL",
				zap.String("namespace", s.id.Namespace),
				zap.String("changefeed", s.id.ID),
				zap.String("DDL", job.Query),
				zap.Int64("jobID", job.ID),
				zap.Uint64("finishTs", job.BinlogInfo.FinishedTS),
				zap.Int64("schemaVersion", s.schemaVersion),
				zap.Int64("jobSchemaVersion", job.BinlogInfo.SchemaVersion),
				zap.String("role", s.role.String()))
			return nil
		}
		snap = lastSnap.Copy()
	} else {
		snap = schema.NewEmptySnapshot(s.forceReplicate)
	}
	if err := snap.HandleDDL(job); err != nil {
		log.Error("schemaStorage: update snapshot by the DDL job failed",
			zap.String("namespace", s.id.Namespace),
			zap.String("changefeed", s.id.ID),
			zap.String("schema", job.SchemaName),
			zap.String("table", job.TableName),
			zap.String("query", job.Query),
			zap.Uint64("finishedTs", job.BinlogInfo.FinishedTS),
			zap.String("role", s.role.String()),
			zap.Error(err))
		return errors.Trace(err)
	}
	s.snaps = append(s.snaps, snap)
	s.schemaVersion = job.BinlogInfo.SchemaVersion
	s.AdvanceResolvedTs(job.BinlogInfo.FinishedTS)
	log.Info("schemaStorage: update snapshot by the DDL job",
		zap.String("namespace", s.id.Namespace),
		zap.String("changefeed", s.id.ID),
		zap.String("schema", job.SchemaName),
		zap.String("table", job.TableName),
		zap.String("query", job.Query),
		zap.Uint64("finishedTs", job.BinlogInfo.FinishedTS),
		zap.Uint64("schemaVersion", uint64(s.schemaVersion)),
		zap.String("role", s.role.String()))
	return nil
}

// AllPhysicalTables returns the table IDs of all tables and partition tables.
func (s *schemaStorage) AllPhysicalTables(ctx context.Context, ts model.Ts) ([]model.TableID, error) {
	// NOTE: it's better to pre-allocate the vector. However, in the current implementation
	// we can't know how many valid tables in the snapshot.
	res := make([]model.TableID, 0)
	snap, err := s.GetSnapshot(ctx, ts)
	if err != nil {
		return nil, err
	}

	snap.IterTables(true, func(tblInfo *model.TableInfo) {
		if s.shouldIgnoreTable(tblInfo) {
			return
		}
		if pi := tblInfo.GetPartitionInfo(); pi != nil {
			for _, partition := range pi.Definitions {
				res = append(res, partition.ID)
			}
		} else {
			res = append(res, tblInfo.ID)
		}
	})
	log.Debug("get new schema snapshot",
		zap.Uint64("ts", ts),
		zap.Uint64("snapTs", snap.CurrentTs()),
		zap.Any("tables", res),
		zap.String("snapshot", snap.DumpToString()))

	return res, nil
}

// AllTables returns table info of all tables that are being replicated.
func (s *schemaStorage) AllTables(ctx context.Context, ts model.Ts) ([]*model.TableInfo, error) {
	tables := make([]*model.TableInfo, 0)
	snap, err := s.GetSnapshot(ctx, ts)
	if err != nil {
		return nil, err
	}
	snap.IterTables(true, func(tblInfo *model.TableInfo) {
		if !s.shouldIgnoreTable(tblInfo) {
			tables = append(tables, tblInfo)
		}
	})
	return tables, nil
}

func (s *schemaStorage) shouldIgnoreTable(t *model.TableInfo) bool {
	schemaName := t.TableName.Schema
	tableName := t.TableName.Table
	if s.filter.ShouldIgnoreTable(schemaName, tableName) {
		return true
	}
	if t.IsEligible(s.forceReplicate) {
		return false
	}

	// Sequence is not supported yet, and always ineligible.
	// Skip Warn to avoid confusion.
	// See https://github.com/pingcap/tiflow/issues/4559
	if !t.IsSequence() {
		log.Warn("skip ineligible table",
			zap.String("namespace", s.id.Namespace),
			zap.String("changefeed", s.id.ID),
			zap.Int64("tableID", t.ID),
			zap.Stringer("tableName", t.TableName),
		)
	}
	return true
}

// IsIneligibleTable returns whether the table is ineligible.
// It uses the snapshot of the given ts to check the table.
// Ineligible means that the table does not have a primary key
// or not null unique key.
func (s *schemaStorage) IsIneligibleTable(
	ctx context.Context, tableID model.TableID, ts model.Ts,
) (bool, error) {
	snap, err := s.GetSnapshot(ctx, ts)
	if err != nil {
		return false, err
	}
	return snap.IsIneligibleTableID(tableID), nil
}

// AdvanceResolvedTs advances the resolved. Not thread safe.
// NOTE: SHOULD NOT call it concurrently
func (s *schemaStorage) AdvanceResolvedTs(ts uint64) {
	if ts > s.ResolvedTs() {
		atomic.StoreUint64(&s.resolvedTs, ts)
	}
}

// ResolvedTs returns the resolved ts of the schema storage
func (s *schemaStorage) ResolvedTs() uint64 {
	return atomic.LoadUint64(&s.resolvedTs)
}

// DoGC removes snaps which of ts less than this specified ts
func (s *schemaStorage) DoGC(ts uint64) (lastSchemaTs uint64) {
	s.snapsMu.Lock()
	defer s.snapsMu.Unlock()
	var startIdx int
	for i, snap := range s.snaps {
		if snap.CurrentTs() > ts {
			break
		}
		startIdx = i
	}
	if startIdx == 0 {
		return s.snaps[0].CurrentTs()
	}
	if log.GetLevel() == zapcore.DebugLevel {
		log.Debug("Do GC in schema storage")
		for i := 0; i < startIdx; i++ {
			s.snaps[i].PrintStatus(log.Debug)
		}
	}

	// NOTE: Drop must be called to remove stale versions.
	s.snaps[startIdx-1].Drop()

	// copy the part of the slice that is needed instead of re-slicing it
	// to maximize efficiency of Go runtime GC.
	newSnaps := make([]*schema.Snapshot, len(s.snaps)-startIdx)
	copy(newSnaps, s.snaps[startIdx:])
	s.snaps = newSnaps

	lastSchemaTs = s.snaps[0].CurrentTs()
	atomic.StoreUint64(&s.gcTs, lastSchemaTs)
	return
}

// SkipJob skip the job should not be executed
// TiDB write DDL Binlog for every DDL Job,
// we must ignore jobs that are cancelled or rollback
// For older version TiDB, it writes DDL Binlog in the txn
// that the state of job is changed to *synced*
// Now, it writes DDL Binlog in the txn that the state of
// job is changed to *done* (before change to *synced*)
// At state *done*, it will be always and only changed to *synced*.
func (s *schemaStorage) skipJob(job *timodel.Job) bool {
	log.Debug("handle DDL new commit",
		zap.String("DDL", job.Query), zap.Stringer("job", job),
		zap.String("namespace", s.id.Namespace),
		zap.String("changefeed", s.id.ID),
		zap.String("role", s.role.String()))
	return !job.IsDone()
}

// BuildDDLEvents by parsing the DDL job
func (s *schemaStorage) BuildDDLEvents(
	ctx context.Context, job *timodel.Job,
) (ddlEvents []*model.DDLEvent, err error) {
	switch job.Type {
	case timodel.ActionRenameTables:
		// The result contains more than one DDLEvent for a rename tables job.
		ddlEvents, err = s.buildRenameEvents(ctx, job)
		if err != nil {
			return nil, errors.Trace(err)
		}
	default:
		// parse preTableInfo
		preSnap, err := s.GetSnapshot(ctx, job.BinlogInfo.FinishedTS-1)
		if err != nil {
			return nil, errors.Trace(err)
		}
		preTableInfo, err := preSnap.PreTableInfo(job)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// parse tableInfo
		var tableInfo *model.TableInfo
		err = preSnap.FillSchemaName(job)
		if err != nil {
			log.Error("build DDL event fail", zap.Any("job", job), zap.Error(err))
			return nil, errors.Trace(err)
		}
		// TODO: find a better way to refactor this. For example, drop table job should not
		// have table info.
		if job.BinlogInfo != nil && job.BinlogInfo.TableInfo != nil {
			tableInfo = model.WrapTableInfo(job.SchemaID, job.SchemaName, job.BinlogInfo.FinishedTS, job.BinlogInfo.TableInfo)

			// TODO: remove this after job is fixed by TiDB.
			// ref: https://github.com/pingcap/tidb/issues/43819
			if job.Type == timodel.ActionExchangeTablePartition {
				oldTableInfo, ok := preSnap.PhysicalTableByID(job.BinlogInfo.TableInfo.ID)
				if !ok {
					return nil, cerror.ErrSchemaStorageTableMiss.GenWithStackByArgs(job.TableID)
				}
				tableInfo.SchemaID = oldTableInfo.SchemaID
				tableInfo.TableName = oldTableInfo.TableName
			}
		} else {
			// Just retrieve the schema name for a DDL job that does not contain TableInfo.
			// Currently supported by cdc are: ActionCreateSchema, ActionDropSchema,
			// and ActionModifySchemaCharsetAndCollate.
			tableInfo = &model.TableInfo{
				TableName: model.TableName{Schema: job.SchemaName},
				Version:   job.BinlogInfo.FinishedTS,
			}
		}
		event := new(model.DDLEvent)
		event.FromJob(job, preTableInfo, tableInfo)
		ddlEvents = append(ddlEvents, event)
	}
	return ddlEvents, nil
}

// TODO: find a better way to refactor this function.
// buildRenameEvents gets a list of DDLEvent from a rename tables DDL job.
func (s *schemaStorage) buildRenameEvents(
	ctx context.Context, job *timodel.Job,
) ([]*model.DDLEvent, error) {
	var (
		oldSchemaIDs, newSchemaIDs, oldTableIDs []int64
		newTableNames, oldSchemaNames           []*timodel.CIStr
		ddlEvents                               []*model.DDLEvent
	)
	err := job.DecodeArgs(&oldSchemaIDs, &newSchemaIDs,
		&newTableNames, &oldTableIDs, &oldSchemaNames)
	if err != nil {
		return nil, errors.Trace(err)
	}

	multiTableInfos := job.BinlogInfo.MultipleTableInfos
	if len(multiTableInfos) != len(oldSchemaIDs) ||
		len(multiTableInfos) != len(newSchemaIDs) ||
		len(multiTableInfos) != len(newTableNames) ||
		len(multiTableInfos) != len(oldTableIDs) ||
		len(multiTableInfos) != len(oldSchemaNames) {
		return nil, cerror.ErrInvalidDDLJob.GenWithStackByArgs(job.ID)
	}

	preSnap, err := s.GetSnapshot(ctx, job.BinlogInfo.FinishedTS-1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for i, tableInfo := range multiTableInfos {
		newSchema, ok := preSnap.SchemaByID(newSchemaIDs[i])
		if !ok {
			return nil, cerror.ErrSnapshotSchemaNotFound.GenWithStackByArgs(
				newSchemaIDs[i])
		}
		newSchemaName := newSchema.Name.O
		oldSchemaName := oldSchemaNames[i].O
		event := new(model.DDLEvent)
		preTableInfo, ok := preSnap.PhysicalTableByID(tableInfo.ID)
		if !ok {
			return nil, cerror.ErrSchemaStorageTableMiss.GenWithStackByArgs(
				job.TableID)
		}

		tableInfo := model.WrapTableInfo(newSchemaIDs[i], newSchemaName,
			job.BinlogInfo.FinishedTS, tableInfo)
		event.FromJobWithArgs(job, preTableInfo, tableInfo, oldSchemaName, newSchemaName)
		ddlEvents = append(ddlEvents, event)
	}
	return ddlEvents, nil
}

// MockSchemaStorage is for tests.
type MockSchemaStorage struct {
	Resolved uint64
}

// AllPhysicalTables implements SchemaStorage.
func (s *MockSchemaStorage) AllPhysicalTables(ctx context.Context, ts model.Ts) ([]model.TableID, error) {
	return nil, nil
}

// IsIneligibleTable implements SchemaStorage.
func (s *MockSchemaStorage) IsIneligibleTable(ctx context.Context, tableID model.TableID, ts model.Ts) (bool, error) {
	return true, nil
}

// AllTables implements SchemaStorage.
func (s *MockSchemaStorage) AllTables(ctx context.Context, ts model.Ts) ([]*model.TableInfo, error) {
	return nil, nil
}

// GetSnapshot implements SchemaStorage.
func (s *MockSchemaStorage) GetSnapshot(ctx context.Context, ts uint64) (*schema.Snapshot, error) {
	return nil, nil
}

// GetLastSnapshot implements SchemaStorage.
func (s *MockSchemaStorage) GetLastSnapshot() *schema.Snapshot {
	return nil
}

// HandleDDLJob implements SchemaStorage.
func (s *MockSchemaStorage) HandleDDLJob(job *timodel.Job) error {
	return nil
}

// BuildDDLEvents implements SchemaStorage.
func (s *MockSchemaStorage) BuildDDLEvents(
	_ context.Context, _ *timodel.Job,
) (ddlEvents []*model.DDLEvent, err error) {
	return nil, nil
}

// AdvanceResolvedTs implements SchemaStorage.
func (s *MockSchemaStorage) AdvanceResolvedTs(ts uint64) {
	atomic.StoreUint64(&s.Resolved, ts)
}

// ResolvedTs implements SchemaStorage.
func (s *MockSchemaStorage) ResolvedTs() uint64 {
	return atomic.LoadUint64(&s.Resolved)
}

// DoGC implements SchemaStorage.
func (s *MockSchemaStorage) DoGC(ts uint64) uint64 {
	return atomic.LoadUint64(&s.Resolved)
}
