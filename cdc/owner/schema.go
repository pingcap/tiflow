// Copyright 2021 PingCAP, Inc.
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

package owner

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	timeta "github.com/pingcap/tidb/meta"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type schemaWrap4Owner struct {
	entry.SchemaStorage
	filter                      filter.Filter
	config                      *config.ReplicaConfig
	id                          model.ChangeFeedID
	metricIgnoreDDLEventCounter prometheus.Counter
}

func newSchemaWrap4Owner(
	kvStorage tidbkv.Storage, startTs model.Ts,
	config *config.ReplicaConfig, id model.ChangeFeedID,
	filter filter.Filter,
) (*schemaWrap4Owner, error) {
	var meta *timeta.Meta

	if kvStorage != nil {
		var err error
		meta, err = kv.GetSnapshotMeta(kvStorage, startTs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	schemaStorage, err := entry.NewSchemaStorage(
		meta, startTs, config.ForceReplicate, id, util.RoleOwner, filter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// It is no matter to use an empty as timezone here because schemaWrap4Owner
	// doesn't use expression filter's method.

	schema := &schemaWrap4Owner{
		filter: filter,
		config: config,
		id:     id,
		metricIgnoreDDLEventCounter: changefeedIgnoredDDLEventCounter.
			WithLabelValues(id.Namespace, id.ID),
	}
	schema.SchemaStorage = schemaStorage
	return schema, nil
}

// AllPhysicalTables returns the table IDs of all tables and partition tables.
func (s *schemaWrap4Owner) AllPhysicalTables(
	ctx context.Context,
	ts model.Ts,
) ([]model.TableID, error) {
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
func (s *schemaWrap4Owner) AllTables(
	ctx context.Context,
	ts model.Ts,
) ([]*model.TableInfo, error) {
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

// IsIneligibleTable returns whether the table is ineligible.
// It uses the snapshot of the given ts to check the table.
func (s *schemaWrap4Owner) IsIneligibleTable(
	ctx context.Context,
	tableID model.TableID,
	ts model.Ts,
) (bool, error) {
	snap, err := s.GetSnapshot(ctx, ts)
	if err != nil {
		return false, err
	}
	return snap.IsIneligibleTableID(tableID), nil
}

// TODO: find a better way to refactor this function.
// buildRenameEvents gets a list of DDLEvent from a rename tables DDL job.
func (s *schemaWrap4Owner) buildRenameEvents(
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

// BuildDDLEvents builds ddl events from a DDL job.
// Note: If BuildDDLEvents return (nil, nil), it means the DDL Job should be ignored.
func (s *schemaWrap4Owner) BuildDDLEvents(
	ctx context.Context,
	job *timodel.Job,
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
	return s.filterDDLEvents(ddlEvents)
}

// TODO: delete this function after integration test passed.
func (s *schemaWrap4Owner) filterDDLEvents(ddlEvents []*model.DDLEvent) ([]*model.DDLEvent, error) {
	res := make([]*model.DDLEvent, 0, len(ddlEvents))
	for _, event := range ddlEvents {
		var (
			ignored bool
			err     error
		)
		if event.Type == timodel.ActionRenameTable {
			ignored, err = s.filter.ShouldDiscardDDL(
				event.StartTs,
				event.Type,
				event.PreTableInfo.TableName.Schema,
				event.PreTableInfo.TableName.Table,
				event.Query)
			if err != nil {
				return nil, errors.Trace(err)
			}
		} else {
			ignored, err = s.filter.ShouldDiscardDDL(
				event.StartTs,
				event.Type,
				event.TableInfo.TableName.Schema,
				event.TableInfo.TableName.Table,
				event.Query)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		if ignored {
			s.metricIgnoreDDLEventCounter.Inc()
			log.Error(
				"ignored DDL event should not be sent to owner"+
					"please report a bug to TiCDC if you see this log"+
					"but it is no harm to your replication",
				zap.String("namespace", s.id.Namespace),
				zap.String("changefeed", s.id.ID),
				zap.String("query", event.Query),
				zap.String("type", event.Type.String()),
				zap.String("schema", event.TableInfo.TableName.Schema),
				zap.String("table", event.TableInfo.TableName.Table),
				zap.Uint64("startTs", event.StartTs),
				zap.Uint64("commitTs", event.CommitTs),
			)
			continue
		}
		res = append(res, event)
	}
	return res, nil
}

func (s *schemaWrap4Owner) shouldIgnoreTable(t *model.TableInfo) bool {
	schemaName := t.TableName.Schema
	tableName := t.TableName.Table
	if s.filter.ShouldIgnoreTable(schemaName, tableName) {
		return true
	}
	if !t.IsEligible(s.config.ForceReplicate) {
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
	return false
}
