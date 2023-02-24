// Copyright 2023 PingCAP, Inc.
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

// TODO: remove this tag after ddlManager is used.
// nolint:unused,unparam
package owner

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/cdc/redo"
	"go.uber.org/zap"
)

// globalDDLs is the DDLs that affect all tables in the changefeed.
// we need to wait all tables checkpointTs reach the DDL commitTs
// before we can execute the DDL.
var globalDDLs = []timodel.ActionType{
	timodel.ActionCreateSchema,
	timodel.ActionDropSchema,
	timodel.ActionModifySchemaCharsetAndCollate,
	// We treat create table ddl as a global ddl, because before we execute the ddl,
	// there is no a tablePipeline for the new table. So we can't prevent the checkpointTs
	// from advancing. To solve this problem, we just treat create table ddl as a global ddl here.
	// TODO: Find a better way to handle create table ddl.
	timodel.ActionCreateTable,
	timodel.ActionRenameTable,
	timodel.ActionRenameTables,
	timodel.ActionExchangeTablePartition,
}

// ddlManager holds the pending DDL events of all tables and responsible for
// executing them to downstream.
// It also provides the ability to calculate the barrier of a changefeed.
type ddlManager struct {
	changfeedID  model.ChangeFeedID
	startTs      model.Ts
	checkpointTs model.Ts
	// use to pull DDL jobs from TiDB
	ddlPuller puller.DDLPuller
	// schema store multiple version of schema, it is used by scheduler
	schema *schemaWrap4Owner
	// redoManager is used to send DDL events to redo log and get redo resolvedTs.
	redoManager redo.LogManager
	// ddlSink is used to ddlSink DDL events to the downstream
	ddlSink DDLSink
	// tableCheckpoint store the tableCheckpoint of each table. We need to wait
	// for the tableCheckpoint to reach the next ddl commitTs before executing the ddl
	tableCheckpoint map[model.TableName]model.Ts
	// pendingDDLs store the pending DDL events of all tables
	// the DDL events in the same table are ordered by commitTs.
	pendingDDLs map[model.TableName][]*model.DDLEvent
	// executingDDL is the ddl that is currently being executed,
	// it is nil if there is no ddl being executed.
	executingDDL *model.DDLEvent
	// justSentDDL is the ddl that just be sent to the downstream in the current tick.
	// we need it to prevent the checkpointTs from advancing in the same tick.
	justSentDDL *model.DDLEvent
	// tableInfoCache is the tables that the changefeed is watching.
	// And it contains only the tables of the ddl that have been processed.
	// The ones that have not been executed yet do not have.
	tableInfoCache      []*model.TableInfo
	physicalTablesCache []model.TableID

	BDRMode       bool
	sinkType      model.DownStreamType
	ddlResolvedTs model.Ts
}

func newDDLManager(
	changefeedID model.ChangeFeedID,
	startTs model.Ts,
	checkpointTs model.Ts,
	ddlSink DDLSink,
	ddlPuller puller.DDLPuller,
	schema *schemaWrap4Owner,
	redoManager redo.LogManager,
	sinkType model.DownStreamType,
	bdrMode bool,
) *ddlManager {
	log.Info("create ddl manager",
		zap.String("namaspace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID),
		zap.Uint64("startTs", startTs),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Bool("bdrMode", bdrMode),
	)
	return &ddlManager{
		changfeedID:   changefeedID,
		ddlSink:       ddlSink,
		ddlPuller:     ddlPuller,
		schema:        schema,
		redoManager:   redoManager,
		startTs:       startTs,
		checkpointTs:  checkpointTs,
		ddlResolvedTs: checkpointTs,
		BDRMode:       bdrMode,
		// use the passed sinkType after we support get resolvedTs from sink
		sinkType:        model.DB,
		tableCheckpoint: make(map[model.TableName]model.Ts),
		pendingDDLs:     make(map[model.TableName][]*model.DDLEvent),
	}
}

// tick the ddlHandler, it does the following things:
// 1. get DDL jobs from ddlPuller.
// 2. uses schema to turn DDL jobs into DDLEvents.
// 3. applies DDL jobs to the schema.
// 4. send DDLEvents to redo log.
// 5. adds the DDLEvents to the ddlHandler.pendingDDLs
// 6. iterates the ddlHandler.pendingDDLs, find next DDL event to be executed.
// 7.checks if checkpointTs reach next ddl commitTs, if so, execute the ddl.
// 8. removes the executed DDL events from executingDDL and pendingDDLs.
func (m *ddlManager) tick(
	ctx context.Context,
	checkpointTs model.Ts,
	tableCheckpoint map[model.TableName]model.Ts,
) ([]model.TableID, model.Barrier, error) {
	barrier := model.Barrier{}

	m.updateCheckpointTs(checkpointTs, tableCheckpoint)

	currentTables, err := m.allTables(ctx)
	if err != nil {
		return nil, barrier, errors.Trace(err)
	}

	tableIDs, err := m.allPhysicalTables(ctx)
	if err != nil {
		return nil, barrier, errors.Trace(err)
	}

	m.ddlSink.emitCheckpointTs(m.checkpointTs, currentTables)

	for {
		ts, job := m.ddlPuller.PopFrontDDL()
		if job == nil {
			m.schema.schemaStorage.AdvanceResolvedTs(ts)
			break
		}

		if job != nil && job.BinlogInfo != nil {
			log.Info("handle a ddl job",
				zap.String("namespace", m.changfeedID.Namespace),
				zap.String("ID", m.changfeedID.ID),
				zap.Any("ddlJob", job))
			events, err := m.schema.BuildDDLEvents(job)
			if err != nil {
				return nil, barrier, err
			}
			// Apply ddl to update changefeed schema.
			err = m.schema.HandleDDL(job)
			if err != nil {
				return nil, barrier, err
			}
			// Clear the table cache after the schema is updated.
			m.cleanCache()

			for _, event := range events {
				// If changefeed is in BDRMode, skip ddl.
				if m.BDRMode {
					log.Info("changefeed is in BDRMode, skip a ddl event",
						zap.String("namespace", m.changfeedID.Namespace),
						zap.String("ID", m.changfeedID.ID),
						zap.Any("ddlEvent", event))
					continue
				}

				if event.TableInfo != nil &&
					m.schema.IsIneligibleTableID(event.TableInfo.TableName.TableID) {
					log.Warn("ignore the DDL event of ineligible table",
						zap.String("changefeed", m.changfeedID.ID), zap.Any("ddl", event))
					continue
				}
				tableName := event.TableInfo.TableName
				// Add all valid DDL events to the pendingDDLs.
				m.pendingDDLs[tableName] = append(m.pendingDDLs[tableName], event)
			}

			// Send DDL events to redo log.
			// Fixme: get redo resolvedTs from redo log.
			if m.redoManager.Enabled() {
				for _, event := range events {
					err := m.redoManager.EmitDDLEvent(ctx, event)
					if err != nil {
						return nil, barrier, err
					}
				}
			}

			// Refresh checkpointTs and tableInfoCache when a ddl job is received
			currentTables, err := m.allTables(ctx)
			if err != nil {
				return nil, barrier, err
			}
			m.ddlSink.emitCheckpointTs(m.checkpointTs, currentTables)
		}
	}

	// Use ddlPuller ResolvedTs to update ddlResolvedTs.
	if m.ddlResolvedTs <= m.ddlPuller.ResolvedTs() {
		m.ddlResolvedTs = m.ddlPuller.ResolvedTs()
	}

	nextDDL := m.getNextDDL()
	if nextDDL != nil {
		if m.checkpointTs > nextDDL.CommitTs {
			log.Panic("checkpointTs is greater than next ddl commitTs",
				zap.Uint64("checkpointTs", m.checkpointTs),
				zap.Uint64("commitTs", nextDDL.CommitTs))
		}

		// TODO: Complete this logic, when sinkType is not DB,
		// we should not block the execution of DDLs by the checkpointTs.
		if m.sinkType == model.Other {
			log.Panic("sink type is not DB, it never happens in current version")
		}

		// TiCDC guarantees all dml(s) that happen before a ddl was sent to
		// downstream when this ddl is sent. So, we need to wait checkpointTs is
		// fullyBlocked at ddl commitTs (equivalent to ddl commitTs here) before we
		// execute the next ddl.
		// For example, let say there are some events are replicated by cdc:
		// [dml-1(ts=5), dml-2(ts=8), ddl-1(ts=11), ddl-2(ts=12)].
		// We need to wait `checkpointTs == ddlCommitTs(ts=11)` before execute ddl-1.
		if m.checkpointTs == nextDDL.CommitTs {
			log.Info("execute a ddl event",
				zap.String("query", nextDDL.Query),
				zap.Uint64("commitTs", nextDDL.CommitTs),
				zap.Uint64("checkpointTs", m.checkpointTs))

			if m.executingDDL == nil {
				m.executingDDL = nextDDL
				m.cleanCache()
			}

			err := m.executeDDL(ctx)
			if err != nil {
				return nil, barrier, err
			}
		}
	}

	barrier = m.barrier()

	return tableIDs, barrier, nil
}

// executeDDL executes ddlManager.executingDDL.
func (m *ddlManager) executeDDL(ctx context.Context) error {
	m.justSentDDL = nil
	if m.executingDDL == nil {
		return nil
	}
	done, err := m.ddlSink.emitDDLEvent(ctx, m.executingDDL)
	if err != nil {
		return err
	}
	if done {
		tableName := m.executingDDL.TableInfo.TableName
		// Set it to nil first to accelerate GC.
		m.pendingDDLs[tableName][0] = nil
		m.pendingDDLs[tableName] = m.pendingDDLs[tableName][1:]
		m.schema.schemaStorage.DoGC(m.executingDDL.CommitTs - 1)
		m.justSentDDL = m.executingDDL
		m.executingDDL = nil
	}
	return nil
}

// getNextDDL returns the next ddl event to execute.
func (m *ddlManager) getNextDDL() *model.DDLEvent {
	if m.executingDDL != nil {
		return m.executingDDL
	}
	var res *model.DDLEvent
	for tb, ddls := range m.pendingDDLs {
		if len(ddls) == 0 {
			log.Debug("no more ddl event, gc the table from pendingDDLs",
				zap.String("table", tb.String()))
			delete(m.pendingDDLs, tb)
			continue
		}
		if res == nil || res.CommitTs > ddls[0].CommitTs {
			res = ddls[0]
		}
	}
	return res
}

// updateCheckpointTs updates ddlHandler's tableCheckpoint and checkpointTs.
func (m *ddlManager) updateCheckpointTs(checkpointTs model.Ts,
	tableCheckpoint map[model.TableName]model.Ts,
) {
	m.checkpointTs = checkpointTs
	// update tableCheckpoint
	for table, ts := range tableCheckpoint {
		m.tableCheckpoint[table] = ts
	}

	// gc tableCheckpoint
	for table := range m.tableCheckpoint {
		if _, ok := tableCheckpoint[table]; !ok {
			delete(m.tableCheckpoint, table)
		}
	}
}

// getAllTableNextDDL returns the next DDL of all tables.
func (m *ddlManager) getAllTableNextDDL() []*model.DDLEvent {
	res := make([]*model.DDLEvent, 0, 1)
	for _, events := range m.pendingDDLs {
		if len(events) > 0 {
			res = append(res, events[0])
		}
	}
	return res
}

// barrier returns ddlResolvedTs and tableBarrier
func (m *ddlManager) barrier() model.Barrier {
	var tableBarrier []model.TableBarrier

	minTableBarrierTs := m.ddlResolvedTs
	globalBarrierTs := m.ddlResolvedTs

	ddls := m.getAllTableNextDDL()
	if m.justSentDDL != nil {
		ddls = append(ddls, m.justSentDDL)
	}
	for _, ddl := range ddls {
		// When there is a global DDL, we need to wait all tables
		// checkpointTs reach its commitTs before we can execute it.
		if isGlobalDDL(ddl) {
			if ddl.CommitTs < globalBarrierTs {
				globalBarrierTs = ddl.CommitTs
			}
		} else {
			ids := getPhysicalTableIDs(ddl)
			for _, id := range ids {
				tableBarrier = append(tableBarrier, model.TableBarrier{
					ID:        id,
					BarrierTs: ddl.CommitTs,
				})
			}
		}

		// minTableBarrierTs is the min commitTs of all tables DDLs,
		// it is used to prevent the checkpointTs from advancing too fast.
		if ddl.CommitTs < minTableBarrierTs {
			minTableBarrierTs = ddl.CommitTs
		}
	}

	m.justSentDDL = nil
	return model.Barrier{
		GlobalBarrierTs:   globalBarrierTs,
		TableBarrier:      tableBarrier,
		MinTableBarrierTs: minTableBarrierTs,
	}
}

// allTables returns all tables in the schema that
// less or equal than the checkpointTs.
func (m *ddlManager) allTables(ctx context.Context) ([]*model.TableInfo, error) {
	if m.tableInfoCache != nil {
		return m.tableInfoCache, nil
	}
	var err error

	ts := m.getSnapshotTs()
	// If there is an executing ddl at the checkpointTs, we need to get all tables
	// before the ddl commitTs. So we always use the checkpointTs-1 to get the snapshot.
	m.tableInfoCache, err = m.schema.AllTablesNew(ctx, ts)
	if err != nil {
		return nil, err
	}
	log.Debug("changefeed current tables updated",
		zap.String("namespace", m.changfeedID.Namespace),
		zap.String("changefeed", m.changfeedID.ID),
		zap.Uint64("checkpointTs", ts),
		zap.Any("tables", m.tableInfoCache),
	)
	return m.tableInfoCache, nil
}

// allPhysicalTables returns all table ids in the schema
// that less or equal than the checkpointTs.
func (m *ddlManager) allPhysicalTables(ctx context.Context) ([]model.TableID, error) {
	if m.physicalTablesCache != nil {
		return m.physicalTablesCache, nil
	}
	var err error

	ts := m.getSnapshotTs()
	// If there is an executing ddl at the checkpointTs, we need to get all physical tables
	// before the ddl commitTs. So we always use the checkpointTs-1 to get the snapshot.
	m.physicalTablesCache, err = m.schema.AllPhysicalTablesNew(ctx, ts)
	if err != nil {
		return nil, err
	}
	log.Debug("changefeed physical tables updated",
		zap.String("namespace", m.changfeedID.Namespace),
		zap.String("changefeed", m.changfeedID.ID),
		zap.Uint64("checkpointTs", ts),
		zap.Any("tables", m.physicalTablesCache),
	)
	return m.physicalTablesCache, nil
}

// getSnapshotTs returns the ts that we should use
// to get the snapshot of the schema, the rules are:
// 1. If the changefeed is just started, we use the startTs,
// otherwise we use the checkpointTs.
// 2. If the changefeed is in BDRMode, we use the ddlManager.ddlResolvedTs.
// Since TiCDC ignore the DDLs in BDRMode, we don't need to care about whether
// the DDLs are executed or not. We should use the ddlResolvedTs to get the up-to-date
// schema.
func (m *ddlManager) getSnapshotTs() uint64 {
	ts := m.checkpointTs

	if m.BDRMode {
		ts = m.ddlResolvedTs
	}

	if m.checkpointTs == m.startTs+1 {
		// If checkpointTs is equal to startTs+1, it means that the changefeed
		// is just started, and the physicalTablesCache is empty. So we need to
		// get all tables from the snapshot at the startTs.
		ts = m.startTs
	}

	log.Debug("snapshotTs", zap.Uint64("ts", ts))
	return ts
}

// cleanCache cleans the tableInfoCache and physicalTablesCache.
// It should be called after a DDL is applied to schema or a DDL
// is sent to downstream successfully.
func (m *ddlManager) cleanCache() {
	m.tableInfoCache = nil
	m.physicalTablesCache = nil
}

// getPhysicalTableIDs get all related physical table ids of a ddl event.
// It is a helper function to calculate tableBarrier.
func getPhysicalTableIDs(ddl *model.DDLEvent) []model.TableID {
	res := make([]model.TableID, 0, 1)
	table := ddl.TableInfo
	if ddl.PreTableInfo != nil {
		table = ddl.PreTableInfo
	}
	if table == nil {
		// If the table is nil, it means that the ddl is a global ddl.
		// It should never go here.
		log.Panic("tableInfo of this ddl is nil", zap.Any("ddl", ddl))
	}
	res = append(res, table.ID)
	partitionInfo := table.TableInfo.GetPartitionInfo()
	if partitionInfo != nil {
		for _, def := range partitionInfo.Definitions {
			res = append(res, def.ID)
		}
	}
	return res
}

// isGlobalDDL returns whether the ddl is a global ddl.
func isGlobalDDL(ddl *model.DDLEvent) bool {
	for _, tp := range globalDDLs {
		if ddl.Type == tp {
			return true
		}
	}
	return false
}
