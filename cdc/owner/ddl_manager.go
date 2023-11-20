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

package owner

import (
	"context"
	"math/rand"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/puller"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/filter"
	"go.uber.org/zap"
)

// tableBarrierNumberLimit is used to limit the number
// of tableBarrier in a single barrier.
const tableBarrierNumberLimit = 256

// The ddls below is globalDDLs, they affect all tables in the changefeed.
// we need to wait all tables checkpointTs reach the DDL commitTs
// before we can execute the DDL.
//timodel.ActionCreateSchema
//timodel.ActionDropSchema
//timodel.ActionModifySchemaCharsetAndCollate
//// We treat create table ddl as a global ddl, because before we execute the ddl,
//// there is no a tablePipeline for the new table. So we can't prevent the checkpointTs
//// from advancing. To solve this problem, we just treat create table ddl as a global ddl here.
//// TODO: Find a better way to handle create table ddl.
//timodel.ActionCreateTable
//timodel.ActionRenameTable
//timodel.ActionRenameTables
//timodel.ActionExchangeTablePartition

// nonGlobalDDLs are the DDLs that only affect related table
// so that we should only block related table before execute them.
var nonGlobalDDLs = map[timodel.ActionType]struct{}{
	timodel.ActionDropTable:                    {},
	timodel.ActionAddColumn:                    {},
	timodel.ActionDropColumn:                   {},
	timodel.ActionAddIndex:                     {},
	timodel.ActionDropIndex:                    {},
	timodel.ActionTruncateTable:                {},
	timodel.ActionModifyColumn:                 {},
	timodel.ActionSetDefaultValue:              {},
	timodel.ActionModifyTableComment:           {},
	timodel.ActionRenameIndex:                  {},
	timodel.ActionAddTablePartition:            {},
	timodel.ActionDropTablePartition:           {},
	timodel.ActionCreateView:                   {},
	timodel.ActionModifyTableCharsetAndCollate: {},
	timodel.ActionTruncateTablePartition:       {},
	timodel.ActionDropView:                     {},
	timodel.ActionRecoverTable:                 {},
	timodel.ActionAddPrimaryKey:                {},
	timodel.ActionDropPrimaryKey:               {},
	timodel.ActionRebaseAutoID:                 {},
	timodel.ActionAlterIndexVisibility:         {},
	timodel.ActionMultiSchemaChange:            {},
	timodel.ActionReorganizePartition:          {},
	timodel.ActionAlterTTLInfo:                 {},
	timodel.ActionAlterTTLRemove:               {},
}

var redoBarrierDDLs = map[timodel.ActionType]struct{}{
	timodel.ActionCreateTable:            {},
	timodel.ActionTruncateTable:          {},
	timodel.ActionAddTablePartition:      {},
	timodel.ActionTruncateTablePartition: {},
	timodel.ActionRecoverTable:           {},
	timodel.ActionReorganizePartition:    {},
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
	// redoDDLManager is used to send DDL events to redo log and get redo resolvedTs.
	redoDDLManager  redo.DDLManager
	redoMetaManager redo.MetaManager
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
	sinkType      model.DownstreamType
	ddlResolvedTs model.Ts
}

func newDDLManager(
	changefeedID model.ChangeFeedID,
	startTs model.Ts,
	checkpointTs model.Ts,
	ddlSink DDLSink,
	ddlPuller puller.DDLPuller,
	schema *schemaWrap4Owner,
	redoManager redo.DDLManager,
	redoMetaManager redo.MetaManager,
	sinkType model.DownstreamType,
	bdrMode bool,
) *ddlManager {
	log.Info("create ddl manager",
		zap.String("namaspace", changefeedID.Namespace),
		zap.String("changefeed", changefeedID.ID),
		zap.Uint64("startTs", startTs),
		zap.Uint64("checkpointTs", checkpointTs),
		zap.Bool("bdrMode", bdrMode),
		zap.Stringer("sinkType", sinkType))

	return &ddlManager{
		changfeedID:     changefeedID,
		ddlSink:         ddlSink,
		ddlPuller:       ddlPuller,
		schema:          schema,
		redoDDLManager:  redoManager,
		redoMetaManager: redoMetaManager,
		startTs:         startTs,
		checkpointTs:    checkpointTs,
		ddlResolvedTs:   startTs,
		BDRMode:         bdrMode,
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
) ([]model.TableID, *schedulepb.BarrierWithMinTs, error) {
	m.justSentDDL = nil
	m.updateCheckpointTs(checkpointTs, tableCheckpoint)

	currentTables, err := m.allTables(ctx)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	if m.executingDDL == nil {
		m.ddlSink.emitCheckpointTs(m.checkpointTs, currentTables)
	}

	tableIDs, err := m.allPhysicalTables(ctx)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	// drain all ddl jobs from ddlPuller
	for {
		_, job := m.ddlPuller.PopFrontDDL()
		// no more ddl jobs
		if job == nil {
			break
		}

		if job != nil && job.BinlogInfo != nil {
			log.Info("handle a ddl job",
				zap.String("namespace", m.changfeedID.Namespace),
				zap.String("ID", m.changfeedID.ID),
				zap.Int64("tableID", job.TableID),
				zap.Int64("jobID", job.ID),
				zap.String("query", job.Query),
				zap.Uint64("finishedTs", job.BinlogInfo.FinishedTS),
			)
			events, err := m.schema.BuildDDLEvents(ctx, job)
			if err != nil {
				return nil, nil, err
			}

			for _, event := range events {
				// TODO: find a better place to do this check
				// check if the ddl event is belong to an ineligible table.
				// If so, we should ignore it.
				if !filter.IsSchemaDDL(event.Type) {
					ignore, err := m.schema.
						IsIneligibleTable(ctx, event.TableInfo.TableName.TableID, event.CommitTs)
					if err != nil {
						return nil, nil, errors.Trace(err)
					}
					if ignore {
						log.Warn("ignore the DDL event of ineligible table",
							zap.String("changefeed", m.changfeedID.ID), zap.Any("ddl", event))
						continue
					}
				}

				tableName := event.TableInfo.TableName
				// Add all valid DDL events to the pendingDDLs.
				m.pendingDDLs[tableName] = append(m.pendingDDLs[tableName], event)
			}

			// Send DDL events to redo log.
			if m.redoDDLManager.Enabled() {
				for _, event := range events {
					err := m.redoDDLManager.EmitDDLEvent(ctx, event)
					if err != nil {
						return nil, nil, err
					}
				}
			}
		}
	}

	// advance resolvedTs
	ddlRts := m.ddlPuller.ResolvedTs()
	m.schema.AdvanceResolvedTs(ddlRts)
	if m.redoDDLManager.Enabled() {
		err := m.redoDDLManager.UpdateResolvedTs(ctx, ddlRts)
		if err != nil {
			return nil, nil, err
		}
		redoFlushedDDLRts := m.redoDDLManager.GetResolvedTs()
		if redoFlushedDDLRts < ddlRts {
			ddlRts = redoFlushedDDLRts
		}
	}
	if m.ddlResolvedTs <= ddlRts {
		m.ddlResolvedTs = ddlRts
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
		if m.sinkType != model.DB {
			log.Panic("Downstream type is not DB, it never happens in current version")
		}

		if m.shouldExecDDL(nextDDL) {
			if m.executingDDL == nil {
				log.Info("execute a ddl event",
					zap.String("query", nextDDL.Query),
					zap.Uint64("commitTs", nextDDL.CommitTs),
					zap.Uint64("checkpointTs", m.checkpointTs))
				m.executingDDL = nextDDL
			}
			err := m.executeDDL(ctx)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	return tableIDs, m.barrier(), nil
}

func (m *ddlManager) shouldExecDDL(nextDDL *model.DDLEvent) bool {
	// TiCDC guarantees all dml(s) that happen before a ddl was sent to
	// downstream when this ddl is sent. So, we need to wait checkpointTs is
	// fullyBlocked at ddl commitTs (equivalent to ddl commitTs here) before we
	// execute the next ddl.
	// For example, let say there are some events are replicated by cdc:
	// [dml-1(ts=5), dml-2(ts=8), dml-3(ts=11), ddl-1(ts=11), ddl-2(ts=12)].
	// We need to wait `checkpointTs == ddlCommitTs(ts=11)` before executing ddl-1.
	checkpointReachBarrier := m.checkpointTs == nextDDL.CommitTs

	redoCheckpointReachBarrier := true
	redoDDLResolvedTsExceedBarrier := true
	if m.redoMetaManager.Enabled() {
		if !m.redoDDLManager.Enabled() {
			log.Panic("Redo meta manager is enabled but redo ddl manager is not enabled")
		}
		flushed := m.redoMetaManager.GetFlushedMeta()
		// Use the same example as above, let say there are some events are replicated by cdc:
		// [dml-1(ts=5), dml-2(ts=8), dml-3(ts=11), ddl-1(ts=11), ddl-2(ts=12)].
		// Suppose redoCheckpointTs=10 and ddl-1(ts=11) is executed, the redo apply operation
		// would fail when applying the old data dml-3(ts=11) to a new schmea. Therefore, We
		// need to wait `redoCheckpointTs == ddlCommitTs(ts=11)` before executing ddl-1.
		redoCheckpointReachBarrier = flushed.CheckpointTs == nextDDL.CommitTs

		// If redo is enabled, m.ddlResolvedTs == redoDDLManager.GetResolvedTs(), so we need to
		// wait nextDDL to be written to redo log before executing this DDL.
		redoDDLResolvedTsExceedBarrier = m.ddlResolvedTs >= nextDDL.CommitTs
	}

	return checkpointReachBarrier && redoCheckpointReachBarrier && redoDDLResolvedTsExceedBarrier
}

// executeDDL executes ddlManager.executingDDL.
func (m *ddlManager) executeDDL(ctx context.Context) error {
	if m.executingDDL == nil {
		return nil
	}

	// If changefeed is in BDRMode, skip ddl.
	if m.BDRMode {
		log.Info("changefeed is in BDRMode, skip a ddl event",
			zap.String("namespace", m.changfeedID.Namespace),
			zap.String("ID", m.changfeedID.ID),
			zap.Any("ddlEvent", m.executingDDL))
		tableName := m.executingDDL.TableInfo.TableName
		// Set it to nil first to accelerate GC.
		m.pendingDDLs[tableName][0] = nil
		m.pendingDDLs[tableName] = m.pendingDDLs[tableName][1:]
		m.schema.DoGC(m.executingDDL.CommitTs - 1)
		m.justSentDDL = m.executingDDL
		m.executingDDL = nil
		m.cleanCache()
		return nil
	}

	failpoint.Inject("ExecuteNotDone", func() {
		// This ddl will never finish executing.
		// It is used to test the logic that a ddl only block the related table
		// and other tables can still advance.
		if m.executingDDL.TableInfo.TableName.Table == "ddl_not_done" {
			time.Sleep(time.Second * 1)
			failpoint.Return(nil)
		}
	})

	failpoint.Inject("ExecuteDDLSlowly", func() {
		lag := time.Duration(rand.Intn(5000)) * time.Millisecond
		log.Warn("execute ddl slowly", zap.Duration("lag", lag))
		time.Sleep(lag)
	})

	done, err := m.ddlSink.emitDDLEvent(ctx, m.executingDDL)
	if err != nil {
		return err
	}
	if done {
		tableName := m.executingDDL.TableInfo.TableName
		log.Info("execute a ddl event successfully",
			zap.String("ddl", m.executingDDL.Query),
			zap.Uint64("commitTs", m.executingDDL.CommitTs),
			zap.Stringer("table", tableName),
		)
		// Set it to nil first to accelerate GC.
		m.pendingDDLs[tableName][0] = nil
		m.pendingDDLs[tableName] = m.pendingDDLs[tableName][1:]
		m.schema.DoGC(m.executingDDL.CommitTs - 1)
		m.justSentDDL = m.executingDDL
		m.executingDDL = nil
		m.cleanCache()
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
func (m *ddlManager) barrier() *schedulepb.BarrierWithMinTs {
	barrier := schedulepb.NewBarrierWithMinTs(m.ddlResolvedTs)
	tableBarrierMap := make(map[model.TableID]model.Ts)
	ddls := m.getAllTableNextDDL()
	if m.justSentDDL != nil {
		ddls = append(ddls, m.justSentDDL)
	}

	for _, ddl := range ddls {
		if ddl.CommitTs < barrier.MinTableBarrierTs {
			barrier.MinTableBarrierTs = ddl.CommitTs
		}
		if m.redoMetaManager.Enabled() && isRedoBarrierDDL(ddl) {
			// The pipeline for a new table does not exist until the ddl is successfully
			// executed, so the table's resolvedTs will not be calculated in redo.
			// To solve this problem, resovedTs of redo manager should not be greater
			// than the min commitTs of ddls that create a new physical table.
			if ddl.CommitTs < barrier.RedoBarrierTs {
				barrier.RedoBarrierTs = ddl.CommitTs
			}
		}
		if isGlobalDDL(ddl) {
			// When there is a global DDL, we need to wait all tables
			// checkpointTs reach its commitTs before we can execute it.
			if ddl.CommitTs < barrier.GlobalBarrierTs {
				barrier.GlobalBarrierTs = ddl.CommitTs
			}
		} else {
			// barrier related physical tables
			ids := getRelatedPhysicalTableIDs(ddl)
			for _, id := range ids {
				tableBarrierMap[id] = ddl.CommitTs
			}
		}
	}

	// calculate tableBarriers
	var tableBarriers []*schedulepb.TableBarrier
	for tableID, tableBarrierTs := range tableBarrierMap {
		if tableBarrierTs > barrier.GlobalBarrierTs {
			continue
		}
		tableBarriers = append(tableBarriers, &schedulepb.TableBarrier{
			TableID:   tableID,
			BarrierTs: tableBarrierTs,
		})
	}
	// Limit the tableBarrier size to avoid too large barrier. Since it will
	// cause the scheduler to be slow.
	sort.Slice(tableBarriers, func(i, j int) bool {
		return tableBarriers[i].BarrierTs < tableBarriers[j].BarrierTs
	})
	if len(tableBarriers) > tableBarrierNumberLimit {
		barrier.GlobalBarrierTs = tableBarriers[tableBarrierNumberLimit].BarrierTs
		tableBarriers = tableBarriers[:tableBarrierNumberLimit]
	}

	m.justSentDDL = nil
	barrier.TableBarriers = tableBarriers
	return barrier
}

// allTables returns all tables in the schema that
// less or equal than the checkpointTs.
func (m *ddlManager) allTables(ctx context.Context) ([]*model.TableInfo, error) {
	if m.tableInfoCache != nil {
		return m.tableInfoCache, nil
	}
	var err error

	ts := m.getSnapshotTs()
	m.tableInfoCache, err = m.schema.AllTables(ctx, ts)
	if err != nil {
		return nil, err
	}
	log.Debug("changefeed current tables updated",
		zap.String("namespace", m.changfeedID.Namespace),
		zap.String("changefeed", m.changfeedID.ID),
		zap.Uint64("checkpointTs", m.checkpointTs),
		zap.Uint64("snapshotTs", ts),
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
	m.physicalTablesCache, err = m.schema.AllPhysicalTables(ctx, ts)
	if err != nil {
		return nil, err
	}
	log.Debug("changefeed physical tables updated",
		zap.String("namespace", m.changfeedID.Namespace),
		zap.String("changefeed", m.changfeedID.ID),
		zap.Uint64("checkpointTs", m.checkpointTs),
		zap.Uint64("snapshotTs", ts),
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
func (m *ddlManager) getSnapshotTs() (ts uint64) {
	ts = m.checkpointTs

	if m.ddlResolvedTs == m.startTs {
		// If ddlResolvedTs is equal to startTs it means that the changefeed is just started,
		// So we need to get all tables from the snapshot at the startTs.
		ts = m.startTs
		log.Debug("changefeed is just started, use startTs to get snapshot",
			zap.String("namespace", m.changfeedID.Namespace),
			zap.String("changefeed", m.changfeedID.ID),
			zap.Uint64("startTs", m.startTs),
			zap.Uint64("checkpointTs", m.checkpointTs),
			zap.Uint64("ddlResolvedTs", m.ddlResolvedTs),
		)
		return
	}

	if m.BDRMode {
		ts = m.ddlResolvedTs
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

// getRelatedPhysicalTableIDs get all related physical table ids of a ddl event.
// It is a helper function to calculate tableBarrier.
func getRelatedPhysicalTableIDs(ddl *model.DDLEvent) []model.TableID {
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
	_, ok := nonGlobalDDLs[ddl.Type]
	return !ok
}

func isRedoBarrierDDL(ddl *model.DDLEvent) bool {
	_, ok := redoBarrierDDLs[ddl.Type]
	return ok
}
