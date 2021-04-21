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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	"github.com/pingcap/ticdc/pkg/filter"
	"go.uber.org/zap"
)

type schema4Owner interface {
	AllPhysicalTables() []model.TableID
	HandleDDL(job *timodel.Job) error
	BuildDDLEvent(job *timodel.Job) (*model.DDLEvent, error)
	IsIneligibleTableID(tableID model.TableID) bool
}

type schemaWrap4Owner struct {
	schemaSnapshot *entry.SingleSchemaSnapshot
	filter         *filter.Filter
	config         *config.ReplicaConfig

	allPhysicalTablesCache []model.TableID
}

func newSchemaWrap4Owner(schemaSnapshot *entry.SingleSchemaSnapshot, filter *filter.Filter, config *config.ReplicaConfig) schema4Owner {
	return &schemaWrap4Owner{
		schemaSnapshot: schemaSnapshot,
		filter:         filter,
		config:         config,
	}
}

// AllPhysicalTables returns the table IDs of all tables and partition tables.
func (s *schemaWrap4Owner) AllPhysicalTables() []model.TableID {
	if s.allPhysicalTablesCache != nil {
		return s.allPhysicalTablesCache
	}
	tables := s.schemaSnapshot.CloneTables()
	s.allPhysicalTablesCache = make([]model.TableID, 0, len(tables))
	for tid := range tables {
		tblInfo, exist := s.schemaSnapshot.TableByID(tid)
		if !exist {
			log.Panic("table not found for table ID", zap.Int64("tid", tid))
		}
		if s.shouldIgnoreTable(tblInfo) {
			continue
		}

		if pi := tblInfo.GetPartitionInfo(); pi != nil {
			for _, partition := range pi.Definitions {
				s.allPhysicalTablesCache = append(s.allPhysicalTablesCache, partition.ID)
			}
		} else {
			s.allPhysicalTablesCache = append(s.allPhysicalTablesCache, tblInfo.ID)
		}
	}
	return s.allPhysicalTablesCache
}

func (s *schemaWrap4Owner) HandleDDL(job *timodel.Job) error {
	return s.schemaSnapshot.HandleDDL(job)
}

func (s *schemaWrap4Owner) IsIneligibleTableID(tableID model.TableID) bool {
	return s.schemaSnapshot.IsIneligibleTableID(tableID)
}

func (s *schemaWrap4Owner) shouldIgnoreTable(tableInfo *model.TableInfo) bool {
	schemaName := tableInfo.TableName.Schema
	tableName := tableInfo.TableName.Table
	if s.filter.ShouldIgnoreTable(schemaName, tableName) {
		return true
	}
	if s.config.Cyclic.IsEnabled() && mark.IsMarkTable(schemaName, tableName) {
		// skip the mark table if cyclic is enabled
		return true
	}
	if !tableInfo.IsEligible(s.config.ForceReplicate) {
		log.Warn("skip ineligible table", zap.Int64("tid", tableInfo.ID), zap.Stringer("table", tableInfo.TableName))
		return true
	}
	return false
}

type schemaManager struct {
	schemas        map[model.SchemaID]map[model.TableID]struct{}
	partitions     map[model.TableID][]model.TableID
	schemaSnapshot *entry.SingleSchemaSnapshot

	filter *filter.Filter
	config *config.ReplicaConfig
}

type ddlJobWithPreTableInfo struct {
	*timodel.Job
	preTableInfo *model.TableInfo
}

func newSchemaManager(schemaSnapshot *entry.SingleSchemaSnapshot, filter *filter.Filter, config *config.ReplicaConfig) *schemaManager {
	ret := &schemaManager{
		schemas:        make(map[model.SchemaID]map[model.TableID]struct{}),
		partitions:     make(map[model.TableID][]int64),
		schemaSnapshot: schemaSnapshot,
		filter:         filter,
		config:         config,
	}

	for tid := range schemaSnapshot.CloneTables() {
		tblInfo, exist := schemaSnapshot.TableByID(tid)
		if !exist {
			log.Panic("table not found for table ID", zap.Int64("tid", tid))
		}
		schema, exist := schemaSnapshot.SchemaByTableID(tid)
		if !exist {
			log.Panic("schema not found for table", zap.Int64("tid", tid))
		}
		if ret.shouldIgnoreTable(tblInfo) {
			continue
		}

		schemaID := schema.ID
		if _, ok := ret.schemas[schemaID]; !ok {
			ret.schemas[schemaID] = make(map[model.TableID]struct{})
		}

		ret.schemas[schemaID][tid] = struct{}{}

		if pi := tblInfo.GetPartitionInfo(); pi != nil {
			delete(ret.partitions, tid)
			for _, partition := range pi.Definitions {
				id := partition.ID
				ret.partitions[tid] = append(ret.partitions[tid], id)
			}
		}
	}

	return ret
}

func (m *schemaManager) PreprocessDDL(job *ddlJobWithPreTableInfo) error {
	var err error
	job.preTableInfo, err = m.schemaSnapshot.PreTableInfo(job.Job)
	if err != nil {
		return errors.Trace(err)
	}

	err = m.schemaSnapshot.FillSchemaName(job.Job)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (m *schemaManager) SinkTableInfos() []*model.SimpleTableInfo {
	var sinkTableInfos []*model.SimpleTableInfo

	for _, tableIDs := range m.schemas {
		for tableID := range tableIDs {
			tblInfo, ok := m.schemaSnapshot.TableByID(tableID)
			if !ok {
				log.Panic("table not found for table ID", zap.Int64("tid", tableID))
				continue
			}

			// TODO separate function for initializing SimpleTableInfo
			sinkTableInfo := new(model.SimpleTableInfo)
			sinkTableInfo.TableID = tableID
			sinkTableInfo.ColumnInfo = make([]*model.ColumnInfo, len(tblInfo.Cols()))
			for i, colInfo := range tblInfo.Cols() {
				sinkTableInfo.ColumnInfo[i] = new(model.ColumnInfo)
				sinkTableInfo.ColumnInfo[i].FromTiColumnInfo(colInfo)
			}
			sinkTableInfos = append(sinkTableInfos, sinkTableInfo)
		}
	}

	return sinkTableInfos
}

func (m *schemaManager) shouldIgnoreTable(tableInfo *model.TableInfo) bool {
	schemaName := tableInfo.TableName.Schema
	tableName := tableInfo.TableName.Table
	if m.filter.ShouldIgnoreTable(schemaName, tableName) {
		return true
	}
	if m.config.Cyclic.IsEnabled() && mark.IsMarkTable(schemaName, tableName) {
		// skip the mark table if cyclic is enabled
		return true
	}
	if !tableInfo.IsEligible(m.config.ForceReplicate) {
		log.Warn("skip ineligible table", zap.Int64("tid", tableInfo.ID), zap.Stringer("table", tableInfo.TableName))
		return true
	}
	return false
}

/*
func (m *schemaManager) TopDDLJobToExec() (*timodel.Job, error) {
	for len(m.pendingDDLJob) > 0 {
		job := m.pendingDDLJob[0]
		if !m.shouldIgnoreDDL(job) {
			return job, nil
		}
		err := m.schemaSnapshot.HandleDDL(job)
		if err != nil {
			return nil, errors.Trace(err)
		}
		m.pendingDDLJob = m.pendingDDLJob[1:]
	}
	return nil, nil
}

func (m *schemaManager) MarkDDLExecuted() ([]tableAction, error) {
	// check len > 0
	job := m.pendingDDLJob[0]
	log.Info("apply job", zap.Stringer("job", job),
		zap.String("schema", job.SchemaName),
		zap.String("query", job.Query),
		zap.Uint64("ts", job.BinlogInfo.FinishedTS))

	var (
		startTableIDs  []model.TableID
		removeTableIDs []model.TableID
	)

	err := m.schemaSnapshot.HandleDDL(job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m.pendingDDLJob = m.pendingDDLJob[1:]

	switch job.Type {
	case timodel.ActionCreateSchema:
		m.addSchema(job.SchemaID)
	case timodel.ActionDropSchema:
		removeTableIDs = m.dropSchema(job.SchemaID)
	case timodel.ActionCreateTable, timodel.ActionRecoverTable:
		startTableIDs = m.addTable(job.BinlogInfo.TableInfo.ID)
	case timodel.ActionDropTable:
		removeTableIDs = m.removeTable(job.SchemaID, job.TableID)
	case timodel.ActionRenameTable:
		// TODO do we need this?
	case timodel.ActionTruncateTable:
		removeTableIDs = m.removeTable(job.SchemaID, job.TableID)
		startTableIDs = m.addTable(job.BinlogInfo.TableInfo.ID)
	case timodel.ActionTruncateTablePartition, timodel.ActionAddTablePartition, timodel.ActionDropTablePartition:
		startTableIDs, removeTableIDs = m.updatePartitions(job.TableID)
	default:
		log.Debug("ignore the job type which is no table action", zap.Stringer("job", job))
	}

	var tableActions []tableAction

	for _, dropTableID := range removeTableIDs {
		tableActions = append(tableActions, tableAction{
			Action:  DropTableAction,
			tableID: dropTableID,
		})
	}

	for _, addTableID := range startTableIDs {
		tableActions = append(tableActions, tableAction{
			Action:  AddTableAction,
			tableID: addTableID,
		})
	}

	return tableActions, nil
}

func (m *schemaManager) BuildDDLEvent(job *timodel.Job) (*model.DDLEvent, error) {
	ddlEvent := new(model.DDLEvent)
	preTableInfo, err := m.schemaSnapshot.PreTableInfo(job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = m.schemaSnapshot.FillSchemaName(job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddlEvent.FromJob(job, preTableInfo)
	return ddlEvent, nil
}


func (m *schemaManager) shouldIgnoreDDL(job *timodel.Job) bool {
	if m.filter.ShouldDiscardDDL(job.Type) {
		return true
	}
	if job.BinlogInfo == nil || job.BinlogInfo.TableInfo == nil {
		return true
	}

	postTableInfo := model.WrapTableInfo(job.SchemaID, job.SchemaName,
		job.BinlogInfo.FinishedTS,
		job.BinlogInfo.TableInfo)

	if job.Type == timodel.ActionRenameTable {
		tableInfo, ok := m.schemaSnapshot.TableByID(job.TableID)
		if !ok {
			log.Panic("preFilterDDL: tableID not found", zap.Stringer("job", job))
		}

		if m.shouldIgnoreTable(tableInfo) && m.shouldIgnoreTable(postTableInfo) {
			return true
		}
	} else {
		if m.shouldIgnoreTable(postTableInfo) {
			return true
		}
	}
	return false
}

func (m *schemaManager) addSchema(schemaID model.SchemaID) {
	if _, ok := m.schemas[schemaID]; ok {
		log.Warn("schema already exists", zap.Int("schemaID", int(schemaID)))
		return
	}

	m.schemas[schemaID] = make(map[model.TableID]struct{})
}

func (m *schemaManager) dropSchema(schemaID model.SchemaID) (removeTableIDs []model.TableID) {
	if _, ok := m.schemas[schemaID]; !ok {
		log.Warn("schema does not exist", zap.Int("schemaID", int(schemaID)))
		return
	}

	for tid := range m.schemas[schemaID] {
		removeIDs := m.removeTable(schemaID, tid)
		removeTableIDs = append(removeTableIDs, removeIDs...)
	}

	delete(m.schemas, schemaID)
	return removeTableIDs
}

func (m *schemaManager) addTable(tableID model.TableID) (startTableIDs []model.TableID) {
	tableInfo, ok := m.schemaSnapshot.TableByID(tableID)
	if !ok {
		log.Panic("table not found", zap.Int("tableID", int(tableID)))
	}

	schemaID := tableInfo.SchemaID
	if _, ok := m.schemas[schemaID]; !ok {
		m.addSchema(schemaID)
	}

	m.schemas[schemaID][tableID] = struct{}{}

	if partitionInfo := tableInfo.GetPartitionInfo(); partitionInfo != nil {
		if _, ok := m.partitions[tableID]; ok {
			log.Panic("partition list already exists", zap.Int("tableID", int(tableID)))
		}

		for _, partitionDefinition := range partitionInfo.Definitions {
			m.partitions[tableID] = append(m.partitions[tableID], partitionDefinition.ID)
			startTableIDs = append(startTableIDs, partitionDefinition.ID)
		}
	} else {
		startTableIDs = append(startTableIDs, tableID)
	}

	return
}

func (m *schemaManager) removeTable(schemaID model.SchemaID, tableID model.TableID) (removeTableIDs []model.TableID) {
	if _, ok := m.schemas[schemaID]; !ok {
		log.Panic("schema does not exist", zap.Int("schemaID", int(schemaID)))
	}

	delete(m.schemas[schemaID], schemaID)

	if partitions, ok := m.partitions[tableID]; ok {
		removeTableIDs = partitions
		delete(m.partitions, tableID)
	} else {
		removeTableIDs = append(removeTableIDs, tableID)
	}

	return
}

func (m *schemaManager) updatePartitions(tableID model.TableID) (startTableIDs []model.TableID, removeTableIDs []model.TableID) {
	oldPartitions, ok := m.partitions[tableID]
	if !ok || len(oldPartitions) == 0 {
		return
	}

	oldPartitionSet := make(map[model.TableID]struct{}, len(oldPartitions))
	for _, partitionID := range oldPartitions {
		oldPartitionSet[partitionID] = struct{}{}
	}

	tableInfo, ok := m.schemaSnapshot.TableByID(tableID)
	if !ok {
		log.Panic("table not found", zap.Int("tableID", int(tableID)))
	}

	var newPartitions []model.TableID
	partitionInfo := tableInfo.GetPartitionInfo()
	if partitionInfo == nil {
		log.Panic("no partition info", zap.Int("tableID", int(tableID)))
		panic("unreachable")
	}

	for _, partitionDefinition := range partitionInfo.Definitions {
		partitionID := partitionDefinition.ID
		newPartitions = append(newPartitions, partitionID)
		if _, ok := oldPartitionSet[partitionID]; !ok {
			startTableIDs = append(startTableIDs, partitionID)
		} else {
			delete(oldPartitionSet, partitionID)
		}
	}

	m.partitions[tableID] = newPartitions

	// The partitionIDs present in newPartitions have been removed from oldPartitionSet
	for tableID := range oldPartitionSet {
		removeTableIDs = append(removeTableIDs, tableID)
	}

	return
}
*/
