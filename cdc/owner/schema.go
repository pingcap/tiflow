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
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	"github.com/pingcap/ticdc/pkg/filter"
	tidbkv "github.com/pingcap/tidb/kv"
	timeta "github.com/pingcap/tidb/meta"
	"go.uber.org/zap"
)

type schemaWrap4Owner struct {
	schemaSnapshot *entry.SingleSchemaSnapshot
	filter         *filter.Filter
	config         *config.ReplicaConfig

	allPhysicalTablesCache []model.TableID
	ddlHandledTs           model.Ts
}

func newSchemaWrap4Owner(kvStorage tidbkv.Storage, startTs model.Ts, config *config.ReplicaConfig) (*schemaWrap4Owner, error) {
	var meta *timeta.Meta
	if kvStorage != nil {
		var err error
		meta, err = kv.GetSnapshotMeta(kvStorage, startTs)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	// We do a snapshot read of the metadata from TiKV at (startTs-1) instead of startTs,
	// because the DDL puller might send a DDL at startTs, which would cause schema conflicts if
	// the DDL's result is already contained in the snapshot.
	schemaSnap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, startTs-1, config.ForceReplicate)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f, err := filter.NewFilter(config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &schemaWrap4Owner{
		schemaSnapshot: schemaSnap,
		filter:         f,
		config:         config,
		ddlHandledTs:   startTs,
	}, nil
}

// AllPhysicalTables returns the table IDs of all tables and partition tables.
func (s *schemaWrap4Owner) AllPhysicalTables() []model.TableID {
	if s.allPhysicalTablesCache != nil {
		return s.allPhysicalTablesCache
	}
	tables := s.schemaSnapshot.Tables()
	s.allPhysicalTablesCache = make([]model.TableID, 0, len(tables))
	for _, tblInfo := range tables {
		if s.shouldIgnoreTable(tblInfo) {
			continue
		}

		if pi := tblInfo.GetPartitionInfo(); pi != nil {
			for _, partition := range pi.Definitions {
				s.allPhysicalTablesCache = append(s.allPhysicalTablesCache, partition.ID)
			}
			continue
		}

		s.allPhysicalTablesCache = append(s.allPhysicalTablesCache, tblInfo.ID)
	}
	return s.allPhysicalTablesCache
}

func (s *schemaWrap4Owner) HandleDDL(job *timodel.Job) error {
	if job.BinlogInfo.FinishedTS <= s.ddlHandledTs {
		return nil
	}
	s.allPhysicalTablesCache = nil
	err := s.schemaSnapshot.HandleDDL(job)
	if err != nil {
		return errors.Trace(err)
	}
	s.ddlHandledTs = job.BinlogInfo.FinishedTS
	return nil
}

func (s *schemaWrap4Owner) IsIneligibleTableID(tableID model.TableID) bool {
	return s.schemaSnapshot.IsIneligibleTableID(tableID)
}

func (s *schemaWrap4Owner) BuildDDLEvent(job *timodel.Job) (*model.DDLEvent, error) {
	ddlEvent := new(model.DDLEvent)
	preTableInfo, err := s.schemaSnapshot.PreTableInfo(job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = s.schemaSnapshot.FillSchemaName(job)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ddlEvent.FromJob(job, preTableInfo)
	return ddlEvent, nil
}

func (s *schemaWrap4Owner) SinkTableInfos() []*model.SimpleTableInfo {
	var sinkTableInfos []*model.SimpleTableInfo
	for tableID := range s.schemaSnapshot.CloneTables() {
		tblInfo, ok := s.schemaSnapshot.TableByID(tableID)
		if !ok {
			log.Panic("table not found for table ID", zap.Int64("tid", tableID))
		}
		if s.shouldIgnoreTable(tblInfo) {
			continue
		}
		dbInfo, ok := s.schemaSnapshot.SchemaByTableID(tableID)
		if !ok {
			log.Panic("schema not found for table ID", zap.Int64("tid", tableID))
		}

		sinkTableInfos = append(sinkTableInfos, newSimpleTableInfo(dbInfo, tableID, tblInfo))
	}
	return sinkTableInfos
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

func newSimpleTableInfo(dbInfo *timodel.DBInfo, tableID model.TableID, tblInfo *model.TableInfo) *model.SimpleTableInfo {
	info := &model.SimpleTableInfo{
		Schema:     dbInfo.Name.O,
		TableID:    tableID,
		Table:      tblInfo.TableName.Table,
		ColumnInfo: make([]*model.ColumnInfo, len(tblInfo.Cols())),
	}

	for i, col := range tblInfo.Cols() {
		info.ColumnInfo[i] = new(model.ColumnInfo)
		info.ColumnInfo[i].FromTiColumnInfo(col)
	}

	return info
}
