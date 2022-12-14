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
	tidbkv "github.com/pingcap/tidb/kv"
	timeta "github.com/pingcap/tidb/meta"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	"github.com/pingcap/tiflow/pkg/filter"
	"go.uber.org/zap"
)

type schemaWrap4Owner struct {
	schemaSnapshot *schema.Snapshot
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
	schemaSnap, err := schema.NewSingleSnapshotFromMeta(meta, startTs, config.ForceReplicate)
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
	// NOTE: it's better to pre-allocate the vector. However in the current implementation
	// we can't know how many valid tables in the snapshot.
	s.allPhysicalTablesCache = make([]model.TableID, 0)
	s.schemaSnapshot.IterTables(true, func(tblInfo *model.TableInfo) {
		if s.shouldIgnoreTable(tblInfo) {
			return
		}
		if pi := tblInfo.GetPartitionInfo(); pi != nil {
			for _, partition := range pi.Definitions {
				s.allPhysicalTablesCache = append(s.allPhysicalTablesCache, partition.ID)
			}
		} else {
			s.allPhysicalTablesCache = append(s.allPhysicalTablesCache, tblInfo.ID)
		}
	})
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
	var schemaIDs []int64
	s.schemaSnapshot.IterTables(true, func(tblInfo *model.TableInfo) {
		if s.shouldIgnoreTable(tblInfo) {
			return
		}
		sinkTableInfo := new(model.SimpleTableInfo)
		sinkTableInfo.TableID = tblInfo.ID
		sinkTableInfo.Table = tblInfo.TableName.Table
		sinkTableInfo.ColumnInfo = make([]*model.ColumnInfo, len(tblInfo.Cols()))
		for i, colInfo := range tblInfo.Cols() {
			sinkTableInfo.ColumnInfo[i] = new(model.ColumnInfo)
			sinkTableInfo.ColumnInfo[i].FromTiColumnInfo(colInfo)
		}
		sinkTableInfos = append(sinkTableInfos, sinkTableInfo)
		schemaIDs = append(schemaIDs, tblInfo.SchemaID)
	})

	schemaNames := make(map[int64]string)
	for i, schemaID := range schemaIDs {
		if name, exists := schemaNames[schemaID]; exists {
			sinkTableInfos[i].Schema = name
		} else {
			schema, exists := s.schemaSnapshot.SchemaByID(schemaID)
			if !exists {
				log.Panic("schema not found", zap.Int64("schemaID", schemaID))
			}
			schemaNames[schemaID] = schema.Name.O
			sinkTableInfos[i].Schema = schema.Name.O
		}
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
