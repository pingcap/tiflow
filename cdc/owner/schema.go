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
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	"github.com/pingcap/tiflow/pkg/filter"
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
	schemaSnap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, startTs, config.ForceReplicate)
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
		} else {
			s.allPhysicalTablesCache = append(s.allPhysicalTablesCache, tblInfo.ID)
		}
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

func (s *schemaWrap4Owner) shouldIgnoreTable(t *model.TableInfo) bool {
	schemaName := t.TableName.Schema
	tableName := t.TableName.Table
	if s.filter.ShouldIgnoreTable(schemaName, tableName) {
		return true
	}
	if s.config.Cyclic.IsEnabled() && mark.IsMarkTable(schemaName, tableName) {
		// skip the mark table if cyclic is enabled
		return true
	}
	if !t.IsEligible(s.config.ForceReplicate) {
		// Sequence is not supported yet, and always ineligible.
		// Skip Warn to avoid confusion.
		// See https://github.com/pingcap/tiflow/issues/4559
		if !t.IsSequence() {
			log.Warn("skip ineligible table",
				zap.Int64("tableID", t.ID), zap.Stringer("tableName", t.TableName))
		}
		return true
	}
	return false
}
