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
	tidbkv "github.com/pingcap/tidb/kv"
	timeta "github.com/pingcap/tidb/meta"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type schemaWrap4Owner struct {
	schemaSnapshot *schema.Snapshot
	filter         *filter.Filter
	config         *config.ReplicaConfig

	allPhysicalTablesCache []model.TableID
	ddlHandledTs           model.Ts

	id model.ChangeFeedID
}

func newSchemaWrap4Owner(
	kvStorage tidbkv.Storage, startTs model.Ts,
	config *config.ReplicaConfig, id model.ChangeFeedID,
) (*schemaWrap4Owner, error) {
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
		id:             id,
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

// AllTableNames returns the table names of all tables that are being replicated.
func (s *schemaWrap4Owner) AllTableNames() []model.TableName {
	names := make([]model.TableName, 0, len(s.allPhysicalTablesCache))
	s.schemaSnapshot.IterTables(true, func(tblInfo *model.TableInfo) {
		if !s.shouldIgnoreTable(tblInfo) {
			names = append(names, tblInfo.TableName)
		}
	})
	return names
}

func (s *schemaWrap4Owner) HandleDDL(job *timodel.Job) error {
	if job.BinlogInfo.FinishedTS <= s.ddlHandledTs {
		log.Warn("job finishTs is less than schema handleTs, discard invalid job",
			zap.String("changefeed", s.id), zap.Stringer("job", job),
			zap.Any("ddlHandledTs", s.ddlHandledTs))
		return nil
	}
	s.allPhysicalTablesCache = nil
	err := s.schemaSnapshot.HandleDDL(job)
	if err != nil {
		log.Error("handle DDL failed", zap.String("changefeed", s.id),
			zap.String("DDL", job.Query),
			zap.Stringer("job", job), zap.Error(err),
			zap.Any("role", util.RoleOwner))
		return errors.Trace(err)
	}
	log.Info("handle DDL", zap.String("changefeed", s.id),
		zap.String("DDL", job.Query), zap.Stringer("job", job),
		zap.Any("role", util.RoleOwner))

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
		log.Error("build DDL event fail", zap.Reflect("job", job), zap.Error(err))
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
			log.Warn("skip ineligible table", zap.Int64("tableID", t.ID),
				zap.Stringer("tableName", t.TableName), zap.String("changefeed", s.id))
		}
		return true
	}
	return false
}
