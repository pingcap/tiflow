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
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type schemaWrap4Owner struct {
	entry.SchemaStorage
	filter         filter.Filter
	forceReplicate bool
	id             model.ChangeFeedID
}

func newSchemaWrap4Owner(
	kvStorage tidbkv.Storage, startTs model.Ts,
	forceReplicate bool, id model.ChangeFeedID,
	filter filter.Filter,
) (*schemaWrap4Owner, error) {
	schemaStorage, err := entry.NewSchemaStorage(
		kvStorage, startTs, forceReplicate, id, util.RoleOwner, filter)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// It is no matter to use an empty as timezone here because schemaWrap4Owner
	// doesn't use expression filter's method.
	schema := &schemaWrap4Owner{
		SchemaStorage: schemaStorage,
		filter:        filter,
		id:            id,
	}
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

func (s *schemaWrap4Owner) shouldIgnoreTable(t *model.TableInfo) bool {
	schemaName := t.TableName.Schema
	tableName := t.TableName.Table
	if s.filter.ShouldIgnoreTable(schemaName, tableName) {
		return true
	}
	if !t.IsEligible(s.forceReplicate) {
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
