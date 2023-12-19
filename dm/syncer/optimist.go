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

package syncer

import (
	"context"

	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// initOptimisticShardDDL initializes the shard DDL support in the optimistic mode.
func (s *Syncer) initOptimisticShardDDL(ctx context.Context) error {
	// fetch tables from source and filter them
	sourceTables, err := s.fromDB.FetchAllDoTables(ctx, s.baList)
	if err != nil {
		return err
	}

	// convert according to router rules.
	// downstream-schema -> downstream-table -> upstream-schema -> upstream-table.
	// TODO: refine to downstream-ID -> upstream-ID
	mapper := make(map[string]map[string]map[string]map[string]struct{})
	for upSchema, UpTables := range sourceTables {
		for _, upTable := range UpTables {
			up := &filter.Table{Schema: upSchema, Name: upTable}
			down := s.route(up)
			downSchema, downTable := down.Schema, down.Name
			if _, ok := mapper[downSchema]; !ok {
				mapper[downSchema] = make(map[string]map[string]map[string]struct{})
			}
			if _, ok := mapper[downSchema][downTable]; !ok {
				mapper[downSchema][downTable] = make(map[string]map[string]struct{})
			}
			if _, ok := mapper[downSchema][downTable][upSchema]; !ok {
				mapper[downSchema][downTable][upSchema] = make(map[string]struct{})
			}
			mapper[downSchema][downTable][upSchema][upTable] = struct{}{}
		}
	}

	return s.optimist.Init(mapper)
}

func (s *Syncer) resolveOptimisticDDL(ec *eventContext, sourceTable, targetTable *filter.Table) bool {
	if sourceTable != nil && targetTable != nil {
		if s.osgk.inConflictStage(sourceTable, targetTable) {
			// in the following two situations we should resolve this ddl lock at now
			// 1. after this worker's ddl, the ddl lock is resolved
			// 2. other worker has resolved this ddl lock, receives resolve command from master
			// TODO: maybe we don't need to resolve ddl lock in situation 1, because when situation 1 happens we
			// 	should always receive a resolve operation like situation 2.
			group, redirectLocation := s.osgk.resolveGroup(targetTable)
			if len(group) > 0 {
				s.optimist.DoneRedirectOperation(utils.GenTableID(targetTable))
				resync := &ShardingReSync{
					currLocation:   redirectLocation,
					latestLocation: ec.endLocation,
					targetTable:    targetTable,
					allResolved:    true,
				}
				s.osgk.tctx.L().Info("sending resync operation in optimistic shard mode",
					zap.Stringer("shardingResync", resync))
				*ec.shardingReSyncCh <- resync
				s.osgk.addShardingReSync(resync)
				return true
			}
		}
	} else {
		s.osgk.tctx.L().Warn("invalid resolveOptimistic deploy without sourceTable/targetTable in optimistic shard mode",
			zap.Bool("emptySourceTable", sourceTable == nil),
			zap.Bool("emptyTargetTable", targetTable == nil))
	}
	return false
}
