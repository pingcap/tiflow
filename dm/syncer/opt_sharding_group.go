// Copyright 2022 PingCAP, Inc.
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
	"sync"

	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// OptShardingGroup represents a optimistic sharding DDL sync group.
type OptShardingGroup struct {
	sync.RWMutex

	// the conflict tableIDs hash set to quickly check whether this table is in conflict stage
	// sourceTableID -> each table's conflicted ddl's startLocation
	conflictTables map[string]binlog.Location

	firstConflictLocation binlog.Location
	flavor                string
	enableGTID            bool
}

func NewOptShardingGroup(firstConflictLocation binlog.Location, flavor string, enableGTID bool) *OptShardingGroup {
	return &OptShardingGroup{
		firstConflictLocation: firstConflictLocation,
		flavor:                flavor,
		enableGTID:            enableGTID,
		conflictTables:        make(map[string]binlog.Location, 1),
	}
}

func (s *OptShardingGroup) appendConflictTable(table *filter.Table, location binlog.Location) {
	s.Lock()
	defer s.Unlock()
	s.conflictTables[table.String()] = location
}

func (s *OptShardingGroup) inConflictStage(table *filter.Table) bool {
	s.RLock()
	defer s.RUnlock()
	_, ok := s.conflictTables[utils.GenTableID(table)]
	return ok
}

func (s *OptShardingGroup) Remove(sourceTableIDs []string) {
	s.Lock()
	defer s.Unlock()
	for _, sourceTbl := range sourceTableIDs {
		delete(s.conflictTables, sourceTbl)
	}
}

// OptShardingGroupKeeper used to keep OptShardingGroup.
// It's used to keep sharding group meta data to make sure optimistic sharding resync redirection works correctly.
//
//	                                                 newer
//	│                       ───────────────────────► time
//	│
//	│ tb1 conflict DDL1     │  ▲      │
//	│                       │  │      │
//	│       ...             │  │      │
//	│                       │  │      │
//	│ tb1 conflict DDL2     │  │      │  ▲     │
//	│                       │  │      │  │     │
//	│       ...             │  │      │  │     │
//	│                       │  │      │  │     │
//	│ tb2 conflict DDL1     ▼  │      │  │     │
//	│                                 │  │     │
//	│       ...           redirect    │  │     │
//	│                                 │  │     │
//	│ tb2 conflict DDL2               ▼  │     │
//	│                                          │
//	│       ...                     redirect   │
//	│                                          │
//	│  other dml events                        ▼
//	│
//	│                                       continue
//	▼                                      replicating
//
// newer
// binlog
// One redirection example is listed as above.
type OptShardingGroupKeeper struct {
	sync.RWMutex
	groups map[string]*OptShardingGroup // target table ID -> ShardingGroup
	cfg    *config.SubTaskConfig
	tctx   *tcontext.Context
	// shardingReSyncs is used to save the shardingResyncs' redirect locations that are resolved but not finished
	shardingReSyncs map[string]binlog.Location
}

// NewOptShardingGroupKeeper creates a new OptShardingGroupKeeper.
func NewOptShardingGroupKeeper(tctx *tcontext.Context, cfg *config.SubTaskConfig) *OptShardingGroupKeeper {
	return &OptShardingGroupKeeper{
		groups:          make(map[string]*OptShardingGroup),
		cfg:             cfg,
		tctx:            tctx.WithLogger(tctx.L().WithFields(zap.String("component", "optimistic shard group keeper"))),
		shardingReSyncs: make(map[string]binlog.Location),
	}
}

func (k *OptShardingGroupKeeper) resolveGroup(targetTable *filter.Table) (map[string]binlog.Location, binlog.Location) {
	targetTableID := utils.GenTableID(targetTable)
	k.Lock()
	defer k.Unlock()
	group, ok := k.groups[targetTableID]
	if !ok {
		return nil, binlog.Location{}
	}
	delete(k.groups, targetTableID)
	return group.conflictTables, group.firstConflictLocation
}

func (k *OptShardingGroupKeeper) inConflictStage(sourceTable, targetTable *filter.Table) bool {
	targetTableID := utils.GenTableID(targetTable)
	k.RLock()
	group, ok := k.groups[targetTableID]
	k.RUnlock()
	if !ok {
		return false
	}

	return group.inConflictStage(sourceTable)
}

func (k *OptShardingGroupKeeper) tableInConflict(targetTable *filter.Table) bool {
	targetTableID := utils.GenTableID(targetTable)
	k.RLock()
	defer k.RUnlock()
	_, ok := k.groups[targetTableID]
	return ok
}

// appendConflictTable returns whether sourceTable is the first conflict table for targetTable.
func (k *OptShardingGroupKeeper) appendConflictTable(sourceTable, targetTable *filter.Table,
	conflictLocation binlog.Location, flavor string, enableGTID bool,
) bool {
	targetTableID := utils.GenTableID(targetTable)
	k.Lock()
	group, ok := k.groups[targetTableID]
	if !ok {
		group = NewOptShardingGroup(conflictLocation, flavor, enableGTID)
		k.groups[targetTableID] = group
	}
	k.Unlock()
	group.appendConflictTable(sourceTable, conflictLocation)
	return !ok
}

func (k *OptShardingGroupKeeper) addShardingReSync(shardingReSync *ShardingReSync) {
	if shardingReSync != nil {
		k.shardingReSyncs[shardingReSync.targetTable.String()] = shardingReSync.currLocation
	}
}

func (k *OptShardingGroupKeeper) removeShardingReSync(shardingReSync *ShardingReSync) {
	if shardingReSync != nil {
		delete(k.shardingReSyncs, shardingReSync.targetTable.String())
	}
}

func (k *OptShardingGroupKeeper) getShardingResyncs() map[string]binlog.Location {
	return k.shardingReSyncs
}

func (k *OptShardingGroupKeeper) lowestFirstLocationInGroups() *binlog.Location {
	k.RLock()
	defer k.RUnlock()
	var lowest *binlog.Location
	for _, group := range k.groups {
		if lowest == nil || binlog.CompareLocation(*lowest, group.firstConflictLocation, k.cfg.EnableGTID) > 0 {
			lowest = &group.firstConflictLocation
		}
	}
	for _, currLocation := range k.shardingReSyncs {
		if lowest == nil || binlog.CompareLocation(*lowest, currLocation, k.cfg.EnableGTID) > 0 {
			loc := currLocation // make sure lowest can point to correct variable
			lowest = &loc
		}
	}
	if lowest == nil {
		return nil
	}
	loc := lowest.Clone()
	return &loc
}

// AdjustGlobalLocation adjusts globalLocation with sharding groups' lowest first point.
func (k *OptShardingGroupKeeper) AdjustGlobalLocation(globalLocation binlog.Location) binlog.Location {
	lowestFirstLocation := k.lowestFirstLocationInGroups()
	if lowestFirstLocation != nil && binlog.CompareLocation(*lowestFirstLocation, globalLocation, k.cfg.EnableGTID) < 0 {
		return *lowestFirstLocation
	}
	return globalLocation
}

func (k *OptShardingGroupKeeper) RemoveGroup(targetTable *filter.Table, sourceTableIDs []string) {
	targetTableID := utils.GenTableID(targetTable)

	k.Lock()
	defer k.Unlock()
	if group, ok := k.groups[targetTableID]; ok {
		group.Remove(sourceTableIDs)
		if len(group.conflictTables) == 0 {
			delete(k.groups, targetTableID)
		}
	}
}

func (k *OptShardingGroupKeeper) RemoveSchema(schema string) {
	k.Lock()
	defer k.Unlock()
	for targetTableID := range k.groups {
		if targetTable := utils.UnpackTableID(targetTableID); targetTable.Schema == schema {
			delete(k.groups, targetTableID)
		}
	}
}

// Reset resets the keeper.
func (k *OptShardingGroupKeeper) Reset() {
	k.Lock()
	defer k.Unlock()
	k.groups = make(map[string]*OptShardingGroup)
	k.shardingReSyncs = make(map[string]binlog.Location)
}
