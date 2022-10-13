// Copyright 2019 PingCAP, Inc.
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

package shardmeta

import (
	"encoding/json"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/pingcap/tiflow/dm/pkg/gtid"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// DDLItem records ddl information used in sharding sequence organization.
type DDLItem struct {
	FirstLocation binlog.Location `json:"-"`      // first DDL's binlog Pos, not the End_log_pos of the event
	DDLs          []string        `json:"ddls"`   // DDLs, these ddls are in the same QueryEvent
	Source        string          `json:"source"` // source table ID

	// just used for json's marshal and unmarshal, because gtid set in FirstLocation is interface,
	// can't be marshal and unmarshal
	FirstPosition mysql.Position `json:"first-position"`
	FirstGTIDSet  string         `json:"first-gtid-set"`
}

// NewDDLItem creates a new DDLItem.
func NewDDLItem(location binlog.Location, ddls []string, source string) *DDLItem {
	gsetStr := ""
	if location.GetGTID() != nil {
		gsetStr = location.GetGTID().String()
	}

	return &DDLItem{
		FirstLocation: location,
		DDLs:          ddls,
		Source:        source,

		FirstPosition: location.Position,
		FirstGTIDSet:  gsetStr,
	}
}

// String returns the item's format string value.
func (item *DDLItem) String() string {
	return fmt.Sprintf("first-location: %s ddls: %+v source: %s", item.FirstLocation, item.DDLs, item.Source)
}

// ShardingSequence records a list of DDLItem.
type ShardingSequence struct {
	Items []*DDLItem `json:"items"`
}

// IsPrefixSequence checks whether a ShardingSequence is the prefix sequence of other.
func (seq *ShardingSequence) IsPrefixSequence(other *ShardingSequence) bool {
	if len(seq.Items) > len(other.Items) {
		return false
	}
	for idx := range seq.Items {
		if !utils.CompareShardingDDLs(seq.Items[idx].DDLs, other.Items[idx].DDLs) {
			return false
		}
	}
	return true
}

// String returns the ShardingSequence's json string.
func (seq *ShardingSequence) String() string {
	jsonSeq, err := json.Marshal(seq.Items)
	if err != nil {
		log.L().DPanic("fail to marshal ShardingSequence to json", zap.Reflect("shard sequence", seq))
	}
	return string(jsonSeq)
}

// ShardingMeta stores sharding ddl sequence
// including global sequence and each source's own sequence
// NOTE: sharding meta is not thread safe, it must be used in thread safe context.
type ShardingMeta struct {
	activeIdx int                          // the first unsynced DDL index
	global    *ShardingSequence            // merged sharding sequence of all source tables
	sources   map[string]*ShardingSequence // source table ID -> its sharding sequence
	tableName string                       // table name (with schema) used in downstream meta db

	enableGTID bool // whether enableGTID, used to compare location
}

// NewShardingMeta creates a new ShardingMeta.
func NewShardingMeta(schema, table string, enableGTID bool) *ShardingMeta {
	return &ShardingMeta{
		tableName: dbutil.TableName(schema, table),
		global:    &ShardingSequence{Items: make([]*DDLItem, 0)},
		sources:   make(map[string]*ShardingSequence),

		enableGTID: enableGTID,
	}
}

// RestoreFromData restores ShardingMeta from given data.
func (meta *ShardingMeta) RestoreFromData(sourceTableID string, activeIdx int, isGlobal bool, data []byte, flavor string) error {
	items := make([]*DDLItem, 0)
	err := json.Unmarshal(data, &items)
	if err != nil {
		return terror.ErrSyncUnitInvalidShardMeta.Delegate(err)
	}

	// set FirstLocation
	for _, item := range items {
		gset, err1 := gtid.ParserGTID(flavor, item.FirstGTIDSet)
		if err1 != nil {
			return err1
		}
		item.FirstLocation = binlog.NewLocation(
			item.FirstPosition,
			gset,
		)
	}

	if isGlobal {
		meta.global = &ShardingSequence{Items: items}
	} else {
		meta.sources[sourceTableID] = &ShardingSequence{Items: items}
	}
	meta.activeIdx = activeIdx
	return nil
}

// ActiveIdx returns the activeIdx of sharding meta.
func (meta *ShardingMeta) ActiveIdx() int {
	return meta.activeIdx
}

// Reinitialize reinitialize the shardingmeta.
func (meta *ShardingMeta) Reinitialize() {
	meta.activeIdx = 0
	meta.global = &ShardingSequence{make([]*DDLItem, 0)}
	meta.sources = make(map[string]*ShardingSequence)
}

// checkItemExists checks whether DDLItem exists in its source sequence
// if exists, return the index of DDLItem in source sequence.
// if not exists, return the next index in source sequence.
func (meta *ShardingMeta) checkItemExists(item *DDLItem) (int, bool) {
	source, ok := meta.sources[item.Source]
	if !ok {
		return 0, false
	}
	for idx, ddlItem := range source.Items {
		if binlog.CompareLocation(item.FirstLocation, ddlItem.FirstLocation, meta.enableGTID) == 0 {
			return idx, true
		}
	}
	return len(source.Items), false
}

// AddItem adds a new coming DDLItem into ShardingMeta
// 1. if DDLItem already exists in source sequence, check whether it is active DDL only
// 2. add the DDLItem into its related source sequence
// 3. if it is a new DDL in global sequence, which means len(source.Items) > len(global.Items), add it into global sequence
// 4. check the source sequence is the prefix-sequence of global sequence, if not, return an error
// returns:
//
//	active: whether the DDL will be processed in this round
func (meta *ShardingMeta) AddItem(item *DDLItem) (active bool, err error) {
	index, exists := meta.checkItemExists(item)
	if exists {
		return index == meta.activeIdx, nil
	}

	if source, ok := meta.sources[item.Source]; !ok {
		meta.sources[item.Source] = &ShardingSequence{Items: []*DDLItem{item}}
	} else {
		source.Items = append(source.Items, item)
	}

	global, source := meta.global, meta.sources[item.Source]
	if len(source.Items) > len(global.Items) {
		global.Items = append(global.Items, item)
	}

	if !source.IsPrefixSequence(global) {
		return false, terror.ErrSyncUnitDDLWrongSequence.Generate(source.Items, global.Items)
	}

	return index == meta.activeIdx, nil
}

// GetGlobalActiveDDL returns activeDDL in global sequence.
func (meta *ShardingMeta) GetGlobalActiveDDL() *DDLItem {
	if meta.activeIdx < len(meta.global.Items) {
		return meta.global.Items[meta.activeIdx]
	}
	return nil
}

// GetGlobalItems returns global DDLItems.
func (meta *ShardingMeta) GetGlobalItems() []*DDLItem {
	return meta.global.Items
}

// GetActiveDDLItem returns the source table's active DDLItem
// if in DDL unsynced procedure, the active DDLItem means the syncing DDL
// if in re-sync procedure, the active DDLItem means the next syncing DDL in DDL syncing sequence, may be nil.
func (meta *ShardingMeta) GetActiveDDLItem(tableSource string) *DDLItem {
	source, ok := meta.sources[tableSource]
	if !ok {
		return nil
	}
	if meta.activeIdx < len(source.Items) {
		return source.Items[meta.activeIdx]
	}
	return nil
}

// InSequenceSharding returns whether in sequence sharding.
func (meta *ShardingMeta) InSequenceSharding() bool {
	globalItemCount := len(meta.global.Items)
	return globalItemCount > 0 && meta.activeIdx < globalItemCount
}

// ResolveShardingDDL resolves one sharding DDL and increase activeIdx
// if activeIdx equals to the length of global sharding sequence, it means all
// sharding DDL in this ShardingMeta sequence is resolved and will reinitialize
// the ShardingMeta, return true if all DDLs are resolved.
func (meta *ShardingMeta) ResolveShardingDDL() bool {
	meta.activeIdx++
	if meta.activeIdx == len(meta.global.Items) {
		meta.Reinitialize()
		return true
	}
	return false
}

// ActiveDDLFirstLocation returns the first binlog position of active DDL.
func (meta *ShardingMeta) ActiveDDLFirstLocation() (binlog.Location, error) {
	if meta.activeIdx >= len(meta.global.Items) {
		return binlog.Location{}, terror.ErrSyncUnitDDLActiveIndexLarger.Generate(meta.activeIdx, meta.global.Items)
	}

	return meta.global.Items[meta.activeIdx].FirstLocation, nil
}

// FlushData returns sharding meta flush SQL and args.
func (meta *ShardingMeta) FlushData(sourceID, tableID string) ([]string, [][]interface{}) {
	if len(meta.global.Items) == 0 {
		sql2 := fmt.Sprintf("DELETE FROM %s where source_id=? and target_table_id=?", meta.tableName)
		args2 := []interface{}{sourceID, tableID}
		return []string{sql2}, [][]interface{}{args2}
	}
	var (
		sqls    = make([]string, 1+len(meta.sources))
		args    = make([][]interface{}, 0, 1+len(meta.sources))
		baseSQL = fmt.Sprintf("INSERT INTO %s (`source_id`, `target_table_id`, `source_table_id`, `active_index`, `is_global`, `data`) VALUES(?,?,?,?,?,?) ON DUPLICATE KEY UPDATE `data`=?, `active_index`=?", meta.tableName)
	)
	for i := range sqls {
		sqls[i] = baseSQL
	}
	args = append(args, []interface{}{sourceID, tableID, "", meta.activeIdx, true, meta.global.String(), meta.global.String(), meta.activeIdx})
	for source, seq := range meta.sources {
		args = append(args, []interface{}{sourceID, tableID, source, meta.activeIdx, false, seq.String(), seq.String(), meta.activeIdx})
	}
	return sqls, args
}

func (meta *ShardingMeta) genRemoveSQL(sourceID, tableID string) (string, []interface{}) {
	sql := fmt.Sprintf("DELETE FROM %s where source_id=? and target_table_id=?", meta.tableName)
	return sql, []interface{}{sourceID, tableID}
}

// CheckAndUpdate check and fix schema and table names for all the sharding groups.
func (meta *ShardingMeta) CheckAndUpdate(logger log.Logger, targetID string, schemaMap map[string]string, tablesMap map[string]map[string]string) ([]string, [][]interface{}, error) {
	if len(schemaMap) == 0 && len(tablesMap) == 0 {
		return nil, nil, nil
	}

	checkSourceID := func(source string) (string, bool) {
		sourceTable := utils.UnpackTableID(source)
		schemaName, tblName := sourceTable.Schema, sourceTable.Name
		realSchema, changed := schemaMap[schemaName]
		if !changed {
			realSchema = schemaName
		}
		tblMap := tablesMap[schemaName]
		realTable, ok := tblMap[tblName]
		if ok {
			changed = true
		} else {
			realTable = tblName
		}
		newTableID := utils.GenTableID(&filter.Table{Schema: realSchema, Name: realTable})
		return newTableID, changed
	}

	for _, item := range meta.global.Items {
		newID, changed := checkSourceID(item.Source)
		if changed {
			item.Source = newID
		}
	}

	sourceIDsMap := make(map[string]string)
	for sourceID, seqs := range meta.sources {
		newSourceID, changed := checkSourceID(sourceID)
		for _, item := range seqs.Items {
			newID, hasChanged := checkSourceID(item.Source)
			if hasChanged {
				item.Source = newID
				changed = true
			}
		}
		if changed {
			sourceIDsMap[sourceID] = newSourceID
		}
	}
	var (
		sqls []string
		args [][]interface{}
	)
	for oldID, newID := range sourceIDsMap {
		if oldID != newID {
			seqs := meta.sources[oldID]
			delete(meta.sources, oldID)
			meta.sources[newID] = seqs
			removeSQL, arg := meta.genRemoveSQL(oldID, targetID)
			sqls = append(sqls, removeSQL)
			args = append(args, arg)
		}
		logger.Info("fix sharding meta", zap.String("old", oldID), zap.String("new", newID))
		fixedSQLs, fixedArgs := meta.FlushData(newID, targetID)
		sqls = append(sqls, fixedSQLs...)
		args = append(args, fixedArgs...)
	}

	return sqls, args, nil
}
