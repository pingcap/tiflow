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

package optimism

import (
	"context"
	"encoding/json"
	"fmt"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/tiflow/dm/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
)

// SourceTables represents the upstream/sources tables for a data migration **subtask**.
// This information should be persistent in etcd so can be retrieved after the DM-master leader restarted or changed.
// We need this because only one shard group exists for **every** target table in the optimistic mode (in DM-master),
// so we need DM-worker to report its upstream table names to DM-master.
// NOTE: `Task` and `Source` are redundant in the etcd key path for convenient.
// SourceTables is putted when starting the subtask by DM-worker,
// and is updated when new tables added/removed in the upstream source by DM-worker,
// and **may be** deleted when stopping the subtask by DM-worker later.
type SourceTables struct {
	Task   string `json:"task"`   // data migration task name
	Source string `json:"source"` // upstream source ID

	// downstream-schema-name -> downstream-table-name -> upstream-schema-name -> upstream-table-name -> struct{},
	// multiple downstream/target tables (<downstream-schema-name, downstream-table-name> pair) may exist in one subtask.
	Tables map[string]map[string]map[string]map[string]struct{} `json:"tables"`

	// only used to report to the caller of the watcher, do not marsh it.
	// if it's true, it means the SourceTables has been deleted in etcd.
	IsDeleted bool `json:"-"`
}

// TargetTable represents some upstream/sources tables for **one** target table.
// It is often generated from `SourceTables` for the specified downstream table.
type TargetTable struct {
	Task       string `json:"task"`        // data migration task name
	Source     string `json:"source"`      // upstream source ID
	DownSchema string `json:"down-schema"` // downstream schema name
	DownTable  string `json:"down-table"`  // downstream table name

	// upstream-schema-name -> upstream-table-name -> struct{}
	UpTables map[string]map[string]struct{} `json:"up-tables"`
}

// emptyTargetTable returns an empty TargetTable instance.
func emptyTargetTable() TargetTable {
	return TargetTable{}
}

// newTargetTable returns a TargetTable instance.
func newTargetTable(task, source, downSchema, downTable string,
	upTables map[string]map[string]struct{},
) TargetTable {
	return TargetTable{
		Task:       task,
		Source:     source,
		DownSchema: downSchema,
		DownTable:  downTable,
		UpTables:   upTables,
	}
}

// IsEmpty returns whether the TargetTable instance is empty.
func (tt TargetTable) IsEmpty() bool {
	return tt.Task == "" // now we treat it as empty if no task name specified.
}

// NewSourceTables creates a new SourceTables instances.
func NewSourceTables(task, source string) SourceTables {
	return SourceTables{
		Task:   task,
		Source: source,
		Tables: make(map[string]map[string]map[string]map[string]struct{}),
	}
}

// String implements Stringer interface.
func (st SourceTables) String() string {
	s, _ := st.toJSON()
	return s
}

// toJSON returns the string of JSON represent.
func (st SourceTables) toJSON() (string, error) {
	data, err := json.Marshal(st)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// AddTable adds a table into SourceTables.
// it returns whether added (not exist before).
func (st *SourceTables) AddTable(upSchema, upTable, downSchema, downTable string) bool {
	if _, ok := st.Tables[downSchema]; !ok {
		st.Tables[downSchema] = make(map[string]map[string]map[string]struct{})
	}
	if _, ok := st.Tables[downSchema][downTable]; !ok {
		st.Tables[downSchema][downTable] = make(map[string]map[string]struct{})
	}
	if _, ok := st.Tables[downSchema][downTable][upSchema]; !ok {
		st.Tables[downSchema][downTable][upSchema] = make(map[string]struct{})
	}
	if _, ok := st.Tables[downSchema][downTable][upSchema][upTable]; !ok {
		st.Tables[downSchema][downTable][upSchema][upTable] = struct{}{}
		return true
	}
	return false
}

// RemoveTable removes a table from SourceTables.
// it returns whether removed (exist before).
func (st *SourceTables) RemoveTable(upSchema, upTable, downSchema, downTable string) bool {
	if _, ok := st.Tables[downSchema]; !ok {
		return false
	}
	if _, ok := st.Tables[downSchema][downTable]; !ok {
		return false
	}
	if _, ok := st.Tables[downSchema][downTable][upSchema]; !ok {
		return false
	}
	if _, ok := st.Tables[downSchema][downTable][upSchema][upTable]; !ok {
		return false
	}

	delete(st.Tables[downSchema][downTable][upSchema], upTable)
	if len(st.Tables[downSchema][downTable][upSchema]) == 0 {
		delete(st.Tables[downSchema][downTable], upSchema)
	}
	if len(st.Tables[downSchema][downTable]) == 0 {
		delete(st.Tables[downSchema], downTable)
	}
	if len(st.Tables[downSchema]) == 0 {
		delete(st.Tables, downSchema)
	}
	return true
}

// RouteTable represents a table in upstream/downstream.
type RouteTable struct {
	UpSchema   string
	UpTable    string
	DownSchema string
	DownTable  string
}

func (st *SourceTables) toRouteTable() map[RouteTable]struct{} {
	tables := make(map[RouteTable]struct{})
	for downSchema, downTables := range st.Tables {
		for downTable, upSchemas := range downTables {
			for upSchema, upTables := range upSchemas {
				for upTable := range upTables {
					t := RouteTable{
						UpSchema:   upSchema,
						UpTable:    upTable,
						DownSchema: downSchema,
						DownTable:  downTable,
					}
					tables[t] = struct{}{}
				}
			}
		}
	}
	return tables
}

func DiffSourceTables(oldST, newST SourceTables) (map[RouteTable]struct{}, map[RouteTable]struct{}) {
	oldTables := oldST.toRouteTable()
	newTables := newST.toRouteTable()

	droppedTables := make(map[RouteTable]struct{})
	addedTables := make(map[RouteTable]struct{})
	for table := range oldTables {
		if _, ok := newTables[table]; !ok {
			droppedTables[table] = struct{}{}
		} else {
			delete(newTables, table)
		}
	}

	for table := range newTables {
		addedTables[table] = struct{}{}
	}
	return addedTables, droppedTables
}

// TargetTable returns a TargetTable instance for a specified downstream table,
// returns an empty TargetTable instance if no tables exist.
func (st *SourceTables) TargetTable(downSchema, downTable string) TargetTable {
	ett := emptyTargetTable()
	if _, ok := st.Tables[downSchema]; !ok {
		return ett
	}
	if _, ok := st.Tables[downSchema][downTable]; !ok {
		return ett
	}

	// copy upstream tables.
	tables := make(map[string]map[string]struct{})
	for upSchema, upTables := range st.Tables[downSchema][downTable] {
		tables[upSchema] = make(map[string]struct{})
		for upTable := range upTables {
			tables[upSchema][upTable] = struct{}{}
		}
	}

	return newTargetTable(st.Task, st.Source, downSchema, downTable, tables)
}

// sourceTablesFromJSON constructs SourceTables from its JSON represent.
func sourceTablesFromJSON(s string) (st SourceTables, err error) {
	err = json.Unmarshal([]byte(s), &st)
	return
}

// PutSourceTables puts source tables into etcd.
// This function should often be called by DM-worker.
func PutSourceTables(cli *clientv3.Client, st SourceTables) (int64, error) {
	op, err := putSourceTablesOp(st)
	if err != nil {
		return 0, err
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(op))
	return rev, err
}

// DeleteSourceTables deletes the source tables in etcd.
// This function should often be called by DM-worker.
func DeleteSourceTables(cli *clientv3.Client, st SourceTables) (int64, error) {
	key := common.ShardDDLOptimismSourceTablesKeyAdapter.Encode(st.Task, st.Source)
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(clientv3.OpDelete(key)))
	return rev, err
}

// GetAllSourceTables gets all source tables in etcd currently.
// This function should often be called by DM-master.
// k/k/v: task-name -> source-ID -> source tables.
func GetAllSourceTables(cli *clientv3.Client) (map[string]map[string]SourceTables, int64, error) {
	respTxn, _, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(clientv3.OpGet(common.ShardDDLOptimismSourceTablesKeyAdapter.Path(), clientv3.WithPrefix())))
	if err != nil {
		return nil, 0, err
	}
	resp := respTxn.Responses[0].GetResponseRange()

	stm := make(map[string]map[string]SourceTables)
	for _, kv := range resp.Kvs {
		st, err2 := sourceTablesFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, 0, err2
		}

		if _, ok := stm[st.Task]; !ok {
			stm[st.Task] = make(map[string]SourceTables)
		}
		stm[st.Task][st.Source] = st
	}

	return stm, resp.Header.Revision, nil
}

// WatchSourceTables watches PUT & DELETE operations for source tables.
// This function should often be called by DM-master.
func WatchSourceTables(ctx context.Context, cli *clientv3.Client, revision int64,
	outCh chan<- SourceTables, errCh chan<- error,
) {
	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := cli.Watch(wCtx, common.ShardDDLOptimismSourceTablesKeyAdapter.Path(),
		clientv3.WithPrefix(), clientv3.WithRev(revision))

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-ch:
			if !ok {
				return
			}
			if resp.Canceled {
				select {
				case errCh <- resp.Err():
				case <-ctx.Done():
				}
				return
			}

			for _, ev := range resp.Events {
				var (
					st  SourceTables
					err error
				)

				switch ev.Type {
				case mvccpb.PUT:
					st, err = sourceTablesFromJSON(string(ev.Kv.Value))
				case mvccpb.DELETE:
					st, err = sourceTablesFromKey(string(ev.Kv.Key))
					st.IsDeleted = true
				default:
					// this should not happen.
					err = fmt.Errorf("unsupported ectd event type %v", ev.Type)
				}

				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case outCh <- st:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// sourceTablesFromKey constructs an incomplete SourceTables from an etcd key.
func sourceTablesFromKey(key string) (SourceTables, error) {
	var st SourceTables
	ks, err := common.ShardDDLOptimismSourceTablesKeyAdapter.Decode(key)
	if err != nil {
		return st, err
	}
	st.Task = ks[0]
	st.Source = ks[1]
	return st, nil
}

// putSourceTablesOp returns a PUT etcd operation for source tables.
func putSourceTablesOp(st SourceTables) (clientv3.Op, error) {
	value, err := st.toJSON()
	if err != nil {
		return clientv3.Op{}, err
	}
	key := common.ShardDDLOptimismSourceTablesKeyAdapter.Encode(st.Task, st.Source)
	return clientv3.OpPut(key, value), nil
}

// CheckSourceTables try to check and fix all the source schemas and table names.
func CheckSourceTables(cli *clientv3.Client, source string, schemaMap map[string]string, talesMap map[string]map[string]string) error {
	allSourceTables, _, err := GetAllSourceTables(cli)
	if err != nil {
		return err
	}

	for _, taskSourceTables := range allSourceTables {
		sourceTables, ok := taskSourceTables[source]
		if !ok {
			continue
		}
		schemaKeys := make([]string, 0)
		tblKeys := make([]string, 0)
		hasChange := false
		for _, tableSources := range sourceTables.Tables {
			for _, sources := range tableSources {
				for schema, tbls := range sources {
					if _, ok := schemaMap[schema]; ok {
						schemaKeys = append(schemaKeys, schema)
						hasChange = true
					}

					tblMap, ok := talesMap[schema]
					if !ok {
						continue
					}
					for tbl := range tbls {
						if t, ok := tblMap[tbl]; ok {
							tblKeys = append(tblKeys, t)
							hasChange = true
						}
					}
					for _, t := range tblKeys {
						tbls[tblMap[t]] = tbls[t]
						delete(tbls, t)
					}
					tblKeys = tblKeys[:0]
				}
				for _, s := range schemaKeys {
					sources[schemaMap[s]] = sources[s]
					delete(sources, s)
				}
				schemaKeys = schemaKeys[:0]
			}
		}
		if hasChange {
			if _, err = PutSourceTables(cli, sourceTables); err != nil {
				return err
			}
		}
	}
	return err
}
