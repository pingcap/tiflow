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

package optimism

import (
	"encoding/json"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/tiflow/dm/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
)

// GetAllDroppedColumns gets the all partially dropped columns.
// return lockID -> column-name -> source-id -> upstream-schema-name -> upstream-table-name.
func GetAllDroppedColumns(cli *clientv3.Client) (map[string]map[string]map[string]map[string]map[string]DropColumnStage, int64, error) {
	var done DropColumnStage
	colm := make(map[string]map[string]map[string]map[string]map[string]DropColumnStage)
	op := clientv3.OpGet(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Path(), clientv3.WithPrefix())
	respTxn, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(op))
	if err != nil {
		return colm, 0, err
	}
	resp := respTxn.Responses[0].GetResponseRange()

	if resp.Count > 0 {
		for _, kv := range resp.Kvs {
			keys, err := common.ShardDDLOptimismDroppedColumnsKeyAdapter.Decode(string(kv.Key))
			if err != nil {
				return colm, 0, err
			}
			err = json.Unmarshal(kv.Value, &done)
			if err != nil {
				return colm, 0, err
			}
			lockID := keys[0]
			column := keys[1]
			source := keys[2]
			upSchema := keys[3]
			upTable := keys[4]
			if _, ok := colm[lockID]; !ok {
				colm[lockID] = make(map[string]map[string]map[string]map[string]DropColumnStage)
			}
			if _, ok := colm[lockID][column]; !ok {
				colm[lockID][column] = make(map[string]map[string]map[string]DropColumnStage)
			}
			if _, ok := colm[lockID][column][source]; !ok {
				colm[lockID][column][source] = make(map[string]map[string]DropColumnStage)
			}
			if _, ok := colm[lockID][column][source][upSchema]; !ok {
				colm[lockID][column][source][upSchema] = make(map[string]DropColumnStage)
			}
			colm[lockID][column][source][upSchema][upTable] = done
		}
	}
	return colm, rev, nil
}

// PutDroppedColumn puts the partially dropped column names into ectd.
// When we drop a column, we save this column's name in etcd.
func PutDroppedColumns(cli *clientv3.Client, lockID, source, upSchema, upTable string, cols []string, done DropColumnStage) (int64, bool, error) {
	ops := make([]clientv3.Op, 0, len(cols))
	for _, column := range cols {
		key := common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(lockID, column, source, upSchema, upTable)
		val, err := json.Marshal(done)
		if err != nil {
			return 0, false, err
		}
		op := clientv3.OpPut(key, string(val))
		ops = append(ops, op)
	}

	resp, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// DeleteDroppedColumns tries to delete the partially dropped columns for the specified lock ID.
// Only when this column is fully dropped in downstream database,
// in other words, **we receive all `Done` operation from dm-worker**,
// we can delete this column's name from the etcd.
func DeleteDroppedColumns(cli *clientv3.Client, lockID string, columns ...string) (rev int64, deleted bool, err error) {
	ops := make([]clientv3.Op, 0, len(columns))
	for _, col := range columns {
		ops = append(ops, deleteDroppedColumnByColumnOp(lockID, col))
	}
	resp, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// deleteDroppedColumnOp returns a DELETE etcd operation for the specified task and column name.
func deleteDroppedColumnByColumnOp(lockID, column string) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(lockID, column), clientv3.WithPrefix())
}

// deleteDroppedColumnsByLockOp returns a DELETE etcd operation for the specified lock.
func deleteDroppedColumnsByLockOp(lockID string) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(lockID), clientv3.WithPrefix())
}

// deleteSourceDroppedColumnsOp return a DELETE etcd operation for the specified lock relate to a upstream table.
func deleteSourceDroppedColumnsOp(lockID, column, source, upSchema, upTable string) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(lockID, column, source, upSchema, upTable))
}

// CheckColumns try to check and fix all the schema and table names for delete columns infos.
func CheckColumns(cli *clientv3.Client, source string, schemaMap map[string]string, tablesMap map[string]map[string]string) error {
	allColInfos, _, err := GetAllDroppedColumns(cli)
	if err != nil {
		return err
	}

	for lockID, colDropInfo := range allColInfos {
		for columnName, sourceDropInfo := range colDropInfo {
			tableInfos, ok := sourceDropInfo[source]
			if !ok {
				continue
			}
			for schema, tableDropInfo := range tableInfos {
				realSchema, hasChange := schemaMap[schema]
				if !hasChange {
					realSchema = schema
				}
				tableMap := tablesMap[schema]
				for table, stage := range tableDropInfo {
					realTable, tblChange := tableMap[table]
					if !tblChange {
						realTable = table
						tblChange = hasChange
					}
					if tblChange {
						key := common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(lockID, columnName, source, realSchema, realTable)
						val, err := json.Marshal(stage)
						if err != nil {
							return err
						}
						opPut := clientv3.OpPut(key, string(val))
						opDel := deleteSourceDroppedColumnsOp(lockID, columnName, source, schema, table)

						_, _, err = etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(opPut, opDel))
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}
