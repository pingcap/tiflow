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
	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// DeleteInfosOperationsColumns deletes the shard DDL infos, operations, and dropped columns in etcd.
// This function should often be called by DM-master when removing the lock.
// Only delete when all info's version are greater or equal to etcd's version, otherwise it means new info was putted into etcd before.
func DeleteInfosOperationsColumns(cli *clientv3.Client, infos []Info, ops []Operation, lockID string) (int64, bool, error) {
	opsDel := make([]clientv3.Op, 0, len(infos)+len(ops))
	cmps := make([]clientv3.Cmp, 0, len(infos))
	for _, info := range infos {
		key := common.ShardDDLOptimismInfoKeyAdapter.Encode(info.Task, info.Source, info.UpSchema, info.UpTable)
		cmps = append(cmps, clientv3.Compare(clientv3.Version(key), "<", info.Version+1))
		opsDel = append(opsDel, deleteInfoOp(info))
	}
	for _, op := range ops {
		opsDel = append(opsDel, deleteOperationOp(op))
	}
	opsDel = append(opsDel, deleteDroppedColumnsByLockOp(lockID))
	resp, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.FullOpFunc(cmps, opsDel, []clientv3.Op{}))
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// DeleteInfosOperationsTablesByTask deletes the shard DDL infos and operations in etcd.
// This function should often be called by DM-master when stop a task for all sources.
func DeleteInfosOperationsTablesByTask(cli *clientv3.Client, task string, lockIDSet map[string]struct{}) (int64, error) {
	opsDel := make([]clientv3.Op, 0, 5)
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismInfoKeyAdapter.Encode(task), clientv3.WithPrefix()))
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismOperationKeyAdapter.Encode(task), clientv3.WithPrefix()))
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismSourceTablesKeyAdapter.Encode(task), clientv3.WithPrefix()))
	for lockID := range lockIDSet {
		opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(lockID), clientv3.WithPrefix()))
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(opsDel...))
	return rev, err
}

// DeleteInfosOperationsTablesByTaskAndSource deletes the shard DDL infos and operations in etcd by task and source.
// This function should often be called by DM-master when stop a task for sources.
func DeleteInfosOperationsTablesByTaskAndSource(cli *clientv3.Client, task string, sources []string, dropColumns map[string][]string) (int64, error) {
	opsDel := make([]clientv3.Op, 0, 5)
	for _, source := range sources {
		opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismInfoKeyAdapter.Encode(task, source), clientv3.WithPrefix()))
		opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismOperationKeyAdapter.Encode(task, source), clientv3.WithPrefix()))
		opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismSourceTablesKeyAdapter.Encode(task, source), clientv3.WithPrefix()))
		for lockID, cols := range dropColumns {
			for _, col := range cols {
				for _, source := range sources {
					opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(lockID, col, source), clientv3.WithPrefix()))
				}
			}
		}
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(opsDel...))
	return rev, err
}

// DeleteInfosOperationsTablesByTable deletes the shard DDL infos and operations in etcd by table
// This function should often be called by DM-master when drop a table.
func DeleteInfosOperationsTablesByTable(cli *clientv3.Client, task, source, upSchema, upTable, lockID string, dropCols []string) (int64, error) {
	opsDel := make([]clientv3.Op, 0, 5)
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismInfoKeyAdapter.Encode(task, source, upSchema, upTable)))
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismOperationKeyAdapter.Encode(task, source, upSchema, upTable)))
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismSourceTablesKeyAdapter.Encode(task, source, upSchema, upTable)))
	for _, col := range dropCols {
		opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(lockID, col, source, upSchema, upTable)))
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(opsDel...))
	return rev, err
}
