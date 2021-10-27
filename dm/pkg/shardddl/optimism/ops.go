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
	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/utils"
)

// PutSourceTablesInfo puts source tables and a shard DDL info.
// This function is often used in DM-worker when handling `CREATE TABLE`.
func PutSourceTablesInfo(cli *clientv3.Client, st SourceTables, info Info) (int64, error) {
	stOp, err := putSourceTablesOp(st)
	if err != nil {
		return 0, err
	}
	infoOp, err := putInfoOp(info)
	if err != nil {
		return 0, err
	}
	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, stOp, infoOp)
	return rev, err
}

// PutSourceTablesDeleteInfo puts source tables and deletes a shard DDL info.
// This function is often used in DM-worker when handling `DROP TABLE`.
func PutSourceTablesDeleteInfo(cli *clientv3.Client, st SourceTables, info Info) (int64, error) {
	stOp, err := putSourceTablesOp(st)
	if err != nil {
		return 0, err
	}
	infoOp := deleteInfoOp(info)
	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, stOp, infoOp)
	return rev, err
}

// DeleteInfosOperationsSchemaColumn deletes the shard DDL infos, operations, init schemas and dropped columns in etcd.
// This function should often be called by DM-master when removing the lock.
// Only delete when all info's version are greater or equal to etcd's version, otherwise it means new info was putted into etcd before.
func DeleteInfosOperationsSchemaColumn(cli *clientv3.Client, infos []Info, ops []Operation, schema InitSchema) (int64, bool, error) {
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
	opsDel = append(opsDel, deleteInitSchemaOp(schema.Task, schema.DownSchema, schema.DownTable))
	opsDel = append(opsDel, deleteDroppedColumnsByLockOp(utils.GenDDLLockID(schema.Task, schema.DownSchema, schema.DownTable)))
	resp, rev, err := etcdutil.DoOpsInOneCmpsTxnWithRetry(cli, cmps, opsDel, []clientv3.Op{})
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// DeleteInfosOperationsTablesSchemasByTask deletes the shard DDL infos and operations in etcd.
func DeleteInfosOperationsTablesSchemasByTask(cli *clientv3.Client, task string, lockIDSet map[string]struct{}) (int64, error) {
	opsDel := make([]clientv3.Op, 0, 5)
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismInfoKeyAdapter.Encode(task), clientv3.WithPrefix()))
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismOperationKeyAdapter.Encode(task), clientv3.WithPrefix()))
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismSourceTablesKeyAdapter.Encode(task), clientv3.WithPrefix()))
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismInitSchemaKeyAdapter.Encode(task), clientv3.WithPrefix()))
	for lockID := range lockIDSet {
		opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Encode(lockID), clientv3.WithPrefix()))
	}
	_, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, opsDel...)
	return rev, err
}
