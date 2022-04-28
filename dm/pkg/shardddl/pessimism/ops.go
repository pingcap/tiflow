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

package pessimism

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/pingcap/tiflow/dm/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
)

// TODO(csuzhangxc): assign terror code before merged into the master branch.

// PutOperationDeleteExistInfo puts an operation and deletes an info in one txn,
// if the info exists in etcd before.
// This function should often be called by DM-worker.
func PutOperationDeleteExistInfo(cli *clientv3.Client, op Operation, info Info) (done bool, rev int64, err error) {
	putOp, err := putOperationOp(op)
	if err != nil {
		return false, 0, nil
	}
	delOp := deleteInfoOp(info)

	infoCmp := infoExistCmp(info)

	getOp := getOperationOp(op)

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Txn(ctx).If(infoCmp).Then(putOp, delOp).Else(getOp).Commit()
	if err != nil {
		return false, 0, err
	}

	if !resp.Succeeded && len(resp.Responses) == 1 {
		if getResponse := resp.Responses[0].GetResponseRange(); getResponse.GetCount() == 1 {
			// err must be nil here, or the last putOperationOp will return an error.
			opBytes, _ := op.toJSON()
			// we may successfully delete the info in the previous txn but fail to get the result
			// before the connection is broken. If we check the operation is the same with the put
			// operation, we can safely assume the done operation is okay.
			return opBytes == string(getResponse.Kvs[0].Value), resp.Header.Revision, nil
		}
	}
	return resp.Succeeded, resp.Header.Revision, nil
}

// DeleteInfosOperations deletes the shard DDL infos and operations in etcd.
// This function should often be called by DM-master when calling UnlockDDL.
func DeleteInfosOperations(cli *clientv3.Client, infos []Info, ops []Operation) (int64, error) {
	opsDel := make([]clientv3.Op, 0, len(infos)+len(ops))
	for _, info := range infos {
		opsDel = append(opsDel, deleteInfoOp(info))
	}
	for _, op := range ops {
		opsDel = append(opsDel, deleteOperationOp(op))
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(opsDel...))
	return rev, err
}

// DeleteInfosOperationsByTask deletes the shard DDL infos and operations of a specified task in etcd.
// This function should often be called by DM-master when deleting ddl meta data.
func DeleteInfosOperationsByTask(cli *clientv3.Client, task string) (int64, error) {
	opsDel := make([]clientv3.Op, 0, 2)
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLPessimismInfoKeyAdapter.Encode(task), clientv3.WithPrefix()))
	opsDel = append(opsDel, clientv3.OpDelete(common.ShardDDLPessimismOperationKeyAdapter.Encode(task), clientv3.WithPrefix()))
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(opsDel...))
	return rev, err
}
