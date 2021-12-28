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

package ha

import (
	"context"

	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/tiflow/dm/dm/common"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// PutTaskCliArgs puts TaskCliArgs into etcd.
func PutTaskCliArgs(cli *clientv3.Client, taskName string, args config.TaskCliArgs) error {
	data, err := args.ToJSON()
	if err != nil {
		return err
	}
	key := common.TaskCliArgsKeyAdapter.Encode(taskName)
	op := clientv3.OpPut(key, data)
	_, _, err = etcdutil.DoOpsInOneTxnWithRetry(cli, op)
	return err
}

// GetTaskCliArgs gets the command line arguments for the specified task.
func GetTaskCliArgs(cli *clientv3.Client, taskName string) (*config.TaskCliArgs, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.TaskCliArgsKeyAdapter.Encode(taskName))
	if err != nil {
		return nil, err
	}

	if resp.Count == 0 {
		return nil, nil
	} else if resp.Count > 1 {
		// this should not happen.
		return nil, terror.ErrConfigMoreThanOne.Generate(resp.Count, "TaskCliArgs", "task: "+taskName)
	}

	args := &config.TaskCliArgs{}
	err = args.Decode(resp.Kvs[0].Value)
	if err != nil {
		return nil, err
	}

	return args, nil
}

// DeleteTaskCliArgs deleted the command line arguments of this task.
func DeleteTaskCliArgs(cli *clientv3.Client, taskName string) error {
	key := common.TaskCliArgsKeyAdapter.Encode(taskName)
	op := clientv3.OpDelete(key)
	_, _, err := etcdutil.DoOpsInOneTxnWithRetry(cli, op)
	return err
}
