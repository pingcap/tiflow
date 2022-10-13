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
	"fmt"

	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func putTaskCliArgsOp(taskname string, sources []string, args config.TaskCliArgs) ([]clientv3.Op, error) {
	data, err := args.ToJSON()
	if err != nil {
		return nil, err
	}

	ops := []clientv3.Op{}
	for _, source := range sources {
		key := common.TaskCliArgsKeyAdapter.Encode(taskname, source)
		ops = append(ops, clientv3.OpPut(key, data))
	}
	return ops, nil
}

// PutTaskCliArgs puts TaskCliArgs into etcd.
func PutTaskCliArgs(cli *clientv3.Client, taskName string, sources []string, args config.TaskCliArgs) error {
	ops, err := putTaskCliArgsOp(taskName, sources, args)
	if err != nil {
		return err
	}
	_, _, err = etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return err
}

// GetTaskCliArgs gets the command line arguments for the specified task.
func GetTaskCliArgs(cli *clientv3.Client, taskName, source string) (*config.TaskCliArgs, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.TaskCliArgsKeyAdapter.Encode(taskName, source))
	if err != nil {
		return nil, terror.ErrHAFailTxnOperation.Delegate(err, fmt.Sprintf("fail to get task cli args, taskName: %s, source: %s", taskName, source))
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

// DeleteAllTaskCliArgs deleted the command line arguments of this task.
func DeleteAllTaskCliArgs(cli *clientv3.Client, taskName string) error {
	key := common.TaskCliArgsKeyAdapter.Encode(taskName)
	op := clientv3.OpDelete(key, clientv3.WithPrefix())
	_, _, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(op))
	return err
}

// DeleteTaskCliArgs deleted the command line arguments of this task.
func DeleteTaskCliArgs(cli *clientv3.Client, taskName string, sources []string) error {
	if len(sources) == 0 {
		return nil
	}
	ops := []clientv3.Op{}
	for _, source := range sources {
		key := common.TaskCliArgsKeyAdapter.Encode(taskName, source)
		ops = append(ops, clientv3.OpDelete(key))
	}
	_, _, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	return err
}
