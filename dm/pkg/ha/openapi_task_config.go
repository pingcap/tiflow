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

package ha

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"

	"github.com/pingcap/tiflow/dm/dm/common"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

func openAPITaskFromResp(resp *clientv3.GetResponse) (*openapi.Task, error) {
	task := &openapi.Task{}
	if resp.Count == 0 {
		return nil, nil
	} else if resp.Count > 1 {
		// this should not happen.
		return task, terror.ErrConfigMoreThanOne.Generate(resp.Count, "openapi.Task", "")
	}
	// we make sure only have one task config.
	if err := task.FromJSON(resp.Kvs[0].Value); err != nil {
		return task, err
	}
	return task, nil
}

// PutOpenAPITaskTemplate puts the openapi task config of task-name.
func PutOpenAPITaskTemplate(cli *clientv3.Client, task openapi.Task, overWrite bool) error {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	key := common.OpenAPITaskTemplateKeyAdapter.Encode(task.Name)
	taskJSON, err := task.ToJSON()
	if err != nil {
		return err // it should not happen.
	}
	txn := cli.Txn(ctx)
	if !overWrite {
		txn = txn.If(clientv3util.KeyMissing(key))
	}
	resp, err := txn.Then(clientv3.OpPut(key, string(taskJSON))).Commit()
	if err != nil {
		return terror.ErrHAFailTxnOperation.Delegate(err, "put openapi task template")
	}
	// user don't want to overwrite and key already exists.
	if !overWrite && !resp.Succeeded {
		return terror.ErrOpenAPITaskConfigExist.Generate(task.Name)
	}
	return nil
}

// UpdateOpenAPITaskTemplate updates the openapi task config by task-name.
func UpdateOpenAPITaskTemplate(cli *clientv3.Client, task openapi.Task) error {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	key := common.OpenAPITaskTemplateKeyAdapter.Encode(task.Name)
	taskJSON, err := task.ToJSON()
	if err != nil {
		return err // it should not happen.
	}
	txn := cli.Txn(ctx).If(clientv3util.KeyExists(key)).Then(clientv3.OpPut(key, string(taskJSON)))
	resp, err := txn.Commit()
	if err != nil {
		return terror.ErrHAFailTxnOperation.Delegate(err, "update openapi task template")
	}
	// user want to update a key not exists.
	if !resp.Succeeded {
		return terror.ErrOpenAPITaskConfigNotExist.Generate(task.Name)
	}
	return nil
}

// DeleteOpenAPITaskTemplate deletes the openapi task config of task-name.
func DeleteOpenAPITaskTemplate(cli *clientv3.Client, taskName string) error {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()
	if _, err := cli.Delete(ctx, common.OpenAPITaskTemplateKeyAdapter.Encode(taskName)); err != nil {
		return terror.ErrHAFailTxnOperation.Delegate(err, "delete openapi task template")
	}
	return nil
}

// GetOpenAPITaskTemplate gets the openapi task config of task-name.
func GetOpenAPITaskTemplate(cli *clientv3.Client, taskName string) (*openapi.Task, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var (
		task *openapi.Task
		resp *clientv3.GetResponse
		err  error
	)
	resp, err = cli.Get(ctx, common.OpenAPITaskTemplateKeyAdapter.Encode(taskName))
	if err != nil {
		return task, terror.ErrHAFailTxnOperation.Delegate(err, "get openapi task template")
	}
	return openAPITaskFromResp(resp)
}

// GetAllOpenAPITaskTemplate gets all openapi task config s.
func GetAllOpenAPITaskTemplate(cli *clientv3.Client) ([]*openapi.Task, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.OpenAPITaskTemplateKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, terror.ErrHAFailTxnOperation.Delegate(err, "get all openapi task templates")
	}
	tasks := make([]*openapi.Task, resp.Count)
	for i, kv := range resp.Kvs {
		t := &openapi.Task{}
		if err := t.FromJSON(kv.Value); err != nil {
			return nil, err
		}
		tasks[i] = t
	}
	return tasks, nil
}
