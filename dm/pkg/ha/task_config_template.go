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

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/clientv3util"

	"github.com/pingcap/ticdc/dm/dm/common"
	"github.com/pingcap/ticdc/dm/openapi"
	"github.com/pingcap/ticdc/dm/pkg/etcdutil"
	"github.com/pingcap/ticdc/dm/pkg/terror"
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

// PutTaskConfigTemplate puts the task config template of task-name.
func PutTaskConfigTemplate(cli *clientv3.Client, task openapi.Task, overWrite bool) error {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	key := common.TaskConfigTemplateKeyAdapter.Encode(task.Name)
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
		return err
	}
	// user don't want to overwrite and key already exists.
	if !overWrite && !resp.Succeeded {
		return terror.ErrTaskConfigTemplateExists.Generate(task.Name)
	}
	return nil
}

// DeleteTaskConfigTemplate deletes the task config template of task-name.
func DeleteTaskConfigTemplate(cli *clientv3.Client, taskName string) error {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()
	if _, err := cli.Delete(ctx, common.TaskConfigTemplateKeyAdapter.Encode(taskName)); err != nil {
		return err
	}
	return nil
}

// GetTaskConfigTemplate gets the task config template of task-name.
func GetTaskConfigTemplate(cli *clientv3.Client, taskName string) (*openapi.Task, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	var (
		task *openapi.Task
		resp *clientv3.GetResponse
		err  error
	)
	resp, err = cli.Get(ctx, common.TaskConfigTemplateKeyAdapter.Encode(taskName))
	if err != nil {
		return task, err
	}
	task, err = openAPITaskFromResp(resp)
	if err != nil {
		return task, err
	}
	return task, nil
}

// GetAllTaskConfigTemplate gets all task config templates.
func GetAllTaskConfigTemplate(cli *clientv3.Client) ([]*openapi.Task, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.TaskConfigTemplateKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
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
