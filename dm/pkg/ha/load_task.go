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
	"encoding/json"
	"fmt"

	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// LoadTask uses to watch load worker events.
type LoadTask struct {
	Task     string
	Source   string
	Worker   string
	IsDelete bool
}

// GetLoadTask gets the worker which in load stage for the source of the subtask.
// k/v: (task, sourceID) -> worker-name.
func GetLoadTask(cli *clientv3.Client, task, sourceID string) (string, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()
	resp, err := cli.Get(ctx, common.LoadTaskKeyAdapter.Encode(task, sourceID))
	if err != nil {
		return "", 0, terror.ErrHAFailTxnOperation.Delegate(err, fmt.Sprintf("fail to get load task, task: %s, sourceID: %s", task, sourceID))
	}

	if resp.Count <= 0 {
		return "", resp.Header.Revision, nil
	}

	var worker string
	err = json.Unmarshal(resp.Kvs[0].Value, &worker)

	return worker, resp.Header.Revision, terror.ErrHAInvalidItem.Delegate(err, "fail to unmarshal load task")
}

// GetAllLoadTask gets all the worker which in load stage.
// k/v: (task, sourceID) -> worker-name.
func GetAllLoadTask(cli *clientv3.Client) (map[string]map[string]string, int64, error) {
	var (
		worker      string
		loadTaskMap = make(map[string]map[string]string)
	)
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()
	resp, err := cli.Get(ctx, common.LoadTaskKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return loadTaskMap, 0, terror.ErrHAFailTxnOperation.Delegate(err, "fail to get all load task")
	}

	for _, kv := range resp.Kvs {
		keys, err2 := common.LoadTaskKeyAdapter.Decode(string(kv.Key))
		if err2 != nil {
			return nil, 0, err2
		}

		task := keys[0]
		source := keys[1]

		err = json.Unmarshal(kv.Value, &worker)
		if err != nil {
			return loadTaskMap, 0, terror.ErrHAInvalidItem.Delegate(err, "fail to unmarshal load task")
		}

		if _, ok := loadTaskMap[task]; !ok {
			loadTaskMap[task] = make(map[string]string)
		}
		loadTaskMap[task][source] = worker
	}

	return loadTaskMap, resp.Header.Revision, nil
}

// WatchLoadTask watches PUT & DELETE operations for worker in load stage.
// This function should often be called by DM-master.
func WatchLoadTask(ctx context.Context, cli *clientv3.Client, revision int64,
	outCh chan<- LoadTask, errCh chan<- error,
) {
	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// NOTE: WithPrevKV used to get a valid `ev.PrevKv` for deletion.
	ch := cli.Watch(wCtx, common.LoadTaskKeyAdapter.Path(),
		clientv3.WithPrefix(), clientv3.WithRev(revision), clientv3.WithPrevKV())

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
				case errCh <- terror.ErrHAFailWatchEtcd.Delegate(resp.Err(), "watch load task canceled"):
				case <-ctx.Done():
				}
				return
			}

			for _, ev := range resp.Events {
				var (
					loadTask LoadTask
					err      error
					keys     []string
				)

				switch ev.Type {
				case mvccpb.PUT, mvccpb.DELETE:
					keys, err = common.LoadTaskKeyAdapter.Decode(string(ev.Kv.Key))
					if err == nil {
						loadTask.Task = keys[0]
						loadTask.Source = keys[1]
						if ev.Type == mvccpb.PUT {
							err = json.Unmarshal(ev.Kv.Value, &loadTask.Worker)
						} else {
							loadTask.IsDelete = true
						}
					}
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
					case outCh <- loadTask:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// PutLoadTask puts the worker which load stage for the source of the subtask.
// k/v: (task, sourceID) -> worker.
// This function should often be called by DM-worker.
func PutLoadTask(cli *clientv3.Client, task, sourceID, worker string) (int64, error) {
	data, err := json.Marshal(worker)
	if err != nil {
		return 0, err
	}
	key := common.LoadTaskKeyAdapter.Encode(task, sourceID)

	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(clientv3.OpPut(key, string(data))))
	if err != nil {
		return 0, err
	}
	return rev, nil
}

// DelLoadTask dels the worker in load stage for the source of the subtask.
// k/v: (task, sourceID) -> worker.
func DelLoadTask(cli *clientv3.Client, task, sourceID string) (int64, bool, error) {
	key := common.LoadTaskKeyAdapter.Encode(task, sourceID)

	resp, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(clientv3.OpDelete(key)))
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// DelLoadTaskByTask del the worker in load stage for the source by task.
func DelLoadTaskByTask(cli *clientv3.Client, task string) (int64, bool, error) {
	key := common.LoadTaskKeyAdapter.Encode(task)

	resp, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(clientv3.OpDelete(key, clientv3.WithPrefix())))
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}
