// Copyright 2022 PingCAP, Inc.
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
	"encoding/json"

	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Simple status transition to make all lightning synchronized in physical import
// mode.
// When DM-master creates the task, it will write LightningNotReady for all subtasks.
// When DM-worker enters load unit, it will mark itself as LightningReady. When
// all DM-workers are ready, lightning can be started so its parallel import can
// work.
// When lightning is finished and its returned error is also resolved, it will mark
// itself as LightningFinished. When all DM-workers are finished, sync unit can
// be continued.
const (
	LightningNotReady = "not-ready"
	LightningReady    = "ready"
	LightningFinished = "finished"
)

// PutLightningStatus puts the status for the source of the subtask.
// k/v: (task, sourceID) -> status.
// This function should be called by DM-worker.
func PutLightningStatus(cli *clientv3.Client, task, sourceID, status string) (int64, error) {
	data, err := json.Marshal(status)
	if err != nil {
		return 0, err
	}
	key := common.LightningCoordinationKeyAdapter.Encode(task, sourceID)

	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(clientv3.OpPut(key, string(data))))
	if err != nil {
		return 0, err
	}
	return rev, nil
}

// PutLightningNotReadyForAllSources puts LightningNotReady for all sources of the subtask.
// This function should be called by DM-master.
func PutLightningNotReadyForAllSources(cli *clientv3.Client, task string, sources []string) (int64, error) {
	data, err := json.Marshal(LightningNotReady)
	if err != nil {
		return 0, err
	}
	ops := make([]clientv3.Op, 0, len(sources))
	for _, source := range sources {
		key := common.LightningCoordinationKeyAdapter.Encode(task, source)
		ops = append(ops, clientv3.OpPut(key, string(data)))
	}

	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(ops...))
	if err != nil {
		return 0, err
	}
	return rev, nil
}

// DeleteLightningStatusForTask deletes the status for all sources of the task.
func DeleteLightningStatusForTask(cli *clientv3.Client, task string) (int64, error) {
	key := common.LightningCoordinationKeyAdapter.Encode(task)
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(clientv3.OpDelete(key, clientv3.WithPrefix())))
	if err != nil {
		return 0, err
	}
	return rev, nil
}

// GetAllLightningStatus gets the status for all source of the task.
func GetAllLightningStatus(cli *clientv3.Client, task string) ([]string, error) {
	key := common.LightningCoordinationKeyAdapter.Encode(task)
	resp, err := cli.Get(cli.Ctx(), key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	ret := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var s string
		if err2 := json.Unmarshal(kv.Value, &s); err2 != nil {
			return nil, err2
		}
		ret = append(ret, s)
	}
	return ret, nil
}
