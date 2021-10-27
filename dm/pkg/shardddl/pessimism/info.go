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
	"encoding/json"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/clientv3util"
	"go.etcd.io/etcd/mvcc/mvccpb"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
	"github.com/pingcap/dm/pkg/utils"
)

// Info represents the shard DDL information.
// This information should be persistent in etcd so can be retrieved after the DM-master leader restarted or changed.
// NOTE: `Task` and `Source` are redundant in the etcd key path for convenient.
type Info struct {
	Task   string   `json:"task"`   // data migration task name
	Source string   `json:"source"` // upstream source ID
	Schema string   `json:"schema"` // schema name of the DDL
	Table  string   `json:"table"`  // table name of the DDL
	DDLs   []string `json:"ddls"`   // DDL statements
}

// NewInfo creates a new Info instance.
func NewInfo(task, source, schema, table string, ddls []string) Info {
	return Info{
		Task:   task,
		Source: source,
		Schema: schema,
		Table:  table,
		DDLs:   ddls,
	}
}

// String implements Stringer interface.
func (i Info) String() string {
	s, _ := i.toJSON()
	return s
}

// toJSON returns the string of JSON represent.
func (i Info) toJSON() (string, error) {
	data, err := json.Marshal(i)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// infoFromJSON constructs Info from its JSON represent.
func infoFromJSON(s string) (i Info, err error) {
	err = json.Unmarshal([]byte(s), &i)
	return
}

// PutInfo puts the shard DDL info into etcd.
// NOTE:
//   In some cases before the lock resolved, the same DDL info may be PUT multiple times:
//     1. start-task after stop-task.
//     2. resume-task after paused manually or automatically.
//     3. the task scheduled to another DM-worker instance (just like case-1).
//   Then we need to ensure re-PUT is safe:
//     1. DM-master can construct the lock and do the coordination correctly.
//     2. DM-worker can re-PUT and comply with the coordination correctly.
// This function should often be called by DM-worker.
func PutInfo(cli *clientv3.Client, info Info) (int64, error) {
	value, err := info.toJSON()
	if err != nil {
		return 0, err
	}
	key := common.ShardDDLPessimismInfoKeyAdapter.Encode(info.Task, info.Source)

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Put(ctx, key, value)
	if err != nil {
		return 0, err
	}
	return resp.Header.Revision, nil
}

// PutInfoIfOpNotDone puts the shard DDL info into etcd if the operation not exists or not `done`.
func PutInfoIfOpNotDone(cli *clientv3.Client, info Info) (rev int64, putted bool, err error) {
	infoValue, err := info.toJSON()
	if err != nil {
		return 0, false, err
	}
	infoKey := common.ShardDDLPessimismInfoKeyAdapter.Encode(info.Task, info.Source)
	opKey := common.ShardDDLPessimismOperationKeyAdapter.Encode(info.Task, info.Source)
	infoPut := clientv3.OpPut(infoKey, infoValue)
	opGet := clientv3.OpGet(opKey)

	// try to PUT info if the operation not exist.
	resp, rev, err := etcdutil.DoOpsInOneCmpsTxnWithRetry(cli, []clientv3.Cmp{clientv3util.KeyMissing(opKey)},
		[]clientv3.Op{infoPut}, []clientv3.Op{opGet})
	if err != nil {
		return 0, false, err
	} else if resp.Succeeded {
		return rev, resp.Succeeded, nil
	}

	opsResp := resp.Responses[0].GetResponseRange()
	opBefore, err := operationFromJSON(string(opsResp.Kvs[0].Value))
	switch {
	case err != nil:
		return 0, false, err
	case opBefore.Done:
		// the operation with `done` exist before, abort the PUT.
		return rev, false, nil
	case utils.CompareShardingDDLs(opBefore.DDLs, info.DDLs):
		// TODO: try to handle put the same `done` DDL later.
	}

	// NOTE: try to PUT info if the operation still not done.
	opNotDone := clientv3.Compare(clientv3.Value(opKey), "=", string(opsResp.Kvs[0].Value))
	resp, rev, err = etcdutil.DoOpsInOneCmpsTxnWithRetry(cli, []clientv3.Cmp{opNotDone}, []clientv3.Op{infoPut}, []clientv3.Op{})
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// GetAllInfo gets all shard DDL info in etcd currently.
// k/k/v: task-name -> source-ID -> DDL info.
// This function should often be called by DM-master.
func GetAllInfo(cli *clientv3.Client) (map[string]map[string]Info, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.ShardDDLPessimismInfoKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}

	ifm := make(map[string]map[string]Info)
	for _, kv := range resp.Kvs {
		info, err2 := infoFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, 0, err2
		}

		if _, ok := ifm[info.Task]; !ok {
			ifm[info.Task] = make(map[string]Info)
		}
		ifm[info.Task][info.Source] = info
	}

	return ifm, resp.Header.Revision, nil
}

// WatchInfoPut watches PUT operations for info.
// This function should often be called by DM-master.
func WatchInfoPut(ctx context.Context, cli *clientv3.Client, revision int64, outCh chan<- Info, errCh chan<- error) {
	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := cli.Watch(wCtx, common.ShardDDLPessimismInfoKeyAdapter.Path(),
		clientv3.WithPrefix(), clientv3.WithRev(revision))

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
				case errCh <- resp.Err():
				case <-ctx.Done():
				}
				return
			}

			for _, ev := range resp.Events {
				if ev.Type != mvccpb.PUT {
					continue
				}

				info, err := infoFromJSON(string(ev.Kv.Value))
				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case outCh <- info:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// deleteInfoOp returns a DELETE etcd operation for info.
// This operation should often be sent by DM-worker.
func deleteInfoOp(info Info) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLPessimismInfoKeyAdapter.Encode(info.Task, info.Source))
}

// infoExistCmp returns a etcd Cmp which indicates the info exists.
func infoExistCmp(info Info) clientv3.Cmp {
	return clientv3util.KeyExists(common.ShardDDLPessimismInfoKeyAdapter.Encode(info.Task, info.Source))
}
