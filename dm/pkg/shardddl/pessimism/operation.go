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

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"

	"github.com/pingcap/tiflow/dm/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
)

// Operation represents a shard DDL coordinate operation.
// This information should be persistent in etcd so can be retrieved after the DM-master leader restarted or changed.
// NOTE: `Task` and `Source` are redundant in the etcd key path for convenient.
type Operation struct {
	ID     string   `json:"id"`     // the corresponding DDL lock ID
	Task   string   `json:"task"`   // data migration task name
	Source string   `json:"source"` // upstream source ID
	DDLs   []string `json:"ddls"`   // DDL statements
	Exec   bool     `json:"exec"`   // execute or skip the DDL statements
	Done   bool     `json:"done"`   // whether the `Exec` operation has done

	// only used to report to the caller of the watcher, do not marsh it.
	// if it's true, it means the Operation has been deleted in etcd.
	IsDeleted bool `json:"-"`
}

// NewOperation creates a new Operation instance.
func NewOperation(id, task, source string, ddls []string, exec, done bool) Operation {
	return Operation{
		ID:     id,
		Task:   task,
		Source: source,
		DDLs:   ddls,
		Exec:   exec,
		Done:   done,
	}
}

// String implements Stringer interface.
func (o Operation) String() string {
	s, _ := o.toJSON()
	return s
}

// toJSON returns the string of JSON represent.
func (o Operation) toJSON() (string, error) {
	data, err := json.Marshal(o)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// operationFromJSON constructs Operation from its JSON represent.
func operationFromJSON(s string) (o Operation, err error) {
	err = json.Unmarshal([]byte(s), &o)
	return
}

// PutOperations puts the shard DDL operations into etcd.
// if `skipDone` is `true`:
//   - PUT: all of kvs ("not exist" or "the `done` field is not `true`")
//   - skip PUT: any of kvs ("exist" and "the `done` field is `true`")
//
// NOTE:
//
//	`clientv3.Value` has a strange behavior for *not-exist* kv,
//	see https://github.com/etcd-io/etcd/issues/10566.
//	In addition, etcd compare has no `OR` operator now,
//	see https://github.com/etcd-io/etcd/issues/10571.
//	So, it's hard to do our `skipDone` logic in one txn.
//	We break the logic into two txn, but this may lead to problem when PUT operations concurrently.
//
// This function should often be called by DM-master.
func PutOperations(cli *clientv3.Client, skipDone bool, ops ...Operation) (rev int64, putted bool, err error) {
	cmpsNotExist := make([]clientv3.Cmp, 0, len(ops))
	cmpsNotDone := make([]clientv3.Cmp, 0, len(ops))
	opsPut := make([]clientv3.Op, 0, len(ops))
	for _, op := range ops {
		value, err2 := op.toJSON()
		if err2 != nil {
			return 0, false, err2
		}

		key := common.ShardDDLPessimismOperationKeyAdapter.Encode(op.Task, op.Source)
		opsPut = append(opsPut, clientv3.OpPut(key, value))

		if skipDone {
			opDone := op
			opDone.Done = true // set `done` to `true`.
			valueDone, err3 := opDone.toJSON()
			if err3 != nil {
				return 0, false, err3
			}
			cmpsNotExist = append(cmpsNotExist, clientv3util.KeyMissing(key))
			cmpsNotDone = append(cmpsNotDone, clientv3.Compare(clientv3.Value(key), "!=", valueDone))
		}
	}

	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	// txn 1: try to PUT if all of kvs "not exist".
	resp, err := cli.Txn(ctx).If(cmpsNotExist...).Then(opsPut...).Commit()
	if err != nil {
		return 0, false, err
	} else if resp.Succeeded {
		return resp.Header.Revision, resp.Succeeded, nil
	}

	// txn 2: try to PUT if all of kvs "the `done` field is not `true`.
	// FIXME: if any "not `done`" kv putted after txn 1, this txn 2 will fail, but this is not what we want.
	resp, err = cli.Txn(ctx).If(cmpsNotDone...).Then(opsPut...).Commit()
	if err != nil {
		return 0, false, err
	}
	return resp.Header.Revision, resp.Succeeded, nil
}

// DeleteOperations deletes the shard DDL operations in etcd.
// This function should often be called by DM-master.
func DeleteOperations(cli *clientv3.Client, ops ...Operation) (int64, error) {
	opsDel := make([]clientv3.Op, 0, len(ops))
	for _, op := range ops {
		opsDel = append(opsDel, deleteOperationOp(op))
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(opsDel...))
	return rev, err
}

// GetAllOperations gets all DDL lock operation in etcd currently.
// k/k/v: task-name -> source-ID -> lock operation.
func GetAllOperations(cli *clientv3.Client) (map[string]map[string]Operation, int64, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	resp, err := cli.Get(ctx, common.ShardDDLPessimismOperationKeyAdapter.Path(), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, err
	}

	opm := make(map[string]map[string]Operation)
	for _, kv := range resp.Kvs {
		op, err2 := operationFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, 0, err2
		}

		if _, ok := opm[op.Task]; !ok {
			opm[op.Task] = make(map[string]Operation)
		}
		opm[op.Task][op.Source] = op
	}

	return opm, resp.Header.Revision, nil
}

// GetInfosOperationsByTask gets all DDL lock infos and operations in etcd currently.
func GetInfosOperationsByTask(cli *clientv3.Client, task string) ([]Info, []Operation, int64, error) {
	respTxn, _, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(
		clientv3.OpGet(common.ShardDDLPessimismInfoKeyAdapter.Encode(task), clientv3.WithPrefix()),
		clientv3.OpGet(common.ShardDDLPessimismOperationKeyAdapter.Encode(task), clientv3.WithPrefix())))
	if err != nil {
		return nil, nil, 0, err
	}
	infoResp := respTxn.Responses[0].GetResponseRange()
	opsResp := respTxn.Responses[1].GetResponseRange()
	var (
		infos = make([]Info, 0, len(infoResp.Kvs))
		ops   = make([]Operation, 0, len(opsResp.Kvs))
	)
	for _, kv := range infoResp.Kvs {
		info, err2 := infoFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, nil, 0, err2
		}
		infos = append(infos, info)
	}
	for _, kv := range opsResp.Kvs {
		op, err2 := operationFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, nil, 0, err2
		}
		ops = append(ops, op)
	}
	return infos, ops, respTxn.Header.Revision, nil
}

// WatchOperationPut watches PUT operations for DDL lock operation.
// If want to watch all operations, pass empty string for `task` and `source`.
// This function can be called by DM-worker and DM-master.
// TODO(csuzhangxc): report error and do some retry.
func WatchOperationPut(ctx context.Context, cli *clientv3.Client, task, source string, revision int64, outCh chan<- Operation, errCh chan<- error) {
	watchOperation(ctx, cli, mvccpb.PUT, task, source, revision, outCh, errCh)
}

// WatchOperationDelete watches DELETE operations for DDL lock operation.
// If want to watch all operations, pass empty string for `task` and `source`.
// This function is often called by DM-worker.
func WatchOperationDelete(ctx context.Context, cli *clientv3.Client, task, source string, revision int64, outCh chan<- Operation, errCh chan<- error) {
	watchOperation(ctx, cli, mvccpb.DELETE, task, source, revision, outCh, errCh)
}

// watchOperation watches PUT or DELETE operations for DDL lock operation.
func watchOperation(ctx context.Context, cli *clientv3.Client, watchType mvccpb.Event_EventType,
	task, source string, revision int64,
	outCh chan<- Operation, errCh chan<- error,
) {
	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var ch clientv3.WatchChan
	// caller may use empty keys to expect a prefix watch
	if source == "" {
		ch = cli.Watch(wCtx, common.ShardDDLPessimismOperationKeyAdapter.Path(), clientv3.WithPrefix(),
			clientv3.WithRev(revision), clientv3.WithPrevKV())
	} else {
		ch = cli.Watch(wCtx, common.ShardDDLPessimismOperationKeyAdapter.Encode(task, source),
			clientv3.WithRev(revision), clientv3.WithPrevKV())
	}

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
				var (
					op  Operation
					err error
				)

				if ev.Type == mvccpb.PUT && watchType == mvccpb.PUT {
					op, err = operationFromJSON(string(ev.Kv.Value))
				} else if ev.Type == mvccpb.DELETE && watchType == mvccpb.DELETE {
					op, err = operationFromJSON(string(ev.PrevKv.Value))
					op.IsDeleted = true
				}

				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
						return
					}
				} else if op.Task != "" { // valid operation got.
					select {
					case outCh <- op:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// putOperationOp returns a PUT etcd operation for Operation.
// This operation should often be sent by DM-worker.
func putOperationOp(o Operation) (clientv3.Op, error) {
	value, err := o.toJSON()
	if err != nil {
		return clientv3.Op{}, err
	}
	key := common.ShardDDLPessimismOperationKeyAdapter.Encode(o.Task, o.Source)

	return clientv3.OpPut(key, value), nil
}

// deleteOperationOp returns a DELETE etcd operation for Operation.
func deleteOperationOp(op Operation) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLPessimismOperationKeyAdapter.Encode(op.Task, op.Source))
}

func getOperationOp(op Operation) clientv3.Op {
	return clientv3.OpGet(common.ShardDDLPessimismOperationKeyAdapter.Encode(op.Task, op.Source))
}
