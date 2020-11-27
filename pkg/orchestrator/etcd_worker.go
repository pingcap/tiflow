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

package orchestrator

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

type EtcdWorker struct {
	client *etcd.Client
	reactor Reactor
	state ReactorState
	rawState map[string][]byte
	prefix string
	revision int64
}

func NewEtcdWorker(client *etcd.Client, reactor Reactor, initState ReactorState) (*EtcdWorker, error) {
	return &EtcdWorker{
		client:  client,
		reactor: reactor,
		state: initState,
	}, nil
}

func (worker *EtcdWorker) Run(ctx context.Context, prefix string) error {
	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()

	watchCh := worker.client.Watch(ctx1, prefix, clientv3.WithPrefix())
	for {
		var response clientv3.WatchResponse
		select {
		case <-ctx.Done():
			return ctx.Err()
		case response = <-watchCh:
		}

		if err := response.Err() ; err != nil {
			return errors.Trace(err)
		}

		if response.IsProgressNotify() {
			continue
		}

		if worker.revision >= response.Header.GetRevision() {
			continue
		}
		worker.revision = response.Header.GetRevision()

		isUpdated := false
		for _, event := range response.Events {
			err := worker.handleEvent(ctx, event)
			if err != nil {
				return errors.Trace(err)
			}
			isUpdated = true
		}

		if isUpdated {
			nextState, err := worker.reactor.Tick(ctx, worker.state)
			if err != nil {
				return errors.Trace(err)
			}

			err = worker.applyUpdates(ctx, nextState.GetPatches())
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (worker *EtcdWorker) handleEvent(_ context.Context, event *clientv3.Event) error {
	switch event.Type {
	case mvccpb.PUT:
		worker.state.Update(event.Kv.Key, event.Kv.Value)
		worker.rawState[string(event.Kv.Key)] = event.Kv.Value
	case mvccpb.DELETE:
		worker.state.Update(event.Kv.Key, nil)
		delete(worker.rawState, string(event.Kv.Key))
	}
	return nil
}

func (worker *EtcdWorker) applyUpdates(ctx context.Context, patches []*DataPatch) error {
	for {
		cmps := make([]clientv3.Cmp, 0)
		ops := make([]clientv3.Op, 0)

		for _, patch := range patches {
			old, ok := worker.rawState[string(patch.key)]

			// make sure someone else has not updated the key after the last snapshot
			if ok {
				cmp := clientv3.Compare(clientv3.ModRevision(string(patch.key)), "<=", worker.revision)
				cmps = append(cmps, cmp)
			}

			value, err := patch.fun(old)
			if err != nil {
				if _, ok := errors.Cause(err).(*EtcdTryAgain); ok {
					continue
				}
				return errors.Trace(err)
			}

			var op clientv3.Op
			if value != nil {
			 	op = clientv3.OpPut(string(patch.key), string(value))
			} else {
				op = clientv3.OpDelete(string(patch.key))
			}
			ops = append(ops, op)
		}

		resp, err := worker.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
		if err != nil {
			return errors.Trace(err)
		}

		worker.revision = resp.Header.GetRevision()
		if resp.Succeeded {
			for _, op := range ops {
				if op.IsPut() {
					worker.rawState[string(op.KeyBytes())] = op.ValueBytes()
				} else if op.IsDelete() {
					delete(worker.rawState, string(op.KeyBytes()))
				}
			}
			return nil
		}

		getResp, err := worker.client.Get(ctx, worker.prefix, clientv3.WithPrefix())


		if err != nil {
			return errors.Trace(err)
		}
	}
}
