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
	"bytes"
	"context"
	"log"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
)

// EtcdWorker handles all interactions with Etcd
type EtcdWorker struct {
	client  *etcd.Client
	reactor Reactor
	state   ReactorState
	// rawState is the local cache of the latest Etcd state.
	rawState       map[string][]byte
	pendingUpdates []*etcdUpdate
	// revision is the Etcd revision of the latest event received from Etcd
	// (which has not necessarily been applied to the ReactorState)
	revision int64
	prefix   string
}

type etcdUpdate struct {
	key   []byte
	value []byte
}

// NewEtcdWorker returns a new EtcdWorker
func NewEtcdWorker(client *etcd.Client, prefix string, reactor Reactor, initState ReactorState) (*EtcdWorker, error) {
	return &EtcdWorker{
		client:   client,
		reactor:  reactor,
		state:    initState,
		rawState: make(map[string][]byte),
		prefix:   normalizePrefix(prefix),
	}, nil
}

// Run starts the EtcdWorker event loop.
// A tick is generated either on a timer whose interval is timerInterval, or on an Etcd event.
func (worker *EtcdWorker) Run(ctx context.Context, timerInterval time.Duration) error {
	defer worker.cleanUp()

	err := worker.syncRawState(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()

	ticker := time.NewTicker(timerInterval)
	defer ticker.Stop()

	watchCh := worker.client.Watch(ctx1, worker.prefix, clientv3.WithPrefix())
	var pendingPatches []*DataPatch

	for {
		var response clientv3.WatchResponse
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// There is no new event to handle on timer ticks, so we have nothing here.
		case response = <-watchCh:
			// In this select case, we receive new events from Etcd, and call handleEvent if appropriate.

			if err := response.Err(); err != nil {
				return errors.Trace(err)
			}

			// ProgressNotify implies no new events.
			if response.IsProgressNotify() {
				continue
			}

			// Check whether the response is stale.
			if worker.revision >= response.Header.GetRevision() {
				continue
			}
			worker.revision = response.Header.GetRevision()

			for _, event := range response.Events {
				// handleEvent will apply the event to our internal `rawState`.
				err := worker.handleEvent(ctx, event)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}

		if len(pendingPatches) > 0 {
			// Here we have some patches yet to be uploaded to Etcd.
			err := worker.applyPatches(ctx, pendingPatches)
			if err != nil {
				if errors.Cause(err) == ErrEtcdTryAgain {
					continue
				}
				return errors.Trace(err)
			}
			// If we are here, all patches have been successfully applied to Etcd.
			// `applyPatches` is all-or-none, so in case of success, we should clear all the pendingPatches.
			pendingPatches = pendingPatches[:0]
		} else {
			// We are safe to update the ReactorState only if there is no pending patch.
			for _, update := range worker.pendingUpdates {
				worker.state.Update(worker.removePrefixBytes(update.key), update.value)
			}

			worker.pendingUpdates = worker.pendingUpdates[:0]
			nextState, err := worker.reactor.Tick(ctx, worker.state)
			if err != nil {
				if errors.Cause(err) == ErrReactorFinished {
					// normal exit
					return nil
				}
				return errors.Trace(err)
			}
			worker.state = nextState
			pendingPatches = nextState.GetPatches()
		}
	}
}

func (worker *EtcdWorker) handleEvent(_ context.Context, event *clientv3.Event) error {
	switch event.Type {
	case mvccpb.PUT:
		worker.pendingUpdates = append(worker.pendingUpdates, &etcdUpdate{
			key:   event.Kv.Key,
			value: event.Kv.Value,
		})
		worker.rawState[string(event.Kv.Key)] = event.Kv.Value
	case mvccpb.DELETE:
		worker.pendingUpdates = append(worker.pendingUpdates, &etcdUpdate{
			key:   event.Kv.Key,
			value: event.Kv.Value,
		})
		delete(worker.rawState, string(event.Kv.Key))
	}
	return nil
}

func (worker *EtcdWorker) syncRawState(ctx context.Context) error {
	resp, err := worker.client.Get(ctx, worker.prefix, clientv3.WithPrefix())
	if err != nil {
		return errors.Trace(err)
	}

	worker.rawState = make(map[string][]byte)
	for _, kv := range resp.Kvs {
		worker.rawState[string(kv.Key)] = kv.Value
	}

	worker.revision = resp.Header.Revision
	return nil
}

func (worker *EtcdWorker) applyPatches(ctx context.Context, patches []*DataPatch) error {
	for {
		cmps := make([]clientv3.Cmp, 0)
		ops := make([]clientv3.Op, 0)

		for _, patch := range patches {
			old, ok := worker.rawState[worker.prefix+string(patch.Key)]

			// make sure someone else has not updated the key after the last snapshot
			if ok {
				cmp := clientv3.Compare(clientv3.ModRevision(worker.prefix+string(patch.Key)), "<", worker.revision+1)
				cmps = append(cmps, cmp)
			}

			value, err := patch.Fun(old)

			if err != nil {
				if errors.Cause(err) == ErrEtcdIgnore {
					continue
				}
				return errors.Trace(err)
			}

			var op clientv3.Op
			if value != nil {
				op = clientv3.OpPut(worker.prefix+string(patch.Key), string(value))
			} else {
				op = clientv3.OpDelete(worker.prefix + string(patch.Key))
			}
			ops = append(ops, op)
		}

		resp, err := worker.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
		if err != nil {
			return errors.Trace(err)
		}

		if resp.Succeeded {
			worker.revision = resp.Header.GetRevision()
			for _, op := range ops {
				if op.IsPut() {
					worker.rawState[string(op.KeyBytes())] = op.ValueBytes()
					worker.pendingUpdates = append(worker.pendingUpdates, &etcdUpdate{
						key:   op.KeyBytes(),
						value: op.ValueBytes(),
					})
				} else if op.IsDelete() {
					delete(worker.rawState, string(op.KeyBytes()))
					worker.pendingUpdates = append(worker.pendingUpdates, &etcdUpdate{
						key:   op.KeyBytes(),
						value: nil,
					})
				}
			}
			return nil
		}

		return ErrEtcdTryAgain
	}
}

func (worker *EtcdWorker) cleanUp() {
	worker.rawState = nil
	worker.revision = 0
	worker.pendingUpdates = worker.pendingUpdates[:0]
}

func (worker *EtcdWorker) removePrefixBytes(key []byte) []byte {
	if bytes.Index(key, []byte(worker.prefix)) != 0 {
		log.Panic("prefix not found", zap.ByteString("key", key), zap.String("prefix", worker.prefix))
	}
	return bytes.TrimPrefix(key, []byte(worker.prefix))
}

// normalizePrefix adds a slash to the beginning of `prefix` if none is present,
// and removes a trailing slash, if one is present.
func normalizePrefix(prefix string) string {
	ret := prefix
	if !strings.HasPrefix(prefix, "/") {
		ret = "/" + prefix
	}
	return strings.TrimSuffix(ret, "/")
}
