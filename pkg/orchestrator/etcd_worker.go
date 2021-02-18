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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// EtcdWorker handles all interactions with Etcd
type EtcdWorker struct {
	client  *etcd.Client
	reactor Reactor
	state   ReactorState
	// rawState is the local cache of the latest Etcd state.
	rawState map[util.EtcdKey][]byte
	// pendingUpdates stores updates initiated by the Reactor that have not yet been uploaded to Etcd.
	pendingUpdates []*etcdUpdate
	// revision is the Etcd revision of the latest event received from Etcd
	// (which has not necessarily been applied to the ReactorState)
	revision int64
	// reactor.Tick() should not be called until revision >= barrierRev.
	barrierRev int64
	// prefix is the scope of Etcd watch
	prefix util.EtcdPrefix
}

type etcdUpdate struct {
	key      util.EtcdKey
	value    []byte
	revision int64
}

// NewEtcdWorker returns a new EtcdWorker
func NewEtcdWorker(client *etcd.Client, prefix string, reactor Reactor, initState ReactorState) (*EtcdWorker, error) {
	return &EtcdWorker{
		client:     client,
		reactor:    reactor,
		state:      initState,
		rawState:   make(map[util.EtcdKey][]byte),
		prefix:     util.NormalizePrefix(prefix),
		barrierRev: -1, // -1 indicates no barrier
	}, nil
}

// Run starts the EtcdWorker event loop.
// A tick is generated either on a timer whose interval is timerInterval, or on an Etcd event.
// If the specified etcd session is Done, this Run function will exit with cerrors.ErrEtcdSessionDone.
// And the specified etcd session is nil-safty.
func (worker *EtcdWorker) Run(ctx context.Context, session *concurrency.Session, timerInterval time.Duration) error {
	defer worker.cleanUp()

	err := worker.syncRawState(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()

	ticker := time.NewTicker(timerInterval)
	defer ticker.Stop()

	watchCh := worker.client.Watch(ctx1, worker.prefix.String(), clientv3.WithPrefix())
	var (
		pendingPatches []*DataPatch
		exiting        bool
		sessionDone    <-chan struct{}
	)
	if session != nil {
		sessionDone = session.Done()
	} else {
		// should never be closed
		sessionDone = make(chan struct{})
	}

	for {
		var response clientv3.WatchResponse
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sessionDone:
			return cerrors.ErrEtcdSessionDone.GenWithStackByArgs()
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
				if cerrors.ErrEtcdTryAgain.Equal(errors.Cause(err)) {
					continue
				}
				return errors.Trace(err)
			}
			// If we are here, all patches have been successfully applied to Etcd.
			// `applyPatches` is all-or-none, so in case of success, we should clear all the pendingPatches.
			pendingPatches = pendingPatches[:0]
		} else {
			if exiting {
				// If exiting is true here, it means that the reactor returned `ErrReactorFinished` last tick, and all pending patches is applied.
				return nil
			}
			if worker.revision < worker.barrierRev {
				// We hold off notifying the Reactor because barrierRev has not been reached.
				// This usually happens when a committed write Txn has not been received by Watch.
				continue
			}

			// We are safe to update the ReactorState only if there is no pending patch.
			if err := worker.applyUpdates(); err != nil {
				return errors.Trace(err)
			}

			nextState, err := worker.reactor.Tick(ctx, worker.state)
			if err != nil {
				if !cerrors.ErrReactorFinished.Equal(errors.Cause(err)) {
					return errors.Trace(err)
				}
				// normal exit
				exiting = true
			}
			worker.state = nextState
			pendingPatches = append(pendingPatches, nextState.GetPatches()...)
		}
	}
}

func (worker *EtcdWorker) handleEvent(_ context.Context, event *clientv3.Event) error {
	worker.pendingUpdates = append(worker.pendingUpdates, &etcdUpdate{
		key:      util.NewEtcdKeyFromBytes(event.Kv.Key),
		value:    event.Kv.Value,
		revision: event.Kv.ModRevision,
	})

	switch event.Type {
	case mvccpb.PUT:
		worker.rawState[util.NewEtcdKeyFromBytes(event.Kv.Key)] = event.Kv.Value
	case mvccpb.DELETE:
		delete(worker.rawState, util.NewEtcdKeyFromBytes(event.Kv.Key))
	}
	return nil
}

func (worker *EtcdWorker) syncRawState(ctx context.Context) error {
	resp, err := worker.client.Get(ctx, worker.prefix.String(), clientv3.WithPrefix())
	if err != nil {
		return errors.Trace(err)
	}

	worker.rawState = make(map[util.EtcdKey][]byte)
	for _, kv := range resp.Kvs {
		key := util.NewEtcdKeyFromBytes(kv.Key)
		worker.rawState[key] = kv.Value
		err := worker.state.Update(key, kv.Value, true)
		if err != nil {
			return errors.Trace(err)
		}
	}

	worker.revision = resp.Header.Revision
	return nil
}

func (worker *EtcdWorker) applyPatches(ctx context.Context, patches []*DataPatch) error {
	cmpSet := make(map[util.EtcdKey]clientv3.Cmp)
	opSet := make(map[util.EtcdKey]clientv3.Op)

	// tmpRawState cache the changed values which are uncommitted to etcd.
	tmpRawState := make(map[util.EtcdKey][]byte)
	for _, patch := range patches {
		// check the tmpRawState to get the newest uncommitted value if exists
		old, ok := tmpRawState[patch.Key]
		if !ok {
			// get the committed value
			old, ok = worker.rawState[patch.Key]
		}

		value, err := patch.Fun(old)
		if err != nil {
			if cerrors.ErrEtcdIgnore.Equal(errors.Cause(err)) {
				continue
			}
			return errors.Trace(err)
		}

		// if there are multiple patches with the same key, we should keep the compare of the first patch
		if _, cmpExist := cmpSet[patch.Key]; !cmpExist {
			// make sure someone else has not updated the key after the last snapshot
			var cmp clientv3.Cmp
			// if ok is false, it means that the key of this patch is not exist in a committed state
			// and also not exist in the uncommitted value cache(tmpRawState)
			if ok {
				cmp = clientv3.Compare(clientv3.ModRevision(patch.Key.String()), "<", worker.revision+1)
			} else {
				// this compare is equivalent to `patch.Key` is not exist
				cmp = clientv3.Compare(clientv3.ModRevision(patch.Key.String()), "=", 0)
			}
			cmpSet[patch.Key] = cmp
		}

		if bytes.Equal(old, value) {
			// Ignore patches that produce a new value that is the same as the old value.
			continue
		}

		var op clientv3.Op
		if value != nil {
			op = clientv3.OpPut(patch.Key.String(), string(value))
		} else {
			op = clientv3.OpDelete(patch.Key.String())
		}
		// if there are multiple patches with the same key, we should keep the value of the last patch
		opSet[patch.Key] = op
		tmpRawState[patch.Key] = value
	}
	cmps := make([]clientv3.Cmp, 0, len(cmpSet))
	ops := make([]clientv3.Op, 0, len(opSet))
	for _, cmp := range cmpSet {
		cmps = append(cmps, cmp)
	}
	for _, op := range opSet {
		ops = append(ops, op)
	}

	resp, err := worker.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
	if err != nil {
		return errors.Trace(err)
	}

	logEtcdOps(ops, resp.Succeeded)
	if resp.Succeeded {
		worker.barrierRev = resp.Header.GetRevision()
		return nil
	}
	return cerrors.ErrEtcdTryAgain.GenWithStackByArgs()
}

func (worker *EtcdWorker) applyUpdates() error {
	for _, update := range worker.pendingUpdates {
		err := worker.state.Update(update.key, update.value, false)
		if err != nil {
			return errors.Trace(err)
		}
	}

	worker.pendingUpdates = worker.pendingUpdates[:0]
	return nil
}

func logEtcdOps(ops []clientv3.Op, commited bool) {
	if log.GetLevel() != zapcore.DebugLevel {
		return
	}
	for _, op := range ops {
		log.Debug("[etcd worker] ==========Update State to ETCD==========")
		if op.IsDelete() {
			log.Debug("[etcd worker] delete key", zap.ByteString("key", op.KeyBytes()))
		} else {
			log.Debug("[etcd worker] put key", zap.ByteString("key", op.KeyBytes()), zap.ByteString("value", op.ValueBytes()))
		}
		log.Debug("[etcd worker] ============State Commit=============", zap.Bool("committed", commited))
	}
}

func (worker *EtcdWorker) cleanUp() {
	worker.rawState = nil
	worker.revision = 0
	worker.pendingUpdates = worker.pendingUpdates[:0]
}
