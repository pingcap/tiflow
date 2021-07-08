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

package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
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
	client     *etcd.Client
	reactor    Reactor
	state      ReactorState
	txnManager *txnObserver
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
	// clientID is the unique ID for this client in this Etcd cluster.
	clientID int64

	// for testing only
	forceBigTxn bool
}

type etcdUpdate struct {
	key      util.EtcdKey
	value    []byte
	revision int64
}

// NewEtcdWorker returns a new EtcdWorker
func NewEtcdWorker(client *etcd.Client, prefix string, reactor Reactor, initState ReactorState) (*EtcdWorker, error) {
	prefixNormalied := util.NormalizePrefix(prefix)
	return &EtcdWorker{
		client:      client,
		reactor:     reactor,
		state:       initState,
		rawState:    make(map[util.EtcdKey][]byte),
		prefix:      prefixNormalied,
		barrierRev:  -1, // -1 indicates no barrier
		txnManager:  newTxnObserver(prefixNormalied),
		forceBigTxn: false,
	}, nil
}

const (
	etcdRequestProgressDuration = 2 * time.Second
	etcdCleanUpBigTxnInterval   = 2 * time.Second
	etcdBigTxnMetaPrefix        = "/meta-txn"
	etcdBigTxnLockPrefix        = "/meta-lock"
)

// Run starts the EtcdWorker event loop.
// A tick is generated either on a timer whose interval is timerInterval, or on an Etcd event.
// If the specified etcd session is Done, this Run function will exit with cerrors.ErrEtcdSessionDone.
// And the specified etcd session is nil-safty.
func (worker *EtcdWorker) Run(ctx context.Context, session *concurrency.Session, timerInterval time.Duration) error {
	defer worker.cleanUp()

	if session == nil {
		var err error
		session, err = concurrency.NewSession(worker.client.Unwrap())
		if err != nil {
			return errors.Trace(err)
		}
		defer func() {
			_ = session.Close()
		}()
	}
	worker.clientID = int64(session.Lease())

	err := worker.syncRawState(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	ctx1, cancel := context.WithCancel(ctx)
	defer cancel()

	ticker := time.NewTicker(timerInterval)
	defer ticker.Stop()

	watchCh := worker.client.Watch(ctx1, worker.prefix.String(), clientv3.WithPrefix(), clientv3.WithRev(worker.revision+1))
	var (
		pendingPatches [][]DataPatch
		exiting        bool
		sessionDone    <-chan struct{}
	)
	if session != nil {
		sessionDone = session.Done()
	} else {
		// should never be closed
		sessionDone = make(chan struct{})
	}
	lastReceivedEventTime := time.Now()
	lastCleanedUpStaleBigTxn := time.Now()
	for {
		var response clientv3.WatchResponse
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sessionDone:
			return cerrors.ErrEtcdSessionDone.GenWithStackByArgs()
		case <-ticker.C:
			// There is no new event to handle on timer ticks, so we have nothing here.
			if time.Since(lastReceivedEventTime) > etcdRequestProgressDuration {
				if err := worker.client.RequestProgress(ctx); err != nil {
					log.Warn("failed to request progress for etcd watcher", zap.Error(err))
				}
			}
			// Checking for orphan txns is not very costly so we can do it in every tick.
			err = worker.txnManager.cleanUpOrphanTxns(ctx, worker.client.Unwrap())
			if err != nil {
				return errors.Trace(err)
			}
			if time.Since(lastCleanedUpStaleBigTxn) > etcdCleanUpBigTxnInterval {
				err := worker.txnManager.cleanUpLocks(ctx, worker.client.Unwrap())
				if err != nil {
					return errors.Trace(err)
				}
				lastCleanedUpStaleBigTxn = time.Now()
			}
		case response = <-watchCh:
			// In this select case, we receive new events from Etcd, and call handleEvent if appropriate.

			if err := response.Err(); err != nil {
				return errors.Trace(err)
			}
			lastReceivedEventTime = time.Now()

			// Check whether the response is stale.
			if worker.revision >= response.Header.GetRevision() {
				continue
			}
			worker.revision = response.Header.GetRevision()

			// ProgressNotify implies no new events.
			if response.IsProgressNotify() {
				continue
			}

			for _, event := range response.Events {
				// handleEvent will apply the event to our internal `rawState`.
				err := worker.handleEvent(ctx, event)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}

		if worker.revision < worker.barrierRev {
			// We hold off notifying the Reactor because barrierRev has not been reached.
			// This usually happens when a committed write Txn has not been received by Watch.
			continue
		}

		if worker.revision >= worker.txnManager.minLockRevision() {
			log.Debug("waiting for big txn to finish", zap.Int("num-locks", len(worker.txnManager.lockMap)))
			continue
		}

		if len(pendingPatches) > 0 {
			// Here we have some patches yet to be uploaded to Etcd.
			pendingPatches, err = worker.applyPatchGroups(ctx, pendingPatches)
			if err != nil {
				if cerrors.ErrEtcdTryAgain.Equal(errors.Cause(err)) {
					continue
				}
				return errors.Trace(err)
			}
		} else {
			if exiting {
				// If exiting is true here, it means that the reactor returned `ErrReactorFinished` last tick, and all pending patches is applied.
				return nil
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
	txnPrefix := worker.prefix.String() + etcdBigTxnMetaPrefix
	lockPrefix := worker.prefix.String() + etcdBigTxnLockPrefix

	if strings.HasPrefix(string(event.Kv.Key), txnPrefix) {
		ownerIDStr := strings.TrimPrefix(string(event.Kv.Key), txnPrefix+"/")
		ownerID, err := strconv.Atoi(ownerIDStr)
		if err != nil {
			return errors.Trace(err)
		}

		switch event.Type {
		case mvccpb.DELETE:
			worker.txnManager.deleteTxn(int64(ownerID))
		case mvccpb.PUT:
			var txn EtcdWorkerTxn
			err := json.Unmarshal(event.Kv.Value, &txn)
			if err != nil {
				return errors.Trace(err)
			}
			txn.txnRevision = event.Kv.ModRevision
			worker.txnManager.upsertTxn(int64(ownerID), &txn)
		}
	} else if strings.HasPrefix(string(event.Kv.Key), lockPrefix) {
		modifiedKeyStr := strings.TrimPrefix(string(event.Kv.Key), lockPrefix)
		modifiedKey := worker.prefix.FullKey(util.NewEtcdRelKey(modifiedKeyStr))

		switch event.Type {
		case mvccpb.DELETE:
			worker.txnManager.deleteLock(modifiedKey)
		case mvccpb.PUT:
			var lock EtcdWorkerLock
			err := json.Unmarshal(event.Kv.Value, &lock)
			if err != nil {
				return errors.Trace(err)
			}
			lock.lockRevision = event.Kv.ModRevision
			worker.txnManager.addLock(modifiedKey, &lock)
		}
	} else {
		worker.pendingUpdates = append(worker.pendingUpdates, &etcdUpdate{
			key:      util.NewEtcdKeyFromBytes(event.Kv.Key),
			value:    event.Kv.Value,
			revision: event.Kv.ModRevision,
		})

		switch event.Type {
		case mvccpb.PUT:
			value := event.Kv.Value
			if value == nil {
				value = []byte{}
			}
			worker.rawState[util.NewEtcdKeyFromBytes(event.Kv.Key)] = value
		case mvccpb.DELETE:
			delete(worker.rawState, util.NewEtcdKeyFromBytes(event.Kv.Key))
		}
	}
	return nil
}

func (worker *EtcdWorker) syncRawState(ctx context.Context) error {
	txnPrefix := worker.prefix.String() + etcdBigTxnMetaPrefix
	lockPrefix := worker.prefix.String() + etcdBigTxnLockPrefix

	resp, err := worker.client.Get(ctx, worker.prefix.String(), clientv3.WithPrefix())
	if err != nil {
		return errors.Trace(err)
	}

	worker.rawState = make(map[util.EtcdKey][]byte)
	for _, kv := range resp.Kvs {
		key := util.NewEtcdKeyFromBytes(kv.Key)
		worker.rawState[key] = kv.Value

		if strings.HasPrefix(string(kv.Key), txnPrefix) {
			ownerIDStr := strings.TrimPrefix(string(kv.Key), txnPrefix+"/")
			ownerID, err := strconv.Atoi(ownerIDStr)
			if err != nil {
				return errors.Trace(err)
			}

			var txn EtcdWorkerTxn
			err = json.Unmarshal(kv.Value, &txn)
			if err != nil {
				return errors.Trace(err)
			}
			txn.txnRevision = resp.Header.Revision
			worker.txnManager.upsertTxn(int64(ownerID), &txn)
		} else if strings.HasPrefix(string(kv.Key), lockPrefix) {
			modifiedKeyStr := strings.TrimPrefix(string(kv.Key), lockPrefix)
			modifiedKey := worker.prefix.FullKey(util.NewEtcdRelKey(modifiedKeyStr))

			var lock EtcdWorkerLock
			err := json.Unmarshal(kv.Value, &lock)
			if err != nil {
				return errors.Trace(err)
			}
			lock.lockRevision = kv.ModRevision
			worker.txnManager.addLock(modifiedKey, &lock)
		} else {
			err := worker.state.Update(key, kv.Value, true)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	worker.revision = resp.Header.Revision
	return nil
}

func (worker *EtcdWorker) cloneRawState() map[util.EtcdKey][]byte {
	ret := make(map[util.EtcdKey][]byte)
	for k, v := range worker.rawState {
		cloneV := make([]byte, len(v))
		copy(cloneV, v)
		ret[util.NewEtcdKey(k.String())] = cloneV
	}
	return ret
}

func (worker *EtcdWorker) applyPatchGroups(ctx context.Context, patchGroups [][]DataPatch) ([][]DataPatch, error) {
	for len(patchGroups) > 0 {
		patches := patchGroups[0]
		err := worker.applyPatches(ctx, patches)
		if err != nil {
			return patchGroups, err
		}
		patchGroups = patchGroups[1:]
	}
	return patchGroups, nil
}

func (worker *EtcdWorker) applyPatches(ctx context.Context, patches []DataPatch) error {
	if len(patches) == 0 {
		return nil
	}

	state := worker.cloneRawState()
	changedSet := make(map[util.EtcdKey]struct{})
	for _, patch := range patches {
		err := patch.Patch(state, changedSet)
		if err != nil {
			if cerrors.ErrEtcdIgnore.Equal(errors.Cause(err)) {
				continue
			}
			return errors.Trace(err)
		}
	}

	if len(changedSet) == 0 {
		return nil
	}

	newKVMap := make(map[util.EtcdKey][]byte, len(changedSet))
	for key := range changedSet {
		newKVMap[key] = state[key]
	}

	changeSet := newTxnChangeSet(newKVMap, worker.rawState)
	numEntries, numBytes := changeSet.stats()

	useBigTxn := worker.forceBigTxn || numEntries > 128 || numBytes > 1024*512
	failpoint.Inject("injectForceUseBigTxn", func() {
		useBigTxn = true
	})
	if useBigTxn {
		return worker.applyBigTxnPatches(ctx, changeSet)
	}

	cmps := make([]clientv3.Cmp, 0, len(changedSet))
	ops := make([]clientv3.Op, 0, len(changedSet))
	for key := range changedSet {
		// make sure someone else has not updated the key after the last snapshot
		var cmp clientv3.Cmp
		if _, ok := worker.rawState[key]; ok {
			cmp = clientv3.Compare(clientv3.ModRevision(key.String()), "<", worker.revision+1)
		} else {
			// if ok is false, it means that the key of this patch is not exist in a committed state
			// this compare is equivalent to `patch.Key` is not exist
			cmp = clientv3.Compare(clientv3.ModRevision(key.String()), "=", 0)
		}
		cmps = append(cmps, cmp)

		value := state[key]
		var op clientv3.Op
		if value != nil {
			op = clientv3.OpPut(key.String(), string(value))
		} else {
			op = clientv3.OpDelete(key.String())
		}
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

func (worker *EtcdWorker) applyBigTxnPatches(ctx context.Context, changeSet txnChangeSet) error {
	newTxnKey := txnKeyFromOwnerID(worker.prefix, worker.clientID)
	newTxn := &EtcdWorkerTxn{
		StartPhysicalTs: time.Now(),
		StartRevision:   worker.revision,
		Committed:       false,
	}
	newTxnBytes, err := json.Marshal(newTxn)
	if err != nil {
		return errors.Trace(err)
	}
	resp, err := worker.client.
		Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(newTxnKey.String()), "=", 0)).
		Then(clientv3.OpPut(newTxnKey.String(), string(newTxnBytes), clientv3.WithLease(clientv3.LeaseID(worker.clientID)))).
		Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		log.Debug("Conflicting big txn by another EtcdWorker in the same process")
		return cerrors.ErrEtcdTryAgain.FastGenByArgs()
	}
	needRemoveTxnKeyOnFailure := true
	failpoint.Inject("etcdBigTxnFailAfterPutMeta", func() {
		failpoint.Return(cerrors.ErrEtcdMockCrash.GenWithStackByArgs())
	})
	failpoint.Inject("etcdBigTxnPauseAfterPutMeta", func() {})
	if err != nil {
		return errors.Trace(err)
	}
	txnRevision := resp.Header.Revision
	defer func() {
		if !needRemoveTxnKeyOnFailure {
			return
		}
		resp, err := worker.client.Delete(ctx, newTxnKey.String())
		if err != nil {
			log.Warn("failed to clean up big txn entry", zap.Int64("owner-id", worker.clientID))
			return
		}
		worker.barrierRev = resp.Header.Revision
	}()

	var undoLog []*bigTxnUndoLogEntry
	defer func() {
		for _, undoLogEntry := range undoLog {
			resp, err := worker.client.Txn(ctx).If(undoLogEntry.Cmps()...).Then(undoLogEntry.Ops()...).Commit()
			if err != nil {
				log.Warn("Could not undo change", zap.Error(err))
				return
			}
			if !resp.Succeeded {
				log.Warn("Undo Txn failed", zap.Reflect("undo-log", undoLogEntry))
			}
		}
	}()

	changeSet.sort()

	for _, change := range changeSet {
		key := change.key
		value := change.new
		prev := change.old
		var ops []clientv3.Op
		if value != nil {
			ops = append(ops, clientv3.OpPut(key.String(), string(value)))
		} else {
			ops = append(ops, clientv3.OpDelete(key.String()))
		}

		var cmp clientv3.Cmp
		if prev != nil {
			cmp = clientv3.Compare(clientv3.ModRevision(key.String()), "<", worker.revision+1)
		} else {
			cmp = clientv3.Compare(clientv3.ModRevision(key.String()), "=", 0)
		}

		lockKey := lockKeyFromDataKey(worker.prefix, key)
		lock := &EtcdWorkerLock{
			StartRevision: worker.revision,
			OwnerID:       worker.clientID,
		}
		newLockBytes, err := json.Marshal(lock)
		if err != nil {
			return errors.Trace(err)
		}
		ops = append(ops, clientv3.OpPut(lockKey.String(), string(newLockBytes)))

		resp, err := worker.client.
			Txn(ctx).
			If(cmp, clientv3.Compare(clientv3.CreateRevision(lockKey.String()), "=", 0)).
			Then(ops...).
			Commit()
		if err != nil {
			return errors.Trace(err)
		}
		if !resp.Succeeded {
			return cerrors.ErrEtcdTryAgain.GenWithStackByArgs()
		}
		// Txn has been successfully committed
		undoLog = append(undoLog, &bigTxnUndoLogEntry{
			Key:      key,
			LockKey:  lockKey,
			PreValue: prev,
			PostRev:  resp.Header.Revision,
			IsDelete: value == nil,
		})
		failpoint.Inject("etcdBigTxnFailAfterPrewrite", func() {
			failpoint.Return(cerrors.ErrEtcdMockCrash.GenWithStackByArgs())
		})
	}

	failpoint.Inject("etcdBigTxnFailBeforeCommit", func() {
		failpoint.Return(cerrors.ErrEtcdMockCrash.GenWithStackByArgs())
	})
	// Commit the Txn
	newTxn.Committed = true
	newTxnBytes, err = json.Marshal(newTxn)
	if err != nil {
		return errors.Trace(err)
	}

	txnResp, err := worker.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(newTxnKey.String()), "<", txnRevision+1)).
		Then(clientv3.OpPut(newTxnKey.String(), string(newTxnBytes), clientv3.WithLease(clientv3.NoLease))).
		Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if txnResp.Succeeded {
		log.Info("Committing big txn successful")
		logEtcdChangeSet(changeSet, true)
	} else {
		log.Info("Failed to commit big txn", zap.Int64("owner-id", worker.clientID))
		logEtcdChangeSet(changeSet, false)
		return cerrors.ErrEtcdTryAgain.GenWithStackByArgs()
	}

	undoLog = nil

	failpoint.Inject("etcdBigTxnFailAfterCommit", func() {
		needRemoveTxnKeyOnFailure = false
		failpoint.Return(cerrors.ErrEtcdMockCrash.GenWithStackByArgs())
	})

	// Clear the locks
	for _, change := range changeSet {
		key := change.key
		relKey := key.RemovePrefix(&worker.prefix)
		lockKey := worker.prefix.FullKey(util.NewEtcdRelKey(fmt.Sprintf("%s%s", etcdBigTxnLockPrefix, relKey.String())))
		_, err := worker.client.Delete(ctx, lockKey.String())
		if err != nil {
			log.Warn("Could not clean lock", zap.Error(err))
			needRemoveTxnKeyOnFailure = false
		}
	}

	// txnKey will be deleted in the deferred function.
	return nil
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
	if log.GetLevel() != zapcore.DebugLevel || len(ops) == 0 {
		return
	}
	log.Debug("[etcd worker]" +
		" ==========Update State to ETCD==========")
	for _, op := range ops {
		if op.IsDelete() {
			log.Debug("[etcd worker] delete key", zap.ByteString("key", op.KeyBytes()))
		} else {
			log.Debug("[etcd worker] put key", zap.ByteString("key", op.KeyBytes()), zap.ByteString("value", op.ValueBytes()))
		}
	}
	log.Debug("[etcd worker] ============State Commit=============", zap.Bool("committed", commited))
}

func logEtcdChangeSet(changeSet txnChangeSet, committed bool) {
	if log.GetLevel() != zapcore.DebugLevel {
		return
	}
	log.Debug("[etcd worker]" +
		" ==========Update State to ETCD==========")
	for _, change := range changeSet {
		if change.new == nil {
			log.Debug("[etcd worker] delete key", zap.ByteString("key", change.key.Bytes()))
		} else {
			log.Debug("[etcd worker] put key", zap.ByteString("key", change.key.Bytes()), zap.ByteString("value", change.new))
		}
	}
	log.Debug("[etcd worker] ============State Commit=============", zap.Bool("committed", committed))
}

func (worker *EtcdWorker) cleanUp() {
	worker.rawState = nil
	worker.revision = 0
	worker.pendingUpdates = worker.pendingUpdates[:0]
}
