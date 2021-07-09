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
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/orchestrator/util"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	txnStaleDuration = time.Second * 30
)

// EtcdWorkerTxn is the struct for Etcd metadata used to record individual
// Etcd big transactions.
type EtcdWorkerTxn struct {
	StartPhysicalTs time.Time `json:"start_physical_ts"`
	StartRevision   int64     `json:"start_revision"`
	Committed       bool      `json:"committed"`

	// for internal use only
	txnRevision int64
}

// EtcdWorkerLock is the struct for Etcd metadata used to record locks used
// to implement Etcd big transactions.
type EtcdWorkerLock struct {
	StartRevision int64  `json:"start_revision"`
	OwnerID       int64  `json:"owner_id"`
	OldValue      []byte `json:"old_value"`

	// for internal use only
	heapIndex    int
	lockRevision int64
}

type txnObserver struct {
	heap    lockHeap
	lockMap map[util.EtcdKey]*EtcdWorkerLock
	TxnMap  map[int64]*EtcdWorkerTxn
	prefix  util.EtcdPrefix
}

func newTxnObserver(prefix util.EtcdPrefix) *txnObserver {
	return &txnObserver{
		heap:    make([]*EtcdWorkerLock, 0, 16),
		lockMap: make(map[util.EtcdKey]*EtcdWorkerLock),
		TxnMap:  make(map[int64]*EtcdWorkerTxn),
		prefix:  prefix,
	}
}

func (o *txnObserver) addLock(modifiedKey util.EtcdKey, lock *EtcdWorkerLock) {
	heap.Push(&o.heap, lock)
	o.lockMap[modifiedKey] = lock
}

func (o *txnObserver) deleteLock(modifiedKey util.EtcdKey) {
	lock, ok := o.lockMap[modifiedKey]
	if !ok {
		log.Warn("Lock not found", zap.String("key", modifiedKey.String()))
		return
	}
	heap.Remove(&o.heap, lock.heapIndex)
	delete(o.lockMap, modifiedKey)
}

func (o *txnObserver) minLockRevision() int64 {
	if o.heap.Len() == 0 {
		return math.MaxInt64
	}
	return o.heap[0].StartRevision
}

func (o *txnObserver) upsertTxn(ownerID int64, txn *EtcdWorkerTxn) {
	o.TxnMap[ownerID] = txn
}

func (o *txnObserver) deleteTxn(ownerID int64) {
	if _, ok := o.TxnMap[ownerID]; !ok {
		log.Panic("Txn not found", zap.Int64("owner-id", ownerID))
	}

	delete(o.TxnMap, ownerID)
}

func (o *txnObserver) cleanUpOrphanTxns(ctx context.Context, client *clientv3.Client) error {
	txnMapClone := make(map[int64]*EtcdWorkerTxn, len(o.TxnMap))
	for k, v := range o.TxnMap {
		if time.Since(v.StartPhysicalTs) > time.Second*2 {
			txnMapClone[k] = v
		}
	}

	// Deletes non-orphan txns from txnMapClone
	for _, lock := range o.lockMap {
		delete(txnMapClone, lock.OwnerID)
	}

	// What remains now is the orphan txns.
	for ownerID, txn := range txnMapClone {
		log.Debug("Cleaning up orphan txn",
			zap.Int64("owner-id", ownerID),
			zap.Int64("revision", txn.txnRevision))

		txnKey := txnKeyFromOwnerID(o.prefix, ownerID)
		resp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(txnKey.String()), "<", txn.txnRevision+1)).
			Then(clientv3.OpDelete(txnKey.String())).
			Commit()
		if err != nil {
			return errors.Trace(err)
		}
		if resp.Succeeded {
			log.Debug("Cleaned up orphan txn", zap.Int64("owner-id", ownerID))
		} else {
			log.Warn("Failed to clean up orphan txn", zap.Int64("owner-id", ownerID))
		}
	}

	return nil
}

type cleanUpLockOp int

const (
	lockNoOp = cleanUpLockOp(iota)
	lockOpRollBack
	lockOpRollForward
)

func (o *txnObserver) cleanUpLocks(ctx context.Context, client *clientv3.Client) error {
	forceCleanUp := false
	failpoint.Inject("forceRollForwardByOthers", func() {
		forceCleanUp = true
	})
	op := lockNoOp
	for key, lock := range o.lockMap {
		txn, ok := o.TxnMap[lock.OwnerID]
		if !ok {
			op = lockOpRollBack
		} else if forceCleanUp || time.Since(txn.StartPhysicalTs) > txnStaleDuration {
			if txn.Committed {
				op = lockOpRollForward
			} else if forceCleanUp {
				// this is for unit testing only
				op = lockOpRollBack
			}
		}

		lockKey := lockKeyFromDataKey(o.prefix, key)
		switch op {
		case lockOpRollBack:
			log.Debug("etcd big txn: lock rollback",
				zap.String("lock-key", lockKey.String()),
				zap.Int64("owner-id", lock.OwnerID))
			oldValue := lock.OldValue
			var op clientv3.Op
			if oldValue != nil {
				op = clientv3.OpPut(key.String(), string(oldValue))
			} else {
				op = clientv3.OpDelete(key.String())
			}

			txnResp, err := client.
				Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(lockKey.String()), "<", lock.lockRevision+1),
					clientv3.Compare(clientv3.ModRevision(key.String()), "<", lock.lockRevision+1)).
				Then(op, clientv3.OpDelete(lockKey.String())).
				Commit()
			if err != nil {
				return errors.Trace(err)
			}
			if !txnResp.Succeeded {
				log.Warn("Failed to roll back etcd key", zap.String("key", key.String()))
			}
		case lockOpRollForward:
			log.Debug("etcd big txn: lock roll-forward",
				zap.String("lock-key", lockKey.String()),
				zap.Int64("owner-id", lock.OwnerID))
			txnResp, err := client.
				Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(lockKey.String()), "<", lock.lockRevision+1)).
				Then(clientv3.OpDelete(lockKey.String())).
				Commit()
			if err != nil {
				return errors.Trace(err)
			}
			if !txnResp.Succeeded {
				log.Warn("Failed to roll forward etcd key", zap.String("key", key.String()))
			}
		}
	}

	return nil
}

func lockKeyFromDataKey(prefix util.EtcdPrefix, dataKey util.EtcdKey) util.EtcdKey {
	relKey := dataKey.RemovePrefix(&prefix)
	return prefix.FullKey(util.NewEtcdRelKey(fmt.Sprintf("%s%s", etcdBigTxnLockPrefix, relKey.String())))
}

func txnKeyFromOwnerID(prefix util.EtcdPrefix, ownerID int64) util.EtcdKey {
	return prefix.FullKey(util.NewEtcdRelKey(fmt.Sprintf("%s/%d", etcdBigTxnMetaPrefix, ownerID)))
}

type txnChangeSetEntry struct {
	key util.EtcdKey
	old []byte
	new []byte
}

type txnChangeSet []*txnChangeSetEntry

func newTxnChangeSet(changeSet map[util.EtcdKey][]byte, prevState map[util.EtcdKey][]byte) txnChangeSet {
	var ret []*txnChangeSetEntry
	for key, value := range changeSet {
		prev := prevState[key]
		ret = append(ret, &txnChangeSetEntry{
			key: key,
			old: prev,
			new: value,
		})
	}
	return ret
}

func (s txnChangeSet) sort() {
	sort.Slice(s, func(i, j int) bool {
		a := s[i].key.Bytes()
		b := s[j].key.Bytes()
		return bytes.Compare(a, b) == -1
	})
}

func (s txnChangeSet) stats() (numEntries, numBytes int) {
	numEntries = len(s)
	for _, entry := range s {
		numBytes += len(entry.new)
	}
	return
}

type bigTxnUndoLogEntry struct {
	Key      util.EtcdKey
	LockKey  util.EtcdKey
	PreValue []byte
	PostRev  int64
	IsDelete bool
}

func (e *bigTxnUndoLogEntry) Ops() []clientv3.Op {
	var ops []clientv3.Op
	if e.PreValue != nil {
		ops = append(ops, clientv3.OpPut(e.Key.String(), string(e.PreValue)))
	} else {
		ops = append(ops, clientv3.OpDelete(e.Key.String()))
	}

	ops = append(ops, clientv3.OpDelete(e.LockKey.String()))
	return ops
}

func (e *bigTxnUndoLogEntry) Cmps() []clientv3.Cmp {
	var cmps []clientv3.Cmp
	if e.IsDelete {
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(e.Key.String()), "=", 0))
	} else {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(e.Key.String()), "=", e.PostRev))
	}

	cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(e.LockKey.String()), "=", e.PostRev))
	return cmps
}

type lockHeap []*EtcdWorkerLock

func (l lockHeap) Len() int {
	return len(l)
}

func (l lockHeap) Less(i, j int) bool {
	return l[i].StartRevision < l[j].StartRevision
}

func (l lockHeap) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
	l[i].heapIndex = i
	l[j].heapIndex = j
}

func (l *lockHeap) Push(x interface{}) {
	x.(*EtcdWorkerLock).heapIndex = len(*l)
	*l = append(*l, x.(*EtcdWorkerLock))
}

func (l *lockHeap) Pop() interface{} {
	old := *l
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*l = old[0 : n-1]
	x.heapIndex = -1
	return x
}
