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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/chdelay"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/orchestrator/util"
	pkgutil "github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
)

const (
	// When EtcdWorker commits a txn to etcd or ticks its reactor
	// takes more than etcdWorkerLogsWarnDuration, it will print a log
	etcdWorkerLogsWarnDuration = 1 * time.Second
	deletionCounterKey         = "/meta/ticdc-delete-etcd-key-count"
	changefeedInfoKey          = "/changefeed/info"
)

// EtcdWorker handles all interactions with Etcd
type EtcdWorker struct {
	client  *etcd.Client
	reactor Reactor
	state   ReactorState
	// rawState is the local cache of the latest Etcd state.
	rawState map[util.EtcdKey]rawStateEntry
	// pendingUpdates stores Etcd updates that the Reactor has not been notified of.
	pendingUpdates []*etcdUpdate
	// revision is the Etcd revision of the latest event received from Etcd
	// (which has not necessarily been applied to the ReactorState)
	revision int64
	// reactor.Tick() should not be called until revision >= barrierRev.
	barrierRev int64
	// prefix is the scope of Etcd watch
	prefix util.EtcdPrefix
	// deleteCounter maintains a local copy of a value stored in Etcd used to
	// keep track of how many deletes have been committed by an EtcdWorker
	// watching this key prefix.
	// This mechanism is necessary as a workaround to correctly detect
	// write-write conflicts when at least a transaction commits a delete,
	// because deletes in Etcd reset the mod-revision of keys, making it
	// difficult to use it as a unique version identifier to implement
	// a `compare-and-swap` semantics, which is essential for implementing
	// snapshot isolation for Reactor ticks.
	deleteCounter int64

	metrics *etcdWorkerMetrics
}

type etcdWorkerMetrics struct {
	// kv events related metrics
	metricEtcdTxnSize            prometheus.Observer
	metricEtcdTxnDuration        prometheus.Observer
	metricEtcdWorkerTickDuration prometheus.Observer
}

type etcdUpdate struct {
	key      util.EtcdKey
	value    []byte
	revision int64
}

// rawStateEntry stores the latest version of a key as seen by the EtcdWorker.
// modRevision is stored to implement `compare-and-swap` semantics more reliably.
type rawStateEntry struct {
	value       []byte
	modRevision int64
}

// NewEtcdWorker returns a new EtcdWorker
func NewEtcdWorker(client *etcd.Client, prefix string, reactor Reactor, initState ReactorState) (*EtcdWorker, error) {
	return &EtcdWorker{
		client:     client,
		reactor:    reactor,
		state:      initState,
		rawState:   make(map[util.EtcdKey]rawStateEntry),
		prefix:     util.NormalizePrefix(prefix),
		barrierRev: -1, // -1 indicates no barrier
	}, nil
}

func (worker *EtcdWorker) initMetrics() {
	metrics := &etcdWorkerMetrics{}
	metrics.metricEtcdTxnSize = etcdTxnSize
	metrics.metricEtcdTxnDuration = etcdTxnExecDuration
	metrics.metricEtcdWorkerTickDuration = etcdWorkerTickDuration
	worker.metrics = metrics
}

// Run starts the EtcdWorker event loop.
// A tick is generated either on a timer whose interval is timerInterval, or on an Etcd event.
// If the specified etcd session is Done, this Run function will exit with cerrors.ErrEtcdSessionDone.
// And the specified etcd session is nil-safety.
func (worker *EtcdWorker) Run(ctx context.Context, session *concurrency.Session, timerInterval time.Duration, role string) error {
	defer worker.cleanUp()
	worker.initMetrics()

	err := worker.syncRawState(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	ticker := time.NewTicker(timerInterval)
	defer ticker.Stop()

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	watchCh := worker.client.Watch(watchCtx, worker.prefix.String(), role, clientv3.WithPrefix(), clientv3.WithRev(worker.revision+1))

	if role == pkgutil.RoleProcessor.String() {
		failpoint.Inject("ProcessorEtcdDelay", func() {
			delayer := chdelay.NewChannelDelayer(time.Second*3, watchCh, 1024, 16)
			defer delayer.Close()
			watchCh = delayer.Out()
		})
	}

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

	// tickRate represents the number of times EtcdWorker can tick
	// the reactor per second
	tickRate := time.Second / timerInterval
	rl := rate.NewLimiter(rate.Limit(tickRate), 1)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-sessionDone:
			return cerrors.ErrEtcdSessionDone.GenWithStackByArgs()
		case <-ticker.C:
			// There is no new event to handle on timer ticks, so we have nothing here.
		case response := <-watchCh:
			// In this select case, we receive new events from Etcd, and call handleEvent if appropriate.
			if err := response.Err(); err != nil {
				return errors.Trace(err)
			}

			// ProgressNotify implies no new events.
			if response.IsProgressNotify() {
				log.Debug("Etcd progress notification",
					zap.Int64("revision", response.Header.GetRevision()))
				// Note that we don't need to update the revision here, and we
				// should not do so, because the revision of the progress notification
				// may not satisfy the strict monotonicity we have expected.
				//
				// Updating `worker.revision` can cause a useful event with the
				// same revision to be dropped erroneously.
				//
				// Refer to https://etcd.io/docs/v3.3/dev-guide/interacting_v3/#watch-progress
				// "Note: The revision number in the progress notify response is the revision
				// from the local etcd server node that the watch stream is connected to. [...]"
				// This implies that the progress notification will NOT go through the raft
				// consensus, thereby NOT affecting the revision (index).
				continue
			}

			// Check whether the response is stale.
			if worker.revision >= response.Header.GetRevision() {
				log.Info("Stale Etcd event dropped",
					zap.Int64("eventRevision", response.Header.GetRevision()),
					zap.Int64("previousRevision", worker.revision),
					zap.Any("events", response.Events),
					zap.String("role", role))
				continue
			}
			worker.revision = response.Header.GetRevision()

			for _, event := range response.Events {
				// handleEvent will apply the event to our internal `rawState`.
				worker.handleEvent(ctx, event)
			}

		}

		if len(pendingPatches) > 0 {
			// Here we have some patches yet to be uploaded to Etcd.
			pendingPatches, err = worker.applyPatchGroups(ctx, pendingPatches)
			if isRetryableError(err) {
				continue
			}
			if err != nil {
				return errors.Trace(err)
			}
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

			// If !rl.Allow(), skip this Tick to avoid etcd worker tick reactor too frequency.
			// It make etcdWorker to batch etcd changed event in worker.state.
			// The semantics of `ReactorState` requires that any implementation
			// can batch updates internally.
			if !rl.Allow() {
				continue
			}
			startTime := time.Now()
			// it is safe that a batch of updates has been applied to worker.state before worker.reactor.Tick
			nextState, err := worker.reactor.Tick(ctx, worker.state)
			costTime := time.Since(startTime)
			if costTime > etcdWorkerLogsWarnDuration {
				log.Warn("EtcdWorker reactor tick took too long",
					zap.Duration("duration", costTime),
					zap.String("role", role))
			}
			worker.metrics.metricEtcdWorkerTickDuration.Observe(costTime.Seconds())
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

func isRetryableError(err error) bool {
	err = errors.Cause(err)
	if cerrors.ErrEtcdTryAgain.Equal(err) ||
		context.DeadlineExceeded == err {
		return true
	}
	// When encountering an abnormal connection with etcd, the worker will keep retrying
	// until the session is done.
	_, ok := err.(rpctypes.EtcdError)
	return ok
}

func (worker *EtcdWorker) handleEvent(_ context.Context, event *clientv3.Event) {
	if worker.isDeleteCounterKey(event.Kv.Key) {
		switch event.Type {
		case mvccpb.PUT:
			worker.handleDeleteCounter(event.Kv.Value)
		case mvccpb.DELETE:
			log.Warn("deletion counter key deleted", zap.Reflect("event", event))
			worker.handleDeleteCounter(nil)
		}
		// We return here because the delete-counter is not used for business logic,
		// and it should not be exposed further to the Reactor.
		return
	}

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
		worker.rawState[util.NewEtcdKeyFromBytes(event.Kv.Key)] = rawStateEntry{
			value:       value,
			modRevision: event.Kv.ModRevision,
		}
	case mvccpb.DELETE:
		delete(worker.rawState, util.NewEtcdKeyFromBytes(event.Kv.Key))
	}
}

func (worker *EtcdWorker) syncRawState(ctx context.Context) error {
	resp, err := worker.client.Get(ctx, worker.prefix.String(), clientv3.WithPrefix())
	if err != nil {
		return errors.Trace(err)
	}

	worker.rawState = make(map[util.EtcdKey]rawStateEntry)
	for _, kv := range resp.Kvs {
		if worker.isDeleteCounterKey(kv.Key) {
			worker.handleDeleteCounter(kv.Value)
			continue
		}
		key := util.NewEtcdKeyFromBytes(kv.Key)
		worker.rawState[key] = rawStateEntry{
			value:       kv.Value,
			modRevision: kv.ModRevision,
		}
		err := worker.state.Update(key, kv.Value, true)
		if err != nil {
			return errors.Trace(err)
		}
	}

	worker.revision = resp.Header.Revision
	return nil
}

func (worker *EtcdWorker) cloneRawState() map[util.EtcdKey][]byte {
	ret := make(map[util.EtcdKey][]byte)
	for k, v := range worker.rawState {
		vCloned := make([]byte, len(v.value))
		copy(vCloned, v.value)
		ret[util.NewEtcdKey(k.String())] = vCloned
	}
	return ret
}

func (worker *EtcdWorker) applyPatchGroups(ctx context.Context, patchGroups [][]DataPatch) ([][]DataPatch, error) {
	state := worker.cloneRawState()
	for len(patchGroups) > 0 {
		changeSate, n, size, err := getBatchChangedState(state, patchGroups)
		if err != nil {
			return patchGroups, err
		}
		err = worker.commitChangedState(ctx, changeSate, size)
		if err != nil {
			return patchGroups, err
		}
		patchGroups = patchGroups[n:]
	}
	return patchGroups, nil
}

func (worker *EtcdWorker) commitChangedState(ctx context.Context, changedState map[util.EtcdKey][]byte, size int) error {
	if len(changedState) == 0 {
		return nil
	}

	cmps := make([]clientv3.Cmp, 0, len(changedState))
	opsThen := make([]clientv3.Op, 0, len(changedState))
	hasDelete := false

	for key, value := range changedState {
		// make sure someone else has not updated the key after the last snapshot
		var cmp clientv3.Cmp
		if entry, ok := worker.rawState[key]; ok {
			cmp = clientv3.Compare(clientv3.ModRevision(key.String()), "=", entry.modRevision)
		} else {
			// if ok is false, it means that the key of this patch is not exist in a committed state
			// this compare is equivalent to `patch.Key` is not exist
			cmp = clientv3.Compare(clientv3.ModRevision(key.String()), "=", 0)
		}
		cmps = append(cmps, cmp)

		var op clientv3.Op
		if value != nil {
			op = clientv3.OpPut(key.String(), string(value))
		} else {
			op = clientv3.OpDelete(key.String())
			hasDelete = true
		}
		opsThen = append(opsThen, op)
	}

	if hasDelete {
		opsThen = append(opsThen, clientv3.OpPut(worker.prefix.String()+deletionCounterKey, fmt.Sprint(worker.deleteCounter+1)))
	}
	if worker.deleteCounter > 0 {
		cmps = append(cmps, clientv3.Compare(clientv3.Value(worker.prefix.String()+deletionCounterKey), "=", fmt.Sprint(worker.deleteCounter)))
	} else if worker.deleteCounter == 0 {
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(worker.prefix.String()+deletionCounterKey), "=", 0))
	} else {
		panic("unreachable")
	}

	worker.metrics.metricEtcdTxnSize.Observe(float64(size))
	startTime := time.Now()
	resp, err := worker.client.Txn(ctx, cmps, opsThen, etcd.TxnEmptyOpsElse)

	// For testing the situation where we have a progress notification that
	// has the same revision as the committed Etcd transaction.
	failpoint.Inject("InjectProgressRequestAfterCommit", func() {
		if err := worker.client.RequestProgress(ctx); err != nil {
			failpoint.Return(errors.Trace(err))
		}
	})

	costTime := time.Since(startTime)
	if costTime > etcdWorkerLogsWarnDuration {
		log.Warn("Etcd transaction took too long", zap.Duration("duration", costTime))
	}
	worker.metrics.metricEtcdTxnDuration.Observe(costTime.Seconds())
	if err != nil {
		return errors.Trace(err)
	}

	logEtcdOps(opsThen, resp.Succeeded)
	if resp.Succeeded {
		worker.barrierRev = resp.Header.GetRevision()
		return nil
	}

	// Logs the conditions for the failed Etcd transaction.
	worker.logEtcdCmps(cmps)
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

func logEtcdOps(ops []clientv3.Op, committed bool) {
	if committed && (log.GetLevel() != zapcore.DebugLevel || len(ops) == 0) {
		return
	}
	logFn := log.Debug
	if !committed {
		logFn = log.Info
	}

	logFn("[etcd worker] ==========Update State to ETCD==========")
	for _, op := range ops {
		if op.IsDelete() {
			logFn("[etcd worker] delete key", zap.ByteString("key", op.KeyBytes()))
		} else {
			etcdKey := util.NewEtcdKeyFromBytes(op.KeyBytes())
			value := string(op.ValueBytes())
			// we need to mask the sink-uri in changefeedInfo
			if strings.Contains(etcdKey.String(), changefeedInfoKey) {
				changefeedInfo := &model.ChangeFeedInfo{}
				if err := json.Unmarshal(op.ValueBytes(), changefeedInfo); err != nil {
					logFn("[etcd worker] unmarshal changefeed info failed", zap.Error(err))
					continue
				}
				value = changefeedInfo.String()
			}
			logFn("[etcd worker] put key", zap.ByteString("key", op.KeyBytes()), zap.String("value", value))
		}
	}
	logFn("[etcd worker] ============State Commit=============", zap.Bool("committed", committed))
}

func (worker *EtcdWorker) logEtcdCmps(cmps []clientv3.Cmp) {
	log.Info("[etcd worker] ==========Failed Etcd Txn Cmps==========")
	for _, cmp := range cmps {
		cmp := etcdserverpb.Compare(cmp)
		log.Info("[etcd worker] compare",
			zap.String("cmp", cmp.String()))
	}
	log.Info("[etcd worker] ============End Failed Etcd Txn Cmps=============")
}

func (worker *EtcdWorker) cleanUp() {
	worker.rawState = nil
	worker.revision = 0
	worker.pendingUpdates = worker.pendingUpdates[:0]
}

func (worker *EtcdWorker) isDeleteCounterKey(key []byte) bool {
	return string(key) == worker.prefix.String()+deletionCounterKey
}

func (worker *EtcdWorker) handleDeleteCounter(value []byte) {
	if len(value) == 0 {
		// The delete counter key has been deleted, resetting the internal counter
		worker.deleteCounter = 0
		return
	}

	var err error
	worker.deleteCounter, err = strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		// This should never happen unless Etcd server has been tampered with.
		log.Panic("strconv failed. Unexpected Etcd state.", zap.Error(err))
	}
	if worker.deleteCounter <= 0 {
		log.Panic("unexpected delete counter", zap.Int64("value", worker.deleteCounter))
	}
}
