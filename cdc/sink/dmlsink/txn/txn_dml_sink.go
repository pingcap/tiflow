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

package txn

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/txn/mysql"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/pingcap/tiflow/pkg/causality"
	"github.com/pingcap/tiflow/pkg/config"
	psink "github.com/pingcap/tiflow/pkg/sink"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"golang.org/x/sync/errgroup"
)

const (
	// defaultConflictDetectorSlots indicates the default slot count of conflict detector.
	// it's not configurable for users but can be adjusted by tests.
	defaultConflictDetectorSlots uint64 = 16 * 1024
)

// Assert EventSink[E event.TableEvent] implementation
var _ dmlsink.EventSink[*model.SingleTableTxn] = (*dmlSink)(nil)

// dmlSink is the dmlSink for SingleTableTxn.
type dmlSink struct {
	conflictDetector *causality.ConflictDetector[*worker, *txnEvent]
	workers          []*worker
	cancel           func()

	wg     sync.WaitGroup
	dead   chan struct{}
	isDead atomic.Bool

	statistics *metrics.Statistics
}

// GetDBConnImpl is the implementation of pmysql.Factory.
// Exported for testing.
// Maybe we can use a better way to do this. Because this is not thread-safe.
// You can use `SetupSuite` and `TearDownSuite` to do this to get a better way.
var GetDBConnImpl pmysql.Factory = pmysql.CreateMySQLDBConn

// NewMySQLSink creates a mysql dmlSink with given parameters.
func NewMySQLSink(
	ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	errCh chan<- error,
) (*dmlSink, error) {
	ctx, cancel := context.WithCancel(ctx)
	statistics := metrics.NewStatistics(ctx, psink.TxnSink)

	backendImpls, err := mysql.NewMySQLBackends(ctx, sinkURI, replicaConfig, GetDBConnImpl, statistics)
	if err != nil {
		cancel()
		return nil, err
	}

	backends := make([]backend, 0, len(backendImpls))
	for _, impl := range backendImpls {
		backends = append(backends, impl)
	}
	sink := newSink(ctx, backends, errCh, defaultConflictDetectorSlots)
	sink.statistics = statistics
	sink.cancel = cancel

	return sink, nil
}

func newSink(ctx context.Context, backends []backend,
	errCh chan<- error, conflictDetectorSlots uint64,
) *dmlSink {
	ctx, cancel := context.WithCancel(ctx)
	sink := &dmlSink{
		workers: make([]*worker, 0, len(backends)),
		cancel:  cancel,
		dead:    make(chan struct{}),
	}

	g, ctx1 := errgroup.WithContext(ctx)
	for i, backend := range backends {
		w := newWorker(ctx1, i, backend, len(backends))
		g.Go(func() error { return w.runLoop() })
		sink.workers = append(sink.workers, w)
	}

	sink.wg.Add(1)
	go func() {
		defer sink.wg.Done()
		err := g.Wait()
		sink.isDead.Store(true)
		close(sink.dead)
		if err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
			case errCh <- err:
			}
		}
	}()

	sink.conflictDetector = causality.NewConflictDetector[*worker, *txnEvent](sink.workers, causality.Config{
		// TODO: use SingleConflictDispatchSerialize if batch dml across txns is enabled.
		OnSingleConflict: causality.SingleConflictDispatchPipeline,
		NumSlots:         conflictDetectorSlots,
	})
	return sink
}

// WriteEvents writes events to the dmlSink.
func (s *dmlSink) WriteEvents(txnEvents ...*dmlsink.TxnCallbackableEvent) error {
	if s.isDead.Load() {
		return errors.Trace(errors.New("dead dmlSink"))
	}

	for _, txn := range txnEvents {
		if txn.GetTableSinkState() != state.TableSinkSinking {
			// The table where the event comes from is in stopping, so it's safe
			// to drop the event directly.
			txn.Callback()
			continue
		}

		s.conflictDetector.Add(newTxnEvent(txn))
	}
	return nil
}

// Close closes the dmlSink. It won't wait for all pending items backend handled.
func (s *dmlSink) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()

	for _, w := range s.workers {
		w.close()
	}
	// workers could call callback, which will send data to channel in conflict
	// detector, so we can't close conflict detector until all workers are closed.
	s.conflictDetector.Close()

	if s.statistics != nil {
		s.statistics.Close()
	}
}

// Dead checks whether it's dead or not.
func (s *dmlSink) Dead() <-chan struct{} {
	return s.dead
}
