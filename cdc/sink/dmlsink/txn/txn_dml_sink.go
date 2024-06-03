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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/txn/mysql"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/pingcap/tiflow/pkg/causality"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"golang.org/x/sync/errgroup"
)

const (
	// DefaultConflictDetectorSlots indicates the default slot count of conflict detector.
	DefaultConflictDetectorSlots uint64 = 16 * 1024
)

// Assert EventSink[E event.TableEvent] implementation
var _ dmlsink.EventSink[*model.SingleTableTxn] = (*dmlSink)(nil)

// dmlSink is the dmlSink for SingleTableTxn.
type dmlSink struct {
	alive struct {
		sync.RWMutex
		conflictDetector *causality.ConflictDetector[*txnEvent]
		isDead           bool
	}

	workers []*worker
	cancel  func()

	wg   sync.WaitGroup
	dead chan struct{}

	statistics *metrics.Statistics

	scheme string
}

// GetDBConnImpl is the implementation of pmysql.Factory.
// Exported for testing.
// Maybe we can use a better way to do this. Because this is not thread-safe.
// You can use `SetupSuite` and `TearDownSuite` to do this to get a better way.
var GetDBConnImpl pmysql.Factory = pmysql.CreateMySQLDBConn

// NewMySQLSink creates a mysql dmlSink with given parameters.
func NewMySQLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	errCh chan<- error,
	conflictDetectorSlots uint64,
) (*dmlSink, error) {
	ctx, cancel := context.WithCancel(ctx)
	statistics := metrics.NewStatistics(ctx, changefeedID, sink.TxnSink)

	backendImpls, err := mysql.NewMySQLBackends(ctx, changefeedID, sinkURI, replicaConfig, GetDBConnImpl, statistics)
	if err != nil {
		cancel()
		return nil, err
	}

	backends := make([]backend, 0, len(backendImpls))
	for _, impl := range backendImpls {
		backends = append(backends, impl)
	}

	s := newSink(ctx, changefeedID, backends, errCh, conflictDetectorSlots)
	s.statistics = statistics
	s.cancel = cancel
	s.scheme = sink.GetScheme(sinkURI)

	return s, nil
}

func newSink(ctx context.Context,
	changefeedID model.ChangeFeedID,
	backends []backend,
	errCh chan<- error, conflictDetectorSlots uint64,
) *dmlSink {
	ctx, cancel := context.WithCancel(ctx)
	sink := &dmlSink{
		workers: make([]*worker, 0, len(backends)),
		cancel:  cancel,
		dead:    make(chan struct{}),
	}

	sink.alive.conflictDetector = causality.NewConflictDetector[*txnEvent](conflictDetectorSlots, causality.TxnCacheOption{
		Count:         len(backends),
		Size:          1024,
		BlockStrategy: causality.BlockStrategyWaitEmpty,
	})

	g, ctx1 := errgroup.WithContext(ctx)
	for i, backend := range backends {
		w := newWorker(ctx1, changefeedID, i, backend, len(backends))
		txnCh := sink.alive.conflictDetector.GetOutChByCacheID(int64(i))
		g.Go(func() error { return w.runLoop(txnCh) })
		sink.workers = append(sink.workers, w)
	}

	sink.wg.Add(1)
	go func() {
		defer sink.wg.Done()
		err := g.Wait()

		sink.alive.Lock()
		sink.alive.isDead = true
		sink.alive.conflictDetector.Close()
		sink.alive.Unlock()
		close(sink.dead)

		if err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
			case errCh <- err:
			}
		}
	}()

	return sink
}

// WriteEvents writes events to the dmlSink.
func (s *dmlSink) WriteEvents(txnEvents ...*dmlsink.TxnCallbackableEvent) error {
	s.alive.RLock()
	defer s.alive.RUnlock()
	if s.alive.isDead {
		return errors.Trace(errors.New("dead dmlSink"))
	}

	for _, txn := range txnEvents {
		if txn.GetTableSinkState() != state.TableSinkSinking {
			// The table where the event comes from is in stopping, so it's safe
			// to drop the event directly.
			txn.Callback()
			continue
		}
		s.alive.conflictDetector.Add(newTxnEvent(txn))
	}
	return nil
}

// Close closes the dmlSink. It won't wait for all pending items backend handled.
func (s *dmlSink) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()

	if s.statistics != nil {
		s.statistics.Close()
	}
}

// Dead checks whether it's dead or not.
func (s *dmlSink) Dead() <-chan struct{} {
	return s.dead
}

func (s *dmlSink) SchemeOption() (string, bool) {
	return s.scheme, false
}
