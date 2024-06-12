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
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/txn/mysql"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink/state"
	"github.com/pingcap/tiflow/pkg/causality"
	"github.com/pingcap/tiflow/pkg/config"
	psink "github.com/pingcap/tiflow/pkg/sink"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"golang.org/x/sync/errgroup"
)

const (
	// DefaultConflictDetectorSlots indicates the default slot count of conflict detector.
	DefaultConflictDetectorSlots uint64 = 16 * 1024
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*sink)(nil)

// sink is the sink for SingleTableTxn.
type sink struct {
	alive struct {
		sync.RWMutex
		conflictDetector *causality.ConflictDetector[*txnEvent]
		isDead           bool
	}
	scheme string

	workers []*worker
	cancel  func()

	wg   sync.WaitGroup
	dead chan struct{}

	statistics *metrics.Statistics
}

// GetDBConnImpl is the implementation of pmysql.Factory.
// Exported for testing.
// Maybe we can use a better way to do this. Because this is not thread-safe.
// You can use `SetupSuite` and `TearDownSuite` to do this to get a better way.
var GetDBConnImpl pmysql.Factory = pmysql.CreateMySQLDBConn

// NewMySQLSink creates a mysql sink with given parameters.
func NewMySQLSink(
	ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	errCh chan<- error,
	conflictDetectorSlots uint64,
) (*sink, error) {
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
	sink := newSink(ctx, backends, errCh, conflictDetectorSlots)
	sink.scheme = strings.ToLower(sinkURI.Scheme)
	sink.statistics = statistics
	sink.cancel = cancel

	return sink, nil
}

func newSink(ctx context.Context, backends []backend,
	errCh chan<- error, conflictDetectorSlots uint64,
) *sink {
	ctx, cancel := context.WithCancel(ctx)
	sink := &sink{
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
		w := newWorker(ctx1, i, backend, len(backends))
		txnCh := sink.alive.conflictDetector.GetOutChByCacheID(int64(i))
		g.Go(func() error { return w.run(txnCh) })
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

// WriteEvents writes events to the sink.
func (s *sink) WriteEvents(txnEvents ...*eventsink.TxnCallbackableEvent) error {
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

// SchemeOption returns the sink scheme.
func (s *sink) SchemeOption() (string, bool) {
	return s.scheme, false
}

// Close closes the sink. It won't wait for all pending items backend handled.
func (s *sink) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()

	if s.statistics != nil {
		s.statistics.Close()
	}
}

// Dead checks whether it's dead or not.
func (s *sink) Dead() <-chan struct{} {
	return s.dead
}
