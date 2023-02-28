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
)

const (
	// DefaultConflictDetectorSlots indicates the default slot count of conflict detector.
	DefaultConflictDetectorSlots uint64 = 16 * 1024
)

// Assert EventSink[E event.TableEvent] implementation
var _ dmlsink.EventSink[*model.SingleTableTxn] = (*dmlSink)(nil)

// dmlSink is the dmlSink for SingleTableTxn.
type dmlSink struct {
	conflictDetector *causality.ConflictDetector[*worker, *txnEvent]
	workers          []*worker
	cancel           func()
	// set when the dmlSink is closed explicitly. and then subsequence `WriteEvents` call
	// should return an error.
	closed int32

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
	conflictDetectorSlots uint64,
) (*dmlSink, error) {
	ctx1, cancel := context.WithCancel(ctx)
	statistics := metrics.NewStatistics(ctx1, psink.TxnSink)
	backendImpls, err := mysql.NewMySQLBackends(ctx, sinkURI,
		replicaConfig, GetDBConnImpl, statistics)
	if err != nil {
		cancel()
		return nil, err
	}

	backends := make([]backend, 0, len(backendImpls))
	for _, impl := range backendImpls {
		backends = append(backends, impl)
	}
	sink := newSink(ctx, backends, errCh, conflictDetectorSlots)
	sink.statistics = statistics
	sink.cancel = cancel

	return sink, nil
}

func newSink(ctx context.Context, backends []backend,
	errCh chan<- error, conflictDetectorSlots uint64,
) *dmlSink {
	workers := make([]*worker, 0, len(backends))
	for i, backend := range backends {
		w := newWorker(ctx, i, backend, errCh, len(backends))
		w.runBackgroundLoop()
		workers = append(workers, w)
	}
	detector := causality.NewConflictDetector[*worker, *txnEvent](workers, conflictDetectorSlots)
	return &dmlSink{conflictDetector: detector, workers: workers}
}

// WriteEvents writes events to the dmlSink.
func (s *dmlSink) WriteEvents(txnEvents ...*dmlsink.TxnCallbackableEvent) error {
	if atomic.LoadInt32(&s.closed) != 0 {
		return errors.Trace(errors.New("closed dmlSink"))
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
	atomic.StoreInt32(&s.closed, 1)
	for _, w := range s.workers {
		w.Close()
	}
	// workers could call callback, which will send data to channel in conflict
	// detector, so we can't close conflict detector until all workers are closed.
	s.conflictDetector.Close()
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	if s.statistics != nil {
		s.statistics.Close()
	}
}
