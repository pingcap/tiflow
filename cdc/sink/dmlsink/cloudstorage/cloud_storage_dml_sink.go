// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"context"
	"math"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/pingcap/tiflow/cdc/sink/util"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/pingcap/tiflow/pkg/sink/codec/builder"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	putil "github.com/pingcap/tiflow/pkg/util"
	"golang.org/x/sync/errgroup"
)

const (
	defaultEncodingConcurrency = 8
	defaultChannelSize         = 1024
)

// Assert EventSink[E event.TableEvent] implementation
var _ dmlsink.EventSink[*model.SingleTableTxn] = (*DMLSink)(nil)

// eventFragment is used to attach a sequence number to TxnCallbackableEvent.
type eventFragment struct {
	event          *dmlsink.TxnCallbackableEvent
	versionedTable cloudstorage.VersionedTableName

	// The sequence number is mainly useful for TxnCallbackableEvent defragmentation.
	// e.g. TxnCallbackableEvent 1~5 are dispatched to a group of encoding workers, but the
	// encoding completion time varies. Let's say the final completion sequence are 1,3,2,5,4,
	// we can use the sequence numbers to do defragmentation so that the events can arrive
	// at dmlWorker sequentially.
	seqNumber uint64
	// encodedMsgs denote the encoded messages after the event is handled in encodingWorker.
	encodedMsgs []*common.Message
}

// DMLSink is the cloud storage sink.
// It will send the events to cloud storage systems.
type DMLSink struct {
	changefeedID model.ChangeFeedID
	scheme       string
	// last sequence number
	lastSeqNum uint64
	// encodingWorkers defines a group of workers for encoding events.
	encodingWorkers []*encodingWorker
	// defragmenter is used to defragment the out-of-order encoded messages and
	// sends encoded messages to individual dmlWorkers.
	defragmenter *defragmenter
	// workers defines a group of workers for writing events to external storage.
	workers []*dmlWorker

	alive struct {
		sync.RWMutex
		// msgCh is a channel to hold eventFragment.
		msgCh  chan eventFragment
		isDead bool
	}

	statistics *metrics.Statistics

	cancel func()
	wg     sync.WaitGroup
	dead   chan struct{}
}

// NewDMLSink creates a cloud storage sink.
func NewDMLSink(ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	errCh chan error,
) (*DMLSink, error) {
	// create cloud storage config and then apply the params of sinkURI to it.
	cfg := cloudstorage.NewConfig()
	err := cfg.Apply(ctx, sinkURI, replicaConfig)
	if err != nil {
		return nil, err
	}

	// create an external storage.
	storage, err := putil.GetExternalStorageFromURI(ctx, sinkURI.String())
	if err != nil {
		return nil, err
	}

	// fetch protocol from replicaConfig defined by changefeed config file.
	protocol, err := util.GetProtocol(replicaConfig.Sink.Protocol)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// get cloud storage file extension according to the specific protocol.
	ext := util.GetFileExtension(protocol)
	// the last param maxMsgBytes is mainly to limit the size of a single message for
	// batch protocols in mq scenario. In cloud storage sink, we just set it to max int.
	encoderConfig, err := util.GetEncoderConfig(sinkURI, protocol, replicaConfig, math.MaxInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	encoderBuilder, err := builder.NewTxnEventEncoderBuilder(encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrStorageSinkInvalidConfig, err)
	}

	wgCtx, wgCancel := context.WithCancel(ctx)
	s := &DMLSink{
		changefeedID:    contextutil.ChangefeedIDFromCtx(wgCtx),
		scheme:          strings.ToLower(sinkURI.Scheme),
		encodingWorkers: make([]*encodingWorker, defaultEncodingConcurrency),
		workers:         make([]*dmlWorker, cfg.WorkerCount),
		statistics:      metrics.NewStatistics(wgCtx, sink.TxnSink),
		cancel:          wgCancel,
		dead:            make(chan struct{}),
	}
	s.alive.msgCh = make(chan eventFragment, defaultChannelSize)

	encodedCh := make(chan eventFragment, defaultChannelSize)
	workerChannels := make([]*chann.DrainableChann[eventFragment], cfg.WorkerCount)

	// create a group of encoding workers.
	for i := 0; i < defaultEncodingConcurrency; i++ {
		encoder := encoderBuilder.Build()
		s.encodingWorkers[i] = newEncodingWorker(i, s.changefeedID, encoder, s.alive.msgCh, encodedCh)
	}
	// create defragmenter.
	s.defragmenter = newDefragmenter(encodedCh, workerChannels)
	// create a group of dml workers.
	clock := clock.New()
	for i := 0; i < cfg.WorkerCount; i++ {
		inputCh := chann.NewAutoDrainChann[eventFragment]()
		s.workers[i] = newDMLWorker(i, s.changefeedID, storage, cfg, ext,
			inputCh, clock, s.statistics)
		workerChannels[i] = inputCh
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		err := s.run(wgCtx)

		s.alive.Lock()
		s.alive.isDead = true
		close(s.alive.msgCh)
		s.alive.Unlock()
		close(s.dead)

		if err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-wgCtx.Done():
			case errCh <- err:
			}
		}
	}()

	return s, nil
}

func (s *DMLSink) run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	// run the encoding workers.
	for i := 0; i < defaultEncodingConcurrency; i++ {
		encodingWorker := s.encodingWorkers[i]
		eg.Go(func() error {
			return encodingWorker.run(ctx)
		})
	}

	// run the defragmenter.
	eg.Go(func() error {
		return s.defragmenter.run(ctx)
	})

	// run dml workers.
	for i := 0; i < len(s.workers); i++ {
		worker := s.workers[i]
		eg.Go(func() error {
			return worker.run(ctx)
		})
	}

	return eg.Wait()
}

// WriteEvents write events to cloud storage sink.
func (s *DMLSink) WriteEvents(txns ...*dmlsink.CallbackableEvent[*model.SingleTableTxn]) error {
	s.alive.RLock()
	defer s.alive.RUnlock()
	if s.alive.isDead {
		return errors.Trace(errors.New("dead dmlSink"))
	}

	for _, txn := range txns {
		if txn.GetTableSinkState() != state.TableSinkSinking {
			// The table where the event comes from is in stopping, so it's safe
			// to drop the event directly.
			txn.Callback()
			continue
		}

		tbl := cloudstorage.VersionedTableName{
			TableNameWithPhysicTableID: *txn.Event.Table,
			TableInfoVersion:           txn.Event.TableInfoVersion,
		}
		seq := atomic.AddUint64(&s.lastSeqNum, 1)

		s.statistics.ObserveRows(txn.Event.Rows...)
		// emit a TxnCallbackableEvent encoupled with a sequence number starting from one.
		s.alive.msgCh <- eventFragment{
			seqNumber:      seq,
			versionedTable: tbl,
			event:          txn,
		}
	}

	return nil
}

// Scheme returns the sink scheme.
func (s *DMLSink) Scheme() string {
	return s.scheme
}

// Close closes the cloud storage sink.
func (s *DMLSink) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()

	for _, encodingWorker := range s.encodingWorkers {
		encodingWorker.close()
	}

	for _, worker := range s.workers {
		worker.close()
	}

	if s.statistics != nil {
		s.statistics.Close()
	}
}

// Dead checks whether it's dead or not.
func (s *DMLSink) Dead() <-chan struct{} {
	return s.dead
}
