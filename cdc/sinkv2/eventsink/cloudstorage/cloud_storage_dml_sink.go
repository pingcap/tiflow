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
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/builder"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink/state"
	"github.com/pingcap/tiflow/cdc/sinkv2/util"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	putil "github.com/pingcap/tiflow/pkg/util"
	"golang.org/x/sync/errgroup"
)

const (
	defaultEncodingConcurrency = 8
	defaultChannelSize         = 1024
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*dmlSink)(nil)

// versionedTable is used to wrap TableName with a version
type versionedTable struct {
	model.TableName
	version uint64
}

// eventFragment is used to attach a sequence number to TxnCallbackableEvent.
// The sequence number is mainly useful for TxnCallbackableEvent defragmentation.
// e.g. TxnCallbackableEvent 1~5 are dispatched to a group of encoding workers, but the
// encoding completion time varies. Let's say the final completion sequence are 1,3,2,5,4,
// we can use the sequence numbers to do defragmentation so that the events can arrive
// at dmlWorker sequentially.
type eventFragment struct {
	// event sequence number
	seqNumber uint64
	versionedTable
	event *eventsink.TxnCallbackableEvent
	// encodedMsgs denote the encoded messages after the event is handled in encodingWorker.
	encodedMsgs []*common.Message
}

// dmlSink is the cloud storage sink.
// It will send the events to cloud storage systems.
type dmlSink struct {
	changefeedID model.ChangeFeedID
	// msgCh is a channel to hold eventFragment.
	msgCh chan eventFragment
	// encodingWorkers defines a group of workers for encoding events.
	encodingWorkers []*encodingWorker
	// defragmenter is used to defragment the out-of-order encoded messages.
	defragmenter *defragmenter
	// writer is a dmlWriter which manages a group of dmlWorkers and
	// sends encoded messages to individual dmlWorkers.
	writer     *dmlWriter
	statistics *metrics.Statistics
	// last sequence number
	lastSeqNum uint64
	wg         sync.WaitGroup
}

// NewCloudStorageSink creates a cloud storage sink.
func NewCloudStorageSink(ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	errCh chan error,
) (*dmlSink, error) {
	s := &dmlSink{}
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
	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCloudStorageInvalidConfig, err)
	}

	s.changefeedID = contextutil.ChangefeedIDFromCtx(ctx)
	s.msgCh = make(chan eventFragment, defaultChannelSize)
	s.defragmenter = newDefragmenter(ctx)
	orderedCh := s.defragmenter.orderedOut()
	s.statistics = metrics.NewStatistics(ctx, sink.TxnSink)
	s.writer = newDMLWriter(s.changefeedID, storage, cfg, ext, s.statistics, orderedCh, errCh)
	s.encodingWorkers = make([]*encodingWorker, 0, defaultEncodingConcurrency)
	// create a group of encoding workers.
	for i := 0; i < defaultEncodingConcurrency; i++ {
		encoder := encoderBuilder.Build()
		w := newEncodingWorker(i, s.changefeedID, encoder, s.msgCh, s.defragmenter)
		s.encodingWorkers = append(s.encodingWorkers, w)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			errCh <- err
		}
	}()

	return s, nil
}

func (s *dmlSink) run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	// run dml writer
	eg.Go(func() error {
		return s.writer.run(ctx)
	})

	// run the encoding workers.
	for i := 0; i < defaultEncodingConcurrency; i++ {
		worker := s.encodingWorkers[i]
		eg.Go(func() error {
			return worker.run(ctx)
		})
	}

	return eg.Wait()
}

// WriteEvents write events to cloud storage sink.
func (s *dmlSink) WriteEvents(txns ...*eventsink.CallbackableEvent[*model.SingleTableTxn]) error {
	var tbl versionedTable

	for _, txn := range txns {
		if txn.GetTableSinkState() != state.TableSinkSinking {
			// The table where the event comes from is in stopping, so it's safe
			// to drop the event directly.
			txn.Callback()
			continue
		}

		tbl = versionedTable{
			TableName: txn.Event.TableInfo.TableName,
			version:   txn.Event.TableInfo.Version,
		}
		seq := atomic.AddUint64(&s.lastSeqNum, 1)
		// emit a TxnCallbackableEvent encoupled with a sequence number starting from one.
		s.msgCh <- eventFragment{
			seqNumber:      seq,
			versionedTable: tbl,
			event:          txn,
		}
	}

	return nil
}

// Close closes the cloud storage sink.
func (s *dmlSink) Close() {
	if s.defragmenter != nil {
		s.defragmenter.close()
	}

	for _, w := range s.encodingWorkers {
		w.close()
	}

	if s.writer != nil {
		s.writer.close()
	}

	if s.statistics != nil {
		s.statistics.Close()
	}
	s.wg.Wait()
}
