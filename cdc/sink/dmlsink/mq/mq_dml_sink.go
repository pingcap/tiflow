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

package mq

import (
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/manager"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/transformer"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Assert EventSink[E event.TableEvent] implementation
var _ dmlsink.EventSink[*model.SingleTableTxn] = (*dmlSink)(nil)

// dmlSink is the mq sink.
// It will send the events to the MQ system.
type dmlSink struct {
	// id indicates this sink belongs to which processor(changefeed).
	id model.ChangeFeedID
	// protocol indicates the protocol used by this sink.
	protocol config.Protocol

	alive struct {
		sync.RWMutex

		transformer transformer.Transformer
		// eventRouter used to route events to the right topic and partition.
		eventRouter *dispatcher.EventRouter
		// topicManager used to manage topics.
		// It is also responsible for creating topics.
		topicManager manager.TopicManager
		worker       *worker
		isDead       bool
	}

	// adminClient is used to query kafka cluster information, it's shared among
	// multiple place, it's sink's responsibility to close it.
	adminClient kafka.ClusterAdminClient

	ctx    context.Context
	cancel context.CancelCauseFunc

	wg   sync.WaitGroup
	dead chan struct{}

	scheme string
}

func newDMLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	producer dmlproducer.DMLProducer,
	adminClient kafka.ClusterAdminClient,
	topicManager manager.TopicManager,
	eventRouter *dispatcher.EventRouter,
	transformer transformer.Transformer,
	encoderGroup codec.EncoderGroup,
	protocol config.Protocol,
	scheme string,
	errCh chan error,
) *dmlSink {
	ctx, cancel := context.WithCancelCause(ctx)
	statistics := metrics.NewStatistics(ctx, changefeedID, sink.RowSink)
	worker := newWorker(changefeedID, protocol, producer, encoderGroup, statistics)

	s := &dmlSink{
		id:          changefeedID,
		protocol:    protocol,
		adminClient: adminClient,
		ctx:         ctx,
		cancel:      cancel,
		dead:        make(chan struct{}),
		scheme:      scheme,
	}
	s.alive.transformer = transformer
	s.alive.eventRouter = eventRouter
	s.alive.topicManager = topicManager
	s.alive.worker = worker

	// Spawn a goroutine to send messages by the worker.
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		err := s.alive.worker.run(ctx)

		s.alive.Lock()
		s.alive.isDead = true
		s.alive.worker.close()
		s.alive.Unlock()
		close(s.dead)

		if err != nil {
			if errors.Cause(err) == context.Canceled {
				err = context.Cause(ctx)
			}
			select {
			case errCh <- err:
				log.Warn("mq dml sink meet error",
					zap.String("namespace", s.id.Namespace),
					zap.String("changefeed", s.id.ID),
					zap.Error(err))
			default:
				log.Info("mq dml sink meet error, ignored",
					zap.String("namespace", s.id.Namespace),
					zap.String("changefeed", s.id.ID),
					zap.Error(err))
			}
		}
	}()

	return s
}

// WriteEvents writes events to the sink.
// This is an asynchronously and thread-safe method.
func (s *dmlSink) WriteEvents(txns ...*dmlsink.CallbackableEvent[*model.SingleTableTxn]) error {
	s.alive.RLock()
	defer s.alive.RUnlock()
	if s.alive.isDead {
		return errors.Trace(errors.New("dead dmlSink"))
	}
	// merge the split row callback into one callback
	mergedCallback := func(outCallback func(), totalCount uint64) func() {
		var acked atomic.Uint64
		return func() {
			if acked.Add(1) == totalCount {
				outCallback()
			}
		}
	}
	for _, txn := range txns {
		if txn.GetTableSinkState() != state.TableSinkSinking {
			// The table where the event comes from is in stopping, so it's safe
			// to drop the event directly.
			txn.Callback()
			continue
		}
		callback := mergedCallback(txn.Callback, uint64(len(txn.Event.Rows)))

		for _, row := range txn.Event.Rows {
			topic := s.alive.eventRouter.GetTopicForRowChange(row)
			partitionNum, err := s.alive.topicManager.GetPartitionNum(s.ctx, topic)
			failpoint.Inject("MQSinkGetPartitionError", func() {
				log.Info("failpoint MQSinkGetPartitionError injected", zap.String("changefeedID", s.id.ID))
				err = errors.New("MQSinkGetPartitionError")
			})
			if err != nil {
				s.cancel(err)
				return errors.Trace(err)
			}

			err = s.alive.transformer.Apply(row)
			if err != nil {
				s.cancel(err)
				return errors.Trace(err)
			}

			index, key, err := s.alive.eventRouter.GetPartitionForRowChange(row, partitionNum)
			if err != nil {
				s.cancel(err)
				return errors.Trace(err)
			}
			// This never be blocked because this is an unbounded channel.
			s.alive.worker.msgChan.In() <- mqEvent{
				key: TopicPartitionKey{
					Topic: topic, Partition: index, PartitionKey: key,
				},
				rowEvent: &dmlsink.RowChangeCallbackableEvent{
					Event:     row,
					Callback:  callback,
					SinkState: txn.SinkState,
				},
			}
		}
	}
	return nil
}

// Close closes the sink.
func (s *dmlSink) Close() {
	if s.cancel != nil {
		s.cancel(nil)
	}
	s.wg.Wait()

	s.alive.RLock()
	if s.alive.topicManager != nil {
		s.alive.topicManager.Close()
	}
	s.alive.RUnlock()

	if s.adminClient != nil {
		s.adminClient.Close()
	}
}

// Dead checks whether it's dead or not.
func (s *dmlSink) Dead() <-chan struct{} {
	return s.dead
}

// Scheme returns the scheme of this sink.
func (s *dmlSink) Scheme() string {
	return s.scheme
}
