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
	"net/url"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/builder"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	mqv1 "github.com/pingcap/tiflow/cdc/sink/mq"
	"github.com/pingcap/tiflow/cdc/sink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/mq/manager"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/pingcap/tiflow/cdc/sinkv2/tablesink/state"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.SingleTableTxn] = (*dmlSink)(nil)

// dmlSink is the mq sink.
// It will send the events to the MQ system.
type dmlSink struct {
	// id indicates this sink belongs to which processor(changefeed).
	id     model.ChangeFeedID
	scheme string
	// protocol indicates the protocol used by this sink.
	protocol config.Protocol

	alive struct {
		sync.RWMutex
		// eventRouter used to route events to the right topic and partition.
		eventRouter *dispatcher.EventRouter
		// topicManager used to manage topics.
		// It is also responsible for creating topics.
		topicManager manager.TopicManager
		worker       *worker
		isDead       bool
	}

	ctx       context.Context
	cancel    context.CancelFunc
	cancelErr error

	wg   sync.WaitGroup
	dead chan struct{}
}

func newSink(ctx context.Context,
	sinkURI *url.URL,
	producer dmlproducer.DMLProducer,
	topicManager manager.TopicManager,
	eventRouter *dispatcher.EventRouter,
	encoderConfig *common.Config,
	encoderConcurrency int,
	errCh chan error,
) (*dmlSink, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	ctx, cancel := context.WithCancel(ctx)

	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		cancel()
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	statistics := metrics.NewStatistics(ctx, sink.TxnSink)
	worker := newWorker(changefeedID, encoderConfig.Protocol,
		encoderBuilder, encoderConcurrency, producer, statistics)

	s := &dmlSink{
		id:       changefeedID,
		scheme:   strings.ToLower(sinkURI.Scheme),
		protocol: encoderConfig.Protocol,
		ctx:      ctx,
		cancel:   cancel,
		dead:     make(chan struct{}),
	}
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
				if s.cancelErr == nil {
					return
				}
				err = s.cancelErr
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

	return s, nil
}

// WriteEvents writes events to the sink.
// This is an asynchronously and thread-safe method.
func (s *dmlSink) WriteEvents(txns ...*eventsink.CallbackableEvent[*model.SingleTableTxn]) error {
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
			partitionNum, err := s.alive.topicManager.GetPartitionNum(topic)
			failpoint.Inject("MQSinkGetPartitionError", func() {
				log.Info("failpoint MQSinkGetPartitionError injected", zap.String("changefeedID", s.id.ID))
				err = errors.New("MQSinkGetPartitionError")
			})
			if err != nil {
				s.cancelErr = err
				s.cancel()
				return errors.Trace(err)
			}
			partition := s.alive.eventRouter.GetPartitionForRowChange(row, partitionNum)
			// This never be blocked because this is an unbounded channel.
			s.alive.worker.msgChan.In() <- mqEvent{
				key: mqv1.TopicPartitionKey{
					Topic: topic, Partition: partition,
				},
				rowEvent: &eventsink.RowChangeCallbackableEvent{
					Event:     row,
					Callback:  callback,
					SinkState: txn.SinkState,
				},
			}
		}
	}

	return nil
}

func (s *dmlSink) Scheme() string {
	return s.scheme
}

// Close closes the sink.
func (s *dmlSink) Close() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()

	s.alive.RLock()
	s.alive.topicManager.Close()
	s.alive.RUnlock()
}

// Dead checks whether it's dead or not.
func (s *dmlSink) Dead() <-chan struct{} {
	return s.dead
}
