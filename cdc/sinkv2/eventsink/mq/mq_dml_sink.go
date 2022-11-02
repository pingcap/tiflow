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

	"github.com/pingcap/errors"
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
	"github.com/pingcap/tiflow/pkg/hash"
	"github.com/pingcap/tiflow/pkg/sink"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.RowChangedEvent] = (*dmlSink)(nil)

// dmlSink is the mq sink.
// It will send the events to the MQ system.
type dmlSink struct {
	// id indicates this sink belongs to which processor(changefeed).
	id model.ChangeFeedID
	// protocol indicates the protocol used by this sink.
	protocol config.Protocol

	// eventRouter used to route events to the right topic and partition.
	eventRouter *dispatcher.EventRouter
	// topicManager used to manage topics.
	// It is also responsible for creating topics.
	topicManager manager.TopicManager
	producer     dmlproducer.DMLProducer

	workers []*worker
	hasher  *hash.PositionInertia
}

const (
	defaultWorkerNum = 8
)

func newSink(ctx context.Context,
	producer dmlproducer.DMLProducer,
	topicManager manager.TopicManager,
	eventRouter *dispatcher.EventRouter,
	encoderConfig *common.Config,
	errCh chan error,
) (*dmlSink, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)

	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	s := &dmlSink{
		id:           changefeedID,
		protocol:     encoderConfig.Protocol,
		eventRouter:  eventRouter,
		topicManager: topicManager,
		workers:      make([]*worker, defaultWorkerNum),
		producer:     producer,
		hasher:       hash.NewPositionInertia(),
	}

	statistics := metrics.NewStatistics(ctx, sink.RowSink)
	for i := 0; i < defaultWorkerNum; i++ {
		w := newWorker(i, changefeedID, encoderConfig.Protocol, encoderBuilder.Build(), producer, statistics)
		s.workers[i] = w
	}

	// Spawn a goroutine to send messages by the worker.
	go func() {
		if err := s.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("Error channel is full in DML sink",
					zap.String("namespace", changefeedID.Namespace),
					zap.String("changefeed", changefeedID.ID),
					zap.Error(err))
			}
		}
	}()

	return s, nil
}

func (s *dmlSink) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for _, w := range s.workers {
		w := w
		g.Go(func() error {
			return w.run(ctx)
		})
	}
	return g.Wait()
}

// WriteEvents writes events to the sink.
// This is an asynchronously and thread-safe method.
func (s *dmlSink) WriteEvents(rows ...*eventsink.RowChangeCallbackableEvent) error {
	for _, row := range rows {
		if row.GetTableSinkState() != state.TableSinkSinking {
			// The table where the event comes from is in stopping, so it's safe
			// to drop the event directly.
			row.Callback()
			continue
		}
		topic := s.eventRouter.GetTopicForRowChange(row.Event)
		partitionNum, err := s.topicManager.GetPartitionNum(topic)
		if err != nil {
			return errors.Trace(err)
		}
		partition := s.eventRouter.GetPartitionForRowChange(row.Event, partitionNum)
		index := s.getWorkerIndex(row.Event)
		// This never be blocked because this is an unbounded channel.
		s.workers[index].msgChan.In() <- mqEvent{
			key: mqv1.TopicPartitionKey{
				Topic: topic, Partition: partition,
			},
			rowEvent: row,
		}
	}

	return nil
}

func (s *dmlSink) getWorkerIndex(row *model.RowChangedEvent) int {
	s.hasher.Reset()
	s.hasher.Write([]byte(row.Table.Schema), []byte(row.Table.Table))

	dispatchCols := row.Columns
	if len(row.Columns) == 0 {
		dispatchCols = row.PreColumns
	}
	for _, col := range dispatchCols {
		if col == nil {
			continue
		}
		if col.Flag.IsHandleKey() {
			s.hasher.Write([]byte(col.Name), []byte(model.ColumnValueString(col.Value)))
		}
	}
	return int(s.hasher.Sum32()) % len(s.workers)
}

// Close closes the sink.
func (s *dmlSink) Close() error {
	for _, w := range s.workers {
		w.close()
	}
	s.producer.Close()
	return nil
}
