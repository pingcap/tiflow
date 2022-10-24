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
	"github.com/pingcap/tiflow/cdc/sink/codec"
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
	"go.uber.org/zap"
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

	worker *worker
	// eventRouter used to route events to the right topic and partition.
	eventRouter *dispatcher.EventRouter
	// topicManager used to manage topics.
	// It is also responsible for creating topics.
	topicManager manager.TopicManager

	// encoderBuilder builds encoder for the sink.
	encoderBuilder codec.EncoderBuilder
}

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
	encoder := encoderBuilder.Build()

	statistics := metrics.NewStatistics(ctx, sink.RowSink)
	w := newWorker(changefeedID, encoderConfig.Protocol, encoder, producer, statistics)

	s := &dmlSink{
		id:             changefeedID,
		protocol:       encoderConfig.Protocol,
		worker:         w,
		eventRouter:    eventRouter,
		topicManager:   topicManager,
		encoderBuilder: encoderBuilder,
	}

	// Spawn a goroutine to send messages by the worker.
	go func() {
		if err := s.worker.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("Error channel is full in DML sink", zap.Error(err),
					zap.String("namespace", changefeedID.Namespace),
					zap.String("changefeed", changefeedID.ID))
			}
		}
	}()

	return s, nil
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
		// This never be blocked because this is an unbounded channel.
		s.worker.msgChan.In() <- mqEvent{
			key: mqv1.TopicPartitionKey{
				Topic: topic, Partition: partition,
			},
			rowEvent: row,
		}
	}

	return nil
}

// Close closes the sink.
func (s *dmlSink) Close() error {
	s.worker.close()
	return nil
}
