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
	mqv1 "github.com/pingcap/tiflow/cdc/sink/mq"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
	"github.com/pingcap/tiflow/cdc/sink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/mq/manager"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink/mq/producer"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// Assert EventSink[E event.TableEvent] implementation
var _ eventsink.EventSink[*model.RowChangedEvent] = (*sink)(nil)

// sink is the mq sink.
// It will send the events to the MQ system.
type sink struct {
	// id indicates this sink belongs to which processor(changefeed).
	id model.ChangeFeedID
	// role indicates this sink used for what.
	role util.Role
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

//nolint:deadcode
func newSink(ctx context.Context,
	producer producer.Producer,
	topicManager manager.TopicManager,
	eventRouter *dispatcher.EventRouter,
	encoderConfig *codec.Config,
	errCh chan error,
) (*sink, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	role := contextutil.RoleFromCtx(ctx)

	encoderBuilder, err := codec.NewEventBatchEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}
	encoder := encoderBuilder.Build()

	w := newWorker(changefeedID, encoder, producer)

	s := &sink{
		id:             changefeedID,
		role:           role,
		protocol:       encoderConfig.Protocol(),
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
				log.Error("error channel is full", zap.Error(err),
					zap.String("namespace", changefeedID.Namespace),
					zap.String("changefeed", changefeedID.ID),
					zap.Any("role", s.role))
			}
		}
	}()

	return s, nil
}

// WriteEvents writes events to the sink.
// This is an asynchronously and thread-safe method.
func (s *sink) WriteEvents(rows ...*eventsink.RowChangeCallbackableEvent) error {
	for _, row := range rows {
		topic := s.eventRouter.GetTopicForRowChange(row.Event)
		partitionNum, err := s.topicManager.GetPartitionNum(topic)
		if err != nil {
			return errors.Trace(err)
		}
		partition := s.eventRouter.GetPartitionForRowChange(row.Event, partitionNum)
		// This never be blocked.
		s.worker.msgChan.In() <- mqEvent{
			key: mqv1.TopicPartitionKey{
				Topic: topic, Partition: partition,
			},
			row: row,
		}
	}
	return nil
}

// Close closes the sink.
func (s *sink) Close() error {
	s.worker.close()
	return nil
}
