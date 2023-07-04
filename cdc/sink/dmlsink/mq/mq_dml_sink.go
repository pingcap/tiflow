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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/dmlproducer"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/mq/manager"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/tablesink/state"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/pingcap/tiflow/pkg/sink/codec/builder"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// Assert EventSink[E event.TableEvent] implementation
var _ dmlsink.EventSink[*model.RowChangedEvent] = (*dmlSink)(nil)

// dmlSink is the mq sink.
// It will send the events to the MQ system.
type dmlSink struct {
	// id indicates this sink belongs to which processor(changefeed).
	id model.ChangeFeedID
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

	// adminClient is used to query kafka cluster information, it's shared among
	// multiple place, it's sink's responsibility to close it.
	adminClient kafka.ClusterAdminClient

	ctx    context.Context
	cancel context.CancelFunc

	wg   sync.WaitGroup
	dead chan struct{}
}

func newDMLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	producer dmlproducer.DMLProducer,
	adminClient kafka.ClusterAdminClient,
	topicManager manager.TopicManager,
	eventRouter *dispatcher.EventRouter,
	encoderConfig *common.Config,
	replicaConfig *config.ReplicaConfig,
	errCh chan error,
) (*dmlSink, error) {
	encoderBuilder, err := builder.NewRowEventEncoderBuilder(ctx, changefeedID, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	var claimCheck *ClaimCheck
	if replicaConfig.Sink.LargeMessageHandle.EnableClaimCheck() {
		storageURI := replicaConfig.Sink.LargeMessageHandle.ClaimCheckStorageURI
		claimCheck, err = NewClaimCheck(ctx, storageURI)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
		log.Info("claim-check enabled",
			zap.String("namespace", changefeedID.Namespace),
			zap.String("changefeed", changefeedID.ID),
			zap.String("storageURI", storageURI))
	}

	ctx, cancel := context.WithCancel(ctx)
	encoderConcurrency := util.GetOrZero(replicaConfig.Sink.EncoderConcurrency)
	statistics := metrics.NewStatistics(ctx, changefeedID, sink.RowSink)
	worker := newWorker(changefeedID, encoderConfig.Protocol,
		encoderBuilder, encoderConcurrency, producer, claimCheck, statistics)

	s := &dmlSink{
		id:          changefeedID,
		protocol:    encoderConfig.Protocol,
		adminClient: adminClient,
		ctx:         ctx,
		cancel:      cancel,
		dead:        make(chan struct{}),
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

		if err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
			case errCh <- err:
			}
		}
	}()

	return s, nil
}

// WriteEvents writes events to the sink.
// This is an asynchronously and thread-safe method.
func (s *dmlSink) WriteEvents(rows ...*dmlsink.RowChangeCallbackableEvent) error {
	s.alive.RLock()
	defer s.alive.RUnlock()
	if s.alive.isDead {
		return errors.Trace(errors.New("dead dmlSink"))
	}

	for _, row := range rows {
		if row.GetTableSinkState() != state.TableSinkSinking {
			// The table where the event comes from is in stopping, so it's safe
			// to drop the event directly.
			row.Callback()
			continue
		}
		topic := s.alive.eventRouter.GetTopicForRowChange(row.Event)
		partitionNum, err := s.alive.topicManager.GetPartitionNum(s.ctx, topic)
		if err != nil {
			return errors.Trace(err)
		}
		partition := s.alive.eventRouter.GetPartitionForRowChange(row.Event, partitionNum)
		// This never be blocked because this is an unbounded channel.
		s.alive.worker.msgChan.In() <- mqEvent{
			key: TopicPartitionKey{
				Topic: topic, Partition: partition,
			},
			rowEvent: row,
		}
	}

	return nil
}

// Close closes the sink.
func (s *dmlSink) Close() {
	if s.cancel != nil {
		s.cancel()
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
