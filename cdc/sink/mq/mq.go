// Copyright 2020 PingCAP, Inc.
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

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/codec/builder"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/mq/dispatcher"
	"github.com/pingcap/tiflow/cdc/sink/mq/manager"
	"github.com/pingcap/tiflow/cdc/sink/mq/producer"
	"github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type resolvedTsEvent struct {
	tableID  model.TableID
	resolved model.ResolvedTs
}

type mqSink struct {
	mqProducer     producer.Producer
	eventRouter    *dispatcher.EventRouter
	encoderBuilder codec.EncoderBuilder
	protocol       config.Protocol

	topicManager         manager.TopicManager
	flushWorker          *flushWorker
	tableCheckpointTsMap sync.Map
	resolvedBuffer       *chann.DrainableChann[resolvedTsEvent]

	statistics *metrics.Statistics

	role util.Role
	id   model.ChangeFeedID
}

func newMqSink(
	ctx context.Context,
	topicManager manager.TopicManager,
	mqProducer producer.Producer,
	defaultTopic string,
	replicaConfig *config.ReplicaConfig, encoderConfig *common.Config,
	errCh chan error,
	changefeedID model.ChangeFeedID,
) (*mqSink, error) {
	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(ctx, encoderConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	eventRouter, err := dispatcher.NewEventRouter(replicaConfig, defaultTopic)
	if err != nil {
		return nil, errors.Trace(err)
	}

	captureAddr := contextutil.CaptureAddrFromCtx(ctx)
	role := contextutil.RoleFromCtx(ctx)

	encoder := encoderBuilder.Build()
	statistics := metrics.NewStatistics(ctx, captureAddr, metrics.SinkTypeMQ)
	flushWorker := newFlushWorker(encoder, mqProducer, statistics)

	s := &mqSink{
		mqProducer:     mqProducer,
		eventRouter:    eventRouter,
		encoderBuilder: encoderBuilder,
		protocol:       encoderConfig.Protocol,
		topicManager:   topicManager,
		flushWorker:    flushWorker,
		resolvedBuffer: chann.NewDrainableChann[resolvedTsEvent](),
		statistics:     statistics,
		role:           role,
		id:             changefeedID,
	}

	go func() {
		if err := s.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
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

func (k *mqSink) AddTable(tableID model.TableID) error {
	// We need to clean up the old values of the table,
	// otherwise when the table is dispatched back again,
	// it may read the old values.
	// See: https://github.com/pingcap/tiflow/issues/4464#issuecomment-1085385382.
	if checkpoint, loaded := k.tableCheckpointTsMap.LoadAndDelete(tableID); loaded {
		log.Info("clean up table checkpoint ts in MQ sink",
			zap.Int64("tableID", tableID),
			zap.Uint64("checkpointTs", checkpoint.(model.ResolvedTs).Ts))
	}

	return nil
}

// EmitRowChangedEvents emits row changed events to the flush worker by partition.
// Concurrency Note: This method is thread-safe.
func (k *mqSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	rowsCount := 0
	for _, row := range rows {
		topic := k.eventRouter.GetTopicForRowChange(row)
		partitionNum, err := k.topicManager.GetPartitionNum(topic)
		if err != nil {
			return errors.Trace(err)
		}
		partition := k.eventRouter.GetPartitionForRowChange(row, partitionNum)
		err = k.flushWorker.addEvent(ctx, mqEvent{
			row: row,
			key: TopicPartitionKey{
				Topic: topic, Partition: partition,
			},
		})
		if err != nil {
			return err
		}
		rowsCount++
	}
	k.statistics.AddRowsCount(rowsCount)
	return nil
}

// FlushRowChangedEvents asynchronously ensures
// that the data before the resolvedTs has been
// successfully written downstream.
// FlushRowChangedEvents is thread-safe.
func (k *mqSink) FlushRowChangedEvents(
	ctx context.Context, tableID model.TableID, resolved model.ResolvedTs,
) (model.ResolvedTs, error) {
	checkpoint := k.getTableCheckpointTs(tableID)
	if checkpoint.EqualOrGreater(resolved) {
		return checkpoint, nil
	}
	select {
	case <-ctx.Done():
		return model.NewResolvedTs(0), ctx.Err()
	case k.resolvedBuffer.In() <- resolvedTsEvent{
		tableID:  tableID,
		resolved: resolved,
	}:
	}
	k.statistics.PrintStatus(ctx)
	return checkpoint, nil
}

// bgFlushTs flush resolvedTs to workers and flush the mqProducer
func (k *mqSink) bgFlushTs(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case msg, ok := <-k.resolvedBuffer.Out():
			if !ok {
				log.Warn("resolved ts buffer is closed",
					zap.String("namespace", k.id.Namespace),
					zap.String("changefeed", k.id.ID),
					zap.Any("role", k.role))
				return nil
			}
			resolved := msg.resolved
			err := k.flushTsToWorker(ctx, resolved)
			if err != nil {
				return errors.Trace(err)
			}
			// Since CDC does not guarantee exactly once semantic, it won't cause any problem
			// here even if the table was moved or removed.
			// ref: https://github.com/pingcap/tiflow/pull/4356#discussion_r787405134
			k.tableCheckpointTsMap.Store(msg.tableID, resolved)
		}
	}
}

func (k *mqSink) flushTsToWorker(ctx context.Context, resolvedTs model.ResolvedTs) error {
	flushed := make(chan struct{})
	flush := &flushEvent{
		resolvedTs: resolvedTs,
		flushed:    flushed,
	}
	if err := k.flushWorker.addEvent(ctx, mqEvent{flush: flush}); err != nil {
		if errors.Cause(err) != context.Canceled {
			log.Warn("failed to flush TS to worker", zap.Error(err))
		} else {
			log.Debug("flushing TS to worker has been canceled", zap.Error(err))
		}
		return err
	}
	// We must wait until all messages have been acked before returning.
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case <-flushed:
	}
	return nil
}

// EmitCheckpointTs emits the checkpointTs to
// default topic or the topics of all tables.
// Concurrency Note: EmitCheckpointTs is thread-safe.
func (k *mqSink) EmitCheckpointTs(ctx context.Context, ts uint64, tables []*model.TableInfo) error {
	encoder := k.encoderBuilder.Build()
	msg, err := encoder.EncodeCheckpointEvent(ts)
	if err != nil {
		return errors.Trace(err)
	}
	if msg == nil {
		return nil
	}
	// NOTICE: When there is no table sync,
	// we need to send checkpoint ts to the default topic. T
	// This will be compatible with the old behavior.
	if len(tables) == 0 {
		topic := k.eventRouter.GetDefaultTopic()
		partitionNum, err := k.topicManager.GetPartitionNum(topic)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("emit checkpointTs to default topic",
			zap.String("topic", topic), zap.Uint64("checkpointTs", ts))
		err = k.mqProducer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
		return errors.Trace(err)
	}
	var tableNames []model.TableName
	for _, table := range tables {
		tableNames = append(tableNames, table.TableName)
	}
	topics := k.eventRouter.GetActiveTopics(tableNames)
	log.Debug("MQ sink current active topics", zap.Any("topics", topics))
	for _, topic := range topics {
		partitionNum, err := k.topicManager.GetPartitionNum(topic)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("emit checkpointTs to active topic",
			zap.String("topic", topic), zap.Uint64("checkpointTs", ts))
		err = k.mqProducer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// EmitDDLEvent sends a DDL event to the default topic or the table's corresponding topic.
// Concurrency Note: EmitDDLEvent is thread-safe.
func (k *mqSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	encoder := k.encoderBuilder.Build()
	msg, err := encoder.EncodeDDLEvent(ddl)
	if err != nil {
		return errors.Trace(err)
	}
	if msg == nil {
		return nil
	}

	topic := k.eventRouter.GetTopicForDDL(ddl)
	partitionRule := k.eventRouter.GetDLLDispatchRuleByProtocol(k.protocol)
	k.statistics.AddDDLCount()
	log.Debug("emit ddl event",
		zap.Uint64("commitTs", ddl.CommitTs),
		zap.String("query", ddl.Query),
		zap.String("namespace", k.id.Namespace),
		zap.String("changefeed", k.id.ID),
		zap.Any("role", k.role))
	if partitionRule == dispatcher.PartitionAll {
		partitionNum, err := k.topicManager.GetPartitionNum(topic)
		if err != nil {
			return errors.Trace(err)
		}
		err = k.mqProducer.SyncBroadcastMessage(ctx, topic, partitionNum, msg)
		return errors.Trace(err)
	}
	// Notice: We must call GetPartitionNum here,
	// which will be responsible for automatically creating topics when they don't exist.
	// If it is not called here and kafka has `auto.create.topics.enable` turned on,
	// then the auto-created topic will not be created as configured by ticdc.
	_, err = k.topicManager.GetPartitionNum(topic)
	if err != nil {
		return errors.Trace(err)
	}
	err = k.asyncFlushToPartitionZero(ctx, topic, msg)
	return errors.Trace(err)
}

// Close closes the sink.
// It is only called in the processor, and the processor destroys the
// table sinks before closing it. So there is no writing after closing.
func (k *mqSink) Close(_ context.Context) error {
	k.resolvedBuffer.CloseAndDrain()
	// NOTICE: We must close the resolved buffer before closing the flush worker.
	// Otherwise, bgFlushTs method will panic.
	k.flushWorker.close()
	k.topicManager.Close()
	// We need to close it asynchronously.
	// Otherwise, we might get stuck with it in an unhealthy state of kafka.
	go k.mqProducer.Close()
	return nil
}

func (k *mqSink) RemoveTable(cxt context.Context, tableID model.TableID) error {
	// RemoveTable does nothing because FlushRowChangedEvents in mq sink had flushed
	// all buffered events by force.
	return nil
}

func (k *mqSink) getTableCheckpointTs(tableID model.TableID) model.ResolvedTs {
	v, ok := k.tableCheckpointTsMap.Load(tableID)
	if ok {
		return v.(model.ResolvedTs)
	}
	return model.NewResolvedTs(0)
}

func (k *mqSink) run(ctx context.Context) error {
	wg, ctx := errgroup.WithContext(ctx)
	wg.Go(func() error {
		return k.bgFlushTs(ctx)
	})
	wg.Go(func() error {
		return k.flushWorker.run(ctx)
	})
	return wg.Wait()
}

// asyncFlushToPartitionZero writes message to
// partition zero asynchronously and flush it immediately.
func (k *mqSink) asyncFlushToPartitionZero(
	ctx context.Context, topic string, message *common.Message,
) error {
	err := k.mqProducer.AsyncSendMessage(ctx, topic, dispatcher.PartitionZero, message)
	if err != nil {
		return err
	}
	return k.mqProducer.Flush(ctx)
}

// NewKafkaSaramaSink creates a new Kafka mqSink.
func NewKafkaSaramaSink(ctx context.Context, sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	errCh chan error, changefeedID model.ChangeFeedID,
) (*mqSink, error) {
	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return nil, cerror.ErrKafkaInvalidConfig.GenWithStack("no topic is specified in sink-uri")
	}

	baseConfig := kafka.NewConfig()
	if err := baseConfig.Apply(sinkURI, replicaConfig); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	saramaConfig, err := kafka.NewSaramaConfig(ctx, baseConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	adminClient, err := kafka.NewAdminClientImpl(baseConfig.BrokerEndpoints, saramaConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	// we must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to an goroutine leak
	defer func() {
		if err != nil {
			adminClient.Close()
		}
	}()

	if err := kafka.AdjustConfig(adminClient, baseConfig, saramaConfig, topic); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	protocol, err := config.ParseSinkProtocolFromString(replicaConfig.Sink.Protocol)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	encoderConfig := common.NewConfig(protocol)
	if err := encoderConfig.Apply(sinkURI, replicaConfig); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}
	// always set encoder's `MaxMessageBytes` equal to producer's `MaxMessageBytes`
	// to prevent that the encoder generate batched message too large then cause producer meet `message too large`
	encoderConfig = encoderConfig.WithMaxMessageBytes(saramaConfig.Producer.MaxMessageBytes)

	if err := encoderConfig.Validate(); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	client, err := sarama.NewClient(baseConfig.BrokerEndpoints, saramaConfig)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	topicManager := manager.NewKafkaTopicManager(ctx, topic, adminClient, baseConfig.DeriveTopicConfig())
	if _, err := topicManager.CreateTopicAndWaitUntilVisible(topic); err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaCreateTopic, err)
	}

	sProducer, err := kafka.NewKafkaSaramaProducer(
		ctx,
		client,
		adminClient,
		baseConfig,
		saramaConfig,
		errCh,
		changefeedID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sink, err := newMqSink(
		ctx,
		topicManager,
		sProducer,
		topic,
		replicaConfig,
		encoderConfig,
		errCh,
		changefeedID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}
